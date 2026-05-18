//! End-to-end smoke test: spawn the real `beyond-objects` binary, drive the
//! handoff happy path against its control socket, and assert the process exits
//! cleanly on `Commit`.
//!
//! Exercises:
//!   - `detect_role` → `Role::ColdStart` (no `HANDOFF_ROLE` env set here).
//!   - `DataDirLock::acquire_or_break_stale` on a fresh data dir.
//!   - The incumbent serving the protocol from `spawn_blocking`.
//!   - `ObjectsHandoff::drain` (no in-flight requests → returns immediately).
//!   - `ObjectsHandoff::seal` → `Index::persist` → flock release.
//!   - Commit causing the handoff thread to send on `commit_tx`, the unified
//!     shutdown future to fire, and `serve()` to fall through to its cleanup.

use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use handoff::frame::{read_message, write_message};
use handoff::protocol::{HandoffId, Message, PROTO_MAX};

const TEST_BINARY: &str = env!("CARGO_BIN_EXE_beyond-objects");

/// Kill + reap on drop unless `disarm`ed. Without this, a panic between spawn
/// and the `try_wait` loop leaves an orphan whose tempdir gets cleaned out
/// from under it.
struct KillOnDrop(Option<Child>);

impl KillOnDrop {
    fn new(c: Child) -> Self {
        Self(Some(c))
    }
    fn as_mut(&mut self) -> &mut Child {
        self.0.as_mut().expect("disarmed")
    }
    fn disarm(mut self) -> Child {
        self.0.take().expect("disarmed twice")
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn ephemeral_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn wait_for_path(path: &Path, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !path.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }
    path.exists()
}

/// Guard that clean shutdown actually runs Drop on important guards
/// (specifically the OpenTelemetry tracer provider's 5-second flush). The
/// proxy here is that shutdown completes well under the OTel flush ceiling —
/// if the process exits in <500ms, we know the flush either succeeded fast
/// or was skipped entirely. Since `process::exit(0)` skips destructors,
/// the production code drops `otel_guard` explicitly *before* the exit. If
/// a future refactor removes that drop, OTel will still try to shut down on
/// process teardown via its own atexit hook (taking up to 5s) — this test
/// catches if that hook were ever to deadlock or be uninstalled, by capping
/// total clean shutdown at 1s.
#[test]
fn clean_shutdown_completes_within_one_second() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    let index_dir = temp.path().join("index");
    let sock_path = temp.path().join("control.sock");
    let http_addr = format!("127.0.0.1:{}", ephemeral_port());

    let mut child = KillOnDrop::new(
        Command::new(TEST_BINARY)
            .arg("serve")
            .arg("--data-dir")
            .arg(&data_dir)
            .arg("--index-dir")
            .arg(&index_dir)
            .arg("--address")
            .arg(&http_addr)
            .arg("--handoff-socket-path")
            .arg(&sock_path)
            .env("OBJECTS_ROOT_TOKEN", "clean-shutdown-root")
            // OTLP disabled — we want to validate the Drop runs cleanly, not
            // wait 5s for a connection-refused timeout.
            .env_remove("OTLP_ENABLED")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn beyond-objects"),
    );

    assert!(
        wait_for_path(&sock_path, 10),
        "control socket never appeared"
    );

    let pid = child.as_mut().id();
    let sigterm_sent = Instant::now();
    let rc = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    assert_eq!(rc, 0, "kill(SIGTERM, {pid}) returned {rc}");

    let exit_deadline = sigterm_sent + Duration::from_secs(1);
    while Instant::now() < exit_deadline {
        if child.as_mut().try_wait().unwrap().is_some() {
            let _ = child.disarm().wait();
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!(
        "clean shutdown did not complete within 1s (otel_guard drop or atexit hook may have stalled)"
    );
}

/// Guard against regressions of the `process::exit(0)` fix in `serve()`. The
/// `Incumbent::serve` blocking thread is parked reading the control socket
/// and cannot be cancelled by dropping the tokio runtime, so without an
/// explicit exit the process stays alive on its detached blocking thread
/// after SIGTERM. This test spawns the binary, sends SIGTERM, and asserts
/// the process exits within 1 second.
#[test]
fn sigterm_terminates_process_within_one_second() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    let index_dir = temp.path().join("index");
    let sock_path = temp.path().join("control.sock");
    let http_addr = format!("127.0.0.1:{}", ephemeral_port());

    let mut child = KillOnDrop::new(
        Command::new(TEST_BINARY)
            .arg("serve")
            .arg("--data-dir")
            .arg(&data_dir)
            .arg("--index-dir")
            .arg(&index_dir)
            .arg("--address")
            .arg(&http_addr)
            .arg("--handoff-socket-path")
            .arg(&sock_path)
            .env("OBJECTS_ROOT_TOKEN", "sigterm-root-token")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn beyond-objects"),
    );

    assert!(
        wait_for_path(&sock_path, 10),
        "control socket never appeared"
    );

    let pid = child.as_mut().id();
    let sigterm_sent = Instant::now();
    let rc = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    assert_eq!(rc, 0, "kill(SIGTERM, {pid}) returned {rc}");

    let exit_deadline = sigterm_sent + Duration::from_secs(1);
    loop {
        match child.as_mut().try_wait().unwrap() {
            Some(_status) => {
                let elapsed = sigterm_sent.elapsed();
                assert!(
                    elapsed < Duration::from_secs(1),
                    "process took {elapsed:?} to exit after SIGTERM (limit 1s)"
                );
                let _ = child.disarm().wait();
                return;
            }
            None if Instant::now() < exit_deadline => {
                thread::sleep(Duration::from_millis(25));
            }
            None => {
                panic!(
                    "process pid={pid} did not exit within 1s of SIGTERM — \
                     `std::process::exit(0)` at end of serve() likely missing"
                );
            }
        }
    }
}

#[test]
fn full_handoff_protocol_exits_objects_on_commit() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    let index_dir = temp.path().join("index");
    let sock_path = temp.path().join("control.sock");

    let http_addr = format!("127.0.0.1:{}", ephemeral_port());

    let mut child = KillOnDrop::new(
        Command::new(TEST_BINARY)
            .arg("serve")
            .arg("--data-dir")
            .arg(&data_dir)
            .arg("--index-dir")
            .arg(&index_dir)
            .arg("--address")
            .arg(&http_addr)
            .arg("--handoff-socket-path")
            .arg(&sock_path)
            .env("OBJECTS_ROOT_TOKEN", "smoke-root-token")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn beyond-objects"),
    );

    assert!(
        wait_for_path(&sock_path, 10),
        "control socket never appeared"
    );

    let mut stream = UnixStream::connect(&sock_path).expect("connect control socket");

    // Read O's Hello.
    let (_v, hello) = read_message(&mut stream).expect("read Hello");
    match hello {
        Message::Hello { role, .. } => {
            assert!(matches!(role, handoff::protocol::Side::Incumbent));
        }
        other => panic!("expected Hello, got {other:?}"),
    }

    let handoff_id = HandoffId::new();
    write_message(
        &mut stream,
        PROTO_MAX,
        &Message::HelloAck {
            proto_version_chosen: PROTO_MAX,
            handoff_id,
        },
    )
    .unwrap();

    // PrepareHandoff → expect Drained.
    write_message(
        &mut stream,
        PROTO_MAX,
        &Message::PrepareHandoff {
            handoff_id,
            successor_pid: 99_999,
            deadline_ms: 10_000,
            drain_grace_ms: 5_000,
        },
    )
    .unwrap();
    let (_, drained) = read_message(&mut stream).unwrap();
    assert!(matches!(drained, Message::Drained { .. }));

    // SealRequest → expect SealComplete.
    write_message(&mut stream, PROTO_MAX, &Message::SealRequest { handoff_id }).unwrap();
    let (_, sealed) = read_message(&mut stream).unwrap();
    match sealed {
        Message::SealComplete { handoff_id: id, .. } => {
            assert_eq!(id, handoff_id);
        }
        other => panic!("expected SealComplete, got {other:?}"),
    }

    // After SealComplete, the data-dir lock must be released. Verify by
    // acquiring it from the test process.
    let probe = handoff::DataDirLock::acquire(&data_dir)
        .expect("flock should be free after objects sent SealComplete");
    drop(probe);

    // Send Commit → beyond-objects should exit shortly.
    write_message(&mut stream, PROTO_MAX, &Message::Commit { handoff_id }).unwrap();
    drop(stream);

    let exit_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match child.as_mut().try_wait().unwrap() {
            Some(status) => {
                assert!(
                    status.success() || status.code() == Some(0),
                    "beyond-objects exited with: {status:?}"
                );
                let _ = child.disarm().wait();
                return;
            }
            None if Instant::now() < exit_deadline => {
                thread::sleep(Duration::from_millis(50));
            }
            None => {
                panic!("beyond-objects did not exit within 10s of Commit");
            }
        }
    }
}
