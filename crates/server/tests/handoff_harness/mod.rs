//! Reusable end-to-end harness for the `handoff` integration.
//!
//! Every test that wants to exercise the real moving parts (process spawning,
//! listener-FD inheritance, the flock dance, data persistence across a binary
//! swap) goes through this module. Each scenario is one method call; the
//! harness owns lifecycle (spawn, wait-ready, reap), the supervisor, and the
//! listener FD that survives across primitive processes.
//!
//! Modeled after `/home/jared/kv/crates/server/tests/handoff_harness/mod.rs`,
//! with the kv-specific bits (RESP, redis client, multi-shard, BGREWRITEAOF)
//! stripped and replaced by HTTP PUT/GET against the objects REST API.

#![allow(dead_code)]

use std::io::ErrorKind;
use std::net::{SocketAddr, TcpListener};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use handoff::supervisor::{SpawnSpec, Supervisor};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tempfile::TempDir;

/// The compiled `beyond-objects` binary path. Set by cargo for integration tests
/// of the `beyond-objects` package.
const OBJECTS_BINARY: &str = env!("CARGO_BIN_EXE_beyond-objects");

/// Default bucket created at cold-start so upload tests have a target.
pub const TEST_BUCKET: &str = "test";

/// What happened from one `Harness::handoff()` call.
#[derive(Debug)]
pub struct HandoffSummary {
    pub committed: bool,
    pub abort_reason: Option<String>,
    pub handoff_id: handoff::HandoffId,
    pub elapsed: Duration,
}

/// One in-progress handoff scenario.
///
/// Owns:
/// - A temporary data dir (always on tmpfs / cargo target dir).
/// - The Unix-domain control socket path.
/// - The HTTP listener FD that survives across primitive processes.
/// - The currently-tracked beyond-objects [`Child`] handle.
/// - A [`Supervisor`] pre-loaded with the listener FD and journal path.
pub struct Harness {
    binary: PathBuf,
    _temp: TempDir,
    data_dir: PathBuf,
    index_dir: PathBuf,
    control_socket: PathBuf,
    journal_path: PathBuf,
    http_listener: TcpListener,
    http_addr: SocketAddr,
    root_token: String,
    extra_args: Vec<String>,
    /// `Some` for the very first (cold-start) child. After a committed
    /// handoff this becomes `None` — the successor's `Child` handle was
    /// dropped inside `perform_handoff`.
    current: Option<Child>,
    supervisor: Arc<Supervisor>,
    bucket_created: bool,
}

impl Harness {
    /// Allocate ephemeral port + temp dir + listener. Does **not** start
    /// beyond-objects yet (call [`cold_start`](Self::cold_start)).
    pub fn new() -> Self {
        let binary = PathBuf::from(OBJECTS_BINARY);
        let temp = tempfile::tempdir().expect("tempdir");
        let data_dir = temp.path().join("data");
        let index_dir = temp.path().join("index");
        let control_socket = temp.path().join("control.sock");
        let journal_path = temp.path().join("handoff-state.bin");

        let http_listener = TcpListener::bind("127.0.0.1:0").expect("bind http");
        let http_addr = http_listener.local_addr().unwrap();

        let supervisor = Supervisor::new(&control_socket)
            .expect("Supervisor::new")
            .with_listener("http", http_listener.as_raw_fd())
            .with_journal(journal_path.clone());
        let supervisor = Arc::new(supervisor);

        // Random root token per harness so we never accidentally hit a real
        // server with a baked-in test secret. uuid v4 is cryptographic-grade
        // random under the hood (via getrandom); plenty for a test token.
        let root_token = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );

        Self {
            binary,
            _temp: temp,
            data_dir,
            index_dir,
            control_socket,
            journal_path,
            http_listener,
            http_addr,
            root_token,
            extra_args: Vec::new(),
            current: None,
            supervisor,
            bucket_created: false,
        }
    }

    // ── Lifecycle ────────────────────────────────────────────────────────

    /// Spawn the first beyond-objects process (no `HANDOFF_ROLE`, so
    /// `Role::ColdStart`). Blocks until the control socket appears and
    /// `/livez` is reachable; creates the default `TEST_BUCKET`.
    pub fn cold_start(&mut self) -> &mut Self {
        self.cold_start_with_env(Vec::new())
    }

    /// Like [`cold_start`](Self::cold_start) but with extra env vars passed
    /// to the cold-start child.
    pub fn cold_start_with_env(&mut self, env: Vec<(String, String)>) -> &mut Self {
        assert!(self.current.is_none(), "beyond-objects already running");
        let listener_fds = vec![("http".to_string(), self.http_listener.as_raw_fd())];
        let args = self.objects_args();
        let mut env_with_token = env.clone();
        env_with_token.push(("OBJECTS_ROOT_TOKEN".to_string(), self.root_token.clone()));
        let child = spawn_cold_start_with_inherited_and_env(
            &self.binary,
            &args,
            &listener_fds,
            &env_with_token,
        );
        self.current = Some(child);
        self.wait_ready();
        if !self.bucket_created {
            self.create_bucket(TEST_BUCKET);
            self.bucket_created = true;
        }
        self
    }

    /// Drive a full happy-path handoff: spawn successor, run Hello → Commit.
    /// Reaps the old child if the handoff commits. Blocks until the
    /// successor is reachable on the same port.
    pub fn handoff(&mut self) -> HandoffSummary {
        self.handoff_with_env(Vec::new())
    }

    /// Like [`handoff`](Self::handoff) but with extra env vars passed to the
    /// successor process. Used by tests that inject faults via env-var hooks
    /// (e.g. `OBJECTS_TEST_PANIC_BEFORE_READY=1`).
    pub fn handoff_with_env(&mut self, env: Vec<(String, String)>) -> HandoffSummary {
        let started = Instant::now();
        let args = self.objects_args();
        let mut env_with_token = env;
        env_with_token.push(("OBJECTS_ROOT_TOKEN".to_string(), self.root_token.clone()));
        let spec = SpawnSpec {
            binary: self.binary.clone(),
            args,
            env: env_with_token,
            deadline: Duration::from_secs(15),
            drain_grace: Duration::from_secs(5),
        };
        let mut outcome = self
            .supervisor
            .perform_handoff(spec)
            .expect("perform_handoff");

        if outcome.committed {
            if let Some(mut old) = self.current.take() {
                let _ = old.wait();
            }
            self.current = outcome.child.take();
            self.wait_ready();
        }
        // On abort, the old child is still alive and serving.

        HandoffSummary {
            committed: outcome.committed,
            abort_reason: outcome.abort_reason,
            handoff_id: outcome.handoff_id,
            elapsed: started.elapsed(),
        }
    }

    /// Block until the control socket exists and `/livez` answers 200.
    pub fn wait_ready(&self) {
        assert!(
            wait_for_path(&self.control_socket, Duration::from_secs(10)),
            "control socket {:?} never appeared",
            self.control_socket
        );
        wait_for_tcp(self.http_addr, Duration::from_secs(10));
        // Probe /livez with a short timeout so we don't move on while the
        // axum router is still warming up.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let agent = ureq::AgentBuilder::new()
                .timeout(Duration::from_secs(1))
                .build();
            match agent
                .get(&format!("http://{}/livez", self.http_addr))
                .call()
            {
                Ok(_) => return,
                Err(_) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(e) => panic!("/livez never returned 200: {e}"),
            }
        }
    }

    /// Kill the currently-tracked child (best-effort).
    pub fn kill_current(&mut self) {
        if let Some(mut c) = self.current.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// SIGKILL the current child (simulates a hard crash). The flock is
    /// released by the kernel; the pidfile remains as a stale hint.
    pub fn sigkill_current(&mut self) {
        if let Some(mut c) = self.current.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// Cold-start again on the same data dir + listener. Required to
    /// exercise `acquire_or_break_stale` after `sigkill_current`.
    pub fn cold_start_after_crash(&mut self) -> &mut Self {
        assert!(self.current.is_none(), "kill current child first");
        let listener_fds = vec![("http".to_string(), self.http_listener.as_raw_fd())];
        let args = self.objects_args();
        let env = vec![("OBJECTS_ROOT_TOKEN".to_string(), self.root_token.clone())];
        let child =
            spawn_cold_start_with_inherited_and_env(&self.binary, &args, &listener_fds, &env);
        self.current = Some(child);
        self.wait_ready();
        self
    }

    // ── Inspection ───────────────────────────────────────────────────────

    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }
    pub fn http_url(&self) -> String {
        format!("http://{}", self.http_addr)
    }
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
    pub fn control_socket(&self) -> &Path {
        &self.control_socket
    }
    pub fn journal_path(&self) -> &Path {
        &self.journal_path
    }
    pub fn root_token(&self) -> &str {
        &self.root_token
    }
    pub fn current_pid(&self) -> Option<u32> {
        self.current.as_ref().map(|c| c.id())
    }
    pub fn supervisor(&self) -> Arc<Supervisor> {
        Arc::clone(&self.supervisor)
    }

    /// `HMAC-SHA256(root_token, bucket)` in lowercase hex — the token clients
    /// present when writing/reading objects in `bucket`.
    pub fn bucket_token(&self, bucket: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.root_token.as_bytes()).expect("hmac new");
        mac.update(bucket.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Build a `SpawnSpec` matching the harness's defaults — used by tests
    /// that call `perform_handoff` directly.
    pub fn make_spawn_spec(&self) -> SpawnSpec {
        let env = vec![("OBJECTS_ROOT_TOKEN".to_string(), self.root_token.clone())];
        SpawnSpec {
            binary: self.binary.clone(),
            args: self.objects_args(),
            env,
            deadline: Duration::from_secs(15),
            drain_grace: Duration::from_secs(5),
        }
    }

    /// Append additional CLI args to every objects spawn (both cold-start
    /// and handoff). Use before `cold_start`.
    pub fn with_extra_args(mut self, args: Vec<String>) -> Self {
        assert!(self.current.is_none(), "set extra args before cold_start");
        self.extra_args = args;
        self
    }

    /// Try to start a second beyond-objects process pointed at the same
    /// data dir, on a *different* ephemeral port and a *different* handoff
    /// socket. No FD inheritance, no supervisor coordination — just a plain
    /// process that should refuse to start because the data-dir lock is held.
    pub fn try_spawn_competitor(&self) -> Child {
        let extra = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = extra.local_addr().unwrap().port();
        drop(extra); // free the port; competitor will bind it itself
        let other_socket = self._temp.path().join("competitor-control.sock");

        let mut cmd = Command::new(&self.binary);
        cmd.args([
            "serve",
            "--data-dir",
            self.data_dir.to_str().unwrap(),
            "--index-dir",
            self.index_dir.to_str().unwrap(),
            "--address",
            &format!("127.0.0.1:{port}"),
            "--handoff-socket-path",
            other_socket.to_str().unwrap(),
        ]);
        cmd.env("OBJECTS_ROOT_TOKEN", &self.root_token);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd.spawn().expect("spawn competitor")
    }

    // ── Internals ────────────────────────────────────────────────────────

    fn objects_args(&self) -> Vec<String> {
        let mut v = vec![
            "serve".into(),
            "--data-dir".into(),
            self.data_dir.to_str().unwrap().into(),
            "--index-dir".into(),
            self.index_dir.to_str().unwrap().into(),
            "--address".into(),
            self.http_addr.to_string(),
            "--handoff-socket-path".into(),
            self.control_socket.to_str().unwrap().into(),
        ];
        v.extend(self.extra_args.iter().cloned());
        v
    }

    fn create_bucket(&self, bucket: &str) {
        let url = format!("{}/v1/buckets", self.http_url());
        let body = format!("{{\"name\":\"{bucket}\"}}");
        let agent = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(5))
            .build();
        let resp = agent
            .post(&url)
            .set("Authorization", &format!("Bearer {}", self.root_token))
            .set("Content-Type", "application/json")
            .send_string(&body);
        match resp {
            Ok(r) => assert!(
                (200..300).contains(&r.status()),
                "create_bucket got status {}",
                r.status()
            ),
            Err(ureq::Error::Status(409, _)) => {} // already exists
            Err(e) => panic!("create_bucket: {e}"),
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.kill_current();
    }
}

// ── Free helpers ─────────────────────────────────────────────────────────

/// Cold-start spawn that mirrors the production supervisor's FD inheritance:
/// `dup2` each listener FD into FD 3..3+N in the child via `pre_exec`,
/// clearing `FD_CLOEXEC` so the FDs survive `execve`.
pub fn spawn_cold_start_with_inherited(
    binary: &Path,
    args: &[String],
    listener_fds: &[(String, RawFd)],
) -> Child {
    spawn_cold_start_with_inherited_and_env(binary, args, listener_fds, &[])
}

pub fn spawn_cold_start_with_inherited_and_env(
    binary: &Path,
    args: &[String],
    listener_fds: &[(String, RawFd)],
    extra_env: &[(String, String)],
) -> Child {
    let mut cmd = Command::new(binary);
    cmd.args(args);
    let names: Vec<String> = listener_fds.iter().map(|(n, _)| n.clone()).collect();
    cmd.env("LISTEN_FDS", listener_fds.len().to_string());
    cmd.env("LISTEN_FDNAMES", names.join(":"));
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.stdin(Stdio::null()).stdout(Stdio::null());
    if std::env::var("OBJECTS_TEST_LOGS").is_ok() {
        cmd.stderr(Stdio::inherit());
    } else {
        cmd.stderr(Stdio::null());
    }

    let sources: Vec<RawFd> = listener_fds.iter().map(|(_, f)| *f).collect();
    // SAFETY: `pre_exec` runs in the forked child before `execve`. Only
    // async-signal-safe libc calls; no allocations.
    unsafe {
        cmd.pre_exec(move || {
            for (i, src) in sources.iter().enumerate() {
                let dst = 3 + i as RawFd;
                if *src == dst {
                    if libc::fcntl(*src, libc::F_SETFD, 0) == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                } else if libc::dup2(*src, dst) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    cmd.spawn().expect("spawn beyond-objects (cold start)")
}

/// Wait for `path` to exist, polling at 25 ms.
pub fn wait_for_path(path: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while !path.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(25));
    }
    path.exists()
}

/// Wait until a TCP connection to `addr` succeeds.
pub fn wait_for_tcp(addr: SocketAddr, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        match std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(250)) {
            Ok(_) => return,
            Err(e) if Instant::now() < deadline => {
                let _ = e;
                thread::sleep(Duration::from_millis(25));
            }
            Err(e) if e.kind() == ErrorKind::TimedOut => continue,
            Err(e) => panic!("wait_for_tcp({addr}): {e}"),
        }
    }
}

// ── HTTP helpers ─────────────────────────────────────────────────────────

/// Blocking HTTP PUT object. Returns the status code; returns 0 for transport
/// errors (connection drops during a handoff window) so callers can treat them
/// as ordinary non-2xx failures and retry.
pub fn http_put(addr: SocketAddr, token: &str, bucket: &str, key: &str, body: &[u8]) -> u16 {
    let url = format!("http://{addr}/v1/{bucket}/{key}");
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(10))
        .build();
    match agent
        .put(&url)
        .set("Authorization", &format!("Bearer {token}"))
        .set("Content-Type", "application/octet-stream")
        .send_bytes(body)
    {
        Ok(resp) => resp.status(),
        Err(ureq::Error::Status(code, _)) => code,
        Err(ureq::Error::Transport(_)) => 0,
    }
}

/// Blocking HTTP GET object. Returns `Some(body bytes)` for 200, `None` for 404.
pub fn http_get(addr: SocketAddr, token: &str, bucket: &str, key: &str) -> Option<Vec<u8>> {
    let url = format!("http://{addr}/v1/{bucket}/{key}");
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(10))
        .build();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match agent
            .get(&url)
            .set("Authorization", &format!("Bearer {token}"))
            .call()
        {
            Ok(resp) => {
                let mut buf = Vec::new();
                resp.into_reader().read_to_end(&mut buf).expect("read body");
                return Some(buf);
            }
            Err(ureq::Error::Status(404, _)) => return None,
            Err(ureq::Error::Transport(_)) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(25));
            }
            Err(e) => panic!("http_get({url}): {e}"),
        }
    }
}

use std::io::Read as _;

// ── Uploader ─────────────────────────────────────────────────────────────

/// One acked upload the [`Uploader`] has produced.
#[derive(Debug, Clone)]
pub struct AckedUpload {
    pub key: String,
    pub body: Vec<u8>,
}

/// Stats collected by [`Uploader::stop`].
#[derive(Debug)]
pub struct UploaderResult {
    /// Every (key, body) pair that the server returned 2xx for.
    /// Post-handoff, `GET key` MUST return `body` for every entry here.
    pub acked: Vec<AckedUpload>,
    /// Count of PUT attempts that failed (transport or 5xx).
    pub errors: u64,
    /// Time the uploader was active.
    pub elapsed: Duration,
}

/// Background uploader thread. Continuously PUTs `obj-<N>` with body `body-<N>`
/// as fast as it can, recording each ack. Reconnects on transport error.
pub struct Uploader {
    handle: Option<thread::JoinHandle<UploaderResult>>,
    stop: Arc<AtomicBool>,
    acked_count: Arc<AtomicU64>,
}

impl Uploader {
    pub fn start(addr: SocketAddr, token: String, bucket: String) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let acked_count = Arc::new(AtomicU64::new(0));
        let acked_snapshot = Arc::new(Mutex::new(Vec::<AckedUpload>::new()));
        let stop_for_thread = Arc::clone(&stop);
        let count_for_thread = Arc::clone(&acked_count);
        let acked_for_thread = Arc::clone(&acked_snapshot);

        let handle = thread::Builder::new()
            .name("objects-handoff-uploader".into())
            .spawn(move || {
                let started = Instant::now();
                let mut errors = 0u64;
                let mut seq = 0u64;

                while !stop_for_thread.load(Ordering::Relaxed) {
                    let key = format!("obj-{seq}");
                    let body = format!("body-{seq}").into_bytes();
                    let status = http_put(addr, &token, &bucket, &key, &body);
                    if (200..300).contains(&status) {
                        acked_for_thread.lock().unwrap().push(AckedUpload {
                            key: key.clone(),
                            body,
                        });
                        count_for_thread.fetch_add(1, Ordering::Relaxed);
                        seq += 1;
                    } else {
                        errors += 1;
                        thread::sleep(Duration::from_millis(2));
                    }
                }
                let acked = acked_for_thread.lock().unwrap().clone();
                UploaderResult {
                    acked,
                    errors,
                    elapsed: started.elapsed(),
                }
            })
            .expect("spawn uploader thread");

        Self {
            handle: Some(handle),
            stop,
            acked_count,
        }
    }

    pub fn acked_count(&self) -> u64 {
        self.acked_count.load(Ordering::Relaxed)
    }

    pub fn stop(mut self) -> UploaderResult {
        self.stop.store(true, Ordering::SeqCst);
        self.handle
            .take()
            .expect("handle")
            .join()
            .expect("uploader panic")
    }
}

impl Drop for Uploader {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}
