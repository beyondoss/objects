import { type ChildProcess, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

let serverProcess: ChildProcess | undefined;
let tempDataDir: string | undefined;

function findFreePort(): Promise<number> {
  return new Promise((res, rej) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address() as { port: number };
      srv.close((err) => (err ? rej(err) : res(port)));
    });
    srv.on("error", rej);
  });
}

async function waitForHealthy(url: string, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${url}/livez`);
      if (res.ok) return;
    } catch {
      // server not up yet
    }
    await new Promise<void>((r) => setTimeout(r, 150));
  }
  throw new Error(
    `beyond-objects did not become healthy at ${url} within ${timeoutMs}ms`,
  );
}

export async function setup(): Promise<void> {
  const [httpPort, metricsPort] = await Promise.all([
    findFreePort(),
    findFreePort(),
  ]);

  tempDataDir = mkdtempSync(join(tmpdir(), "beyond-objects-test-"));
  const rootToken = randomUUID();
  const baseUrl = `http://127.0.0.1:${httpPort}`;

  const binaryPath = process.env["BEYOND_OBJECTS_BINARY"]
    ?? resolve(__dirname, "../../../target/debug/beyond-objects");

  serverProcess = spawn(binaryPath, ["serve"], {
    env: {
      ...process.env,
      OBJECTS_ROOT_TOKEN: rootToken,
      OBJECTS_DATA_DIR: tempDataDir,
      OBJECTS_INDEX_DIR: join(tempDataDir, ".index"),
      OBJECTS_HANDOFF_SOCKET_PATH: join(tempDataDir, "handoff.sock"),
      ADDRESS: `127.0.0.1:${httpPort}`,
      METRICS_ADDRESS: `127.0.0.1:${metricsPort}`,
      OBJECTS_URL: baseUrl,
      LOG_LEVEL: "error",
      RUST_LOG: "error",
    },
    stdio: ["pipe", "pipe", "inherit"],
  });

  serverProcess.on("error", (err) => {
    throw new Error(`Failed to start beyond-objects: ${err.message}`);
  });

  await waitForHealthy(baseUrl);

  process.env["OBJECTS_TEST_URL"] = baseUrl;
  process.env["OBJECTS_TEST_ROOT_TOKEN"] = rootToken;
}

export async function teardown(): Promise<void> {
  serverProcess?.kill("SIGTERM");
  serverProcess = undefined;
  if (tempDataDir != null) {
    rmSync(tempDataDir, { recursive: true, force: true });
    tempDataDir = undefined;
  }
}
