/**
 * mTLS end-to-end test.
 *
 * Spins up a real beyond-objects binary with TLS enabled, generates ephemeral
 * CA / server / client certificates via @peculiar/x509 + WebCrypto, and
 * verifies that:
 *   1. A client configured with `tls` options can connect and make real requests.
 *   2. A client without TLS options cannot connect (connection error / rejection).
 */

// @peculiar/x509 depends on tsyringe which requires Reflect metadata.
import "reflect-metadata";

import {
  BasicConstraintsExtension,
  ExtendedKeyUsageExtension,
  SubjectAlternativeNameExtension,
  X509CertificateGenerator,
} from "@peculiar/x509";
import { type ChildProcess, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createObjectsClient } from "../src/index.js";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

// ── Certificate generation ───────────────────────────────────────────────────

interface CertBundle {
  caPem: string;
  serverCertPem: string;
  serverKeyPem: string;
  clientCertPem: string;
  clientKeyPem: string;
}

function toPem(label: string, der: ArrayBuffer): string {
  const b64 = Buffer.from(der).toString("base64");
  return `-----BEGIN ${label}-----\n${
    b64.match(/.{1,64}/g)!.join("\n")
  }\n-----END ${label}-----\n`;
}

async function generateTestCerts(): Promise<CertBundle> {
  const alg = { name: "ECDSA", namedCurve: "P-256" };
  const signingAlgorithm = { name: "ECDSA", hash: "SHA-256" } as EcdsaParams;
  const notBefore = new Date("2020-01-01");
  const notAfter = new Date("2099-12-31");

  // CA
  const caKeys = await crypto.subtle.generateKey(alg, true, ["sign", "verify"]);
  const caCert = await X509CertificateGenerator.createSelfSigned({
    keys: caKeys,
    name: "CN=Test CA",
    notBefore,
    notAfter,
    signingAlgorithm,
    extensions: [new BasicConstraintsExtension(true, undefined, true)],
  });

  // Server cert — localhost DNS + 127.0.0.1 IP SAN, serverAuth + clientAuth EKU
  const serverKeys = await crypto.subtle.generateKey(alg, true, [
    "sign",
    "verify",
  ]);
  const serverCert = await X509CertificateGenerator.create({
    subject: "CN=localhost",
    issuer: caCert.subject,
    publicKey: serverKeys.publicKey,
    signingKey: caKeys.privateKey,
    notBefore,
    notAfter,
    signingAlgorithm,
    extensions: [
      new SubjectAlternativeNameExtension([
        { type: "dns", value: "localhost" },
        { type: "ip", value: "127.0.0.1" },
      ]),
      new ExtendedKeyUsageExtension([
        "1.3.6.1.5.5.7.3.1", // serverAuth
        "1.3.6.1.5.5.7.3.2", // clientAuth
      ]),
    ],
  });

  // Client cert — clientAuth EKU only
  const clientKeys = await crypto.subtle.generateKey(alg, true, [
    "sign",
    "verify",
  ]);
  const clientCert = await X509CertificateGenerator.create({
    subject: "CN=client",
    issuer: caCert.subject,
    publicKey: clientKeys.publicKey,
    signingKey: caKeys.privateKey,
    notBefore,
    notAfter,
    signingAlgorithm,
    extensions: [
      new ExtendedKeyUsageExtension([
        "1.3.6.1.5.5.7.3.2", // clientAuth
      ]),
    ],
  });

  return {
    caPem: caCert.toString("pem"),
    serverCertPem: serverCert.toString("pem"),
    serverKeyPem: toPem(
      "PRIVATE KEY",
      await crypto.subtle.exportKey("pkcs8", serverKeys.privateKey),
    ),
    clientCertPem: clientCert.toString("pem"),
    clientKeyPem: toPem(
      "PRIVATE KEY",
      await crypto.subtle.exportKey("pkcs8", clientKeys.privateKey),
    ),
  };
}

// ── Server lifecycle ─────────────────────────────────────────────────────────

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

async function waitForHealthy(
  url: string,
  caPem: string,
  clientCertPem: string,
  clientKeyPem: string,
  timeoutMs = 30_000,
): Promise<void> {
  const { request } = await import("node:https");

  function getOk(target: string): Promise<boolean> {
    return new Promise((resolve) => {
      try {
        const req = request(
          target,
          {
            ca: caPem,
            cert: clientCertPem,
            key: clientKeyPem,
            rejectUnauthorized: true,
            method: "GET",
          },
          (res) => {
            res.resume();
            resolve(
              (res.statusCode ?? 0) >= 200 && (res.statusCode ?? 0) < 300,
            );
          },
        );
        req.on("error", () => resolve(false));
        req.end();
      } catch {
        resolve(false);
      }
    });
  }

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await getOk(`${url}/livez`)) return;
    await new Promise<void>((r) => setTimeout(r, 150));
  }
  throw new Error(
    `beyond-objects TLS server did not become healthy at ${url} within ${timeoutMs}ms`,
  );
}

// ── Test state ───────────────────────────────────────────────────────────────

let serverProcess: ChildProcess | undefined;
let tempDir: string | undefined;
let serverUrl: string;
let rootToken: string;
let certs: CertBundle;

beforeAll(async () => {
  certs = await generateTestCerts();

  tempDir = mkdtempSync(join(tmpdir(), "beyond-objects-tls-test-"));
  const dataDir = join(tempDir, "data");
  const indexDir = join(tempDir, ".index");

  // Write cert files to temp dir
  const certFile = join(tempDir, "server.crt");
  const keyFile = join(tempDir, "server.key");
  const caFile = join(tempDir, "ca.crt");
  writeFileSync(certFile, certs.serverCertPem);
  writeFileSync(keyFile, certs.serverKeyPem);
  writeFileSync(caFile, certs.caPem);

  const [httpPort, metricsPort] = await Promise.all([
    findFreePort(),
    findFreePort(),
  ]);
  rootToken = randomUUID();
  serverUrl = `https://127.0.0.1:${httpPort}`;

  const binaryPath = process.env["BEYOND_OBJECTS_BINARY"]
    ?? resolve(__dirname, "../../../target/debug/beyond-objects");

  serverProcess = spawn(binaryPath, ["serve"], {
    env: {
      ...process.env,
      OBJECTS_ROOT_TOKEN: rootToken,
      OBJECTS_DATA_DIR: dataDir,
      OBJECTS_INDEX_DIR: indexDir,
      OBJECTS_HANDOFF_SOCKET_PATH: join(dataDir, "handoff.sock"),
      ADDRESS: `127.0.0.1:${httpPort}`,
      METRICS_ADDRESS: `127.0.0.1:${metricsPort}`,
      OBJECTS_URL: serverUrl,
      LOG_LEVEL: "error",
      RUST_LOG: "error",
      BEYOND_TLS_CERT: certFile,
      BEYOND_TLS_KEY: keyFile,
      BEYOND_TLS_CA: caFile,
    },
    stdio: ["pipe", "pipe", "inherit"],
  });

  serverProcess.on("error", (err) => {
    throw new Error(
      `Failed to start beyond-objects TLS server: ${err.message}`,
    );
  });

  await waitForHealthy(
    serverUrl,
    certs.caPem,
    certs.clientCertPem,
    certs.clientKeyPem,
  );
}, 60_000);

afterAll(async () => {
  serverProcess?.kill("SIGTERM");
  serverProcess = undefined;
  if (tempDir != null) {
    rmSync(tempDir, { recursive: true, force: true });
    tempDir = undefined;
  }
});

// ── Tests ────────────────────────────────────────────────────────────────────

describe("mTLS client support", () => {
  it("succeeds with valid CA + client cert", async () => {
    const client = createObjectsClient({
      url: serverUrl,
      token: rootToken,
      tls: {
        ca: certs.caPem,
        cert: certs.clientCertPem,
        key: certs.clientKeyPem,
      },
    });

    const { data, error } = await client.buckets.list();
    expect(error).toBeUndefined();
    expect(Array.isArray(data)).toBe(true);
  });

  it("fails without TLS options (no client cert, untrusted CA)", async () => {
    const client = createObjectsClient({
      url: serverUrl,
      token: rootToken,
      // No tls — plain undici H2 fetch with default system CAs; the
      // self-signed CA will not be trusted so the TLS handshake fails.
      timeout: 5_000,
      retries: 0,
    });

    await expect(client.buckets.list()).rejects.toThrow();
  });
});
