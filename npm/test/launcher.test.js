"use strict";

const assert = require("node:assert/strict");
const { spawn, execFile } = require("node:child_process");
const { createHash } = require("node:crypto");
const fs = require("node:fs");
const fsp = require("node:fs/promises");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");
const { promisify } = require("node:util");
const execFileAsync = promisify(execFile);

const packageJson = require("../package.json");
const launcher = path.resolve(__dirname, "../bin/twinning-mcp.js");
const targetByHost = {
  "darwin-x64": "x86_64-apple-darwin",
  "darwin-arm64": "aarch64-apple-darwin",
  "linux-x64": "x86_64-unknown-linux-gnu",
  "linux-arm64": "aarch64-unknown-linux-gnu"
};
const hostTarget = targetByHost[process.platform + "-" + process.arch];

function print(event, fields) {
  const values = Object.entries(fields).map(([key, value]) => {
    const rendered = value && typeof value === "object" ? JSON.stringify(value) : String(value);
    return key + "=" + rendered.replace(/\n/g, "\\n");
  });
  process.stdout.write("[NPM01] " + event + (values.length ? " " + values.join(" ") : "") + "\n");
}

async function pollUntil(check, timeoutMs, what) {
  const started = Date.now();
  const deadline = started + timeoutMs;
  let last = "not ready";
  while (Date.now() < deadline) {
    const result = check();
    if (result.ready) {
      print("poll", { what, elapsed_ms: Date.now() - started, observation: result.observation });
      return;
    }
    last = result.observation;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error("timed out waiting for " + what + "; last observation: " + last);
}

function runLauncher(args, input, env) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [launcher, ...args], { env, stdio: ["pipe", "pipe", "pipe"] });
    const stdout = [];
    const stderr = [];
    child.stdout.on("data", (chunk) => stdout.push(chunk));
    child.stderr.on("data", (chunk) => stderr.push(chunk));
    child.on("error", reject);
    child.on("close", (code, signal) => resolve({
      code,
      signal,
      stdout: Buffer.concat(stdout).toString("utf8"),
      stderr: Buffer.concat(stderr).toString("utf8")
    }));
    child.stdin.end(input);
  });
}

async function main() {
  const artifacts = path.resolve(process.argv[2] || path.join(os.tmpdir(), "twinning-mcp-tests"));
  await fsp.mkdir(artifacts, { recursive: true });
  const staging = await fsp.mkdtemp(path.join(artifacts, "asset-"));
  const fakeBinary = "#!/bin/sh\n" +
    "printf 'FAKE-BINARY\\n'\n" +
    "printf 'ARG1=%s\\n' \"$1\"\n" +
    "printf 'ARG2=%s\\n' \"$2\"\n" +
    "value=$(cat)\n" +
    "printf 'STDIN=%s\\n' \"$value\"\n" +
    "printf 'FAKE-STDERR\\n' >&2\n";
  await fsp.writeFile(path.join(staging, "twinning-mcp"), fakeBinary, { mode: 0o755 });
  await fsp.writeFile(path.join(staging, "twinning"), "#!/bin/sh\nexit 0\n", { mode: 0o755 });
  await fsp.writeFile(path.join(staging, "LICENSE"), "fake license\n");
  await fsp.writeFile(path.join(staging, "README.md"), "fake archive\n");

  const archiveName = "twinning-v" + packageJson.version + "-" + (hostTarget || "x86_64-unknown-linux-gnu") + ".tar.gz";
  const archivePath = path.join(artifacts, archiveName);
  await execFileAsync("tar", ["-czf", archivePath, "-C", staging, "."]);
  const archiveBytes = await fsp.readFile(archivePath);
  const correctDigest = createHash("sha256").update(archiveBytes).digest("hex");
  const requestLog = [];
  let checksumMode = "good";
  const checksumsPath = "/v" + packageJson.version + "/SHA256SUMS";
  const assetPath = "/v" + packageJson.version + "/" + archiveName;
  const server = http.createServer((request, response) => {
    requestLog.push(request.url);
    if (request.url === checksumsPath) {
      const digest = checksumMode === "good" ? correctDigest : "0".repeat(64);
      response.writeHead(200, { "content-type": "text/plain" });
      response.end(digest + "  " + archiveName + "\n");
      return;
    }
    if (request.url === assetPath) {
      response.writeHead(200, { "content-type": "application/gzip" });
      response.end(archiveBytes);
      return;
    }
    response.writeHead(404, { "content-type": "text/plain" });
    response.end("not found\n");
  });

  try {
    server.listen(0, "127.0.0.1");
    await pollUntil(
      () => ({ ready: Boolean(server.listening && server.address()), observation: server.address() || "listener not bound" }),
      5000,
      "local asset server readiness"
    );
    const address = server.address();
    const releaseBase = "http://127.0.0.1:" + address.port + "/v" + packageJson.version;
    print("server_ready", { address: "127.0.0.1:" + address.port, version: packageJson.version, target: hostTarget || "host unsupported" });
    if (!hostTarget) throw new Error("launcher test host is not one of the supported macOS/Linux targets");

    const sharedEnv = {
      ...process.env,
      TWINNING_MCP_RELEASE_BASE_URL: releaseBase
    };
    const goodCache = path.join(artifacts, "cache-good");
    const goodEnv = { ...sharedEnv, TWINNING_MCP_CACHE_DIR: goodCache };
    const good = await runLauncher(["doctor", "arg with spaces"], "input payload", goodEnv);
    const goodLog = "command=node " + launcher + " doctor 'arg with spaces'\n" +
      "exit_code=" + good.code + "\nsignal=" + good.signal + "\nstdout:\n" + good.stdout + "stderr:\n" + good.stderr;
    await fsp.writeFile(path.join(artifacts, "NPM01-good.log"), goodLog);
    requestLog.forEach((url) => print("http_request", { url }));
    print("case", { id: "NPM01-good", exit_code: good.code, stderr: good.stderr.trim(), stdout: good.stdout.trim() });
    assert.equal(good.code, 0, good.stderr);
    assert.equal(good.signal, null);
    assert.match(good.stdout, /FAKE-BINARY/);
    assert.match(good.stdout, /ARG1=doctor/);
    assert.match(good.stdout, /ARG2=arg with spaces/);
    assert.match(good.stdout, /STDIN=input payload/);
    assert.match(good.stderr, /FAKE-STDERR/);
    assert.deepEqual(requestLog, [checksumsPath, assetPath]);

    const requestCountBeforeCacheHit = requestLog.length;
    const cached = await runLauncher(["doctor", "arg with spaces"], "input payload", goodEnv);
    await fsp.writeFile(path.join(artifacts, "NPM01-cache.log"),
      "command=node " + launcher + " doctor 'arg with spaces'\nexit_code=" + cached.code +
      "\nstdout:\n" + cached.stdout + "stderr:\n" + cached.stderr);
    print("case", { id: "NPM01-cache", exit_code: cached.code, downloads: requestLog.length - requestCountBeforeCacheHit, stderr: cached.stderr.trim() });
    assert.equal(cached.code, 0, cached.stderr);
    assert.equal(requestLog.length, requestCountBeforeCacheHit, "a valid cache entry must not trigger downloads");

    checksumMode = "bad";
    const badEnv = { ...sharedEnv, TWINNING_MCP_CACHE_DIR: path.join(artifacts, "cache-bad") };
    const bad = await runLauncher(["doctor"], "", badEnv);
    await fsp.writeFile(path.join(artifacts, "NPM01-bad-checksum.log"),
      "command=node " + launcher + " doctor\nexit_code=" + bad.code + "\nstdout:\n" + bad.stdout + "stderr:\n" + bad.stderr);
    requestLog.slice(requestCountBeforeCacheHit).forEach((url) => print("http_request", { url }));
    print("case", { id: "NPM01-bad-checksum", exit_code: bad.code, stderr: bad.stderr.trim() });
    assert.notEqual(bad.code, 0, "a bad checksum must stop launch");
    assert.match(bad.stderr, /SHA-256 mismatch/);
    assert.equal(fs.existsSync(path.join(artifacts, "cache-bad", packageJson.version, hostTarget)), false,
      "a bad checksum must not create an install cache");

    await fsp.writeFile(path.join(artifacts, "server.log"), requestLog.map((url) => "GET " + url).join("\n") + "\n");
    print("result", { status: "PASS", good_download: "checksum verified and args/stdio forwarded", cache_hit: "no download", bad_checksum: "rejected before extraction" });
  } finally {
    await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
}

main().catch((error) => {
  process.stderr.write("[NPM01] FAIL " + (error && error.stack ? error.stack : String(error)) + "\n");
  process.exitCode = 1;
});
