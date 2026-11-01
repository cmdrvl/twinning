#!/usr/bin/env node
"use strict";

const { spawn, spawnSync } = require("node:child_process");
const { createHash } = require("node:crypto");
const fsp = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");

const PACKAGE_VERSION = require("../package.json").version;
const MAX_DOWNLOAD_BYTES = 512 * 1024 * 1024;
const ALLOWED_ARCHIVE_FILES = new Set(["twinning", "twinning-mcp", "LICENSE", "README.md"]);

function targetFor(platform, arch) {
  const targets = {
    "darwin-x64": "x86_64-apple-darwin",
    "darwin-arm64": "aarch64-apple-darwin",
    "linux-x64": "x86_64-unknown-linux-gnu",
    "linux-arm64": "aarch64-unknown-linux-gnu"
  };
  const target = targets[platform + "-" + arch];
  if (!target) {
    throw new Error("unsupported platform " + platform + "/" + arch + "; supported targets are macOS and Linux x64 and arm64");
  }
  return target;
}

function releaseBase(version) {
  const override = process.env.TWINNING_MCP_RELEASE_BASE_URL;
  const raw = override || ("https://github.com/cmdrvl/twinning/releases/download/v" + version);
  let url;
  try {
    url = new URL(raw);
  } catch {
    throw new Error("TWINNING_MCP_RELEASE_BASE_URL must be a valid URL");
  }
  const localHttp = url.protocol === "http:" &&
    (url.hostname === "127.0.0.1" || url.hostname === "localhost" || url.hostname === "[::1]");
  if (url.protocol !== "https:" && !localHttp) {
    throw new Error("release downloads require HTTPS (HTTP is allowed only for loopback tests)");
  }
  if (url.username || url.password || url.search || url.hash) {
    throw new Error("release base URL must not contain credentials, a query, or a fragment");
  }
  return url.toString().replace(/\/+$/, "");
}

async function download(url) {
  const response = await fetch(url, {
    signal: AbortSignal.timeout(120000),
    redirect: "follow"
  });
  if (!response.ok) {
    throw new Error("download failed with HTTP " + response.status + " for " + url);
  }
  const finalUrl = new URL(response.url || url);
  const finalIsLocalHttp = finalUrl.protocol === "http:" &&
    (finalUrl.hostname === "127.0.0.1" || finalUrl.hostname === "localhost" || finalUrl.hostname === "[::1]");
  if (finalUrl.protocol !== "https:" && !finalIsLocalHttp) {
    throw new Error("release download redirect did not remain on HTTPS");
  }
  const chunks = [];
  let size = 0;
  if (!response.body) {
    throw new Error("download returned an empty response for " + url);
  }
  const reader = response.body.getReader();
  while (true) {
    const result = await reader.read();
    if (result.done) break;
    size += result.value.byteLength;
    if (size > MAX_DOWNLOAD_BYTES) {
      await reader.cancel();
      throw new Error("download exceeded the 512 MiB safety limit");
    }
    chunks.push(Buffer.from(result.value));
  }
  return Buffer.concat(chunks, size);
}

function expectedChecksum(checksums, assetName) {
  const matches = [];
  for (const line of checksums.split(/\r?\n/)) {
    const match = /^([0-9a-fA-F]{64})\s+\*?(.+)$/.exec(line);
    if (match && path.posix.basename(match[2]) === assetName) {
      matches.push(match[1].toLowerCase());
    }
  }
  if (matches.length !== 1) {
    throw new Error("SHA256SUMS must contain exactly one entry for " + assetName);
  }
  return matches[0];
}

function inspectArchive(archivePath) {
  const listed = spawnSync("tar", ["-tzf", archivePath], { encoding: "utf8" });
  if (listed.error) throw new Error("could not run tar to inspect the release archive: " + listed.error.message);
  if (listed.status !== 0) throw new Error("could not inspect the release archive: " + listed.stderr.trim());
  const files = [];
  for (const rawName of listed.stdout.split(/\r?\n/)) {
    if (!rawName || rawName === "." || rawName === "./") continue;
    const name = rawName.replace(/^\.\//, "");
    if (name.startsWith("/") || name.split("/").includes("..") || path.posix.normalize(name) !== name) {
      throw new Error("release archive contains an unsafe path");
    }
    if (!ALLOWED_ARCHIVE_FILES.has(name)) {
      throw new Error("release archive contains an unexpected path: " + name);
    }
    files.push(name);
  }
  if (!files.includes("twinning") || !files.includes("twinning-mcp")) {
    throw new Error("release archive must contain sibling twinning and twinning-mcp binaries");
  }
}

async function installTarget(target) {
  const assetName = "twinning-v" + PACKAGE_VERSION + "-" + target + ".tar.gz";
  const base = releaseBase(PACKAGE_VERSION);
  const cacheRoot = process.env.TWINNING_MCP_CACHE_DIR ||
    path.join(process.env.XDG_CACHE_HOME || path.join(os.homedir(), ".cache"), "twinning-mcp");
  const destination = path.join(cacheRoot, PACKAGE_VERSION, target);
  const binary = path.join(destination, "twinning-mcp");
  const sibling = path.join(destination, "twinning");

  try {
    const [binaryStat, siblingStat] = await Promise.all([fsp.stat(binary), fsp.stat(sibling)]);
    if (binaryStat.isFile() && siblingStat.isFile() && (binaryStat.mode & 0o111) !== 0) {
      return binary;
    }
  } catch {
    // A cache miss falls through to a checksum-verified install.
  }

  await fsp.mkdir(path.dirname(destination), { recursive: true });
  const temporary = await fsp.mkdtemp(path.join(path.dirname(destination), ".install-"));
  try {
    const checksumBytes = await download(base + "/SHA256SUMS");
    const expected = expectedChecksum(checksumBytes.toString("utf8"), assetName);
    const archiveBytes = await download(base + "/" + assetName);
    const actual = createHash("sha256").update(archiveBytes).digest("hex");
    if (actual !== expected) {
      throw new Error("SHA-256 mismatch for " + assetName + ": expected " + expected + ", got " + actual);
    }

    const archivePath = path.join(temporary, assetName);
    await fsp.writeFile(archivePath, archiveBytes, { mode: 0o600 });
    inspectArchive(archivePath);
    const extracted = path.join(temporary, "extracted");
    await fsp.mkdir(extracted);
    const extraction = spawnSync("tar", ["-xzf", archivePath, "-C", extracted], { encoding: "utf8" });
    if (extraction.error) throw new Error("could not run tar to extract the release archive: " + extraction.error.message);
    if (extraction.status !== 0) throw new Error("could not extract the release archive: " + extraction.stderr.trim());

    for (const name of ["twinning", "twinning-mcp"]) {
      const stat = await fsp.lstat(path.join(extracted, name));
      if (!stat.isFile() || (stat.mode & 0o111) === 0) {
        throw new Error("release archive entry " + name + " is not an executable regular file");
      }
    }
    await fsp.mkdir(destination, { recursive: true });
    for (const name of ["twinning", "twinning-mcp", "LICENSE", "README.md"]) {
      try {
        await fsp.rename(path.join(extracted, name), path.join(destination, name));
      } catch (error) {
        if (error.code !== "ENOENT") throw error;
      }
    }
    return binary;
  } finally {
    await fsp.rm(temporary, { recursive: true, force: true });
  }
}

async function main() {
  const target = targetFor(process.platform, process.arch);
  const binary = await installTarget(target);
  const child = spawn(binary, process.argv.slice(2), { stdio: "inherit" });
  child.on("error", (error) => {
    process.stderr.write("twinning-mcp launcher: could not start cached binary: " + error.message + "\n");
    process.exitCode = 1;
  });
  child.on("exit", (code, signal) => {
    if (signal) {
      process.kill(process.pid, signal);
    } else {
      process.exitCode = code === null ? 1 : code;
    }
  });
}

main().catch((error) => {
  process.stderr.write("twinning-mcp launcher: " + error.message + "\n");
  process.exitCode = 1;
});
