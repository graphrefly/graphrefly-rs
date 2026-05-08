/* eslint-disable */
/**
 * @graphrefly/native — napi-rs binding loader.
 *
 * Phase E rustImpl activation slice (D074). This loader is hand-written
 * for the local-platform single-target use case; napi-cli generates a
 * more elaborate version for the cross-platform publish path (D075). The
 * JS adapter `packages/parity-tests/impls/rust.ts` consumes this module.
 *
 * Resolution order:
 *   1. Local `.node` artifact in this directory (produced by `pnpm build`).
 *   2. Per-platform sub-package via require (when @graphrefly/native-<plat>
 *      packages are published; not in this slice).
 *
 * On failure, throws a diagnostic pointing at the build command.
 */

const { existsSync } = require('node:fs');
const { join } = require('node:path');

const platforms = {
  'darwin-arm64': 'graphrefly-native.darwin-arm64.node',
  'darwin-x64': 'graphrefly-native.darwin-x64.node',
  'linux-x64-gnu': 'graphrefly-native.linux-x64-gnu.node',
  'linux-arm64-gnu': 'graphrefly-native.linux-arm64-gnu.node',
  'win32-x64-msvc': 'graphrefly-native.win32-x64-msvc.node',
  'win32-arm64-msvc': 'graphrefly-native.win32-arm64-msvc.node',
};

function pickPlatformKey() {
  const { platform, arch } = process;
  const isMusl = (() => {
    try {
      // ESM-safe musl detection: read /proc/self/status for "Musl"
      // (cheap heuristic; for napi platform sub-packages, gnu vs musl
      // is only relevant on linux-* targets).
      const fs = require('node:fs');
      const status = fs.readFileSync('/proc/self/maps', 'utf8');
      return status.includes('musl');
    } catch {
      return false;
    }
  })();

  if (platform === 'darwin') return arch === 'arm64' ? 'darwin-arm64' : 'darwin-x64';
  if (platform === 'linux') {
    const suffix = isMusl ? 'musl' : 'gnu';
    return arch === 'arm64' ? `linux-arm64-${suffix}` : `linux-x64-${suffix}`;
  }
  if (platform === 'win32') return arch === 'arm64' ? 'win32-arm64-msvc' : 'win32-x64-msvc';
  return null;
}

function load() {
  const key = pickPlatformKey();
  if (!key) {
    throw new Error(
      `[graphrefly/native] Unsupported platform/arch: ${process.platform}/${process.arch}`,
    );
  }

  // Local .node artifact (produced by `pnpm --filter @graphrefly/native build`).
  const localPath = join(__dirname, platforms[key] || `graphrefly-native.${key}.node`);
  if (existsSync(localPath)) {
    return require(localPath);
  }

  // Per-platform sub-package (when @graphrefly/native-<plat> publishes).
  // Not used in this slice; left here so the resolution shape matches the
  // canonical napi-rs publish layout (D075).
  try {
    return require(`@graphrefly/native-${key}`);
  } catch (e) {
    throw new Error(
      `[graphrefly/native] No native artifact found for ${key}. ` +
        `Run \`pnpm --filter @graphrefly/native build\` (or \`napi build --platform --release --features standard\` from ${__dirname}).`,
    );
  }
}

const native = load();

module.exports = native;
