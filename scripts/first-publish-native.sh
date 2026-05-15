#!/usr/bin/env bash
#
# first-publish-native.sh — One-time bootstrap publish of @graphrefly/native.
#
# WHY THIS EXISTS:
#   .github/workflows/native-npm-publish.yml publishes via npm OIDC
#   "trusted publishing" — no NPM_TOKEN. OIDC is chicken-and-egg: npm
#   only accepts an OIDC publish for a package that ALREADY EXISTS and
#   has a trusted publisher configured, and you can only configure a
#   trusted publisher on a package that exists. So the very first
#   publish of every one of the 6 packages must happen once from a
#   local machine using your interactive `npm login` (NOT a CI token).
#   After this script runs and you wire the trusted publishers, every
#   future `native-v*` tag publishes via OIDC with zero secrets.
#
#   The 6 packages (canonical napi-rs layout):
#     - @graphrefly/native                   (umbrella)
#     - @graphrefly/native-darwin-arm64
#     - @graphrefly/native-linux-x64-gnu
#     - @graphrefly/native-linux-arm64-gnu
#     - @graphrefly/native-win32-x64-msvc
#     - @graphrefly/native-win32-arm64-msvc
#
# WHAT THIS SCRIPT DOES:
#   - Cross-builds all 5 configured targets locally (darwin native;
#     linux-gnu via napi cross-toolchain; windows-msvc via cargo-xwin).
#   - Assembles the per-platform npm/<target>/ sub-packages + injects
#     publishConfig.access=public into each (scoped packages default to
#     restricted on first publish).
#   - Publishes all 5 sub-packages + the umbrella via your local
#     `npm login` (interactive 2FA OTP supported). NO provenance on
#     this first publish — provenance can only be minted from CI/OIDC;
#     subsequent CI publishes get it automatically.
#
# WHAT THIS SCRIPT DOES NOT DO:
#   - It does NOT log you in. Run `npm login` first (a normal user
#     login, not a CI token). Verify with `npm whoami`.
#   - It does NOT configure the npm trusted publishers — that is a
#     manual one-time UI step printed at the end (per package).
#   - It does NOT bump the version. Set `version` in
#     crates/graphrefly-bindings-js/package.json first (currently 0.0.1).
#   - It does NOT create the @graphrefly npm org. If nothing under the
#     @graphrefly scope has ever been published, create the org on
#     npmjs.com first (or publish @graphrefly/pure-ts etc. — whichever
#     claims the scope).
#
# USAGE:
#   ./scripts/first-publish-native.sh              # real publish
#   ./scripts/first-publish-native.sh --dry-run    # build + npm pack-check, no publish
#
# PREREQS (the script hard-checks these and fails fast with hints):
#   rustup, cargo, Node >= 22, npm >= 11.5.1, @napi-rs/cli (devDep),
#   `npm whoami` (logged in), and the 5 rustup targets (auto-added).
#   On a non-Linux host: zig + cargo-zigbuild (linux-gnu cross) and
#   cargo-xwin + LLVM (windows-msvc cross). One-time installs:
#     brew install zig llvm
#     cargo install --locked cargo-zigbuild cargo-xwin
#   cargo-xwin auto-downloads the Windows CRT/SDK on first build.

set -euo pipefail

cd "$(dirname "$0")/.."
PKG_DIR="crates/graphrefly-bindings-js"

DRY_RUN=""
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN="1"
  echo "===> DRY RUN — cross-builds all targets, runs 'npm publish --dry-run'."
  echo "     No package is actually published."
  echo ""
fi

TARGETS=(
  aarch64-apple-darwin
  x86_64-unknown-linux-gnu
  aarch64-unknown-linux-gnu
  x86_64-pc-windows-msvc
  aarch64-pc-windows-msvc
)

fail() { echo "ERROR: $1" >&2; exit 1; }

# ── Preflight ──────────────────────────────────────────────────────────────
echo "===> Preflight checks ..."

command -v rustup >/dev/null || fail "rustup not found. Install: https://rustup.rs"
command -v cargo  >/dev/null || fail "cargo not found (install rust toolchain)."

NODE_MAJOR="$(node -p 'process.versions.node.split(".")[0]' 2>/dev/null || echo 0)"
[[ "$NODE_MAJOR" -ge 22 ]] || fail "Node >= 22 required for OIDC-era npm (have: $(node -v 2>/dev/null || echo none))."

NPM_VER="$(npm --version 2>/dev/null || echo 0.0.0)"
# 11.5.1 is the OIDC-trusted-publishing floor. Local first publish does
# not strictly need OIDC, but matching the CI floor avoids surprises.
npm_ge_1151="$(node -e 'const[a,b,c]=process.argv[1].split(".").map(Number);process.stdout.write((a>11||(a===11&&(b>5||(b===5&&c>=1))))?"1":"0")' "$NPM_VER")"
[[ "$npm_ge_1151" == "1" ]] || fail "npm >= 11.5.1 required (have: $NPM_VER). Run: npm install -g npm@latest"

if [[ -z "$DRY_RUN" ]]; then
  npm whoami >/dev/null 2>&1 || fail "Not logged in to npm. Run: npm login  (interactive user login, NOT a CI token)."
  echo "     npm user: $(npm whoami)"
fi

( cd "$PKG_DIR" && pnpm exec napi --version >/dev/null 2>&1 ) || fail "@napi-rs/cli not resolvable. Run: pnpm install --frozen-lockfile (in repo root)."

# The host's own Rust target builds natively; every other target uses
# napi `--cross-compile` (cargo-zigbuild for linux-gnu, cargo-xwin for
# windows-msvc). @napi-rs/cross-toolchain (--use-napi-cross) is NOT
# usable here: it ships Linux-host ELF gcc that cannot exec on macOS
# (exit 126 "cannot execute binary file" → blake3 build fails).
HOST_OS="$(uname -s)"
HOST_TARGET="$(rustc -vV | sed -n 's/^host: //p')"
[[ -n "$HOST_TARGET" ]] || fail "could not determine host rust target from 'rustc -vV'."
echo "     host target: $HOST_TARGET"

# zig/cargo-zigbuild are what napi's --cross-compile uses for the
# linux-gnu targets; only needed when the host isn't itself linux-gnu.
if [[ "$HOST_OS" != "Linux" ]]; then
  command -v zig >/dev/null 2>&1 || fail "zig not found (napi --cross-compile uses cargo-zigbuild for the linux-gnu targets). Install: brew install zig"
  command -v cargo-zigbuild >/dev/null 2>&1 || fail "cargo-zigbuild not found (napi --cross-compile uses it for linux-gnu). Install: cargo install --locked cargo-zigbuild"
fi

command -v cargo-xwin >/dev/null 2>&1 || fail "cargo-xwin not found (napi --cross-compile uses it for the windows-msvc targets). Install: cargo install --locked cargo-xwin  (also: brew install llvm)."

echo "     adding rustup targets ..."
for t in "${TARGETS[@]}"; do rustup target add "$t" >/dev/null 2>&1 || true; done

VERSION="$(node -p "require('./$PKG_DIR/package.json').version")"
[[ "$VERSION" != "0.0.0" ]] || fail "package.json version is 0.0.0 — set a real version (e.g. 0.0.1) first."
echo "     publishing version: $VERSION"

if [[ -z "$DRY_RUN" ]]; then
  if npm view "@graphrefly/native@${VERSION}" version >/dev/null 2>&1; then
    fail "@graphrefly/native@${VERSION} already exists on npm. Bump the version before re-running (npm versions are immutable)."
  fi
fi

echo "     preflight OK."
echo ""

# ── Cross-build all targets ────────────────────────────────────────────────
cd "$PKG_DIR"
rm -rf artifacts && mkdir -p artifacts

for t in "${TARGETS[@]}"; do
  echo "===> Building $t ..."
  # No bash array here: macOS ships bash 3.2, where `"${arr[@]}"` on an
  # empty array under `set -u` errors "unbound variable". Branch the
  # full command instead. The host's own target (aarch64-apple-darwin
  # on the documented macOS bootstrap machine) builds natively;
  # everything else uses napi's `--cross-compile` (`-x`), which (per
  # `napi build --help`) uses `cargo-zigbuild` for the linux-gnu
  # targets and `cargo-xwin` for the windows-msvc targets. (napi 3.6.2
  # has no `--zig`; `--use-napi-cross` is Linux-host-only.)
  case "$t" in
    "$HOST_TARGET")
      pnpm exec napi build --platform --release --features full --target "$t" ;;
    *)
      pnpm exec napi build --platform --release --features full --target "$t" --cross-compile ;;
  esac
  # Collect the produced binary for the artifacts → npm/<target>/ step.
  mv graphrefly-native.*.node artifacts/ 2>/dev/null || \
    fail "no .node produced for $t (check the cross toolchain for this target)."
done

echo ""
echo "===> Assembling per-platform npm/ sub-packages ..."
# create-npm-dirs writes npm/<target>/package.json (name/os/cpu/version);
# artifacts moves each .node into its matching dir.
pnpm exec napi create-npm-dirs >/dev/null 2>&1 || true
pnpm exec napi artifacts --dir artifacts

# Scoped packages default to RESTRICTED on first publish. Force public on
# every generated sub-package manifest (belt-and-suspenders with the
# explicit --access public below).
for d in npm/*/; do
  [[ -f "$d/package.json" ]] || continue
  node -e 'const f=process.argv[1],p=require("path").resolve(f);const j=require(p);j.publishConfig={...(j.publishConfig||{}),access:"public"};require("fs").writeFileSync(p,JSON.stringify(j,null,2)+"\n")' "$d/package.json"
done

# ── Publish ────────────────────────────────────────────────────────────────
PUB=(npm publish --access public)
[[ -n "$DRY_RUN" ]] && PUB+=(--dry-run)

echo ""
echo "===> Publishing 5 per-platform sub-packages ..."
for d in npm/*/; do
  [[ -f "$d/package.json" ]] || continue
  name="$(node -p "require('./$d/package.json').name")"
  echo "  -> $name@$VERSION"
  ( cd "$d" && "${PUB[@]}" )
done

echo ""
echo "===> Publishing umbrella @graphrefly/native@$VERSION ..."
"${PUB[@]}"

# ── Done ───────────────────────────────────────────────────────────────────
echo ""
if [[ -n "$DRY_RUN" ]]; then
  echo "===> DRY RUN complete — nothing was published."
  exit 0
fi

cat <<EOF

===> First publish DONE: 6 packages at $VERSION.

NEXT (one-time, required before CI OIDC works) — configure the trusted
publisher on EACH of the 6 packages. On npmjs.com, for each of:

  @graphrefly/native
  @graphrefly/native-darwin-arm64
  @graphrefly/native-linux-x64-gnu
  @graphrefly/native-linux-arm64-gnu
  @graphrefly/native-win32-x64-msvc
  @graphrefly/native-win32-arm64-msvc

  Package → Settings → Trusted Publisher → GitHub Actions →
    Organization: graphrefly
    Repository:   graphrefly-rs
    Workflow:     native-npm-publish.yml
    Environment:  (leave blank)

After that, every future release is just:

  # bump version in $PKG_DIR/package.json, commit, then:
  git tag native-v<version> && git push origin native-v<version>

→ native-npm-publish.yml builds all 5 targets and publishes all 6
  packages via OIDC. No NPM_TOKEN, ever. Provenance is attached
  automatically (it could not be on this local bootstrap publish).
EOF
