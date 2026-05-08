#!/usr/bin/env bash
#
# reserve-crate-names.sh — Claim graphrefly-* names on crates.io.
#
# crates.io has a flat global namespace (no orgs / scopes). Once a name is
# taken, you can't reclaim it without crates.io support intervention.
# This script publishes a reservation version (default 0.0.1) of each
# user-facing crate so the names are yours before the project goes
# public. See PART 14 of
# `~/src/graphrefly-ts/archive/docs/SESSION-rust-port-architecture.md`
# for the full rationale.
#
# WHAT THIS SCRIPT DOES:
#   - Publishes the 5 user-facing crates to crates.io in dependency order.
#   - Waits between dep-tier publishes so crates.io has time to index.
#   - Bindings crates (graphrefly-bindings-js / -py / -wasm) are
#     marked `publish = false` and are skipped automatically.
#
# WHAT THIS SCRIPT DOES NOT DO:
#   - It does NOT bump the workspace version. crates.io rejects 0.0.0
#     ("invalid semver tier" / "must not be a prerelease tier"). If your
#     workspace is at 0.0.0, bump it to 0.0.1 first, then run this.
#   - It does NOT log you in. Run `cargo login <token>` first; get a
#     token at https://crates.io → Account Settings → API Tokens.
#   - It does NOT handle the umbrella `graphrefly` crate (deferred per
#     PART 14 — ship if Rust users emerge).
#
# USAGE:
#   ./scripts/reserve-crate-names.sh             # real publish
#   ./scripts/reserve-crate-names.sh --dry-run   # validate without publishing
#
# REPO VISIBILITY:
#   The `repository` field in `Cargo.toml` becomes a clickable link from
#   each crates.io page. If `~/src/graphrefly-rs` isn't public yet, either
#   (a) make it public before reserving, or (b) temporarily replace the
#   repository URL with a placeholder (e.g. https://graphrefly.dev) and
#   revert after reservation.

set -euo pipefail

cd "$(dirname "$0")/.."

DRY_RUN=""
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN="1"
  echo "===> DRY RUN MODE — no publishes will be made."
  echo "===> Note: dry-run only validates the leaf crate (graphrefly-core)."
  echo "     Downstream crates (-graph / -operators / -storage / -structures)"
  echo "     are SKIPPED in dry-run because cargo always resolves their deps"
  echo "     against crates.io even with --no-verify, and -core isn't published"
  echo "     yet during dry-run. This is a fundamental cargo behavior, not a"
  echo "     script bug. The leaf crate's dry-run validates the shared"
  echo "     workspace metadata (description, license, repository, homepage,"
  echo "     authors) which all 5 crates inherit via 'workspace = true' — so"
  echo "     a clean leaf dry-run is good evidence that all 5 manifests are"
  echo "     publish-ready."
  echo ""
fi

# Verify cargo login state (skip in dry-run since we don't talk to crates.io
# beyond the leaf crate's index check, which is non-auth).
if [[ -z "$DRY_RUN" ]]; then
  if [[ ! -f "$HOME/.cargo/credentials.toml" && ! -f "$HOME/.cargo/credentials" ]]; then
    echo "ERROR: cargo not logged in to crates.io."
    echo "Run: cargo login <token>"
    echo "Get token at: https://crates.io → Account Settings → API Tokens"
    exit 1
  fi
fi

# Verify workspace version is publishable.
WORKSPACE_VERSION=$(grep -E '^version\s*=' Cargo.toml | head -1 | sed -E 's/.*"(.*)".*/\1/')
if [[ "$WORKSPACE_VERSION" == "0.0.0" ]]; then
  echo "ERROR: workspace version is 0.0.0 — crates.io rejects this."
  echo "Bump [workspace.package].version in Cargo.toml to 0.0.1 first."
  exit 1
fi
echo "===> Workspace version: $WORKSPACE_VERSION"

# Publish in dependency order. Each tier waits for crates.io indexing
# (~30–60s) before the next tier so dependents resolve correctly.
publish_leaf() {
  local crate="$1"
  echo ""
  echo "===> Publishing $crate (leaf, no in-workspace deps) ..."
  if [[ -n "$DRY_RUN" ]]; then
    cargo publish -p "$crate" --dry-run
  else
    cargo publish -p "$crate"
  fi
}

publish_dependent() {
  local crate="$1"
  if [[ -n "$DRY_RUN" ]]; then
    echo "===> Skipping $crate in dry-run (depends on a not-yet-published crate)."
    return
  fi
  echo ""
  echo "===> Publishing $crate ..."
  cargo publish -p "$crate"
}

wait_for_index() {
  if [[ -z "$DRY_RUN" ]]; then
    local seconds="$1"
    echo "===> Waiting ${seconds}s for crates.io index propagation ..."
    sleep "$seconds"
  fi
}

# Tier 1 — foundational; no in-workspace deps.
publish_leaf graphrefly-core
wait_for_index 60

# Tier 2 — depends only on -core.
publish_dependent graphrefly-graph
publish_dependent graphrefly-operators
wait_for_index 30

# Tier 3 — depends on -core and -graph.
publish_dependent graphrefly-storage
publish_dependent graphrefly-structures

echo ""
echo "===> Done. Verify with: cargo search graphrefly-core"
echo "===> Each crate's page on crates.io should show the README + repo link."
echo ""
echo "Next steps:"
echo "  1. Add CI bot as crate co-owner (per crate):"
echo "       cargo owner --add github:graphrefly/<bot-team> graphrefly-core"
echo "       (repeat for graph / operators / storage / structures)"
echo "  2. Add CRATES_IO_TOKEN to GitHub Actions secrets for future CI publishes."
echo "  3. Set up release-plz when M5 is closer to landing (PART 14 Item E)."
