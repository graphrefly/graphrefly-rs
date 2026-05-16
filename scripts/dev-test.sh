#!/usr/bin/env bash
# dev-test.sh — per-worktree-isolated parallel test loop.
#
# THE problem this solves: cargo takes a build-directory file lock on
# `target/`. When two agent/dev sessions run `cargo test` against the same
# checkout, the second BLOCKS on that lock until the first finishes — and
# if the first deadlocks (a hung cross-thread test held the lock for ~1h45m
# in practice), every other session is wedged behind it.
#
# Fix: give each git worktree its own CARGO_TARGET_DIR, keyed by the
# worktree root path. Parallel sessions in different worktrees never share
# a lock. Sessions in the SAME worktree still share (by design — same
# build), but should run this serially or branch into a worktree.
#
# Also runs via cargo-nextest (cross-binary parallelism + slow-timeout
# kills hung tests; see .config/nextest.toml).
#
# Usage:
#   scripts/dev-test.sh                       # all default-members tests
#   scripts/dev-test.sh -p graphrefly-core    # one crate
#   scripts/dev-test.sh -p graphrefly-core serialization_groups   # filter
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
KEY="$(printf '%s' "$ROOT" | shasum | cut -c1-12)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/.cache/graphrefly-rs-target/$KEY}"

if ! command -v cargo-nextest >/dev/null 2>&1; then
  echo "cargo-nextest not found. Install (prebuilt, no compile):" >&2
  echo "  curl -LsSf https://get.nexte.st/latest/mac | tar zxf - -C ~/.cargo/bin" >&2
  exit 127
fi

echo "→ CARGO_TARGET_DIR=$CARGO_TARGET_DIR (worktree: $ROOT)" >&2
exec cargo nextest run "$@"
