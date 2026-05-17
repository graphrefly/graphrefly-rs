#!/usr/bin/env bash
# gate.sh — the ONLY sanctioned long-gate runner for graphrefly-rs.
#
# ─── THE problem this solves ────────────────────────────────────────────────
# The Slice B-2 session burned ~2h on a self-inflicted "deadlock" that was
# actually swap thrash from OVERLAPPING cargo invocations. The anti-pattern:
#
#     sh -c "fmt; clippy; cargo nextest run; cargo nextest run --profile ci" &
#
# Each `pkill cargo-nextest` killed only the *nextest child*; the surviving
# `;`-chain immediately relaunched the next cargo command. Multiple cargo
# runs then fought the single `target/.cargo-lock`, spawning dozens of
# parallel debug rustc/test binaries → RAM exhaustion → swap thrash. Every
# process went `SN` / 0%-CPU / RSS ~32 KB with millions of pageins — a
# signature visually IDENTICAL to a parking_lot deadlock, for an hour.
#
# Durable fix (this script). Hard guarantees:
#   (a) MUTEX — a second concurrent invocation REFUSES (atomic mkdir lock +
#       a pre-flight scan for any cargo-nextest / cargo-lock holder).
#   (b) SEQUENTIAL — fmt --check, clippy, nextest run as separate steps.
#       Never a `;`-chain in one `sh -c`: one cargo invocation per target
#       at a time, full stop.
#   (c) PROCESS-GROUP TEARDOWN — every step runs in its own process group
#       (job-control monitor mode; macOS has no `setsid`). Any kill / Ctrl-C /
#       timeout tears down the WHOLE group: cargo AND every rustc/test child.
#       Zero orphans.
#   (d) BOUNDED + RAM PRECHECK — CARGO_BUILD_JOBS and nextest --test-threads
#       are capped; a free-memory precheck fails fast instead of thrashing.
#   (e) DIRECT FILE LOG — output goes straight to a file, never piped through
#       `tail` (tail buffers until EOF and hid cargo's deterministic
#       "Blocking waiting for file lock on artifact directory" line).
#   (f) SELF-TIMEOUT WITH DIAGNOSTIC — on expiry it prints process state
#       codes, the cargo-lock holder, and swap usage so a stall is
#       *classified* (thrash vs deadlock), not guessed. macOS has no
#       `timeout(1)`, so the watchdog is built in.
#
# See `~/src/graphrefly-ts/docs/test-guidance.md` § "Running the full Rust
# gate / diagnosing a stuck run" and memory feedback
# `feedback_no_chained_background_cargo.md` for the full rationale + the
# thrash-vs-deadlock signature table.
#
# ─── Usage ──────────────────────────────────────────────────────────────────
#   scripts/gate.sh                 # FULL gate: fmt --check, clippy,
#                                   #   `cargo nextest run --profile ci`
#                                   #   (default-members, incl. cascade_depth)
#   scripts/gate.sh core            # FAST variant: fmt --check, clippy
#                                   #   (-p graphrefly-core), `cargo nextest
#                                   #   run -p graphrefly-core` (default profile)
#   scripts/gate.sh [core] -- ARGS  # ARGS appended to the nextest step
#                                   #   (e.g. a filter: -- serialization_groups)
#
# Prefer the mise wrappers: `mise run gate` / `mise run gate:core`.
#
# ─── Env knobs (all optional) ───────────────────────────────────────────────
#   GATE_JOBS=4            CARGO_BUILD_JOBS cap (parallel rustc)
#   GATE_TEST_THREADS=4    nextest --test-threads cap
#   GATE_TIMEOUT=<secs>    overall watchdog (default 2400 full / 900 core)
#   GATE_MIN_FREE_PCT=8    refuse to start if free memory % is below this
#   GATE_MAX_SWAP_MB=      hard-refuse if swap-used MB exceeds this (default
#                          unset: macOS keeps swap sticky, so absolute
#                          swap-used is a poor "about to thrash" signal —
#                          free-memory-% is the real precheck; opt in only
#                          if you want a hard ceiling)
#   GATE_CLIPPY_DENY=1     pass `-- -D warnings` to clippy (default off, to
#                          match the project's warn-by-default convention)
#   GATE_LOG=<path>        override the log file path
set -u

# ─── Resolve repo root + per-worktree target dir ────────────────────────────
# Mirror scripts/dev-test.sh EXACTLY so the gate and the inner dev loop share
# one build dir per worktree (and parallel worktrees never fight the lock).
ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || {
  echo "gate.sh: not inside a git repo" >&2; exit 2;
}
cd "$ROOT" || exit 2
KEY="$(printf '%s' "$ROOT" | shasum | cut -c1-12)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/.cache/graphrefly-rs-target/$KEY}"
TARGET="$CARGO_TARGET_DIR"
# cargo's build-directory locks (both historical locations).
LOCK_GLOBS=("$TARGET/.cargo-lock" "$TARGET/debug/.cargo-lock")

# ─── Mode + arg parse ───────────────────────────────────────────────────────
MODE="full"
if [ "${1:-}" = "core" ]; then MODE="core"; shift; fi
if [ "${1:-}" = "--" ]; then shift; fi
NEXTEST_EXTRA=("$@")            # appended to the nextest step (e.g. a filter)

GATE_JOBS="${GATE_JOBS:-4}"
GATE_TEST_THREADS="${GATE_TEST_THREADS:-4}"
GATE_MIN_FREE_PCT="${GATE_MIN_FREE_PCT:-8}"
GATE_MAX_SWAP_MB="${GATE_MAX_SWAP_MB:-}"
if [ "$MODE" = "core" ]; then
  GATE_TIMEOUT="${GATE_TIMEOUT:-900}"
else
  GATE_TIMEOUT="${GATE_TIMEOUT:-2400}"
fi
export CARGO_BUILD_JOBS="$GATE_JOBS"

GATE_RUN_DIR="$ROOT/.gate"
LOCK_DIR="$GATE_RUN_DIR/lock"
mkdir -p "$GATE_RUN_DIR"
TS="$(date '+%Y%m%d-%H%M%S')"
LOG="${GATE_LOG:-$GATE_RUN_DIR/gate-$MODE-$TS.log}"

START_EPOCH="$(date +%s)"
MAIN_PID=$$
CUR_PGID=""
WATCHDOG_PID=""
LOCK_HELD=0
TIMED_OUT=0

# ─── Console logging (status to stderr; cargo output goes to $LOG) ──────────
say()  { printf '\033[1;36m[gate:%s]\033[0m %s\n' "$MODE" "$*" >&2; }
warn() { printf '\033[1;33m[gate:%s] %s\033[0m\n' "$MODE" "$*" >&2; }
err()  { printf '\033[1;31m[gate:%s] %s\033[0m\n' "$MODE" "$*" >&2; }

elapsed() { echo $(( $(date +%s) - START_EPOCH ))s; }

# ─── Memory / swap probes (macOS) ───────────────────────────────────────────
free_pct() {
  # `memory_pressure` prints: "System-wide memory free percentage: NN%"
  memory_pressure 2>/dev/null \
    | awk -F': ' '/free percentage/ {gsub(/[ %]/,"",$2); print $2; exit}'
}
swap_used_mb() {
  # vm.swapusage: "total = 7168.00M  used = 6013.00M  free = 1155.00M ..."
  sysctl -n vm.swapusage 2>/dev/null \
    | awk '{for(i=1;i<=NF;i++) if($i=="used"){gsub(/M/,"",$(i+2)); printf "%d", $(i+2); exit}}'
}

# ─── Diagnostic dump (the whole point of (f)) ───────────────────────────────
diagnose() {
  local why="$1"
  {
    echo
    echo "════════════════════════════════════════════════════════════════"
    echo "  GATE DIAGNOSTIC — $why  (elapsed $(elapsed))"
    echo "════════════════════════════════════════════════════════════════"
    echo
    echo "── cargo / rustc / nextest processes (STAT = state code) ──"
    echo "   D=uninterruptible-IO/swap  T=stopped  Z=zombie"
    echo "   uniform SN + tiny RSS + swap-full ⇒ OVERLAPPING-CARGO THRASH,"
    echo "   NOT a parking_lot deadlock (which parks only the contending"
    echo "   test while nextest's 60s slow-timeout kills it)."
    ps -axo pid,ppid,pgid,stat,rss,%cpu,etime,command 2>/dev/null \
      | awk 'NR==1 || /cargo|rustc|nextest/' | grep -v -E 'awk |gate\.sh'
    echo
    echo "── cargo-lock holder(s) (the deterministic contention signal) ──"
    local g found=0
    for g in "${LOCK_GLOBS[@]}"; do
      if [ -e "$g" ]; then
        found=1
        lsof -- "$g" 2>/dev/null || echo "   (no holder; lock file present, unlocked)"
      fi
    done
    [ "$found" = 0 ] && echo "   (no cargo-lock file present at $TARGET)"
    echo
    echo "── swap + memory ──"
    echo "   vm.swapusage: $(sysctl -n vm.swapusage 2>/dev/null)"
    echo "   free memory%: $(free_pct)%"
    vm_stat 2>/dev/null | awk '/Pageins|Pageouts|Swapins|Swapouts/ {print "   "$0}'
    echo
    echo "── tail of $LOG (static file read; NOT a live pipe) ──"
    tail -n 40 "$LOG" 2>/dev/null | sed 's/^/   /'
    echo "════════════════════════════════════════════════════════════════"
  } >&2
}

# ─── Process-group teardown (no setsid on macOS → job-control pgid) ──────────
teardown_step() {
  [ -z "$CUR_PGID" ] && return 0
  local pg="$CUR_PGID"
  CUR_PGID=""
  kill -TERM "-${pg}" 2>/dev/null
  local i=0
  while [ $i -lt 12 ]; do
    kill -0 "-${pg}" 2>/dev/null || break
    sleep 0.25
    i=$((i + 1))
  done
  kill -KILL "-${pg}" 2>/dev/null
}

cleanup() {
  trap - EXIT INT TERM USR1
  [ -n "$WATCHDOG_PID" ] && kill "$WATCHDOG_PID" 2>/dev/null
  teardown_step
  [ "$LOCK_HELD" = 1 ] && rm -rf "$LOCK_DIR"
}

on_signal() {
  err "interrupted — tearing down the whole process group"
  diagnose "interrupted (signal)"
  cleanup
  exit 130
}

on_timeout() {
  TIMED_OUT=1
  err "TIMEOUT after ${GATE_TIMEOUT}s — classifying the stall, then tearing down"
  diagnose "self-timeout (${GATE_TIMEOUT}s)"
  cleanup
  exit 124
}

trap on_signal INT TERM
trap on_timeout USR1
trap 'cleanup' EXIT

# ─── (a) MUTEX: atomic mkdir lock + stale reclaim ───────────────────────────
acquire_lock() {
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "$MAIN_PID" > "$LOCK_DIR/pid"
    LOCK_HELD=1
    return 0
  fi
  local oldpid
  oldpid="$(cat "$LOCK_DIR/pid" 2>/dev/null || echo "")"
  if [ -n "$oldpid" ] && kill -0 "$oldpid" 2>/dev/null; then
    err "REFUSING: another gate run is active (pid $oldpid)."
    err "  Lock: $LOCK_DIR  —  wait for it, or kill pid $oldpid (it tears"
    err "  down its own process group cleanly). Do NOT launch a second cargo."
    return 1
  fi
  warn "reclaiming stale lock (holder pid ${oldpid:-?} is dead)"
  rm -rf "$LOCK_DIR"
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "$MAIN_PID" > "$LOCK_DIR/pid"
    LOCK_HELD=1
    return 0
  fi
  err "REFUSING: lost the lock race"
  return 1
}

# ─── (a cont.) Pre-flight: any OTHER cargo touching this target? ────────────
preflight_cargo() {
  local hits
  hits="$(pgrep -f 'cargo-nextest|cargo nextest' 2>/dev/null \
            | grep -v "^$MAIN_PID\$" || true)"
  if [ -n "$hits" ]; then
    err "REFUSING: a cargo-nextest process is already running (pid(s):"
    err "  $(echo "$hits" | tr '\n' ' ')). One cargo invocation per target"
    err "  at a time. Kill it by process group, do not stack a second run."
    return 1
  fi
  local g
  for g in "${LOCK_GLOBS[@]}"; do
    if [ -e "$g" ] && lsof -- "$g" >/dev/null 2>&1; then
      err "REFUSING: $g is held by another process:"
      lsof -- "$g" 2>/dev/null | sed 's/^/  /' >&2
      return 1
    fi
  done
  return 0
}

# ─── (d) RAM precheck — fail fast instead of thrashing ──────────────────────
ram_precheck() {
  local fp sw
  fp="$(free_pct)"
  sw="$(swap_used_mb)"
  say "memory: free ${fp:-?}%  ·  swap used ${sw:-?}MB  ·  jobs=$GATE_JOBS test-threads=$GATE_TEST_THREADS"
  if [ -n "${fp:-}" ] && [ "$fp" -lt "$GATE_MIN_FREE_PCT" ] 2>/dev/null; then
    err "REFUSING: free memory ${fp}% < GATE_MIN_FREE_PCT=${GATE_MIN_FREE_PCT}%."
    err "  Starting a full debug build now would swap-thrash. Free memory"
    err "  (close apps / let other builds finish) and retry."
    return 1
  fi
  if [ -n "$GATE_MAX_SWAP_MB" ] && [ -n "${sw:-}" ] \
     && [ "$sw" -gt "$GATE_MAX_SWAP_MB" ] 2>/dev/null; then
    err "REFUSING: swap used ${sw}MB > GATE_MAX_SWAP_MB=${GATE_MAX_SWAP_MB}MB."
    return 1
  fi
  return 0
}

# ─── (b)+(c)+(e) one sequential step in its own process group → $LOG ────────
run_step() {
  local label="$1"; shift
  say "▶ $label"
  {
    echo
    echo "═══════════════════════════════════════════════════════════════"
    echo "  $label   ·   $(date '+%Y-%m-%d %H:%M:%S')"
    echo "  cwd=$ROOT  CARGO_TARGET_DIR=$TARGET"
    echo "  CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS"
    echo "  \$ $*"
    echo "═══════════════════════════════════════════════════════════════"
  } >> "$LOG"

  # Job-control monitor mode → the backgrounded job is a process-group
  # LEADER whose PGID == its PID. `kill -- -PGID` then reaps cargo AND
  # every rustc/test child. This is the macOS-portable `setsid` substitute.
  set -m
  { "$@"; } >> "$LOG" 2>&1 &
  CUR_PGID=$!
  set +m
  wait "$CUR_PGID"
  local rc=$?
  CUR_PGID=""
  if [ "$rc" -ne 0 ]; then
    err "✗ $label FAILED (exit $rc) after $(elapsed) — log: $LOG"
    return "$rc"
  fi
  say "✓ $label ($(elapsed))"
  return 0
}

# ─── Main ───────────────────────────────────────────────────────────────────
command -v cargo-nextest >/dev/null 2>&1 || {
  err "cargo-nextest not found. Install (prebuilt, no compile):"
  err "  curl -LsSf https://get.nexte.st/latest/mac | tar zxf - -C ~/.cargo/bin"
  exit 127
}

acquire_lock || exit 75            # EX_TEMPFAIL: contend, don't stack
preflight_cargo || exit 75
ram_precheck || exit 75

say "log → $LOG   (tail it yourself in another shell if you want a live view)"
say "timeout $GATE_TIMEOUT s · mode=$MODE · target=$TARGET"

# Built-in watchdog (no macOS timeout(1)). On expiry it signals the main
# pid; the USR1 trap classifies the stall before teardown.
( sleep "$GATE_TIMEOUT"; kill -USR1 "$MAIN_PID" 2>/dev/null ) &
WATCHDOG_PID=$!

# NOTE: empty-array expansion under `set -u` is a bash 3.2 bug (macOS ships
# 3.2). `${arr[@]+"${arr[@]}"}` is the portable guard — expands to nothing
# when the array is empty/unset, to the quoted elements otherwise.
CLIPPY_DENY=()
[ "${GATE_CLIPPY_DENY:-0}" = 1 ] && CLIPPY_DENY=(-- -D warnings)

# Step 1 — formatting (fast; fail before any expensive compile).
run_step "rustfmt --check" \
  cargo fmt --all --check || exit $?

# Step 2 — clippy. default-members only (NO --workspace): the binding
# crates need napi-rs/maturin/wasm-pack toolchains and are excluded from
# default-members by design (see Cargo.toml).
if [ "$MODE" = "core" ]; then
  run_step "clippy (-p graphrefly-core)" \
    cargo clippy -p graphrefly-core --all-targets \
      ${CLIPPY_DENY[@]+"${CLIPPY_DENY[@]}"} || exit $?
else
  run_step "clippy (default-members, --all-targets)" \
    cargo clippy --all-targets \
      ${CLIPPY_DENY[@]+"${CLIPPY_DENY[@]}"} || exit $?
fi

# Step 3 — the test suite. ONE nextest invocation, bounded threads.
if [ "$MODE" = "core" ]; then
  run_step "nextest -p graphrefly-core (default profile)" \
    cargo nextest run -p graphrefly-core --test-threads "$GATE_TEST_THREADS" \
      ${NEXTEST_EXTRA[@]+"${NEXTEST_EXTRA[@]}"} || exit $?
else
  run_step "nextest --profile ci (default-members, incl. cascade_depth)" \
    cargo nextest run --profile ci --test-threads "$GATE_TEST_THREADS" \
      ${NEXTEST_EXTRA[@]+"${NEXTEST_EXTRA[@]}"} || exit $?
fi

[ "$WATCHDOG_PID" ] && kill "$WATCHDOG_PID" 2>/dev/null
say "GATE PASSED ✓  (total $(elapsed))  ·  log: $LOG"
exit 0
