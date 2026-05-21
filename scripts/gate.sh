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
# ─── Architecture (post run-logged.sh extraction) ───────────────────────────
# The generic "run a long command, ALWAYS know its terminal state, never
# false-hang" mechanism now lives in `scripts/run-logged.sh` and is reused
# by ANY long command (not just this gate). gate.sh SOURCES it and keeps
# only the gate-specific policy on top:
#
#   gate.sh OWNS                       run-logged.sh OWNS (sourced rl_*)
#   ──────────────────────────────     ───────────────────────────────────
#   (a) MUTEX — atomic mkdir lock      DIRECT UNBUFFERED FILE LOG (never
#       + cargo-nextest/lock preflight     tail-piped → cargo's "Blocking
#   (b) SEQUENTIAL fmt→clippy→nextest      waiting for file lock" line is
#       (never a `;`-chain)                visible the instant it prints)
#   (d) BOUNDED jobs + RAM precheck     GUARANTEED TERMINAL SENTINEL on
#   the thrash-vs-deadlock signature       every path (a monitor greps it)
#       table (passed as diagnostic    PROCESS-GROUP launch + teardown
#       context)                       SELF-TIMEOUT WITH macOS-correct
#                                          state/swap/lock DIAGNOSTIC
#
# This is behaviour-preserving: same mutex, same sequential steps, same
# bounded jobs, same `.gate/*.log` location, same diagnostic content. The
# ONE addition is the guaranteed terminal sentinel — emitted on success,
# failure, timeout and signal — so a monitor can NEVER false-hang on the
# gate either.
#
# See `~/src/graphrefly-ts/docs/test-guidance.md` § "Running long commands
# reliably / diagnosing a stuck run" and memory feedback
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
#   GATE_CLIPPY_DENY=0     opt-out of `-- -D warnings` (clippy default IS
#                          deny-warnings to match CI; only opt out when
#                          a slice intentionally lands warnings).
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

# ─── Reuse the generic run+observe+sentinel core ────────────────────────────
# Sourced (not executed): only defines rl_* helpers, runs no main. Provides
# rl_say/rl_warn/rl_err/rl_elapsed, rl_free_pct/rl_swap_used_mb,
# rl_run (process-group step → direct log), rl_diagnose (macOS-correct
# state/swap/lock dump), rl_teardown_pgid, rl_start_watchdog/rl_stop_watchdog,
# rl_finish (the GUARANTEED terminal sentinel).
# shellcheck source=scripts/run-logged.sh
. "$ROOT/scripts/run-logged.sh"

# ─── Mode + arg parse ───────────────────────────────────────────────────────
MODE="full"
if [ "${1:-}" = "core" ]; then MODE="core"; shift; fi
if [ "${1:-}" = "--" ]; then shift; fi
NEXTEST_EXTRA=("$@")            # appended to the nextest step (e.g. a filter)

RL_TAG="gate:$MODE"             # console prefix for rl_say/rl_warn/rl_err
RL_LABEL="gate:$MODE"           # stable sentinel label (not the last step's)

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
RL_LOG="${GATE_LOG:-$GATE_RUN_DIR/gate-$MODE-$TS.log}"   # rl_* write here

MAIN_PID=$$
LOCK_HELD=0

# ─── Diagnostic context handed to rl_diagnose ───────────────────────────────
# rl_diagnose already prints process STAT, memory_pressure, vm.swapusage,
# vm_stat pageins, lock holders and a static log tail. These add the
# cargo-lock globs to lsof, the ps filter, and the thrash-vs-deadlock
# signature table — so the gate's diagnostic stays content-identical.
export RUN_LOCK_GLOBS="${LOCK_GLOBS[*]}"
export RUN_PS_FILTER='cargo|rustc|nextest'
export RUN_DIAG_EXTRA="── thrash-vs-deadlock signature (gate-specific) ──
   uniform SN + tiny RSS + swap-full + millions of pageins
     ⇒ OVERLAPPING-CARGO THRASH, not a parking_lot deadlock
       (a real deadlock parks only the contending test while
        nextest's 60s slow-timeout kills it).
   A present cargo-lock holder above + a 'Blocking waiting for
     file lock' line in the log ⇒ a SECOND cargo is running.
     Fix: one cargo per target — kill by process group, never
     stack a second run."

# ─── Terminal paths: gate-specific cleanup + the guaranteed sentinel ────────
# Behaviour-preserving vs the pre-extraction gate: same teardown + same
# mutex release. The ONE addition is rl_finish — the terminal sentinel a
# monitor greps (exit/reason on success, failure, timeout and signal).
cleanup() {
  trap - EXIT INT TERM USR1
  rl_stop_watchdog
  rl_teardown_pgid
  [ "$LOCK_HELD" = 1 ] && rm -rf "$LOCK_DIR"
}
on_signal() {
  rl_err "interrupted — tearing down the whole process group"
  rl_diagnose "interrupted (signal)"
  cleanup
  rl_finish signal 130
  exit 130
}
on_timeout() {
  rl_err "TIMEOUT after ${GATE_TIMEOUT}s — classifying the stall, then tearing down"
  rl_diagnose "self-timeout (${GATE_TIMEOUT}s)"
  cleanup
  rl_finish timeout 124
  exit 124
}
on_exit() {
  local ec=$?
  cleanup
  if [ "$ec" = 0 ]; then rl_finish ok 0; else rl_finish fail "$ec"; fi
}
trap on_signal INT TERM
trap on_timeout USR1
trap on_exit EXIT

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
    rl_err "REFUSING: another gate run is active (pid $oldpid)."
    rl_err "  Lock: $LOCK_DIR  —  wait for it, or kill pid $oldpid (it tears"
    rl_err "  down its own process group cleanly). Do NOT launch a second cargo."
    return 1
  fi
  rl_warn "reclaiming stale lock (holder pid ${oldpid:-?} is dead)"
  rm -rf "$LOCK_DIR"
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "$MAIN_PID" > "$LOCK_DIR/pid"
    LOCK_HELD=1
    return 0
  fi
  rl_err "REFUSING: lost the lock race"
  return 1
}

# ─── (a cont.) Pre-flight: any OTHER cargo touching this target? ────────────
preflight_cargo() {
  local hits
  hits="$(pgrep -f 'cargo-nextest|cargo nextest' 2>/dev/null \
            | grep -v "^$MAIN_PID\$" || true)"
  if [ -n "$hits" ]; then
    rl_err "REFUSING: a cargo-nextest process is already running (pid(s):"
    rl_err "  $(echo "$hits" | tr '\n' ' ')). One cargo invocation per target"
    rl_err "  at a time. Kill it by process group, do not stack a second run."
    return 1
  fi
  local g
  for g in "${LOCK_GLOBS[@]}"; do
    if [ -e "$g" ] && lsof -- "$g" >/dev/null 2>&1; then
      rl_err "REFUSING: $g is held by another process:"
      lsof -- "$g" 2>/dev/null | sed 's/^/  /' >&2
      return 1
    fi
  done
  return 0
}

# ─── (d) RAM precheck — fail fast instead of thrashing ──────────────────────
ram_precheck() {
  local fp sw
  fp="$(rl_free_pct)"
  sw="$(rl_swap_used_mb)"
  rl_say "memory: free ${fp:-?}%  ·  swap used ${sw:-?}MB  ·  jobs=$GATE_JOBS test-threads=$GATE_TEST_THREADS"
  if [ -n "${fp:-}" ] && [ "$fp" -lt "$GATE_MIN_FREE_PCT" ] 2>/dev/null; then
    rl_err "REFUSING: free memory ${fp}% < GATE_MIN_FREE_PCT=${GATE_MIN_FREE_PCT}%."
    rl_err "  Starting a full debug build now would swap-thrash. Free memory"
    rl_err "  (close apps / let other builds finish) and retry."
    return 1
  fi
  if [ -n "$GATE_MAX_SWAP_MB" ] && [ -n "${sw:-}" ] \
     && [ "$sw" -gt "$GATE_MAX_SWAP_MB" ] 2>/dev/null; then
    rl_err "REFUSING: swap used ${sw}MB > GATE_MAX_SWAP_MB=${GATE_MAX_SWAP_MB}MB."
    return 1
  fi
  return 0
}

# ─── Main ───────────────────────────────────────────────────────────────────
command -v cargo-nextest >/dev/null 2>&1 || {
  rl_err "cargo-nextest not found. Install (prebuilt, no compile):"
  rl_err "  curl -LsSf https://get.nexte.st/latest/mac | tar zxf - -C ~/.cargo/bin"
  exit 127
}

acquire_lock || exit 75            # EX_TEMPFAIL: contend, don't stack
preflight_cargo || exit 75
ram_precheck || exit 75

rl_log_init                        # ensure $RL_LOG exists/writable
{
  echo "gate mode=$MODE  ·  $(date '+%Y-%m-%d %H:%M:%S')"
  echo "cwd=$ROOT"
  echo "CARGO_TARGET_DIR=$TARGET"
  echo "CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS  ·  test-threads=$GATE_TEST_THREADS"
  echo "timeout=${GATE_TIMEOUT}s"
} >> "$RL_LOG"

rl_say "log → $RL_LOG   (DIRECT file — tail it yourself for a live view; never pipe through tail)"
rl_say "timeout $GATE_TIMEOUT s · mode=$MODE · target=$TARGET"
rl_say "monitor contract: grep the log (or stdout) for the literal token  $RL_SENTINEL_TOKEN"

# Built-in watchdog (no macOS timeout(1)). On expiry it signals the main
# pid; the USR1 trap (on_timeout) classifies the stall before teardown.
rl_start_watchdog "$GATE_TIMEOUT"

# NOTE: empty-array expansion under `set -u` is a bash 3.2 bug (macOS ships
# 3.2). `${arr[@]+"${arr[@]}"}` is the portable guard — expands to nothing
# when the array is empty/unset, to the quoted elements otherwise.
CLIPPY_DENY=()
# Default-on so the local gate catches what CI catches. Opt-out via
# GATE_CLIPPY_DENY=0 for slices that need to land warnings deliberately.
# Accept "0" or "false"/"no" as opt-out; any other value (including the
# default "1") enables deny-warnings. /qa-fix 2026-05-21: was previously
# `!= 0` which silently accepted typos like "fasle" as opt-in.
case "${GATE_CLIPPY_DENY:-1}" in
  0|false|no|FALSE|NO) ;;
  *) CLIPPY_DENY=(-- -D warnings) ;;
esac

# Step 1 — formatting (fast; fail before any expensive compile).
rl_run "rustfmt --check" \
  cargo fmt --all --check || exit $?

# Step 2 — clippy. default-members only (NO --workspace): the binding
# crates need napi-rs/maturin/wasm-pack toolchains and are excluded from
# default-members by design (see Cargo.toml).
if [ "$MODE" = "core" ]; then
  rl_run "clippy (-p graphrefly-core)" \
    cargo clippy -p graphrefly-core --all-targets \
      ${CLIPPY_DENY[@]+"${CLIPPY_DENY[@]}"} || exit $?
else
  rl_run "clippy (default-members, --all-targets)" \
    cargo clippy --all-targets \
      ${CLIPPY_DENY[@]+"${CLIPPY_DENY[@]}"} || exit $?
fi

# Step 3 — the test suite. ONE nextest invocation, bounded threads.
if [ "$MODE" = "core" ]; then
  rl_run "nextest -p graphrefly-core (default profile)" \
    cargo nextest run -p graphrefly-core --test-threads "$GATE_TEST_THREADS" \
      ${NEXTEST_EXTRA[@]+"${NEXTEST_EXTRA[@]}"} || exit $?
else
  rl_run "nextest --profile ci (default-members, incl. cascade_depth)" \
    cargo nextest run --profile ci --test-threads "$GATE_TEST_THREADS" \
      ${NEXTEST_EXTRA[@]+"${NEXTEST_EXTRA[@]}"} || exit $?
fi

rl_stop_watchdog
rl_say "GATE PASSED ✓  (total $(rl_elapsed))  ·  log: $RL_LOG"
# on_exit (EXIT trap) runs cleanup + emits the success sentinel (rl_finish ok 0).
exit 0
