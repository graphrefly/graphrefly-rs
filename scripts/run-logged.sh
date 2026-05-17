#!/usr/bin/env bash
# run-logged.sh — the ONE sanctioned way to run ANY long command and
# reliably know its terminal state, with zero false "is it hung?".
#
# ─── THE problem this solves ────────────────────────────────────────────────
# A long Slice B-2 session burned ~2h+ not on a real deadlock but on the
# *observation layer*: the agent repeatedly could not tell whether a long
# command (cargo build/test/bench, `mise run gate`) was hung, finished, or
# stalled. Distinct, recurring causes:
#
#   (a) The monitor grepped a pattern that the command does NOT guarantee to
#       emit (or grepped harness-buffered tool output), so it "timed out"
#       though the command had finished.
#   (b) Output piped through `tail`/a buffered pipe → nothing appeared until
#       EOF → looked hung the entire time it was actually working.
#   (c) macOS misreads: `vm_stat "Pages free"` is NOT memory pressure; there
#       is no GNU `timeout(1)` and no `setsid`; BSD `ps`/`sed`/`grep` differ.
#   (d) Subagent-spawned background processes leaking as stale parent-session
#       task entries (nothing actually running, but the UI says "running").
#   (e) Monitor cadence vs the prompt-cache window mismatch (sleeping a flat
#       300s is the worst case: cache miss with no amortization).
#   (f) cargo's own deterministic "Blocking waiting for file lock on artifact
#       directory" line being invisible because of (b).
#
# ─── The durable fix (this script) ──────────────────────────────────────────
# Make the failure STRUCTURALLY impossible, not "more guidance to remember":
#
#   1. DIRECT UNBUFFERED FILE LOG. Command stdout+stderr append straight to a
#      file. NEVER piped through `tail`/`grep`/a subshell. cargo's lock-wait
#      line (f) is therefore always visible the instant cargo prints it.
#   2. GUARANTEED TERMINAL SENTINEL. On EVERY terminal path — success,
#      failure, self-timeout, signal, crash, even an unexpected `set -e`
#      bail — exactly one canonical line is emitted to BOTH stdout and the
#      log:  <<<RUN-LOGGED:DONE>>> exit=<rc> reason=<...> ...
#      A monitor greps THAT (never tool output, never a non-guaranteed
#      progress string). If the command terminated, the sentinel exists.
#   3. PROCESS-GROUP LAUNCH + TEARDOWN. Job-control monitor mode makes the
#      job a process-group leader (PGID==PID); `kill -- -PGID` reaps the
#      command AND every child. macOS-portable `setsid` substitute. Zero
#      orphans → no leaked "still running" entries (d).
#   4. SELF-TIMEOUT WITH DIAGNOSTIC. Built-in watchdog (no `timeout(1)` on
#      macOS). On expiry it CLASSIFIES the stall — process STAT codes (not
#      %CPU), `memory_pressure` (not `vm_stat`), `vm.swapusage`, lock
#      holders via `lsof`, and cargo's lock-wait line hoisted out of the
#      log — then tears down and still emits the sentinel (reason=timeout).
#
# ─── Usage ──────────────────────────────────────────────────────────────────
# Standalone (any long command):
#   scripts/run-logged.sh -- cargo build --release
#   scripts/run-logged.sh -l bench -t 1800 -- cargo bench
#   RUN_TIMEOUT=600 scripts/run-logged.sh -- pnpm test
#   mise run run-logged -- <cmd> <args...>
#
# As a library (sourced by another script — e.g. gate.sh):
#   source "$ROOT/scripts/run-logged.sh"        # only defines rl_* funcs
#   rl_log_init; rl_traps_install; rl_start_watchdog "$TIMEOUT"
#   rl_run "step label" -- some command
#   rl_stop_watchdog; rl_finish ok 0
#
# ─── Options / env knobs (all optional) ─────────────────────────────────────
#   -l LABEL | RUN_LABEL=     label in the log header + sentinel (default: cmd)
#   -t SECS  | RUN_TIMEOUT=   watchdog seconds (default 1800)
#   -o PATH  | RUN_LOG=       log file path (default $RUN_LOG_DIR/run-<ts>.log)
#              RUN_LOG_DIR=   log dir (default <repo-or-cwd>/.runlog)
#              RUN_TAG=       console line prefix (default "run-logged")
#              RUN_LOCK_GLOBS=  space-sep lock files to `lsof` in the
#                               diagnostic (gate.sh passes cargo-lock globs)
#              RUN_PS_FILTER=   regex of process names to show in the
#                               diagnostic ps table (default: this run's
#                               pgid subtree only; gate passes
#                               'cargo|rustc|nextest')
#              RUN_DIAG_EXTRA=  extra text appended to the diagnostic (gate
#                               passes its thrash-vs-deadlock signature table)
#
# Exit codes: the command's own rc; 124 on self-timeout; 130 on signal.
set -u

# The ONE token a monitor greps. Guaranteed on every terminal path, emitted
# to stdout AND the log. Do not change without updating test-guidance.md and
# every monitor that depends on it.
RL_SENTINEL_TOKEN='<<<RUN-LOGGED:DONE>>>'

RL_TAG="${RUN_TAG:-run-logged}"
RL_START_EPOCH="$(date +%s)"
RL_MAIN_PID=$$
RL_CUR_PGID=""
RL_WATCHDOG_PID=""
RL_FINISHED=0
RL_LOG="${RUN_LOG:-}"
RL_LABEL="${RUN_LABEL:-}"

# ─── Console logging (status → stderr; command output → $RL_LOG only) ────────
rl_say()  { printf '\033[1;36m[%s]\033[0m %s\n' "$RL_TAG" "$*" >&2; }
rl_warn() { printf '\033[1;33m[%s] %s\033[0m\n'  "$RL_TAG" "$*" >&2; }
rl_err()  { printf '\033[1;31m[%s] %s\033[0m\n'  "$RL_TAG" "$*" >&2; }
rl_elapsed() { echo $(( $(date +%s) - RL_START_EPOCH ))s; }

# ─── macOS-correct memory / swap probes ─────────────────────────────────────
# `vm_stat "Pages free"` is NOT pressure (it ignores the compressor / file
# cache). `memory_pressure` is the real signal. There is no `free(1)`.
rl_free_pct() {
  memory_pressure 2>/dev/null \
    | awk -F': ' '/free percentage/ {gsub(/[ %]/,"",$2); print $2; exit}'
}
rl_swap_used_mb() {
  # vm.swapusage: "total = 7168.00M  used = 6013.00M  free = 1155.00M ..."
  sysctl -n vm.swapusage 2>/dev/null \
    | awk '{for(i=1;i<=NF;i++) if($i=="used"){gsub(/M/,"",$(i+2)); printf "%d", $(i+2); exit}}'
}

# ─── Resolve a log path ─────────────────────────────────────────────────────
rl_log_init() {
  if [ -z "$RL_LOG" ]; then
    local root dir ts
    root="$(git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")"
    dir="${RUN_LOG_DIR:-$root/.runlog}"
    mkdir -p "$dir"
    ts="$(date '+%Y%m%d-%H%M%S')"
    RL_LOG="$dir/run-$ts-$$.log"
  else
    mkdir -p "$(dirname "$RL_LOG")"
  fi
  : >> "$RL_LOG" || { rl_err "cannot write log: $RL_LOG"; exit 2; }
}

# ─── The universal stuck-run diagnostic (the whole point of #4) ─────────────
# Classifies a stall instead of guessing. Reads the log as a STATIC file
# (never a live pipe). Safe to call on any terminal path.
rl_diagnose() {
  local why="$1" g found=0
  {
    echo
    echo "════════════════════════════════════════════════════════════════"
    echo "  RUN-LOGGED DIAGNOSTIC — $why  (elapsed $(rl_elapsed))"
    echo "════════════════════════════════════════════════════════════════"
    echo
    echo "── process state (STAT, not %CPU) ──"
    echo "   D=uninterruptible-IO/swap  T=stopped  Z=zombie  R=running"
    echo "   uniform S/SN + tiny RSS + swap-full ⇒ swap THRASH (often"
    echo "   overlapping builds), NOT a code deadlock."
    if [ -n "${RUN_PS_FILTER:-}" ]; then
      ps -axo pid,ppid,pgid,stat,rss,%cpu,etime,command 2>/dev/null \
        | awk -v re="$RUN_PS_FILTER" 'NR==1 || $0 ~ re' \
        | grep -v -E 'awk |run-logged\.sh' | head -n 40
    elif [ -n "$RL_CUR_PGID" ]; then
      ps -axo pid,ppid,pgid,stat,rss,%cpu,etime,command 2>/dev/null \
        | awk -v pg="$RL_CUR_PGID" 'NR==1 || $3==pg' | head -n 40
    else
      echo "   (no live process group; command already terminated)"
    fi
    echo
    echo "── lock holders (deterministic contention signal) ──"
    for g in ${RUN_LOCK_GLOBS:-}; do
      if [ -e "$g" ]; then
        found=1
        lsof -- "$g" 2>/dev/null || echo "   $g present, no holder (unlocked)"
      fi
    done
    [ "$found" = 0 ] && echo "   (no RUN_LOCK_GLOBS lock file present)"
    echo
    echo "── cargo lock-wait line, if any (hoisted from the log) ──"
    if grep -n 'Blocking waiting for file lock' "$RL_LOG" 2>/dev/null | tail -n 3; then
      :
    else
      echo "   (none — cargo is not blocked on the artifact-directory lock)"
    fi
    echo
    echo "── memory / swap (macOS-correct) ──"
    echo "   memory free%: $(rl_free_pct)%   (memory_pressure, NOT vm_stat)"
    echo "   vm.swapusage: $(sysctl -n vm.swapusage 2>/dev/null)"
    vm_stat 2>/dev/null | awk '/Pageins|Pageouts|Swapins|Swapouts/ {print "   "$0}'
    echo
    echo "── tail of $RL_LOG (static file read, NOT a live pipe) ──"
    tail -n 40 "$RL_LOG" 2>/dev/null | sed 's/^/   /'
    [ -n "${RUN_DIAG_EXTRA:-}" ] && { echo; echo "$RUN_DIAG_EXTRA"; }
    echo "════════════════════════════════════════════════════════════════"
  } >&2
}

# ─── Process-group teardown (macOS has no setsid → job-control pgid) ─────────
rl_teardown_pgid() {
  [ -z "$RL_CUR_PGID" ] && return 0
  local pg="$RL_CUR_PGID"
  RL_CUR_PGID=""
  kill -TERM "-${pg}" 2>/dev/null
  local i=0
  while [ $i -lt 12 ]; do
    kill -0 "-${pg}" 2>/dev/null || break
    sleep 0.25
    i=$((i + 1))
  done
  kill -KILL "-${pg}" 2>/dev/null
}

# ─── The GUARANTEED terminal sentinel — idempotent, on every path ───────────
# Emits ONE canonical line to stdout AND the log. A monitor that greps
# "$RL_SENTINEL_TOKEN" can never false-hang: if the command reached any
# terminal state, this line exists.
rl_finish() {
  [ "$RL_FINISHED" = 1 ] && return 0
  RL_FINISHED=1
  local reason="$1" rc="$2"
  local line
  line="$RL_SENTINEL_TOKEN exit=${rc} reason=${reason} elapsed=$(rl_elapsed) end=$(date '+%Y-%m-%dT%H:%M:%S%z') label=\"${RL_LABEL}\" log=${RL_LOG}"
  # Log first (so the file ends with the marker), then stdout (so a
  # run_in_background capture also sees it without reading the file).
  [ -n "$RL_LOG" ] && printf '\n%s\n' "$line" >> "$RL_LOG" 2>/dev/null
  printf '%s\n' "$line"
}

# ─── Watchdog (no timeout(1) on macOS): signal main → trap classifies ───────
rl_start_watchdog() {
  local secs="$1"
  ( sleep "$secs"; kill -USR1 "$RL_MAIN_PID" 2>/dev/null ) &
  RL_WATCHDOG_PID=$!
}
rl_stop_watchdog() {
  [ -n "$RL_WATCHDOG_PID" ] && kill "$RL_WATCHDOG_PID" 2>/dev/null
  RL_WATCHDOG_PID=""
}

rl__on_signal() {
  rl_err "interrupted — tearing down the whole process group"
  rl_diagnose "interrupted (signal)"
  rl_teardown_pgid
  rl_stop_watchdog
  rl_finish signal 130
  exit 130
}
rl__on_timeout() {
  rl_err "TIMEOUT after ${RL_TIMEOUT:-?}s — classifying the stall, then tearing down"
  rl_diagnose "self-timeout (${RL_TIMEOUT:-?}s)"
  rl_teardown_pgid
  rl_stop_watchdog
  rl_finish timeout 124
  exit 124
}
rl__on_exit() {
  # Backstop: if we exit any other way without a sentinel, still emit one.
  [ "$RL_FINISHED" = 1 ] && return 0
  rl_teardown_pgid
  rl_stop_watchdog
  rl_finish exit "${1:-0}"
}
rl_traps_install() {
  trap rl__on_signal INT TERM
  trap rl__on_timeout USR1
  trap 'rl__on_exit $?' EXIT
}

# ─── Run ONE command in its own process group, output → $RL_LOG ─────────────
# Returns the command's rc. Does NOT emit the terminal sentinel itself (the
# caller decides ok/fail after possibly running more steps — gate.sh runs
# three). Writes a per-step header + footer to the log so progress is
# visible the instant it happens (direct write, never tail-buffered).
rl_run() {
  local label="$1"; shift
  [ "${1:-}" = "--" ] && shift
  [ -z "$RL_LABEL" ] && RL_LABEL="$label"
  rl_say "▶ $label"
  {
    echo
    echo "═══════════════════════════════════════════════════════════════"
    echo "  $label   ·   $(date '+%Y-%m-%d %H:%M:%S')"
    echo "  cwd=$PWD"
    echo "  \$ $*"
    echo "═══════════════════════════════════════════════════════════════"
  } >> "$RL_LOG"

  # Job-control monitor mode → the backgrounded job is a process-group
  # LEADER (PGID==PID). `kill -- -PGID` then reaps it AND every child.
  set -m
  { "$@"; } >> "$RL_LOG" 2>&1 &
  RL_CUR_PGID=$!
  set +m
  wait "$RL_CUR_PGID"
  local rc=$?
  RL_CUR_PGID=""
  {
    echo "── step '$label' exit=$rc  ·  $(date '+%Y-%m-%d %H:%M:%S') ──"
  } >> "$RL_LOG"
  if [ "$rc" -ne 0 ]; then
    if [ "$rc" -gt 128 ]; then
      rl_err "✗ $label CRASHED (signal $((rc - 128)), exit $rc) after $(rl_elapsed) — log: $RL_LOG"
    else
      rl_err "✗ $label FAILED (exit $rc) after $(rl_elapsed) — log: $RL_LOG"
    fi
  else
    rl_say "✓ $label ($(rl_elapsed))"
  fi
  return "$rc"
}

# ─── Standalone entrypoint (only when executed, not when sourced) ───────────
rl__main() {
  RL_LABEL="${RUN_LABEL:-}"
  RL_TIMEOUT="${RUN_TIMEOUT:-1800}"
  while [ $# -gt 0 ]; do
    case "$1" in
      -l) RL_LABEL="$2"; shift 2 ;;
      -t) RL_TIMEOUT="$2"; shift 2 ;;
      -o) RL_LOG="$2"; shift 2 ;;
      --) shift; break ;;
      -h|--help)
        sed -n '2,60p' "$0" | sed 's/^# \{0,1\}//' >&2
        exit 0 ;;
      *) break ;;
    esac
  done
  if [ $# -eq 0 ]; then
    rl_err "no command given. Usage: run-logged.sh [-l label] [-t secs] [-o log] -- CMD ARGS"
    exit 2
  fi
  [ -z "$RL_LABEL" ] && RL_LABEL="$1"

  rl_log_init
  rl_traps_install
  rl_say "log → $RL_LOG   (it is a DIRECT file — tail it yourself for a live view; never pipe the command through tail)"
  rl_say "timeout ${RL_TIMEOUT}s · label=$RL_LABEL"
  rl_say "monitor contract: grep the log (or this stdout) for the literal token  $RL_SENTINEL_TOKEN"
  rl_start_watchdog "$RL_TIMEOUT"

  rl_run "$RL_LABEL" -- "$@"
  local rc=$?

  rl_stop_watchdog
  if [ "$rc" -eq 0 ]; then
    rl_finish ok 0
  elif [ "$rc" -gt 128 ]; then
    rl_finish "crash" "$rc"
  else
    rl_finish fail "$rc"
  fi
  # rl__on_exit is idempotent (RL_FINISHED guard) — safe with the EXIT trap.
  exit "$rc"
}

# Dual-mode: define-only when sourced; run when executed. BASH_SOURCE[0]
# differs from $0 under `source` (works on macOS bash 3.2).
if [ "${BASH_SOURCE[0]:-$0}" = "${0}" ]; then
  rl__main "$@"
fi
