#!/usr/bin/env bash
# check-tripwire-wiring.sh — D291 / D289 QA F2 hygiene check.
#
# Pins the D288 Q2 invariant ("no sink fire during BenchBatchContext
# handle dispatch") at the *wiring* level: every `bridge_sync*` fn in
# the bindings-js crate MUST call `assert_no_batch_handle(...)` somewhere
# in its body. The runtime invariant itself is pinned by the
# `assert_no_batch_handle_panics_when_flag_set` cargo regression test in
# `crates/graphrefly-bindings-js/src/batch_bindings.rs::tests`; this
# script protects against a future operator/structures binding adding a
# new `bridge_sync*` site and FORGETTING the tripwire call — that's
# silently invisible until exercised inside a held-batch window.
#
# Mechanism: for every `fn bridge_sync*` declaration found in the
# bindings-js sources, extract the fn body via awk (brace-counting that
# correctly handles multi-line `where` clauses BEFORE the opening `{`)
# and verify the body contains an `assert_no_batch_handle(` call.
#
# Exit codes:
#   0 — every `fn bridge_sync*` body contains `assert_no_batch_handle`.
#   1 — at least one violation (offender printed to stderr).
#   2 — invocation/repo error.
#
# Source: D289/D291 binding tripwire follow-on. Historical port-era notes were
# deleted from the active docs tree; keep this script self-contained.
# D291 /qa A6 (2026-05-25): strict mode (`set -euo pipefail`) + nullglob
# so empty `*.rs` globs don't iterate the literal pattern. The directory
# guard above catches the typical missing-dir case, but a renamed crate
# layout that drops the `.rs` files would otherwise silently pass.
set -euo pipefail
shopt -s nullglob

ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || {
  echo "check-tripwire-wiring.sh: not inside a git repo" >&2
  exit 2
}
cd "$ROOT" || exit 2

DIR="crates/graphrefly-bindings-js/src"
if [ ! -d "$DIR" ]; then
  echo "check-tripwire-wiring.sh: $DIR not found (run from graphrefly-rs root)" >&2
  exit 2
fi

violations=0

for file in "$DIR"/*.rs; do
  # awk pass:
  #   - When we see `fn bridge_sync...`, enter fn-tracking mode and
  #     buffer lines.
  #   - Brace-count to find the body's open `{` and matching close `}`.
  #   - The `body_started` flag is the KEY fix: we only check
  #     `depth == 0` AFTER seeing the first `{` (so multi-line `where`
  #     clauses between the fn signature and the body don't trick us
  #     into thinking the fn ended at depth=0 on its decl line).
  result="$(awk -v file="$file" '
    BEGIN {
      in_fn = 0
      body_started = 0
      depth = 0
      buf = ""
      fn_name = ""
      fn_start_line = 0
    }
    # D291 /qa A5 (2026-05-25): skip line-comment lines so a doc-comment
    # mention of `fn bridge_sync_foo` (e.g. a `/// See [`bridge_sync_unit`]`
    # reference, or a `// historical: fn bridge_sync_legacy ...` note)
    # does not enter fn-tracking mode and confuse the brace counter.
    # Whole-line `//` and `///` are stripped; mid-line `//` comments are
    # NOT (Rust forbids them inside fn signatures anyway).
    {
      stripped = $0
      sub(/^[ \t]*/, "", stripped)
      if (substr(stripped, 1, 2) == "//") next
    }
    !in_fn && match($0, /fn[ \t]+bridge_sync[A-Za-z0-9_]*/) {
      fn_name = substr($0, RSTART, RLENGTH)
      fn_start_line = NR
      in_fn = 1
      body_started = 0
      depth = 0
      buf = $0
      for (i = 1; i <= length($0); i++) {
        c = substr($0, i, 1)
        if (c == "{") { depth++; body_started = 1 }
        else if (c == "}") depth--
      }
      next
    }
    in_fn {
      buf = buf "\n" $0
      for (i = 1; i <= length($0); i++) {
        c = substr($0, i, 1)
        if (c == "{") { depth++; body_started = 1 }
        else if (c == "}") depth--
      }
      if (body_started && depth == 0) {
        if (index(buf, "assert_no_batch_handle(") == 0) {
          printf("VIOLATION %s:%d: %s body missing assert_no_batch_handle(...)\n",
                 file, fn_start_line, fn_name)
        }
        in_fn = 0
        body_started = 0
        depth = 0
        buf = ""
        fn_name = ""
      }
    }
  ' "$file")"

  if [ -n "$result" ]; then
    echo "$result" >&2
    violations=$((violations + 1))
  fi
done

if [ "$violations" -gt 0 ]; then
  echo "" >&2
  echo "check-tripwire-wiring.sh: $violations file(s) with at least one" >&2
  echo "  bridge_sync* fn missing the D288 Q2 tripwire call. Add" >&2
  echo "  \`crate::batch_bindings::assert_no_batch_handle(\"<file>::<fn>\");\`" >&2
  echo "  at the top of every bridge_sync* fn body so the Q2 invariant" >&2
  echo "  (\"no sink fire during BenchBatchContext handle dispatch\")" >&2
  echo "  is enforced uniformly across the binding surface." >&2
  exit 1
fi

echo "check-tripwire-wiring.sh: OK — every bridge_sync* fn body contains assert_no_batch_handle(...)"
exit 0
