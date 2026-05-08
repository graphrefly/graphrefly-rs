#!/usr/bin/env bash
# Serve the GraphReFly Rust port review site.
#
# Browsers block fetch() from file:// origins, so this site must run over HTTP.
# Defaults to port 8765. Override with the first arg, e.g. `./serve.sh 9000`.
#
# Run it — don't source it:
#     ./docs/review/site/serve.sh           (after `chmod +x`, already done)
#     bash docs/review/site/serve.sh        (works without execute bit)

# ─── Guard: detect being sourced (bash and zsh) ──────────────────────────────
# `. serve.sh` or `source serve.sh` would run inside the user's interactive
# shell — `set -e` would propagate, and any `exec` would replace the shell.
sourced=0
if [ -n "${ZSH_VERSION:-}" ]; then
  case "${ZSH_EVAL_CONTEXT:-}" in *:file*) sourced=1 ;; esac
elif [ -n "${BASH_VERSION:-}" ]; then
  (return 0 2>/dev/null) && sourced=1
fi
if [ "$sourced" = "1" ]; then
  printf '%s\n' \
    "serve.sh: do not source this script — run it as a command instead:" \
    "    bash docs/review/site/serve.sh" \
    "  or" \
    "    ./docs/review/site/serve.sh" >&2
  return 1 2>/dev/null || exit 1
fi

set -euo pipefail

PORT="${1:-8765}"

# Resolve script location (works in bash; zsh executing via shebang also works).
script_path="${BASH_SOURCE[0]:-$0}"
script_dir="$(cd "$(dirname "$script_path")" && pwd)"
docs_dir="$(cd "$script_dir/../../.." && pwd)/docs"

if [ ! -f "$docs_dir/flowcharts.md" ]; then
  echo "serve.sh: cannot find docs/flowcharts.md at $docs_dir — is the repo intact?" >&2
  exit 1
fi

# ─── Port-in-use detection + auto-fallback ────────────────────────────────────
# If the user passed an explicit port and it's busy, fail loudly. If they took
# the default (8765) and it's busy, walk forward to the next free port and
# announce the change clearly.
port_in_use() {
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
  else
    # Fallback: try to bind via python; loud but reliable.
    python3 - "$1" <<'PY' >/dev/null 2>&1
import socket, sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", int(sys.argv[1])))
    s.close()
    sys.exit(1)  # free
except OSError:
    sys.exit(0)  # busy
PY
  fi
}

explicit_port=0
[ $# -gt 0 ] && explicit_port=1

if port_in_use "$PORT"; then
  if [ "$explicit_port" = "1" ]; then
    cat <<EOF >&2
serve.sh: port $PORT is already in use.

Likely already-running review server (or a stale Claude Code preview_start).
Pick another port:
    bash docs/review/site/serve.sh 9000

Or find what's holding the port:
    lsof -nP -iTCP:$PORT -sTCP:LISTEN
EOF
    exit 1
  fi
  start=$PORT
  for offset in 1 2 3 4 5 6 7 8 9 10; do
    candidate=$((start + offset))
    if ! port_in_use "$candidate"; then PORT=$candidate; break; fi
  done
  if [ "$PORT" = "$start" ]; then
    echo "serve.sh: ports $start..$((start+10)) all busy; pass an explicit port" >&2
    exit 1
  fi
  echo "serve.sh: port $start busy, using $PORT instead" >&2
fi

cat <<EOF
GraphReFly Rust port — review site
   serving:  $docs_dir
   open:     http://localhost:$PORT/review/site/

Press Ctrl-C to stop.
EOF

# No `exec` here on purpose — keeps the parent bash so Ctrl-C semantics stay clean
# even if a user accidentally sources this from a future, less-defensive copy.
python3 -m http.server "$PORT" --directory "$docs_dir"
