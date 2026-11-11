#!/usr/bin/env python3
"""Fail when active Rust source still carries a phrase for a retired concept.

The list names concepts the D797 kernel and the migration review retired
(graphrefly-rs:D11); a hit in crates/graphrefly/src means stale code or docs.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CRATE_SRC = ROOT / "crates" / "graphrefly" / "src"
STALE_PATTERNS = [
    "actor model as current",
    "structural Impl parity",
    "port-model authority",
    "cross-track ledger",
    "graphrefly-ts-owned shared docs",
    # Retired by graphrefly:D797 and the migration review (graphrefly-rs:D11).
    "ctx.up(",
    "up_next",
    "upNext",
    "onDeactivation",
    "onInvalidate",
    "resubscribable",
    "rewire_next",
    "replace_deps",
    "ctx.data::<",
    "ctx.batch::<",
    "observe_path",
    "ObserveMessage",
    "DescribeValue",
    "NodeVersion",
    "R-dirty-before-data",
    "first-run gate holds",
]


def main() -> int:
    hits = 0
    for path in sorted(CRATE_SRC.rglob("*.rs")):
        text = path.read_text()
        for phrase in STALE_PATTERNS:
            if phrase in text:
                print(f"{path.relative_to(ROOT)}: stale phrase: {phrase}", file=sys.stderr)
                hits += 1
    if hits:
        return 1
    print("active Rust source stale-phrase scan is clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
