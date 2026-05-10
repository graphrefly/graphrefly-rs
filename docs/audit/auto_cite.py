#!/usr/bin/env python3
"""
auto_cite.py — flag tests whose names look like they cover a canonical rule
but whose body has no `R<x.y>` citation. Suggest a `// covers R<x.y>` comment
the author can paste in.

Reads:
  - docs/audit/data/tests.jsonl  (output of extract.py)
  - docs/audit/data/rules.jsonl
Writes:
  - docs/audit/data/auto_cite_suggestions.jsonl
  - stdout summary

Heuristic — match a test name against a rule when:

  1. Direct embedded id: `pause_overflow_r1_3_8_c` → R1.3.8.c
     Matches `r\\d+_\\d+(_\\d+)?(_[a-z])?` in the snake-cased name and re-folds
     the underscores back into `R1.3.8.c`.

  2. Topical token match (looser):
     The rule title is tokenized (lowercase, split on non-alnum). The test name
     is split the same way. If ≥3 distinctive title tokens appear in the test
     name, suggest the rule. "Distinctive" = not in a stop-word set
     (the, and, of, by, …) AND not a single character.

For each suggestion the script emits:
  { test_name, file, line, suggested_rule, reason: "embedded-id" | "title-match",
    confidence: 0..1, snippet: "// covers Rx.y.z" }

Run:
  python3 docs/audit/auto_cite.py
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(os.environ.get("GRAPHREFLY_RS_ROOT", "/Users/davidchenallio/src/graphrefly-rs")).resolve()
DATA = ROOT / "docs" / "audit" / "data"

STOP = {
    "the", "a", "an", "and", "or", "of", "to", "for", "in", "on", "is", "be",
    "by", "as", "at", "with", "via", "from", "onto", "out", "into", "than",
    "this", "that", "it", "its", "we", "if", "then", "else", "but",
    "test", "tests", "tested", "testing",
    "rs",
}


def load_jsonl(path: Path):
    if not path.is_file():
        print(f"warn: {path} missing", file=sys.stderr)
        return []
    out = []
    for ln in path.read_text().split("\n"):
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.append(json.loads(ln))
        except json.JSONDecodeError:
            print(f"warn: bad json in {path}: {ln[:80]}", file=sys.stderr)
    return out


def tokenize(s: str) -> list[str]:
    return [t for t in re.split(r"[^a-z0-9]+", s.lower()) if t]


def distinctive(tokens: list[str]) -> set[str]:
    return {t for t in tokens if len(t) > 1 and t not in STOP}


# Embedded-id regex: r1_3_8_c, r2_4, r10_1_2_a
EMBEDDED_RE = re.compile(r"\br(\d+(?:_\d+)+(?:_[a-z])?)\b", re.IGNORECASE)


def fold_id(raw: str) -> str:
    """`1_3_8_c` → `R1.3.8.c`; `2_4` → `R2.4`."""
    return "R" + raw.replace("_", ".").lower()


def main():
    tests = load_jsonl(DATA / "tests.jsonl")
    rules = load_jsonl(DATA / "rules.jsonl")

    rule_ids = {r["id"]: r for r in rules}
    # Pre-tokenize rule titles + their parent section for the title-match path.
    # Section titles add valuable context (e.g. "Message Protocol" → "message",
    # "protocol" tokens that small rules like R1.3.x can also match against).
    rule_tokens = {}
    for r in rules:
        title = r.get("title", "")
        section = r.get("section", "")
        toks = distinctive(tokenize(title) + tokenize(section))
        if toks:
            rule_tokens[r["id"]] = toks

    suggestions = []

    for t in tests:
        if t.get("covers_rules"):
            continue  # already cites at least one rule
        name = t.get("name", "")
        if not name:
            continue

        candidates = []  # (rule_id, reason, confidence)

        # 1) Embedded-id hits
        for m in EMBEDDED_RE.finditer(name):
            folded = fold_id(m.group(1))
            if folded in rule_ids:
                candidates.append((folded, "embedded-id", 0.98))

        # 2) Topical token match
        if not candidates:
            test_toks = distinctive(tokenize(name))
            if len(test_toks) >= 2:
                best = []
                for rid, rtoks in rule_tokens.items():
                    overlap = test_toks & rtoks
                    if len(overlap) >= 2:
                        # Confidence: overlap-count weighted by rarity-in-title.
                        # 2 hits → 0.55, 3 → 0.70, 4+ → 0.80
                        conf = min(0.80, 0.40 + 0.12 * len(overlap))
                        best.append((rid, "title-match", conf, len(overlap)))
                # Keep the top-3 most-overlapping rules
                best.sort(key=lambda x: (-x[3], x[0]))
                for rid, reason, conf, _ in best[:3]:
                    candidates.append((rid, reason, conf))

        for rid, reason, conf in candidates:
            suggestions.append({
                "test_name": name,
                "file": t.get("file"),
                "line": t.get("line"),
                "suggested_rule": rid,
                "reason": reason,
                "confidence": round(conf, 2),
                "snippet": f"// covers {rid}",
            })

    # Write JSONL
    out_path = DATA / "auto_cite_suggestions.jsonl"
    with out_path.open("w") as f:
        for s in suggestions:
            f.write(json.dumps(s, separators=(",", ":")))
            f.write("\n")

    # Summary
    by_reason = defaultdict(int)
    by_confidence_bucket = defaultdict(int)
    files_touched = set()
    for s in suggestions:
        by_reason[s["reason"]] += 1
        bucket = (
            "high (≥0.85)" if s["confidence"] >= 0.85 else
            "med  (≥0.65)" if s["confidence"] >= 0.65 else
            "low  (<0.65)"
        )
        by_confidence_bucket[bucket] += 1
        files_touched.add(s["file"])

    total_uncited = sum(1 for t in tests if not t.get("covers_rules"))
    print(f"Tests: {len(tests)} total · {total_uncited} uncited ({len(tests) - total_uncited} cite ≥1 rule)")
    print(f"Suggestions: {len(suggestions)} across {len(files_touched)} files")
    for k, v in sorted(by_reason.items()):
        print(f"  by reason   · {k:12s}: {v}")
    for k in ["high (≥0.85)", "med  (≥0.65)", "low  (<0.65)"]:
        if k in by_confidence_bucket:
            print(f"  by conf     · {k}: {by_confidence_bucket[k]}")
    print(f"\nWrote {out_path}")
    if suggestions:
        print("\nFirst 10 high-confidence suggestions:")
        high = [s for s in suggestions if s["confidence"] >= 0.85][:10]
        for s in high:
            print(f"  {s['file']}:{s['line']:<5} {s['test_name']}")
            print(f"    → suggest: {s['snippet']}  (conf {s['confidence']}, via {s['reason']})")


if __name__ == "__main__":
    main()
