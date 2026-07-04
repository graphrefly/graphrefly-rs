#!/usr/bin/env python3
"""Generate neutral rustdoc comments from rustdoc missing-doc diagnostics.

This script is intentionally conservative:

- rustdoc remains the source of truth for what is missing.
- existing doc comments are preserved.
- generated comments are neutral and descriptive, not semantic claims.
- comments are only inserted with --apply.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CRATE_SRC = ROOT / "crates" / "graphrefly" / "src"
STALE_PATTERNS = [
    "actor model as current",
    "structural Impl parity",
    "port-model authority",
    "cross-track ledger",
    "graphrefly-ts-owned shared docs",
]


@dataclass(frozen=True)
class Diagnostic:
    path: Path
    line: int
    kind: str


def run_rustdoc() -> tuple[int, str]:
    env = os.environ.copy()
    env["RUSTDOCFLAGS"] = "-D missing_docs"
    if shutil.which("mise"):
        cmd = ["mise", "exec", "--", "cargo", "doc", "-p", "graphrefly-rs", "--all-features", "--no-deps"]
    else:
        cmd = ["cargo", "doc", "-p", "graphrefly-rs", "--all-features", "--no-deps"]
    proc = subprocess.run(cmd, cwd=ROOT, env=env, text=True, capture_output=True, check=False)
    return proc.returncode, proc.stdout + proc.stderr


def parse_diagnostics(output: str) -> list[Diagnostic]:
    diagnostics: list[Diagnostic] = []
    current_kind: str | None = None
    error_re = re.compile(r"^error: missing documentation for (?:an? )?(?P<kind>.+)$")
    loc_re = re.compile(r"^\s*-->\s+(?P<path>[^:]+):(?P<line>\d+):\d+")
    for raw in output.splitlines():
        line = raw.rstrip()
        error_match = error_re.match(line)
        if error_match:
            current_kind = error_match.group("kind").strip()
            continue
        loc_match = loc_re.match(line)
        if current_kind and loc_match:
            path = Path(loc_match.group("path"))
            if not path.is_absolute():
                path = ROOT / path
            diagnostics.append(Diagnostic(path=path, line=int(loc_match.group("line")), kind=current_kind))
            current_kind = None
    return diagnostics


def module_phrase(path: Path) -> str:
    try:
        relative = path.relative_to(CRATE_SRC)
    except ValueError:
        return "GraphReFly's Rust package"
    parts = list(relative.with_suffix("").parts)
    if parts[-1] == "mod":
        parts = parts[:-1]
    name = "::".join(parts)
    return f"the `{name}` module" if name else "GraphReFly's Rust package"


def extract_name(source_line: str, kind: str) -> str:
    line = source_line.strip()
    if kind in {"field", "struct field"}:
        field = line.split(":", 1)[0].replace("pub", "").strip()
        return field or "field"
    if kind in {"variant", "enum variant"}:
        return re.split(r"[\s({,=]", line, maxsplit=1)[0].strip() or "variant"
    match = re.search(
        r"\b(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:unsafe\s+)?"
        r"(?:(?:const\s+)?fn|struct|enum|trait|type|const|static|mod)\s+([A-Za-z_][A-Za-z0-9_]*)",
        line,
    )
    if match:
        return match.group(1)
    macro_match = re.search(r"macro_rules!\s*([A-Za-z_][A-Za-z0-9_]*)", line)
    if macro_match:
        return macro_match.group(1)
    return "item"


def comment_for(diag: Diagnostic, source_line: str) -> list[str]:
    kind = diag.kind
    name = extract_name(source_line, kind)
    context = module_phrase(diag.path)
    if kind in {"field", "struct field"}:
        return [f"/// `{name}` value for {context}."]
    if kind in {"variant", "enum variant"}:
        return [f"/// `{name}` variant for {context}."]
    if kind == "module":
        return [f"/// Rust package support for {context}."]
    article = "an" if kind[:1].lower() in {"a", "e", "i", "o", "u"} else "a"
    return [f"/// {name} is {article} {kind} for {context}."]


def has_doc_before(lines: list[str], index: int) -> bool:
    cursor = index - 1
    while cursor >= 0 and (not lines[cursor].strip() or lines[cursor].lstrip().startswith("#[")):
        cursor -= 1
    if cursor < 0:
        return False
    stripped = lines[cursor].lstrip()
    return stripped.startswith("///") or stripped.startswith("//!") or stripped.startswith("/**")


def apply_diagnostics(diagnostics: list[Diagnostic], dry_run: bool) -> int:
    by_file: dict[Path, list[Diagnostic]] = {}
    for diag in diagnostics:
        by_file.setdefault(diag.path, []).append(diag)

    inserted = 0
    for path, file_diags in sorted(by_file.items()):
        lines = path.read_text().splitlines()
        offset = 0
        for diag in sorted(file_diags, key=lambda item: item.line):
            index = diag.line - 1 + offset
            if index < 0 or index >= len(lines) or has_doc_before(lines, index):
                continue
            indent = re.match(r"^\s*", lines[index]).group(0)
            docs = [indent + line for line in comment_for(diag, lines[index])]
            lines[index:index] = docs
            offset += len(docs)
            inserted += len(docs)
            print(f"{path.relative_to(ROOT)}:{diag.line}: inserted generated {diag.kind} docs")
        if not dry_run and offset:
            path.write_text("\n".join(lines) + "\n")
    return inserted


def check_stale_phrases() -> int:
    hits = 0
    for path in sorted(CRATE_SRC.rglob("*.rs")):
        text = path.read_text()
        for phrase in STALE_PATTERNS:
            if phrase in text:
                print(f"{path.relative_to(ROOT)}: stale phrase: {phrase}", file=sys.stderr)
                hits += 1
    return hits


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="insert generated rustdoc comments")
    parser.add_argument("--check-stale", action="store_true", help="fail on known stale doc phrases in active Rust source")
    args = parser.parse_args()

    status, output = run_rustdoc()
    diagnostics = parse_diagnostics(output)
    if status == 0 and not diagnostics:
        print("rustdoc missing-doc gate is clean; no generated comments needed")
    elif not diagnostics:
        print(output, file=sys.stderr)
        return status or 1
    else:
        inserted = apply_diagnostics(diagnostics, dry_run=not args.apply)
        if not args.apply:
            print(f"{len(diagnostics)} missing-doc diagnostics found; rerun with --apply to insert templates")
            return 1
        print(f"inserted {inserted} generated rustdoc comment lines")
        if inserted:
            return 1

    if args.check_stale:
        stale_hits = check_stale_phrases()
        if stale_hits:
            return 1
        print("active Rust source stale-phrase scan is clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
