#!/usr/bin/env python3
"""
extract.py — emit items.jsonl + rules.jsonl for the graphrefly-rs audit site.

Walks crates/*/{src,tests}/**/*.rs with a small regex+brace-depth scanner
(NOT a real parser — naive but stable enough for v0). Emits one row per
public-or-crate-private item with file/line/loc/visibility/unsafe/attrs
plus the spec-rule IDs (R<x.y>[a-z]?) cited in the doc-comment block
immediately preceding the item.

Spec rules are extracted from the canonical-spec markdown via header regex.

Usage:
    python3 docs/audit/extract.py              # uses defaults
    GRAPHREFLY_RS_ROOT=… GRAPHREFLY_TS_ROOT=… python3 docs/audit/extract.py
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

REPO_ROOT  = Path(os.environ.get("GRAPHREFLY_RS_ROOT", "/Users/davidchenallio/src/graphrefly-rs")).resolve()
SPEC_PATH  = Path(os.environ.get(
    "GRAPHREFLY_SPEC", "/Users/davidchenallio/src/graphrefly-ts/docs/implementation-plan-13.6-canonical-spec.md"
)).resolve()
FLOWCHARTS_PATH = Path(os.environ.get(
    "GRAPHREFLY_FLOWCHARTS", str(REPO_ROOT / "docs" / "flowcharts.md")
)).resolve()
OUT_DIR    = REPO_ROOT / "docs" / "audit" / "data"

CRATE_DIRS = REPO_ROOT / "crates"

# Regex toolkit
RULE_ID_RE     = re.compile(r"\bR\d+(?:\.[\da-z]+)+\b")
ITEM_RE        = re.compile(
    r"""^(?P<vis>pub(?:\([^)]*\))?\s+|)                # visibility
         (?P<modifier>(?:async\s+|const\s+|unsafe\s+|extern\s+\"[^\"]*\"\s+)*)
         (?P<kind>fn|struct|enum|trait|type|const|static|mod|impl|union|macro_rules!)
         \b(?P<rest>.*)$""",
    re.VERBOSE,
)
DOC_LINE_RE    = re.compile(r"^\s*///(?:!)?\s?(.*)$")
ATTR_LINE_RE   = re.compile(r"^\s*#\[(.*?)\]\s*$")
INNER_DOC_RE   = re.compile(r"^\s*//!\s?(.*)$")
SPEC_HEAD_RE   = re.compile(r"^####\s+(?:R)?(\d+\.\d+(?:\.[a-z\d])?)\s+(?:[—-]\s+)?(.+?)\s*$")
SPEC_SECTION_RE = re.compile(r"^##\s+(.+?)\s*$")  # used to track parent section


def write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r, separators=(",", ":")))
            f.write("\n")


def split_signature_name(kind: str, rest: str) -> str:
    """
    Best-effort name extraction from the `rest` capture of ITEM_RE.
    For `fn foo<T>(...)` returns "foo". For `impl Foo for Bar` returns
    "impl Foo for Bar" (we don't try to canonicalize). Returns "" if
    nothing identifier-shaped is found.
    """
    rest = rest.strip()
    if kind == "impl":
        # "impl Foo" or "impl<T> Foo<T>" or "impl Foo for Bar"
        m = re.match(r"^(?:<[^>]*>\s*)?(?P<head>[\w:]+)(?:<[^>]*>)?(?:\s+for\s+(?P<for>[\w:]+))?", rest)
        if not m:
            return rest.split("{", 1)[0].strip()
        if m.group("for"):
            return f"{m.group('head')} for {m.group('for')}"
        return m.group("head")
    if kind == "macro_rules!":
        m = re.match(r"^(\w+)", rest)
        return m.group(1) if m else ""
    # generic identifier
    m = re.match(r"^(\w+)", rest)
    return m.group(1) if m else ""


def find_item_end(lines: list[str], start_idx: int) -> int:
    """
    Naive brace-depth scan. Returns the line index (0-based, inclusive)
    where the item ends. For `;`-terminated items (use, type aliases,
    const decls without body) returns the line containing the semicolon
    at depth 0. Strings/chars/line-comments are not perfectly handled,
    but we're producing a metric (LOC), not parsing. Good enough.
    """
    depth = 0
    started = False
    in_block_comment = False
    for i in range(start_idx, len(lines)):
        line = lines[i]
        # strip line comments to avoid // }} fooling us
        # very rough: treat the first occurrence of // as end of code
        code = line
        if not in_block_comment:
            # quick string/char/comment skip — naive
            j = 0
            cleaned = []
            in_str = False
            in_chr = False
            while j < len(code):
                c = code[j]
                if not in_str and not in_chr:
                    if c == "/" and j + 1 < len(code) and code[j + 1] == "/":
                        break
                    if c == "/" and j + 1 < len(code) and code[j + 1] == "*":
                        in_block_comment = True
                        j += 2
                        continue
                    if c == '"':
                        in_str = True
                    elif c == "'":
                        in_chr = True
                    cleaned.append(c)
                    j += 1
                elif in_str:
                    if c == "\\" and j + 1 < len(code):
                        j += 2
                        continue
                    if c == '"':
                        in_str = False
                    j += 1
                else:  # in_chr
                    if c == "\\" and j + 1 < len(code):
                        j += 2
                        continue
                    if c == "'":
                        in_chr = False
                    j += 1
            code = "".join(cleaned)
        else:
            # consume until */
            end = code.find("*/")
            if end == -1:
                continue
            in_block_comment = False
            code = code[end + 2 :]

        for c in code:
            if c == "{":
                depth += 1
                started = True
            elif c == "}":
                depth -= 1
                if started and depth <= 0:
                    return i
            elif c == ";" and not started and depth == 0:
                return i
    return len(lines) - 1


def consume_doc_block(lines: list[str], idx: int) -> tuple[list[str], list[str], int]:
    """
    Walks BACKWARD from idx-1, collecting consecutive doc lines (`///`)
    and outer attributes (`#[...]`). Stops at the first non-doc / non-attr
    / non-blank line.

    Returns (doc_lines_top_down, attrs_top_down, first_line_of_block).
    `first_line_of_block` is the 0-based index of the topmost line in the
    block (could equal idx if there's nothing).
    """
    docs = []
    attrs = []
    i = idx - 1
    first = idx
    while i >= 0:
        line = lines[i]
        stripped = line.strip()
        if stripped == "":
            # allow ONE blank between attrs/docs and the item — but bail
            # if we hit two in a row (rare in idiomatic rust)
            if i - 1 >= 0 and lines[i - 1].strip() == "":
                break
            i -= 1
            continue
        m_doc = DOC_LINE_RE.match(line)
        m_attr = ATTR_LINE_RE.match(line)
        if m_doc:
            docs.append(m_doc.group(1))
            first = i
            i -= 1
            continue
        if m_attr:
            attrs.append(m_attr.group(1))
            first = i
            i -= 1
            continue
        break
    docs.reverse()
    attrs.reverse()
    return docs, attrs, first


def doc_summary(doc_lines: list[str]) -> str:
    """First non-empty doc paragraph, joined with spaces, capped at 200 chars."""
    para = []
    for ln in doc_lines:
        if ln.strip() == "":
            if para:
                break
            continue
        para.append(ln.strip())
    text = " ".join(para)
    if len(text) > 200:
        text = text[:200].rstrip() + "…"
    return text


def normalize_visibility(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("pub(crate)"):
        return "pub(crate)"
    if raw.startswith("pub(super)"):
        return "pub(super)"
    if raw.startswith("pub("):
        return "pub-restricted"
    if raw.startswith("pub"):
        return "pub"
    return "priv"


def detect_unsafe(modifier: str, attrs: list[str]) -> bool:
    if "unsafe " in (modifier + " "):
        return True
    return any(a.startswith("unsafe") for a in attrs)


def crate_name_from_cargo_toml(cargo_toml: Path) -> str:
    try:
        text = cargo_toml.read_text()
    except OSError:
        return cargo_toml.parent.name
    m = re.search(r'^\s*name\s*=\s*"([^"]+)"', text, re.MULTILINE)
    return m.group(1) if m else cargo_toml.parent.name


def discover_crates() -> list[tuple[str, Path]]:
    """Walk crates/*/Cargo.toml, return (crate_name, crate_dir) list."""
    out = []
    if not CRATE_DIRS.exists():
        return out
    for sub in sorted(CRATE_DIRS.iterdir()):
        cargo = sub / "Cargo.toml"
        if cargo.is_file():
            out.append((crate_name_from_cargo_toml(cargo), sub))
    return out


def module_path_from_file(crate_dir: Path, file_path: Path, role: str) -> str:
    """
    Build a `::`-joined module path. For `src/foo/bar.rs` → `foo::bar`.
    For `src/lib.rs` → "" (root). `tests/foo.rs` → `tests::foo`.
    """
    rel = file_path.relative_to(crate_dir)
    parts = list(rel.parts)
    if parts and parts[0] in {"src", "tests"}:
        parts = parts[1:]
    if parts and parts[-1].endswith(".rs"):
        last = parts[-1][:-3]
        if last == "lib" or last == "mod":
            parts = parts[:-1]
        else:
            parts[-1] = last
    if role == "tests":
        parts = ["tests"] + parts
    return "::".join(parts)


# ─── extraction ──────────────────────────────────────────────────────

def is_test_fn(attrs: list[str]) -> bool:
    """A fn is a test iff one of its attrs is #[test] or #[tokio::test] etc."""
    for a in attrs:
        a = a.strip()
        if a == "test" or a.startswith("test("): return True
        if a in {"tokio::test", "test::test", "rstest", "test_log::test"}: return True
        if a.startswith("tokio::test"): return True
    return False


def is_ignored_test(attrs: list[str]) -> bool:
    return any(a.strip().startswith("ignore") for a in attrs)


def extract_file(crate_name: str, crate_dir: Path, file_path: Path, role: str, tests_out: list):
    """Yield item rows for one .rs file. Also appends test rows to `tests_out`."""
    try:
        raw = file_path.read_text(errors="replace")
    except OSError as e:
        print(f"warn: {file_path}: {e}", file=sys.stderr)
        return
    lines = raw.split("\n")
    file_loc = sum(1 for ln in lines if ln.strip() != "")
    rel_file = str(file_path.relative_to(REPO_ROOT))
    module = module_path_from_file(crate_dir, file_path, role)

    # File-level "module" row: doc summary from inner //! comments + cumulative
    # rule citations in any /// or //! line (used for matrix completeness).
    inner_docs = [m.group(1) for m in (INNER_DOC_RE.match(ln) for ln in lines) if m]
    file_rules_cited = sorted(set(RULE_ID_RE.findall("\n".join(inner_docs))))
    file_unsafe_count = len(re.findall(r"\bunsafe\b", raw))
    file_test_count = len(re.findall(r"^\s*#\[(?:test|tokio::test)\]", raw, re.MULTILINE))
    file_ignore_count = len(re.findall(r"^\s*#\[ignore", raw, re.MULTILINE))

    yield {
        "kind": "file",
        "crate": crate_name,
        "module": module,
        "file": rel_file,
        "line": 1,
        "loc": file_loc,
        "lines_total": len(lines),
        "visibility": "—",
        "unsafe": file_unsafe_count > 0,
        "unsafe_count": file_unsafe_count,
        "tests_in_file": file_test_count,
        "ignored_tests_in_file": file_ignore_count,
        "doc_summary": doc_summary(inner_docs),
        "rules_cited": file_rules_cited,
        "name": file_path.stem,
        "role": role,
    }

    # Item rows: scan line-by-line
    i = 0
    while i < len(lines):
        line = lines[i]
        m = ITEM_RE.match(line)
        if not m:
            i += 1
            continue
        # Skip lines that are clearly mid-statement (very rare false positives
        # for lines starting with `pub fn` or `impl` — we accept them).
        kind = m.group("kind")
        rest = m.group("rest")
        modifier = m.group("modifier") or ""
        vis_raw = m.group("vis") or ""
        # exclude `mod foo;` filesystem-mounts (not interesting on their own)
        # Actually keep them — they show subgraph mounts.
        docs, attrs, _ = consume_doc_block(lines, i)
        end = find_item_end(lines, i)
        item_loc = sum(1 for ln in lines[i:end + 1] if ln.strip() != "")
        name = split_signature_name(kind, rest)
        rules_cited = sorted(set(RULE_ID_RE.findall("\n".join(docs))))
        is_unsafe = detect_unsafe(modifier, attrs)
        path = "::".join([crate_name] + ([module] if module else []) + ([name] if name else []))

        # Test extraction: if this fn is a #[test], capture it in tests_out with
        # rule citations harvested from BOTH the doc-comment block and the body
        # (limited to the lines up through `end`, not the whole file).
        if kind == "fn" and is_test_fn(attrs):
            body_blob = "\n".join(lines[i:end + 1])
            body_rules = set(RULE_ID_RE.findall(body_blob))
            covers = sorted(body_rules | set(rules_cited))
            tests_out.append({
                "name": name,
                "fn_path": path,
                "crate": crate_name,
                "module": module,
                "file": rel_file,
                "line": i + 1,
                "end_line": end + 1,
                "loc": item_loc,
                "status": "ignored" if is_ignored_test(attrs) else "active",
                "covers_rules": covers,
                "doc_summary": doc_summary(docs),
                "attrs": attrs,
            })

        yield {
            "kind": "fn" if kind == "fn" else
                    "method" if kind == "fn" and " for " in rest else  # never reached but reserved
                    kind,
            "name": name,
            "path": path,
            "crate": crate_name,
            "module": module,
            "file": rel_file,
            "line": i + 1,
            "end_line": end + 1,
            "loc": item_loc,
            "visibility": normalize_visibility(vis_raw),
            "unsafe": is_unsafe,
            "attrs": attrs,
            "doc_summary": doc_summary(docs),
            "rules_cited": rules_cited,
            "role": role,
        }
        i = end + 1


def extract_workspace():
    crates = discover_crates()
    crate_names = {c[0] for c in crates}
    print(f"Found {len(crates)} crates", file=sys.stderr)
    items = []
    tests = []
    topology_edges = []  # raw rows; rolled up after
    locks = []
    for crate_name, crate_dir in crates:
        for role_dir, role in (("src", "src"), ("tests", "tests")):
            base = crate_dir / role_dir
            if not base.exists():
                continue
            for rs_file in sorted(base.rglob("*.rs")):
                items.extend(extract_file(crate_name, crate_dir, rs_file, role, tests))
                extract_topology_and_locks(crate_name, crate_names, rs_file, role, topology_edges, locks)
    # Roll topology edges to per-(from,to) aggregates
    aggregated = {}
    for e in topology_edges:
        key = (e["from"], e["to"], e["kind"])
        agg = aggregated.setdefault(key, {
            "from": e["from"], "to": e["to"], "kind": e["kind"],
            "count": 0, "files": set(),
        })
        agg["count"] += 1
        agg["files"].add(e["file"])
    topology = []
    for agg in aggregated.values():
        agg["files"] = sorted(agg["files"])
        topology.append(agg)
    return items, tests, topology, locks


# ─── topology + locks ──────────────────────────────────────────────────
USE_STMT_RE = re.compile(r"^\s*(?:pub(?:\s*\([^)]*\))?\s+)?use\s+([\w:{}*\s,]+)\s*;", re.MULTILINE)
USE_HEAD_RE = re.compile(r"^\s*([\w]+)")  # captures the first segment of a use path
EXTERN_PATH_RE = re.compile(r"\b([a-z][a-z0-9_]+(?:_[a-z0-9_]+)*)::")

LOCK_OP_RES = [
    (re.compile(r"\.lock\(\)"),       "lock"),
    (re.compile(r"\.read\(\)"),       "read"),
    (re.compile(r"\.write\(\)"),      "write"),
    (re.compile(r"\.try_lock\(\)"),   "try_lock"),
    (re.compile(r"\.try_read\(\)"),   "try_read"),
    (re.compile(r"\.try_write\(\)"),  "try_write"),
]
LOCK_NEW_RE = re.compile(r"\b(Mutex|RwLock|RefCell|parking_lot::(?:Mutex|RwLock|FairMutex))::new\b")
LOCK_TYPE_FROM_FN = {
    "lock": "Mutex/parking_lot",
    "try_lock": "Mutex/parking_lot",
    "read": "RwLock",
    "try_read": "RwLock",
    "write": "RwLock",
    "try_write": "RwLock",
}


def normalize_crate_name_dashes(s: str) -> str:
    """Rust converts `graphrefly_core` (Rust ident) ↔ `graphrefly-core` (cargo name)."""
    return s.replace("_", "-")


def extract_topology_and_locks(crate_name, crate_names, file_path, role, edges, locks):
    """
    Topology: for each `use foo::...` statement, if `foo` matches another
    workspace crate (after `_` ↔ `-` normalization), emit a use-edge.
    Bare path mentions (`foo::bar(...)`) are also harvested as call-edges
    when the head matches a known crate.

    Locks: scan for known lock-method calls and `Mutex::new` / `RwLock::new`.
    Each match becomes a row in `locks.jsonl`.
    """
    try:
        text = file_path.read_text(errors="replace")
    except OSError:
        return
    rel_file = str(file_path.relative_to(REPO_ROOT))
    lines = text.split("\n")

    # ── Topology: use statements
    for m in USE_STMT_RE.finditer(text):
        body = m.group(1)
        head_match = USE_HEAD_RE.match(body)
        if not head_match:
            continue
        head = head_match.group(1)
        head_dashed = normalize_crate_name_dashes(head)
        # Skip self-imports + std/core/alloc + unrelated externs
        if head in {"crate", "self", "super", "std", "core", "alloc"}:
            continue
        if head_dashed == crate_name:
            continue
        if head_dashed not in crate_names and ("graphrefly_" + head_dashed[len("graphrefly-"):] if head_dashed.startswith("graphrefly-") else "") not in crate_names:
            continue
        edges.append({"from": crate_name, "to": head_dashed, "kind": "use", "file": rel_file})

    # ── Topology: bare path mentions (best-effort call-edges)
    seen_calls = set()
    for m in EXTERN_PATH_RE.finditer(text):
        head = m.group(1)
        head_dashed = normalize_crate_name_dashes(head)
        if head in {"crate", "self", "super", "std", "core", "alloc"}:
            continue
        if head_dashed == crate_name or head_dashed not in crate_names:
            continue
        key = head_dashed
        if key in seen_calls:
            continue
        seen_calls.add(key)
        edges.append({"from": crate_name, "to": head_dashed, "kind": "ref", "file": rel_file})

    # ── Locks: scan each line
    for lineno, line in enumerate(lines, start=1):
        for op_re, op_name in LOCK_OP_RES:
            for _ in op_re.finditer(line):
                locks.append({
                    "crate": crate_name,
                    "file": rel_file,
                    "line": lineno,
                    "op": op_name,
                    "lock_type": LOCK_TYPE_FROM_FN.get(op_name, "?"),
                    "snippet": line.strip()[:160],
                    "role": role,
                })
        for _ in LOCK_NEW_RE.finditer(line):
            locks.append({
                "crate": crate_name,
                "file": rel_file,
                "line": lineno,
                "op": "new",
                "lock_type": (LOCK_NEW_RE.search(line).group(1) if LOCK_NEW_RE.search(line) else "?"),
                "snippet": line.strip()[:160],
                "role": role,
            })


def extract_rules():
    """
    Two-pass: (1) scan all #### headings to build {section_id → title};
    (2) scan the entire body for every `R\\d+\\.\\d+(\\.[a-z\\d])?` mention
    and attach each unique ID to its nearest enclosing section.

    A rule's `title` is taken from its own #### heading if it has one (e.g.
    `#### R3.9.a — graph.state…`); otherwise it falls back to the parent
    section heading (`#### 1.3.2 Equals substitution and cache discipline`)
    suffixed with the sub-letter so the row is still informative.
    """
    if not SPEC_PATH.exists():
        print(f"warn: {SPEC_PATH} missing — rules.jsonl will be empty", file=sys.stderr)
        return []

    text = SPEC_PATH.read_text()
    lines = text.split("\n")

    # Pass 1 — build heading map: section_num_str → (title, line)
    # Also build a map: top-level section number ("1", "2", …) → h2 title,
    # so a rule like R2.1.1 mentioned BEFORE section 2's ## heading can still
    # be attached to "2. Node".
    head_titles: dict[str, tuple[str, int]] = {}
    section_h2 = ""
    h2_map: dict[str, str] = {}  # section_num → enclosing h2
    h2_by_top: dict[str, str] = {}  # "1" → "1. Message Protocol", "2" → "2. Node", …
    for i, line in enumerate(lines):
        m_h2 = SPEC_SECTION_RE.match(line)
        if m_h2:
            section_h2 = m_h2.group(1).strip()
            top_match = re.match(r"(\d+)\.", section_h2)
            if top_match:
                h2_by_top[top_match.group(1)] = section_h2
            continue
        m = SPEC_HEAD_RE.match(line)
        if m:
            sec, title = m.group(1), re.sub(r"^[`'\"]|[`'\"]$", "", m.group(2).strip())
            head_titles[sec] = (title, i)
            h2_map[sec] = section_h2

    # Pass 2 — every distinct rule mention; attach to its nearest section
    rule_re = re.compile(r"\bR(\d+(?:\.[\da-z]+)+)\b")
    seen: dict[str, dict] = {}
    section_h2 = ""
    current_section_num = None
    current_section_h2 = ""
    for i, line in enumerate(lines):
        m_h2 = SPEC_SECTION_RE.match(line)
        if m_h2:
            section_h2 = m_h2.group(1).strip()
            continue
        m_h = SPEC_HEAD_RE.match(line)
        if m_h:
            current_section_num = m_h.group(1)
            current_section_h2 = section_h2
        for m in rule_re.finditer(line):
            sec_num = m.group(1)
            rid = f"R{sec_num}"
            if rid in seen:
                continue
            # Title preference: explicit heading > parent section title > derived
            if sec_num in head_titles:
                title, _ = head_titles[sec_num]
                section = h2_map.get(sec_num, current_section_h2)
            else:
                # Walk up: 1.3.2.d → 1.3.2 → 1.3 → 1
                parent = sec_num
                while "." in parent:
                    parent = parent.rsplit(".", 1)[0]
                    if parent in head_titles:
                        ptitle, _ = head_titles[parent]
                        # tag the sub-id we matched
                        suffix = sec_num[len(parent) + 1:]
                        title = f"{ptitle}" + (f" — sub-rule {suffix}" if suffix else "")
                        section = h2_map.get(parent, current_section_h2)
                        break
                else:
                    title = "(undocumented)"
                    section = current_section_h2
            seen[rid] = {
                "id": rid,
                "title": title,
                "section": section,
                "first_seen_line": i + 1,
                "anchor": f"../implementation-plan-13.6-canonical-spec.md#L{i + 1}",
            }
    # Always derive the section from the rule's numeric prefix, NOT from the
    # nearest ## heading at first mention — body text often references rules
    # from a later section before that section's ## heading appears.
    for r in seen.values():
        m = re.match(r"R(\d+)\.", r["id"])
        if m and m.group(1) in h2_by_top:
            r["section"] = h2_by_top[m.group(1)]

    def _sort_key(rid: str):
        # "R1.3.2.d" → ((1, ""), (3, ""), (2, ""), (0, "d"))
        out = []
        for p in rid[1:].split("."):
            if p.isdigit():
                out.append((int(p), ""))
            else:
                out.append((10**6, p))  # alpha suffixes sort after numerics
        return tuple(out)
    return sorted(seen.values(), key=lambda r: _sort_key(r["id"]))


def extract_flowcharts():
    """
    Parse `flowcharts.md` into one JSONL row per ``### x.y title`` heading.

    Schema:
      {
        "id":         "1.2",
        "batch":      "Batch 1 — Core shape & lock discipline",
        "batch_num":  1,
        "title":      "Core::up tier dispatch",
        "kind":       "flowchart" | "sequenceDiagram" | "stateDiagram-v2" | "classDiagram",
        "source":     "<raw mermaid block>",
        "prose":      "<paragraphs between the diagram and next heading>",
        "rules_cited": ["R1.4.1", ...],
        "node_count": <approx count, naive — for size hinting in the SPA>,
        "edge_count": <approx count>,
        "anchor":     "../../flowcharts.md#fc-1-2"
      }

    The Mermaid `source` is preserved verbatim so the SPA can hand it to the
    Mermaid library for rendering; structured node/edge extraction can land
    later without breaking the file.
    """
    if not FLOWCHARTS_PATH.exists():
        print(f"warn: {FLOWCHARTS_PATH} missing — flowcharts.jsonl will be empty", file=sys.stderr)
        return []

    text = FLOWCHARTS_PATH.read_text()
    lines = text.split("\n")
    out = []
    cur_batch = ""
    cur_batch_num = 0
    cur = None  # in-progress flowchart record
    in_mermaid = False
    mermaid_buf = []
    prose_buf = []

    h2_re = re.compile(r"^##\s+(.+?)\s*$")
    h3_re = re.compile(r"^###\s+(\d+\.\d+)\s+(.+?)\s*$")
    fence_open  = re.compile(r"^```(mermaid)\s*$")
    fence_close = re.compile(r"^```\s*$")

    def flush():
        nonlocal cur, mermaid_buf, prose_buf
        if cur is None:
            return
        src = "\n".join(mermaid_buf).strip()
        prose = "\n".join(prose_buf).strip()
        cur["source"] = src
        cur["prose"] = prose
        # Detect diagram kind from the first non-empty line of source
        first = next((l.strip() for l in src.split("\n") if l.strip()), "")
        kind = "flowchart"
        for k in ["sequenceDiagram", "stateDiagram-v2", "stateDiagram",
                  "classDiagram", "erDiagram", "flowchart", "graph"]:
            if first.startswith(k):
                kind = k
                break
        cur["kind"] = kind
        # Naive size estimate: count lines in source that look like node/edge defs.
        cur["node_count"] = sum(1 for l in src.split("\n") if re.match(r"^\s*[A-Za-z][\w-]*\s*[\[\(\{]", l))
        cur["edge_count"] = sum(1 for l in src.split("\n") if "-->" in l or "==>" in l or "-.->" in l)
        cur["rules_cited"] = sorted(set(RULE_ID_RE.findall(cur["title"] + " " + prose + " " + src)))
        out.append(cur)
        cur = None
        mermaid_buf = []
        prose_buf = []

    for i, raw in enumerate(lines):
        line = raw

        m_h2 = h2_re.match(line)
        if m_h2:
            flush()
            cur_batch = m_h2.group(1).strip()
            num_match = re.match(r"^Batch\s+(\d+)", cur_batch)
            cur_batch_num = int(num_match.group(1)) if num_match else 0
            continue

        m_h3 = h3_re.match(line)
        if m_h3:
            flush()
            fid, title = m_h3.group(1), m_h3.group(2).strip()
            cur = {
                "id": fid,
                "batch": cur_batch,
                "batch_num": cur_batch_num,
                "title": title,
                "anchor": f"../../flowcharts.md#fc-{fid.replace('.', '-')}",
            }
            continue

        if cur is None:
            continue

        if not in_mermaid and fence_open.match(line):
            in_mermaid = True
            continue
        if in_mermaid and fence_close.match(line):
            in_mermaid = False
            continue

        if in_mermaid:
            mermaid_buf.append(line)
        else:
            # Prose between mermaid fence-close and the next ### / ## /
            # is captured. Skip empty leading lines.
            if line.strip() or prose_buf:
                prose_buf.append(line)

    flush()
    return out


def main():
    items, tests, topology, locks = extract_workspace()
    rules = extract_rules()
    flowcharts = extract_flowcharts()
    write_jsonl(OUT_DIR / "items.jsonl", items)
    write_jsonl(OUT_DIR / "rules.jsonl", rules)
    write_jsonl(OUT_DIR / "tests.jsonl", tests)
    write_jsonl(OUT_DIR / "topology.jsonl", topology)
    write_jsonl(OUT_DIR / "locks.jsonl", locks)
    write_jsonl(OUT_DIR / "flowcharts.jsonl", flowcharts)

    # Heartbeat metadata for the SPA's top strip
    files = [i for i in items if i["kind"] == "file"]
    code_files = [i for i in files if i["role"] == "src"]
    test_files = [i for i in files if i["role"] == "tests"]
    crates = sorted({i["crate"] for i in files})
    rules_with_tests = {r for t in tests for r in t["covers_rules"]}
    meta = {
        "generated_at": _now_iso(),
        "repo_root": str(REPO_ROOT),
        "spec_path": str(SPEC_PATH),
        "totals": {
            "crates": len(crates),
            "src_files": len(code_files),
            "test_files": len(test_files),
            "src_loc": sum(f["loc"] for f in code_files),
            "test_loc": sum(f["loc"] for f in test_files),
            "items": sum(1 for i in items if i["kind"] != "file"),
            "items_unsafe": sum(1 for i in items if i["kind"] != "file" and i["unsafe"]),
            "tests": len(tests),
            "active_tests": sum(1 for t in tests if t["status"] == "active"),
            "ignored_tests": sum(1 for t in tests if t["status"] == "ignored"),
            "tests_with_rules": sum(1 for t in tests if t["covers_rules"]),
            "rules": len(rules),
            "rules_cited_in_src": len({r for f in code_files for r in f["rules_cited"]}),
            "rules_with_tests": len(rules_with_tests),
            "topology_edges": len(topology),
            "lock_acquisitions": sum(1 for l in locks if l["op"] in {"lock", "read", "write", "try_lock", "try_read", "try_write"}),
            "lock_constructions": sum(1 for l in locks if l["op"] == "new"),
            "flowcharts": len(flowcharts),
            "flowcharts_with_rules": sum(1 for f in flowcharts if f["rules_cited"]),
        },
        "crates": crates,
    }
    (OUT_DIR / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")

    print(f"Wrote {OUT_DIR/'items.jsonl'}: {len(items)} rows ({len(files)} file rows)")
    print(f"Wrote {OUT_DIR/'rules.jsonl'}: {len(rules)} rules")
    print(f"Wrote {OUT_DIR/'tests.jsonl'}: {len(tests)} tests ({sum(1 for t in tests if t['covers_rules'])} cite ≥1 rule)")
    print(f"Wrote {OUT_DIR/'topology.jsonl'}: {len(topology)} edges")
    print(f"Wrote {OUT_DIR/'locks.jsonl'}: {len(locks)} lock sites")
    print(f"Wrote {OUT_DIR/'flowcharts.jsonl'}: {len(flowcharts)} diagrams ({sum(1 for f in flowcharts if f['rules_cited'])} cite ≥1 rule)")
    print(f"Wrote {OUT_DIR/'meta.json'}")


def _now_iso():
    import datetime
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


if __name__ == "__main__":
    main()
