# GraphReFly Rust Port — Legacy review reports (historical archive)

The 6 `reports-*.md` files in this directory are **frozen historical artifacts** from the pre-2026-05-09 review workflow. They are preserved as flat-file historical citations: every entry in `docs/audit/data/findings.jsonl` cites one of them by filename in its `source` field, and the canonical decision log + migration-status entries reference them as the origin of several open findings.

**The legacy site renderer (`docs/review/site/` + `serve.sh`) was removed 2026-05-24** to eliminate the dual-dashboard confusion. The directory was moved to `~/src/graphrefly-rs/TRASH/review-site-removed-2026-05-24/` per the trash-instead-of-delete convention; see `TRASH-FILES.md` at the repo root.

## Where active review work lives now

All `/rust-review` runs append structured rows to `docs/audit/data/{reviews,findings,flowcharts}.jsonl`. The audit dashboard is a data-driven SPA that reads those JSONL files directly:

```bash
mise run audit-serve              # port 8769
# then open: http://localhost:8769/audit/site/
```

See the `/rust-review` skill at `~/src/graphrefly-ts/.claude/skills/rust-review/SKILL.md` for the workflow.

## Reading the legacy reports

The files in this directory are plain markdown — open them in any editor. They were originally chained together with chip-styled flowchart links (`[F7.2](#fc-7.2)`) that the removed renderer resolved into modal Mermaid diagrams. Those chips will appear as raw markdown link syntax now; the diagrams they pointed at live in `docs/flowcharts.md` under matching `## Batch N` / `### N.M` headings.

## Files

| File | What it covers |
|---|---|
| `reports-000-overview.md` | M0–M3 review-workflow framing |
| `reports-001-m1-and-m2.md` | M1 lifecycle + M2 Slice F base |
| `reports-002-m3-substrate.md` | M3 Slice A–C substrate |
| `reports-003-m3-operators.md` | M3 Slice C-3 operators + flow combinators |
| `reports-004-m3-combinators-and-higher-order.md` | M3 Slice D–E higher-order ops |
| `reports-005-m3-correctness-and-typed-errors.md` | M3 Slice E1/F/G/H correctness pass; **origin of findings F001–F008** |
