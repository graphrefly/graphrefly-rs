# Rust documentation map

| Category | Question | Current entrypoint | Maintenance boundary |
|---|---|---|---|
| Rules | What applies now? | [Root revision contract](../../graphrefly/authority/README.md#protocol-revision-contract), [Rust contributor guidance](development.md), [docs policy](docs.jsonl) | Package guidance does not define a second wave protocol |
| Draft rules | What is inactive or proposed? | Root [rule revisions](../../graphrefly/spec/rules.jsonl), local owner proposals | No activation inferred from a crate status or test pass |
| Decisions | Why this choice? | [Local ledger](../decisions/decisions.jsonl), [root locator](../../graphrefly/authority/ledgers.jsonl) | Root-origin history is immutable relocation-only material |
| Work | What is being done? | [Local work](../plan/work.jsonl), [root work locator](../../graphrefly/authority/work-ledgers.jsonl) | Do not invent a local cursor or copy foreign work |
| Evidence | What was checked? | Owning work receipts, [crate conformance map](../crates/graphrefly/CLEAN-SLATE.md), [B142 checks](../../graphrefly/sessions/active/docs-reorganization-evidence/2026-11-05-rs.md) | Legacy pass claims are not fresh runtime evidence |
| Design / guides | How is it understood? | [Architecture](architecture.md), [development](development.md), [rustdoc](https://rs.graphrefly.dev/) | API bodies stay generated/package-owned |
| Archive | What did earlier work say? | Git history for already-retired port material | No old source or archive is recreated or deleted here |
| Reference | What informs further work? | Historical context in [architecture](architecture.md#historical-port-material), [shared guides](../../graphrefly/guide/guide.jsonl) | Historical port terminology is not current implementation authority |

[Docs policy](docs.jsonl) owns maintenance. Shared public website records live in
root [guide/](../../graphrefly/guide/guide.jsonl) and [website/](../../graphrefly/website/);
root [docs/README.md](../../graphrefly/docs/README.md) is their navigation map.
No archive/trash moves were needed. Existing local guides retain their ownership.
