# Rust agent entrypoint

Start with [README](README.md) and the [documentation map](docs/README.md).
Rust owns its engine/package and Python native foundation; Python owns its host
facade. [Root authority](../graphrefly/AGENTS.md) governs protocol/cross-project work.

- [Architecture](docs/architecture.md) locates crate ownership and package scope.
- [Development](docs/development.md) owns Rust contributor constraints, shell
  observation, commands and documentation maintenance.
- [Docs policy](docs/docs.jsonl) owns package-local documentation.
- [Local work](plan/work.jsonl) and [root work locator](../graphrefly/authority/work-ledgers.jsonl)
  locate delivery; an empty ledger is not an inferred completion.
- [Local decisions](decisions/decisions.jsonl), [root locator](../graphrefly/authority/ledgers.jsonl)
  and [admission contract](../graphrefly/authority/README.md) locate rationale/history.
- [Current protocol and drafts](../graphrefly/authority/README.md#protocol-revision-contract)
  stay distinct; semantic changes require root spec-amend before code.

Follow [personal governance](/Users/davidchenallio/.codex/skills/project-governance/SKILL.md)
for authority/execution classification. Use local [bmad-build](.agents/skills/bmad-build/SKILL.md)
and [bmad-qa](.agents/skills/bmad-qa/SKILL.md) within approved work, with the package
checks in development guidance. Human review uses
[bmad-checkpoint-preview](.agents/skills/bmad-checkpoint-preview/SKILL.md).
Ordinary delivery is build-handoff; explicit checkpoint/`--practice` is interactive.
Shared contract/design workflows remain [root-owned](../graphrefly/AGENTS.md#workflow-routing).
