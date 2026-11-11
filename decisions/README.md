# Rust-local decisions

This ledger owns only `graphrefly-rs` package and implementation decisions. Its record identity is
`graphrefly-rs:<D#>`. Language-neutral product, protocol, cross-runtime and cross-project decisions
remain owned by `graphrefly`; protocol changes still use `/spec-amend` there.

`root-origin-history.jsonl` separately owns records originally authored as `graphrefly:<D#>` and
physically relocated here under D783. Their origin-qualified identity, body and provenance do not
change. New Rust decisions never enter that historical ledger.

`decisions.jsonl` holds package-local durable decisions; `execution.jsonl` holds package-local
execution approvals (`decision_kind: "execution-approval"`). Both share one `graphrefly-rs:<D#>`
sequence. Do not copy upstream decision text between ledgers.
