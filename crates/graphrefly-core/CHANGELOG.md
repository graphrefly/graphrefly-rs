# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.8](https://github.com/graphrefly/graphrefly-rs/compare/graphrefly-core-v0.0.7...graphrefly-core-v0.0.8) - 2026-05-26

### Added

- *(graph)* D285 — Graph::tag_factory + Graph::resource_profile substrate (R3.1.2 + R3.6.3)

### Fixed

- update batch binding
- *(D281)* Core::up(Invalidate) R1.4.2 plain-forward (Commit 3 of 3)
- clean up
- *(/qa)* D272-D274 cleanup — F1..F10 (build-break under graph-codec + stale doc residue)

### Other

- *(D278)* E-i+ii+iv doc-hygiene batch (Commit 1 of 3 — 4 keeps)
- bench
- *(AMEND-D)* D262/P4 inline comment + D267 scope narrowing (audit L5-001/L6-001)
- doc-hygiene cleanup (audit L4-001/L4-002/L1-001/L8-001/L8-002/L8-003)
- *(D274)* delete vestigial union-find + defer-shim surface
- *(D273)* family-2 Cat-3 Arc<Mutex<X>> → Rc<RefCell<X>> sweep
- *(D272)* family-1 sink Arc<dyn Fn> → Rc<dyn Fn> + drop 12 clippy-allow

## [0.0.7](https://github.com/graphrefly/graphrefly-rs/compare/graphrefly-core-v0.0.6...graphrefly-core-v0.0.7) - 2026-05-21

### Added

- *(D263/D264)* setDeps/addDep/removeDep napi+wrapper trio + terminal_as_real_input substrate flag
- *(D246)* S4 — actor-model test rebuilds + rule-8 coalescing + wave-scope doc reconciliation
- *(D246)* S3 — SerializationGroupId → SchedulingGroupId rename
- *(D246)* S2c — StateCell collapse + single-owner !Send Core (D247/D248/D249)
- *(D246)* boundary-1 β-simplification — Core-free Graph + OwnedCore + one facade
- B-2 Step 2b-ii — correct cross-shard routing infra (parallelism gate UNMET; finding banked)
- B-2 Step 2b-i — per-ShardKey shard map + lock_arc (behaviour-identical; floor preserved)
- B-2 Step 2a — hoist CoreShared to its own lock (combined-guard, behaviour-identical)

### Fixed

- napi binding
- fix switchMap
- s7
- s6
- s5
- s2b qa
- s2b again
- s2b
- s2a
- *(core)* R2.6.0 — Default-mode leaf source self-emit delivers immediately while self-paused
- Slice B-2 /qa — A(i)+B(i) fixes, exact-count tests, deferrals banked
- B-3 — delete vestigial per_subgraph_parallelism.rs (CI-red fixed); reassess §7-B/symbol defer
- B-2 Step 1 — CoreShared sub-struct extraction (behaviour-identical)
- B-1
- nextest and reduce per wave
- fix parallelism

### Other

- *(D246 S2c+S3+S4)* M1 cross-queue contract + M2 compact_every per-emission + F1-F12 hygiene
- *(core)* R2.6.0 QA B1 — Default-mode lock-arithmetic + cross-node-isolation coverage

## [0.0.6](https://github.com/graphrefly/graphrefly-rs/compare/graphrefly-core-v0.0.5...graphrefly-core-v0.0.6) - 2026-05-16

### Added

- *(native)* Option C — ergonomic async @graphrefly/native public surface (D206)
- clean up M1-M3
- clean up slice G

### Fixed

- fix ci
- fix batch
- *(qa)* native-auto-release is structurally loop-proof (tag-only)
- fix ci
- more porting
- clean up
- more operators
- clean up
- M4
- H+

## [0.0.5](https://github.com/graphrefly/graphrefly-rs/compare/graphrefly-core-v0.0.4...graphrefly-core-v0.0.5) - 2026-05-10

### Added

- rearchitecture

## [0.0.4](https://github.com/graphrefly/graphrefly-rs/compare/graphrefly-core-v0.0.3...graphrefly-core-v0.0.4) - 2026-05-10

### Added

- phase E F

### Fixed

- slice Q
- slice H i J K

### Other

- release v0.0.3

## [0.0.3](https://github.com/graphrefly/graphrefly-rs/compare/graphrefly-core-v0.0.2...graphrefly-core-v0.0.3) - 2026-05-09

### Fixed

- slice E+F

## [0.0.2](https://github.com/graphrefly/graphrefly-rs/compare/v0.0.1...v0.0.2) - 2026-05-09

### Fixed

- add semantic version control + slice X

### Other

- v0.0.2
