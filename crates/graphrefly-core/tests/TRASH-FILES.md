
## D253 (S5, 2026-05-19) — SchedulingGroupId surface deleted
- `scheduling_groups.rs` — moved to TRASH/ — tests the deleted `SchedulingGroupId` newtype + `set_scheduling_group`/`partition_of`/`group_of` API surface + `NodeOpts.scheduling_group` field + `SetGroupError` (~25 tests). The §7 declared-group identity surface is removed until M6 re-introduces it with M6's actual scheduling needs in view.
- `group_sharding.rs` — moved to TRASH/ — tests the deleted cross-shard component migration / regrouping shape (~15 tests). The shard model was deleted by D246/S2c (single-owner ⇒ one shard always); D253 (S5) drops the residual identity-only surface.
