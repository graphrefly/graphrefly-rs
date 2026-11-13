# Node and module names

The current language-neutral contract is [R-naming](../../graphrefly/spec/rules.jsonl), introduced by graphrefly:D809. `::` crosses a mounted graph boundary; `/` joins module roles inside one graph. Local node IDs/names, group names and mount paths reject `::` and leading/trailing `:` before registration or restore recipes run. The exact errors are `name must not contain '::'` and `name must not start or end with ':'`; the first takes precedence. An internal single colon, such as `tenant:item`, remains a legal user label.

Fixed module segments use lower camelCase and a short role. Caller labels and payload keys are preserved. Storage keys (including the legacy agentic-memory persistence default), function names, recipe references, checkpoint format tags, and anonymous core `factory#N` IDs are unchanged. Generated names have no aliases or migration map. Built-in collection restore descriptors require the new role suffixes and reject the previous dotted collection IDs. Generic custom recipes can still restore legal caller-authored dotted IDs; no blanket old-name rejection or migration is added.

## Naming migration map

Here `name` is an explicit module scope or the listed default, and `N` is a decimal instance counter. Unlisted module roles are unchanged.

| Module | Previous generated ID/group | Current generated ID/group |
|---|---|---|
| Cascading cache | `name.ledger`, `.events`, `.status`, `.value` | `name/ledger`, `/events`, `/status`, `/value` |
| Wire edge receiver | `name/edgeKey` | `name/edges/edgeKey` |
| Wire edge receiver | `name.issues` | `name/issues` |
| Reactive list/index/map/log | `name.delta`, `.snapshotRequest`, `.snapshot` | `name/delta`, `/request`, `/snapshot` |
| Collection bindings | `name.bind#N` | `name/bindN` |
| Index range | `name.range#N` and its `.delta`, `.snapshotRequest`, `.snapshot` nodes | `name/rangeN` and `/delta`, `/request`, `/snapshot` |
| Map selection | `name.select#N` and its `.delta`, `.snapshotRequest`, `.snapshot` nodes | `name/selectN` and `/delta`, `/request`, `/snapshot` |
| Log page | `name.page#N` and its `.delta`, `.snapshotRequest`, `.snapshot` nodes | `name/pageN` and `/delta`, `/request`, `/snapshot` |
| Unnamed collections | `reactiveList@pointer`, `reactiveIndex@pointer`, `reactiveMap@pointer`, `reactiveLog@pointer` | `reactiveList`, `reactiveIndex`, `reactiveMap`, `reactiveLog`; later instances append a number |
| Log tail/slice/scan | anonymous `reactiveLog.tail#N`, `.slice#N`, `.scan#N` | `name/tail`, `/slice`, `/scan`; later instances append a number |
| Merge logs | anonymous `mergeReactiveLogs#N` | `mergedLogs`; later instances append a number |
| Agentic record persistence | default `agenticMemoryRecords.persistence` | `agenticMemoryRecords/persistence` |
| Collection persistence | default `reactiveCollection.persistence` | `reactiveList/persistence`, `reactiveIndex/persistence`, `reactiveMap/persistence`, `reactiveLog/persistence`, according to collection kind |
| Persistence facts | `name.ready`, `.status`, `.error`, `.cursor` | `name/ready`, `/status`, `/error`, `/cursor` |
| Process | `name/effect_request` | `name/effectRequests` |
| Process errors | `name/error` | `name/errors` |
| WebSocket session default | `websocketSession` | `webSocketSession` |
| Agentic memory KG projection aggregate | `name/snapshot` | `name` (the default already includes `Projection`) |
| Memory retention/consolidation/packing aggregate | `name/snapshot` | `name/projection` |
| Memory retention | `name/active_records`, `/archived_records`, `/consolidation_requests` | `name/activeRecords`, `/archivedRecords`, `/requests` |
| Memory consolidation | `name/proposed_record_drafts` | `name/drafts` |
| Memory context packing | default `agenticMemoryContextPacking`, child `name/packed_context` | default `agenticMemoryPacking`, child `name/context` |
| Message-bus topic/subscription defaults | `bus/topic/topic`, `bus/topic/subscription` | `bus/topics/topic`, `bus/subscriptions/topic/subscription`; repeated default scopes append a number |
| Stratify branches | `prefix/ruleName` | `prefix/branches/ruleName`; the rule-state remains `prefix/rules` |

Collection view group names follow their view scope once, for example `items/select0`. `statusLedger`, `responsesLedger`, `errorsLedger` and `cursorLedger` retain their role qualifiers where multiple ledgers coexist. `activeRecords` and `archivedRecords` remain distinct roles. Caller keys live below `/edges` or `/branches` where a key such as `issues` or `rules` would otherwise collide with a fixed role.

Default optional module scopes use the graph's collision-safe allocator, preserving the first existing default and adding a decimal suffix for later scopes. It checks registered/retired node namespaces and previously allocated scopes. Explicit names are never silently changed. This module allocator does not change anonymous core allocation.

## Shared checks and remaining structure differences

`GRAPHREFLY_SPEC_ROOT=../graphrefly/spec mise exec -- cargo run -p graphrefly-rs --example node_naming_probe` checks real node inventories and role identities against the pinned root fixture. `tests/naming.rs` scans default modules and dynamic collection views, tests name admission before mutation/recipes, and exercises reserved-key collisions.

Rust's cascading cache has a separate durable ledger node; TypeScript's inventory differs. Rust has no work-item or separate ack-driver module. Rust graph-registered collection helpers also differ from TypeScript's unregistered core helpers. Naming does not add dummy nodes or alter those ownership/lifecycle structures; structural convergence requires separate analysis.
