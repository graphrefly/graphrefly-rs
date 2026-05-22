//! `stratify_branch` — substrate operator for classifier-routing.
//!
//! Substrate counterpart of TS `extra/composition/stratify.ts` (D199,
//! Unit 5 Q9.2 of `SESSION-rust-port-layer-boundary.md`). The TS
//! `stratify(name, source, rules, opts) -> Graph` factory composes N
//! instances of this branch operator (one per rule) inside a single
//! Graph; the Graph wrapper itself stays binding-side (presentation
//! under D193). The classifier-routing logic — two-input subscribe,
//! reactive-rules cache, two-dep DIRTY gating, per-fire dispatch — is
//! the Rust substrate.
//!
//! # Semantics (mirrors TS `_addBranch` in stratify.ts)
//!
//! - Subscribes to BOTH `source` and `rules`. Rules first, then
//!   source — so push-on-subscribe of rules' cached DATA arrives
//!   before any source DATA the branch processes (R2.2.7).
//! - **Two-dep DIRTY gating** (TS parity). On either dep's DIRTY, the
//!   handler buffers and waits. When BOTH deps have settled in the
//!   same wave (no pending DIRTY on either), the cached source value
//!   is classified under the cached rules. This eliminates the
//!   stale-rules race when both deps update inside a single
//!   `core.batch()`.
//! - `rules` DATA: replaces the cached rules handle. Retains the new
//!   handle; releases the previously cached one. No downstream emit
//!   ("future items only" — rules updates affect FUTURE source items
//!   classified under them, not the current one).
//! - `source` DATA: buffered (with retain) until both deps settle.
//!   On resolve, invokes
//!   `binding.invoke_stratify_classifier_fn(classifier_fn_id,
//!   rules_handle, value_handle)`. If `true`, the buffered handle's
//!   retain transfers to the emit queue. If `false`, the handle is
//!   released. If no rules handle has arrived (sentinel state), the
//!   buffered source DATA is dropped — matches TS "rule not found →
//!   false".
//! - `source` COMPLETE / ERROR / TEARDOWN: forwarded unchanged. F1
//!   fix (QA 2026-05-14) — TEARDOWN forwarding restored to match TS
//!   `if (depIndex === 0) actions.down([msg])` for tier 5 + 6.
//! - `rules` COMPLETE / ERROR / TEARDOWN / INVALIDATE: silently
//!   absorbed. The branch keeps its last-seen rules cache and
//!   continues; rules' terminals don't propagate downstream of the
//!   branch (TS parity). F5 fix (QA 2026-05-14) — rules INVALIDATE
//!   intentionally does NOT clear `latest_rules`; matches TS, where
//!   rules' INVALIDATE message is silently absorbed and `latestRules`
//!   keeps its previous value.
//! - On producer deactivation: the Drop impl on `StratifyState`
//!   releases both the cached rules handle and any buffered source
//!   value via `Weak<dyn BindingBoundary>` (leaf-op safe).
//! - **N2 fix (QA 2026-05-14)** — under Phase H+ STRICT scheduling,
//!   `subscribe_to(rules, ...)` may return `Deferred`, in which case
//!   the post-subscribe sync push doesn't populate `latest_rules`
//!   before the source's first DATA arrives. The build closure
//!   defensively reads `core.cache_of(rules)` and pre-seeds the cache
//!   when the sync push didn't. Matches the TS factory-time
//!   `latestRules = rulesNode.cache` seeding.

#![allow(clippy::too_many_lines)]

use std::rc::Rc;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use smallvec::SmallVec;

use graphrefly_core::{BindingBoundary, Core, FnId, HandleId, NodeId, Sink, NO_HANDLE};

use crate::producer::{ProducerBinding, ProducerBuildFn, ProducerCtx, SubscribeOutcome};

// =====================================================================
// stratify_branch(source, rules, classifier_fn_id)
// =====================================================================

/// Internal state shared between source-sink, rules-sink, and the
/// `Drop` impl. Tracks the cached rules handle, the buffered source
/// value awaiting classification, and the two-dep DIRTY gating
/// counters.
///
/// `clippy::struct_excessive_bools` is allowed because the four
/// gating bools (`source_dirty`, `rules_dirty`, `source_phase2`,
/// `terminated`) each represent an independent lifecycle / per-wave
/// signal. A state-machine refactor would be more code without
/// changing semantics; keep the flat shape per TS parity.
#[allow(clippy::struct_excessive_bools)]
struct StratifyState {
    /// Latest rules handle, retain held while present. Replaced (with
    /// release-of-old / retain-of-new) on each rules DATA. None means
    /// either no rules emission has arrived yet OR rules emitted
    /// RESOLVED-only without prior DATA.
    latest_rules: Option<HandleId>,
    /// Buffered source DATA value awaiting classification, retain held
    /// while present. Set when source emits DATA and gating buffers
    /// it; cleared on resolve (transferring retain to emit) or on
    /// drop (releasing).
    source_value: Option<HandleId>,
    /// True after source emits DIRTY this wave; cleared on source
    /// DATA/RESOLVED. Gating predicate: resolve only fires when both
    /// `source_dirty` and `rules_dirty` are false.
    source_dirty: bool,
    /// True after rules emits DIRTY this wave; cleared on rules
    /// DATA/RESOLVED. Symmetric to `source_dirty`.
    rules_dirty: bool,
    /// True after source DATA/RESOLVED arrives but resolve hasn't
    /// fired yet (waiting for rules to settle). Reset by resolve.
    source_phase2: bool,
    /// Set on first source terminal (COMPLETE / ERROR / TEARDOWN).
    /// Subsequent sink callbacks short-circuit so duplicate terminals
    /// never leak retains.
    terminated: bool,
    /// Weak ref to the binding for the `Drop` impl. Held as `Weak`
    /// rather than `Arc` so the state's drop doesn't keep the binding
    /// alive past graph teardown.
    binding_weak: Weak<dyn BindingBoundary>,
}

impl Drop for StratifyState {
    fn drop(&mut self) {
        let bb = self.binding_weak.upgrade();
        let Some(bb) = bb else {
            return;
        };
        if let Some(h) = self.latest_rules.take() {
            bb.release_handle(h);
        }
        if let Some(h) = self.source_value.take() {
            bb.release_handle(h);
        }
    }
}

/// Try to resolve a buffered source value under the cached rules.
/// Returns:
/// - `Some(handle)` if the classifier matched — caller emits DATA;
///   the returned handle's retain has transferred to the caller.
/// - `None` if there was nothing to resolve OR the classifier dropped
///   the value (release already issued via the returned `released`
///   handle in the second tuple slot).
///
/// Caller must hold `state` lock-released for `bb.release_handle` /
/// `core.emit_or_defer` calls per leaf-op contract.
#[allow(clippy::option_option)]
fn try_resolve(
    s: &mut StratifyState,
    bb: &Arc<dyn BindingBoundary>,
    classifier_fn_id: FnId,
) -> ResolveOutcome {
    if s.source_dirty || s.rules_dirty || !s.source_phase2 {
        return ResolveOutcome::NotReady;
    }
    s.source_phase2 = false;
    let Some(value_h) = s.source_value.take() else {
        // Source emitted RESOLVED-only — nothing to classify.
        return ResolveOutcome::ResolvedNoValue;
    };
    let Some(rules_h) = s.latest_rules else {
        // No rules cache — drop.
        return ResolveOutcome::Drop(value_h);
    };
    // Call the classifier under the state lock — consistent with
    // valve's `predicate_each` call site. Classifier may re-enter
    // binding for handle deref, but MUST NOT re-enter Core.
    if bb.invoke_stratify_classifier_fn(classifier_fn_id, rules_h, value_h) {
        // Match — transfer retain to emit.
        ResolveOutcome::Emit(value_h)
    } else {
        // Miss — release.
        ResolveOutcome::Drop(value_h)
    }
}

enum ResolveOutcome {
    /// Both deps settled; classifier matched. Emit the handle (caller
    /// owns retain).
    Emit(HandleId),
    /// Both deps settled; classifier missed OR no rules cache. Drop
    /// the handle (caller releases retain).
    Drop(HandleId),
    /// Both deps settled; source emitted RESOLVED-only. Nothing to
    /// emit; nothing to release.
    ResolvedNoValue,
    /// Either dep still dirty OR no source DATA buffered yet. No
    /// action.
    NotReady,
}

/// Single classifier-routing branch. Each rule in a TS `stratify(...)
/// -> Graph` becomes one instance of this operator.
///
/// `classifier_fn_id` is a binding-registered closure of shape
/// `(rules_handle, value_handle) -> bool`. The binding-side closure
/// dereferences `rules_handle` to the latest rules array, looks up the
/// branch's rule by name (captured in the closure), and runs the
/// rule's `classify(value)` predicate. Returning `false` for "rule not
/// found" or "classifier threw" matches TS semantics.
///
/// # Returns
///
/// The produced node's `NodeId`. The node emits DATA only for values
/// whose classifier returned `true` after BOTH source and rules have
/// settled in the same wave.
#[must_use]
pub fn stratify_branch(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    rules: NodeId,
    classifier_fn_id: FnId,
) -> NodeId {
    let binding_weak_for_state: Weak<dyn BindingBoundary> =
        Arc::downgrade(binding) as Weak<dyn BindingBoundary>;

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        // S2b/D231: build-side `&Core` via ctx; sinks use `em`.
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let state: Arc<Mutex<StratifyState>> = Arc::new(Mutex::new(StratifyState {
            latest_rules: None,
            source_value: None,
            source_dirty: false,
            rules_dirty: false,
            source_phase2: false,
            terminated: false,
            binding_weak: binding_weak_for_state.clone(),
        }));

        // --- rules sink ---
        let st_rules = state.clone();
        let bb_rules: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_rules = em.clone();
        let rules_sink: Sink = Rc::new(move |msgs| {
            enum Act {
                ReleaseOldRules(HandleId),
                Emit(HandleId),
                Drop(HandleId),
            }
            let mut actions: SmallVec<[Act; 4]> = SmallVec::new();
            {
                let mut s = st_rules.lock();
                if s.terminated {
                    return;
                }
                for m in msgs {
                    if s.terminated {
                        break;
                    }
                    match m.tier() {
                        1 => {
                            // Rules DIRTY — gating signal.
                            s.rules_dirty = true;
                        }
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                // Rules DATA — replace cached rules.
                                bb_rules.retain_handle(h);
                                if let Some(old) = s.latest_rules.replace(h) {
                                    actions.push(Act::ReleaseOldRules(old));
                                }
                                s.rules_dirty = false;
                            } else {
                                // Rules RESOLVED-only: cache unchanged.
                                s.rules_dirty = false;
                            }
                            // Try resolve any buffered source value.
                            match try_resolve(&mut s, &bb_rules, classifier_fn_id) {
                                ResolveOutcome::Emit(h) => actions.push(Act::Emit(h)),
                                ResolveOutcome::Drop(h) => actions.push(Act::Drop(h)),
                                ResolveOutcome::ResolvedNoValue | ResolveOutcome::NotReady => {}
                            }
                        }
                        // Tier 4 (INVALIDATE), Tier 5 (COMPLETE/ERROR),
                        // Tier 6 (TEARDOWN) — silently absorbed (F5 doc).
                        // Branch keeps cached rules and continues; rules
                        // terminals do not propagate downstream.
                        _ => {}
                    }
                }
            }
            for a in actions {
                match a {
                    Act::ReleaseOldRules(h) | Act::Drop(h) => bb_rules.release_handle(h),
                    Act::Emit(h) => core_rules.emit_or_defer(pid, h),
                }
            }
        });

        // Subscribe to rules FIRST so push-on-subscribe of rules'
        // cached DATA arrives before any source DATA the branch
        // processes (R2.2.7).
        let rules_outcome = ctx.subscribe_to(rules, rules_sink);
        let _ = rules_outcome;

        // N2 — defensive pre-seed for the Deferred / Dead path where
        // the post-subscribe sync push didn't populate latest_rules
        // before source DATA could arrive. Matches TS factory-time
        // `latestRules = rulesNode.cache` seeding.
        let pre_seed = core_s.cache_of(rules);
        if pre_seed != NO_HANDLE {
            let already_set = state.lock().latest_rules.is_some();
            if !already_set {
                // Lock-released retain per leaf-op contract, then
                // re-check (covers the race where the rules sink
                // fires between drop and re-lock, though under the
                // build closure's partition lock this race is not
                // expected today).
                binding_s.retain_handle(pre_seed);
                let mut s = state.lock();
                if s.latest_rules.is_none() {
                    s.latest_rules = Some(pre_seed);
                } else {
                    // Sink fired between drop+re-lock. Release our
                    // pre-seed retain.
                    drop(s);
                    binding_s.release_handle(pre_seed);
                }
            }
        }

        // --- source sink ---
        let st_src = state.clone();
        let bb_src: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_src = em.clone();
        let source_sink: Sink = Rc::new(move |msgs| {
            enum Act {
                Emit(HandleId),
                Drop(HandleId),
                Complete,
                Error(HandleId),
                Teardown,
            }
            let mut actions: SmallVec<[Act; 4]> = SmallVec::new();
            {
                let mut s = st_src.lock();
                for m in msgs {
                    match m.tier() {
                        // Source DIRTY — gating signal. Skip if
                        // already terminated (post-terminal DIRTY
                        // would be spec-illegal but harmless to
                        // ignore). Match-guard form preferred over
                        // collapsed `1 if !s.terminated => ...` for
                        // readability alongside the other tiers'
                        // explicit `if s.terminated { continue; }`
                        // pattern.
                        #[allow(clippy::collapsible_match)]
                        1 => {
                            if !s.terminated {
                                s.source_dirty = true;
                            }
                        }
                        3 => {
                            if s.terminated {
                                continue;
                            }
                            if let Some(h) = m.payload_handle() {
                                // Source DATA — buffer with retain.
                                // If a previous source value was
                                // buffered (multi-emit batch), release
                                // it (use-latest semantic).
                                bb_src.retain_handle(h);
                                if let Some(prev) = s.source_value.replace(h) {
                                    actions.push(Act::Drop(prev));
                                }
                                s.source_dirty = false;
                                s.source_phase2 = true;
                            } else {
                                // Source RESOLVED-only — release any
                                // buffered value (this wave settled
                                // without DATA).
                                if let Some(prev) = s.source_value.take() {
                                    actions.push(Act::Drop(prev));
                                }
                                s.source_dirty = false;
                                s.source_phase2 = true;
                            }
                            // Try resolve under both-deps-settled
                            // gating.
                            match try_resolve(&mut s, &bb_src, classifier_fn_id) {
                                ResolveOutcome::Emit(h) => actions.push(Act::Emit(h)),
                                ResolveOutcome::Drop(h) => actions.push(Act::Drop(h)),
                                ResolveOutcome::ResolvedNoValue | ResolveOutcome::NotReady => {}
                            }
                        }
                        5 => {
                            if s.terminated {
                                continue;
                            }
                            // Source COMPLETE / ERROR — terminate +
                            // forward. Release any buffered source
                            // value first (we're terminating before
                            // resolving it).
                            if let Some(prev) = s.source_value.take() {
                                actions.push(Act::Drop(prev));
                            }
                            if let Some(h) = m.payload_handle() {
                                s.terminated = true;
                                bb_src.retain_handle(h);
                                actions.push(Act::Error(h));
                            } else {
                                s.terminated = true;
                                actions.push(Act::Complete);
                            }
                        }
                        6 => {
                            // F1 — Source TEARDOWN forward (TS
                            // parity). ALWAYS forwards, even after
                            // COMPLETE in the same wave: per spec
                            // R2.6.4 the framework auto-emits
                            // COMPLETE before TEARDOWN on a non-
                            // terminated state node, so suppressing
                            // TEARDOWN under a `terminated` check
                            // would silently swallow the lifecycle
                            // signal that downstream subscribers
                            // expect. Buffered source value (if any)
                            // is released defensively in case the
                            // tier 5 arm above didn't run (would be
                            // spec-illegal, but defensive).
                            if let Some(prev) = s.source_value.take() {
                                actions.push(Act::Drop(prev));
                            }
                            s.terminated = true;
                            actions.push(Act::Teardown);
                        }
                        _ => {}
                    }
                }
            }
            for a in actions {
                match a {
                    Act::Emit(h) => core_src.emit_or_defer(pid, h),
                    Act::Drop(h) => bb_src.release_handle(h),
                    Act::Complete => core_src.complete_or_defer(pid),
                    Act::Error(h) => core_src.error_or_defer(pid, h),
                    Act::Teardown => {
                        // D234: sink-side terminal forward via em.defer.
                        let _ = core_src.defer(move |c| c.teardown(pid));
                    }
                }
            }
        });

        let src_outcome = ctx.subscribe_to(source, source_sink);
        if matches!(src_outcome, SubscribeOutcome::Dead { .. }) {
            let mut s = state.lock();
            if !s.terminated {
                s.terminated = true;
                drop(s);
                core_s.complete_or_defer(pid);
            }
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("stratify_branch: register_producer failed")
}
