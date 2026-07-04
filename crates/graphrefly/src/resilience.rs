//! Passive resilience policies for environment adapters (D130).
//!
//! These types do not schedule work and do not own graph state. Adapter-native
//! retry/reconnect code can use them while surfacing attempts/status/errors as
//! graph-visible data.

use crate::ctx::DepTerminal;
use crate::graph::{Graph, GraphNodeOpts};
use crate::node::Node;
use crate::time::timeout;

/// Delay strategy for retry/reconnect attempts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum BackoffPolicy {
    #[default]
    /// `None` variant.
    None,
    /// `Constant` variant.
    Constant {
        /// `delay_ms` field for delay ms.
        delay_ms: u64,
    },
    /// `Linear` variant.
    Linear {
        /// `initial_ms` field for initial ms.
        initial_ms: u64,
        /// `step_ms` field for step ms.
        step_ms: u64,
        /// `max_ms` field for max ms.
        max_ms: Option<u64>,
    },
    /// `Exponential` variant.
    Exponential {
        /// `initial_ms` field for initial ms.
        initial_ms: u64,
        /// `factor` field for factor.
        factor: u32,
        /// `max_ms` field for max ms.
        max_ms: Option<u64>,
    },
    /// `Fibonacci` variant.
    Fibonacci {
        /// `unit_ms` field for unit ms.
        unit_ms: u64,
        /// `max_ms` field for max ms.
        max_ms: Option<u64>,
    },
}

impl BackoffPolicy {
    /// Updates or reads `delay_ms`.
    pub fn delay_ms(&self, attempt: u32) -> u64 {
        match self {
            BackoffPolicy::None => 0,
            BackoffPolicy::Constant { delay_ms } => *delay_ms,
            BackoffPolicy::Linear {
                initial_ms,
                step_ms,
                max_ms,
            } => cap(
                initial_ms.saturating_add(step_ms.saturating_mul(attempt.saturating_sub(1) as u64)),
                *max_ms,
            ),
            BackoffPolicy::Exponential {
                initial_ms,
                factor,
                max_ms,
            } => {
                let multiplier = factor.saturating_pow(attempt.saturating_sub(1));
                cap(initial_ms.saturating_mul(multiplier as u64), *max_ms)
            }
            BackoffPolicy::Fibonacci { unit_ms, max_ms } => {
                cap(unit_ms.saturating_mul(fibonacci(attempt) as u64), *max_ms)
            }
        }
    }
}

/// Passive retry policy shared by environment adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    /// `max_attempts` field for max attempts.
    pub max_attempts: u32,
    /// `backoff` field for backoff.
    pub backoff: BackoffPolicy,
}

impl RetryPolicy {
    /// Creates or computes `new`.
    pub fn new(max_attempts: u32, backoff: BackoffPolicy) -> Self {
        assert!(max_attempts > 0, "RetryPolicy: max_attempts must be > 0");
        Self {
            max_attempts,
            backoff,
        }
    }

    /// Updates or reads `should_retry`.
    pub fn should_retry(&self, failed_attempt: u32) -> bool {
        failed_attempt < self.max_attempts
    }

    /// Updates or reads `next_delay_ms`.
    pub fn next_delay_ms(&self, next_attempt: u32) -> Option<u64> {
        if next_attempt == 0 || next_attempt > self.max_attempts {
            return None;
        }
        Some(self.backoff.delay_ms(next_attempt))
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            backoff: BackoffPolicy::None,
        }
    }
}

/// Graph-visible retry/reconnect status payload shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryStatus {
    /// `attempt` field for attempt.
    pub attempt: u32,
    /// `max_attempts` field for max attempts.
    pub max_attempts: u32,
    /// `delay_ms` field for delay ms.
    pub delay_ms: Option<u64>,
    /// `state` field for state.
    pub state: RetryState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `RetryState` variants.
pub enum RetryState {
    /// `Idle` variant.
    Idle,
    /// `Running` variant.
    Running,
    /// `Waiting` variant.
    Waiting,
    /// `Succeeded` variant.
    Succeeded,
    /// `Failed` variant.
    Failed,
    /// `Exhausted` variant.
    Exhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `RetryEvent` variants.
pub enum RetryEvent {
    /// `Attempt` variant.
    Attempt {
        /// `attempt` field for attempt.
        attempt: u32,
    },
    /// `Retry` variant.
    Retry {
        /// `attempt` field for attempt.
        attempt: u32,
        /// `delay_ms` field for delay ms.
        delay_ms: u64,
        /// `error` field for error.
        error: String,
    },
    /// `Success` variant.
    Success {
        /// `attempt` field for attempt.
        attempt: u32,
    },
    /// `Failure` variant.
    Failure {
        /// `attempt` field for attempt.
        attempt: u32,
        /// `error` field for error.
        error: String,
    },
    /// `Exhausted` variant.
    Exhausted {
        /// `attempt` field for attempt.
        attempt: u32,
        /// `error` field for error.
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `BreakerStatus` data container.
pub struct BreakerStatus {
    /// `state` field for state.
    pub state: BreakerState,
    /// `failures` field for failures.
    pub failures: u32,
    /// `opened_at_ms` field for opened at ms.
    pub opened_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `BreakerState` variants.
pub enum BreakerState {
    /// `Closed` variant.
    Closed,
    /// `Open` variant.
    Open,
    /// `HalfOpen` variant.
    HalfOpen,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `RateLimitStatus` data container.
pub struct RateLimitStatus {
    /// `allowed` field for allowed.
    pub allowed: u64,
    /// `dropped` field for dropped.
    pub dropped: u64,
    /// `remaining` field for remaining.
    pub remaining: u32,
    /// `reset_at_ms` field for reset at ms.
    pub reset_at_ms: u64,
}

/// `RateLimitBundle` data container.
pub struct RateLimitBundle<T: 'static> {
    /// `allowed` field for allowed.
    pub allowed: Node<T>,
    /// `dropped` field for dropped.
    pub dropped: Node<T>,
    /// `status` field for status.
    pub status: Node<RateLimitStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `TimeoutStatus` variants.
pub enum TimeoutStatus {
    /// `Running` variant.
    Running,
    /// `Completed` variant.
    Completed,
    /// `Errored` variant.
    Errored,
}

/// `TimeoutBundle` data container.
pub struct TimeoutBundle<T: 'static> {
    /// `node` field for node.
    pub node: Node<T>,
    /// `status` field for status.
    pub status: Node<TimeoutStatus>,
    /// `errors` field for errors.
    pub errors: Node<String>,
}

#[derive(Clone)]
struct RateLimitEvent<T> {
    kind: RateLimitEventKind,
    value: T,
    status: RateLimitStatus,
}

#[derive(Clone, Copy)]
enum RateLimitEventKind {
    Allowed,
    Dropped,
}

#[derive(Clone)]
struct RateLimitState {
    count: u32,
    reset_at_ms: u64,
    allowed: u64,
    dropped: u64,
}

/// Creates or computes `retry_status_node`.
pub fn retry_status_node(
    graph: &Graph,
    events: &Node<RetryEvent>,
    policy: RetryPolicy,
    name: impl Into<String>,
) -> Node<RetryStatus> {
    let name = name.into();
    graph.node_opts::<RetryStatus, _>(
        vec![events.erased()],
        move |ctx| {
            let mut next = RetryStatus {
                attempt: 0,
                max_attempts: policy.max_attempts,
                delay_ms: None,
                state: RetryState::Idle,
            };
            for event in ctx.batch::<RetryEvent>(0) {
                next = match event.as_ref() {
                    RetryEvent::Attempt { attempt } => RetryStatus {
                        attempt: *attempt,
                        max_attempts: policy.max_attempts,
                        delay_ms: None,
                        state: RetryState::Running,
                    },
                    RetryEvent::Retry {
                        attempt, delay_ms, ..
                    } => RetryStatus {
                        attempt: *attempt,
                        max_attempts: policy.max_attempts,
                        delay_ms: Some(*delay_ms),
                        state: RetryState::Waiting,
                    },
                    RetryEvent::Success { attempt } => RetryStatus {
                        attempt: *attempt,
                        max_attempts: policy.max_attempts,
                        delay_ms: None,
                        state: RetryState::Succeeded,
                    },
                    RetryEvent::Failure { attempt, .. } => RetryStatus {
                        attempt: *attempt,
                        max_attempts: policy.max_attempts,
                        delay_ms: policy.next_delay_ms(attempt.saturating_add(1)),
                        state: if policy.should_retry(*attempt) {
                            RetryState::Failed
                        } else {
                            RetryState::Exhausted
                        },
                    },
                    RetryEvent::Exhausted { attempt, .. } => RetryStatus {
                        attempt: *attempt,
                        max_attempts: policy.max_attempts,
                        delay_ms: None,
                        state: RetryState::Exhausted,
                    },
                };
            }
            ctx.emit(next);
        },
        GraphNodeOpts::named(format!("{name}/status")),
    )
}

/// Creates or computes `breaker_status_node`.
pub fn breaker_status_node(
    graph: &Graph,
    events: &Node<RetryEvent>,
    failure_threshold: u32,
    now_ms: impl Fn() -> u64 + 'static,
    name: impl Into<String>,
) -> Node<BreakerStatus> {
    assert!(
        failure_threshold > 0,
        "breaker_status_node: failure_threshold must be > 0"
    );
    let name = name.into();
    graph.node_opts::<BreakerStatus, _>(
        vec![events.erased()],
        move |ctx| {
            let mut status = ctx.state_get::<BreakerStatus>().map_or(
                BreakerStatus {
                    state: BreakerState::Closed,
                    failures: 0,
                    opened_at_ms: None,
                },
                |value| (*value).clone(),
            );
            for event in ctx.batch::<RetryEvent>(0) {
                match event.as_ref() {
                    RetryEvent::Success { .. } => {
                        status = BreakerStatus {
                            state: BreakerState::Closed,
                            failures: 0,
                            opened_at_ms: None,
                        };
                    }
                    RetryEvent::Failure { .. } | RetryEvent::Exhausted { .. } => {
                        status.failures = status.failures.saturating_add(1);
                        if status.failures >= failure_threshold {
                            status.state = BreakerState::Open;
                            status.opened_at_ms = Some(now_ms());
                        }
                    }
                    RetryEvent::Attempt { .. } | RetryEvent::Retry { .. } => {}
                }
            }
            ctx.state_set(status.clone());
            ctx.emit(status);
        },
        GraphNodeOpts::named(format!("{name}/status")),
    )
}

/// Creates or computes `rate_limit_bundle`.
pub fn rate_limit_bundle<T>(
    graph: &Graph,
    source: &Node<T>,
    max: u32,
    window_ms: u64,
    now_ms: impl Fn() -> u64 + 'static,
    name: impl Into<String>,
) -> RateLimitBundle<T>
where
    T: Clone + 'static,
{
    assert!(max > 0, "rate_limit_bundle: max must be > 0");
    assert!(window_ms > 0, "rate_limit_bundle: window_ms must be > 0");
    let name = name.into();
    let events = graph.node_opts::<RateLimitEvent<T>, _>(
        vec![source.erased()],
        move |ctx| {
            let current = now_ms();
            let mut state = ctx.state_get::<RateLimitState>().map_or(
                RateLimitState {
                    count: 0,
                    reset_at_ms: current.saturating_add(window_ms),
                    allowed: 0,
                    dropped: 0,
                },
                |value| (*value).clone(),
            );
            if current >= state.reset_at_ms {
                state.count = 0;
                state.reset_at_ms = current.saturating_add(window_ms);
            }
            for value in ctx.batch::<T>(0) {
                let kind = if state.count < max {
                    state.count = state.count.saturating_add(1);
                    state.allowed = state.allowed.saturating_add(1);
                    RateLimitEventKind::Allowed
                } else {
                    state.dropped = state.dropped.saturating_add(1);
                    RateLimitEventKind::Dropped
                };
                let status = RateLimitStatus {
                    allowed: state.allowed,
                    dropped: state.dropped,
                    remaining: max.saturating_sub(state.count),
                    reset_at_ms: state.reset_at_ms,
                };
                ctx.emit(RateLimitEvent {
                    kind,
                    value: (*value).clone(),
                    status,
                });
            }
            ctx.state_set(state);
        },
        GraphNodeOpts::named(format!("{name}/events")),
    );
    let allowed = graph.node_opts::<T, _>(
        vec![events.erased()],
        move |ctx| {
            for event in ctx.batch::<RateLimitEvent<T>>(0) {
                if matches!(event.kind, RateLimitEventKind::Allowed) {
                    ctx.emit(event.value.clone());
                }
            }
        },
        GraphNodeOpts::named(format!("{name}/allowed")),
    );
    let dropped = graph.node_opts::<T, _>(
        vec![events.erased()],
        move |ctx| {
            for event in ctx.batch::<RateLimitEvent<T>>(0) {
                if matches!(event.kind, RateLimitEventKind::Dropped) {
                    ctx.emit(event.value.clone());
                }
            }
        },
        GraphNodeOpts::named(format!("{name}/dropped")),
    );
    let status = graph.node_opts::<RateLimitStatus, _>(
        vec![events.erased()],
        move |ctx| {
            for event in ctx.batch::<RateLimitEvent<T>>(0) {
                ctx.emit(event.status.clone());
            }
        },
        GraphNodeOpts::named(format!("{name}/status")),
    );
    RateLimitBundle {
        allowed,
        dropped,
        status,
    }
}

/// Creates or computes `timeout_bundle`.
pub fn timeout_bundle<T>(
    graph: &Graph,
    source: &Node<T>,
    ms: u64,
    name: impl Into<String>,
) -> TimeoutBundle<T>
where
    T: Clone + 'static,
{
    let name = name.into();
    let node = timeout(source, ms);
    let status = graph.node_opts::<TimeoutStatus, _>(
        vec![node.erased()],
        move |ctx| match ctx.terminal(0) {
            Some(DepTerminal::Complete) => ctx.emit(TimeoutStatus::Completed),
            Some(DepTerminal::Error(_)) => ctx.emit(TimeoutStatus::Errored),
            None => {
                if !ctx.batch::<T>(0).is_empty() {
                    ctx.emit(TimeoutStatus::Running);
                }
            }
        },
        timeout_projection_opts(format!("{name}/status")),
    );
    let errors = graph.node_opts::<String, _>(
        vec![node.erased()],
        move |ctx| {
            if let Some(DepTerminal::Error(error)) = ctx.terminal(0) {
                ctx.emit(error.to_string());
            }
        },
        timeout_projection_opts(format!("{name}/errors")),
    );
    TimeoutBundle {
        node,
        status,
        errors,
    }
}

fn timeout_projection_opts(name: String) -> GraphNodeOpts {
    GraphNodeOpts {
        name: Some(name),
        node: crate::node::NodeOpts {
            complete_when_deps_complete: false,
            error_when_deps_error: false,
            terminal_as_real_input: true,
            ..crate::node::NodeOpts::default()
        },
        ..GraphNodeOpts::default()
    }
}

fn cap(value: u64, max: Option<u64>) -> u64 {
    max.map_or(value, |max| value.min(max))
}

fn fibonacci(n: u32) -> u32 {
    match n {
        0 => 0,
        1 => 1,
        _ => {
            let mut prev = 0u32;
            let mut curr = 1u32;
            for _ in 1..n {
                let next = prev.saturating_add(curr);
                prev = curr;
                curr = next;
            }
            curr
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_policy_calculates_bounded_delays() {
        assert_eq!(BackoffPolicy::None.delay_ms(1), 0);
        assert_eq!(BackoffPolicy::Constant { delay_ms: 25 }.delay_ms(3), 25);
        assert_eq!(
            BackoffPolicy::Linear {
                initial_ms: 10,
                step_ms: 5,
                max_ms: Some(18),
            }
            .delay_ms(4),
            18
        );
        assert_eq!(
            BackoffPolicy::Exponential {
                initial_ms: 10,
                factor: 2,
                max_ms: Some(50),
            }
            .delay_ms(4),
            50
        );
        assert_eq!(
            BackoffPolicy::Fibonacci {
                unit_ms: 10,
                max_ms: None,
            }
            .delay_ms(6),
            80
        );
    }

    #[test]
    fn retry_policy_bounds_attempts() {
        let policy = RetryPolicy::new(
            3,
            BackoffPolicy::Linear {
                initial_ms: 10,
                step_ms: 10,
                max_ms: None,
            },
        );

        assert!(policy.should_retry(1));
        assert!(policy.should_retry(2));
        assert!(!policy.should_retry(3));
        assert_eq!(policy.next_delay_ms(1), Some(10));
        assert_eq!(policy.next_delay_ms(3), Some(30));
        assert_eq!(policy.next_delay_ms(4), None);
    }

    #[test]
    fn retry_and_breaker_status_nodes_project_event_facts() {
        let g = crate::graph::graph();
        let events = g.state_empty::<RetryEvent>();
        let policy = RetryPolicy::new(2, BackoffPolicy::Constant { delay_ms: 25 });
        let retry = retry_status_node(&g, &events, policy, "retry");
        let breaker = breaker_status_node(&g, &events, 1, || 100, "breaker");
        let _retry_sub = retry.subscribe(|_| {});
        let _breaker_sub = breaker.subscribe(|_| {});

        events.set(RetryEvent::Attempt { attempt: 1 });
        events.set(RetryEvent::Failure {
            attempt: 1,
            error: "nope".to_owned(),
        });

        assert_eq!(
            retry.cache(),
            Some(RetryStatus {
                attempt: 1,
                max_attempts: 2,
                delay_ms: Some(25),
                state: RetryState::Failed,
            })
        );
        assert_eq!(
            breaker.cache(),
            Some(BreakerStatus {
                state: BreakerState::Open,
                failures: 1,
                opened_at_ms: Some(100),
            })
        );
    }

    #[test]
    fn rate_limit_bundle_projects_allowed_dropped_and_status() {
        let g = crate::graph::graph();
        let source = g.state_empty::<i32>();
        let now = std::rc::Rc::new(std::cell::Cell::new(0_u64));
        let now_for_bundle = now.clone();
        let bundle = rate_limit_bundle(&g, &source, 2, 100, move || now_for_bundle.get(), "limit");
        let _allowed = bundle.allowed.subscribe(|_| {});
        let _dropped = bundle.dropped.subscribe(|_| {});
        let _status = bundle.status.subscribe(|_| {});

        source.set(1);
        source.set(2);
        source.set(3);
        now.set(101);
        source.set(4);

        assert_eq!(bundle.allowed.cache(), Some(4));
        assert_eq!(bundle.dropped.cache(), Some(3));
        assert_eq!(
            bundle.status.cache(),
            Some(RateLimitStatus {
                allowed: 3,
                dropped: 1,
                remaining: 1,
                reset_at_ms: 201,
            })
        );
    }
}
