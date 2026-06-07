//! Passive resilience policies for environment adapters (D130).
//!
//! These types do not schedule work and do not own graph state. Adapter-native
//! retry/reconnect code can use them while surfacing attempts/status/errors as
//! graph-visible data.

/// Delay strategy for retry/reconnect attempts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum BackoffPolicy {
    #[default]
    None,
    Constant {
        delay_ms: u64,
    },
    Linear {
        initial_ms: u64,
        step_ms: u64,
        max_ms: Option<u64>,
    },
    Exponential {
        initial_ms: u64,
        factor: u32,
        max_ms: Option<u64>,
    },
    Fibonacci {
        unit_ms: u64,
        max_ms: Option<u64>,
    },
}

impl BackoffPolicy {
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
    pub max_attempts: u32,
    pub backoff: BackoffPolicy,
}

impl RetryPolicy {
    pub fn new(max_attempts: u32, backoff: BackoffPolicy) -> Self {
        assert!(max_attempts > 0, "RetryPolicy: max_attempts must be > 0");
        Self {
            max_attempts,
            backoff,
        }
    }

    pub fn should_retry(&self, failed_attempt: u32) -> bool {
        failed_attempt < self.max_attempts
    }

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
    pub attempt: u32,
    pub max_attempts: u32,
    pub delay_ms: Option<u64>,
    pub state: RetryState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryState {
    Idle,
    Running,
    Waiting,
    Succeeded,
    Failed,
    Exhausted,
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
}
