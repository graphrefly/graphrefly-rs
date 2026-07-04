//! MessageBus-backed generic work lifecycle infrastructure.
//!
//! D299-D324/D330 keep this layer above retained messaging and below recipes:
//! workQueue owns admission, claim/lease/retry/dead-letter records, status,
//! issues, and read projections. It intentionally stays independent of the
//! higher-layer recipe and orchestration types.

pub mod readiness;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use crate::ctx::Ctx;
use crate::graph::{Graph, GraphNodeOpts};
use crate::identity::{canonical_tuple_key, compound_tuple_key};
use crate::messaging::{
    attach_message_bus_deferred_command_sink, DataIssue, MessageBus, MessageBusAvailablePage,
    MessageBusAvailableParams, MessageBusCommand, MessageBusStatus, MessageBusStatusKind,
    MessageBusSubscriptionFrom, MessageBusSubscriptionOptions, MessageEnvelope,
};
use crate::node::{Core, Node, NodeOpts};
use crate::protocol::{LockId, Message, PullDemand};
use crate::resilience::{BackoffPolicy, RetryPolicy};

#[derive(Debug, Clone, PartialEq)]
/// `WorkQueueSubmit` data container.
pub struct WorkQueueSubmit<T> {
    /// `payload` field for payload.
    pub payload: T,
    /// `work_id` field for work id.
    pub work_id: Option<String>,
    /// `priority` field for priority.
    pub priority: Option<i64>,
    /// `tags` field for tags.
    pub tags: Vec<String>,
    /// `requirements` field for requirements.
    pub requirements: Vec<String>,
    /// `not_before_ms` field for not before ms.
    pub not_before_ms: Option<u64>,
    /// `deadline_ms` field for deadline ms.
    pub deadline_ms: Option<u64>,
}

impl<T> WorkQueueSubmit<T> {
    /// Creates or computes `new`.
    pub fn new(payload: T) -> Self {
        Self {
            payload,
            work_id: None,
            priority: None,
            tags: Vec::new(),
            requirements: Vec::new(),
            not_before_ms: None,
            deadline_ms: None,
        }
    }

    /// Updates or reads `with_work_id`.
    pub fn with_work_id(mut self, work_id: impl Into<String>) -> Self {
        self.work_id = Some(work_id.into());
        self
    }

    /// Updates or reads `with_priority`.
    pub fn with_priority(mut self, priority: i64) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Updates or reads `with_not_before_ms`.
    pub fn with_not_before_ms(mut self, not_before_ms: u64) -> Self {
        self.not_before_ms = Some(not_before_ms);
        self
    }
}

#[derive(Clone)]
/// `WorkQueueOptions` data container.
pub struct WorkQueueOptions<T> {
    /// `queue_id` field for queue id.
    pub queue_id: String,
    /// `bus` field for bus.
    pub bus: MessageBus<WorkQueueSubmit<T>>,
    /// `topic` field for topic.
    pub topic: String,
    /// `subscription_id` field for subscription id.
    pub subscription_id: String,
    /// `from` field for from.
    pub from: MessageBusSubscriptionFrom,
    /// `name` field for name.
    pub name: Option<String>,
    /// `now` field for now.
    pub now: Rc<dyn Fn() -> u64>,
    /// `lease_duration_ms` field for lease duration ms.
    pub lease_duration_ms: u64,
    /// `retry` field for retry.
    pub retry: RetryPolicy,
}

impl<T> WorkQueueOptions<T> {
    /// Creates or computes `new`.
    pub fn new(
        queue_id: impl Into<String>,
        bus: MessageBus<WorkQueueSubmit<T>>,
        topic: impl Into<String>,
        subscription_id: impl Into<String>,
    ) -> Self {
        Self {
            queue_id: queue_id.into(),
            bus,
            topic: topic.into(),
            subscription_id: subscription_id.into(),
            from: MessageBusSubscriptionFrom::Earliest,
            name: None,
            now: Rc::new(|| 0),
            lease_duration_ms: 30_000,
            retry: RetryPolicy::new(3, BackoffPolicy::None),
        }
    }

    /// Updates or reads `named`.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Updates or reads `with_now`.
    pub fn with_now(mut self, now: impl Fn() -> u64 + 'static) -> Self {
        self.now = Rc::new(now);
        self
    }

    /// Updates or reads `with_lease_duration_ms`.
    pub fn with_lease_duration_ms(mut self, lease_duration_ms: u64) -> Self {
        self.lease_duration_ms = lease_duration_ms;
        self
    }

    /// Updates or reads `with_retry`.
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
/// `WorkQueueCommand` variants.
pub enum WorkQueueCommand<T = ()> {
    /// `Submit` variant.
    Submit {
        /// `payload` field for payload.
        payload: T,
        /// `command_id` field for command id.
        command_id: String,
        /// `queue_id` field for queue id.
        queue_id: Option<String>,
        /// `idempotency_key` field for idempotency key.
        idempotency_key: Option<String>,
    },
    /// `Claim` variant.
    Claim {
        /// `command_id` field for command id.
        command_id: String,
        /// `queue_id` field for queue id.
        queue_id: Option<String>,
        /// `idempotency_key` field for idempotency key.
        idempotency_key: Option<String>,
        /// `worker_id` field for worker id.
        worker_id: String,
        /// `requested_work_ids` field for requested work ids.
        requested_work_ids: Vec<String>,
        /// `limit` field for limit.
        limit: Option<usize>,
        /// `lease_duration_ms` field for lease duration ms.
        lease_duration_ms: Option<u64>,
        /// `now_ms` field for now ms.
        now_ms: Option<u64>,
    },
    /// `RenewLease` variant.
    RenewLease {
        /// `command_id` field for command id.
        command_id: String,
        /// `queue_id` field for queue id.
        queue_id: Option<String>,
        /// `idempotency_key` field for idempotency key.
        idempotency_key: Option<String>,
        /// `work_id` field for work id.
        work_id: String,
        /// `lease_id` field for lease id.
        lease_id: String,
        /// `attempt` field for attempt.
        attempt: u32,
        /// `worker_id` field for worker id.
        worker_id: String,
        /// `lease_duration_ms` field for lease duration ms.
        lease_duration_ms: Option<u64>,
        /// `lease_expires_at_ms` field for lease expires at ms.
        lease_expires_at_ms: Option<u64>,
        /// `now_ms` field for now ms.
        now_ms: Option<u64>,
    },
    /// `Release` variant.
    Release {
        /// `command_id` field for command id.
        command_id: String,
        /// `queue_id` field for queue id.
        queue_id: Option<String>,
        /// `idempotency_key` field for idempotency key.
        idempotency_key: Option<String>,
        /// `work_id` field for work id.
        work_id: String,
        /// `lease_id` field for lease id.
        lease_id: String,
        /// `attempt` field for attempt.
        attempt: u32,
        /// `worker_id` field for worker id.
        worker_id: String,
        /// `reason` field for reason.
        reason: Option<String>,
        /// `now_ms` field for now ms.
        now_ms: Option<u64>,
    },
    /// `Complete` variant.
    Complete {
        /// `command_id` field for command id.
        command_id: String,
        /// `queue_id` field for queue id.
        queue_id: Option<String>,
        /// `idempotency_key` field for idempotency key.
        idempotency_key: Option<String>,
        /// `work_id` field for work id.
        work_id: String,
        /// `lease_id` field for lease id.
        lease_id: String,
        /// `attempt` field for attempt.
        attempt: u32,
        /// `worker_id` field for worker id.
        worker_id: String,
        /// `result` field for result.
        result: Option<String>,
        /// `now_ms` field for now ms.
        now_ms: Option<u64>,
    },
    /// `Fail` variant.
    Fail {
        /// `command_id` field for command id.
        command_id: String,
        /// `queue_id` field for queue id.
        queue_id: Option<String>,
        /// `idempotency_key` field for idempotency key.
        idempotency_key: Option<String>,
        /// `work_id` field for work id.
        work_id: String,
        /// `lease_id` field for lease id.
        lease_id: String,
        /// `attempt` field for attempt.
        attempt: u32,
        /// `worker_id` field for worker id.
        worker_id: String,
        /// `error` field for error.
        error: Option<String>,
        /// `retryable` field for retryable.
        retryable: Option<bool>,
        /// `now_ms` field for now ms.
        now_ms: Option<u64>,
    },
    /// `Cancel` variant.
    Cancel {
        /// `command_id` field for command id.
        command_id: String,
        /// `queue_id` field for queue id.
        queue_id: Option<String>,
        /// `idempotency_key` field for idempotency key.
        idempotency_key: Option<String>,
        /// `work_id` field for work id.
        work_id: String,
        /// `reason` field for reason.
        reason: Option<String>,
        /// `now_ms` field for now ms.
        now_ms: Option<u64>,
    },
    /// `Schedule` variant.
    Schedule {
        /// `command_id` field for command id.
        command_id: String,
        /// `queue_id` field for queue id.
        queue_id: Option<String>,
        /// `idempotency_key` field for idempotency key.
        idempotency_key: Option<String>,
        /// `work_id` field for work id.
        work_id: String,
        /// `schedule_id` field for schedule id.
        schedule_id: Option<String>,
        /// `not_before_ms` field for not before ms.
        not_before_ms: u64,
        /// `deadline_ms` field for deadline ms.
        deadline_ms: Option<u64>,
        /// `reason` field for reason.
        reason: Option<String>,
        /// `now_ms` field for now ms.
        now_ms: Option<u64>,
    },
    /// `ExpireLeases` variant.
    ExpireLeases {
        /// `command_id` field for command id.
        command_id: String,
        /// `queue_id` field for queue id.
        queue_id: Option<String>,
        /// `idempotency_key` field for idempotency key.
        idempotency_key: Option<String>,
        /// `work_ids` field for work ids.
        work_ids: Vec<String>,
        /// `limit` field for limit.
        limit: Option<usize>,
        /// `now_ms` field for now ms.
        now_ms: Option<u64>,
    },
}

impl<T> WorkQueueCommand<T> {
    fn command_id(&self) -> &str {
        match self {
            Self::Submit { command_id, .. }
            | Self::Claim { command_id, .. }
            | Self::RenewLease { command_id, .. }
            | Self::Release { command_id, .. }
            | Self::Complete { command_id, .. }
            | Self::Fail { command_id, .. }
            | Self::Cancel { command_id, .. }
            | Self::Schedule { command_id, .. }
            | Self::ExpireLeases { command_id, .. } => command_id,
        }
    }

    fn queue_id(&self) -> Option<&str> {
        match self {
            Self::Submit { queue_id, .. }
            | Self::Claim { queue_id, .. }
            | Self::RenewLease { queue_id, .. }
            | Self::Release { queue_id, .. }
            | Self::Complete { queue_id, .. }
            | Self::Fail { queue_id, .. }
            | Self::Cancel { queue_id, .. }
            | Self::Schedule { queue_id, .. }
            | Self::ExpireLeases { queue_id, .. } => queue_id.as_deref(),
        }
    }

    fn idempotency_key(&self) -> Option<&str> {
        match self {
            Self::Submit {
                idempotency_key, ..
            }
            | Self::Claim {
                idempotency_key, ..
            }
            | Self::RenewLease {
                idempotency_key, ..
            }
            | Self::Release {
                idempotency_key, ..
            }
            | Self::Complete {
                idempotency_key, ..
            }
            | Self::Fail {
                idempotency_key, ..
            }
            | Self::Cancel {
                idempotency_key, ..
            }
            | Self::Schedule {
                idempotency_key, ..
            }
            | Self::ExpireLeases {
                idempotency_key, ..
            } => idempotency_key.as_deref(),
        }
    }

    fn now_ms(&self) -> Option<u64> {
        match self {
            Self::Claim { now_ms, .. }
            | Self::RenewLease { now_ms, .. }
            | Self::Release { now_ms, .. }
            | Self::Complete { now_ms, .. }
            | Self::Fail { now_ms, .. }
            | Self::Cancel { now_ms, .. }
            | Self::Schedule { now_ms, .. }
            | Self::ExpireLeases { now_ms, .. } => *now_ms,
            Self::Submit { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WorkQueueMessageBusRef` data container.
pub struct WorkQueueMessageBusRef {
    /// `topic` field for topic.
    pub topic: String,
    /// `seq` field for seq.
    pub seq: u64,
    /// `subscription_id` field for subscription id.
    pub subscription_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `WorkQueueDerivedState` variants.
pub enum WorkQueueDerivedState {
    /// `Scheduled` variant.
    Scheduled,
    /// `Ready` variant.
    Ready,
    /// `Leased` variant.
    Leased,
    /// `RetryWait` variant.
    RetryWait,
    /// `Completed` variant.
    Completed,
    /// `Canceled` variant.
    Canceled,
    /// `DeadLettered` variant.
    DeadLettered,
}

#[derive(Debug, Clone, PartialEq)]
/// `WorkQueueRecord` variants.
pub enum WorkQueueRecord<T> {
    /// `WorkAdmitted` variant.
    WorkAdmitted {
        /// `record_seq` field for record seq.
        record_seq: u64,
        /// `queue_id` field for queue id.
        queue_id: String,
        /// `work_id` field for work id.
        work_id: String,
        /// `payload` field for payload.
        payload: T,
        /// `message_bus` field for message bus.
        message_bus: WorkQueueMessageBusRef,
        /// `priority` field for priority.
        priority: Option<i64>,
        /// `tags` field for tags.
        tags: Vec<String>,
        /// `requirements` field for requirements.
        requirements: Vec<String>,
        /// `not_before_ms` field for not before ms.
        not_before_ms: Option<u64>,
        /// `deadline_ms` field for deadline ms.
        deadline_ms: Option<u64>,
        /// `recorded_at_ms` field for recorded at ms.
        recorded_at_ms: u64,
    },
    /// `AdmissionDeduped` variant.
    AdmissionDeduped {
        /// `record_seq` field for record seq.
        record_seq: u64,
        /// `queue_id` field for queue id.
        queue_id: String,
        /// `work_id` field for work id.
        work_id: String,
        /// `message_bus` field for message bus.
        message_bus: WorkQueueMessageBusRef,
        /// `reason` field for reason.
        reason: String,
        /// `existing_work_id` field for existing work id.
        existing_work_id: String,
        /// `recorded_at_ms` field for recorded at ms.
        recorded_at_ms: u64,
    },
    /// `WorkScheduled` variant.
    WorkScheduled {
        /// `record_seq` field for record seq.
        record_seq: u64,
        /// `queue_id` field for queue id.
        queue_id: String,
        /// `work_id` field for work id.
        work_id: String,
        /// `command_id` field for command id.
        command_id: String,
        /// `schedule_id` field for schedule id.
        schedule_id: Option<String>,
        /// `not_before_ms` field for not before ms.
        not_before_ms: u64,
        /// `deadline_ms` field for deadline ms.
        deadline_ms: Option<u64>,
        /// `reason` field for reason.
        reason: Option<String>,
        /// `recorded_at_ms` field for recorded at ms.
        recorded_at_ms: u64,
    },
    /// `WorkClaimed` variant.
    WorkClaimed {
        /// `record_seq` field for record seq.
        record_seq: u64,
        /// `queue_id` field for queue id.
        queue_id: String,
        /// `work_id` field for work id.
        work_id: String,
        /// `command_id` field for command id.
        command_id: String,
        /// `lease_id` field for lease id.
        lease_id: String,
        /// `attempt` field for attempt.
        attempt: u32,
        /// `worker_id` field for worker id.
        worker_id: String,
        /// `claimed_at_ms` field for claimed at ms.
        claimed_at_ms: u64,
        /// `lease_expires_at_ms` field for lease expires at ms.
        lease_expires_at_ms: u64,
    },
    /// `LeaseRenewed` variant.
    LeaseRenewed {
        /// `record_seq` field for record seq.
        record_seq: u64,
        /// `queue_id` field for queue id.
        queue_id: String,
        /// `work_id` field for work id.
        work_id: String,
        /// `command_id` field for command id.
        command_id: String,
        /// `lease_id` field for lease id.
        lease_id: String,
        /// `attempt` field for attempt.
        attempt: u32,
        /// `worker_id` field for worker id.
        worker_id: String,
        /// `previous_lease_expires_at_ms` field for previous lease expires at ms.
        previous_lease_expires_at_ms: u64,
        /// `lease_expires_at_ms` field for lease expires at ms.
        lease_expires_at_ms: u64,
        /// `renewed_at_ms` field for renewed at ms.
        renewed_at_ms: u64,
    },
    /// `WorkReleased` variant.
    WorkReleased {
        /// `record_seq` field for record seq.
        record_seq: u64,
        /// `queue_id` field for queue id.
        queue_id: String,
        /// `work_id` field for work id.
        work_id: String,
        /// `command_id` field for command id.
        command_id: String,
        /// `lease_id` field for lease id.
        lease_id: String,
        /// `attempt` field for attempt.
        attempt: u32,
        /// `worker_id` field for worker id.
        worker_id: String,
        /// `released_at_ms` field for released at ms.
        released_at_ms: u64,
        /// `reason` field for reason.
        reason: Option<String>,
    },
    /// `LeaseExpired` variant.
    LeaseExpired {
        /// `record_seq` field for record seq.
        record_seq: u64,
        /// `queue_id` field for queue id.
        queue_id: String,
        /// `work_id` field for work id.
        work_id: String,
        /// `command_id` field for command id.
        command_id: Option<String>,
        /// `lease_id` field for lease id.
        lease_id: String,
        /// `attempt` field for attempt.
        attempt: u32,
        /// `worker_id` field for worker id.
        worker_id: String,
        /// `lease_expires_at_ms` field for lease expires at ms.
        lease_expires_at_ms: u64,
        /// `expired_at_ms` field for expired at ms.
        expired_at_ms: u64,
    },
    /// `AttemptCompleted` variant.
    AttemptCompleted {
        /// `record_seq` field for record seq.
        record_seq: u64,
        /// `queue_id` field for queue id.
        queue_id: String,
        /// `work_id` field for work id.
        work_id: String,
        /// `command_id` field for command id.
        command_id: String,
        /// `lease_id` field for lease id.
        lease_id: String,
        /// `attempt` field for attempt.
        attempt: u32,
        /// `worker_id` field for worker id.
        worker_id: String,
        /// `result` field for result.
        result: Option<String>,
        /// `recorded_at_ms` field for recorded at ms.
        recorded_at_ms: u64,
    },
    /// `WorkCompleted` variant.
    WorkCompleted {
        /// `record_seq` field for record seq.
        record_seq: u64,
        /// `queue_id` field for queue id.
        queue_id: String,
        /// `work_id` field for work id.
        work_id: String,
        /// `command_id` field for command id.
        command_id: String,
        /// `lease_id` field for lease id.
        lease_id: String,
        /// `attempt` field for attempt.
        attempt: u32,
        /// `worker_id` field for worker id.
        worker_id: String,
        /// `result` field for result.
        result: Option<String>,
        /// `recorded_at_ms` field for recorded at ms.
        recorded_at_ms: u64,
    },
    /// `AttemptFailed` variant.
    AttemptFailed {
        /// `record_seq` field for record seq.
        record_seq: u64,
        /// `queue_id` field for queue id.
        queue_id: String,
        /// `work_id` field for work id.
        work_id: String,
        /// `command_id` field for command id.
        command_id: String,
        /// `lease_id` field for lease id.
        lease_id: String,
        /// `attempt` field for attempt.
        attempt: u32,
        /// `worker_id` field for worker id.
        worker_id: String,
        /// `error` field for error.
        error: Option<String>,
        /// `retryable` field for retryable.
        retryable: Option<bool>,
        /// `recorded_at_ms` field for recorded at ms.
        recorded_at_ms: u64,
    },
    /// `RetryScheduled` variant.
    RetryScheduled {
        /// `record_seq` field for record seq.
        record_seq: u64,
        /// `queue_id` field for queue id.
        queue_id: String,
        /// `work_id` field for work id.
        work_id: String,
        /// `command_id` field for command id.
        command_id: String,
        /// `retry_at_ms` field for retry at ms.
        retry_at_ms: u64,
        /// `delay_ms` field for delay ms.
        delay_ms: u64,
        /// `reason` field for reason.
        reason: Option<String>,
        /// `recorded_at_ms` field for recorded at ms.
        recorded_at_ms: u64,
    },
    /// `WorkDeadLettered` variant.
    WorkDeadLettered {
        /// `record_seq` field for record seq.
        record_seq: u64,
        /// `queue_id` field for queue id.
        queue_id: String,
        /// `work_id` field for work id.
        work_id: String,
        /// `command_id` field for command id.
        command_id: String,
        /// `reason` field for reason.
        reason: String,
        /// `exhausted_attempts` field for exhausted attempts.
        exhausted_attempts: Option<u32>,
        /// `recorded_at_ms` field for recorded at ms.
        recorded_at_ms: u64,
    },
    /// `WorkCanceled` variant.
    WorkCanceled {
        /// `record_seq` field for record seq.
        record_seq: u64,
        /// `queue_id` field for queue id.
        queue_id: String,
        /// `work_id` field for work id.
        work_id: String,
        /// `command_id` field for command id.
        command_id: String,
        /// `reason` field for reason.
        reason: Option<String>,
        /// `canceled_at_ms` field for canceled at ms.
        canceled_at_ms: u64,
        /// `canceled_lease_id` field for canceled lease id.
        canceled_lease_id: Option<String>,
        /// `attempt` field for attempt.
        attempt: Option<u32>,
    },
}

impl<T> WorkQueueRecord<T> {
    /// Updates or reads `record_seq`.
    pub fn record_seq(&self) -> u64 {
        match self {
            Self::WorkAdmitted { record_seq, .. }
            | Self::AdmissionDeduped { record_seq, .. }
            | Self::WorkScheduled { record_seq, .. }
            | Self::WorkClaimed { record_seq, .. }
            | Self::LeaseRenewed { record_seq, .. }
            | Self::WorkReleased { record_seq, .. }
            | Self::LeaseExpired { record_seq, .. }
            | Self::AttemptCompleted { record_seq, .. }
            | Self::WorkCompleted { record_seq, .. }
            | Self::AttemptFailed { record_seq, .. }
            | Self::RetryScheduled { record_seq, .. }
            | Self::WorkDeadLettered { record_seq, .. }
            | Self::WorkCanceled { record_seq, .. } => *record_seq,
        }
    }

    /// Updates or reads `work_id`.
    pub fn work_id(&self) -> &str {
        match self {
            Self::WorkAdmitted { work_id, .. }
            | Self::AdmissionDeduped { work_id, .. }
            | Self::WorkScheduled { work_id, .. }
            | Self::WorkClaimed { work_id, .. }
            | Self::LeaseRenewed { work_id, .. }
            | Self::WorkReleased { work_id, .. }
            | Self::LeaseExpired { work_id, .. }
            | Self::AttemptCompleted { work_id, .. }
            | Self::WorkCompleted { work_id, .. }
            | Self::AttemptFailed { work_id, .. }
            | Self::RetryScheduled { work_id, .. }
            | Self::WorkDeadLettered { work_id, .. }
            | Self::WorkCanceled { work_id, .. } => work_id,
        }
    }

    fn admission_message_bus(&self) -> Option<&WorkQueueMessageBusRef> {
        match self {
            Self::WorkAdmitted { message_bus, .. } | Self::AdmissionDeduped { message_bus, .. } => {
                Some(message_bus)
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `WorkQueueStatusKind` variants.
pub enum WorkQueueStatusKind {
    /// `CommandAccepted` variant.
    CommandAccepted,
    /// `CommandRejected` variant.
    CommandRejected,
    /// `AdmissionAccepted` variant.
    AdmissionAccepted,
    /// `AdmissionRejected` variant.
    AdmissionRejected,
    /// `ProjectionReady` variant.
    ProjectionReady,
    /// `ProjectionPartial` variant.
    ProjectionPartial,
    /// `MaintenanceApplied` variant.
    MaintenanceApplied,
    /// `MaintenanceNoop` variant.
    MaintenanceNoop,
    /// `PolicyWarning` variant.
    PolicyWarning,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WorkQueueStatus` data container.
pub struct WorkQueueStatus {
    /// `kind` field for kind.
    pub kind: WorkQueueStatusKind,
    /// `queue_id` field for queue id.
    pub queue_id: String,
    /// `work_id` field for work id.
    pub work_id: Option<String>,
    /// `command_id` field for command id.
    pub command_id: Option<String>,
    /// `record_seq` field for record seq.
    pub record_seq: Option<u64>,
    /// `as_of_record_seq` field for as of record seq.
    pub as_of_record_seq: Option<u64>,
    /// `issue_code` field for issue code.
    pub issue_code: Option<String>,
    /// `timestamp_ms` field for timestamp ms.
    pub timestamp_ms: u64,
    /// `details` field for details.
    pub details: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
/// `WorkQueueAvailableItem` data container.
pub struct WorkQueueAvailableItem<T> {
    /// `work_id` field for work id.
    pub work_id: String,
    /// `state` field for state.
    pub state: WorkQueueDerivedState,
    /// `payload` field for payload.
    pub payload: T,
    /// `admission_seq` field for admission seq.
    pub admission_seq: u64,
    /// `priority` field for priority.
    pub priority: Option<i64>,
    /// `tags` field for tags.
    pub tags: Vec<String>,
    /// `requirements` field for requirements.
    pub requirements: Vec<String>,
    /// `not_before_ms` field for not before ms.
    pub not_before_ms: Option<u64>,
    /// `retry_at_ms` field for retry at ms.
    pub retry_at_ms: Option<u64>,
    /// `deadline_ms` field for deadline ms.
    pub deadline_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
/// `WorkQueueAvailablePage` data container.
pub struct WorkQueueAvailablePage<T> {
    /// `items` field for items.
    pub items: Vec<WorkQueueAvailableItem<T>>,
    /// `next_after_work_id` field for next after work id.
    pub next_after_work_id: Option<String>,
    /// `next_after_admission_seq` field for next after admission seq.
    pub next_after_admission_seq: Option<u64>,
    /// `has_more` field for has more.
    pub has_more: bool,
    /// `as_of_record_seq` field for as of record seq.
    pub as_of_record_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WorkQueueActiveLease` data container.
pub struct WorkQueueActiveLease {
    /// `lease_id` field for lease id.
    pub lease_id: String,
    /// `attempt` field for attempt.
    pub attempt: u32,
    /// `worker_id` field for worker id.
    pub worker_id: String,
    /// `lease_expires_at_ms` field for lease expires at ms.
    pub lease_expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
/// `WorkQueueWorkSnapshot` data container.
pub struct WorkQueueWorkSnapshot<T> {
    /// `work_id` field for work id.
    pub work_id: String,
    /// `state` field for state.
    pub state: Option<WorkQueueDerivedState>,
    /// `payload` field for payload.
    pub payload: Option<T>,
    /// `active_lease` field for active lease.
    pub active_lease: Option<WorkQueueActiveLease>,
    /// `records` field for records.
    pub records: Vec<WorkQueueRecord<T>>,
    /// `as_of_record_seq` field for as of record seq.
    pub as_of_record_seq: u64,
}

#[derive(Debug, Clone, PartialEq)]
/// `WorkQueueDeadLetterPage` data container.
pub struct WorkQueueDeadLetterPage<T> {
    /// `entries` field for entries.
    pub entries: Vec<WorkQueueRecord<T>>,
    /// `next_after_dead_letter_seq` field for next after dead letter seq.
    pub next_after_dead_letter_seq: Option<u64>,
    /// `has_more` field for has more.
    pub has_more: bool,
    /// `as_of_record_seq` field for as of record seq.
    pub as_of_record_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// `WorkQueueAvailableParams` data container.
pub struct WorkQueueAvailableParams {
    /// `limit` field for limit.
    pub limit: Option<usize>,
    /// `after_work_id` field for after work id.
    pub after_work_id: Option<String>,
    /// `after_admission_seq` field for after admission seq.
    pub after_admission_seq: Option<u64>,
    /// `now_ms` field for now ms.
    pub now_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// `WorkQueueDeadLetterParams` data container.
pub struct WorkQueueDeadLetterParams {
    /// `limit` field for limit.
    pub limit: Option<usize>,
    /// `after_dead_letter_seq` field for after dead letter seq.
    pub after_dead_letter_seq: Option<u64>,
    /// `after_work_id` field for after work id.
    pub after_work_id: Option<String>,
}

#[derive(Clone)]
/// `WorkQueueProjection` data container.
pub struct WorkQueueProjection<TPage> {
    /// `snapshot` field for snapshot.
    pub snapshot: Node<TPage>,
    /// `snapshot_pull_id` field for snapshot pull id.
    pub snapshot_pull_id: LockId,
    /// `status` field for status.
    pub status: Node<WorkQueueStatus>,
    /// `issues` field for issues.
    pub issues: Node<DataIssue>,
}

#[derive(Clone)]
/// `WorkQueueAvailableProjection` data container.
pub struct WorkQueueAvailableProjection<T> {
    /// `available` field for available.
    pub available: Node<WorkQueueAvailablePage<T>>,
    /// `available_pull_id` field for available pull id.
    pub available_pull_id: LockId,
    /// `status` field for status.
    pub status: Node<WorkQueueStatus>,
    /// `issues` field for issues.
    pub issues: Node<DataIssue>,
}

#[derive(Clone)]
/// `WorkQueue` data container.
pub struct WorkQueue<T> {
    name: Rc<String>,
    queue_id: Rc<String>,
    topic: Rc<String>,
    bus: MessageBus<WorkQueueSubmit<T>>,
    command_seq: Rc<Cell<u64>>,
    /// `commands` field for commands.
    pub commands: Node<WorkQueueCommand<T>>,
    /// `records` field for records.
    pub records: Node<WorkQueueRecord<T>>,
    /// `status` field for status.
    pub status: Node<WorkQueueStatus>,
    /// `issues` field for issues.
    pub issues: Node<DataIssue>,
    state: Rc<RefCell<RuntimeState<T>>>,
    graph: Graph,
    _admission_ack_commands: Node<MessageBusCommand<WorkQueueSubmit<T>>>,
    _retain: Rc<WorkQueueRetain>,
}

struct WorkQueueRetain {
    releases: RefCell<Vec<Box<dyn FnOnce()>>>,
}

impl WorkQueueRetain {
    fn new(releases: Vec<Box<dyn FnOnce()>>) -> Self {
        Self {
            releases: RefCell::new(releases),
        }
    }
}

impl Drop for WorkQueueRetain {
    fn drop(&mut self) {
        for release in self.releases.borrow_mut().drain(..) {
            release();
        }
    }
}

#[derive(Clone)]
enum QueueEvent<T> {
    Record(WorkQueueRecord<T>),
    Status(WorkQueueStatus),
    Issue(DataIssue),
}

#[derive(Clone)]
struct WorkState<T> {
    work_id: String,
    payload: T,
    state: WorkQueueDerivedState,
    admission_seq: u64,
    priority: Option<i64>,
    tags: Vec<String>,
    requirements: Vec<String>,
    not_before_ms: Option<u64>,
    retry_at_ms: Option<u64>,
    deadline_ms: Option<u64>,
    attempt: u32,
    lease_id: Option<String>,
    worker_id: Option<String>,
    lease_expires_at_ms: Option<u64>,
}

struct RuntimeState<T> {
    record_seq: u64,
    lease_seq: u64,
    works: BTreeMap<String, WorkState<T>>,
    source_seqs: HashSet<String>,
    command_ids: HashSet<String>,
    idempotency_keys: HashSet<String>,
    records: Vec<WorkQueueRecord<T>>,
    dead_letters: Vec<WorkQueueRecord<T>>,
}

impl<T> Default for RuntimeState<T> {
    fn default() -> Self {
        Self {
            record_seq: 0,
            lease_seq: 0,
            works: BTreeMap::new(),
            source_seqs: HashSet::new(),
            command_ids: HashSet::new(),
            idempotency_keys: HashSet::new(),
            records: Vec::new(),
            dead_letters: Vec::new(),
        }
    }
}

impl<T: Clone + 'static> WorkQueue<T> {
    /// Updates or reads `submit`.
    pub fn submit(
        &self,
        payload: T,
        opts: WorkQueueSubmitOptions,
    ) -> MessageBusCommand<WorkQueueSubmit<T>> {
        let command_id = opts
            .command_id
            .unwrap_or_else(|| self.next_command_id("submit"));
        self.bus.publish(
            (*self.topic).clone(),
            WorkQueueSubmit {
                payload,
                work_id: opts.work_id,
                priority: opts.priority,
                tags: opts.tags,
                requirements: opts.requirements,
                not_before_ms: opts.not_before_ms,
                deadline_ms: opts.deadline_ms,
            },
            None,
            Some(command_id),
            opts.idempotency_key,
        )
    }

    /// Updates or reads `claim`.
    pub fn claim(&self, opts: WorkQueueClaimOptions) -> WorkQueueCommand<T> {
        self.publish_command(WorkQueueCommand::Claim {
            command_id: opts
                .command_id
                .unwrap_or_else(|| self.next_command_id("claim")),
            queue_id: opts.queue_id,
            idempotency_key: opts.idempotency_key,
            worker_id: opts.worker_id,
            requested_work_ids: opts.requested_work_ids,
            limit: opts.limit,
            lease_duration_ms: opts.lease_duration_ms,
            now_ms: opts.now_ms,
        })
    }

    /// Updates or reads `renew_lease`.
    pub fn renew_lease(
        &self,
        work_id: impl Into<String>,
        lease_id: impl Into<String>,
        attempt: u32,
        worker_id: impl Into<String>,
        command_id: impl Into<String>,
    ) -> WorkQueueCommand<T> {
        self.publish_command(WorkQueueCommand::RenewLease {
            command_id: command_id.into(),
            queue_id: None,
            idempotency_key: None,
            work_id: work_id.into(),
            lease_id: lease_id.into(),
            attempt,
            worker_id: worker_id.into(),
            lease_duration_ms: None,
            lease_expires_at_ms: None,
            now_ms: None,
        })
    }

    /// Updates or reads `release`.
    pub fn release(
        &self,
        work_id: impl Into<String>,
        lease_id: impl Into<String>,
        attempt: u32,
        worker_id: impl Into<String>,
        command_id: impl Into<String>,
    ) -> WorkQueueCommand<T> {
        self.publish_command(WorkQueueCommand::Release {
            command_id: command_id.into(),
            queue_id: None,
            idempotency_key: None,
            work_id: work_id.into(),
            lease_id: lease_id.into(),
            attempt,
            worker_id: worker_id.into(),
            reason: None,
            now_ms: None,
        })
    }

    /// Updates or reads `complete`.
    pub fn complete(
        &self,
        work_id: impl Into<String>,
        lease_id: impl Into<String>,
        attempt: u32,
        worker_id: impl Into<String>,
        command_id: impl Into<String>,
        result: Option<String>,
    ) -> WorkQueueCommand<T> {
        self.publish_command(WorkQueueCommand::Complete {
            command_id: command_id.into(),
            queue_id: None,
            idempotency_key: None,
            work_id: work_id.into(),
            lease_id: lease_id.into(),
            attempt,
            worker_id: worker_id.into(),
            result,
            now_ms: None,
        })
    }

    /// Updates or reads `fail`.
    pub fn fail(
        &self,
        work_id: impl Into<String>,
        lease_id: impl Into<String>,
        attempt: u32,
        worker_id: impl Into<String>,
        command_id: impl Into<String>,
        retryable: Option<bool>,
    ) -> WorkQueueCommand<T> {
        self.publish_command(WorkQueueCommand::Fail {
            command_id: command_id.into(),
            queue_id: None,
            idempotency_key: None,
            work_id: work_id.into(),
            lease_id: lease_id.into(),
            attempt,
            worker_id: worker_id.into(),
            error: None,
            retryable,
            now_ms: None,
        })
    }

    /// Updates or reads `cancel`.
    pub fn cancel(
        &self,
        work_id: impl Into<String>,
        command_id: impl Into<String>,
        reason: Option<String>,
    ) -> WorkQueueCommand<T> {
        self.publish_command(WorkQueueCommand::Cancel {
            command_id: command_id.into(),
            queue_id: None,
            idempotency_key: None,
            work_id: work_id.into(),
            reason,
            now_ms: None,
        })
    }

    /// Updates or reads `schedule`.
    pub fn schedule(
        &self,
        work_id: impl Into<String>,
        not_before_ms: u64,
        command_id: impl Into<String>,
    ) -> WorkQueueCommand<T> {
        self.publish_command(WorkQueueCommand::Schedule {
            command_id: command_id.into(),
            queue_id: None,
            idempotency_key: None,
            work_id: work_id.into(),
            schedule_id: None,
            not_before_ms,
            deadline_ms: None,
            reason: None,
            now_ms: None,
        })
    }

    /// Updates or reads `expire_leases`.
    pub fn expire_leases(&self, command_id: impl Into<String>) -> WorkQueueCommand<T> {
        self.publish_command(WorkQueueCommand::ExpireLeases {
            command_id: command_id.into(),
            queue_id: None,
            idempotency_key: None,
            work_ids: Vec::new(),
            limit: None,
            now_ms: None,
        })
    }

    /// Updates or reads `available`.
    pub fn available(&self) -> WorkQueueAvailableProjection<T> {
        self.available_named(None::<String>)
    }

    /// Updates or reads `available_named`.
    pub fn available_named(
        &self,
        name: Option<impl Into<String>>,
    ) -> WorkQueueAvailableProjection<T> {
        let available_pull_id = LockId::new(format!("{}/available", self.name));
        let state = self.state.clone();
        let snapshot = self.graph.node_opts::<WorkQueueAvailablePage<T>, _>(
            vec![self.records.erased()],
            move |ctx| {
                let params = pull_params::<WorkQueueAvailableParams>(ctx);
                ctx.emit(available_page(&state.borrow(), &params));
            },
            pull_node_opts(
                name.map(Into::into)
                    .unwrap_or_else(|| format!("{}/available", self.name)),
                "workQueueAvailable",
                available_pull_id.clone(),
            ),
        );
        WorkQueueAvailableProjection {
            available: snapshot,
            available_pull_id,
            status: self.status.clone(),
            issues: self.issues.clone(),
        }
    }

    /// Updates or reads `work`.
    pub fn work(
        &self,
        work_id: impl Into<String>,
    ) -> WorkQueueProjection<WorkQueueWorkSnapshot<T>> {
        self.work_named(work_id, None::<String>)
    }

    /// Updates or reads `work_named`.
    pub fn work_named(
        &self,
        work_id: impl Into<String>,
        name: Option<impl Into<String>>,
    ) -> WorkQueueProjection<WorkQueueWorkSnapshot<T>> {
        let work_id = work_id.into();
        let snapshot_pull_id = LockId::new(format!("{}/{work_id}/snapshot", self.name));
        let state = self.state.clone();
        let work_id_for_fn = work_id.clone();
        let snapshot = self.graph.node_opts::<WorkQueueWorkSnapshot<T>, _>(
            vec![self.records.erased()],
            move |ctx| {
                let _ = ctx.pull();
                ctx.emit(work_snapshot(&state.borrow(), &work_id_for_fn));
            },
            pull_node_opts(
                name.map(Into::into)
                    .unwrap_or_else(|| format!("{}/{work_id}", self.name)),
                "workQueueWorkSnapshot",
                snapshot_pull_id.clone(),
            ),
        );
        WorkQueueProjection {
            snapshot,
            snapshot_pull_id,
            status: self.status.clone(),
            issues: self.issues.clone(),
        }
    }

    /// Updates or reads `dead_letter`.
    pub fn dead_letter(&self) -> WorkQueueProjection<WorkQueueDeadLetterPage<T>> {
        self.dead_letter_named(None::<String>)
    }

    /// Updates or reads `dead_letter_named`.
    pub fn dead_letter_named(
        &self,
        name: Option<impl Into<String>>,
    ) -> WorkQueueProjection<WorkQueueDeadLetterPage<T>> {
        let snapshot_pull_id = LockId::new(format!("{}/deadLetter", self.name));
        let state = self.state.clone();
        let snapshot = self.graph.node_opts::<WorkQueueDeadLetterPage<T>, _>(
            vec![self.records.erased()],
            move |ctx| {
                let params = pull_params::<WorkQueueDeadLetterParams>(ctx);
                ctx.emit(dead_letter_page(&state.borrow(), &params));
            },
            pull_node_opts(
                name.map(Into::into)
                    .unwrap_or_else(|| format!("{}/deadLetter", self.name)),
                "workQueueDeadLetter",
                snapshot_pull_id.clone(),
            ),
        );
        WorkQueueProjection {
            snapshot,
            snapshot_pull_id,
            status: self.status.clone(),
            issues: self.issues.clone(),
        }
    }

    fn publish_command(&self, command: WorkQueueCommand<T>) -> WorkQueueCommand<T> {
        self.commands.set(command.clone());
        command
    }

    fn next_command_id(&self, kind: &str) -> String {
        let next = self.command_seq.get() + 1;
        self.command_seq.set(next);
        compound_tuple_key(
            "work-queue-command",
            &[&self.queue_id, kind, &next.to_string()],
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// `WorkQueueSubmitOptions` data container.
pub struct WorkQueueSubmitOptions {
    /// `command_id` field for command id.
    pub command_id: Option<String>,
    /// `idempotency_key` field for idempotency key.
    pub idempotency_key: Option<String>,
    /// `work_id` field for work id.
    pub work_id: Option<String>,
    /// `priority` field for priority.
    pub priority: Option<i64>,
    /// `tags` field for tags.
    pub tags: Vec<String>,
    /// `requirements` field for requirements.
    pub requirements: Vec<String>,
    /// `not_before_ms` field for not before ms.
    pub not_before_ms: Option<u64>,
    /// `deadline_ms` field for deadline ms.
    pub deadline_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WorkQueueClaimOptions` data container.
pub struct WorkQueueClaimOptions {
    /// `command_id` field for command id.
    pub command_id: Option<String>,
    /// `queue_id` field for queue id.
    pub queue_id: Option<String>,
    /// `idempotency_key` field for idempotency key.
    pub idempotency_key: Option<String>,
    /// `worker_id` field for worker id.
    pub worker_id: String,
    /// `requested_work_ids` field for requested work ids.
    pub requested_work_ids: Vec<String>,
    /// `limit` field for limit.
    pub limit: Option<usize>,
    /// `lease_duration_ms` field for lease duration ms.
    pub lease_duration_ms: Option<u64>,
    /// `now_ms` field for now ms.
    pub now_ms: Option<u64>,
}

impl WorkQueueClaimOptions {
    /// Creates or computes `new`.
    pub fn new(worker_id: impl Into<String>) -> Self {
        Self {
            command_id: None,
            queue_id: None,
            idempotency_key: None,
            worker_id: worker_id.into(),
            requested_work_ids: Vec::new(),
            limit: None,
            lease_duration_ms: None,
            now_ms: None,
        }
    }

    /// Updates or reads `command_id`.
    pub fn command_id(mut self, command_id: impl Into<String>) -> Self {
        self.command_id = Some(command_id.into());
        self
    }

    /// Updates or reads `requested_work_ids`.
    pub fn requested_work_ids(mut self, ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.requested_work_ids = ids.into_iter().map(Into::into).collect();
        self
    }

    /// Updates or reads `idempotency_key`.
    pub fn idempotency_key(mut self, idempotency_key: impl Into<String>) -> Self {
        self.idempotency_key = Some(idempotency_key.into());
        self
    }

    /// Updates or reads `now_ms`.
    pub fn now_ms(mut self, now_ms: u64) -> Self {
        self.now_ms = Some(now_ms);
        self
    }
}

/// Creates or computes `work_queue`.
pub fn work_queue<T: Clone + 'static>(graph: &Graph, opts: WorkQueueOptions<T>) -> WorkQueue<T> {
    assert_non_empty(&opts.queue_id, "workQueue.queueId");
    assert_non_empty(&opts.topic, "workQueue.topic");
    assert_non_empty(&opts.subscription_id, "workQueue.subscriptionId");
    assert!(
        opts.lease_duration_ms > 0,
        "workQueue: leaseDurationMs must be positive"
    );
    assert!(
        opts.retry.max_attempts > 0,
        "workQueue: retry.maxAttempts must be positive"
    );

    let queue_id = Rc::new(opts.queue_id.clone());
    let name = Rc::new(
        opts.name
            .clone()
            .unwrap_or_else(|| format!("workQueue/{}", opts.queue_id)),
    );
    let topic = Rc::new(opts.topic.clone());
    let admission = opts.bus.subscription(
        MessageBusSubscriptionOptions::new(opts.topic.clone(), opts.subscription_id.clone())
            .from(opts.from)
            .named(format!("{name}/admission")),
    );
    let commands = graph.node_opts::<WorkQueueCommand<T>, _>(
        Vec::new(),
        work_queue_command_body::<T>(0),
        node_opts(format!("{name}/commands"), "workQueueCommands"),
    );
    let state = Rc::new(RefCell::new(RuntimeState::default()));
    let admission_kick = graph.state_opts(
        "poll".to_owned(),
        node_opts(format!("{name}/admissionKick"), "workQueueAdmissionKick"),
    );
    let admission_pages = graph.node_opts::<MessageBusAvailablePage<WorkQueueSubmit<T>>, _>(
        vec![
            admission.available.erased(),
            admission_kick.erased(),
            opts.bus.messages.erased(),
            opts.bus.status.erased(),
        ],
        {
            let admission_pull_id = admission.available_pull_id.clone();
            let topic = opts.topic.clone();
            let subscription_id = opts.subscription_id.clone();
            move |ctx| {
                for page in ctx.batch::<MessageBusAvailablePage<WorkQueueSubmit<T>>>(0) {
                    ctx.emit((*page).clone());
                }
                let mut should_pull = !ctx.batch::<String>(1).is_empty();
                for message in ctx.batch::<MessageEnvelope<WorkQueueSubmit<T>>>(2) {
                    if message.topic == topic {
                        should_pull = true;
                    }
                }
                for status in ctx.batch::<MessageBusStatus>(3) {
                    if should_poll_admission(&status, &topic, &subscription_id) {
                        should_pull = true;
                    }
                }
                if should_pull {
                    ctx.up_next_toward(
                        0,
                        vec![Message::Pull(PullDemand::with_params(
                            admission_pull_id.clone(),
                            MessageBusAvailableParams {
                                limit: Some(100),
                                after_seq: None,
                            },
                        ))],
                    );
                }
            }
        },
        {
            let mut opts = node_opts(format!("{name}/admissionPages"), "workQueueAdmissionPages");
            opts.node.partial = true;
            opts
        },
    );
    let runtime_state = state.clone();
    let runtime_opts = RuntimeOptions {
        queue_id: opts.queue_id.clone(),
        subscription_id: opts.subscription_id.clone(),
        now: opts.now.clone(),
        lease_duration_ms: opts.lease_duration_ms,
        retry: opts.retry,
    };
    let runtime = graph.node_opts::<QueueEvent<T>, _>(
        vec![commands.erased(), admission_pages.erased()],
        move |ctx| {
            for page in ctx.batch::<MessageBusAvailablePage<WorkQueueSubmit<T>>>(1) {
                for message in &page.messages {
                    let now_ms = timestamp_or_zero(&runtime_opts.now);
                    let events = {
                        let mut state = runtime_state.borrow_mut();
                        admit_message(&runtime_opts, &mut state, message, now_ms)
                    };
                    for event in events {
                        ctx.emit(event);
                    }
                }
            }
            for command in ctx.batch::<WorkQueueCommand<T>>(0) {
                let now_ms = command
                    .now_ms()
                    .unwrap_or_else(|| timestamp_or_zero(&runtime_opts.now));
                let events = {
                    let mut state = runtime_state.borrow_mut();
                    reduce_queue_command(&runtime_opts, &mut state, (*command).clone(), now_ms)
                };
                for event in events {
                    ctx.emit(event);
                }
            }
        },
        {
            let mut opts = node_opts(format!("{name}/runtime"), "workQueueRuntime");
            opts.node.partial = true;
            opts
        },
    );
    let records = graph.node_opts::<WorkQueueRecord<T>, _>(
        vec![runtime.erased()],
        move |ctx| {
            for event in ctx.batch::<QueueEvent<T>>(0) {
                if let QueueEvent::Record(record) = event.as_ref() {
                    ctx.emit(record.clone());
                }
            }
        },
        node_opts(format!("{name}/records"), "workQueueRecords"),
    );
    let status = graph.node_opts::<WorkQueueStatus, _>(
        vec![runtime.erased()],
        move |ctx| {
            for event in ctx.batch::<QueueEvent<T>>(0) {
                if let QueueEvent::Status(status) = event.as_ref() {
                    ctx.emit(status.clone());
                }
            }
        },
        node_opts(format!("{name}/status"), "workQueueStatus"),
    );
    let issues = graph.node_opts::<DataIssue, _>(
        vec![runtime.erased()],
        move |ctx| {
            for event in ctx.batch::<QueueEvent<T>>(0) {
                if let QueueEvent::Issue(issue) = event.as_ref() {
                    ctx.emit(issue.clone());
                }
            }
        },
        node_opts(format!("{name}/issues"), "workQueueIssues"),
    );
    let admission_ack_commands = graph.node_opts::<MessageBusCommand<WorkQueueSubmit<T>>, _>(
        vec![records.erased()],
        {
            move |ctx| {
                for record in ctx.batch::<WorkQueueRecord<T>>(0) {
                    if let Some(command) = admission_ack_command(&record) {
                        ctx.emit(command);
                    }
                }
            }
        },
        node_opts(
            format!("{name}/admissionAckCommands"),
            "workQueueAdmissionAckCommands",
        ),
    );
    let ack_release =
        attach_message_bus_deferred_command_sink(graph, &opts.bus, &admission_ack_commands);
    let runtime_release = graph.retain(&runtime, &format!("{name}.workQueue.runtime"));
    WorkQueue {
        name,
        queue_id,
        topic,
        bus: opts.bus,
        command_seq: Rc::new(Cell::new(0)),
        commands,
        records,
        status,
        issues,
        state,
        graph: graph.clone(),
        _admission_ack_commands: admission_ack_commands,
        _retain: Rc::new(WorkQueueRetain::new(vec![runtime_release, ack_release])),
    }
}

struct RuntimeOptions {
    queue_id: String,
    subscription_id: String,
    now: Rc<dyn Fn() -> u64>,
    lease_duration_ms: u64,
    retry: RetryPolicy,
}

fn work_queue_command_body<T: Clone + 'static>(
    command_source_count: usize,
) -> impl Fn(&Ctx) + 'static {
    move |ctx| {
        for index in 0..command_source_count {
            for command in ctx.batch::<WorkQueueCommand<T>>(index) {
                ctx.emit((*command).clone());
            }
        }
    }
}

fn should_poll_admission(status: &MessageBusStatus, topic: &str, subscription_id: &str) -> bool {
    if status.topic.as_deref() != Some(topic) {
        return false;
    }
    matches!(
        status.kind,
        MessageBusStatusKind::MessagePublished | MessageBusStatusKind::RetentionTrimmed
    ) || (status.subscription_id.as_deref() == Some(subscription_id)
        && matches!(
            status.kind,
            MessageBusStatusKind::SubscriptionAcked | MessageBusStatusKind::SubscriptionSought
        ))
}

fn admission_ack_command<T>(
    record: &WorkQueueRecord<T>,
) -> Option<MessageBusCommand<WorkQueueSubmit<T>>> {
    let message_bus = record.admission_message_bus()?;
    Some(MessageBusCommand::Ack {
        topic: message_bus.topic.clone(),
        subscription_id: message_bus.subscription_id.clone(),
        seq: message_bus.seq,
        command_id: Some(compound_tuple_key(
            "work-queue-admission-ack",
            &[
                record_queue_id(record),
                &message_bus.topic,
                &message_bus.subscription_id,
                &message_bus.seq.to_string(),
            ],
        )),
    })
}

fn admit_message<T: Clone + 'static>(
    opts: &RuntimeOptions,
    state: &mut RuntimeState<T>,
    message: &MessageEnvelope<WorkQueueSubmit<T>>,
    now_ms: u64,
) -> Vec<QueueEvent<T>> {
    let source = canonical_tuple_key(&[&message.topic, &message.seq.to_string()]);
    if !state.source_seqs.insert(source) {
        return Vec::new();
    }
    let submit = &message.payload;
    let work_id = submit.work_id.clone().unwrap_or_else(|| {
        compound_tuple_key(
            "work-queue-work",
            &[&opts.queue_id, &message.topic, &message.seq.to_string()],
        )
    });
    let message_bus = WorkQueueMessageBusRef {
        topic: message.topic.clone(),
        seq: message.seq,
        subscription_id: opts.subscription_id.clone(),
    };
    if state.works.contains_key(&work_id) {
        let record = append_record(
            state,
            WorkQueueRecord::AdmissionDeduped {
                record_seq: 0,
                queue_id: opts.queue_id.clone(),
                work_id: work_id.clone(),
                message_bus,
                reason: "duplicate-work".to_owned(),
                existing_work_id: work_id.clone(),
                recorded_at_ms: now_ms,
            },
        );
        return vec![
            QueueEvent::Record(record.clone()),
            status_event(
                opts,
                WorkQueueStatusKind::AdmissionRejected,
                now_ms,
                StatusFields {
                    work_id: Some(work_id),
                    record_seq: Some(record.record_seq()),
                    issue_code: Some("duplicate-work".to_owned()),
                    ..StatusFields::default()
                },
            ),
        ];
    }
    let initial_state = if submit.not_before_ms.is_some_and(|t| t > now_ms) {
        WorkQueueDerivedState::Scheduled
    } else {
        WorkQueueDerivedState::Ready
    };
    state.works.insert(
        work_id.clone(),
        WorkState {
            work_id: work_id.clone(),
            payload: submit.payload.clone(),
            state: initial_state,
            admission_seq: message.seq,
            priority: submit.priority,
            tags: submit.tags.clone(),
            requirements: submit.requirements.clone(),
            not_before_ms: submit.not_before_ms,
            retry_at_ms: None,
            deadline_ms: submit.deadline_ms,
            attempt: 0,
            lease_id: None,
            worker_id: None,
            lease_expires_at_ms: None,
        },
    );
    let record = append_record(
        state,
        WorkQueueRecord::WorkAdmitted {
            record_seq: 0,
            queue_id: opts.queue_id.clone(),
            work_id: work_id.clone(),
            payload: submit.payload.clone(),
            message_bus,
            priority: submit.priority,
            tags: submit.tags.clone(),
            requirements: submit.requirements.clone(),
            not_before_ms: submit.not_before_ms,
            deadline_ms: submit.deadline_ms,
            recorded_at_ms: now_ms,
        },
    );
    vec![
        QueueEvent::Record(record.clone()),
        status_event(
            opts,
            WorkQueueStatusKind::AdmissionAccepted,
            now_ms,
            StatusFields {
                work_id: Some(work_id),
                record_seq: Some(record.record_seq()),
                ..StatusFields::default()
            },
        ),
    ]
}

fn reduce_queue_command<T: Clone + 'static>(
    opts: &RuntimeOptions,
    state: &mut RuntimeState<T>,
    command: WorkQueueCommand<T>,
    now_ms: u64,
) -> Vec<QueueEvent<T>> {
    if let Some(error) = validate_queue_command(&opts.queue_id, &command) {
        return reject_queue_command(opts, &command, &error.0, &error.1, now_ms);
    }
    if state.command_ids.contains(command.command_id()) {
        return reject_queue_command(
            opts,
            &command,
            "duplicate-command",
            "duplicate commandId",
            now_ms,
        );
    }
    if let Some(idempotency_key) = command.idempotency_key() {
        if state.idempotency_keys.contains(idempotency_key) {
            return reject_queue_command(
                opts,
                &command,
                "duplicate-command",
                "duplicate idempotencyKey",
                now_ms,
            );
        }
    }
    state.command_ids.insert(command.command_id().to_owned());
    if let Some(idempotency_key) = command.idempotency_key() {
        state.idempotency_keys.insert(idempotency_key.to_owned());
    }
    match command {
        WorkQueueCommand::Submit { command_id, .. } => vec![status_event(
            opts,
            WorkQueueStatusKind::CommandAccepted,
            now_ms,
            StatusFields {
                command_id: Some(command_id),
                details: Some("submit uses messageBus".to_owned()),
                ..StatusFields::default()
            },
        )],
        WorkQueueCommand::Claim { .. } => claim_work(opts, state, command, now_ms),
        WorkQueueCommand::RenewLease { .. } => renew_lease(opts, state, command, now_ms),
        WorkQueueCommand::Release { .. } => release_work(opts, state, command, now_ms),
        WorkQueueCommand::Complete { .. } => complete_work(opts, state, command, now_ms),
        WorkQueueCommand::Fail { .. } => fail_work(opts, state, command, now_ms),
        WorkQueueCommand::Cancel { .. } => cancel_work(opts, state, command, now_ms),
        WorkQueueCommand::Schedule { .. } => schedule_work(opts, state, command, now_ms),
        WorkQueueCommand::ExpireLeases { .. } => expire_leases(opts, state, command, now_ms),
    }
}

fn claim_work<T: Clone + 'static>(
    opts: &RuntimeOptions,
    state: &mut RuntimeState<T>,
    command: WorkQueueCommand<T>,
    now_ms: u64,
) -> Vec<QueueEvent<T>> {
    let WorkQueueCommand::Claim {
        command_id,
        worker_id,
        requested_work_ids,
        limit,
        lease_duration_ms,
        ..
    } = command
    else {
        return Vec::new();
    };
    let limit = positive_limit(limit.unwrap_or_else(|| requested_work_ids.len().max(1)));
    let requested = requested_work_ids.iter().cloned().collect::<HashSet<_>>();
    let mut events = Vec::new();
    let expired = state
        .works
        .values()
        .filter(|work| {
            (requested.is_empty() || requested.contains(&work.work_id)) && is_expired(work, now_ms)
        })
        .map(|work| work.work_id.clone())
        .collect::<Vec<_>>();
    for work_id in expired {
        if let Some(work) = state.works.get(&work_id).cloned() {
            events.extend(materialize_lease_expired(
                opts,
                state,
                work,
                Some(command_id.clone()),
                now_ms,
                None,
            ));
        }
    }
    let mut candidates = state
        .works
        .values()
        .filter(|work| requested.is_empty() || requested.contains(&work.work_id))
        .filter(|work| is_ready(work, now_ms))
        .map(|work| (work.admission_seq, work.work_id.clone()))
        .collect::<Vec<_>>();
    candidates
        .sort_by(|(a_seq, a_id), (b_seq, b_id)| a_seq.cmp(b_seq).then_with(|| a_id.cmp(b_id)));
    let candidates = candidates
        .into_iter()
        .take(limit)
        .map(|(_, work_id)| work_id)
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        if requested.is_empty() {
            events.extend(reject_queue_command_by_id(
                opts,
                Some(command_id),
                "not-ready",
                "no ready work",
                now_ms,
            ));
        } else {
            events.extend(claim_miss_events(
                opts,
                state,
                &command_id,
                &requested,
                &HashSet::new(),
                now_ms,
            ));
        }
        return events;
    }
    let mut claimed = HashSet::new();
    let lease_duration_ms = lease_duration_ms.unwrap_or(opts.lease_duration_ms);
    let lease_expires_at_ms = match checked_timestamp(
        opts,
        Some(command_id.clone()),
        now_ms,
        lease_duration_ms,
        "lease duration overflows timestamp",
    ) {
        Ok(value) => value,
        Err(events) => return events,
    };
    for work_id in candidates {
        let lease_seq = state.lease_seq + 1;
        state.lease_seq = lease_seq;
        let work = state
            .works
            .get_mut(&work_id)
            .expect("candidate work id came from state");
        claimed.insert(work_id.clone());
        work.state = WorkQueueDerivedState::Leased;
        work.attempt += 1;
        work.lease_id = Some(compound_tuple_key(
            "work-queue-lease",
            &[&work.work_id, &lease_seq.to_string()],
        ));
        work.worker_id = Some(worker_id.clone());
        work.lease_expires_at_ms = Some(lease_expires_at_ms);
        let lease_id = work.lease_id.clone().expect("lease just set");
        let record = append_record(
            state,
            WorkQueueRecord::WorkClaimed {
                record_seq: 0,
                queue_id: opts.queue_id.clone(),
                work_id: work_id.clone(),
                command_id: command_id.clone(),
                lease_id,
                attempt: state.works[&work_id].attempt,
                worker_id: worker_id.clone(),
                claimed_at_ms: now_ms,
                lease_expires_at_ms: state.works[&work_id]
                    .lease_expires_at_ms
                    .expect("lease expiry just set"),
            },
        );
        events.push(QueueEvent::Record(record.clone()));
        events.push(status_event(
            opts,
            WorkQueueStatusKind::CommandAccepted,
            now_ms,
            StatusFields {
                work_id: Some(work_id),
                command_id: Some(command_id.clone()),
                record_seq: Some(record.record_seq()),
                ..StatusFields::default()
            },
        ));
    }
    if !requested.is_empty() {
        events.extend(claim_miss_events(
            opts,
            state,
            &command_id,
            &requested,
            &claimed,
            now_ms,
        ));
    }
    events
}

fn claim_miss_events<T: Clone + 'static>(
    opts: &RuntimeOptions,
    state: &RuntimeState<T>,
    command_id: &str,
    requested: &HashSet<String>,
    claimed: &HashSet<String>,
    now_ms: u64,
) -> Vec<QueueEvent<T>> {
    let mut events = Vec::new();
    for work_id in requested {
        if claimed.contains(work_id) {
            continue;
        }
        let Some(work) = state.works.get(work_id) else {
            events.extend(reject_queue_command_by_id(
                opts,
                Some(command_id.to_owned()),
                "unknown-work",
                &format!("unknown work '{work_id}'"),
                now_ms,
            ));
            continue;
        };
        if is_ready(work, now_ms) {
            continue;
        }
        let code = if is_terminal(work.state) {
            "terminal-work"
        } else if work.state == WorkQueueDerivedState::Leased {
            "already-leased"
        } else {
            "not-ready"
        };
        events.extend(reject_queue_command_by_id(
            opts,
            Some(command_id.to_owned()),
            code,
            &format!("requested work '{work_id}' is not claimable"),
            now_ms,
        ));
    }
    events
}

fn renew_lease<T: Clone + 'static>(
    opts: &RuntimeOptions,
    state: &mut RuntimeState<T>,
    command: WorkQueueCommand<T>,
    now_ms: u64,
) -> Vec<QueueEvent<T>> {
    let WorkQueueCommand::RenewLease {
        command_id,
        work_id,
        lease_id,
        attempt,
        worker_id,
        lease_duration_ms,
        lease_expires_at_ms,
        ..
    } = command
    else {
        return Vec::new();
    };
    let checked = current_lease(
        opts,
        state,
        LeaseCheck {
            work_id: &work_id,
            lease_id: &lease_id,
            attempt,
            worker_id: &worker_id,
            command_id: Some(command_id.clone()),
            now_ms,
        },
    );
    if let Err(events) = checked {
        return events;
    }
    let work = state.works.get_mut(&work_id).expect("checked lease");
    let previous = work.lease_expires_at_ms.expect("checked lease expiry");
    let next = match lease_expires_at_ms {
        Some(value) => value,
        None => match checked_timestamp(
            opts,
            Some(command_id.clone()),
            now_ms,
            lease_duration_ms.unwrap_or(opts.lease_duration_ms),
            "lease renewal duration overflows timestamp",
        ) {
            Ok(value) => value,
            Err(events) => return events,
        },
    };
    work.lease_expires_at_ms = Some(next);
    let record = append_record(
        state,
        WorkQueueRecord::LeaseRenewed {
            record_seq: 0,
            queue_id: opts.queue_id.clone(),
            work_id: work_id.clone(),
            command_id: command_id.clone(),
            lease_id,
            attempt,
            worker_id,
            previous_lease_expires_at_ms: previous,
            lease_expires_at_ms: next,
            renewed_at_ms: now_ms,
        },
    );
    accepted(opts, record, Some(work_id), Some(command_id), now_ms)
}

fn release_work<T: Clone + 'static>(
    opts: &RuntimeOptions,
    state: &mut RuntimeState<T>,
    command: WorkQueueCommand<T>,
    now_ms: u64,
) -> Vec<QueueEvent<T>> {
    let WorkQueueCommand::Release {
        command_id,
        work_id,
        lease_id,
        attempt,
        worker_id,
        reason,
        ..
    } = command
    else {
        return Vec::new();
    };
    if let Err(events) = current_lease(
        opts,
        state,
        LeaseCheck {
            work_id: &work_id,
            lease_id: &lease_id,
            attempt,
            worker_id: &worker_id,
            command_id: Some(command_id.clone()),
            now_ms,
        },
    ) {
        return events;
    }
    clear_lease(state.works.get_mut(&work_id).expect("checked lease"));
    state.works.get_mut(&work_id).expect("checked lease").state = WorkQueueDerivedState::Ready;
    let record = append_record(
        state,
        WorkQueueRecord::WorkReleased {
            record_seq: 0,
            queue_id: opts.queue_id.clone(),
            work_id: work_id.clone(),
            command_id: command_id.clone(),
            lease_id,
            attempt,
            worker_id,
            released_at_ms: now_ms,
            reason,
        },
    );
    accepted(opts, record, Some(work_id), Some(command_id), now_ms)
}

fn complete_work<T: Clone + 'static>(
    opts: &RuntimeOptions,
    state: &mut RuntimeState<T>,
    command: WorkQueueCommand<T>,
    now_ms: u64,
) -> Vec<QueueEvent<T>> {
    let WorkQueueCommand::Complete {
        command_id,
        work_id,
        lease_id,
        attempt,
        worker_id,
        result,
        ..
    } = command
    else {
        return Vec::new();
    };
    if let Err(events) = current_lease(
        opts,
        state,
        LeaseCheck {
            work_id: &work_id,
            lease_id: &lease_id,
            attempt,
            worker_id: &worker_id,
            command_id: Some(command_id.clone()),
            now_ms,
        },
    ) {
        return events;
    }
    let work = state.works.get_mut(&work_id).expect("checked lease");
    work.state = WorkQueueDerivedState::Completed;
    clear_lease(work);
    let attempt_record = append_record(
        state,
        WorkQueueRecord::AttemptCompleted {
            record_seq: 0,
            queue_id: opts.queue_id.clone(),
            work_id: work_id.clone(),
            command_id: command_id.clone(),
            lease_id: lease_id.clone(),
            attempt,
            worker_id: worker_id.clone(),
            result: result.clone(),
            recorded_at_ms: now_ms,
        },
    );
    let done = append_record(
        state,
        WorkQueueRecord::WorkCompleted {
            record_seq: 0,
            queue_id: opts.queue_id.clone(),
            work_id: work_id.clone(),
            command_id: command_id.clone(),
            lease_id,
            attempt,
            worker_id,
            result,
            recorded_at_ms: now_ms,
        },
    );
    vec![
        QueueEvent::Record(attempt_record),
        QueueEvent::Record(done.clone()),
        status_event(
            opts,
            WorkQueueStatusKind::CommandAccepted,
            now_ms,
            StatusFields {
                work_id: Some(work_id),
                command_id: Some(command_id),
                record_seq: Some(done.record_seq()),
                ..StatusFields::default()
            },
        ),
    ]
}

fn fail_work<T: Clone + 'static>(
    opts: &RuntimeOptions,
    state: &mut RuntimeState<T>,
    command: WorkQueueCommand<T>,
    now_ms: u64,
) -> Vec<QueueEvent<T>> {
    let WorkQueueCommand::Fail {
        command_id,
        work_id,
        lease_id,
        attempt,
        worker_id,
        error,
        retryable,
        ..
    } = command
    else {
        return Vec::new();
    };
    if let Err(events) = current_lease(
        opts,
        state,
        LeaseCheck {
            work_id: &work_id,
            lease_id: &lease_id,
            attempt,
            worker_id: &worker_id,
            command_id: Some(command_id.clone()),
            now_ms,
        },
    ) {
        return events;
    }
    let attempt_for_policy = state.works.get(&work_id).expect("checked lease").attempt;
    let should_retry = retryable.unwrap_or(true) && opts.retry.should_retry(attempt_for_policy);
    let retry_delay_ms = if should_retry {
        opts.retry
            .next_delay_ms(attempt_for_policy.saturating_add(1))
            .unwrap_or_default()
    } else {
        0
    };
    let retry_at_ms = if should_retry {
        match checked_timestamp(
            opts,
            Some(command_id.clone()),
            now_ms,
            retry_delay_ms,
            "retry delay overflows timestamp",
        ) {
            Ok(value) => value,
            Err(events) => return events,
        }
    } else {
        now_ms
    };
    let failed = append_record(
        state,
        WorkQueueRecord::AttemptFailed {
            record_seq: 0,
            queue_id: opts.queue_id.clone(),
            work_id: work_id.clone(),
            command_id: command_id.clone(),
            lease_id,
            attempt,
            worker_id,
            error,
            retryable,
            recorded_at_ms: now_ms,
        },
    );
    let work = state.works.get_mut(&work_id).expect("checked lease");
    if !should_retry {
        work.state = WorkQueueDerivedState::DeadLettered;
        clear_lease(work);
        let dead = append_record(
            state,
            WorkQueueRecord::WorkDeadLettered {
                record_seq: 0,
                queue_id: opts.queue_id.clone(),
                work_id: work_id.clone(),
                command_id: command_id.clone(),
                reason: "attempts-exhausted".to_owned(),
                exhausted_attempts: Some(attempt),
                recorded_at_ms: now_ms,
            },
        );
        state.dead_letters.push(dead.clone());
        return vec![
            QueueEvent::Record(failed),
            QueueEvent::Record(dead.clone()),
            status_event(
                opts,
                WorkQueueStatusKind::CommandAccepted,
                now_ms,
                StatusFields {
                    work_id: Some(work_id),
                    command_id: Some(command_id),
                    record_seq: Some(dead.record_seq()),
                    ..StatusFields::default()
                },
            ),
        ];
    }
    work.state = if retry_delay_ms > 0 {
        WorkQueueDerivedState::RetryWait
    } else {
        WorkQueueDerivedState::Ready
    };
    work.retry_at_ms = Some(retry_at_ms);
    clear_lease(work);
    let retry = append_record(
        state,
        WorkQueueRecord::RetryScheduled {
            record_seq: 0,
            queue_id: opts.queue_id.clone(),
            work_id: work_id.clone(),
            command_id: command_id.clone(),
            retry_at_ms,
            delay_ms: retry_delay_ms,
            reason: Some("retry-policy".to_owned()),
            recorded_at_ms: now_ms,
        },
    );
    vec![
        QueueEvent::Record(failed),
        QueueEvent::Record(retry.clone()),
        status_event(
            opts,
            WorkQueueStatusKind::CommandAccepted,
            now_ms,
            StatusFields {
                work_id: Some(work_id),
                command_id: Some(command_id),
                record_seq: Some(retry.record_seq()),
                ..StatusFields::default()
            },
        ),
    ]
}

fn cancel_work<T: Clone + 'static>(
    opts: &RuntimeOptions,
    state: &mut RuntimeState<T>,
    command: WorkQueueCommand<T>,
    now_ms: u64,
) -> Vec<QueueEvent<T>> {
    let WorkQueueCommand::Cancel {
        command_id,
        work_id,
        reason,
        ..
    } = command
    else {
        return Vec::new();
    };
    let Some(work) = state.works.get_mut(&work_id) else {
        return reject_queue_command_by_id(
            opts,
            Some(command_id),
            "unknown-work",
            "unknown work",
            now_ms,
        );
    };
    if is_terminal(work.state) {
        return reject_queue_command_by_id(
            opts,
            Some(command_id),
            "terminal-work",
            "work is terminal",
            now_ms,
        );
    }
    let canceled_lease_id = work.lease_id.clone();
    let attempt = (work.attempt > 0).then_some(work.attempt);
    work.state = WorkQueueDerivedState::Canceled;
    clear_lease(work);
    let record = append_record(
        state,
        WorkQueueRecord::WorkCanceled {
            record_seq: 0,
            queue_id: opts.queue_id.clone(),
            work_id: work_id.clone(),
            command_id: command_id.clone(),
            reason,
            canceled_at_ms: now_ms,
            canceled_lease_id,
            attempt,
        },
    );
    accepted(opts, record, Some(work_id), Some(command_id), now_ms)
}

fn schedule_work<T: Clone + 'static>(
    opts: &RuntimeOptions,
    state: &mut RuntimeState<T>,
    command: WorkQueueCommand<T>,
    now_ms: u64,
) -> Vec<QueueEvent<T>> {
    let WorkQueueCommand::Schedule {
        command_id,
        work_id,
        schedule_id,
        not_before_ms,
        deadline_ms,
        reason,
        ..
    } = command
    else {
        return Vec::new();
    };
    let Some(work) = state.works.get_mut(&work_id) else {
        return reject_queue_command_by_id(
            opts,
            Some(command_id),
            "unknown-work",
            "unknown work",
            now_ms,
        );
    };
    if is_terminal(work.state) {
        return reject_queue_command_by_id(
            opts,
            Some(command_id),
            "terminal-work",
            "work is terminal",
            now_ms,
        );
    }
    if work.state == WorkQueueDerivedState::Leased {
        return reject_queue_command_by_id(
            opts,
            Some(command_id),
            "schedule-conflict",
            "leased work cannot be scheduled without release or cancel",
            now_ms,
        );
    }
    work.state = WorkQueueDerivedState::Scheduled;
    work.not_before_ms = Some(not_before_ms);
    work.deadline_ms = deadline_ms;
    let record = append_record(
        state,
        WorkQueueRecord::WorkScheduled {
            record_seq: 0,
            queue_id: opts.queue_id.clone(),
            work_id: work_id.clone(),
            command_id: command_id.clone(),
            schedule_id,
            not_before_ms,
            deadline_ms,
            reason,
            recorded_at_ms: now_ms,
        },
    );
    accepted(opts, record, Some(work_id), Some(command_id), now_ms)
}

fn expire_leases<T: Clone + 'static>(
    opts: &RuntimeOptions,
    state: &mut RuntimeState<T>,
    command: WorkQueueCommand<T>,
    now_ms: u64,
) -> Vec<QueueEvent<T>> {
    let WorkQueueCommand::ExpireLeases {
        command_id,
        work_ids,
        limit,
        ..
    } = command
    else {
        return Vec::new();
    };
    let requested = work_ids.into_iter().collect::<HashSet<_>>();
    let expired = state
        .works
        .values()
        .filter(|work| is_expired(work, now_ms))
        .filter(|work| requested.is_empty() || requested.contains(&work.work_id))
        .take(positive_limit(limit.unwrap_or(usize::MAX)))
        .map(|work| work.work_id.clone())
        .collect::<Vec<_>>();
    if expired.is_empty() {
        return vec![status_event(
            opts,
            WorkQueueStatusKind::MaintenanceNoop,
            now_ms,
            StatusFields {
                command_id: Some(command_id),
                ..StatusFields::default()
            },
        )];
    }
    let mut events = Vec::new();
    for work_id in expired.iter() {
        if let Some(work) = state.works.get(work_id).cloned() {
            events.extend(materialize_lease_expired(
                opts,
                state,
                work,
                Some(command_id.clone()),
                now_ms,
                None,
            ));
        }
    }
    events.push(status_event(
        opts,
        WorkQueueStatusKind::MaintenanceApplied,
        now_ms,
        StatusFields {
            command_id: Some(command_id),
            details: Some(format!("expired={}", expired.len())),
            ..StatusFields::default()
        },
    ));
    events
}

struct LeaseCheck<'a> {
    work_id: &'a str,
    lease_id: &'a str,
    attempt: u32,
    worker_id: &'a str,
    command_id: Option<String>,
    now_ms: u64,
}

fn current_lease<T: Clone + 'static>(
    opts: &RuntimeOptions,
    state: &mut RuntimeState<T>,
    check: LeaseCheck<'_>,
) -> Result<(), Vec<QueueEvent<T>>> {
    let Some(work) = state.works.get(check.work_id).cloned() else {
        return Err(reject_queue_command_by_id(
            opts,
            check.command_id,
            "unknown-work",
            "unknown work",
            check.now_ms,
        ));
    };
    if is_terminal(work.state) {
        return Err(reject_queue_command_by_id(
            opts,
            check.command_id,
            "terminal-work",
            "work is terminal",
            check.now_ms,
        ));
    }
    if work.state != WorkQueueDerivedState::Leased {
        return Err(reject_queue_command_by_id(
            opts,
            check.command_id,
            "lease-not-current",
            "work is not leased",
            check.now_ms,
        ));
    }
    if work.lease_id.as_deref() != Some(check.lease_id) {
        return Err(reject_queue_command_by_id(
            opts,
            check.command_id,
            "stale-lease",
            "lease is not current",
            check.now_ms,
        ));
    }
    if work.attempt != check.attempt {
        return Err(reject_queue_command_by_id(
            opts,
            check.command_id,
            "attempt-mismatch",
            "attempt mismatch",
            check.now_ms,
        ));
    }
    if work.worker_id.as_deref() != Some(check.worker_id) {
        return Err(reject_queue_command_by_id(
            opts,
            check.command_id,
            "worker-mismatch",
            "worker mismatch",
            check.now_ms,
        ));
    }
    if is_expired(&work, check.now_ms) {
        let command_id = check.command_id;
        let mut events =
            materialize_lease_expired(opts, state, work, command_id.clone(), check.now_ms, None);
        events.extend(reject_queue_command_by_id(
            opts,
            command_id,
            "lease-expired",
            "lease expired",
            check.now_ms,
        ));
        return Err(events);
    }
    Ok(())
}

fn materialize_lease_expired<T: Clone + 'static>(
    opts: &RuntimeOptions,
    state: &mut RuntimeState<T>,
    work: WorkState<T>,
    command_id: Option<String>,
    now_ms: u64,
    rejection: Option<(&str, &str)>,
) -> Vec<QueueEvent<T>> {
    let Some(current) = state.works.get_mut(&work.work_id) else {
        return Vec::new();
    };
    current.state = WorkQueueDerivedState::Ready;
    clear_lease(current);
    let record = append_record(
        state,
        WorkQueueRecord::LeaseExpired {
            record_seq: 0,
            queue_id: opts.queue_id.clone(),
            work_id: work.work_id.clone(),
            command_id: command_id.clone(),
            lease_id: work.lease_id.unwrap_or_default(),
            attempt: work.attempt,
            worker_id: work.worker_id.unwrap_or_default(),
            lease_expires_at_ms: work.lease_expires_at_ms.unwrap_or(now_ms),
            expired_at_ms: now_ms,
        },
    );
    let mut events = vec![QueueEvent::Record(record)];
    if let Some((code, message)) = rejection {
        events.extend(reject_queue_command_by_id(
            opts, command_id, code, message, now_ms,
        ));
    }
    events
}

fn accepted<T: Clone>(
    opts: &RuntimeOptions,
    record: WorkQueueRecord<T>,
    work_id: Option<String>,
    command_id: Option<String>,
    now_ms: u64,
) -> Vec<QueueEvent<T>> {
    vec![
        QueueEvent::Record(record.clone()),
        status_event(
            opts,
            WorkQueueStatusKind::CommandAccepted,
            now_ms,
            StatusFields {
                work_id,
                command_id,
                record_seq: Some(record.record_seq()),
                ..StatusFields::default()
            },
        ),
    ]
}

fn validate_queue_command<T>(
    queue_id: &str,
    command: &WorkQueueCommand<T>,
) -> Option<(String, String)> {
    if command.command_id().is_empty() {
        return Some((
            "malformed-command".to_owned(),
            "commandId must be non-empty".to_owned(),
        ));
    }
    if command.queue_id().is_some_and(|id| id != queue_id) {
        return Some((
            "queue-mismatch".to_owned(),
            "command queueId does not match this queue".to_owned(),
        ));
    }
    match command {
        WorkQueueCommand::Claim {
            worker_id,
            requested_work_ids,
            limit,
            lease_duration_ms,
            ..
        } => {
            if worker_id.is_empty() {
                return Some((
                    "malformed-command".to_owned(),
                    "claim workerId is required".to_owned(),
                ));
            }
            if limit.is_some_and(|limit| limit == 0)
                || lease_duration_ms.is_some_and(|duration| duration == 0)
                || requested_work_ids.iter().any(String::is_empty)
            {
                return Some((
                    "malformed-command".to_owned(),
                    "claim options are malformed".to_owned(),
                ));
            }
        }
        WorkQueueCommand::RenewLease {
            work_id,
            lease_id,
            attempt,
            worker_id,
            lease_duration_ms,
            ..
        } => {
            if work_id.is_empty()
                || lease_id.is_empty()
                || worker_id.is_empty()
                || *attempt == 0
                || lease_duration_ms.is_some_and(|duration| duration == 0)
            {
                return Some((
                    "malformed-command".to_owned(),
                    "lease command is malformed".to_owned(),
                ));
            }
        }
        WorkQueueCommand::Release {
            work_id,
            lease_id,
            attempt,
            worker_id,
            ..
        }
        | WorkQueueCommand::Complete {
            work_id,
            lease_id,
            attempt,
            worker_id,
            ..
        }
        | WorkQueueCommand::Fail {
            work_id,
            lease_id,
            attempt,
            worker_id,
            ..
        } => {
            if work_id.is_empty() || lease_id.is_empty() || worker_id.is_empty() || *attempt == 0 {
                return Some((
                    "malformed-command".to_owned(),
                    "lease command is malformed".to_owned(),
                ));
            }
        }
        WorkQueueCommand::Cancel { work_id, .. } | WorkQueueCommand::Schedule { work_id, .. } => {
            if work_id.is_empty() {
                return Some((
                    "malformed-command".to_owned(),
                    "workId must be non-empty".to_owned(),
                ));
            }
        }
        WorkQueueCommand::ExpireLeases {
            work_ids, limit, ..
        } => {
            if limit.is_some_and(|limit| limit == 0) || work_ids.iter().any(String::is_empty) {
                return Some((
                    "malformed-command".to_owned(),
                    "expire-leases options are malformed".to_owned(),
                ));
            }
        }
        WorkQueueCommand::Submit { .. } => {}
    }
    None
}

fn reject_queue_command<T: Clone + 'static>(
    opts: &RuntimeOptions,
    command: &WorkQueueCommand<T>,
    code: &str,
    message: &str,
    now_ms: u64,
) -> Vec<QueueEvent<T>> {
    reject_queue_command_by_id(
        opts,
        Some(command.command_id().to_owned()),
        code,
        message,
        now_ms,
    )
}

fn reject_queue_command_by_id<T>(
    opts: &RuntimeOptions,
    command_id: Option<String>,
    code: &str,
    message: &str,
    now_ms: u64,
) -> Vec<QueueEvent<T>> {
    vec![
        QueueEvent::Issue(DataIssue {
            kind: "issue".to_owned(),
            code: code.to_owned(),
            message: message.to_owned(),
            severity: "error".to_owned(),
            source: "workQueue".to_owned(),
            topic: None,
            details: Some(format!("queueId={}", opts.queue_id)),
        }),
        status_event(
            opts,
            WorkQueueStatusKind::CommandRejected,
            now_ms,
            StatusFields {
                command_id,
                issue_code: Some(code.to_owned()),
                ..StatusFields::default()
            },
        ),
    ]
}

fn checked_timestamp<T>(
    opts: &RuntimeOptions,
    command_id: Option<String>,
    now_ms: u64,
    delta_ms: u64,
    message: &str,
) -> Result<u64, Vec<QueueEvent<T>>> {
    now_ms.checked_add(delta_ms).ok_or_else(|| {
        reject_queue_command_by_id(opts, command_id, "clock-overflow", message, now_ms)
    })
}

#[derive(Default)]
struct StatusFields {
    work_id: Option<String>,
    command_id: Option<String>,
    record_seq: Option<u64>,
    as_of_record_seq: Option<u64>,
    issue_code: Option<String>,
    details: Option<String>,
}

fn status_event<T>(
    opts: &RuntimeOptions,
    kind: WorkQueueStatusKind,
    timestamp_ms: u64,
    fields: StatusFields,
) -> QueueEvent<T> {
    QueueEvent::Status(WorkQueueStatus {
        kind,
        queue_id: opts.queue_id.clone(),
        work_id: fields.work_id,
        command_id: fields.command_id,
        record_seq: fields.record_seq,
        as_of_record_seq: fields.as_of_record_seq,
        issue_code: fields.issue_code,
        timestamp_ms,
        details: fields.details,
    })
}

fn append_record<T: Clone>(
    state: &mut RuntimeState<T>,
    record: WorkQueueRecord<T>,
) -> WorkQueueRecord<T> {
    state.record_seq += 1;
    let record = with_record_seq(record, state.record_seq);
    state.records.push(record.clone());
    record
}

fn with_record_seq<T>(record: WorkQueueRecord<T>, next: u64) -> WorkQueueRecord<T> {
    match record {
        WorkQueueRecord::WorkAdmitted {
            queue_id,
            work_id,
            payload,
            message_bus,
            priority,
            tags,
            requirements,
            not_before_ms,
            deadline_ms,
            recorded_at_ms,
            ..
        } => WorkQueueRecord::WorkAdmitted {
            record_seq: next,
            queue_id,
            work_id,
            payload,
            message_bus,
            priority,
            tags,
            requirements,
            not_before_ms,
            deadline_ms,
            recorded_at_ms,
        },
        WorkQueueRecord::AdmissionDeduped {
            queue_id,
            work_id,
            message_bus,
            reason,
            existing_work_id,
            recorded_at_ms,
            ..
        } => WorkQueueRecord::AdmissionDeduped {
            record_seq: next,
            queue_id,
            work_id,
            message_bus,
            reason,
            existing_work_id,
            recorded_at_ms,
        },
        WorkQueueRecord::WorkScheduled {
            queue_id,
            work_id,
            command_id,
            schedule_id,
            not_before_ms,
            deadline_ms,
            reason,
            recorded_at_ms,
            ..
        } => WorkQueueRecord::WorkScheduled {
            record_seq: next,
            queue_id,
            work_id,
            command_id,
            schedule_id,
            not_before_ms,
            deadline_ms,
            reason,
            recorded_at_ms,
        },
        WorkQueueRecord::WorkClaimed {
            queue_id,
            work_id,
            command_id,
            lease_id,
            attempt,
            worker_id,
            claimed_at_ms,
            lease_expires_at_ms,
            ..
        } => WorkQueueRecord::WorkClaimed {
            record_seq: next,
            queue_id,
            work_id,
            command_id,
            lease_id,
            attempt,
            worker_id,
            claimed_at_ms,
            lease_expires_at_ms,
        },
        WorkQueueRecord::LeaseRenewed {
            queue_id,
            work_id,
            command_id,
            lease_id,
            attempt,
            worker_id,
            previous_lease_expires_at_ms,
            lease_expires_at_ms,
            renewed_at_ms,
            ..
        } => WorkQueueRecord::LeaseRenewed {
            record_seq: next,
            queue_id,
            work_id,
            command_id,
            lease_id,
            attempt,
            worker_id,
            previous_lease_expires_at_ms,
            lease_expires_at_ms,
            renewed_at_ms,
        },
        WorkQueueRecord::WorkReleased {
            queue_id,
            work_id,
            command_id,
            lease_id,
            attempt,
            worker_id,
            released_at_ms,
            reason,
            ..
        } => WorkQueueRecord::WorkReleased {
            record_seq: next,
            queue_id,
            work_id,
            command_id,
            lease_id,
            attempt,
            worker_id,
            released_at_ms,
            reason,
        },
        WorkQueueRecord::LeaseExpired {
            queue_id,
            work_id,
            command_id,
            lease_id,
            attempt,
            worker_id,
            lease_expires_at_ms,
            expired_at_ms,
            ..
        } => WorkQueueRecord::LeaseExpired {
            record_seq: next,
            queue_id,
            work_id,
            command_id,
            lease_id,
            attempt,
            worker_id,
            lease_expires_at_ms,
            expired_at_ms,
        },
        WorkQueueRecord::AttemptCompleted {
            queue_id,
            work_id,
            command_id,
            lease_id,
            attempt,
            worker_id,
            result,
            recorded_at_ms,
            ..
        } => WorkQueueRecord::AttemptCompleted {
            record_seq: next,
            queue_id,
            work_id,
            command_id,
            lease_id,
            attempt,
            worker_id,
            result,
            recorded_at_ms,
        },
        WorkQueueRecord::WorkCompleted {
            queue_id,
            work_id,
            command_id,
            lease_id,
            attempt,
            worker_id,
            result,
            recorded_at_ms,
            ..
        } => WorkQueueRecord::WorkCompleted {
            record_seq: next,
            queue_id,
            work_id,
            command_id,
            lease_id,
            attempt,
            worker_id,
            result,
            recorded_at_ms,
        },
        WorkQueueRecord::AttemptFailed {
            queue_id,
            work_id,
            command_id,
            lease_id,
            attempt,
            worker_id,
            error,
            retryable,
            recorded_at_ms,
            ..
        } => WorkQueueRecord::AttemptFailed {
            record_seq: next,
            queue_id,
            work_id,
            command_id,
            lease_id,
            attempt,
            worker_id,
            error,
            retryable,
            recorded_at_ms,
        },
        WorkQueueRecord::RetryScheduled {
            queue_id,
            work_id,
            command_id,
            retry_at_ms,
            delay_ms,
            reason,
            recorded_at_ms,
            ..
        } => WorkQueueRecord::RetryScheduled {
            record_seq: next,
            queue_id,
            work_id,
            command_id,
            retry_at_ms,
            delay_ms,
            reason,
            recorded_at_ms,
        },
        WorkQueueRecord::WorkDeadLettered {
            queue_id,
            work_id,
            command_id,
            reason,
            exhausted_attempts,
            recorded_at_ms,
            ..
        } => WorkQueueRecord::WorkDeadLettered {
            record_seq: next,
            queue_id,
            work_id,
            command_id,
            reason,
            exhausted_attempts,
            recorded_at_ms,
        },
        WorkQueueRecord::WorkCanceled {
            queue_id,
            work_id,
            command_id,
            reason,
            canceled_at_ms,
            canceled_lease_id,
            attempt,
            ..
        } => WorkQueueRecord::WorkCanceled {
            record_seq: next,
            queue_id,
            work_id,
            command_id,
            reason,
            canceled_at_ms,
            canceled_lease_id,
            attempt,
        },
    }
}

fn available_page<T: Clone>(
    state: &RuntimeState<T>,
    params: &WorkQueueAvailableParams,
) -> WorkQueueAvailablePage<T> {
    let limit = positive_limit(params.limit.unwrap_or(100));
    let mut items = state
        .works
        .values()
        .filter(|work| is_ready_for_projection(work, params.now_ms))
        .filter(
            |work| match (params.after_admission_seq, params.after_work_id.as_ref()) {
                (Some(after_seq), Some(after_id)) => {
                    (work.admission_seq, &work.work_id) > (after_seq, after_id)
                }
                (Some(after_seq), None) => work.admission_seq > after_seq,
                (None, Some(after_id)) => &work.work_id > after_id,
                (None, None) => true,
            },
        )
        .map(|work| WorkQueueAvailableItem {
            work_id: work.work_id.clone(),
            state: work.state,
            payload: work.payload.clone(),
            admission_seq: work.admission_seq,
            priority: work.priority,
            tags: work.tags.clone(),
            requirements: work.requirements.clone(),
            not_before_ms: work.not_before_ms,
            retry_at_ms: work.retry_at_ms,
            deadline_ms: work.deadline_ms,
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| {
        a.admission_seq
            .cmp(&b.admission_seq)
            .then_with(|| a.work_id.cmp(&b.work_id))
    });
    let has_more = items.len() > limit;
    let page = items.into_iter().take(limit).collect::<Vec<_>>();
    let next_after_work_id = has_more
        .then(|| page.last().map(|item| item.work_id.clone()))
        .flatten();
    let next_after_admission_seq = has_more
        .then(|| page.last().map(|item| item.admission_seq))
        .flatten();
    WorkQueueAvailablePage {
        items: page,
        next_after_work_id,
        next_after_admission_seq,
        has_more,
        as_of_record_seq: state.record_seq,
    }
}

fn work_snapshot<T: Clone>(state: &RuntimeState<T>, work_id: &str) -> WorkQueueWorkSnapshot<T> {
    let work = state.works.get(work_id);
    WorkQueueWorkSnapshot {
        work_id: work_id.to_owned(),
        state: work.map(|work| work.state),
        payload: work.map(|work| work.payload.clone()),
        active_lease: work.and_then(|work| {
            if work.state != WorkQueueDerivedState::Leased {
                return None;
            }
            Some(WorkQueueActiveLease {
                lease_id: work.lease_id.clone()?,
                attempt: work.attempt,
                worker_id: work.worker_id.clone()?,
                lease_expires_at_ms: work.lease_expires_at_ms?,
            })
        }),
        records: state
            .records
            .iter()
            .filter(|record| record.work_id() == work_id)
            .cloned()
            .collect(),
        as_of_record_seq: state.record_seq,
    }
}

fn dead_letter_page<T: Clone>(
    state: &RuntimeState<T>,
    params: &WorkQueueDeadLetterParams,
) -> WorkQueueDeadLetterPage<T> {
    let limit = positive_limit(params.limit.unwrap_or(100));
    let entries = state
        .dead_letters
        .iter()
        .filter(|record| {
            params
                .after_dead_letter_seq
                .is_none_or(|after| record.record_seq() > after)
                && params
                    .after_work_id
                    .as_ref()
                    .is_none_or(|after| record.work_id() > after.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();
    let has_more = entries.len() > limit;
    let page = entries.into_iter().take(limit).collect::<Vec<_>>();
    let next_after_dead_letter_seq = has_more
        .then(|| page.last().map(WorkQueueRecord::record_seq))
        .flatten();
    WorkQueueDeadLetterPage {
        entries: page,
        next_after_dead_letter_seq,
        has_more,
        as_of_record_seq: state.record_seq,
    }
}

fn clear_lease<T>(work: &mut WorkState<T>) {
    work.lease_id = None;
    work.worker_id = None;
    work.lease_expires_at_ms = None;
}

fn is_ready<T>(work: &WorkState<T>, now_ms: u64) -> bool {
    match work.state {
        WorkQueueDerivedState::Scheduled => work.not_before_ms.is_some_and(|t| t <= now_ms),
        WorkQueueDerivedState::RetryWait => work.retry_at_ms.is_some_and(|t| t <= now_ms),
        WorkQueueDerivedState::Ready => true,
        _ => false,
    }
}

fn is_ready_for_projection<T>(work: &WorkState<T>, now_ms: Option<u64>) -> bool {
    match work.state {
        WorkQueueDerivedState::Scheduled | WorkQueueDerivedState::RetryWait => {
            now_ms.is_some_and(|now| is_ready(work, now))
        }
        WorkQueueDerivedState::Ready => true,
        _ => false,
    }
}

fn is_expired<T>(work: &WorkState<T>, now_ms: u64) -> bool {
    work.state == WorkQueueDerivedState::Leased
        && work
            .lease_expires_at_ms
            .is_some_and(|expires| expires <= now_ms)
}

fn is_terminal(state: WorkQueueDerivedState) -> bool {
    matches!(
        state,
        WorkQueueDerivedState::Completed
            | WorkQueueDerivedState::Canceled
            | WorkQueueDerivedState::DeadLettered
    )
}

fn record_queue_id<T>(record: &WorkQueueRecord<T>) -> &str {
    match record {
        WorkQueueRecord::WorkAdmitted { queue_id, .. }
        | WorkQueueRecord::AdmissionDeduped { queue_id, .. }
        | WorkQueueRecord::WorkScheduled { queue_id, .. }
        | WorkQueueRecord::WorkClaimed { queue_id, .. }
        | WorkQueueRecord::LeaseRenewed { queue_id, .. }
        | WorkQueueRecord::WorkReleased { queue_id, .. }
        | WorkQueueRecord::LeaseExpired { queue_id, .. }
        | WorkQueueRecord::AttemptCompleted { queue_id, .. }
        | WorkQueueRecord::WorkCompleted { queue_id, .. }
        | WorkQueueRecord::AttemptFailed { queue_id, .. }
        | WorkQueueRecord::RetryScheduled { queue_id, .. }
        | WorkQueueRecord::WorkDeadLettered { queue_id, .. }
        | WorkQueueRecord::WorkCanceled { queue_id, .. } => queue_id,
    }
}

fn pull_params<T: Clone + Default + 'static>(ctx: &Ctx) -> T {
    ctx.pull()
        .and_then(|pull| pull.params::<T>())
        .map(|params| (*params).clone())
        .unwrap_or_default()
}

fn timestamp_or_zero(now: &Rc<dyn Fn() -> u64>) -> u64 {
    catch_unwind(AssertUnwindSafe(|| now())).unwrap_or(0)
}

fn positive_limit(limit: usize) -> usize {
    assert!(limit > 0, "workQueue: limit must be positive");
    limit
}

fn assert_non_empty(value: &str, owner: &str) {
    assert!(!value.is_empty(), "{owner}: must be a non-empty string");
}

fn node_opts(name: impl Into<String>, factory: impl Into<String>) -> GraphNodeOpts {
    let mut opts = GraphNodeOpts::named(name);
    opts.node = NodeOpts {
        factory: Some(factory.into()),
        complete_when_deps_complete: false,
        error_when_deps_error: false,
        ..opts.node
    };
    opts
}

fn pull_node_opts(
    name: impl Into<String>,
    factory: impl Into<String>,
    pull_id: LockId,
) -> GraphNodeOpts {
    let mut opts = node_opts(name, factory);
    opts.node.pull_id = Some(pull_id);
    opts.node.partial = true;
    opts
}

fn _assert_no_process_or_worker_deps(_: Option<Core>) {}
