//! Integrated transport retention / reclaim workflow.
//!
//! Computes safe per-partition watermarks from durable checkpoints, coalescer pending
//! state, tailer read positions, optional DLQ pins, and optional TTL — then truncates
//! the encrypted transport log via [`crate::storage::StoragePort::truncate_before`].
//!
//! # Limitations
//!
//! - Ephemeral-only subscribers are not pinned; use durable checkpoints or conservative TTL.
//! - `get_event` returns `None` for truncated events (mitigated by index prune).
//! - DLQ metadata survives but payloads are gone after truncate (no replay in v0.1.x).
//! - Run one reclaimer per shared storage destination in distributed deployments.
//!
//! See also: [`crate::checkpoint`], [`crate::storage`], [`crate::delivery`].

mod config;
mod hook;
mod partition;
mod reclaimer;
mod watermark;

pub use config::{retention_max_age_secs, retention_pin_dlq, retention_sweep_ms, RetentionPolicy};
pub use hook::{PartitionReclaim, RetentionHook};
pub use partition::{SubscriptionPartition, TopicPartition};
pub use reclaimer::{ReclaimReport, RetentionDeps, RetentionReclaimer};
pub use watermark::{
    merge_truncate_bound, min_seq_pins, truncate_before_arg, truncate_bound_from_checkpoint,
    WatermarkContext,
};
