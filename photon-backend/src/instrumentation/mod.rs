//! Backend instrumentation (L0 publish telemetry + ops log helpers).

mod backend;
mod events;
mod labels;
mod metrics;

pub use backend::{wrap_backend, InstrumentedPhotonBackend};
pub use events::{dlq_fields, log_ops, ops_log_fields, PhotonDlqRow};
pub use labels::FailureReason;
pub use metrics::{
    record_drain, record_handler_failure, record_publish, record_publish_error,
    record_retention_reclaim, record_retention_safe_seq,
};
