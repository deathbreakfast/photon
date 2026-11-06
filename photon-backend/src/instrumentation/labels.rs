//! Stable label values for Photon self-metrics.

/// Failure reason for [`super::metrics::record_handler_failure`] and DLQ rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureReason {
    /// Identity reconstruction failed before the handler ran.
    IdentityBuild,
    /// Handler returned an error.
    HandlerError,
    /// Checkpoint commit failed after successful delivery.
    CheckpointError,
    /// Backend yielded a bad stream item (decode/decrypt/ack) instead of an event.
    StreamItem,
}

impl FailureReason {
    /// Stable metric / ops-log label for this reason.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IdentityBuild => "identity_build",
            Self::HandlerError => "handler_error",
            Self::CheckpointError => "checkpoint_error",
            Self::StreamItem => "stream_item",
        }
    }
}
