//! Dead-letter metadata (no payload) for failed handler delivery.

use chrono::Utc;

use crate::error::{PhotonError, Result};
use crate::instrumentation::{dlq_fields, record_handler_failure, FailureReason};
use photon_telemetry::ops_log;

/// Metadata-only DLQ record shape.
#[derive(Debug, Clone)]
pub struct DlqRecord {
    /// Failed event id.
    pub event_id: String,
    /// Topic the event belonged to.
    pub topic_name: String,
    /// Optional partition key.
    pub topic_key: Option<String>,
    /// Event sequence number.
    pub seq: i64,
    /// Durable subscription name when known.
    pub subscription_name: Option<String>,
    /// Truncated error message.
    pub error: String,
    /// Delivery attempt count at failure.
    pub attempt: u32,
    /// When the DLQ row was recorded.
    pub recorded_at: chrono::DateTime<Utc>,
}

/// Parameters for [`DlqSink::record`].
pub struct DlqRecordParams<'a> {
    /// Failed event id.
    pub event_id: &'a str,
    /// Topic the event belonged to.
    pub topic_name: &'a str,
    /// Optional partition key.
    pub topic_key: Option<&'a str>,
    /// Event sequence number.
    pub seq: i64,
    /// Durable subscription name when known.
    pub subscription_name: Option<&'a str>,
    /// Failure classification for metrics and ops log.
    pub reason: FailureReason,
    /// Error message (truncated on record).
    pub error: String,
}

/// In-memory DLQ sink until a persistent schema is wired by the host.
#[derive(Default)]
pub struct DlqSink {
    records: std::sync::Mutex<Vec<DlqRecord>>,
}

impl DlqSink {
    /// Create an empty in-memory DLQ sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a failed delivery and emit DLQ telemetry.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn record(&self, params: &DlqRecordParams<'_>) -> Result<()> {
        record_handler_failure(params.topic_name, params.reason);
        {
            let mut guard = self
                .records
                .lock()
                .map_err(|_| PhotonError::Internal("dlq lock poisoned".into()))?;
            guard.push(DlqRecord {
                event_id: params.event_id.to_string(),
                topic_name: params.topic_name.to_string(),
                topic_key: params.topic_key.map(String::from),
                seq: params.seq,
                subscription_name: params.subscription_name.map(String::from),
                error: params.error.clone(),
                attempt: 1,
                recorded_at: Utc::now(),
            });
        }
        ops_log().log_event(
            "photon_dlq",
            &dlq_fields(
                params.event_id,
                params.topic_name,
                params.topic_key,
                params.seq,
                params.subscription_name,
                params.reason,
                &params.error,
            ),
        );
        Ok(())
    }

    /// Number of recorded DLQ rows.
    pub fn len(&self) -> usize {
        self.records.lock().map_or(0, |g| g.len())
    }

    /// Whether no DLQ rows have been recorded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Minimum seq among DLQ records for a transport partition (retention pin).
    pub fn min_seq_for(&self, topic: &str, topic_key: Option<&str>) -> Option<i64> {
        self.records
            .lock()
            .ok()
            .and_then(|guard| {
                guard
                    .iter()
                    .filter(|r| r.topic_name == topic && r.topic_key.as_deref() == topic_key)
                    .map(|r| r.seq)
                    .min()
            })
    }
}
