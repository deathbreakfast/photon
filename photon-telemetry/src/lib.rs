//! Operations telemetry port for Photon.
//!
//! Hosts install a process-wide [`OpsLog`] via [`install_ops_log`] or
//! [`PhotonBuilder::ops_log`](https://docs.rs/photon/latest/photon/struct.PhotonBuilder.html#method.ops_log).
//! Backend instrumentation (publish counters, DLQ rows, checkpoint failures) calls [`ops_log`].
//!
//! ## Entry points
//!
//! - [`OpsLog`] — counter / gauge / event trait
//! - [`install_ops_log`] / [`ops_log`] / [`ops_log_from_env`] — process-wide adapter
//! - [`ConsoleOpsLog`] / [`NoOpsLog`] — shipped adapters

mod console;
mod global;
mod noop;

#[cfg(feature = "recording")]
mod recording;

pub use console::ConsoleOpsLog;
pub use global::{install_ops_log, ops_log, ops_log_from_env};
pub use noop::NoOpsLog;

#[cfg(feature = "recording")]
pub use recording::{RecordedCounter, RecordedEvent, RecordedGauge, RecordingOpsLog};

/// Structured ops metrics/events for publish, drain, DLQ, checkpoints.
pub trait OpsLog: Send + Sync {
    /// Increment a counter with optional labels.
    fn record_counter(&self, name: &str, labels: &[(&str, &str)], value: f64);

    /// Set a gauge with optional labels.
    fn record_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64);

    /// Emit a structured diagnostic event.
    fn log_event(&self, name: &str, payload: &serde_json::Value);
}

#[cfg(test)]
mod tests {
    use super::{ConsoleOpsLog, NoOpsLog, OpsLog};

    #[test]
    fn noop_ops_log_is_silent() {
        let log = NoOpsLog;
        log.record_counter("c", &[], 1.0);
        log.record_gauge("g", &[], 2.0);
        log.log_event("e", &serde_json::json!({}));
    }

    #[test]
    fn console_ops_log_does_not_panic() {
        let log = ConsoleOpsLog;
        log.record_counter("photon_publishes", &[("topic", "t")], 1.0);
    }
}
