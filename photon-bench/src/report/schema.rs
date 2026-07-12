//! Bench report JSON schema for `photon-bench` runs.

use serde::Serialize;

use crate::harness::HardwareDetail;
use crate::harness::ResourceProfile;
use crate::stats::MetricStats;

#[derive(Debug, Serialize)]
pub struct BenchReport {
    pub experiment: String,
    pub matrix_slug: String,
    pub scenario_id: String,
    pub hardware: String,
    pub backend_id: String,
    pub topology: String,
    pub telemetry: String,
    pub storage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscriber_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_ms: Option<MetricStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_wait_ms: Option<MetricStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub achieved_ops_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backlog_peak: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replay_events_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_p50_delta_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slope_vs_index: Option<f64>,
    pub pass: bool,
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hardware_detail: Option<HardwareDetail>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_profile: Option<ResourceProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fleet_aggregate_ops_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<serde_json::Value>,
}

impl BenchReport {
    pub fn hardware_profile() -> String {
        std::env::var("PHOTON_BENCH_HARDWARE").unwrap_or_else(|_| "dev-wsl".into())
    }

    /// Shared matrix identity fields for ok and skipped reports.
    pub const fn matrix_shell(
        experiment: String,
        hardware: String,
        matrix_slug: String,
        backend_id: String,
        topology: String,
        telemetry: String,
        storage: String,
    ) -> Self {
        Self {
            experiment,
            matrix_slug,
            scenario_id: String::new(),
            hardware,
            backend_id,
            topology,
            telemetry,
            storage,
            subscriber_count: None,
            publish_ms: None,
            delivery_wait_ms: None,
            achieved_ops_per_sec: None,
            error_rate: None,
            backlog_peak: None,
            replay_events_per_sec: None,
            publish_p50_delta_ms: None,
            slope_vs_index: None,
            pass: false,
            status: "skipped_broker_pending",
            error: None,
            hardware_detail: None,
            resource_profile: None,
            node_count: None,
            fleet_aggregate_ops_per_sec: None,
            dimensions: None,
            diagnostics: None,
        }
    }
}
