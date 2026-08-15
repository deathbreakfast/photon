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
    /// Checkpointed consume throughput (BM-PD*). Distinct from publisher `achieved_ops_per_sec`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivered_ops_per_sec: Option<f64>,
    /// Per-message consume-and-ack latency (BM-PD*). Not BM-P1 `delivery_wait_ms`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consume_ack_ms: Option<MetricStats>,
    /// Successful `set_checkpoint` commits across all declared subscribers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acked_deliveries: Option<u32>,
    /// Per-subscriber checkpoint commit counts (fanout).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fanout_acked: Option<Vec<u32>>,
    /// Whether every fanout subscriber acked the same published set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fanout_equal: Option<bool>,
    /// Offered publish rate for a PD capacity cell.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offered_rate: Option<u32>,
    /// Highest passing offered rate from a PD2/PD3 sweep (last cell in the array).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highest_passing_offered_rate: Option<u32>,
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
            delivered_ops_per_sec: None,
            consume_ack_ms: None,
            acked_deliveries: None,
            fanout_acked: None,
            fanout_equal: None,
            offered_rate: None,
            highest_passing_offered_rate: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::MetricStats;

    #[test]
    fn pd_optional_fields_serialize_when_set() {
        let mut report = BenchReport::matrix_shell(
            "bm-pd1".into(),
            "dev-wsl".into(),
            "mem-isolated-lab-off-none".into(),
            "mem".into(),
            "isolated-lab".into(),
            "off".into(),
            "mem".into(),
        );
        report.delivered_ops_per_sec = Some(500.0);
        report.consume_ack_ms = Some(MetricStats::summarize(vec![1.0, 2.0, 3.0]));
        report.acked_deliveries = Some(40);
        report.fanout_acked = Some(vec![10, 10, 10, 10]);
        report.fanout_equal = Some(true);
        report.offered_rate = Some(500);
        report.highest_passing_offered_rate = Some(500);
        let v = serde_json::to_value(&report).expect("serialize");
        assert_eq!(v["delivered_ops_per_sec"], 500.0);
        assert!(v["consume_ack_ms"]["p50"].is_number());
        assert_eq!(v["acked_deliveries"], 40);
        assert_eq!(v["fanout_acked"].as_array().map(Vec::len), Some(4));
        assert_eq!(v["fanout_equal"], true);
        assert_eq!(v["offered_rate"], 500);
        assert_eq!(v["highest_passing_offered_rate"], 500);
    }

    #[test]
    fn pd_optional_fields_omitted_when_none() {
        let report = BenchReport::matrix_shell(
            "bm-p0".into(),
            "dev-wsl".into(),
            "mem-isolated-lab-off-none".into(),
            "mem".into(),
            "isolated-lab".into(),
            "off".into(),
            "mem".into(),
        );
        let v = serde_json::to_value(&report).expect("serialize");
        assert!(v.get("delivered_ops_per_sec").is_none());
        assert!(v.get("consume_ack_ms").is_none());
        assert!(v.get("acked_deliveries").is_none());
        assert!(v.get("fanout_acked").is_none());
        assert!(v.get("fanout_equal").is_none());
    }
}
