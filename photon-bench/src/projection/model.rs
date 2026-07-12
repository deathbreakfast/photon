//! Fleet projection model for 1B/s decomposition.

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct FleetProjection {
    pub hardware: String,
    pub storage: String,
    pub r_shard_ops_per_sec: Option<f64>,
    pub delta_publish_p50_ms: Option<f64>,
    pub fanout_multiplier_at_16: Option<f64>,
    pub partitions_for_1e9: Option<u64>,
    pub photon_nodes_estimate: Option<u64>,
    /// `NATS` `JetStream` broker nodes for 1B/s ingress (from `BM-PFH` scaling curve).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_nodes_for_1e9: Option<u64>,
    /// `NATS` `JetStream` broker nodes for 1M/s ingress (from `BM-PFH` scaling curve).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_nodes_for_1m: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nats_bottleneck_verdict: Option<String>,
    pub bottleneck_ranking: Vec<String>,
    pub disclaimer: String,
}

pub fn compute(inputs: &super::inputs::ProjectionInputs) -> FleetProjection {
    let r_shard = inputs.pl2_rate.or(inputs.pl1_rate).or(inputs.p0_rate);
    let partitions = r_shard.and_then(|r| {
        if r > 0.0 {
            let n = (1_000_000_000_f64 / r).ceil() as u64;
            Some(n)
        } else {
            None
        }
    });

    let mut bottlenecks = Vec::new();
    if inputs.backlog_peak.unwrap_or(0) > 512 {
        bottlenecks.push("append-buffer".into());
    }
    if inputs.error_rate.unwrap_or(0.0) > 0.001 {
        bottlenecks.push("executor".into());
    }
    if inputs.p6_delivery_p99_ms.is_some() {
        bottlenecks.push("cross-node".into());
    }
    if bottlenecks.is_empty() {
        bottlenecks.push("store".into());
    }

    FleetProjection {
        hardware: inputs.hardware.clone(),
        storage: inputs.storage.clone(),
        r_shard_ops_per_sec: r_shard,
        delta_publish_p50_ms: None,
        fanout_multiplier_at_16: inputs.p2_p95_ratio,
        partitions_for_1e9: partitions,
        photon_nodes_estimate: partitions.map(|p| (p / 1000).max(1)),
        broker_nodes_for_1e9: None,
        broker_nodes_for_1m: None,
        nats_bottleneck_verdict: None,
        bottleneck_ranking: bottlenecks,
        disclaimer: "Projection from measured shape; not a 1B/s demo.".into(),
    }
}
