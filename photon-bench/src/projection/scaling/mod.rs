//! `NATS` firehose scaling curve from `BM-PFH` sweep reports.

mod filter;
mod fit;
mod load;
mod render;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};

pub use load::load_scaling_curve;
pub use render::render_scaling_markdown;

/// One peak throughput point per broker node count (or per bench client count for multibench).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingPoint {
    pub broker_nodes: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bench_client_count: Option<u32>,
    pub peak_ops_per_sec: f64,
    pub config: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nats_bench_peak: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vs_baseline: Option<f64>,
    pub report_file: String,
}

/// Full scaling curve for `NATS` firehose workload.
#[derive(Debug, Serialize)]
pub struct ScalingCurve {
    pub hardware: String,
    pub storage: String,
    pub workload: String,
    pub baseline_broker_nodes: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_shards: Option<u32>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sharded_ladder: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub multibench_ladder: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bottleneck_verdict: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nats_bench_peak_n1: Option<f64>,
    pub points: Vec<ScalingPoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scaling_exponent: Option<f64>,
    pub broker_nodes_for_target: HashMap<String, u64>,
    pub disclaimer: String,
}

pub fn scaling_curve(
    hardware: &str,
    storage: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
    stream_shards: Option<u32>,
    match_broker_nodes: bool,
    multibench_ladder: bool,
    bench_client_count_filter: Option<u32>,
    primary_row: bool,
) -> Result<()> {
    let curve = load_scaling_curve(
        reports_dir,
        hardware,
        storage,
        stream_shards,
        match_broker_nodes,
        multibench_ladder,
        bench_client_count_filter,
        primary_row,
    )?;
    let suffix = if multibench_ladder {
        "-multibench".into()
    } else if match_broker_nodes {
        "-sharded".into()
    } else if stream_shards.is_some_and(|n| n > 1) {
        format!("-sharded-s{}", stream_shards.unwrap_or(1))
    } else {
        String::new()
    };
    let out_path = out.unwrap_or_else(|| {
        reports_dir.join(format!(
            "scaling-curve-{hardware}-{storage}-firehose{suffix}.json"
        ))
    });
    let json = serde_json::to_string_pretty(&curve)?;
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&out_path, &json)?;
    println!("wrote {}", out_path.display());
    println!("{}", render_scaling_markdown(&curve));
    Ok(())
}
