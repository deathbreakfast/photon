//! Single experiment run orchestration.

use std::path::PathBuf;

use anyhow::{Context, Result};
use photon_testkit::{BootstrapSession, RunMode, ScenarioRunner};

use crate::experiments::{resolve_experiment, ExperimentPlan};
use crate::harness::{self, ResourceSampler};
use crate::matrix::{matrix_from_cli, shared_store_skip_reason};
use crate::metrics::{evaluate_pass, publish_slope_vs_index, LoadSummary, PassContext};
use crate::report::BenchReport;
use crate::stats::MetricStats;

pub struct RunArgs {
    pub experiment: String,
    pub storage: String,
    pub telemetry: String,
    pub topology: Option<String>,
    pub ops: Option<u32>,
    pub warmup: u32,
    pub hardware: String,
    pub report: Option<PathBuf>,
    pub nodes: Option<u32>,
    pub publishers: Option<u32>,
    pub offered_rate: Option<u32>,
}

pub async fn run_experiment(args: RunArgs) -> Result<()> {
    std::env::set_var("PHOTON_BENCH_HARDWARE", &args.hardware);
    if let Some(publishers) = args.publishers {
        std::env::set_var("PHOTON_BENCH_PUBLISHERS", publishers.to_string());
    }
    if let Some(nodes) = args.nodes {
        std::env::set_var("PHOTON_BENCH_NODES", nodes.to_string());
    }
    if let Some(rate) = args.offered_rate {
        std::env::set_var("PHOTON_BENCH_OFFERED_RATE", rate.to_string());
    }
    let matrix = matrix_from_cli(&args.storage, &args.telemetry, args.topology.as_deref())?;

    if let Some(reason) = shared_store_skip_reason(&matrix, &args.experiment) {
        let out = skipped_report(&args, &matrix, &reason);
        return emit_report(&out, args.report.as_ref());
    }

    if let Some(reason) = pd_crypto_disabled_reason(
        &args.experiment,
        std::env::var("PHOTON_BENCH_CRYPTO").ok().as_deref(),
    ) {
        let out = fail_closed_report(&args, &matrix, &reason);
        return emit_report(&out, args.report.as_ref());
    }

    let plan = match resolve_experiment(&args.experiment, args.ops, &matrix) {
        Ok(p) => p,
        Err(e) => {
            let out = skipped_report(&args, &matrix, &e.to_string());
            return emit_report(&out, args.report.as_ref());
        }
    };

    if args.experiment.eq_ignore_ascii_case("bm-p9") {
        return run_p9_crypto_delta(args, matrix, plan).await;
    }

    if is_pd_capacity(&args.experiment) && args.offered_rate.is_none() {
        return run_pd_capacity_sweep(args, matrix).await;
    }

    execute_plan(args, matrix, plan).await
}

async fn execute_plan(
    args: RunArgs,
    matrix: photon_testkit::MatrixSpec,
    plan: ExperimentPlan,
) -> Result<()> {
    let mut out = measure_plan(&args, &matrix, &plan).await?;
    if is_pd_capacity(&args.experiment) {
        out.offered_rate = plan.target_rate;
        out.highest_passing_offered_rate = plan.target_rate.filter(|_| out.pass);
    }
    emit_report(&out, args.report.as_ref())
}

async fn measure_plan(
    args: &RunArgs,
    matrix: &photon_testkit::MatrixSpec,
    plan: &ExperimentPlan,
) -> Result<BenchReport> {
    let mut session = BootstrapSession::new(matrix.clone());
    session.install_async().await?;

    if args.warmup > 0 {
        let warm = photon_testkit::ScenarioSpec::publish_only(args.warmup);
        ScenarioRunner::new(&session)
            .run(&warm, RunMode::Benchmark)
            .await?;
    }

    let profile_enabled = harness::resource_profiling_enabled(&args.hardware);
    let sampler = profile_enabled.then(ResourceSampler::start);

    let result = ScenarioRunner::new(&session)
        .run(&plan.scenario, RunMode::Benchmark)
        .await?;

    let resource_profile = if let Some(s) = sampler {
        Some(s.finish().await)
    } else {
        None
    };

    Ok(build_report(args, matrix, plan, &result, resource_profile))
}

fn is_pd_capacity(id: &str) -> bool {
    matches!(id.to_ascii_lowercase().as_str(), "bm-pd2" | "bm-pd3")
}

fn pd_offered_rate_ladder() -> Vec<u32> {
    let raw = std::env::var("PHOTON_BENCH_OFFERED_RATE_SWEEP")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "50,100,250,500,1000".into());
    parse_offered_rate_ladder(&raw)
}

fn parse_offered_rate_ladder(raw: &str) -> Vec<u32> {
    raw.split(',')
        .filter_map(|s| s.trim().parse().ok())
        .filter(|&n| n > 0)
        .collect()
}

async fn run_pd_capacity_sweep(args: RunArgs, matrix: photon_testkit::MatrixSpec) -> Result<()> {
    let rates = pd_offered_rate_ladder();
    if rates.is_empty() {
        anyhow::bail!("PD capacity sweep needs a positive offered-rate ladder");
    }
    let mut last_ok: Option<BenchReport> = None;
    let mut last_ok_rate: Option<u32> = None;
    for rate in rates {
        std::env::set_var("PHOTON_BENCH_OFFERED_RATE", rate.to_string());
        let plan = resolve_experiment(&args.experiment, args.ops, &matrix)?;
        let mut report = measure_plan(&args, &matrix, &plan).await?;
        report.offered_rate = Some(rate);
        if report.pass && report.status == "ok" {
            last_ok_rate = Some(rate);
            last_ok = Some(report);
            continue;
        }
        if let Some(mut ok) = last_ok.take() {
            ok.highest_passing_offered_rate = last_ok_rate;
            return emit_report(&ok, args.report.as_ref());
        }
        report.highest_passing_offered_rate = None;
        return emit_report(&report, args.report.as_ref());
    }
    match last_ok {
        Some(mut ok) => {
            ok.highest_passing_offered_rate = last_ok_rate;
            emit_report(&ok, args.report.as_ref())
        }
        None => anyhow::bail!("PD capacity sweep produced no cells"),
    }
}

async fn run_p9_crypto_delta(
    args: RunArgs,
    matrix: photon_testkit::MatrixSpec,
    plan: ExperimentPlan,
) -> Result<()> {
    let mut session = BootstrapSession::new(matrix.clone());
    session.install_async().await?;

    std::env::set_var("PHOTON_BENCH_CRYPTO", "1");
    let crypto_on = ScenarioRunner::new(&session)
        .run(&plan.scenario, RunMode::Benchmark)
        .await?;
    std::env::set_var("PHOTON_BENCH_CRYPTO", "0");
    let crypto_off = ScenarioRunner::new(&session)
        .run(&plan.scenario, RunMode::Benchmark)
        .await?;

    let on_p50 = publish_p50(&crypto_on);
    let off_p50 = publish_p50(&crypto_off);
    let delta = on_p50.zip(off_p50).map(|(a, b)| a - b);

    let mut report = build_report(&args, &matrix, &plan, &crypto_on, None);
    report.publish_p50_delta_ms = delta;
    report.pass = delta.is_some_and(|d| d <= 15.0) && crypto_on.error.is_none();
    emit_report(&report, args.report.as_ref())
}

fn build_report(
    args: &RunArgs,
    matrix: &photon_testkit::MatrixSpec,
    plan: &ExperimentPlan,
    result: &photon_testkit::ScenarioResult,
    resource_profile: Option<harness::ResourceProfile>,
) -> BenchReport {
    let (publish_samples, delivery_samples, consume_ack_samples) = sample_vectors(result);
    let publish_ms =
        (!publish_samples.is_empty()).then(|| MetricStats::summarize(publish_samples.clone()));
    let delivery_wait_ms =
        (!delivery_samples.is_empty()).then(|| MetricStats::summarize(delivery_samples));
    let consume_ack_ms =
        (!consume_ack_samples.is_empty()).then(|| MetricStats::summarize(consume_ack_samples));

    let slope = publish_ms
        .as_ref()
        .map(|_| publish_slope_vs_index(&publish_samples));

    let load_summary = result.load.as_ref().map(|l| {
        let mut s = LoadSummary::from_rate_run(
            l.target_rate.or(plan.target_rate).unwrap_or(0),
            l.duration_secs.or(plan.duration_secs).unwrap_or(1),
            l.published,
            l.publish_errors,
            l.backlog_peak,
        );
        s.replay_events_per_sec = l.replay_events_per_sec;
        s
    });

    let status = if result.error.is_none() { "ok" } else { "fail" };

    let delivered_ops_per_sec = result
        .load
        .as_ref()
        .and_then(|l| l.delivered_ops_per_sec)
        .or_else(|| (result.acked_deliveries > 0).then_some(f64::from(result.acked_deliveries)));
    let acked_deliveries = (result.acked_deliveries > 0).then_some(result.acked_deliveries);
    let fanout_acked = (!result.fanout_acked.is_empty()).then(|| result.fanout_acked.clone());
    let fanout_equal = fanout_acked.as_ref().and_then(|counts| {
        (counts.len() > 1)
            .then(|| counts.windows(2).all(|w| w[0] == w[1]) && counts.iter().all(|n| *n > 0))
    });

    let ctx = PassContext {
        experiment: plan.id.clone(),
        publish_ms,
        delivery_wait_ms,
        slope,
        load: load_summary,
        target_rate: result
            .load
            .as_ref()
            .and_then(|l| l.target_rate)
            .or(plan.target_rate),
        subscriber_count: plan.subscriber_count,
        deliveries: Some(result.deliveries),
        publish_p50_delta_ms: None,
        run_error: result.error.clone(),
        status,
        acked_deliveries,
        consume_ack_ms,
        fanout_acked: fanout_acked.clone(),
        fanout_equal,
        delivered_ops_per_sec,
    };

    let hardware_detail = Some(harness::capture_hardware());
    let (topology, telemetry, storage) = matrix_json_fields(matrix);
    let dimensions = pfh_dimensions(&plan.id, storage.as_str());
    let diagnostics = pfh_diagnostics(storage.as_str());

    BenchReport {
        experiment: plan.id.clone(),
        matrix_slug: matrix.report_slug(),
        scenario_id: result.scenario_id.clone(),
        hardware: BenchReport::hardware_profile(),
        backend_id: matrix.storage.as_str().to_string(),
        topology,
        telemetry,
        storage,
        subscriber_count: plan.subscriber_count,
        publish_ms,
        delivery_wait_ms,
        achieved_ops_per_sec: load_summary.map(|l| l.achieved_ops_per_sec),
        error_rate: load_summary.map(|l| l.error_rate),
        backlog_peak: load_summary.map(|l| l.backlog_peak),
        replay_events_per_sec: load_summary.and_then(|l| l.replay_events_per_sec),
        publish_p50_delta_ms: None,
        slope_vs_index: slope.map(|s| s.slope),
        pass: evaluate_pass(&ctx),
        status,
        error: result.error.clone(),
        hardware_detail,
        resource_profile,
        node_count: args.nodes,
        fleet_aggregate_ops_per_sec: None,
        dimensions,
        diagnostics,
        delivered_ops_per_sec,
        consume_ack_ms,
        acked_deliveries,
        fanout_acked,
        fanout_equal,
        offered_rate: plan.target_rate,
        highest_passing_offered_rate: None,
    }
}

fn skipped_report(
    args: &RunArgs,
    matrix: &photon_testkit::MatrixSpec,
    reason: &str,
) -> BenchReport {
    let (topology, telemetry, storage) = matrix_json_fields(matrix);
    let mut report = BenchReport::matrix_shell(
        args.experiment.clone(),
        args.hardware.clone(),
        matrix.report_slug(),
        matrix.storage.as_str().to_string(),
        topology,
        telemetry,
        storage,
    );
    report.error = Some(reason.into());
    report
}

fn fail_closed_report(
    args: &RunArgs,
    matrix: &photon_testkit::MatrixSpec,
    reason: &str,
) -> BenchReport {
    let mut report = skipped_report(args, matrix, reason);
    report.status = "fail";
    report.pass = false;
    report
}

fn is_pd_experiment(id: &str) -> bool {
    matches!(
        id.to_ascii_lowercase().as_str(),
        "bm-pd0" | "bm-pd1" | "bm-pd2" | "bm-pd3"
    )
}

/// PD experiments require envelope crypto. `PHOTON_BENCH_CRYPTO=0` fails closed.
///
/// BM-PFH still disables crypto for ingress measurement; this gate is PD-only.
fn pd_crypto_disabled_reason(experiment: &str, crypto_env: Option<&str>) -> Option<String> {
    if !is_pd_experiment(experiment) {
        return None;
    }
    match crypto_env.map(str::trim) {
        Some("0" | "false" | "FALSE" | "no" | "NO") => Some(format!(
            "{experiment} requires envelope crypto; PHOTON_BENCH_CRYPTO=0 is rejected"
        )),
        _ => None,
    }
}

fn matrix_json_fields(matrix: &photon_testkit::MatrixSpec) -> (String, String, String) {
    (
        serde_json::to_string(&matrix.topology)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
        serde_json::to_string(&matrix.telemetry)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
        serde_json::to_string(&matrix.storage)
            .unwrap_or_default()
            .trim_matches('"')
            .to_string(),
    )
}

fn emit_report(out: &BenchReport, path: Option<&PathBuf>) -> Result<()> {
    let json = serde_json::to_string_pretty(out)?;
    if let Some(path) = path {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, &json).with_context(|| format!("write {}", path.display()))?;
        println!("wrote {}", path.display());
    } else {
        println!("{json}");
    }
    Ok(())
}

fn sample_vectors(result: &photon_testkit::ScenarioResult) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let mut publish_samples = Vec::new();
    let mut delivery_samples = Vec::new();
    let mut consume_ack_samples = result.consume_ack_samples_ms.clone();
    for timing in &result.step_timings {
        match timing.op.as_str() {
            "publish" => publish_samples.extend(timing.samples_ms.iter().copied()),
            "delivery_wait" => delivery_samples.extend(timing.samples_ms.iter().copied()),
            "consume_ack" if consume_ack_samples.is_empty() => {
                consume_ack_samples.extend(timing.samples_ms.iter().copied());
            }
            _ => {}
        }
    }
    (publish_samples, delivery_samples, consume_ack_samples)
}

fn publish_p50(result: &photon_testkit::ScenarioResult) -> Option<f64> {
    let (samples, _, _) = sample_vectors(result);
    if samples.is_empty() {
        None
    } else {
        Some(MetricStats::summarize(samples).p50)
    }
}

struct PfhBrokerEnv {
    topic_shards: &'static str,
    replay_cursor: &'static str,
    sync_ack: &'static str,
    max_inflight: &'static str,
}

fn pfh_broker_env(storage: &str) -> PfhBrokerEnv {
    match storage {
        "kafka" => PfhBrokerEnv {
            topic_shards: "PHOTON_KAFKA_TOPIC_SHARDS",
            replay_cursor: "PHOTON_KAFKA_REPLAY_CURSOR",
            sync_ack: "PHOTON_KAFKA_SYNC_ACK",
            max_inflight: "PHOTON_KAFKA_MAX_INFLIGHT",
        },
        "fluvio" => PfhBrokerEnv {
            topic_shards: "PHOTON_FLUVIO_TOPIC_SHARDS",
            replay_cursor: "PHOTON_FLUVIO_REPLAY_CURSOR",
            sync_ack: "PHOTON_FLUVIO_SYNC_ACK",
            max_inflight: "PHOTON_FLUVIO_MAX_INFLIGHT",
        },
        _ => PfhBrokerEnv {
            topic_shards: "PHOTON_NATS_STREAM_SHARDS",
            replay_cursor: "PHOTON_NATS_REPLAY_CURSOR",
            sync_ack: "PHOTON_NATS_SYNC_ACK",
            max_inflight: "PHOTON_NATS_MAX_INFLIGHT",
        },
    }
}

fn pfh_env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn pfh_dimensions(experiment: &str, storage: &str) -> Option<serde_json::Value> {
    if experiment != "bm-pfh" {
        return None;
    }
    let env = pfh_broker_env(storage);
    let topic_shards = pfh_env_var("PHOTON_BENCH_TOPIC_SHARDS")
        .or_else(|| pfh_env_var(env.topic_shards))
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    Some(serde_json::json!({
        "broker_nodes": std::env::var("PHOTON_BENCH_NODES").ok().and_then(|v| v.parse().ok()).unwrap_or(1),
        "stream_shards": topic_shards,
        "publishers": std::env::var("PHOTON_BENCH_PUBLISHERS").ok().and_then(|v| v.parse().ok()).unwrap_or(32),
        "replay_cursor": pfh_env_var(env.replay_cursor).unwrap_or_else(|| "stream_seq".into()),
        "sync_ack": pfh_env_var(env.sync_ack).unwrap_or_else(|| "1".into()),
        "max_inflight": pfh_env_var(env.max_inflight),
        "hardware": std::env::var("PHOTON_BENCH_HARDWARE").unwrap_or_else(|_| "dev-wsl".into()),
        "bench_client_index": std::env::var("PHOTON_BENCH_CLIENT_INDEX").ok().and_then(|v| v.parse::<u32>().ok()),
        "bench_client_count": std::env::var("PHOTON_BENCH_CLIENT_COUNT").ok().and_then(|v| v.parse::<u32>().ok()),
    }))
}

fn pfh_diagnostics(storage: &str) -> Option<serde_json::Value> {
    let peak_key = match storage {
        "kafka" => "PHOTON_BENCH_KAFKA_BENCH_PEAK",
        "fluvio" => "PHOTON_BENCH_FLUVIO_BENCH_PEAK",
        _ => "PHOTON_BENCH_NATS_BENCH_PEAK",
    };
    let peak_label = match storage {
        "kafka" => "kafka_bench_peak_ops",
        "fluvio" => "fluvio_bench_peak_ops",
        _ => "nats_bench_peak_ops",
    };
    let peak = std::env::var(peak_key)
        .ok()
        .and_then(|v| v.parse::<f64>().ok());
    let broker = std::env::var("PHOTON_BENCH_BROKER_RESOURCE_JSON").ok();
    let mut obj = serde_json::Map::new();
    if let Some(p) = peak {
        obj.insert(peak_label.into(), serde_json::json!(p));
    }
    if let Some(raw) = broker {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
            obj.insert("broker_resource".into(), v);
        }
    }
    if obj.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(obj))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pd_rejects_crypto_disabled() {
        let reason = pd_crypto_disabled_reason("bm-pd0", Some("0")).expect("reject");
        assert!(reason.contains("envelope crypto"));
        assert!(pd_crypto_disabled_reason("bm-pd1", Some("false")).is_some());
        assert!(pd_crypto_disabled_reason("bm-pd2", Some("0")).is_some());
        assert!(pd_crypto_disabled_reason("bm-pd3", Some("false")).is_some());
    }

    #[test]
    fn offered_rate_ladder_parses_positive_cells() {
        assert_eq!(
            parse_offered_rate_ladder("50,100,250,500,1000"),
            vec![50, 100, 250, 500, 1000]
        );
        assert_eq!(parse_offered_rate_ladder("0, ,-1,abc"), Vec::<u32>::new());
    }

    #[test]
    fn pfh_still_allows_crypto_disabled() {
        assert!(pd_crypto_disabled_reason("bm-pfh", Some("0")).is_none());
        assert!(pd_crypto_disabled_reason("bm-p1", Some("0")).is_none());
    }

    #[test]
    fn pd_allows_crypto_on_or_unset() {
        assert!(pd_crypto_disabled_reason("bm-pd0", None).is_none());
        assert!(pd_crypto_disabled_reason("bm-pd0", Some("1")).is_none());
    }
}
