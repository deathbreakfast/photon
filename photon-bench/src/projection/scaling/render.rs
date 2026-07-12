//! Markdown rendering for scaling curve output.

use super::ScalingCurve;

pub fn render_scaling_markdown(curve: &ScalingCurve) -> String {
    let mut lines = vec![
        "# Photon NATS firehose scaling curve".into(),
        String::new(),
        format!("- hardware: `{}`", curve.hardware),
        format!("- storage: `{}`", curve.storage),
        format!("- workload: `{}`", curve.workload),
    ];
    if let Some(v) = &curve.bottleneck_verdict {
        lines.push(format!("- bottleneck: `{v}`"));
    }
    if let Some(shards) = curve.stream_shards {
        lines.push(format!("- stream_shards: `{shards}`"));
    }
    if curve.multibench_ladder {
        lines.push("- multibench_ladder: `true`".into());
    }
    if let Some(b) = curve.nats_bench_peak_n1 {
        lines.push(format!("- nats bench peak (N=1): {b:.0} ops/s"));
    }
    lines.push(String::new());
    if curve.multibench_ladder {
        lines.push("| bench_clients | broker_nodes | peak ops/s | vs bc=1 | config |".into());
        lines.push("| --- | --- | --- | --- | --- |".into());
        for p in &curve.points {
            let vs = p
                .vs_baseline
                .map_or_else(|| "—".into(), |v| format!("{v:.2}×"));
            lines.push(format!(
                "| {} | {} | {:.0} | {} | {} |",
                p.bench_client_count.unwrap_or(1),
                p.broker_nodes,
                p.peak_ops_per_sec,
                vs,
                p.config
            ));
        }
    } else {
        lines.push("| broker_nodes | peak ops/s | vs N=1 | config |".into());
        lines.push("| --- | --- | --- | --- |".into());
        for p in &curve.points {
            let vs = p
                .vs_baseline
                .map_or_else(|| "—".into(), |v| format!("{v:.2}×"));
            lines.push(format!(
                "| {} | {:.0} | {} | {} |",
                p.broker_nodes, p.peak_ops_per_sec, vs, p.config
            ));
        }
    }
    lines.push(String::new());
    lines.push("**broker_nodes_for_target:**".into());
    for (target, nodes) in &curve.broker_nodes_for_target {
        lines.push(format!("- {target} ops/s → {nodes} NATS broker nodes"));
    }
    lines.push(String::new());
    lines.push(curve.disclaimer.clone());
    lines.join("\n")
}
