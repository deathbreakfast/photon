//! Sustained load summary from scenario metrics.

#[derive(Debug, Clone, Copy, Default)]
pub struct LoadSummary {
    pub achieved_ops_per_sec: f64,
    pub error_rate: f64,
    pub backlog_peak: u64,
    pub replay_events_per_sec: Option<f64>,
}

impl LoadSummary {
    pub fn from_rate_run(
        target_rate: u32,
        duration_secs: u32,
        published: u32,
        publish_errors: u32,
        backlog_peak: u64,
    ) -> Self {
        let _ = target_rate;
        let elapsed = f64::from(duration_secs.max(1));
        Self {
            achieved_ops_per_sec: f64::from(published) / elapsed,
            error_rate: f64::from(publish_errors) / f64::from((published + publish_errors).max(1)),
            backlog_peak,
            replay_events_per_sec: None,
        }
    }
}
