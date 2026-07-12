//! Parse `PHOTON_NATS_RETENTION` for `JetStream` `max_age`.

use std::time::Duration;

/// Environment variable for `JetStream` stream retention.
pub const RETENTION_ENV: &str = "PHOTON_NATS_RETENTION";

const DEFAULT_RETENTION: Duration = Duration::from_mins(15);

/// Read retention duration from the environment, defaulting to 15 minutes.
#[must_use]
pub fn retention_from_env() -> Duration {
    std::env::var(RETENTION_ENV)
        .ok()
        .and_then(|s| parse_retention(&s))
        .unwrap_or(DEFAULT_RETENTION)
}

/// Parse duration strings such as `15m`, `1h`, `900s`.
#[must_use]
pub fn parse_retention(s: &str) -> Option<Duration> {
    let s = s.trim();
    if let Some(num) = s.strip_suffix('m') {
        return num
            .parse::<u64>()
            .ok()
            .map(|minutes| Duration::from_secs(minutes * 60));
    }
    if let Some(num) = s.strip_suffix('h') {
        return num
            .parse::<u64>()
            .ok()
            .map(|hours| Duration::from_secs(hours * 3600));
    }
    if let Some(num) = s.strip_suffix('s') {
        return num.parse::<u64>().ok().map(Duration::from_secs);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_retention_suffixes() {
        assert_eq!(parse_retention("15m"), Some(Duration::from_mins(15)));
        assert_eq!(parse_retention("1h"), Some(Duration::from_hours(1)));
        assert_eq!(parse_retention("30s"), Some(Duration::from_secs(30)));
        assert!(parse_retention("nope").is_none());
    }
}
