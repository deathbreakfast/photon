//! `JetStream` replica count from `PHOTON_NATS_REPLICAS`.

/// Environment variable for stream replica count.
pub const REPLICAS_ENV: &str = "PHOTON_NATS_REPLICAS";

/// Read replica count from env (default 1).
#[must_use]
pub fn replicas_from_env() -> usize {
    std::env::var(REPLICAS_ENV)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
        .clamp(1, 5)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_one() {
        std::env::remove_var(REPLICAS_ENV);
        assert_eq!(replicas_from_env(), 1);
    }
}
