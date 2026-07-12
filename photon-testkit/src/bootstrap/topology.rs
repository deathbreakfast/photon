//! Topology-specific Photon runtime finishing (lab harness).

use photon_runtime::{configure, Photon};

#[cfg(test)]
use photon_runtime::default;

use crate::matrix::Topology;

/// Set `PHOTON_TOPOLOGY` for host parity and tests.
pub fn set_topology_env(topology: Topology) {
    std::env::set_var("PHOTON_TOPOLOGY", topology.env_value());
}

/// Apply topology-specific wiring after a successful Photon build.
pub fn finish_photon_for_topology(photon: Photon, topology: Topology) -> Photon {
    set_topology_env(topology);
    match topology {
        Topology::IsolatedLab | Topology::BrokerCluster => photon,
        Topology::EmbeddedComposite => {
            configure(photon.clone());
            photon
        }
        Topology::SplitRuntime => {
            // Headless worker: no process-wide default; yield models cold bootstrap.
            std::thread::yield_now();
            photon
        }
    }
}

/// Whether the process-wide default Photon is configured (embedded-composite only).
#[cfg(test)]
pub fn default_photon_configured() -> bool {
    default().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matrix::MatrixSpec;
    use crate::BootstrapSession;
    use serial_test::serial;

    fn build_with_topology(topology: Topology) -> Photon {
        let matrix = MatrixSpec::ci_mem_embedded().with_topology(topology);
        let mut session = BootstrapSession::new(matrix);
        session.install().expect("install");
        session.build_photon().expect("build")
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial(photon_process_env)]
    async fn topology_matrix_wiring() {
        let _ = build_with_topology(Topology::IsolatedLab);
        assert!(
            !default_photon_configured(),
            "isolated-lab must not configure process default"
        );
        assert_eq!(
            std::env::var("PHOTON_TOPOLOGY").ok().as_deref(),
            Some("isolated-lab")
        );

        let _ = build_with_topology(Topology::EmbeddedComposite);
        assert!(
            default_photon_configured(),
            "embedded-composite must configure process default"
        );
        assert_eq!(
            std::env::var("PHOTON_TOPOLOGY").ok().as_deref(),
            Some("embedded-composite")
        );

        let had_default = default_photon_configured();
        let _ = build_with_topology(Topology::SplitRuntime);
        assert_eq!(
            default_photon_configured(),
            had_default,
            "split-runtime must not change process default"
        );
        assert_eq!(
            std::env::var("PHOTON_TOPOLOGY").ok().as_deref(),
            Some("split-runtime")
        );
    }
}
