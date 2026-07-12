//! Resolve optional Photon subsystem HTTP base for tooling / split deployments.
//!
//! 1. `PHOTON_REMOTE_BASE_URL`
//! 2. `SUBSYSTEM_GATEWAY_BASE_URL` + `SUBSYSTEM_CELL_SLUG` (default `home`) → `{base}/cell/{cell}/sub/photon`

/// Origin + path prefix before `/api/photon` (no trailing slash).
#[must_use]
pub fn resolve_photon_remote_base_url() -> Option<String> {
    if let Ok(u) = std::env::var("PHOTON_REMOTE_BASE_URL") {
        let t = u.trim();
        if !t.is_empty() {
            return Some(trim_slash(t));
        }
    }
    let base = std::env::var("SUBSYSTEM_GATEWAY_BASE_URL").ok()?;
    let base = base.trim();
    if base.is_empty() {
        return None;
    }
    let cell = std::env::var("SUBSYSTEM_CELL_SLUG").unwrap_or_else(|_| "home".to_string());
    let cell = cell.trim();
    if cell.is_empty() {
        return None;
    }
    Some(format!("{}/cell/{}/sub/photon", trim_slash(base), cell))
}

fn trim_slash(s: &str) -> String {
    s.trim_end_matches('/').to_string()
}
