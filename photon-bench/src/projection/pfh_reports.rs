//! Shared `BM-PFH` JSON report loading for projection tools.

use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::Value;

/// Load all `BM-PFH` JSON reports matching hardware and storage from a directory.
pub fn read_pfh_reports(
    reports_dir: &Path,
    hardware: &str,
    storage: &str,
) -> Result<Vec<(PathBuf, Value)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(reports_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        let v: Value = serde_json::from_str(&text)?;
        if v.get("experiment").and_then(|e| e.as_str()) != Some("bm-pfh") {
            continue;
        }
        if v.get("hardware").and_then(|h| h.as_str()) != Some(hardware) {
            continue;
        }
        if v.get("storage").and_then(|s| s.as_str()) != Some(storage) {
            continue;
        }
        out.push((path, v));
    }
    Ok(out)
}
