//! Hardware introspection for report appendices.

mod capture;
mod resource;

pub use capture::HardwareDetail;
pub use resource::{resource_profiling_enabled, ResourceProfile, ResourceSampler};

pub fn capture_hardware() -> HardwareDetail {
    capture::capture()
}
