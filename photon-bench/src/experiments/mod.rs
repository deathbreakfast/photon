//! Experiment registry and resolution.

mod registry;
mod resolve;
mod specs;

pub use registry::{REGISTRY, status_label};
pub use resolve::{ExperimentPlan, resolve_experiment, requires_shared_store};
