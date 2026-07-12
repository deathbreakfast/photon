//! [`Photon`] builder — storage port + backend assembly (**Integrating the host**).

use std::sync::Arc;

use photon_telemetry::{install_ops_log, OpsLog};

use photon_backend::{
    instrumentation, BackendContext, EmbeddedBackend, ExecutorServices, GenericPhotonBackend,
    InProcStoragePort, PhotonBackend, PhotonError, Result, RetentionHook, RetentionPolicy,
    StoragePort, TopicRegistry, TransportCrypto,
};

use crate::executor::ExecutorController;
use crate::{Photon, PhotonRuntimeState};

type BackendInstallFn =
    Box<dyn FnOnce(BackendContext) -> Result<Arc<dyn PhotonBackend>> + Send>;

/// Builder for constructing [`Photon`] runtimes — **Integrating the host**.
///
/// # Example
///
/// ```rust,no_run
/// use photon_runtime::Photon;
///
/// # fn main() -> photon_backend::Result<()> {
/// let _photon = Photon::builder().auto_registry().build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct PhotonBuilder {
    storage_port: Option<Arc<dyn StoragePort>>,
    backend: Option<Arc<dyn PhotonBackend>>,
    backend_install: Option<BackendInstallFn>,
    use_auto_registry: bool,
    ops_log: Option<Arc<dyn OpsLog>>,
    retention_policy: Option<RetentionPolicy>,
    retention_hook: Option<Arc<dyn RetentionHook>>,
}

impl PhotonBuilder {
    /// Explicit storage port (defaults to in-process `mem` tier).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    ///
    /// use photon_backend::{InProcStoragePort, StoragePort, TransportCrypto};
    /// use photon_runtime::Photon;
    ///
    /// # fn main() -> photon_backend::Result<()> {
    /// let port: Arc<dyn StoragePort> = Arc::new(InProcStoragePort::new(
    ///     TransportCrypto::from_env()?,
    /// ));
    /// let _photon = Photon::builder().storage_port(port).auto_registry().build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn storage_port(mut self, port: Arc<dyn StoragePort>) -> Self {
        self.storage_port = Some(port);
        self
    }

    /// Pre-built backend instance.
    #[must_use]
    pub fn backend(mut self, backend: Arc<dyn PhotonBackend>) -> Self {
        self.backend = Some(backend);
        self.backend_install = None;
        self
    }

    /// Build backend from shared [`BackendContext`] (typical for custom install fns).
    #[must_use]
    pub fn backend_with_context<F>(mut self, install: F) -> Self
    where
        F: FnOnce(BackendContext) -> Result<Arc<dyn PhotonBackend>> + Send + 'static,
    {
        self.backend = None;
        self.backend_install = Some(Box::new(install));
        self
    }

    /// Shorthand for [`Self::backend_with_context`](GenericPhotonBackend::install_mem).
    #[must_use]
    pub fn mem_backend(mut self) -> Self {
        self.backend = None;
        self.backend_install = Some(Box::new(EmbeddedBackend::install_mem));
        self
    }

    /// Install a concrete [`OpsLog`] adapter before build.
    #[must_use]
    pub fn ops_log(mut self, log: impl OpsLog + 'static) -> Self {
        self.ops_log = Some(Arc::new(log));
        self
    }

    /// Install a shared [`OpsLog`] trait object before build.
    #[must_use]
    pub fn ops_log_arc(mut self, log: Arc<dyn OpsLog>) -> Self {
        self.ops_log = Some(log);
        self
    }

    /// Discover `#[photon::topic]` descriptors via Quark inventory instead of an empty registry.
    ///
    /// Required when using `#[topic]` / `#[subscribe]` in the same crate graph as the host.
    /// Runnable: `cargo run -p uf-photon --example embedded_mem --features runtime,mem`.
    #[must_use]
    pub const fn auto_registry(mut self) -> Self {
        self.use_auto_registry = true;
        self
    }

    /// Override default retention policy (env fallbacks apply for unset fields).
    #[must_use]
    pub fn retention_policy(mut self, policy: RetentionPolicy) -> Self {
        self.retention_policy = Some(policy);
        self
    }

    /// Host hook for extra subscriptions and legal-hold floors.
    #[must_use]
    pub fn retention_hook(mut self, hook: Arc<dyn RetentionHook>) -> Self {
        self.retention_hook = Some(hook);
        self
    }

    /// Assemble the [`Photon`] runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn build(self) -> Result<Photon> {
        if let Some(log) = self.ops_log {
            install_ops_log(log);
        }

        let registry = if self.use_auto_registry {
            TopicRegistry::auto_discover()
        } else {
            TopicRegistry::new()
        };

        let port = match self.storage_port {
            Some(port) => port,
            None => Arc::new(InProcStoragePort::new(TransportCrypto::from_env()?)),
        };

        let ctx = BackendContext {
            registry: registry.clone(),
        };

        let backend = match (self.backend, self.backend_install) {
            (Some(b), None) => b,
            (None, Some(install)) => install(ctx)?,
            (None, None) => GenericPhotonBackend::install_with_port(
                BackendContext { registry },
                Arc::clone(&port),
            )?,
            (Some(_), Some(_)) => {
                return Err(PhotonError::Internal(
                    "PhotonBuilder: set backend() or backend_with_context(), not both".into(),
                ));
            }
        };

        let retention_policy = self.retention_policy.unwrap_or_default();
        let runtime = PhotonRuntimeState {
            storage_port: Arc::clone(&port),
            executor_services: Arc::new(ExecutorServices::new(
                port,
                retention_policy,
                self.retention_hook,
            )),
            executor: Arc::new(ExecutorController::default()),
        };

        let backend = instrumentation::wrap_backend(backend);
        Ok(Photon::new(backend, runtime))
    }
}
