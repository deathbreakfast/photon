//! `SQLite` adapter configuration.
//!
//! **Configuration lives here.** Open a path explicitly or resolve from env:
//!
//! | API / env | Default | Purpose |
//! |-----------|---------|---------|
//! | [`SqliteStoragePort::open`](crate::SqliteStoragePort::open) | — | Open (or create) a database file. |
//! | [`SqliteStoragePort::from_env`](crate::SqliteStoragePort::from_env) / [`PATH_ENV`] | temp file | Resolve path via [`sqlite_path_from_env`]. |
//!
//! Production hosts must set [`PATH_ENV`] or call
//! [`SqliteStoragePort::open`](crate::SqliteStoragePort::open) with a durable, explicit path.
//! The temporary fallback is intended only for local development and tests.
//!
//! # Example
//!
//! ```rust,no_run
//! use photon_backend_sqlite::SqliteStoragePort;
//!
//! # async fn wire() -> photon_backend::Result<()> {
//! let _port = SqliteStoragePort::open("/var/lib/photon/events.db").await?;
//! // Photon::builder().storage_port(Arc::new(port)).auto_registry().build()?;
//! # Ok(())
//! # }
//! ```

/// Environment variable for the `SQLite` database file path.
pub const PATH_ENV: &str = "PHOTON_SQLITE_PATH";

/// Resolve database path from [`PATH_ENV`], or a unique file under the system temp dir.
///
/// The temporary fallback is not durable across hosts and must not be used for production data.
#[must_use]
pub fn sqlite_path_from_env() -> String {
    std::env::var(PATH_ENV).unwrap_or_else(|_| {
        let path = std::env::temp_dir().join(format!("photon-{}.db", uuid::Uuid::new_v4()));
        path.to_string_lossy().into_owned()
    })
}
