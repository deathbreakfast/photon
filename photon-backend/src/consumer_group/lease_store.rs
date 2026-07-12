//! Durable shard lease metadata.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::error::{PhotonError, Result};

/// One shard lease held by a group member.
#[derive(Debug, Clone)]
pub struct ConsumerLease {
    /// Consumer group id.
    pub group_id: String,
    /// Virtual shard being leased.
    pub shard_id: u32,
    /// Member instance holding the lease.
    pub instance_id: String,
    /// Lease time-to-live in seconds.
    pub ttl_secs: u64,
}

struct LeaseRecord {
    instance_id: String,
    expires_at: Instant,
}

/// Persistence for `(group, shard) → instance` assignments.
#[async_trait]
pub trait LeaseStore: Send + Sync {
    /// Claim a shard lease for an instance (fails if held by another live member).
    async fn claim(&self, lease: ConsumerLease) -> Result<()>;
    /// Renew all leases held by an instance in the group.
    async fn renew(&self, group_id: &str, instance_id: &str, ttl_secs: u64) -> Result<()>;
    /// Release all leases held by an instance in the group.
    async fn release(&self, group_id: &str, instance_id: &str) -> Result<()>;
    /// List live shard ids leased by an instance.
    async fn list_for_instance(&self, group_id: &str, instance_id: &str) -> Result<Vec<u32>>;
}

/// In-process lease table for lab and unit tests.
pub struct MemoryLeaseStore {
    inner: Arc<RwLock<HashMap<(String, u32), LeaseRecord>>>,
}

impl Default for MemoryLeaseStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryLeaseStore {
    /// Create an empty in-memory lease table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn key(group_id: &str, shard_id: u32) -> (String, u32) {
        (group_id.to_string(), shard_id)
    }
}

#[async_trait]
impl LeaseStore for MemoryLeaseStore {
    #[allow(clippy::significant_drop_tightening)]
    async fn claim(&self, lease: ConsumerLease) -> Result<()> {
        let expires_at = Instant::now() + Duration::from_secs(lease.ttl_secs.max(1));
        let k = Self::key(&lease.group_id, lease.shard_id);
        let mut guard = self.inner.write().await;
        if let Some(existing) = guard.get(&k) {
            if existing.expires_at > Instant::now() && existing.instance_id != lease.instance_id {
                return Err(PhotonError::Internal(format!(
                    "shard {} already leased by {}",
                    lease.shard_id, existing.instance_id
                )));
            }
        }
        guard.insert(
            k,
            LeaseRecord {
                instance_id: lease.instance_id,
                expires_at,
            },
        );
        Ok(())
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn renew(&self, group_id: &str, instance_id: &str, ttl_secs: u64) -> Result<()> {
        let expires_at = Instant::now() + Duration::from_secs(ttl_secs.max(1));
        let mut guard = self.inner.write().await;
        for ((g, _shard), rec) in guard.iter_mut() {
            if g == group_id && rec.instance_id == instance_id {
                rec.expires_at = expires_at;
            }
        }
        Ok(())
    }

    async fn release(&self, group_id: &str, instance_id: &str) -> Result<()> {
        self.inner
            .write()
            .await
            .retain(|(g, _), rec| !(g == group_id && rec.instance_id == instance_id));
        Ok(())
    }

    async fn list_for_instance(&self, group_id: &str, instance_id: &str) -> Result<Vec<u32>> {
        let now = Instant::now();
        let mut shards: Vec<u32> = {
            let guard = self.inner.read().await;
            guard
                .iter()
                .filter(|((g, _), rec)| {
                    g == group_id && rec.instance_id == instance_id && rec.expires_at > now
                })
                .map(|((_, shard), _)| *shard)
                .collect()
        };
        shards.sort_unstable();
        Ok(shards)
    }
}
