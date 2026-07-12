//! Checkpoint coalescing for durable subscriptions.
//!
//! [`CheckpointCoalescer`] batches high-water sequence updates per subscription partition and
//! flushes on interval or batch threshold (`PHOTON_CHECKPOINT_COALESCE_EVERY`,
//! `PHOTON_CHECKPOINT_FLUSH_MS`). Used by `#[photon::subscribe]` executor dispatch and durable
//! typed `.subscribe()` streams.
//!
//! See also: [`crate::storage`], [`crate::delivery`], [`crate::retention`].

mod coalescer;

pub use coalescer::CheckpointCoalescer;
