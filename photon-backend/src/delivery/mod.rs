//! Handler delivery: backpressure and dead-letter queue.
//!
//! [`WorkerPool`] bounds concurrent handler tasks per subscription partition.
//! [`DlqSink`] records metadata-only DLQ rows when identity reconstruction, handler errors, or
//! checkpoint persistence fail during executor dispatch.

mod dlq;
mod worker_pool;

pub use dlq::{DlqRecord, DlqRecordParams, DlqSink};
pub use worker_pool::WorkerPool;
