//! Multiplex assigned virtual shard streams for consumer-group subscribers.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::pin::Pin;
use std::sync::Arc;

use async_stream::stream;
use futures::stream::Stream;
use tokio::sync::mpsc;

use crate::backend::PhotonBackend;
use crate::error::Result;
use crate::models::Event;
use crate::shard_router::shard_storage_key;

/// Merge tail streams for `shard_ids`, each with its own replay cursor.
pub fn merge_shard_streams<S: BuildHasher + Send + Sync + 'static>(
    backend: Arc<dyn PhotonBackend>,
    topic_name: String,
    shard_ids: &[u32],
    after_seq_by_shard: HashMap<u32, Option<i64>, S>,
) -> Pin<Box<dyn Stream<Item = Result<Event>> + Send>> {
    let shards: Vec<u32> = shard_ids.to_vec();
    Box::pin(stream! {
        if shards.is_empty() {
            return;
        }
        let (tx, mut rx) = mpsc::channel::<Result<Event>>(256);
        for shard_id in shards {
            let key = shard_storage_key(shard_id);
            let after = after_seq_by_shard.get(&shard_id).copied().flatten();
            let topic = topic_name.clone();
            let backend = Arc::clone(&backend);
            let mut sub = backend.subscribe(topic, Some(key), after);
            let tx = tx.clone();
            tokio::spawn(async move {
                while let Some(item) = futures::StreamExt::next(&mut sub).await {
                    if tx.send(item).await.is_err() {
                        break;
                    }
                }
            });
        }
        drop(tx);
        while let Some(item) = rx.recv().await {
            yield item;
        }
    })
}
