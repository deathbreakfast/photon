//! A bad stream item is dead-lettered and the handler keeps receiving later events.

#![allow(missing_docs)]
#![allow(clippy::unused_async)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use photon::{configure, subscribe, topic, JsonIdentityFactory};
use photon_core::Actor;
use photon_testkit::{FaultyBackend, StreamFault};

static SEEN: Mutex<Vec<u32>> = Mutex::new(Vec::new());

#[topic(name = "test.stream.item.error")]
pub struct ItemErrorEvent {
    pub value: u32,
}

#[subscribe(topic = "test.stream.item.error", durable = "item-error-handler")]
async fn on_item_error_event(_actor: Box<dyn Actor>, event: ItemErrorEvent) -> photon::Result<()> {
    SEEN.lock().unwrap().push(event.value);
    Ok(())
}

#[tokio::test]
async fn bad_item_mid_stream_goes_to_dlq_and_later_events_arrive() {
    let (photon, backend) = FaultyBackend::photon(
        "test.stream.item.error",
        StreamFault::BadItemAfter { after: 1 },
    )
    .expect("build photon");
    configure(photon.clone());

    for value in 1..=3 {
        ItemErrorEvent { value }.publish().await.expect("publish");
    }

    photon
        .start_executor(Arc::new(JsonIdentityFactory))
        .expect("start executor");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while SEEN.lock().unwrap().len() < 3 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(*SEEN.lock().unwrap(), vec![1, 2, 3]);
    assert_eq!(
        backend.subscribe_calls(),
        1,
        "a bad item must not resubscribe"
    );
    assert_eq!(photon.runtime().executor_services.dlq.len(), 1);
}
