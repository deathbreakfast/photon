//! Event envelope encryption.

mod envelope;

use crate::error::Result;
use crate::models::Event;

pub use envelope::{TransportCrypto, ENVELOPE_JSON_KEY};

/// Build plaintext and sealed variants of an event for API delivery and persistence.
///
/// The first result remains suitable for callers and live fanout; the second has opaque actor and
/// payload fields and is suitable for storage or broker transport.
///
/// # Errors
///
/// Returns an error if the event JSON cannot be sealed.
pub fn seal_event_for_storage(
    crypto: &TransportCrypto,
    mut event: Event,
) -> Result<(Event, Event)> {
    let plain = event.clone();
    let (actor_json, payload_json) =
        crypto.seal_json_fields(&event.actor_json, &event.payload_json)?;
    event.actor_json = actor_json;
    event.payload_json = payload_json;
    Ok((plain, event))
}

/// Open the actor and payload fields of a stored or wire event.
///
/// Legacy plaintext event fields are returned unchanged.
///
/// # Errors
///
/// Returns an error if a sealed envelope cannot be decoded or decrypted.
pub fn open_stored_event(crypto: &TransportCrypto, mut event: Event) -> Result<Event> {
    let (actor_json, payload_json) =
        crypto.open_json_fields(&event.actor_json, &event.payload_json)?;
    event.actor_json = actor_json;
    event.payload_json = payload_json;
    Ok(event)
}
