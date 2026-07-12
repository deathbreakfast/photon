//! Proc macros for Photon pub/sub.
//!
//! ## Entry points
//!
//! - [`topic`] — typed publish/subscribe on a struct; submits a topic descriptor to inventory
//! - [`subscribe`] — registers a handler; requires `Photon::start_executor` at boot

use proc_macro::TokenStream;

mod subscribe;
mod topic;

/// Marks a struct as a Photon topic, generating typed publish/subscribe APIs.
///
/// # Usage
///
/// ```ignore
/// use photon::topic;
///
/// #[topic(name = "user.notifications", keyed_by = "user_id")]
/// pub struct NotificationPushed {
///     pub user_id: String,
/// }
/// ```
#[proc_macro_attribute]
pub fn topic(attr: TokenStream, item: TokenStream) -> TokenStream {
    topic::topic_impl(attr, item)
}

/// Marks a function as a subscription handler registered via inventory.
///
/// # Usage (v1 — `Box<dyn Actor>`)
///
/// ```ignore
/// use photon::{topic, subscribe, Actor, Result};
///
/// #[topic(name = "user.notifications")]
/// pub struct NotificationPushed {
///     pub user_id: String,
/// }
///
/// #[subscribe(topic = "user.notifications", durable = "push-worker")]
/// async fn on_notification(
///     _actor: Box<dyn Actor>,
///     _event: NotificationPushed,
/// ) -> Result<()> {
///     Ok(())
/// }
/// ```
///
/// # Actor bindings (v2)
///
/// The first parameter must be a simple identifier typed as one of:
///
/// - `Box<dyn Actor>` — reconstruct as-is (v1)
/// - `Arc<dyn Actor>` — `Arc::from(reconstruct()?)`
/// - `Box<Concrete>` / `Arc<Concrete>` — downcast via `Actor::into_any`; failure maps to
///   `PhotonError::Identity`
///
/// # Optional injectables (v2)
///
/// After `(actor, payload)` you may add trailing parameters detected by type path:
///
/// - `&Event` — transport event (metadata + raw JSON)
/// - `HandlerCtx` — delivery metadata (`event_id`, `topic_name`, `topic_key`, `seq`)
///
/// Unknown trailing types are rejected at compile time.
///
/// The handler must be `async` and return `photon::Result<()>`. Call
/// `Photon::start_executor` at startup with an identity factory.
#[proc_macro_attribute]
pub fn subscribe(attr: TokenStream, item: TokenStream) -> TokenStream {
    subscribe::subscribe_impl(attr, item)
}
