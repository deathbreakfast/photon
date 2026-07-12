//! Delivery metadata injectable into `#[photon::subscribe]` handlers (v2).

/// Delivery metadata for a single handler invocation.
///
/// Inject as a trailing parameter on a `#[photon::subscribe]` handler (after actor and
/// payload), alone or alongside `&Event`:
///
/// ```ignore
/// #[photon::subscribe(topic = "orders", durable = "worker")]
/// async fn on_order(
///     _actor: Box<dyn Actor>,
///     order: OrderPlaced,
///     ctx: HandlerCtx<'_>,
/// ) -> photon::Result<()> {
///     tracing::info!(event_id = %ctx.event_id, seq = ctx.seq, "handling");
///     Ok(())
/// }
/// ```
///
/// Values borrow from the transport [`crate::models::Event`] passed to the generated
/// invoker for the duration of the handler future.
#[derive(Debug, Clone, Copy)]
pub struct HandlerCtx<'a> {
    /// Unique event ID (UUID string from the transport event).
    pub event_id: &'a str,
    /// Topic name the event was published on.
    pub topic_name: &'a str,
    /// Partition / routing key when the topic is keyed.
    pub topic_key: Option<&'a str>,
    /// Monotonic sequence number for this topic (and key, when keyed).
    pub seq: i64,
}

impl<'a> HandlerCtx<'a> {
    /// Build from a transport [`crate::models::Event`].
    #[must_use]
    pub fn from_event(event: &'a crate::models::Event) -> Self {
        Self {
            event_id: &event.event_id,
            topic_name: &event.topic_name,
            topic_key: event.topic_key.as_deref(),
            seq: event.seq,
        }
    }
}
