# Photon macro expansion

## `#[photon::subscribe]` (v1 + v2)

Defined in `photon-macros/src/subscribe.rs`. Inventory registration submits a
[`HandlerDescriptor`](../photon-backend/src/handler_descriptor.rs) whose `invoke`
receives `(&dyn IdentityFactory, &Event)`.

### Handler signature

```text
async fn handler(
    actor:   /* actor binding */,
    payload: /* typed topic payload */,
    /* optional injectables… */
) -> photon::Result<()>
```

### Actor bindings

| Parameter type | Expansion |
|----------------|-----------|
| `Box<dyn Actor>` | `identity.reconstruct(actor_json)?` (v1) |
| `Arc<dyn Actor>` | `Arc::from(reconstruct()?)` |
| `Box<Concrete>` | `reconstruct()?.into_any().downcast::<Concrete>()?` → `PhotonError::Identity` on mismatch |
| `Arc<Concrete>` | same downcast, then `Arc::from` |

`Concrete` must implement [`Actor`](../photon-core/src/identity.rs) with the `Any`
downcast helpers (`as_any` / `as_any_mut` / `into_any`). Use
`photon_core::actor_downcast_methods!()` inside the `impl Actor` block.

The actor parameter pattern must be a simple identifier.

### Optional injectables

Trailing parameters after `(actor, payload)` are matched by type-path suffix:

| Type | Injected value |
|------|----------------|
| `&Event` | Borrow of the transport event |
| `HandlerCtx` / `HandlerCtx<'_>` | [`HandlerCtx::from_event`](../photon-backend/src/handler_ctx.rs) |
| `&HandlerCtx` | Reference to a local `HandlerCtx` |

Unknown trailing types fail compilation with a message listing allowed injectables.
Duplicates of the same injectable are rejected.

### Generated invoker shape

```rust,ignore
fn __photon_subscribe_handler<'a>(
    identity: &'a dyn photon_core::IdentityFactory,
    event: &'a photon::Event,
) -> Pin<Box<dyn Future<Output = photon::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let actor_json = event.actor_json.to_string();
        // actor binding…
        let payload: PayloadTy = serde_json::from_value(event.payload_json.clone())?;
        // optional HandlerCtx / &Event binds…
        handler(actor, payload /* , injectables… */).await
    })
}
```

Runnable: `cargo run -p uf-photon --example subscribe_v2 --features runtime,mem`.
