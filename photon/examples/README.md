# Photon examples

Runnable proofs for embedded pub/sub, durable SQLite, and brokered publisher–worker pairs. Start with the canonical path; branch when you need keyed topics, manual streams, or ops telemetry.

Full runbooks (NATS Docker, env vars, start order): [`../README.md` — How to run examples](../README.md#how-to-run-examples).

All examples require transport crypto:

```bash
export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
```

## Canonical path

### 1. Embedded — [`embedded_mem.rs`](embedded_mem.rs)

One process, in-memory storage — proves `#[photon::topic]` + `#[photon::subscribe]` + `start_executor` with zero external deps.

```bash
cargo run -p uf-photon --example embedded_mem --features runtime,mem
```

Success: tracing shows `published` event id and handler `received greeting`.

### 2. Embedded durable — [`embedded_sqlite.rs`](embedded_sqlite.rs)

Same API with file-backed SQLite — reach for this when you need restart-safe checkpoints in a single binary.

```bash
cargo run -p uf-photon --example embedded_sqlite --features runtime,sqlite
```

Success: tracing shows `published` with path and handler log line.

### 3. Brokered NATS — [`nats_worker.rs`](nats_worker.rs) · [`nats_publisher.rs`](nats_publisher.rs)

Worker first, then publisher — models fleet ingress with shared JetStream env. Swap the storage port builder for Kafka/Fluvio in production.

```bash
docker run -d --name photon-nats -p 4222:4222 nats:2.10 -js
export PHOTON_NATS_URL=nats://127.0.0.1:4222 PHOTON_NATS_STREAM=photon PHOTON_ALLOW_INSECURE_BROKER=1
cargo run -p uf-photon --example nats_worker --features runtime,nats
# cargo run -p uf-photon --example nats_publisher --features runtime,nats
```

Success: worker `worker received greeting`; publisher `nats_publisher: published`.

## Other examples

| Example | When you'd open it | Command | Success signal |
|---------|-------------------|---------|----------------|
| [`subscribe_v2.rs`](subscribe_v2.rs) | `Arc<dyn Actor>` + `HandlerCtx` + `configure` sugar | `cargo run -p uf-photon --example subscribe_v2 --features runtime,mem` | tracing publish + handler lines |
| [`keyed_topic.rs`](keyed_topic.rs) | `keyed_by` partition filter on subscribe | `cargo run -p uf-photon --example keyed_topic --features runtime,mem` | handler logs only matching partition |
| [`manual_subscribe.rs`](manual_subscribe.rs) | Raw topic-name stream without macro worker | `cargo run -p uf-photon --example manual_subscribe --features runtime,mem` | `received via manual subscribe` |
| [`consumer_group.rs`](consumer_group.rs) | Group delivery / shard semantics (single member) | `cargo run -p uf-photon --example consumer_group --features runtime,mem` | `consumer group example OK` |
| [`telemetry_ops_log.rs`](telemetry_ops_log.rs) | `PhotonBuilder::ops_log` instrumentation | `cargo run -p uf-photon --example telemetry_ops_log --features runtime,mem` | `handled with ops log installed` |

Topology reference: [Embedded](https://docs.rs/uf-photon/latest/photon/#embedded-one-binary) · [Brokered](https://docs.rs/uf-photon/latest/photon/#brokered-publisher--worker-binaries).
