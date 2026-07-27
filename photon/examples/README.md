# Photon examples

Runnable proofs for embedded pub/sub, durable SQLite, brokered publisher–worker pairs (NATS, Kafka, Fluvio, and secure TLS NATS), and checkpoint restart recovery. Start with the canonical path; branch when you need keyed topics, manual streams, ops telemetry, another broker, or durable failover.

Full runbooks (broker Docker, env vars, start order): [`../README.md` — How to run examples](../README.md#how-to-run-examples).

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

### 4. Secure brokered NATS — [`nats_secure_worker.rs`](nats_secure_worker.rs) · [`nats_secure_publisher.rs`](nats_secure_publisher.rs)

Same pair as (3), wired the **production** way: `.require_tls()` + `.credentials_file` / `PHOTON_NATS_CREDS`. Never sets `PHOTON_ALLOW_INSECURE_BROKER`; without a TLS broker on hand, both binaries print the runbook and exit cleanly instead of falling back to plaintext.

```bash
export PHOTON_NATS_URL=tls://nats.example.internal:4222 PHOTON_NATS_STREAM=photon PHOTON_NATS_CREDS=/run/secrets/photon-nats.creds
cargo run -p uf-photon --example nats_secure_worker --features runtime,nats
# cargo run -p uf-photon --example nats_secure_publisher --features runtime,nats
```

Success: worker `worker received greeting`; publisher `published over TLS`. No TLS broker: both log a `PHOTON_NATS_URL must be tls://…` warning and exit `Ok`.

### 5. Brokered Kafka — [`kafka_worker.rs`](kafka_worker.rs) · [`kafka_publisher.rs`](kafka_publisher.rs)

Same publisher/worker contract as (3), backed by `KafkaStoragePortBuilder`. Local lab: `infra/broker/scripts/kafka-single.sh` + `scripts/export-kafka-env.sh`.

```bash
cd infra/broker && ./scripts/kafka-single.sh && source scripts/export-kafka-env.sh && cd -
export PHOTON_ALLOW_INSECURE_BROKER=1
cargo run -p uf-photon --example kafka_worker --features runtime,kafka
# cargo run -p uf-photon --example kafka_publisher --features runtime,kafka
```

Success: worker `worker received greeting`; publisher `kafka_publisher: published`.

### 6. Brokered Fluvio — [`fluvio_worker.rs`](fluvio_worker.rs) · [`fluvio_publisher.rs`](fluvio_publisher.rs)

Same publisher/worker contract as (3), backed by `FluvioStoragePortBuilder`. Local lab: `infra/broker/scripts/fluvio-single.sh` + `scripts/export-fluvio-env.sh`.

```bash
cd infra/broker && ./scripts/fluvio-single.sh && source scripts/export-fluvio-env.sh && cd -
export PHOTON_ALLOW_INSECURE_BROKER=1
cargo run -p uf-photon --example fluvio_worker --features runtime,fluvio
# cargo run -p uf-photon --example fluvio_publisher --features runtime,fluvio
```

Success: worker `worker received greeting`; publisher `fluvio_publisher: published`.

### 7. Durable consumer recovery — [`durable_consumer_recovery.rs`](durable_consumer_recovery.rs)

Single process, two phases on file-backed SQLite: handle a batch, force-flush the checkpoint, "crash" (drop the `Photon` handle), then reopen the same file and resume the identical `durable = "…"` subscription — proving the checkpoint-driven restart contract that the brokered workers above rely on against their own brokers.

```bash
cargo run -p uf-photon --example durable_consumer_recovery --features runtime,sqlite
```

Success: `phase 1: … simulating a process crash` then `phase 2: resumed from checkpoint with no redelivery`.

## Other examples

| Example | When you'd open it | Command | Success signal |
|---------|-------------------|---------|----------------|
| [`subscribe_v2.rs`](subscribe_v2.rs) | `Arc<dyn Actor>` + `HandlerCtx` + `configure` sugar | `cargo run -p uf-photon --example subscribe_v2 --features runtime,mem` | tracing publish + handler lines |
| [`keyed_topic.rs`](keyed_topic.rs) | `keyed_by` partition filter on subscribe | `cargo run -p uf-photon --example keyed_topic --features runtime,mem` | handler logs only matching partition |
| [`manual_subscribe.rs`](manual_subscribe.rs) | Raw topic-name stream without macro worker | `cargo run -p uf-photon --example manual_subscribe --features runtime,mem` | `received via manual subscribe` |
| [`consumer_group.rs`](consumer_group.rs) | Group delivery / shard semantics (single member) | `cargo run -p uf-photon --example consumer_group --features runtime,mem` | `consumer group example OK` |
| [`telemetry_ops_log.rs`](telemetry_ops_log.rs) | `PhotonBuilder::ops_log` instrumentation | `cargo run -p uf-photon --example telemetry_ops_log --features runtime,mem` | `handled with ops log installed` |
| [`custom_storage_port_stub.rs`](custom_storage_port_stub.rs) | Decorator `StoragePort` wrapping `mem` to validate/audit append | `cargo run -p uf-photon --example custom_storage_port_stub --features runtime,mem` | `blank topic_name rejected; published … through AuditingStoragePort` |

Topology reference: [Embedded](https://docs.rs/uf-photon/latest/photon/#embedded-one-binary) · [Brokered](https://docs.rs/uf-photon/latest/photon/#brokered-publisher--worker-binaries).
