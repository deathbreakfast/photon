# Mem adapter profiling notes (aws-t3-medium)

Captured: 2026-07-12T15:34Z on SQLite smoke EC2.

## Measured

See `criterion-aws-t3-medium-aws.json` for hot-path Criterion timings
(envelope crypto, shard routing, empty dispatch stub).

## Observed / follow-up targets (not rewritten this pass)

- String partition keys (`topic:key` formatting) remain allocation-heavy in `InProcStoragePort`.
- Replay buffer uses `RwLock<HashMap<String, VecDeque<Event>>>` — growth under long replay
  windows should be watched before claiming mem as a production baseline.
- Cloned JSON payloads on subscribe fanout remain a structural cost.

No `InProcStoragePort` rewrite in this campaign; numbers do not yet justify a redesign.
