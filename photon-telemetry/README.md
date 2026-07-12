# photon-telemetry

`OpsLog` telemetry port for Photon self-metrics and ops events.

## Adapters

| Adapter | Crate |
|---------|-------|
| `ConsoleOpsLog` | this crate |
| `NoOpsLog` | this crate |

Hosts may install additional `OpsLog` adapters at boot. Instrumentation in [`photon/src/runtime/instrumentation/`](../photon/src/runtime/instrumentation/) uses injected [`OpsLog`](src/lib.rs) via `ops_log()`.

## Status

Trait + console/no-op + global install wired.
