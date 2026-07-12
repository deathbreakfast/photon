#!/usr/bin/env bash
# Source this file: source infra/broker/scripts/export-env.sh
export PHOTON_NATS_URL="${PHOTON_NATS_URL:-nats://127.0.0.1:4222,nats://127.0.0.1:4225,nats://127.0.0.1:4224}"
export PHOTON_NATS_STREAM="${PHOTON_NATS_STREAM:-photon}"
export PHOTON_NATS_RETENTION="${PHOTON_NATS_RETENTION:-15m}"
export PHOTON_NATS_REPLICAS="${PHOTON_NATS_REPLICAS:-3}"
