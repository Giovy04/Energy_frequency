#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"

echo "[kafka-init] waiting for kafka at $BOOTSTRAP..."
for i in $(seq 1 60); do
  if cub kafka-ready -b "$BOOTSTRAP" 1 5 >/dev/null 2>&1; then
    echo "[kafka-init] kafka ready"
    break
  fi
  echo "[kafka-init] retry $i/60"
  sleep 2
done

topics=(
  frequency.raw
  frequency.dlq
  frequency.telemetry
  frequency.alerts
  frequency.spark-alerts
  frequency.ml-alerts
)

for t in "${topics[@]}"; do
  kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$t" --partitions 3 --replication-factor 1
done

echo "[kafka-init] done"
