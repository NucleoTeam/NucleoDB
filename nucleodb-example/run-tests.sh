#!/usr/bin/env bash
# Run all NucleoDB example tests.
#
# By default, runs self-contained tests with Local MQS (no external deps).
# To test with Kafka, first start it:   ../docker/kafka-local/start.sh -d
# Then:                                  ./run-tests.sh --kafka
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE="${1:-local}"

echo "=== Step 1: Build project ==="
./gradlew :nucleodb-example:classes --no-daemon -q

if [ "$MODE" = "--kafka" ]; then
  # Kafka mode: start Kafka, launch two server processes, run cross-instance tests
  KAFKA_DIR="$ROOT_DIR/docker/kafka-local"
  PIDS=()

  cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    for pid in "${PIDS[@]}"; do
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    done
    bash "$KAFKA_DIR/stop.sh" 2>/dev/null || true
    echo "Cleanup complete."
  }
  trap cleanup EXIT

  echo ""
  echo "=== Step 2: Start Kafka (standalone, no Docker) ==="
  bash "$KAFKA_DIR/start.sh" -d

  echo ""
  echo "=== Step 3: Start NucleoDB Instance 1 (port 9091) ==="
  MQS_MODE=kafka ./gradlew :nucleodb-example:run --args='9091' --no-daemon -q &
  PIDS+=($!)

  echo ""
  echo "=== Step 4: Start NucleoDB Instance 2 (port 9092) ==="
  MQS_MODE=kafka ./gradlew :nucleodb-example:run --args='9092' --no-daemon -q &
  PIDS+=($!)

  echo "Waiting for instances to connect to Kafka..."
  sleep 30

  echo ""
  echo "=== Step 5: Run CRUD Tests ==="
  ./gradlew :nucleodb-example:runCrudTest --no-daemon -q || echo "CRUD test exited with $?"

  echo ""
  echo "=== Step 6: Run Cross-Instance Consistency Tests ==="
  ./gradlew :nucleodb-example:runConsistencyTest --no-daemon -q || echo "Consistency test exited with $?"

else
  # Local mode: self-contained tests, no external dependencies
  echo ""
  echo "=== Step 2: Run Integration Test (single instance, local MQS) ==="
  ./gradlew :nucleodb-example:runIntegrationTest --no-daemon || echo "Integration test exited with $?"

  echo ""
  echo "=== Step 3: Run Multi-Instance Test (two instances, local MQS) ==="
  ./gradlew :nucleodb-example:runMultiInstanceTest --no-daemon || echo "Multi-instance test exited with $?"
fi

echo ""
echo "=== All tests complete ==="
