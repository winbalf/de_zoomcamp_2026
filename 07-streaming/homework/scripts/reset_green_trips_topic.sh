#!/usr/bin/env bash
# Delete and recreate `green-trips` inside the homework compose stack.
# Run from repo root or anywhere if you pass COMPOSE_FILE.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOMEWORK_DIR="$(dirname "$SCRIPT_DIR")"
export COMPOSE_FILE="${COMPOSE_FILE:-$HOMEWORK_DIR/docker-compose.yaml}"

cd "$HOMEWORK_DIR"

docker compose exec -T redpanda rpk topic delete green-trips || true
docker compose exec -T redpanda rpk topic create green-trips

echo "Topic green-trips is empty; run the producer, then start Flink jobs."
