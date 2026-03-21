#!/usr/bin/env bash
# Fail if any consumed value is not strict JSON (e.g. bare NaN tokens from legacy producers).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOMEWORK_DIR="$(dirname "$SCRIPT_DIR")"
export COMPOSE_FILE="${COMPOSE_FILE:-$HOMEWORK_DIR/docker-compose.yaml}"
cd "$HOMEWORK_DIR"

LIMIT="${1:-200000}"
docker compose exec -T redpanda rpk topic consume green-trips -n "$LIMIT" -f '%v' | python3 -c "
import json, sys
n = 0
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        json.loads(line)
    except json.JSONDecodeError as e:
        print('INVALID_JSON at record', n, ':', e, '|', line[:160], file=sys.stderr)
        sys.exit(1)
    n += 1
print('OK: parsed', n, 'records (strict JSON)')
"
