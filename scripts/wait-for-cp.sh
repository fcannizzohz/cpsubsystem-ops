#!/usr/bin/env bash
# Wait until the CP subsystem has fully initialized (all 5 CP members joined).
# Usage: ./scripts/wait-for-cp.sh [member_host] [timeout_seconds]

set -euo pipefail

MEMBER="${1:-localhost}"
PORT="5701"
TIMEOUT="${2:-120}"
ELAPSED=0
INTERVAL=5

echo "Waiting for CP subsystem to initialize on ${MEMBER}:${PORT} (timeout: ${TIMEOUT}s)..."

while true; do
  # Query CP member list via REST API
  RESPONSE=$(curl -sf "http://${MEMBER}:${PORT}/hazelcast/rest/cp/members" 2>/dev/null || echo "")

  if [[ -n "$RESPONSE" ]]; then
    COUNT=$(echo "$RESPONSE" | python3 -c "import sys,json; data=json.load(sys.stdin); print(len(data))" 2>/dev/null || echo "0")
    echo "[${ELAPSED}s] CP members online: ${COUNT}/5"
    if [[ "$COUNT" -ge 5 ]]; then
      echo "CP subsystem is ready! All 5 CP members are active."
      exit 0
    fi
  else
    echo "[${ELAPSED}s] Member not yet reachable..."
  fi

  if [[ $ELAPSED -ge $TIMEOUT ]]; then
    echo "Timeout reached. CP subsystem did not initialize within ${TIMEOUT}s."
    exit 1
  fi

  sleep "$INTERVAL"
  ELAPSED=$((ELAPSED + INTERVAL))
done
