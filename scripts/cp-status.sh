#!/usr/bin/env bash
# Print the current CP subsystem status: groups, members, and leadership.
# Requires: curl, jq
# Usage: ./scripts/cp-status.sh [member_host]

set -euo pipefail

MEMBER="${1:-localhost}"
PORT="5701"
BASE="http://${MEMBER}:${PORT}/hazelcast/rest/cp"

check_dep() { command -v "$1" >/dev/null 2>&1 || { echo "Error: $1 is required"; exit 1; }; }
check_dep curl
check_dep jq

separator() { printf '%*s\n' 60 '' | tr ' ' '-'; }

echo ""
separator
echo " Hazelcast CP Subsystem Status  →  ${MEMBER}:${PORT}"
separator

echo ""
echo "■ CP Members"
curl -sf "${BASE}/members" | jq -r '.[] | "  \(.uuid)  state=\(.state)  address=\(.address.host):\(.address.port)"' 2>/dev/null \
  || echo "  (could not retrieve – CP may still be initializing)"

echo ""
echo "■ CP Groups"
curl -sf "${BASE}/groups" | jq -r '.[] | "  \(.name)  id=\(.id.seed)"' 2>/dev/null \
  || echo "  (could not retrieve)"

echo ""
echo "■ CP Group Details"
GROUPS=$(curl -sf "${BASE}/groups" | jq -r '.[].name' 2>/dev/null || echo "")
for GROUP in $GROUPS; do
  echo ""
  echo "  ── ${GROUP} ──"
  curl -sf "${BASE}/groups/${GROUP}" | jq '{
    name,
    status,
    members: [.members[] | {uuid, address: "\(.address.host):\(.address.port)"}]
  }' 2>/dev/null | sed 's/^/  /' || echo "  (error fetching group details)"
done

echo ""
separator
