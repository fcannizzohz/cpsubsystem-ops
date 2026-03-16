#!/usr/bin/env bash
# Discover all CP-related Prometheus metrics aggregated by Management Center.
# Useful for verifying metric names before editing the Grafana dashboard.
# Usage: ./scripts/discover-metrics.sh [mc_host]

MC="${1:-localhost}"
PORT="8080"

echo "Fetching Prometheus metrics from http://${MC}:${PORT}/metrics ..."
echo ""

curl -sf "http://${MC}:${PORT}/metrics" \
  | grep -i "^hz_cp\|^# HELP hz_cp\|^# TYPE hz_cp" \
  || {
    echo "No hz_cp_* metrics found or endpoint unreachable."
    echo ""
    echo "Ensure MC_PROMETHEUS_ENABLED=true is set on the management-center container"
    echo "and that the cluster is connected to Management Center."
    exit 1
  }

echo ""
echo "--- All hz_runtime_* metrics ---"
curl -sf "http://${MC}:${PORT}/metrics" | grep "^hz_runtime" | head -30
