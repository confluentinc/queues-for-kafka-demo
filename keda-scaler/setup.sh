#!/usr/bin/env bash
#
# Creates a Kind cluster and installs Prometheus and KEDA using Helm.
# Run this before deploying the demo manifests.
#
# Usage:  ./setup.sh
#
set -euo pipefail

CLUSTER_NAME="qfk-demo"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Kind cluster ─────────────────────────────────────────────────────────────
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "Kind cluster '${CLUSTER_NAME}' already exists, skipping creation."
else
  echo "Creating Kind cluster '${CLUSTER_NAME}'..."
  kind create cluster --name "$CLUSTER_NAME" --config "${SCRIPT_DIR}/kind-config.yaml"
fi

# ── Helm repos ───────────────────────────────────────────────────────────────
echo "Adding Helm repos..."
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm repo add kedacore https://kedacore.github.io/charts 2>/dev/null || true
helm repo update

# ── Prometheus ───────────────────────────────────────────────────────────────
echo ""
echo "Installing Prometheus into the 'monitoring' namespace..."
helm upgrade --install prometheus prometheus-community/prometheus \
  --namespace monitoring --create-namespace \
  --set alertmanager.enabled=false \
  --set kube-state-metrics.enabled=false \
  --set prometheus-node-exporter.enabled=false \
  --set prometheus-pushgateway.enabled=false \
  --set server.global.scrape_interval=1s \
  --set server.global.scrape_timeout=1s \
  --wait

# ── KEDA ─────────────────────────────────────────────────────────────────────
echo ""
echo "Installing KEDA into the 'keda' namespace..."
helm upgrade --install keda kedacore/keda \
  --namespace keda --create-namespace \
  --wait

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "=== Setup complete ==="
echo ""
echo "Prometheus : prometheus-server.monitoring.svc.cluster.local:80"
echo "KEDA       : running in the 'keda' namespace"
echo ""
echo "Next steps:"
echo "  1. Build the Docker images (see README step 3)"
echo "  2. Load them into Kind:"
echo "       kind load docker-image qfk-lag-exporter:latest --name ${CLUSTER_NAME}"
echo "       kind load docker-image my-qfk-consumer:latest --name ${CLUSTER_NAME}"
echo "       kind load docker-image my-qfk-dashboard:latest --name ${CLUSTER_NAME}"
echo "  3. Configure and apply the k8s/ manifests"
