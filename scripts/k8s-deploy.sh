#!/usr/bin/env bash
# Deploys the infrastructure to the current kubectl context.
#   ./scripts/k8s-deploy.sh             # overlay k8s/overlays/dev
#   ./scripts/k8s-deploy.sh dev --dry   # only render the manifests
#
# The base kustomization reuses files outside k8s/ (config/topics.conf, monitoring/...),
# hence --load-restrictor LoadRestrictionsNone. Plain `kubectl apply -k` will NOT work.
set -euo pipefail
export MSYS_NO_PATHCONV=1
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OVERLAY="${1:-dev}"
NS=kol-system

render() {
  kubectl kustomize --load-restrictor LoadRestrictionsNone "$ROOT/k8s/overlays/$OVERLAY"
}

if [[ "${2:-}" == "--dry" ]]; then
  render
  exit 0
fi

echo ">> context: $(kubectl config current-context)"
# Jobs are immutable; the topic job is re-created on every deploy (it is idempotent).
kubectl -n "$NS" delete job kafka-topics-init --ignore-not-found >/dev/null 2>&1 || true

render | kubectl apply -f -

echo ">> waiting for Kafka StatefulSet..."
kubectl -n "$NS" rollout status statefulset/kafka --timeout=600s
echo ">> waiting for topic job..."
kubectl -n "$NS" wait --for=condition=complete job/kafka-topics-init --timeout=300s
kubectl -n "$NS" get pods -o wide

cat <<EOF

Deployed. Access from your machine:
  kubectl -n $NS port-forward svc/grafana 3000:3000      -> http://localhost:3000
  kubectl -n $NS port-forward svc/prometheus 9090:9090   -> http://localhost:9090
  kubectl -n $NS port-forward svc/kafka-ui 8080:8080     -> http://localhost:8080
  Kafka from host (NodePort): localhost:30090,localhost:30091,localhost:30092
  Kafka in-cluster:           kafka.$NS.svc.cluster.local:9092
EOF
