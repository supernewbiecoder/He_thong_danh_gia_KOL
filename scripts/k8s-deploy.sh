#!/usr/bin/env bash
# Deploys the whole platform to the current kubectl context (minikube, Docker Desktop, kind...).
#   ./scripts/k8s-deploy.sh             # overlay k8s/overlays/dev
#   ./scripts/k8s-deploy.sh dev --dry   # only render the manifests
#
#   KOL_EXTERNAL_HOST=<name>  address Kafka advertises to clients OUTSIDE the cluster
#                             (default 127.0.0.1; use this machine's Tailscale name to share it)
#
# Steps: install the CloudNativePG operator (once), create the database secrets (once, random
# passwords, never overwritten), apply the manifests, wait for Kafka, topics and PostgreSQL.
# The base kustomization reuses files outside k8s/, hence --load-restrictor LoadRestrictionsNone:
# plain `kubectl apply -k` will NOT work.
set -euo pipefail
export MSYS_NO_PATHCONV=1
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Windows: kubectl.exe does not understand Git Bash paths such as /c/Users/...
command -v cygpath >/dev/null 2>&1 && ROOT="$(cygpath -m "$ROOT")"
OVERLAY="${1:-dev}"
NS=kol-system
CNPG_VERSION="${CNPG_VERSION:-1.30.1}"
CNPG_MANIFEST="https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-${CNPG_VERSION%.*}/releases/cnpg-${CNPG_VERSION}.yaml"

render() {
  kubectl kustomize --load-restrictor LoadRestrictionsNone "$ROOT/k8s/overlays/$OVERLAY" \
    | awk -v host="${KOL_EXTERNAL_HOST:-}" '
        host != "" && /name: KOL_EXTERNAL_HOST$/ { print; getline; sub(/value: .*/, "value: " host) }
        { print }'
}

if [[ "${2:-}" == "--dry" ]]; then
  render
  exit 0
fi

echo ">> context: $(kubectl config current-context)"

# 1. CloudNativePG operator: runs PostgreSQL with replication and automatic failover.
if ! kubectl get crd clusters.postgresql.cnpg.io >/dev/null 2>&1; then
  echo ">> installing CloudNativePG ${CNPG_VERSION}"
  kubectl apply --server-side -f "$CNPG_MANIFEST" >/dev/null
fi
kubectl -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=300s

# 2. Database secrets, before the cluster that needs them.
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
create_db_secret() {
  local name="$1" user="$2"
  kubectl -n "$NS" get secret "$name" >/dev/null 2>&1 && return 0
  kubectl -n "$NS" create secret generic "$name" --type=kubernetes.io/basic-auth \
    --from-literal=username="$user" --from-literal=password="$(openssl rand -hex 24)" >/dev/null
  echo ">> created secret $name (random password)"
}
create_db_secret kol-db-app kol_app
create_db_secret kol-db-readonly kol_readonly
create_db_secret kol-db-superuser postgres

# 3. Manifests. Jobs are immutable: the topic job is re-created on every deploy (it is idempotent).
kubectl -n "$NS" delete job kafka-topics-init --ignore-not-found >/dev/null 2>&1 || true
for attempt in 1 2 3; do
  # The operator's admission webhook can need a few seconds after its first start.
  if render | kubectl apply -f -; then break; fi
  [[ "$attempt" == 3 ]] && exit 1
  echo ">> apply failed, retrying in 10s"; sleep 10
done

# 4. Wait for the stateful parts.
echo ">> waiting for Kafka..."
kubectl -n "$NS" rollout status statefulset/kafka --timeout=600s
echo ">> waiting for the topic job..."
kubectl -n "$NS" wait --for=condition=complete job/kafka-topics-init --timeout=300s
echo ">> waiting for PostgreSQL (1 primary + 2 replicas)..."
kubectl -n "$NS" wait cluster/kol-db --for=condition=Ready --timeout=900s
kubectl -n "$NS" get pods -o wide

cat <<EOF

Deployed. Next steps:
  ./scripts/k8s-expose.sh            # keep it open: Kafka, PostgreSQL, Grafana, Kafka UI on 127.0.0.1
  Kafka (outside the cluster): ${KOL_EXTERNAL_HOST:-127.0.0.1}:30090,${KOL_EXTERNAL_HOST:-127.0.0.1}:30091,${KOL_EXTERNAL_HOST:-127.0.0.1}:30092
  Kafka (inside the cluster):  kafka.$NS.svc.cluster.local:9092
  PostgreSQL (inside):         kol-db-rw.$NS.svc.cluster.local:5432 (primary), kol-db-ro (replicas)

YouTube key (optional):
  kubectl -n $NS create secret generic collector-credentials --from-literal=youtube-api-key=KEY
  kubectl -n $NS rollout restart deploy/social-collectors

Database passwords:
  kubectl -n $NS get secret kol-db-app -o jsonpath='{.data.password}' | base64 -d
EOF
