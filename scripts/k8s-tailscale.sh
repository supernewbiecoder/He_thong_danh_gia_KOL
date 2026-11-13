#!/usr/bin/env bash
# OPTIONAL - share the cluster with the team through the Tailscale Kubernetes operator, without
# port-forward: every broker, PostgreSQL, Grafana and Kafka UI get their own tailnet name.
# NOT tested in this repo yet; the port-forward route (k8s-expose.sh) is the tested one.
#
# Prerequisites (https://tailscale.com/kb/1236/kubernetes-operator):
#   - helm
#   - in the tailnet policy: tagOwners "tag:k8s-operator": [] and "tag:k8s": ["tag:k8s-operator"]
#   - an OAuth client (admin console > Settings > OAuth clients) with write scopes
#     "Devices Core" and "Auth Keys", tag tag:k8s-operator
#
#   TS_CLIENT_ID=... TS_CLIENT_SECRET=... TAILNET=tail1234.ts.net ./scripts/k8s-tailscale.sh
#
# Result: Kafka  kol-kafka-0.<tailnet>:9094,kol-kafka-1.<tailnet>:9094,kol-kafka-2.<tailnet>:9094
#         PostgreSQL kol-db.<tailnet>:5432 · Grafana http://kol-grafana.<tailnet>:3000
set -euo pipefail
export MSYS_NO_PATHCONV=1
NS=kol-system
: "${TS_CLIENT_ID:?set TS_CLIENT_ID}" "${TS_CLIENT_SECRET:?set TS_CLIENT_SECRET}" "${TAILNET:?set TAILNET, e.g. tail1234.ts.net}"

echo ">> installing / upgrading the Tailscale operator"
helm repo add tailscale https://pkgs.tailscale.com/helmcharts >/dev/null 2>&1 || true
helm repo update tailscale >/dev/null
helm upgrade --install tailscale-operator tailscale/tailscale-operator \
  --namespace tailscale --create-namespace \
  --set-string oauth.clientId="$TS_CLIENT_ID" \
  --set-string oauth.clientSecret="$TS_CLIENT_SECRET" \
  --wait

expose() {
  kubectl -n "$NS" annotate service "$1" --overwrite \
    tailscale.com/expose=true tailscale.com/hostname="$2" >/dev/null
  echo ">> $1 -> $2.$TAILNET"
}
for i in 0 1 2; do expose "kafka-$i-external" "kol-kafka-$i"; done
expose grafana kol-grafana
expose kafka-ui kol-kafka-ui

# The services CloudNativePG creates are managed by the operator, so PostgreSQL gets its own
# service pointing at the current primary.
kubectl -n "$NS" apply -f - <<'EOF'
apiVersion: v1
kind: Service
metadata:
  name: kol-db-tailnet
  annotations:
    tailscale.com/expose: "true"
    tailscale.com/hostname: kol-db
spec:
  selector:
    cnpg.io/cluster: kol-db
    cnpg.io/instanceRole: primary
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
EOF
echo ">> kol-db-tailnet -> kol-db.$TAILNET"

# Each broker now advertises its own tailnet name (rolling restart, one broker at a time).
kubectl -n "$NS" set env statefulset/kafka KOL_EXTERNAL_HOST_TEMPLATE="kol-kafka-ORDINAL.$TAILNET"
kubectl -n "$NS" rollout status statefulset/kafka --timeout=600s

cat <<EOF

Send to the team (they only need to be in the tailnet):
  Kafka       kol-kafka-0.$TAILNET:9094,kol-kafka-1.$TAILNET:9094,kol-kafka-2.$TAILNET:9094
  PostgreSQL  kol-db.$TAILNET:5432 (database kol, user kol_app or kol_readonly)
  Grafana     http://kol-grafana.$TAILNET:3000
  Kafka UI    http://kol-kafka-ui.$TAILNET:8080
EOF
