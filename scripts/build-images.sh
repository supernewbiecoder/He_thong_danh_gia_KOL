#!/usr/bin/env bash
# Builds the project images and (optionally) loads them into a local Kubernetes cluster.
#   ./scripts/build-images.sh            # Docker Desktop Kubernetes: local images are visible directly
#   ./scripts/build-images.sh kind       # + kind load docker-image
#   ./scripts/build-images.sh minikube   # + minikube image load
set -euo pipefail
source "$(dirname "$0")/lib.sh"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

KAFKA_VERSION="${KAFKA_VERSION:-3.9.0}"
IMAGES=("kol-kafka:${KAFKA_VERSION}" "kol-loadgen:1.0.0")

step "Building images"
docker build --build-arg KAFKA_VERSION="$KAFKA_VERSION" -t "kol-kafka:${KAFKA_VERSION}" "$ROOT/docker/kafka"
docker build -t kol-loadgen:1.0.0 "$ROOT/loadtest"

case "${1:-}" in
  kind)
    step "Loading into kind"
    for img in "${IMAGES[@]}"; do kind load docker-image "$img"; done ;;
  minikube)
    step "Loading into minikube"
    for img in "${IMAGES[@]}"; do minikube image load "$img"; done ;;
  "") ;;
  *) echo "Unknown target: $1 (kind|minikube)"; exit 1 ;;
esac
log "Images ready: ${IMAGES[*]}"
