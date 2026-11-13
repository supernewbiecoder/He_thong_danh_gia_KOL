#!/usr/bin/env bash
# Builds the project images where the Kubernetes cluster can use them.
#   ./scripts/build-images.sh minikube   # build INSIDE minikube (minikube image build): no Docker needed
#   ./scripts/build-images.sh            # docker build (Docker Desktop Kubernetes shares Docker's images)
#   ./scripts/build-images.sh kind       # docker build + kind load docker-image
#
# MINIKUBE_PROFILE=<name> selects a minikube profile (default: minikube).
set -euo pipefail
source "$(dirname "$0")/lib.sh"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Windows: minikube.exe / docker.exe do not understand Git Bash paths such as /c/Users/...
command -v cygpath >/dev/null 2>&1 && ROOT="$(cygpath -m "$ROOT")"

KAFKA_VERSION="${KAFKA_VERSION:-3.9.0}"
MINIKUBE="${MINIKUBE:-minikube}"
MINIKUBE_PROFILE="${MINIKUBE_PROFILE:-minikube}"

# tag | build context (Dockerfiles are plain OCI build files: minikube, BuildKit or Docker can build them)
IMAGES=(
  "kol-kafka:${KAFKA_VERSION}|docker/kafka"
  "kol-collectors:1.0.0|collectors"
  "kol-stream-processor:1.0.0|processor"
  "kol-dbwriter:1.0.0|dbwriter"
  "kol-loadgen:1.0.0|loadtest"
)

TARGET="${1:-docker}"
case "$TARGET" in
  minikube|docker|kind) ;;
  *) echo "Unknown target: $TARGET (minikube|docker|kind)"; exit 1 ;;
esac

step "Building ${#IMAGES[@]} images for: $TARGET"
for entry in "${IMAGES[@]}"; do
  tag="${entry%%|*}"
  context="$ROOT/${entry#*|}"
  log "$tag  <-  ${entry#*|}/"
  if [[ "$TARGET" == minikube ]]; then
    "$MINIKUBE" -p "$MINIKUBE_PROFILE" image build -t "$tag" "$context"
  else
    docker build -t "$tag" "$context"
    [[ "$TARGET" == kind ]] && kind load docker-image "$tag"
  fi
done

log "Images ready: $(printf '%s ' "${IMAGES[@]%%|*}")"
