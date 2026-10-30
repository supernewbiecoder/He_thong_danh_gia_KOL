#!/usr/bin/env bash
# Shows the state of the Compose Kafka cluster: containers, KRaft quorum, topics, consumer groups.
#   ./scripts/cluster-status.sh            # everything
#   ./scripts/cluster-status.sh topics     # only one section: containers|quorum|topics|groups
set -euo pipefail
source "$(dirname "$0")/lib.sh"

SECTION="${1:-all}"
BS="$(running_bootstrap)"

if [[ "$SECTION" == all || "$SECTION" == containers ]]; then
  step "Containers"
  for b in "${BROKERS[@]}" kafka-ui kafka-exporter prometheus grafana; do
    printf '  %-16s running=%-5s health=%s\n' "$b" "$(is_running "$b" && echo yes || echo no)" "$(health_of "$b")"
  done
fi

if [[ "$SECTION" == all || "$SECTION" == quorum ]]; then
  step "KRaft quorum"
  kexec kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status
  echo
  kexec kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --replication
fi

if [[ "$SECTION" == all || "$SECTION" == topics ]]; then
  step "Topics"
  kexec kafka-topics.sh --bootstrap-server "$BS" --describe --exclude-internal
  step "Under-replicated partitions (empty = healthy)"
  kexec kafka-topics.sh --bootstrap-server "$BS" --describe --under-replicated-partitions
fi

if [[ "$SECTION" == all || "$SECTION" == groups ]]; then
  step "Consumer groups"
  kexec kafka-consumer-groups.sh --bootstrap-server "$BS" --describe --all-groups 2>/dev/null \
    || echo "  (no consumer groups yet)"
fi
