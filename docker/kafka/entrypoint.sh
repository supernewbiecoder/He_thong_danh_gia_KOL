#!/usr/bin/env bash
# Starts the Kafka broker with the Prometheus JMX exporter java agent attached.
#
# The agent is injected here instead of through a container-level KAFKA_OPTS so that
# CLI tools run with `docker exec` / `kubectl exec` (kafka-topics.sh, ...) do not try
# to bind the exporter port a second time and crash.
set -euo pipefail

JMX_EXPORTER_PORT="${JMX_EXPORTER_PORT:-7071}"
export KAFKA_OPTS="${KAFKA_OPTS:-} -javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=${JMX_EXPORTER_PORT}:/opt/jmx-exporter/kafka.yml"

exec /etc/kafka/docker/run "$@"
