# syntax=docker/dockerfile:1
# Kafka image (KRaft mode) for the KOL platform, with the Prometheus JMX exporter attached.
ARG KAFKA_VERSION=3.9.0
FROM apache/kafka:${KAFKA_VERSION}

ARG JMX_EXPORTER_VERSION=1.0.1

USER root

RUN mkdir -p /opt/jmx-exporter /var/lib/kafka/data \
    && chown -R appuser:root /var/lib/kafka

ADD --chmod=644 \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar \
    /opt/jmx-exporter/jmx_prometheus_javaagent.jar
COPY --chmod=644 jmx-exporter.yml /opt/jmx-exporter/kafka.yml
COPY --chmod=755 entrypoint.sh /usr/local/bin/kol-kafka-entrypoint.sh

USER appuser

# 9092/19092: client listeners, 19093/9093: KRaft controller, 7071: Prometheus metrics
EXPOSE 9092 19092 19093 9093 7071

ENTRYPOINT ["/usr/local/bin/kol-kafka-entrypoint.sh"]
