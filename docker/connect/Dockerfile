# syntax=docker/dockerfile:1
# Kafka Connect: Elasticsearch Sink + FileStream + JMX Exporter
FROM confluentinc/cp-kafka-connect:7.6.1

ARG ES_CONNECTOR_VERSION=15.0.0
ARG JMX_EXPORTER_VERSION=0.20.0

USER root

# --- JMX Exporter ---
RUN mkdir -p /opt/jmx-exporter

ADD --chmod=644 \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar \
    /opt/jmx-exporter/jmx_prometheus_javaagent.jar

# Проверка JAR
RUN ls -lh /opt/jmx-exporter/jmx_prometheus_javaagent.jar && \
    test -s /opt/jmx-exporter/jmx_prometheus_javaagent.jar && \
    jar tf /opt/jmx-exporter/jmx_prometheus_javaagent.jar > /dev/null

# --- Elasticsearch Sink Connector ---
RUN confluent-hub install --no-prompt \
      --component-dir /usr/share/confluent-hub-components \
      --worker-configs /dev/null \
      confluentinc/kafka-connect-elasticsearch:${ES_CONNECTOR_VERSION}

# FileStream connectors уже есть в /usr/share/filestream-connectors
# Каталог для FileStreamSink
RUN mkdir -p /data/sink && chown appuser:appuser /data/sink

USER appuser
