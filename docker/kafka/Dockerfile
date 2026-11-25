# syntax=docker/dockerfile:1
# Брокер Kafka + Prometheus JMX Exporter (java-агент).
# Агент подключается через KAFKA_OPTS в docker-compose.yml и отдаёт метрики на :7071/metrics.
FROM confluentinc/cp-kafka:7.6.1

ARG JMX_EXPORTER_VERSION=0.20.0

USER root

RUN mkdir -p /opt/jmx-exporter

ADD --chmod=644 \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar \
    /opt/jmx-exporter/jmx_prometheus_javaagent.jar

# Проверка, что jar реально скачался и валидный
RUN ls -lh /opt/jmx-exporter/jmx_prometheus_javaagent.jar && \
    test -s /opt/jmx-exporter/jmx_prometheus_javaagent.jar && \
    jar tf /opt/jmx-exporter/jmx_prometheus_javaagent.jar > /dev/null

USER appuser