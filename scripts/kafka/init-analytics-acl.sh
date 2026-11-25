
#!/usr/bin/env bash
# ACL аналитического кластера (kafka-b1..3).
# Выполняется внутри kafka-tools от имени admin:
#   docker compose exec -T kafka-tools bash /scripts/init-analytics-acl.sh
#
# Топики в analytics создаёт MirrorMaker 2:
#   primary.shop-products
#   primary.products-filtered
#   primary.client-requests
#   primary.orders
#
# Остальным клиентам разрешено только чтение копий.
# Скрипт идемпотентен: повторный запуск не ломает существующие ACL.

set -euo pipefail

BS="${ANALYTICS_BOOTSTRAP:-kafka-b1:19092,kafka-b2:19092,kafka-b3:19092}"
ADMIN=/etc/kafka/clients/admin.properties

acl() {
  kafka-acls \
    --bootstrap-server "$BS" \
    --command-config "$ADMIN" \
    "$@" >/dev/null
}

user() {
  echo "User:CN=$1"
}

echo "== ACL аналитического кластера"

# ============================================================
# MirrorMaker 2
# ============================================================

echo "== MirrorMaker 2"

# Разрешаем создание топиков на уровне кластера.
acl --add \
    --allow-principal "$(user mirror-maker)" \
    --operation Create \
    --cluster

# Права на реплицируемые топики primary.*
acl --add \
    --allow-principal "$(user mirror-maker)" \
    --operation Read \
    --operation Write \
    --operation Describe \
    --operation DescribeConfigs \
    --operation Alter \
    --topic primary. \
    --resource-pattern-type prefixed

# ============================================================
# Spark (загрузка в HDFS)
# ============================================================

echo "== Spark"

# Чтение реплицированных топиков.
acl --add \
    --allow-principal "$(user analytics)" \
    --operation Read \
    --operation Describe \
    --topic primary. \
    --resource-pattern-type prefixed

# Consumer groups Spark.
acl --add \
    --allow-principal "$(user analytics)" \
    --operation Read \
    --operation Describe \
    --group spark-kafka-source \
    --resource-pattern-type prefixed

# ============================================================
# Kafka Exporter
# ============================================================

echo "== Kafka Exporter"

# Просмотр метаданных реплицированных топиков.
acl --add \
    --allow-principal "$(user kafka-exporter)" \
    --operation Describe \
    --topic primary. \
    --resource-pattern-type prefixed

# ============================================================
# Kafka UI
# ============================================================

echo "== Kafka UI"

# Просмотр метаданных кластера.
acl --add \
    --allow-principal "$(user kafka-ui)" \
    --operation Describe \
    --operation DescribeConfigs \
    --cluster

# Просмотр реплицированных топиков.
acl --add \
    --allow-principal "$(user kafka-ui)" \
    --operation Read \
    --operation Describe \
    --operation DescribeConfigs \
    --topic primary. \
    --resource-pattern-type prefixed

# Служебные consumer groups Kafka UI.
acl --add \
    --allow-principal "$(user kafka-ui)" \
    --operation Read \
    --operation Describe \
    --group kafka-ui \
    --resource-pattern-type prefixed

# ============================================================
# Итог
# ============================================================

echo
echo "== Итог: ACL аналитического кластера"

kafka-acls \
    --bootstrap-server "$BS" \
    --command-config "$ADMIN" \
    --list

echo
echo "== ACL успешно применены"
