#!/usr/bin/env bash
# Создание топиков и ACL основного кластера. Выполняется внутри контейнера kafka-tools
# от имени администратора (CN=admin, super user):
#   docker compose exec -T kafka-tools bash /scripts/init-topics-acl.sh
# Скрипт идемпотентен: повторный запуск ничего не ломает.
set -euo pipefail

BS="${PRIMARY_BOOTSTRAP:-kafka-1:19092,kafka-2:19092,kafka-3:19092}"
ADMIN=/etc/kafka/clients/admin.properties

topics() { kafka-topics --bootstrap-server "$BS" --command-config "$ADMIN" "$@"; }
acl()    { kafka-acls   --bootstrap-server "$BS" --command-config "$ADMIN" "$@" >/dev/null; }
user()   { echo "User:CN=$1"; }

# create <topic> [доп. --config ...]
create() {
  local name=$1; shift
  topics --create --if-not-exists --topic "$name" \
    --partitions 3 --replication-factor 3 \
    --config min.insync.replicas=2 "$@"
}

echo "== Топики (3 партиции, RF=3, min.insync.replicas=2)"
create shop-products      --config retention.ms=604800000      # 7 дней
create shop-products-dlq  --config retention.ms=1209600000     # 14 дней на разбор
create products-filtered  --config retention.ms=604800000
create banned-products    --config cleanup.policy=compact      # последнее состояние по каждому product_id
create client-requests    --config retention.ms=1209600000
create orders             --config retention.ms=2592000000     # 30 дней
create recommendations    --config cleanup.policy=compact      # последние рекомендации по каждому user_id

# allow <principal> <ресурс...> -- <операции...>
allow_topic() {
  local who=$1 topic=$2; shift 2
  local ops=()
  for op in "$@"; do ops+=(--operation "$op"); done
  acl --add --allow-principal "$(user "$who")" "${ops[@]}" --topic "$topic"
}
allow_topic_prefix() {
  local who=$1 prefix=$2; shift 2
  local ops=()
  for op in "$@"; do ops+=(--operation "$op"); done
  acl --add --allow-principal "$(user "$who")" "${ops[@]}" --topic "$prefix" --resource-pattern-type prefixed
}
allow_group() {
  local who=$1 group=$2 type=$3; shift 3
  local ops=()
  for op in "$@"; do ops+=(--operation "$op"); done
  acl --add --allow-principal "$(user "$who")" "${ops[@]}" --group "$group" --resource-pattern-type "$type"
}

echo "== ACL"
# SHOP API: только запись товаров
allow_topic shop-api shop-products Write Describe

# Faust (потоковая фильтрация). Create на существующих топиках нужен потому, что Faust
# при старте объявляет свои топики; топик уже есть, поэтому ничего не создаётся.
allow_topic moderation shop-products      Read Describe Create
allow_topic moderation banned-products    Read Describe Create
allow_topic moderation products-filtered  Write Describe Create
allow_topic moderation shop-products-dlq  Write Describe Create
allow_topic moderation orders             Read Describe Create   # учёт резервов по заказам
allow_topic_prefix moderation faust-moderation- All           # changelog таблицы и служебные топики Faust
allow_group moderation faust-moderation prefixed Read Describe

# CLI управления списком запрещённых товаров
allow_topic moderator banned-products Write Read Describe
allow_group moderator moderator- prefixed Read Describe

# CLIENT API: только запись запросов клиентов и заказов
allow_topic client-api client-requests Write Describe
allow_topic client-api orders          Write Describe

# Kafka Connect: чтение топиков для хранилища + свои служебные топики и группы (connect-*)
allow_topic connect products-filtered Read Describe
allow_topic connect client-requests   Read Describe
allow_topic connect orders            Read Describe
allow_topic connect recommendations   Read Describe
allow_topic_prefix connect connect- All
allow_group connect connect- prefixed Read Describe

# MirrorMaker 2: только чтение реплицируемых топиков (в primary он ничего не пишет)
for t in shop-products products-filtered client-requests orders; do
  allow_topic mirror-maker "$t" Read Describe DescribeConfigs
done
# Группы MM2 в primary не нужны: офсеты не коммитятся, checkpoints отключены

# Spark (аналитика): только запись рекомендаций
allow_topic analytics recommendations Write Describe

# Бизнес-топики, которые разрешено просматривать мониторингу и Kafka UI (без wildcard '*')
BUSINESS_TOPICS="shop-products products-filtered shop-products-dlq banned-products client-requests orders recommendations"
# Группы потребителей платформы (для lag в мониторинге)
GROUP_PREFIXES="faust-moderation connect-"

# Kafka UI: только просмотр бизнес-топиков и групп платформы
acl --add --allow-principal "$(user kafka-ui)" --operation Describe --operation DescribeConfigs --cluster
for t in $BUSINESS_TOPICS; do
  allow_topic kafka-ui "$t" Read Describe DescribeConfigs
done
for g in $GROUP_PREFIXES; do
  allow_group kafka-ui "$g" prefixed Describe
done
allow_group kafka-ui kafka-ui prefixed Read Describe   # служебные группы самого Kafka UI

# kafka-exporter (мониторинг lag): Describe топиков и групп платформы
for t in $BUSINESS_TOPICS; do
  allow_topic kafka-exporter "$t" Describe
done
for g in $GROUP_PREFIXES; do
  allow_group kafka-exporter "$g" prefixed Describe
done

# Удаляем широкие права, если они остались от предыдущей версии скрипта
acl --remove --force --allow-principal "$(user kafka-ui)" --operation Read --operation Describe --operation DescribeConfigs --topic '*' || true
acl --remove --force --allow-principal "$(user kafka-ui)" --operation Read --operation Describe --group '*' || true
acl --remove --force --allow-principal "$(user mirror-maker)" --operation Describe --group '*' || true

echo
echo "== Итог: топики"
topics --describe --exclude-internal | grep PartitionCount || true
echo
echo "== Итог: ACL"
kafka-acls --bootstrap-server "$BS" --command-config "$ADMIN" --list
