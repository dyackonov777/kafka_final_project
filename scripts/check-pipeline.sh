#!/usr/bin/env bash
# Проверка движения данных по платформе: сообщения в топиках обоих кластеров,
# содержимое хранилища (Elasticsearch, файл), статус коннекторов, DLQ, Data Lake.
source "$(dirname "$0")/common.sh"

step "Основной кластер (primary): сообщений в топиках (последний офсет)"
for t in shop-products products-filtered shop-products-dlq banned-products client-requests orders recommendations; do
  printf '  %-22s %s\n' "$t" "$(topic_end_offset "$t")"
done
echo "  products-filtered содержит и обновления остатков после заказов, и tombstone при запретах,"
echo "  поэтому он может быть больше числа разрешённых товаров."

step "Аналитический кластер (analytics): копии MirrorMaker 2"
kafka_admin_analytics kafka-topics --list 2>/dev/null | grep '^primary\.' || true
for t in shop-products products-filtered client-requests orders; do
  printf '  %-28s primary=%-6s analytics=%s\n' "primary.$t" "$(topic_end_offset "$t")" "$(analytics_topic_end_offset "primary.$t")"
done

step "Фильтрация: ID товаров в Elasticsearch и запрещённые ID"
es_curl "$ES_URL/products-filtered/_search?size=100&_source=false" \
  | grep -o '"_id":"[^"]*"' | cut -d'"' -f4 | sort | tr '\n' ' '
echo
echo "  запрещённые (data/banned-products.json): $(grep -o '"product_id": *"[^"]*"' data/banned-products.json | grep -o '[0-9]\+' | tr '\n' ' ')"
echo "  текущий список запрещённых:"
bash cli.sh banned list 2>/dev/null | sed 's/^/  /'

step "Хранилище: Elasticsearch"
es_curl "$ES_URL/_cat/indices/products-filtered,client-requests,orders,recommendations?v&h=index,health,docs.count" || true

step "Хранилище: файл FileStream Sink (/data/sink/products-filtered.jsonl)"
dc exec -T kafka-connect sh -c 'echo "  строк: $(wc -l < /data/sink/products-filtered.jsonl)"; head -c 300 /data/sink/products-filtered.jsonl; echo' || true

step "Коннекторы"
for c in elasticsearch-sink file-sink; do
  curl -fsS "$CONNECT_URL/connectors/$c/status" || true; echo
done

step "Lag групп потребителей основного кластера"
kafka_admin kafka-consumer-groups --describe --group faust-moderation 2>/dev/null \
  | awk 'NF >= 6 {printf "  %-18s %-36s %-9s %-8s %-8s %s\n", $1, $2, $3, $4, $5, $6}' || true

step "DLQ потоковой обработки (shop-products-dlq)"
dc exec -T kafka-tools kafka-console-consumer --bootstrap-server "$PRIMARY_BS" \
  --consumer.config /etc/kafka/clients/admin.properties --topic shop-products-dlq \
  --from-beginning --timeout-ms 8000 2>/dev/null | head -5 || true

step "Data Lake (HDFS)"
dc exec -T namenode hdfs dfs -du -h /datalake/raw /datalake/marts 2>/dev/null || true
