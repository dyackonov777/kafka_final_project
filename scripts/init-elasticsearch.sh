#!/usr/bin/env bash
# Elasticsearch: роли, пользователи и индексы с явными маппингами
# (выполняется до регистрации коннектора, чтобы маппинги были на месте до первых данных).
#
# Пользователи (пароли в .env):
#   kafka_connect — Kafka Connect: запись в индексы платформы
#   client_api    — CLIENT API: только чтение
# Индексы:
#   products-filtered — товары после фильтрации (поиск CLIENT API по названию)
#   client-requests   — запросы клиентов
#   orders            — заказы клиентов
#   recommendations   — рекомендации от Spark
source "$(dirname "$0")/common.sh"

INDICES='["products-filtered", "client-requests", "orders", "recommendations"]'

wait_until "Elasticsearch" 180 es_curl -o /dev/null "$ES_URL/_cluster/health?wait_for_status=yellow&timeout=5s"

echo "  роли и пользователи"
es_curl -o /dev/null -X PUT "$ES_URL/_security/role/marketplace_writer" -H 'Content-Type: application/json' -d "{
  \"cluster\": [\"monitor\"],
  \"indices\": [{\"names\": $INDICES, \"privileges\": [\"all\"]}]
}"
es_curl -o /dev/null -X PUT "$ES_URL/_security/role/marketplace_reader" -H 'Content-Type: application/json' -d "{
  \"indices\": [{\"names\": $INDICES, \"privileges\": [\"read\", \"view_index_metadata\"]}]
}"
# PUT пользователя идемпотентен: при повторном запуске обновит пароль из .env
es_curl -o /dev/null -X PUT "$ES_URL/_security/user/kafka_connect" -H 'Content-Type: application/json' -d "{
  \"password\": \"${ES_CONNECT_PASSWORD}\", \"roles\": [\"marketplace_writer\"], \"full_name\": \"Kafka Connect\"
}"
es_curl -o /dev/null -X PUT "$ES_URL/_security/user/client_api" -H 'Content-Type: application/json' -d "{
  \"password\": \"${ES_CLIENT_PASSWORD}\", \"roles\": [\"marketplace_reader\"], \"full_name\": \"CLIENT API\"
}"
echo "  [+] роли marketplace_writer / marketplace_reader, пользователи kafka_connect / client_api"

for f in config/elasticsearch/*.json; do
  index=$(basename "$f" .json)
  if es_curl -o /dev/null "$ES_URL/$index" 2>/dev/null; then
    echo "  [=] индекс $index уже существует"
    continue
  fi
  es_curl -o /dev/null -X PUT "$ES_URL/$index" -H 'Content-Type: application/json' --data-binary "@$f"
  echo "  [+] индекс $index создан"
done

es_curl "$ES_URL/_cat/indices/products-filtered,client-requests,orders,recommendations?v&h=index,health,docs.count"
