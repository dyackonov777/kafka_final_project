#!/usr/bin/env bash
# Подъём всей платформы одной командой:  bash start.sh
#
#  1. .env и TLS-сертификаты
#  2. сборка образов
#  3. оба кластера Kafka -> топики и ACL (primary), ACL (analytics)
#  4. остальные сервисы; docker compose ждёт healthcheck каждого сервиса
#  5. Elasticsearch (пользователи, роли, индексы) и коннекторы Kafka Connect
#  6. начальный список запрещённых товаров
# Повторный запуск безопасен: все шаги идемпотентны.
source "$(dirname "$0")/scripts/common.sh"

step "1/6 Конфигурация и сертификаты"
if [ ! -f .env ]; then
  cp .env.example .env
  echo "  создан .env из .env.example — поменяйте пароли перед использованием вне учебного стенда"
  set -a; source <(tr -d '\r' < .env); set +a
fi
bash scripts/generate-certs.sh

step "2/6 Сборка образов"
dc build

step "3/6 Кластеры Kafka, топики и ACL"
dc up -d --wait kafka-1 kafka-2 kafka-3 kafka-b1 kafka-b2 kafka-b3 kafka-tools
dc exec -T kafka-tools bash /scripts/init-topics-acl.sh
dc exec -T kafka-tools bash /scripts/init-analytics-acl.sh

step "4/6 Остальные сервисы (первый запуск занимает несколько минут)"
dc up -d --wait

step "5/6 Хранилище: Elasticsearch и коннекторы"
bash scripts/init-elasticsearch.sh
bash scripts/register-connectors.sh

step "6/6 Начальный список запрещённых товаров"
dc exec -T moderator-cli python -m marketplace.banned_cli load --file data/banned-products.json

cat <<EOF

Платформа запущена. Все адреса доступны только с этой машины (BIND_ADDRESS=${BIND_ADDRESS:-127.0.0.1}).
  Kafka UI ............ http://localhost:8080   (${KAFKA_UI_USER:-admin} / KAFKA_UI_PASSWORD из .env)
  Grafana ............. http://localhost:3000   (${GRAFANA_ADMIN_USER:-admin} / GRAFANA_ADMIN_PASSWORD из .env)
  Prometheus .......... http://localhost:9090/alerts
  Alertmanager ........ http://localhost:9099
  Kafka Connect ....... http://localhost:8083/connectors
  Elasticsearch ....... http://localhost:9200   (elastic / ELASTIC_PASSWORD из .env)
  HDFS NameNode ....... http://localhost:9870
  Spark Master ........ http://localhost:8090

Дальше:
  bash scripts/demo.sh             # сквозной сценарий
  bash scripts/check-security.sh   # проверка TLS и ACL
  bash cli.sh help                 # команды SHOP API / CLIENT API / списка запрещённых товаров
EOF
