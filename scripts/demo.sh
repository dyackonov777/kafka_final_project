#!/usr/bin/env bash
# Сквозной сценарий работы платформы (запускать после bash start.sh):
#   SHOP API -> Kafka (primary) -> Faust-фильтр -> Connect -> Elasticsearch/файл
#   CLIENT API -> поиск в Elasticsearch + события и заказы в Kafka
#   MirrorMaker 2 -> analytics -> spark-ingest -> HDFS -> Spark -> recommendations -> Elasticsearch -> CLIENT API
#
# Вместо фиксированных пауз сценарий ждёт выполнения условий (документы в ES, офсеты копий и т.п.).
source "$(dirname "$0")/common.sh"

TOTAL=$(grep -c '"product_id"' data/products.json)
BANNED=$(grep -c '"product_id"' data/banned-products.json)
ALLOWED=$((TOTAL - BANNED))

step "1. Текущий список запрещённых товаров"
bash cli.sh banned list

step "2. SHOP API: магазины отправляют $TOTAL товаров из data/products.json (+1 битое сообщение)"
bash cli.sh shop send --inject-bad 1
wait_until "в Elasticsearch $ALLOWED разрешённых товаров" 120 es_count_at_least products-filtered "$ALLOWED"

step "3. Решения потокового фильтра (лог faust-moderation)"
dc logs --since 5m faust-moderation 2>/dev/null | grep -E "PASSED|BLOCKED|INVALID" | tail -25 || true

step "4. CLIENT API: поиск товаров по названию (запросы уходят в Kafka и Elasticsearch)"
bash cli.sh client search "умные часы" --user-id user_001
bash cli.sh client search "наушники" --user-id user_001
bash cli.sh client search "кроссовки" --user-id user_002
bash cli.sh client search "коврик для йоги" --user-id user_002
bash cli.sh client search "кофемашина" --user-id user_003
bash cli.sh client search "книга kafka" --user-id user_004
bash cli.sh client search "часы" --user-id user_004
# Запрещённый товар не находится: его нет в хранилище
bash cli.sh client search "электронная сигарета" --user-id user_005
bash cli.sh client recommend --user-id user_006

step "5. CLIENT API: заказы (топик orders -> Elasticsearch, резерв на складе, аналитика)"
bash cli.sh client buy --user-id user_001 --product-id 12345
bash cli.sh client buy --user-id user_002 --product-id 22001 --quantity 2
bash cli.sh client buy --user-id user_003 --product-id 33002
bash cli.sh client buy --user-id user_007 --product-id 22004
# Запрещённый товар купить нельзя: его нет в хранилище
bash cli.sh client buy --user-id user_005 --product-id 90001 || true
wait_until "4 заказа в Elasticsearch" 60 es_count_at_least orders 4
bash cli.sh client orders --user-id user_002
echo "  Остаток товара 22001 после заказа 2 шт. (Faust учёл резерв):"
es_curl "$ES_URL/products-filtered/_doc/22001?_source_includes=stock" | sed 's/.*"_source"/  /'
echo

step "6. Дублирование во второй кластер"
wait_until "MirrorMaker 2 скопировал данные в analytics" 120 \
  mirror_caught_up products-filtered client-requests orders

step "7. Аналитика: HDFS -> Spark -> топик recommendations"
bash scripts/run-analytics.sh
wait_until "рекомендации в Elasticsearch" 60 es_doc_exists recommendations user_001

step "8. CLIENT API: персональные рекомендации"
bash cli.sh client recommend --user-id user_001
bash cli.sh client recommend --user-id user_002
bash cli.sh client recommend --user-id user_006
bash cli.sh client recommend --user-id user_007
bash cli.sh client history --user-id user_001

step "9. Запрет товара, который уже есть в хранилище"
bash cli.sh banned add --product-id 12346 --reason "Проверка: товар удаляется из хранилища"
wait_until "товар 12346 удалён из Elasticsearch" 60 es_doc_missing products-filtered 12346
bash cli.sh client search "умные часы" --user-id user_001
bash cli.sh client buy --user-id user_001 --product-id 12346 || true
# В рекомендациях user_001 товар 12346 был — теперь он скрыт как недоступный
bash cli.sh client recommend --user-id user_001

step "10. Снятие запрета: Faust публикует последнюю версию товара снова"
bash cli.sh banned remove --product-id 12346
wait_until "товар 12346 вернулся в Elasticsearch" 60 es_doc_exists products-filtered 12346

step "11. Итоговое состояние данных"
bash scripts/check-pipeline.sh
