#!/usr/bin/env bash
# Проверка TLS и ACL основного кластера. Выполняется внутри kafka-tools:
#   docker compose exec -T kafka-tools bash /scripts/check-security.sh
# Каждая проверка печатает OK, если поведение совпало с ожидаемым.
set -uo pipefail

BS="${BS:-kafka-1:19092}"
CLIENTS=/etc/kafka/clients
pass=0; fail=0

result() {  # result <ok:0|1> <описание>
  if [ "$1" -eq 0 ]; then echo "  [OK]   $2"; pass=$((pass+1)); else echo "  [FAIL] $2"; fail=$((fail+1)); fi
}

# Пишем одно сообщение от имени клиента; возвращаем вывод
produce_as() {  # produce_as <client> <topic>
  echo '{"check":"security"}' | timeout 40 kafka-console-producer \
    --bootstrap-server "$BS" --topic "$2" \
    --producer.config "$CLIENTS/$1.properties" \
    --producer-property max.block.ms=15000 \
    --producer-property delivery.timeout.ms=20000 \
    --producer-property request.timeout.ms=10000 2>&1
}

consume_as() {  # consume_as <client> <topic> <group>
  timeout 40 kafka-console-consumer \
    --bootstrap-server "$BS" --topic "$2" --group "$3" \
    --consumer.config "$CLIENTS/$1.properties" \
    --from-beginning --max-messages 1 --timeout-ms 15000 2>&1
}

echo "== 1. TLS: подключение без клиентского сертификата отклоняется (ssl.client.auth=required)"
out=$(timeout 40 kafka-broker-api-versions --bootstrap-server "$BS" \
        --command-config "$CLIENTS/no-cert.properties" 2>&1)
echo "$out" | grep -qiE "SSL|handshake|certificate|Connection to node .* failed|TimeoutException|disconnected" \
  && ! echo "$out" | grep -q "ApiVersion"
result $? "клиент без сертификата не получил доступ к брокеру"

echo "== 2. TLS: подключение по PLAINTEXT к TLS-порту невозможно"
out=$(timeout 30 kafka-broker-api-versions --bootstrap-server "$BS" \
        --command-config /dev/null 2>&1)
! echo "$out" | grep -q "ApiVersion"
result $? "PLAINTEXT-клиент не получил ответ брокера"

echo "== 3. ACL: client-api НЕ может писать в shop-products (топик закрыт для чужих клиентов)"
out=$(produce_as client-api shop-products)
echo "$out" | grep -qiE "TopicAuthorization|not authorized"
result $? "запись client-api -> shop-products запрещена"

echo "== 4. ACL: shop-api НЕ может писать в products-filtered (туда пишет только фильтр Faust)"
out=$(produce_as shop-api products-filtered)
echo "$out" | grep -qiE "TopicAuthorization|not authorized"
result $? "запись shop-api -> products-filtered запрещена"

echo "== 5. ACL: shop-api НЕ может читать shop-products"
out=$(consume_as shop-api shop-products shop-api-check)
echo "$out" | grep -qiE "Authorization|not authorized"
result $? "чтение shop-api <- shop-products запрещено"

echo "== 6. ACL: mirror-maker НЕ может писать в основной кластер"
out=$(produce_as mirror-maker shop-products)
echo "$out" | grep -qiE "TopicAuthorization|not authorized"
result $? "запись mirror-maker -> shop-products запрещена"

echo "== 7. ACL: client-api МОЖЕТ писать в client-requests"
out=$(produce_as client-api client-requests)
! echo "$out" | grep -qiE "Authorization|not authorized|ERROR"
result $? "запись client-api -> client-requests разрешена"

echo "== 7a. ACL: shop-api НЕ может писать в orders"
out=$(produce_as shop-api orders)
echo "$out" | grep -qiE "TopicAuthorization|not authorized"
result $? "запись shop-api -> orders запрещена"

echo "== 8. Репликация: у топиков RF=3 и min.insync.replicas=2"
desc=$(kafka-topics --bootstrap-server "$BS" --command-config "$CLIENTS/admin.properties" \
         --describe --topic shop-products 2>/dev/null)
echo "$desc" | grep PartitionCount
echo "$desc" | grep -q "ReplicationFactor: 3" && echo "$desc" | grep -q "min.insync.replicas=2"
result $? "shop-products: ReplicationFactor 3, min.insync.replicas=2"

echo "== 9. Аналитический кластер: подключение без клиентского сертификата отклоняется"
out=$(timeout 40 kafka-broker-api-versions --bootstrap-server kafka-b1:19092 \
        --command-config "$CLIENTS/no-cert.properties" 2>&1)
! echo "$out" | grep -q "ApiVersion"
result $? "клиент без сертификата не получил доступ к kafka-b1"

echo "== 10. Аналитический кластер: analytics (Spark) НЕ может писать в копии топиков"
out=$(BS=kafka-b1:19092 produce_as analytics primary.orders)
echo "$out" | grep -qiE "TopicAuthorization|not authorized"
result $? "запись analytics -> primary.orders запрещена (копии пишет только MirrorMaker 2)"

echo "== 11. Аналитический кластер: копии имеют RF=3"
desc=$(kafka-topics --bootstrap-server kafka-b1:19092 --command-config "$CLIENTS/admin.properties" \
         --describe --topic primary.orders 2>/dev/null)
echo "$desc" | grep PartitionCount
echo "$desc" | grep -q "ReplicationFactor: 3"
result $? "primary.orders: ReplicationFactor 3"

echo
echo "Итог: успешно $pass, ошибок $fail"
[ "$fail" -eq 0 ]
