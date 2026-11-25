#!/usr/bin/env bash
# Генерация собственного CA, сертификатов брокеров обоих кластеров и клиентов.
#
#   bash scripts/generate-certs.sh          # создать certs/, если его ещё нет
#   bash scripts/generate-certs.sh --force  # пересоздать всё заново
#
# Пароль keystore/truststore берётся из .env (KEYSTORE_PASSWORD).
# Требуется OpenSSL >= 3.2 (опция -jdktrust для Java-truststore в формате PKCS12).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CERTS="$ROOT/certs"
DAYS=3650

# --- пароль из .env ---
if [ -f "$ROOT/.env" ]; then
  # shellcheck disable=SC1091
  set -a; source <(tr -d '\r' < "$ROOT/.env"); set +a
fi
PASS="${KEYSTORE_PASSWORD:?KEYSTORE_PASSWORD не задан: создайте .env из .env.example}"

# Брокеры основного (kafka-1..3) и аналитического (kafka-b1..3) кластеров, общий CA
BROKERS=(kafka-1 kafka-2 kafka-3 kafka-b1 kafka-b2 kafka-b3)
# Каждый сервис ходит в Kafka со своим сертификатом. CN сертификата = principal в ACL (User:CN=<имя>)
CLIENTS=(admin shop-api client-api moderation moderator connect mirror-maker analytics kafka-ui kafka-exporter)

# --- проверка версии OpenSSL ---
ver=$(openssl version | awk '{print $2}')
major=${ver%%.*}; rest=${ver#*.}; minor=${rest%%.*}
if [ "$major" -lt 3 ] || { [ "$major" -eq 3 ] && [ "$minor" -lt 2 ]; }; then
  echo "Нужен OpenSSL >= 3.2, найден $ver" >&2
  exit 1
fi

if [ -e "$CERTS" ]; then
  if [ "${1:-}" = "--force" ]; then
    rm -rf "$CERTS"
  else
    echo "Каталог certs/ уже существует, пропускаю генерацию (--force для пересоздания)."
    exit 0
  fi
fi
mkdir -p "$CERTS/clients"
cd "$CERTS"

# ---------- 1. Корневой CA ----------
cat > ca.cnf <<'EOF'
[ req ]
prompt = no
distinguished_name = dn
default_md = sha256
x509_extensions = v3_ca

[ dn ]
countryName = KZ
organizationName = Pokupay Vygodno
organizationalUnitName = Analytics Platform
commonName = marketplace-root-ca

[ v3_ca ]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer:always
basicConstraints = critical,CA:true
keyUsage = critical,keyCertSign,cRLSign
EOF

openssl req -new -x509 -nodes -newkey rsa:4096 -days "$DAYS" \
  -config ca.cnf -keyout ca.key -out ca.crt 2>/dev/null

# Общий truststore для Java-клиентов (Kafka CLI, Connect, MirrorMaker 2, Spark, Kafka UI)
openssl pkcs12 -export -nokeys -in ca.crt -name ca -jdktrust anyExtendedKeyUsage \
  -out clients/client.truststore.p12 -passout "pass:$PASS"

# ---------- Выпуск сертификата ----------
# $1 - каталог, $2 - CN, $3 - extendedKeyUsage, $4 - subjectAltName (может быть пустым)
issue() {
  local dir=$1 cn=$2 eku=$3 san=${4:-}
  mkdir -p "$dir"
  {
    echo "[ req ]"
    echo "prompt = no"
    echo "distinguished_name = dn"
    echo "default_md = sha256"
    echo "req_extensions = v3_req"
    echo
    echo "[ dn ]"
    echo "commonName = $cn"
    echo
    echo "[ v3_req ]"
    echo "subjectKeyIdentifier = hash"
    echo "basicConstraints = CA:FALSE"
    echo "keyUsage = critical,digitalSignature,keyEncipherment"
    echo "extendedKeyUsage = $eku"
    if [ -n "$san" ]; then
      echo "subjectAltName = $san"
    fi
  } > "$dir/$cn.cnf"

  openssl req -new -nodes -newkey rsa:2048 -config "$dir/$cn.cnf" \
    -keyout "$dir/$cn.key" -out "$dir/$cn.csr" 2>/dev/null
  openssl x509 -req -in "$dir/$cn.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days "$DAYS" -extfile "$dir/$cn.cnf" -extensions v3_req -out "$dir/$cn.crt" 2>/dev/null
  rm -f "$dir/$cn.csr"

  # Keystore для Java-клиентов: ключ + сертификат + цепочка до CA.
  # Python-клиенты (confluent-kafka, aiokafka) используют PEM-файлы .crt/.key напрямую.
  openssl pkcs12 -export -in "$dir/$cn.crt" -inkey "$dir/$cn.key" \
    -chain -CAfile ca.crt -name "$cn" \
    -out "$dir/$cn.keystore.p12" -passout "pass:$PASS"

  openssl verify -CAfile ca.crt "$dir/$cn.crt" >/dev/null
  echo "  [ok] $cn"
}

# Свойства Java-клиента (пути внутри контейнеров: certs/clients смонтирован в /etc/kafka/clients)
client_properties() {
  local cn=$1
  cat > "clients/$cn.properties" <<EOF
# Клиент CN=$cn (сгенерировано scripts/generate-certs.sh)
security.protocol=SSL
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/clients/client.truststore.p12
ssl.truststore.password=$PASS
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/clients/$cn/$cn.keystore.p12
ssl.keystore.password=$PASS
ssl.key.password=$PASS
ssl.endpoint.identification.algorithm=https
EOF
}

echo "== Брокеры"
for b in "${BROKERS[@]}"; do
  # SAN: имя в docker-сети + localhost для подключения с хоста
  issue "$b" "$b" "serverAuth,clientAuth" "DNS:$b,DNS:localhost"
  # Образ confluentinc/cp-kafka ждёт файлы с одинаковыми именами в /etc/kafka/secrets
  mv "$b/$b.keystore.p12" "$b/broker.keystore.p12"
  openssl pkcs12 -export -nokeys -in ca.crt -name ca -jdktrust anyExtendedKeyUsage \
    -out "$b/broker.truststore.p12" -passout "pass:$PASS"
  printf '%s' "$PASS" > "$b/keystore_creds"
  printf '%s' "$PASS" > "$b/sslkey_creds"
  printf '%s' "$PASS" > "$b/truststore_creds"
done

echo "== Клиенты"
for c in "${CLIENTS[@]}"; do
  issue "clients/$c" "$c" "clientAuth"
  client_properties "$c"
done

# Клиент без сертификата: только truststore. Нужен для проверки, что mTLS обязателен.
cat > clients/no-cert.properties <<EOF
# Клиент БЕЗ клиентского сертификата (для проверки ssl.client.auth=required)
security.protocol=SSL
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/clients/client.truststore.p12
ssl.truststore.password=$PASS
EOF

rm -f ca.srl
# Контейнеры работают под разными UID (appuser=1000, spark=185, root), поэтому файлы делаем читаемыми.
# Это учебный стенд: в production ключи раздаются только своим сервисам и с правами 0400.
chmod -R a+rX "$CERTS"
echo "Готово: сертификаты в $CERTS"
