#!/usr/bin/env bash
# Проверка TLS и ACL основного кластера (запускается в контейнере kafka-tools).
source "$(dirname "$0")/common.sh"

step "Проверка TLS и ACL"
dc exec -T kafka-tools bash /scripts/check-security.sh

step "Сертификат брокера kafka-1 (выдан нашим CA, SAN = имя в сети + localhost)"
openssl x509 -in certs/kafka-1/kafka-1.crt -noout -subject -issuer -ext subjectAltName 2>/dev/null \
  || openssl x509 -in certs/kafka-1/kafka-1.crt -noout -subject -issuer
