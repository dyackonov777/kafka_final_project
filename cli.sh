#!/usr/bin/env bash
# Единая точка входа для CLI-команд платформы (выполняются в контейнерах *-cli).
#
#   bash cli.sh shop send [--file data/products.json] [--delay 0.2] [--inject-bad N]
#   bash cli.sh client search "<название>" --user-id user_001
#   bash cli.sh client recommend --user-id user_001
#   bash cli.sh client history --user-id user_001
#   bash cli.sh client buy --user-id user_001 --product-id 12345 [--quantity 2]
#   bash cli.sh client orders --user-id user_001
#   bash cli.sh banned add --product-id 12350 --reason "Причина"
#   bash cli.sh banned remove --product-id 12350
#   bash cli.sh banned list
#   bash cli.sh banned load [--file data/banned-products.json]
source "$(dirname "$0")/scripts/common.sh"

usage() { sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; }

group=${1:-help}
shift || true
# У каждой группы команд свой контейнер и свой сертификат (CN=shop-api / client-api / moderator)
case "$group" in
  shop)   container=shop-api-cli;   module=marketplace.shop_api ;;
  client) container=client-api-cli; module=marketplace.client_api ;;
  banned) container=moderator-cli;  module=marketplace.banned_cli ;;
  help|-h|--help) usage; exit 0 ;;
  *) echo "Неизвестная группа команд: $group" >&2; usage; exit 2 ;;
esac

dc exec -T "$container" python -m "$module" "$@"
