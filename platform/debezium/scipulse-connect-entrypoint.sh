#!/bin/sh
# Writes the Debezium database credentials for the FileConfigProvider from DEBEZIUM_DB_USER and
# DEBEZIUM_DB_PASSWORD into /secrets/debezium.properties, then starts the stock Debezium entrypoint.
# The credentials live only in the container filesystem; nothing secret is committed (§4.4).
set -eu

: "${DEBEZIUM_DB_USER:?DEBEZIUM_DB_USER must be set}"
: "${DEBEZIUM_DB_PASSWORD:?DEBEZIUM_DB_PASSWORD must be set}"

# Escape the characters that are special in Java .properties values.
escape() {
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/[=:#! ]/\\&/g'
}

umask 077
{
  printf 'user=%s\n' "$(escape "$DEBEZIUM_DB_USER")"
  printf 'password=%s\n' "$(escape "$DEBEZIUM_DB_PASSWORD")"
} > /secrets/debezium.properties

exec /docker-entrypoint.sh "$@"
