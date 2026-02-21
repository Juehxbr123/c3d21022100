#!/usr/bin/env sh
set -eu

echo "[bot] Waiting for MySQL at ${MYSQL_HOST}:${MYSQL_PORT}..."
for i in $(seq 1 60); do
  if nc -z "$MYSQL_HOST" "$MYSQL_PORT"; then
    echo "[bot] MySQL port is reachable"
    exec python bot.py
  fi
  echo "[bot] MySQL is not ready yet ($i/60)"
  sleep 2
done

echo "[bot] MySQL did not become available in time"
exit 1
