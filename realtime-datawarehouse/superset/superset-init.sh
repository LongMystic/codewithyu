#!/bin/bash
set -e

superset fab create-admin \
    --username "$ADMIN_USERNAME" \
    --firstname Superset \
    --lastname Admin \
    --email "$ADMIN_EMAIL" \
    --password "$ADMIN_PASSWORD" || true

superset db upgrade

superset init

exec /usr/bin/run-server.sh
