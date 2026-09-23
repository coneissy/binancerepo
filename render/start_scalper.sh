#!/usr/bin/env bash
set -euo pipefail

: "${HBOT_PASSWORD:?HBOT_PASSWORD is required}"
: "${BINANCE_API_KEY:?BINANCE_API_KEY is required}"
: "${BINANCE_API_SECRET:?BINANCE_API_SECRET is required}"

mkdir -p /home/hummingbot/conf/connectors /home/hummingbot/conf/controllers /home/hummingbot/conf/scripts /home/hummingbot/logs /home/hummingbot/data

export PYTHONPATH="/home/hummingbot:/home/hummingbot/controllers:${PYTHONPATH:-}"

python /home/hummingbot/render/bootstrap_binance_credentials.py

exec hbot start binance_futures_scalper.yml --foreground
