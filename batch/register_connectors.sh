#!/bin/sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG_DIR="${CONFIG_DIR:-$(dirname "$0")}"

register_connector() {
  name="$1"
  file="$2"

  echo "Registering ${name} from ${file}"
  curl -fsS -X PUT "${CONNECT_URL}/connectors/${name}/config" \
    -H "Content-Type: application/json" \
    --data-binary "@${file}"
  echo
}

register_connector "s3-sink-weather-realtime" "${CONFIG_DIR}/config.json"
register_connector "s3-sink-weather-forecast" "${CONFIG_DIR}/config_forecast.json"
register_connector "s3-sink-pollution" "${CONFIG_DIR}/config_pollution.json"
