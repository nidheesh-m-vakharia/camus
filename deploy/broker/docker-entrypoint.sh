#!/bin/sh
# Start camus-broker. With no arguments, listen on 18001 and 9092 and form a
# one-node group. Compose and other callers pass the broker flags themselves.
set -eu

auto=0
if [ "$#" -eq 0 ]; then
    auto=1
    set -- \
        --node-id "${CAMUS_NODE_ID:-1}" \
        --http-addr "${CAMUS_HTTP_ADDR:-0.0.0.0:18001}" \
        --kafka-addr "${CAMUS_KAFKA_ADDR:-0.0.0.0:9092}" \
        --data-dir "${CAMUS_DATA_DIR:-/data}" \
        --dev-plaintext \
        --allow-non-loopback
fi

if [ "${CAMUS_AUTO_INIT:-$auto}" != "1" ]; then
    exec camus-broker "$@"
fi

camus-broker "$@" &
pid=$!
trap 'kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true' INT TERM

http_port=18001
prev=
for arg in "$@"; do
    if [ "$prev" = "--http-addr" ]; then
        http_port=${arg##*:}
    fi
    prev=$arg
done

i=0
while [ "$i" -lt 50 ]; do
    if curl -sf "http://127.0.0.1:${http_port}/admin/metrics" >/dev/null; then
        break
    fi
    i=$((i + 1))
    sleep 0.2
done

node_id=${CAMUS_NODE_ID:-1}
curl -sf -X POST "http://127.0.0.1:${http_port}/admin/init" \
    -H 'content-type: application/json' \
    -d "[{\"id\":${node_id},\"addr\":\"127.0.0.1:${http_port}\"}]" \
    || true

wait "$pid"
