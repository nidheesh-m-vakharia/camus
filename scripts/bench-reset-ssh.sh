#!/bin/sh
# Restart remote Camus brokers on empty data directories, for the benchmarks'
# remote mode (`BenchCluster` in crates/broker/tests/support). Use it as
# `CAMUS_BENCH_RESET`:
#
#   CAMUS_BENCH_HOSTS="1=10.0.0.11 2=10.0.0.12 3=10.0.0.13" \
#   CAMUS_BENCH_SSH="-i bench.pem" \
#   CAMUS_BENCH_RESET=scripts/bench-reset-ssh.sh \
#   CAMUS_BENCH_CLUSTER=1=10.0.0.11:9092=10.0.0.11:9100,... \
#   cargo test --release -p camus-broker --test latency -- --ignored --nocapture
#
# Each host needs `~/camus-broker` and a writable `/data`. Every `CAMUS_*`
# variable of this shell except `CAMUS_BENCH_*` is passed to the brokers, so
# the run's settings (`CAMUS_FLUSH_MS`) and the object store (`CAMUS_S3_*`)
# reach them. Kafka listens on 9092 and admin HTTP on 9100, on the host's
# address.
set -eu

: "${CAMUS_BENCH_HOSTS:?id=address list}"
ssh_opts="${CAMUS_BENCH_SSH:-} -o BatchMode=yes -o StrictHostKeyChecking=accept-new"
user="${CAMUS_BENCH_USER:-ubuntu}"

# `KEY='value' ...` for the broker's environment. Values are quoted for the
# remote shell; a value may not contain a single quote.
broker_env=$(env | grep '^CAMUS_' | grep -v '^CAMUS_BENCH_' | while IFS='=' read -r key value; do
    printf "%s='%s' " "$key" "$value"
done)

pids=""
for entry in $CAMUS_BENCH_HOSTS; do
    id=${entry%%=*}
    host=${entry#*=}
    # The broker environment and the host's id expand here, on purpose.
    # shellcheck disable=SC2086,SC2029
    ssh $ssh_opts "$user@$host" "
        pkill -x camus-broker || true
        while pgrep -x camus-broker >/dev/null; do sleep 0.2; done
        rm -rf /data/camus && mkdir -p /data/camus
        env $broker_env CAMUS_WAL_SHARDS=voters RUST_LOG=info \
            nohup \$HOME/camus-broker --node-id $id \
            --http-addr $host:9100 --kafka-addr $host:9092 \
            --data-dir /data/camus --dev-plaintext --allow-non-loopback \
            >/data/broker-$id.log 2>&1 </dev/null &
    " &
    pids="$pids $!"
done
status=0
for pid in $pids; do
    wait "$pid" || status=1
done
exit $status
