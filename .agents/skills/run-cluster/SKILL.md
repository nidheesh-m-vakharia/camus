---
name: run-cluster
description: Start one or three Camus brokers and MinIO locally, initialize the cluster, and inspect it through admin HTTP, metrics, or the camus CLI.
---

# Run a cluster

## MinIO

`docker compose up -d minio create-bucket`. It listens on
`http://127.0.0.1:9100`, bucket `camus`, keys `camus` / `camus-dev-secret`.
These are the broker defaults.

## One broker from a checkout

```
cargo run -p camus-broker -- --node-id 1 --http-addr 127.0.0.1:18080 \
  --kafka-addr 127.0.0.1:19092 --data-dir /tmp/camus --dev-plaintext
curl -sf -X POST http://127.0.0.1:18080/admin/init \
  -H 'content-type: application/json' -d '[{"id":1,"addr":"127.0.0.1:18080"}]'
```

Run it in the background and wait for `/health`. Clients connect to
`127.0.0.1:19092`.

## Three brokers

`docker compose --profile brokers up`. To init with a fixed shard count,
send `{"peers":[...],"shards":N}`.

## Inspect

- `GET /admin/metrics` returns JSON, including `current_leader` and
  `wal_leader`. `GET /metrics` is Prometheus. `GET /health`.
- Admin CLI: `cargo run -p camus-cli -- admin --help`.
- A broker's log is `<data-dir>/broker.log` in tests.

## Gotchas

- `--dev-plaintext` refuses a non-loopback address unless you also pass
  `--allow-non-loopback`.
- In a container, MinIO is not at `127.0.0.1`. Set `CAMUS_S3_ENDPOINT` to
  the host.
- The broker deletes objects nothing references, so give each cluster its
  own bucket or `CAMUS_S3_PREFIX`.
- Env vars and flags: README "Configuration".
