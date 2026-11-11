# camus-storage

S3/R2 client through `object_store` with conditional puts, plus the local
disk read cache (`disk_cache.rs`). ADR 0007.

- Puts use `If-None-Match` or `If-Match`. Don't overwrite an object
  without the condition.
- The disk cache is capped by `CAMUS_READ_DISK_CACHE_BYTES` and
  `CAMUS_READ_DISK_CACHE_ENTRIES`. Keep every cache bounded.
- Tests: `CAMUS_MINIO=1 cargo test -p camus-storage --test minio`.
