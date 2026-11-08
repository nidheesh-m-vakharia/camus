# Contributing

Camus is Apache-2.0. See `LICENSE` and `NOTICE`.

Run the tests before you open a pull request:

```
cargo test --workspace --locked
```

Storage tests that talk to MinIO stay skipped unless `CAMUS_MINIO=1`. Do not put credentials, tokens, or customer data in an issue or a pull request. Report vulnerabilities through [Security advisories](https://github.com/nidheesh-m-vakharia/camus/security/advisories/new), not a public issue.
