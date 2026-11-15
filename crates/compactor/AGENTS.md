# camus-compactor

Retention (ADR 0010) and the orphan sweep (ADR 0007), planning only.

- This crate decides. The broker does all I/O and commits. Keep it pure so
  decisions are testable without a bucket.
- The sweep never deletes an object younger than `CAMUS_SWEEP_MIN_AGE_MS`,
  or one that metadata references.
