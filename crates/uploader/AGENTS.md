# camus-uploader

One flush becomes one object: sealed batches, then a footer. ADRs 0007
and 0009.

- Keys are write-once: `object_key` makes a new ULID on every call.
- The footer must let recovery list the slices in an object without
  metadata. The fetch path never returns the footer.
- Batch bytes may change only where ADR 0009 allows it.
