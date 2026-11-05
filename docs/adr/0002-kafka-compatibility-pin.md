# ADR 0002: Kafka compatibility pin

Status: Accepted
Date: 2026-11-05

## Context

Kafka's wire protocol, error codes, and coordinator behavior move between
releases. KIPs describe intent. The broker that clients are tested against
is the source that resolves disagreements. Camus needs one pinned tree so
every API review cites the same files.

## Decision

The behavior reference is Apache Kafka **4.4.0**.

- Tag: `4.4.0`
- Annotated tag object: `9661b1ef7a0120e476399250b31d530792506787`
- Commit: `1156b2752aaa28eafc0b4c4d387e63356793268d`
- Released: 2026-10-22
- Source: https://github.com/apache/kafka

`docs/compatibility.md` records the pin. Phase 0 clones that tag into
`vendor/kafka`, which is gitignored. The clone is a reading copy, not a
submodule and not a build dependency.

Rules for later work:

- Before implementing an API, read the message schema under
  `clients/src/main/resources/common/message/`, the handler in
  `core/src/main/scala/kafka/server/KafkaApis.scala` (or its 4.4.0
  equivalent if the file moved), and the error constants in
  `clients/src/main/java/org/apache/kafka/common/protocol/Errors.java`.
- Port the relevant cases from that API's Kafka tests. Name the test and
  the file in the Rust test.
- If the KIP and the pinned code disagree, match the code and add a note to
  this ADR's log at the bottom.
- Each change that adds an API lists the files it was checked against in
  the commit or PR body.
- Kafka is Apache-2.0. Closely translated logic gets a comment with the
  source path and tag, and `NOTICE` already attributes the project.

4.4.0 is the newest stable 4.x feature release as of 2026-11-05. 4.3.2
(2026-10-23) is a bugfix on the previous minor and is not the pin.

## Consequences

- Protocol generation (ADR 0003) tracks this commit, not whatever
  `kafka-protocol` last published.
- Bumping the pin is an explicit change to `compatibility.md` plus a
  schema diff. It is not a drive-by dependency update.
- We will lag Kafka's newest trunk on purpose.

## Discrepancy log

None yet. The deliberate client-visible differences are in ADR 0003
(`ApiVersions` contents), ADR 0005 (`acks=1`), and ADR 0008 (no unclean
leader election). Those are Camus choices, not Kafka-internal disagreements.
