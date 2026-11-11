---
name: adr
description: Write or amend an architecture decision record in docs/adr when a change alters durability, storage layout, protocol behaviour, crate boundaries, or recovery.
---

# ADR

Write one when a change alters an earlier decision, or affects durability,
the R2 layout, recovery, Kafka compatibility, or crate boundaries.

- File: `docs/adr/NNNN-kebab-title.md`, next number.
- Header: `# ADR NNNN: Title`, then `Status: Proposed` and
  `Date: YYYY-MM-DD`.
- Sections: `## Context`, `## Decision`, `## Consequences`,
  `## Alternatives`. Add `## As built` once implemented.
- Say what the decision is, then the numbers or failure cases behind it. Be
  short.
- Add a row to `docs/adr/README.md`. Once accepted, update `docs/PLAN.md`,
  and `docs/compatibility.md` if Kafka behaviour changes.
- To change an accepted ADR, amend it with a dated note. Don't rewrite its
  history.
