---
name: ship
description: Branch, commit, and open a pull request for Camus, matching the repo's naming, commit style, PR template, and CI gate.
---

# Ship

1. Branch from `main`. Prefix: `perf/`, `docs/`, `kafka-compat/`, or
   `fix/`.
2. Run `scripts/agent-check.sh`. Run the gated suites your change touches
   (see `testing`).
3. Run the `review` skill on the diff.
4. Commit subject: one sentence saying what changed and why, ending in a
   period. Example: "Read sealed extents in parallel, and cache what was
   read." The body is optional and explains why.
5. PR body follows `.github/pull_request_template.md`: `## Summary`, then
   `## Test plan`. List the exact commands run, and any gated tests that
   were skipped.
6. CI runs `cargo fmt --all --check`, `scripts/agent-check.sh --no-test`,
   and `cargo test --workspace --locked`, then builds the image.
