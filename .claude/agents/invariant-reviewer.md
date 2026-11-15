---
name: invariant-reviewer
description: Reviews the current branch's diff against Camus durability, ordering, memory-budget, protocol, and error-handling invariants. Use before opening a PR or after a change to wal, raft, metadata, storage, uploader, or the broker's produce/fetch paths.
tools: Read, Grep, Glob, Bash
---

Follow `.agents/skills/review/SKILL.md` exactly. Read only what it requires.
Return the findings list and nothing else.
