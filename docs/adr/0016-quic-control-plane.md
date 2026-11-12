# ADR 0016: QUIC for Raft control traffic

Status: Proposed
Date: 2026-11-11

## Context

Brokers talk to each other only for Raft: one metadata group and one group
per WAL shard (ADR 0004, ADR 0014), all over HTTP/1.1 with postcard bodies
(`crates/metadata/src/network.rs`, `crates/wal/src/network.rs`). Each RPC is
one HTTP request on a pooled TCP connection: votes, heartbeats, log entries
and snapshots alike.

Two kinds of traffic share that path:

- **Control**: votes, heartbeats (AppendEntries with no entries) and, at the
  broker level, shard leader announcements. Small, frequent, and safe to lose:
  Raft retries them and they carry no state that a later one does not.
- **Data**: AppendEntries with entries (up to about 1 MiB per WAL group, eight
  per RPC) and snapshots.

Control traffic decides how fast a failure is noticed and a new leader is in
place, so it should not wait behind data, a full connection pool, or TCP
connect and retransmit timeouts. Today both Raft groups use
`heartbeat_interval: 50` and `election_timeout: 150–300 ms`, and a shard's
leadership reaches other brokers through `announce_shard`, a metadata write
every 200 ms (`crates/broker/src/kafka.rs`).

### A constraint from openraft 0.9.25

Each follower gets one replication task, which sends one AppendEntries at a
time. A heartbeat is queued only when no data is waiting ("Never overwrite
action with payload", `openraft/src/replication/mod.rs`). So sending empty
AppendEntries over another transport changes how heartbeats travel, not when
they are sent: while a follower is receiving data, the data RPCs are its
heartbeats. Votes go out on their own, outside the replication task.

The gains this ADR expects are therefore:

1. Votes, and heartbeats to idle followers, do not depend on the HTTP server,
   its connection pool or TCP connect timeouts.
2. Control messages are cheap enough to shorten `heartbeat_interval` and the
   election timeout, which is what bounds how fast a dead leader is noticed.
3. A new shard leader can tell its peers directly, instead of within the next
   200 ms metadata write.

It does not expect lower ack latency: an ack is fsync-bound (milliseconds),
and the transport costs tens of microseconds.

## Decision

Add a QUIC endpoint to every broker for control traffic only. Data
AppendEntries and snapshots stay on HTTP.

| Message | Transport | Why |
| --- | --- | --- |
| `vote` (metadata and every shard) | QUIC stream, request/response | Needs its answer; a stream per RPC costs no handshake once connected |
| AppendEntries with no entries | QUIC stream, request/response | openraft needs the follower's response (matching, conflict, term) |
| AppendEntries with entries | HTTP, unchanged | Bulk data; TCP is already right for it |
| `install_snapshot` | HTTP, unchanged | Bulk data |
| Shard leader announcement (new) | QUIC unreliable datagram, no reply | Safe to lose: sent again each interval, ordered by term |

### Endpoint

- `quinn` with rustls (already a dependency) and TLS 1.3. One QUIC connection
  per peer, kept open and reconnected on loss. One bidirectional stream per
  RPC. Datagrams for announcements (`max_datagram_frame_size` enabled).
- The UDP port is the HTTP port by default, so peers need no new address:
  `BasicNode.addr` already names `host:port`. `CAMUS_QUIC_PORT` overrides it,
  and `CAMUS_QUIC=off` turns the endpoint off.
- Frame on a stream: `route u8` (metadata = 0, WAL = 1), `shard u32`,
  `kind u8` (vote, heartbeat), `len u32`, then the postcard body the HTTP route
  takes today. The reply is the postcard response, as now.
- Peers authenticate each other. A vote from anyone who can reach the port
  could unseat a leader, so brokers present certificates from a cluster CA
  (`--tls-cert` / `--tls-key` / `--tls-client-ca`, as the Kafka listener does)
  and refuse unknown peers. `--dev-plaintext` clusters on loopback use a
  self-signed certificate generated at start and pinned per run; that mode is
  refused off loopback, like `--dev-plaintext` is.

### Routing in the Raft network

`HttpConn::vote` and `HttpConn::append_entries` (both groups) try QUIC first
when the RPC is a vote or carries no entries, with the RPC's `hard_ttl` as the
timeout. If the peer has no QUIC endpoint, or the connection cannot be made,
they fall back to HTTP for that RPC and retry QUIC after a backoff. A cluster
in the middle of a rolling upgrade, or one where UDP is blocked, keeps
working on HTTP.

### Leader announcements

A broker that leads a shard sends `(shard, term, leader, epoch)` as a datagram
to every peer each `heartbeat_interval`. A receiver keeps the highest term per
shard and uses it as a hint, the same way it uses a stored `ShardView`: for
routing produces and building Metadata responses. The metadata write in
`announce_shard` stays the record of truth, written as now; a hint never
outranks a stored view with a higher term.

## Plan

Each phase is one PR, off by default until phase 5, and measured against the
phase-0 baseline.

0. **Measure first.** Add a failover benchmark: three brokers, kill the shard
   leader, and record the time until a produce acks on the new leader, plus
   the metadata leader the same way. Record vote RPC time, heartbeat RPC p50
   and p99, and spurious elections per hour under the throughput benchmark.
   Run on macOS and on the Linux VM.
1. **The endpoint.** `crates/raft/src/quic.rs`: the endpoint, a peer connection
   cache, the frame codec, certificates, `CAMUS_QUIC` / `CAMUS_QUIC_PORT`. Tests:
   request/response over loopback, reconnect after a peer restarts, an unknown
   certificate refused. This phase is in the tree. The endpoint stays off
   unless `CAMUS_QUIC=on`, and the broker does not open it yet. Phases 2–5
   have not started: every Raft RPC is still HTTP, and the timings are
   unchanged.
2. **Votes over QUIC** for metadata and WAL, with HTTP fallback. Tests:
   `wal_cluster`, `wal_majority` and `group_failover` pass with QUIC on; an
   election completes with HTTP to that peer blocked.
3. **Heartbeats over QUIC** (AppendEntries with no entries). Tests: commit
   index still reaches followers; a follower behind a dropped heartbeat
   catches up from the next data RPC; 5% packet loss injected on the UDP
   port causes no spurious election in a 10-minute run.
4. **Leader announcement datagrams**, used as hints. Tests: a produce routed to
   the new shard leader within one heartbeat interval of the election, before
   the metadata write lands; a stale (lower-term) datagram is ignored.
5. **Shorter timings, then on by default.** Try `heartbeat_interval` 10–20 ms
   and election timeout 50–100 ms. Keep them only if phase 0's spurious
   election count does not rise. Then make QUIC the default.

## Consequences

- One more port (UDP) to open between brokers. Clusters where it is blocked
  keep working on HTTP, at today's speeds.
- Control traffic gets its own failure domain: an HTTP server busy with data
  does not delay an election.
- Two transports to test. The fallback has to be exercised in CI, not just
  the QUIC path.
- More, smaller packets at shorter intervals: about 3 heartbeats × voters ×
  shards every interval. At 10 ms with 24 shard groups per broker, that is a
  few thousand small datagrams a second, well within cloud packet limits, but
  phase 5 has to check it.

## Alternatives

- **Everything over QUIC.** It would also remove HTTP framing from data RPCs,
  but bulk replication is where TCP is strongest and the measured cost is in
  fsync, not transport. A later ADR can revisit it with phase-0 numbers.
- **Raw UDP with our own reliability.** It would mean writing sequencing,
  retransmission, congestion control and encryption for the same result QUIC
  gives.
- **Raw TCP framing instead of HTTP.** It is cheaper per RPC, but keeps TCP's
  connect and retransmit behaviour on the failure path, which is the part this
  ADR is about.
- **Moving to openraft 0.10** for its scheduling changes. It's worth checking
  whether its heartbeats can go out while data is in flight. That would widen
  this ADR's gains, but it is an upgrade of its own.

## Not decided here

- Whether followers should also learn the commit index from announcement
  datagrams, which would speed applies on idle shards.
- How the Linux direct-write path (#129) changes the timing budget.
