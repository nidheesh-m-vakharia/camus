//! A consumer replays a topic while object storage is down.
//!
//! A warm cache is served. A miss is `KAFKA_STORAGE_ERROR` (56) and carries
//! no records, so the consumer does not advance. The `wal` tail, which was
//! acknowledged and not yet sealed, stays readable. When object storage
//! returns, a fetch from the start has every record once. An `r2` topic has
//! no unsealed tail: its produce ack waits for the object.

mod support;

use support::*;

fn minio() -> bool {
    let on = std::env::var("CAMUS_MINIO").ok().as_deref() == Some("1");
    if !on {
        support::skipped("replay outage test; set CAMUS_MINIO=1");
    }
    on
}

fn values(from: usize, count: usize) -> Vec<String> {
    (from..from + count)
        .map(|index| format!("v{index}"))
        .collect()
}

fn produce_values(addr: &str, from: usize, count: usize) {
    for value in values(from, count) {
        let (code, _) = produce(addr, &value);
        assert_eq!(code, 0, "{value}");
    }
}

/// Restart so the memory cache is gone, and delete the disk cache too.
fn restart_cold(node: &mut Node) {
    node.kill();
    let _ = std::fs::remove_dir_all(node.dir.join("read-cache"));
    node.restart();
    wait_settled(&[&*node]);
}

fn miss_does_not_advance(addr: &str, offset: i64) {
    let (code, got) = fetch_at(addr, offset);
    assert_eq!(
        code, 56,
        "a cache miss during an outage is KAFKA_STORAGE_ERROR"
    );
    assert!(
        got.is_empty(),
        "the error carried records, which would advance a consumer: {got:?}"
    );
}

#[test]
fn wal_replay_uses_the_cache_and_the_tail_during_an_outage() -> std::io::Result<()> {
    if !minio() {
        return Ok(());
    }
    let proxy = Proxy::start("127.0.0.1:9100");
    let root = tempfile::tempdir()?;
    let mut node = single(root.path(), Opts::default().s3(&proxy.url()));
    let kafka = kafka_addr(&node);
    produce_values(&kafka, 0, 4);
    wait_sealed(&node.http, 4);
    assert_eq!(read_all(&kafka, 4), values(0, 4));

    proxy.cut();
    produce_values(&kafka, 4, 2);
    assert_eq!(
        read_all(&kafka, 6),
        values(0, 6),
        "the warm cache and the unsealed tail are served while R2 is down"
    );

    restart_cold(&mut node);
    let kafka = kafka_addr(&node);
    miss_does_not_advance(&kafka, 0);
    let (code, tail) = fetch_at(&kafka, 4);
    assert_eq!(code, 0, "the unsealed tail does not need R2");
    assert_eq!(tail, values(4, 2));

    proxy.restore();
    wait_sealed(&node.http, 6);
    assert_eq!(read_all(&kafka, 6), values(0, 6));
    Ok(())
}

#[test]
fn r2_replay_serves_the_cache_and_errors_on_a_miss() -> std::io::Result<()> {
    if !minio() {
        return Ok(());
    }
    let proxy = Proxy::start("127.0.0.1:9100");
    let root = tempfile::tempdir()?;
    let mut node = Node::with(1, root.path(), Opts::default().s3(&proxy.url()));
    init(std::slice::from_ref(&node), &[1]);
    wait_settled(&[&node]);
    let kafka = kafka_addr(&node);
    create_topic(&kafka, "r2");
    partition_leader(&kafka, None);
    produce_values(&kafka, 0, 4);
    wait_sealed(&node.http, 4);
    assert_eq!(read_all(&kafka, 4), values(0, 4));

    proxy.cut();
    assert_eq!(
        read_all(&kafka, 4),
        values(0, 4),
        "a warm r2 fetch does not go back to R2"
    );

    restart_cold(&mut node);
    let kafka = kafka_addr(&node);
    miss_does_not_advance(&kafka, 0);

    proxy.restore();
    assert_eq!(read_all(&kafka, 4), values(0, 4));
    Ok(())
}
