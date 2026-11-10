//! Concurrent metadata writes share log entries, each still gets its own
//! response, and concurrent linearizable reads all succeed (#110).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use camus_metadata::{BasicNode, Command, MetadataNode};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const WRITES: usize = 50;

async fn leader(
    dir: &std::path::Path,
) -> Result<Arc<MetadataNode>, Box<dyn std::error::Error + Send + Sync>> {
    let node = Arc::new(MetadataNode::open(1, dir, "127.0.0.1:1").await?);
    let voters: BTreeMap<_, _> = [(
        1,
        BasicNode {
            addr: "mem-1".into(),
        },
    )]
    .into_iter()
    .collect();
    node.initialize(voters).await?;
    for _ in 0..100 {
        if node.write("ready", "1").await.is_ok() {
            return Ok(node);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err("the node never accepted a write".into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_share_entries_and_keep_their_responses() -> TestResult {
    let dir = tempfile::tempdir()?;
    let node = leader(dir.path()).await?;
    // Give each key a first value, so each Put below returns it.
    for index in 0..WRITES {
        node.write(format!("key/{index}"), format!("first-{index}"))
            .await?;
    }
    let before = node
        .raft
        .metrics()
        .borrow()
        .last_log_index
        .ok_or("no log index")?;

    let mut writes = tokio::task::JoinSet::new();
    for index in 0..WRITES {
        let node = Arc::clone(&node);
        writes.spawn(async move {
            let response = node
                .apply(Command::Put {
                    key: format!("key/{index}"),
                    value: format!("second-{index}"),
                })
                .await?;
            Ok::<_, anyhow::Error>((index, response))
        });
    }
    while let Some(joined) = writes.join_next().await {
        let (index, response) = joined??;
        assert_eq!(response.error_code, 0);
        assert_eq!(
            response.value.as_deref(),
            Some(format!("first-{index}").as_str()),
            "each write gets its own command's response"
        );
    }
    let after = node
        .raft
        .metrics()
        .borrow()
        .last_log_index
        .ok_or("no log index")?;
    let entries = after - before;
    assert!(
        entries < WRITES as u64,
        "{WRITES} concurrent writes took {entries} log entries"
    );
    for index in 0..WRITES {
        assert_eq!(
            node.read_local(&format!("key/{index}"))?.as_deref(),
            Some(format!("second-{index}").as_str())
        );
    }

    let mut reads = tokio::task::JoinSet::new();
    for _ in 0..WRITES {
        let node = Arc::clone(&node);
        reads.spawn(async move { node.linearizable().await });
    }
    while let Some(joined) = reads.join_next().await {
        joined??;
    }
    node.shutdown().await?;
    Ok(())
}
