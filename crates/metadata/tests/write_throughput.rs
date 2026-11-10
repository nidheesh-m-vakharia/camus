//! Benchmark: metadata writes per second with many concurrent writers, as
//! consumer groups committing offsets make them (#110).
//!
//! ```text
//! cargo test --release -p camus-metadata --test write_throughput -- --ignored --nocapture
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use camus_metadata::{BasicNode, MetadataNode};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const WRITERS: usize = 64;
const EACH: usize = 200;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark; run with --ignored --release"]
async fn concurrent_metadata_writes() -> TestResult {
    let dir = tempfile::tempdir()?;
    let node = Arc::new(MetadataNode::open(1, dir.path(), "127.0.0.1:1").await?);
    let voters: BTreeMap<_, _> = [(
        1,
        BasicNode {
            addr: "mem-1".into(),
        },
    )]
    .into_iter()
    .collect();
    node.initialize(voters).await?;
    let mut ready = false;
    for _ in 0..100 {
        if node.write("ready", "1").await.is_ok() {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !ready {
        return Err("the node never accepted a write".into());
    }
    let started = Instant::now();
    let mut writers = tokio::task::JoinSet::new();
    for writer in 0..WRITERS {
        let node = Arc::clone(&node);
        writers.spawn(async move {
            for index in 0..EACH {
                node.write(format!("offset/{writer}"), index.to_string())
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        });
    }
    while let Some(joined) = writers.join_next().await {
        joined??;
    }
    let seconds = started.elapsed().as_secs_f64();
    let writes = (WRITERS * EACH) as f64;
    println!(
        "| {WRITERS} writers | {} writes | {:.0} writes/s |",
        WRITERS * EACH,
        writes / seconds
    );
    node.shutdown().await?;
    Ok(())
}
