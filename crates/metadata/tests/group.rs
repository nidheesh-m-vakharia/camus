use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use camus_metadata::{BasicNode, MetadataNode, NodeId, SharedRouter};

fn voters(ids: &[NodeId]) -> BTreeMap<NodeId, BasicNode> {
    ids.iter()
        .map(|id| {
            (
                *id,
                BasicNode {
                    addr: format!("mem-{id}"),
                },
            )
        })
        .collect()
}

async fn wait_until<F, Fut>(mut probe: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..100 {
        if probe().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_write_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let node = MetadataNode::open(1, dir.path(), "127.0.0.1:1")
        .await
        .unwrap();
    node.initialize(voters(&[1])).await.unwrap();
    let wrote = wait_until(|| async { node.write("topic", "events").await.is_ok() }).await;
    assert!(wrote, "leader did not accept a write");
    assert_eq!(node.read_local("topic").unwrap().as_deref(), Some("events"));
    node.shutdown().await.unwrap();
    drop(node);

    let node = MetadataNode::open(1, dir.path(), "127.0.0.1:1")
        .await
        .unwrap();
    assert_eq!(node.read_local("topic").unwrap().as_deref(), Some("events"));
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn two_groups_do_not_share_writes() {
    let root = tempfile::tempdir().unwrap();
    let router = Arc::new(SharedRouter::default());
    let mut nodes = Vec::new();
    for group in ["alpha", "beta"] {
        for id in [1, 2] {
            let dir = root.path().join(format!("{group}-{id}"));
            let node = MetadataNode::open_in_process(id, &dir, group, router.clone())
                .await
                .unwrap();
            node.register_in_process(group, id, &router);
            nodes.push((group, id, node));
        }
    }

    let alpha = nodes
        .iter()
        .find(|(group, id, _)| *group == "alpha" && *id == 1)
        .unwrap();
    let beta = nodes
        .iter()
        .find(|(group, id, _)| *group == "beta" && *id == 1)
        .unwrap();
    alpha.2.initialize(voters(&[1, 2])).await.unwrap();
    beta.2.initialize(voters(&[1, 2])).await.unwrap();

    let wrote = wait_until(|| async { alpha.2.write("only-alpha", "yes").await.is_ok() }).await;
    assert!(wrote, "alpha did not elect a leader");
    assert_eq!(
        alpha.2.read_local("only-alpha").unwrap().as_deref(),
        Some("yes")
    );

    let beta_has_it = nodes
        .iter()
        .filter(|(group, _, _)| *group == "beta")
        .any(|(_, _, node)| node.read_local("only-alpha").unwrap().is_some());
    assert!(!beta_has_it);
}

#[tokio::test(flavor = "multi_thread")]
async fn every_replica_stores_the_same_topic_id() {
    let root = tempfile::tempdir().unwrap();
    let router = Arc::new(SharedRouter::default());
    let mut nodes = Vec::new();
    for id in [1, 2, 3] {
        let dir = root.path().join(format!("n{id}"));
        let node = MetadataNode::open_in_process(id, &dir, "meta", router.clone())
            .await
            .unwrap();
        node.register_in_process("meta", id, &router);
        nodes.push(node);
    }
    nodes[0].initialize(voters(&[1, 2, 3])).await.unwrap();
    let created = wait_until(|| async {
        for node in &nodes {
            if node
                .create_topic("events", 1, "wal", Vec::new())
                .await
                .is_ok()
            {
                return true;
            }
        }
        false
    })
    .await;
    assert!(created, "no leader took the create");
    let key = camus_metadata::topic_storage_key("events");
    let all_applied = wait_until(|| async {
        nodes
            .iter()
            .all(|node| node.read_local(&key).unwrap().is_some())
    })
    .await;
    assert!(all_applied);
    let ids: std::collections::BTreeSet<String> = nodes
        .iter()
        .map(|node| {
            camus_metadata::parse_topic(&node.read_local(&key).unwrap().unwrap())
                .unwrap()
                .id
        })
        .collect();
    assert_eq!(ids.len(), 1, "replicas disagree on the topic id: {ids:?}");
}
