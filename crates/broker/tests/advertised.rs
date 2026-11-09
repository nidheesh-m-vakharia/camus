//! `--advertised-addr` is what Metadata hands out, as Kafka's
//! `advertised.listeners`, while the broker listens elsewhere.

mod support;

use camus_protocol::messages::metadata_response::MetadataResponse;
use camus_protocol::messages::{ApiKey, MetadataRequest};
use support::{connect, init, kafka_addr, recv, send, wait_settled, Node, Opts};

#[test]
fn metadata_names_the_advertised_address() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::with(
        1,
        dir.path(),
        Opts::default()
            .arg("--advertised-addr")
            .arg("kafka.example:9093"),
    );
    init(std::slice::from_ref(&node), &[1]);
    wait_settled(&[&node]);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let mut stream = connect(&kafka_addr(&node));
        send(
            &mut stream,
            ApiKey::Metadata,
            12,
            &MetadataRequest::default().with_topics(Some(Vec::new())),
        );
        let metadata = recv::<MetadataResponse>(&mut stream, 12);
        if let Some(broker) = metadata.brokers.first() {
            assert_eq!(broker.host.as_str(), "kafka.example");
            assert_eq!(broker.port, 9093);
            return;
        }
        assert!(std::time::Instant::now() < deadline, "no broker registered");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}
