//! Sealed ranges a broker has read stay on its local disk, and serve a fetch
//! after a restart with object storage out of reach (#114, ADR 0010).

mod support;

use std::time::{Duration, Instant};

use support::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const BATCHES: i64 = 5;

/// Files in the broker's read disk cache.
fn cached_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir.join("read-cache"))
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().ends_with(".range"))
                .count()
        })
        .unwrap_or(0)
}

#[test]
fn a_restarted_broker_serves_sealed_data_from_disk_without_object_storage() -> TestResult {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        support::skipped("read cache test; set CAMUS_MINIO=1");
        return Ok(());
    }
    let proxy = Proxy::start("127.0.0.1:9100");
    let dir = tempfile::tempdir()?;
    let mut node = single(dir.path(), Opts::default().s3(&proxy.url()));
    let addr = kafka_addr(&node);
    for index in 0..BATCHES {
        assert_eq!(produce(&addr, &format!("v{index}")), (0, index));
    }
    wait_sealed(&node.http, BATCHES);
    let expected: Vec<String> = (0..BATCHES).map(|index| format!("v{index}")).collect();
    assert_eq!(read_all(&addr, BATCHES), expected);
    // The disk tier is written in the background.
    let deadline = Instant::now() + Duration::from_secs(10);
    while cached_files(dir.path()) == 0 {
        if Instant::now() > deadline {
            return Err("nothing was written to the read disk cache".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    // A restart empties the memory tier; cutting the proxy leaves only disk.
    node.restart();
    wait_settled(&[&node]);
    proxy.cut();
    let addr = kafka_addr(&node);
    assert_eq!(read_all(&addr, BATCHES), expected);
    proxy.restore();
    Ok(())
}
