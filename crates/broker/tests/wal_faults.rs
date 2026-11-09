//! ADR 0013 Phase 2 fault acceptance for `wal` produce, on one broker with
//! object storage behind a proxy the test controls:
//!
//! - R2 slow or answering 503: acks continue from the WAL and the upload
//!   catches up when R2 recovers.
//! - R2 gone: once unsealed bytes pass `wal.disk.limit`, produce is held and
//!   the hold is counted in `produce_hold_ms` (not `throttle_time_ms`, so a
//!   client does not wait twice); it times out only at the client's
//!   `timeout_ms`, and no acknowledged batch is dropped.
//! - Crash between the metadata publish and the WAL seal: the restart does
//!   not publish the same offsets twice.
//! - A full WAL disk answers `KAFKA_STORAGE_ERROR` (macOS disk image, or a
//!   small mount named by `CAMUS_SMALL_FS`).
//!
//! Needs MinIO (`CAMUS_MINIO=1`).

mod support;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use support::*;

fn minio() -> bool {
    let on = std::env::var("CAMUS_MINIO").ok().as_deref() == Some("1");
    if !on {
        eprintln!("skipping; set CAMUS_MINIO=1");
    }
    on
}

fn value(index: usize, size: usize) -> String {
    format!("{index:06}{}", "x".repeat(size))
}

fn unsealed(http: &str) -> u64 {
    admin(http).unwrap()["wal_unsealed_bytes"].as_u64().unwrap()
}

#[test]
fn slow_and_failing_r2_do_not_block_acks() {
    if !minio() {
        return;
    }
    let proxy = Proxy::start("127.0.0.1:9100");
    let root = tempfile::tempdir().unwrap();
    let node = single(root.path(), Opts::default().s3(&proxy.url()));
    let kafka = kafka_addr(&node);
    let mut acked = Vec::new();

    proxy.set(Fault::Delay(Duration::from_millis(300)));
    for index in 0..5 {
        let started = Instant::now();
        let (code, offset) = produce(&kafka, &value(index, 100));
        assert_eq!((code, offset), (0, index as i64));
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "a wal ack does not wait for R2: {:?}",
            started.elapsed()
        );
        acked.push(value(index, 100));
    }
    wait_sealed(&node.http, 5);

    proxy.set(Fault::Unavailable);
    for index in 5..8 {
        assert_eq!(produce(&kafka, &value(index, 100)), (0, index as i64));
        acked.push(value(index, 100));
    }
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(sealed_end(&node.http), 5, "503s seal nothing");
    assert!(unsealed(&node.http) > 0);

    proxy.restore();
    wait_sealed(&node.http, 8);
    assert_eq!(read_all(&kafka, 8), acked);
    let ranges = extents(&node.http);
    assert!(ranges.windows(2).all(|pair| pair[0].1 == pair[1].0));
    assert_eq!(ranges.first().unwrap().0, 0);
    assert_eq!(ranges.last().unwrap().1, 8);
}

#[test]
fn r2_outage_throttles_at_the_disk_limit_and_catches_up() {
    if !minio() {
        return;
    }
    let proxy = Proxy::start("127.0.0.1:9100");
    let root = tempfile::tempdir().unwrap();
    // About three 1 KiB batches fit under the limit.
    let node = single(
        root.path(),
        Opts::default()
            .s3(&proxy.url())
            .env("CAMUS_WAL_DISK_LIMIT", "4000"),
    );
    let kafka = kafka_addr(&node);
    let mut acked = Vec::new();
    for index in 0..2 {
        assert_eq!(produce(&kafka, &value(index, 1000)).0, 0);
        acked.push(value(index, 1000));
    }
    wait_sealed(&node.http, 2);

    proxy.cut();
    let mut held = None;
    for index in 2..20 {
        let (code, offset, throttle) = produce_full(&kafka, &value(index, 1000), 800);
        if code == 0 {
            assert_eq!(offset, index as i64);
            acked.push(value(index, 1000));
            continue;
        }
        assert_eq!(code, 7, "held until timeout_ms, then REQUEST_TIMED_OUT");
        assert_eq!(throttle, 0, "the hold is not reported as a throttle");
        assert!(
            hold_ms(&node.http) >= 700,
            "produce_hold_ms counts the hold"
        );
        held = Some(index);
        break;
    }
    let held = held.expect("the disk limit never held a produce");
    assert!(held > 2, "acks continue below the limit while R2 is down");
    assert!(unsealed(&node.http) <= 4000);

    // R2 comes back while a produce is held. It is released, not dropped.
    let started = Instant::now();
    let held_before = hold_ms(&node.http);
    let pending = std::thread::spawn({
        let kafka = kafka.clone();
        let next = value(held, 1000);
        move || produce_full(&kafka, &next, 30_000)
    });
    std::thread::sleep(Duration::from_secs(1));
    proxy.restore();
    let (code, offset, throttle) = pending.join().unwrap();
    assert_eq!(code, 0);
    assert_eq!(offset, acked.len() as i64);
    assert_eq!(throttle, 0);
    assert!(
        hold_ms(&node.http) > held_before,
        "the released produce's hold is counted"
    );
    assert!(started.elapsed() >= Duration::from_millis(900));
    acked.push(value(held, 1000));

    wait_sealed(&node.http, acked.len() as i64);
    assert_eq!(read_all(&kafka, acked.len() as i64), acked);
}

#[test]
fn crash_after_publish_does_not_duplicate_extents() {
    if !minio() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let mut node = single(
        root.path(),
        Opts::default().env("CAMUS_FAILPOINT", "after-publish"),
    );
    let kafka = kafka_addr(&node);
    assert_eq!(produce(&kafka, "v0"), (0, 0));
    let deadline = Instant::now() + Duration::from_secs(10);
    while node.child.try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "the failpoint never fired");
        std::thread::sleep(Duration::from_millis(20));
    }
    // The publish committed before the abort; the WAL still holds the batch.
    node.restart_with(Opts::default());
    wait_settled(&[&node]);
    let deadline = Instant::now() + Duration::from_secs(10);
    while unsealed(&node.http) > 0 {
        assert!(Instant::now() < deadline, "the restart never sealed");
        std::thread::sleep(Duration::from_millis(50));
    }
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(extents(&node.http), vec![(0, 1)], "one extent, not two");
    assert_eq!(read_all(&kafka, 1), vec!["v0"]);

    assert_eq!(produce(&kafka, "v1"), (0, 1));
    wait_sealed(&node.http, 2);
    assert_eq!(extents(&node.http), vec![(0, 1), (1, 2)]);
    assert_eq!(read_all(&kafka, 2), vec!["v0", "v1"]);
}

/// A small filesystem for the WAL. Detached on drop.
struct SmallFs {
    mount: PathBuf,
    attached: bool,
}

impl SmallFs {
    fn new(root: &Path) -> Option<Self> {
        if let Ok(dir) = std::env::var("CAMUS_SMALL_FS") {
            return Some(Self {
                mount: PathBuf::from(dir),
                attached: false,
            });
        }
        if !cfg!(target_os = "macos") {
            return None;
        }
        let image = root.join("wal.dmg");
        let mount = root.join("mnt");
        std::fs::create_dir_all(&mount).unwrap();
        let made = Command::new("hdiutil")
            .args([
                "create", "-size", "16m", "-fs", "HFS+", "-volname", "camuswal",
            ])
            .arg(&image)
            .output()
            .ok()?;
        if !made.status.success() {
            return None;
        }
        let attached = Command::new("hdiutil")
            .args(["attach", "-nobrowse", "-mountpoint"])
            .arg(&mount)
            .arg(&image)
            .output()
            .ok()?;
        attached.status.success().then_some(Self {
            mount,
            attached: true,
        })
    }
}

impl Drop for SmallFs {
    fn drop(&mut self) {
        if self.attached {
            let _ = Command::new("hdiutil")
                .args(["detach", "-force"])
                .arg(&self.mount)
                .output();
        }
    }
}

#[test]
fn full_wal_disk_answers_storage_error() {
    if !minio() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let Some(fs) = SmallFs::new(root.path()) else {
        eprintln!("skipping; no small filesystem (macOS hdiutil or CAMUS_SMALL_FS)");
        return;
    };
    let proxy = Proxy::start("127.0.0.1:9100");
    proxy.cut();
    let wal_dir = fs.mount.join("wal-1");
    let mut node = single(
        &root.path().join("node"),
        Opts::default()
            .s3(&proxy.url())
            .arg("--wal-dir")
            .arg(wal_dir.to_str().unwrap()),
    );
    let kafka = kafka_addr(&node);
    let mut code = 0;
    let mut acked = 0;
    for index in 0..64 {
        let (got, offset, _) = produce_full(&kafka, &value(index, 900 * 1024), 10_000);
        code = got;
        if got != 0 {
            break;
        }
        assert_eq!(offset, index as i64);
        acked += 1;
    }
    assert!(acked > 0, "the disk accepted batches before it filled");
    assert_eq!(code, 56, "a full WAL disk is KAFKA_STORAGE_ERROR");
    let log = std::fs::read_to_string(node.dir.join("broker.log")).unwrap();
    assert!(
        log.contains("os error 28") || log.contains("No space left"),
        "the failure is ENOSPC"
    );
    assert_eq!(produce(&kafka, "after").0, 56, "and stays that way");
    assert_eq!(admin(&node.http).unwrap()["wal_storage_failed"], true);
    node.kill();
}

fn hold_ms(http: &str) -> u64 {
    support::admin(http)
        .and_then(|json| json["produce_hold_ms"].as_u64())
        .unwrap_or(0)
}
