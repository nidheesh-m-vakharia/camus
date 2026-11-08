use bytes::Bytes;
use camus_storage::{Bucket, BucketConfig};

/// MinIO must already be listening. `docker compose up -d minio create-bucket`
/// from the repo root starts it. The test is ignored unless `CAMUS_MINIO=1`,
/// so `cargo test --workspace` stays usable without Docker, and CI / the
/// Phase 0 script opts in.
#[tokio::test]
async fn put_range_and_delete() {
    if std::env::var("CAMUS_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipping MinIO test; set CAMUS_MINIO=1");
        return;
    }
    let bucket = Bucket::connect(&BucketConfig {
        endpoint: std::env::var("CAMUS_S3_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9100".into()),
        bucket: "camus".into(),
        region: "us-east-1".into(),
        access_key: "camus".into(),
        secret_key: "camus-dev-secret".into(),
        virtual_hosted: false,
    })
    .unwrap();

    let key = format!("phase0/{}.bin", std::process::id());
    let body = Bytes::from(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    bucket.put_new(&key, body.clone()).await.unwrap();
    let err = bucket.put_new(&key, body.clone()).await.unwrap_err();
    let message = err.to_string().to_ascii_lowercase();
    assert!(
        message.contains("precondition")
            || message.contains("exist")
            || message.contains("conflict"),
        "second create should fail, got {err}"
    );

    let slice = bucket.get_range(&key, 2..6).await.unwrap();
    assert_eq!(slice.as_ref(), &body[2..6]);

    bucket.delete(&key).await.unwrap();
    assert!(bucket.is_missing(&key).await.unwrap());
}
