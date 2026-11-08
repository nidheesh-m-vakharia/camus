//! Object storage for sealed log bundles.
//!
//! The client speaks the S3 API through `object_store`, with conditional puts
//! enabled (`If-Match` / `If-None-Match`). Cloudflare R2 and MinIO both
//! implement that. It covers put, ranged get, list, and delete. The uploader
//! is a later crate and is not wired in here.

#![forbid(unsafe_code)]

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path;
use object_store::{
    BackoffConfig, ClientOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    RetryConfig,
};

/// An S3-compatible bucket.
#[derive(Clone)]
pub struct Bucket {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

/// How to reach the bucket.
#[derive(Debug, Clone)]
pub struct BucketConfig {
    /// `http://127.0.0.1:9100` for MinIO, or the R2 S3 endpoint.
    pub endpoint: String,
    /// Bucket name. It must already exist.
    pub bucket: String,
    /// S3 region. MinIO accepts `us-east-1`. R2 accepts `auto`.
    pub region: String,
    /// Access key.
    pub access_key: String,
    /// Secret key.
    pub secret_key: String,
    /// Prepended to every key, so several clusters can share one bucket.
    /// Empty, or ending in `/`. Callers never see it.
    pub prefix: String,
}

impl Bucket {
    /// Build a client. Conditional put is on, because R2 and MinIO support it.
    pub fn connect(config: &BucketConfig) -> anyhow::Result<Self> {
        let store = AmazonS3Builder::new()
            .with_endpoint(&config.endpoint)
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_access_key_id(&config.access_key)
            .with_secret_access_key(&config.secret_key)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            // A few quick retries per call. The uploader keeps its own
            // jittered backoff across calls, so an outage ends in seconds,
            // not after object_store's three-minute default.
            .with_retry(RetryConfig {
                backoff: BackoffConfig {
                    init_backoff: std::time::Duration::from_millis(50),
                    max_backoff: std::time::Duration::from_millis(500),
                    base: 2.0,
                },
                max_retries: 2,
                retry_timeout: std::time::Duration::from_secs(5),
            })
            .with_client_options(
                ClientOptions::new()
                    .with_allow_http(true)
                    .with_connect_timeout(std::time::Duration::from_secs(2))
                    .with_timeout(std::time::Duration::from_secs(30)),
            )
            .build()?;
        let prefix = if config.prefix.is_empty() || config.prefix.ends_with('/') {
            config.prefix.clone()
        } else {
            format!("{}/", config.prefix)
        };
        Ok(Self {
            store: Arc::new(store),
            prefix,
        })
    }

    fn path(&self, key: &str) -> Path {
        Path::from(format!("{}{key}", self.prefix))
    }

    /// Write `bytes` to `key`. A second create of the same key fails.
    pub async fn put_new(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        self.store
            .put_opts(&self.path(key), PutPayload::from_bytes(bytes), opts)
            .await?;
        Ok(())
    }

    /// Read `range` of an object. `range` is half-open.
    pub async fn get_range(&self, key: &str, range: Range<u64>) -> anyhow::Result<Bytes> {
        let bytes = self.store.get_range(&self.path(key), range).await?;
        Ok(bytes)
    }

    /// Delete `key`. Missing keys are an error from the store, not a success.
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.store.delete(&self.path(key)).await?;
        Ok(())
    }

    /// Every object under `prefix`, as `(key, size, last-modified Unix ms)`.
    /// Keys are returned without the bucket's own prefix.
    pub async fn list(&self, prefix: &str) -> anyhow::Result<Vec<(String, u64, i64)>> {
        use futures::TryStreamExt;
        let objects: Vec<_> = self
            .store
            .list(Some(&self.path(prefix)))
            .try_collect()
            .await?;
        Ok(objects
            .into_iter()
            .filter_map(|meta| {
                let key = meta.location.to_string();
                let key = key.strip_prefix(&self.prefix)?.to_string();
                Some((key, meta.size, meta.last_modified.timestamp_millis()))
            })
            .collect())
    }

    /// Whether `key` is absent. `true` means a get would not return bytes.
    pub async fn is_missing(&self, key: &str) -> anyhow::Result<bool> {
        match self.store.head(&self.path(key)).await {
            Ok(_) => Ok(false),
            Err(object_store::Error::NotFound { .. }) => Ok(true),
            Err(err) => Err(err.into()),
        }
    }
}
