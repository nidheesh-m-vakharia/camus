//! Object storage for sealed log bundles.
//!
//! The client speaks the S3 API through `object_store`, with conditional puts
//! enabled (`If-Match` / `If-None-Match`). Cloudflare R2 and MinIO both
//! implement that. Phase 0 covers put, ranged get, and delete. The uploader
//! is a later crate and is not wired in here.

#![forbid(unsafe_code)]

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};

/// An S3-compatible bucket.
#[derive(Clone)]
pub struct Bucket {
    store: Arc<dyn ObjectStore>,
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
            .with_allow_http(true)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    /// Write `bytes` to `key`. A second create of the same key fails.
    pub async fn put_new(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        self.store
            .put_opts(&Path::from(key), PutPayload::from_bytes(bytes), opts)
            .await?;
        Ok(())
    }

    /// Read `range` of an object. `range` is half-open.
    pub async fn get_range(&self, key: &str, range: Range<u64>) -> anyhow::Result<Bytes> {
        let bytes = self.store.get_range(&Path::from(key), range).await?;
        Ok(bytes)
    }

    /// Delete `key`. Missing keys are an error from the store, not a success.
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.store.delete(&Path::from(key)).await?;
        Ok(())
    }

    /// Whether `key` is absent. `true` means a get would not return bytes.
    pub async fn is_missing(&self, key: &str) -> anyhow::Result<bool> {
        match self.store.head(&Path::from(key)).await {
            Ok(_) => Ok(false),
            Err(object_store::Error::NotFound { .. }) => Ok(true),
            Err(err) => Err(err.into()),
        }
    }
}
