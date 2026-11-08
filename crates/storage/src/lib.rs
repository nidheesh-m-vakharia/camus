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
use object_store::{
    BackoffConfig, ClientOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    RetryConfig,
};

/// Endpoint `object_store` can use for virtual-hosted requests. The bucket
/// name has to be the first label of the host. A path-style endpoint such as
/// `https://t3.storageapi.dev` becomes `https://camus.t3.storageapi.dev`.
fn virtual_host_endpoint(endpoint: &str, bucket: &str) -> anyhow::Result<String> {
    let mut url = url::Url::parse(endpoint)?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("S3 endpoint has no host"))?
        .to_string();
    let prefixed = format!("{bucket}.");
    if !host.eq_ignore_ascii_case(bucket)
        && !host
            .to_ascii_lowercase()
            .starts_with(&prefixed.to_ascii_lowercase())
    {
        url.set_host(Some(&format!("{bucket}.{host}")))?;
    }
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string().trim_end_matches('/').to_string())
}

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
    /// Virtual-hosted requests (`https://bucket.endpoint/...`). Railway
    /// Buckets need this. MinIO and R2 stay path-style.
    pub virtual_hosted: bool,
}

impl Bucket {
    /// Build a client. Conditional put is on, because R2 and MinIO support it.
    pub fn connect(config: &BucketConfig) -> anyhow::Result<Self> {
        let endpoint = if config.virtual_hosted {
            virtual_host_endpoint(&config.endpoint, &config.bucket)?
        } else {
            config.endpoint.clone()
        };
        let store = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_access_key_id(&config.access_key)
            .with_secret_access_key(&config.secret_key)
            .with_virtual_hosted_style_request(config.virtual_hosted)
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

#[cfg(test)]
mod tests {
    use super::virtual_host_endpoint;

    #[test]
    fn virtual_host_inserts_the_bucket_label() {
        let endpoint = virtual_host_endpoint("https://t3.storageapi.dev", "camus-abc").unwrap();
        assert_eq!(endpoint, "https://camus-abc.t3.storageapi.dev");
    }

    #[test]
    fn virtual_host_leaves_an_already_prefixed_endpoint() {
        let endpoint =
            virtual_host_endpoint("https://camus-abc.t3.storageapi.dev/extra", "camus-abc")
                .unwrap();
        assert_eq!(endpoint, "https://camus-abc.t3.storageapi.dev");
    }
}
