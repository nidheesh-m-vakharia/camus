//! Object storage for sealed log bundles.
//!
//! The client speaks the S3 API through `object_store`, with conditional puts
//! enabled (`If-Match` / `If-None-Match`). Cloudflare R2 and MinIO both
//! implement that. It covers put, ranged get, list, and delete. The uploader
//! is a later crate and is not wired in here.

#![forbid(unsafe_code)]

mod disk_cache;

pub use disk_cache::DiskCache;

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
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

/// Object-store calls made through [`Bucket`], for `/metrics`. One count per
/// call: `object_store` may retry a call, and a list may fetch several pages.
#[derive(Default)]
pub struct Stats {
    /// Puts, conditional or not (R2 Class A).
    pub puts: AtomicU64,
    /// Ranged gets (R2 Class B).
    pub gets: AtomicU64,
    /// Heads (R2 Class B).
    pub heads: AtomicU64,
    /// Lists (R2 Class A).
    pub lists: AtomicU64,
    /// Deletes (free on R2).
    pub deletes: AtomicU64,
    /// Calls that returned an error, including a conditional put that lost.
    /// A head that finds nothing is not one.
    pub errors: AtomicU64,
    /// Bytes written by puts.
    pub put_bytes: AtomicU64,
    /// Bytes returned by gets.
    pub get_bytes: AtomicU64,
}

/// Process-wide counters.
pub static STATS: Stats = Stats {
    puts: AtomicU64::new(0),
    gets: AtomicU64::new(0),
    heads: AtomicU64::new(0),
    lists: AtomicU64::new(0),
    deletes: AtomicU64::new(0),
    errors: AtomicU64::new(0),
    put_bytes: AtomicU64::new(0),
    get_bytes: AtomicU64::new(0),
};

/// Count one call to `op`, and an error if it failed.
fn counted<T, E>(op: &AtomicU64, result: Result<T, E>) -> Result<T, E> {
    op.fetch_add(1, Relaxed);
    if result.is_err() {
        STATS.errors.fetch_add(1, Relaxed);
    }
    result
}

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

    /// Write `bytes` to `key`, replacing an object that is already there.
    pub async fn put(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        let len = bytes.len() as u64;
        let result = self
            .store
            .put(&self.path(key), PutPayload::from_bytes(bytes))
            .await;
        counted(&STATS.puts, result)?;
        STATS.put_bytes.fetch_add(len, Relaxed);
        Ok(())
    }

    /// Write `bytes` to `key`. A second create of the same key fails.
    pub async fn put_new(&self, key: &str, bytes: Bytes) -> anyhow::Result<()> {
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        let len = bytes.len() as u64;
        let result = self
            .store
            .put_opts(&self.path(key), PutPayload::from_bytes(bytes), opts)
            .await;
        counted(&STATS.puts, result)?;
        STATS.put_bytes.fetch_add(len, Relaxed);
        Ok(())
    }

    /// Read `range` of an object. `range` is half-open.
    pub async fn get_range(&self, key: &str, range: Range<u64>) -> anyhow::Result<Bytes> {
        let result = self.store.get_range(&self.path(key), range).await;
        let bytes = counted(&STATS.gets, result)?;
        STATS.get_bytes.fetch_add(bytes.len() as u64, Relaxed);
        Ok(bytes)
    }

    /// Delete `key`. Missing keys are an error from the store, not a success.
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let result = self.store.delete(&self.path(key)).await;
        counted(&STATS.deletes, result)?;
        Ok(())
    }

    /// A listing of `prefix`, read a page at a time. One list is counted
    /// when this is created, not once per page.
    pub fn list_pages(&self, prefix: &str) -> Listing {
        STATS.lists.fetch_add(1, Relaxed);
        Listing {
            stream: self.store.list(Some(&self.path(prefix))),
            prefix: self.prefix.clone(),
            done: false,
        }
    }

    /// Every object under `prefix`, as `(key, size, last-modified Unix ms)`.
    /// Keys are returned without the bucket's own prefix.
    pub async fn list(&self, prefix: &str) -> anyhow::Result<Vec<(String, u64, i64)>> {
        let mut listing = self.list_pages(prefix);
        let mut all = Vec::new();
        loop {
            let page = listing.next_page(1024).await?;
            if page.is_empty() {
                break;
            }
            all.extend(page);
        }
        Ok(all)
    }

    /// Whether `key` is absent. `true` means a get would not return bytes.
    pub async fn is_missing(&self, key: &str) -> anyhow::Result<bool> {
        STATS.heads.fetch_add(1, Relaxed);
        match self.store.head(&self.path(key)).await {
            Ok(_) => Ok(false),
            Err(object_store::Error::NotFound { .. }) => Ok(true),
            Err(err) => {
                STATS.errors.fetch_add(1, Relaxed);
                Err(err.into())
            }
        }
    }
}

/// One object listing. [`Listing::next_page`] returns at most the requested
/// number of keys, and an empty page means the listing is finished.
pub struct Listing {
    stream: futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>,
    prefix: String,
    /// The store's list stream panics if it is polled after it has ended.
    done: bool,
}

impl Listing {
    /// The next `limit` objects, or fewer when the listing ends.
    pub async fn next_page(&mut self, limit: usize) -> anyhow::Result<Vec<(String, u64, i64)>> {
        use futures::StreamExt;
        if self.done {
            return Ok(Vec::new());
        }
        let mut page = Vec::new();
        while page.len() < limit {
            let Some(meta) = self.stream.next().await else {
                self.done = true;
                break;
            };
            let meta = meta?;
            let key = meta.location.to_string();
            let Some(key) = key.strip_prefix(&self.prefix) else {
                continue;
            };
            page.push((
                key.to_string(),
                meta.size,
                meta.last_modified.timestamp_millis(),
            ));
        }
        Ok(page)
    }
}

/// Recently read byte ranges of sealed objects, up to a fixed number of
/// bytes, least recently used first out. An object is never rewritten under
/// its key, so a cached range never goes stale.
pub struct ReadCache {
    cap: usize,
    inner: std::sync::Mutex<CacheInner>,
}

type RangeKey = (String, u64, u64);

#[derive(Default)]
struct CacheInner {
    entries: std::collections::HashMap<RangeKey, (Bytes, u64)>,
    /// Use stamp to key, oldest first.
    order: std::collections::BTreeMap<u64, RangeKey>,
    bytes: usize,
    next: u64,
}

impl ReadCache {
    /// A cache holding at most `cap` bytes. `0` keeps nothing.
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            inner: std::sync::Mutex::new(CacheInner::default()),
        }
    }

    /// The cache's state. A panic while it was held may have left the maps
    /// out of step with each other, so a poisoned cache starts again empty.
    fn lock(&self) -> std::sync::MutexGuard<'_, CacheInner> {
        self.inner.lock().unwrap_or_else(|poisoned| {
            let mut inner = poisoned.into_inner();
            *inner = CacheInner::default();
            self.inner.clear_poison();
            inner
        })
    }

    /// The cached bytes of `key` at `range`, if held.
    pub fn get(&self, key: &str, range: &Range<u64>) -> Option<Bytes> {
        let mut inner = self.lock();
        let id = (key.to_string(), range.start, range.end);
        let stamp = inner.next;
        let (bytes, used) = inner.entries.get_mut(&id)?;
        let bytes = bytes.clone();
        let old = std::mem::replace(used, stamp);
        inner.next += 1;
        inner.order.remove(&old);
        inner.order.insert(stamp, id);
        Some(bytes)
    }

    /// Keep `bytes` for `key` at `range`, evicting the least recently used
    /// ranges to stay within the cap. A range larger than the cap is not kept.
    pub fn insert(&self, key: &str, range: &Range<u64>, bytes: Bytes) {
        if bytes.len() > self.cap {
            return;
        }
        let mut inner = self.lock();
        let id = (key.to_string(), range.start, range.end);
        if inner.entries.contains_key(&id) {
            return;
        }
        let stamp = inner.next;
        inner.next += 1;
        inner.bytes += bytes.len();
        inner.order.insert(stamp, id.clone());
        inner.entries.insert(id, (bytes, stamp));
        while inner.bytes > self.cap {
            let Some((_, oldest)) = inner.order.pop_first() else {
                break;
            };
            if let Some((evicted, _)) = inner.entries.remove(&oldest) {
                inner.bytes -= evicted.len();
            }
        }
    }

    /// Bytes held now.
    pub fn bytes(&self) -> usize {
        self.lock().bytes
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

#[cfg(test)]
mod stats_tests {
    use super::*;

    #[tokio::test]
    async fn each_call_counts_its_operation_and_bytes() -> anyhow::Result<()> {
        let bucket = Bucket {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: String::new(),
        };
        let before = |counter: &AtomicU64| counter.load(Relaxed);
        let (puts, gets, heads, lists, deletes) = (
            before(&STATS.puts),
            before(&STATS.gets),
            before(&STATS.heads),
            before(&STATS.lists),
            before(&STATS.deletes),
        );
        let (put_bytes, get_bytes) = (before(&STATS.put_bytes), before(&STATS.get_bytes));

        bucket
            .put_new("a", Bytes::from_static(b"0123456789"))
            .await?;
        bucket.get_range("a", 2..6).await?;
        bucket.list("").await?;
        bucket.delete("a").await?;
        anyhow::ensure!(bucket.is_missing("a").await?);

        // Other tests may run in parallel, so check at least this test's calls.
        anyhow::ensure!(STATS.puts.load(Relaxed) > puts);
        anyhow::ensure!(STATS.gets.load(Relaxed) > gets);
        anyhow::ensure!(STATS.heads.load(Relaxed) > heads);
        anyhow::ensure!(STATS.lists.load(Relaxed) > lists);
        anyhow::ensure!(STATS.deletes.load(Relaxed) > deletes);
        anyhow::ensure!(STATS.put_bytes.load(Relaxed) >= put_bytes + 10);
        anyhow::ensure!(STATS.get_bytes.load(Relaxed) >= get_bytes + 4);
        Ok(())
    }

    #[tokio::test]
    async fn a_listing_returns_a_page_and_then_the_rest() -> anyhow::Result<()> {
        let bucket = Bucket {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: String::new(),
        };
        bucket.put_new("bundle/a", Bytes::from_static(b"a")).await?;
        bucket.put_new("bundle/b", Bytes::from_static(b"b")).await?;
        bucket.put_new("bundle/c", Bytes::from_static(b"c")).await?;
        let mut listing = bucket.list_pages("bundle/");
        let first = listing.next_page(2).await?;
        let second = listing.next_page(2).await?;
        let third = listing.next_page(2).await?;
        anyhow::ensure!(first.len() == 2);
        anyhow::ensure!(second.len() == 1);
        anyhow::ensure!(third.is_empty());
        let mut keys = first;
        keys.extend(second);
        keys.sort();
        anyhow::ensure!(keys
            .iter()
            .map(|(key, _, _)| key.as_str())
            .eq(["bundle/a", "bundle/b", "bundle/c"]));
        Ok(())
    }

    #[tokio::test]
    async fn a_finished_page_is_not_polled_again() -> anyhow::Result<()> {
        use futures::StreamExt;
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let bucket = Bucket {
            store: Arc::clone(&store),
            prefix: String::new(),
        };
        bucket.put_new("k", Bytes::from_static(b"x")).await?;
        let meta = store
            .list(None)
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("empty"))??;
        // The cloud list stream panics if polled after it ends. This one does too.
        let stream = futures::stream::unfold(Some(meta), |state| async move {
            let meta = state?;
            Some((Ok::<_, object_store::Error>(meta), None))
        })
        .boxed();
        let mut listing = Listing {
            stream,
            prefix: String::new(),
            done: false,
        };
        let page = listing.next_page(8).await?;
        anyhow::ensure!(page.len() == 1);
        let again = listing.next_page(8).await?;
        anyhow::ensure!(again.is_empty());
        Ok(())
    }
}

#[cfg(test)]
mod cache_tests {
    use super::*;

    fn bytes(len: usize) -> Bytes {
        Bytes::from(vec![7u8; len])
    }

    #[test]
    fn the_least_recently_used_range_goes_first_and_the_cap_holds() {
        let cache = ReadCache::new(30);
        cache.insert("a", &(0..10), bytes(10));
        cache.insert("b", &(0..10), bytes(10));
        cache.insert("c", &(0..10), bytes(10));
        // Reading `a` makes `b` the oldest.
        assert!(cache.get("a", &(0..10)).is_some());
        cache.insert("d", &(0..10), bytes(10));
        assert!(cache.get("b", &(0..10)).is_none(), "evicted");
        assert!(cache.get("a", &(0..10)).is_some());
        assert!(cache.get("c", &(0..10)).is_some());
        assert!(cache.get("d", &(0..10)).is_some());
        assert_eq!(cache.bytes(), 30);
    }

    #[test]
    fn a_range_is_keyed_by_its_offsets_and_one_over_the_cap_is_not_kept() {
        let cache = ReadCache::new(16);
        cache.insert("a", &(0..10), bytes(10));
        assert!(cache.get("a", &(0..9)).is_none());
        assert!(cache.get("a", &(10..20)).is_none());
        cache.insert("big", &(0..17), bytes(17));
        assert!(cache.get("big", &(0..17)).is_none());
        assert_eq!(cache.bytes(), 10);
        let off = ReadCache::new(0);
        off.insert("a", &(0..1), bytes(1));
        assert!(off.get("a", &(0..1)).is_none());
    }
}
