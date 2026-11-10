//! Sealed ranges kept on local disk, the tier under the memory cache
//! (ADR 0010). An object is never rewritten under its key, so a cached range
//! never goes stale.
//!
//! Each range is one file. The file starts with the object key and range it
//! holds, and a read checks them, so two ranges whose file names collide
//! read as a miss, never as each other's bytes. The index of what is on disk
//! lives in memory and is capped by entries as well as bytes, so it stays a
//! fixed size however small the ranges are. It is rebuilt from the files on
//! open, oldest first. Disk reads and writes run on the blocking pool.

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;

const MAGIC: &[u8; 8] = b"CAMUSRC1";
const SUFFIX: &str = ".range";

/// Recently read sealed ranges on local disk, least recently used first out.
#[derive(Clone)]
pub struct DiskCache {
    shared: Arc<Shared>,
}

struct Shared {
    dir: PathBuf,
    max_bytes: u64,
    max_entries: usize,
    index: Mutex<Index>,
}

#[derive(Default)]
struct Index {
    /// File name to its size and last-use stamp.
    entries: HashMap<String, (u64, u64)>,
    /// Last-use stamp to file name, oldest first.
    order: BTreeMap<u64, String>,
    bytes: u64,
    next: u64,
}

impl Index {
    fn touch(&mut self, name: &str) -> bool {
        let stamp = self.next;
        let Some((_, used)) = self.entries.get_mut(name) else {
            return false;
        };
        let old = std::mem::replace(used, stamp);
        self.next += 1;
        self.order.remove(&old);
        self.order.insert(stamp, name.to_string());
        true
    }

    /// Record `name`, and return the files to delete to stay within the caps.
    fn add(&mut self, name: String, size: u64, max_bytes: u64, max_entries: usize) -> Vec<String> {
        if let Some((old_size, old_stamp)) = self.entries.remove(&name) {
            self.bytes -= old_size;
            self.order.remove(&old_stamp);
        }
        let stamp = self.next;
        self.next += 1;
        self.bytes += size;
        self.order.insert(stamp, name.clone());
        self.entries.insert(name, (size, stamp));
        let mut evicted = Vec::new();
        while self.bytes > max_bytes || self.entries.len() > max_entries {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            if let Some((size, _)) = self.entries.remove(&oldest) {
                self.bytes -= size;
            }
            evicted.push(oldest);
        }
        evicted
    }

    fn forget(&mut self, name: &str) {
        if let Some((size, stamp)) = self.entries.remove(name) {
            self.bytes -= size;
            self.order.remove(&stamp);
        }
    }
}

impl DiskCache {
    /// Open the cache in `dir`, keeping at most `max_bytes` in at most
    /// `max_entries` files. Files already there are indexed, oldest first;
    /// what does not fit is deleted.
    pub fn open(
        dir: impl AsRef<Path>,
        max_bytes: u64,
        max_entries: usize,
    ) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let mut found = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let meta = entry.metadata()?;
            if !name.ends_with(SUFFIX) || !meta.is_file() {
                // A write cut short leaves a temporary file.
                let _ = std::fs::remove_file(entry.path());
                continue;
            }
            let modified = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
            found.push((modified, name, meta.len()));
        }
        found.sort();
        let mut index = Index::default();
        for (_, name, size) in found {
            for evicted in index.add(name, size, max_bytes, max_entries) {
                let _ = std::fs::remove_file(dir.join(evicted));
            }
        }
        Ok(Self {
            shared: Arc::new(Shared {
                dir,
                max_bytes,
                max_entries,
                index: Mutex::new(index),
            }),
        })
    }

    /// The cached bytes of `key` at `range`, if on disk.
    pub async fn get(&self, key: &str, range: &Range<u64>) -> Option<Bytes> {
        let name = file_name(key, range);
        if !self.shared.lock().entries.contains_key(&name) {
            return None;
        }
        let shared = Arc::clone(&self.shared);
        let key = key.to_string();
        let range = range.clone();
        tokio::task::spawn_blocking(move || {
            let path = shared.dir.join(&name);
            match std::fs::read(&path)
                .ok()
                .and_then(|raw| payload(raw, &key, &range))
            {
                Some(bytes) => {
                    shared.lock().touch(&name);
                    Some(bytes)
                }
                None => {
                    shared.lock().forget(&name);
                    None
                }
            }
        })
        .await
        .ok()
        .flatten()
    }

    /// Keep `bytes` for `key` at `range`, in the background. A range larger
    /// than the byte cap is not kept. A failed write leaves nothing behind.
    pub fn insert(&self, key: &str, range: &Range<u64>, bytes: Bytes) {
        if bytes.len() as u64 > self.shared.max_bytes {
            return;
        }
        let shared = Arc::clone(&self.shared);
        let key = key.to_string();
        let range = range.clone();
        tokio::task::spawn_blocking(move || {
            let name = file_name(&key, &range);
            let Ok(size) = write_file(&shared.dir, &name, &key, &range, &bytes) else {
                return;
            };
            let evicted = shared
                .lock()
                .add(name, size, shared.max_bytes, shared.max_entries);
            for name in evicted {
                let _ = std::fs::remove_file(shared.dir.join(name));
            }
        });
    }

    /// Bytes and files held now.
    pub fn usage(&self) -> (u64, usize) {
        let index = self.shared.lock();
        (index.bytes, index.entries.len())
    }
}

impl Shared {
    /// The index. A panic while it was held may have left it out of step with
    /// the files, so a poisoned index starts again empty; the files it no
    /// longer lists are found on the next open.
    fn lock(&self) -> MutexGuard<'_, Index> {
        self.index.lock().unwrap_or_else(|poisoned| {
            let mut index = poisoned.into_inner();
            *index = Index::default();
            self.index.clear_poison();
            index
        })
    }
}

fn file_name(key: &str, range: &Range<u64>) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    (key, range.start, range.end).hash(&mut hasher);
    format!("{:016x}{SUFFIX}", hasher.finish())
}

fn header(key: &str, range: &Range<u64>) -> Vec<u8> {
    let mut out = Vec::with_capacity(MAGIC.len() + 4 + key.len() + 16);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key.as_bytes());
    out.extend_from_slice(&range.start.to_le_bytes());
    out.extend_from_slice(&range.end.to_le_bytes());
    out
}

/// The payload of a cache file, if it holds `key` at `range`.
fn payload(raw: Vec<u8>, key: &str, range: &Range<u64>) -> Option<Bytes> {
    let expected = header(key, range);
    if raw.len() < expected.len() || raw[..expected.len()] != expected[..] {
        return None;
    }
    let body = Bytes::from(raw).slice(expected.len()..);
    (body.len() as u64 == range.end - range.start).then_some(body)
}

/// Write the file under a temporary name and rename it into place, so a
/// reader never sees part of one. Returns its size.
fn write_file(
    dir: &Path,
    name: &str,
    key: &str,
    range: &Range<u64>,
    bytes: &[u8],
) -> std::io::Result<u64> {
    let head = header(key, range);
    let temporary = dir.join(format!(
        "{name}.{}.tmp",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0)
    ));
    let written = (|| {
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(&head)?;
        file.write_all(bytes)?;
        std::fs::rename(&temporary, dir.join(name))
    })();
    if written.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    written.map(|()| (head.len() + bytes.len()) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// Wait for the background writes to land.
    async fn settle(cache: &DiskCache, entries: usize) {
        for _ in 0..200 {
            if cache.usage().1 >= entries {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn a_range_comes_back_and_survives_a_reopen() -> TestResult {
        let dir = tempfile::tempdir()?;
        let cache = DiskCache::open(dir.path(), 1 << 20, 100)?;
        cache.insert("bundle/a.seg", &(10..15), Bytes::from_static(b"hello"));
        settle(&cache, 1).await;
        let got = cache.get("bundle/a.seg", &(10..15)).await;
        assert_eq!(got.as_deref(), Some(b"hello".as_slice()));
        assert!(cache.get("bundle/a.seg", &(10..14)).await.is_none());
        assert!(cache.get("bundle/b.seg", &(10..15)).await.is_none());

        let reopened = DiskCache::open(dir.path(), 1 << 20, 100)?;
        assert_eq!(reopened.usage().1, 1);
        let got = reopened.get("bundle/a.seg", &(10..15)).await;
        assert_eq!(got.as_deref(), Some(b"hello".as_slice()));
        Ok(())
    }

    #[tokio::test]
    async fn the_entry_cap_evicts_the_least_recently_used_file() -> TestResult {
        let dir = tempfile::tempdir()?;
        let cache = DiskCache::open(dir.path(), 1 << 20, 2)?;
        cache.insert("a", &(0..1), Bytes::from_static(b"a"));
        settle(&cache, 1).await;
        cache.insert("b", &(0..1), Bytes::from_static(b"b"));
        settle(&cache, 2).await;
        // Reading `a` makes `b` the oldest.
        assert!(cache.get("a", &(0..1)).await.is_some());
        cache.insert("c", &(0..1), Bytes::from_static(b"c"));
        for _ in 0..200 {
            if cache.get("c", &(0..1)).await.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(cache.get("b", &(0..1)).await.is_none(), "evicted");
        assert!(cache.get("a", &(0..1)).await.is_some());
        assert_eq!(cache.usage().1, 2);
        let files = std::fs::read_dir(dir.path())?.count();
        assert_eq!(files, 2, "the evicted file is deleted");
        Ok(())
    }

    #[tokio::test]
    async fn a_file_holding_another_range_reads_as_a_miss() -> TestResult {
        let dir = tempfile::tempdir()?;
        let cache = DiskCache::open(dir.path(), 1 << 20, 100)?;
        cache.insert("a", &(0..5), Bytes::from_static(b"right"));
        settle(&cache, 1).await;
        // Overwrite the file with another range's contents under the same name,
        // as a hash collision would.
        let name = file_name("a", &(0..5));
        std::fs::write(
            dir.path().join(&name),
            [header("z", &(0..5)), b"wrong".to_vec()].concat(),
        )?;
        assert!(cache.get("a", &(0..5)).await.is_none());
        assert_eq!(cache.usage().1, 0, "the bad entry is forgotten");
        Ok(())
    }
}
