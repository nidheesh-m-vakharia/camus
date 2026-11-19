//! Reading a linked source cluster's bucket (ADR 0018). A link names the
//! bucket; this module opens it with the keys from the broker's environment
//! and reads rows out of the source's newest indexed metadata snapshot, a
//! block run at a time, without fetching the whole snapshot.

use camus_metadata::mirror::MirrorLink;
use camus_metadata::snapshot::{index_span, SnapshotIndex, MAX_INDEX_BYTES, TRAILER_BYTES};
use camus_metadata::Topic;
use camus_storage::{Bucket, BucketConfig};

/// The pointer to the source's newest snapshot.
const CURRENT: &str = "meta/snap/CURRENT";
/// Most bytes a `CURRENT` pointer holds: one snapshot key.
const CURRENT_BYTES: u64 = 1024;
/// Tail bytes read first. An index entry is a key plus about 10 bytes, so
/// this holds the trailer and the index of a snapshot up to roughly 70 MiB:
/// such an open is two GETs. A larger source's index takes a third.
const TAIL_BYTES: u64 = 64 * 1024;

/// Most bytes one `rows` call reads. A single key is one block, 64 KiB plus
/// its row; a wider range of an honest source grows with the data, and the
/// copier reads such ranges in pieces under this cap.
pub const MAX_RUN_BYTES: u64 = 8 * 1024 * 1024;

/// The environment variables holding a link's keys: the alias upper-cased,
/// with `-` as `_`.
pub fn key_vars(alias: &str) -> (String, String) {
    let name = alias.to_ascii_uppercase().replace('-', "_");
    (
        format!("CAMUS_MIRROR_{name}_ACCESS_KEY"),
        format!("CAMUS_MIRROR_{name}_SECRET_KEY"),
    )
}

/// A client for a link's bucket. Fails, naming the variable, when this
/// broker's environment lacks a key.
pub fn open_link(link: &MirrorLink) -> anyhow::Result<Bucket> {
    let (access, secret) = key_vars(&link.alias);
    let read = |name: &str| {
        std::env::var(name).map_err(|_| anyhow::anyhow!("{name} is not set on this broker"))
    };
    Bucket::connect(&BucketConfig {
        endpoint: link.endpoint.clone(),
        bucket: link.bucket.clone(),
        region: link.region.clone(),
        access_key: read(&access)?,
        secret_key: read(&secret)?,
        prefix: link.prefix.clone(),
        virtual_hosted: link.virtual_host,
    })
}

/// A source's newest metadata snapshot, with its index loaded.
pub struct SourceSnapshot {
    bucket: Bucket,
    key: String,
    index: SnapshotIndex,
}

impl SourceSnapshot {
    /// Read `CURRENT`, then the snapshot's tail and index. A snapshot that is
    /// not `CAMUSKV3` is refused: only an indexed one can be read in part.
    pub async fn open(bucket: Bucket) -> anyhow::Result<Self> {
        let (pointer, size) = bucket.get_tail(CURRENT, CURRENT_BYTES).await?;
        anyhow::ensure!(
            size <= CURRENT_BYTES && pointer.len() as u64 == size,
            "{CURRENT} is {size} bytes"
        );
        let key = String::from_utf8(pointer.to_vec())?.trim().to_string();
        anyhow::ensure!(
            key.starts_with("meta/snap/") && key.ends_with(".snap"),
            "{CURRENT} names {key:?}, not a snapshot"
        );
        let (tail, body_len) = bucket.get_tail(&key, TAIL_BYTES).await?;
        let trailer = tail
            .len()
            .checked_sub(TRAILER_BYTES)
            .and_then(|at| tail.get(at..))
            .ok_or_else(|| anyhow::anyhow!("{key} is shorter than a trailer"))?;
        let span = index_span(trailer, body_len)
            .map_err(|err| anyhow::anyhow!("{key} is not an indexed CAMUSKV3 snapshot: {err}"))?;
        let tail_start = body_len
            .checked_sub(tail.len() as u64)
            .ok_or_else(|| anyhow::anyhow!("{key} sent more than its size"))?;
        let index = if span.range.start >= tail_start {
            let at = usize::try_from(span.range.start - tail_start)?;
            let end = usize::try_from(span.range.end - tail_start)?;
            tail.get(at..end)
                .ok_or_else(|| anyhow::anyhow!("{key} index is out of its tail"))?
                .to_vec()
        } else {
            anyhow::ensure!(
                span.range.end - span.range.start <= MAX_INDEX_BYTES,
                "{key} index is over the cap"
            );
            bucket.get_exact(&key, span.range.clone()).await?.to_vec()
        };
        let index = SnapshotIndex::parse(&span, &index).map_err(|err| anyhow::anyhow!(err))?;
        Ok(Self { bucket, key, index })
    }

    /// Every row with a key in `from..to`, from one ranged GET of at most
    /// [`MAX_RUN_BYTES`]. A wider run is refused, not read.
    pub async fn rows(&self, from: &str, to: &str) -> anyhow::Result<Vec<(String, String)>> {
        let Some(run) = self.index.run(from, to) else {
            return Ok(Vec::new());
        };
        let len = run.range.end - run.range.start;
        anyhow::ensure!(
            len <= MAX_RUN_BYTES,
            "{}: rows {from}..{to} span {len} bytes, over {MAX_RUN_BYTES}",
            self.key
        );
        let bytes = self.bucket.get_exact(&self.key, run.range.clone()).await?;
        self.index
            .rows(&run, &bytes, from, to)
            .map_err(|err| anyhow::anyhow!("{}: {err}", self.key))
    }

    /// The source's topic named `name`, if it has one.
    pub async fn topic(&self, name: &str) -> anyhow::Result<Option<Topic>> {
        let key = camus_metadata::topic_storage_key(name);
        // `\0` sorts before every other byte, so this range is the one key.
        let rows = self.rows(&key, &format!("{key}\0")).await?;
        match rows.into_iter().next() {
            Some((_, raw)) => Ok(Some(
                camus_metadata::parse_topic(&raw).map_err(|err| anyhow::anyhow!(err))?,
            )),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_variables_follow_the_alias() {
        assert_eq!(
            key_vars("us-east-1"),
            (
                "CAMUS_MIRROR_US_EAST_1_ACCESS_KEY".to_string(),
                "CAMUS_MIRROR_US_EAST_1_SECRET_KEY".to_string()
            )
        );
    }
}
