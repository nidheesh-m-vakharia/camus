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
    pub(crate) bucket: Bucket,
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

    /// The first rows, up to `limit`, with a key in `from..to`, read one
    /// block at a time so a long range costs only the blocks it needs.
    pub async fn first_rows(
        &self,
        from: &str,
        to: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let Some(run) = self.index.run(from, to) else {
            return Ok(Vec::new());
        };
        let mut rows = Vec::new();
        for block in self.index.each_block(&run) {
            let len = block.range.end - block.range.start;
            anyhow::ensure!(len <= MAX_RUN_BYTES, "{}: a block of {len} bytes", self.key);
            let bytes = self
                .bucket
                .get_exact(&self.key, block.range.clone())
                .await?;
            rows.extend(
                self.index
                    .rows(&block, &bytes, from, to)
                    .map_err(|err| anyhow::anyhow!("{}: {err}", self.key))?,
            );
            if rows.len() >= limit {
                rows.truncate(limit);
                break;
            }
        }
        Ok(rows)
    }

    /// The value at `key`, if the source has it.
    pub async fn row(&self, key: &str) -> anyhow::Result<Option<String>> {
        // `\0` sorts before every other byte, so this range is the one key.
        let rows = self.rows(key, &format!("{key}\0")).await?;
        Ok(rows.into_iter().next().map(|(_, value)| value))
    }

    /// The source's topic named `name`, if it has one.
    pub async fn topic(&self, name: &str) -> anyhow::Result<Option<Topic>> {
        match self.row(&camus_metadata::topic_storage_key(name)).await? {
            Some(raw) => Ok(Some(
                camus_metadata::parse_topic(&raw).map_err(|err| anyhow::anyhow!(err))?,
            )),
            None => Ok(None),
        }
    }
}

/// How long the copier waits between rounds on the metadata leader.
/// `CAMUS_MIRROR_COPY_MS`, 5 s by default. A source snapshot is at most one
/// snapshot interval old, so copy lag is that plus about one round.
fn copy_interval() -> std::time::Duration {
    let ms = std::env::var("CAMUS_MIRROR_COPY_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5_000u64)
        .max(50);
    std::time::Duration::from_millis(ms)
}

/// Bytes read from a source object at a time, and so the most one copied
/// object holds. Equal to the largest batch Camus accepts, so a window
/// always holds at least one whole batch.
const WINDOW_BYTES: u64 = camus_protocol::batch::MAX_BATCH_CEILING as u64;

/// Windows one partition may copy per round, so one partition far behind
/// does not hold up the rest.
const WINDOWS_PER_ROUND: usize = 8;

/// Bytes of batches the copier has published into mirrors.
pub static COPIED_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Mirror partitions the last round could not advance: a gap at the source,
/// a batch the copier cannot carry yet, or a source it could not read.
pub static STALLED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Rounds or partitions that failed with an error and will be retried.
pub static COPY_ERRORS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Copy sealed batches from each link's source into its mirror topics, on
/// the metadata leader, until the process stops (ADR 0018).
pub async fn run_copier(node: std::sync::Arc<camus_metadata::MetadataNode>, dest: Bucket) {
    use std::sync::atomic::Ordering::Relaxed;
    let interval = copy_interval();
    loop {
        tokio::time::sleep(interval).await;
        let metrics = node.raft.metrics().borrow().clone();
        if metrics.current_leader != Some(metrics.id) {
            STALLED.store(0, Relaxed);
            continue;
        }
        match copy_round(&node, &dest).await {
            Ok(stalled) => STALLED.store(stalled, Relaxed),
            Err(err) => {
                COPY_ERRORS.fetch_add(1, Relaxed);
                tracing::warn!(error = format!("{err:#}"), "mirror copy round failed");
            }
        }
    }
}

/// One pass over every link and mirror. Returns the stalled partitions.
async fn copy_round(node: &camus_metadata::MetadataNode, dest: &Bucket) -> anyhow::Result<u64> {
    use std::sync::atomic::Ordering::Relaxed;
    let links = node.scan_local(camus_metadata::mirror::links_prefix())?;
    let mut mirrors = Vec::new();
    for (_, raw) in node.scan_local("topic/")? {
        let topic = camus_metadata::parse_topic(&raw).map_err(|err| anyhow::anyhow!(err))?;
        if topic.mirror_of.is_some() {
            mirrors.push(topic);
        }
    }
    let mut stalled = 0;
    for (_, raw) in links {
        let link: MirrorLink = serde_json::from_str(&raw)?;
        let linked: Vec<&Topic> = mirrors
            .iter()
            .filter(|topic| {
                topic
                    .mirror_of
                    .as_ref()
                    .is_some_and(|of| of.alias == link.alias)
            })
            .collect();
        if linked.is_empty() {
            continue;
        }
        let opened = match open_link(&link) {
            Ok(bucket) => SourceSnapshot::open(bucket).await,
            Err(err) => Err(err),
        };
        let source = match opened {
            Ok(source) => source,
            Err(err) => {
                COPY_ERRORS.fetch_add(1, Relaxed);
                tracing::warn!(
                    alias = link.alias,
                    error = format!("{err:#}"),
                    "cannot read a mirror source"
                );
                stalled += linked
                    .iter()
                    .map(|topic| topic.partitions.max(0) as u64)
                    .sum::<u64>();
                continue;
            }
        };
        for topic in linked {
            for partition in 0..topic.partitions {
                match copy_partition(node, dest, &source, topic, partition).await {
                    Ok(Copied::Stalled(why)) => {
                        stalled += 1;
                        tracing::warn!(
                            topic = topic.name,
                            partition,
                            why,
                            "mirror partition stalled"
                        );
                    }
                    Ok(_) => {}
                    Err(err) => {
                        COPY_ERRORS.fetch_add(1, Relaxed);
                        tracing::warn!(
                            topic = topic.name,
                            partition,
                            error = format!("{err:#}"),
                            "mirror copy failed"
                        );
                    }
                }
            }
        }
    }
    Ok(stalled)
}

/// What one partition's copy did.
#[derive(Debug, PartialEq, Eq)]
enum Copied {
    /// Nothing left to copy.
    CaughtUp,
    /// Copied up to the per-round limit; more may remain.
    Some,
    /// Cannot go on without an operator or a later phase.
    Stalled(String),
}

/// Copy up to [`WINDOWS_PER_ROUND`] windows of `topic`'s `partition`.
async fn copy_partition(
    node: &camus_metadata::MetadataNode,
    dest: &Bucket,
    source: &SourceSnapshot,
    topic: &Topic,
    partition: i32,
) -> anyhow::Result<Copied> {
    let Some(of) = &topic.mirror_of else {
        return Ok(Copied::CaughtUp);
    };
    let Some(raw) = source
        .row(&camus_metadata::partition_storage_key(
            &of.topic_id,
            partition,
        ))
        .await?
    else {
        return Ok(Copied::Stalled("the source has no such partition".into()));
    };
    let from_source = camus_metadata::parse_partition(&raw).map_err(|err| anyhow::anyhow!(err))?;
    for _ in 0..WINDOWS_PER_ROUND {
        let raw = node
            .read_local(&camus_metadata::partition_storage_key(&topic.id, partition))?
            .ok_or_else(|| anyhow::anyhow!("{} has no partition {partition}", topic.name))?;
        let here = camus_metadata::parse_partition(&raw).map_err(|err| anyhow::anyhow!(err))?;
        let empty = here.log_start == here.next_offset;
        let end = here.next_offset;
        if !empty && from_source.log_start > end {
            return Ok(Copied::Stalled(format!(
                "the source's log starts at {}, past this mirror's end {end}",
                from_source.log_start
            )));
        }
        let Some(extent) = next_extent(source, &of.topic_id, partition, &from_source, end).await?
        else {
            return Ok(Copied::CaughtUp);
        };
        if !empty && extent.start > end {
            return Ok(Copied::Stalled(format!(
                "the source's next extent starts at {}, past this mirror's end {end}",
                extent.start
            )));
        }
        let start = if empty { end.max(extent.start) } else { end };
        if let Some(why) = copy_window(node, dest, source, topic, partition, &extent, start).await?
        {
            return Ok(Copied::Stalled(why));
        }
    }
    Ok(Copied::Some)
}

/// The source's first extent ending after `after`.
async fn next_extent(
    source: &SourceSnapshot,
    id: &str,
    partition: i32,
    state: &camus_metadata::Partition,
    after: i64,
) -> anyhow::Result<Option<camus_metadata::Extent>> {
    // Rows written before extents had keys of their own.
    if !state.extents.is_empty() {
        return Ok(state
            .extents
            .iter()
            .find(|extent| extent.end > after)
            .cloned());
    }
    let (from, to) = camus_metadata::extent_range(id, partition, after);
    match source.first_rows(&from, &to, 1).await?.into_iter().next() {
        Some((_, raw)) => Ok(Some(serde_json::from_str(&raw)?)),
        None => Ok(None),
    }
}

/// Copy whole batches of `extent` from offset `start` on, at most one
/// window, into a new object here, and publish them. `Some` is why it
/// stopped short of publishing.
async fn copy_window(
    node: &camus_metadata::MetadataNode,
    dest: &Bucket,
    source: &SourceSnapshot,
    topic: &Topic,
    partition: i32,
    extent: &camus_metadata::Extent,
    start: i64,
) -> anyhow::Result<Option<String>> {
    use std::sync::atomic::Ordering::Relaxed;
    let extent_end = extent.file_offset + u64::from(extent.length);
    let mut pos = extent.file_offset;
    // Skip batches below `start`, a window at a time; after a source
    // compaction the mirror's end can fall inside an extent.
    let (window, batches) = loop {
        anyhow::ensure!(
            pos < extent_end,
            "extent {} ends before offset {start}",
            extent.key
        );
        let bytes = source
            .bucket
            .get_exact(&extent.key, pos..extent_end.min(pos + WINDOW_BYTES))
            .await?;
        let whole = whole_batches(&bytes)?;
        let bytes = bytes.slice(..whole);
        let infos =
            camus_protocol::batch::inspect(&bytes, camus_protocol::batch::MAX_BATCH_CEILING)
                .map_err(|err| {
                    anyhow::anyhow!("{}: a batch fails its checks: {err:?}", extent.key)
                })?;
        let skip = infos
            .iter()
            .take_while(|info| info.last_offset < start)
            .count();
        if skip == infos.len() {
            pos += whole as u64;
            continue;
        }
        let skipped: usize = infos.iter().take(skip).map(|info| info.len).sum();
        let first = &infos[skip];
        if first.base_offset < start {
            return Ok(Some(format!(
                "offset {start} falls inside a batch at {} in {}",
                first.base_offset, extent.key
            )));
        }
        pos += skipped as u64;
        break (bytes.slice(skipped..), infos[skip..].to_vec());
    };
    if batches.iter().any(|info| info.transactional) {
        return Ok(Some(
            "transactional batches are copied once their spans are (phase 2)".into(),
        ));
    }
    let to_end = pos + window.len() as u64 == extent_end;
    // One part per run of batches sealed in the same leader epoch, so the
    // mirror's epoch history matches the source's.
    let topic_id = *uuid::Uuid::parse_str(&topic.id)?.as_bytes();
    let mut pieces = Vec::new();
    let mut epochs = Vec::new();
    let mut at = 0;
    let mut next_start = start;
    let mut runs = batches.iter().peekable();
    while let Some(first) = runs.next() {
        let epoch = camus_protocol::batch::leader_epoch(window.get(at..).unwrap_or_default());
        let mut len = first.len;
        let (mut base_ts, mut max_ts, mut last) =
            (first.base_timestamp, first.max_timestamp, first.last_offset);
        while let Some(next) = runs.peek() {
            let next_epoch =
                camus_protocol::batch::leader_epoch(window.get(at + len..).unwrap_or_default());
            if next_epoch != epoch {
                break;
            }
            base_ts = base_ts.min(next.base_timestamp);
            max_ts = max_ts.max(next.max_timestamp);
            last = next.last_offset;
            len += next.len;
            runs.next();
        }
        let end = if runs.peek().is_none() && to_end {
            extent.end
        } else {
            last + 1
        };
        pieces.push(camus_uploader::Piece {
            topic_id,
            partition,
            start: next_start,
            end,
            base_timestamp: base_ts,
            max_timestamp: max_ts,
            bytes: window.slice(at..at + len),
        });
        epochs.push(epoch);
        next_start = end;
        at += len;
    }
    let (index, body) = camus_uploader::encode(&pieces);
    let key = camus_uploader::object_key();
    dest.put_new(&key, body).await?;
    let parts = pieces
        .iter()
        .zip(index)
        .zip(epochs)
        .map(
            |((piece, (file_offset, length)), epoch)| camus_metadata::PublishPart {
                epoch,
                topic: topic.name.clone(),
                partition,
                start: piece.start,
                end: piece.end,
                object_key: key.clone(),
                file_offset,
                length,
                base_timestamp: piece.base_timestamp,
                max_timestamp: piece.max_timestamp,
            },
        )
        .collect();
    let published = node
        .apply(camus_metadata::Command::PublishMirror { parts })
        .await?;
    match published.error_code {
        0 => {
            COPIED_BYTES.fetch_add(window.len() as u64, Relaxed);
            Ok(None)
        }
        // Another round, or a leader before this one, got there first. The
        // object is an orphan for the sweep.
        1 => Ok(None),
        code => anyhow::bail!("publishing into {} failed with {code}", topic.name),
    }
}

/// Length of the prefix of `bytes` that is whole Kafka batches.
fn whole_batches(bytes: &[u8]) -> anyhow::Result<usize> {
    let mut at = 0;
    while let Some(length) = bytes.get(at + 8..at + 12) {
        let length = i32::from_be_bytes(length.try_into()?);
        anyhow::ensure!(length > 0, "a batch with length {length}");
        let total = 12 + length as usize;
        if at + total > bytes.len() {
            break;
        }
        at += total;
    }
    anyhow::ensure!(at > 0, "no whole batch in {} bytes", bytes.len());
    Ok(at)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fake batch of `total` bytes: only the length field matters here.
    fn batch_of(total: usize) -> Vec<u8> {
        let mut bytes = vec![0u8; total];
        let length = (total - 12) as i32;
        bytes[8..12].copy_from_slice(&length.to_be_bytes());
        bytes
    }

    #[test]
    fn whole_batches_stops_before_a_cut_batch() -> anyhow::Result<()> {
        let mut bytes = batch_of(100);
        bytes.extend(batch_of(70));
        assert_eq!(whole_batches(&bytes)?, 170);
        let cut = batch_of(50);
        bytes.extend(cut.get(..30).unwrap_or_default());
        assert_eq!(
            whole_batches(&bytes)?,
            170,
            "the cut batch waits for the next window"
        );
        assert!(whole_batches(cut.get(..30).unwrap_or_default()).is_err());
        assert!(whole_batches(&[0u8; 8]).is_err(), "no length field");
        Ok(())
    }

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
