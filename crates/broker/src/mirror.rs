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

    /// The snapshot object's key. A newer snapshot has another.
    pub fn key(&self) -> &str {
        &self.key
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
/// Mirror partitions the last round left waiting at a transaction the
/// source has not ended. One that waits long has a source that cannot end
/// it, such as one whose brokers are gone.
pub static WAITING: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Committed offsets the copier has synced into mirrors.
pub static GROUP_OFFSETS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Most partitions remembered as waiting. Past it the memory is cleared, and
/// those partitions are read again once.
const MAX_WAITS: usize = 65_536;

/// Partitions waiting at an open transaction, by mirror topic id and
/// partition, with the source snapshot they waited in. Until the source
/// writes a newer snapshot nothing can have changed, so the copier does not
/// read the extent again.
type Waits = std::collections::HashMap<(String, i32), String>;
/// Rounds or partitions that failed with an error and will be retried.
pub static COPY_ERRORS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Copy sealed batches from each link's source into its mirror topics, on
/// the metadata leader, until the process stops (ADR 0018).
pub async fn run_copier(
    node: std::sync::Arc<camus_metadata::MetadataNode>,
    dest: Bucket,
    coordinator: std::sync::Arc<camus_coordinator::Coordinator>,
) {
    use std::sync::atomic::Ordering::Relaxed;
    let interval = copy_interval();
    let mut waits = Waits::new();
    loop {
        tokio::time::sleep(interval).await;
        let metrics = node.raft.metrics().borrow().clone();
        if metrics.current_leader != Some(metrics.id) {
            STALLED.store(0, Relaxed);
            WAITING.store(0, Relaxed);
            waits.clear();
            continue;
        }
        if waits.len() > MAX_WAITS {
            waits.clear();
        }
        match copy_round(&node, &dest, &coordinator, &mut waits).await {
            Ok((stalled, waiting)) => {
                STALLED.store(stalled, Relaxed);
                WAITING.store(waiting, Relaxed);
            }
            Err(err) => {
                COPY_ERRORS.fetch_add(1, Relaxed);
                tracing::warn!(error = format!("{err:#}"), "mirror copy round failed");
            }
        }
    }
}

/// One pass over every link and mirror. Returns the stalled and the waiting
/// partitions.
async fn copy_round(
    node: &camus_metadata::MetadataNode,
    dest: &Bucket,
    coordinator: &camus_coordinator::Coordinator,
    waits: &mut Waits,
) -> anyhow::Result<(u64, u64)> {
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
    let mut waiting = 0;
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
        for topic in linked.iter().copied() {
            for partition in 0..topic.partitions {
                let at = (topic.id.clone(), partition);
                if waits.get(&at).is_some_and(|key| key == source.key()) {
                    waiting += 1;
                    continue;
                }
                let copied = copy_partition(node, dest, &source, topic, partition).await;
                if matches!(copied, Ok(Copied::Gone)) {
                    waits.retain(|(id, _), _| *id != topic.id);
                    break;
                }
                if !matches!(copied, Ok(Copied::Waiting)) {
                    waits.remove(&at);
                }
                match copied {
                    Ok(Copied::Waiting) => {
                        waiting += 1;
                        if waits.insert(at, source.key().to_string()).is_none() {
                            tracing::info!(
                                topic = topic.name,
                                partition,
                                "mirror partition waits for the source to end a transaction"
                            );
                        }
                    }
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
        if let Err(err) = sync_groups(node, coordinator, &source, &link.alias, &linked).await {
            COPY_ERRORS.fetch_add(1, Relaxed);
            tracing::warn!(
                alias = link.alias,
                error = format!("{err:#}"),
                "mirror group sync failed"
            );
        }
    }
    Ok((stalled, waiting))
}

/// Most committed offsets read for one group per round, a block of the
/// source snapshot at a time. A group with more is synced up to here.
const MAX_GROUP_OFFSETS: usize = 65_536;

/// Sync the committed offsets of the groups `alias` lists from the source
/// into the mirrors in `linked`, as MirrorMaker 2's
/// `sync.group.offsets.enabled` does. Each group is synced on its own, so
/// one that fails does not hold up the rest.
async fn sync_groups(
    node: &camus_metadata::MetadataNode,
    coordinator: &camus_coordinator::Coordinator,
    source: &SourceSnapshot,
    alias: &str,
    linked: &[&Topic],
) -> anyhow::Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    let get = |key: &str| node.read_local(key).map_err(|err| err.to_string());
    let listed =
        camus_metadata::mirror::groups_of(&get, alias).map_err(|err| anyhow::anyhow!(err))?;
    for group in listed {
        if let Err(err) = sync_group(node, coordinator, source, &group, linked).await {
            COPY_ERRORS.fetch_add(1, Relaxed);
            tracing::warn!(
                alias,
                group,
                error = format!("{err:#}"),
                "mirror group sync failed"
            );
        }
    }
    Ok(())
}

/// Sync one group. A group with members here is consuming the mirror, so
/// its offsets are its own and are left alone. Mirror offsets equal source
/// offsets, so nothing is translated. Each offset is clamped to the mirror's
/// end, and only one that moves the group forward is written. The commit
/// is stamped with this broker's clock, as a commit made here would be, so
/// the mirror's offset retention counts from the sync, not from the source.
async fn sync_group(
    node: &camus_metadata::MetadataNode,
    coordinator: &camus_coordinator::Coordinator,
    source: &SourceSnapshot,
    group: &str,
    linked: &[&Topic],
) -> anyhow::Result<()> {
    use camus_metadata::groups::{self, OffsetCommit, OffsetRecord};
    use std::sync::atomic::Ordering::Relaxed;
    let get = |key: &str| node.read_local(key).map_err(|err| err.to_string());
    let here = groups::read_group(&get, group).map_err(|err| anyhow::anyhow!(err))?;
    if here.is_some_and(|record| !record.members.is_empty()) {
        return Ok(());
    }
    let prefix = groups::offset_prefix(group);
    // `0` sorts right after `/`, so this bounds the group's keys.
    let end = format!("{}0", prefix.trim_end_matches('/'));
    let rows = source.first_rows(&prefix, &end, MAX_GROUP_OFFSETS).await?;
    if rows.len() == MAX_GROUP_OFFSETS {
        tracing::warn!(
            group,
            "mirror group sync read its first {MAX_GROUP_OFFSETS} offsets only"
        );
    }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64);
    let mut offsets = Vec::new();
    for (key, raw) in rows {
        let Some((topic_id, partition)) = groups::offset_from_key(group, &key) else {
            continue;
        };
        let Some(topic) = linked.iter().find(|topic| {
            topic
                .mirror_of
                .as_ref()
                .is_some_and(|of| of.topic_id == topic_id)
        }) else {
            continue;
        };
        if partition < 0 || partition >= topic.partitions {
            continue;
        }
        let record: OffsetRecord = serde_json::from_str(&raw)?;
        if record.offset < 0 {
            continue;
        }
        let offset = record.offset.min(next_end(node, topic, partition)?);
        let mine = get(&groups::offset_key(group, &topic.id, partition))
            .map_err(|err| anyhow::anyhow!(err))?
            .map(|raw| serde_json::from_str::<OffsetRecord>(&raw))
            .transpose()?;
        if mine.is_some_and(|mine| mine.offset >= offset) {
            continue;
        }
        // A clamped offset names a record the mirror does not hold yet, so
        // its epoch is not one the mirror can answer for.
        let leader_epoch = if offset < record.offset {
            -1
        } else {
            record.leader_epoch
        };
        offsets.push(OffsetCommit {
            topic_id: topic.id.clone(),
            partition,
            record: OffsetRecord {
                offset,
                leader_epoch,
                metadata: record.metadata,
                commit_timestamp: now_ms,
            },
        });
    }
    if offsets.is_empty() {
        return Ok(());
    }
    let count = offsets.len() as u64;
    let committed = node
        .apply(camus_metadata::Command::SyncMirrorOffsets {
            group: group.to_string(),
            offsets,
        })
        .await?;
    if committed.error_code == 0 {
        GROUP_OFFSETS.fetch_add(count, Relaxed);
        // List the group here, as a commit through OffsetCommit would.
        coordinator.touch(group);
    } else {
        // 3: a topic was promoted after this round began. Otherwise a
        // member has joined.
        tracing::info!(
            group,
            code = committed.error_code,
            "mirror group sync refused"
        );
    }
    Ok(())
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
    /// At a transaction the source has not ended.
    Waiting,
    /// The topic stopped being a mirror during the round.
    Gone,
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
    // Where the last window of an extent stopped, so the next window of the
    // same extent reads on from there instead of from its first byte.
    let mut resume: Option<(String, u64, i64)> = None;
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
        // A mirror that holds nothing starts at the source's log start, so it
        // does not serve records the source has deleted.
        let start = if empty {
            end.max(extent.start).max(from_source.log_start)
        } else {
            end
        };
        if empty && start > end {
            tracing::info!(
                topic = topic.name,
                partition,
                from = end,
                to = start,
                "mirror starts at the source's log start"
            );
        }
        let from = match &resume {
            Some((key, pos, at)) if *key == extent.key && *at == start => Some(*pos),
            _ => None,
        };
        let at = Place {
            extent: &extent,
            start,
            empty,
            from,
        };
        match copy_window(node, dest, source, &from_source, topic, partition, at).await? {
            Window::Published { pos } => {
                resume = Some((extent.key.clone(), pos, next_end(node, topic, partition)?))
            }
            Window::Raced => return Ok(Copied::Some),
            Window::Gone => return Ok(Copied::Gone),
            // A transaction the source has not ended: the next snapshot
            // that shows its span lets the copy go on.
            Window::Waiting => return Ok(Copied::Waiting),
            Window::Stalled(why) => return Ok(Copied::Stalled(why)),
        }
    }
    Ok(Copied::Some)
}

/// The mirror partition's next offset, as this broker has applied it.
fn next_end(
    node: &camus_metadata::MetadataNode,
    topic: &Topic,
    partition: i32,
) -> anyhow::Result<i64> {
    let raw = node
        .read_local(&camus_metadata::partition_storage_key(&topic.id, partition))?
        .ok_or_else(|| anyhow::anyhow!("{} has no partition {partition}", topic.name))?;
    Ok(camus_metadata::parse_partition(&raw)
        .map_err(|err| anyhow::anyhow!(err))?
        .next_offset)
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

/// What one window did.
enum Window {
    /// Published, and the extent's bytes before `pos` are done.
    Published { pos: u64 },
    /// Another copier published first. Stop this partition for the round.
    Raced,
    /// The topic is no longer a mirror: it was promoted, or deleted, since
    /// the round began.
    Gone,
    /// The next batch belongs to a transaction the source has not ended.
    Waiting,
    /// Cannot go on.
    Stalled(String),
}

/// Where a window starts.
struct Place<'a> {
    /// The source extent to read.
    extent: &'a camus_metadata::Extent,
    /// The first offset to publish.
    start: i64,
    /// The mirror partition holds nothing.
    empty: bool,
    /// A byte position inside the extent to read on from, when an earlier
    /// window of the same extent ended there.
    from: Option<u64>,
}

/// Copy whole batches of the extent `at` names, from its start offset on,
/// at most one window, into a new object here, and publish them with the
/// source's transaction spans over them, from `from_source`.
async fn copy_window(
    node: &camus_metadata::MetadataNode,
    dest: &Bucket,
    source: &SourceSnapshot,
    from_source: &camus_metadata::Partition,
    topic: &Topic,
    partition: i32,
    at: Place<'_>,
) -> anyhow::Result<Window> {
    let Place {
        extent,
        start,
        empty,
        from,
    } = at;
    let extent_end = extent
        .file_offset
        .checked_add(u64::from(extent.length))
        .ok_or_else(|| anyhow::anyhow!("extent {} overflows", extent.key))?;
    let mut pos = from.unwrap_or(extent.file_offset);
    // Skip batches below `start`, a window at a time: after a source
    // compaction the mirror's end can fall inside an extent.
    let (window, infos) = loop {
        if pos >= extent_end {
            // Every batch from here on is below `start`: a compaction
            // dropped the extent's tail. Its offsets hold nothing; advance
            // over them without an object.
            return publish(
                node,
                dest,
                topic,
                partition,
                &[],
                &bytes::Bytes::new(),
                start,
                extent.end,
                pos,
                Spans::default(),
            )
            .await;
        }
        let stop = pos.saturating_add(WINDOW_BYTES).min(extent_end);
        let bytes = source.bucket.get_exact(&extent.key, pos..stop).await?;
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
        let skipped: usize = infos.iter().take(skip).map(|info| info.len).sum();
        pos += skipped as u64;
        if skip < infos.len() {
            break (
                bytes.slice(skipped..),
                infos.get(skip..).unwrap_or_default().to_vec(),
            );
        }
    };
    let to_end = pos + window.len() as u64 == extent_end;
    let mut epochs = Vec::with_capacity(infos.len());
    let mut at = 0;
    for info in &infos {
        epochs.push(camus_protocol::batch::leader_epoch(
            window.get(at..).unwrap_or_default(),
        ));
        at += info.len;
    }
    // Each transactional batch's producer, to find its span at the source.
    let producers: Vec<i64> = if infos.iter().any(|info| info.transactional) {
        camus_protocol::batch::producer_batches(&window, camus_protocol::batch::MAX_BATCH_CEILING)
            .map_err(|err| anyhow::anyhow!("{}: {err:?}", extent.key))?
            .iter()
            .map(|batch| batch.producer_id)
            .collect()
    } else {
        Vec::new()
    };
    let ended = |offset: i64, producer: i64| {
        from_source
            .txn_committed
            .iter()
            .chain(&from_source.txn_aborted)
            .any(|span| span.covers(offset, producer))
    };
    let planned = plan(
        &infos, &epochs, &producers, &ended, start, extent.end, to_end, empty,
    );
    let runs = match planned {
        Ok(runs) if runs.is_empty() => return Ok(Window::Waiting),
        Ok(runs) => runs,
        Err(why) => return Ok(Window::Stalled(format!("{}: {why}", extent.key))),
    };
    let first = runs.first().map_or(start, |run| run.start);
    let last = runs.last().map_or(start, |run| run.end);
    let over = |span: &&camus_metadata::Span| span.start < last && span.end > first;
    let spans = Spans {
        committed: from_source
            .txn_committed
            .iter()
            .filter(over)
            .cloned()
            .collect(),
        aborted: from_source
            .txn_aborted
            .iter()
            .filter(over)
            .cloned()
            .collect(),
    };
    let copied: usize = runs.iter().map(|run| run.bytes.len()).sum();
    let next = pos + copied as u64;
    publish(
        node, dest, topic, partition, &runs, &window, start, extent.end, next, spans,
    )
    .await
}

/// The source's transaction spans over a copied range.
#[derive(Default)]
struct Spans {
    committed: Vec<camus_metadata::Span>,
    aborted: Vec<camus_metadata::Span>,
}

/// Put `runs` of `window` into a new object and commit them with `spans`.
/// With no runs, commit only the move of the next offset from `start` to
/// `empty_end`.
#[allow(clippy::too_many_arguments)]
async fn publish(
    node: &camus_metadata::MetadataNode,
    dest: &Bucket,
    topic: &Topic,
    partition: i32,
    runs: &[Run],
    window: &bytes::Bytes,
    start: i64,
    empty_end: i64,
    pos: u64,
    spans: Spans,
) -> anyhow::Result<Window> {
    use std::sync::atomic::Ordering::Relaxed;
    let parts: Vec<camus_metadata::PublishPart> = if runs.is_empty() {
        vec![camus_metadata::PublishPart {
            epoch: -1,
            topic: topic.name.clone(),
            partition,
            start,
            end: empty_end,
            object_key: String::new(),
            file_offset: 0,
            length: 0,
            base_timestamp: -1,
            max_timestamp: -1,
        }]
    } else {
        let topic_id = *uuid::Uuid::parse_str(&topic.id)?.as_bytes();
        let pieces: Vec<camus_uploader::Piece> = runs
            .iter()
            .map(|run| camus_uploader::Piece {
                topic_id,
                partition,
                start: run.start,
                end: run.end,
                base_timestamp: run.base_timestamp,
                max_timestamp: run.max_timestamp,
                bytes: window.slice(run.bytes.clone()),
            })
            .collect();
        let (index, body) = camus_uploader::encode(&pieces);
        let key = camus_uploader::object_key();
        dest.put_new(&key, body).await?;
        runs.iter()
            .zip(index)
            .map(|(run, (file_offset, length))| camus_metadata::PublishPart {
                epoch: run.epoch,
                topic: topic.name.clone(),
                partition,
                start: run.start,
                end: run.end,
                object_key: key.clone(),
                file_offset,
                length,
                base_timestamp: run.base_timestamp,
                max_timestamp: run.max_timestamp,
            })
            .collect()
    };
    let published = node
        .apply(camus_metadata::Command::PublishMirror {
            parts,
            committed: spans.committed.iter().map(Into::into).collect(),
            aborted: spans.aborted.iter().map(Into::into).collect(),
        })
        .await?;
    match published.error_code {
        0 => {
            let copied: usize = runs.iter().map(|run| run.bytes.len()).sum();
            COPIED_BYTES.fetch_add(copied as u64, Relaxed);
            Ok(Window::Published { pos })
        }
        // Another copier, such as a leader before this one, published first.
        // An object put for this attempt is an orphan for the sweep.
        1 => Ok(Window::Raced),
        3 => Ok(Window::Gone),
        code => anyhow::bail!("publishing into {} failed with {code}", topic.name),
    }
}

/// Batches of one leader epoch, as one published part.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Run {
    start: i64,
    end: i64,
    epoch: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    /// Byte range in the window.
    bytes: std::ops::Range<usize>,
}

/// Plan the parts for `infos`, whole batches of one window whose CRCs were
/// checked, in order, the first ending at or after `start`. `epochs` holds
/// each batch's leader epoch, and `producers` each batch's producer id when
/// any batch is transactional. `ended(offset, producer)` says the source
/// has a committed or aborted span over that batch. `extent_end` is the
/// source extent's exclusive end offset, and `to_end` says the window
/// reaches its last byte. `new_mirror` lets the first batch begin below
/// `start`: a new mirror starts at a source log start that can fall inside
/// a batch, as Kafka's can.
///
/// The plan stops before the first transactional batch the source has not
/// ended; an empty plan means wait for it. `Err` is why the copy stalls.
#[allow(clippy::too_many_arguments)]
fn plan(
    infos: &[camus_protocol::batch::BatchInfo],
    epochs: &[i32],
    producers: &[i64],
    ended: &dyn Fn(i64, i64) -> bool,
    start: i64,
    extent_end: i64,
    to_end: bool,
    new_mirror: bool,
) -> Result<Vec<Run>, String> {
    let open = infos.iter().enumerate().position(|(index, info)| {
        info.transactional
            && !producers
                .get(index)
                .is_some_and(|producer| ended(info.base_offset, *producer))
    });
    let (infos, to_end) = match open {
        Some(at) => (infos.get(..at).unwrap_or_default(), false),
        None => (infos, to_end),
    };
    let Some(first) = infos.first() else {
        return Ok(Vec::new());
    };
    if first.base_offset < start && !new_mirror {
        return Err(format!(
            "offset {start} falls inside a batch at {}",
            first.base_offset
        ));
    }
    // Each batch must start past the one before and stay inside the extent,
    // so the published offsets match the bytes.
    let mut floor = first.base_offset.min(start);
    for info in infos {
        if info.base_offset < floor
            || info.last_offset < info.base_offset
            || info.last_offset >= extent_end
        {
            return Err(format!(
                "a batch at {}..={} does not fit after {floor} in an extent ending at {extent_end}",
                info.base_offset, info.last_offset
            ));
        }
        floor = info.last_offset + 1;
    }
    let mut runs: Vec<Run> = Vec::new();
    let mut at = 0;
    for (index, info) in infos.iter().enumerate() {
        let epoch = epochs.get(index).copied().unwrap_or(-1);
        match runs.last_mut() {
            Some(run) if run.epoch == epoch => {
                run.end = info.last_offset + 1;
                run.base_timestamp = run.base_timestamp.min(info.base_timestamp);
                run.max_timestamp = run.max_timestamp.max(info.max_timestamp);
                run.bytes.end = at + info.len;
            }
            _ => {
                let run_start = runs.last().map_or(start, |run| run.end);
                runs.push(Run {
                    start: run_start,
                    end: info.last_offset + 1,
                    epoch,
                    base_timestamp: info.base_timestamp,
                    max_timestamp: info.max_timestamp,
                    bytes: at..at + info.len,
                });
            }
        }
        at += info.len;
    }
    // Records a compaction dropped from the extent's tail still count: the
    // last part ends where the extent does.
    if to_end {
        if let Some(run) = runs.last_mut() {
            run.end = extent_end;
        }
    }
    Ok(runs)
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

    use camus_protocol::batch::BatchInfo;

    /// A checked batch of `len` bytes covering `base..=last`.
    fn info(base: i64, last: i64, len: usize) -> BatchInfo {
        BatchInfo {
            len,
            base_offset: base,
            last_offset: last,
            records: (last - base + 1) as i32,
            base_timestamp: base,
            max_timestamp: last,
            transactional: false,
        }
    }

    fn none(_: i64, _: i64) -> bool {
        false
    }

    #[test]
    fn a_plan_splits_parts_where_the_epoch_changes() -> Result<(), String> {
        let infos = [info(0, 4, 10), info(5, 9, 20), info(10, 14, 30)];
        let runs = plan(&infos, &[3, 3, 4], &[], &none, 0, 15, false, false)?;
        assert_eq!(
            runs,
            vec![
                Run {
                    start: 0,
                    end: 10,
                    epoch: 3,
                    base_timestamp: 0,
                    max_timestamp: 9,
                    bytes: 0..30
                },
                Run {
                    start: 10,
                    end: 15,
                    epoch: 4,
                    base_timestamp: 10,
                    max_timestamp: 14,
                    bytes: 30..60
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn the_last_part_of_an_extent_ends_where_the_extent_does() -> Result<(), String> {
        // A compaction dropped 15..20 from the extent's tail.
        let runs = plan(&[info(0, 14, 10)], &[1], &[], &none, 0, 20, true, false)?;
        assert_eq!(runs.last().map(|run| run.end), Some(20));
        let short = plan(&[info(0, 14, 10)], &[1], &[], &none, 0, 20, false, false)?;
        assert_eq!(
            short.last().map(|run| run.end),
            Some(15),
            "more of the extent follows"
        );
        Ok(())
    }

    #[test]
    fn a_compaction_gap_inside_a_window_is_kept() -> Result<(), String> {
        // Offsets 5..9 were compacted away; the parts still cover them.
        let runs = plan(
            &[info(0, 4, 10), info(10, 14, 10)],
            &[1, 1],
            &[],
            &none,
            0,
            15,
            true,
            false,
        )?;
        assert_eq!((runs[0].start, runs[0].end), (0, 15));
        Ok(())
    }

    #[test]
    fn a_start_inside_a_batch_stalls_unless_the_mirror_is_new() -> Result<(), String> {
        let infos = [info(0, 9, 10)];
        assert!(plan(&infos, &[1], &[], &none, 5, 10, true, false).is_err());
        let runs = plan(&infos, &[1], &[], &none, 5, 10, true, true)?;
        assert_eq!(
            (runs[0].start, runs[0].end),
            (5, 10),
            "a new mirror starts at the log start"
        );
        Ok(())
    }

    #[test]
    fn a_plan_stops_before_an_open_transaction() -> Result<(), String> {
        let txn = |base, last| {
            let mut batch = info(base, last, 10);
            batch.transactional = true;
            batch
        };
        let infos = [info(0, 4, 10), txn(5, 9), txn(10, 14), info(15, 19, 10)];
        let producers = [-1, 7, 8, 9];
        // Producer 7's transaction ended at the source; producer 8's did not.
        let ended = |offset: i64, producer: i64| producer == 7 && (5..10).contains(&offset);
        let runs = plan(&infos, &[1; 4], &producers, &ended, 0, 20, true, false)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(
            (runs[0].start, runs[0].end, runs[0].bytes.clone()),
            (0, 10, 0..20)
        );
        // Waiting at the very first batch is an empty plan, not a stall.
        let waiting = plan(
            &infos[2..],
            &[1; 2],
            &producers[2..],
            &ended,
            10,
            20,
            true,
            false,
        )?;
        assert!(waiting.is_empty());
        // Every transaction ended: the whole window, to the extent's end.
        let all = |_: i64, _: i64| true;
        let runs = plan(&infos, &[1; 4], &producers, &all, 0, 20, true, false)?;
        assert_eq!(runs.last().map(|run| run.end), Some(20));
        Ok(())
    }

    #[test]
    fn batches_that_do_not_fit_their_extent_stall() {
        // Past the extent's end: publishing would claim offsets twice.
        assert!(plan(&[info(0, 149, 10)], &[1], &[], &none, 0, 100, true, false).is_err());
        // Going backwards.
        assert!(plan(
            &[info(0, 9, 10), info(5, 12, 10)],
            &[1, 1],
            &[],
            &none,
            0,
            20,
            true,
            false
        )
        .is_err());
        // Last below base.
        assert!(plan(&[info(5, 4, 10)], &[1], &[], &none, 0, 20, true, false).is_err());
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
