#![forbid(unsafe_code)]

//! Append-only Raft log segments.
//!
//! Each record is `index u64`, `len u32`, payload, `crc32c u32`, all
//! little-endian. The CRC covers the index, the length, and the payload.
//! A short or bad-CRC tail is a crash remnant: open truncates it and keeps
//! every record that was synced before it.
//!
//! One writer and any number of readers share a log. The writer holds
//! `writer` across its write and `fsync`; it takes `index` only to publish
//! the records once they are durable. Readers take `index` just long enough
//! to copy record locations, then `pread` from read handles opened once per
//! segment, with no lock held. A truncate bumps the index generation, and a
//! reader that saw it change reads again, so it never returns bytes from
//! past a truncation. The most recent appends also stay in memory, up to a
//! fixed byte budget, so reading back what was just written does not reach
//! the disk.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Bound;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use crc::{Crc, CRC_32_ISCSI};
use serde::{Deserialize, Serialize};

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const HEADER: usize = 12;
const CRC_LEN: usize = 4;
/// A record with this index is alignment padding for a direct write. It is
/// not a Raft entry. Open skips it. `u64::MAX` is not a Raft log index this
/// store assigns.
const PAD_INDEX: u64 = u64::MAX;
/// Larger than this, a length field is a torn write rather than a record.
const MAX_PAYLOAD: u32 = 32 * 1024 * 1024;

/// Default segment size from ADR 0011.
pub const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Default memory for recent appends. An entry is a packed group of up to
/// about 1 MiB, and two can be committing at once, so this covers what
/// replication and apply read back right after the write. A read past it
/// goes to disk; `0` turns it off.
pub const DEFAULT_TAIL_BYTES: usize = 4 * 1024 * 1024;

/// A record's payload, shared between the tail and readers.
pub type Payload = Arc<Vec<u8>>;

#[derive(Clone, Copy, Debug)]
struct Loc {
    segment: u64,
    /// Offset of the record header inside the segment file.
    header: u64,
    len: u32,
}

#[derive(Serialize, Deserialize, Default)]
struct SegmentMeta {
    /// Indexes at or below this value are purged and must not be served.
    skip_through: Option<u64>,
}

/// Durable append-only log of opaque payloads, keyed by Raft index.
#[derive(Debug)]
pub struct SegmentLog {
    dir: PathBuf,
    /// Lock order: `writer`, then `index`.
    writer: Mutex<Writer>,
    index: Mutex<Index>,
}

/// What only the writer touches.
#[derive(Debug)]
struct Writer {
    segment_bytes: u64,
    active_segment: u64,
    /// Written with direct I/O on Linux. Readers use their own handles.
    active: File,
    active_len: u64,
    /// Bumped each time `active` is a different file. Registered `io_uring`
    /// descriptors follow this, not the raw number, which the kernel reuses.
    /// One writer means one submission per ring at a time.
    active_gen: u64,
    skip_through: Option<u64>,
    submit: crate::submit::Submitter,
}

/// What readers need, published by the writer after each durable change.
#[derive(Debug)]
struct Index {
    locs: BTreeMap<u64, Loc>,
    /// Read-only handles, opened on first read of a segment.
    files: BTreeMap<u64, Arc<File>>,
    /// Bumped by every truncate.
    generation: u64,
    /// The most recent appended payloads, at most `tail_cap` bytes.
    tail: BTreeMap<u64, Payload>,
    tail_bytes: usize,
    tail_cap: usize,
}

impl Index {
    fn new(locs: BTreeMap<u64, Loc>, tail_cap: usize) -> Self {
        Self {
            locs,
            files: BTreeMap::new(),
            generation: 0,
            tail: BTreeMap::new(),
            tail_bytes: 0,
            tail_cap,
        }
    }

    fn remember(&mut self, index: u64, payload: Payload) {
        if payload.len() > self.tail_cap {
            return;
        }
        self.tail_bytes += payload.len();
        self.tail.insert(index, payload);
        while self.tail_bytes > self.tail_cap {
            let (_, oldest) = self.tail.pop_first().expect("tail holds bytes");
            self.tail_bytes -= oldest.len();
        }
    }

    fn forget(&mut self, indexes: impl IntoIterator<Item = u64>) {
        for index in indexes {
            if let Some(payload) = self.tail.remove(&index) {
                self.tail_bytes -= payload.len();
            }
        }
    }

    /// Drop read handles for segments no record points into.
    fn close_unused(&mut self) {
        let locs = &self.locs;
        self.files
            .retain(|segment, _| locs.values().any(|loc| loc.segment == *segment));
    }

    fn file(&mut self, dir: &Path, segment: u64) -> io::Result<Arc<File>> {
        if let Some(file) = self.files.get(&segment) {
            return Ok(file.clone());
        }
        let file = Arc::new(File::open(segment_path(dir, segment))?);
        self.files.insert(segment, file.clone());
        Ok(file)
    }
}

/// Where one read gets a record from.
enum Source {
    Tail(Payload),
    Disk(Loc, Arc<File>),
}

impl SegmentLog {
    /// Open `dir`, rebuild the index, and drop a torn tail.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with(dir, DEFAULT_SEGMENT_BYTES, None, DEFAULT_TAIL_BYTES)
    }

    /// Open with a small segment size. Tests use this to force rotation.
    pub fn open_with_segment_size(dir: impl AsRef<Path>, segment_bytes: u64) -> io::Result<Self> {
        Self::open_with(dir, segment_bytes, None, DEFAULT_TAIL_BYTES)
    }

    /// Open, skipping indexes at or below `purged` as well as any this log
    /// recorded itself, and keeping up to `tail_bytes` of recent appends in
    /// memory. A caller that persists its own purge point passes it here
    /// and purges with [`SegmentLog::drop_through`].
    pub fn open_with(
        dir: impl AsRef<Path>,
        segment_bytes: u64,
        purged: Option<u64>,
        tail_bytes: usize,
    ) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let skip_through = load_meta(&dir)?.skip_through.max(purged);
        let mut segments = segment_files(&dir)?;
        let assemble = |dir: PathBuf, locs, writer| Self {
            dir,
            writer: Mutex::new(writer),
            index: Mutex::new(Index::new(locs, tail_bytes)),
        };
        if segments.is_empty() {
            let active = create_segment(&dir, 0)?;
            let writer = Writer {
                segment_bytes,
                active_segment: 0,
                active,
                active_len: 0,
                active_gen: 1,
                skip_through,
                submit: crate::submit::Submitter::new(),
            };
            return Ok(assemble(dir, BTreeMap::new(), writer));
        }

        let mut locs = BTreeMap::new();
        for &(id, ref path) in &segments {
            let valid = scan_segment(path, id, skip_through, &mut locs)?;
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            let meta_len = file.metadata()?.len();
            if meta_len != valid {
                file.set_len(valid)?;
                file.sync_all()?;
            }
        }
        // Drop a segment that lost every record to a torn tail.
        segments.retain(|(id, path)| {
            let keep = locs.values().any(|loc| loc.segment == *id);
            if !keep && *id != 0 {
                let _ = fs::remove_file(path);
            }
            keep || *id == 0
        });
        if locs.is_empty() {
            for (_, path) in &segments {
                let _ = fs::remove_file(path);
            }
            let active = create_segment(&dir, 0)?;
            let writer = Writer {
                segment_bytes,
                active_segment: 0,
                active,
                active_len: 0,
                active_gen: 1,
                skip_through,
                submit: crate::submit::Submitter::new(),
            };
            return Ok(assemble(dir, locs, writer));
        }

        let active_segment = locs
            .values()
            .map(|loc| loc.segment)
            .max()
            .expect("locs non-empty");
        let active_path = segment_path(&dir, active_segment);
        let mut active = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&active_path)?;
        let active_len = active.metadata()?.len();
        // Open leaves the cursor at byte 0. The next append must extend the tail.
        active.seek(SeekFrom::Start(active_len))?;
        let writer = Writer {
            segment_bytes,
            active_segment,
            active,
            active_len,
            active_gen: 1,
            skip_through,
            submit: crate::submit::Submitter::new(),
        };
        Ok(assemble(dir, locs, writer))
    }

    fn writer(&self) -> io::Result<MutexGuard<'_, Writer>> {
        self.writer
            .lock()
            .map_err(|_| io::Error::other("segment writer lock poisoned"))
    }

    fn index(&self) -> io::Result<MutexGuard<'_, Index>> {
        self.index
            .lock()
            .map_err(|_| io::Error::other("segment index lock poisoned"))
    }

    /// Highest stored index.
    pub fn last_index(&self) -> Option<u64> {
        self.index().ok()?.locs.keys().next_back().copied()
    }

    /// Write records and fsync once per segment. Readers see them only after
    /// that sync. On Linux the write and the fsync are one `io_uring` submission.
    pub fn append_batch(&self, records: &[(u64, &[u8])]) -> io::Result<()> {
        let mut writer = self.writer()?;
        let added = writer.write(&self.dir, self.last_index(), records)?;
        let mut index = self.index()?;
        index.locs.extend(added);
        Ok(())
    }

    /// [`SegmentLog::append_batch`], keeping the payloads in the in-memory
    /// tail so reading them back right away does not reach the disk.
    pub fn append_shared(&self, records: Vec<(u64, Payload)>) -> io::Result<()> {
        let mut writer = self.writer()?;
        let borrowed: Vec<(u64, &[u8])> = records
            .iter()
            .map(|(index, payload)| (*index, payload.as_slice()))
            .collect();
        let added = writer.write(&self.dir, self.last_index(), &borrowed)?;
        let mut index = self.index()?;
        index.locs.extend(added);
        for (at, payload) in records {
            index.remember(at, payload);
        }
        Ok(())
    }
}

impl Writer {
    /// Write and sync `records` after `last_stored`, and return where each
    /// landed. Nothing is published to readers here.
    fn write(
        &mut self,
        dir: &Path,
        last_stored: Option<u64>,
        records: &[(u64, &[u8])],
    ) -> io::Result<Vec<(u64, Loc)>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let mark = self.active_len;
        let mark_segment = self.active_segment;
        let mut added: Vec<(u64, Loc)> = Vec::with_capacity(records.len());
        let mut chunk: Vec<u8> = Vec::new();
        let mut chunk_at = self.active_len;
        for &(index, payload) in records {
            if payload.len() > MAX_PAYLOAD as usize {
                return self.fail_append(
                    mark_segment,
                    mark,
                    chunk_at,
                    io::Error::new(io::ErrorKind::InvalidInput, "payload exceeds 32MiB"),
                );
            }
            let last_batch = added.last().map(|(index, _)| *index);
            if let Some(last) = last_stored.max(last_batch) {
                if index <= last {
                    return self.fail_append(
                        mark_segment,
                        mark,
                        chunk_at,
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("index {index} is not after {last}"),
                        ),
                    );
                }
            }
            let need = record_len(payload.len());
            if self.active_len > 0 && self.active_len + need > self.segment_bytes {
                if let Err(err) = self.flush_chunk(chunk_at, &mut chunk) {
                    return self.fail_append(mark_segment, mark, chunk_at, err);
                }
                chunk.clear();
                let next = index;
                self.active = create_segment(dir, next)?;
                self.active_segment = next;
                self.active_len = 0;
                self.active_gen = self.active_gen.wrapping_add(1);
                chunk_at = 0;
            }
            let header = self.active_len;
            push_record(&mut chunk, index, payload);
            self.active_len += need;
            added.push((
                index,
                Loc {
                    segment: self.active_segment,
                    header,
                    len: payload.len() as u32,
                },
            ));
        }
        if let Err(err) = self.flush_chunk(chunk_at, &mut chunk) {
            return self.fail_append(mark_segment, mark, chunk_at, err);
        }
        Ok(added)
    }

    fn flush_chunk(&mut self, offset: u64, bytes: &mut Vec<u8>) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let before = bytes.len();
        let align = self.submit.io_align();
        if align > 1 {
            // The file end lands on a block boundary so the next append can
            // be one direct force-unit-access write.
            push_pad(bytes, offset, align);
        }
        let gen = self.active_gen;
        self.submit
            .write_at_sync(&mut self.active, gen, offset, bytes)?;
        self.active_len += (bytes.len() - before) as u64;
        Ok(())
    }

    /// The bytes did not hit disk. Roll the active file back when this batch
    /// did not rotate it.
    fn fail_append<T>(
        &mut self,
        mark_segment: u64,
        mark: u64,
        chunk_at: u64,
        err: io::Error,
    ) -> io::Result<T> {
        let rewind = if self.active_segment == mark_segment {
            mark
        } else {
            chunk_at
        };
        let _ = self.active.set_len(rewind);
        let _ = self.active.sync_all();
        self.active_len = rewind;
        Err(err)
    }
}

impl SegmentLog {
    /// `len` bytes of the payload at `index`, starting `offset` bytes in.
    /// Reads that range and nothing after it.
    pub fn read_slice(&self, index: u64, offset: u64, len: usize) -> io::Result<Option<Vec<u8>>> {
        if len > MAX_PAYLOAD as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "slice too large",
            ));
        }
        loop {
            let (generation, loc, file) = {
                let mut found = self.index()?;
                let Some(loc) = found.locs.get(&index).copied() else {
                    return Ok(None);
                };
                let file = found.file(&self.dir, loc.segment)?;
                (found.generation, loc, file)
            };
            if offset.saturating_add(len as u64) > u64::from(loc.len) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "slice outside the record",
                ));
            }
            let mut buf = vec![0u8; len];
            if len > 0 {
                file.read_exact_at(&mut buf, loc.header + HEADER as u64 + offset)?;
            }
            if self.index()?.generation == generation {
                return Ok(Some(buf));
            }
        }
    }

    /// Payload at `index`, if it is still in the log.
    pub fn get(&self, index: u64) -> io::Result<Option<Payload>> {
        Ok(self
            .range(Bound::Included(index), Bound::Included(index))?
            .into_iter()
            .next()
            .map(|(_, payload)| payload))
    }

    /// Records whose indexes fall in `range`, in order. No lock is held
    /// while reading from disk.
    pub fn range(&self, start: Bound<u64>, end: Bound<u64>) -> io::Result<Vec<(u64, Payload)>> {
        loop {
            let (generation, plan) = {
                let mut index = self.index()?;
                let mut plan = Vec::new();
                let wanted: Vec<(u64, Loc)> = index
                    .locs
                    .range((start, end))
                    .map(|(at, loc)| (*at, *loc))
                    .collect();
                for (at, loc) in wanted {
                    let source = match index.tail.get(&at) {
                        Some(payload) => Source::Tail(payload.clone()),
                        None => Source::Disk(loc, index.file(&self.dir, loc.segment)?),
                    };
                    plan.push((at, source));
                }
                (index.generation, plan)
            };
            let from_disk = plan
                .iter()
                .any(|(_, source)| matches!(source, Source::Disk(..)));
            let read: io::Result<Vec<(u64, Payload)>> = plan
                .into_iter()
                .map(|(at, source)| match source {
                    Source::Tail(payload) => Ok((at, payload)),
                    Source::Disk(loc, file) => Ok((at, Arc::new(read_payload(&file, loc)?))),
                })
                .collect();
            // A truncate while reading may have cut or rewritten those bytes.
            if !from_disk || self.index()?.generation == generation {
                return read;
            }
        }
    }

    /// Drop every record with `index >= from`, inclusive.
    pub fn truncate_from(&self, from: u64) -> io::Result<()> {
        let mut writer = self.writer()?;
        let mut index = self.index()?;
        let doomed: Vec<u64> = index.locs.range(from..).map(|(k, _)| *k).collect();
        if doomed.is_empty() {
            return Ok(());
        }
        index.generation += 1;
        let mut unique_segments: Vec<u64> = index.locs.values().map(|loc| loc.segment).collect();
        unique_segments.sort_unstable();
        unique_segments.dedup();

        for at in &doomed {
            index.locs.remove(at);
        }
        index.forget(doomed);
        index.close_unused();
        // Delete segment files that no longer have records, then truncate the
        // one that still does if the cut lands inside it.
        let mut survivors: Vec<u64> = index.locs.values().map(|loc| loc.segment).collect();
        survivors.sort_unstable();
        survivors.dedup();
        for segment in unique_segments {
            if !survivors.contains(&segment) && segment != 0 {
                let _ = fs::remove_file(segment_path(&self.dir, segment));
            }
        }
        if index.locs.is_empty() {
            let _ = fs::remove_file(segment_path(&self.dir, writer.active_segment));
            writer.active = create_segment(&self.dir, 0)?;
            writer.active_segment = 0;
            writer.active_len = 0;
            writer.active_gen = writer.active_gen.wrapping_add(1);
            return Ok(());
        }
        let active_segment = survivors.last().copied().unwrap_or(0);
        if active_segment != writer.active_segment {
            writer.active = OpenOptions::new()
                .read(true)
                .write(true)
                .open(segment_path(&self.dir, active_segment))?;
            writer.active_segment = active_segment;
            writer.active_gen = writer.active_gen.wrapping_add(1);
        }
        let last = index
            .locs
            .values()
            .filter(|loc| loc.segment == active_segment)
            .max_by_key(|loc| loc.header);
        let end = last
            .map(|loc| loc.header + record_len(loc.len as usize))
            .unwrap_or(0);
        writer.active.set_len(end)?;
        writer.active.seek(SeekFrom::Start(end))?;
        writer.active.sync_all()?;
        writer.active_len = end;
        Ok(())
    }

    /// Drop records with `index <= through`. Whole segments below the cut are
    /// deleted. The cut is persisted in this log's own meta file first.
    pub fn purge_through(&self, through: u64) -> io::Result<()> {
        let mut writer = self.writer()?;
        // Persist the skip first. A crash before the bytes are removed must
        // not resurrect those indexes on the next open.
        let skip = writer
            .skip_through
            .map_or(through, |current| current.max(through));
        store_meta(
            &self.dir,
            &SegmentMeta {
                skip_through: Some(skip),
            },
        )?;
        writer.skip_through = Some(skip);
        self.forget_through(&writer, through)
    }

    /// [`SegmentLog::purge_through`] for a caller that has already made the
    /// cut durable, and passes it to [`SegmentLog::open_with`] on the next
    /// open. Writes no meta file.
    pub fn drop_through(&self, through: u64) -> io::Result<()> {
        let mut writer = self.writer()?;
        writer.skip_through = writer.skip_through.max(Some(through));
        self.forget_through(&writer, through)
    }

    fn forget_through(&self, writer: &Writer, through: u64) -> io::Result<()> {
        let doomed_segments = {
            let mut index = self.index()?;
            let doomed: Vec<u64> = index.locs.range(..=through).map(|(k, _)| *k).collect();
            let mut touched = Vec::new();
            for at in &doomed {
                if let Some(loc) = index.locs.remove(at) {
                    touched.push(loc.segment);
                }
            }
            index.forget(doomed);
            touched.sort_unstable();
            touched.dedup();
            touched.retain(|segment| {
                *segment != writer.active_segment
                    && !index.locs.values().any(|loc| loc.segment == *segment)
            });
            index.close_unused();
            touched
        };
        for segment in doomed_segments {
            let _ = fs::remove_file(segment_path(&self.dir, segment));
        }
        Ok(())
    }
}

fn record_len(payload: usize) -> u64 {
    (HEADER + payload + CRC_LEN) as u64
}

fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:020}.seg"))
}

fn segment_files(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(stem) = name.strip_suffix(".seg") {
            if let Ok(id) = stem.parse::<u64>() {
                out.push((id, entry.path()));
            }
        }
    }
    out.sort_by_key(|(id, _)| *id);
    Ok(out)
}

fn create_segment(dir: &Path, id: u64) -> io::Result<File> {
    let path = segment_path(dir, id);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;
    file.sync_all()?;
    sync_dir(dir)?;
    Ok(file)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Extend `buf` so `offset + buf.len()` is a multiple of `align`, using one
/// padding record. `align` of 0 or 1 leaves the buffer alone.
fn push_pad(buf: &mut Vec<u8>, offset: u64, align: usize) {
    if align <= 1 {
        return;
    }
    let end = offset.saturating_add(buf.len() as u64);
    let mis = (end % align as u64) as usize;
    if mis == 0 {
        return;
    }
    let need = align - mis;
    let overhead = HEADER + CRC_LEN;
    let payload = if need >= overhead {
        need - overhead
    } else {
        need + align - overhead
    };
    push_record(buf, PAD_INDEX, &vec![0u8; payload]);
}

fn push_record(buf: &mut Vec<u8>, index: u64, payload: &[u8]) {
    let start = buf.len();
    buf.extend_from_slice(&index.to_le_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
    let mut digest = CRC32C.digest();
    digest.update(&buf[start..]);
    buf.extend_from_slice(&digest.finalize().to_le_bytes());
}

fn meta_path(dir: &Path) -> PathBuf {
    dir.join("segment-meta.json")
}

fn load_meta(dir: &Path) -> io::Result<SegmentMeta> {
    let path = meta_path(dir);
    if !path.exists() {
        return Ok(SegmentMeta::default());
    }
    let text = fs::read_to_string(path)?;
    serde_json::from_str(&text).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

fn store_meta(dir: &Path, meta: &SegmentMeta) -> io::Result<()> {
    let path = meta_path(dir);
    let tmp = dir.join("segment-meta.json.tmp");
    let bytes =
        serde_json::to_vec(meta).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    sync_dir(dir)?;
    Ok(())
}

fn scan_segment(
    path: &Path,
    segment: u64,
    skip_through: Option<u64>,
    locs: &mut BTreeMap<u64, Loc>,
) -> io::Result<u64> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut offset = 0u64;
    loop {
        if offset == file_len {
            return Ok(offset);
        }
        if file_len - offset < (HEADER + CRC_LEN) as u64 {
            return Ok(offset);
        }
        let mut header = [0u8; HEADER];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut header)?;
        let index = u64::from_le_bytes(header[..8].try_into().unwrap());
        let len = u32::from_le_bytes(header[8..12].try_into().unwrap());
        if len > MAX_PAYLOAD {
            return Ok(offset);
        }
        let total = record_len(len as usize);
        if file_len - offset < total {
            return Ok(offset);
        }
        let mut payload = vec![0u8; len as usize];
        file.read_exact(&mut payload)?;
        let mut crc_bytes = [0u8; 4];
        file.read_exact(&mut crc_bytes)?;
        let mut digest = CRC32C.digest();
        digest.update(&header);
        digest.update(&payload);
        if digest.finalize() != u32::from_le_bytes(crc_bytes) {
            return Ok(offset);
        }
        if index == PAD_INDEX {
            offset += total;
            continue;
        }
        if skip_through.is_some_and(|skip| index <= skip) {
            offset += total;
            continue;
        }
        locs.insert(
            index,
            Loc {
                segment,
                header: offset,
                len,
            },
        );
        offset += total;
    }
}

fn read_payload(file: &File, loc: Loc) -> io::Result<Vec<u8>> {
    let mut payload = vec![0u8; loc.len as usize];
    file.read_exact_at(&mut payload, loc.header + HEADER as u64)?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_reopen_and_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let log = SegmentLog::open_with_segment_size(dir.path(), 1024).unwrap();
        log.append_batch(&[(1, b"one"), (2, b"two")]).unwrap();
        drop(log);

        let mut raw = OpenOptions::new()
            .append(true)
            .open(segment_path(dir.path(), 0))
            .unwrap();
        raw.write_all(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).unwrap();
        raw.sync_all().unwrap();
        drop(raw);

        let log = SegmentLog::open(dir.path()).unwrap();
        assert_eq!(log.get(1).unwrap().unwrap().as_slice(), b"one");
        assert_eq!(log.get(2).unwrap().unwrap().as_slice(), b"two");
        assert_eq!(log.last_index(), Some(2));
        assert!(log.get(3).unwrap().is_none());
        log.append_batch(&[(3, b"three")]).unwrap();
        assert_eq!(log.get(1).unwrap().unwrap().as_slice(), b"one");
        assert_eq!(log.get(3).unwrap().unwrap().as_slice(), b"three");
    }

    #[test]
    fn truncate_and_purge() {
        let dir = tempfile::tempdir().unwrap();
        let log = SegmentLog::open_with_segment_size(dir.path(), 80).unwrap();
        log.append_batch(&[(1, b"aaaa"), (2, b"bbbb"), (3, b"cccc"), (4, b"dddd")])
            .unwrap();
        log.truncate_from(3).unwrap();
        assert!(log.get(3).unwrap().is_none());
        assert_eq!(log.get(2).unwrap().unwrap().as_slice(), b"bbbb");
        log.append_batch(&[(3, b"eee")]).unwrap();
        assert_eq!(log.get(3).unwrap().unwrap().as_slice(), b"eee");
        log.purge_through(2).unwrap();
        assert!(log.get(1).unwrap().is_none());
        assert!(log.get(2).unwrap().is_none());
        assert_eq!(log.get(3).unwrap().unwrap().as_slice(), b"eee");
        drop(log);
        let log = SegmentLog::open(dir.path()).unwrap();
        assert_eq!(log.get(3).unwrap().unwrap().as_slice(), b"eee");
        assert!(log.get(1).unwrap().is_none());
        assert_eq!(log.last_index(), Some(3));
    }

    #[test]
    fn padding_record_is_skipped_and_lands_on_a_block() {
        let mut buf = Vec::new();
        push_record(&mut buf, 1, b"one");
        push_pad(&mut buf, 0, 4096);
        assert_eq!(buf.len() % 4096, 0);
        assert!(buf.len() > 16);

        let mut unaligned = Vec::new();
        push_record(&mut unaligned, 7, b"x");
        push_pad(&mut unaligned, 10, 4096);
        assert_eq!((10 + unaligned.len()) % 4096, 0);

        let dir = tempfile::tempdir().unwrap();
        let path = segment_path(dir.path(), 0);
        std::fs::write(&path, &buf).unwrap();
        let log = SegmentLog::open(dir.path()).unwrap();
        assert_eq!(log.get(1).unwrap().unwrap().as_slice(), b"one");
        assert_eq!(log.last_index(), Some(1));
        assert!(log.get(u64::MAX).unwrap().is_none());
    }

    fn shared(records: &[(u64, &str)]) -> Vec<(u64, Payload)> {
        records
            .iter()
            .map(|(index, text)| (*index, Arc::new(text.as_bytes().to_vec())))
            .collect()
    }

    #[test]
    fn reads_do_not_wait_for_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(SegmentLog::open_with(dir.path(), 1024, None, 0).unwrap());
        log.append_batch(&[(1, b"one"), (2, b"two")]).unwrap();
        // A writer mid-fsync holds this lock.
        let held = log.writer.lock().unwrap();
        let reader = log.clone();
        let (done, wait) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = done.send(reader.range(Bound::Unbounded, Bound::Unbounded).unwrap());
        });
        let read = wait
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("a read blocked on the writer lock");
        drop(held);
        assert_eq!(read.len(), 2);
        assert_eq!(read[1].1.as_slice(), b"two");
    }

    #[test]
    fn recent_appends_are_read_from_memory() {
        let dir = tempfile::tempdir().unwrap();
        let log = SegmentLog::open_with(dir.path(), 1024, None, 1024).unwrap();
        log.append_shared(shared(&[(1, "one"), (2, "two")]))
            .unwrap();
        // Wipe the bytes on disk. Only the tail can still answer.
        OpenOptions::new()
            .write(true)
            .open(segment_path(dir.path(), 0))
            .unwrap()
            .set_len(0)
            .unwrap();
        assert_eq!(log.get(2).unwrap().unwrap().as_slice(), b"two");

        let dir = tempfile::tempdir().unwrap();
        let log = SegmentLog::open_with(dir.path(), 1024, None, 0).unwrap();
        log.append_shared(shared(&[(1, "one")])).unwrap();
        assert!(
            log.index.lock().unwrap().tail.is_empty(),
            "budget 0 keeps nothing"
        );
        assert_eq!(log.get(1).unwrap().unwrap().as_slice(), b"one");
    }

    #[test]
    fn the_tail_stays_within_its_budget() {
        let dir = tempfile::tempdir().unwrap();
        let log = SegmentLog::open_with(dir.path(), 1024, None, 10).unwrap();
        log.append_shared(shared(&[(1, "aaaa"), (2, "bbbb"), (3, "cccc")]))
            .unwrap();
        log.append_shared(shared(&[(4, "this is longer than ten")]))
            .unwrap();
        {
            let index = log.index.lock().unwrap();
            assert_eq!(index.tail.keys().copied().collect::<Vec<_>>(), vec![2, 3]);
            assert_eq!(index.tail_bytes, 8);
        }
        log.truncate_from(3).unwrap();
        log.drop_through(2).unwrap();
        let index = log.index.lock().unwrap();
        assert!(index.tail.is_empty());
        assert_eq!(index.tail_bytes, 0);
    }

    #[test]
    fn drop_through_relies_on_the_callers_purge_point() {
        let dir = tempfile::tempdir().unwrap();
        let log = SegmentLog::open_with(dir.path(), 64, None, 0).unwrap();
        log.append_batch(&[(1, b"aaaa"), (2, b"bbbb"), (3, b"cccc")])
            .unwrap();
        log.drop_through(2).unwrap();
        assert!(log.get(2).unwrap().is_none());
        assert!(
            !meta_path(dir.path()).exists(),
            "the caller persists the purge point, not the log"
        );
        drop(log);
        let log = SegmentLog::open_with(dir.path(), 64, Some(2), 0).unwrap();
        assert!(log.get(1).unwrap().is_none());
        assert!(log.get(2).unwrap().is_none());
        assert_eq!(log.get(3).unwrap().unwrap().as_slice(), b"cccc");
    }

    /// Readers race a writer that keeps truncating and rewriting the same
    /// indexes with new contents. Every record a reader returns has to be
    /// whole: its payload names its own index.
    #[test]
    fn reads_racing_truncation_never_return_torn_records() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(SegmentLog::open_with(dir.path(), 4096, None, 0).unwrap());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let log = log.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut seen = 0;
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        for (index, payload) in
                            log.range(Bound::Unbounded, Bound::Unbounded).unwrap()
                        {
                            let text = std::str::from_utf8(&payload).expect("torn utf-8");
                            let (named, _) = text.split_once(':').expect("torn record");
                            assert_eq!(named.parse::<u64>().unwrap(), index, "{text}");
                            seen += 1;
                        }
                    }
                    seen
                })
            })
            .collect();
        // Indexes 1-4 stay. 5-20 are cut and rewritten in place each round,
        // with lengths that change, so a stale location lands mid-record.
        let keep: Vec<(u64, Vec<u8>)> = (1..=4)
            .map(|index| (index, format!("{index}:keep").into_bytes()))
            .collect();
        let borrowed: Vec<(u64, &[u8])> = keep
            .iter()
            .map(|(index, bytes)| (*index, bytes.as_slice()))
            .collect();
        log.append_batch(&borrowed).unwrap();
        for round in 0..300u64 {
            let records: Vec<(u64, Vec<u8>)> = (5..=20)
                .map(|index| {
                    let pad = "x".repeat(((index * 7 + round) % 50) as usize);
                    (index, format!("{index}:{round}:{pad}").into_bytes())
                })
                .collect();
            let borrowed: Vec<(u64, &[u8])> = records
                .iter()
                .map(|(index, bytes)| (*index, bytes.as_slice()))
                .collect();
            log.append_batch(&borrowed).unwrap();
            log.truncate_from(5).unwrap();
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let seen: usize = readers
            .into_iter()
            .map(|reader| reader.join().unwrap())
            .sum();
        assert!(seen > 0, "readers never overlapped the writer");
    }
}
