//! Append-only Raft log segments.
//!
//! Each record is `index u64`, `len u32`, payload, `crc32c u32`, all
//! little-endian. The CRC covers the index, the length, and the payload.
//! A short or bad-CRC tail is a crash remnant: open truncates it and keeps
//! every record that was synced before it.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use crc::{Crc, CRC_32_ISCSI};
use serde::{Deserialize, Serialize};

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const HEADER: usize = 12;
const CRC_LEN: usize = 4;
/// Larger than this, a length field is a torn write rather than a record.
const MAX_PAYLOAD: u32 = 32 * 1024 * 1024;

/// Default segment size from ADR 0011.
pub const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

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
    segment_bytes: u64,
    locs: BTreeMap<u64, Loc>,
    active_segment: u64,
    active: File,
    active_len: u64,
    skip_through: Option<u64>,
}

impl SegmentLog {
    /// Open `dir`, rebuild the index, and drop a torn tail.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_segment_size(dir, DEFAULT_SEGMENT_BYTES)
    }

    /// Open with a small segment size. Tests use this to force rotation.
    pub fn open_with_segment_size(dir: impl AsRef<Path>, segment_bytes: u64) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let skip_through = load_meta(&dir)?.skip_through;
        let mut segments = segment_files(&dir)?;
        if segments.is_empty() {
            let active = create_segment(&dir, 0)?;
            return Ok(Self {
                dir,
                segment_bytes,
                locs: BTreeMap::new(),
                active_segment: 0,
                active,
                active_len: 0,
                skip_through,
            });
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
            return Ok(Self {
                dir,
                segment_bytes,
                locs,
                active_segment: 0,
                active,
                active_len: 0,
                skip_through,
            });
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
        Ok(Self {
            dir,
            segment_bytes,
            locs,
            active_segment,
            active,
            active_len,
            skip_through,
        })
    }

    /// Highest stored index.
    pub fn last_index(&self) -> Option<u64> {
        self.locs.keys().next_back().copied()
    }

    /// Write records and `fsync` once. Readers see them only after the sync.
    pub fn append_batch(&mut self, records: &[(u64, &[u8])]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mark = self.active_len;
        let mark_segment = self.active_segment;
        let mut added: Vec<(u64, Loc)> = Vec::with_capacity(records.len());
        for &(index, payload) in records {
            if payload.len() > MAX_PAYLOAD as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "payload exceeds 32MiB",
                ));
            }
            let last_stored = self.locs.keys().next_back().copied();
            let last_batch = added.last().map(|(index, _)| *index);
            if let Some(last) = last_stored.max(last_batch) {
                if index <= last {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("index {index} is not after {last}"),
                    ));
                }
            }
            if self.active_len > 0
                && self.active_len + record_len(payload.len()) > self.segment_bytes
            {
                self.active.sync_all()?;
                let next = index;
                self.active = create_segment(&self.dir, next)?;
                self.active_segment = next;
                self.active_len = 0;
            }
            let header = self.active_len;
            // Truncate and reopen both leave the cursor somewhere other than
            // the logical tail. Pin it so the bytes land at `header`.
            self.active.seek(SeekFrom::Start(header))?;
            write_record(&mut self.active, index, payload)?;
            self.active_len += record_len(payload.len());
            added.push((
                index,
                Loc {
                    segment: self.active_segment,
                    header,
                    len: payload.len() as u32,
                },
            ));
        }
        if let Err(err) = self.active.sync_all() {
            // The new bytes did not hit disk. Roll the active file back when
            // we did not rotate; a rotated empty segment is removed.
            if self.active_segment == mark_segment {
                let _ = self.active.set_len(mark);
                let _ = self.active.sync_all();
                self.active_len = mark;
            }
            return Err(err);
        }
        for (index, loc) in added {
            self.locs.insert(index, loc);
        }
        Ok(())
    }

    /// Payload at `index`, if it is still in the log.
    pub fn get(&self, index: u64) -> io::Result<Option<Vec<u8>>> {
        let Some(loc) = self.locs.get(&index).copied() else {
            return Ok(None);
        };
        Ok(Some(read_payload(&self.dir, loc)?))
    }

    /// Records whose indexes fall in `range`, in order.
    pub fn range(&self, start: Bound<u64>, end: Bound<u64>) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let mut out = Vec::new();
        for (&index, &loc) in self.locs.range((start, end)) {
            out.push((index, read_payload(&self.dir, loc)?));
        }
        Ok(out)
    }

    /// Drop every record with `index >= from`, inclusive.
    pub fn truncate_from(&mut self, from: u64) -> io::Result<()> {
        let doomed: Vec<u64> = self.locs.range(from..).map(|(k, _)| *k).collect();
        if doomed.is_empty() {
            return Ok(());
        }
        let segments: Vec<u64> = self.locs.values().map(|loc| loc.segment).collect();
        let mut unique_segments: Vec<u64> = segments;
        unique_segments.sort_unstable();
        unique_segments.dedup();

        for index in &doomed {
            self.locs.remove(index);
        }
        // Delete segment files that no longer have records, then truncate the
        // one that still does if the cut lands inside it.
        let mut survivors: Vec<u64> = self.locs.values().map(|loc| loc.segment).collect();
        survivors.sort_unstable();
        survivors.dedup();
        for segment in unique_segments {
            if !survivors.contains(&segment) && segment != 0 {
                let _ = fs::remove_file(segment_path(&self.dir, segment));
            }
        }
        if self.locs.is_empty() {
            let _ = fs::remove_file(segment_path(&self.dir, self.active_segment));
            self.active = create_segment(&self.dir, 0)?;
            self.active_segment = 0;
            self.active_len = 0;
            return Ok(());
        }
        let active_segment = survivors.last().copied().unwrap_or(0);
        if active_segment != self.active_segment {
            self.active = OpenOptions::new()
                .read(true)
                .write(true)
                .open(segment_path(&self.dir, active_segment))?;
            self.active_segment = active_segment;
        }
        let last = self
            .locs
            .values()
            .filter(|loc| loc.segment == active_segment)
            .max_by_key(|loc| loc.header);
        let end = last
            .map(|loc| loc.header + record_len(loc.len as usize))
            .unwrap_or(0);
        self.active.set_len(end)?;
        self.active.seek(SeekFrom::Start(end))?;
        self.active.sync_all()?;
        self.active_len = end;
        Ok(())
    }

    /// Drop records with `index <= through`. Whole segments below the cut are deleted.
    pub fn purge_through(&mut self, through: u64) -> io::Result<()> {
        // Persist the skip first. A crash before the bytes are removed must
        // not resurrect those indexes on the next open.
        let skip = self
            .skip_through
            .map_or(through, |current| current.max(through));
        store_meta(
            &self.dir,
            &SegmentMeta {
                skip_through: Some(skip),
            },
        )?;
        self.skip_through = Some(skip);
        let doomed: Vec<u64> = self.locs.range(..=through).map(|(k, _)| *k).collect();
        let mut touched = Vec::new();
        for index in doomed {
            if let Some(loc) = self.locs.remove(&index) {
                touched.push(loc.segment);
            }
        }
        touched.sort_unstable();
        touched.dedup();
        for segment in touched {
            let still = self.locs.values().any(|loc| loc.segment == segment);
            if !still && segment != self.active_segment {
                let _ = fs::remove_file(segment_path(&self.dir, segment));
            }
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

fn write_record(file: &mut File, index: u64, payload: &[u8]) -> io::Result<()> {
    let mut header = [0u8; HEADER];
    header[..8].copy_from_slice(&index.to_le_bytes());
    header[8..12].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    let mut digest = CRC32C.digest();
    digest.update(&header);
    digest.update(payload);
    let crc = digest.finalize().to_le_bytes();
    file.write_all(&header)?;
    file.write_all(payload)?;
    file.write_all(&crc)?;
    Ok(())
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

fn read_payload(dir: &Path, loc: Loc) -> io::Result<Vec<u8>> {
    let mut file = File::open(segment_path(dir, loc.segment))?;
    file.seek(SeekFrom::Start(loc.header + HEADER as u64))?;
    let mut payload = vec![0u8; loc.len as usize];
    file.read_exact(&mut payload)?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_reopen_and_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentLog::open_with_segment_size(dir.path(), 1024).unwrap();
        log.append_batch(&[(1, b"one"), (2, b"two")]).unwrap();
        drop(log);

        let mut raw = OpenOptions::new()
            .append(true)
            .open(segment_path(dir.path(), 0))
            .unwrap();
        raw.write_all(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).unwrap();
        raw.sync_all().unwrap();
        drop(raw);

        let mut log = SegmentLog::open(dir.path()).unwrap();
        assert_eq!(log.get(1).unwrap().unwrap(), b"one");
        assert_eq!(log.get(2).unwrap().unwrap(), b"two");
        assert_eq!(log.last_index(), Some(2));
        assert!(log.get(3).unwrap().is_none());
        log.append_batch(&[(3, b"three")]).unwrap();
        assert_eq!(log.get(1).unwrap().unwrap(), b"one");
        assert_eq!(log.get(3).unwrap().unwrap(), b"three");
    }

    #[test]
    fn truncate_and_purge() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SegmentLog::open_with_segment_size(dir.path(), 80).unwrap();
        log.append_batch(&[(1, b"aaaa"), (2, b"bbbb"), (3, b"cccc"), (4, b"dddd")])
            .unwrap();
        log.truncate_from(3).unwrap();
        assert!(log.get(3).unwrap().is_none());
        assert_eq!(log.get(2).unwrap().unwrap(), b"bbbb");
        log.append_batch(&[(3, b"eee")]).unwrap();
        assert_eq!(log.get(3).unwrap().unwrap(), b"eee");
        log.purge_through(2).unwrap();
        assert!(log.get(1).unwrap().is_none());
        assert!(log.get(2).unwrap().is_none());
        assert_eq!(log.get(3).unwrap().unwrap(), b"eee");
        drop(log);
        let log = SegmentLog::open(dir.path()).unwrap();
        assert_eq!(log.get(3).unwrap().unwrap(), b"eee");
        assert!(log.get(1).unwrap().is_none());
        assert_eq!(log.last_index(), Some(3));
    }
}
