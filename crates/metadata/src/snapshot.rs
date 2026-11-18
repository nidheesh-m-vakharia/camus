//! Reading part of an indexed metadata snapshot (`CAMUSKV3`) from object
//! storage without the rest of it (ADR 0018). A mirror copier fetches the
//! body's tail, parses the index here, then ranged-reads only the blocks that
//! hold the keys it needs. Every block it reads is checked against its CRC32C.

use std::ops::Range;

use crate::machine::{parse_trailer, IndexEntry, CRC32C, INDEXED_MAGIC, TRAILER_LEN};

/// Bytes of a body's tail that hold its trailer.
pub const TRAILER_BYTES: usize = TRAILER_LEN;

/// Largest index a reader accepts, encoded. An honest entry is a key plus
/// about 10 bytes, so this holds tens of thousands of blocks: snapshots of
/// several GiB at one entry per 64 KiB.
pub const MAX_INDEX_BYTES: u64 = 4 * 1024 * 1024;

/// Most index entries a reader decodes. Checked before decoding, so a
/// hostile index of tiny entries cannot multiply into a large allocation:
/// at most this many keys, each at most [`MAX_INDEX_BYTES`] in total.
pub const MAX_INDEX_ENTRIES: u64 = 128 * 1024;

/// Where an indexed body's index lies, read from its trailer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpan {
    /// Byte range of the index in the body.
    pub range: Range<u64>,
    crc: u32,
}

/// Parse a body's trailer: its last [`TRAILER_BYTES`] bytes. `body_len` is
/// the whole body's length. An index over [`MAX_INDEX_BYTES`] is refused.
pub fn index_span(trailer: &[u8], body_len: u64) -> Result<IndexSpan, String> {
    let (at, len, crc) = parse_trailer(trailer, body_len)?;
    if len > MAX_INDEX_BYTES {
        return Err(format!(
            "snapshot index is {len} bytes, over the {MAX_INDEX_BYTES} byte cap"
        ));
    }
    Ok(IndexSpan {
        range: at..at + len,
        crc,
    })
}

/// A parsed block index.
#[derive(Debug, Clone)]
pub struct SnapshotIndex {
    entries: Vec<IndexEntry>,
    /// Where the rows end and the index starts.
    rows_end: u64,
}

/// A run of consecutive blocks, and the byte range that holds them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRun {
    /// Byte range in the body.
    pub range: Range<u64>,
    blocks: Range<usize>,
}

impl SnapshotIndex {
    /// Parse the index bytes `span` names, checked against its CRC32C.
    pub fn parse(span: &IndexSpan, index: &[u8]) -> Result<Self, String> {
        if index.len() as u64 != span.range.end - span.range.start {
            return Err("snapshot index is not the length its trailer names".into());
        }
        if CRC32C.checksum(index) != span.crc {
            return Err("snapshot index fails its CRC32C".into());
        }
        let count = leading_varint(index)?;
        if count > MAX_INDEX_ENTRIES {
            return Err(format!(
                "snapshot index has {count} entries, over the {MAX_INDEX_ENTRIES} cap"
            ));
        }
        let entries: Vec<IndexEntry> =
            camus_raft::codec::decode(index).map_err(|err| err.to_string())?;
        let mut last: Option<(&str, u64)> = None;
        for (key, at, _) in &entries {
            let in_order =
                last.is_none_or(|(prev_key, prev_at)| prev_key < key.as_str() && prev_at < *at);
            if !in_order || *at < INDEXED_MAGIC.len() as u64 || *at >= span.range.start {
                return Err(format!(
                    "snapshot index entry {key:?} at {at} is out of order"
                ));
            }
            last = Some((key, *at));
        }
        Ok(Self {
            entries,
            rows_end: span.range.start,
        })
    }

    /// The blocks that can hold keys in `from..to`, or `None` when no block
    /// can.
    pub fn run(&self, from: &str, to: &str) -> Option<BlockRun> {
        if from >= to || self.entries.is_empty() {
            return None;
        }
        // The last block starting at or before `from` may hold it.
        let first = self
            .entries
            .partition_point(|(key, _, _)| key.as_str() <= from)
            .saturating_sub(1);
        // Blocks starting at or after `to` hold nothing in range.
        let end = self
            .entries
            .partition_point(|(key, _, _)| key.as_str() < to);
        if end <= first {
            return None;
        }
        let start = self.entries.get(first)?.1;
        let stop = self.block_end(end - 1);
        Some(BlockRun {
            range: start..stop,
            blocks: first..end,
        })
    }

    /// The rows in `from..to` from `bytes`, the body bytes `run` names. Each
    /// block in the run is checked against its CRC32C.
    pub fn rows(
        &self,
        run: &BlockRun,
        bytes: &[u8],
        from: &str,
        to: &str,
    ) -> Result<Vec<(String, String)>, String> {
        if bytes.len() as u64 != run.range.end - run.range.start {
            return Err("snapshot blocks are not the length of their run".into());
        }
        let mut rows = Vec::new();
        for block in run.blocks.clone() {
            let (_, at, crc) = self
                .entries
                .get(block)
                .ok_or("snapshot block out of the index")?;
            let start = usize::try_from(at - run.range.start).map_err(|err| err.to_string())?;
            let end = usize::try_from(self.block_end(block) - run.range.start)
                .map_err(|err| err.to_string())?;
            let block_bytes = bytes
                .get(start..end)
                .ok_or("snapshot block out of its run")?;
            if CRC32C.checksum(block_bytes) != *crc {
                return Err(format!("snapshot block {block} fails its CRC32C"));
            }
            let mut rest = block_bytes;
            while !rest.is_empty() {
                let key = take_text(&mut rest)?;
                let value = take_text(&mut rest)?;
                if key.as_str() >= from && key.as_str() < to {
                    rows.push((key, value));
                }
            }
        }
        Ok(rows)
    }

    /// `run` split into one run per block, in key order, so a reader can
    /// stop once it has the rows it needs.
    pub fn each_block(&self, run: &BlockRun) -> Vec<BlockRun> {
        run.blocks
            .clone()
            .filter_map(|block| {
                let start = self.entries.get(block)?.1;
                Some(BlockRun {
                    range: start..self.block_end(block),
                    blocks: block..block + 1,
                })
            })
            .collect()
    }

    fn block_end(&self, block: usize) -> u64 {
        self.entries
            .get(block + 1)
            .map_or(self.rows_end, |(_, at, _)| *at)
    }
}

/// The varint at the start of `bytes`: a postcard sequence's length.
fn leading_varint(bytes: &[u8]) -> Result<u64, String> {
    let mut rest = bytes;
    take_varint(&mut rest)
}

/// One length-prefixed UTF-8 string, as postcard writes a `String`.
fn take_text(rest: &mut &[u8]) -> Result<String, String> {
    let len = take_varint(rest)?;
    let len = usize::try_from(len).map_err(|err| err.to_string())?;
    if rest.len() < len {
        return Err("snapshot row is truncated".into());
    }
    let (text, tail) = rest.split_at(len);
    *rest = tail;
    String::from_utf8(text.to_vec()).map_err(|err| err.to_string())
}

fn take_varint(rest: &mut &[u8]) -> Result<u64, String> {
    let mut len: u64 = 0;
    let mut shift = 0;
    loop {
        let (&byte, tail) = rest.split_first().ok_or("snapshot row is truncated")?;
        *rest = tail;
        if shift >= 64 {
            return Err("snapshot row length overflows".into());
        }
        len |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::machine::encode_indexed;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    /// A body, its index, and a reader over a byte range of it, as a copier
    /// would ranged-GET.
    struct Remote {
        body: Vec<u8>,
        index: SnapshotIndex,
    }

    impl Remote {
        fn new(rows: &[(String, String)]) -> Result<Self, Box<dyn std::error::Error>> {
            let borrowed: Vec<(&str, &str)> = rows
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect();
            let body = encode_indexed(&borrowed)?;
            let trailer = body.get(body.len() - TRAILER_BYTES..).ok_or("no trailer")?;
            let span = index_span(trailer, body.len() as u64)?;
            let index = body
                .get(span.range.start as usize..span.range.end as usize)
                .ok_or("no index")?;
            let index = SnapshotIndex::parse(&span, index)?;
            Ok(Self { body, index })
        }

        fn read(&self, from: &str, to: &str) -> Result<Vec<(String, String)>, String> {
            let Some(run) = self.index.run(from, to) else {
                return Ok(Vec::new());
            };
            let bytes = self
                .body
                .get(run.range.start as usize..run.range.end as usize)
                .ok_or("run out of bounds")?;
            self.index.rows(&run, bytes, from, to)
        }
    }

    /// Topics `a` to `e`, each with 400 extent rows, spanning many blocks.
    fn rows() -> Vec<(String, String)> {
        let mut rows = Vec::new();
        for topic in ["a", "b", "c", "d", "e"] {
            for at in 0..400 {
                rows.push((format!("ext/{topic}/{at:010}"), "v".repeat(200)));
            }
            rows.push((format!("part/{topic}/0"), "p".into()));
        }
        rows.sort();
        rows
    }

    #[test]
    fn a_range_read_returns_exactly_the_rows_in_range() -> TestResult {
        let all = rows();
        let remote = Remote::new(&all)?;
        for (from, to) in [
            ("ext/c/", "ext/c0"),
            ("ext/a/", "ext/a0"),
            ("ext/e/", "ext/e0"),
            ("part/b/", "part/b0"),
            ("ext/c/0000000100", "ext/c/0000000200"),
            ("zzz", "zzzz"),
            ("", "a"),
        ] {
            let expected: Vec<(String, String)> = all
                .iter()
                .filter(|(key, _)| key.as_str() >= from && key.as_str() < to)
                .cloned()
                .collect();
            assert_eq!(remote.read(from, to)?, expected, "{from}..{to}");
        }
        Ok(())
    }

    #[test]
    fn runs_at_block_boundaries_and_single_keys() -> TestResult {
        let all = rows();
        let remote = Remote::new(&all)?;
        let firsts: Vec<String> = remote
            .index
            .entries
            .iter()
            .map(|(key, _, _)| key.clone())
            .collect();
        assert!(firsts.len() > 3, "{} blocks", firsts.len());
        let expect = |from: &str, to: &str| -> Vec<(String, String)> {
            all.iter()
                .filter(|(key, _)| key.as_str() >= from && key.as_str() < to)
                .cloned()
                .collect()
        };
        // Bounds that are exactly a block's first key, on either side.
        for pair in firsts.windows(3) {
            let (from, to) = (&pair[0], &pair[2]);
            assert_eq!(remote.read(from, to)?, expect(from, to), "{from}..{to}");
            let run = remote.index.run(from, to).ok_or("no run")?;
            assert_eq!(run.blocks.len(), 2, "{to} starts the block after the run");
        }
        // One key, as `SourceSnapshot::topic` reads it.
        for (key, value) in all.iter().step_by(97) {
            let one = remote.read(key, &format!("{key}\0"))?;
            assert_eq!(one, vec![(key.clone(), value.clone())]);
            let run = remote.index.run(key, &format!("{key}\0")).ok_or("no run")?;
            assert_eq!(run.blocks.len(), 1, "one key is one block");
        }
        assert!(
            remote.index.run("ext/b/", "ext/b/").is_none(),
            "empty range"
        );
        assert!(remote.index.run("z", "a").is_none(), "reversed range");
        assert!(remote.index.run("", "a").is_none(), "before every key");
        Ok(())
    }

    #[test]
    fn a_run_read_block_by_block_matches_the_whole_run() -> TestResult {
        let remote = Remote::new(&rows())?;
        let (from, to) = ("ext/b/", "ext/d0");
        let run = remote.index.run(from, to).ok_or("no run")?;
        let blocks = remote.index.each_block(&run);
        assert_eq!(blocks.len(), run.blocks.len());
        let mut pieced = Vec::new();
        for block in &blocks {
            let bytes = remote
                .body
                .get(block.range.start as usize..block.range.end as usize)
                .ok_or("block out of bounds")?;
            pieced.extend(remote.index.rows(block, bytes, from, to)?);
        }
        assert_eq!(pieced, remote.read(from, to)?);
        Ok(())
    }

    #[test]
    fn an_index_claiming_too_many_entries_is_refused() -> TestResult {
        let mut index = Vec::new();
        let mut count = MAX_INDEX_ENTRIES + 1;
        while count >= 0x80 {
            index.push((count as u8 & 0x7f) | 0x80);
            count >>= 7;
        }
        index.push(count as u8);
        let span = IndexSpan {
            range: 8..8 + index.len() as u64,
            crc: CRC32C.checksum(&index),
        };
        let err = SnapshotIndex::parse(&span, &index)
            .err()
            .ok_or("huge index parsed")?;
        assert!(err.contains("cap"), "{err}");
        Ok(())
    }

    #[test]
    fn a_run_reads_far_less_than_the_body() -> TestResult {
        let remote = Remote::new(&rows())?;
        let run = remote.index.run("ext/c/", "ext/c0").ok_or("no run")?;
        let read = run.range.end - run.range.start;
        assert!(
            read * 3 < remote.body.len() as u64,
            "{read} of {} bytes",
            remote.body.len()
        );
        Ok(())
    }

    #[test]
    fn a_corrupt_block_in_the_run_is_refused() -> TestResult {
        let mut remote = Remote::new(&rows())?;
        let run = remote.index.run("ext/c/", "ext/c0").ok_or("no run")?;
        let at = (run.range.start + 20) as usize;
        let byte = remote.body.get_mut(at).ok_or("no byte")?;
        *byte ^= 0x01;
        let err = remote
            .read("ext/c/", "ext/c0")
            .err()
            .ok_or("corrupt block read")?;
        assert!(err.contains("CRC32C"), "{err}");
        Ok(())
    }

    #[test]
    fn an_oversized_or_misplaced_index_is_refused() -> TestResult {
        let remote = Remote::new(&rows())?;
        let len = remote.body.len() as u64;
        let trailer = remote
            .body
            .get(remote.body.len() - TRAILER_BYTES..)
            .ok_or("no trailer")?;
        assert!(index_span(trailer, len + 1).is_err(), "wrong body length");
        // A trailer that claims an index one byte over the cap, in a body
        // long enough to hold it.
        let body_len = MAX_INDEX_BYTES * 2;
        let index_len = MAX_INDEX_BYTES + 1;
        let index_at = body_len - TRAILER_BYTES as u64 - index_len;
        let mut big = index_at.to_le_bytes().to_vec();
        big.extend_from_slice(&index_len.to_le_bytes());
        big.extend_from_slice(&0u32.to_le_bytes());
        big.extend_from_slice(INDEXED_MAGIC);
        let err = index_span(&big, body_len)
            .err()
            .ok_or("big index accepted")?;
        assert!(err.contains("cap"), "{err}");
        Ok(())
    }

    #[test]
    fn an_empty_body_has_no_runs() -> TestResult {
        let remote = Remote::new(&[])?;
        assert!(remote.index.run("a", "z").is_none());
        Ok(())
    }
}
