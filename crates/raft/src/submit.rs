//! Durable append submission.
//!
//! On Linux an aligned append is one direct write with force-unit-access.
//! Completing that write is the ack. Anything else links a buffered write
//! to an `fsync`. The ring does not poll: a polling ring cannot `fsync`,
//! and a disk that does not poll never completes the direct write. The
//! segment file is registered, and the write buffer is pinned. Elsewhere,
//! and when direct I/O is refused, the call seeks, writes, and `sync_all`s.
//! `Ok` means the bytes survived a crash.

use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};

/// Bytes pinned for `WriteFixed`. A group commit of 16 × 32 KiB fits, with
/// the record headers. Larger appends are written in pieces of this size and
/// fsynced once, on the last piece.
#[cfg(target_os = "linux")]
const FIXED_BYTES: usize = 1024 * 1024;
/// Direct writes use this alignment. It satisfies a 512-byte and a 4096-byte
/// logical block. A drive that needs a larger one rejects the write, and the
/// append falls back to a buffered `fsync`.
#[cfg(target_os = "linux")]
const DIRECT_ALIGN: usize = 4096;

/// How a [`crate::segment::SegmentLog`] makes an append durable.
#[derive(Debug)]
pub struct Submitter {
    #[cfg(target_os = "linux")]
    ring: Option<Ring>,
    /// Buffer the produce path fills and then submits. On Linux it is aligned
    /// for a direct write, so that write does not copy it again.
    out: OutBuf,
}

#[cfg(target_os = "linux")]
struct Ring {
    /// Dropped before `fixed`, so the kernel releases the pin while the
    /// buffer is still allocated.
    ring: io_uring::IoUring,
    fixed: AlignedBuf,
    use_fixed: bool,
    use_files: bool,
    /// Aligned force-unit-access writes are the ack. Cleared when the kernel
    /// or the device rejects them.
    direct: bool,
    /// The ring was built with `IORING_SETUP_IOPOLL`. It cannot submit an
    /// `fsync`, so a rejected direct write uses `pwrite` instead.
    iopoll: bool,
    align: usize,
    /// `active_gen` last installed in slot 0. `None` until the first bind.
    bound_gen: Option<u64>,
}

#[cfg(target_os = "linux")]
struct AlignedBuf {
    ptr: *mut u8,
    layout: std::alloc::Layout,
    cap: usize,
}

#[cfg(target_os = "linux")]
// SAFETY: the allocation is owned by this value and is not shared. Moving the
// pointer across threads does not alias it.
unsafe impl Send for AlignedBuf {}

#[cfg(target_os = "linux")]
impl AlignedBuf {
    fn new(cap: usize, align: usize) -> io::Result<Self> {
        let cap = cap.next_multiple_of(align);
        let layout = std::alloc::Layout::from_size_align(cap, align)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        let ptr = unsafe { std::alloc::alloc(layout) };
        if ptr.is_null() {
            return Err(io::Error::from(io::ErrorKind::OutOfMemory));
        }
        Ok(Self { ptr, layout, cap })
    }

    fn copy_from(&mut self, src: &[u8]) {
        debug_assert!(src.len() <= self.cap);
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), self.ptr, src.len()) };
    }

    fn as_ptr(&self) -> *const u8 {
        self.ptr
    }
}

#[cfg(target_os = "linux")]
impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { std::alloc::dealloc(self.ptr, self.layout) };
    }
}

#[cfg(target_os = "linux")]
impl std::fmt::Debug for Ring {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Ring")
            .field("fixed_bytes", &self.fixed.cap)
            .field("use_fixed", &self.use_fixed)
            .field("use_files", &self.use_files)
            .field("direct", &self.direct)
            .field("iopoll", &self.iopoll)
            .finish()
    }
}

impl Submitter {
    /// Open a ring when the kernel supports write and fsync opcodes.
    pub fn new() -> Self {
        #[cfg(target_os = "linux")]
        {
            return Self {
                ring: open_ring(),
                out: OutBuf::new(),
            };
        }
        #[cfg(not(target_os = "linux"))]
        Self { out: OutBuf::new() }
    }

    /// Drop bytes staged for the next durable write.
    pub fn out_clear(&mut self) {
        self.out.clear();
    }

    /// Bytes staged for the next durable write.
    pub fn out_len(&self) -> usize {
        self.out.len()
    }

    /// Make room for `additional` more bytes so a later extend does not move
    /// what is already staged.
    pub fn out_reserve(&mut self, additional: usize) -> io::Result<()> {
        self.out.reserve(additional)
    }

    /// Append `src` to the staged write.
    pub fn out_extend(&mut self, src: &[u8]) -> io::Result<()> {
        self.out.extend_from_slice(src)
    }

    /// Staged bytes.
    pub fn out_slice(&self) -> &[u8] {
        self.out.as_slice()
    }

    /// Make the staged bytes durable at `offset` and then leave the buffer
    /// in place (the caller clears it). No second userspace copy.
    pub fn write_out(&mut self, file: &mut File, file_gen: u64, offset: u64) -> io::Result<()> {
        if self.out.len() == 0 {
            return file.sync_all();
        }
        #[cfg(target_os = "linux")]
        {
            let ptr = self.out.as_ptr();
            let len = self.out.len();
            let rejected = if let Some(ring) = self.ring.as_mut() {
                match ring.write_raw(file, file_gen, offset, ptr, len) {
                    Ok(()) => return Ok(()),
                    Err(err) if ring_rejected(&err) => Some(err),
                    Err(err) => return Err(err),
                }
            } else {
                None
            };
            if rejected.is_some() {
                tracing::warn!("io_uring append is unavailable; using pwrite and fsync");
                self.ring = None;
            }
            let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
            return std_write_at_sync(file, offset, bytes);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = file_gen;
            std_write_at_sync(file, offset, self.out.as_slice())
        }
    }

    /// Block size of a direct durable write. `0` means a buffered `fsync`.
    pub fn io_align(&self) -> usize {
        #[cfg(target_os = "linux")]
        if let Some(ring) = &self.ring {
            if ring.direct {
                return ring.align;
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = self;
        0
    }

    /// Write `bytes` at `offset` and make them durable before returning.
    ///
    /// `file_gen` changes when `file` is a different open file, including
    /// when the kernel reuses the same descriptor number.
    pub fn write_at_sync(
        &mut self,
        file: &mut File,
        file_gen: u64,
        offset: u64,
        bytes: &[u8],
    ) -> io::Result<()> {
        if bytes.is_empty() {
            return file.sync_all();
        }
        #[cfg(target_os = "linux")]
        {
            let rejected = if let Some(ring) = self.ring.as_mut() {
                match ring.write_at_sync(file, file_gen, offset, bytes) {
                    Ok(()) => return Ok(()),
                    Err(err) if ring_rejected(&err) => Some(err),
                    Err(err) => return Err(err),
                }
            } else {
                None
            };
            if rejected.is_some() {
                tracing::warn!("io_uring append is unavailable; using pwrite and fsync");
                self.ring = None;
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = file_gen;
        std_write_at_sync(file, offset, bytes)
    }
}

fn std_write_at_sync(file: &mut File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(bytes)?;
    file.sync_all()
}

#[cfg(target_os = "linux")]
fn open_ring() -> Option<Ring> {
    // `IORING_SETUP_IOPOLL` is not requested. The kernel accepts it on a disk
    // whose `queue/io_poll` is 0, and the direct write then never completes.
    // The same ring has to `fsync` when that write is refused.
    let (ring, iopoll) = match io_uring::IoUring::new(8) {
        Ok(ring) => (ring, false),
        Err(err) => {
            tracing::warn!(%err, "io_uring is unavailable; using pwrite and fsync");
            return None;
        }
    };
    let mut probe = io_uring::Probe::new();
    if ring.submitter().register_probe(&mut probe).is_err()
        || !probe.is_supported(io_uring::opcode::Write::CODE)
    {
        tracing::warn!("io_uring write is unsupported; using pwrite and fsync");
        return None;
    }
    if !iopoll && !probe.is_supported(io_uring::opcode::Fsync::CODE) {
        tracing::warn!("io_uring fsync is unsupported; using pwrite and fsync");
        return None;
    }
    // The ring's own descriptor is looked up on every enter. Registration
    // skips that. A kernel without it still submits normally.
    if let Err(err) = ring.submitter().register_ring_fd() {
        tracing::debug!(%err, "io_uring ring descriptor stays unregistered");
    }
    let fixed = AlignedBuf::new(FIXED_BYTES, DIRECT_ALIGN).ok()?;
    let use_fixed = probe.is_supported(io_uring::opcode::WriteFixed::CODE)
        && unsafe {
            // SAFETY: `fixed` keeps this allocation until the ring is dropped,
            // and the ring field is dropped first. The address does not change.
            // The kernel pins these pages until the ring goes away.
            let iov = libc::iovec {
                iov_base: fixed.ptr.cast::<libc::c_void>(),
                iov_len: fixed.cap,
            };
            ring.submitter()
                .register_buffers(std::slice::from_ref(&iov))
                .is_ok()
        };
    if !use_fixed {
        tracing::debug!("io_uring fixed buffer is unavailable");
    }
    Some(Ring {
        ring,
        fixed,
        use_fixed,
        use_files: true,
        direct: true,
        iopoll,
        align: DIRECT_ALIGN,
        bound_gen: None,
    })
}

#[cfg(target_os = "linux")]
fn ring_rejected(err: &io::Error) -> bool {
    // ENOSYS, EOPNOTSUPP. A later fsync error is a failed append, not a reason
    // to drop the ring.
    matches!(err.raw_os_error(), Some(38 | 95))
}

#[cfg(target_os = "linux")]
fn direct_rejected(err: &io::Error) -> bool {
    // ENOSYS, EOPNOTSUPP, EINVAL. EINVAL is what a filesystem returns when it
    // will not do a direct or force-unit-access write.
    matches!(err.raw_os_error(), Some(38 | 95 | 22))
}

#[cfg(target_os = "linux")]
fn set_direct(file: &File, on: bool) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let fd = file.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    let next = if on {
        flags | libc::O_DIRECT
    } else {
        flags & !libc::O_DIRECT
    };
    if next == flags {
        return Ok(());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, next) } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl Ring {
    fn write_at_sync(
        &mut self,
        file: &mut File,
        file_gen: u64,
        mut offset: u64,
        mut bytes: &[u8],
    ) -> io::Result<()> {
        self.bind(file, file_gen)?;
        let aligned =
            self.direct && offset % self.align as u64 == 0 && bytes.len() % self.align == 0;
        if aligned {
            match self.direct_write(file, offset, bytes) {
                Ok(()) => return Ok(()),
                Err(err) if direct_rejected(&err) => {
                    tracing::warn!(%err, "direct durable write is unavailable; using buffered fsync");
                    self.direct = false;
                    let _ = set_direct(file, false);
                }
                Err(err) => return Err(err),
            }
        }
        if self.iopoll {
            let _ = set_direct(file, false);
            return std_write_at_sync(file, offset, bytes);
        }
        let _ = set_direct(file, false);
        while !bytes.is_empty() {
            let window = if self.use_fixed {
                bytes.len().min(self.fixed.cap)
            } else {
                bytes.len().min(u32::MAX as usize)
            };
            let last = window == bytes.len();
            let wrote = self.submit_window(file, offset, &bytes[..window], last)?;
            if wrote == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "io_uring write returned 0",
                ));
            }
            offset += wrote as u64;
            bytes = &bytes[wrote..];
        }
        Ok(())
    }

    /// Install `file` in registered slot 0 when this is a new open file.
    fn bind(&mut self, file: &File, file_gen: u64) -> io::Result<()> {
        if !self.use_files || self.bound_gen == Some(file_gen) {
            return Ok(());
        }
        use std::os::fd::AsRawFd;
        let fd = file.as_raw_fd();
        let submitter = self.ring.submitter();
        let registered = if self.bound_gen.is_none() {
            submitter.register_files(&[fd])
        } else {
            submitter.register_files_update(0, &[fd]).map(|_| ())
        };
        match registered {
            Ok(()) => self.bound_gen = Some(file_gen),
            Err(err) => {
                tracing::warn!(%err, "io_uring file registration is unavailable");
                self.use_files = false;
                self.bound_gen = None;
            }
        }
        Ok(())
    }

    fn submit_window(
        &mut self,
        file: &File,
        offset: u64,
        bytes: &[u8],
        sync: bool,
    ) -> io::Result<usize> {
        use std::os::fd::AsRawFd;

        const WRITE: u64 = 1;
        const FSYNC: u64 = 2;

        let ptr = if self.use_fixed {
            self.fixed.copy_from(bytes);
            self.fixed.as_ptr()
        } else {
            bytes.as_ptr()
        };
        let len = bytes.len() as u32;
        let raw = file.as_raw_fd();
        let write = if self.use_files {
            let fd = io_uring::types::Fixed(0);
            if self.use_fixed {
                io_uring::opcode::WriteFixed::new(fd, ptr, len, 0)
                    .offset(offset)
                    .build()
            } else {
                io_uring::opcode::Write::new(fd, ptr, len)
                    .offset(offset)
                    .build()
            }
        } else if self.use_fixed {
            let fd = io_uring::types::Fd(raw);
            io_uring::opcode::WriteFixed::new(fd, ptr, len, 0)
                .offset(offset)
                .build()
        } else {
            let fd = io_uring::types::Fd(raw);
            io_uring::opcode::Write::new(fd, ptr, len)
                .offset(offset)
                .build()
        };
        let write = write.user_data(WRITE);
        let write = if sync {
            write.flags(io_uring::squeue::Flags::IO_LINK)
        } else {
            write
        };
        let fsync = if self.use_files {
            io_uring::opcode::Fsync::new(io_uring::types::Fixed(0))
                .build()
                .user_data(FSYNC)
        } else {
            io_uring::opcode::Fsync::new(io_uring::types::Fd(raw))
                .build()
                .user_data(FSYNC)
        };
        // SAFETY: `ptr` addresses either `self.fixed` or `bytes`. Both stay
        // put until `submit_and_wait` returns, and this ring has no other
        // submission in flight. `bytes` is borrowed for this call. `fixed`
        // is not reallocated. The kernel reads that memory, and uses the
        // descriptor, only for these operations.
        unsafe {
            let mut queue = self.ring.submission();
            queue
                .push(&write)
                .map_err(|err| io::Error::other(format!("io_uring push: {err:?}")))?;
            if sync {
                queue
                    .push(&fsync)
                    .map_err(|err| io::Error::other(format!("io_uring push: {err:?}")))?;
            }
        }
        let mut ids = vec![WRITE];
        if sync {
            ids.push(FSYNC);
        }
        let results = self.reap(&ids)?;
        let write_res = results[0];
        if write_res < 0 {
            return Err(io::Error::from_raw_os_error(-write_res));
        }
        if sync {
            let sync_res = results[1];
            if sync_res < 0 {
                return Err(io::Error::from_raw_os_error(-sync_res));
            }
        }
        Ok(write_res as usize)
    }

    /// One or more direct writes. Each write carries `RWF_DSYNC`, so its
    /// completion is the durable ack. There is no following `fsync`.
    fn direct_write(
        &mut self,
        file: &mut File,
        mut offset: u64,
        mut bytes: &[u8],
    ) -> io::Result<()> {
        set_direct(file, true)?;
        while !bytes.is_empty() {
            let n = bytes.len().min(self.fixed.cap);
            let wrote = self.submit_dsync(file, offset, &bytes[..n])?;
            if wrote == 0 || wrote % self.align != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "direct write returned a short unaligned count",
                ));
            }
            offset += wrote as u64;
            bytes = &bytes[wrote..];
        }
        Ok(())
    }

    fn submit_dsync(&mut self, file: &File, offset: u64, bytes: &[u8]) -> io::Result<usize> {
        use std::os::fd::AsRawFd;
        const WRITE: u64 = 1;
        self.fixed.copy_from(bytes);
        let ptr = self.fixed.as_ptr();
        let len = bytes.len() as u32;
        let raw = file.as_raw_fd();
        let flags = libc::RWF_DSYNC as i32;
        let write = if self.use_files && self.use_fixed {
            io_uring::opcode::WriteFixed::new(io_uring::types::Fixed(0), ptr, len, 0)
                .offset(offset)
                .rw_flags(flags)
                .build()
        } else if self.use_files {
            io_uring::opcode::Write::new(io_uring::types::Fixed(0), ptr, len)
                .offset(offset)
                .rw_flags(flags)
                .build()
        } else if self.use_fixed {
            io_uring::opcode::WriteFixed::new(io_uring::types::Fd(raw), ptr, len, 0)
                .offset(offset)
                .rw_flags(flags)
                .build()
        } else {
            io_uring::opcode::Write::new(io_uring::types::Fd(raw), ptr, len)
                .offset(offset)
                .rw_flags(flags)
                .build()
        }
        .user_data(WRITE);
        // SAFETY: `ptr` is `self.fixed`, which stays allocated until this wait
        // returns. The kernel reads it only for this one write. No other
        // submission is in flight on this ring.
        unsafe {
            let mut queue = self.ring.submission();
            queue
                .push(&write)
                .map_err(|err| io::Error::other(format!("io_uring push: {err:?}")))?;
        }
        let write_res = self.reap(&[WRITE])?[0];
        if write_res < 0 {
            Err(io::Error::from_raw_os_error(-write_res))
        } else {
            Ok(write_res as usize)
        }
    }

    /// Durable write of `len` bytes at `ptr`. The pointer is the caller's
    /// buffer, not the registered fixed buffer, so this does not copy.
    fn write_raw(
        &mut self,
        file: &mut File,
        file_gen: u64,
        offset: u64,
        ptr: *const u8,
        len: usize,
    ) -> io::Result<()> {
        self.bind(file, file_gen)?;
        let aligned = self.direct
            && offset % self.align as u64 == 0
            && len % self.align == 0
            && !ptr.is_null()
            && (ptr as usize) % self.align == 0;
        if aligned {
            match self.direct_raw(file, offset, ptr, len) {
                Ok(()) => return Ok(()),
                Err(err) if direct_rejected(&err) => {
                    tracing::warn!(
                        %err,
                        "direct durable write is unavailable; using buffered fsync"
                    );
                    self.direct = false;
                    let _ = set_direct(file, false);
                }
                Err(err) => return Err(err),
            }
        }
        if self.iopoll {
            let _ = set_direct(file, false);
            let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
            return std_write_at_sync(file, offset, bytes);
        }
        let _ = set_direct(file, false);
        let mut done = 0;
        while done < len {
            let window = (len - done).min(u32::MAX as usize);
            let last = done + window == len;
            let wrote = self.submit_ptr(
                file,
                offset + done as u64,
                unsafe { ptr.add(done) },
                window,
                last,
                false,
            )?;
            if wrote == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "io_uring write returned 0",
                ));
            }
            done += wrote;
        }
        Ok(())
    }

    fn direct_raw(
        &mut self,
        file: &mut File,
        mut offset: u64,
        ptr: *const u8,
        len: usize,
    ) -> io::Result<()> {
        set_direct(file, true)?;
        let mut done = 0;
        while done < len {
            let window = (len - done).min(FIXED_BYTES);
            let wrote =
                self.submit_ptr(file, offset, unsafe { ptr.add(done) }, window, false, true)?;
            if wrote == 0 || wrote % self.align != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "direct write returned a short unaligned count",
                ));
            }
            offset += wrote as u64;
            done += wrote;
        }
        Ok(())
    }

    /// `dsync` sets `RWF_DSYNC` and is the ack. `sync` links an `fsync`.
    fn submit_ptr(
        &mut self,
        file: &File,
        offset: u64,
        ptr: *const u8,
        len: usize,
        sync: bool,
        dsync: bool,
    ) -> io::Result<usize> {
        use std::os::fd::AsRawFd;
        const WRITE: u64 = 1;
        const FSYNC: u64 = 2;
        let raw = file.as_raw_fd();
        let flags = if dsync { libc::RWF_DSYNC as i32 } else { 0 };
        let write = if self.use_files {
            io_uring::opcode::Write::new(io_uring::types::Fixed(0), ptr, len as u32)
                .offset(offset)
                .rw_flags(flags)
                .build()
        } else {
            io_uring::opcode::Write::new(io_uring::types::Fd(raw), ptr, len as u32)
                .offset(offset)
                .rw_flags(flags)
                .build()
        }
        .user_data(WRITE);
        let write = if sync {
            write.flags(io_uring::squeue::Flags::IO_LINK)
        } else {
            write
        };
        let fsync = if self.use_files {
            io_uring::opcode::Fsync::new(io_uring::types::Fixed(0))
                .build()
                .user_data(FSYNC)
        } else {
            io_uring::opcode::Fsync::new(io_uring::types::Fd(raw))
                .build()
                .user_data(FSYNC)
        };
        // SAFETY: `ptr` addresses the caller's `OutBuf`, which stays allocated
        // and is not written until this wait returns. This ring has no other
        // submission in flight. The kernel reads that memory, and uses the
        // descriptor, only for these operations.
        unsafe {
            let mut queue = self.ring.submission();
            queue
                .push(&write)
                .map_err(|err| io::Error::other(format!("io_uring push: {err:?}")))?;
            if sync {
                queue
                    .push(&fsync)
                    .map_err(|err| io::Error::other(format!("io_uring push: {err:?}")))?;
            }
        }
        let mut ids = vec![WRITE];
        if sync {
            ids.push(FSYNC);
        }
        let results = self.reap(&ids)?;
        if results[0] < 0 {
            return Err(io::Error::from_raw_os_error(-results[0]));
        }
        if sync && results[1] < 0 {
            return Err(io::Error::from_raw_os_error(-results[1]));
        }
        Ok(results[0] as usize)
    }

    /// Wait for `ids` and return one result each, in that order.
    ///
    /// A completion whose `user_data` is not one of `ids` is still consumed.
    /// Exactly one batch is in flight, and a kernel that echoes `0` would
    /// otherwise leave the result unread and fail the append.
    fn reap(&mut self, ids: &[u64]) -> io::Result<Vec<i32>> {
        let submitted = self.ring.submit_and_wait(ids.len())?;
        let mut got = self.drain_completions();
        if got.is_empty() && submitted > 0 {
            self.ring.submit_and_wait(ids.len())?;
            got = self.drain_completions();
        }
        let mut ordered = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(pos) = got.iter().position(|(user, _)| user == id) {
                ordered.push(got.remove(pos).1);
            }
        }
        if ordered.len() == ids.len() {
            return Ok(ordered);
        }
        if got.len() + ordered.len() == ids.len() {
            ordered.extend(got.into_iter().map(|(_, result)| result));
            return Ok(ordered);
        }
        // Nothing was submitted, or the completion could not be matched.
        // `EOPNOTSUPP` is the signal the caller uses to leave this ring.
        Err(io::Error::from_raw_os_error(libc::EOPNOTSUPP))
    }

    fn drain_completions(&mut self) -> Vec<(u64, i32)> {
        self.ring
            .completion()
            .map(|completed| (completed.user_data(), completed.result()))
            .collect()
    }
}

/// Bytes the produce path fills and the durable write submits.
#[cfg(not(target_os = "linux"))]
#[derive(Debug)]
struct OutBuf {
    bytes: Vec<u8>,
}

#[cfg(not(target_os = "linux"))]
impl OutBuf {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn clear(&mut self) {
        self.bytes.clear();
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn reserve(&mut self, additional: usize) -> io::Result<()> {
        self.bytes.reserve(additional);
        Ok(())
    }

    fn extend_from_slice(&mut self, src: &[u8]) -> io::Result<()> {
        self.bytes.extend_from_slice(src);
        Ok(())
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

/// Aligned allocation. A direct write submits this pointer and does not copy.
#[cfg(target_os = "linux")]
struct OutBuf {
    ptr: *mut u8,
    cap: usize,
    len: usize,
    align: usize,
}

#[cfg(target_os = "linux")]
unsafe impl Send for OutBuf {}

#[cfg(target_os = "linux")]
impl std::fmt::Debug for OutBuf {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OutBuf")
            .field("len", &self.len)
            .field("cap", &self.cap)
            .finish()
    }
}

#[cfg(target_os = "linux")]
impl OutBuf {
    fn new() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            cap: 0,
            len: 0,
            align: DIRECT_ALIGN,
        }
    }

    fn clear(&mut self) {
        self.len = 0;
    }

    fn len(&self) -> usize {
        self.len
    }

    fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    fn as_slice(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    fn reserve(&mut self, additional: usize) -> io::Result<()> {
        let need = self.len.saturating_add(additional);
        if need <= self.cap {
            return Ok(());
        }
        let mut cap = self.cap.max(self.align);
        while cap < need {
            cap = cap.saturating_mul(2).max(need);
        }
        cap = cap.next_multiple_of(self.align);
        self.realloc(cap)
    }

    fn extend_from_slice(&mut self, src: &[u8]) -> io::Result<()> {
        self.reserve(src.len())?;
        if src.is_empty() {
            return Ok(());
        }
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), self.ptr.add(self.len), src.len());
        }
        self.len += src.len();
        Ok(())
    }

    fn realloc(&mut self, cap: usize) -> io::Result<()> {
        let layout = std::alloc::Layout::from_size_align(cap, self.align)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        let ptr = unsafe { std::alloc::alloc(layout) };
        if ptr.is_null() {
            return Err(io::Error::from(io::ErrorKind::OutOfMemory));
        }
        if self.len > 0 {
            unsafe { std::ptr::copy_nonoverlapping(self.ptr, ptr, self.len) };
        }
        if self.cap > 0 {
            if let Ok(old) = std::alloc::Layout::from_size_align(self.cap, self.align) {
                unsafe { std::alloc::dealloc(self.ptr, old) };
            }
        }
        self.ptr = ptr;
        self.cap = cap;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl Drop for OutBuf {
    fn drop(&mut self) {
        if self.cap == 0 {
            return;
        }
        if let Ok(layout) = std::alloc::Layout::from_size_align(self.cap, self.align) {
            unsafe { std::alloc::dealloc(self.ptr, layout) };
        }
    }
}
