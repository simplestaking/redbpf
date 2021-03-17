use std::{
    io::{self, Write},
    ptr,
    slice,
    mem,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
    pin::Pin,
    os::unix::io::AsRawFd,
};
use futures::Stream;
use tokio::io::{Interest, unix::AsyncFd};
use smallvec::SmallVec;

/// The data read from the `RingBuffer`.
/// TODO: `RingBuffer` unable to read more data until this is not consumed,
/// it is possible to fix, and consume data in parallel.
pub struct RingBufferData {
    data: Box<[u8]>,
}

impl AsRef<[u8]> for RingBufferData {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

impl Drop for RingBufferData {
    fn drop(&mut self) {
        let data = mem::replace(&mut self.data, Box::new([]));
        mem::forget(data);
    }
}

/// The `RingBuffer` userspace object.
pub struct RingBuffer {
    inner: AsyncFd<RingBufferInner>,
    depth: usize,
    pending_order: usize,
    report: Option<RingBufferReport>,
}

struct RingBufferReport {
    inner: Vec<u8>,
}

impl RingBufferReport {
    fn new() -> Self {
        RingBufferReport {
            inner: Vec::with_capacity(16 * 1024 * 1024),
        }
    }

    fn on_poll(&mut self) {
        self.inner.push(0x00);
    }

    fn on_pending(&mut self) {
        self.inner.push(0x01);
    }

    fn on_ready(&mut self) {
        self.inner.push(0x02);
    }

    fn on_pos(&mut self, p_pos: usize, c_pos: usize) {
        self.inner.push(0x03);
        self.inner.extend_from_slice(&(c_pos as u64).to_be_bytes());
        self.inner.extend_from_slice(&(p_pos as u64).to_be_bytes());
    }
}

impl Drop for RingBufferReport {
    fn drop(&mut self) {
        if !self.inner.is_empty() {
            let mut f = std::fs::File::create("target/rb_report").unwrap();
            f.write_all(&self.inner).unwrap();
        }
    }
}

struct RingBufferInner {
    fd: i32,
    mask: usize,
    page_size: usize,
    // pointers to shared memory
    consumer_pos: Box<AtomicUsize>,
    producer_pos: Box<AtomicUsize>,
    consumer_pos_value: usize,
    last_reported_percent: usize,
    data: Box<[AtomicUsize]>,
}

impl AsRawFd for RingBufferInner {
    fn as_raw_fd(&self) -> i32 {
        self.fd.clone()
    }
}

pub struct RingBufferDump {
    data: *const u8,
    length: usize,
    pub pos: usize,
}

impl AsRef<[u8]> for RingBufferDump {
    fn as_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data, self.length) }
    }
}

impl RingBuffer {
    /// Create the buffer from redbpf `Map` object.
    pub fn from_map(map: &crate::Map) -> Result<Self, io::Error> {
        let fd = map.fd;
        let max_length = map.config.max_entries as usize;
        debug_assert_eq!(map.kind, 27);
        Self::new(fd, max_length)
    }

    /// Create the buffer, `fd` is the file descriptor, `max_length` should be power of two.
    pub fn new(fd: i32, max_length: usize) -> io::Result<Self> {
        let inner = RingBufferInner::new(fd, max_length)?;
        let inner = AsyncFd::with_interest(inner, Interest::READABLE)?;
        Ok(RingBuffer { inner, depth: 0, pending_order: 0, report: None })
    }

    pub fn with_report(mut self) -> Self {
        self.report = Some(RingBufferReport::new());
        self
    }

    pub fn dump(&self) -> RingBufferDump {
        self.inner.get_ref().dump()
    }
}

impl RingBufferInner {
    fn new(fd: i32, max_length: usize) -> io::Result<Self> {
        debug_assert_eq!(max_length & (max_length - 1), 0);

        // it is a constant, most likely 0x1000
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;

        // consumers page, currently contains only one integer value,
        // offset where consumer should read;
        // map it read/write
        let consumer_pos = unsafe {
            let p = libc::mmap(
                ptr::null_mut(),
                page_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            );
            if p == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }

            Box::from_raw(p as *mut AtomicUsize)
        };

        // producers page and the buffer itself,
        // currently producers page contains only one integer value,
        // offset where producer has wrote, or still writing;
        // let's refer buffer data as a slice of `AtomicUsize` array 
        // because we care only about data headers which sized and aligned by 8;
        // map it read only
        let (producer_pos, data) = unsafe {
            let p = libc::mmap(
                ptr::null_mut(),
                page_size + max_length * 2,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                page_size as i64,
            );
            if p == libc::MAP_FAILED {
                libc::munmap(Box::into_raw(consumer_pos) as *mut _, page_size);
                return Err(io::Error::last_os_error());
            }

            let length = max_length * 2 / mem::size_of::<AtomicUsize>();
            let q = (p as usize) + page_size;
            let q = slice::from_raw_parts_mut(q as *mut AtomicUsize, length);
            (
                Box::from_raw(p as *mut AtomicUsize),
                Box::from_raw(q as *mut [AtomicUsize]),
            )
        };

        log::info!("new RingBuffer: fd: {}, page_size: 0x{:016x}, mask: 0x{:016x}", fd, page_size, max_length - 1);
        Ok(RingBufferInner {
            fd,
            mask: max_length - 1,
            page_size,
            consumer_pos,
            producer_pos,
            consumer_pos_value: 0,
            last_reported_percent: 0,
            data,
        })
    }

    fn dump(&self) -> RingBufferDump {
        RingBufferDump {
            data: self.data.as_ptr() as *const u8,
            length: self.producer_pos.load(Ordering::SeqCst),
            pos: self.consumer_pos.load(Ordering::SeqCst),
        }
    }

    // store our position to notify kernel it can overwrite memory behind our position
    fn update_pos(&mut self) {
        self.consumer_pos.store(self.consumer_pos_value, Ordering::Release);
    }

    // try to read a data slice from the ring buffer, advance our position
    fn read(&mut self) -> io::Result<SmallVec<[RingBufferData; 64]>> {
        const BUSY_BIT: usize = 1 << 31;
        const DISCARD_BIT: usize = 1 << 30;
        const HEADER_SIZE: usize = 8;

        let would_block_error = io::Error::new(io::ErrorKind::WouldBlock, "");

        // if tokio runtime call this function, it means previous memory slice is consumed
        // we can advance the position
        self.update_pos();

        let mut vec = SmallVec::new();

        // try read something
        loop {
            let pr_pos = self.producer_pos.load(Ordering::Acquire);
            if self.consumer_pos_value > pr_pos {
                // it means we were read a slice of memory which wasn't written yet
                return Err(io::Error::new(io::ErrorKind::Other, "read uninitialized data"));
            } else if self.consumer_pos_value == pr_pos {
                // nothing to read more
                // tell the kernel were we are
                self.update_pos();
                break;
            } else {
                // determine how far we are, how many unseen data is in the buffer
                let distance = pr_pos - self.consumer_pos_value;
                let quant = (self.mask + 1) / 100;
                let percent = distance / quant;
                if percent >= 100 {
                    log::error!("the buffer is overflow");
                }
                if percent > self.last_reported_percent {
                    log::warn!("the buffer is filled by: {}%, increasing", percent);
                    self.last_reported_percent = percent;
                } else if percent < self.last_reported_percent {
                    log::info!("the buffer is filled by: {}%, decreasing", percent);
                    self.last_reported_percent = percent;
                }
            }

            // the first 8 bytes of the memory slice is a header (length and flags)
            let (header, data_offset) = {
                let masked_pos = self.consumer_pos_value & self.mask;
                let index_in_array = masked_pos / mem::size_of::<AtomicUsize>();
                let header = self.data[index_in_array].load(Ordering::Acquire);
                // keep only 32 bits
                (header & 0xffffffff, masked_pos + HEADER_SIZE)
            };

            if header & BUSY_BIT != 0 {
                // nothing to read, kernel is writing to this slice right now
                // tell the kernel were we are
                self.update_pos();
                break;
            }

            let (length, discard) = (header & !DISCARD_BIT, (header & DISCARD_BIT) != 0);

            // align the length by 8, and advance our position
            self.consumer_pos_value += HEADER_SIZE + (length + 7) / 8 * 8;

            if !discard {
                // if not discard, yield the slice
                let data = unsafe {
                    let slice = slice::from_raw_parts_mut(((self.data.as_ptr() as usize) + data_offset) as *mut u8, length);
                    Box::from_raw(slice as *mut [u8])
                };
                vec.push(RingBufferData { data });
            }
            // if kernel decide to discard this slice, go to the next iteration
        };

        if vec.is_empty() {
            Err(would_block_error)
        } else {
            Ok(vec)
        }
    }
}

impl Drop for RingBufferInner {
    fn drop(&mut self) {
        let p = mem::replace(&mut self.consumer_pos, Box::new(AtomicUsize::new(0)));
        let q = mem::replace(&mut self.producer_pos, Box::new(AtomicUsize::new(0)));
        let data = mem::replace(&mut self.data, Box::new([]));
        unsafe {
            libc::munmap(Box::into_raw(p) as *mut _, self.page_size);
            libc::munmap(Box::into_raw(q) as *mut _, self.page_size + (self.mask + 1) * 2);
        }
        Box::leak(data);
    }
}

impl Stream for RingBuffer {
    type Item = SmallVec<[RingBufferData; 64]>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.report.as_mut().map(RingBufferReport::on_poll);
        if self.depth > 1 {
            log::warn!("depth: {}", self.depth);
        }
        // poll io
        match self.inner.poll_read_ready_mut(cx) {
            Poll::Pending => {
                self.depth = 0;
                self.pending_order += 1;
                if self.pending_order >= 3 {
                    log::warn!("too many pending {}", self.pending_order);
                    let p_pos = self.inner.get_ref().producer_pos.load(Ordering::SeqCst);
                    let c_pos = self.inner.get_ref().consumer_pos_value;
                    self.report.as_mut().map(|r| r.on_pos(p_pos, c_pos));
                }
                self.report.as_mut().map(RingBufferReport::on_pending);
                Poll::Pending
            },
            Poll::Ready(Err(error)) => {
                log::error!("{}", error);
                Poll::Ready(None)
            },
            Poll::Ready(Ok(mut guard)) => {
                // have something, try to read
                // it might be false ready, or busy slice
                // in both cases `read` return `WouldBlock`
                match guard.try_io(|inner| inner.get_mut().read()) {
                    Ok(Ok(data)) => {
                        self.depth = 0;
                        self.pending_order = 0;
                        self.report.as_mut().map(RingBufferReport::on_ready);
                        Poll::Ready(Some(data))
                    },
                    Ok(Err(error)) => {
                        log::error!("{}", error);
                        Poll::Ready(None)
                    },
                    // `read` return `WouldBlock`
                    // poll again as documentation of `try_io` suggesting
                    Err(_) => {
                        self.depth += 1;
                        self.poll_next(cx)
                    },
                }
            },
        }
    }
}
