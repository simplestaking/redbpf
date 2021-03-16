use std::{
    io,
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

/// The data read from the `RingBuffer`.
/// TODO: `RingBuffer` unable to read more data until this is not consumed,
/// it is possible to fix, and consume data in parallel.
pub struct RingBufferData {
    data: Box<[u8]>,
    end_position: usize,
}

impl AsRef<[u8]> for RingBufferData {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

impl Drop for RingBufferData {
    fn drop(&mut self) {
        let _ = self.end_position;
        let data = mem::replace(&mut self.data, Box::new([]));
        mem::forget(data);
    }
}

/// The `RingBuffer` userspace object.
pub struct RingBuffer {
    inner: AsyncFd<RingBufferInner>,
    depth: usize,
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
        Ok(RingBuffer { inner, depth: 0 })
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

            log::info!("consumer: 0x{:016x}", p as usize);
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

            log::info!("producer: 0x{:016x}", p as usize);
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

    fn update_pos(&mut self) {
        self.consumer_pos.store(self.consumer_pos_value, Ordering::Release);
    }

    fn read(&mut self) -> io::Result<RingBufferData> {
        const BUSY_BIT: usize = 1 << 31;
        const DISCARD_BIT: usize = 1 << 30;
        const HEADER_SIZE: usize = 8;

        let would_block_error = io::Error::new(io::ErrorKind::WouldBlock, "");

        self.update_pos();

        loop {
            let pr_pos = self.producer_pos.load(Ordering::Acquire);
            if self.consumer_pos_value >= pr_pos {
                self.update_pos();
                break Err(would_block_error);
            } else {
                let distance = pr_pos - self.consumer_pos_value;
                let quant = (self.mask + 1) / 100;
                let percent = distance / quant;
                if percent > self.last_reported_percent {
                    log::warn!("the buffer is filled by: {}, increasing", percent);
                    self.last_reported_percent = percent;
                } else if percent < self.last_reported_percent {
                    log::info!("the buffer is filled by: {}, decreasing", percent);
                    self.last_reported_percent = percent;

                }
            }

            let (length, data_offset) = {
                let masked_pos = self.consumer_pos_value & self.mask;
                let reduced_pos = masked_pos / mem::size_of::<AtomicUsize>();
                let length = self.data[reduced_pos].load(Ordering::Acquire);
                // keep only 32 bits
                (length & 0xffffffff, masked_pos + HEADER_SIZE)
            };

            if length & BUSY_BIT != 0 {
                self.update_pos();
                break Err(would_block_error);
            }

            let (length, discard) = (length & !DISCARD_BIT, (length & DISCARD_BIT) != 0);
            self.consumer_pos_value += HEADER_SIZE + (length + 7) / 8 * 8;

            if !discard {
                let data = unsafe {
                    let slice = slice::from_raw_parts_mut(((self.data.as_ptr() as usize) + data_offset) as *mut u8, length);
                    Box::from_raw(slice as *mut [u8])
                };
                break Ok(RingBufferData {
                    data,
                    end_position: self.consumer_pos_value,
                });
            }
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
    type Item = RingBufferData;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.depth > 1 {
            log::warn!("depth: {}", self.depth);
        }
        match self.inner.poll_read_ready_mut(cx) {
            Poll::Pending => {
                self.depth = 0;
                Poll::Pending
            },
            Poll::Ready(Err(error)) => {
                log::error!("{}", error);
                Poll::Ready(None)
            },
            Poll::Ready(Ok(mut guard)) => {
                match guard.try_io(|inner| inner.get_mut().read()) {
                    Ok(Ok(data)) => {
                        self.depth = 0;
                        Poll::Ready(Some(data))
                    },
                    Ok(Err(error)) => {
                        log::error!("{}", error);
                        Poll::Ready(None)
                    },
                    Err(_) => {
                        self.depth += 1;
                        self.poll_next(cx)
                    },
                }
            },
        }
    }
}
