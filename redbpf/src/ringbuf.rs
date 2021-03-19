use std::{
    fmt,
    io::{self, Write},
    marker::PhantomData,
    ptr,
    slice,
    mem,
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    task::{Context, Poll},
    pin::Pin,
    os::unix::io::AsRawFd,
};
use futures::Stream;
use tokio::io::{Interest, unix::AsyncFd};
use smallvec::SmallVec;

pub trait RingBufferData
where
    Self: Sized,
{
    type Error: fmt::Debug;

    fn from_rb_slice(slice: &[u8]) -> Result<Self, Self::Error>;
}

/// The `RingBuffer` userspace object.
#[must_use = "streams do nothing unless polled"]
pub struct RingBuffer<D> {
    inner: AsyncFd<RingBufferInner>,
    depth: usize,
    pending_order: usize,
    report: Option<RingBufferReport>,
    phantom_data: PhantomData<D>,
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
    consumer_pos_value: usize,
    last_reported_percent: usize,
    // pointers to shared memory
    observer: Arc<RingBufferObserver>,
}

impl AsRawFd for RingBufferInner {
    fn as_raw_fd(&self) -> i32 {
        self.fd.clone()
    }
}

pub struct RingBufferObserver {
    page_size: usize,
    data: Box<[AtomicUsize]>,
    consumer_pos: Box<AtomicUsize>,
    producer_pos: Box<AtomicUsize>,
}

impl RingBufferObserver {
    pub fn consumer_pos(&self) -> usize {
        self.consumer_pos.load(Ordering::SeqCst)
    }

    pub fn producer_pos(&self) -> usize {
        self.producer_pos.load(Ordering::SeqCst)
    }

    pub fn len(&self) -> usize {
        self.data.len() * mem::size_of::<AtomicUsize>()
    }
}

impl AsRef<[u8]> for RingBufferObserver {
    fn as_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data.as_ptr() as *const u8, self.len()) }
    }
}

impl<D> RingBuffer<D> {
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
        Ok(RingBuffer {
            inner,
            depth: 0,
            pending_order: 0,
            report: None,
            phantom_data: PhantomData,
        })
    }

    pub fn with_report(mut self) -> Self {
        self.report = Some(RingBufferReport::new());
        self
    }

    pub fn observer(&self) -> Arc<RingBufferObserver> {
        self.inner.get_ref().observer.clone()
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
            consumer_pos_value: 0,
            last_reported_percent: 0,
            observer: Arc::new(RingBufferObserver {
                page_size,
                consumer_pos,
                producer_pos,
                data,
            }),
        })
    }

    // try to read a data slice from the ring buffer, advance our position
    fn read<D>(&mut self) -> io::Result<SmallVec<[D; 64]>>
    where
        D: RingBufferData,
    {
        const BUSY_BIT: usize = 1 << 31;
        const DISCARD_BIT: usize = 1 << 30;
        const HEADER_SIZE: usize = 8;

        let mut vec = SmallVec::new();

        // try read something
        loop {
            let pr_pos = self.observer.producer_pos.load(Ordering::Acquire);
            if self.consumer_pos_value > pr_pos {
                // it means we were read a slice of memory which wasn't written yet
                return Err(io::Error::new(io::ErrorKind::Other, "read uninitialized data"));
            } else if self.consumer_pos_value == pr_pos {
                // nothing to read more
                // tell the kernel were we are
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
                let header = self.observer.data[index_in_array].load(Ordering::Acquire);
                // keep only 32 bits
                (header & 0xffffffff, masked_pos + HEADER_SIZE)
            };

            if header & BUSY_BIT != 0 {
                // nothing to read, kernel is writing to this slice right now
                // tell the kernel were we are
                break;
            }

            let (length, discard) = (header & !DISCARD_BIT, (header & DISCARD_BIT) != 0);

            // align the length by 8, and advance our position
            self.consumer_pos_value += HEADER_SIZE + (length + 7) / 8 * 8;

            if !discard {
                // if not discard, yield the slice
                let s = unsafe {
                    slice::from_raw_parts(
                        ((self.observer.data.as_ptr() as usize) + data_offset) as *mut u8,
                        length,
                    )
                };
                match D::from_rb_slice(s) {
                    Ok(data) => vec.push(data),
                    Err(error) => log::error!("rb parse data: {:?}", error),
                }
            }
            // if kernel decide to discard this slice, go to the next iteration
        };

        // store our position to tell kernel it can overwrite memory behind our position
        self.observer.consumer_pos.store(self.consumer_pos_value, Ordering::Release);

        if vec.is_empty() {
            Err(io::Error::new(io::ErrorKind::WouldBlock, ""))
        } else {
            Ok(vec)
        }
    }
}

impl Drop for RingBufferObserver {
    fn drop(&mut self) {
        let len = self.len();
        let p = mem::replace(&mut self.consumer_pos, Box::new(AtomicUsize::new(0)));
        let q = mem::replace(&mut self.producer_pos, Box::new(AtomicUsize::new(0)));
        let data = mem::replace(&mut self.data, Box::new([]));
        unsafe {
            libc::munmap(Box::into_raw(p) as *mut _, self.page_size);
            libc::munmap(Box::into_raw(q) as *mut _, self.page_size + len);
        }
        Box::leak(data);
    }
}

impl<D> Stream for RingBuffer<D>
where
    D: RingBufferData + Unpin,
{
    type Item = SmallVec<[D; 64]>;

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
                    let p_pos = self.inner.get_ref().observer.producer_pos.load(Ordering::SeqCst);
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
