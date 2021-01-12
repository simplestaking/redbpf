use std::{
    io,
    ptr,
    slice,
    mem,
    sync::{atomic::{AtomicUsize, AtomicBool, Ordering}, Arc},
    task::{Context, Poll},
    pin::Pin,
};
use mio::Ready;
use futures::Stream;
use tokio::io::PollEvented;
use crate::load::map_io::MapIo;

/// The data read from the `RingBuffer`.
/// TODO: `RingBuffer` unable to read more data until this is not consumed,
/// it is possible to fix, and consume data in parallel.
pub struct RingBufferData {
    data: Box<[u8]>,
    end_position: usize,
    busy: Arc<AtomicBool>,
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
        self.busy.store(false, Ordering::SeqCst);
    }
}

/// The `RingBuffer` userspace object.
pub struct RingBuffer {
    poll: PollEvented<MapIo>,
    lock: Arc<AtomicBool>,
    mask: usize,
    page_size: usize,
    // pointers to shared memory
    consumer_pos: Box<AtomicUsize>,
    producer_pos: Box<AtomicUsize>,
    data: Box<[AtomicUsize]>,
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
    pub fn new(fd: i32, max_length: usize) -> Result<Self, io::Error> {
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
        Ok(RingBuffer {
            poll: PollEvented::new(MapIo(fd))?,
            lock: Arc::new(AtomicBool::new(false)),
            mask: max_length - 1,
            page_size,
            consumer_pos,
            producer_pos,
            data,
        })
    }

    pub fn dump(&self) -> RingBufferDump {
        let producer_pos = self.producer_pos.load(Ordering::Acquire);
        RingBufferDump {
            data: self.data.as_ptr() as *const u8,
            length: producer_pos,
            pos: self.consumer_pos.load(Ordering::Acquire),
        }
    }
}

impl Drop for RingBuffer {
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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        const BUSY_BIT: usize = 1 << 31;
        const DISCARD_BIT: usize = 1 << 30;
        const HEADER_SIZE: usize = 8;

        if let Poll::Pending = self.poll.poll_read_ready(cx, Ready::readable()) {
            return Poll::Pending;
        }

        if self.lock.load(Ordering::SeqCst) {
            self.poll.clear_read_ready(cx, Ready::readable()).unwrap();
            return Poll::Pending;
        }

        let mut consumer_pos = self.consumer_pos.load(Ordering::Acquire);
        loop {
            if consumer_pos >= self.producer_pos.load(Ordering::Acquire) {
                self.poll.clear_read_ready(cx, Ready::readable()).unwrap();
                return Poll::Pending;
            }

            let (length, data_offset) = {
                let masked_pos = consumer_pos & self.mask;
                let reduced_pos = masked_pos / mem::size_of::<AtomicUsize>();
                let length = self.data[reduced_pos].load(Ordering::Acquire);
                // keep only 32 bits
                (length & 0xffffffff, masked_pos + HEADER_SIZE)
            };

            if length & BUSY_BIT != 0 {
                self.poll.clear_read_ready(cx, Ready::readable()).unwrap();
                return Poll::Pending;
            }

            let (length, discard) = (length & !DISCARD_BIT, (length & DISCARD_BIT) != 0);
            consumer_pos += HEADER_SIZE + (length + 7) / 8 * 8;

            // TODO: update it when user drop the `RingBufferData`
            self.consumer_pos.store(consumer_pos, Ordering::Release);

            if !discard {
                self.lock.store(true, Ordering::SeqCst);
                let data = unsafe {
                    let slice = slice::from_raw_parts_mut(((self.data.as_ptr() as usize) + data_offset) as *mut u8, length);
                    Box::from_raw(slice as *mut [u8])
                };
                return Poll::Ready(Some(RingBufferData {
                    data,
                    end_position: consumer_pos,
                    busy: self.lock.clone(),
                }));
            }
        }
    }
}
