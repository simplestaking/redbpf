use std::{
    io,
    ptr,
    slice,
    mem,
    sync::atomic::{AtomicUsize, Ordering},
};

/// The `RingBuffer` userspace object.
pub struct RingBufferSync {
    fd: i32,
    mask: usize,
    page_size: usize,
    // pointers to shared memory
    consumer_pos: Box<AtomicUsize>,
    producer_pos: Box<AtomicUsize>,
    data: Box<[AtomicUsize]>,
}

pub struct RingBufferSyncIterator {
    rb: RingBufferSync,
    pos: usize,
}

impl RingBufferSync {
    /// Create the buffer from redbpf `Map` object.
    pub fn from_map(map: &crate::Map) -> Result<Self, io::Error> {
        let fd = map.fd;
        let max_length = map.config.max_entries as usize;
        debug_assert_eq!(map.kind, 27);
        Self::new(fd, max_length)
    }

    /// Create the buffer, `fd` is the file descriptor, `max_length` should be power of two.
    pub fn new(fd: i32, max_length: usize) -> Result<Self, io::Error> {
        let _ = max_length;
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
        Ok(RingBufferSync {
            fd,
            mask: max_length - 1,
            page_size,
            consumer_pos,
            producer_pos,
            data,
        })
    }

    fn wait(&self) {
        let mut fds = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        loop {
            match unsafe { libc::poll(&mut fds, 1, 1000) } {
                0 => log::info!("ringbuf timeout"),
                1 => {
                    if fds.revents & libc::POLLIN != 0 {
                        break;
                    }
                },
                i32::MIN..=-1 => log::error!("ringbuf error: {:?}", io::Error::last_os_error()),
                // poll should not return bigger then number of fds, we have 1
                r @ 2..=i32::MAX => log::error!("ringbuf poll {}", r),
            }
            fds.revents = 0;
        }
    }
}

impl Drop for RingBufferSync {
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

impl IntoIterator for RingBufferSync {
    type Item = Vec<u8>;
    type IntoIter = RingBufferSyncIterator;

    fn into_iter(self) -> Self::IntoIter {
        let consumer_pos = self.consumer_pos.load(Ordering::Acquire) & self.mask;

        RingBufferSyncIterator {
            rb: self,
            pos: consumer_pos,
        }
    }
}

impl RingBufferSyncIterator {
    fn next_or_should_wait(&mut self) -> Option<Vec<u8>> {
        const BUSY_BIT: usize = 1 << 31;
        const DISCARD_BIT: usize = 1 << 30;
        const HEADER_SIZE: usize = 8;

        let consumer_pos = self.pos;

        if consumer_pos >= self.rb.producer_pos.load(Ordering::Acquire) {
            return None;
        }

        let reduced_pos = consumer_pos / mem::size_of::<AtomicUsize>();
        let length = self.rb.data[reduced_pos].load(Ordering::Acquire);
        // keep only 32 bits
        let length = length & ((1 << 32) - 1);

        if length & BUSY_BIT != 0 {
            return None;
        }

        let data_offset = consumer_pos + HEADER_SIZE;
        let (length, discard) = (length & !DISCARD_BIT, (length & DISCARD_BIT) != 0);

        self.pos = data_offset + (length + 7) / 8 * 8;

        if !discard {
            Some(unsafe {
                let slice = slice::from_raw_parts_mut(((self.rb.data.as_ptr() as usize) + data_offset) as *mut u8, length);
                slice.to_vec()
            })
        } else {
            self.next_or_should_wait()
        }
    }
}

impl Iterator for RingBufferSyncIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.next_or_should_wait() {
                self.rb.consumer_pos.store(self.pos, Ordering::Release);
                break Some(item);
            } else {
                self.rb.wait();
            }
        }
    }
}
