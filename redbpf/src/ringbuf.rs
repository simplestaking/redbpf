use std::{os::unix::io::RawFd, io, os::raw, ptr, slice, mem, task::{Context, Poll}, pin::Pin};
use mio::{unix::EventedFd, Evented, Ready, Token, PollOpt};
use futures::Stream;
use tokio::io::PollEvented;

pub struct RingBufferManager {
    inner: Option<*mut bpf_sys::ring_buffer>,
    rings: Vec<*mut raw::c_void>,
}

unsafe impl Send for RingBufferManager {
}

pub trait RingBufferItem {
    fn consume(slice: &[u8]) -> Self;
}

struct RingBufferFd(RawFd);

impl Evented for RingBufferFd {
    fn register(
        &self,
        poll: &mio::Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        EventedFd(&self.0).register(poll, token, interest, opts)
    }

    fn reregister(
        &self,
        poll: &mio::Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        EventedFd(&self.0).reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &mio::Poll) -> io::Result<()> {
        EventedFd(&self.0).deregister(poll)
    }
}

#[pin_project::pin_project]
pub struct RingBuffer<I>
where
    I: RingBufferItem,
{
    poll: PollEvented<RingBufferFd>,
    manager: *mut bpf_sys::ring_buffer,
    items: Vec<I>,
}

unsafe impl<I> Send for RingBuffer<I>
where
    I: RingBufferItem,
{
}

impl<I> RingBuffer<I>
where
    I: RingBufferItem,
{
    fn new(fd: RawFd) -> Result<Self, io::Error> {
        Ok(RingBuffer {
            poll: PollEvented::new(RingBufferFd(fd))?,
            manager: ptr::null_mut(),
            items: vec![],
        })
    }

    fn callback(&mut self, slice: &[u8]) {
        self.items.push(I::consume(slice))
    }

    fn take(&mut self) -> Vec<I> {
        unsafe {
            bpf_sys::ring_buffer__consume(self.manager);
        }
        mem::replace(&mut self.items, vec![])
    }
}

unsafe extern "C" fn callback<I>(
    ctx: *mut raw::c_void,
    data: *mut raw::c_void,
    size: raw::c_ulong,
) -> raw::c_int
where
    I: RingBufferItem,
{
    let rb = &mut *(ctx as *mut RingBuffer<I>);
    let data = slice::from_raw_parts(data as *mut u8 as *const u8, size as usize);
    rb.callback(data);
    0
}

impl RingBufferManager {
    pub fn new() -> Self {
        RingBufferManager {
            inner: None,
            rings: vec![],
        }
    }

    pub fn add<'a, I>(&'a mut self, fd: RawFd) -> Result<&'a mut RingBuffer<I>, io::Error>
    where
        I: 'static + RingBufferItem,
    {
        let &mut RingBufferManager {
            ref mut inner,
            ref mut rings,
        } = self;

        let rb = Box::new(RingBuffer::new(fd)?);
        let rb = Box::into_raw(rb);
        rings.push(rb as *mut _);

        if let &mut Some(ref mut inner) = inner {
            let r = unsafe { bpf_sys::ring_buffer__add(*inner, fd, Some(callback::<I>), rb as *mut _) };
            if r < 0 {
                Err(io::Error::from_raw_os_error(r))
            } else {
                let rb = unsafe { &mut *rb };
                rb.manager = *inner;
                Ok(rb)
            }
        } else {
            let inner = unsafe {
                bpf_sys::ring_buffer__new(fd, Some(callback::<I>), rb as *mut _, ptr::null())
                // warning: mutable borrowing of rb ends here,
                // but actually it will be mutated during callback
            };
    
            if inner.is_null() {
                Err(io::Error::last_os_error())
            } else {
                let rb = unsafe { &mut *rb };
                rb.manager = inner;
                Ok(rb)
            }    
        }
    }
}

impl Drop for RingBufferManager {
    fn drop(&mut self) {
        if let &mut Some(ref mut inner) = &mut self.inner {
            unsafe {
                bpf_sys::ring_buffer__free(*inner)
            }
        }
        for rb in self.rings.drain(..) {
            let rb = unsafe { Box::from_raw(rb) };
            let _ = rb;
        }
    }
}

impl<I> Stream for RingBuffer<I>
where
    I: RingBufferItem,
{
    type Item = Vec<I>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let ready = Ready::readable();
        if let Poll::Pending = self.poll.poll_read_ready(cx, ready) {
            return Poll::Pending;
        }

        let messages = self.take();
        self.poll.clear_read_ready(cx, ready).unwrap();
        Poll::Ready(Some(messages))
    }
}
