// Copyright 2019 Authors of Red Sift
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use futures::prelude::*;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::slice;
use std::task::{Context, Poll};
use tokio::io::{Interest, unix::AsyncFd};

use crate::{Event, PerfMap};

pub struct PerfMessageStream {
    poll: AsyncFd<RawFd>,
    map: PerfMap,
    name: String,
}

impl PerfMessageStream {
    pub fn new(name: String, map: PerfMap) -> Self {
        let poll = AsyncFd::with_interest(map.fd, Interest::READABLE).unwrap();
        PerfMessageStream { poll, map, name }
    }

    fn read_messages(&mut self) -> Vec<Box<[u8]>> {
        let mut ret = Vec::new();
        while let Some(ev) = self.map.read() {
            match ev {
                Event::Lost(lost) => {
                    eprintln!("Possibly lost {} samples for {}", lost.count, &self.name);
                }
                Event::Sample(sample) => {
                    let msg = unsafe {
                        slice::from_raw_parts(sample.data.as_ptr(), sample.size as usize)
                            .to_vec()
                            .into_boxed_slice()
                    };
                    ret.push(msg);
                }
            };
        }

        ret
    }
}

impl Stream for PerfMessageStream {
    type Item = Vec<Box<[u8]>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.poll.poll_read_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(mut guard)) => {
                guard.clear_ready();
                Poll::Ready(Some(self.read_messages()))
            },
            Poll::Ready(Err(error)) => {
                log::error!("{}", error);
                Poll::Ready(None)
            },
        }

    }
}
