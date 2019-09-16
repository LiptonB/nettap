use bytes::Bytes;
use failure::Error;
use futures::{try_ready, Poll};
use tokio::prelude::*;

type ByteSink = Box<dyn Sink<SinkItem = Bytes, SinkError = Error> + Send>;
type ByteStream = Box<dyn Stream<Item = Message, Error = Error> + Send>;

enum Message {
    Data(Bytes),
    Connection(ByteSink),
}

use Message::*;

pub struct Coordinator {
    incoming: ByteStream,
    outgoing: Vec<ByteSink>,
    buffer: Option<Bytes>,
    is_buffered: Vec<bool>,
}

impl Coordinator {
    pub fn new<D, C>(data_rx: D, conn_rx: C) -> Coordinator
    where
        D: Stream<Item = Bytes, Error = Error> + Send + 'static,
        C: Stream<Item = ByteSink, Error = Error> + Send + 'static,
    {
        let data_stream = data_rx.map(|data| Data(data)).map_err(|err| err.into());
        let conn_stream = conn_rx
            .map(|conn| Connection(conn))
            .map_err(|err| err.into());
        let stream = data_stream.select(conn_stream);

        Coordinator {
            incoming: Box::new(stream),
            outgoing: Vec::new(),
            buffer: None,
            is_buffered: Vec::new(),
        }
    }

    // TODO: should actually write to all but the one it came from
    fn write_to_all(&mut self, data: Bytes) -> Poll<(), Error> {
        debug_assert!(self.buffer.is_none());
        self.buffer = Some(data);
        for item in self.is_buffered.iter_mut() {
            *item = true;
        }
        self.flush_buffer()
    }

    fn flush_buffer(&mut self) -> Poll<(), Error> {
        Ok(if let Some(data) = self.buffer.take() {
            let mut any_still_buffered = false;
            for (conn, is_buffered) in self
                .outgoing
                .iter_mut()
                .zip(self.is_buffered.iter_mut())
                .filter(|(_, is_buffered)| **is_buffered)
            {
                // TODO: the error handling here is wrong. What if one of the connections closes or
                // just becomes unavailable for a while?
                match conn.start_send(data.clone())? {
                    AsyncSink::Ready => *is_buffered = false,
                    AsyncSink::NotReady(_) => any_still_buffered = true,
                }
            }

            if any_still_buffered {
                Async::NotReady
            } else {
                Async::Ready(())
            }
        } else {
            Async::Ready(())
        })
    }

    fn poll_complete_all(&mut self) -> Poll<(), Error> {
        let mut any_not_ready = false;
        for conn in &mut self.outgoing {
            if let Async::NotReady = conn.poll_complete()? {
                any_not_ready = true;
            }
        }

        Ok(if any_not_ready {
            Async::NotReady
        } else {
            Async::Ready(())
        })
    }

    /* TODO
    fn close_all(&mut self) -> Poll<(), Error> {

    }
    */
}

impl Future for Coordinator {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        try_ready!(self.flush_buffer());

        loop {
            match self.incoming.poll()? {
                Async::Ready(Some(Data(data))) => try_ready!(self.write_to_all(data)),
                Async::Ready(Some(Connection(conn))) => {
                    self.outgoing.push(conn);
                    self.is_buffered.push(self.buffer.is_some());
                }
                Async::NotReady => {
                    try_ready!(self.poll_complete_all());
                    return Ok(Async::NotReady);
                }
                Async::Ready(None) => {
                    // TODO: If any of these returns NotReady this is probably going to get screwed
                    // up when we get polled next.
                    // TODO: should probably be close_all but it's not implemented yet
                    try_ready!(self.poll_complete_all());
                    return Ok(Async::Ready(()));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::coordinator::*;

    fn run_future<E: Send>(f: impl Future<Item = (), Error = E> + Send + 'static) {
        let f = f.map_err(|_| panic!("returned error"));
        tokio::run(f);
    }

    #[test]
    fn creation_and_wait_succeeds() {
        let data_rx = stream::empty();
        let conn_rx = stream::empty();

        let c = Coordinator::new(data_rx, conn_rx);
        run_future(c);
    }

    #[test]
    fn send_with_no_conns_succeeds() {
        let data_rx = stream::once(Ok(Bytes::from("somestr")));
        let conn_rx = stream::empty();

        let c = Coordinator::new(data_rx, conn_rx);
        run_future(c);
    }

    #[test]
    fn send_with_conn_transfers_data() {
        let data_rx = stream::once(Ok(Bytes::from("somestr")));
        // TODO
        let conn_rx = stream::empty();

        let c = Coordinator::new(data_rx, conn_rx);
        run_future(c);
    }
}
