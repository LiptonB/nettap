use bytes::BytesMut;
use failure::Error;
use futures::{try_ready, Poll};
use tokio::prelude::*;
use tokio::sync::mpsc;

use crate::connection::Connection;

enum Message {
    Data(BytesMut),
    Connection(Box<dyn AsyncWrite>),
}

use Message::*;

pub struct Coordinator {
    // TODO: does this really have to be a trait object? The type is complicated but it's only ever
    // going to be one type
    incoming: Box<Stream<Item = Message, Error = Error>>,
    outgoing: Vec<Box<dyn AsyncWrite>>,
    buffer: Option<BytesMut>,
    is_buffered: Vec<bool>,
}

impl Coordinator {
    pub fn new() -> (Coordinator, Connection, mpsc::Sender<Box<dyn AsyncWrite>>) {
        let (data_tx, data_rx) = mpsc::channel(1024);
        let (conn_tx, conn_rx) = mpsc::channel(1024);
        let stream = data_rx
            .map(|data| Data(data))
            .map_err(|err| err.into())
            .select(
                conn_rx
                    .map(|conn| Connection(conn))
                    .map_err(|err| err.into()),
            );

        (
            Coordinator {
                incoming: Box::new(stream),
                outgoing: Vec::new(),
                buffer: None,
                is_buffered: Vec::new(),
            },
            Connection { incoming: data_tx },
            conn_tx,
        )
    }

    fn write_to_all(&mut self, data: BytesMut) -> Poll<(), Error> {
        debug_assert!(self.buffer.is_none());
        self.buffer = Some(data);
        for item in self.is_buffered.iter_mut() {
            *item = true;
        }
        self.flush_buffer()
    }

    fn flush_buffer(&mut self) -> Poll<(), Error> {
        if let Some(data) = self.buffer {
            let mut any_still_buffered = false;
            for (idx, conn) in self
                .outgoing
                .iter()
                .enumerate()
                .filter(|(idx, _)| self.is_buffered[*idx])
            {
                if let Async::Ready(_) = conn.poll_write(&data)? {
                    self.is_buffered[idx] = false;
                } else {
                    any_still_buffered = true;
                }
            }

            if any_still_buffered {
                Ok(Async::NotReady)
            } else {
                Ok(Async::Ready(()))
            }
        } else {
            Ok(Async::Ready(()))
        }
    }

    fn shutdown_all(&self) -> Poll<(), Error> {}

    fn poll_flush_all(&self) -> Poll<(), Error> {}
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
                Async::Ready(None) => {
                    try_ready!(self.shutdown_all());
                    return Ok(Async::Ready(()));
                }
                Async::NotReady => {
                    try_ready!(self.poll_flush_all());
                    return Ok(Async::NotReady);
                }
            }
        }
    }
}
