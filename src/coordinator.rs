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
    incoming: Box<Stream<Item = Message, Error = Error>>,
    outgoing: Vec<Box<dyn AsyncWrite>>,
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
            },
            Connection { incoming: data_tx },
            conn_tx,
        )
    }

    fn write_to_all(&self, data: BytesMut) -> Poll<(), Error> {}

    fn shutdown_all(&self) -> Poll<(), Error> {}

    fn poll_flush_all(&self) -> Poll<(), Error> {}
}

impl Future for Coordinator {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.incoming.poll()? {
                Async::Ready(Some(Data(data))) => try_ready!(self.write_to_all(data)),
                Async::Ready(Some(Connection(conn))) => {
                    self.outgoing.push(conn);
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
