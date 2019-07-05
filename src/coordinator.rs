use bytes::BytesMut;
use futures::{try_ready, Poll};
use tokio::prelude::*;
use tokio::sync::mpsc;

use crate::connection::Connection;

enum Message {
    Data(BytesMut),
    Connection(Box<dyn AsyncWrite>),
}

use Message::*;

pub struct Coordinator<S: Stream> {
    incoming: S,
    outgoing: Vec<Box<dyn AsyncWrite>>,
}

pub fn coordinator<S>() -> (
    Coordinator<S>,
    Connection,
    mpsc::Sender<Box<dyn AsyncWrite>>,
)
where
    S: Stream,
{
    let (data_tx, data_rx) = mpsc::channel(1024);
    let (conn_tx, conn_rx) = mpsc::channel(1024);
    let stream = data_rx
        .map(|data| Data(data))
        .select(conn_rx.map(|conn| Connection(conn)));

    (
        Coordinator {
            incoming: stream,
            outgoing: Vec::new(),
        },
        Connection { incoming: data_tx },
        conn_tx,
    )
}

impl<S: Stream> Future for Coordinator<S> {
    type Item = ();
    type Error = ();

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
