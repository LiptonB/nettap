use bytes::Bytes;
use futures::{future::FutureExt, sink::Sink, stream::Stream};
use tokio::sync::mpsc;

use super::common::{stream_to_sender, stream_to_sink};
use super::{DataStream, Message, NewConnection};

/// Creates a Connection from a Stream of Message and a Sink of Bytes
pub fn new<R, W>(read_stream: R, write_sink: W) -> NewConnection
where
    R: Stream<Item = Message> + Unpin + Send + 'static,
    W: Sink<Bytes> + Unpin + Send + 'static,
    <W as Sink<Bytes>>::Error: std::fmt::Display,
{
    Box::new(move |sender: mpsc::Sender<Message>, receiver: DataStream| {
        Box::pin(
            futures::future::join(
                stream_to_sender(read_stream, sender),
                stream_to_sink(receiver, write_sink),
            )
            .map(|_| ()),
        )
    })
}
