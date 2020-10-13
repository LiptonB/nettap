use bytes::Bytes;
use futures::stream::Stream;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::mpsc;

pub type DataStream = Box<dyn Stream<Item = Bytes> + Unpin + Send>;
type Task = Pin<Box<dyn Future<Output = ()> + Send>>;
pub type NewConnection = Box<dyn FnOnce(mpsc::Sender<Message>, DataStream) -> Task + Send>;

pub enum Message {
    Data(Bytes),
    NewConnection(NewConnection),
}

pub mod stream_connection {
    use bytes::Bytes;
    use futures::{future::FutureExt, sink::Sink, stream::Stream};
    use tokio::sync::mpsc;

    use super::common::*;
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
}

pub mod tokio_connection {
    use bytes::{Bytes, BytesMut};
    use futures::stream::StreamExt;
    use std::marker::Unpin;
    use tokio::{io, join, prelude::*, stream::Stream, sync::mpsc};
    use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};
    use tracing::{debug, error};

    use super::common::*;
    use super::{DataStream, Message, NewConnection};

    // TODO: is it possible to replace this closure with a method call on a struct like the
    // coordinator.run method? (May need to use async-trait)
    pub fn new_tokio_connection<R, W>(read_socket: R, write_socket: W) -> NewConnection
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        debug!("new_tokio_connection");
        Box::new(move |sender: mpsc::Sender<Message>, receiver: DataStream| {
            Box::pin(tokio_connection(
                sender,
                receiver,
                read_socket,
                write_socket,
            ))
        })
    }

    pub fn new_spawner_connection<S, SS>(spawner: S) -> NewConnection
    where
        S: Stream<Item = Result<SS, std::io::Error>> + Unpin + Send + 'static,
        SS: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Box::new(move |sender: mpsc::Sender<Message>, receiver: DataStream| {
            Box::pin(spawner_connection(sender, receiver, spawner))
        })
    }

    async fn spawner_connection<S, SS>(
        mut sender: mpsc::Sender<Message>,
        _receiver: DataStream,
        mut spawner: S,
    ) where
        S: Stream<Item = Result<SS, std::io::Error>> + Unpin + Send + 'static,
        SS: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        while let Some(stream) = spawner.next().await {
            match stream {
                Ok(stream) => {
                    let (read, write) = io::split(stream);
                    let connection = new_tokio_connection(read, write);
                    if let Err(err) = sender.send(Message::NewConnection(connection)).await {
                        error!("Queue error: {}", err);
                        break;
                    }
                }
                Err(err) => {
                    error!("Spawner error: {}", err);
                }
            }
        }
    }

    pub async fn tokio_connection<S, R, W>(
        sender: mpsc::Sender<Message>,
        receiver: S,
        read_socket: R,
        write_socket: W,
    ) where
        S: Stream<Item = Bytes> + Unpin,
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        debug!("setting up tokio_connection");
        let socket_in = Box::pin(FramedRead::new(read_socket, BytesCodec::new()).filter_map(
            |item: Result<BytesMut, _>| async {
                match item {
                    Ok(bytes_mut) => Some(Message::Data(bytes_mut.freeze())),
                    Err(err) => {
                        error!("Read error: {}", err);
                        None
                    }
                }
            },
        ));
        let socket_out = FramedWrite::new(write_socket, BytesCodec::new());

        let input_fut = stream_to_sender(socket_in, sender);
        let output_fut = stream_to_sink(receiver, socket_out);
        join!(input_fut, output_fut);
    }
}

mod common {
    use futures::{
        sink::{Sink, SinkExt},
        stream::{Stream, StreamExt},
    };
    use tokio::sync::mpsc;
    use tracing::{debug, error};

    /// Forwards a Stream to a tokio::sync::mpsc::Sender of the same item type
    pub async fn stream_to_sender<Item, S>(mut stream: S, mut sender: mpsc::Sender<Item>)
    where
        S: Stream<Item = Item> + Unpin,
    {
        debug!("stream_to_sender starting");
        while let Some(next) = stream.next().await {
            if let Err(err) = sender.send(next).await {
                error!("Queue error: {}", err);
                break;
            }
        }
        debug!("stream_to_sender closing");
    }

    /// Forwards a Stream to a Sink of the same item type
    pub async fn stream_to_sink<Item, S, K>(mut stream: S, mut sink: K)
    where
        S: Stream<Item = Item> + Unpin,
        K: Sink<Item> + Unpin,
        <K as Sink<Item>>::Error: std::fmt::Display,
    {
        debug!("stream_to_sink starting");
        while let Some(item) = stream.next().await {
            debug!("stream_to_sink got item");
            if let Err(err) = sink.send(item).await {
                error!("Write error: {}", err);
            }
        }
        debug!("stream_to_sink closing");
    }
}
