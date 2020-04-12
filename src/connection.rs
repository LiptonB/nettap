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

pub mod tokio_connection {
    use bytes::{Bytes, BytesMut};
    use futures::{
        sink::{Sink, SinkExt},
        stream::StreamExt,
    };
    use log::{debug, error};
    use std::marker::Unpin;
    use tokio::{io, join, prelude::*, stream::Stream, sync::mpsc};
    use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

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
        let socket_in = FramedRead::new(read_socket, BytesCodec::new());
        let socket_out = FramedWrite::new(write_socket, BytesCodec::new());

        let input_fut = stream_to_sender(socket_in, sender);
        let output_fut = stream_to_sink(receiver, socket_out);
        join!(input_fut, output_fut);
    }

    async fn stream_to_sender<S>(mut stream: S, mut sender: mpsc::Sender<Message>)
    where
        S: Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
    {
        debug!("stream_to_sender starting");
        while let Some(next) = stream.next().await {
            match next {
                Ok(bytes_mut) => {
                    debug!("stream_to_sender got bytes");
                    if let Err(err) = sender.send(Message::Data(bytes_mut.freeze())).await {
                        error!("Queue error: {}", err);
                        break;
                    }
                }
                Err(err) => {
                    error!("Read error: {}", err);
                }
            }
        }
        debug!("stream_to_sender closing");
    }

    async fn stream_to_sink<S, K>(mut stream: S, mut sink: K)
    where
        S: Stream<Item = Bytes> + Unpin,
        K: Sink<Bytes> + Unpin,
        <K as Sink<Bytes>>::Error: std::fmt::Display,
    {
        debug!("stream_to_sink starting");
        while let Some(bytes) = stream.next().await {
            debug!("stream_to_sink got bytes");
            if let Err(err) = sink.send(bytes).await {
                error!("Write error: {}", err);
            }
        }
        debug!("stream_to_sink closing");
    }
}
