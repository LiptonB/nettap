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
    use log::error;
    use std::marker::Unpin;
    use tokio::{join, prelude::*, stream::Stream, sync::mpsc};
    use tokio_util::codec::{BytesCodec, Framed};

    use super::{DataStream, Message, NewConnection};

    pub fn new_tokio_connection<S>(socket: S) -> NewConnection
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Box::new(move |sender: mpsc::Sender<Message>, receiver: DataStream| {
            Box::pin(tokio_connection(sender, receiver, socket))
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
                    let connection = new_tokio_connection(stream);
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

    pub async fn tokio_connection<R, S>(sender: mpsc::Sender<Message>, receiver: R, socket: S)
    where
        R: Stream<Item = Bytes> + Unpin,
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let framed = Framed::new(socket, BytesCodec::new());
        let (socket_out, socket_in) = framed.split();

        let input_fut = stream_to_sender(socket_in, sender);
        let output_fut = stream_to_sink(receiver, socket_out);
        join!(input_fut, output_fut);
    }

    async fn stream_to_sender<S>(mut stream: S, mut sender: mpsc::Sender<Message>)
    where
        S: Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
    {
        while let Some(next) = stream.next().await {
            match next {
                Ok(bytes_mut) => {
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
    }

    async fn stream_to_sink<S, K>(mut stream: S, mut sink: K)
    where
        S: Stream<Item = Bytes> + Unpin,
        K: Sink<Bytes> + Unpin,
        <K as Sink<Bytes>>::Error: std::fmt::Display,
    {
        while let Some(bytes) = stream.next().await {
            if let Err(err) = sink.send(bytes).await {
                error!("Write error: {}", err);
            }
        }
    }
}
