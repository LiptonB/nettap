use bytes::Bytes;
use futures::stream::Stream;
use std::future::Future;
use tokio::sync::mpsc;
use uuid::Uuid;

pub type DataStream = Box<dyn Stream<Item = (Uuid, Bytes)>>;
pub type NewConnection =
    Box<dyn FnOnce(mpsc::Sender<Message>, DataStream) -> Box<dyn Future<Output = ()>>>;

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
        S: AsyncRead + AsyncWrite + Unpin,
    {
        move |sender: mpsc::Sender<Message>, receiver: DataStream| {
            tokio_connection(sender, receiver, socket)
        }
    }

    pub async fn tokio_connection<R, S>(sender: mpsc::Sender<Bytes>, receiver: R, socket: S)
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

    async fn stream_to_sender<S>(stream: S, sender: mpsc::Sender<Bytes>)
    where
        S: Stream<Item = Result<BytesMut, std::io::Error>> + Unpin,
    {
        while let Some(next) = stream.next().await {
            match next {
                Ok(bytes_mut) => {
                    if let Err(err) = sender.send(bytes_mut.freeze()).await {
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

    async fn stream_to_sink<S, K>(stream: S, sink: K)
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
