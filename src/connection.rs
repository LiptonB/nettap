use bytes::{Bytes, BytesMut};
use futures::stream::StreamExt;
use futures::sink::{Sink, SinkExt};
use tokio::prelude::*;
use tokio::{join, stream::Stream, sync::mpsc};
use tokio_util::codec::{BytesCodec, Framed};
use log::error;
use std::marker::Unpin;

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
