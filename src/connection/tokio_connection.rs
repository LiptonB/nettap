use bytes::Bytes;
use futures::stream::StreamExt;
use std::marker::Unpin;
use tokio::{
    io::{self, AsyncRead, AsyncWrite},
    join,
    sync::mpsc,
};
use tokio_stream::Stream;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};
use tracing::{debug, error};

use super::common::{stream_to_sender, stream_to_sink};
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
    sender: mpsc::Sender<Message>,
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
        |item: Result<bytes::BytesMut, std::io::Error>| async {
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
