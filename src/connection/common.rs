use futures::{
    sink::{Sink, SinkExt},
    stream::{Stream, StreamExt},
};
use tokio::sync::mpsc;
use tracing::{debug, error};

/// Forwards a Stream to a tokio::sync::mpsc::Sender of the same item type
pub async fn stream_to_sender<Item, S>(mut stream: S, sender: mpsc::Sender<Item>)
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
