use bytes::Bytes;
use futures::stream::Stream;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::mpsc;

mod common;
#[cfg(test)]
pub mod stream_connection;
pub mod tokio_connection;

pub type DataStream = Box<dyn Stream<Item = Bytes> + Unpin + Send>;
type Task = Pin<Box<dyn Future<Output = ()> + Send>>;
pub type NewConnection = Box<dyn FnOnce(mpsc::Sender<Message>, DataStream) -> Task + Send>;

pub enum Message {
    Data(Bytes),
    NewConnection(NewConnection),
}
