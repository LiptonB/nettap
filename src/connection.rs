use bytes::BytesMut;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct Connection {
    incoming: mpsc::Sender<BytesMut>,
}
