use bytes::BytesMut;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct Connection {}

impl Connection {
    fn new(incoming: Stream<BytesMut>, outgoing: Sink<BytesMut>) -> Self {}

    fn to_future(self) -> impl Future<>
}
