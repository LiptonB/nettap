use bytes::{Bytes, BytesMut};
use failure::Error;
use tokio::codec::{BytesCodec, Framed};
use tokio::prelude::*;

pub fn tokio_connection<I, O, S>(
    incoming: I,
    outgoing: O,
    socket: S,
) -> impl Future<Item = (), Error = ()>
where
    I: Sink<SinkItem = BytesMut, SinkError = Error>,
    O: Stream<Item = Bytes, Error = Error>,
    S: AsyncRead + AsyncWrite,
{
    let framed = Framed::new(socket, BytesCodec::new());
    let (socket_out, socket_in) = framed.split();

    let in_fut = socket_in.from_err().forward(incoming).map(||);
    let out_fut = outgoing.forward(socket_out.sink_from_err()).map(||);
    in_fut.join(out_fut)
}
