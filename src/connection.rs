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

    let in_fut = socket_in.from_err().forward(incoming);
    let out_fut = outgoing.forward(socket_out.sink_from_err());
    // TODO: Are we happy with this "swallow all errors" business
    in_fut.join(out_fut).map(|_| ()).map_err(|_| ())
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
