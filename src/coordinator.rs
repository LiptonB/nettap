use bytes::Bytes;
use log::{debug, error};
use tokio::{
    stream::{StreamExt, StreamMap},
    sync::{broadcast, mpsc},
};
use uuid::Uuid;

use crate::connection::{
    DataStream,
    Message::{self, *},
    NewConnection,
};

const CHANNEL_CAPACITY: usize = 10;

pub struct Coordinator {
    incoming: StreamMap<Uuid, mpsc::Receiver<Message>>,
    broadcast_sender: broadcast::Sender<(Uuid, Bytes)>,
}

impl Coordinator {
    pub fn new() -> Coordinator {
        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        Coordinator {
            incoming: StreamMap::new(),
            broadcast_sender: sender,
        }
    }

    pub async fn run(mut self) {
        while let Some((sender, message)) = self.incoming.next().await {
            match message {
                Data(bytes) => {
                    if let Err(err) = self.broadcast_sender.send((sender, bytes)) {
                        error!("Send error: {:?}", err);
                    }
                }
                NewConnection(nc) => {
                    self.add_connection(nc);
                }
            }
        }
    }

    pub fn add_connection(&mut self, nc: NewConnection) {
        let (input_sender, input_receiver) = mpsc::channel(CHANNEL_CAPACITY);
        let output_receiver = self.broadcast_sender.subscribe();
        let id = Uuid::new_v4();
        let output_receiver = filter_receiver(output_receiver, id);
        self.incoming.insert(id, input_receiver);
        tokio::spawn(nc(input_sender, output_receiver));
        debug!("started connection: {}", id);
    }
}

pub fn filter_receiver(receiver: broadcast::Receiver<(Uuid, Bytes)>, id: Uuid) -> DataStream {
    Box::new(receiver.filter_map(move |received| {
        let (msg_id, data) = received.ok()?;
        if msg_id == id {
            None
        } else {
            Some(data)
        }
    }))
}

#[cfg(test)]
mod tests {
    use crate::coordinator::*;

    fn run_future<E: Send>(f: impl Future<Item = (), Error = E> + Send + 'static) {
        let f = f.map_err(|_| panic!("returned error"));
        tokio::run(f);
    }

    #[test]
    fn creation_and_wait_succeeds() {
        let data_rx = stream::empty();
        let conn_rx = stream::empty();

        let c = Coordinator::new(data_rx, conn_rx);
        run_future(c);
    }

    #[test]
    fn send_with_no_conns_succeeds() {
        let data_rx = stream::once(Ok(Bytes::from("somestr")));
        let conn_rx = stream::empty();

        let c = Coordinator::new(data_rx, conn_rx);
        run_future(c);
    }

    #[test]
    fn send_with_conn_transfers_data() {
        let data_rx = stream::once(Ok(Bytes::from("somestr")));
        // TODO
        let conn_rx = stream::empty();

        let c = Coordinator::new(data_rx, conn_rx);
        run_future(c);
    }
}
