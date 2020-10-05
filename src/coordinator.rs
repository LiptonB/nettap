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

        // Deliver messages to this Connection
        let output_receiver = self.broadcast_sender.subscribe();
        let id = Uuid::new_v4();
        let output_receiver = filter_receiver(output_receiver, id);

        // Listen for messages produced by this Connection
        self.incoming.insert(id, input_receiver);

        // Run the Connection
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
    use futures::stream;

    use crate::connection::*;
    use crate::coordinator::*;

    #[tokio::test]
    async fn no_conns_succeeds() {
        let c = Coordinator::new();

        c.run().await;

        // TODO: do we need an assert here?
    }

    #[tokio::test]
    async fn send_succeeds() {
        let mut c = Coordinator::new();

        let stream = Box::pin(stream::once(async { Data(Bytes::from("somestr")) }));
        let (send, mut recv) = futures::channel::mpsc::unbounded();
        c.add_connection(stream_connection::new(stream, send));

        c.run().await;

        assert_eq!(recv.next().await, Some(Bytes::from("somestr")));
    }
}
