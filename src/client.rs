use mqtt3::Packet;
use futures::sync::mpsc;

#[derive(Clone)]
pub struct Client {
    tx: mpsc::Sender<Packet>,
    id: String,
}

impl Client {
    fn new(tx: mpsc::Sender<Packet>, id: &str) -> Client {
        Client {
            tx: tx,
            id: id.to_string(),
        }
    }
}
