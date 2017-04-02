use std::fmt::{self, Debug};
use std::net::SocketAddr;
use futures::sync::mpsc::Sender;

use mqtt3::Packet;

#[derive(Clone)]
pub struct Client {
    pub id: String,
    pub addr: SocketAddr,
    pub tx: Sender<Packet>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, " [  id = {:?}, address = {:?}  ]", self.id, self.addr)
    }
}

impl Client {
    pub fn new(id: &str, addr: SocketAddr, tx: Sender<Packet>) -> Client {
        Client {
            addr: addr,
            id: id.to_string(),
            tx: tx,
        }
    }
}
