use std::fmt::{self, Debug};
use std::net::SocketAddr;
use futures::sync::mpsc::Sender;

use mqtt3::*;

#[derive(Clone)]
pub struct Client {
    pub id: String,
    pub addr: SocketAddr,
    pub tx: Sender<Packet>,
    pub last_pkid: PacketIdentifier,
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
            last_pkid: PacketIdentifier(0),
        }
    }

    pub fn next_pkid(&mut self) -> PacketIdentifier {
        let PacketIdentifier(mut pkid) = self.last_pkid;
        if pkid == 65535 {
            pkid = 0;
        }
        self.last_pkid = PacketIdentifier(pkid + 1);
        self.last_pkid
    }
}
