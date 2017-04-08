use std::fmt::{self, Debug};
use std::net::SocketAddr;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::collections::VecDeque;

use futures::sync::mpsc::Sender;
use futures::{Future, Sink};

use mqtt3::*;

pub struct ClientState {
    pub last_pkid: PacketIdentifier,
    /// For QoS 1. Stores outgoing publishes
    pub outgoing_pub: VecDeque<Box<Message>>,
    /// For QoS 2. Stores outgoing publishes
    pub outgoing_rec: VecDeque<Box<Message>>,
    /// For QoS 2. Stores outgoing release
    pub outgoing_rel: VecDeque<PacketIdentifier>,
    /// For QoS 2. Stores outgoing comp
    pub outgoing_comp: VecDeque<PacketIdentifier>,
}

impl ClientState {
    pub fn new() -> Self {
        ClientState {
            last_pkid: PacketIdentifier(0),
            outgoing_pub: VecDeque::new(),
            outgoing_rec: VecDeque::new(),
            outgoing_rel: VecDeque::new(),
            outgoing_comp: VecDeque::new(),
        }
    }
}

#[derive(Clone)]
pub struct Client {
    pub id: String,
    pub addr: SocketAddr,
    pub tx: Sender<Packet>,

    pub state: Rc<RefCell<ClientState>>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, " [  id = {:?}, address = {:?}  ]", self.id, self.addr)
    }
}

impl Client {
    pub fn new(id: &str, addr: SocketAddr, tx: Sender<Packet>) -> Client {
        let state = ClientState::new();
        Client {
            addr: addr,
            id: id.to_string(),
            tx: tx,

            state: Rc::new(RefCell::new(state)),
        }
    }

    pub fn next_pkid(&mut self) -> PacketIdentifier {
        let mut state = self.state.borrow_mut();
        let PacketIdentifier(mut pkid) = state.last_pkid;
        if pkid == 65535 {
            pkid = 0;
        }
        state.last_pkid = PacketIdentifier(pkid + 1);
        state.last_pkid
    }

    pub fn send(&self, packet: Packet) {
        let _ = self.tx.clone().send(packet).wait();
    }

    pub fn suback_packet(&self, pkid: PacketIdentifier, return_codes: Vec<SubscribeReturnCodes>) -> Packet {

        Packet::Suback(Box::new(Suback {
                                    pid: pkid,
                                    return_codes: return_codes,
                                }))
    }

    pub fn publish_packet(&mut self, topic: &str, qos: QoS, payload: Arc<Vec<u8>>, dup: bool, retain: bool) -> Packet {

        let pkid = if qos == QoS::AtMostOnce {
            None
        } else {
            Some(self.next_pkid())
        };

        Packet::Publish(Box::new(Publish {
                                     dup: dup,
                                     qos: qos,
                                     retain: retain,
                                     pid: pkid,
                                     topic_name: topic.to_owned(),
                                     payload: payload.clone(),
                                 }))

    }
}
