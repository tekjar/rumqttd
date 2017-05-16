use std::fmt::{self, Debug};
use std::net::SocketAddr;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use futures::sync::mpsc::Sender;
use futures::{Future, Sink};

use mqtt3::*;

use slog::{Logger, Drain};
use slog_term;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
}

#[derive(Debug)]
pub struct ClientState {
    /// Connection status of this client for handling persistent sessions
    pub status: ConnectionStatus,
    /// Time at which this client received last control packet
    pub last_control_at: Instant,
    pub last_pkid: PacketIdentifier,
    /// For QoS 1. Stores outgoing publishes
    pub outgoing_pub: VecDeque<Box<Publish>>,
    /// For QoS 2. Stores outgoing publishes
    pub outgoing_rec: VecDeque<Box<Publish>>,
    /// For QoS 2. Stores outgoing release
    pub outgoing_rel: VecDeque<PacketIdentifier>,
    /// For QoS 2. Stores outgoing comp
    pub outgoing_comp: VecDeque<PacketIdentifier>,
}

impl ClientState {
    pub fn new() -> Self {
        ClientState {
            status: ConnectionStatus::Connected,
            last_control_at: Instant::now(),
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
    pub uid: u8, // unique id for verifying replacements in unittests
    pub addr: SocketAddr,
    pub tx: Sender<Packet>,
    pub keep_alive: Option<Duration>,
    pub clean_session: bool,
    pub state: Rc<RefCell<ClientState>>,
    logger: Logger,
}

impl Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, " [  id = {:?}, address = {:?}\n ]", self.id, self.addr)
    }
}

impl Client {
    pub fn new(id: &str, addr: SocketAddr, tx: Sender<Packet>) -> Client {
        let state = ClientState::new();
        let logger = rumqttd_logger(id);

        Client {
            addr: addr,
            id: id.to_string(),
            uid: 0,
            tx: tx,
            keep_alive: None,
            clean_session: true,
            logger: logger,
            state: Rc::new(RefCell::new(state)),
        }
    }

    // NOTE: this broker sets keep alive time to 30 seconds (to invoke keep alive checking timer)
    // if connect packet has a keep alive of 0. this helps broker to disconnect sedentary clients.
    // spec says it's upto broker when to disconnect sedentary clients
    pub fn set_keep_alive(&mut self, t: u16) {
        if t == 0 {
            self.keep_alive = Some(Duration::new(30, 0));
        } else {
            self.keep_alive = Some(Duration::new(t as u64, 0));
        }
    }

    pub fn set_persisent_session(&mut self) {
        self.clean_session = false;
    }

    pub fn next_pkid(&self) -> PacketIdentifier {
        let mut state = self.state.borrow_mut();
        let PacketIdentifier(mut pkid) = state.last_pkid;
        if pkid == 65535 {
            pkid = 0;
        }
        state.last_pkid = PacketIdentifier(pkid + 1);
        state.last_pkid
    }

    pub fn status(&self) -> ConnectionStatus {
        self.state.borrow().status
    }

    // reset the last control packet received time
    pub fn reset_last_control_at(&self) {
        let mut state = self.state.borrow_mut();
        state.last_control_at = Instant::now();
    }

    // check when the last control packet/pingreq packet
    // is received and return the status which tells if
    // keep alive time has exceeded
    // NOTE: status will be checked for zero keepalive times also
    pub fn has_exceeded_keep_alive(&self) -> bool {
        let state = self.state.borrow_mut();
        let last_control_at = state.last_control_at;

        if let Some(keep_alive) = self.keep_alive  {
            let keep_alive = keep_alive.as_secs();
            let keep_alive = Duration::new(f32::ceil(1.5 * keep_alive as f32) as u64, 0);
            if last_control_at.elapsed() > keep_alive {
                true
            } else {
                false
            }
        } else {
            true
        }
    }

    // TODO: Find out if broker should drop message if a new massage with existing
    // pkid is received
    pub fn store_publish(&self, publish: Box<Publish>) {
        let mut state = self.state.borrow_mut();
        state.outgoing_pub.push_back(publish.clone());
    }

    pub fn remove_publish(&self, pkid: PacketIdentifier) -> Option<Box<Publish>> {
        let mut state = self.state.borrow_mut();

        if let Some(index) = state.outgoing_pub
                                  .iter()
                                  .position(|x| x.pid == Some(pkid)) {
            state.outgoing_pub.remove(index)
        } else {
            error!(self.logger, "Unsolicited PUBLISH packet: {:?}", pkid);
            None
        }
    }

    pub fn store_record(&self, publish: Box<Publish>) {
        let mut state = self.state.borrow_mut();
        state.outgoing_rec.push_back(publish.clone());
    }

    pub fn remove_record(&self, pkid: PacketIdentifier) -> Option<Box<Publish>> {
        let mut state = self.state.borrow_mut();

        if let Some(index) = state.outgoing_rec
                                  .iter()
                                  .position(|x| x.pid == Some(pkid)) {
            state.outgoing_rec.remove(index)
        } else {
            error!(self.logger, "Unsolicited RECORD packet: {:?}", pkid);
            None
        }
    }

    pub fn store_rel(&self, pkid: PacketIdentifier) {
        let mut state = self.state.borrow_mut();
        state.outgoing_rel.push_back(pkid);
    }

    pub fn remove_rel(&self, pkid: PacketIdentifier) -> Option<PacketIdentifier> {
        let mut state = self.state.borrow_mut();

        if let Some(index) = state.outgoing_rel.iter().position(|x| *x == pkid) {
            state.outgoing_rel.remove(index)
        } else {
            error!(self.logger, "Unsolicited RELEASE packet: {:?}", pkid);
            None
        }
    }

    pub fn store_comp(&self, pkid: PacketIdentifier) {
        let mut state = self.state.borrow_mut();
        state.outgoing_comp.push_back(pkid);
    }

    pub fn remove_comp(&self, pkid: PacketIdentifier) -> Option<PacketIdentifier> {
        let mut state = self.state.borrow_mut();

        if let Some(index) = state.outgoing_comp.iter().position(|x| *x == pkid) {
            state.outgoing_comp.remove(index)
        } else {
            error!(self.logger, "Unsolicited COMPLETE packet: {:?}", pkid);
            None
        }
    }

    pub fn send(&self, packet: Packet) {
        let _ = self.tx.clone().send(packet).wait();
    }

    pub fn suback_packet(&self, pkid: PacketIdentifier, return_codes: Vec<SubscribeReturnCodes>) -> Box<Suback> {

        Box::new(Suback {
                     pid: pkid,
                     return_codes: return_codes,
                 })
    }

    pub fn publish_packet(&self, topic: &str, qos: QoS, payload: Arc<Vec<u8>>, dup: bool, retain: bool) -> Box<Publish> {

        let pkid = if qos == QoS::AtMostOnce {
            None
        } else {
            Some(self.next_pkid())
        };

        Box::new(Publish {
                     dup: dup,
                     qos: qos,
                     retain: retain,
                     pid: pkid,
                     topic_name: topic.to_owned(),
                     payload: payload.clone(),
                 })

    }

    pub fn queues(&self) {
        let state = self.state.borrow();

        print!("OUTGOING REC = [");
        for e in state.outgoing_rec.iter() {
            print!("{:?} ", e.pid);
        }
        println!(" ]");

        print!("OUTGOING REL = [");
        for e in state.outgoing_rel.iter() {
            print!("{:?} ", e);
        }
        println!(" ]");
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use futures::sync::mpsc::{self, Receiver};
    use super::Client;
    use mqtt3::*;

    fn mock_client() -> (Client, Receiver<Packet>) {
        let (tx, rx) = mpsc::channel::<Packet>(8);
        (Client::new("mock-client", "127.0.0.1:80".parse().unwrap(), tx), rx)
    }


    #[test]
    fn next_pkid_roll() {
        let (client, ..) = mock_client();
        let mut pkid = PacketIdentifier(0);
        for _ in 0..65536 {
            pkid = client.next_pkid();
        }
        assert_eq!(PacketIdentifier(1), pkid);
    }

    #[test]
    fn add_and_remove_of_message_from_publish_queue() {
        let (client, ..) = mock_client();

        for i in 0..100 {
            let publish = Box::new(Publish {
                                       dup: false,
                                       qos: QoS::AtLeastOnce,
                                       retain: false,
                                       pid: Some(PacketIdentifier(i)),
                                       topic_name: "hello/world".to_owned(),
                                       payload: Arc::new(vec![1, 2, 3]),
                                   });

            client.store_publish(publish);
        }

        // sequential remove
        for i in 0..10 {
            client.remove_publish(PacketIdentifier(i));
        }

        {
            // to make sure that the following client methods doesn't panic
            let state = client.state.borrow_mut();

            for i in 0..10 {
                let index = state.outgoing_pub
                                 .iter()
                                 .position(|x| x.pid == Some(PacketIdentifier(i)));
                assert_eq!(index, None);
            }

        }

        // big sequential remove
        for i in 10..90 {
            client.remove_publish(PacketIdentifier(i));
        }

        {
            // to make sure that the following client methods doesn't panic
            let state = client.state.borrow_mut();
            for i in 10..90 {
                let index = state.outgoing_pub
                                 .iter()
                                 .position(|x| x.pid == Some(PacketIdentifier(i)));
                assert_eq!(index, None);
            }
        }

        // intermediate removes
        for i in [91_u16, 93, 95, 97, 99].iter() {
            client.remove_publish(PacketIdentifier(*i));
        }

        {
            // to make sure that the following client methods doesn't panic
            let state = client.state.borrow_mut();
            let mut expected_index = 0;

            for i in [90, 92, 94, 96, 98].iter() {
                let index = state.outgoing_pub
                                 .iter()
                                 .position(|x| x.pid == Some(PacketIdentifier(*i)));
                assert_eq!(index, Some(expected_index));
                expected_index += 1;
            }
        }
    }
}

fn rumqttd_logger(client_id: &str) -> Logger {
    use std::sync::Mutex;

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = Mutex::new(drain).fuse();
    Logger::root(drain, o!("client-id" => client_id.to_owned()))
}
