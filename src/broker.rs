use std::cell::RefCell;
use std::rc::Rc;
use std::collections::{VecDeque};
use std::fmt::{self, Debug};
use std::net::SocketAddr;

use futures::sync::mpsc::{self, Receiver};

use slog::{Logger, Drain};
use slog_term;

use mqtt3::*;
use error::{Result, Error};

use client::{ConnectionStatus, Client};
use subscription_list::SubscriptionList;
use client_list::ClientList;

#[derive(Debug)]
pub struct BrokerState {
    /// For QoS 1. Stores incoming publishes
    pub incoming_pub: VecDeque<Box<Publish>>,
    /// For QoS 2. Stores incoming publishes
    pub incoming_rec: VecDeque<Box<Publish>>,
    /// For QoS 2. Stores incoming release
    pub incoming_rel: VecDeque<PacketIdentifier>,
    /// For QoS 2. Stores incoming comp
    pub incoming_comp: VecDeque<PacketIdentifier>,
}

impl BrokerState {
    fn new() -> Self {
        BrokerState {
            incoming_pub: VecDeque::new(),
            incoming_rec: VecDeque::new(),
            incoming_rel: VecDeque::new(),
            incoming_comp: VecDeque::new(),
        }
    }
}

#[derive(Clone)]
pub struct Broker {
    /// All the active clients mapped to their IDs
    clients: Rc<RefCell<ClientList>>,
    /// Subscriptions mapped to interested clients
    subscriptions: Rc<RefCell<SubscriptionList>>,
    pub state: Rc<RefCell<BrokerState>>,
    pub logger: Logger,
}

impl Broker {
    pub fn new() -> Self {
        let state = BrokerState::new();
        let logger = rumqttd_logger();

        Broker {
            clients: Rc::new(RefCell::new(ClientList::new())),
            subscriptions: Rc::new(RefCell::new(SubscriptionList::new())),
            state: Rc::new(RefCell::new(state)),
            logger: logger,
        }
    }

    fn has_client(&self, id: &str) -> Option<Vec<u16>> {
        let clients = self.clients.borrow();
        clients.has_client(id)
    }

    /// Adds a new client to the broker
    pub fn add_client(&self, mut client: Client) -> Result<bool> {
        let mut clients = self.clients.borrow_mut();
        if let Some(uids) = clients.has_client(&client.id) {
            for uid in uids.iter() {
                clients.send(&client.id, *uid, Packet::Disconnect)?;
            }
            client.set_uid(*uids.last().unwrap() + 1)
        }
        
        clients.add_client(client)
    }

    /// Adds client to a subscription. If the subscription doesn't exist,
    /// new subscription is created and the client will be added to it
    fn add_subscription_client(&self, topic: SubscribeTopic, client: Client) -> Result<bool> {
        let mut subscriptions = self.subscriptions.borrow_mut();
        subscriptions.add_subscription(topic, client)
    }

    /// Remove a client from a subscription
    pub fn remove_subscription_client(&self, topic: SubscribeTopic, id: &str, uid: u16) -> Result<()> {
        let mut subscriptions = self.subscriptions.borrow_mut();
        subscriptions.remove_subscription_client(topic, id, uid)
    }

    /// Get the list of clients for a given subscription
    fn get_subscribed_clients(&self, topic: SubscribeTopic) -> Result<Vec<Client>> {
        let mut subscriptions = self.subscriptions.borrow_mut();
        subscriptions.get_subscribed_clients(topic)
    }

    // Remove the client from broker (including subscriptions)
    pub fn remove_client(&self, id: &str, uid: u16) -> Result<()> {
        self.clients.borrow_mut().remove_client(id, uid)?;
        let mut subscriptions = self.subscriptions.borrow_mut();
        subscriptions.remove_client(id, uid)
    }

    // TODO: Find out if broker should drop message if a new massage with existing
    // pkid is received
    pub fn store_publish(&self, publish: Box<Publish>) {
        let mut state = self.state.borrow_mut();
        state.incoming_pub.push_back(publish.clone());
    }

    pub fn remove_publish(&self, pkid: PacketIdentifier) -> Option<Box<Publish>> {
        let mut state = self.state.borrow_mut();

        match state.incoming_pub
                   .iter()
                   .position(|x| x.pid == Some(pkid)) {
            Some(i) => state.incoming_pub.remove(i),
            None => None,
        }
    }

    pub fn store_record(&self, publish: Box<Publish>) {
        let mut state = self.state.borrow_mut();
        state.incoming_rec.push_back(publish.clone());
    }

    pub fn remove_record(&self, pkid: PacketIdentifier) -> Option<Box<Publish>> {
        let mut state = self.state.borrow_mut();

        match state.incoming_pub
                   .iter()
                   .position(|x| x.pid == Some(pkid)) {
            Some(i) => state.incoming_rec.remove(i),
            None => None,
        }
    }

    pub fn store_rel(&self, pkid: PacketIdentifier) {
        let mut state = self.state.borrow_mut();
        state.incoming_rel.push_back(pkid);
    }

    pub fn remove_rel(&self, pkid: PacketIdentifier) {
        let mut state = self.state.borrow_mut();

        match state.incoming_rel.iter().position(|x| *x == pkid) {
            Some(i) => state.incoming_rel.remove(i),
            None => None,
        };
    }

    pub fn store_comp(&self, pkid: PacketIdentifier) {
        let mut state = self.state.borrow_mut();
        state.incoming_comp.push_back(pkid);
    }

    pub fn remove_comp(&self, pkid: PacketIdentifier) {
        let mut state = self.state.borrow_mut();

        match state.incoming_comp.iter().position(|x| *x == pkid) {
            Some(i) => state.incoming_comp.remove(i),
            None => None,
        };
    }

    pub fn handle_connect(&self, connect: Box<Connect>, addr: SocketAddr) -> Result<(Client, Receiver<Packet>)> {
        // TODO: Do connect packet validation here
        if connect.client_id.is_empty() || connect.client_id.chars().next() == Some(' ') {
            error!(self.logger, "Client shouldn't be empty or start with space");
            return Err(Error::InvalidClientId)
        }

        let (tx, rx) = mpsc::channel::<Packet>(100);
        let mut client = Client::new(&connect.client_id, addr, tx);
        client.set_keep_alive(connect.keep_alive);
        if !connect.clean_session {
            client.set_persisent_session();
        }

        self.add_client(client.clone())?;

        Ok((client, rx))
    }

    pub fn handle_disconnect(&self, id: &str, uid: u16) -> Result<()> {
        self.remove_client(id, uid)
    }

    pub fn handle_subscribe(&self, subscribe: Box<Subscribe>, client: &Client) -> Result<()> {
        let pkid = subscribe.pid;
        let mut return_codes = Vec::new();

        // Add current client's id to this subscribe topic
        for topic in subscribe.topics {
            self.add_subscription_client(topic.clone(), client.clone())?;
            return_codes.push(SubscribeReturnCodes::Success(topic.qos));
        }

        let suback = client.suback_packet(pkid, return_codes);
        let packet = Packet::Suback(suback);
        client.send(packet);
        Ok(())
    }

    fn forward_to_subscribers(&self, publish: Box<Publish>) -> Result<()> {
        let topic = publish.topic_name.clone();
        let payload = publish.payload.clone();

        // publish to all the subscribers in different qos `SubscribeTopic`
        // hash keys
        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce].iter() {

            let subscribe_topic = SubscribeTopic {
                topic_path: topic.clone(),
                qos: qos.clone(),
            };

            for client in self.get_subscribed_clients(subscribe_topic)? {
                let publish = client.publish_packet(&topic, qos.clone(), payload.clone(), false, false);
                let packet = Packet::Publish(publish.clone());

                match *qos {
                    QoS::AtLeastOnce => client.store_publish(publish),
                    QoS::ExactlyOnce => client.store_record(publish),
                    _ => (),
                }

                match client.status() {
                    ConnectionStatus::Connected => client.send(packet),
                    _ => (),
                }
            }
        }
        Ok(())
    }

    pub fn handle_publish(&self, publish: Box<Publish>, client: &Client) -> Result<()> {
        let pkid = publish.pid;
        let qos = publish.qos;

        match qos {
            QoS::AtMostOnce => self.forward_to_subscribers(publish)?,
            // send puback for qos1 packet immediately
            QoS::AtLeastOnce => {
                if let Some(pkid) = pkid {
                    let packet = Packet::Puback(pkid);
                    client.send(packet);
                    // we should fwd only qos1 packets to all the subscribers (any qos) at this point
                    self.forward_to_subscribers(publish)?;
                } else {
                    error!(self.logger,
                           "Ignoring publish packet. No pkid for QoS1 packet");
                }
            }
            // save the qos2 packet and send pubrec
            QoS::ExactlyOnce => {
                if let Some(pkid) = pkid {
                    self.store_record(publish.clone());
                    let packet = Packet::Pubrec(pkid);
                    client.send(packet);
                } else {
                    error!(self.logger,
                           "Ignoring record packet. No pkid for QoS2 packet");
                }
            }
        };

        Ok(())
    }

    pub fn handle_puback(&self, pkid: PacketIdentifier, client: &Client) -> Result<()> {
        client.remove_publish(pkid);
        Ok(())
    }

    pub fn handle_pubrec(&self, pkid: PacketIdentifier, client: &Client) -> Result<()> {
        debug!(self.logger, "PubRec <= {:?}", pkid);

        // remove record packet from state queues
        if let Some(record) = client.remove_record(pkid) {
            // record and send pubrel packet
            client.store_rel(record.pid.unwrap()); //TODO: Remove unwrap. Might be a problem if client behaves incorrectly
            let packet = Packet::Pubrel(pkid);
            client.send(packet);
        }
        Ok(())
    }

    pub fn handle_pubcomp(&self, pkid: PacketIdentifier, client: &Client) -> Result<()> {
        // remove release packet from state queues
        client.remove_rel(pkid);
        Ok(())
    }

    pub fn handle_pubrel(&self, pkid: PacketIdentifier, client: &Client) -> Result<()> {
        // client is asking to release all the recorded packets

        // send pubcomp packet to the client first
        let packet = Packet::Pubcomp(pkid);
        client.send(packet);

        if let Some(record) = client.remove_record(pkid) {
            let topic = record.topic_name.clone();
            let payload = record.payload;

            // publish to all the subscribers in different qos `SubscribeTopic`
            // hash keys
            for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce].iter() {

                let subscribe_topic = SubscribeTopic {
                    topic_path: topic.clone(),
                    qos: qos.clone(),
                };

                for client in self.get_subscribed_clients(subscribe_topic)? {
                    let publish = client.publish_packet(&topic, qos.clone(), payload.clone(), false, false);
                    let packet = Packet::Publish(publish.clone());

                    match *qos {
                        QoS::AtLeastOnce => client.store_publish(publish),
                        QoS::ExactlyOnce => client.store_record(publish),
                        _ => (),
                    }

                    client.send(packet);
                }
            }
        }

        Ok(())
    }

    pub fn handle_pingreq(&self, client: &Client) -> Result<()> {
        debug!(self.logger, "PingReq <= {:?}",  client.id);
        let pingresp = Packet::Pingresp;
        client.send(pingresp);
        Ok(())
    }
}

impl Debug for Broker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "{:#?}\n{:#?}\n{:#?}",
               self.clients.borrow(),
               self.subscriptions.borrow(),
               self.state.borrow())
    }
}

fn rumqttd_logger() -> Logger {
    use std::sync::Mutex;

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = Mutex::new(drain).fuse();
    Logger::root(drain, o!("module" => "broker"))
}

#[cfg(test)]
mod test {
    use futures::sync::mpsc::{self, Receiver};
    use client::Client;
    use super::Broker;
    use mqtt3::*;

    fn mock_client(id: &str) -> (Client, Receiver<Packet>) {
        let (tx, rx) = mpsc::channel::<Packet>(8);
        (Client::new(id, "127.0.0.1:80".parse().unwrap(), tx), rx)
    }

    #[test]
    fn remove_clients_from_broker_subscriptions_using_aliases() {
        let (c1, ..) = mock_client("mock-client-1");
        let (c2, ..) = mock_client("mock-client-2");

        let s1 = SubscribeTopic {
            topic_path: "hello/mqtt".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let broker = Broker::new();

        // add c1 to to s1, s2, s3 & s4
        broker.add_subscription_client(s1.clone(), c1.clone()).unwrap();
        broker.add_subscription_client(s1.clone(), c2.clone()).unwrap();

        let broker_alias = broker.clone();

        // remove c1 & c2 from all subscriptions and verify clients
        broker_alias.remove_client(&c1.id, 0).unwrap();
        broker_alias.remove_client(&c2.id, 0).unwrap();

        for s in [s1].iter() {
            let clients = broker.get_subscribed_clients(s.clone()).unwrap();
            assert_eq!(clients.len(), 0);
        }
    }

    #[test]
    fn add_clients_with_same_ids_to_broker_and_verify_uids() {
        let (c1, ..) = mock_client("mock-client-1");
        let (c2, ..) = mock_client("mock-client-1");
        let (c3, ..) = mock_client("mock-client-1");
        let (c4, ..) = mock_client("mock-client-1");
        let (c5, ..) = mock_client("mock-client-1");

        let broker = Broker::new();

        broker.add_client(c1).unwrap();
        broker.add_client(c2).unwrap();
        broker.add_client(c3).unwrap();
        broker.add_client(c4).unwrap();
        broker.add_client(c5).unwrap();

        if let Some(uids) = broker.has_client("mock-client-1") {
            let mut uids_iter = uids.iter();
            assert_eq!(0, *uids_iter.next().unwrap());
            assert_eq!(1, *uids_iter.next().unwrap());
            assert_eq!(2, *uids_iter.next().unwrap());
            assert_eq!(3, *uids_iter.next().unwrap());
            assert_eq!(4, *uids_iter.next().unwrap());
        } else {
            assert!(false);
        }
    }

    #[test]
    fn add_and_remove_clients_with_same_ids_to_broker_and_verify_uids() {
        let (c1, ..) = mock_client("mock-client-1");
        let (c2, ..) = mock_client("mock-client-1");
        let (c3, ..) = mock_client("mock-client-1");
        let (c4, ..) = mock_client("mock-client-1");
        let (c5, ..) = mock_client("mock-client-1");

        let broker = Broker::new();

        broker.add_client(c1).unwrap();
        broker.add_client(c2).unwrap();
        broker.add_client(c3).unwrap();
        broker.add_client(c4).unwrap();
        broker.add_client(c5).unwrap();

        broker.remove_client("mock-client-1", 1).unwrap();
        broker.remove_client("mock-client-1", 3).unwrap();

        if let Some(uids) = broker.has_client("mock-client-1") {
            let mut uids_iter = uids.iter();
            assert_eq!(0, *uids_iter.next().unwrap());
            assert_eq!(2, *uids_iter.next().unwrap());
            assert_eq!(4, *uids_iter.next().unwrap());
        } else {
            assert!(false);
        }
    }

    #[test]
    fn store_and_remove_from_broker_state_queues_using_aliases() {
        let broker = Broker::new();
        let broker_alias = broker.clone();

        let (pkid1, pkid2, pkid3) = (PacketIdentifier(1), PacketIdentifier(2), PacketIdentifier(3));

        broker.store_rel(pkid1);
        broker.store_rel(pkid2);
        broker.store_rel(pkid3);

        broker_alias.remove_rel(pkid2);

        {
            let state = broker.state.borrow_mut();
            let mut it = state.incoming_rel.iter();

            assert_eq!(pkid1, *it.next().unwrap());
            assert_eq!(pkid3, *it.next().unwrap());
            assert_eq!(None, it.next());
        }
    }
}
