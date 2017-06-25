pub mod subscription_list;
pub mod client_list;

use std::cell::RefCell;
use std::rc::Rc;
use std::collections::{VecDeque, HashMap};
use std::fmt::{self, Debug};
use std::net::SocketAddr;

use futures::sync::mpsc::{self, Receiver};

use mqtt3::*;
use error::{Result, Error};

use client::{ConnectionStatus, Client};
use self::subscription_list::SubscriptionList;
use self::client_list::ClientList;

#[derive(Debug)]
pub struct BrokerState {
    /// Retained Publishes
    pub retains: HashMap<String, Box<Publish>>,
}

impl BrokerState {
    fn new() -> Self {
        BrokerState {
            retains: HashMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct Broker {
    /// All the active clients mapped to their IDs
    clients: Rc<RefCell<ClientList>>,
    /// Subscriptions mapped to interested clients
    subscriptions: Rc<RefCell<SubscriptionList>>,
    state: Rc<RefCell<BrokerState>>,
}

impl Broker {
    pub fn new() -> Self {
        let state = BrokerState::new();
        Broker {
            clients: Rc::new(RefCell::new(ClientList::new())),
            subscriptions: Rc::new(RefCell::new(SubscriptionList::new())),
            state: Rc::new(RefCell::new(state)),
        }
    }

    pub fn has_client(&self, id: &str) -> Option<u8> {
        let clients = self.clients.borrow();
        clients.has_client(id)
    }

    pub fn get_uid(&self, id: &str) -> Option<u8> {
        let clients = self.clients.borrow();
        clients.get_uid(id)
    }

    /// Adds a new client to the broker
    pub fn add_client(&self, mut client: Client) -> Result<Client> {
        let mut clients = self.clients.borrow_mut();
        let id = client.id.clone();

        // there is already a client existing with this id, 
        // send disconnect request this client's handle & replace this client
        if let Some(uid) = clients.has_client(&id) {
            // if clean session is set for new client, clean the old client's state
            // before replacing
            if client.clean_session {
                clients.clear(&client.id, client.uid).unwrap();
            }

            // change the unique id of the new client
            client.set_uid(uid + 1);

            // send disconnect packet if the client is still connected
            if clients.status(&client.id).unwrap() == ConnectionStatus::Connected {
                clients.send(&client.id, Packet::Disconnect)?;
            }

            // replace old client not state members to new new client's members
            let client = clients.replace_client(client).unwrap();
            client.send_all_backlogs();

            // return replaced client which has old 'state' information
            Ok(client)
        } else {
            clients.add_client(client.clone()).unwrap();
            Ok(client)
        }
    }

    /// Adds client to a subscription. If the subscription doesn't exist,
    /// new subscription is created and the client will be added to it
    fn add_subscription_client(&self, topic: SubscribeTopic, client: Client) -> Result<()> {
        let mut subscriptions = self.subscriptions.borrow_mut();
        subscriptions.add_subscription(topic, client)
    }

    /// Remove a client from a subscription
    pub fn remove_subscription_client(&self, topic: SubscribeTopic, id: &str) -> Result<()> {
        let mut subscriptions = self.subscriptions.borrow_mut();
        subscriptions.remove_subscription_client(topic, id)
    }

    /// Get the list of clients for a given subscription
    fn get_subscribed_clients(&self, topic: SubscribeTopic) -> Result<Vec<Client>> {
        let mut subscriptions = self.subscriptions.borrow_mut();
        subscriptions.get_subscribed_clients(topic)
    }

    // Remove the client from broker (including subscriptions)
    pub fn remove_client(&self, id: &str, uid: u8) -> Result<()> {
        self.clients.borrow_mut().remove_client(id, uid)?;
        let mut subscriptions = self.subscriptions.borrow_mut();
        subscriptions.remove_client(id, uid)
    }

    pub fn store_retain(&self, publish: Box<Publish>) {
        let mut state = self.state.borrow_mut();
        state.retains.insert(publish.topic_name.clone(), publish);
    }

    pub fn get_retain(&self, topic: &str) -> Option<Publish> {
        let state = self.state.borrow_mut();
        if let Some(publish) = state.retains.get(topic) {
            Some(*publish.clone())
        } else {
            None
        }
    }

    pub fn handle_connect(&self, connect: Box<Connect>, addr: SocketAddr) -> Result<(Client, Receiver<Packet>)> {
        // TODO: Do connect packet validation here
        if connect.client_id.is_empty() || connect.client_id.chars().next() == Some(' ') {
            error!("Client shouldn't be empty or start with space");
            return Err(Error::InvalidClientId)
        }

        let (tx, rx) = mpsc::channel::<Packet>(100);
        let mut client = Client::new(&connect.client_id, addr, tx);
        client.set_keep_alive(connect.keep_alive);
        if !connect.clean_session {
            client.set_persisent_session();
        }

        if let Some(last_will) = connect.last_will {
            client.set_lastwill(last_will);
        }

        let client = self.add_client(client)?;

        Ok((client, rx))
    }

    // clears the session state in case of clean session
    // NOTE: Don't do anything based on just client id here because this method
    // is called asynchronously on the eventloop. It is possible that disconnect is
    // sent to event loop because of new connections just before client 'replace' 
    // happens and removing client based on just ID here might remove the replaced
    // client from queues
    pub fn handle_disconnect(&self, id: &str, uid: u8, clean_session: bool) -> Result<()> {
        {
            let clients = self.clients.borrow_mut();
            clients.set_status(id, uid, ConnectionStatus::Disconnected).unwrap();
            if clean_session {
                let _ = clients.clear(id, uid);
            }

            // forward lastwill message to all the subscribers
            if let Some(publish) = clients.get_lastwill_publish(id) {
                let _ = self.forward_to_subscribers(Box::new(publish));
            }
        }
        
        if clean_session {
            self.remove_client(id, uid).unwrap();
        }

        Ok(())
    }

    pub fn handle_subscribe(&self, subscribe: Box<Subscribe>, client: &Client) -> Result<()> {
        let pkid = subscribe.pid;
        let mut return_codes = Vec::new();

        // Add current client's id to this subscribe topic
        for topic in subscribe.topics.clone() {
            self.add_subscription_client(topic.clone(), client.clone())?;
            return_codes.push(SubscribeReturnCodes::Success(topic.qos));
        }

        let suback = client.suback_packet(pkid, return_codes);
        let packet = Packet::Suback(suback);
        client.send(packet);

        // publish retained messages to the new client's subscriptions
        for topic in subscribe.topics {
            if let Some(publish) = self.get_retain(&topic.topic_path) {
                client.publish(&publish.topic_name, topic.qos, publish.payload, false, publish.retain)
            }
        }

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
                    QoS::AtLeastOnce => client.store_outgoing_publish(publish),
                    QoS::ExactlyOnce => client.store_outgoing_record(publish),
                    _ => (),
                }

                // forward to eventloop only when client status is Connected
                match client.status() {
                    ConnectionStatus::Connected => client.send(packet),
                    _ => (),
                }
            }
        }
        Ok(())
    }

    pub fn handle_publish(&self, mut publish: Box<Publish>) -> Result<()> {
        let pkid = publish.pid;
        let qos = publish.qos;
        let retain = publish.retain;

        if retain {
            self.store_retain(publish.clone());
            publish.retain = false;
        }

        match qos {
            QoS::AtMostOnce => self.forward_to_subscribers(publish)?,
            // send puback for qos1 packet immediately
            QoS::AtLeastOnce => {
                if let Some(pkid) = pkid {
                    // we should fwd only qos1 packets to all the subscribers (any qos) at this point
                    self.forward_to_subscribers(publish)?;
                } else {
                    error!("Ignoring publish packet. No pkid for QoS1 packet");
                }
            }
            QoS::ExactlyOnce => (),
        };

        Ok(())
    }

    pub fn handle_pubrel(&self, pkid: PacketIdentifier, client: &Client) -> Result<()> {
        // client is asking to release all the recorded packets

        // send pubcomp packet to the client first
        let packet = Packet::Pubcomp(pkid);
        client.send(packet);

        if let Some(record) = client.remove_outgoing_record(pkid) {
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
                        QoS::AtLeastOnce => client.store_outgoing_publish(publish),
                        QoS::ExactlyOnce => client.store_outgoing_record(publish),
                        _ => (),
                    }

                    client.send(packet);
                }
            }
        }

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

#[cfg(test)]
mod test {
    use std::net::{SocketAddr, IpAddr, Ipv4Addr};

    use futures::sync::mpsc::{self, Receiver};
    use client::{Client, ConnectionStatus};
    use super::Broker;
    use mqtt3::*;

    fn mock_client(id: &str, uid: u8) -> (Client, Receiver<Packet>) {
        let (tx, rx) = mpsc::channel::<Packet>(8);
        let mut client = Client::new(id, "127.0.0.1:80".parse().unwrap(), tx);
        client.uid = uid;
        (client, rx)
    }

    #[test]
    fn remove_clients_from_broker_subscriptions_using_aliases() {
        let (c1, ..) = mock_client("mock-client-1", 0);
        let (c2, ..) = mock_client("mock-client-2", 0);

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
        let (c1, ..) = mock_client("mock-client-1", 0);
        let (c2, ..) = mock_client("mock-client-1", 0);
        let (c3, ..) = mock_client("mock-client-1", 0);
        let (c4, ..) = mock_client("mock-client-1", 0);
        let (c5, ..) = mock_client("mock-client-1", 0);

        let broker = Broker::new();

        // replaces previous clients with new uids
        broker.add_client(c1).unwrap();
        broker.add_client(c2).unwrap();
        broker.add_client(c3).unwrap();
        broker.add_client(c4).unwrap();
        broker.add_client(c5).unwrap();

        if let Some(uid) = broker.get_uid("mock-client-1") {
            assert_eq!(4, uid);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn add_and_remove_clients_with_same_ids_to_broker_and_verify_uids() {
        let (c1, ..) = mock_client("mock-client-1", 1);
        let (c2, ..) = mock_client("mock-client-1", 2);
        let (c3, ..) = mock_client("mock-client-1", 3);
        let (c4, ..) = mock_client("mock-client-1", 4);
        let (c5, ..) = mock_client("mock-client-1", 5);

        let broker = Broker::new();

        broker.add_client(c1).unwrap();
        broker.add_client(c2).unwrap();
        broker.add_client(c3).unwrap();
        broker.add_client(c4).unwrap();
        broker.add_client(c5).unwrap();

        broker.remove_client("mock-client-1", 5).unwrap();
        broker.remove_client("mock-client-1", 5).unwrap();

        if let Some(_) = broker.has_client("mock-client-1") {
            assert!(false);
        } else {
            assert!(true);
        }
    }



    #[test]
    fn change_connection_status_of_clients_and_verify_status_in_subscriptions() {
        let (c1, ..) = mock_client("mock-client-1", 0);

        let s1 = SubscribeTopic {
            topic_path: "hello/mqtt".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let broker = Broker::new();
        assert_eq!(ConnectionStatus::Connected, c1.status());
        broker.add_client(c1.clone()).unwrap();

        // add c1 to to s1, s2, s3 & s4
        broker.add_subscription_client(s1.clone(), c1.clone()).unwrap();
        
        // change connection status of a client in 'clients'
        broker.handle_disconnect("mock-client-1", 0, false).unwrap();

        let subscribed_clients = broker.get_subscribed_clients(s1).unwrap();
        assert_eq!(ConnectionStatus::Disconnected, subscribed_clients[0].status());
    }

    #[test]
    fn new_clients_state_after_reconnections() {
        let connect = Connect {
            protocol:  Protocol::new("MQTT", 4).unwrap(),
            keep_alive: 100,
            client_id: "session-test".to_owned(),
            clean_session: false,
            last_will: None,
            username: None,
            password: None,
        };

        let broker = Broker::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 80456); 
        let client = broker.handle_connect(Box::new(connect), addr).unwrap();
    }
}
