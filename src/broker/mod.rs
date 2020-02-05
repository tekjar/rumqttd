pub mod client_list;
pub mod subscription_list;

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::net::SocketAddr;

use rand::{self, Rng};
use tokio::sync::mpsc::{self, Receiver};

use crate::error::{Error, Result};
use rumq_core::*;

use self::client_list::ClientList;
use self::subscription_list::SubscriptionList;
use crate::client::{Client, ConnectionStatus};

#[derive(Debug)]
pub struct BrokerState {
    /// Retained Publishes
    pub retains: HashMap<String, Publish>,
}

impl BrokerState {
    fn new() -> Self {
        BrokerState { retains: HashMap::new() }
    }
}

pub struct Broker {
    /// All the active clients mapped to their IDs
    clients: ClientList,
    /// Subscriptions mapped to interested clients
    subscriptions: SubscriptionList,
    state: BrokerState,
}

impl Broker {
    pub fn new() -> Self {
        Broker {
            clients: ClientList::new(),
            subscriptions: SubscriptionList::new(),
            state: BrokerState::new(),
        }
    }

    pub fn has_client(&self, id: &str) -> Option<u8> {
        self.clients.has_client(id)
    }

    pub fn get_uid(&self, id: &str) -> Option<u8> {
        self.clients.get_uid(id)
    }

    /// Adds a new client to the broker & return the client along with
    /// session present status
    fn add_client(&mut self, mut client: Client) -> Result<(Client, bool)> {
        let id = client.id.clone();
        let mut session_exists = false;

        // there is already a client existing with this id,
        // send disconnect request this client's handle & replace this client
        if let Some(uid) = self.clients.has_client(&id) {
            session_exists = true;
            // if clean session is set for new client, clean the old client's state
            // before replacing
            if client.clean_session {
                self.clients.clear(&client.id, client.uid).unwrap();
                self.subscriptions.remove_client(&client.id, client.uid).unwrap();
            }

            // change the unique id of the new client
            client.set_uid(uid + 1);

            // send disconnect packet if the client is still connected
            if self.clients.status(&client.id).unwrap() == ConnectionStatus::Connected {
                self.clients.send(&client.id, Packet::Disconnect)?;
            }

            // replace old client not state members to new new client's members
            let client = self.clients.replace_client(client).unwrap();
            let _ = self.subscriptions.replace_client(client.clone());

            // return replaced client which has old 'state' information
            Ok((client, session_exists))
        } else {
            self.clients.add_client(client.clone()).unwrap();
            Ok((client, session_exists))
        }
    }

    /// Adds client to a subscription. If the subscription doesn't exist,
    /// new subscription is created and the client will be added to it
    fn add_subscription_client(&mut self, topic: SubscribeTopic, client: Client) -> Result<()> {
        self.subscriptions.add_subscription(topic, client)
    }

    /// Remove a client from a subscription
    // fn remove_subscription_client(&mut self, topic: SubscribeTopic, id: &str) -> Result<()> {
    //     self.subscriptions.remove_subscription_client(topic, id)
    // }

    /// Get the list of clients for a given subscription
    fn get_subscribed_clients(&mut self, topic: SubscribeTopic) -> Result<Vec<Client>> {
        self.subscriptions.get_subscribed_clients(topic)
    }

    // Remove the client from broker (including subscriptions)
    fn remove_client(&mut self, id: &str, uid: u8) -> Result<()> {
        self.clients.remove_client(id, uid)?;
        self.subscriptions.remove_client(id, uid)
    }

    fn store_retain(&mut self, publish: Publish) {
        let retain_subscription = SubscribeTopic {
            topic_path: publish.topic_name.clone(),
            qos: publish.qos,
        };
        if publish.payload.len() == 0 {
            // remove existing retains if new retain publish's payload len = 0
            let _ = self.state.retains.remove(&retain_subscription.topic_path);
        } else {
            self.state.retains.insert(retain_subscription.topic_path, publish);
        }
    }

    fn get_retain(&self, topic: &SubscribeTopic) -> Option<Vec<Publish>> {
        None
    }

    pub fn handle_connect(&mut self, connect: Connect, addr: SocketAddr) -> Result<(Client, Connack, Receiver<Packet>)> {
        // TODO: Do connect packet validation here
        if connect.client_id.starts_with(' ') || (!connect.clean_session && connect.client_id.is_empty()) {
            error!("Client id shouldn't start with space (or) shouldn't be empty in persistent sessions");
            return Err(Error::InvalidClientId);
        }

        let client_id = if connect.client_id.is_empty() {
            gen_client_id()
        } else {
            connect.client_id.clone()
        };

        let (tx, rx) = mpsc::channel::<Packet>(100);
        let mut client = Client::new(&client_id, addr, tx);
        let clean_session = connect.clean_session;

        client.set_keep_alive(connect.keep_alive);
        if !connect.clean_session {
            client.set_persisent_session();
        }

        if let Some(last_will) = connect.last_will {
            client.set_lastwill(last_will);
        }

        let (client, session_exists) = self.add_client(client)?;

        let connack = if clean_session {
            Connack {
                session_present: false,
                code: ConnectReturnCode::Accepted,
            }
        } else {
            Connack {
                session_present: session_exists,
                code: ConnectReturnCode::Accepted,
            }
        };

        Ok((client, connack, rx))
    }

    // clears the session state in case of clean session
    // NOTE: Don't do anything based on just client id here because this method
    // is called asynchronously on the eventloop. It is possible that disconnect is
    // sent to event loop because of new connections just before client 'replace'
    // happens and removing client based on just ID here might remove the replaced
    // client from queues
    pub fn handle_disconnect(&mut self, id: &str, uid: u8, clean_session: bool) -> Result<()> {
        {
            self.clients.set_status(id, uid, ConnectionStatus::Disconnected).unwrap();
            if clean_session {
                let _ = self.clients.clear(id, uid);
            }

            // forward lastwill message to all the subscribers
            if let Some(publish) = self.clients.get_lastwill_publish(id) {
                let _ = self.forward_to_subscribers(publish);
            }
        }

        if clean_session {
            self.remove_client(id, uid).unwrap();
        }

        Ok(())
    }

    pub fn handle_subscribe(&mut self, topics: Vec<SubscribeTopic>, client: &Client) -> Result<()> {
        // Add current client's id to this subscribe topic & publish retains
        for topic in topics {
            self.add_subscription_client(topic.clone(), client.clone())?;

            if let Some(mut publishes) = self.get_retain(&topic) {
                let len = publishes.len();

                for _ in 0..len {
                    // TODO: Use vecdeque instead
                    let p = publishes.remove(0);
                    // TODO: Should we publish with publish qos or topic qos ??
                    client.publish(&p.topic_name.clone(), topic.qos, p.payload, false, true)
                }
            }
        }

        Ok(())
    }

    fn forward_to_subscribers(&mut self, publish: Publish) -> Result<()> {
        let topic = publish.topic_name.clone();
        let payload = publish.payload.clone();

        // publish to all the subscribers in different qos `SubscribeTopic`
        // hash keys
        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce].iter() {
            let subscribe_topic = SubscribeTopic {
                topic_path: topic.clone(),
                qos: *qos,
            };

            for client in self.get_subscribed_clients(subscribe_topic)? {
                let publish = client.publish_packet(&topic, *qos, payload.clone(), false, false);
                let packet = Packet::Publish(publish.clone());

                match *qos {
                    QoS::AtLeastOnce => client.store_outgoing_publish(publish),
                    QoS::ExactlyOnce => unimplemented!(),
                    _ => (),
                }

                // forward to eventloop only when client status is Connected
                if let ConnectionStatus::Connected = client.status() {
                    client.send(packet)
                }
            }
        }
        Ok(())
    }

    pub fn handle_publish(&mut self, mut publish: Publish) -> Result<()> {
        let pkid = publish.pkid;
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
                if pkid.is_some() {
                    // we should fwd only qos1 packets to all the subscribers (any qos) at this point
                    self.forward_to_subscribers(publish)?;
                } else {
                    error!("Ignoring publish packet. No pkid for QoS1 packet");
                }
            }
            QoS::ExactlyOnce => unimplemented!(),
        };

        Ok(())
    }
}

fn gen_client_id() -> String {
    let random: String = rand::thread_rng().gen_ascii_chars().take(7).collect();
    format!("rumqttd-{}", random)
}

impl Debug for Broker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}\n{:#?}\n{:#?}", self.clients, self.subscriptions, self.state)
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::rc::Rc;

    use super::Broker;
    use client::{Client, ConnectionStatus};
    use futures::sync::mpsc::{self, Receiver};
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

        let broker = Rc::new(RefCell::new(Broker::new()));

        {
            let mut broker = broker.borrow_mut();
            // add c1 to to s1, s2, s3 & s4
            broker.add_subscription_client(s1.clone(), c1.clone()).unwrap();
            broker.add_subscription_client(s1.clone(), c2.clone()).unwrap();
        }

        let broker_alias = broker.clone();

        {
            let mut broker_alias = broker_alias.borrow_mut();
            // remove c1 & c2 from all subscriptions and verify clients
            broker_alias.remove_client(&c1.id, 0).unwrap();
            broker_alias.remove_client(&c2.id, 0).unwrap();
        }

        for s in [s1].iter() {
            let mut broker = broker.borrow_mut();
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

        let mut broker = Broker::new();

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

        let mut broker = Broker::new();

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

        let mut broker = Broker::new();
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
            protocol: Protocol::new("MQTT", 4).unwrap(),
            keep_alive: 100,
            client_id: "session-test".to_owned(),
            clean_session: false,
            last_will: None,
            username: None,
            password: None,
        };

        let mut broker = Broker::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8045);
        let _ = broker.handle_connect(connect, addr).unwrap();
    }
}
