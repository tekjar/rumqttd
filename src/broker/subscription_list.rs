use std::collections::HashMap;
use std::mem;

use crate::client::{Client, ConnectionStatus};
use crate::error::Result;
use rumq_core::*;

// NOTE: split subscription list into concrete & and wild card subscriptions
// all concrete subscription clients could be fetched in O(1)~

#[derive(Debug)]
pub struct SubscriptionList {
    concrete: HashMap<String, Vec<Client>>,
    wild: HashMap<String, Vec<Client>>,
}

impl SubscriptionList {
    pub fn new() -> Self {
        SubscriptionList {
            concrete: HashMap::new(),
            wild: HashMap::new(),
        }
    }

    pub fn add_subscription(&mut self, topic: SubscribeTopic, client: Client) -> Result<()> {
        let topic_path = topic.topic_path.clone();

        let clients = self.concrete.entry(topic_path).or_insert(Vec::new());
        // add client to a subscription only if it doesn't already exist or
        // else replace the existing one
        if let Some(index) = clients.iter().position(|v| v.id == client.id) {
            clients[index] = client;
        } else {
            clients.push(client);
        }
        Ok(())
    }

    pub fn remove_subscription_client(&mut self, topic: SubscribeTopic, id: &str) -> Result<()> {
        let topic_path = topic.topic_path.clone();
        if let Some(clients) = self.concrete.get_mut(&topic_path) {
            if let Some(index) = clients.iter().position(|v| v.id == id) {
                clients.remove(index);
            }
        }
        Ok(())
    }

    // replace an existing client in list by matching id of 'client'. Unlike replacement that
    // `add_subscription` does, this preserves client state but changes other parts like 'tx'
    // to send n/w write requests to correct connection in the event loop
    pub fn replace_client(&mut self, client: Client) -> Result<()> {
        let id = client.id.clone();

        for clients in self.concrete.values_mut() {
            if let Some(index) = clients.iter().position(|v| v.id == id) {
                clients[index].set_status(ConnectionStatus::Connected);
                let _ = mem::replace(&mut clients[index].uid, client.uid);
                let _ = mem::replace(&mut clients[index].addr, client.addr);
                let _ = mem::replace(&mut clients[index].tx, client.tx.clone());
                let _ = mem::replace(&mut clients[index].keep_alive, client.keep_alive);
                let _ = mem::replace(&mut clients[index].clean_session, client.clean_session);
            }
        }

        for clients in self.wild.values_mut() {
            if let Some(index) = clients.iter().position(|v| v.id == id) {
                let _ = mem::replace(&mut clients[index].uid, client.uid);
                let _ = mem::replace(&mut clients[index].addr, client.addr);
                let _ = mem::replace(&mut clients[index].tx, client.tx.clone());
                let _ = mem::replace(&mut clients[index].keep_alive, client.keep_alive);
                let _ = mem::replace(&mut clients[index].clean_session, client.clean_session);
            }
        }

        Ok(())
    }

    /// Remove a client from all the subscriptions
    pub fn remove_client(&mut self, id: &str, uid: u8) -> Result<()> {
        for clients in self.concrete.values_mut() {
            if let Some(index) = clients.iter().position(|v| v.id == id && v.uid == uid) {
                clients.remove(index);
            }
        }

        for clients in self.wild.values_mut() {
            if let Some(index) = clients.iter().position(|v| v.id == id && v.uid == uid) {
                clients.remove(index);
            }
        }

        Ok(())
    }

    /// For a given concrete topic, match topics & return list of subscribed clients
    pub fn get_subscribed_clients(&mut self, topic: SubscribeTopic) -> Result<Vec<Client>> {
        let topic_path = topic.topic_path.clone();
        let qos = topic.qos;

        let mut all_clients = vec![];

        // O(1) matches from concrete hashmap
        if let Some(clients) = self.concrete.get(&topic_path) {
            all_clients.extend(clients.clone());
        }

        Ok(all_clients)
    }
}

#[cfg(test)]
mod test {
    use super::SubscriptionList;
    use client::Client;
    use futures::sync::mpsc::{self, Receiver};
    use mqtt3::*;

    fn mock_client(id: &str, uid: u8) -> (Client, Receiver<Packet>) {
        let (tx, rx) = mpsc::channel::<Packet>(8);
        let mut client = Client::new(id, "127.0.0.1:80".parse().unwrap(), tx);
        client.uid = uid;
        (client, rx)
    }

    #[test]
    fn add_clients_to_subscriptions_and_verify_wildcard_and_concrete_counts() {
        let (c1, ..) = mock_client("mock-client-1", 0);
        let (c2, ..) = mock_client("mock-client-2", 0);
        let (c3, ..) = mock_client("mock-client-2", 10);

        let s1 = SubscribeTopic {
            topic_path: "hello/mqtt/rumqttd".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let s2 = SubscribeTopic {
            topic_path: "hello/+/rumqttd".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let mut subscription_list = SubscriptionList::new();
        subscription_list.add_subscription(s1.clone(), c1).unwrap();
        // c2 & c3's client id is same, so previous one should be replaced
        subscription_list.add_subscription(s2.clone(), c2).unwrap();
        subscription_list.add_subscription(s2.clone(), c3).unwrap();

        assert_eq!(1, subscription_list.concrete.get(&s1).unwrap().len());
        assert_eq!(1, subscription_list.wild.get(&s2).unwrap().len());
        assert_eq!(10, subscription_list.wild.get(&s2).unwrap()[0].uid);
    }

    #[test]
    fn remove_clients_from_subscription_list_and_verify_wild_and_concrete_counts() {
        let (c1, ..) = mock_client("mock-client-1", 0);
        let (c2, ..) = mock_client("mock-client-2", 0);
        let (c3, ..) = mock_client("mock-client-2", 1);

        let s1 = SubscribeTopic {
            topic_path: "hello/mqtt/rumqttd".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let s2 = SubscribeTopic {
            topic_path: "hello/+/rumqttd".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let mut subscription_list = SubscriptionList::new();
        subscription_list.add_subscription(s1.clone(), c1).unwrap();
        subscription_list.add_subscription(s2.clone(), c2).unwrap();
        subscription_list.add_subscription(s2.clone(), c3).unwrap();

        subscription_list.remove_client("mock-client-1", 0).unwrap();
        subscription_list.remove_client("mock-client-2", 1).unwrap();

        assert_eq!(0, subscription_list.concrete.get(&s1).unwrap().len());
        assert_eq!(0, subscription_list.wild.get(&s2).unwrap().len());
    }

    #[test]
    fn remove_non_existant_clients_from_subscription_list_and_verify_counts() {
        let (c1, ..) = mock_client("mock-client-1", 0);
        let (c2, ..) = mock_client("mock-client-1", 1);

        let (c3, ..) = mock_client("mock-client-2", 0);
        let (c4, ..) = mock_client("mock-client-2", 1);

        let s1 = SubscribeTopic {
            topic_path: "hello/mqtt/rumqttd".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let s2 = SubscribeTopic {
            topic_path: "hello/+/rumqttd".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let mut subscription_list = SubscriptionList::new();
        subscription_list.add_subscription(s1.clone(), c1).unwrap();
        // will replace the old client
        subscription_list.add_subscription(s1.clone(), c2).unwrap();
        subscription_list.add_subscription(s2.clone(), c3).unwrap();
        // will replace the old client
        subscription_list.add_subscription(s2.clone(), c4).unwrap();

        // remove clients with wrong uids
        subscription_list.remove_client("mock-client-1", 0).unwrap();
        subscription_list.remove_client("mock-client-2", 0).unwrap();

        assert_eq!(1, subscription_list.concrete.get(&s1).unwrap().len());
        assert_eq!(1, subscription_list.wild.get(&s2).unwrap().len());

        // remove clients with correct uids
        subscription_list.remove_client("mock-client-1", 1).unwrap();
        subscription_list.remove_client("mock-client-2", 1).unwrap();

        assert_eq!(0, subscription_list.concrete.get(&s1).unwrap().len());
        assert_eq!(0, subscription_list.wild.get(&s2).unwrap().len());
    }

    #[test]
    fn verify_uid_after_replacing_an_existing_client() {
        let (c2, ..) = mock_client("mock-client-2", 0);
        let (c3, ..) = mock_client("mock-client-2", 10);

        let s2 = SubscribeTopic {
            topic_path: "hello/+/rumqttd".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let mut subscription_list = SubscriptionList::new();
        subscription_list.add_subscription(s2.clone(), c2).unwrap();
        subscription_list.replace_client(c3).unwrap();

        assert_eq!(10, subscription_list.wild.get(&s2).unwrap()[0].uid);
    }

    #[test]
    fn match_topics_with_wild_card_and_concrete_subscription_clients() {
        let (c1, ..) = mock_client("mock-client-1", 0);
        let (c2, ..) = mock_client("mock-client-2", 0);
        let (c3, ..) = mock_client("mock-client-3", 0);

        let s1 = SubscribeTopic {
            topic_path: "hello/mqtt/rumqttd".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let s2 = SubscribeTopic {
            topic_path: "hello/+/rumqttd".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let s3 = SubscribeTopic {
            topic_path: "hello/#".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let mut sub_list = SubscriptionList::new();
        sub_list.add_subscription(s1, c1).unwrap();
        sub_list.add_subscription(s2, c2).unwrap();
        sub_list.add_subscription(s3, c3).unwrap();

        let s4 = SubscribeTopic {
            topic_path: "hello/mqtt/rumqttd".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let clients = sub_list.get_subscribed_clients(s4).unwrap();

        assert_eq!(clients.len(), 3);

        assert!(clients.iter().any(|e| e.id == "mock-client-1"));
        assert!(clients.iter().any(|e| e.id == "mock-client-2"));
        assert!(clients.iter().any(|e| e.id == "mock-client-3"));
    }

    /// subscription("a/+/c", atmostonce) shouldn't match with ("a/b/c", atleastonce)
    #[test]
    fn dont_match_subscription_with_matching_topic_but_nonmatching_qos() {
        let (c1, ..) = mock_client("mock-client-1", 0);
        let s1 = SubscribeTopic {
            topic_path: "hello/+/rumqttd".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let mut sub_list = SubscriptionList::new();
        sub_list.add_subscription(s1, c1).unwrap();

        let s2 = SubscribeTopic {
            topic_path: "hello/mqtt/rumqttd".to_owned(),
            qos: QoS::AtLeastOnce,
        };

        let clients = sub_list.get_subscribed_clients(s2).unwrap();
        assert_eq!(clients.len(), 0);
    }

    /// subscriptions like 'a/#/c' shouldn't be allowed
    #[test]
    #[should_panic]
    fn dont_allow_subscription_with_multiwildcard_inbetween_topic() {
        let (c1, ..) = mock_client("mock-client-1", 0);
        let s1 = SubscribeTopic {
            topic_path: "hello/#/rumqttd".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let mut sub_list = SubscriptionList::new();
        sub_list.add_subscription(s1, c1).unwrap();
    }
}
