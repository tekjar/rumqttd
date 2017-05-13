use std::collections::HashMap;

use client::Client;
use mqtt3::{TopicPath, SubscribeTopic, ToTopicPath};
use error::Result;

// NOTE: split subscription list into concrete & and wild card subscriptions
// all concrete subscription clients could be fetched in O(1)~

#[derive(Debug)]
pub struct SubscriptionList {
    concrete: HashMap<SubscribeTopic, Vec<Client>>,
    wild: HashMap<SubscribeTopic, Vec<Client>>,
}

impl SubscriptionList {
    pub fn new() -> Self {
        SubscriptionList {
            concrete: HashMap::new(),
            wild: HashMap::new(),
        }
    }

    pub fn add_subscription(&mut self, topic: SubscribeTopic, client: Client) -> Result<()> {
        let topic_path = TopicPath::from_str(topic.topic_path.clone())?;

        if topic_path.wildcards {
            let clients = self.wild.entry(topic).or_insert(Vec::new());
            // add client to a subscription only if it doesn't already exist or
            // else replace the existing one
            if let Some(index) = clients.iter().position(|v| v.id == client.id) {
                clients.insert(index, client);
            } else {
                clients.push(client);
            }
        } else {
            let clients = self.concrete.entry(topic).or_insert(Vec::new());
            // add client to a subscription only if it doesn't already exist or
            // else replace the existing one
            if let Some(index) = clients.iter().position(|v| v.id == client.id) {
                clients.insert(index, client);
            } else {
                clients.push(client);
            }
        }

        Ok(())
    }

    /// Remove a client from a subscription
    pub fn remove_subscription_client(&mut self, topic: SubscribeTopic, id: &str) -> Result<()> {
        let topic_path = TopicPath::from_str(topic.topic_path.clone())?;

        if topic_path.wildcards {
            if let Some(clients) = self.wild.get_mut(&topic) {
                if let Some(index) = clients.iter().position(|v| v.id == id) {
                    clients.remove(index);
                }
            }
        } else {
            if let Some(clients) = self.concrete.get_mut(&topic) {
                if let Some(index) = clients.iter().position(|v| v.id == id) {
                    clients.remove(index);
                }
            }
        }
        Ok(())
    }

    /// Remove a client from all the subscriptions
    pub fn remove_client(&mut self, id: &str) -> Result<()> {
        for clients in self.concrete.values_mut() {
            if let Some(index) = clients.iter().position(|v| v.id == id) {
                clients.remove(index);
            }
        }

        for clients in self.wild.values_mut() {
            if let Some(index) = clients.iter().position(|v| v.id == id) {
                clients.remove(index);
            }
        }

        Ok(())
    }

    /// Get the list of subscribed clients for a given concrete subscription topic
    pub fn get_subscribed_clients(&mut self, topic: SubscribeTopic) -> Result<Vec<Client>> {
        let topic_path = TopicPath::from_str(topic.topic_path.clone())?;
        let qos = topic.qos;

        // subscription topic should only have concrete topic path
        let _ = topic_path.to_topic_name()?;
        
        let mut all_clients = vec![];

        // O(1) matches from concrete hashmap
        if let Some(clients) = self.concrete.get(&topic) {
            all_clients.extend(clients.clone());
        }

        for (subscription, clients) in self.wild.iter() {
            let wild_subscription_topic = TopicPath::from_str(subscription.topic_path.clone())?;
            let wild_subscription_qos = subscription.qos;

            if wild_subscription_qos == qos && wild_subscription_topic.is_match(&topic_path) {
                all_clients.extend(clients.clone());
            }
        }

        Ok(all_clients)
    }
}

#[cfg(test)]
mod test {
    use futures::sync::mpsc::{self, Receiver};
    use client::Client;
    use super::SubscriptionList;
    use mqtt3::*;

    fn mock_client(id: &str) -> (Client, Receiver<Packet>) {
        let (tx, rx) = mpsc::channel::<Packet>(8);
        (Client::new(id, "127.0.0.1:80".parse().unwrap(), tx), rx)
    }

    #[test]
    fn match_topics_with_wild_card_and_concrete_subscription_clients() {
        let (c1, ..) = mock_client("mock-client-1");
        let (c2, ..) = mock_client("mock-client-2");
        let (c3, ..) = mock_client("mock-client-3");

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
    fn dont_match_subscription_with_matching_topic_but_nonmatching_qos(){
        let (c1, ..) = mock_client("mock-client-1");
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
        let (c1, ..) = mock_client("mock-client-1");
        let s1 = SubscribeTopic {
            topic_path: "hello/#/rumqttd".to_owned(),
            qos: QoS::AtMostOnce,
        };

        let mut sub_list = SubscriptionList::new();
        sub_list.add_subscription(s1, c1).unwrap();
    }

}