use std::collections::HashMap;

use client::Client;
use mqtt3::{TopicPath, SubscribeTopic};
use error::Result;

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

    fn match_with_wild_topic(concrete: String, wild: String) -> bool {
        true
    }

    /// Get the list of clients for a given subscription
    pub fn get_subscribed_clients(&mut self, topic: SubscribeTopic) -> Result<Vec<Client>> {
        let topic_path = TopicPath::from_str(topic.topic_path.clone())?;

        let clients = if topic_path.wildcards {
            if let Some(v) = self.wild.get(&topic) {
                v.clone()
            } else {
                vec![]
            }
        } else {
            if let Some(v) = self.concrete.get(&topic) {
                v.clone()
            } else {
                vec![]
            }
        };

        Ok(clients)
    }
}