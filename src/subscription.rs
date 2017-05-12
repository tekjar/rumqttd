use std::collections::HashMap;

use client::Client;
use mqtt3::{TopicPath, SubscribeTopic, ToTopicPath};
use error::Result;

// NOTE: split subscription list into concrete & and wild card subscriptions
// all concrete subscription clients could be fetched in O(1)~
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

    /// Get the list of subscribed clients for a given concrete subscription topic
    pub fn get_subscribed_clients(&mut self, topic: SubscribeTopic) -> Result<Vec<Client>> {
        let topic_path = TopicPath::from_str(topic.topic_path.clone())?;
        // subscription topic should only have concrete topic path
        let _ = topic_path.to_topic_name()?;
        
        let mut all_clients = vec![];

        // O(1) matches from concrete hashmap
        if let Some(clients) = self.concrete.get(&topic) {
            all_clients.extend(clients.clone());
        }

        for (subscription, clients) in self.wild.iter() {
            let wild_subscription_topic = TopicPath::from_str(subscription.topic_path.clone())?;
            if wild_subscription_topic.is_match(&topic_path) {
                all_clients.extend(clients.clone());
            }
        }

        Ok(all_clients)
    }
}

#[cfg(test)]
mod test {
    use super::SubscriptionList;

    
}