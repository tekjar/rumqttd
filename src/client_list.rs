use std::collections::{HashMap, VecDeque};

use client::Client;
use error::Result;

#[derive(Debug)]
pub struct ClientList {
    /// map of id and clients.
    // NOTE: Item is a vec instead of a Client because, while adding
    // new client with same id, old client should be disconnected to
    // prevent tcp halfopen connections. since the the final disconnect
    // handling happens in main through a channel, to prevent removal of
    // the new client (incase we replaced using insert() when using 
    // Hashmap<String, Client>) we use a vec where old client will be remove first
    list: HashMap<String, VecDeque<Client>>,
}

impl ClientList {
    pub fn new() -> ClientList {
        ClientList {
            list: HashMap::new(),
        }
    }

    pub fn add_client(&mut self, client: Client) -> Result<bool> {
        if let Some(clients) = self.list.get_mut(&client.id) {
            clients.push_back(client);
            return Ok(true)
        }

        let mut clients = VecDeque::new();
        let id = client.id.clone();
        clients.push_back(client);
        self.list.insert(id, clients);
        
        Ok(false)
    }

    pub fn remove_client(&mut self, id: &str) -> Result<()> {
        if let Some(clients) = self.list.get_mut(id) {
            clients.pop_front();
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use futures::sync::mpsc::{self, Receiver};
    use client::Client;
    use super::ClientList;
    use mqtt3::*;

    fn mock_client(id: &str) -> (Client, Receiver<Packet>) {
        let (tx, rx) = mpsc::channel::<Packet>(8);
        (Client::new(id, "127.0.0.1:80".parse().unwrap(), tx), rx)
    }

    #[test]
    fn add_clients_to_list() {
        let (c1, ..) = mock_client("mock-client-1");
        let (c2, ..) = mock_client("mock-client-2");
        let (c3, ..) = mock_client("mock-client-2");

        let mut client_list = ClientList::new();
        let r = client_list.add_client(c1);
        assert_eq!(false, r.unwrap());

        let r = client_list.add_client(c2);
        assert_eq!(false, r.unwrap());

        let r = client_list.add_client(c3);
        assert_eq!(true, r.unwrap());

        assert_eq!(1, client_list.list.get("mock-client-1").unwrap().len());
        assert_eq!(2, client_list.list.get("mock-client-2").unwrap().len());
    }

    #[test]
    fn remove_clients_from_list() {
        let (c1, ..) = mock_client("mock-client-1");
        let (c2, ..) = mock_client("mock-client-2");
        let (c3, ..) = mock_client("mock-client-2");
        let (c4, ..) = mock_client("mock-client-2");

        let mut client_list = ClientList::new();
        let _ = client_list.add_client(c1);
        let _ = client_list.add_client(c2);
        let _ = client_list.add_client(c3);
        let _ = client_list.add_client(c4);

        client_list.remove_client("mock-client-2").unwrap();
        client_list.remove_client("mock-client-2").unwrap();

        assert_eq!(1, client_list.list.get("mock-client-1").unwrap().len());
        assert_eq!(1, client_list.list.get("mock-client-2").unwrap().len());
    }
}