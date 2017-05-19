use std::collections::{HashMap};
use std::mem;

use mqtt3::{Packet, PacketIdentifier};

use client::{Client, ConnectionStatus};
use error::{Result, Error};

#[derive(Debug)]
pub struct ClientList {
    /// map of id and clients.
    // NOTE: Item is a vec instead of a Client because, while adding
    // new client with same id, old client should be disconnected to
    // prevent tcp halfopen connections. since the the final disconnect
    // handling happens in main through a channel, to prevent removal of
    // the new client (incase we replaced using insert() when using 
    // Hashmap<String, Client>) we use a vec where old client will be remove first
    list: HashMap<String, Client>,
}

impl ClientList {
    pub fn new() -> ClientList {
        ClientList {
            list: HashMap::new(),
        }
    }

    pub fn add_client(&mut self, client: Client) -> Result<()> {
        if let Some(_) = self.list.get_mut(&client.id) {
            return Err(Error::ClientIdExists)
        }

        let id = client.id.clone();
        self.list.insert(id, client);
        
        Ok(())
    }

    // this preserves client state but changes other parts like 'tx'
    // to send n/w write requests to correct connection in the event loop
    pub fn replace_client(&mut self, client: Client) -> Result<()> {
        if let Some(c) = self.list.get_mut(&client.id) {
            let _ = mem::replace(&mut c.uid, client.uid);
            let _ = mem::replace(&mut c.addr, client.addr);
            let _ = mem::replace(&mut c.tx, client.tx);
            let _ = mem::replace(&mut c.keep_alive, client.keep_alive);
            let _ = mem::replace(&mut c.clean_session, client.clean_session);
        }
        Ok(())
    }

    pub fn remove_client(&mut self, id: &str, uid: u8) -> Result<()> {
        let mut should_remove = false;

        if let Some(client) = self.list.get_mut(id) {
            if client.uid == uid {
                should_remove = true;
            }
        }

        if should_remove {
            self.list.remove(id);
        }
        Ok(())
    }

    // ask a particular client from the list to perform a send
    pub fn send(&self, id: &str, packet: Packet) -> Result<()> {
        if let Some(client) = self.list.get(id) {
            client.send(packet);
        }
        Ok(())
    }

    // Set conneciton status of client with given id 
    pub fn set_status(&self, id: &str, uid: u8, status: ConnectionStatus) -> Result<()> {
        if let Some(client) = self.list.get(id) {
            if client.uid == uid {
                client.set_status(status);
            }
            Ok(())
        } else {
            Err(Error::NoClient)
        }
    }

    // Set conneciton status of client with given id 
    pub fn status(&self, id: &str) -> Option<ConnectionStatus> {
        if let Some(client) = self.list.get(id) {
            Some(client.status())
        } else {
            None
        }
    }

    pub fn clear(&self, id: &str, uid: u8) -> Result<()> {
        if let Some(client) = self.list.get(id) {
            if client.uid == uid {
                client.clear();
            }
            Ok(())
        } else {
            Err(Error::NoClient)
        }
    }

    pub fn stats(&self, id: &str) -> Result<(ConnectionStatus, PacketIdentifier, usize, usize, usize, usize)> {
        if let Some(client) = self.list.get(id) {
            Ok(client.stats())
        } else {
            Err(Error::NoClient)
        }
    }

    // check if there are clients existing with this id & return a list of uids if so
    pub fn has_client(&self, id: &str) -> Option<u8> {
        if let Some(client) = self.list.get(id) {
            Some(client.uid)
        } else {
            None
        }
    }

    // get uid of client for given client id
    pub fn get_uid(&self, id: &str) -> Option<u8> {
        if let Some(client) = self.list.get(id) {
            return Some(client.uid)
        }
        None
    }
}

#[cfg(test)]
mod test {
    use futures::sync::mpsc::{self, Receiver};
    use client::Client;
    use super::ClientList;
    use mqtt3::*;

    fn mock_client(id: &str, uid: u8) -> (Client, Receiver<Packet>) {
        let (tx, rx) = mpsc::channel::<Packet>(8);
        let mut client = Client::new(id, "127.0.0.1:80".parse().unwrap(), tx);
        client.uid = uid;
        (client, rx)
    }

    #[test]
    fn add_clients_to_list() {
        let (c1, ..) = mock_client("mock-client-1", 0);
        let (c2, ..) = mock_client("mock-client-2", 0);
        let (c3, ..) = mock_client("mock-client-2", 10);

        let mut client_list = ClientList::new();
        let r = client_list.add_client(c1);
        assert_eq!((), r.unwrap());

        let r = client_list.add_client(c2);
        assert_eq!((), r.unwrap());

        let r = client_list.add_client(c3);
        assert_eq!(true, r.is_err());
    }

    #[test]
    fn remove_clients_from_list() {
        let (c1, ..) = mock_client("mock-client-1", 0);
        let (c2, ..) = mock_client("mock-client-2", 3);

        let mut client_list = ClientList::new();
        let _ = client_list.add_client(c1);
        let _ = client_list.add_client(c2);

        client_list.remove_client("mock-client-1", 0).unwrap();
        client_list.remove_client("mock-client-2", 3).unwrap();

        assert_eq!(false, client_list.list.contains_key("mock-client-1"));
        assert_eq!(false, client_list.list.contains_key("mock-client-2"));
    }

    #[test]
    fn remove_non_existant_uid_clients_from_list() {
        let (c1, ..) = mock_client("mock-client-1", 1);

        let mut client_list = ClientList::new();
        let _ = client_list.add_client(c1);

        client_list.remove_client("mock-client-1", 0).unwrap();
        assert_eq!(true, client_list.list.contains_key("mock-client-1"));

        client_list.remove_client("mock-client-1", 1).unwrap();
        assert_eq!(false, client_list.list.contains_key("mock-client-2"));
    }

    #[test]
    fn verify_uid_after_replacing_an_existing_client() {
        let (c2, ..) = mock_client("mock-client-2", 0);
        let (c3, ..) = mock_client("mock-client-2", 10);

        let mut client_list = ClientList::new();
        client_list.add_client(c2).unwrap();
        client_list.replace_client(c3).unwrap();

        assert_eq!(10, client_list.list.get("mock-client-2").unwrap().uid);
    }
}