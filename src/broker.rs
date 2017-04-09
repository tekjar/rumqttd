use error::{Result, Error};

use std::cell::RefCell;
use std::rc::Rc;

use std::collections::HashMap;
use std::io;
use std::fmt::{self, Debug};

use futures::stream::Stream;
use futures::Sink;
use futures::Future;
use futures::sync::mpsc;

use tokio_core::reactor::Core;
use tokio_core::net::TcpListener;
use tokio_io::AsyncRead;

use mqtt3::*;

use codec::MqttCodec;
use client::Client;

#[derive(Clone)]
pub struct Broker {
    /// All the active clients mapped to their IDs
    clients: Rc<RefCell<HashMap<String, Client>>>,
    /// Subscriptions mapped to interested clients
    subscriptions: Rc<RefCell<HashMap<SubscribeTopic, Vec<Client>>>>,
}

impl Broker {
    pub fn new() -> Self {
        Broker {
            clients: Rc::new(RefCell::new(HashMap::new())),
            subscriptions: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub fn add_client(&self, client: Client) {
        self.clients
            .borrow_mut()
            .insert(client.id.clone(), client);
    }

    pub fn add_subscription(&self, topic: SubscribeTopic, client: Client) {
        let mut subscriptions = self.subscriptions.borrow_mut();
        let clients = subscriptions.entry(topic).or_insert(Vec::new());

        if let Some(index) = clients.iter().position(|v| v.id == client.id) {
            clients.insert(index, client);
        } else {
            clients.push(client);
        }
    }

    pub fn get_subscribed_clients(&self, topic: SubscribeTopic) -> Vec<Client> {
        let subscriptions = self.subscriptions.borrow_mut();

        if let Some(v) = subscriptions.get(&topic) {
            v.clone()
        } else {
            vec![]
        }
    }

    pub fn remove(&self, id: &str) -> Option<Client> {
        self.clients.borrow_mut().remove(id)
    }

    pub fn handle_subscribe(&self, subscribe: Box<Subscribe>, client: &Client) {
        let pkid = subscribe.pid;
        let mut return_codes = Vec::new();

        // Add current client's id to this subscribe topic
        for topic in subscribe.topics {
            self.add_subscription(topic.clone(), client.clone());
            return_codes.push(SubscribeReturnCodes::Success(topic.qos));
        }

        let suback = client.suback_packet(pkid, return_codes);
        let packet = Packet::Suback(suback);
        client.send(packet);
    }

    pub fn handle_publish(&self, publish: Box<Publish>, client: &Client) {
        let topic = publish.topic_name.clone();
        let qos = publish.qos;
        let payload = publish.payload.clone();
        let pkid = publish.pid;

        match qos {
            QoS::AtMostOnce => (),
            // send puback for qos1 packet immediately
            QoS::AtLeastOnce => {
                if let Some(pkid) = pkid {
                    let packet = Packet::Puback(pkid);
                    client.send(packet);
                } else {
                    // error!("Ignoring publish packet. No pkid for QoS1 packet");
                    return;
                }
            }
            // save the qos2 packet and send pubrec
            QoS::ExactlyOnce => {
                if let Some(pkid) = pkid {
                    client.store_record(publish.clone());
                    let packet = Packet::Pubrec(pkid);
                    client.send(packet);
                    // we should fwd only qos1 packets to all the subscribers (any qos) at this point
                    return;
                } else {
                    // error!("Ignoring record packet. No pkid for QoS2 packet");
                    return;
                }
            }
        }

        // publish to all the subscribers in different qos `SubscribeTopic`
        // hash keys
        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce].iter() {

            let subscribe_topic = SubscribeTopic {
                topic_path: topic.clone(),
                qos: qos.clone(),
            };

            for client in self.get_subscribed_clients(subscribe_topic) {
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

    pub fn handle_puback(&self, pkid: PacketIdentifier, client: &Client) {
        client.remove_publish(pkid);
    }

    pub fn handle_pubrec(&self, pkid: PacketIdentifier, client: &Client) {
        // remove record packet from state queues
        client.remove_record(pkid);
        // record and send pubrel packet
        client.store_rel(pkid);
        let packet = Packet::Pubrel(pkid);
        client.send(packet);
    }

    pub fn handle_pubcomp(&self, pkid: PacketIdentifier, client: &Client) {
        // remove release packet from state queues
        client.remove_rel(pkid);
    }

    pub fn handle_pubrel(&self, pkid: PacketIdentifier, client: &Client) {
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

                for client in self.get_subscribed_clients(subscribe_topic) {
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
        } else {
            // error!("Dropping. Unsolicited record");
        }
    }

    pub fn handle_pingreq(&self, client: &Client) {
        let pingresp = Packet::Pingresp;
        client.send(pingresp);
    }
}

impl Debug for Broker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "{:#?}\n{:#?}",
               self.clients.borrow_mut(),
               self.subscriptions.borrow_mut())
    }
}


pub fn start() -> Result<()> {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let address = "0.0.0.0:1883".parse().unwrap();
    let listener = TcpListener::bind(&address, &core.handle()).unwrap();

    let broker = Broker::new();

    let welcomes = listener
        .incoming()
        .and_then(|(socket, addr)| {
            println!("New connection from: {:?}\n", addr);
            let framed = socket.framed(MqttCodec);

            let broker = broker.clone();

            // Creates a 'Self' from stream, whose error match to that of and_then's closure
            let handshake = framed.into_future()
                                  .map_err(|(err, _)| err) // for accept errors, get error and discard the stream
                                  .and_then(move |(packet,framed)| { // only accepted connections from here

                let broker = broker.clone();

                if let Some(Packet::Connect(c)) = packet {
                    // TODO: Do connect packet validation here
                    let (tx, rx) = mpsc::channel::<Packet>(8);

                    let client = Client::new(&c.client_id, addr, tx.clone());
                    broker.add_client(client.clone());

                    Ok((framed, client, rx))
                } else {
                    println!("Not a handshake packet");
                    Err(io::Error::new(io::ErrorKind::Other, "invalid handshake"))
                }
            });

            handshake

        });

    let server = welcomes
        .map(|w| Some(w))
        .or_else(|_| Ok::<_, ()>(None))
        .for_each(|handshake| {

            let broker = broker.clone();

            // handle each connections n/w send and recv here
            if let Some((framed, client, rx)) = handshake {

                let (sender, receiver) = framed.split();

                let connack = Packet::Connack(Connack {
                                                  session_present: false,
                                                  code: ConnectReturnCode::Accepted,
                                              });

                let connack_tx = client.tx.clone();
                let _ = connack_tx.send(connack).wait();

                // current connections incoming n/w packets
                let rx_future = receiver
                    .for_each(move |msg| {
                        match msg {
                            Packet::Publish(p) => broker.handle_publish(p, &client),
                            Packet::Subscribe(s) => broker.handle_subscribe(s, &client),
                            Packet::Puback(pkid) => broker.handle_puback(pkid, &client),
                            Packet::Pubrec(pkid) => broker.handle_pubrec(pkid, &client),
                            Packet::Pubrel(pkid) => broker.handle_pubrel(pkid, &client),
                            Packet::Pubcomp(pkid) => broker.handle_pubcomp(pkid, &client),
                            Packet::Pingreq => broker.handle_pingreq(&client),
                            _ => println!("Incoming Misc: {:?}", msg),
                        }
                        Ok(())
                    })
                    .then(|_| Ok(()));


                //FIND: what happens to rx_future when socket disconnects
                handle.spawn(rx_future);

                // current connections outgoing n/w packets
                let tx_future = rx.map_err(|_| Error::Other)
                    .map(|r| match r {
                             Packet::Publish(p) => Packet::Publish(p),
                             Packet::Connack(c) => Packet::Connack(c),
                             Packet::Suback(sa) => Packet::Suback(sa),
                             Packet::Puback(pa) => Packet::Puback(pa),
                             Packet::Pubrec(pr) => Packet::Pubrec(pr),
                             Packet::Pubrel(pr) => Packet::Pubrel(pr),
                             Packet::Pubcomp(pc) => Packet::Pubcomp(pc),
                             Packet::Pingresp => Packet::Pingresp,
                             _ => panic!("Outgoing Misc: {:?}", r),
                         })
                    .forward(sender)
                    .then(|_| Ok(()));

                handle.spawn(tx_future);
            }
            Ok(())
        });

    core.run(server).unwrap();
    Ok(())
}
