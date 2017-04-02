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
    /// All the active clients
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
        let mut subscriptions = self.subscriptions.borrow_mut();

        if let Some(v) = subscriptions.get(&topic) {
            v.clone()
        } else {
            vec![]
        }
    }

    pub fn remove(&self, id: &str) -> Option<Client> {
        self.clients.borrow_mut().remove(id)
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

            // and_then<F, B>(self, f: F) -> AndThen<Self, B, F>
            // where F: FnOnce(Self::Item) -> B,
            //       B: IntoFuture<Error=Self::Error>, // Error of value returned by 'F' and Error of Self should match
            //       Self: Sized

            // => If Self resolves to Ok(_), Execute 'F' with '_'

            // AndThen<Self, B, F> => F: FnOnce(Self::Item) -> B, B: IntoFuture<Error=Self::Error>, Self: Sized

            /// handshake = AndThen<
            ///                MapErr< Stream<Framed>, closure>, --> Self
            ///                Result<Framed, io::Error>,        --> B (Should be an IntoFuture whose error = Self's error)
            ///                closure >                         --> F (Should be which returns 'B')

            /// Creates a 'Self' from stream, whose error match to that of and_then's closure
            let handshake = framed.into_future()
                                  .map_err(|(err, _)| err) // for accept errors, get error and discard the stream
                                  .and_then(move |(packet,framed)| { // only accepted connections from here

                let broker = broker.clone();

                if let Some(Packet::Connect(c)) = packet {
                    //TODO: Do connect packet validation here
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
                            Packet::Publish(p) => {
                                let topic = p.topic_name.clone();
                                let qos = p.qos;
                                let payload = p.payload;

                                //TODO: Handle acks here

                                // publish to all the subscribers in different qos `SubscribeTopic`
                                // hash keys
                                for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce].iter() {

                                    let subscribe_topic = SubscribeTopic {
                                        topic_path: topic.clone(),
                                        qos: qos.clone(),
                                    };

                                    for mut client in broker.get_subscribed_clients(subscribe_topic) {

                                        let pkid = if *qos == QoS::AtMostOnce {
                                            None
                                        } else {
                                            Some(client.next_pkid())
                                        };

                                        let publish = Packet::Publish(Box::new(Publish {
                                                                                   dup: false,
                                                                                   qos: qos.clone(),
                                                                                   retain: false,
                                                                                   topic_name: topic.clone(),
                                                                                   pid: pkid,
                                                                                   payload: payload.clone(),
                                                                               }));

                                        client.tx.clone().send(publish).wait();
                                    }
                                }

                            }
                            Packet::Connack(c) => (),
                            Packet::Subscribe(s) => {
                                let pkid = s.pid;
                                let mut return_codes = Vec::new();

                                // Add current client's id to this subscribe topic
                                for topic in s.topics {
                                    broker.add_subscription(topic.clone(), client.clone());
                                    return_codes.push(SubscribeReturnCodes::Success(topic.qos));
                                }

                                let suback = Packet::Suback(Box::new(Suback {
                                                                         pid: pkid,
                                                                         return_codes: return_codes,
                                                                     }));
                                client.tx.clone().send(suback).wait();
                            }
                            _ => println!("Incoming Misc: {:?}", msg),
                        }
                        Ok(())
                    })
                    .then(|_| Ok(()));


                //FIND: what happens to rx_future when socket disconnects
                handle.spawn(rx_future);

                // current connections outgoing n/w packets
                let tx_future = rx.map_err(|e| Error::Other)
                    .map(|r| match r {
                             Packet::Publish(m) => Packet::Publish(m),
                             Packet::Connack(c) => Packet::Connack(c),
                             Packet::Suback(s) => Packet::Suback(s),
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
