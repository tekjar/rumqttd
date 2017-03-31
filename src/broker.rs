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

use mqtt3::{Packet, Connack, ConnectReturnCode};

use codec::MqttCodec;
use client::Client;

#[derive(Clone)]
pub struct Broker {
    /// All the active clients
    clients: Rc<RefCell<HashMap<String, Client>>>,
    /// Subscriptions mapped to interested clients
    subscriptions: Rc<RefCell<HashMap<String, Vec<String>>>>,
}

impl Broker {
    pub fn new() -> Self {
        Broker {
            clients: Rc::new(RefCell::new(HashMap::new())),
            subscriptions: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub fn add(&self, client: Client) {
        self.clients
            .borrow_mut()
            .insert(client.id.clone(), client);
    }

    pub fn remove(&self, id: &str) -> Option<Client> {
        self.clients.borrow_mut().remove(id)
    }
}

impl Debug for Broker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.clients.borrow_mut())
    }
}


pub fn start() -> Result<()> {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let address = "0.0.0.0:1883".parse().unwrap();
    let listener = TcpListener::bind(&address, &core.handle()).unwrap();

    let broker = Broker::new();

    let welcomes = listener.incoming().and_then(|(socket, addr)| {
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
                    let (tx, rx) = mpsc::channel(8);

                    let client = Client::new(&c.client_id, addr, tx);
                    broker.add(client);
                    println!("{:?}", broker);

                    Ok((framed, rx))
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
            // handle each connections n/w send and recv here
            if let Some((framed, rx)) = handshake {

                let (sender, receiver) = framed.split();

                let connack = Packet::Connack(Connack {
                                                  session_present: false,
                                                  code: ConnectReturnCode::Accepted,
                                              });

                let sender = sender.send(connack).wait();

                // current connections incoming n/w packets
                let rx_future = receiver
                    .for_each(|msg| {
                                  println!("Incoming packet: {:?}", msg);
                                  Ok(())
                              })
                    .then(|_| Ok(()));


                //FIND: what happens to rx_future when socket disconnects
                handle.spawn(rx_future);

                // Sender implements Sink which allows us to
                // send messages to the underlying socket connection.
                // let tx_future = rx.for_each(|r| {
                //     match r {
                //         Packet::Publish(m) => Packet::Publish(m)),
                //         _ => panic!("Misc"),
                //     }
                // }).and_then(|p| p)
                //   .forward(sender);
            }
            Ok(())
        });

    core.run(server).unwrap();
    Ok(())
}
