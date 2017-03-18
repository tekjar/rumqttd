use error::{Result, Error};

use std::cell::RefCell;
use std::rc::Rc;
use std::net::SocketAddr;
use std::collections::HashMap;
use std::io;

use futures::stream::Stream;
use futures::Sink;
use futures::Future;

use tokio_core::reactor::Core;
use tokio_core::net::TcpListener;
use tokio_io::AsyncRead;

use mqtt3::{Packet, Connack, ConnectReturnCode};

use codec::MqttCodec;
use client::Client;

pub struct Broker {
    clients: Rc<RefCell<HashMap<SocketAddr, Client>>>,
}

impl Broker {
    pub fn new() -> Self {
        Broker { clients: Rc::new(RefCell::new(HashMap::new())) }
    }

    pub fn start(&mut self) -> Result<()> {
        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let address = "0.0.0.0:1883".parse().unwrap();
        let listener = TcpListener::bind(&address, &core.handle()).unwrap();

        let welcomes = listener.incoming().and_then(|(socket, addr)| {
            let framed = socket.framed(MqttCodec);

            let handshake = framed.into_future()
                                  .map_err(|(err, _)| err)
                                  .and_then(|(packet, framed)|{
                                      if let Some(Packet::Connect(c)) = packet {
                                          println!("{:?}", c);
                                          Ok(framed)
                                      } else {
                                        Err(io::Error::new(io::ErrorKind::Other, "invalid handshake"))
                                      }
                                  });
            handshake
        });

        let server = welcomes.for_each(|framed| {
            
            let (sender, receiver) = framed.split();

            let connack = Packet::Connack(Connack{session_present: false, code: ConnectReturnCode::Accepted}); 
            let sender = sender.send(connack).wait();
            
            let rx_future = receiver.for_each(|msg| {
                println!("{:?}", msg);
                Ok(())
            }).then(|_| Ok(()));

            handle.spawn(rx_future);
            Ok(())
        });

        core.run(server)?;
        Ok(())
    }

    pub fn add(&self, addr: SocketAddr, client: Client) {
        self.clients.borrow_mut().insert(addr, client);
    }

    pub fn remove(&self, addr: &SocketAddr) -> Option<Client> {
        self.clients.borrow_mut().remove(addr)
    }
}
