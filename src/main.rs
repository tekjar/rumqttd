extern crate mqtt3;
extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_timer;
extern crate bytes;
#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate slog_async;
#[macro_use]
extern crate quick_error;

pub mod error;
pub mod codec;
pub mod broker;
pub mod client;

use std::io;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

use mqtt3::*;
use tokio_core::reactor::{Core, Interval};
use tokio_core::net::TcpListener;
use tokio_io::AsyncRead;

use futures::stream::Stream;
use futures::Future;
use futures::sync::mpsc;

use slog::{Logger, Drain};

use client::Client;
use broker::Broker;
use codec::MqttCodec;
use error::Error;

fn main() {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let address = "0.0.0.0:1883".parse().unwrap();

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = Logger::root(Arc::new(drain), o!("version" => env!("CARGO_PKG_VERSION")));

    let listener = TcpListener::bind(&address, &core.handle()).unwrap();

    let broker = Broker::new();

    let welcomes = listener.incoming()
                           .and_then(|(socket, addr)| {
        let framed = socket.framed(MqttCodec);

        let broker = broker.clone();

        // Creates a 'Self' from stream, whose error match to that of and_then's closure
        let handshake = framed.into_future()
                                  .map_err(|(err, _)| err) // for accept errors, get error and discard the stream
                                  .and_then(move |(packet,framed)| { // only accepted connections from here

                let broker = broker.clone();

                if let Some(Packet::Connect(c)) = packet {
                    // TODO: Do connect packet validation here
                    let (tx, rx) = mpsc::channel::<Packet>(100);

                    let client = Client::new(&c.client_id, addr, tx.clone());
                    broker.add_client(client.clone());

                    Ok((framed, client, rx))
                } else {
                    Err(io::Error::new(io::ErrorKind::Other, "Invalid Handshake Packet"))
                }
            });

        handshake

    });

    let server = welcomes.map(|w| Some(w))
                         .or_else(|e| {
                                      error!(logger, "{:?}", e);
                                      Ok::<_, ()>(None)
                                  })
                         .for_each(|handshake| {

        let broker = broker.clone();
        let broker2 = broker.clone();

        // handle each connections n/w send and recv here
        if let Some((framed, client, rx)) = handshake {
            let id = client.id.clone();
            let (sender, receiver) = framed.split();

            let connack = Packet::Connack(Connack {
                                              session_present: false,
                                              code: ConnectReturnCode::Accepted,
                                          });

            let _ = client.send(connack);

            // current connections incoming n/w packets
            let rx_future = receiver.for_each(move |msg| {
                match msg {
                    Packet::Publish(p) => broker.handle_publish(p, &client),
                    Packet::Subscribe(s) => broker.handle_subscribe(s, &client),
                    Packet::Puback(pkid) => broker.handle_puback(pkid, &client),
                    Packet::Pubrec(pkid) => broker.handle_pubrec(pkid, &client),
                    Packet::Pubrel(pkid) => broker.handle_pubrel(pkid, &client),
                    Packet::Pubcomp(pkid) => broker.handle_pubcomp(pkid, &client),
                    Packet::Pingreq => broker.handle_pingreq(&client),
                    _ => panic!("Incoming Misc: {:?}", msg),
                }
                Ok(())
            }).then(move |e| {
                println!("{:?}", e);
                Ok::<_, ()>(())
            });

            let interval = Interval::new(Duration::new(5, 0), &handle).unwrap();

            let timer_future =
                interval.for_each(|_| { return Err(io::Error::new(ErrorKind::Other, "Ping Timer Error")); })
                        .then(|_| Ok(()));

            let rx_future = timer_future.select(rx_future);

            // current connections outgoing n/w packets
            let tx_future = rx.map_err(|_| Error::Other)
                              .map(|r| match r {
                                       Packet::Publish(p) => Packet::Publish(p),
                                       Packet::Connack(c) => Packet::Connack(c),
                                       Packet::Suback(sa) => Packet::Suback(sa),
                                       Packet::Puback(pa) => Packet::Puback(pa),
                                       Packet::Pubrec(prec) => Packet::Pubrec(prec),
                                       Packet::Pubrel(prel) => Packet::Pubrel(prel),
                                       Packet::Pubcomp(pc) => Packet::Pubcomp(pc),
                                       Packet::Pingresp => Packet::Pingresp,
                                       _ => panic!("Outgoing Misc: {:?}", r),
                                   })
                              .forward(sender)
                              .then(move |_| -> ::std::result::Result<(), ()> {
                                        // forward error. n/w disconnections.
                                        println!("%%% RX DISCONNECTION");
                                        Ok(())
                                    });

            let rx_future = rx_future.then(|_| Ok(()));

            let connection = rx_future.select(tx_future);
            let connection = connection.then(move |_| {
                                                 println!("Disconnecting client: {:?}", id);
                                                 broker2.remove_client(&id);
                                                 Ok(())
                                             });

            handle.spawn(connection);
        }
        Ok(())
    });

    core.run(server).unwrap();
}
