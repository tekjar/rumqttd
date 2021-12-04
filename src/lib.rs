extern crate mqtt3;
extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_timer;
extern crate bytes;
extern crate toml;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate simplelog;
extern crate chrono;
#[macro_use]
extern crate quick_error;
extern crate structopt;
#[macro_use]
extern crate structopt_derive;


mod error;
mod codec;
mod broker;
mod client;
mod conf;

use std::fs::File;
use std::io::{self, Read};
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::error::Error as StdError;
use std::cell::RefCell;
use std::rc::Rc;

use structopt::StructOpt;
use mqtt3::*;
use tokio_core::reactor::{Core, Interval};
use tokio_core::net::TcpListener;
use tokio_io::AsyncRead;

use futures::stream::Stream;
use futures::Future;

use simplelog::{Config, TermLogger, WriteLogger, CombinedLogger, LevelFilter};

use broker::Broker;
use codec::MqttCodec;
use error::Error;

// #[derive(StructOpt, Debug)]
// #[structopt(name = "Rumqttd", about = "High performance asynchronous mqtt broker")]
// pub struct CommandLine {
//     #[structopt(short = "c", help = "Rumqttd config file", default_value = "/etc/rumqttd.conf")]
//     config_path: String,
// }


// lazy_static! {
//     pub static ref CONF: conf::Rumqttd = {
//         let cl = CommandLine::from_args();

//         let mut conf = String::new();
//         let _ = File::open(cl.config_path).expect("Config Error").read_to_string(&mut conf);
//         toml::from_str::<conf::Rumqttd>(&conf).unwrap()
//     };
// }

pub fn run_with_logger(ip_addr: Ipv4Addr, port: u16) {
    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Info, Config::default()).unwrap()
        ]
    ).unwrap();


    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let address = SocketAddr::new(IpAddr::V4(ip_addr), port);
    info!("🌩️   starting broker");

    let listener = TcpListener::bind(&address, &core.handle()).unwrap();

    let broker_root = Rc::new(RefCell::new(Broker::new()));
    let broker_connections = broker_root.clone();
    let handle_inner = handle.clone();

    info!("👂🏼   listening for connections");

    let connections = listener.incoming().for_each(|(socket, addr)| {
        let framed = socket.framed(MqttCodec::new());
        info!("☄️   new tcp connection from {}", addr);
        let broker_handshake = broker_connections.clone();
        let broker_disconnect = broker_connections.clone();

        let handshake = framed.into_future()
                                  .map_err(move |(err, _)| {
                                      error!("pre handshake error = {:?}", err);
                                      err
                                  }) // for accept errors, get error and discard the stream
                                  .and_then(move |(packet,framed)| { // only accepted connections from here

                let mut broker = broker_handshake.borrow_mut();

                if let Some(Packet::Connect(c)) = packet {
                    match broker.handle_connect(c, addr) {
                        Ok((client, connack, rx)) => {
                            info!("✨   mqtt connection successful. id = {:?}", client.id);
                            Ok((framed, client, rx, connack))
                        }
                        Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.description())),
                    }
                } else {
                    Err(io::Error::new(io::ErrorKind::Other, "Invalid Handshake Packet"))
                }
        });

        // TODO: implement timeout to drop connection if there is no handshake packet after connection
        // let timeout = Timeout::new(Duration::new(3, 0), &handle_inner).unwrap();
        // let timeout = timeout.then(|_| Err(io::Error::new(io::ErrorKind::Other, "Invalid Handshake Packet")));
        // let handshake = handshake.select(timeout);

        let broker_mqtt = broker_connections.clone();
        let handle_inner = handle_inner.clone();
        let mqtt = handshake.map(|w| Some(w))
                            .or_else(move |e| {
                                         error!("{:?}", e);
                                         Ok::<_, ()>(None)
                                     })
                            .map(move |handshake| {
            
            // handle each connections n/w send and recv here.
            // each connection will have one event loop handler and lot of aliased
            // clients with shared state that can communicate with this eventloop handler
            if let Some((framed, client, rx, connack)) = handshake {
                let id: String = client.id.clone();
                let uid = client.uid;
                let clean_session = client.clean_session;
                let keep_alive = client.keep_alive;
                let client_timer = client.clone();

                let (sender, receiver) = framed.split();

                let connack = Packet::Connack(connack);

                client.send(connack);
                // send backlogs only after sending connack
                client.send_all_backlogs();

                let broker_rx = broker_mqtt.clone();
                let handle_inner = handle_inner.clone();

                // current connections incoming n/w packets
                let rx_future = receiver.or_else(|e| {
                                            error!("Receiver error = {:?}", e);
                                            Err::<_, error::Error>(e.into())
                                        })
                                        .for_each(move |msg| {
                    let mut broker = broker_rx.borrow_mut();
                    client.reset_last_control_at();
                    match msg {
                        Packet::Publish(p) => {
                            // sends acks
                            client.handle_publish(p.clone())?;
                            // forward to subscribers
                            broker.handle_publish(p)
                        }
                        Packet::Subscribe(s) => {
                            let successful_subscriptions = client.handle_subscribe(s)?;
                            broker.handle_subscribe(successful_subscriptions, &client)
                        }
                        Packet::Puback(pkid) => client.handle_puback(pkid),
                        Packet::Pubrec(pkid) => client.handle_pubrec(pkid),
                        Packet::Pubrel(pkid) => {
                            // send comp and get the record from queue
                            let record = client.handle_pubrel(pkid)?;
                            // send the record to subscribed clients
                            broker.handle_pubrel(record)
                        }
                        Packet::Pubcomp(pkid) => client.handle_pubcomp(pkid),
                        Packet::Pingreq => client.handle_pingreq(),
                        Packet::Disconnect => Err(error::Error::DisconnectPacket),
                        _ => Err(error::Error::InvalidMqttPacket),
                    }
                }).map_err(move |e| {
                    match e {
                        error::Error::DisconnectPacket => {
                            error!("received disconnect packet. error = {:?}", e);
                            Err(())
                        }
                        _ => {
                            error!("network incoming handle error = {:?}", e);
                            Ok::<_, ()>(())
                        }
                    }
                })
                .then(move |_| {
                    Ok::<_, ()>(())
                });

                let interval = Interval::new(keep_alive.unwrap(), &handle_inner).unwrap();
                let timer_future = interval.for_each(move |_| {
                                if client_timer.has_exceeded_keep_alive() {
                                    Err(io::Error::new(ErrorKind::Other, "Ping Timer Error"))
                                } else {
                                    Ok(())
                                }
                            })
                            .map_err(move |e| {
                                      error!("ping timer error = {:?}", e);
                                      Ok::<_, ()>(())
                            })
                            .then(|_| Ok(()));

                let rx_future = timer_future.select(rx_future);

                // current connections outgoing n/w packets
                let tx_future = rx.map_err(|e| {
                                    error!("Channel error = {:?}", e);
                                    Error::Other
                                  })
                                  .and_then(move |r| match r {
                                           Packet::Publish(p) => Ok(Packet::Publish(p)),
                                           Packet::Connack(c) => Ok(Packet::Connack(c)),
                                           Packet::Suback(sa) => Ok(Packet::Suback(sa)),
                                           Packet::Puback(pa) => Ok(Packet::Puback(pa)),
                                           Packet::Pubrec(prec) => Ok(Packet::Pubrec(prec)),
                                           Packet::Pubrel(prel) => Ok(Packet::Pubrel(prel)),
                                           Packet::Pubcomp(pc) => Ok(Packet::Pubcomp(pc)),
                                           Packet::Pingresp => Ok(Packet::Pingresp),
                                           Packet::Disconnect => Err(Error::DisconnectRequest),
                                           _ => {
                                               error!("improper packet {:?} received. disconnecting", r);
                                               Err(Error::InvalidMqttPacket)
                                           }
                                       })
                                  .forward(sender)
                                  .map_err(move |e| {
                                      error!("transmission error = {:?}", e);
                                      Ok::<_, ()>(())
                                  })
                                  .then(move |_| {
                                      Ok::<_, ()>(())
                                  });

                let rx_future = rx_future.then(|_| Ok(()));


                let connection = rx_future.select(tx_future);
                let c = connection.then(move |_| {
                                            let mut broker = broker_disconnect.borrow_mut();
                                            error!("disconnecting client: {:?}", id);
                                            let _ = broker.handle_disconnect(&id, uid, clean_session);
                                            Ok::<_, ()>(())
                                        });

                handle_inner.spawn(c);
            }
            Ok::<_, ()>(())
        }).then(|_| Ok(()));

        handle.spawn(mqtt);
        Ok(())
    });

    //TODO: why isn't this working for 'listener.incoming().for_each'
    core.run(connections).unwrap();
}