extern crate mqtt3;
extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_timer;
extern crate bytes;
extern crate toml;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate slog;
extern crate slog_term;
#[macro_use]
extern crate quick_error;

pub mod error;
pub mod codec;
pub mod broker;
pub mod client;
pub mod subscription_list;
pub mod client_list;
pub mod conf;

use std::fs::File;
use std::io::{self, Read};
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::error::Error as StdError;

use mqtt3::*;
use tokio_core::reactor::{Core, Interval};
use tokio_core::net::TcpListener;
use tokio_io::AsyncRead;

use futures::stream::Stream;
use futures::Future;

use slog::{Logger, Drain};

use broker::Broker;
use codec::MqttCodec;
use error::Error;

lazy_static! {
    pub static ref CONF: conf::Rumqttd = {
        let mut conf = String::new();
        let _ = File::open("conf/rumqttd.conf").unwrap().read_to_string(&mut conf);
        toml::from_str::<conf::Rumqttd>(&conf).unwrap()
    };
}

fn main() {
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), CONF.connection.port);
    let logger = rumqttd_logger();

    info!(logger, "🌩️   starting broker");

    let listener = TcpListener::bind(&address, &core.handle()).unwrap();

    let broker = Broker::new();
    let broker_inner = broker.clone();
    let handle_inner = handle.clone();
    info!(logger, "👂🏼   listening for connections");

    let connections = listener.incoming().for_each(|(socket, addr)| {
        let framed = socket.framed(MqttCodec::new());
        info!(logger, "☄️   new tcp connection from {}", addr);
        let broker_inner = broker_inner.clone();
        let error_logger = broker_inner.logger.clone();
        let error_logger1 = broker_inner.logger.clone();

        let handshake = framed.into_future()
                                  .map_err(move |(err, _)| {
                                      error!(error_logger, "pre handshake error = {:?}", err);
                                      err
                                  }) // for accept errors, get error and discard the stream
                                  .and_then(move |(packet,framed)| { // only accepted connections from here

                let broker = broker_inner.clone();

                if let Some(Packet::Connect(c)) = packet {
                    match broker.handle_connect(c, addr) {
                        Ok((client, rx)) => {
                            info!(error_logger1, "✨   mqtt connection successful. id = {:?}", client.id);
                            Ok((framed, client, rx))
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

        let broker_inner = broker.clone();
        let handle_inner = handle_inner.clone();
        let logger = logger.clone();
        let mqtt = handshake.map(|w| Some(w))
                            .or_else(move |e| {
                                         error!(logger, "{:?}", e);
                                         Ok::<_, ()>(None)
                                     })
                            .map(move |handshake| {
            let broker_handshake = broker_inner.clone();
            
            // handle each connections n/w send and recv here
            if let Some((framed, client, rx)) = handshake {
                let id: String = client.id.clone();
                let keep_alive = client.keep_alive;
                let client_timer = client.clone();

                let (sender, receiver) = framed.split();

                let connack = Packet::Connack(Connack {
                                                  session_present: false,
                                                  code: ConnectReturnCode::Accepted,
                                              });

                let _ = client.send(connack);
                let broker_inner = broker_handshake.clone();
                let handle_inner = handle_inner.clone();

                let error_logger = broker_handshake.logger.clone();
                // current connections incoming n/w packets
                let rx_future = receiver.or_else(|e| Err::<_, error::Error>(e.into()))
                                        .for_each(move |msg| {
                    let broker = broker_inner.clone();
                    client.reset_last_control_at();
                    match msg {
                        Packet::Publish(p) => broker.handle_publish(p, &client),
                        Packet::Subscribe(s) => broker.handle_subscribe(s, &client),
                        Packet::Puback(pkid) => broker.handle_puback(pkid, &client),
                        Packet::Pubrec(pkid) => broker.handle_pubrec(pkid, &client),
                        Packet::Pubrel(pkid) => broker.handle_pubrel(pkid, &client),
                        Packet::Pubcomp(pkid) => broker.handle_pubcomp(pkid, &client),
                        Packet::Pingreq => broker.handle_pingreq(&client),
                        _ => Err(error::Error::InvalidMqttPacket),
                    }
                }).map_err(move |e| {
                    error!(error_logger, "network incoming handle error = {:?}", e);
                    Ok::<_, ()>(())
                })
                .then(move |_| Ok::<_, ()>(()));

                let interval = Interval::new(keep_alive.unwrap(), &handle_inner).unwrap();
                let error_logger = broker_handshake.logger.clone();
                let timer_future = interval.for_each(move |_| {
                                if client_timer.has_exceeded_keep_alive() {
                                    Err(io::Error::new(ErrorKind::Other, "Ping Timer Error"))
                                } else {
                                    Ok(())
                                }
                            })
                            .map_err(move |e| {
                                      error!(error_logger, "ping timer error = {:?}", e);
                                      Ok::<_, ()>(())
                            })
                            .then(|_| Ok(()));

                let rx_future = timer_future.select(rx_future);

                let error_logger = broker_handshake.logger.clone();
                let error_logger1 = broker_handshake.logger.clone();
                // current connections outgoing n/w packets
                let tx_future = rx.map_err(|_| Error::Other)
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
                                               error!(error_logger1, "improper packet {:?} received. disconnecting", r);
                                               Err(Error::InvalidMqttPacket)
                                           }
                                       })
                                  .forward(sender)
                                  .map_err(move |e| {
                                      error!(error_logger, "network transmission error = {:?}", e);
                                      Ok::<_, ()>(())
                                  })
                                  .then(move |_| {
                                      // broker_handshake.handle_disconnect(&id);
                                      Ok::<_, ()>(())
                                  });

                let rx_future = rx_future.then(|_| Ok(()));


                let broker_inner = broker_handshake.clone();
                let connection = rx_future.select(tx_future);
                let c = connection.then(move |_| {
                                            error!(broker_inner.logger, "disconnecting client: {:?}", id);
                                            let _ = broker_inner.remove_client(&id);
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

fn rumqttd_logger() -> Logger {
    use std::sync::Mutex;

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = Mutex::new(drain).fuse();
    Logger::root(drain, o!("rumqttd" => env!("CARGO_PKG_VERSION")))
}
