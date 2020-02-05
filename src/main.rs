#[macro_use]
extern crate log;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate structopt_derive;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;

pub mod broker;
pub mod client;
pub mod codec;
pub mod conf;
pub mod error;

use std::cell::RefCell;
use std::fs::File;
use std::io::Read;
use std::rc::Rc;
use std::sync::Arc;

use mqtt3::*;
use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::task;
use tokio::select;
use tokio_util::codec::Framed;

use futures::stream::Stream;
use futures::stream::StreamExt;
use futures_util::stream::SplitSink;
use futures_util::stream::SplitStream;
use futures_util::sink::SinkExt; 
use simplelog::{CombinedLogger, Config, LogLevelFilter, TermLogger, WriteLogger};

use broker::Broker;
use codec::MqttCodec;
use error::Error;

#[derive(StructOpt, Debug)]
#[structopt(name = "Rumqttd", about = "High performance asynchronous mqtt broker")]
pub struct CommandLine {
    #[structopt(short = "c", help = "Rumqttd config file", default_value = "/etc/rumqttd.conf")]
    config_path: String,
}

lazy_static! {
    pub static ref CONF: conf::Rumqttd = {
        let cl = CommandLine::from_args();

        let mut conf = String::new();
        let _ = File::open(cl.config_path).expect("Config Error").read_to_string(&mut conf);
        toml::from_str::<conf::Rumqttd>(&conf).unwrap()
    };
}


use std::io;
async fn incoming(client: &mut client::Client, broker: &mut Rc<RefCell<Broker>>, mut stream: impl Stream<Item = std::result::Result<Packet, io::Error>> + Unpin) {
    client.reset_last_control_at();
    let mut broker = broker.borrow_mut();

    let packet = stream.next().await.unwrap();
    let packet = packet.unwrap();
    match packet {
        Packet::Publish(p) => {
            // sends acks
            client.handle_publish(p.clone()).unwrap();
            // forward to subscribers
            broker.handle_publish(p).unwrap();
        }
        Packet::Subscribe(s) => {
            let successful_subscriptions = client.handle_subscribe(s).unwrap();
            broker.handle_subscribe(successful_subscriptions, &client).unwrap();
        }
        Packet::Puback(pkid) => client.handle_puback(pkid).unwrap(),
        Packet::Pingreq => client.handle_pingreq().unwrap(),
        _ => panic!("InvalidMqttPacket = {:?}", packet),
    }
}

// async fn outgoing(mut stream: impl Stream<Item = Packet> + Unpin, mut writer: SplitSink<Framed<TcpStream, MqttCodec>, Packet>) {
//     while let Some(packet) = stream.next().await {
//         let packet = match packet {
//             Packet::Publish(p) => Ok(Packet::Publish(p)),
//             Packet::Connack(c) => Ok(Packet::Connack(c)),
//             Packet::Suback(sa) => Ok(Packet::Suback(sa)),
//             Packet::Puback(pa) => Ok(Packet::Puback(pa)),
//             Packet::Pubrec(prec) => Ok(Packet::Pubrec(prec)),
//             Packet::Pubrel(prel) => Ok(Packet::Pubrel(prel)),
//             Packet::Pubcomp(pc) => Ok(Packet::Pubcomp(pc)),
//             Packet::Pingresp => Ok(Packet::Pingresp),
//             Packet::Disconnect => Err(Error::DisconnectRequest),
//             _ => unimplemented!()
//         };
// 
//         writer.send(packet.unwrap()).await.unwrap();
//     }
// }

async fn outgoing(mut stream: impl Stream<Item = Packet> + Unpin) -> Packet {
    let packet = match stream.next().await.unwrap() {
        Packet::Publish(p) => Ok(Packet::Publish(p)),
        Packet::Connack(c) => Ok(Packet::Connack(c)),
        Packet::Suback(sa) => Ok(Packet::Suback(sa)),
        Packet::Puback(pa) => Ok(Packet::Puback(pa)),
        Packet::Pubrec(prec) => Ok(Packet::Pubrec(prec)),
        Packet::Pubrel(prel) => Ok(Packet::Pubrel(prel)),
        Packet::Pubcomp(pc) => Ok(Packet::Pubcomp(pc)),
        Packet::Pingresp => Ok(Packet::Pingresp),
        Packet::Disconnect => Err(Error::DisconnectRequest),
        _ => unimplemented!()
    };

    packet.unwrap()
}

#[tokio::main(core_threads = 1)]
async fn accept_loop() {
    let local = task::LocalSet::new();
    let local = Arc::new(local);
    let addr = format!("0.0.0.0:{}", 1883);

    info!("Waiting for connections on {}", addr);
    // eventloop which accepts connections
    let mut listener = TcpListener::bind(addr).await.unwrap();
    let broker = Rc::new(RefCell::new(Broker::new()));
    local.run_until(async move {
        loop {
            let mut broker = broker.clone();
            info!("👂🏼   listening for connections");
            let (stream, addr) = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    error!("Tcp connection error = {:?}", e);
                    break;
                }
            };

            info!("Accepting from: {}", addr);

            let framed = Framed::new(stream, MqttCodec::new());
            info!("☄️   new tcp connection from {}", addr);
            let (mut writer, mut reader) = framed.split();
            let packet = reader.next().await.unwrap(); 
            let packet = packet.unwrap();
            let (mut client, connack,mut rx) = match packet {
                Packet::Connect(c) => broker.borrow_mut().handle_connect(c, addr).unwrap(),
                _  => panic!(),
            };

            let connack = Packet::Connack(connack);
            client.send(connack);
            client.send_all_backlogs();

            task::spawn_local(async move {
                loop {
                    incoming(&mut client, &mut broker, &mut reader).await
                }
            });

            task::spawn_local(async move {
                loop {
                    let packet = outgoing(&mut rx).await;
                    writer.send(packet).await.unwrap()
                }
            });
        }
    }).await;
}

fn main() {
    CombinedLogger::init(vec![
        TermLogger::new(LogLevelFilter::Info, Config::default()).unwrap(),
        WriteLogger::new(LogLevelFilter::Info, Config::default(), File::create("/tmp/rumqttd.log").unwrap()),
    ])
        .unwrap();

    info!("🌩️   starting broker");
    accept_loop();
}
