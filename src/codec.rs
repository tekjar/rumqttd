use std::io::{self, ErrorKind, Cursor};
use bytes::BytesMut;
use tokio_io::codec::{Encoder, Decoder};

use mqtt3::{Error, Packet, MqttWrite, MqttRead};

pub struct MqttCodec;

impl Decoder for MqttCodec {
    type Item = Packet;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Packet>> {
        let (packet, len) = {
            let mut buf_ref = buf.as_ref();
            match buf_ref.read_packet_with_len() {
                Err(e) => {
                    if let Error::Io(e) = e {
                        match e.kind() {
                            ErrorKind::TimedOut | ErrorKind::WouldBlock => return Ok(None),
                            _ => {
                                println!("* {:?}", e);
                                return Err(io::Error::new(ErrorKind::Other, "Invalid Mqtt Frame"));
                            }
                        }
                    } else {
                        println!("** {:?}", e);
                        return Err(io::Error::new(ErrorKind::Other, "Invalid Mqtt Frame"));
                    }
                }
                Ok(v) => v,
            }
        };

        buf.split_to(len);
        println!("{:?}, {:?}", len, packet);
        Ok(Some(packet))
    }
}

impl Encoder for MqttCodec {
    type Item = Packet;
    type Error = io::Error;

    fn encode(&mut self, msg: Packet, buf: &mut BytesMut) -> io::Result<()> {
        let mut stream = Cursor::new(Vec::new());

        // TODO: Implement `write_packet` for `&mut BytesMut`
        if let Err(_) = stream.write_packet(&msg) {
            return Err(io::Error::new(io::ErrorKind::Other, "Unable to encode!"));
        }

        buf.extend(stream.get_ref());

        Ok(())
    }
}
