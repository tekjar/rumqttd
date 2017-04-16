use std::io::{self, ErrorKind, Cursor};
use std::error::Error;
use bytes::BytesMut;
use tokio_io::codec::{Encoder, Decoder};

use mqtt3::{self, Packet, MqttWrite, MqttRead};

pub struct MqttCodec;

impl Decoder for MqttCodec {
    type Item = Packet;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Packet>> {
        // NOTE: `decode` might be called with `buf.len == 0` when prevous
        // decode call read all the bytes in the stream. We should return
        // Ok(None) in those cases or else the `read` call will return
        // Ok(0) => translated to UnexpectedEOF by `byteorder` crate.
        // `read` call Ok(0) happens when buffer specified was 0 bytes in len
        // https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read
        if buf.len() == 0 {
            return Ok(None);
        }

        let (packet, len) = {
            let mut buf_ref = buf.as_ref();
            match buf_ref.read_packet_with_len() {
                Err(e) => {
                    if let mqtt3::Error::Io(e) = e {
                        match e.kind() {
                            ErrorKind::TimedOut | ErrorKind::WouldBlock => return Ok(None),
                            _ => return Err(io::Error::new(e.kind(), e.description())),
                        }
                    } else {
                        return Err(io::Error::new(ErrorKind::Other, e.description()));
                    }
                }
                Ok(v) => v,
            }
        };

        buf.split_to(len);

        // println!("{:?}, {:?}, {:?}", len, packet, buf.len());
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
