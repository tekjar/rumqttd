use std::io::{self, Cursor};
use std::str;
use bytes::{BytesMut, BufMut};
use tokio_io::codec::{Encoder, Decoder};

use mqtt3::{Packet, MqttWrite, MqttRead};

pub struct MqttCodec;

impl Decoder for MqttCodec {
    type Item = Packet;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Packet>> {
        let (packet, len) = {
            let mut buf_ref = buf.as_ref();
            match buf_ref.read_packet_with_len() {
                Err(..) => return Ok(None),
                Ok(v) => v,
            }
        };

        buf.split_to(len);
        // println!("{:?}, {:?}", len, packet);
        Ok(Some(packet))
    }
}

impl Encoder for MqttCodec {
    type Item = Packet;
    type Error = io::Error;

    fn encode(&mut self, msg: Packet, buf: &mut BytesMut) -> io::Result<()> {
        let mut stream = Cursor::new(Vec::new());

        // TODO: Implement `write_packet` for `&mut BytesMut`
        if let Err(e) = stream.write_packet(&msg) {
            // println!("{:?}", e);
            return Err(io::Error::new(io::ErrorKind::Other, "Unable to encode!"));
        }

        for i in stream.get_ref() {
            buf.put(*i);
        }
        Ok(())
    }
}
