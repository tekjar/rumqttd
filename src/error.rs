use std::io;
use std::result;

pub type Result<T> = result::Result<T, Error>;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            description("io error")
            display("I/O error: {}", err)
            cause(err)
        }
        NoClient {
            description("No client with this ID")
        }
        ClientIdExists {
            description("Client with that ID already exists")
        }
        InvalidMqttPacket {
            description("Invalid Mqtt Packet")
        }
        InvalidClientId {
            description("Invalid Client ID")
        }
        DisconnectRequest {
            description("Received Disconnect Request")
        }
        NotInQueue {
            description("Couldn't find requested message in the queue")
        }
        DisconnectPacket {
            description("Received disconnect packet from client")
        }
        Other
    }
}
