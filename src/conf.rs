#[derive(Debug, Deserialize)]
pub struct Rumqttd {
    version: String,
    connection: Connection,
    log: Log,
    security: Security,
    session: Session,
    misc: Misc,
}

#[derive(Debug, Deserialize)]
struct Connection {
    port: u32,
    timeout: String,
}

#[derive(Debug, Deserialize)]
struct Log {
    level: String,
    console: bool,
    file: String,
}

#[derive(Debug, Deserialize)]
struct Security {
    username: String,
    password: String,
    max_clients: i32,
    tls: bool,
    key: String,
    cert: String,
    cacert: String,
}

#[derive(Debug, Deserialize)]
struct Session {
    max_inflight: u32,
    retry_interval: String,
    expiry: String,
}

#[derive(Debug, Deserialize)]
struct Misc {
    max_clientid_len: u32,
    max_packet_size: String,
    idle_timeout: String,
}
