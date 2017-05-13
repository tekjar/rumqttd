#[derive(Debug, Deserialize)]
pub struct Rumqttd {
    pub version: String,
    pub connection: Connection,
    pub log: Log,
    pub security: Security,
    pub session: Session,
    pub misc: Misc,
}

#[derive(Debug, Deserialize)]
pub struct Connection {
    pub port: u16,
    pub timeout: String,
}

#[derive(Debug, Deserialize)]
pub struct Log {
    level: String,
    console: bool,
    file: String,
}

#[derive(Debug, Deserialize)]
pub struct Security {
    username: String,
    password: String,
    max_clients: i32,
    tls: bool,
    key: String,
    cert: String,
    cacert: String,
}

#[derive(Debug, Deserialize)]
pub struct Session {
    max_inflight: u32,
    retry_interval: String,
    expiry: String,
}

#[derive(Debug, Deserialize)]
pub struct Misc {
    max_clientid_len: u32,
    max_packet_size: String,
    idle_timeout: String,
}
