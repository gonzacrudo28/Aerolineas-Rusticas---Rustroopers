use std::net::TcpStream;
use std::process::Command;

use aerolineas_rusticas::server::query_execute::startup;
use aerolineas_rusticas::{
    errors::error_types::ErrorTypes, protocol::protocol_body::compression::Compression,
};
use native_tls::TlsConnector;

// Assuming the startup function is defined in the aerolineas_rusticas crate

const COMPRESSION: Option<Compression> = None;
#[test]
#[ignore]
fn test_main() {
    let output = Command::new("sh")
        .arg("-c")
        .arg("./scripts/launch_nodes.sh")
        .output()
        .expect("failed to execute process");

    assert!(output.status.success());

    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let stream = TcpStream::connect("127.0.0.1:8090")
        .map_err(|_| ErrorTypes::new(701, "Error connecting to the server".to_string()))
        .unwrap();
    let mut server = connector.connect("127.0.0.1", stream).unwrap();

    assert!(startup(&mut server, COMPRESSION).is_ok());
    println!("Start up completed!");
}
