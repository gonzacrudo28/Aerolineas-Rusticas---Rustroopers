use std::{net::TcpStream, process::Command};

use aerolineas_rusticas::{
    errors::error_types::ErrorTypes,
    protocol::protocol_body::compression::Compression,
    server::query_execute::{authenticate, create_keyspace, startup, use_keyspace},
};
use native_tls::TlsConnector;

// Assuming the startup function is defined in the aerolineas_rusticas crate

const COMPRESSION: Option<Compression> = None;

#[test]
#[ignore]
fn test_main() -> Result<(), ErrorTypes> {
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
        .map_err(|_| ErrorTypes::new(701, "Error connecting to the server".to_string()))?;
    let mut server = connector.connect("127.0.0.1", stream).unwrap();

    startup(&mut server, COMPRESSION)?;
    println!("Start up completed!");

    authenticate(
        "admin".to_string(),
        "admin".to_string(),
        &mut server,
        COMPRESSION,
    )?;
    println!("Authenticated!");
    assert!(create_keyspace(&mut server, COMPRESSION).is_ok());
    println!("Keyspace created!");
    assert!(use_keyspace(&mut server, COMPRESSION).is_ok());
    println!("Keyspace used!");
    Ok(())
}
