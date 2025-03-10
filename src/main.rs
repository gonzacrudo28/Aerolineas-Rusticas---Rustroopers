use aerolineas_rusticas::{
    errors::error_types::ErrorTypes,
    protocol::protocol_body::compression::Compression,
    server::query_execute::{authenticate, startup},
    ui::lib::MyApp,
};
use native_tls::{TlsConnector, TlsStream};
use std::net::TcpStream;

const COMPRESSION: Option<Compression> = None;

#[cfg(not(target_arch = "wasm32"))]
fn main() -> Result<(), ErrorTypes> {
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let stream: TcpStream = TcpStream::connect("127.0.0.1:8090").unwrap();
    let mut server: TlsStream<TcpStream> = connector.connect("127.0.0.1", stream).unwrap();

    println!("Connected to the server!");
    startup(&mut server, COMPRESSION)?;
    println!("Start up completed!");
    authenticate(
        "client_ui".to_owned(),
        "1234".to_owned(),
        &mut server,
        COMPRESSION,
    )?;
    println!("Authenticated!");
    env_logger::init();
    let _ = eframe::run_native(
        "MyApp",
        Default::default(),
        Box::new(|cc| {
            Ok(Box::new(MyApp::new(
                cc.egui_ctx.clone(),
                &mut server,
                COMPRESSION,
            )))
        }),
    );
    Ok(())
}
