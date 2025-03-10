use aerolineas_rusticas::{
    errors::error_types::ErrorTypes,
    protocol::protocol_body::compression::Compression,
    server::{
        query_execute::{
            authenticate, get_airports, insert_simulador, prompt, send_querys, startup,
        },
        query_simulator::QuerySimulator,
    },
    ui::flight::Flight,
};
use native_tls::{TlsConnector, TlsStream};
use std::{
    net::TcpStream,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
const COMPRESSION: Option<Compression> = None;
use threadpool::ThreadPool;

fn main() -> Result<(), ErrorTypes> {
    let airports = get_airports().map_err(|e| ErrorTypes::new(700, e.to_string()))?;
    let server = conect_server()?;

    let pool = ThreadPool::new(10);
    let server = Arc::new(Mutex::new(server));
    let airports = Arc::new(airports);

    loop {
        match insert_simulador(Arc::clone(&server), COMPRESSION, &airports) {
            Ok(flight) => {
                println!("Inserted flight!");
                let server = Arc::clone(&server);
                pool.execute(move || {
                    if let Err(e) = update_flight(flight, server, COMPRESSION) {
                        eprintln!("Error updating flight: {:?}", e);
                    }
                });
            }
            Err(e) => eprintln!("Error inserting flight: {:?}", e),
        }
    }
}

/// This function creates the connection with the server.
pub fn conect_server() -> Result<TlsStream<TcpStream>, ErrorTypes> {
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let stream = TcpStream::connect("127.0.0.1:8090")
        .map_err(|_| ErrorTypes::new(701, "Error connecting to the server".to_string()))?;
    let mut server = connector.connect("127.0.0.1", stream).unwrap();
    println!("Simulator connected to the server!");
    startup(&mut server, COMPRESSION)?;
    println!("Start up simulator completed!");
    authenticate(
        prompt("Enter the user: ", &QuerySimulator::User, None),
        prompt("Enter the password: ", &QuerySimulator::User, None),
        &mut server,
        COMPRESSION,
    )?;
    println!("Simulator authenticated!");
    Ok(server)
}

/// This function updates the flight position and sends the updated data to the server.
pub fn update_flight(
    mut flight: Flight,
    server: Arc<Mutex<TlsStream<TcpStream>>>,
    compression: Option<Compression>,
) -> Result<(), ErrorTypes> {
    for _i in 0..10 {
        flight.update_flight()?;
        let querys = [
            create_update_query(
                "arrivals",
                &flight,
                "destination",
                flight.get_destination().get_airport_code(),
            ),
            create_update_query(
                "departures",
                &flight,
                "origin",
                flight.get_origin().get_airport_code(),
            ),
        ]
        .to_vec();

        {
            send_querys(querys, Arc::clone(&server), compression.clone())?;
        }

        thread::sleep(Duration::from_secs(5));
    }
    Ok(())
}

/// This function creates the query to update the flight position.
fn create_update_query(table: &str, flight: &Flight, field: &str, place: &str) -> String {
    format!(
        "UPDATE {} SET fuel = {}, distance_traveled = {}, velocity = {}, height = {}, latitude = {}, longitude = {} WHERE id = {} AND {} = '{}';",
        table,
        flight.get_fuel(),
        flight.get_distance_traveled(),
        flight.get_velocity(),
        flight.get_height(),
        flight.get_latitude(),
        flight.get_longitude(),
        flight.get_flight_code(),
        field,
        place
    )
}
