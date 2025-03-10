use native_tls::TlsStream;
use std::{
    collections::HashMap,
    fs,
    io::{self, Read, Write},
    net::TcpStream,
    sync::{Arc, Mutex},
};

use crate::{
    errors::error_types::ErrorTypes,
    protocol::{
        protocol_body::{compression::Compression, query_flags::QueryFlags},
        protocol_notations::consistency::Consistency,
        protocol_writer::Protocol,
    },
    receiver::{
        receiver_impl::receive_message, response_message::ResponseMessage,
        result_response::ResultResponse,
    },
};
use crate::{receiver::message::Message, ui::flight::Flight};
use crate::{receiver::message::Message::ReplyMessage, ui::windows::Airport};

use super::query_simulator::QuerySimulator;

const CONSUMO_HORA: i32 = 14000;
const VELOCIDAD_HORA: i32 = 900;

pub fn get_airports() -> Result<HashMap<String, Airport>, Box<dyn std::error::Error>> {
    let data = fs::read_to_string("airports.json")?;
    let airport_list: Vec<Airport> = serde_json::from_str(&data)?;

    let mut airports = HashMap::new();
    for airport in airport_list {
        airports.insert(airport.code.clone(), airport);
    }
    Ok(airports)
}

fn check_airport(airports: &HashMap<String, Airport>, airport: &String) -> bool {
    airports.contains_key(&airport.to_string().to_uppercase())
}

fn check_distance(
    airports: &HashMap<String, Airport>,
    origin: &str,
    destination: &str,
) -> Result<f64, ErrorTypes> {
    let origin = airports.get(&origin.to_uppercase()).unwrap();
    let destination = airports.get(&destination.to_uppercase()).unwrap();
    origin.distance_to(destination)
}

pub fn min_fuel(distance: f64) -> f64 {
    let time_of_flight = distance / VELOCIDAD_HORA as f64;
    time_of_flight * CONSUMO_HORA as f64
}

pub fn insert_simulador(
    server: Arc<Mutex<TlsStream<TcpStream>>>,
    compression: Option<Compression>,
    airports: &HashMap<String, Airport>,
) -> Result<Flight, ErrorTypes> {
    let mut querys = Vec::new();
    let id: u32 = prompt_and_parse("Enter the flight id: ", QuerySimulator::FlightId, airports);
    let origin: String = prompt(
        "Enter the origin: ",
        &QuerySimulator::Airport,
        Some(airports),
    )
    .to_uppercase();
    let destination: String = prompt(
        "Enter the destination: ",
        &QuerySimulator::Airport,
        Some(airports),
    )
    .to_uppercase();
    let distance: f64 = check_distance(airports, &origin, &destination)?;
    let departure_time: String = prompt(
        "Enter the departure time: ",
        &QuerySimulator::Date,
        Some(airports),
    );
    let arrival_time: String = prompt(
        "Enter the arrival time: ",
        &QuerySimulator::Date,
        Some(airports),
    );
    let min_fuel = min_fuel(distance);
    let mut fuel: f64;
    loop {
        let msg = format!("Enter the fuel, taking into account the speed and minimum average consumption, it should be {:.2}: ", min_fuel);
        fuel = prompt_and_parse(&msg, QuerySimulator::Fuel, airports);
        if fuel >= min_fuel {
            break;
        } else {
            println!("Not enough fuel, try again.");
        }
    }
    for i in 0..2 {
        let table = if i == 0 { "departures" } else { "arrivals" };
        querys.push(format!("INSERT INTO {} (id, status, origin, destination, departure_time, arrival_time, fuel, velocity, height, latitude, longitude, distance_traveled) VALUES ({}, 'ON TIME', '{}', '{}', '{}', '{}', {}, 0, 0, 0, 0, 0);", table, id, origin, destination, arrival_time, departure_time, fuel));
    }
    send_querys(querys, server, compression)?;
    Ok(Flight::new(
        id as i32,
        airports.get(&origin).unwrap().clone(),
        airports.get(&destination).unwrap().clone(),
        arrival_time,
        departure_time,
        distance,
        fuel,
    ))
}

pub fn prompt(
    _message: &str,
    sim_type: &QuerySimulator,
    airports: Option<&HashMap<String, Airport>>,
) -> String {
    print!("{}", _message);
    io::stdout().flush().expect("Failed to flush stdout"); // Vaciar el búfer
    let mut input: String = String::new();
    io::stdin()
        .read_line(&mut input)
        .expect("Failed to read line");
    input = input.trim().to_string();
    match sim_type {
        QuerySimulator::Airport => {
            if check_airport(airports.unwrap(), &input) {
                input
            } else {
                println!("Invalid airport code. Please try again.");
                io::stdout().flush().expect("Failed to flush stdout");
                prompt(_message, sim_type, airports)
            }
        }
        QuerySimulator::Date => {
            if validate_date(&input) {
                input
            } else {
                println!("Invalid date. Please try again.");
                io::stdout().flush().expect("Failed to flush stdout");
                prompt(_message, sim_type, Some(airports.unwrap()))
            }
        }
        _ => input,
    }
}

fn prompt_and_parse<T: std::str::FromStr>(
    message: &str,
    sim_type: QuerySimulator,
    airports: &HashMap<String, Airport>,
) -> T {
    loop {
        let input = prompt(message, &sim_type, Some(airports));
        match input.trim().parse::<T>() {
            Ok(value) => return value,
            Err(_) => println!("Invalid, try again."),
        }
    }
}

fn validate_date(date: &str) -> bool {
    let parts: Vec<&str> = date.trim().split('-').collect();
    if parts.len() != 3 {
        return false;
    }
    if let (Ok(year), Ok(month), Ok(day)) = (
        parts[0].parse::<u32>(),
        parts[1].parse::<u32>(),
        parts[2].parse::<u32>(),
    ) {
        return parts[0].len() == 4
            && parts[1].len() == 2
            && parts[2].len() == 2
            && month <= 12
            && day <= 31
            && year > 2023;
    }
    false
}

pub fn conect_server(
    server: &mut TlsStream<TcpStream>,
    msg: Option<Protocol>,
    compression: &Option<Compression>,
) -> Result<Message, ErrorTypes> {
    if let Some(mut msg) = msg {
        if server.write_all(&msg.get_binary()).is_err() {
            return Err(ErrorTypes::new(
                519,
                "Error sending message to server".to_string(),
            ));
        }
    }
    let mut buffer = [0; 1024];
    let read = server.read(&mut buffer);
    if read.is_err() {
        return Err(ErrorTypes::new(
            520,
            "Error receiving message from server".to_string(),
        ));
    }
    if let Ok(read) = read {
        receive_message(&mut buffer[0..read].to_vec(), &compression.clone())
    } else {
        Err(ErrorTypes::new(
            1,
            "Error receiving message from server".to_string(),
        ))
    }
}

pub fn send_querys(
    querys: Vec<String>,
    server: Arc<Mutex<TlsStream<TcpStream>>>,
    compression: Option<Compression>,
) -> Result<(), ErrorTypes> {
    let mut server = server.lock().unwrap();

    for query in querys {
        let mut msg = Protocol::new();
        msg.set_compress_algorithm(compression.clone());
        msg.write_query(&query, Consistency::Quorum, vec![QueryFlags::SkipMetadata])?;
        let message = conect_server(&mut server, Some(msg), &compression)?;

        let msg = match message {
            ReplyMessage(ResponseMessage::Result {
                kind: ResultResponse::Void,
            }) => Ok(()),
            ReplyMessage(_) => Err(ErrorTypes::new(522, "Unexpected message".to_string())),
            _ => Err(ErrorTypes::new(523, "Error receiving message".to_string())),
        };
        msg.as_ref()
            .map_err(|_| ErrorTypes::new(524, "Error receiving message".to_string()))?;
    }

    Ok(())
}

pub fn use_keyspace(
    server: &mut TlsStream<TcpStream>,
    compression: Option<Compression>,
) -> Result<(), ErrorTypes> {
    let mut msg = Protocol::new();
    msg.set_compress_algorithm(compression.clone());
    msg.write_query(
        "USE flights_keyspace;",
        Consistency::Quorum,
        vec![QueryFlags::SkipMetadata],
    )?;
    let message = conect_server(server, Some(msg), &compression)?;
    match message {
        ReplyMessage(ResponseMessage::Result {
            kind: ResultResponse::SetKeyspace { .. },
        }) => Ok(()),
        ReplyMessage(ResponseMessage::Result { .. }) => {
            Err(ErrorTypes::new(525, "Unexpected message".to_string()))
        }
        ReplyMessage(_) => Err(ErrorTypes::new(526, "Unexpected message".to_string())),
        _ => Err(ErrorTypes::new(527, "Error receiving message".to_string())),
    }
}

pub fn startup(
    server: &mut TlsStream<TcpStream>,
    compression: Option<Compression>,
) -> Result<(), ErrorTypes> {
    let mut msg = Protocol::new();
    msg.write_startup(compression)?;
    let startup = msg.get_binary();
    if server.write_all(&startup).is_err() {
        return Err(ErrorTypes::new(
            528,
            "Error sending message to server".to_string(),
        ));
    }

    let message = conect_server(&mut *server, None, &None)?;

    match message {
        ReplyMessage(msg) => match msg {
            ResponseMessage::Ready { body: _ } => Ok(()),
            ResponseMessage::Authenticate { class: _ } => Ok(()),
            _ => Err(ErrorTypes::new(529, "Unexpected message".to_string())),
        },

        _ => Err(ErrorTypes::new(530, "Error receiving message".to_string())),
    }
}

/// This function handle the authentication part.
pub fn authenticate(
    user: String,
    password: String,
    server: &mut TlsStream<TcpStream>,
    compression: Option<Compression>,
) -> Result<(), ErrorTypes> {
    let mut msg = Protocol::new();
    msg.set_compress_algorithm(compression.clone());
    msg.write_auth_response((user, password))?;

    let message = conect_server(server, Some(msg), &compression)?;
    match message {
        ReplyMessage(msg) => match msg {
            ResponseMessage::AuthSuccess { body: _ } => Ok(()),
            _ => Err(ErrorTypes::new(531, "Unexpected message".to_string())),
        },
        _ => Err(ErrorTypes::new(532, "Error receiving message".to_string())),
    }
}

/// This function creates the tables in the database.
pub fn create_tables(
    server: &mut TlsStream<TcpStream>,
    compression: Option<Compression>,
) -> Result<(), ErrorTypes> {
    let tables = vec![
        ("arrivals", "destination", "arrival_time"),
        ("departures", "origin", "departure_time"),
    ];

    for table in tables {
        let mut msg = Protocol::new();
        msg.set_compress_algorithm(compression.clone());
        let query = format!("CREATE TABLE {} (id int, status text, origin text, destination text, arrival_time date, departure_time date, fuel float, velocity float, height float, latitude float, longitude float, distance_traveled float, PRIMARY KEY (({}), id, {}));", table.0, table.1, table.2);
        msg.write_query(&query, Consistency::Quorum, vec![QueryFlags::SkipMetadata])?;
        let message = conect_server(server, Some(msg), &compression)?;
        match message {
            ReplyMessage(ResponseMessage::Result {
                kind: ResultResponse::SchemaChange { .. },
            }) => continue,
            ReplyMessage(ResponseMessage::Result { .. }) => {
                return Err(ErrorTypes::new(
                    1,
                    format!("Unexpected message: {:?}", message),
                ))
            }
            ReplyMessage(_) => {
                return Err(ErrorTypes::new(
                    1,
                    format!("Unexpected message: {:?}", message),
                ))
            }
            _ => {
                return Err(ErrorTypes::new(1, "Error receiving message".to_string()));
            }
        }
    }
    Ok(())
}

/// This function creates the keyspace in the database.
pub fn create_keyspace(
    server: &mut TlsStream<TcpStream>,
    compression: Option<Compression>,
) -> Result<(), ErrorTypes> {
    let mut msg = Protocol::new();
    msg.set_compress_algorithm(compression.clone());
    msg.write_query(
        "CREATE KEYSPACE flights_keyspace WITH REPLICATION = { 'replication_factor': 3};",
        Consistency::Quorum,
        vec![QueryFlags::SkipMetadata],
    )?;
    let message = conect_server(server, Some(msg), &compression)?;
    match message {
        ReplyMessage(ResponseMessage::Result {
            kind: ResultResponse::SchemaChange { .. },
        }) => Ok(()),
        ReplyMessage(ResponseMessage::Result { .. }) => {
            Err(ErrorTypes::new(537, "Unexpected message".to_string()))
        }
        ReplyMessage(_) => Err(ErrorTypes::new(538, "Unexpected message".to_string())),
        _ => Err(ErrorTypes::new(539, "Error receiving message".to_string())),
    }
}

pub fn insert(
    server: &mut TlsStream<TcpStream>,
    compression: Option<Compression>,
) -> Result<(), ErrorTypes> {
    let mut querys = vec!["INSERT INTO arrivals (id, origin, destination, departure_time, arrival_time, fuel, velocity, altitude) VALUES (1, 'EZE', 'AEP', '2024-10-28','2021-10-28', 900.0,520.5, 737.2);","INSERT INTO arrivals (id, origin, destination, departure_time, arrival_time, fuel, velocity, altitude) VALUES (3, 'EZE', 'AEP', '2024-10-28','2021-10-28', 900.0,520.5, 737.2);"];
    querys.push("DELETE FROM arrivals WHERE id = 1 AND destination = 'AEP';");
    querys.push("INSERT INTO arrivals (id, origin, destination, departure_time, arrival_time, fuel, velocity, altitude) VALUES (4, 'EZE', 'AEP', '2024-10-28','2021-10-28', 900.0,520.5, 737.2);");
    for query in querys {
        let mut msg = Protocol::new();
        msg.set_compress_algorithm(compression.clone());
        msg.write_query(query, Consistency::One, vec![QueryFlags::SkipMetadata])?;
        conect_server(server, Some(msg), &compression)?;
    }
    Ok(())
}
