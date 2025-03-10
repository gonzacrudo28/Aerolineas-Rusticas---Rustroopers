use super::address::Address;
use super::connection::Connection;
use super::gossiper::Gossiper;
use super::log_type::LogType;
use super::node_message::{NodeMessage, SchemaChange};
use super::schema::Schema;
use crate::errors::error_types::ErrorTypes;
use crate::protocol::protocol_notations::consistency::Consistency;
use crate::protocol::query_parser::clause::Clause;
use crate::protocol::{
    protocol_body::{
        compression::Compression, result_kind::ResultKind, schema_change::SchemaChangeType,
    },
    protocol_writer::Protocol,
    query_parser::query::Query,
};
use crate::receiver::{
    message::Message::SolicitationMessage, receiver_impl::receive_message,
    request_message::RequestMessage,
};
use std::collections::HashMap;
use std::io;
use std::sync::mpsc::{self, channel, Sender};

use crate::server::{
    application_state::ApplicationState, endpoint_state::EndpointState,
    gossip_message::GossipMessage, gossiper::get_gossiper, heartbeat_state::HeartbeatState,
    status::Status, users::User,
};
use native_tls::{Identity, TlsAcceptor, TlsStream};
use std::{
    fs::File,
    fs::OpenOptions,
    io::{BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

const SEED_IP_ADDRESS: &str = "127.0.0.1:8080";
const NODE_MESSAGE: u8 = 1;

/// ep struct represents the node. It contains the address, the mem tables and the commit log.
#[derive(Debug)]
pub struct Node {
    address: Address,
    endpoint_state: EndpointState,
    schema: Arc<Mutex<Schema>>,
}

impl Node {
    pub fn new(internal_address: &str, client_address: &str) -> Result<Node, ErrorTypes> {
        let heartbeat_state = HeartbeatState::new();
        let port = internal_address.split(":").collect::<Vec<&str>>()[1].to_string();
        let application_state = ApplicationState::new(Status::Up, internal_address.to_string());
        let endpoint_state = EndpointState::new(heartbeat_state, application_state);
        let node = Node {
            schema: Arc::new(Mutex::new(Schema::new(&port)?)),
            address: Address {
                i_address: internal_address.to_string(),
                c_address: client_address.to_string(),
                i_port: internal_address.split(":").collect::<Vec<&str>>()[1].to_string(),
            },
            endpoint_state,
        };
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(format!("node{}_log.log", port))
            .unwrap();
        Ok(node)
    }

    /// This function is responsible for running the node.
    pub fn run(&mut self) -> Result<(), ErrorTypes> {
        write_log_message(
            &self.address.i_port,
            LogType::Info,
            format!("Listening on {} for peers", self.address.i_address),
        );
        write_log_message(
            &self.address.i_port,
            LogType::Info,
            format!("Listening on {} for clients", self.address.c_address),
        );
        let gossiper = get_gossiper();

        let listener_node: TcpListener = TcpListener::bind(&self.address.i_address).unwrap();
        gossiper.add_node(self.address.i_address.clone());
        gossiper.put_endpoint_state(
            listener_node.local_addr().unwrap().to_string(),
            self.endpoint_state.clone(),
        );
        let address = self.address.clone();

        let address_clone = address.clone();
        if self.address.i_address != SEED_IP_ADDRESS {
            gossiper.try_connect(
                &SEED_IP_ADDRESS.to_string(),
                Arc::clone(&self.schema),
                &address,
                true,
            );
        }
        gossiper.try_connect(
            &self.address.i_address,
            Arc::clone(&self.schema),
            &address,
            true,
        );
        let schema = Arc::clone(&self.schema);
        thread::spawn(move || loop {
            let gossiper = get_gossiper();
            match gossiper.gossip(address_clone.clone(), schema.clone()) {
                Ok(_) => {}
                Err(addr) => {
                    if !gossiper.is_down(&addr) {
                        gossiper.change_status(&addr);
                    }
                }
            };

            thread::sleep(std::time::Duration::from_secs(1));
        });

        let schema = Arc::clone(&self.schema);
        let address = self.address.clone();
        let listener_client: TcpListener = TcpListener::bind(&self.address.c_address).unwrap();
        thread::spawn(move || {
            receive_client_message(listener_client, Arc::clone(&schema), &address)
        });
        let local_address_clone = self.address.i_address.clone();
        thread::spawn(move || {
            let address = local_address_clone.clone();
            let mut input = String::new();
            io::stdin()
                .read_line(&mut input)
                .expect("Error al leer la entrada");
            if input.to_lowercase().trim() == "exit" {
                let gossiper = get_gossiper();

                let msg = NodeMessage::TransferFromNode(address.clone());
                let bytes = msg.to_bytes();
                gossiper.get_sender(&address).unwrap().send(bytes).unwrap();
            }
        });
        for stream in listener_node.incoming() {
            match stream {
                Ok(socket) => {
                    let address_clone = self.address.clone();
                    let (tx_to_thread, rx_from_main) = channel();
                    let (tx_to_main, rx_from_thread) = channel();
                    let connection = Connection::new(tx_to_thread, rx_from_thread);
                    let schema = Arc::clone(&self.schema);
                    thread::spawn(move || {
                        receive_internal_message(
                            socket,
                            Arc::clone(&schema),
                            Some(connection),
                            address_clone,
                            rx_from_main,
                            tx_to_main,
                        )
                    });
                }
                Err(_) => {
                    return Err(ErrorTypes::new(
                        510,
                        "Error connecting to the server".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

pub fn write_log_message(address: &String, log_type: LogType, message: String) {
    let msg = format!(
        "{}   {:?}  {}\n",
        chrono::Utc::now().to_rfc3339(),
        log_type,
        message
    );
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(format!("node{}_log.log", address))
        .unwrap();
    print!("{}", msg);
    file.write_all(msg.as_bytes()).unwrap();
}

fn receive_client_message(
    listener: TcpListener,
    schema: Arc<Mutex<Schema>>,
    address: &Address,
) -> Result<(), ErrorTypes> {
    let mut file = File::open("identity.pfx").unwrap();
    let mut identity = vec![];
    file.read_to_end(&mut identity).unwrap();
    let identity = Identity::from_pkcs12(&identity, "").unwrap();

    let acceptor = TlsAcceptor::new(identity).unwrap();
    let acceptor = Arc::new(acceptor);

    for stream in listener.incoming() {
        match stream {
            Ok(client_stream) => {
                let acceptor = acceptor.clone();
                let schema = Arc::clone(&schema);
                let address = address.clone();
                thread::spawn(move || {
                    write_log_message(
                        &address.i_port,
                        LogType::Info,
                        "Client connected".to_string(),
                    );
                    let mut stream = acceptor.accept(client_stream).unwrap();
                    handle_client_message(&mut stream, Arc::clone(&schema), address).unwrap();
                });
            }
            Err(_) => {
                let error = ErrorTypes::new(511, "Error connecting to the server".to_string());
                write_log_message(
                    &address.i_port,
                    LogType::Error,
                    format!("{} {}", error.get().0, error.get().1),
                );
                return Err(error);
            }
        }
    }
    Ok(())
}

/// This function is responsible for handling the messages.
pub fn receive_internal_message(
    mut socket: TcpStream,
    schema: Arc<Mutex<Schema>>,
    connection: Option<Connection>,
    address: Address,
    rx: mpsc::Receiver<Vec<u8>>,
    tx: mpsc::Sender<Vec<u8>>,
) {
    let mut clone = socket.try_clone().unwrap();
    let node = Arc::new(Mutex::new(None));
    let node1 = Arc::clone(&node);
    thread::spawn(move || {
        match internal_message(&mut clone, connection, &schema, tx.clone(), address.clone()) {
            Ok(Some(address)) => {
                let _ = node.lock().unwrap().replace(address);
            }
            Ok(None) => {}
            Err(_) => return,
        }
        loop {
            match internal_message(&mut clone, None, &schema, tx.clone(), address.clone()) {
                Ok(Some(address)) => {
                    if node.lock().unwrap().is_none() {
                        let _ = node.lock().unwrap().replace(address);
                    }
                }
                Ok(None) => {}
                Err(_) => return,
            }
        }
    });

    for message in rx {
        if socket.write_all(&message).is_err() {
            let gossiper = get_gossiper();
            if let Some(address) = node1.lock().unwrap().as_ref() {
                if !gossiper.is_down(address) {
                    gossiper.change_status(address);
                }
            };
            return;
        };
    }
}

fn internal_message(
    socket: &mut TcpStream,
    connection: Option<Connection>,
    schema: &Arc<Mutex<Schema>>,
    tx: Sender<Vec<u8>>,
    address: Address,
) -> Result<Option<String>, ErrorTypes> {
    let mut source = [0; 1];
    if socket.read_exact(&mut source).is_ok() {
        if source[0] == NODE_MESSAGE {
            handle_node_message(socket, Arc::clone(schema), tx.clone())?;
            Ok(None)
        } else {
            handle_gossip_message(socket, connection, address, Arc::clone(schema))
        }
    } else {
        let e = ErrorTypes::new(512, "A node has disconnected".to_string());
        write_log_message(
            &address.i_port,
            LogType::Error,
            format!("{} {}", e.get().0, e.get().1),
        );
        Err(e)
    }
}

/// This function is responsible for handling the node messages.
fn handle_node_message(
    socket: &mut TcpStream,
    schema: Arc<Mutex<Schema>>,
    sender: Sender<Vec<u8>>,
) -> Result<(), ErrorTypes> {
    let mut len = [0; 1024];
    if socket.read(&mut len).is_ok() {
        let message = NodeMessage::from_bytes(len.to_vec());
        match message {
            NodeMessage::Confirmation() => {
                let _ = sender.send(message.to_bytes());
                return Ok(());
            }
            NodeMessage::SelectResponse(_) => {
                let _ = sender.send(message.to_bytes());
                return Ok(());
            }
            NodeMessage::ChecksumResponse(_) => {
                let _ = sender.send(message.to_bytes());
                return Ok(());
            }
            _ => {}
        }

        schema
            .lock()
            .unwrap()
            .execute_node_message(message, socket)?;

        Ok(())
    } else {
        Err(ErrorTypes::new(513, "Error reading message".to_string()))
    }
}

/// This function is responsible for handling the gossip messages.
fn handle_gossip_message(
    socket: &mut TcpStream,
    connection: Option<Connection>,
    address: Address,
    schema: Arc<Mutex<Schema>>,
) -> Result<Option<String>, ErrorTypes> {
    let message = Gossiper::receive_gossip_message(socket)?;
    let gossiper = get_gossiper();
    match message {
        GossipMessage::Syn(digests, source) => {
            gossiper.syn_handler(digests, source, &address, socket, connection, schema)?;
            //write_log_message(&address.i_port, LogType::Info, "Syn message received".to_string());
            Ok(None)
        }
        GossipMessage::Ack(digests, endpoint_states) => {
            gossiper.ack_handler(digests, endpoint_states, socket, &address)?;
            //write_log_message(&address.i_port,LogType::Info,"Ack message received".to_string());
            Ok(None)
        }
        GossipMessage::Ack2(endpoint_states) => {
            gossiper.ack2_handler(endpoint_states, &address);
            //write_log_message(                &address.i_port,                LogType::Info,                "Ack2 message received".to_string(),            );
            Ok(None)
        }
    }
}

/// This function is responsible for handling the client messages.
fn handle_client_message(
    client_stream: &mut TlsStream<TcpStream>,
    schema: Arc<Mutex<Schema>>,
    address: Address,
) -> Result<(), ErrorTypes> {
    let mut compression: Option<Compression> = None;
    loop {
        let mut buf = [0; 1024];
        match client_stream.read(&mut buf) {
            Ok(len) => {
                let message = receive_message(&mut buf[0..len].to_vec(), &compression);
                match message {
                    Err(_) => {
                        write_log_message(
                            &address.i_port,
                            LogType::Info,
                            format!("The client {} has disconnected", address.i_address),
                        );
                        return Ok(());
                    }
                    Ok(message) => {
                        if let SolicitationMessage(msg) = message {
                            handle_protocol_message(
                                msg,
                                client_stream,
                                Arc::clone(&schema),
                                &mut compression,
                                address.clone(),
                            )?;
                        }
                    }
                }
            }
            Err(_) => {
                //Se desconecto el cliente
                ErrorTypes::new(
                    514,
                    "Error reading message, the client has been disconnected".to_string(),
                );
                write_log_message(
                    &address.i_port,
                    LogType::Error,
                    "514 Error reading message, the client has been disconnected".to_string(),
                );
            }
        }
    }
}

/// This function is responsible for receiving a cassandra protocol message.
fn handle_protocol_message(
    message: RequestMessage,
    client_stream: &mut TlsStream<TcpStream>,
    schema: Arc<Mutex<Schema>>,
    compression_: &mut Option<Compression>,
    address: Address,
) -> Result<(), ErrorTypes> {
    match message {
        RequestMessage::StartUp { compression } => {
            let mut response = Protocol::new();
            *compression_ = compression;
            response.write_authenticate("PasswordAuthenticator")?;
            write_log_message(
                &address.i_port,
                LogType::Info,
                "Start up completed".to_string(),
            );
            client_stream.write_all(&response.get_binary()).unwrap();
            Ok(())
        }
        RequestMessage::AuthResponse { auth_response } => {
            let file = File::open("users.json").unwrap();
            let reader = BufReader::new(file);
            let users: Vec<User> = serde_json::from_reader(reader).unwrap();

            for account in users {
                if account.name == auth_response.0 && account.password == auth_response.1 {
                    let mut response = Protocol::new();
                    response.write_auth_success();
                    write_log_message(
                        &address.i_port,
                        LogType::Info,
                        "Client Authenticated".to_string(),
                    );
                    client_stream.write_all(&response.get_binary()).unwrap();
                    return Ok(());
                }
            }
            let e = ErrorTypes::new(
                515,
                format!(
                    "Authentication error: user {} does not exist.",
                    auth_response.0
                ),
            );
            write_log_message(
                &address.i_port,
                LogType::Error,
                format!("{} {}", e.get().0, e.get().1),
            );
            Err(e)
        }
        RequestMessage::Query(query, consistency, _original) => handle_query(
            query,
            consistency,
            client_stream,
            compression_,
            schema,
            address.clone(),
        ),
    }
}

/// This function is responsible for handling the queries.
fn handle_query(
    query: Query,
    consistency: Consistency,
    client_stream: &mut TlsStream<TcpStream>,
    compression: &Option<Compression>,
    schema: Arc<Mutex<Schema>>,
    address: Address,
) -> Result<(), ErrorTypes> {
    let mut response = Protocol::new();
    response.set_compress_algorithm(compression.clone());
    match query {
        Query::CreateTable {
            table_name,
            columns_type,
            clustering_key,
            primary_key,
        } => handle_query_create_table(
            schema,
            (table_name, columns_type, clustering_key, primary_key),
            address,
            client_stream,
            response,
        ),
        Query::Insert {
            table_name,
            columns_name,
            values,
        } => handle_query_insert(
            schema,
            (table_name, columns_name, values),
            address,
            consistency,
            client_stream,
            response,
        ),
        Query::CreateKeyspace {
            keyspace_name,
            replication,
        } => handle_query_create_keyspace(
            schema,
            address,
            keyspace_name,
            replication,
            client_stream,
            response,
        ),
        Query::Select {
            table_name,
            conditions,
            selected_columns,
            order,
        } => handle_query_select(
            schema,
            (table_name, conditions, selected_columns, order),
            address,
            consistency,
            client_stream,
            response,
        ),
        Query::Use { keyspace_name } => {
            handle_query_use(schema.clone(), keyspace_name, client_stream, response)
        }
        Query::Update {
            table_name,
            column_value,
            conditions,
        } => handle_query_update(
            schema.clone(),
            (table_name, column_value, conditions),
            consistency,
            address.i_address,
            client_stream,
            response,
        ),
        Query::Delete {
            table_name,
            conditions,
        } => handle_query_delete(
            schema.clone(),
            table_name,
            conditions,
            address,
            consistency,
            client_stream,
            response,
        ),
    }
}

type TableInfo = (String, Vec<(String, String)>, Vec<String>, Vec<String>);

fn handle_query_create_table(
    schema: Arc<Mutex<Schema>>,
    info_table: TableInfo,
    address: Address,
    client_stream: &mut TlsStream<TcpStream>,
    mut response: Protocol,
) -> Result<(), ErrorTypes> {
    let (table_name, columns_type, clustering_key, primary_key) = info_table;
    let mut schema_lock = schema.lock().unwrap();
    let result = schema_lock.create_table(
        &table_name,
        columns_type,
        clustering_key,
        primary_key,
        address.i_port.clone(),
    );
    if schema_lock.save_schema().is_err() {
        let e = ErrorTypes::new(516, "Error saving schema".to_string());
        write_log_message(
            &address.i_port,
            LogType::Error,
            format!("{} {}", e.get().0, e.get().1),
        );
        return Err(e);
    }
    drop(schema_lock);
    write_log_message(&address.i_port, LogType::Info, "Table created".to_string());
    match result {
        Ok(table) => {
            let gossiper = get_gossiper();
            let _ = gossiper.schema_change(NodeMessage::SchemaChange(SchemaChange::CreateTable(
                Box::new(table),
            )));
            response.write_result(
                ResultKind::SchemaChange,
                None,
                None,
                Some(SchemaChangeType::Created),
                Some("TABLE".to_string()),
                Some(&table_name),
            );
            client_stream.write_all(&response.get_binary()).unwrap();
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn handle_query_create_keyspace(
    schema: Arc<Mutex<Schema>>,
    address: Address,
    keyspace_name: String,
    replication: usize,
    client_stream: &mut TlsStream<TcpStream>,
    mut response: Protocol,
) -> Result<(), ErrorTypes> {
    let mut schema_lock = schema.lock().unwrap();
    let result = schema_lock.create_keyspace(&keyspace_name, replication);
    if schema_lock.save_schema().is_err() {
        let e = ErrorTypes::new(517, "Error saving schema".to_string());
        write_log_message(
            &address.i_port,
            LogType::Error,
            format!("{} {}", e.get().0, e.get().1),
        );
        return Err(e);
    }
    drop(schema_lock);
    match result {
        Ok(keyspace) => {
            let gossiper = get_gossiper();
            gossiper.schema_change(NodeMessage::SchemaChange(SchemaChange::CreateKeyspace(
                keyspace,
            )))?;
            response.write_result(
                ResultKind::SchemaChange,
                None,
                None,
                Some(SchemaChangeType::Created),
                Some("KEYSPACE".to_string()),
                Some(&keyspace_name),
            );
            client_stream.write_all(&response.get_binary()).unwrap();

            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn handle_query_select(
    schema: Arc<Mutex<Schema>>,
    info_select: (String, Clause, Vec<String>, Vec<String>),
    address: Address,
    consistency: Consistency,
    client_stream: &mut TlsStream<TcpStream>,
    mut response: Protocol,
) -> Result<(), ErrorTypes> {
    let (table_name, conditions, selected_columns, order) = info_select;
    let mut schema_lock = schema.lock().unwrap();
    let rows = schema_lock.execute_select(
        (table_name, conditions, selected_columns, order),
        &address,
        consistency,
    )?;
    drop(schema_lock);
    response.write_result(ResultKind::Rows, Some(rows), None, None, None, None);
    client_stream.write_all(&response.get_binary()).unwrap();
    Ok(())
}

fn handle_query_insert(
    schema: Arc<Mutex<Schema>>,
    info_insert: (String, Vec<String>, Vec<Vec<String>>),
    address: Address,
    consistency: Consistency,
    client_stream: &mut TlsStream<TcpStream>,
    mut response: Protocol,
) -> Result<(), ErrorTypes> {
    let (table_name, columns_name, values) = info_insert;
    let mut schema_lock = schema.lock().unwrap();

    schema_lock.execute_insert(table_name, values, columns_name, &address, consistency)?;
    drop(schema_lock);
    response.write_result(ResultKind::Void, None, None, None, None, None);
    client_stream.write_all(&response.get_binary()).unwrap();
    write_log_message(&address.i_port, LogType::Info, "Row inserted".to_string());
    Ok(())
}

fn handle_query_use(
    schema: Arc<Mutex<Schema>>,
    keyspace_name: String,
    client_stream: &mut TlsStream<TcpStream>,
    mut response: Protocol,
) -> Result<(), ErrorTypes> {
    let mut schema_lock = schema.lock().unwrap();
    let result = schema_lock.set_keyspace(&keyspace_name);
    if schema_lock.save_schema().is_err() {
        return Err(ErrorTypes::new(518, "Error saving schema".to_string()));
    }
    drop(schema_lock);
    match result {
        Ok(keyspace) => {
            response.write_result(
                ResultKind::SetKeyspace,
                None,
                Some(&keyspace_name),
                None,
                None,
                None,
            );
            let gossiper = get_gossiper();
            gossiper.schema_change(NodeMessage::SchemaChange(SchemaChange::UseKeyspace(
                keyspace,
            )))?;
            client_stream.write_all(&response.get_binary()).unwrap();
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn handle_query_update(
    schema: Arc<Mutex<Schema>>,
    info_update: (String, HashMap<String, String>, Clause),
    consistency: Consistency,
    address: String,
    client_stream: &mut TlsStream<TcpStream>,
    mut response: Protocol,
) -> Result<(), ErrorTypes> {
    let (table_name, column_value, conditions) = info_update;
    let mut schema_lock = schema.lock().unwrap();
    schema_lock.execute_update(table_name, column_value, conditions, address, consistency)?;
    drop(schema_lock);
    response.write_result(ResultKind::Void, None, None, None, None, None);
    client_stream.write_all(&response.get_binary()).unwrap();
    Ok(())
}

fn handle_query_delete(
    schema: Arc<Mutex<Schema>>,
    table_name: String,
    conditions: Clause,
    address: Address,
    consistency: Consistency,
    client_stream: &mut TlsStream<TcpStream>,
    mut response: Protocol,
) -> Result<(), ErrorTypes> {
    let mut schema_lock = schema.lock().unwrap();

    schema_lock.execute_delete(table_name, conditions, address.i_address, consistency)?;
    drop(schema_lock);
    response.write_result(ResultKind::Void, None, None, None, None, None);
    client_stream.write_all(&response.get_binary()).unwrap();
    write_log_message(&address.i_port, LogType::Info, "Rows deleted".to_string());
    Ok(())
}
