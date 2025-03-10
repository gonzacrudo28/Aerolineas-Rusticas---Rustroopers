use super::address::Address;
use super::connection::Connection;
use super::endpoint_state::EndpointState;
use super::gossip_digest::GossipDigest;
use super::gossip_message::GossipMessage;
use super::hashring::HashRing;
use super::hashring::NODOS;
use super::log_type::LogType;
use super::node_message::NodeMessage;
use super::nodes::receive_internal_message;
use super::nodes::write_log_message;
use super::schema::Schema;
use crate::errors::error_types::ErrorTypes;
use rand::seq::SliceRandom;
use rand::thread_rng;
use serde_json;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;

/// Struct responsible for managing the gossip state in a distributed system.
/// The `Gossiper` struct maintains the necessary data structures to handle the
/// gossip protocol, ensuring efficient state propagation and consistency across nodes.
pub struct Gossiper {
    pub endpoint_state_map: Mutex<HashMap<String, EndpointState>>,
    connections: Mutex<HashMap<String, Connection>>,
    neighbours: Mutex<Vec<String>>,
    hashring: Mutex<HashRing>,
    removed: Mutex<HashSet<String>>,
}
static GOSSIPER: OnceLock<Arc<Gossiper>> = OnceLock::new();

pub fn get_gossiper() -> Arc<Gossiper> {
    GOSSIPER
        .get_or_init(|| {
            Arc::new(Gossiper {
                endpoint_state_map: Mutex::new(HashMap::new()),
                connections: Mutex::new(HashMap::new()),
                neighbours: Mutex::new(Vec::new()),
                hashring: Mutex::new(HashRing::new()),
                removed: Mutex::new(HashSet::new()),
            })
        })
        .clone()
}

impl Gossiper {
    pub fn get_replicas(
        &self,
        key: u128,
        rf: usize,
        local: &String,
    ) -> Result<Vec<String>, ErrorTypes> {
        self.hashring.lock().unwrap().get_replicas(key, rf, local)
    }

    pub fn get_sender(&self, address: &String) -> Option<Sender<Vec<u8>>> {
        let connections = self.connections.lock().unwrap();
        if let Some(sender) = connections.get(address) {
            return Some(sender.get_sender());
        }
        None
    }
    pub fn receive(&self, address: &String) -> Vec<u8> {
        let connections = self.connections.lock().unwrap();
        if let Some(connection) = connections.get(address) {
            return connection.receive();
        }
        Vec::new()
    }
    /// This function is responsible for returning the endpoint state.
    pub fn get_endpoint_state(&self, endpoint: &str) -> Option<EndpointState> {
        self.endpoint_state_map
            .lock()
            .unwrap()
            .get(endpoint)
            .cloned()
    }

    /// This function is responsible for adding the endpoint state to the gossip state.
    pub fn put_endpoint_state(&self, endpoint: String, endpoint_state: EndpointState) {
        self.endpoint_state_map
            .lock()
            .unwrap()
            .insert(endpoint, endpoint_state);
    }

    /// This function is responsible for comparing the digests of the endpoints.
    pub fn compare_endpoints(
        &self,
        digests: Vec<GossipDigest>,
    ) -> (Vec<GossipDigest>, Vec<EndpointState>) {
        let mut digests_to_request = Vec::new();
        let mut endpoints_to_sync = Vec::new();
        for digest in digests {
            if let Some(endpoint_state) = self.get_endpoint_state(digest.get_endpoint_address()) {
                if digest.compare_digests(endpoint_state.to_digest()) > 0 {
                    digests_to_request.push(endpoint_state.to_digest());
                } else {
                    endpoints_to_sync.push(endpoint_state.clone());
                }
            } else {
                digests_to_request.push(digest);
            }
        }
        (digests_to_request, endpoints_to_sync)
    }

    /// This function is responsible for trying to connect to an endpoint.
    pub fn try_connect(
        &self,
        endpoint_address: &String,
        schema: Arc<Mutex<Schema>>,
        address: &Address,
        need_connection: bool,
    ) {
        if let Ok(mut neighbours) = self.neighbours.lock() {
            if neighbours.contains(endpoint_address)
                || self.removed.lock().unwrap().contains(endpoint_address)
            {
                return;
            }
            self.add_node(endpoint_address.to_string());

            if *endpoint_address != address.i_address {
                neighbours.push(endpoint_address.to_string());
            }

            if need_connection {
                self.establish_connection(endpoint_address, address, Arc::clone(&schema));
            }
            if self.hashring.lock().unwrap().quantity <= NODOS {
                return;
            }
            schema
                .lock()
                .unwrap()
                .new_node(endpoint_address, &address.i_address);
        }
    }

    pub fn get_partitions(
        &self,
        endpoint_address: &String,
        local: &String,
        rf: usize,
    ) -> Vec<(u128, u128)> {
        self.hashring
            .lock()
            .unwrap()
            .get_partitions(endpoint_address, local, rf)
    }

    pub fn get_neighbours(&self) -> Vec<String> {
        self.neighbours.lock().unwrap().clone()
    }

    pub fn establish_connection(
        &self,
        endpoint_address: &String,
        address: &Address,
        schema: Arc<Mutex<Schema>>,
    ) {
        if self
            .connections
            .lock()
            .unwrap()
            .contains_key(endpoint_address)
        {
            return;
        }
        let (tx_to, rx_to) = channel();
        let (tx_from, rx_from) = channel();
        let connection = Connection::new(tx_to, rx_from);
        self.connections
            .lock()
            .unwrap()
            .insert(endpoint_address.to_string(), connection);
        let address = address.clone();
        if let Ok(client_stream) = TcpStream::connect(endpoint_address.clone()) {
            thread::spawn(move || {
                receive_internal_message(client_stream, schema, None, address, rx_to, tx_from);
            });
        }
    }

    /// This function is responsible for adding a node to the hashring.
    pub fn add_node(&self, endpoint_address: String) {
        self.hashring.lock().unwrap().add_node(endpoint_address);
    }

    /// This function is responsible for updating the endpoint state map.
    pub fn update_endpoint_state(&self, endpoint_state: EndpointState, local: &String) {
        let address = endpoint_state.get_address();
        let actual = self.get_endpoint_state(&address);
        if let Some(actual) = actual {
            if address == *local && actual.get_generation() > endpoint_state.get_generation() {
                return;
            }
        }

        self.endpoint_state_map
            .lock()
            .unwrap()
            .insert(address, endpoint_state);
    }

    /// This function is responsible for handling the `Syn` message type.
    pub fn gossip(
        &self,
        adrs: Address,
        schema: Arc<Mutex<Schema>>,
    ) -> Result<Option<String>, String> {
        self.endpoint_state_map
            .lock()
            .unwrap()
            .get_mut(&adrs.i_address)
            .unwrap()
            .increment_heartbeat();
        let digests: Vec<GossipDigest> = self
            .endpoint_state_map
            .lock()
            .unwrap()
            .values()
            .map(|x| x.to_digest())
            .collect();
        let mut rng = thread_rng();
        let neighbours = self.neighbours.lock().unwrap();
        let adresses = neighbours
            .choose_multiple(&mut rng, usize::min(3, neighbours.len()))
            .collect::<Vec<&String>>();
        let message = GossipMessage::Syn(digests, adrs.i_address.clone());
        for address in adresses {
            let syn = message.to_bytes();
            if self.get_sender(address).is_none() {
                self.establish_connection(address, &adrs, Arc::clone(&schema));
            }
            let sender = self.get_sender(address).unwrap();
            if sender.send(syn).is_err() {
                return Err(address.to_string());
            }
            //write_log_message(&adrs.i_address, LogType::Info, "Syn message sent".to_string());
        }

        Ok(None)
    }

    pub fn is_down(&self, address: &String) -> bool {
        if let Some(endpoint) = self.endpoint_state_map.lock().unwrap().get(address) {
            return endpoint.is_down();
        }
        false
    }

    pub fn change_status(&self, address: &String) {
        let mut map = self.endpoint_state_map.lock().unwrap();
        if let Some(endpoint) = map.get_mut(address) {
            endpoint.change_status();
        } else {
            let e = ErrorTypes::new(519, "Error encripting gossip message".to_string());
            let a = address
                .split(":")
                .map(|s| s.to_string())
                .collect::<Vec<String>>();
            write_log_message(
                &a[1],
                LogType::Error,
                format!("{} {}", e.get().0, e.get().1),
            );
        }
    }

    pub fn get_node(&self, key: Vec<&String>) -> (Option<String>, u128) {
        self.hashring.lock().unwrap().get_node(key)
    }

    pub fn get_partitions_remove(
        &self,
        node: &String,
        rf: usize,
    ) -> HashMap<String, Vec<(u128, u128)>> {
        self.hashring
            .lock()
            .unwrap()
            .get_partitions_remove(node, rf)
    }

    pub fn schema_change(&self, data: NodeMessage) -> Result<(), ErrorTypes> {
        let mut agreed = 0;
        let lock = self.neighbours.lock().unwrap();
        let neighbours = lock.clone();
        drop(lock);
        for neighbour in neighbours {
            if let Some(sender) = self.get_sender(&neighbour) {
                if sender.send(data.to_bytes()).is_err() {
                    continue;
                }
            }

            let bytes = self
                .connections
                .lock()
                .unwrap()
                .get(&neighbour)
                .unwrap()
                .receive();

            let message = NodeMessage::from_bytes(bytes[1..].to_vec());
            if let NodeMessage::Confirmation() = message {
                agreed += 1;
            }
        }
        if agreed >= self.neighbours.lock().unwrap().len() / 2 {
            Ok(())
        } else {
            Err(ErrorTypes::new(510, "Error changing schema".to_string()))
        }
    }

    /// This function is responsible for handling the `Ack` message type. It will update the endpoint states and send an `Ack2` message back to the sender with the relevant information.
    pub fn ack_handler(
        &self,
        digests: Vec<GossipDigest>,
        states: Vec<EndpointState>,
        socket: &mut TcpStream,
        address: &Address,
    ) -> Result<(), ErrorTypes> {
        for state in states {
            self.update_endpoint_state(state, &address.i_address);
        }

        let mut requested_endpoints: Vec<EndpointState> = Vec::new();

        for digest in digests {
            let endpoint = self.get_endpoint_state(digest.get_endpoint_address());
            if let Some(endpoint) = endpoint {
                requested_endpoints.push(endpoint);
            }
        }
        let ack2_message = GossipMessage::Ack2(requested_endpoints);

        if socket.write_all(&ack2_message.to_bytes()).is_err() {
            return Err(ErrorTypes::new(
                510,
                "Error sending gossip message".to_string(),
            ));
        }
        //write_log_message(&address.i_address, LogType::Info, "Ack2 message sent".to_string());
        Ok(())
    }

    /// This function is responsible for handling the `Syn` message type. It will attempt to connect to the node that sent the message and compare the endpoint states. It will then send an `Ack` message back to the sender with the relevant information.
    pub fn syn_handler(
        &self,
        digests: Vec<GossipDigest>,
        from_address: String,
        address: &Address,
        socket: &mut TcpStream,
        connection: Option<Connection>,
        schema: Arc<Mutex<Schema>>,
    ) -> Result<Option<String>, ErrorTypes> {
        if let Some(connection) = connection {
            if self.get_sender(&from_address).is_some() && !self.is_down(&from_address) {
                return Err(ErrorTypes::new(574, "Already have a sender".to_string()));
            }
            self.connections
                .lock()
                .unwrap()
                .insert(from_address.clone(), connection);
        }

        self.try_connect(&from_address, schema.clone(), address, false);
        for digest in digests.iter() {
            if digest.get_endpoint_address() == &address.i_address {
                continue;
            };
            self.try_connect(
                &digest.get_endpoint_address().clone(),
                schema.clone(),
                address,
                false,
            );
        }

        let (digests_to_request, endpoints_to_sync) = self.compare_endpoints(digests);

        let ack_message = GossipMessage::Ack(digests_to_request, endpoints_to_sync);
        socket.write_all(&ack_message.to_bytes()).unwrap();
        //write_log_message(&address.i_address, LogType::Info, "Ack message sent".to_string());
        Ok(Some(from_address))
    }

    /// This function is responsible for handling the `Ack2` message type. It will update the endpoint states.
    pub fn ack2_handler(&self, states: Vec<EndpointState>, address: &Address) {
        for state in states {
            self.update_endpoint_state(state, &address.i_address);
        }
    }

    pub fn remove_node(&self, node: &String) {
        let mut ep_lock = self.endpoint_state_map.lock().unwrap();
        let mut hashring_lock = self.hashring.lock().unwrap();
        let mut connections = self.connections.lock().unwrap();
        let mut neighbours = self.neighbours.lock().unwrap();
        let mut removed = self.removed.lock().unwrap();
        connections.remove(node);
        hashring_lock.remove_node(node.to_string());
        ep_lock.remove(node);
        neighbours.retain(|x| x != node);
        removed.insert(node.to_string());
    }
    /// This function is responsible for receiving a gossip message and returning it.
    pub fn receive_gossip_message(socket: &mut TcpStream) -> Result<GossipMessage, ErrorTypes> {
        let mut len = [0; 8];
        let mut size = 0;

        if socket.read_exact(&mut len).is_err() {
            return Err(ErrorTypes::new(
                510,
                "Error reading gossip message".to_string(),
            ));
        }

        for item in len.iter() {
            size = size << 8 | *item as usize;
        }

        let mut buf = vec![0; size];

        if socket.read_exact(&mut buf).is_err() {
            return Err(ErrorTypes::new(
                510,
                "Error reading gossip message".to_string(),
            ));
        }
        if let Ok(message) =
            serde_json::from_str::<GossipMessage>(String::from_utf8(buf.to_vec()).unwrap().as_str())
        {
            Ok(message)
        } else {
            Err(ErrorTypes::new(
                510,
                "Error reading gossip message".to_string(),
            ))
        }
    }
}
