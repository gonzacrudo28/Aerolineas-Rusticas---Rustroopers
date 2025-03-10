use std::sync::mpsc::{Receiver, Sender};

/// Represents a connection between two threads using channels.
///
/// This struct manages communication between threads by utilizing `Sender` and `Receiver` to send and receive messages.
/// It is used to facilitate the exchange of data between different parts.
///
/// ## Fields:
/// - `to`: A `Sender<Vec<u8>>` used to send messages from the current thread to another.
/// - `from`: A `Receiver<Vec<u8>>` used to receive messages from another thread.
///
/// ## Methods:
/// - `new(to: Sender<Vec<u8>>, from: Receiver<Vec<u8>>) -> Self`: Creates a new instance of `Connection` with the provided sender and receiver.
/// - `get_sender(&self) -> Sender<Vec<u8>>`: Returns a clone of the sender, allowing the caller to send messages through the connection.
/// - `send(&self, message: Vec<u8>)`: Sends a message (as a `Vec<u8>`) through the connection using the sender.
/// - `receive(&self) -> Vec<u8>`: Receives a message (as a `Vec<u8>`) from the connection using the receiver.

#[derive(Debug)]
pub struct Connection {
    to: Sender<Vec<u8>>,
    from: Receiver<Vec<u8>>,
}

impl Connection {
    pub fn new(to: Sender<Vec<u8>>, from: Receiver<Vec<u8>>) -> Self {
        Connection { to, from }
    }
    pub fn get_sender(&self) -> Sender<Vec<u8>> {
        self.to.clone()
    }

    pub fn send(&self, message: Vec<u8>) {
        self.to.send(message).unwrap();
    }

    pub fn receive(&self) -> Vec<u8> {
        self.from.recv().unwrap_or_default()
    }
}
