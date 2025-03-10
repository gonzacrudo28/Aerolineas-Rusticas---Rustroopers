use super::status::Status;
use serde::{Deserialize, Serialize};

/// Represents the state of an application node, managing its status and address.
///
/// This struct encapsulates the current state of an application node, including its status (either "Up" or "Down") and its network address.
///
/// ## Fields:
/// - `status`: The current status of the node, which can either be `Up` or `Down`. This is represented by the `Status` enum.
/// - `address`: A `String` that contains the network address of the node.
///
/// ## Methods:
/// - `new(status: Status, address: String) -> Self`: Constructs a new `ApplicationState` instance with the specified status and address.
/// - `get_address(&self) -> Option<&String>`: Returns a reference to the address of the node, wrapped in an `Option` for safe handling of potential `None` values.
/// - `change_status(&mut self)`: Toggles the status of the node between `Up` and `Down`.
/// - `is_down(&self) -> bool`: Returns `true` if the node's status is `Down`, otherwise `false`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplicationState {
    status: Status,
    address: String,
}

impl ApplicationState {
    pub fn new(status: Status, address: String) -> Self {
        ApplicationState { status, address }
    }

    /// This function returns the addres of the node.
    pub fn get_address(&self) -> Option<&String> {
        Some(&self.address)
    }
    /// This function changes the state of the node (Up or Down).
    pub fn change_status(&mut self) {
        self.status = match self.status {
            Status::Up => Status::Down,
            Status::Down => Status::Up,
        }
    }

    pub fn is_down(&self) -> bool {
        self.status == Status::Down
    }
}
