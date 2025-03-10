use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]

/// This enum represents the status of the node.
pub enum Status {
    Up,
    Down,
}
