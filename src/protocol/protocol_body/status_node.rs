/// Represents the status of a node in an Event message.
///
/// This enum is used to indicate the current status of a node in the system.
/// It is typically used in event messages to represent whether a node is active or inactive.
///
/// ### Variants:
/// - **Up**: Represents a node that is currently active and operational.
/// - **Down**: Represents a node that is currently inactive or not operational.
#[derive(Clone, Debug)]
pub enum StatusNode {
    Up,
    Down,
}
