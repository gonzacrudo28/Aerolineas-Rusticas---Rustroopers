/// Struct representing the address of a node.
///
/// This structure is used to store the internal and client-facing addresses,
/// as well as the corresponding ports for a node.
///
/// ### Fields:
/// - `i_address` (`String`): The internal address of the node.
/// - `c_address` (`String`): The client-facing address of the node.
/// - `i_port` (`String`): The internal port used by the node.
#[derive(Debug, Clone)]
pub struct Address {
    pub i_address: String,
    pub c_address: String,
    pub i_port: String,
}
