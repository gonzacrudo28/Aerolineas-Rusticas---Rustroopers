/// Represents changes in the cluster topology.
///
/// This enum is used to indicate the type of change that has occurred in the cluster's topology.
/// It is commonly used in event messages to represent changes in the configuration of the cluster,
/// such as the addition, removal, or movement of nodes.
///
/// ### Variants:
/// - **NewNode**: Represents the addition of a new node to the cluster.
/// - **RemovedNode**: Represents the removal of a node from the cluster.
/// - **MovedNode**: Represents the movement or reallocation of a node within the cluster.
#[derive(Debug, Clone, Copy)]
pub enum TopologyChangeType {
    NewNode,
    RemovedNode,
    MovedNode,
}
