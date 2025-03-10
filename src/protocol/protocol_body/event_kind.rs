use super::{
    schema_change::SchemaChangeType, status_node::StatusNode, topology_change::TopologyChangeType,
};

/// Represents the type of an Event message in the system.
///
/// This enum categorizes the different types of events that can occur in the system.
/// Each variant corresponds to a specific event related to changes in topology, node status,
/// or schema changes.
///
/// ### Variants:
/// - **Topology**: Represents changes in the network topology, using `TopologyChangeType` to describe the change.
/// - **Status**: Represents changes in the status of a node, using `StatusNode` to describe the new status.
/// - **Schema**: Represents schema changes, using `SchemaChangeType` to describe the type of schema change.
#[derive(Clone)]

pub enum EventKindChange {
    Topology(TopologyChangeType),
    Status(StatusNode),
    Schema(SchemaChangeType),
}
