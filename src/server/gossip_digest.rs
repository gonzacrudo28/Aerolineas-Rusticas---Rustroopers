use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]

/// Struct representing a "GossipDigest" object.
///
/// A GossipDigest is used in the context of the gossip protocol to summarize
/// the state of a node in a distributed system. It contains the following fields:
///
/// ### Fields:
/// - `endpoint_address` (`String`)
/// - `generation` (`i32`)
/// - `max_version` (`i32`)
pub struct GossipDigest {
    endpoint_address: String,
    generation: i32,
    max_version: i32,
}

impl GossipDigest {
    pub fn new(endpoint_address: String, generation: i32, max_version: i32) -> GossipDigest {
        GossipDigest {
            endpoint_address,
            generation,
            max_version,
        }
    }

    /// This function is responsible for comparing the digests of two nodes. It will return a value of 0 if the digests are equal, a value greater than 0 if the first digest is greater than the second, and a value less than 0 if the first digest is less than the second.
    pub fn compare_digests(self, g: GossipDigest) -> i32 {
        if self.generation != g.generation {
            return self.generation - g.generation;
        }
        self.max_version - g.max_version
    }

    /// This function is responsible for returning the endpoint address of the node.
    pub fn get_endpoint_address(&self) -> &String {
        &self.endpoint_address
    }
}
