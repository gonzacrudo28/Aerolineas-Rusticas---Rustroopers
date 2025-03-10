use super::{endpoint_state::EndpointState, gossip_digest::GossipDigest};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug)]
/// Enum representing the different types of gossip messages exchanged between nodes.
///
/// Gossip messages are used to propagate state information
/// and maintain consistency among nodes. This enum defines the three primary types
/// of gossip messages and their associated data.
pub enum GossipMessage {
    /// Syn represents the message that starts the gossip process.
    Syn(Vec<GossipDigest>, String),
    /// Ack represents the message that acknowledges the receipt of a Syn message.
    Ack(Vec<GossipDigest>, Vec<EndpointState>),
    /// Ack2 represents the message that acknowledges the receipt of an Ack message and sends the information requested by the Ack transmitter.
    Ack2(Vec<EndpointState>),
}
impl GossipMessage {
    /// This function is responsible for converting the gossip message into a byte array.
    pub fn to_bytes(&self) -> Vec<u8> {
        let msg = serde_json::to_string(self).unwrap();
        let vec_msg = msg.as_bytes();
        let len = vec_msg.len().to_be_bytes();
        let mut send_message = [len.as_slice(), vec_msg].concat();

        send_message.insert(0, 0x02);
        send_message
    }
}
