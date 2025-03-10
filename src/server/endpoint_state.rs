use super::application_state::ApplicationState;
use super::gossip_digest::GossipDigest;
use super::heartbeat_state::HeartbeatState;
use serde::{Deserialize, Serialize};

/// This struct is responsible for managing the endpoint state.
///
/// The `EndpointState` struct stores and manages the state related to an endpoint, which includes the state
/// of the heartbeat mechanism and the application-specific state. This allows tracking and managing the health
/// and status of the endpoint in a networked application.
///
/// ## Fields:
/// - `heartbeat_state`: The current state of the heartbeat mechanism, represented by `HeartbeatState`.
/// - `application_states`: The application-specific state of the endpoint, represented by `ApplicationState`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EndpointState {
    pub heartbeat_state: HeartbeatState,
    pub application_states: ApplicationState,
}

impl EndpointState {
    pub fn new(heart_beat: HeartbeatState, application_state: ApplicationState) -> Self {
        EndpointState {
            heartbeat_state: heart_beat,
            application_states: application_state,
        }
    }

    /// This function is responsible for incrementing the heartbeat.
    pub fn increment_heartbeat(&mut self) {
        self.heartbeat_state.heartbeat += 1;
    }

    /// This function is responsible for transforming the endpoint state into a compressed, more efficient message format known as a `GossipDigest`. It compiles the relevant information from the `EndpointState`, such as the address and heartbeat state, and constructs a new `GossipDigest` object with it.
    pub fn to_digest(&self) -> GossipDigest {
        GossipDigest::new(
            self.application_states.get_address().unwrap().clone(),
            self.heartbeat_state.generation,
            self.heartbeat_state.heartbeat,
        )
    }

    /// This function is responsible for returning the address of the node.
    pub fn get_address(&self) -> String {
        self.application_states.get_address().unwrap().clone()
    }

    pub fn is_down(&self) -> bool {
        self.application_states.is_down()
    }

    /// This function is responsible for changing the status of the node (Up or Down).
    pub fn change_status(&mut self) {
        self.application_states.change_status();
        self.heartbeat_state.generation += 1;
    }

    pub fn get_generation(&self) -> i32 {
        self.heartbeat_state.heartbeat
    }
}
