use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
/// This struct represents the state of the heartbeat.
pub struct HeartbeatState {
    pub generation: i32,
    pub heartbeat: i32,
}

impl Default for HeartbeatState {
    fn default() -> Self {
        Self::new()
    }
}

impl HeartbeatState {
    pub fn new() -> HeartbeatState {
        HeartbeatState {
            generation: 0,
            heartbeat: 0,
        }
    }
    /// This function is responsible for incrementing the heartbeat.
    pub fn increment_heartbeat(&mut self) {
        self.heartbeat += 1;
    }

    pub fn get_generation(&self) -> i32 {
        self.generation
    }
}
