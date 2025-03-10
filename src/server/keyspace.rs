use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
#[derive(Debug, Clone, Eq, Hash, PartialEq)]

/// This struct represents the Keyspace object. It contains the name of the keyspace and the replication factor.
pub struct Keyspace {
    pub name: String,
    pub replication: usize,
}

impl Serialize for Keyspace {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let keyspace_string = format!("{}:{}", self.name, self.replication);
        serializer.serialize_str(&keyspace_string)
    }
}

impl<'de> Deserialize<'de> for Keyspace {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let parts: Vec<&str> = s.split(':').collect();
        let name = parts[0].to_string();
        let replication = parts[1].parse::<usize>().map_err(D::Error::custom)?;
        Ok(Keyspace { name, replication })
    }
}

impl Keyspace {
    pub fn new(name: String, replication: usize) -> Self {
        Self { name, replication }
    }
    pub fn get_name(&self) -> &str {
        &self.name
    }
}
