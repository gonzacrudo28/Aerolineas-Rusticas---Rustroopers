use super::{keyspace::Keyspace, mem_table::MemTable};
use crate::protocol::query_parser::clause::Clause;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
/// This enum represents the internal messages that are sent between nodes
pub enum NodeMessage {
    SchemaChange(SchemaChange),
    Insert(Vec<String>, Vec<String>, String, u128),
    SelectRequest(Clause, Vec<String>, Vec<String>, String, bool),
    SelectResponse(Vec<Vec<String>>),
    ChecksumRequest(Clause, Vec<String>, Vec<String>, String),
    ChecksumResponse(String),
    Update(u128, String, HashMap<String, String>, Clause),
    Delete(String, Clause),
    Confirmation(),
    TransferFromNode(String),
    RemoveNode(String),
}

impl NodeMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        let msg = serde_json::to_string(self).unwrap();
        let vec_msg = msg.as_bytes();
        let len = vec_msg.len().to_be_bytes();
        let mut send_message = [len.as_slice(), vec_msg].concat();
        send_message.insert(0, 0x01);
        send_message
    }

    pub fn from_bytes(bytes: Vec<u8>) -> NodeMessage {
        let mut len = bytes;
        let bytes = len.split_off(8);

        let len = u64::from_be_bytes(len.try_into().unwrap()) as usize;
        serde_json::from_str::<NodeMessage>(
            String::from_utf8(bytes[..len].to_vec()).unwrap().as_str(),
        )
        .unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
/// This enum represents the schema changes that can be made to the database
pub enum SchemaChange {
    CreateKeyspace(Keyspace),
    CreateTable(Box<MemTable>),
    UseKeyspace(Keyspace),
}
