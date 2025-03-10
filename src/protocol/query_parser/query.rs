use super::clause::Clause;
use std::collections::HashMap;
#[derive(Debug, PartialEq)]

/// This enum represents the different types of Queries that can be sent to the server.
pub enum Query {
    Insert {
        table_name: String,
        columns_name: Vec<String>,
        values: Vec<Vec<String>>,
    },
    Update {
        table_name: String,
        column_value: HashMap<String, String>,
        conditions: Clause,
    },
    Delete {
        table_name: String,
        conditions: Clause,
    },
    Select {
        table_name: String,
        selected_columns: Vec<String>,
        conditions: Clause,
        order: Vec<String>,
    },
    CreateTable {
        table_name: String,
        columns_type: Vec<(String, String)>,
        clustering_key: Vec<String>,
        primary_key: Vec<String>,
    },
    CreateKeyspace {
        keyspace_name: String,
        replication: usize,
    },
    Use {
        keyspace_name: String,
    },
}
