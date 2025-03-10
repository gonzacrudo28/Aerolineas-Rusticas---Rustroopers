use crate::protocol::protocol_notations::flags_row::FlagsRow;

/// Represents the different types of Result responses that can be sent by the server.
///
/// This enum categorizes the different types of result responses that can be returned by the server in response to a query.
/// Each variant represents a distinct result type that may include various pieces of information depending on the operation performed.
///
/// ## Variants:
/// - `Void`: Represents a void response, indicating that the query executed successfully without returning any data.
///   
/// - `Rows`: Represents a response that contains query results in the form of rows.
///   - `metadata`: A `FlagsRow` that provides metadata related to the result set (e.g., information on whether there are more pages of results).
///   - `rows`: A vector of vectors of strings, where each inner vector represents a row, and each string represents a value in that row.
///   
/// - `SetKeyspace`: Represents a response that indicates the keyspace has been set or modified.
///   - `keyspace`: A string containing the name of the keyspace that has been set or changed.
///   
/// - `SchemaChange`: Represents a response indicating a schema change operation (e.g., table creation or modification).
///   - `change_type`: A string describing the type of schema change (e.g., "CREATE", "ALTER", etc.).
///   - `target`: A string describing the target of the schema change (e.g., the name of the table or column).
///   - `options`: A string that contains additional options or details about the schema change.
#[derive(Debug)]
pub enum ResultResponse {
    Void,
    Rows {
        metadata: FlagsRow,
        rows: Vec<Vec<String>>,
    },
    SetKeyspace {
        keyspace: String,
    },
    SchemaChange {
        change_type: String,
        target: String,
        options: String,
    },
}
