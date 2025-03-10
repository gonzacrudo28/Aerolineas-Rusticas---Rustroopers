use crate::protocol::protocol_body::compression::Compression;
use crate::protocol::protocol_notations::consistency::Consistency;
use crate::protocol::query_parser::query::Query;
/// Represents the different types of Request messages that can be sent to the server.
///
/// This enum categorizes the types of requests a client can send to the server, encapsulating various kinds of messages
/// with different parameters depending on the type of request. Each variant corresponds to a specific action or operation
/// requested by the client.
///
/// ## Variants:
/// - `StartUp`: Represents the start-up request sent by the client to initialize the connection.
///   - `compression`: An optional field to specify the compression algorithm used for the request (if any).
///   
/// - `AuthResponse`: Represents an authentication response sent by the client to the server.
///   - `auth_response`: A tuple containing the authentication username and password.
///   
/// - `Options`: Represents a request to retrieve the server's options.
///   - `options`: A string representing the specific options the client is requesting.
///   
/// - `Register`: Represents a request to register for certain events or notifications from the server.
///   - `string_list`: A list of strings that specifies the events the client wants to register for.
///   
/// - `Query`: Represents a query sent to the server to execute an operation.
///   - `Query`: A `Query` object that defines the query to be executed.
///   - `Consistency`: The consistency level for the query to ensure how the data is replicated or distributed.
///   - `String`: An additional string (e.g., keyspace or session-related information) to include with the query.
#[derive(Debug)]
pub enum RequestMessage {
    StartUp { compression: Option<Compression> },
    AuthResponse { auth_response: (String, String) },
    Query(Query, Consistency, String),
}
