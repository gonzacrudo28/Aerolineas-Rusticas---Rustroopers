/// Enum representing the different types of Result messages that can be received after sending a query.
///
/// This enum defines the possible types of responses a Cassandra server can return after executing a query.
/// Each variant corresponds to a specific type of result message, represented by a unique hexadecimal code.
///
/// ## Variants:
/// - `Void (0x0001)`: Indicates that no result is returned. Typically used for operations like `INSERT` or `DELETE`
///   where no data is expected to be returned.
/// - `Rows (0x0002)`: Represents a response containing rows of data. Used in `SELECT` queries that return data.
/// - `SetKeyspace (0x0003)`: Indicates that the keyspace has been changed in the database. Used for operations that modify
///   the active keyspace.
/// - `Prepared (0x0004)`: Represents a message that returns a prepared query. Used when a query is prepared for later execution,
///   improving performance by reusing query plans.
/// - `SchemaChange (0x0005)`: Represents a schema change event, indicating that a schema change operation (like creating or
///   dropping tables) has occurred.
#[derive(Debug, PartialEq)]
pub enum Result {
    Void = 0x0001,
    Rows = 0x0002,
    SetKeyspace = 0x0003,
    Prepared = 0x0004,
    SchemaChange = 0x0005,
}
