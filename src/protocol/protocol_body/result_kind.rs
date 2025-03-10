/// Represents the type of a Result message.
///
/// This enum categorizes the different types of result messages that can be returned
/// from a server in response to a query. Each variant corresponds to a specific
/// result type, indicating the outcome or content of the query response.
///
/// ### Variants:
/// - **Void**: Represents a result with no content (0x0001).
/// - **Rows**: Represents a result containing rows of data (0x0002).
/// - **SetKeyspace**: Represents a result indicating the setting of a keyspace (0x0003).
/// - **Prepared**: Represents a prepared statement result (0x0004).
/// - **SchemaChange**: Represents a result indicating a schema change (0x0005).
#[derive(Copy, Clone, Debug)]
pub enum ResultKind {
    Void = 0x0001,
    Rows = 0x0002,
    SetKeyspace = 0x0003,
    Prepared = 0x0004,
    SchemaChange = 0x0005,
}
