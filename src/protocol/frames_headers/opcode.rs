#[derive(Debug, Clone, Copy, PartialEq)]
/// Represents the different types of opcodes that can be set in the header of a frame.
///
/// Opcodes define the type of operation or message being communicated in a frame. Each opcode
/// corresponds to a specific command, response, or event within the protocol.
///
/// ### Opcodes:
/// - **Error (0x00)**: Indicates an error in processing the frame.
/// - **StartUp (0x01)**: Signals the start of a connection handshake.
/// - **Ready (0x02)**: Confirms readiness after a successful handshake.
/// - **Authenticate (0x03)**: Initiates authentication procedures.
/// - **Options (0x05)**: Requests supported options from the server.
/// - **Supported (0x06)**: Lists options supported by the server.
/// - **Query (0x07)**: Sends a query to be executed.
/// - **Result (0x08)**: Contains the result of a query or operation.
/// - **Prepare (0x09)**: Prepares a query for execution.
/// - **Execute (0x0A)**: Executes a prepared query.
/// - **Register (0x0B)**: Registers for server events.
/// - **Event (0x0C)**: Represents an event notification from the server.
/// - **Batch (0x0D)**: Sends a batch of queries for execution.
/// - **AuthChallenge (0x0E)**: Represents a challenge in the authentication process.
/// - **AuthResponse (0x0F)**: Responds to an authentication challenge.
/// - **AuthSuccess (0x10)**: Indicates successful authentication.
pub enum Opcode {
    Error = 0x00,
    StartUp = 0x01,
    Ready = 0x02,
    Authenticate = 0x03,
    Query = 0x07,
    Result = 0x08,
    AuthResponse = 0x0F,
    AuthSuccess = 0x10,
}
