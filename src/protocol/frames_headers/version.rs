#[derive(Debug, Clone, Copy, PartialEq)]
/// Represents the different protocol versions used in messages.
///
/// The `Version` enum distinguishes between requests and responses within the protocol.
/// Each version is encoded as a specific byte value.
///
/// ### Variants:
/// - **Request (0x05)**: Indicates that the message is a request.
/// - **Response (0x85)**: Indicates that the message is a response.
pub enum Version {
    Request = 0x05,
    Response = 0x85,
}
