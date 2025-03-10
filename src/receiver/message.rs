use crate::receiver::{request_message::RequestMessage, response_message::ResponseMessage};

/// Represents the different types of messages that can be sent to the server.
///
/// This enum is used to categorize the types of messages in the communication protocol between a client and the server.
/// It distinguishes between two main types of messages: requests and responses, encapsulating them in the respective variants.
///
/// ## Variants:
/// - `SolicitationMessage`: Represents a request message sent to the server, encapsulated in a `RequestMessage` type.
/// - `ReplyMessage`: Represents a response message from the server, encapsulated in a `ResponseMessage` type.
#[derive(Debug)]
pub enum Message {
    SolicitationMessage(RequestMessage),
    ReplyMessage(ResponseMessage),
}
