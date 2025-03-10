use super::result_response::ResultResponse;
use std::collections::HashMap;

#[derive(Debug)]

/// Represents the different types of Response messages that can be sent by the server.
///
/// This enum categorizes the different types of responses the server can send back to the client.
/// Each variant represents a distinct message type that may carry different data related to the response.
///
/// ## Variants:
/// - `Error`: Represents an error response sent by the server in case of failure or an invalid request.
///   - `code`: An integer representing the error code.
///   - `message`: A string containing the description of the error.
///   
/// - `Ready`: Represents a response indicating the server is ready for further communication or operations.
///   - `body`: A string providing additional information related to the readiness of the server.
///   
/// - `AuthSuccess`: Represents a successful authentication response.
///   - `body`: A string containing information regarding the successful authentication.
///   
/// - `Authenticate`: Represents a response requesting authentication from the client.
///   - `class`: A string indicating the authentication class or method required by the server.
///   
/// - `Supported`: Represents the response that lists the supported options or features from the server.
///   - `options`: A `HashMap` where the key is a string representing the option name and the value is a list of strings specifying supported values or configurations for that option.
///   
/// - `Result`: Represents a response containing the result of a query or operation.
///   - `kind`: A `ResultResponse` object that encapsulates the details of the query or operation result.
pub enum ResponseMessage {
    Error {
        code: i32,
        message: String,
    },
    Ready {
        body: String,
    },
    AuthSuccess {
        body: String,
    },
    Authenticate {
        class: String,
    },
    Supported {
        options: HashMap<String, Vec<String>>,
    },
    Result {
        kind: ResultResponse,
    },
}
