/// Enum representing the different types of errors that can occur during program execution.
///
/// ### Error Codes:
/// - **100–199**: Errors in the `protocol_notations` module.
/// - **200–299**: Errors in the `query_parser` module.
/// - **300–399**: Errors in the `receiver` module.
/// - **500–599**: Errors in the `server` module.
/// - **600–699**: Errors in the `ui` module.
/// - **700–799**: Errors in the `simulator` module.
///
/// Each error is represented by:
/// - `code` (`i32`): The unique error code.
/// - `message` (`String`): A detailed error message.
///
/// This structure ensures a standardized way to handle and propagate errors across the system.
#[derive(PartialEq)]
pub enum ErrorTypes {
    /// Represents an error with a specific code and message.
    Error { code: i32, message: String },
}

impl ErrorTypes {
    /// Creates a new `ErrorTypes::Error` instance.
    ///
    /// # Arguments:
    /// - `code`: The error code associated with this error.
    /// - `message`: A descriptive message explaining the error.
    ///
    /// # Returns:
    /// A new `ErrorTypes` instance.
    pub fn new(code: i32, message: String) -> Self {
        ErrorTypes::Error { code, message }
    }

    /// Retrieves the code and message of the error.
    ///
    /// # Returns:
    /// A tuple containing the error code (`i32`) and the error message (`String`).
    pub fn get(&self) -> (i32, String) {
        match self {
            ErrorTypes::Error { code, message } => (*code, message.clone()),
        }
    }
}

impl std::fmt::Debug for ErrorTypes {
    /// Formats the error information for debugging purposes.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorTypes::Error { code, message } => {
                write!(f, "An error has occured: {:?}, code: [{:?}]", message, code)
            }
        }
    }
}
