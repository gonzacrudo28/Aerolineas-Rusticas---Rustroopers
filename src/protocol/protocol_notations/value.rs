/// Represents the possible values when reading bytes of a protocol's notation.
///
/// This enum is used to represent the various possible values encountered while reading bytes in the protocol.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Normal(Vec<u8>),
    NotSet,
    Null,
}
