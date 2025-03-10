use super::value::Value;
use std::collections::HashMap;

/// A map that associates string keys with tuples containing an integer and a `Value`.
///
/// This type is a specialized `HashMap` where:
/// - The **key** is a `String`.
/// - The **value** is a tuple consisting of:
///     - An `i32` value, which may represent some numeric attribute or metadata.
///     - A `Value`, which represents a more complex or varied data type (defined in the `Value` enum or struct).
///
/// This map is useful for storing and looking up data where each entry contains both an integer identifier and a corresponding complex value.
pub type BytesMap = HashMap<String, (i32, Value)>;
