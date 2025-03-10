/// Type alias representing column types in a database schema.
///
/// This type is used to define the structure of a database table, where each column is
/// described by a pair of values:
/// - A `String` representing the column name.
/// - A `String` representing the column's data type
pub type ColumnTypes = Vec<(String, String)>;
