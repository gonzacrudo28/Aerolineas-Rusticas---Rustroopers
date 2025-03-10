/// This enum represents the flags for a row in a database result set.
///
/// Flags can be used to indicate special conditions or characteristics of a row in the result set.
/// Each flag corresponds to a specific condition, represented by a bitmask value.
///
/// - `HasMorePages`: Indicates that there are more pages of data available, meaning the result set is paginated.
/// - `NoMetadata`: Specifies that the row does not contain metadata information (such as column names or types).
#[derive(Debug, PartialEq)]
pub enum FlagsRow {
    HasMorePages = 0x0002,
    NoMetadata = 0x0004,
}
