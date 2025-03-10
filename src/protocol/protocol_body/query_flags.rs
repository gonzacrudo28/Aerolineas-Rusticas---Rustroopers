/// Represents the flags of a Query message.
///
/// This enum defines the various flags that can be set in a Query message, controlling different
/// aspects of the query's behavior. Each flag corresponds to a specific feature or option that
/// can be enabled or disabled for the query.
#[derive(Copy, Clone)]
pub enum QueryFlags {
    Values = 0x0001,
    SkipMetadata = 0x0002,
    PageSize = 0x0004,
    PagingState = 0x0008,
    SerialConsistency = 0x0010,
    DefaultTimestamp = 0x0020,
    NamesForValues = 0x0040,
    Keyspace = 0x0080,
    NowInSeconds = 0x0100,
}
