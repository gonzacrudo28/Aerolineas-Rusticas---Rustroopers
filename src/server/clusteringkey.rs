/// Type alias representing a clustering key in a distributed database.
///
/// It is composed of a vector of tuples, where each tuple contains:
/// - A `String` representing the column name.
/// - A `usize` representing the position or order of the key in the clustering definition.
pub type ClusteringKey = Vec<(String, usize)>;
