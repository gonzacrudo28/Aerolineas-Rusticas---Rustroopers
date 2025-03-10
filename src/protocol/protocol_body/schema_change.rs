/// Represents the different types of schema changes that can occur during execution.
///
/// This enum categorizes the types of changes that can be made to the database schema.
/// It is used to indicate the nature of the schema modification in response to a schema change event.
///
/// ### Variants:
/// - **Created**: Represents the creation of a new schema object (e.g., table, index, etc.).
/// - **Updated**: Represents an update to an existing schema object.
/// - **Dropped**: Represents the removal of an existing schema object.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaChangeType {
    Created,
    Updated,
    Dropped,
}
