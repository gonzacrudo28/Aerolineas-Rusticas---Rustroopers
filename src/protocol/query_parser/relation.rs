use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
/// This enum represents the possbile relations between two values in a query.
pub enum Relation {
    Equal { v1: String, v2: String },
    Higher { v1: String, v2: String },
    HigherEqual { v1: String, v2: String },
    LowerEqual { v1: String, v2: String },
    Lower { v1: String, v2: String },
}
