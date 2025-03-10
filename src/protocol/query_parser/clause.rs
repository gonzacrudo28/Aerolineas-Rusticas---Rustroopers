use serde::{Deserialize, Serialize};

use super::relation::Relation;

/// Represents the components of a logical expression in a query.
///
/// This enum is used to represent the clauses in a query, supporting different logical operations and terms. Each variant
/// can be used to express logical conjunctions, disjunctions, negations, and individual terms or placeholders in a query structure.
///
/// ## Variants:
/// - `And`: Represents a logical AND operation between two clauses. It contains two subclauses, `left` and `right`.
/// - `Not`: Represents a logical NOT operation applied to a single clause, stored in `right`.
/// - `Or`: Represents a logical OR operation between two clauses. It contains two subclauses, `left` and `right`.
/// - `Term`: Represents a single term in the query, which is associated with a `relation`. This could be a comparison or other relational operation.
/// - `Placeholder`: Represents a placeholder in the query, usually for prepared statements or parameterized queries.
/// - `Lpar`: Represents a left parenthesis (`(`) in the query, used for grouping clauses.
/// - `Rpar`: Represents a right parenthesis (`)`) in the query, used for grouping clauses.

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Clause {
    And {
        left: Box<Clause>,
        right: Box<Clause>,
    },
    Not {
        right: Box<Clause>,
    },
    Or {
        left: Box<Clause>,
        right: Box<Clause>,
    },
    Term {
        relation: Relation,
    },
    Placeholder,
    Lpar,
    Rpar,
}
