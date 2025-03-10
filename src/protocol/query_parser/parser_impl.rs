use super::{
    clause::Clause, parser_create::parse_create, parser_delete::parse_delete,
    parser_insert::parse_insert, parser_keyspace::parse_keyspace, parser_select::parse_select,
    parser_update::parse_update, parser_use::parse_use, parser_utils::*, query::Query,
    relation::Relation,
};
use crate::errors::error_types::ErrorTypes;

/// This function is the main function that will parse the query and return a Query object
pub fn parse_query(query: String) -> Result<Query, ErrorTypes> {
    let mut query = query.replace("\n", " ");
    if query.ends_with(";") {
        query.pop();
    } else {
        return Err(ErrorTypes::new(
            204,
            "Queries must end with ';'".to_string(),
        ));
    }
    let splitted_query: Vec<String> = query.split_whitespace().map(|s| s.to_string()).collect();
    match splitted_query[0].to_lowercase().as_str() {
        "insert" => parse_insert(splitted_query),
        "update" => parse_update(splitted_query),
        "delete" => parse_delete(splitted_query),
        "select" => parse_select(splitted_query),
        "create" if splitted_query[1].to_lowercase().as_str() == "keyspace" => {
            parse_keyspace(splitted_query)
        }
        "create" => parse_create(splitted_query),
        "use" => parse_use(splitted_query),

        _ => Err(ErrorTypes::new(205, "Invalid query".to_string())),
    }
}

/// This function validates the query to check if it is a valid query
pub fn generic_validate(
    query: &[String],
    pos_keyword1: &usize,
    pos_keyword2: &usize,
) -> Result<(), ErrorTypes> {
    if pos_keyword2 < pos_keyword1 {
        return Err(ErrorTypes::new(206, "Invalid query".to_string()));
    }
    if *pos_keyword2 == pos_keyword1 + 1 && query[*pos_keyword1].to_lowercase() != *"from" {
        return Err(ErrorTypes::new(207, "No columns where written".to_string()));
    }
    if *pos_keyword2 == query.len() - 1 {
        return Err(ErrorTypes::new(208, "No values where written".to_string()));
    }
    Ok(())
}

/// This function allows to parse the conditions of the query
pub fn parse_conditions(vec: Vec<String>) -> Result<Clause, ErrorTypes> {
    let vec = join_conditions(join_compounds(split_par(vec)))?;
    let mut clauses: Vec<Clause> = Vec::new();
    for elem in vec {
        if elem.is_empty() {
            continue;
        }
        match elem.to_lowercase().as_str() {
            "and" => clauses.push(Clause::And {
                left: Box::new(Clause::Placeholder),
                right: Box::new(Clause::Placeholder),
            }),
            "or" => clauses.push(Clause::Or {
                left: Box::new(Clause::Placeholder),
                right: Box::new(Clause::Placeholder),
            }),
            "not" => clauses.push(Clause::Not {
                right: Box::new(Clause::Placeholder),
            }),
            "(" => {
                clauses.push(Clause::Lpar);
            }
            ")" => {
                clauses.push(Clause::Rpar);
            }
            _ => clauses.push(Clause::Term {
                relation: parse_relation(split_operators(vec![elem]))?,
            }),
        }
    }
    if clauses.len() == 1 {
        return Ok(clauses.remove(0));
    }
    deepen_clauses(clauses)
}

/// This functuon allows to join the clauses, the most internal clauses will be the ones inside the parenthesis to respect the evaluation order. Thanks to the stack we can always join the most internal clauses
fn deepen_clauses(vector: Vec<Clause>) -> Result<Clause, ErrorTypes> {
    let mut stack: Vec<Vec<Clause>> = Vec::new();
    let mut actual: Vec<Clause> = Vec::new();
    for clause in vector {
        match clause {
            Clause::Lpar => {
                stack.push(actual);
                actual = Vec::new();
            }
            Clause::Rpar => match stack.pop() {
                Some(anterior) => {
                    let clause_parcial = join_clauses(actual);
                    actual = anterior;
                    actual.push(clause_parcial);
                }
                None => return Err(ErrorTypes::new(210, "Unbalanced parenthesis".to_string())),
            },
            _ => actual.push(clause),
        }
    }
    Ok(join_clauses(actual))
}
/// This function allows to join a vector of clauses in a single clause
fn join_clauses(mut vector: Vec<Clause>) -> Clause {
    while let Some(pos) = vector.iter().position(|s| {
        if let Clause::Not { right } = s {
            matches!(**right, Clause::Placeholder)
        } else {
            false
        }
    }) {
        let right = vector.remove(pos + 1);
        let new = Clause::Not {
            right: Box::new(right),
        };
        vector[pos] = new;
    }

    while let Some(pos) = vector.iter().position(|s| {
        if let Clause::And { left, right } = s {
            matches!(**left, Clause::Placeholder) && matches!(**right, Clause::Placeholder)
        } else {
            false
        }
    }) {
        let left = vector.remove(pos - 1);
        let right = vector.remove(pos); // Note: pos is now the position of `right` after removing `left`
        vector[pos - 1] = Clause::And {
            left: Box::new(left),
            right: Box::new(right),
        };
    }

    while let Some(pos) = vector.iter().position(|s| {
        if let Clause::Or { left, right } = s {
            matches!(**left, Clause::Placeholder) && matches!(**right, Clause::Placeholder)
        } else {
            false
        }
    }) {
        let left = vector.remove(pos - 1);
        let right = vector.remove(pos); // Note: pos is now the position of `right` after removing `left`
        vector[pos - 1] = Clause::Or {
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    vector.pop().unwrap_or(Clause::Placeholder)
}

/// This function allows to join the conditions of the query
fn join_conditions(vec: Vec<String>) -> Result<Vec<String>, ErrorTypes> {
    let vector: Vec<String> = split_operators(vec);
    let mut result: Vec<String> = vec![];
    for i in 0..vector.len() {
        let elem_lower = vector[i].to_lowercase();
        let elem = elem_lower.as_str();
        match elem {
            "=" | ">" | "<" | ">=" | "<=" => {
                if i == 0 || i == vector.len() - 1 {
                    return Err(ErrorTypes::new(211, "Syntax error".to_string()));
                }
                result.push(format!("{} {} {}", vector[i - 1], elem, vector[i + 1]));
            }
            "and" | "or" | "not" | "(" | ")" => {
                result.push(elem.to_string());
            }
            _ => (),
        }
    }
    Ok(result)
}

/// This function allows to split the operators of the query
fn split_operators(vec: Vec<String>) -> Vec<String> {
    let mut result = Vec::new();
    let operators = [
        String::from(">="),
        String::from("<="),
        String::from(">"),
        String::from("<"),
        String::from("="),
    ];

    for elem in vec.iter() {
        let mut added = false;
        for op in operators.iter() {
            if elem.contains(op) {
                let partes: Vec<&str> = elem.split(op).collect();
                for (i, parte) in partes.iter().enumerate() {
                    result.push(parte.trim());
                    if i < partes.len() - 1 {
                        result.push(op);
                    }
                }
                added = true;
                break;
            }
        }

        if !added {
            result.push(elem.as_str());
        }
    }

    result
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

/// This function allows to parse the relation of the query
fn parse_relation(mut vec: Vec<String>) -> Result<Relation, ErrorTypes> {
    if vec.len() == 3 {
        let v1 = vec.remove(0);
        let operator = vec.remove(0);
        let v2 = vec.remove(0);

        match operator.as_str() {
            "=" => Ok(Relation::Equal { v1, v2 }),
            ">" => Ok(Relation::Higher { v1, v2 }),
            "<" => Ok(Relation::Lower { v1, v2 }),
            ">=" => Ok(Relation::HigherEqual { v1, v2 }),
            "<=" => Ok(Relation::LowerEqual { v1, v2 }),
            _ => Err(ErrorTypes::new(212, "Not supported operator".to_string())),
        }
    } else {
        Err(ErrorTypes::new(213, "Invalid input format".to_string()))
    }
}
