use super::{clause::Clause, parser_impl::*, parser_utils::*, query::Query};
use crate::errors::error_types::ErrorTypes;
use std::collections::HashMap;

/// This function parses an UPDATE query
pub fn parse_update(mut query: Vec<String>) -> Result<Query, ErrorTypes> {
    if query.len() < 2 {
        return Err(ErrorTypes::new(227, "Table name missing".to_string()));
    }
    let set = String::from("set");
    let table_name = query.remove(1);
    let size = query.len();
    let pos_set = get_position(&query, &set)?;
    let where_ = String::from("where");
    let pos_where = get_position_conditional(&query, &where_);
    generic_validate(&query, &pos_set, &pos_where)?;
    update_validate(&query, &pos_set, &pos_where)?;

    let mut column_value_vectors = query.split_off(pos_set + 1);
    let mut conditions_vector = column_value_vectors.split_off(pos_where - query.len());
    let column_value = parse_column_value(normalize_vector(column_value_vectors))?;
    let mut conditions = Clause::Placeholder;
    if pos_where != size {
        conditions = parse_conditions(join_compounds(split_comma(conditions_vector.split_off(1))))?;
    }
    Ok(Query::Update {
        table_name,
        column_value,
        conditions,
    })
}

/// This function parses the column value pairs
fn parse_column_value(mut vec: Vec<String>) -> Result<HashMap<String, String>, ErrorTypes> {
    let mut hash = HashMap::new();
    while vec.len() % 3 == 0 {
        if vec.is_empty() {
            return Ok(hash);
        }
        let v1 = vec.remove(0);
        let _ = vec.remove(0);
        let v2 = vec.remove(0);

        hash.insert(v1, v2);
    }
    if !vec.is_empty() {
        return Err(ErrorTypes::new(
            228,
            "The column value pairs are not correct".to_string(),
        ));
    }
    Ok(hash)
}

/// Validates the UPDATE query
fn update_validate(_: &Vec<String>, pos_set: &usize, _: &usize) -> Result<(), ErrorTypes> {
    if *pos_set != 1 {
        return Err(ErrorTypes::new(
            229,
            "The keyword 'set' must be next to 'update'".to_string(),
        ));
    }

    Ok(())
}
