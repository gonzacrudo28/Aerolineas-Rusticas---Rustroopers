use super::{
    clause::Clause,
    parser_impl::{generic_validate, parse_conditions},
    parser_utils::{get_position, get_position_conditional},
    query::Query,
};

use crate::errors::error_types::ErrorTypes;

/// This function parses a DELETE query
pub fn parse_delete(mut query: Vec<String>) -> Result<Query, ErrorTypes> {
    let from = String::from("from");
    let pos_from = get_position(&query, &from)?;
    if pos_from + 1 >= query.len() {
        return Err(ErrorTypes::new(200, "Table name not found".to_string()));
    }
    let table_name = query.remove(pos_from + 1);
    let size = query.len();
    let pos_from = get_position(&query, &from)?;
    let where_ = String::from("where");
    let pos_where = get_position_conditional(&query, &where_);
    generic_validate(&query, &pos_from, &pos_where)?;
    delete_validate(&query, &pos_from, &pos_where)?;
    let mut conditions = Clause::Placeholder;
    if pos_where != size {
        let conditions_vec = query.split_off(pos_where + 1);
        conditions = parse_conditions(conditions_vec)?;
    }

    Ok(Query::Delete {
        table_name,
        conditions,
    })
}

/// This function validates the DELETE query
fn delete_validate(
    query: &[String],
    pos_from: &usize,
    pos_where: &usize,
) -> Result<(), ErrorTypes> {
    if *pos_from != 1 || *pos_where != 2 {
        return Err(ErrorTypes::new(202, "Invalid delete query".to_string()));
    }
    if *pos_where == query.len() - 1 {
        return Err(ErrorTypes::new(203, "WHERE clause not found".to_string()));
    }
    Ok(())
}
