use super::{
    clause::Clause,
    parser_impl::{generic_validate, parse_conditions},
    parser_utils::{get_position, get_position_conditional, normalize_vector},
    query::Query,
};
use crate::errors::error_types::ErrorTypes;

/// This function parses a SELECT query.
pub fn parse_select(mut query: Vec<String>) -> Result<Query, ErrorTypes> {
    let from = String::from("from");
    let pos_from = get_position(&query, &from)?;
    if pos_from + 1 >= query.len() {
        return Err(ErrorTypes::new(223, "Table name missing".to_string()));
    }
    let table_name = query.remove(pos_from + 1);
    let size = query.len();

    let pos_from = get_position(&query, &from)?;
    let where_ = String::from("where");
    let order = String::from("order");
    let pos_where = get_position_conditional(&query, &where_);
    let pos_order = get_position_conditional(&query, &order);
    let by = String::from("by");
    let pos_by = get_position_conditional(&query, &by);

    generic_validate(&query, &pos_from, &pos_where)?;
    generic_validate(&query, &pos_from, &pos_order)?;
    select_validate(&query, &pos_from, &pos_where, &pos_order, &pos_by)?;

    let mut where_order = query.split_off(pos_from);
    let selected_columns = normalize_vector(query.split_off(1));

    let mut rest = vec![];

    if pos_where != size {
        rest = where_order.split_off(2);
    } else if pos_order != size && pos_by != size {
        rest = where_order.split_off(pos_by - query.len() - 1);
    }
    let mut order: Vec<String> = vec![];
    if pos_order != size {
        let mut order_vec = rest.split_off(pos_by - query.len() - where_order.len() - 1);
        order_vec.remove(0);
        order = normalize_vector(order_vec);
    }
    let mut conditions = Clause::Placeholder;
    if pos_where != size {
        conditions = parse_conditions(rest)?;
    }
    Ok(Query::Select {
        table_name,
        selected_columns,
        conditions,
        order,
    })
}

/// Validates the SELECT query
fn select_validate(
    query: &[String],
    pos_from: &usize,
    pos_where: &usize,
    pos_order: &usize,
    pos_by: &usize,
) -> Result<(), ErrorTypes> {
    if *pos_where != query.len() && *pos_from != *pos_where - 1 {
        return Err(ErrorTypes::new(224, "Invalid syntax".to_string()));
    }

    if *pos_where != query.len() && *pos_order != query.len() && *pos_order == pos_where + 1 {
        return Err(ErrorTypes::new(225, "There is no WHERE clause".to_string()));
    }
    if (*pos_order != query.len() || *pos_by != query.len()) && *pos_by != pos_order + 1 {
        return Err(ErrorTypes::new(226, "Invalid syntax".to_string()));
    }
    Ok(())
}
