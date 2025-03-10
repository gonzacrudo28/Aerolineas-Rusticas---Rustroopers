use super::{
    parser_impl::generic_validate,
    parser_utils::{get_position, normalize_vector},
    query::Query,
};
use crate::errors::error_types::ErrorTypes;

/// This function parses an INSERT query
pub fn parse_insert(mut query: Vec<String>) -> Result<Query, ErrorTypes> {
    let into = String::from("into");
    let pos_into: usize = get_position(&query, &into)?;
    if pos_into + 1 >= query.len() {
        return Err(ErrorTypes::new(214, "Table name missing".to_string()));
    }
    let table_name = query.remove(pos_into + 1);
    let pos_into: usize = get_position(&query, &into)?;
    let values = String::from("values");
    let pos_values = get_position(&query, &values)?;
    generic_validate(&query, &pos_into, &pos_values)?;
    insert_validate(&query, &pos_into, &pos_values)?;
    let mut columns = query.split_off(pos_into + 1);
    let mut values = columns.split_off(pos_values - query.len());

    let columns_name = normalize_vector(columns);
    let values = juntar_values(normalize_vector(values.split_off(1)), columns_name.len())?;
    Ok(Query::Insert {
        table_name,
        columns_name,
        values,
    })
}

/// This function validates the INSERT query
fn insert_validate(
    query: &[String],
    pos_into: &usize,
    pos_values: &usize,
) -> Result<(), ErrorTypes> {
    if *pos_into != 1 {
        return Err(ErrorTypes::new(
            215,
            "The keyword INTO must be after INSERT".to_string(),
        ));
    }
    if let Some(columna1) = query.get(pos_into + 1) {
        if !columna1.starts_with("(") {
            return Err(ErrorTypes::new(
                216,
                "The columns must start with (".to_string(),
            ));
        }
    }
    if let Some(columna1) = query.get(pos_values - 1) {
        if !columna1.ends_with(")") {
            return Err(ErrorTypes::new(
                217,
                "The columns must end with )".to_string(),
            ));
        }
    }
    if *pos_values == query.len() - 1 {
        return Err(ErrorTypes::new(218, "There are no values".to_string()));
    }
    Ok(())
}

/// This function joins the values of the query
fn juntar_values(vec: Vec<String>, cant_columns: usize) -> Result<Vec<Vec<String>>, ErrorTypes> {
    let mut res: Vec<Vec<String>> = vec![];
    if vec.len() % cant_columns != 0 {
        return Err(ErrorTypes::new(
            219,
            "The number of values is not correct".to_string(),
        ));
    }
    let mut i = 0;
    let mut current = vec![];
    for valor in vec {
        current.push(valor);
        i += 1;
        if i == cant_columns {
            res.push(current);
            i = 0;
            current = vec![];
        }
    }
    Ok(res)
}
