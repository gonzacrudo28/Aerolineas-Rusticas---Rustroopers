use std::{
    cmp::Ordering,
    collections::HashMap,
    fs::OpenOptions,
    io::{BufRead, BufReader},
};

use serde::{Deserialize, Serialize};

use crate::{
    errors::error_types::ErrorTypes,
    protocol::query_parser::{clause::Clause, relation::Relation},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
/// This struct represents a SSTable, which is a file that contains the data of a table.
pub struct SSTable {
    route: String,
}
impl SSTable {
    pub fn new(route: String) -> SSTable {
        SSTable { route }
    }
    /// This function returns the route of the SSTable.
    pub fn get_route(&self) -> String {
        self.route.clone()
    }

    pub fn set_route(&mut self, route: String) {
        self.route = route;
    }

    /// This function returns the sstables rows that should be updated.
    pub fn execute_select(
        &self,
        conditions: &Clause,
        columns: &[String],
    ) -> Result<Vec<(u128, Vec<String>)>, ErrorTypes> {
        let mut result: Vec<(u128, Vec<String>)> = Vec::new();
        let mut hash: HashMap<&String, String> = HashMap::new();
        let table = match OpenOptions::new().read(true).open(&self.route) {
            Ok(table) => table,
            Err(_) => {
                return Ok(result);
            }
        };
        let reader = BufReader::new(table);
        for line in reader.lines() {
            let line = match line {
                Ok(line) => line,
                Err(_) => {
                    return Err(ErrorTypes::new(
                        574,
                        "Error reading sstable file".to_string(),
                    ))
                }
            };

            let mut splitted_line: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
            let time_stamp_line = splitted_line[1..].to_vec();
            splitted_line.pop();
            let id = splitted_line.remove(0).parse::<u128>().unwrap();
            for i in 0..columns.len() {
                hash.insert(&columns[i], splitted_line[i].clone());
            }
            match meets_conditions(&hash, conditions) {
                Ok(true) => result.push((id, time_stamp_line)),
                Ok(false) => continue,
                _ => {
                    return Err(ErrorTypes::new(
                        575,
                        "Checking line conditions failed".to_string(),
                    ))
                }
            }
            hash.clear();
        }
        Ok(result)
    }
}

/// This function checks if the values meet the conditions of the parsed clause.
pub fn meets_conditions(
    values: &HashMap<&String, String>,
    conditions: &Clause,
) -> Result<bool, ErrorTypes> {
    match conditions {
        Clause::And { left, right } => {
            Ok(meets_conditions(values, left)? && meets_conditions(values, right)?)
        }
        Clause::Not { right } => Ok(!meets_conditions(values, right)?),
        Clause::Or { left, right } => {
            Ok(meets_conditions(values, left)? || meets_conditions(values, right)?)
        }
        Clause::Term { relation } => meets_relation(relation, values),
        Clause::Placeholder => Ok(true),
        _ => Ok(false),
    }
}

/// This function cleans a line from a file removing the id and timestamp.
pub fn clean_line(line: String) -> Vec<String> {
    let mut splitted_line: Vec<String> = line.split(",").map(|x| x.to_string()).collect();
    splitted_line.pop();
    splitted_line.remove(0);
    splitted_line
}

/// This function checks if two values meets the parsed relation.
fn meets_relation(
    relation: &Relation,
    values: &HashMap<&String, String>,
) -> Result<bool, ErrorTypes> {
    match relation {
        Relation::Equal { v1, v2 } => {
            if values.contains_key(v1) && !values.contains_key(v2) {
                Ok(values.get(v1) == Some(v2))
            } else if values.contains_key(v2) && !values.contains_key(v1) {
                Ok(values.get(v2) == Some(v1))
            } else if !values.contains_key(v1) && !values.contains_key(v2) {
                Err(ErrorTypes::new(576, "The columns are invalid".to_string()))
            } else {
                Ok(values.get(v1) == values.get(v2))
            }
        }
        Relation::Higher { v1, v2 } => {
            if let (Some(r1), Some(r2)) = (values.get(v1), values.get(v2)) {
                return Ok(comparing_parser(r1, r2) == std::cmp::Ordering::Greater);
            }
            if let Some(r1) = values.get(v1) {
                return Ok(comparing_parser(r1, v2) == std::cmp::Ordering::Greater);
            }
            if let Some(r2) = values.get(v2) {
                return Ok(comparing_parser(v1, r2) == std::cmp::Ordering::Greater);
            }
            Err(ErrorTypes::new(577, "The columns are invalid".to_string()))
        }
        Relation::HigherEqual { v1, v2 } => {
            if let (Some(r1), Some(r2)) = (values.get(v1), values.get(v2)) {
                return Ok(comparing_parser(r1, r2) != std::cmp::Ordering::Less);
            }
            if let Some(r1) = values.get(v1) {
                return Ok(comparing_parser(r1, v2) != std::cmp::Ordering::Less);
            }
            if let Some(r2) = values.get(v2) {
                return Ok(comparing_parser(v1, r2) != std::cmp::Ordering::Less);
            }
            Err(ErrorTypes::new(578, "The columns are invalid".to_string()))
        }

        Relation::Lower { v1, v2 } => {
            if let (Some(r1), Some(r2)) = (values.get(v1), values.get(v2)) {
                return Ok(comparing_parser(r1, r2) == std::cmp::Ordering::Less);
            }
            if let Some(r1) = values.get(v1) {
                return Ok(comparing_parser(r1, v2) == std::cmp::Ordering::Less);
            }
            if let Some(r2) = values.get(v2) {
                return Ok(comparing_parser(v1, r2) == std::cmp::Ordering::Less);
            }
            Err(ErrorTypes::new(579, "The columns are invalid".to_string()))
        }
        Relation::LowerEqual { v1, v2 } => {
            if let (Some(r1), Some(r2)) = (values.get(v1), values.get(v2)) {
                return Ok(comparing_parser(r1, r2) != std::cmp::Ordering::Greater);
            }
            if let Some(r1) = values.get(v1) {
                return Ok(comparing_parser(r1, v2) != std::cmp::Ordering::Greater);
            }
            if let Some(r2) = values.get(v2) {
                return Ok(comparing_parser(v1, r2) != std::cmp::Ordering::Greater);
            }
            Err(ErrorTypes::new(580, "The columns are invalid".to_string()))
        }
    }
}

/// This function compares two values depending on their type.
fn comparing_parser(v1: &String, v2: &String) -> std::cmp::Ordering {
    let r1 = v1.parse::<i32>();
    let r2 = v2.parse::<i32>();

    match (r1, r2) {
        (Ok(r1), Ok(r2)) => r1.cmp(&r2),
        _ => v1.cmp(v2),
    }
}

/// This function sorts an array of rows by a specified column.
pub fn sort_by_columns(
    order: &[String],
    mut chosen: Vec<Vec<String>>,
    file_columns: &[String],
) -> Result<Vec<Vec<String>>, ErrorTypes> {
    let mut positions = Vec::new();
    let mut sup_limit = order.len() - 1;
    if order.len() == 1 {
        sup_limit = 1;
    }
    for elem in order.iter().take(sup_limit) {
        positions.push(get_position(file_columns, elem)?);
    }
    let mut order = order.to_vec();
    if file_columns.contains(order.last().unwrap()) {
        order.push("asc".to_string());
    }
    if let Some(last) = order.last() {
        if last == "asc" {
            chosen.sort_by(|a, b| {
                let mut ord: Ordering = Ordering::Equal;
                for position in positions.iter() {
                    ord = a[*position].cmp(&b[*position]);
                    if ord == std::cmp::Ordering::Equal {
                        continue;
                    }
                    break;
                }
                ord
            });
        } else if order[1].to_lowercase().as_str() == "desc" {
            chosen.sort_by(|a, b| {
                let mut ord: Ordering = Ordering::Equal;
                for position in positions.iter() {
                    ord = b[*position].cmp(&a[*position]);
                    if ord == std::cmp::Ordering::Equal {
                        continue;
                    }
                    break;
                }
                ord
            });
        } else {
            return Err(ErrorTypes::new(581, "Invalid sorting method".to_string()));
        }
    }

    Ok(chosen)
}

/// This function returns the position of an element that is mandatory to be in the vector, if it is not, it returns an error.
pub fn get_position(vec: &[String], keyword: &String) -> Result<usize, ErrorTypes> {
    match vec.iter().position(|t| t.to_lowercase() == *keyword) {
        Some(pos) => Ok(pos),
        None => Err(ErrorTypes::new(
            582,
            format!("No se encontró la keyword {}", keyword),
        )),
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn order() {
        let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];

        let values = vec![
            vec!["2".to_string(), "Pedro".to_string(), "30".to_string()],
            vec!["1".to_string(), "Juan".to_string(), "20".to_string()],
            vec!["3".to_string(), "Maria".to_string(), "25".to_string()],
        ];
        let order = vec!["id".to_string(), "desc".to_string()];

        let result = sort_by_columns(&order, values, &columns).unwrap();

        assert_eq!(result[0][0], "3");
        assert_eq!(result[1][0], "2");
        assert_eq!(result[2][0], "1");
    }
}
