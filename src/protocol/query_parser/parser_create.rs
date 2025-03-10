use super::{parser_utils::split_par, query::Query};
use crate::errors::error_types::ErrorTypes;

/// This function parses the create query.
pub fn parse_create(query: Vec<String>) -> Result<Query, ErrorTypes> {
    let query_split = split_par(query);
    let mut table_name = String::new();
    let mut columns_type: Vec<(String, String)> = Vec::new();
    let mut primary_key = Vec::new();
    let mut clustering_key = Vec::new();

    let mut index_table = None;
    let mut index_lpar = None;
    let mut index_lpar2 = None;
    let mut index_key = None;
    let mut vec_columns = Vec::new();
    for (i, word) in query_split.iter().enumerate() {
        if word.to_uppercase() == "TABLE" {
            index_table = Some(i);
        } else if let Some(i_table) = index_table {
            if i == i_table + 1 {
                table_name = word.to_string();
            }
            index_table = None;
        } else if word == "(" && index_key.is_none() {
            index_lpar = Some(i);
        } else if index_lpar.is_some() && index_key.is_none() {
            if word == "PRIMARY" {
                index_lpar = None;
            } else {
                vec_columns.push(word.trim_end_matches(',').to_string());
                if vec_columns.len() == 2 {
                    columns_type.push((vec_columns[0].to_string(), vec_columns[1].to_string()));
                    vec_columns.clear();
                }
            }
        } else if word.to_uppercase() == "KEY" {
            index_key = Some(i);
        } else if index_key.is_some() && word == "(" && index_lpar.is_none() {
            index_lpar = Some(i);
        } else if index_lpar.is_some() && index_key.is_some() {
            if word == "(" {
                index_lpar2 = Some(i);
            } else if index_lpar2.is_some() {
                if word.trim_matches(',') == ")" {
                    index_lpar2 = None;
                } else {
                    primary_key.push(word.trim_end_matches(',').to_string());
                }
            } else if word != ")" {
                clustering_key.push(word.trim_end_matches(',').to_string());
            }
        }
    }
    clustering_key.retain(|x| !x.is_empty());
    primary_key.retain(|x| !x.is_empty());

    let query = Query::CreateTable {
        table_name,
        columns_type,
        clustering_key,
        primary_key,
    };
    Ok(query)
}

#[cfg(test)]
pub mod test {
    use crate::protocol::query_parser::{parser_impl::parse_query, query::Query};
    #[test]
    fn test_create_table() {
        let query = "CREATE TABLE cycling (race_name text, race_position int, PRIMARY KEY ((race_name), race_position));"
        .to_string();
        let result = parse_query(query).unwrap();
        assert_eq!(
            result,
            Query::CreateTable {
                table_name: "cycling".to_string(),
                columns_type: vec![
                    ("race_name".to_string(), "text".to_string()),
                    ("race_position".to_string(), "int".to_string())
                ],
                clustering_key: vec!["race_position".to_string()],
                primary_key: vec!["race_name".to_string()]
            }
        )
    }
    #[test]
    fn test_create_table_2() {
        let query = "CREATE TABLE flights (id int, flight_name text, origin text PRIMARY KEY ((id, origin), flight_name));";
        let result = parse_query(query.to_string()).unwrap();
        assert_eq!(
            result,
            Query::CreateTable {
                table_name: "flights".to_string(),
                columns_type: vec![
                    ("id".to_string(), "int".to_string()),
                    ("flight_name".to_string(), "text".to_string()),
                    ("origin".to_string(), "text".to_string())
                ],
                clustering_key: vec!["flight_name".to_string()],
                primary_key: vec!["id".to_string(), "origin".to_string()]
            }
        )
    }
    #[test]
    fn test_create_table_3() {
        let query = "CREATE TABLE flights (id int, flight_name text, origin text PRIMARY KEY (origin, flight_name));";
        let result = parse_query(query.to_string()).unwrap();
        assert_eq!(
            result,
            Query::CreateTable {
                table_name: "flights".to_string(),
                columns_type: vec![
                    ("id".to_string(), "int".to_string()),
                    ("flight_name".to_string(), "text".to_string()),
                    ("origin".to_string(), "text".to_string())
                ],
                clustering_key: vec!["origin".to_string(), "flight_name".to_string()],
                primary_key: vec![]
            }
        )
    }

    #[test]
    fn test_create_table_4() {
        let query = "CREATE TABLE arrivals (id int,  origin text, destination text, date date PRIMARY KEY ((destination), date));";
        let result = parse_query(query.to_string()).unwrap();
        assert_eq!(
            result,
            Query::CreateTable {
                table_name: "arrivals".to_string(),
                columns_type: vec![
                    ("id".to_string(), "int".to_string()),
                    ("origin".to_string(), "text".to_string()),
                    ("destination".to_string(), "text".to_string()),
                    ("date".to_string(), "date".to_string())
                ],
                clustering_key: vec!["date".to_string()],
                primary_key: vec!["destination".to_string()]
            }
        )
    }

    #[test]
    fn test_create_table_5() {
        let query = "CREATE TABLE departures (id int,  origin text, destination text, date date PRIMARY KEY ((origin), date));";
        let result = parse_query(query.to_string()).unwrap();
        assert_eq!(
            result,
            Query::CreateTable {
                table_name: "departures".to_string(),
                columns_type: vec![
                    ("id".to_string(), "int".to_string()),
                    ("origin".to_string(), "text".to_string()),
                    ("destination".to_string(), "text".to_string()),
                    ("date".to_string(), "date".to_string())
                ],
                clustering_key: vec!["date".to_string()],
                primary_key: vec!["origin".to_string()]
            }
        )
    }
}
