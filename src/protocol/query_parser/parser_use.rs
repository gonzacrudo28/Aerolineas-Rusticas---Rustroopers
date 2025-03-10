use super::query::Query;
use crate::errors::error_types::ErrorTypes;

///This function parses the keyspace query
pub fn parse_use(query: Vec<String>) -> Result<Query, ErrorTypes> {
    if query.len() < 2 {
        return Err(ErrorTypes::new(230, "Table name missing".to_string()));
    }
    let keyspace_name = query[1]
        .trim_end_matches(';')
        .trim_matches('\'')
        .to_string();
    Ok(Query::Use { keyspace_name })
}

#[cfg(test)]
pub mod test {
    use crate::protocol::query_parser::{parser_impl::parse_query, query::Query};
    #[test]
    fn test_use_keyspace() {
        let query = "USE flights_keyspace;".to_string();

        let result = parse_query(query).unwrap();
        assert_eq!(
            result,
            Query::Use {
                keyspace_name: "flights_keyspace".to_string(),
            }
        )
    }
}
