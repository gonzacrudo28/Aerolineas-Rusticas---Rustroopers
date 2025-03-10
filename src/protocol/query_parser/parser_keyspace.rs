use super::{parser_utils::split_keyspace, query::Query};
use crate::errors::error_types::ErrorTypes;

///This function parses the keyspace query
pub fn parse_keyspace(query: Vec<String>) -> Result<Query, ErrorTypes> {
    let query_split: Vec<String> = split_keyspace(query);

    if query_split.len() < 2 {
        return Err(ErrorTypes::new(220, "Table name missing".to_string()));
    }
    let keyspace_name = query_split[2].trim_matches('\'').to_string();
    let mut rep = None;

    let mut index_colon = None;

    for (i, word) in query_split.iter().enumerate() {
        if word == ":"
            && query_split[i - 1].to_lowercase().trim_matches('\'') == "replication_factor"
        {
            index_colon = Some(i);
        } else if let Some(i_equal) = index_colon {
            if i == i_equal + 1 {
                rep = Some(word.parse::<usize>().unwrap());
                break;
            } else {
                return Err(ErrorTypes::new(221, "Invalid query".to_string()));
            }
        }
    }
    if let Some(replic) = rep {
        let query = Query::CreateKeyspace {
            keyspace_name,
            replication: replic,
        };
        Ok(query)
    } else {
        Err(ErrorTypes::new(222, "Invalid query".to_string()))
    }
}

///Test of the parse_keyspace function  
#[cfg(test)]
pub mod test {
    use crate::protocol::query_parser::{parser_impl::parse_query, query::Query};
    #[test]
    fn test_create_keyspace() {
        let query =
            "CREATE KEYSPACE flights_keyspace WITH REPLICATION = {  'replication_factor': 4};"
                .to_string();

        let result = parse_query(query).unwrap();
        assert_eq!(
            result,
            Query::CreateKeyspace {
                keyspace_name: "flights_keyspace".to_string(),
                replication: 4
            }
        )
    }
}
