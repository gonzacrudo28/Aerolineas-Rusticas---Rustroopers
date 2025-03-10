use crate::errors::error_types::ErrorTypes;

/// This function splits a vector of strings'
pub fn split_par(vec: Vec<String>) -> Vec<String> {
    let mut result: Vec<String> = Vec::new();

    for elem in vec {
        let left_part: Vec<&str> = elem.split_inclusive("(").collect();

        for part in left_part {
            let right_part: Vec<&str> = part.split(")").collect();

            for (i, sub_part) in right_part.iter().enumerate() {
                if !sub_part.is_empty() {
                    result.push(sub_part.trim_end_matches(";").into());
                }
                if i < right_part.len() - 1 {
                    result.push(")".into());
                }
            }
        }
    }

    result
}
/// This function splits a vector of strings by keyspaces.
pub fn split_keyspace(vec: Vec<String>) -> Vec<String> {
    let mut result: Vec<String> = Vec::new();
    for elem in vec {
        let left_part: Vec<&str> = elem.split_inclusive("{").collect();

        for part in left_part {
            let right_part: Vec<&str> = part.split("}").collect();

            for (i, sub_part) in right_part.iter().enumerate() {
                if sub_part.contains(":") {
                    result.push(sub_part.trim_end_matches(":").into());
                    result.push(":".into());
                } else if !sub_part.is_empty() {
                    result.push(sub_part.trim_end_matches(";").into());
                }
                if i < right_part.len() - 1 {
                    result.push("}".into());
                }
            }
        }
    }
    result
}

/// This function splits a vector of strings by commas
pub fn split_comma(vec: Vec<String>) -> Vec<String> {
    vec.iter()
        .flat_map(|s| {
            s.split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
        })
        .collect()
}

/// This function joins the compounds
pub fn join_compounds(vec: Vec<String>) -> Vec<String> {
    let mut new: Vec<String> = vec![];
    let mut aux: Vec<String> = vec![];
    let mut operating = false;
    for word in vec {
        if word.starts_with("'") && word.ends_with("'")
            || word.starts_with("'") && word.ends_with("')")
        {
            new.push(
                word.trim_start_matches("'")
                    .trim_end_matches("'")
                    .to_string(),
            );
        } else if word.starts_with("'") {
            aux.push(word.trim_start_matches("'").to_string());
            operating = true
        } else if word.ends_with("'") || word.ends_with("')") || word.ends_with(')') {
            aux.push(word.trim_end_matches("'").to_string());
            new.push(aux.join(" "));
            aux.clear();
            operating = false;
        } else if operating {
            aux.push(format!(" {}", word));
        } else {
            new.push(word);
        }
    }
    new
}

/// This function splits the whitespaces
pub fn split_whitespace(query: String) -> Vec<String> {
    let mut result: Vec<String> = vec![];
    let mut word = String::from("");
    for ch in query.chars() {
        if ch != ' ' {
            word.push(ch);
            continue;
        }
        result.push(word);
        word = String::from("")
    }
    result.push(word);
    result
}

/// This function returns the position of an element that is not mandatory to be in the vector, if it is not, it returns the length of the vector
pub fn get_position_conditional(vec: &[String], keyword: &String) -> usize {
    match vec.iter().position(|t| t.to_lowercase() == *keyword) {
        Some(pos) => pos,
        None => vec.len(),
    }
}

/// This function returns the position of an element that is mandatory to be in the vector, if it is not, it returns an error
pub fn get_position(vec: &[String], keyword: &String) -> Result<usize, ErrorTypes> {
    match vec.iter().position(|t| t.to_lowercase() == *keyword) {
        Some(pos) => Ok(pos),
        None => Err(ErrorTypes::new(
            231,
            format!("Keyword not found {}", keyword),
        )),
    }
}

/// This function orders the selected columns by position
pub fn order_by_position(
    column: String,
    order: Vec<String>,
    mut selected: Vec<Vec<String>>,
    file_columns: &[String],
) -> Result<Vec<Vec<String>>, ErrorTypes> {
    let pos = get_position(file_columns, &column)?;
    if order.is_empty() || order[0].to_lowercase().as_str() == "asc" {
        selected.sort_by(|a, b| b[pos].cmp(&a[pos]));
    } else if order[0].to_lowercase().as_str() == "desc" {
        selected.sort_by(|a, b| a[pos].cmp(&b[pos]));
    } else {
        return Err(ErrorTypes::new(232, "Invalid sorting".to_string()));
    }
    Ok(selected)
}

/// This function normalizes the vector
pub fn normalize_vector(vec: Vec<String>) -> Vec<String> {
    let vector = split_comma(vec);
    join_compounds(
        vector
            .into_iter()
            .map(|s| {
                s.trim_start_matches("(")
                    .trim_end_matches(";")
                    .trim_end_matches(")")
                    .trim_end_matches(")")
                    .trim_end_matches(",")
                    .to_string()
            })
            .collect(),
    )
}
