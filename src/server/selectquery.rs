use crate::protocol::query_parser::clause::Clause;

pub struct SelectQuery<'a> {
    pub conditions: &'a Clause,
    pub selected_columns: &'a [String],
    pub order: &'a [String],
    pub table_name: &'a str,
    pub needs_ts: bool,
    pub needs_tb: bool,
}

impl<'a> SelectQuery<'a> {
    pub fn new(
        conditions: &'a Clause,
        selected_columns: &'a [String],
        order: &'a [String],
        table_name: &'a str,
        needs_ts: bool,
        needs_tb: bool,
    ) -> Self {
        Self {
            conditions,
            selected_columns,
            order,
            table_name,
            needs_ts,
            needs_tb,
        }
    }
}
