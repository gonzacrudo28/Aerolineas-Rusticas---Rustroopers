use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct User {
    pub name: String,
    pub password: String,
}
