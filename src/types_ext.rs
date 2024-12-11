use std::collections::HashMap;

use crate::{Method, PipesMessage};

impl PipesMessage {
    pub fn new(method: Method, params: Option<HashMap<String, Option<serde_json::Value>>>) -> Self {
        Self {
            dagster_pipes_version: "0.1".to_string(), // TODO: Make `const`
            method,
            params,
        }
    }
}
