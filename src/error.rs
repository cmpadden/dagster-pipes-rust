use serde::Deserialize;
use thiserror::Error;

#[derive(Error, Debug, Deserialize)]
pub enum DagsterPipesError {
    #[error("failed to load data")]
    ParseError(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, DagsterPipesError>;
