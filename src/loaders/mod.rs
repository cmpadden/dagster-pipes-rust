use thiserror::Error;

pub mod context_loader;
pub mod params_loader;

// #[derive(Debug, Error)]
// #[error("failed to load information from Dagster")]
// #[non_exhaustive]
// pub struct LoadError {
//     pub source: LoadErrorKind,
// }

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum LoadErrorKind {
    #[error("failed to load data from file")]
    #[non_exhaustive]
    IO(#[from] std::io::Error),

    #[error("failed to deserialize value to JSON")]
    #[non_exhaustive]
    Json(#[from] serde_json::Error),

    #[error("failed to decode base64")]
    #[non_exhaustive]
    Base64(#[from] base64::DecodeError),

    #[error("missing env var")]
    #[non_exhaustive]
    EnvVar(#[from] std::env::VarError),

    // For users to extend with their own error types if they implement custom loaders
    #[error(transparent)]
    UserDefined(Box<dyn std::error::Error + Send + Sync>),
}
