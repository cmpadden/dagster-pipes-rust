use base64::prelude::*;
use flate2::read::ZlibDecoder;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use std::io::Read;

const DAGSTER_PIPES_CONTEXT_ENV_VAR: &str = "DAGSTER_PIPES_CONTEXT";
const DAGSTER_PIPES_MESSAGES_ENV_VAR: &str = "DAGSTER_PIPES_MESSAGES";

/// Load params passed from the orchestration process by the context injector and
/// message reader. These params are used to respectively bootstrap the
/// [`PipesContextLoader`] and [`PipesMessageWriter`].
pub trait PipesParamsLoader {
    /// Whether or not this process has been provided with provided with information
    /// to create a PipesContext or should instead return a mock.
    fn is_dagster_pipes_process(&self) -> bool;
    /// Load params passed by the orchestration-side context injector.
    fn load_context_params(&self) -> Map<String, Value>;
    /// Load params passed by the orchestration-side message reader.
    fn load_message_params(&self) -> Map<String, Value>;
}

#[derive(Debug, Default)]
pub struct PipesEnvVarParamsLoader;

impl PipesEnvVarParamsLoader {
    pub fn new() -> Self {
        Self
    }
}

impl PipesParamsLoader for PipesEnvVarParamsLoader {
    fn is_dagster_pipes_process(&self) -> bool {
        std::env::var(DAGSTER_PIPES_CONTEXT_ENV_VAR).is_ok()
    }

    fn load_context_params(&self) -> Map<String, Value> {
        let param = std::env::var(DAGSTER_PIPES_CONTEXT_ENV_VAR).unwrap();
        decode_env_var(&param)
    }

    fn load_message_params(&self) -> Map<String, Value> {
        let param = std::env::var(DAGSTER_PIPES_MESSAGES_ENV_VAR).unwrap();
        decode_env_var(&param)
    }
}

// translation of
// https://github.com/dagster-io/dagster/blob/258d9ca0db/python_modules/dagster-pipes/dagster_pipes/__init__.py#L354-L367
fn decode_env_var<T>(param: &str) -> T
where
    T: DeserializeOwned,
{
    let zlib_compressed_slice = BASE64_STANDARD.decode(param).unwrap();
    let mut decoder = ZlibDecoder::new(&zlib_compressed_slice[..]);
    let mut json_str = String::new();
    decoder.read_to_string(&mut json_str).unwrap();
    serde_json::from_str(&json_str).unwrap()
}
