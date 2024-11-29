mod context_loader;
mod params_loader;
mod types;

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;

use context_loader::PayloadErrorKind;
use params_loader::ParamsError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use thiserror::Error;

use crate::context_loader::PipesContextLoader;
use crate::context_loader::PipesDefaultContextLoader;
use crate::params_loader::PipesEnvVarParamsLoader;
use crate::params_loader::PipesParamsLoader;
use crate::types::{Method, PipesContextData, PipesMessage};

#[derive(Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum AssetCheckSeverity {
    Warn,
    Error,
}

// partial translation of
// https://github.com/dagster-io/dagster/blob/258d9ca0db/python_modules/dagster-pipes/dagster_pipes/__init__.py#L859-L871
#[derive(Debug)]
pub struct PipesContext {
    data: PipesContextData,
    writer: PipesFileMessageWriter,
}

impl PipesContext {
    pub fn report_asset_materialization(&mut self, asset_key: &str, metadata: serde_json::Value) {
        let params: HashMap<String, Option<serde_json::Value>> = HashMap::from([
            ("asset_key".to_string(), Some(json!(asset_key))),
            ("metadata".to_string(), Some(metadata)),
            ("data_version".to_string(), None), // TODO - support data versions
        ]);

        let msg = PipesMessage {
            dagster_pipes_version: "0.1".to_string(),
            method: Method::ReportAssetMaterialization,
            params: Some(params),
        };
        self.writer.write_message(msg);
    }

    pub fn report_asset_check(
        &mut self,
        check_name: &str,
        passed: bool,
        asset_key: &str,
        severity: AssetCheckSeverity,
        metadata: serde_json::Value,
    ) {
        let params: HashMap<String, Option<serde_json::Value>> = HashMap::from([
            ("asset_key".to_string(), Some(json!(asset_key))),
            ("check_name".to_string(), Some(json!(check_name))),
            ("passed".to_string(), Some(json!(passed))),
            ("severity".to_string(), Some(json!(severity))),
            ("metadata".to_string(), Some(metadata)),
        ]);

        let msg = PipesMessage {
            dagster_pipes_version: "0.1".to_string(),
            method: Method::ReportAssetCheck,
            params: Some(params),
        };
        self.writer.write_message(msg);
    }
}

#[derive(Debug)]
struct PipesFileMessageWriter {
    path: String,
}
impl PipesFileMessageWriter {
    fn write_message(&mut self, message: PipesMessage) {
        let serialized_msg = serde_json::to_string(&message).unwrap();
        let mut file = OpenOptions::new().append(true).open(&self.path).unwrap();
        writeln!(file, "{}", serialized_msg).unwrap();

        // TODO - optional `stderr` based writing
        //eprintln!("{}", serialized_msg);
    }
}

#[derive(Debug, Deserialize)]
struct PipesMessagesParams {
    path: Option<String>,  // write to file
    stdio: Option<String>, // stderr | stdout (unsupported)
}

#[derive(Debug, Error)]
#[error("dagster pipes failure:\n{0}")]
#[non_exhaustive]
pub struct DagsterPipesError(#[from] pub DagsterPipesErrorKind);

#[derive(Debug, Error)]
#[error(transparent)]
#[non_exhaustive]
pub enum DagsterPipesErrorKind {
    #[error("failed to load params:\n{0}")]
    #[non_exhaustive]
    ParamsLoader(#[from] ParamsError),

    #[error("failed to load context:\n{0}")]
    #[non_exhaustive]
    ContextLoader(#[from] PayloadErrorKind),
}

impl From<ParamsError> for DagsterPipesError {
    fn from(value: ParamsError) -> Self {
        DagsterPipesError(DagsterPipesErrorKind::ParamsLoader(value))
    }
}

impl From<PayloadErrorKind> for DagsterPipesError {
    fn from(value: PayloadErrorKind) -> Self {
        DagsterPipesError(DagsterPipesErrorKind::ContextLoader(value))
    }
}

// partial translation of
// https://github.com/dagster-io/dagster/blob/258d9ca0db/python_modules/dagster-pipes/dagster_pipes/__init__.py#L798-L838
pub fn open_dagster_pipes() -> Result<PipesContext, DagsterPipesError> {
    let params_loader = PipesEnvVarParamsLoader::new();
    let context_loader = PipesDefaultContextLoader::new();

    let context_params = params_loader.load_context_params()?;
    let context_data = context_loader.load_context(context_params)?;


    let message_params = params_loader.load_message_params()?;
    // TODO: Refactor into MessageWriter impl
    let path = match &message_params["path"] {
        Value::String(string) => string.clone(),
        _ => panic!("Expected message \"path\" in bootstrap payload"),
    };


    //if stdio != "stderr" {
    //    panic!("only stderr supported for dagster pipes messages")
    //}

    Ok(PipesContext {
        data: context_data,
        writer: PipesFileMessageWriter { path },
    })
}
