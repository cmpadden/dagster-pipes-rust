mod context_loader;
mod params_loader;

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::context_loader::PipesContextLoader;
use crate::context_loader::PipesDefaultContextLoader;
use crate::params_loader::PipesEnvVarParamsLoader;
use crate::params_loader::PipesParamsLoader;

// partial translation of
// https://github.com/dagster-io/dagster/blob/258d9ca0db/python_modules/dagster-pipes/dagster_pipes/__init__.py#L94-L108
#[derive(Debug, Deserialize, PartialEq)]
pub struct PipesContextData {
    asset_keys: Option<Vec<String>>,
    run_id: String,
    extras: HashMap<String, serde_json::Value>,
}

// translation of
// https://github.com/dagster-io/dagster/blob/258d9ca0db/python_modules/dagster-pipes/dagster_pipes/__init__.py#L83-L88
#[derive(Debug, Serialize)]
struct PipesMessage {
    __dagster_pipes_version: String,
    method: String,
    params: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize)]
pub enum AssetCheckSeverity {
    Warn,
    Error,
}

impl std::fmt::Display for AssetCheckSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Self::Warn => write!(f, "WARN"),
            Self::Error => write!(f, "ERROR"),
        }
    }
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
        let params: HashMap<String, serde_json::Value> = HashMap::from([
            ("asset_key".to_string(), json!(asset_key)),
            ("metadata".to_string(), metadata),
            ("data_version".to_string(), json!(null)), // TODO - support data versions
        ]);

        let msg = PipesMessage {
            __dagster_pipes_version: "0.1".to_string(),
            method: "report_asset_materialization".to_string(),
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
        let params: HashMap<String, serde_json::Value> = HashMap::from([
            ("asset_key".to_string(), json!(asset_key)),
            ("check_name".to_string(), json!(check_name)),
            ("passed".to_string(), json!(passed)),
            ("severity".to_string(), json!(severity.to_string())),
            ("metadata".to_string(), metadata),
        ]);

        let msg = PipesMessage {
            __dagster_pipes_version: "0.1".to_string(),
            method: "report_asset_check".to_string(),
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

// partial translation of
// https://github.com/dagster-io/dagster/blob/258d9ca0db/python_modules/dagster-pipes/dagster_pipes/__init__.py#L798-L838
pub fn open_dagster_pipes() -> PipesContext {
    let params_loader = PipesEnvVarParamsLoader::new();
    let context_loader = PipesDefaultContextLoader::new();

    let context_params = params_loader.load_context_params();
    let message_params = params_loader.load_message_params();

    let path = "path".to_string(); // Placeholder variable until MessageWriter is implemented

    //if stdio != "stderr" {
    //    panic!("only stderr supported for dagster pipes messages")
    //}

    PipesContext {
        data: context_loader.load_context(context_params),
        writer: PipesFileMessageWriter { path },
    }
}
