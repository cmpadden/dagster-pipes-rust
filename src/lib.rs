mod context_loader;
mod params_loader;
pub mod types;

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;

use context_loader::PayloadErrorKind;
use params_loader::ParamsError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use thiserror::Error;

use crate::context_loader::DefaultLoader as PipesDefaultContextLoader;
pub use crate::context_loader::LoadContext;
use crate::params_loader::EnvVarLoader as PipesEnvVarParamsLoader;
pub use crate::params_loader::LoadParams;
pub use crate::types::{Method, PipesContextData, PipesMessage, PipesMetadataValue};

#[derive(Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum AssetCheckSeverity {
    Warn,
    Error,
}

impl PipesMetadataValue {
    pub fn new(raw_value: types::RawValue, pipes_metadata_value_type: types::Type) -> Self {
        Self {
            raw_value: Some(raw_value),
            pipes_metadata_value_type: Some(pipes_metadata_value_type),
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
    pub fn report_asset_materialization(
        &mut self,
        asset_key: &str,
        metadata: HashMap<String, PipesMetadataValue>,
    ) {
        let params: HashMap<String, Option<serde_json::Value>> = HashMap::from([
            ("asset_key".to_string(), Some(json!(asset_key))),
            ("metadata".to_string(), Some(json!(metadata))),
            ("data_version".to_string(), None), // TODO - support data versions
        ]);

        let msg = PipesMessage {
            dagster_pipes_version: "0.1".to_string(),
            method: Method::ReportAssetMaterialization,
            params: Some(params),
        };
        self.writer.write_message(&msg);
    }

    pub fn report_asset_check(
        &mut self,
        check_name: &str,
        passed: bool,
        asset_key: &str,
        severity: &AssetCheckSeverity,
        metadata: HashMap<String, PipesMetadataValue>,
    ) {
        let params: HashMap<String, Option<serde_json::Value>> = HashMap::from([
            ("asset_key".to_string(), Some(json!(asset_key))),
            ("check_name".to_string(), Some(json!(check_name))),
            ("passed".to_string(), Some(json!(passed))),
            ("severity".to_string(), Some(json!(severity))),
            ("metadata".to_string(), Some(json!(metadata))),
        ]);

        let msg = PipesMessage {
            dagster_pipes_version: "0.1".to_string(),
            method: Method::ReportAssetCheck,
            params: Some(params),
        };
        self.writer.write_message(&msg);
    }
}

#[derive(Debug)]
struct PipesFileMessageWriter {
    path: String,
}
impl PipesFileMessageWriter {
    fn write_message(&mut self, message: &PipesMessage) {
        let serialized_msg = serde_json::to_string(&message).unwrap();
        let mut file = OpenOptions::new().append(true).open(&self.path).unwrap();
        writeln!(file, "{serialized_msg}").unwrap();

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
#[non_exhaustive]
pub enum DagsterPipesError {
    #[error("dagster pipes failed to load params: {0}")]
    #[non_exhaustive]
    ParamsLoader(#[from] ParamsError),

    #[error("dagster pipes failed to load context: {0}")]
    #[non_exhaustive]
    ContextLoader(#[from] PayloadErrorKind),
}

// partial translation of
// https://github.com/dagster-io/dagster/blob/258d9ca0db/python_modules/dagster-pipes/dagster_pipes/__init__.py#L798-L838
#[must_use]
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_write_pipes_metadata() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap().to_string();

        let writer = PipesFileMessageWriter { path };
        let asset_metadata = HashMap::from([
            (
                "text".to_string(),
                PipesMetadataValue::new(
                    types::RawValue::String("hello".to_string()),
                    types::Type::Text,
                ),
            ),
            (
                "url".to_string(),
                PipesMetadataValue::new(
                    types::RawValue::String("http://someurl.com".to_string()),
                    types::Type::Url,
                ),
            ),
            (
                "path".to_string(),
                PipesMetadataValue::new(
                    types::RawValue::String("file://some/path".to_string()),
                    types::Type::Path,
                ),
            ),
            (
                "notebook".to_string(),
                PipesMetadataValue::new(
                    types::RawValue::String("notebook".to_string()),
                    types::Type::Notebook,
                ),
            ),
            (
                "json_object".to_string(),
                PipesMetadataValue::new(
                    types::RawValue::AnythingMap(HashMap::from([(
                        "key".to_string(),
                        Some(json!("value")),
                    )])),
                    types::Type::Json,
                ),
            ),
            (
                "json_array".to_string(),
                PipesMetadataValue::new(
                    types::RawValue::AnythingArray(vec![Some(json!({"key": "value"}))]),
                    types::Type::Json,
                ),
            ),
            (
                "md".to_string(),
                PipesMetadataValue::new(
                    types::RawValue::String("## markdown".to_string()),
                    types::Type::Md,
                ),
            ),
            (
                "dagster_run".to_string(),
                PipesMetadataValue::new(
                    types::RawValue::String("1234".to_string()),
                    types::Type::DagsterRun,
                ),
            ),
            (
                "asset".to_string(),
                PipesMetadataValue::new(
                    types::RawValue::String("some_asset".to_string()),
                    types::Type::Asset,
                ),
            ),
            (
                "job".to_string(),
                PipesMetadataValue::new(
                    types::RawValue::String("some_job".to_string()),
                    types::Type::Job,
                ),
            ),
            (
                "timestamp".to_string(),
                PipesMetadataValue::new(
                    types::RawValue::String("2012-04-23T18:25:43.511Z".to_string()),
                    types::Type::Timestamp,
                ),
            ),
            (
                "int".to_string(),
                PipesMetadataValue::new(types::RawValue::Integer(100), types::Type::Int),
            ),
            (
                "float".to_string(),
                PipesMetadataValue::new(types::RawValue::Double(100.0), types::Type::Float),
            ),
            (
                "bool".to_string(),
                PipesMetadataValue::new(types::RawValue::Bool(true), types::Type::Bool),
            ),
            (
                "none".to_string(),
                PipesMetadataValue {
                    raw_value: None,
                    pipes_metadata_value_type: None,
                },
            ),
        ]);

        let mut context = PipesContext {
            data: PipesContextData {
                asset_keys: Some(vec!["asset1".to_string()]),
                code_version_by_asset_key: None,
                extras: None,
                job_name: None,
                partition_key: None,
                partition_key_range: None,
                partition_time_window: None,
                provenance_by_asset_key: None,
                retry_number: 0,
                run_id: "012345".to_string(),
            },
            writer,
        };
        context.report_asset_materialization("asset1", asset_metadata);

        assert_eq!(
            serde_json::from_str::<PipesMessage>(&fs::read_to_string(file.path()).unwrap())
                .unwrap(),
            PipesMessage {
                dagster_pipes_version: "0.1".to_string(),
                method: Method::ReportAssetMaterialization,
                params: Some(HashMap::from([
                    ("asset_key".to_string(), Some(json!("asset1"))),
                    (
                        "metadata".to_string(),
                        Some(json!({
                            "text": {
                                "raw_value": "hello",
                                "type": "text"
                            },
                            "url": {
                                "raw_value": "http://someurl.com",
                                "type": "url"
                            },
                            "path": {
                                "raw_value": "file://some/path",
                                "type": "path"
                            },
                            "notebook": {
                                "raw_value": "notebook",
                                "type": "notebook"
                            },
                            "json_object": {
                                "raw_value": {"key": "value"},
                                "type": "json"
                            },
                            "json_array": {
                                "raw_value": [{"key": "value"}],
                                "type": "json"
                            },
                            "md": {
                                "raw_value": "## markdown",
                                "type": "md"
                            },
                            "dagster_run": {
                                "raw_value": "1234",
                                "type": "dagster_run"
                            },
                            "asset": {
                                "raw_value": "some_asset",
                                "type": "asset"
                            },
                            "job": {
                                "raw_value": "some_job",
                                "type": "job"
                            },
                            "timestamp": {
                                "raw_value": "2012-04-23T18:25:43.511Z",
                                "type": "timestamp"
                            },
                            "int": {
                                "raw_value": 100,
                                "type": "int"
                            },
                            "float": {
                                "raw_value": 100.0,
                                "type": "float"
                            },
                            "bool": {
                                "raw_value": true,
                                "type": "bool"
                            },
                            "none": {
                                "raw_value": null,
                                "type": null
                            }
                        }))
                    ),
                    ("data_version".to_string(), None),
                ])),
            }
        );
    }
}
