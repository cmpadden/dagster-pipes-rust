mod context_loader;
mod params_loader;
mod types;
mod types_ext;
mod writer;


use std::collections::HashMap;

use serde::Serialize;
use serde_json::json;
use serde_json::Map;
use serde_json::Value;
use thiserror::Error;

use crate::context_loader::DefaultLoader as PipesDefaultContextLoader;
pub use crate::context_loader::LoadContext;
use crate::context_loader::PayloadErrorKind;
use crate::params_loader::EnvVarLoader as PipesEnvVarParamsLoader;
pub use crate::params_loader::LoadParams;
use crate::params_loader::ParamsError;
pub use crate::types::{Method, PipesContextData, PipesMessage};
use crate::writer::message_writer::get_opened_payload;
use crate::writer::message_writer::DefaultWriter as PipesDefaultMessageWriter;
pub use crate::writer::message_writer::MessageWriter;
pub use crate::writer::message_writer_channel::MessageWriterChannel;

#[derive(Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum AssetCheckSeverity {
    Warn,
    Error,
}

// partial translation of
// https://github.com/dagster-io/dagster/blob/258d9ca0db/python_modules/dagster-pipes/dagster_pipes/__init__.py#L859-L871
#[derive(Debug)]
pub struct PipesContext<W>
where
    W: MessageWriter,
{
    data: PipesContextData,
    message_channel: W::Channel,
}

impl<W> PipesContext<W>
where
    W: MessageWriter,
{
    pub fn new(
        context_data: PipesContextData,
        message_params: Map<String, Value>,
        message_writer: &W,
    ) -> Self {
        let mut message_channel = message_writer.open(message_params);
        let opened_payload = get_opened_payload(message_writer);
        let opened_message = PipesMessage {
            dagster_pipes_version: "0.1".to_string(), // TODO: Convert to `const`
            method: Method::Opened,
            params: Some(opened_payload),
        };
        message_channel.write_message(opened_message);

        Self {
            data: context_data,
            message_channel,
        }
    }

    pub fn report_asset_materialization(&mut self, asset_key: &str, metadata: serde_json::Value) {
        let params: HashMap<String, Option<serde_json::Value>> = HashMap::from([
            ("asset_key".to_string(), Some(json!(asset_key))),
            ("metadata".to_string(), Some(metadata)),
            ("data_version".to_string(), None), // TODO - support data versions
        ]);

        let msg = PipesMessage::new(Method::ReportAssetMaterialization, Some(params));
        self.message_channel.write_message(msg);
    }

    pub fn report_asset_check(
        &mut self,
        check_name: &str,
        passed: bool,
        asset_key: &str,
        severity: &AssetCheckSeverity,
        metadata: serde_json::Value,
    ) {
        let params: HashMap<String, Option<serde_json::Value>> = HashMap::from([
            ("asset_key".to_string(), Some(json!(asset_key))),
            ("check_name".to_string(), Some(json!(check_name))),
            ("passed".to_string(), Some(json!(passed))),
            ("severity".to_string(), Some(json!(severity))),
            ("metadata".to_string(), Some(metadata)),
        ]);

        let msg = PipesMessage::new(Method::ReportAssetCheck, Some(params));
        self.message_channel.write_message(msg);
    }
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
pub fn open_dagster_pipes() -> Result<PipesContext<PipesDefaultMessageWriter>, DagsterPipesError> {
    let params_loader = PipesEnvVarParamsLoader::new();
    let context_loader = PipesDefaultContextLoader::new();
    let message_writer = PipesDefaultMessageWriter::new();

    let context_params = params_loader.load_context_params()?;
    let message_params = params_loader.load_message_params()?;

    let context_data = context_loader.load_context(context_params)?;

    Ok(PipesContext::new(
        context_data,
        message_params,
        &message_writer,
    ))
}
