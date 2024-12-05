pub mod message_writer;
pub mod message_writer_channel;

use serde::Serialize;

// TODO: Might need to rename since `In` is not supported
#[derive(Debug, Clone, Serialize)]
pub enum StdStream {
    #[serde(rename = "stdout")]
    Out,
    #[serde(rename = "stderr")]
    Err,
}
