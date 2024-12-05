use std::{collections::HashMap, ops::Deref};

use serde_json::{Map, Value};

use crate::types::PipesMessage;
use crate::writer::message_writer_channel::{
    BufferedStreamChannel, FileChannel, MessageWriterChannel, StreamChannel,
};
use crate::writer::StdStream;

mod private {
    pub struct Token; // To seal certain trait methods
}

pub trait MessageWriter {
    type Channel: MessageWriterChannel;

    fn open(&self, params: Map<String, Value>) -> Self::Channel;

    /// Return a payload containing information about the external process to be passed back to
    /// the orchestration process. This should contain information that cannot be known before
    /// the external process is launched.
    ///
    /// This method is sealed — it should not be overridden by users.
    /// Instead, users should override `get_opened_extras` to inject custom data.
    fn get_opened_payload(&self, _: private::Token) -> HashMap<String, Option<Value>> {
        let mut extras = HashMap::new();
        extras.insert(
            "extras".to_string(),
            Some(Value::Object(self.get_opened_extras())),
        );
        extras
    }

    fn get_opened_extras(&self) -> Map<String, Value> {
        Map::new()
    }
}

/// Public accessor to the sealed method
pub fn get_opened_payload(writer: &impl MessageWriter) -> HashMap<String, Option<Value>> {
    writer.get_opened_payload(private::Token)
}

pub struct DefaultWriter;

impl DefaultWriter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Implements `PipesMessageWriterChannel` to
/// satisfy `PipesMessageWriter`'s associated types' trait bound
pub struct BoxedChannel(Box<dyn MessageWriterChannel>);

impl MessageWriterChannel for BoxedChannel {
    fn write_message(&mut self, message: PipesMessage) {
        self.0.write_message(message);
    }
}

impl MessageWriter for DefaultWriter {
    type Channel = BoxedChannel;

    fn open(&self, params: Map<String, Value>) -> Self::Channel {
        const FILE_PATH_KEY: &str = "path";
        const STDIO_KEY: &str = "stdio";
        const BUFFERED_STDIO_KEY: &str = "buffered_stdio";
        const STDERR: &str = "stderr";
        const STDOUT: &str = "stdout";
        const INCLUDE_STDIO_IN_MESSAGES_KEY: &str = "include_stdio_in_messages";

        match (
            params.get(FILE_PATH_KEY),
            params.get(STDIO_KEY),
            params.get(BUFFERED_STDIO_KEY),
        ) {
            (Some(Value::String(path)), _, _) => {
                // TODO: This is a simplified implementation. Utilize `PipesLogWriter`
                BoxedChannel(Box::new(FileChannel::new(path.into())))
            }
            (None, Some(Value::String(stream)), _) => match stream.to_lowercase().deref() {
                STDOUT => BoxedChannel(Box::new(StreamChannel::new(StdStream::Out))),
                STDERR => BoxedChannel(Box::new(StreamChannel::new(StdStream::Err))),
                _ => panic!("Invalid stream provided for stdio writer channel"),
            },
            (None, None, Some(Value::String(stream))) => {
                // Once `PipesBufferedStreamMessageWriterChannel` is dropped, the buffered data is written
                match stream.to_lowercase().deref() {
                    STDOUT => BoxedChannel(Box::new(BufferedStreamChannel::new(StdStream::Out))),
                    STDERR => BoxedChannel(Box::new(BufferedStreamChannel::new(StdStream::Err))),
                    _ => panic!("Invalid stream provided for buffered stdio writer channel"),
                }
            }
            _ => panic!("No way to write messages"),
        }
    }
}