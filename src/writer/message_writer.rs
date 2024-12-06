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

/// Write messages back to Dagster, via its associated [`Self::Channel`].
pub trait MessageWriter {
    type Channel: MessageWriterChannel;

    /// Initialize a channel for writing messages back to Dagster.
    ///
    /// This method should takes the params passed by the orchestration-side
    /// `PipesMessageReader` and use them to construct and yield
    /// [`MessageWriterChannel`].
    fn open(&self, params: Map<String, Value>) -> Self::Channel;

    /// Return a payload containing information about the external process to be passed back to
    /// the orchestration process. This should contain information that cannot be known before
    /// the external process is launched.
    ///
    /// # Note
    /// This method is sealed — it should not be overridden by users.
    /// Instead, users should override [`Self::get_opened_extras`] to inject custom data.
    ///
    /// ```compile_fail
    /// # use serde_json::{Map, Value};
    /// # use dagster_pipes_rust::{MessageWriter, MessageWriterChannel, PipesMessage};
    /// #
    /// struct MyMessageWriter(u64);
    /// #
    /// # struct MyChannel;
    /// # impl MessageWriterChannel for MyChannel {
    /// #    fn write_message(&mut self, message: PipesMessage) {
    /// #        todo!()
    /// #    }
    /// # };
    ///
    /// impl MessageWriter for MyMessageWriter {
    /// #    type Channel = MyChannel;
    /// #
    /// #    fn open(&self, params: Map<String, Value>) -> Self::Channel {
    ///      // ...
    /// #          todo!()
    /// #    }
    /// }
    ///
    /// MyMessageWriter(42).get_opened_payload(private::Token); // use of undeclared crate or module `private`
    /// ```
    fn get_opened_payload(&self, _: private::Token) -> HashMap<String, Option<Value>> {
        let mut extras = HashMap::new();
        extras.insert(
            "extras".to_string(),
            Some(Value::Object(self.get_opened_extras())),
        );
        extras
    }

    /// Return arbitary reader-specific information to be passed back to the orchestration
    /// process. The information will be returned under the `extras` key of the initialization payload.
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

/// Extend `Box<dyn MessageWriterChannel>` to implement [`MessageWriterChannel`]
/// to satisfy [`MessageWriter`]'s type constraints
impl MessageWriterChannel for Box<dyn MessageWriterChannel> {
    fn write_message(&mut self, message: PipesMessage) {
        // Dereference twice to the inner field
        (**self).write_message(message);
    }
}

impl MessageWriter for DefaultWriter {
    type Channel = Box<dyn MessageWriterChannel>;

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
                Box::new(FileChannel::new(path.into()))
            }
            (None, Some(Value::String(stream)), _) => match stream.to_lowercase().deref() {
                STDOUT => Box::new(StreamChannel::new(StdStream::Out)),
                STDERR => Box::new(StreamChannel::new(StdStream::Err)),
                _ => panic!("Invalid stream provided for stdio writer channel"),
            },
            (None, None, Some(Value::String(stream))) => {
                // Once `PipesBufferedStreamMessageWriterChannel` is dropped, the buffered data is written
                match stream.to_lowercase().deref() {
                    STDOUT => Box::new(BufferedStreamChannel::new(StdStream::Out)),
                    STDERR => Box::new(BufferedStreamChannel::new(StdStream::Err)),
                    _ => panic!("Invalid stream provided for buffered stdio writer channel"),
                }
            }
            _ => panic!("No way to write messages"),
        }
    }
}
