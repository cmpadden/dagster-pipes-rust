use std::{ffi::OsString, fs::OpenOptions, io::Write};

use crate::types::PipesMessage;

use super::StdStream;

/// Write messages back to the Dagster orchestration process.
/// To be used in conjunction with [`MessageWriter`](crate::MessageWriter).
pub trait MessageWriterChannel {
    /// Write a message to the orchestration process
    fn write_message(&mut self, message: PipesMessage);
}

#[derive(Debug, PartialEq)]
pub struct FileChannel {
    path: OsString,
}

impl FileChannel {
    pub fn new(path: OsString) -> Self {
        Self { path }
    }
}

impl MessageWriterChannel for FileChannel {
    fn write_message(&mut self, message: PipesMessage) {
        let mut file = OpenOptions::new().append(true).open(&self.path).unwrap();
        let json = serde_json::to_string(&message).unwrap();
        writeln!(file, "{json}").unwrap();
    }
}

#[derive(Debug, PartialEq)]
pub struct StreamChannel {
    stream: StdStream,
}

impl StreamChannel {
    pub fn new(stream: StdStream) -> Self {
        Self { stream }
    }

    fn _format_message(message: &PipesMessage) -> Vec<u8> {
        format!("{}\n", serde_json::to_string(message).unwrap()).into_bytes()
    }
}

impl MessageWriterChannel for StreamChannel {
    fn write_message(&mut self, message: PipesMessage) {
        match self.stream {
            StdStream::Out => std::io::stdout()
                .write_all(&Self::_format_message(&message))
                .unwrap(),
            StdStream::Err => std::io::stderr()
                .write_all(&Self::_format_message(&message))
                .unwrap(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct BufferedStreamChannel {
    buffer: Vec<PipesMessage>,
    stream: StdStream,
}

impl BufferedStreamChannel {
    pub fn new(stream: StdStream) -> Self {
        Self {
            buffer: vec![],
            stream,
        }
    }

    /// Flush messages in the buffer to the stream
    /// <div class="warning">This class will called once on `Drop`</div>
    fn flush(&mut self) {
        let _: Vec<_> = self
            .buffer
            .iter()
            .map(|msg| match self.stream {
                StdStream::Out => std::io::stdout()
                    .write(&Self::_format_message(msg))
                    .unwrap(),
                StdStream::Err => std::io::stderr()
                    .write(&Self::_format_message(msg))
                    .unwrap(),
            })
            .collect();
        self.buffer.clear();
    }

    fn _format_message(message: &PipesMessage) -> Vec<u8> {
        format!("{}\n", serde_json::to_string(message).unwrap()).into_bytes()
    }
}

impl Drop for BufferedStreamChannel {
    /// Flush the data when out of scope or panicked.
    /// <div class="warning">Panic aborting will prevent `Drop` and this function from running</div>
    fn drop(&mut self) {
        self.flush();
    }
}

impl MessageWriterChannel for BufferedStreamChannel {
    fn write_message(&mut self, message: PipesMessage) {
        self.buffer.push(message);
    }
}

#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum DefaultChannel {
    File(FileChannel),
    Stream(StreamChannel),
    BufferedStream(BufferedStreamChannel),
}

impl MessageWriterChannel for DefaultChannel {
    fn write_message(&mut self, message: PipesMessage) {
        match self {
            Self::File(channel) => channel.write_message(message),
            Self::Stream(channel) => channel.write_message(message),
            Self::BufferedStream(channel) => channel.write_message(message),
        }
    }
}

#[cfg(test)]
mod tests_file_channel {
    use tempfile::NamedTempFile;

    use crate::{Method, PipesMessage};

    use super::{FileChannel, MessageWriterChannel};

    #[test]
    fn test_write_message() {
        let file = NamedTempFile::new().expect("Failed to create tempfile for testing");
        let mut channel = FileChannel::new(file.path().into());
        let message = PipesMessage::new(Method::Opened, None);
        channel.write_message(message.clone());

        let file_content =
            std::fs::read_to_string(file.path()).expect("Failed to read from tempfile");
        assert_eq!(
            message,
            serde_json::from_str(&file_content).expect("Failed to serialize PipesMessage")
        );
    }
}
