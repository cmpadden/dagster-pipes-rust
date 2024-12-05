use std::{ffi::OsString, fs::OpenOptions, io::Write};

use crate::types::PipesMessage;

use super::StdStream;

pub trait MessageWriterChannel {
    fn write_message(&mut self, message: PipesMessage);
}

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
    /// Flushes the data when out of scope
    fn drop(&mut self) {
        self.flush();
    }
}

impl MessageWriterChannel for BufferedStreamChannel {
    fn write_message(&mut self, message: PipesMessage) {
        self.buffer.push(message);
    }
}
