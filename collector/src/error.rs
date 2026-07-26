use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("SerdeError: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("format error")]
    FormatError,
    #[error("connection abort")]
    ConnectionAbort,
    /// A bounded hand-off refused the record. Fatal by policy, never retried
    /// and never swallowed — see `queue.rs`.
    #[error("{0}")]
    Queue(#[from] crate::queue::SendError),
}
