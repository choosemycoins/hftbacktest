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

impl ConnectorError {
    /// True when the recording has broken, as opposed to one frame being
    /// unreadable.
    ///
    /// `ConnectionAbort` is a rejected subscribe. Bybit fails the whole batch
    /// if any one topic is unknown (`orderbook.500` on a symbol that does not
    /// offer that depth), and retrying the identical batch can never succeed:
    /// the socket stays open, ping-ponging, with no subscription at all — zero
    /// bytes recorded, indefinitely, while the process looks healthy.
    ///
    /// `Queue` is a hand-off that refused a record. Carrying on would discard
    /// market data in silence.
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::ConnectionAbort | Self::Queue(_))
    }
}
