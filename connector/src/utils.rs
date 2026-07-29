use std::{
    collections::HashSet,
    fmt,
    fmt::{Debug, Write},
    future::Future,
    marker::PhantomData,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose};
use chrono::Utc;
use ed25519_dalek::{Signature as Ed25519Signature, Signer, SigningKey, pkcs8::DecodePrivateKey};
use hashbrown::Equivalent;
use hftbacktest::prelude::OrderId;
use hmac::{Hmac, Mac};
use rand::Rng;
use serde::{
    Deserialize,
    Deserializer,
    de,
    de::{Error, Visitor},
};
use sha2::Sha256;

use crate::bybit::BybitError;

struct I64Visitor;

impl Visitor<'_> for I64Visitor {
    type Value = Option<i64>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string containing an i64 number")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if s.is_empty() {
            Ok(Some(0))
        } else {
            Ok(Some(s.parse::<i64>().map_err(Error::custom)?))
        }
    }
}

struct OptionF64Visitor;

impl<'de> Visitor<'de> for OptionF64Visitor {
    type Value = Option<f64>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string containing an f64 number")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(None)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(F64Visitor)
    }
}

struct F64Visitor;

impl Visitor<'_> for F64Visitor {
    type Value = Option<f64>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string containing an f64 number")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if s.is_empty() {
            Ok(None)
        } else {
            Ok(Some(s.parse::<f64>().map_err(Error::custom)?))
        }
    }
}

pub fn from_str_to_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer
        .deserialize_str(I64Visitor)
        .map(|value| value.unwrap_or(0))
}

pub fn from_str_to_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer
        .deserialize_str(F64Visitor)
        .map(|value| value.unwrap_or(0.0))
}

pub fn from_str_to_f64_opt<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_option(OptionF64Visitor)
}

pub fn to_uppercase<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    Ok(s.to_uppercase())
}

pub fn to_lowercase<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    Ok(s.to_lowercase())
}

pub fn sign_hmac_sha256(secret: &str, s: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(s.as_bytes());
    let hash = mac.finalize().into_bytes();
    let mut tmp = String::with_capacity(hash.len() * 2);
    for c in hash {
        write!(&mut tmp, "{c:02x}").unwrap();
    }
    tmp
}

pub fn sign_ed25519(private_key: &str, s: &str) -> String {
    let private_key = SigningKey::from_pkcs8_pem(private_key).unwrap();
    let signature: Ed25519Signature = private_key.sign(s.as_bytes());
    general_purpose::STANDARD.encode(signature.to_bytes())
}

pub fn get_timestamp() -> u64 {
    Utc::now().timestamp_millis() as u64
}

pub type PxQty = (f64, f64);

pub fn parse_depth(
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
) -> Result<(Vec<PxQty>, Vec<PxQty>), BybitError> {
    let mut bids_ = Vec::with_capacity(bids.len());
    for (px, qty) in bids {
        bids_.push(parse_px_qty_tup(px, qty)?);
    }
    let mut asks_ = Vec::with_capacity(asks.len());
    for (px, qty) in asks {
        asks_.push(parse_px_qty_tup(px, qty)?);
    }
    Ok((bids_, asks_))
}

pub fn parse_px_qty_tup(px: String, qty: String) -> Result<PxQty, BybitError> {
    Ok((px.parse()?, qty.parse()?))
}

pub trait BackoffStrategy {
    fn backoff(&mut self) -> Duration;
}

pub struct ExponentialBackoff {
    last_attempt: Instant,
    factor: u32,
    last_delay: Option<Duration>,
    reset_interval: Option<Duration>,
    min_delay: Duration,
    max_delay: Option<Duration>,
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self {
            last_attempt: Instant::now(),
            factor: 2,
            last_delay: None,
            reset_interval: Some(Duration::from_secs(300)),
            min_delay: Duration::from_millis(100),
            max_delay: Some(Duration::from_secs(60)),
        }
    }
}

impl ExponentialBackoff {
    /// A ladder bounded by `min_delay`..`max_delay`, doubling in between.
    ///
    /// The defaults (100 ms..60 s) are not right for every venue: Hyperliquid sits behind
    /// a CDN that drops sockets roughly nine times a day and recovers in about 1.2 s, so
    /// reconnecting sooner than a second only spends rate limit, and waiting a minute
    /// leaves the feed dark far longer than the outage.
    pub fn with_bounds(min_delay: Duration, max_delay: Duration) -> Self {
        Self {
            min_delay,
            max_delay: Some(max_delay),
            ..Default::default()
        }
    }
}

impl BackoffStrategy for ExponentialBackoff {
    fn backoff(&mut self) -> Duration {
        if let Some(reset_interval) = self.reset_interval
            && self.last_attempt.elapsed() > reset_interval
        {
            self.last_delay = None;
        }

        self.last_attempt = Instant::now();

        match self.last_delay {
            None => {
                self.last_delay = Some(self.min_delay);
                self.min_delay
            }
            Some(last_delay) => {
                let mut delay = last_delay.saturating_mul(self.factor);

                if let Some(max_delay) = self.max_delay
                    && delay > max_delay
                {
                    delay = max_delay;
                }
                self.last_delay = Some(delay);
                delay
            }
        }
    }
}

pub struct Retry<O, E, Backoff, ErrorHandler> {
    backoff: Backoff,
    error_handler: Option<ErrorHandler>,
    _o_marker: PhantomData<O>,
    _e_marker: PhantomData<E>,
}

impl<O, E, Backoff, ErrorHandler> Retry<O, E, Backoff, ErrorHandler>
where
    E: Debug,
    Backoff: BackoffStrategy,
    ErrorHandler: FnMut(E) -> Result<(), E>,
{
    pub fn new(backoff: Backoff) -> Self {
        Self {
            backoff,
            error_handler: None,
            _o_marker: Default::default(),
            _e_marker: Default::default(),
        }
    }

    pub fn error_handler(self, error_handler: ErrorHandler) -> Self {
        Self {
            error_handler: Some(error_handler),
            ..self
        }
    }

    pub async fn retry<F, Fut>(&mut self, func: F) -> Result<O, E>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<O, E>>,
    {
        loop {
            match func().await {
                Ok(o) => return Ok(o),
                Err(error) => {
                    if let Some(error_handler) = self.error_handler.as_mut() {
                        error_handler(error)?;
                    }
                    tokio::time::sleep(self.backoff.backoff()).await;
                }
            }
        }
    }
}

#[derive(Eq, Hash, PartialEq, Debug)]
pub struct SymbolOrderId {
    pub symbol: String,
    pub order_id: OrderId,
}

impl SymbolOrderId {
    pub fn new(symbol: String, order_id: OrderId) -> Self {
        Self { symbol, order_id }
    }
}

#[derive(Eq, Hash, PartialEq, Debug)]
pub struct RefSymbolOrderId<'a> {
    pub symbol: &'a str,
    pub order_id: OrderId,
}

impl<'a> RefSymbolOrderId<'a> {
    pub fn new(symbol: &'a str, order_id: OrderId) -> Self {
        Self { symbol, order_id }
    }
}

impl Equivalent<SymbolOrderId> for RefSymbolOrderId<'_> {
    fn equivalent(&self, key: &SymbolOrderId) -> bool {
        key.symbol == self.symbol && key.order_id == self.order_id
    }
}

/// Which symbols have been subscribed **on the current connection**.
///
/// A connection owns one of these and never inherits another's: it is built where the
/// connection is served (`serve` on bybit and both Binance backends, `connect` on
/// Hyperliquid), so every reconnect re-derives its subscriptions from the shared symbol set.
/// Hoisting one to struct level would let it survive a reconnect and suppress exactly the
/// resubscription it exists to allow — the bug in `AGENTS.md` §4.2. Each backend has a test
/// that serves two connections in a row and fails if the second subscribes nothing.
#[derive(Default)]
pub struct SubscriptionTracker {
    subscribed: HashSet<String>,
}

impl SubscriptionTracker {
    /// Registered symbols this connection has not subscribed yet, in a stable order.
    pub fn pending(&self, registered: &HashSet<String>) -> Vec<String> {
        let mut pending: Vec<String> = registered.difference(&self.subscribed).cloned().collect();
        pending.sort();
        pending
    }

    pub fn mark(&mut self, symbols: &[String]) {
        for symbol in symbols {
            self.subscribed.insert(symbol.clone());
        }
    }
}

pub fn generate_rand_string(length: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                             abcdefghijklmnopqrstuvwxyz\
                             0123456789";
    let mut rng = rand::rng();
    (0..length)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod testing {
    //! Seams shared by the connector's socketless tests.

    use std::{
        marker::PhantomData,
        pin::Pin,
        task::{Context, Poll},
    };

    use futures_util::{Sink, Stream, stream};
    use tokio_tungstenite::tungstenite::{Error as WsError, Message};

    /// A write half that records what was written, so a subscribe path can be driven without
    /// a socket.
    ///
    /// Generic over the backend's error type because every backend has its own and none of
    /// them is ever produced here: this sink accepts everything.
    pub struct RecordingSink<E> {
        pub sent: Vec<String>,
        /// `fn() -> E` rather than `E`: a plain `PhantomData<E>` would leak `E`'s auto traits
        /// and cost this sink its unconditional `Unpin`, which `Pin<&mut Self>` needs.
        _error: PhantomData<fn() -> E>,
    }

    impl<E> Default for RecordingSink<E> {
        fn default() -> Self {
            Self {
                sent: Vec::new(),
                _error: PhantomData,
            }
        }
    }

    impl<E> Sink<Message> for RecordingSink<E> {
        type Error = E;

        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            self.sent.push(item.into_text().unwrap().to_string());
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    /// A read half that hands the connection loop `frames` as text and then ends.
    ///
    /// Ending is how the loop under test terminates: a read half that is done is a dropped
    /// socket, which every backend reports as `ConnectionInterrupted`.
    pub fn read_frames(
        frames: Vec<String>,
    ) -> impl Stream<Item = Result<Message, WsError>> + Unpin {
        stream::iter(
            frames
                .into_iter()
                .map(|frame| Ok(Message::Text(frame.into()))),
        )
    }

    /// A read half that is already gone: the loop subscribes, reads nothing, and reports the
    /// connection interrupted. What the write half recorded is therefore exactly what went out
    /// *at connect*.
    pub fn closed_read() -> impl Stream<Item = Result<Message, WsError>> + Unpin {
        read_frames(Vec::new())
    }

    /// A read half that runs `script` the first time the loop polls it — by which point the
    /// connect-time subscribe has already gone out — and then stays quiet for ever.
    ///
    /// This is how a registration arriving *during* a connection is delivered without a socket
    /// and without a second task. The script ends the loop itself by dropping the wake-up
    /// sender it captured: the loops return `Ok(())` when the broadcast closes, and every
    /// wake-up already buffered is delivered before that.
    pub fn read_after_connect<F>(script: F) -> impl Stream<Item = Result<Message, WsError>> + Unpin
    where
        F: FnOnce(),
    {
        let mut script = Some(script);
        stream::poll_fn(move |_| {
            if let Some(script) = script.take() {
                script();
            }
            // Never woken again on purpose: the loop is driven from here on by the wake-ups
            // the script sent, and it must not see a frame it did not ask for.
            Poll::Pending
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        thread,
        time::{Duration, Instant},
    };

    use hashbrown::HashMap;

    use crate::utils::{BackoffStrategy, ExponentialBackoff, RefSymbolOrderId, SymbolOrderId};

    #[test]
    fn equivalent_symbol_order_id() {
        let mut map = HashMap::new();
        map.insert(
            SymbolOrderId::new("key1".to_string(), 1),
            "value1".to_string(),
        );

        assert_eq!(
            map.get(&RefSymbolOrderId::new("key1", 1)).unwrap(),
            "value1"
        )
    }

    #[test]
    fn test_backoff() {
        let mut backoff = ExponentialBackoff {
            last_attempt: Instant::now(),
            factor: 2,
            last_delay: None,
            reset_interval: None,
            min_delay: Duration::from_millis(0),
            max_delay: None,
        };

        let mut value = Duration::from_secs(0);
        for _ in 0..10 {
            let new_value = backoff.backoff();
            assert_eq!(new_value, value * backoff.factor);
            value = new_value;
        }
    }

    #[test]
    fn test_backoff_min_delay() {
        let mut backoff = ExponentialBackoff {
            last_attempt: Instant::now(),
            factor: 2,
            last_delay: None,
            reset_interval: None,
            min_delay: Duration::from_millis(100),
            max_delay: None,
        };

        assert_eq!(backoff.backoff(), backoff.min_delay);
    }

    #[test]
    fn test_backoff_max_delay() {
        let mut backoff = ExponentialBackoff {
            last_attempt: Instant::now(),
            factor: 2,
            last_delay: None,
            reset_interval: None,
            min_delay: Duration::from_millis(100),
            max_delay: Some(Duration::from_secs(1)),
        };

        for _ in 0..100 {
            backoff.backoff();
        }
        assert_eq!(backoff.backoff(), backoff.max_delay.unwrap());
    }

    /// The bounds a caller asks for are the bounds it gets: the first delay is the floor,
    /// the ladder doubles, and it never exceeds the ceiling.
    #[test]
    fn test_backoff_with_bounds() {
        let mut backoff =
            ExponentialBackoff::with_bounds(Duration::from_secs(1), Duration::from_secs(30));

        assert_eq!(backoff.backoff(), Duration::from_secs(1));
        assert_eq!(backoff.backoff(), Duration::from_secs(2));
        assert_eq!(backoff.backoff(), Duration::from_secs(4));
        for _ in 0..10 {
            backoff.backoff();
        }
        assert_eq!(backoff.backoff(), Duration::from_secs(30));
    }

    #[test]
    fn test_backoff_reset_interval() {
        let mut backoff = ExponentialBackoff {
            last_attempt: Instant::now(),
            factor: 2,
            last_delay: None,
            reset_interval: Some(Duration::from_secs(5)),
            min_delay: Duration::from_millis(100),
            max_delay: Some(Duration::from_secs(1)),
        };

        for _ in 0..100 {
            let new_value = backoff.backoff();
            if new_value == backoff.max_delay.unwrap() {
                thread::sleep(backoff.reset_interval.unwrap() + Duration::from_millis(100));
                assert_eq!(backoff.backoff(), backoff.min_delay);
                return;
            } else {
                thread::sleep(Duration::from_millis(100));
            }
        }
        panic!();
    }
}
