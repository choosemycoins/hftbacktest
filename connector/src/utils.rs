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
use hftbacktest::{
    prelude::OrderId,
    types::{ExecDelta, Status},
};
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

/// A **running total** of everything an order has filled, as the venue reports it.
///
/// Most REST APIs — and Lighter's order channel — state how much of an order is filled *so
/// far*, not how much the latest execution filled. `Order::exec_qty` is the opposite: a
/// per-execution delta (`AGENTS.md` §4.6). The two are the same number for an order's first
/// fill and diverge for every one after it, which is what let the confusion survive — it is
/// invisible until an order fills in parts.
///
/// Both Binance backends made exactly that mistake — `order.exec_qty = resp.executed_qty`,
/// with a comment three lines above noting that execution details arrive on the WebSocket
/// stream instead. A second partial fill re-reported everything filled before it.
///
/// This type is what stops it recurring: venue fields carrying a running total are typed with
/// it, and **there is deliberately no `From<CumulativeFilled> for ExecDelta`**. The only way
/// across is [`CumulativeFilled::advance`], which needs a watermark to subtract from — so
/// writing a total into an execution field no longer type-checks (invariant E5).
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Default)]
pub struct CumulativeFilled(f64);

impl CumulativeFilled {
    pub const ZERO: Self = Self(0.0);

    pub const fn new(total: f64) -> Self {
        Self(total)
    }

    /// The total, for arithmetic that genuinely wants the running figure — computing
    /// `leaves_qty` as `orig_qty - filled`, for instance, which is a cumulative question.
    pub const fn get(&self) -> f64 {
        self.0
    }

    /// Advances `watermark` to this total and yields what the difference executed.
    ///
    /// `None` when the total has not moved: the venue is restating a figure already accounted
    /// for, and reporting an execution for it would double-count. This is the one bridge from
    /// a running total to an [`ExecDelta`], and requiring a watermark is the point — a total
    /// cannot become a delta without naming what it is measured against.
    pub fn advance(self, watermark: &mut CumulativeFilled) -> Option<ExecDelta> {
        if self.0 > watermark.0 {
            let delta = self.0 - watermark.0;
            *watermark = self;
            Some(ExecDelta::of_execution(delta))
        } else {
            None
        }
    }
}

/// Deserialises a venue's string-encoded running total into a [`CumulativeFilled`].
pub fn from_str_to_cumulative_filled<'de, D>(deserializer: D) -> Result<CumulativeFilled, D::Error>
where
    D: Deserializer<'de>,
{
    from_str_to_f64(deserializer).map(CumulativeFilled::new)
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

/// Nanoseconds since the Unix epoch — the unit every timestamp the connector writes onto the
/// money path is in (`Order.exch_timestamp`, `Event.exch_ts`, `LiveEvent::Position.exch_ts`).
///
/// A venue reporting microseconds or milliseconds reaches this `i64` **only** through the
/// checked constructors [`Nanos::from_micros`] / [`Nanos::from_millis`]: there is no
/// `From<i64>` and no `Nanos::new`, and the field is private, so a raw microsecond value cannot
/// be assigned where nanoseconds are expected — that assignment does not compile. This
/// forecloses the microsecond/nanosecond/millisecond mix class at the connector boundary where
/// it occurs (`AGENTS.md` §4, correctness-by-construction §1.6); the seed was the Lighter order
/// path writing a raw-microsecond `transaction_time` into a nanosecond `exch_timestamp`.
///
/// The conversion is *checked* because the release profile compiles with overflow checks off
/// (`panic = "abort"`, root `Cargo.toml`): an unchecked `micros * 1_000` on a far-future time
/// would silently wrap to a garbage — possibly negative — nanosecond value and land it on the
/// money path. A value that does not fit is an [`Overflow`], to be handled fail-closed, never
/// wrapped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Nanos(i64);

/// A venue time in microseconds. Type a venue message field as this when the venue documents
/// its time in µs (e.g. Lighter `transaction_time`), so the value can reach nanoseconds only
/// through [`Nanos::from_micros`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Micros(i64);

/// A venue time in milliseconds. Same discipline as [`Micros`], bridged by
/// [`Nanos::from_millis`].
///
/// The millisecond arm of the set is exercised by the unit tests but has **no production caller
/// yet**: the Lighter order path this change fixes is microsecond-only. The connector's ms→ns
/// conversions still live as ad-hoc `checked_mul(1_000_000)` in the Hyperliquid feed path
/// (`hyperliquid/{trades,depth,private_stream}.rs`), which are the intended first callers when
/// that path is folded onto this bridge — hence the `allow(dead_code)` here and on
/// [`Nanos::from_millis`], scoped to the ms arm only.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Millis(i64);

/// A venue time in its source unit does not fit in nanoseconds as an `i64` (roughly, a
/// microsecond time past the year 2262). Returned by the [`Nanos`] constructors instead of a
/// silent wrap; the caller fails closed — it drops the update or leaves the timestamp
/// unadvanced — and never lands a wrapped value on the money path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Overflow {
    /// The source value that overflowed, in its own unit.
    pub value: i64,
}

impl fmt::Display for Overflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "venue time {} does not fit in nanoseconds as an i64",
            self.value
        )
    }
}

impl std::error::Error for Overflow {}

impl Micros {
    #[inline]
    pub const fn new(micros: i64) -> Self {
        Self(micros)
    }

    #[inline]
    pub const fn get(self) -> i64 {
        self.0
    }
}

#[allow(dead_code)] // ms arm: see the note on `Millis`.
impl Millis {
    #[inline]
    pub const fn new(millis: i64) -> Self {
        Self(millis)
    }

    #[inline]
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl Nanos {
    #[inline]
    pub const fn get(self) -> i64 {
        self.0
    }

    /// Converts microseconds to nanoseconds, or [`Overflow`] if the product does not fit `i64`.
    pub fn from_micros(micros: Micros) -> Result<Self, Overflow> {
        micros
            .0
            .checked_mul(1_000)
            .map(Self)
            .ok_or(Overflow { value: micros.0 })
    }

    /// Converts milliseconds to nanoseconds, or [`Overflow`] if the product does not fit `i64`.
    #[allow(dead_code)] // ms arm: see the note on `Millis`.
    pub fn from_millis(millis: Millis) -> Result<Self, Overflow> {
        millis
            .0
            .checked_mul(1_000_000)
            .map(Self)
            .ok_or(Overflow { value: millis.0 })
    }
}

/// A venue order-status string, classified.
///
/// A connector's status mapper returns this instead of a bare [`Status`] so that a status the
/// venue reports but this connector does not recognise is a **distinct case the order manager
/// must handle**, not a value silently folded into a terminal `Status`. A terminal status frees
/// the order id and drops the order from the manager; the strategy then re-submits and stands a
/// **duplicate** live order at the venue — exactly the fail-open `AGENTS.md` §1.1 forbids
/// (correctness-by-construction §1.6, invariant S1).
///
/// Because the arms are different shapes, a manager's removal path cannot reach a drop for
/// [`StatusVerdict::Unrecognised`]: it must destructure the verdict, and only the
/// [`StatusVerdict::Known`] arm yields a `Status` that [`Status::is_terminal`] can send to
/// removal. The compiler enforces it — a mapper written `_ => Status::Unsupported` (the Lighter
/// drop this closes) no longer type-checks against a `-> StatusVerdict` return.
///
/// This is **not** a wire `Status` variant: carrying the uncertainty to the bot as a
/// first-class `Status::Uncertain` is the deferred, wire-touching item S3. `StatusVerdict` is
/// connector-internal and never crosses the bincode boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusVerdict {
    /// A status this connector recognises, mapped onto the wire [`Status`].
    Known(Status),
    /// A status string the venue sent that this connector does not recognise. Carries the raw
    /// text for the operator log. The manager keeps the order tracked and lets reconnect
    /// reconciliation resolve it, rather than dropping a possibly-live order.
    Unrecognised(String),
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

    /// Forgets one symbol, so the next [`Self::pending`] offers it again.
    ///
    /// For a subscribe the venue refused *transiently* — rate limiting, which heals by itself:
    /// the batch has to go out again, and the only thing in the way is this tracker's own record
    /// that it was already asked for. A permanent refusal must **not** come through here, or the
    /// same batch is re-sent for the same answer until the connection dies; the caller decides,
    /// and fails closed when it cannot tell (`classify_rejection` in
    /// `bybit/public_stream.rs`, `AGENTS.md` §4.2).
    ///
    /// One symbol at a time, never a reset: the symbols beside it are live, and resubscribing
    /// those would spend rate limit while recovering from rate limiting. A symbol that was never
    /// marked is a no-op — a rejection whose `req_id` names nobody has nothing to put back.
    pub fn unmark(&mut self, symbol: &str) {
        self.subscribed.remove(symbol);
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

    use crate::utils::{
        BackoffStrategy,
        ExponentialBackoff,
        Micros,
        Millis,
        Nanos,
        Overflow,
        RefSymbolOrderId,
        SymbolOrderId,
    };

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

    /// A known microsecond venue time converts to the nanoseconds the money path expects, and a
    /// value that would overflow `i64` fails closed with [`Overflow`] rather than wrapping.
    #[test]
    fn nanos_from_micros_converts_a_known_time_and_fails_closed_on_overflow() {
        // A real Lighter `transaction_time` (µs) → nanoseconds.
        let us = 1_785_431_774_184_833i64;
        assert_eq!(
            Nanos::from_micros(Micros::new(us)).unwrap().get(),
            us * 1_000
        );

        // The largest microsecond value that still fits, and the first that does not.
        assert!(Nanos::from_micros(Micros::new(i64::MAX / 1_000)).is_ok());
        assert_eq!(
            Nanos::from_micros(Micros::new(i64::MAX / 999)),
            Err(Overflow {
                value: i64::MAX / 999
            })
        );
    }

    /// The millisecond bridge behaves the same: a known time converts, an overflow is `Err`.
    #[test]
    fn nanos_from_millis_converts_a_known_time_and_fails_closed_on_overflow() {
        let ms = 1_785_358_835_916i64;
        assert_eq!(
            Nanos::from_millis(Millis::new(ms)).unwrap().get(),
            ms * 1_000_000
        );

        assert!(Nanos::from_millis(Millis::new(i64::MAX / 1_000_000)).is_ok());
        assert_eq!(
            Nanos::from_millis(Millis::new(i64::MAX / 999_999)),
            Err(Overflow {
                value: i64::MAX / 999_999
            })
        );
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
