use std::{
    collections::{HashMap, hash_map::Entry},
    marker::PhantomData,
    rc::Rc,
    string::FromUtf8Error,
    time::{Duration, Instant},
};

use bincode::{
    Decode,
    Encode,
    config,
    error::{DecodeError, EncodeError},
};
use iceoryx2::{
    port::{LoanError, ReceiveError, SendError, publisher::Publisher, subscriber::Subscriber},
    prelude::{Node, NodeBuilder, ServiceName, ZeroCopySend, ipc},
};
use thiserror::Error;

use crate::{
    live::{
        BotError,
        Instrument,
        ipc::{
            Channel,
            TO_ALL,
            config::{ChannelConfig, MAX_PAYLOAD_SIZE},
        },
    },
    prelude::{LiveEvent, LiveRequest},
    types::BuildError,
};

#[derive(Default, Debug, ZeroCopySend)]
#[repr(C)]
pub struct CustomHeader {
    pub id: u64,
    pub len: usize,
}

#[derive(Error, Debug)]
pub enum ChannelError {
    #[error("BuildError - {0}")]
    BuildError(String),
    #[error("{0:?}")]
    ReceiveError(#[from] ReceiveError),
    #[error("{0:?}")]
    LoanError(#[from] LoanError),
    #[error("{0:?}")]
    SendError(#[from] SendError),
    #[error("{0:?}")]
    Decode(#[from] DecodeError),
    #[error("{0:?}")]
    Encode(#[from] EncodeError),
    #[error("{0:?}")]
    FromUtf8(#[from] FromUtf8Error),
}

pub struct IceoryxBuilder {
    name: String,
    bot: bool,
}

impl IceoryxBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            bot: true,
        }
    }

    pub fn bot(self, bot: bool) -> Self {
        Self { bot, ..self }
    }

    pub fn receiver<T>(self) -> Result<IceoryxReceiver<T>, ChannelError> {
        let config = ChannelConfig::load_config();
        let node = NodeBuilder::new()
            .create::<ipc::Service>()
            .map_err(|error| ChannelError::BuildError(error.to_string()))?;

        let sub_factory = if self.bot {
            let service_name = ServiceName::new(&format!("{}/ToBot", self.name))
                .map_err(|error| ChannelError::BuildError(error.to_string()))?;
            node.service_builder(&service_name)
                .publish_subscribe::<[u8]>()
                .subscriber_max_buffer_size(config.buffer_size)
                .max_publishers(1)
                .max_subscribers(config.max_bots)
                .user_header::<CustomHeader>()
                .open_or_create()
                .map_err(|error| ChannelError::BuildError(error.to_string()))?
        } else {
            let service_name = ServiceName::new(&format!("{}/FromBot", self.name))
                .map_err(|error| ChannelError::BuildError(error.to_string()))?;
            node.service_builder(&service_name)
                .publish_subscribe::<[u8]>()
                .subscriber_max_buffer_size(config.buffer_size)
                .max_publishers(config.max_bots)
                .max_subscribers(1)
                .user_header::<CustomHeader>()
                .open_or_create()
                .map_err(|error| ChannelError::BuildError(error.to_string()))?
        };

        let subscriber = sub_factory
            .subscriber_builder()
            .create()
            .map_err(|error| ChannelError::BuildError(error.to_string()))?;

        Ok(IceoryxReceiver {
            subscriber,
            _t_marker: Default::default(),
        })
    }

    pub fn sender<T>(self) -> Result<IceoryxSender<T>, ChannelError> {
        let config = ChannelConfig::load_config();
        let node = NodeBuilder::new()
            .create::<ipc::Service>()
            .map_err(|error| ChannelError::BuildError(error.to_string()))?;

        let pub_factory = if self.bot {
            let service_name = ServiceName::new(&format!("{}/FromBot", self.name))
                .map_err(|error| ChannelError::BuildError(error.to_string()))?;
            node.service_builder(&service_name)
                .publish_subscribe::<[u8]>()
                .subscriber_max_buffer_size(config.buffer_size)
                .max_publishers(config.max_bots)
                .max_subscribers(1)
                .user_header::<CustomHeader>()
                .open_or_create()
                .map_err(|error| ChannelError::BuildError(error.to_string()))?
        } else {
            let service_name = ServiceName::new(&format!("{}/ToBot", self.name))
                .map_err(|error| ChannelError::BuildError(error.to_string()))?;
            node.service_builder(&service_name)
                .publish_subscribe::<[u8]>()
                .subscriber_max_buffer_size(config.buffer_size)
                .max_publishers(1)
                .max_subscribers(config.max_bots)
                .user_header::<CustomHeader>()
                .open_or_create()
                .map_err(|error| ChannelError::BuildError(error.to_string()))?
        };

        let publisher = pub_factory
            .publisher_builder()
            .initial_max_slice_len(MAX_PAYLOAD_SIZE)
            .create()
            .map_err(|error| ChannelError::BuildError(error.to_string()))?;

        Ok(IceoryxSender {
            publisher,
            _t_marker: Default::default(),
        })
    }
}

pub struct IceoryxSender<T> {
    publisher: Publisher<ipc::Service, [u8], CustomHeader>,
    _t_marker: PhantomData<T>,
}

impl<T> IceoryxSender<T>
where
    T: Encode,
{
    pub fn send(&self, id: u64, data: &T) -> Result<(), ChannelError> {
        let sample = self.publisher.loan_slice_uninit(MAX_PAYLOAD_SIZE)?;
        let mut sample = unsafe { sample.assume_init() };

        let payload = sample.payload_mut();
        let length = bincode::encode_into_slice(data, payload, config::standard())?;

        sample.user_header_mut().id = id;
        sample.user_header_mut().len = length;

        sample.send()?;

        Ok(())
    }
}

pub struct IceoryxReceiver<T> {
    subscriber: Subscriber<ipc::Service, [u8], CustomHeader>,
    _t_marker: PhantomData<T>,
}

impl<T> IceoryxReceiver<T>
where
    T: Decode<()>,
{
    pub fn receive(&self) -> Result<Option<(u64, T)>, ChannelError> {
        match self.subscriber.receive()? {
            None => Ok(None),
            Some(sample) => {
                let id = sample.user_header().id;
                let len = sample.user_header().len;

                let bytes = &sample.payload()[0..len];
                let (decoded, _len): (T, usize) =
                    bincode::decode_from_slice(bytes, config::standard())?;
                Ok(Some((id, decoded)))
            }
        }
    }
}

/// Which instruments one connector's channel carries, and which of them speaks for it.
///
/// Split out of [`IceoryxChannel`] so that the origin bookkeeping can be tested without
/// standing up iceoryx services, which [`IceoryxChannel::new`] does unconditionally.
#[derive(Default)]
struct SymbolRegistry {
    symbol_to_inst_no: HashMap<String, usize>,
    /// The lowest `inst_no` registered here, or `None` before the first registration.
    ///
    /// This is the *representative* instrument of the connector: a valid index into the bot's
    /// `instruments` that is known to belong to this connector. It fills the `usize` slot of
    /// [`Channel::recv_timeout`] for the events that carry no symbol (`BatchStart`,
    /// `BatchEnd`, `Error`), which used to be reported as a hardcoded `0` — valid, but
    /// belonging to whichever connector happened to be registered first.
    origin_inst_no: Option<usize>,
}

impl SymbolRegistry {
    fn register(&mut self, inst_no: usize, symbol: &str) -> bool {
        if self.symbol_to_inst_no.contains_key(symbol) {
            return false;
        }
        self.origin_inst_no = Some(match self.origin_inst_no {
            Some(current) => current.min(inst_no),
            None => inst_no,
        });
        self.symbol_to_inst_no.insert(symbol.to_string(), inst_no);
        true
    }

    /// A channel with no registered instrument has no representative to offer. It cannot arise
    /// from [`Channel::build`], which registers before the channel is ever polled, and the bot
    /// bounds-checks whatever arrives anyway (`LiveBot::process_event`).
    fn origin_inst_no(&self) -> usize {
        self.origin_inst_no.unwrap_or(0)
    }
}

pub struct IceoryxChannel<S, R> {
    publisher: IceoryxSender<S>,
    subscriber: IceoryxReceiver<R>,
    registry: SymbolRegistry,
    /// Name of the connector on the other end. One channel is exactly one connector by
    /// construction ([`Channel::build`] keys them by `connector_name`), so this is what lets a
    /// receive or send failure name who it happened to.
    name: String,
}

impl<S, R> IceoryxChannel<S, R>
where
    S: Encode,
    R: Decode<()>,
{
    pub fn new(name: &str) -> Result<Self, ChannelError> {
        let publisher = IceoryxBuilder::new(name).sender()?;
        let subscriber = IceoryxBuilder::new(name).receiver()?;

        Ok(Self {
            publisher,
            subscriber,
            registry: Default::default(),
            name: name.to_string(),
        })
    }

    pub fn register(&mut self, inst_no: usize, symbol: &str) -> bool {
        self.registry.register(inst_no, symbol)
    }

    pub fn send(&self, id: u64, data: &S) -> Result<(), ChannelError> {
        self.publisher.send(id, data)
    }

    pub fn receive(&self) -> Result<Option<(u64, R)>, ChannelError> {
        self.subscriber.receive()
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// One pollable connector channel, as [`poll_round_robin`] sees it.
///
/// The seam exists because [`IceoryxChannel::new`] stands up real iceoryx services, so the
/// round-robin — which is where the origin of a markerless event is decided — could not
/// otherwise be tested at all. Same treatment `AGENTS.md` §4.2 gave bybit's `serve(write, read)`.
trait PolledSource {
    fn name(&self) -> &str;
    /// A valid `inst_no` belonging to this source's connector. See
    /// [`IceoryxChannel::origin_inst_no`].
    fn origin_inst_no(&self) -> usize;
    fn symbol_to_inst_no(&self) -> &HashMap<String, usize>;
    fn receive(&self) -> Result<Option<(u64, LiveEvent)>, ChannelError>;
}

impl PolledSource for IceoryxChannel<LiveRequest, LiveEvent> {
    fn name(&self) -> &str {
        &self.name
    }

    fn origin_inst_no(&self) -> usize {
        self.registry.origin_inst_no()
    }

    fn symbol_to_inst_no(&self) -> &HashMap<String, usize> {
        &self.registry.symbol_to_inst_no
    }

    fn receive(&self) -> Result<Option<(u64, LiveEvent)>, ChannelError> {
        IceoryxChannel::receive(self)
    }
}

impl<T: PolledSource> PolledSource for Rc<T> {
    fn name(&self) -> &str {
        (**self).name()
    }

    fn origin_inst_no(&self) -> usize {
        (**self).origin_inst_no()
    }

    fn symbol_to_inst_no(&self) -> &HashMap<String, usize> {
        (**self).symbol_to_inst_no()
    }

    fn receive(&self) -> Result<Option<(u64, LiveEvent)>, ChannelError> {
        (**self).receive()
    }
}

/// One connector channel a request can be handed to, as [`send_to`] sees it.
///
/// The mirror of [`PolledSource`], and there for the same reason: [`IceoryxChannel::new`]
/// stands up real iceoryx services, so the one place in the process where a *send-side*
/// [`BotError::Channel`] is built from a real channel's name could not otherwise be exercised
/// at all. Every bot-side test of that error fabricates the name in its own fake, which pins
/// what the bot does with a name and nothing about where the name comes from.
trait SendTarget {
    fn name(&self) -> &str;
    fn send(&self, id: u64, request: &LiveRequest) -> Result<(), ChannelError>;
}

impl SendTarget for IceoryxChannel<LiveRequest, LiveEvent> {
    fn name(&self) -> &str {
        &self.name
    }

    fn send(&self, id: u64, request: &LiveRequest) -> Result<(), ChannelError> {
        IceoryxChannel::send(self, id, request)
    }
}

impl<T: SendTarget> SendTarget for Rc<T> {
    fn name(&self) -> &str {
        (**self).name()
    }

    fn send(&self, id: u64, request: &LiveRequest) -> Result<(), ChannelError> {
        SendTarget::send(&**self, id, request)
    }
}

/// Hands a request to the channel that carries `inst_no`, naming that channel if it will not
/// take it.
///
/// The channel that could not be reached is the one that names itself. `LiveBot::heartbeat`
/// sends to every connector in turn, so an anonymous failure here is indistinguishable from a
/// failure on any other connector — and a consumer that cannot tell which venue went quiet
/// has to treat all of them as fatal (`AGENTS.md` §2.2, road 3).
fn send_to<T: SendTarget>(
    targets: &[T],
    id: u64,
    inst_no: usize,
    request: &LiveRequest,
) -> Result<(), BotError> {
    let target = targets.get(inst_no).ok_or(BotError::InstrumentNotFound)?;
    target.send(id, request).map_err(|err| BotError::Channel {
        connector: target.name().to_string(),
        message: err.to_string(),
    })
}

/// Which instrument an event belongs to, from the point of view of the channel that delivered
/// it.
///
/// Symbol-bearing events are looked up; the three markerless variants take the origin of the
/// channel they arrived on. `None` means the event names a symbol this channel never
/// registered, and the caller drops it — as it always has.
fn attribute(
    ev: &LiveEvent,
    origin_inst_no: usize,
    symbol_to_inst_no: &HashMap<String, usize>,
) -> Option<usize> {
    match ev {
        LiveEvent::BatchStart | LiveEvent::BatchEnd | LiveEvent::Error(_) => Some(origin_inst_no),
        LiveEvent::Feed { symbol, .. }
        | LiveEvent::Order { symbol, .. }
        | LiveEvent::Position { symbol, .. }
        | LiveEvent::SnapshotComplete { symbol, .. } => symbol_to_inst_no.get(symbol).copied(),
    }
}

/// Polls exactly one source and advances the cursor, the way the receive loop always has.
///
/// `Ok(None)` means "nothing usable this turn" — an empty channel, a message addressed to
/// another bot, or a symbol this channel does not know — and the caller comes back around.
fn poll_round_robin<C: PolledSource>(
    sources: &[C],
    ch_i: &mut usize,
    id: u64,
) -> Result<Option<(usize, LiveEvent)>, BotError> {
    let Some(source) = sources.get(*ch_i) else {
        *ch_i = 0;
        return Ok(None);
    };

    *ch_i += 1;
    if *ch_i >= sources.len() {
        *ch_i = 0;
    }

    let received = source.receive().map_err(|err| BotError::Channel {
        connector: source.name().to_string(),
        message: err.to_string(),
    })?;

    let Some((dst_id, ev)) = received else {
        return Ok(None);
    };
    if dst_id != TO_ALL && dst_id != id {
        return Ok(None);
    }

    Ok(
        attribute(&ev, source.origin_inst_no(), source.symbol_to_inst_no())
            .map(|inst_no| (inst_no, ev)),
    )
}

pub struct IceoryxUnifiedChannel {
    channel: Vec<Rc<IceoryxChannel<LiveRequest, LiveEvent>>>,
    unique_channel: Vec<Rc<IceoryxChannel<LiveRequest, LiveEvent>>>,
    ch_i: usize,
    node: Node<ipc::Service>,
}

impl IceoryxUnifiedChannel {
    pub fn new(
        channel: Vec<Rc<IceoryxChannel<LiveRequest, LiveEvent>>>,
    ) -> Result<Self, ChannelError> {
        assert!(!channel.is_empty());

        let unique_channel = {
            let mut unique_vec = Vec::new();

            for item in &channel {
                if !unique_vec.iter().any(|x| Rc::ptr_eq(x, item)) {
                    unique_vec.push(item.clone());
                }
            }

            unique_vec
        };

        let node = NodeBuilder::new()
            .create::<ipc::Service>()
            .map_err(|error| ChannelError::BuildError(error.to_string()))?;
        Ok(Self {
            channel,
            unique_channel,
            ch_i: 0,
            node,
        })
    }
}

impl Channel for IceoryxUnifiedChannel {
    fn build<MD>(instruments: &[Instrument<MD>]) -> Result<Self, BuildError>
    where
        Self: Sized,
    {
        let mut channel: HashMap<String, IceoryxChannel<LiveRequest, LiveEvent>> = HashMap::new();
        for (inst_no, instrument) in instruments.iter().enumerate() {
            match channel.entry(instrument.connector_name.clone()) {
                Entry::Occupied(mut entry) => {
                    let ch = entry.get_mut();
                    if !ch.register(inst_no, &instrument.symbol) {
                        return Err(BuildError::Duplicate(
                            instrument.connector_name.clone(),
                            instrument.symbol.clone(),
                        ));
                    }
                }
                Entry::Vacant(entry) => {
                    let mut ch = IceoryxChannel::new(&instrument.connector_name)
                        .map_err(|error| BuildError::Error(anyhow::Error::from(error)))?;
                    if !ch.register(inst_no, &instrument.symbol) {
                        return Err(BuildError::Duplicate(
                            instrument.connector_name.clone(),
                            instrument.symbol.clone(),
                        ));
                    }
                    entry.insert(ch);
                }
            }
        }
        let channel: HashMap<_, _> = channel.into_iter().map(|(k, v)| (k, Rc::new(v))).collect();
        let channel_list: Vec<_> = instruments
            .iter()
            .map(|i| channel.get(&i.connector_name).unwrap().clone())
            .collect();

        IceoryxUnifiedChannel::new(channel_list)
            .map_err(|error| BuildError::Error(anyhow::Error::from(error)))
    }

    fn recv_timeout(&mut self, id: u64, timeout: Duration) -> Result<(usize, LiveEvent), BotError> {
        let instant = Instant::now();
        loop {
            let elapsed = instant.elapsed();
            if elapsed > timeout {
                return Err(BotError::Timeout);
            }

            // todo: this needs to retrieve Iox2Event without waiting.
            match self.node.wait(Duration::from_nanos(1)) {
                Ok(()) => {
                    if let Some(received) =
                        poll_round_robin(&self.unique_channel, &mut self.ch_i, id)?
                    {
                        return Ok(received);
                    }
                }
                Err(_error) => {
                    return Err(BotError::Interrupted);
                }
            }
        }
    }

    fn send(&mut self, id: u64, inst_no: usize, request: LiveRequest) -> Result<(), BotError> {
        send_to(&self.channel, id, inst_no, &request)
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::VecDeque};

    use super::*;
    use crate::types::{ErrorKind, Event, LiveError};

    /// One connector's channel with the iceoryx taken out: a queue of whatever the subscriber
    /// would have handed back, in order.
    struct FakeSource {
        name: String,
        origin_inst_no: usize,
        symbol_to_inst_no: HashMap<String, usize>,
        queue: RefCell<VecDeque<Result<Option<(u64, LiveEvent)>, ChannelError>>>,
    }

    impl FakeSource {
        fn new(
            name: &str,
            origin_inst_no: usize,
            symbols: &[(&str, usize)],
            queue: Vec<Result<Option<(u64, LiveEvent)>, ChannelError>>,
        ) -> Self {
            Self {
                name: name.to_string(),
                origin_inst_no,
                symbol_to_inst_no: symbols
                    .iter()
                    .map(|(s, i)| ((*s).to_string(), *i))
                    .collect(),
                queue: RefCell::new(queue.into()),
            }
        }
    }

    impl PolledSource for FakeSource {
        fn name(&self) -> &str {
            &self.name
        }

        fn origin_inst_no(&self) -> usize {
            self.origin_inst_no
        }

        fn symbol_to_inst_no(&self) -> &HashMap<String, usize> {
            &self.symbol_to_inst_no
        }

        fn receive(&self) -> Result<Option<(u64, LiveEvent)>, ChannelError> {
            self.queue.borrow_mut().pop_front().unwrap_or(Ok(None))
        }
    }

    fn feed(symbol: &str) -> LiveEvent {
        LiveEvent::Feed {
            symbol: symbol.to_string(),
            event: Event {
                ev: 0,
                exch_ts: 0,
                local_ts: 0,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        }
    }

    /// **The three markerless variants are the whole point.** They carry no symbol, so the only
    /// thing that can say where they came from is the channel that delivered them; reporting a
    /// constant `0` instead names whichever connector was registered first.
    #[test]
    fn attribute_marks_a_markerless_event_with_the_channels_origin() {
        // Deliberately different numbers: the origin of the channel (3) and the instrument the
        // symbol maps to (7). A mutation that confuses the two shows up as the wrong one.
        let map: HashMap<String, usize> = [("BTCUSDT".to_string(), 7usize)].into_iter().collect();

        assert_eq!(attribute(&LiveEvent::BatchStart, 3, &map), Some(3));
        assert_eq!(attribute(&LiveEvent::BatchEnd, 3, &map), Some(3));
        assert_eq!(
            attribute(
                &LiveEvent::Error(LiveError::new(ErrorKind::ConnectionInterrupted)),
                3,
                &map
            ),
            Some(3)
        );

        // Symbol-bearing events are unchanged: they are looked up, never stamped.
        assert_eq!(attribute(&feed("BTCUSDT"), 3, &map), Some(7));
        assert_eq!(attribute(&feed("ETHUSDT"), 3, &map), None);
    }

    /// **A green `attribute` proves nothing on its own.** It shows that the origin it is handed
    /// reaches the result, and says nothing about that origin belonging to the channel that was
    /// just polled — exactly the shape of vacuous attribution `AGENTS.md` §4.2 already paid for
    /// once on bybit. This pins the binding.
    #[test]
    fn each_polled_channel_stamps_its_own_origin() {
        let sources = vec![
            FakeSource::new(
                "hl",
                0,
                &[("BTC", 0)],
                vec![Ok(Some((TO_ALL, LiveEvent::BatchStart)))],
            ),
            FakeSource::new(
                "bnc",
                7,
                &[("BTCUSDT", 7)],
                vec![Ok(Some((TO_ALL, LiveEvent::BatchStart)))],
            ),
        ];
        let mut ch_i = 0;

        let first = poll_round_robin(&sources, &mut ch_i, 42).unwrap();
        assert_eq!(first.map(|(inst_no, _)| inst_no), Some(0));
        assert_eq!(ch_i, 1, "the cursor must move on to the next channel");

        let second = poll_round_robin(&sources, &mut ch_i, 42).unwrap();
        assert_eq!(
            second.map(|(inst_no, _)| inst_no),
            Some(7),
            "the second channel's marker must carry the second channel's origin"
        );
        assert_eq!(ch_i, 0, "and then wrap around");
    }

    /// A failed receive is the first of the two anonymous roads out of `AGENTS.md` §2.2: it
    /// becomes an `Err` that ends the caller's `elapse`, and the caller cannot tell whether the
    /// connector it trades on or a market-data-only one just failed.
    #[test]
    fn a_failed_receive_names_the_channel_it_polled() {
        let sources = vec![
            FakeSource::new("hl", 0, &[], vec![Ok(None)]),
            FakeSource::new(
                "bnc",
                7,
                &[],
                vec![Err(ChannelError::BuildError("decode blew up".to_string()))],
            ),
        ];
        let mut ch_i = 0;

        assert!(matches!(
            poll_round_robin(&sources, &mut ch_i, 42),
            Ok(None)
        ));
        match poll_round_robin(&sources, &mut ch_i, 42) {
            Err(BotError::Channel { connector, message }) => {
                assert_eq!(connector, "bnc");
                assert!(
                    message.contains("decode blew up"),
                    "the venue's own words must survive: {message}"
                );
            }
            other => panic!("expected a named channel fault, got {other:?}"),
        }
    }

    /// Unchanged behaviour, pinned because the code moved: a message addressed to another bot
    /// is skipped, and a broadcast is not.
    #[test]
    fn a_message_addressed_to_another_bot_is_skipped() {
        let sources = vec![FakeSource::new(
            "hl",
            0,
            &[("BTC", 0)],
            vec![
                Ok(Some((99, feed("BTC")))),
                Ok(Some((42, feed("BTC")))),
                Ok(Some((TO_ALL, feed("BTC")))),
            ],
        )];
        let mut ch_i = 0;

        assert!(matches!(
            poll_round_robin(&sources, &mut ch_i, 42),
            Ok(None)
        ));
        assert!(matches!(
            poll_round_robin(&sources, &mut ch_i, 42),
            Ok(Some((0, _)))
        ));
        assert!(matches!(
            poll_round_robin(&sources, &mut ch_i, 42),
            Ok(Some((0, _)))
        ));
    }

    /// The representative instrument is the lowest one registered on that channel, whatever
    /// order the registrations arrive in — and a duplicate symbol is still refused.
    #[test]
    fn a_channel_takes_its_origin_from_its_lowest_registered_instrument() {
        let mut registry = SymbolRegistry::default();
        assert_eq!(
            registry.origin_inst_no(),
            0,
            "an unregistered channel falls back to a valid index"
        );

        assert!(registry.register(5, "SOL"));
        assert_eq!(registry.origin_inst_no(), 5);
        assert!(registry.register(3, "BTC"));
        assert_eq!(registry.origin_inst_no(), 3);
        assert!(registry.register(9, "ETH"));
        assert_eq!(
            registry.origin_inst_no(),
            3,
            "later, higher ones do not win"
        );

        assert!(
            !registry.register(1, "BTC"),
            "a duplicate symbol is refused"
        );
        assert_eq!(
            registry.origin_inst_no(),
            3,
            "and a refused registration changes nothing"
        );
    }

    /// One connector's outgoing half with the iceoryx taken out: either it takes the request
    /// or it refuses it with the venue's own words.
    struct FakeSink {
        name: String,
        refusal: Option<String>,
        taken: RefCell<Vec<(u64, LiveRequest)>>,
    }

    impl FakeSink {
        fn healthy(name: &str) -> Self {
            Self {
                name: name.to_string(),
                refusal: None,
                taken: RefCell::new(Vec::new()),
            }
        }

        fn broken(name: &str, refusal: &str) -> Self {
            Self {
                name: name.to_string(),
                refusal: Some(refusal.to_string()),
                taken: RefCell::new(Vec::new()),
            }
        }
    }

    impl SendTarget for FakeSink {
        fn name(&self) -> &str {
            &self.name
        }

        fn send(&self, id: u64, request: &LiveRequest) -> Result<(), ChannelError> {
            match &self.refusal {
                Some(refusal) => Err(ChannelError::BuildError(refusal.clone())),
                None => {
                    self.taken.borrow_mut().push((id, request.clone()));
                    Ok(())
                }
            }
        }
    }

    /// **Road 3, at the one place in the process where its name is actually chosen.** Every
    /// bot-side pin of a send failure builds the name in its own fake, so all of them stay
    /// green if this routing hands back the wrong channel's name — or a constant. The
    /// heartbeat goes to every connector in turn, and a consumer that cannot tell which one
    /// stopped answering has to treat every venue as fatal.
    #[test]
    fn a_failed_send_names_the_channel_that_carries_that_instrument() {
        // Two instruments, two connectors, deliberately failing on the *second*: a routing
        // that ignores `inst_no` and takes the first channel reports "hl".
        let targets = vec![
            FakeSink::healthy("hl"),
            FakeSink::broken("bnc", "PublisherDisconnected"),
        ];

        match send_to(&targets, 42, 1, &LiveRequest::Heartbeat) {
            Err(BotError::Channel { connector, message }) => {
                assert_eq!(connector, "bnc");
                assert!(
                    message.contains("PublisherDisconnected"),
                    "the transport's own words must survive: {message}"
                );
            }
            other => panic!("expected a named channel fault, got {other:?}"),
        }

        assert!(
            send_to(&targets, 42, 0, &LiveRequest::Heartbeat).is_ok(),
            "and the connector that can be reached still hears it"
        );
        assert_eq!(targets[0].taken.borrow().len(), 1);
        assert!(targets[1].taken.borrow().is_empty());
    }

    /// An index no channel carries is refused rather than routed to whichever channel happens
    /// to sit at that position — the send-side twin of the bounds check in `LiveBot`.
    #[test]
    fn a_send_to_an_instrument_no_channel_carries_is_refused() {
        let targets = vec![FakeSink::healthy("hl")];

        assert!(matches!(
            send_to(&targets, 42, 9, &LiveRequest::Heartbeat),
            Err(BotError::InstrumentNotFound)
        ));
        assert!(targets[0].taken.borrow().is_empty());
    }
}
