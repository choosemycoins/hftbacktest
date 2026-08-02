use std::{
    collections::HashMap,
    io::Error as IoError,
    ops::{Deref, DerefMut},
};

pub use data::DataSource;
use data::Reader;
use models::FeeModel;
use thiserror::Error;

pub use crate::backtest::{
    models::L3QueueModel,
    proc::{L3Local, L3NoPartialFillExchange},
};
use crate::{
    backtest::{
        assettype::AssetType,
        data::{Data, FeedLatencyAdjustment, NpyDTyped},
        evs::{EventIntentKind, EventSet},
        models::{LatencyModel, QueueModel},
        order::order_bus,
        proc::{Local, LocalProcessor, NoPartialFillExchange, PartialFillExchange, Processor},
        state::State,
    },
    depth::{L2MarketDepth, L3MarketDepth, MarketDepth},
    prelude::{
        Bot,
        OrdType,
        Order,
        OrderId,
        OrderRequest,
        Side,
        StateValues,
        TimeInForce,
        UNTIL_END_OF_DATA,
        WaitOrderResponse,
    },
    types::{BuildError, ElapseResult, Event},
};

/// Provides asset types.
pub mod assettype;

pub mod models;

/// OrderBus implementation
pub mod order;

/// Local and exchange models
pub mod proc;

/// Trading state.
pub mod state;

/// Recorder for a bot's trading statistics.
pub mod recorder;

pub mod data;
mod evs;

/// Property tests for the fill accounting identity — see the module's own documentation.
#[cfg(test)]
mod fill_accounting_property;

/// Errors that can occur during backtesting.
#[derive(Error, Debug)]
pub enum BacktestError {
    #[error("Order related to a given order id already exists")]
    OrderIdExist,
    #[error("Order request is in process")]
    OrderRequestInProcess,
    #[error("Order not found")]
    OrderNotFound,
    #[error("order request is invalid")]
    InvalidOrderRequest,
    #[error("order status is invalid to proceed the request")]
    InvalidOrderStatus,
    #[error("end of data")]
    EndOfData,
    #[error("data error: {0:?}")]
    DataError(#[from] IoError),
}

/// Backtesting Asset
pub struct Asset<L: ?Sized, E: ?Sized, D: NpyDTyped + Clone /* todo: ugly bounds */> {
    pub local: Box<L>,
    pub exch: Box<E>,
    pub reader: Reader<D>,
}

impl<L, E, D: NpyDTyped + Clone> Asset<L, E, D> {
    /// Constructs an instance of `Asset`. Use this method if a custom local processor or an
    /// exchange processor is needed.
    pub fn new(local: L, exch: E, reader: Reader<D>) -> Self {
        Self {
            local: Box::new(local),
            exch: Box::new(exch),
            reader,
        }
    }

    /// Returns an `L2AssetBuilder`.
    pub fn l2_builder<LM, AT, QM, MD, FM>() -> L2AssetBuilder<LM, AT, QM, MD, FM>
    where
        AT: AssetType + Clone + 'static,
        MD: MarketDepth + L2MarketDepth + 'static,
        QM: QueueModel<MD> + 'static,
        LM: LatencyModel + Clone + 'static,
        FM: FeeModel + Clone + 'static,
    {
        L2AssetBuilder::new()
    }

    /// Returns an `L3AssetBuilder`.
    pub fn l3_builder<LM, AT, QM, MD, FM>() -> L3AssetBuilder<LM, AT, QM, MD, FM>
    where
        AT: AssetType + Clone + 'static,
        MD: MarketDepth + L3MarketDepth + 'static,
        QM: L3QueueModel<MD> + 'static,
        LM: LatencyModel + Clone + 'static,
        FM: FeeModel + Clone + 'static,
        BacktestError: From<<MD as L3MarketDepth>::Error>,
    {
        L3AssetBuilder::new()
    }
}

/// Exchange model kind.
pub enum ExchangeKind {
    /// Uses [NoPartialFillExchange](`NoPartialFillExchange`).
    NoPartialFillExchange,
    /// Uses [PartialFillExchange](`PartialFillExchange`).
    PartialFillExchange,
}

/// A level-2 asset builder.
pub struct L2AssetBuilder<LM, AT, QM, MD, FM> {
    latency_model: Option<LM>,
    asset_type: Option<AT>,
    data: Vec<DataSource<Event>>,
    parallel_load: bool,
    latency_offset: i64,
    fee_model: Option<FM>,
    exch_kind: ExchangeKind,
    last_trades_cap: usize,
    queue_model: Option<QM>,
    depth_builder: Option<Box<dyn Fn() -> MD>>,
}

impl<LM, AT, QM, MD, FM> L2AssetBuilder<LM, AT, QM, MD, FM>
where
    AT: AssetType + Clone + 'static,
    MD: MarketDepth + L2MarketDepth + 'static,
    QM: QueueModel<MD> + 'static,
    LM: LatencyModel + Clone + 'static,
    FM: FeeModel + Clone + 'static,
{
    /// Constructs an `L2AssetBuilder`.
    pub fn new() -> Self {
        Self {
            latency_model: None,
            asset_type: None,
            data: vec![],
            parallel_load: false,
            latency_offset: 0,
            fee_model: None,
            exch_kind: ExchangeKind::NoPartialFillExchange,
            last_trades_cap: 0,
            queue_model: None,
            depth_builder: None,
        }
    }

    /// Sets the feed data.
    pub fn data(self, data: Vec<DataSource<Event>>) -> Self {
        Self { data, ..self }
    }

    /// Sets whether to load the next data in parallel with backtesting. This can speed up the
    /// backtest by reducing data loading time, but it also increases memory usage.
    /// The default value is `true`.
    pub fn parallel_load(self, parallel_load: bool) -> Self {
        Self {
            parallel_load,
            ..self
        }
    }

    /// Sets the latency offset to adjust the feed latency by the specified amount. This is
    /// particularly useful in cross-exchange backtesting, where the feed data is collected from a
    /// different site than the one where the strategy is intended to run.
    pub fn latency_offset(self, latency_offset: i64) -> Self {
        Self {
            latency_offset,
            ..self
        }
    }

    /// Sets a latency model.
    pub fn latency_model(self, latency_model: LM) -> Self {
        Self {
            latency_model: Some(latency_model),
            ..self
        }
    }

    /// Sets an asset type.
    pub fn asset_type(self, asset_type: AT) -> Self {
        Self {
            asset_type: Some(asset_type),
            ..self
        }
    }

    /// Sets a fee model.
    pub fn fee_model(self, fee_model: FM) -> Self {
        Self {
            fee_model: Some(fee_model),
            ..self
        }
    }

    /// Sets an exchange model. The default value is [`NoPartialFillExchange`].
    pub fn exchange(self, exch_kind: ExchangeKind) -> Self {
        Self { exch_kind, ..self }
    }

    /// Sets the initial capacity of the vector storing the last market trades.
    /// The default value is `0`, indicating that no last trades are stored.
    pub fn last_trades_capacity(self, capacity: usize) -> Self {
        Self {
            last_trades_cap: capacity,
            ..self
        }
    }

    /// Sets a queue model.
    pub fn queue_model(self, queue_model: QM) -> Self {
        Self {
            queue_model: Some(queue_model),
            ..self
        }
    }

    /// Sets a market depth builder.
    pub fn depth<Builder>(self, builder: Builder) -> Self
    where
        Builder: Fn() -> MD + 'static,
    {
        Self {
            depth_builder: Some(Box::new(builder)),
            ..self
        }
    }

    /// Builds an `Asset`.
    pub fn build(self) -> Result<Asset<dyn LocalProcessor<MD>, dyn Processor, Event>, BuildError> {
        let reader = if self.latency_offset == 0 {
            Reader::builder()
                .parallel_load(self.parallel_load)
                .data(self.data)
                .build()
                .map_err(|err| BuildError::Error(err.into()))?
        } else {
            Reader::builder()
                .parallel_load(self.parallel_load)
                .data(self.data)
                .preprocessor(FeedLatencyAdjustment::new(self.latency_offset))
                .build()
                .map_err(|err| BuildError::Error(err.into()))?
        };

        let create_depth = self
            .depth_builder
            .as_ref()
            .ok_or(BuildError::BuilderIncomplete("depth"))?;
        let order_latency = self
            .latency_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("order_latency"))?;
        let asset_type = self
            .asset_type
            .clone()
            .ok_or(BuildError::BuilderIncomplete("asset_type"))?;
        let fee_model = self
            .fee_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("fee_model"))?;

        let (order_e2l, order_l2e) = order_bus(order_latency);

        let local = Local::new(
            create_depth(),
            State::new(asset_type, fee_model),
            self.last_trades_cap,
            order_l2e,
        );

        let queue_model = self
            .queue_model
            .ok_or(BuildError::BuilderIncomplete("queue_model"))?;
        let asset_type = self
            .asset_type
            .clone()
            .ok_or(BuildError::BuilderIncomplete("asset_type"))?;
        let fee_model = self
            .fee_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("fee_model"))?;

        match self.exch_kind {
            ExchangeKind::NoPartialFillExchange => {
                let exch = NoPartialFillExchange::new(
                    create_depth(),
                    State::new(asset_type, fee_model),
                    queue_model,
                    order_e2l,
                );

                Ok(Asset {
                    local: Box::new(local),
                    exch: Box::new(exch),
                    reader,
                })
            }
            ExchangeKind::PartialFillExchange => {
                let exch = PartialFillExchange::new(
                    create_depth(),
                    State::new(asset_type, fee_model),
                    queue_model,
                    order_e2l,
                );

                Ok(Asset {
                    local: Box::new(local),
                    exch: Box::new(exch),
                    reader,
                })
            }
        }
    }
}

impl<LM, AT, QM, MD, FM> Default for L2AssetBuilder<LM, AT, QM, MD, FM>
where
    AT: AssetType + Clone + 'static,
    MD: MarketDepth + L2MarketDepth + 'static,
    QM: QueueModel<MD> + 'static,
    LM: LatencyModel + Clone + 'static,
    FM: FeeModel + Clone + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

/// A level-3 asset builder.
pub struct L3AssetBuilder<LM, AT, QM, MD, FM> {
    latency_model: Option<LM>,
    asset_type: Option<AT>,
    data: Vec<DataSource<Event>>,
    parallel_load: bool,
    latency_offset: i64,
    fee_model: Option<FM>,
    exch_kind: ExchangeKind,
    last_trades_cap: usize,
    queue_model: Option<QM>,
    depth_builder: Option<Box<dyn Fn() -> MD>>,
}

impl<LM, AT, QM, MD, FM> L3AssetBuilder<LM, AT, QM, MD, FM>
where
    AT: AssetType + Clone + 'static,
    MD: MarketDepth + L3MarketDepth + 'static,
    QM: L3QueueModel<MD> + 'static,
    LM: LatencyModel + Clone + 'static,
    FM: FeeModel + Clone + 'static,
    BacktestError: From<<MD as L3MarketDepth>::Error>,
{
    /// Constructs an `L3AssetBuilder`.
    pub fn new() -> Self {
        Self {
            latency_model: None,
            asset_type: None,
            data: vec![],
            parallel_load: false,
            latency_offset: 0,
            fee_model: None,
            exch_kind: ExchangeKind::NoPartialFillExchange,
            last_trades_cap: 0,
            queue_model: None,
            depth_builder: None,
        }
    }

    /// Sets the feed data.
    pub fn data(self, data: Vec<DataSource<Event>>) -> Self {
        Self { data, ..self }
    }

    /// Sets whether to load the next data in parallel with backtesting. This can speed up the
    /// backtest by reducing data loading time, but it also increases memory usage.
    /// The default value is `true`.
    pub fn parallel_load(self, parallel_load: bool) -> Self {
        Self {
            parallel_load,
            ..self
        }
    }

    /// Sets the latency offset to adjust the feed latency by the specified amount. This is
    /// particularly useful in cross-exchange backtesting, where the feed data is collected from a
    /// different site than the one where the strategy is intended to run.
    pub fn latency_offset(self, latency_offset: i64) -> Self {
        Self {
            latency_offset,
            ..self
        }
    }

    /// Sets a latency model.
    pub fn latency_model(self, latency_model: LM) -> Self {
        Self {
            latency_model: Some(latency_model),
            ..self
        }
    }

    /// Sets an asset type.
    pub fn asset_type(self, asset_type: AT) -> Self {
        Self {
            asset_type: Some(asset_type),
            ..self
        }
    }

    /// Sets a fee model.
    pub fn fee_model(self, fee_model: FM) -> Self {
        Self {
            fee_model: Some(fee_model),
            ..self
        }
    }

    /// Sets an exchange model. The default value is [`NoPartialFillExchange`].
    pub fn exchange(self, exch_kind: ExchangeKind) -> Self {
        Self { exch_kind, ..self }
    }

    /// Sets the initial capacity of the vector storing the last market trades.
    /// The default value is `0`, indicating that no last trades are stored.
    pub fn last_trades_capacity(self, capacity: usize) -> Self {
        Self {
            last_trades_cap: capacity,
            ..self
        }
    }

    /// Sets a queue model.
    pub fn queue_model(self, queue_model: QM) -> Self {
        Self {
            queue_model: Some(queue_model),
            ..self
        }
    }

    /// Sets a market depth builder.
    pub fn depth<Builder>(self, builder: Builder) -> Self
    where
        Builder: Fn() -> MD + 'static,
    {
        Self {
            depth_builder: Some(Box::new(builder)),
            ..self
        }
    }

    /// Builds an `Asset`.
    pub fn build(self) -> Result<Asset<dyn LocalProcessor<MD>, dyn Processor, Event>, BuildError> {
        let reader = if self.latency_offset == 0 {
            Reader::builder()
                .parallel_load(self.parallel_load)
                .data(self.data)
                .build()
                .map_err(|err| BuildError::Error(err.into()))?
        } else {
            Reader::builder()
                .parallel_load(self.parallel_load)
                .data(self.data)
                .preprocessor(FeedLatencyAdjustment::new(self.latency_offset))
                .build()
                .map_err(|err| BuildError::Error(err.into()))?
        };

        let create_depth = self
            .depth_builder
            .as_ref()
            .ok_or(BuildError::BuilderIncomplete("depth"))?;
        let order_latency = self
            .latency_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("order_latency"))?;
        let asset_type = self
            .asset_type
            .clone()
            .ok_or(BuildError::BuilderIncomplete("asset_type"))?;
        let fee_model = self
            .fee_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("fee_model"))?;

        let (order_e2l, order_l2e) = order_bus(order_latency);

        let local = L3Local::new(
            create_depth(),
            State::new(asset_type, fee_model),
            self.last_trades_cap,
            order_l2e,
        );

        let queue_model = self
            .queue_model
            .ok_or(BuildError::BuilderIncomplete("queue_model"))?;
        let asset_type = self
            .asset_type
            .clone()
            .ok_or(BuildError::BuilderIncomplete("asset_type"))?;
        let fee_model = self
            .fee_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("fee_model"))?;

        match self.exch_kind {
            ExchangeKind::NoPartialFillExchange => {
                let exch = L3NoPartialFillExchange::new(
                    create_depth(),
                    State::new(asset_type, fee_model),
                    queue_model,
                    order_e2l,
                );

                Ok(Asset {
                    local: Box::new(local),
                    exch: Box::new(exch),
                    reader,
                })
            }
            ExchangeKind::PartialFillExchange => {
                unimplemented!();
            }
        }
    }
}

impl<LM, AT, QM, MD, FM> Default for L3AssetBuilder<LM, AT, QM, MD, FM>
where
    AT: AssetType + Clone + 'static,
    MD: MarketDepth + L3MarketDepth + 'static,
    QM: L3QueueModel<MD> + 'static,
    LM: LatencyModel + Clone + 'static,
    FM: FeeModel + Clone + 'static,
    BacktestError: From<<MD as L3MarketDepth>::Error>,
{
    fn default() -> Self {
        Self::new()
    }
}

/// [`Backtest`] builder.
pub struct BacktestBuilder<MD> {
    local: Vec<BacktestProcessorState<Box<dyn LocalProcessor<MD>>>>,
    exch: Vec<BacktestProcessorState<Box<dyn Processor>>>,
}

impl<MD> BacktestBuilder<MD> {
    /// Adds [`Asset`], which will undergo simulation within the backtester.
    pub fn add_asset(self, asset: Asset<dyn LocalProcessor<MD>, dyn Processor, Event>) -> Self {
        let mut self_ = Self { ..self };
        self_.local.push(BacktestProcessorState::new(
            asset.local,
            asset.reader.clone(),
        ));
        self_
            .exch
            .push(BacktestProcessorState::new(asset.exch, asset.reader));
        self_
    }

    /// Builds [`Backtest`].
    pub fn build(self) -> Result<Backtest<MD>, BuildError> {
        let num_assets = self.local.len();
        if self.local.len() != num_assets || self.exch.len() != num_assets {
            panic!();
        }
        Ok(Backtest {
            cur_ts: i64::MAX,
            evs: EventSet::new(num_assets),
            local: self.local,
            exch: self.exch,
        })
    }
}

/// This backtester provides multi-asset and multi-exchange model backtesting, allowing you to
/// configure different setups such as queue models or asset types for each asset. However, this may
/// result in slightly slower performance compared to [`Backtest`].
pub struct Backtest<MD> {
    cur_ts: i64,
    evs: EventSet,
    local: Vec<BacktestProcessorState<Box<dyn LocalProcessor<MD>>>>,
    exch: Vec<BacktestProcessorState<Box<dyn Processor>>>,
}

impl<P: Processor> Deref for BacktestProcessorState<P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &self.processor
    }
}

impl<P: Processor> DerefMut for BacktestProcessorState<P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.processor
    }
}

/// Per asset backtesting state used internally to advance event buffers.
pub struct BacktestProcessorState<P: Processor> {
    data: Data<Event>,
    processor: P,
    reader: Reader<Event>,
    row: Option<usize>,
}

impl<P: Processor> BacktestProcessorState<P> {
    fn new(processor: P, reader: Reader<Event>) -> BacktestProcessorState<P> {
        Self {
            data: Data::empty(),
            processor,
            reader,
            row: None,
        }
    }

    /// Get the index of the next available row, only advancing the reader if there's no
    /// row currently available.
    fn next_row(&mut self) -> Result<usize, BacktestError> {
        if self.row.is_none() {
            let _ = self.advance()?;
        }

        self.row.ok_or(BacktestError::EndOfData)
    }

    /// Advance the state of this processor to the next available event and return the
    /// timestamp it occurred at, if any.
    fn advance(&mut self) -> Result<i64, BacktestError> {
        loop {
            let start = self.row.map(|rn| rn + 1).unwrap_or(0);

            for rn in start..self.data.len() {
                if let Some(ts) = self.processor.event_seen_timestamp(&self.data[rn]) {
                    self.row = Some(rn);
                    return Ok(ts);
                }
            }

            let next = self.reader.next_data()?;

            self.reader.release(std::mem::replace(&mut self.data, next));
            self.row = None;
        }
    }
}

impl<MD> Backtest<MD>
where
    MD: MarketDepth,
{
    pub fn builder() -> BacktestBuilder<MD> {
        BacktestBuilder {
            local: vec![],
            exch: vec![],
        }
    }

    pub fn new(
        local: Vec<Box<dyn LocalProcessor<MD>>>,
        exch: Vec<Box<dyn Processor>>,
        reader: Vec<Reader<Event>>,
    ) -> Self {
        let num_assets = local.len();
        if local.len() != num_assets || exch.len() != num_assets || reader.len() != num_assets {
            panic!();
        }

        let local = local
            .into_iter()
            .zip(reader.iter())
            .map(|(proc, reader)| BacktestProcessorState::new(proc, reader.clone()))
            .collect();
        let exch = exch
            .into_iter()
            .zip(reader.iter())
            .map(|(proc, reader)| BacktestProcessorState::new(proc, reader.clone()))
            .collect();

        Self {
            local,
            exch,
            cur_ts: i64::MAX,
            evs: EventSet::new(num_assets),
        }
    }

    fn initialize_evs(&mut self) -> Result<(), BacktestError> {
        for (asset_no, local) in self.local.iter_mut().enumerate() {
            match local.advance() {
                Ok(ts) => self.evs.update_local_data(asset_no, ts),
                Err(BacktestError::EndOfData) => {
                    self.evs.invalidate_local_data(asset_no);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        for (asset_no, exch) in self.exch.iter_mut().enumerate() {
            match exch.advance() {
                Ok(ts) => self.evs.update_exch_data(asset_no, ts),
                Err(BacktestError::EndOfData) => {
                    self.evs.invalidate_exch_data(asset_no);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    pub fn goto_end(&mut self) -> Result<ElapseResult, BacktestError> {
        if self.cur_ts == i64::MAX {
            self.initialize_evs()?;
            match self.evs.next() {
                Some(ev) => {
                    self.cur_ts = ev.timestamp;
                }
                None => {
                    return Ok(ElapseResult::EndOfData);
                }
            }
        }
        self.goto::<false>(UNTIL_END_OF_DATA, WaitOrderResponse::None)
    }

    fn goto<const WAIT_NEXT_FEED: bool>(
        &mut self,
        timestamp: i64,
        wait_order_response: WaitOrderResponse,
    ) -> Result<ElapseResult, BacktestError> {
        let mut result = ElapseResult::Ok;
        let mut timestamp = timestamp;
        for (asset_no, local) in self.local.iter().enumerate() {
            self.evs
                .update_exch_order(asset_no, local.earliest_send_order_timestamp());
            self.evs
                .update_local_order(asset_no, local.earliest_recv_order_timestamp());
        }
        loop {
            match self.evs.next() {
                Some(ev) => {
                    if ev.timestamp > timestamp {
                        self.cur_ts = timestamp;
                        return Ok(result);
                    }
                    match ev.kind {
                        EventIntentKind::LocalData => {
                            let local = unsafe { self.local.get_unchecked_mut(ev.asset_no) };
                            let next = local.next_row().and_then(|row| {
                                local.processor.process(&local.data[row])?;
                                local.advance()
                            });

                            match next {
                                Ok(next_ts) => {
                                    self.evs.update_local_data(ev.asset_no, next_ts);
                                }
                                Err(BacktestError::EndOfData) => {
                                    self.evs.invalidate_local_data(ev.asset_no);
                                }
                                Err(e) => {
                                    return Err(e);
                                }
                            }
                            if WAIT_NEXT_FEED {
                                timestamp = ev.timestamp;
                                result = ElapseResult::MarketFeed;
                            }
                        }
                        EventIntentKind::LocalOrder => {
                            let local = unsafe { self.local.get_unchecked_mut(ev.asset_no) };
                            let wait_order_resp_id = match wait_order_response {
                                WaitOrderResponse::Specified {
                                    asset_no: wait_order_asset_no,
                                    order_id: wait_order_id,
                                } if ev.asset_no == wait_order_asset_no => Some(wait_order_id),
                                _ => None,
                            };
                            if local.process_recv_order(ev.timestamp, wait_order_resp_id)?
                                || wait_order_response == WaitOrderResponse::Any
                            {
                                timestamp = ev.timestamp;
                                if WAIT_NEXT_FEED {
                                    result = ElapseResult::OrderResponse;
                                }
                            }
                            self.evs.update_local_order(
                                ev.asset_no,
                                local.earliest_recv_order_timestamp(),
                            );
                        }
                        EventIntentKind::ExchData => {
                            let exch = unsafe { self.exch.get_unchecked_mut(ev.asset_no) };
                            let next = exch.next_row().and_then(|row| {
                                exch.processor.process(&exch.data[row])?;
                                exch.advance()
                            });

                            match next {
                                Ok(next_ts) => {
                                    self.evs.update_exch_data(ev.asset_no, next_ts);
                                }
                                Err(BacktestError::EndOfData) => {
                                    self.evs.invalidate_exch_data(ev.asset_no);
                                }
                                Err(e) => {
                                    return Err(e);
                                }
                            }
                            self.evs.update_local_order(
                                ev.asset_no,
                                exch.earliest_send_order_timestamp(),
                            );
                        }
                        EventIntentKind::ExchOrder => {
                            let exch = unsafe { self.exch.get_unchecked_mut(ev.asset_no) };
                            let _ = exch.process_recv_order(ev.timestamp, None)?;
                            self.evs.update_exch_order(
                                ev.asset_no,
                                exch.earliest_recv_order_timestamp(),
                            );
                            self.evs.update_local_order(
                                ev.asset_no,
                                exch.earliest_send_order_timestamp(),
                            );
                        }
                    }
                }
                None => {
                    return Ok(ElapseResult::EndOfData);
                }
            }
        }
    }
}

impl<MD> Bot<MD> for Backtest<MD>
where
    MD: MarketDepth,
{
    type Error = BacktestError;

    #[inline]
    fn current_timestamp(&self) -> i64 {
        self.cur_ts
    }

    #[inline]
    fn num_assets(&self) -> usize {
        self.local.len()
    }

    #[inline]
    fn position(&self, asset_no: usize) -> f64 {
        self.local.get(asset_no).unwrap().position()
    }

    #[inline]
    fn snapshot_ready(&self, _asset_no: usize) -> bool {
        // Backtest starts fully initialized; there is no registration-time snapshot phase.
        true
    }

    #[inline]
    fn position_observed(&self, _asset_no: usize) -> bool {
        // The local state owns the position from the first tick; there is no venue round trip
        // whose completion a strategy could be waiting on.
        true
    }

    #[inline]
    fn state_values(&self, asset_no: usize) -> &StateValues {
        self.local.get(asset_no).unwrap().state_values()
    }

    fn depth(&self, asset_no: usize) -> &MD {
        self.local.get(asset_no).unwrap().depth()
    }

    fn last_trades(&self, asset_no: usize) -> &[Event] {
        self.local.get(asset_no).unwrap().last_trades()
    }

    #[inline]
    fn clear_last_trades(&mut self, asset_no: Option<usize>) {
        match asset_no {
            Some(an) => {
                let local = self.local.get_mut(an).unwrap();
                local.clear_last_trades();
            }
            None => {
                for local in self.local.iter_mut() {
                    local.clear_last_trades();
                }
            }
        }
    }

    #[inline]
    fn orders(&self, asset_no: usize) -> &HashMap<u64, Order> {
        self.local.get(asset_no).unwrap().orders()
    }

    #[inline]
    fn submit_buy_order(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let local = self.local.get_mut(asset_no).unwrap();
        local.submit_order(
            order_id,
            Side::Buy,
            price,
            qty,
            order_type,
            time_in_force,
            self.cur_ts,
        )?;

        if wait {
            return self.goto::<false>(
                UNTIL_END_OF_DATA,
                WaitOrderResponse::Specified { asset_no, order_id },
            );
        }
        Ok(ElapseResult::Ok)
    }

    #[inline]
    fn submit_sell_order(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let local = self.local.get_mut(asset_no).unwrap();
        local.submit_order(
            order_id,
            Side::Sell,
            price,
            qty,
            order_type,
            time_in_force,
            self.cur_ts,
        )?;

        if wait {
            return self.goto::<false>(
                UNTIL_END_OF_DATA,
                WaitOrderResponse::Specified { asset_no, order_id },
            );
        }
        Ok(ElapseResult::Ok)
    }

    fn submit_order(
        &mut self,
        asset_no: usize,
        order: OrderRequest,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let local = self.local.get_mut(asset_no).unwrap();
        local.submit_order(
            order.order_id,
            order.side,
            order.price,
            order.qty,
            order.order_type,
            order.time_in_force,
            self.cur_ts,
        )?;

        if wait {
            return self.goto::<false>(
                UNTIL_END_OF_DATA,
                WaitOrderResponse::Specified {
                    asset_no,
                    order_id: order.order_id,
                },
            );
        }
        Ok(ElapseResult::Ok)
    }

    #[inline]
    fn modify(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let local = self.local.get_mut(asset_no).unwrap();
        local.modify(order_id, price, qty, self.cur_ts)?;

        if wait {
            return self.goto::<false>(
                UNTIL_END_OF_DATA,
                WaitOrderResponse::Specified { asset_no, order_id },
            );
        }
        Ok(ElapseResult::Ok)
    }

    #[inline]
    fn cancel(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let local = self.local.get_mut(asset_no).unwrap();
        local.cancel(order_id, self.cur_ts)?;

        if wait {
            return self.goto::<false>(
                UNTIL_END_OF_DATA,
                WaitOrderResponse::Specified { asset_no, order_id },
            );
        }
        Ok(ElapseResult::Ok)
    }

    #[inline]
    fn clear_inactive_orders(&mut self, asset_no: Option<usize>) {
        match asset_no {
            Some(asset_no) => {
                self.local
                    .get_mut(asset_no)
                    .unwrap()
                    .clear_inactive_orders();
            }
            None => {
                for local in self.local.iter_mut() {
                    local.clear_inactive_orders();
                }
            }
        }
    }

    #[inline]
    fn wait_order_response(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        timeout: i64,
    ) -> Result<ElapseResult, BacktestError> {
        self.goto::<false>(
            self.cur_ts + timeout,
            WaitOrderResponse::Specified { asset_no, order_id },
        )
    }

    #[inline]
    fn wait_next_feed(
        &mut self,
        include_order_resp: bool,
        timeout: i64,
    ) -> Result<ElapseResult, Self::Error> {
        if self.cur_ts == i64::MAX {
            self.initialize_evs()?;
            match self.evs.next() {
                Some(ev) => {
                    self.cur_ts = ev.timestamp;
                }
                None => {
                    return Ok(ElapseResult::EndOfData);
                }
            }
        }
        if include_order_resp {
            self.goto::<true>(self.cur_ts + timeout, WaitOrderResponse::Any)
        } else {
            self.goto::<true>(self.cur_ts + timeout, WaitOrderResponse::None)
        }
    }

    #[inline]
    fn elapse(&mut self, duration: i64) -> Result<ElapseResult, Self::Error> {
        if self.cur_ts == i64::MAX {
            self.initialize_evs()?;
            match self.evs.next() {
                Some(ev) => {
                    self.cur_ts = ev.timestamp;
                }
                None => {
                    return Ok(ElapseResult::EndOfData);
                }
            }
        }
        self.goto::<false>(self.cur_ts + duration, WaitOrderResponse::None)
    }

    #[inline]
    fn elapse_bt(&mut self, duration: i64) -> Result<ElapseResult, Self::Error> {
        self.elapse(duration)
    }

    #[inline]
    fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline]
    fn feed_latency(&self, asset_no: usize) -> Option<(i64, i64)> {
        self.local.get(asset_no).unwrap().feed_latency()
    }

    #[inline]
    fn order_latency(&self, asset_no: usize) -> Option<(i64, i64, i64)> {
        self.local.get(asset_no).unwrap().order_latency()
    }
}

#[cfg(test)]
mod test {
    use std::error::Error;

    use crate::{
        backtest::{
            Backtest,
            BacktestError,
            DataSource,
            ExchangeKind,
            ExchangeKind::NoPartialFillExchange,
            L2AssetBuilder,
            L3AssetBuilder,
            assettype::LinearAsset,
            data::Data,
            models::{
                CommonFees,
                ConstantLatency,
                L3FIFOQueueModel,
                PowerProbQueueFunc3,
                ProbQueueModel,
                RiskAdverseQueueModel,
                TradingValueFeeModel,
            },
        },
        depth::HashMapMarketDepth,
        prelude::{Bot, Event},
        types::{
            EXCH_ASK_DEPTH_EVENT,
            EXCH_BID_ADD_ORDER_EVENT,
            EXCH_BID_DEPTH_EVENT,
            EXCH_BUY_TRADE_EVENT,
            EXCH_EVENT,
            ExecDelta,
            LOCAL_ASK_DEPTH_EVENT,
            LOCAL_BID_ADD_ORDER_EVENT,
            LOCAL_BID_DEPTH_EVENT,
            LOCAL_BUY_TRADE_EVENT,
            LOCAL_EVENT,
            OrdType,
            OrderId,
            OrderRequest,
            Side,
            Status,
            TimeInForce,
        },
    };

    #[test]
    fn skips_unseen_events() -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[
            Event {
                ev: EXCH_EVENT | LOCAL_EVENT,
                exch_ts: 0,
                local_ts: 0,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: LOCAL_EVENT | EXCH_EVENT,
                exch_ts: 1,
                local_ts: 1,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_EVENT,
                exch_ts: 3,
                local_ts: 4,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: LOCAL_EVENT,
                exch_ts: 3,
                local_ts: 4,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ]);

        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(50, 50))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(ProbQueueModel::new(PowerProbQueueFunc3::new(3.0)))
                    .exchange(NoPartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(0.01, 1.0))
                    .build()?,
            )
            .build()?;

        // Process first events and advance a single timestep
        backtester.elapse_bt(1)?;
        assert_eq!(1, backtester.cur_ts);

        // Check that we correctly skip past events that aren't seen by a given processor
        backtester.elapse_bt(1)?;
        assert_eq!(2, backtester.cur_ts);
        assert_eq!(Some(3), backtester.local[0].row);
        assert_eq!(Some(2), backtester.exch[0].row);

        backtester.elapse_bt(1)?;
        assert_eq!(3, backtester.cur_ts);

        Ok(())
    }

    //
    // Fill accounting: the local state must account for every execution the exchange reports,
    // exactly once.
    //

    const TICK_SIZE: f64 = 0.01;
    const LOT_SIZE: f64 = 1.0;
    const BID_PRICE: f64 = 100.00;
    /// The price of the resting sell order, which is also the best ask.
    const ASK_PRICE: f64 = 100.01;
    /// The quantity resting ahead of the order in the queue.
    const QUEUE_AHEAD_QTY: f64 = 5.0;
    const ORDER_QTY: f64 = 10.0;
    const ORDER_ID: OrderId = 1;
    const LATENCY: i64 = 10_000_000;
    const SEC: i64 = 1_000_000_000;
    const MAKER_FEE: f64 = 0.0002;

    fn feed_event(ev: u64, ts: i64, px: f64, qty: f64) -> Event {
        Event {
            ev,
            exch_ts: ts,
            local_ts: ts,
            px,
            qty,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        }
    }

    /// The initial book followed by buy trades at [`ASK_PRICE`], given as `(timestamp, quantity)`.
    fn feed(trades: &[(i64, f64)]) -> Vec<Event> {
        let mut events = vec![
            feed_event(
                LOCAL_BID_DEPTH_EVENT | EXCH_BID_DEPTH_EVENT,
                0,
                BID_PRICE,
                10.0,
            ),
            feed_event(
                LOCAL_ASK_DEPTH_EVENT | EXCH_ASK_DEPTH_EVENT,
                0,
                ASK_PRICE,
                QUEUE_AHEAD_QTY,
            ),
        ];
        events.extend(trades.iter().map(|&(ts, qty)| {
            feed_event(
                LOCAL_BUY_TRADE_EVENT | EXCH_BUY_TRADE_EVENT,
                ts,
                ASK_PRICE,
                qty,
            )
        }));
        events
    }

    /// A single asset using [`RiskAdverseQueueModel`], whose queue position advances only by trades
    /// at the order's own price. That makes each trade's executable quantity exactly
    /// `traded qty - queue ahead`, so partial executions are deterministic.
    fn backtest(
        exch_kind: ExchangeKind,
        events: &[Event],
    ) -> Result<Backtest<HashMapMarketDepth>, Box<dyn Error>> {
        Ok(Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(Data::from_data(events))])
                    .latency_model(ConstantLatency::new(LATENCY, LATENCY))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(MAKER_FEE, 0.0)))
                    .queue_model(RiskAdverseQueueModel::new())
                    .exchange(exch_kind)
                    .depth(|| HashMapMarketDepth::new(TICK_SIZE, LOT_SIZE))
                    .build()?,
            )
            .build()?)
    }

    fn l3_event(ev: u64, ts: i64, order_id: OrderId, px: f64, qty: f64) -> Event {
        Event {
            ev,
            exch_ts: ts,
            local_ts: ts,
            px,
            qty,
            order_id,
            ival: 0,
            fval: 0.0,
        }
    }

    /// The L3 counterpart of [`backtest`]. [`L3AssetBuilder`] supports
    /// [`NoPartialFillExchange`] only, so the exchange kind is not a parameter.
    fn l3_backtest(events: &[Event]) -> Result<Backtest<HashMapMarketDepth>, Box<dyn Error>> {
        Ok(Backtest::builder()
            .add_asset(
                L3AssetBuilder::default()
                    .data(vec![DataSource::Data(Data::from_data(events))])
                    .latency_model(ConstantLatency::new(LATENCY, LATENCY))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(MAKER_FEE, 0.0)))
                    .queue_model(L3FIFOQueueModel::new())
                    .exchange(NoPartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(TICK_SIZE, LOT_SIZE))
                    .build()?,
            )
            .build()?)
    }

    /// Rests the sell order behind [`QUEUE_AHEAD_QTY`] and waits for the exchange acknowledgement.
    fn submit_resting_sell(bt: &mut Backtest<HashMapMarketDepth>) -> Result<(), Box<dyn Error>> {
        bt.elapse(0)?;
        bt.submit_sell_order(
            0,
            ORDER_ID,
            ASK_PRICE,
            ORDER_QTY,
            TimeInForce::GTX,
            OrdType::Limit,
            true,
        )?;
        assert_eq!(bt.orders(0).get(&ORDER_ID).unwrap().status, Status::New);
        Ok(())
    }

    /// Asserts the state that a sell of `sold` at [`ASK_PRICE`], executed as a maker in
    /// `num_trades` executions, must produce.
    fn assert_sold(bt: &Backtest<HashMapMarketDepth>, sold: f64, num_trades: i64) {
        let values = bt.state_values(0);
        let value = ASK_PRICE * sold;
        assert_eq!(values.position, -sold, "position");
        assert_eq!(values.num_trades, num_trades, "num_trades");
        assert_eq!(values.trading_volume, sold, "trading_volume");
        assert!(
            (values.trading_value - value).abs() < 1e-9,
            "trading_value: expected {value}, got {}",
            values.trading_value
        );
        assert!(
            (values.balance - value).abs() < 1e-9,
            "balance: expected {value}, got {}",
            values.balance
        );
        let fee = MAKER_FEE * value;
        assert!(
            (values.fee - fee).abs() < 1e-9,
            "fee: expected {fee}, got {}",
            values.fee
        );
    }

    /// A backtest has no registration phase and no venue round trip to wait for: its position is
    /// authoritative from the first tick. Both readiness signals therefore answer `true` before
    /// anything has elapsed, so a strategy that gates on them in live mode runs unchanged here.
    #[test]
    fn a_backtest_is_ready_and_reports_its_position_from_the_start() -> Result<(), Box<dyn Error>> {
        let bt = backtest(ExchangeKind::NoPartialFillExchange, &feed(&[]))?;
        assert!(bt.snapshot_ready(0));
        assert!(bt.position_observed(0));
        Ok(())
    }

    /// [`PartialFillExchange`] reports each execution of an order separately, and `exec_qty` is the
    /// quantity executed by that single execution. The local state must accumulate them.
    #[test]
    fn partial_fills_accumulate_in_the_local_state() -> Result<(), Box<dyn Error>> {
        // 5 rests ahead of the order, so the trades execute 3, then 4, then the remaining 3.
        let feed = feed(&[(SEC, 8.0), (2 * SEC, 4.0), (3 * SEC, 5.0)]);
        let mut bt = backtest(ExchangeKind::PartialFillExchange, &feed)?;
        submit_resting_sell(&mut bt)?;

        bt.elapse(SEC)?;
        let order = bt.orders(0).get(&ORDER_ID).unwrap();
        assert_eq!(order.status, Status::PartiallyFilled);
        assert_eq!(order.leaves_qty, 7.0);
        assert_eq!(order.exec_qty, ExecDelta::of_execution(3.0));
        assert_sold(&bt, 3.0, 1);

        bt.elapse(SEC)?;
        let order = bt.orders(0).get(&ORDER_ID).unwrap();
        assert_eq!(order.status, Status::PartiallyFilled);
        assert_eq!(order.leaves_qty, 3.0);
        assert_eq!(order.exec_qty, ExecDelta::of_execution(4.0));
        assert_sold(&bt, 7.0, 2);

        bt.elapse(SEC)?;
        let order = bt.orders(0).get(&ORDER_ID).unwrap();
        assert_eq!(order.status, Status::Filled);
        assert_eq!(order.leaves_qty, 0.0);
        assert_eq!(order.exec_qty, ExecDelta::of_execution(3.0));
        assert_sold(&bt, ORDER_QTY, 3);

        Ok(())
    }

    /// The case that silently lost the position: the remainder is canceled, so the completing
    /// `Filled` response never arrives, and the partial is all there ever is.
    #[test]
    fn partial_fill_survives_the_cancellation_of_the_remainder() -> Result<(), Box<dyn Error>> {
        let feed = feed(&[(SEC, 8.0), (2 * SEC, 8.0)]);
        let mut bt = backtest(ExchangeKind::PartialFillExchange, &feed)?;
        submit_resting_sell(&mut bt)?;

        bt.elapse(SEC)?;
        assert_sold(&bt, 3.0, 1);

        bt.cancel(0, ORDER_ID, true)?;
        let order = bt.orders(0).get(&ORDER_ID).unwrap();
        assert_eq!(order.status, Status::Canceled);
        assert_eq!(order.leaves_qty, 7.0);
        assert_sold(&bt, 3.0, 1);

        // The canceled remainder must not execute against the later trade either.
        bt.elapse(2 * SEC)?;
        assert_sold(&bt, 3.0, 1);

        Ok(())
    }

    /// An acknowledgement is not an execution: modifying a partially filled order echoes its
    /// status back to the local, and applying that echo would count the partial twice.
    #[test]
    fn modify_acknowledgement_does_not_reapply_the_partial_fill() -> Result<(), Box<dyn Error>> {
        let feed = feed(&[(SEC, 8.0)]);
        let mut bt = backtest(ExchangeKind::PartialFillExchange, &feed)?;
        submit_resting_sell(&mut bt)?;

        bt.elapse(SEC)?;
        assert_sold(&bt, 3.0, 1);

        // Same price and a quantity below the remaining 7, so the exchange amends the resting
        // order in place instead of replacing it.
        bt.modify(0, ORDER_ID, ASK_PRICE, 5.0, true)?;
        let order = bt.orders(0).get(&ORDER_ID).unwrap();
        assert_eq!(order.leaves_qty, 5.0);
        assert_sold(&bt, 3.0, 1);

        Ok(())
    }

    /// **An order whose side is not a side is refused where it is submitted** (invariant E2).
    ///
    /// [`Side`] carries `None` and `Unsupported` — a connector's word for "the venue said
    /// something I do not recognise" — and neither has a sign. Position math needs one, so an
    /// order carrying one used to rest, execute, and take the whole process down inside
    /// `State::apply_fill`. The check now lives at the submit boundary, where the caller gets a
    /// recoverable error and nothing has been placed; past it, the sign exists by construction
    /// ([`ResolvedSide`]) and the panic sites are gone.
    #[test]
    fn an_order_whose_side_has_no_sign_is_refused_at_the_boundary() -> Result<(), Box<dyn Error>> {
        for side in [Side::None, Side::Unsupported] {
            let mut bt = backtest(ExchangeKind::NoPartialFillExchange, &feed(&[]))?;
            bt.elapse(0)?;
            let refused = bt.submit_order(
                0,
                OrderRequest {
                    order_id: ORDER_ID,
                    price: ASK_PRICE,
                    qty: ORDER_QTY,
                    side,
                    time_in_force: TimeInForce::GTX,
                    order_type: OrdType::Limit,
                },
                false,
            );
            assert!(
                matches!(refused, Err(BacktestError::InvalidOrderRequest)),
                "{side:?} must be refused, got {refused:?}"
            );
            assert!(
                bt.orders(0).is_empty(),
                "a refused order rests nowhere: {side:?}"
            );
        }
        Ok(())
    }

    /// The L3 twin of [`an_order_whose_side_has_no_sign_is_refused_at_the_boundary`].
    ///
    /// `L3Local::submit_order` carries its **own** copy of the boundary check — it does not call
    /// `Local::submit_order` — so the L2 pin says nothing about it. Measured before this test
    /// existed: deleting the check from `l3_local.rs` left the whole lib suite green (69 tests
    /// then), which is to say the invariant on the L3 path was asserted by nobody.
    /// `L3AssetBuilder` accepts [`NoPartialFillExchange`] only, hence no exchange-kind loop.
    #[test]
    fn an_order_whose_side_has_no_sign_is_refused_at_the_l3_boundary() -> Result<(), Box<dyn Error>>
    {
        let book = [l3_event(
            LOCAL_BID_ADD_ORDER_EVENT | EXCH_BID_ADD_ORDER_EVENT,
            0,
            1000,
            BID_PRICE,
            10.0,
        )];
        for side in [Side::None, Side::Unsupported] {
            let mut bt = l3_backtest(&book)?;
            bt.elapse(0)?;
            let refused = bt.submit_order(
                0,
                OrderRequest {
                    order_id: ORDER_ID,
                    price: ASK_PRICE,
                    qty: ORDER_QTY,
                    side,
                    time_in_force: TimeInForce::GTX,
                    order_type: OrdType::Limit,
                },
                false,
            );
            assert!(
                matches!(refused, Err(BacktestError::InvalidOrderRequest)),
                "{side:?} must be refused at the L3 boundary, got {refused:?}"
            );
            assert!(
                bt.orders(0).is_empty(),
                "a refused order rests nowhere: {side:?}"
            );
        }
        Ok(())
    }

    /// [`NoPartialFillExchange`] executes the whole remaining quantity regardless of the traded
    /// quantity; only the single `Filled` response exists, and it must be applied once.
    #[test]
    fn no_partial_fill_exchange_executes_the_whole_order_at_once() -> Result<(), Box<dyn Error>> {
        let feed = feed(&[(SEC, 8.0)]);
        let mut bt = backtest(NoPartialFillExchange, &feed)?;
        submit_resting_sell(&mut bt)?;

        bt.elapse(SEC)?;
        let order = bt.orders(0).get(&ORDER_ID).unwrap();
        assert_eq!(order.status, Status::Filled);
        assert_eq!(order.leaves_qty, 0.0);
        // Only 3 of the traded 8 was executable by queue position, yet the whole order fills.
        assert_eq!(order.exec_qty, ExecDelta::of_execution(ORDER_QTY));
        assert_sold(&bt, ORDER_QTY, 1);

        Ok(())
    }

    /// A rejected request carries no execution: the exchange has no record of the order, so it
    /// echoes the local's own copy back, execution fields and all.
    #[test]
    fn rejected_cancel_does_not_reapply_the_fill() -> Result<(), Box<dyn Error>> {
        let feed = feed(&[(SEC, 8.0)]);
        let mut bt = backtest(NoPartialFillExchange, &feed)?;
        submit_resting_sell(&mut bt)?;

        bt.elapse(SEC)?;
        assert_sold(&bt, ORDER_QTY, 1);

        // The order is already gone at the exchange, so this cancel is rejected.
        bt.cancel(0, ORDER_ID, true)?;
        assert_sold(&bt, ORDER_QTY, 1);

        Ok(())
    }

    /// The L3 twin of [`rejected_cancel_does_not_reapply_the_fill`]. `L3Local` shares the response
    /// handling with `Local`, but `L3AssetBuilder` supports [`NoPartialFillExchange`] only, so a
    /// partial execution cannot reach it; the rejected echo can.
    #[test]
    fn l3_rejected_cancel_does_not_reapply_the_fill() -> Result<(), Box<dyn Error>> {
        let events = [
            l3_event(
                LOCAL_BID_ADD_ORDER_EVENT | EXCH_BID_ADD_ORDER_EVENT,
                0,
                1000,
                BID_PRICE,
                10.0,
            ),
            // Lifts the best bid to the resting sell order's price, crossing it.
            l3_event(
                LOCAL_BID_ADD_ORDER_EVENT | EXCH_BID_ADD_ORDER_EVENT,
                SEC,
                1001,
                ASK_PRICE,
                10.0,
            ),
        ];
        let mut bt = l3_backtest(&events)?;

        submit_resting_sell(&mut bt)?;

        bt.elapse(SEC)?;
        assert_eq!(bt.orders(0).get(&ORDER_ID).unwrap().status, Status::Filled);
        assert_sold(&bt, ORDER_QTY, 1);

        bt.cancel(0, ORDER_ID, true)?;
        assert_sold(&bt, ORDER_QTY, 1);

        Ok(())
    }

    /// An execution that takes exactly the remaining quantity completes the order, and the
    /// exchange must retire it like any other completed order. Otherwise it rests on, already
    /// [`Status::Filled`], until the next trade at its price reaches it and aborts the entire
    /// backtest with [`BacktestError::InvalidOrderStatus`]. Exact remainders are ordinary with
    /// integer lot sizes.
    #[test]
    fn an_execution_that_exactly_consumes_the_remainder_completes_the_order()
    -> Result<(), Box<dyn Error>> {
        // 5 rests ahead of the order, so the trades execute 3, then exactly the remaining 7.
        let feed = feed(&[(SEC, 8.0), (2 * SEC, 7.0), (3 * SEC, 4.0)]);
        let mut bt = backtest(ExchangeKind::PartialFillExchange, &feed)?;
        submit_resting_sell(&mut bt)?;

        bt.elapse(SEC)?;
        assert_sold(&bt, 3.0, 1);

        bt.elapse(SEC)?;
        let order = bt.orders(0).get(&ORDER_ID).unwrap();
        assert_eq!(order.status, Status::Filled);
        assert_eq!(order.leaves_qty, 0.0);
        assert_eq!(order.exec_qty, ExecDelta::of_execution(7.0));
        assert_sold(&bt, ORDER_QTY, 2);

        // The order is gone from the exchange, so the next trade at its price passes it by.
        bt.elapse(SEC)?;
        assert_sold(&bt, ORDER_QTY, 2);

        Ok(())
    }

    /// Amending the price makes the exchange re-rest the order, and it must re-rest the
    /// amended quantity. `Local::modify` sets the order's `qty`, but the quantity that
    /// actually trades is its `leaves_qty`.
    #[test]
    fn amending_the_price_rests_the_amended_quantity() -> Result<(), Box<dyn Error>> {
        let amended_price = ASK_PRICE + TICK_SIZE;
        let amended_qty = 5.0;

        let mut events = feed(&[]);
        events.push(feed_event(
            LOCAL_BUY_TRADE_EVENT | EXCH_BUY_TRADE_EVENT,
            SEC,
            amended_price,
            20.0,
        ));
        let mut bt = backtest(ExchangeKind::PartialFillExchange, &events)?;
        submit_resting_sell(&mut bt)?;

        bt.modify(0, ORDER_ID, amended_price, amended_qty, true)?;
        let order = bt.orders(0).get(&ORDER_ID).unwrap();
        assert_eq!(order.qty, amended_qty);
        assert_eq!(order.leaves_qty, amended_qty);

        // Nothing rests ahead at the amended price and the trade is larger than the whole
        // order, so whatever rests is what fills.
        bt.elapse(2 * SEC)?;
        assert_eq!(bt.orders(0).get(&ORDER_ID).unwrap().status, Status::Filled);
        assert_eq!(bt.state_values(0).position, -amended_qty, "position");
        assert_eq!(
            bt.state_values(0).trading_volume,
            amended_qty,
            "trading_volume"
        );

        Ok(())
    }

    /// The re-resting counterpart of [`modify_acknowledgement_does_not_reapply_the_partial_fill`]:
    /// whichever way the exchange handles the amendment, the acknowledgement reports no
    /// execution. A request carries the local's copy of the last execution, which the local has
    /// already applied.
    #[test]
    fn amending_the_price_of_a_partially_filled_order_reports_no_execution()
    -> Result<(), Box<dyn Error>> {
        let feed = feed(&[(SEC, 8.0)]);
        let mut bt = backtest(ExchangeKind::PartialFillExchange, &feed)?;
        submit_resting_sell(&mut bt)?;

        bt.elapse(SEC)?;
        assert_sold(&bt, 3.0, 1);

        bt.modify(0, ORDER_ID, ASK_PRICE + TICK_SIZE, 5.0, true)?;
        let order = bt.orders(0).get(&ORDER_ID).unwrap();
        assert_eq!(order.status, Status::New);
        assert_eq!(order.leaves_qty, 5.0);
        assert_eq!(order.exec_qty, ExecDelta::of_execution(0.0));
        assert_sold(&bt, 3.0, 1);

        Ok(())
    }
}
