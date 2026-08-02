//! Property tests for the backtest money path: the fill accounting identity.
//!
//! `position == Σ(exec_qty × side)` holds over *any* sequence of order responses, and no type in
//! this crate can hold it. [`Order::exec_qty`] is the quantity executed by the single execution
//! that reports it, not the order's cumulative executed quantity, so the identity is a statement
//! about a whole sequence rather than about a value — §1.6 layer 2. It is the §4.6 regression class
//! stated once: partial executions that never reached the position, and request echoes that
//! reached it twice.
//!
//! The local half of the harness is the real one — [`Local`], [`LocalToExch::request`], which
//! clears the execution fields of every outgoing request, and `State::apply_fill`. The exchange
//! half is a script: every step declares the quantity the exchange executes, and that declaration,
//! not anything the local derived, is what the state is checked against.
//!
//! [`LocalToExch::request`]: crate::backtest::order::LocalToExch::request

use std::collections::HashMap;

use proptest::{prelude::*, test_runner::TestCaseError};

use crate::{
    backtest::{
        assettype::LinearAsset,
        models::{CommonFees, ConstantLatency, TradingValueFeeModel},
        order::{ExchToLocal, order_bus},
        proc::{Local, LocalProcessor, Processor},
        state::State,
    },
    depth::HashMapMarketDepth,
    types::{Liquidity, OrdType, Order, OrderId, Side, Status, TimeInForce},
};

const TICK_SIZE: f64 = 0.01;
const LOT_SIZE: f64 = 1.0;
/// Every order rests at the same price: this property is about quantities.
const PRICE: f64 = 100.0;
/// One-way order latency, so a request sent in a step is answered inside the same step.
const LATENCY: i64 = 10_000_000;
/// The interval between the strategy's actions, far wider than a round trip.
const STEP: i64 = 1_000_000_000;
/// Quantities are whole lots, so the identity is exact; this leaves room for rounding and nothing
/// else.
const EPS: f64 = 1e-9;

type TestLocal =
    Local<LinearAsset, ConstantLatency, HashMapMarketDepth, TradingValueFeeModel<CommonFees>>;

fn sign(side: Side) -> f64 {
    *AsRef::<f64>::as_ref(&side)
}

fn approx_eq(a: f64, b: f64) -> bool {
    (a - b).abs() <= EPS
}

/// What the script says the market did, which is what the local's state must add up to.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
struct Declared {
    position: f64,
    volume: f64,
    executions: i64,
}

impl Declared {
    fn executed(&mut self, side: Side, qty: f64) {
        self.position += qty * sign(side);
        self.volume += qty;
        self.executions += 1;
    }
}

/// The exchange half of the harness. It holds the exchange's copy of every order it accepted and
/// answers the local the way [`PartialFillExchange`] does:
///
/// * an acknowledgement echoes the request back — an execution is never reported by one;
/// * an execution reports the quantity *that* execution filled and lowers `leaves_qty` by it;
/// * a request naming an order the exchange no longer holds comes back `Status::Rejected`, which
///   hands the local its own copy of the order back.
///
/// [`PartialFillExchange`]: crate::backtest::proc::PartialFillExchange
struct ScriptedExchange {
    bus: ExchToLocal<ConstantLatency>,
    orders: HashMap<OrderId, Order>,
}

impl ScriptedExchange {
    /// The ids the exchange currently rests, in a stable order so a shrunk sequence replays.
    fn resting(&self) -> Vec<OrderId> {
        let mut ids: Vec<OrderId> = self.orders.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Executes `qty` lots of `order_id`, capped by what the order has left, and responds. Returns
    /// the executed quantity and the side it executed on, or `None` when the order is not there to
    /// execute.
    fn execute(&mut self, order_id: OrderId, qty: f64, timestamp: i64) -> Option<(Side, f64)> {
        let order = self.orders.get_mut(&order_id)?;
        if !order.active() {
            return None;
        }
        let exec_qty = qty.min(order.leaves_qty);
        if exec_qty <= 0.0 {
            return None;
        }

        // Every execution in this script is a resting one, and it says so the way the real
        // exchanges do — through the liquidity supplied when the fill is applied (C2).
        order.set_liquidity(Some(Liquidity::Maker));
        order.exec_price_tick = order.price_tick;
        order.exec_qty = exec_qty;
        order.leaves_qty -= exec_qty;
        order.status = if order.leaves_qty > 0.0 {
            Status::PartiallyFilled
        } else {
            Status::Filled
        };
        order.exch_timestamp = timestamp;

        let response = order.clone();
        let side = response.side;
        if response.status == Status::Filled {
            // A completed order is retired, as `remove_filled_orders` retires it.
            self.orders.remove(&order_id);
        }
        self.bus.respond(response);
        Some((side, exec_qty))
    }

    /// Receives every request that has reached the exchange by `until` and answers it.
    fn answer_requests(&mut self, until: i64) {
        while let Some(timestamp) = self.bus.earliest_recv_order_timestamp() {
            if timestamp > until {
                break;
            }
            let mut order = self.bus.receive(timestamp).unwrap();
            let req = order.req;
            order.req = Status::None;
            order.exch_timestamp = timestamp;
            match req {
                Status::New => {
                    order.status = Status::New;
                    self.orders.insert(order.order_id, order.clone());
                }
                Status::Canceled => match self.orders.remove(&order.order_id) {
                    Some(exch_order) => {
                        order = exch_order;
                        order.status = Status::Canceled;
                        order.exch_timestamp = timestamp;
                    }
                    None => order.req = Status::Rejected,
                },
                Status::Replaced => match self.orders.get_mut(&order.order_id) {
                    // Whether the exchange amends in place or re-rests the order, it rests the
                    // quantity the request carries and reports no execution.
                    Some(exch_order) => {
                        exch_order.qty = order.qty;
                        exch_order.leaves_qty = order.qty;
                        exch_order.price_tick = order.price_tick;
                        exch_order.status = Status::New;
                        exch_order.exch_timestamp = timestamp;
                        order.leaves_qty = order.qty;
                        order.status = Status::New;
                    }
                    None => order.req = Status::Rejected,
                },
                other => panic!("the local sent a request that is not a request: {other:?}"),
            }
            self.bus.respond(order);
        }
    }
}

/// One action of the generated sequence. Together they cover every response the local can see: an
/// acknowledgement, a rejection, a partial execution, a completing execution and an amendment.
#[derive(Debug, Clone, Copy)]
enum Step {
    /// The strategy rests a new order of `lots`.
    Rest { buy: bool, lots: u32 },
    /// The exchange executes `lots` of a resting order.
    Execute { nth: usize, lots: u32 },
    /// The strategy cancels an order it knows of. The exchange acknowledges it, or rejects it when
    /// the order is already gone — a rejection echoes the local's own copy of the order back.
    Cancel { nth: usize },
    /// The strategy cancels an order while the exchange executes `lots` of it. The cancel loses
    /// the race: it arrives after the execution, and is rejected outright when the execution
    /// completed the order.
    CancelRacingExecution { nth: usize, lots: u32 },
    /// The strategy amends an order to `lots`.
    Amend { nth: usize, lots: u32 },
}

fn step() -> impl Strategy<Value = Step> {
    let lots = 1u32..=7;
    let nth = 0usize..8;
    prop_oneof![
        3 => (any::<bool>(), lots.clone()).prop_map(|(buy, lots)| Step::Rest { buy, lots }),
        4 => (nth.clone(), lots.clone()).prop_map(|(nth, lots)| Step::Execute { nth, lots }),
        1 => nth.clone().prop_map(|nth| Step::Cancel { nth }),
        2 => (nth.clone(), lots.clone())
            .prop_map(|(nth, lots)| Step::CancelRacingExecution { nth, lots }),
        2 => (nth, lots).prop_map(|(nth, lots)| Step::Amend { nth, lots }),
    ]
}

/// The harness: a real [`Local`] wired to the scripted exchange by a real order bus.
struct Harness {
    local: TestLocal,
    exch: ScriptedExchange,
    declared: Declared,
    next_order_id: OrderId,
    /// The quantity each order last had left, for the monotonicity property. An amendment re-rests
    /// the order and so resets its baseline.
    left: HashMap<OrderId, f64>,
}

impl Harness {
    fn new() -> Self {
        let (exch_bus, local_bus) = order_bus(ConstantLatency::new(LATENCY, LATENCY));
        let local = Local::new(
            HashMapMarketDepth::new(TICK_SIZE, LOT_SIZE),
            State::new(
                LinearAsset::new(1.0),
                TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)),
            ),
            0,
            local_bus,
        );
        Self {
            local,
            exch: ScriptedExchange {
                bus: exch_bus,
                orders: HashMap::new(),
            },
            declared: Declared::default(),
            next_order_id: 1,
            left: HashMap::new(),
        }
    }

    /// The ids the local knows of, in a stable order. It knows of terminal orders too, which is
    /// what lets a step aim a request at an order the exchange has already retired.
    fn known(&self) -> Vec<OrderId> {
        let mut ids: Vec<OrderId> = self.local.orders().keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    fn apply(&mut self, step: Step, now: i64) {
        match step {
            Step::Rest { buy, lots } => {
                let order_id = self.next_order_id;
                self.next_order_id += 1;
                let side = if buy { Side::Buy } else { Side::Sell };
                self.local
                    .submit_order(
                        order_id,
                        side,
                        PRICE,
                        f64::from(lots) * LOT_SIZE,
                        OrdType::Limit,
                        TimeInForce::GTC,
                        now,
                    )
                    .unwrap();
            }
            Step::Execute { nth, lots } => {
                let resting = self.exch.resting();
                if resting.is_empty() {
                    return;
                }
                let order_id = resting[nth % resting.len()];
                if let Some((side, qty)) =
                    self.exch
                        .execute(order_id, f64::from(lots) * LOT_SIZE, now + LATENCY / 2)
                {
                    self.declared.executed(side, qty);
                }
            }
            Step::Cancel { nth } => {
                let known = self.known();
                if known.is_empty() {
                    return;
                }
                self.local.cancel(known[nth % known.len()], now).unwrap();
            }
            Step::CancelRacingExecution { nth, lots } => {
                let resting = self.exch.resting();
                if resting.is_empty() {
                    return;
                }
                let order_id = resting[nth % resting.len()];
                // The cancel is sent first and the execution happens before it arrives, so the
                // local receives the execution and then the answer to a cancel that was already
                // too late.
                self.local.cancel(order_id, now).unwrap();
                if let Some((side, qty)) =
                    self.exch
                        .execute(order_id, f64::from(lots) * LOT_SIZE, now + LATENCY / 2)
                {
                    self.declared.executed(side, qty);
                }
            }
            Step::Amend { nth, lots } => {
                let known = self.known();
                if known.is_empty() {
                    return;
                }
                let order_id = known[nth % known.len()];
                self.local
                    .modify(order_id, PRICE, f64::from(lots) * LOT_SIZE, now)
                    .unwrap();
                // The amendment re-rests the order, so what it had left before says nothing about
                // what it has left now.
                self.left.remove(&order_id);
            }
        }
    }

    /// Delivers everything in flight and checks the identity the state must satisfy.
    fn settle_and_check(&mut self, now: i64) -> Result<(), TestCaseError> {
        let until = now + STEP / 2;
        self.exch.answer_requests(until);
        loop {
            let timestamp = self.local.earliest_recv_order_timestamp();
            if timestamp > until {
                break;
            }
            self.local.process_recv_order(timestamp, None).unwrap();
        }

        let values = self.local.state_values();
        prop_assert!(
            approx_eq(values.position, self.declared.position),
            "position: the script executed {}, the state holds {}",
            self.declared.position,
            values.position
        );
        prop_assert!(
            approx_eq(values.trading_volume, self.declared.volume),
            "trading_volume: the script executed {}, the state holds {}",
            self.declared.volume,
            values.trading_volume
        );
        prop_assert_eq!(
            values.num_trades,
            self.declared.executions,
            "num_trades: every execution is counted exactly once"
        );

        for (order_id, order) in self.local.orders() {
            prop_assert!(
                order.leaves_qty >= 0.0,
                "leaves_qty went negative: {order:?}"
            );
            prop_assert!(
                order.leaves_qty <= order.qty + EPS,
                "leaves_qty exceeds the order: {order:?}"
            );
            if let Some(&before) = self.left.get(order_id) {
                prop_assert!(
                    order.leaves_qty <= before + EPS,
                    "leaves_qty rose from {before} without an amendment: {order:?}"
                );
            }
            self.left.insert(*order_id, order.leaves_qty);
        }
        Ok(())
    }
}

fn run(steps: &[Step]) -> Result<(), TestCaseError> {
    let mut harness = Harness::new();
    for (i, &step) in steps.iter().enumerate() {
        let now = (i as i64 + 1) * STEP;
        harness.apply(step, now);
        harness.settle_and_check(now)?;
    }
    // The end state is checked by the last step's settle, and once more here for the sequence that
    // left something in flight.
    harness.settle_and_check((steps.len() as i64 + 2) * STEP)
}

proptest! {
    /// The identity, after every step and at the end: the position the local holds is the sum of
    /// the executions the exchange reported, each counted once and with its own sign. Nothing else
    /// the local sees — an acknowledgement, a rejection that echoes the local's own copy of the
    /// order back, an amendment — moves it.
    #[test]
    fn the_position_is_the_sum_of_the_executions_the_exchange_reported(
        steps in prop::collection::vec(step(), 1..24)
    ) {
        run(&steps)?;
    }
}

/// The generated sequences are only worth something if the harness executes at all: a scripted
/// exchange that quietly declined every step would satisfy the identity with a position of zero
/// forever. This is the same sequence a generator can produce, with the arithmetic written out.
#[test]
fn the_harness_executes_what_the_script_declares() {
    let steps = [
        Step::Rest {
            buy: false,
            lots: 6,
        },
        // Three executions of the same order: 2, then 3, then the last lot completes it.
        Step::Execute { nth: 0, lots: 2 },
        Step::Execute { nth: 0, lots: 3 },
        Step::Execute { nth: 0, lots: 1 },
        // The order is gone, so this cancel is rejected and hands the local its own copy back.
        Step::Cancel { nth: 0 },
        Step::Rest { buy: true, lots: 4 },
        Step::Execute { nth: 0, lots: 1 },
    ];
    let mut harness = Harness::new();
    for (i, &step) in steps.iter().enumerate() {
        let now = (i as i64 + 1) * STEP;
        harness.apply(step, now);
        harness.settle_and_check(now).unwrap();
    }

    assert_eq!(harness.declared.executions, 4, "the script declared four");
    let values = harness.local.state_values();
    assert_eq!(values.num_trades, 4, "num_trades");
    assert_eq!(values.trading_volume, 7.0, "trading_volume");
    // Sold 6, bought 1.
    assert_eq!(values.position, -5.0, "position");
}
