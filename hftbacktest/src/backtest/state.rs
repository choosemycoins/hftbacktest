#[cfg(debug_assertions)]
use std::collections::HashMap;

#[cfg(debug_assertions)]
use crate::types::OrderId;
use crate::{
    backtest::{assettype::AssetType, models::FeeModel},
    types::{Order, ResolvedSide, StateValues},
};

/// Quantities are sums and differences of the same lot-scaled numbers, so the delta identity is
/// exact; this leaves room for rounding and nothing else.
pub(crate) const QTY_TOLERANCE: f64 = 1e-9;

/// Guards the delta semantics of [`Order::exec_qty`] in debug builds: it holds what each live order
/// had left when it last reported an execution, so the next execution can be checked against it.
///
/// That comparison is the only way to tell a delta from a cumulative quantity. A single response
/// cannot: a cumulative `exec_qty` equals `qty - leaves_qty` exactly, which is what a legitimate
/// first execution reports too. The distinction is temporal, so the check has to be as well.
///
/// **The owner must observe the order's whole life, not just its executions.** An amendment
/// re-rests the order with a new quantity, raising `leaves_qty` without executing anything, and a
/// resubmitted order id starts over — both leave the recorded quantity stale, and a check against a
/// stale quantity is a false alarm, not a finding. That is why this lives in the local processor,
/// which submits and amends, rather than in [`State`], which only ever sees executions: measured,
/// on a generated sequence that amended a partially filled order back up to its full quantity and
/// then executed it (`fill_accounting_property`).
#[cfg(debug_assertions)]
#[derive(Debug, Default)]
pub(crate) struct ExecutionLedger {
    /// What each order had left as of its last execution.
    left: HashMap<OrderId, f64>,
}

#[cfg(debug_assertions)]
impl ExecutionLedger {
    /// Records `order`'s execution and returns whether it lowered `leaves_qty` by exactly the
    /// `exec_qty` it reports — that is, whether `exec_qty` is this execution's own quantity.
    ///
    /// An order's first execution is recorded but not checked, there being nothing to check it
    /// against. That is also what keeps a taker order that walked several price levels out of this:
    /// the local sees a single response for it, carrying the last level's `exec_qty` against a
    /// `leaves_qty` that every level lowered (§4.6, a known and separate shortfall), and that
    /// response is always the order's first.
    pub(crate) fn record(&mut self, order: &Order) -> bool {
        match self.left.insert(order.order_id, order.leaves_qty) {
            Some(before) => {
                let executed = before - order.leaves_qty;
                (executed - order.exec_qty).abs() <= QTY_TOLERANCE * order.qty.abs().max(1.0)
            }
            None => true,
        }
    }

    /// Drops what was recorded for `order_id`. The next execution of that order then starts a new
    /// baseline instead of being checked against a quantity that no longer describes it.
    pub(crate) fn forget(&mut self, order_id: OrderId) {
        self.left.remove(&order_id);
    }
}

#[derive(Debug)]
pub struct State<AT, FM>
where
    AT: AssetType,
    FM: FeeModel,
{
    pub state_values: StateValues,
    pub asset_type: AT,
    pub fee_model: FM,
}

impl<AT, FM> State<AT, FM>
where
    AT: AssetType,
    FM: FeeModel,
{
    pub fn new(asset_type: AT, fee_model: FM) -> Self {
        Self {
            state_values: StateValues {
                position: 0.0,
                balance: 0.0,
                fee: 0.0,
                num_trades: 0,
                trading_volume: 0.0,
                trading_value: 0.0,
            },
            fee_model,
            asset_type,
        }
    }

    /// Applies to this state the execution that `order` reports.
    ///
    /// [`Order::exec_qty`] is the quantity executed by that one execution, not the order's
    /// cumulative executed quantity, so an order that fills in parts is accounted for by applying
    /// every one of its execution responses. The whole money path rests on that reading: a producer
    /// that starts reporting the cumulative quantity, or a consumer that applies an execution
    /// twice, inflates the position and fails nothing else (§4.6).
    ///
    /// What can be checked here is only what one response can say about itself. The delta itself is
    /// checked one level up, by the [`ExecutionLedger`] the local processor keeps, because telling
    /// a delta from a cumulative quantity takes the order's history and its amendments — neither of
    /// which this state sees. The identity over a whole sequence,
    /// `position == Σ(exec_qty × side)`, is a property test: `fill_accounting_property`.
    ///
    /// The direction comes in as a [`ResolvedSide`] rather than being read off `order.side`,
    /// which is a [`Side`](`crate::types::Side`) and may be one of the two values that have no
    /// sign. Those are ruled out where the order is submitted, and this signature is what makes
    /// that the only place they can be ruled out: an unresolved side cannot reach this
    /// arithmetic, so the panic that used to sit inside it is gone (invariant E2).
    #[inline]
    pub fn apply_fill(&mut self, order: &Order, side: ResolvedSide) {
        debug_assert!(
            order.exec_qty > 0.0
                && order.leaves_qty >= 0.0
                && order.exec_qty <= order.qty * (1.0 + QTY_TOLERANCE),
            "an execution reports a positive quantity that fits within the order: {order:?}"
        );

        let amount = self.asset_type.amount(order.exec_price(), order.exec_qty);
        self.state_values.position += order.exec_qty * side.sign();
        self.state_values.balance -= amount * side.sign();
        self.state_values.fee += self.fee_model.amount(order, amount);
        self.state_values.num_trades += 1;
        self.state_values.trading_volume += order.exec_qty;
        self.state_values.trading_value += amount;
    }

    #[inline]
    pub fn equity(&self, mid: f64) -> f64 {
        self.asset_type.equity(
            mid,
            self.state_values.balance,
            self.state_values.position,
            self.state_values.fee,
        )
    }

    #[inline]
    pub fn values(&self) -> &StateValues {
        &self.state_values
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        backtest::{
            assettype::LinearAsset,
            models::{CommonFees, TradingValueFeeModel},
            state::State,
        },
        types::{OrdType, Order, ResolvedSide, Side, Status, TimeInForce},
    };

    fn executed(side: Side, qty: f64) -> Order {
        let mut order = Order::new(1, 10_000, 0.01, qty, side, OrdType::Limit, TimeInForce::GTC);
        order.exec_qty = qty;
        order.exec_price_tick = 10_000;
        order.leaves_qty = 0.0;
        order.status = Status::Filled;
        order
    }

    /// **Position math takes a direction, and it is the one it was handed** (invariant E2).
    ///
    /// The signature is the guarantee: [`State::apply_fill`] cannot be called with a
    /// [`Side`](`crate::types::Side`) at all, so `Side::None` — which used to reach this
    /// arithmetic and abort the process from inside it — has nowhere to enter. Judging the side
    /// is the submit boundary's job, once, and this only spends the verdict.
    ///
    /// The order here carries a side that disagrees with the one supplied, which is not a case
    /// the engine produces; it is here because it is the only way to show the arithmetic reads
    /// the argument. A version that quietly went back to `order.side` would pass every other
    /// test in this crate.
    #[test]
    fn a_fill_moves_the_position_by_the_direction_it_is_given() {
        let fees = || TradingValueFeeModel::new(CommonFees::new(0.0, 0.0));

        let mut state = State::new(LinearAsset::new(1.0), fees());
        state.apply_fill(&executed(Side::Buy, 3.0), ResolvedSide::Buy);
        assert_eq!(state.values().position, 3.0);
        assert_eq!(state.values().balance, -300.0, "a buy pays");

        let mut state = State::new(LinearAsset::new(1.0), fees());
        state.apply_fill(&executed(Side::Buy, 3.0), ResolvedSide::Sell);
        assert_eq!(
            state.values().position,
            -3.0,
            "the direction supplied, not the one the order carries"
        );
        assert_eq!(state.values().balance, 300.0, "a sell is paid");
    }
}
