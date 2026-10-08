//! The slot's **send gate**: whether a transaction may leave for `sendTx` now (design note
//! [`docs/design-lighter-connector.md`](../../../docs/design-lighter-connector.md) §4.15).
//!
//! The venue limits `sendTx` per **L1 address**, not per bot and not per connection: a
//! Standard account gets 60 a rolling minute (§2.5), and the two bots this connector serves
//! trade one account. Going over is answered with HTTP 429/405 and a cooldown during which
//! nothing — cancels included — gets through. So the connector keeps its own count, below
//! the venue's, and spends it in an order that keeps the account able to get *out*:
//!
//! * a **budget** of `tx_budget_per_minute` sends in any rolling minute, the venue's own
//!   definition of its limit. Each send's token comes back one window after it was spent —
//!   not a continuously refilling bucket, which would admit up to twice its size inside one
//!   minute (a full bucket's burst plus a minute of refill) and walk straight into the venue's
//!   limit;
//! * of which `tx_reserve` only **risk-reducing** sends may take: cancels, `CancelAllOrders`,
//!   and a submit that shrinks the position. A burst of new quotes then cannot leave the
//!   account unable to cancel them.
//!
//! Pure state with the clock passed in, owned by the one slot task (single writer,
//! `AGENTS.md` §1.4), so all of it is unit-tested here without a socket.

use std::{collections::VecDeque, fmt, time::Duration};

use tokio::time::Instant;

/// The window the venue states its `sendTx` limit over (§2.5: "per rolling minute").
pub const WINDOW: Duration = Duration::from_secs(60);

/// How often a refusal is logged at most. The bot hears about every refused order (each one
/// comes back with its error); the log only needs to say that the gate is closed, and why.
pub const REFUSAL_LOG_EVERY: Duration = Duration::from_secs(10);

/// What a send is for — the only thing the gate asks about it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxClass {
    /// A new order that does not shrink the position: a quote.
    Open,
    /// A cancel, a `CancelAllOrders`, or a submit that shrinks |position| — what the reserve
    /// is held for.
    RiskReducing,
}

/// At most `per_minute` sends in any rolling [`WINDOW`], `reserve` of them for
/// [`TxClass::RiskReducing`] only.
#[derive(Debug)]
pub struct TxBudget {
    per_minute: u32,
    reserve: u32,
    /// When each send of the last window went out, oldest first. Never longer than
    /// `per_minute`.
    spent: VecDeque<Instant>,
}

impl TxBudget {
    pub fn new(per_minute: u32, reserve: u32) -> Self {
        Self {
            per_minute,
            reserve,
            spent: VecDeque::with_capacity(per_minute as usize),
        }
    }

    pub fn per_minute(&self) -> u32 {
        self.per_minute
    }

    pub fn reserve(&self) -> u32 {
        self.reserve
    }

    /// How many sends of the last window are still counted at `now`.
    pub fn used(&mut self, now: Instant) -> u32 {
        while let Some(&oldest) = self.spent.front() {
            if now.saturating_duration_since(oldest) >= WINDOW {
                self.spent.pop_front();
            } else {
                break;
            }
        }
        self.spent.len() as u32
    }

    /// The tokens a send of `class` may not take: the reserve, for a quote.
    fn floor(&self, class: TxClass) -> u32 {
        match class {
            TxClass::Open => self.reserve,
            TxClass::RiskReducing => 0,
        }
    }

    /// Takes a token for a send of `class` at `now`, or says when one is next free for it.
    pub fn try_spend(&mut self, now: Instant, class: TxClass) -> Result<(), Instant> {
        let used = self.used(now);
        let floor = self.floor(class);
        if used + floor < self.per_minute {
            self.spent.push_back(now);
            return Ok(());
        }
        // `class` needs the window down to `per_minute - floor - 1` sends: the oldest
        // `used - that` must age out first, and the last of them sets the time. A config that
        // leaves `class` nothing at all (`floor >= per_minute`, refused at startup) is never
        // free; one window on is as honest an answer as any.
        let may_stay = self.per_minute.checked_sub(floor + 1);
        let free_at = may_stay
            .and_then(|may_stay| self.spent.get((used - may_stay) as usize - 1))
            .map_or(now + WINDOW, |&sent| sent + WINDOW);
        Err(free_at)
    }
}

/// Why the gate held a send back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The budget has no token for this class until `free_at`.
    Budget {
        class: TxClass,
        per_minute: u32,
        reserve: u32,
        free_at: Instant,
    },
}

impl Refusal {
    /// When the gate would next admit the send it refused.
    pub fn free_at(&self) -> Instant {
        match self {
            Self::Budget { free_at, .. } => *free_at,
        }
    }
}

/// The bot-facing prose. It never contains "no live order": the myhft classifier reads that
/// as a benign race and would not count a refusal toward its rate-limit breaker.
impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Budget {
                class: TxClass::Open,
                per_minute,
                reserve,
                ..
            } => write!(
                f,
                "the connector's sendTx budget is spent ({per_minute} a rolling minute for the \
                 whole account, the last {reserve} held for cancels and position-reducing \
                 orders); rate-limited, not sent"
            ),
            Self::Budget {
                class: TxClass::RiskReducing,
                per_minute,
                ..
            } => write!(
                f,
                "the connector's sendTx budget is spent, its reserve included ({per_minute} a \
                 rolling minute for the whole account); rate-limited, not sent"
            ),
        }
    }
}

/// Says something at most once per period, and how many times it held its tongue meanwhile.
#[derive(Debug)]
pub struct LogThrottle {
    every: Duration,
    last: Option<Instant>,
    held_back: u64,
}

impl LogThrottle {
    pub fn new(every: Duration) -> Self {
        Self {
            every,
            last: None,
            held_back: 0,
        }
    }

    /// `Some(held_back)` when a line may be logged at `now` — with how many were held back
    /// since the last one — and `None` when it must be held back.
    pub fn admit(&mut self, now: Instant) -> Option<u64> {
        match self.last {
            Some(last) if now.saturating_duration_since(last) < self.every => {
                self.held_back += 1;
                None
            }
            _ => {
                self.last = Some(now);
                Some(std::mem::take(&mut self.held_back))
            }
        }
    }
}

/// The gate the slot asks before every `sendTx`.
#[derive(Debug)]
pub struct SendGate {
    budget: Option<TxBudget>,
    refusals: LogThrottle,
}

impl SendGate {
    /// `budget: None` admits everything — the behaviour without `tx_budget_per_minute`.
    pub fn new(budget: Option<TxBudget>) -> Self {
        Self {
            budget,
            refusals: LogThrottle::new(REFUSAL_LOG_EVERY),
        }
    }

    /// Admits a send of `class` at `now` — and counts it — or says why not.
    pub fn admit(&mut self, now: Instant, class: TxClass) -> Result<(), Refusal> {
        let Some(budget) = &mut self.budget else {
            return Ok(());
        };
        budget
            .try_spend(now, class)
            .map_err(|free_at| Refusal::Budget {
                class,
                per_minute: budget.per_minute(),
                reserve: budget.reserve(),
                free_at,
            })
    }

    /// Sends counted in the window at `now`, out of how many — `None` without a budget.
    pub fn usage(&mut self, now: Instant) -> Option<(u32, u32)> {
        self.budget
            .as_mut()
            .map(|budget| (budget.used(now), budget.per_minute()))
    }

    /// Whether a refusal may be logged at `now` (see [`LogThrottle::admit`]).
    pub fn log_refusal(&mut self, now: Instant) -> Option<u64> {
        self.refusals.admit(now)
    }
}

/// Waits until `gate` admits a `class` send — and counts it — as long as that happens by
/// `deadline`; otherwise the refusal that outlasts it, at once and without waiting for nothing.
///
/// What a `CancelAllOrders` sweep does with a closed gate. A refused quote or cancel goes back
/// to the bot, who decides; a sweep is answering for orders nobody is managing any more (a dead
/// bot's, or a stopping connector's), so it is held until the gate opens — but never past the
/// point where whoever waits for its verdict has given up.
pub async fn admit_by(
    gate: &mut SendGate,
    class: TxClass,
    deadline: Instant,
) -> Result<(), Refusal> {
    loop {
        match gate.admit(Instant::now(), class) {
            Ok(()) => return Ok(()),
            Err(refusal) if refusal.free_at() <= deadline => {
                tokio::time::sleep_until(refusal.free_at()).await;
            }
            Err(refusal) => return Err(refusal),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::{LogThrottle, Refusal, SendGate, TxBudget, TxClass, WINDOW, admit_by};

    /// **No more than the budget in ANY rolling minute — the venue's own definition.** A
    /// continuously refilling bucket of the same size passes the first half of this and fails
    /// the second: full at t0, it lets the whole budget through at once and then refills, so
    /// inside one minute it admits close to twice its size — 96 against a venue that answers
    /// the 61st with a 429 and a cooldown.
    #[test]
    fn a_budget_admits_at_most_its_size_in_any_rolling_minute() {
        let t0 = Instant::now();
        let mut budget = TxBudget::new(48, 0);
        for i in 0..48 {
            assert!(budget.try_spend(t0, TxClass::Open).is_ok(), "send {i}");
        }
        assert!(budget.try_spend(t0, TxClass::Open).is_err(), "the 49th");
        assert!(
            budget
                .try_spend(t0 + WINDOW - Duration::from_millis(1), TxClass::Open)
                .is_err(),
            "still inside the minute the 48 went out in"
        );
        assert!(budget.try_spend(t0 + WINDOW, TxClass::Open).is_ok());

        // Spread out, the same holds: a token comes back one window after it went, not sooner.
        let mut budget = TxBudget::new(2, 0);
        assert!(budget.try_spend(t0, TxClass::Open).is_ok());
        assert!(
            budget
                .try_spend(t0 + Duration::from_secs(30), TxClass::Open)
                .is_ok()
        );
        assert!(
            budget
                .try_spend(t0 + Duration::from_secs(59), TxClass::Open)
                .is_err(),
            "two in the last 59 s"
        );
        assert!(
            budget
                .try_spend(t0 + Duration::from_secs(60), TxClass::Open)
                .is_ok(),
            "the first came back"
        );
        assert!(
            budget
                .try_spend(t0 + Duration::from_secs(61), TxClass::Open)
                .is_err(),
            "the second is still out until t0 + 90 s"
        );
    }

    /// **The reserve is for getting out, not for quoting.** With 48 and a reserve of 12, quotes
    /// stop at 36 and cancels, sweeps and position-reducing submits still have 12.
    #[test]
    fn the_reserve_is_only_for_risk_reducing_sends() {
        let t0 = Instant::now();
        let mut budget = TxBudget::new(48, 12);
        for i in 0..36 {
            assert!(budget.try_spend(t0, TxClass::Open).is_ok(), "quote {i}");
        }
        assert!(
            budget.try_spend(t0, TxClass::Open).is_err(),
            "the 37th quote would eat the reserve"
        );
        for i in 0..12 {
            assert!(
                budget.try_spend(t0, TxClass::RiskReducing).is_ok(),
                "cancel {i}"
            );
        }
        assert!(
            budget.try_spend(t0, TxClass::RiskReducing).is_err(),
            "and the reserve is finite"
        );
        assert_eq!(budget.used(t0), 48);
    }

    /// A refusal says when **this** class gets a token back: a cancel as soon as any send of
    /// the window ages out, a quote only once enough have to clear the reserve.
    #[test]
    fn a_refusal_says_when_this_class_gets_a_token_back() {
        let t0 = Instant::now();
        let second = Duration::from_secs(1);
        let mut budget = TxBudget::new(3, 1);
        budget.try_spend(t0, TxClass::Open).unwrap();
        budget.try_spend(t0 + second, TxClass::Open).unwrap();
        // One token left, and it is the reserve.
        assert_eq!(
            budget.try_spend(t0 + 2 * second, TxClass::Open),
            Err(t0 + WINDOW),
            "a quote waits for the oldest send to age out"
        );
        budget
            .try_spend(t0 + 2 * second, TxClass::RiskReducing)
            .unwrap();
        assert_eq!(
            budget.try_spend(t0 + 3 * second, TxClass::RiskReducing),
            Err(t0 + WINDOW),
            "a cancel, too, waits for the first send to age out"
        );
        assert_eq!(
            budget.try_spend(t0 + 3 * second, TxClass::Open),
            Err(t0 + second + WINDOW),
            "a quote needs two to age out: one for itself, one to refill the reserve"
        );
    }

    /// Without `tx_budget_per_minute` the gate admits everything, as the order path did
    /// before it had one.
    #[test]
    fn without_a_budget_the_gate_admits_everything() {
        let t0 = Instant::now();
        let mut gate = SendGate::new(None);
        for _ in 0..1_000 {
            assert!(gate.admit(t0, TxClass::Open).is_ok());
        }
        assert_eq!(gate.usage(t0), None);
    }

    /// The gate's refusal names the budget, and never wears the words the bot's classifier
    /// reads as a benign race.
    #[test]
    fn a_refused_send_is_named_a_rate_limit_and_never_a_missing_order() {
        let t0 = Instant::now();
        let mut gate = SendGate::new(Some(TxBudget::new(1, 0)));
        gate.admit(t0, TxClass::Open).unwrap();
        for class in [TxClass::Open, TxClass::RiskReducing] {
            let refusal = gate.admit(t0, class).expect_err("spent");
            assert!(matches!(refusal, Refusal::Budget { .. }));
            let prose = refusal.to_string();
            assert!(prose.contains("rate-limited"), "{prose}");
            assert!(prose.contains("not sent"), "{prose}");
            assert!(!prose.contains("no live order"), "{prose}");
        }
    }

    /// The refusal log speaks at most once a period and says how much it held back.
    #[test]
    fn a_throttled_log_speaks_at_most_once_a_period_and_counts_the_rest() {
        let t0 = Instant::now();
        let mut log = LogThrottle::new(Duration::from_secs(10));
        assert_eq!(log.admit(t0), Some(0), "the first one speaks");
        assert_eq!(log.admit(t0 + Duration::from_secs(1)), None);
        assert_eq!(log.admit(t0 + Duration::from_secs(9)), None);
        assert_eq!(
            log.admit(t0 + Duration::from_secs(10)),
            Some(2),
            "and the next names the two it held back"
        );
        assert_eq!(log.admit(t0 + Duration::from_secs(11)), None);
    }

    /// **A sweep waits for the gate when it opens in time, and fails at once when it does not.**
    /// Spent through, the next token comes back one window after the first send: a deadline
    /// past that is waited out (and the send counted); one short of it is refused without
    /// sleeping toward a moment that will not come.
    #[tokio::test(start_paused = true)]
    async fn a_sweep_waits_for_the_gate_only_when_it_opens_before_the_deadline() {
        let t0 = Instant::now();
        let mut gate = SendGate::new(Some(TxBudget::new(1, 0)));
        gate.admit(t0, TxClass::Open).unwrap();

        let short = admit_by(
            &mut gate,
            TxClass::RiskReducing,
            t0 + WINDOW - Duration::from_secs(1),
        )
        .await;
        assert!(matches!(short, Err(Refusal::Budget { .. })), "{short:?}");
        assert_eq!(Instant::now(), t0, "and it did not wait to find out");

        admit_by(&mut gate, TxClass::RiskReducing, t0 + WINDOW)
            .await
            .expect("the token is back by the deadline");
        assert_eq!(Instant::now(), t0 + WINDOW, "waited exactly until it was");
        assert!(
            gate.admit(Instant::now(), TxClass::RiskReducing).is_err(),
            "and the send it waited for was counted"
        );
    }
}
