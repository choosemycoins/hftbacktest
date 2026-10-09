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
//!   and an IOC (or reduce-only) submit that shrinks |position| — an order that gets the
//!   account out the moment it lands. A post-only quote on the reducing side is a quote: it
//!   may rest unfilled as long as any other, and three of them once took the whole reserve
//!   between them (2026-10-08 probe). A burst of new quotes then cannot leave the account
//!   unable to cancel them;
//! * and when the venue answers 429/405 anyway, a **cooldown** of `rate_limit_cooldown_ms`
//!   during which nothing is sent at all (§4.16): the venue's firewall holds a rate-limited
//!   address for a static 60 s, and every send into that only earns another refusal;
//! * except, with `rate_limit_cancel_probe_ms`, a risk-reducing send **probing** it: the
//!   first one probe interval in, each refused probe doubling the wait, none lengthening the
//!   cooldown ([`Admission::Probe`]). An API server's cooldown is seconds, not the firewall's
//!   minute, and this side cannot tell which one answered; a cancel that waits the minute out
//!   for the shorter kind leaves a stale quote resting for nothing. Quotes never probe.
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
    /// A new order that is not a way out: every quote — a post-only one on the reducing side
    /// included — and any order that could add to |position|.
    Open,
    /// A cancel, a `CancelAllOrders`, or an IOC (or reduce-only) submit that shrinks
    /// |position| (`OrderManager::reduces_position`) — what the reserve is held for.
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

    /// When a send of `class` next gets a token — `None` when one is free at `now`. Counts
    /// nothing.
    pub fn free_at(&mut self, now: Instant, class: TxClass) -> Option<Instant> {
        let used = self.used(now);
        let floor = self.floor(class);
        if used + floor < self.per_minute {
            return None;
        }
        // `class` needs the window down to `per_minute - floor - 1` sends: the oldest
        // `used - that` must age out first, and the last of them sets the time. A config that
        // leaves `class` nothing at all (`floor >= per_minute`, refused at startup) is never
        // free; one window on is as honest an answer as any.
        let may_stay = self.per_minute.checked_sub(floor + 1);
        Some(
            may_stay
                .and_then(|may_stay| self.spent.get((used - may_stay) as usize - 1))
                .map_or(now + WINDOW, |&sent| sent + WINDOW),
        )
    }

    /// Takes a token for a send of `class` at `now`, or says when one is next free for it.
    pub fn try_spend(&mut self, now: Instant, class: TxClass) -> Result<(), Instant> {
        match self.free_at(now, class) {
            None => {
                self.spent.push_back(now);
                Ok(())
            }
            Some(free_at) => Err(free_at),
        }
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
    /// The venue rate-limited the account (429/405): the cooldown runs until `until`, and the
    /// refused send's class may next try at `retry_at` — the end of it for a quote, the next
    /// probe for a risk-reducing send when probes are configured (§4.16).
    Cooldown { until: Instant, retry_at: Instant },
}

impl Refusal {
    /// When the gate would next admit the send it refused.
    pub fn free_at(&self) -> Instant {
        match self {
            Self::Budget { free_at, .. } => *free_at,
            Self::Cooldown { retry_at, .. } => *retry_at,
        }
    }
}

/// How a send the gate let through is to be read when its answer comes back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    /// An ordinary send: a 429/405 answering it starts the cooldown.
    Send,
    /// A risk-reducing send let through **inside** a cooldown (`rate_limit_cancel_probe_ms`,
    /// §4.16): a 429/405 answering it — or a nonce that cannot be read for it — backs the probes
    /// off and leaves the cooldown, and the bots, as they were ([`SendGate::probe_refused`]).
    Probe,
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
                 IOCs); rate-limited, not sent"
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
            Self::Cooldown { .. } => write!(
                f,
                "the venue rate-limited this account's sendTx (HTTP 429/405) and the connector \
                 sends nothing until its cooldown is over; rate-limited, not sent"
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

/// A cooldown in progress.
#[derive(Clone, Copy, Debug)]
struct Cooling {
    /// When it ends for everything.
    until: Instant,
    /// When a risk-reducing send may next try, if probes are configured.
    next_probe: Option<Instant>,
    /// How long the probes wait after the next one is refused; doubles with every refusal.
    probe_backoff: Duration,
}

/// The gate the slot asks before every `sendTx`.
#[derive(Debug)]
pub struct SendGate {
    budget: Option<TxBudget>,
    /// `[order_path] rate_limit_cooldown_ms`. `None`: a 429/405 starts nothing, and the slot
    /// treats it as the ambiguous answer it was before it had a name.
    cooldown: Option<Duration>,
    /// `[order_path] rate_limit_cancel_probe_ms`: how soon into a cooldown a risk-reducing send
    /// may try — the first wait of the probes' back-off. `None`: they wait the cooldown out.
    probe: Option<Duration>,
    /// The running cooldown. Set by [`Self::rate_limited`].
    cooling: Option<Cooling>,
    refusals: LogThrottle,
    near_limit: LogThrottle,
}

impl SendGate {
    /// `budget: None` and `cooldown: None` admit everything — the behaviour without
    /// `tx_budget_per_minute` and `rate_limit_cooldown_ms`. `probe: None` holds every send for
    /// the whole cooldown — the behaviour without `rate_limit_cancel_probe_ms`.
    pub fn new(
        budget: Option<TxBudget>,
        cooldown: Option<Duration>,
        probe: Option<Duration>,
    ) -> Self {
        Self {
            budget,
            cooldown,
            probe,
            cooling: None,
            refusals: LogThrottle::new(REFUSAL_LOG_EVERY),
            near_limit: LogThrottle::new(REFUSAL_LOG_EVERY),
        }
    }

    /// The venue answered an ordinary send at `now` with 429/405: nothing goes out for the
    /// configured cooldown, and — with probes configured — a risk-reducing send may first try
    /// again one probe interval from now. A second one inside it extends it; nothing shortens
    /// it. Returns when the cooldown ends — `None` when none is configured, and then nothing
    /// changed.
    pub fn rate_limited(&mut self, now: Instant) -> Option<Instant> {
        let until = now + self.cooldown?;
        let until = self
            .cooling
            .map_or(until, |running| running.until.max(until));
        self.cooling = Some(Cooling {
            until,
            next_probe: self.probe.map(|probe| now + probe),
            probe_backoff: self.probe.unwrap_or_default() * 2,
        });
        Some(until)
    }

    /// A probe let through at [`Admission::Probe`] was refused at `now` — answered 429/405, or
    /// it could not even be signed for want of a nonce. The cooldown stands **as it was**: the
    /// venue's firewall holds an address for a static minute (its docs), so a probe into it
    /// buys nothing but another refusal and must not buy a longer cooldown for the quotes. The
    /// next probe waits twice as long as the last. Returns when it may go — `None` when no
    /// cooldown is running at `now` any more, and then nothing changed.
    pub fn probe_refused(&mut self, now: Instant) -> Option<Instant> {
        let cooling = self
            .cooling
            .as_mut()
            .filter(|cooling| cooling.until > now)?;
        cooling.next_probe = Some(now + cooling.probe_backoff);
        cooling.probe_backoff *= 2;
        cooling.next_probe.map(|at| at.min(cooling.until))
    }

    /// When the cooldown running at `now` ends, if one is.
    pub fn cooling(&self, now: Instant) -> Option<Instant> {
        self.cooling
            .map(|cooling| cooling.until)
            .filter(|&until| until > now)
    }

    /// When the cooldown running at `now` next lets a `class` send through, if it does not now:
    /// a quote at its end, a risk-reducing send at its next probe (at its end without probes).
    pub fn cooling_for(&self, now: Instant, class: TxClass) -> Option<Instant> {
        let cooling = self.cooling.filter(|cooling| cooling.until > now)?;
        match (class, cooling.next_probe) {
            (TxClass::RiskReducing, Some(probe)) if probe <= now => None,
            (TxClass::RiskReducing, Some(probe)) => Some(probe.min(cooling.until)),
            _ => Some(cooling.until),
        }
    }

    /// When a `class` send would next be admitted — the later of what the cooldown and the
    /// budget say — or `None` when [`Self::admit`] would let it through at `now`. Counts nothing.
    ///
    /// What a held cancel waits on (§4.16). A probe that is due is no use to a cancel the budget
    /// has no token for — with probes configured, the probes themselves spend it — and a release
    /// that asked only the cooldown met the shut budget again on every tick.
    pub fn next_admission(&mut self, now: Instant, class: TxClass) -> Option<Instant> {
        let cooling = self.cooling_for(now, class);
        let budget = self
            .budget
            .as_mut()
            .and_then(|budget| budget.free_at(now, class));
        // `None` (free now) sorts below any `Some`, so this is the later of two times, and
        // `None` only when neither holds the send back.
        cooling.max(budget)
    }

    /// Whether three quarters of the budget are spent at `now` — the point the venue's own
    /// rate-limit report is worth a warning. Never, without a budget.
    pub fn near_limit(&mut self, now: Instant) -> bool {
        self.budget
            .as_mut()
            .is_some_and(|budget| budget.used(now) * 4 >= budget.per_minute() * 3)
    }

    /// Admits a send of `class` at `now` — and counts it — or says why not. A cooldown is
    /// asked about first, and a send it holds back spends no token; a risk-reducing send it lets
    /// through as a probe spends one like any other.
    pub fn admit(&mut self, now: Instant, class: TxClass) -> Result<Admission, Refusal> {
        let admission = match self.cooling(now) {
            None => Admission::Send,
            Some(until) => match self.cooling_for(now, class) {
                None => Admission::Probe,
                Some(retry_at) => return Err(Refusal::Cooldown { until, retry_at }),
            },
        };
        let Some(budget) = &mut self.budget else {
            return Ok(admission);
        };
        budget
            .try_spend(now, class)
            .map(|()| admission)
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

    /// Whether a near-the-limit warning may be logged at `now` (see [`LogThrottle::admit`]).
    pub fn log_near_limit(&mut self, now: Instant) -> Option<u64> {
        self.near_limit.admit(now)
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
) -> Result<Admission, Refusal> {
    loop {
        match gate.admit(Instant::now(), class) {
            Ok(admission) => return Ok(admission),
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

    use super::{Admission, LogThrottle, Refusal, SendGate, TxBudget, TxClass, WINDOW, admit_by};

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
    /// stop at 36 and cancels, sweeps and position-reducing IOCs still have 12.
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
        let mut gate = SendGate::new(None, None, None);
        for _ in 0..1_000 {
            assert!(gate.admit(t0, TxClass::Open).is_ok());
        }
        assert_eq!(gate.usage(t0), None);
    }

    /// The gate's refusal names the budget or the cooldown, and never wears the words the
    /// bot's classifier reads as a benign race.
    #[test]
    fn a_refused_send_is_named_a_rate_limit_and_never_a_missing_order() {
        let t0 = Instant::now();
        let mut gate = SendGate::new(Some(TxBudget::new(1, 0)), None, None);
        gate.admit(t0, TxClass::Open).unwrap();
        let mut cooling = SendGate::new(None, Some(Duration::from_secs(60)), None);
        cooling.rate_limited(t0);
        for class in [TxClass::Open, TxClass::RiskReducing] {
            let refusal = gate.admit(t0, class).expect_err("spent");
            assert!(matches!(refusal, Refusal::Budget { .. }));
            let cooled = cooling.admit(t0, class).expect_err("cooling down");
            assert!(matches!(cooled, Refusal::Cooldown { .. }));
            for prose in [refusal.to_string(), cooled.to_string()] {
                assert!(prose.contains("rate-limited"), "{prose}");
                assert!(prose.contains("not sent"), "{prose}");
                assert!(!prose.contains("no live order"), "{prose}");
            }
        }
    }

    /// **A 429/405 shuts the gate for the cooldown: nothing goes out — cancels and sweeps
    /// included — and a send held back spends no token.** The docs' firewall cooldown is 60 s
    /// "static" and sending into it buys nothing but another refusal (§4.16). It opens on the
    /// dot.
    #[test]
    fn a_cooldown_holds_every_send_until_it_ends() {
        let t0 = Instant::now();
        let cooldown = Duration::from_secs(60);
        let mut gate = SendGate::new(Some(TxBudget::new(2, 1)), Some(cooldown), None);
        assert_eq!(gate.rate_limited(t0), Some(t0 + cooldown));
        let late = t0 + cooldown - Duration::from_millis(1);
        for class in [TxClass::Open, TxClass::RiskReducing] {
            assert_eq!(
                gate.admit(late, class),
                Err(Refusal::Cooldown {
                    until: t0 + cooldown,
                    retry_at: t0 + cooldown,
                }),
                "{class:?}: without probes everything waits the cooldown out"
            );
        }
        assert_eq!(gate.cooling(late), Some(t0 + cooldown));
        assert_eq!(gate.usage(late), Some((0, 2)), "nothing was counted");
        assert_eq!(
            gate.admit(t0 + cooldown, TxClass::Open),
            Ok(Admission::Send)
        );
        assert_eq!(gate.cooling(t0 + cooldown), None);
    }

    /// A second rate limit inside the cooldown extends it, and an earlier one can never cut
    /// a running cooldown short.
    #[test]
    fn a_rate_limit_inside_the_cooldown_extends_it_and_never_shortens_it() {
        let t0 = Instant::now();
        let cooldown = Duration::from_secs(60);
        let mut gate = SendGate::new(None, Some(cooldown), None);
        gate.rate_limited(t0 + Duration::from_secs(10));
        assert_eq!(
            gate.rate_limited(t0),
            Some(t0 + Duration::from_secs(70)),
            "the later end stands"
        );
        assert_eq!(
            gate.rate_limited(t0 + Duration::from_secs(30)),
            Some(t0 + Duration::from_secs(90))
        );
    }

    /// Without `rate_limit_cooldown_ms` a 429 starts nothing — the caller then hands it on as
    /// the ambiguous answer it was before `RateLimited` existed.
    #[test]
    fn without_a_cooldown_a_rate_limit_starts_nothing() {
        let t0 = Instant::now();
        let mut gate = SendGate::new(Some(TxBudget::new(48, 12)), None, None);
        assert_eq!(gate.rate_limited(t0), None);
        assert_eq!(gate.cooling(t0), None);
        assert!(gate.admit(t0, TxClass::Open).is_ok());
    }

    /// **"Near the limit" is three quarters of the budget spent** — the point the venue's own
    /// rate-limit report is worth a warning. For 48 with 12 held back that is where quotes
    /// start being refused. Without a budget nothing is near anything.
    #[test]
    fn the_account_is_near_its_limit_at_three_quarters_of_the_budget() {
        let t0 = Instant::now();
        let mut gate = SendGate::new(Some(TxBudget::new(48, 12)), None, None);
        for _ in 0..35 {
            gate.admit(t0, TxClass::Open).unwrap();
        }
        assert!(!gate.near_limit(t0), "35 of 48");
        gate.admit(t0, TxClass::Open).unwrap();
        assert!(gate.near_limit(t0), "36 of 48");
        assert!(
            !gate.near_limit(t0 + WINDOW),
            "and not once the window has moved on"
        );
        assert!(!SendGate::new(None, None, None).near_limit(t0));
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
        let mut gate = SendGate::new(Some(TxBudget::new(1, 0)), None, None);
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

    /// **With probes, a risk-reducing send tries before the cooldown is over; a quote never
    /// does.** The docs give two cooldowns and this side cannot tell which one answered: the
    /// firewall's static 60 s, or an API server's `weight / (total / 60)` — seconds. A cancel
    /// that waits the full minute for the second kind leaves a stale quote resting for a minute
    /// it did not have to (§4.16). So a cancel, a sweep or a reducing IOC may probe one interval
    /// in; quotes — the sends a cooldown exists to stop — still wait it out. A probe spends a
    /// token like any other send.
    #[test]
    fn a_risk_reducing_send_probes_the_cooldown_and_a_quote_waits_it_out() {
        let t0 = Instant::now();
        let second = Duration::from_secs(1);
        let mut gate = SendGate::new(
            Some(TxBudget::new(48, 12)),
            Some(60 * second),
            Some(5 * second),
        );
        gate.rate_limited(t0);

        let early = t0 + 5 * second - Duration::from_millis(1);
        assert_eq!(
            gate.admit(early, TxClass::RiskReducing),
            Err(Refusal::Cooldown {
                until: t0 + 60 * second,
                retry_at: t0 + 5 * second,
            }),
            "not before the first probe is due"
        );
        assert_eq!(
            gate.admit(t0 + 5 * second, TxClass::RiskReducing),
            Ok(Admission::Probe)
        );
        assert_eq!(
            gate.admit(t0 + 5 * second, TxClass::Open),
            Err(Refusal::Cooldown {
                until: t0 + 60 * second,
                retry_at: t0 + 60 * second,
            }),
            "a quote waits the whole cooldown"
        );
        assert_eq!(gate.cooling(t0 + 5 * second), Some(t0 + 60 * second));
        assert_eq!(
            gate.usage(t0 + 5 * second),
            Some((1, 48)),
            "the probe was counted, the refused quote was not"
        );
    }

    /// **A refused probe backs the probes off and leaves the cooldown exactly as it was.** The
    /// firewall's minute is static (docs), so a probe into it buys only another refusal — it
    /// must not buy the quotes a longer cooldown too. The waits double after each refusal (5 s,
    /// then 10, 20, 40), and the end of the cooldown caps them: from then on everything goes.
    #[test]
    fn a_refused_probe_backs_off_and_leaves_the_cooldown_as_it_was() {
        let t0 = Instant::now();
        let second = Duration::from_secs(1);
        let mut gate = SendGate::new(None, Some(60 * second), Some(5 * second));
        gate.rate_limited(t0);

        assert_eq!(gate.probe_refused(t0 + 5 * second), Some(t0 + 15 * second));
        assert_eq!(
            gate.cooling_for(t0 + 14 * second, TxClass::RiskReducing),
            Some(t0 + 15 * second)
        );
        assert_eq!(
            gate.cooling_for(t0 + 15 * second, TxClass::RiskReducing),
            None
        );
        assert_eq!(gate.probe_refused(t0 + 15 * second), Some(t0 + 35 * second));
        assert_eq!(
            gate.probe_refused(t0 + 35 * second),
            Some(t0 + 60 * second),
            "the next would be at 75 s; the cooldown is over at 60"
        );
        assert_eq!(
            gate.cooling(t0 + 35 * second),
            Some(t0 + 60 * second),
            "three refused probes and the cooldown ends where it always did"
        );
        assert_eq!(
            gate.admit(t0 + 60 * second, TxClass::Open),
            Ok(Admission::Send)
        );
        assert_eq!(
            gate.probe_refused(t0 + 60 * second),
            None,
            "no cooldown running: nothing to back off"
        );
    }

    /// **When a send would next go out counts the budget as well as the cooldown, and asking
    /// counts nothing.** It is what a held cancel waits on (§4.16): a probe that is due is no use
    /// to a cancel the budget has no token for — with probes, the probes themselves spend it —
    /// and a release that only asked the cooldown found the budget shut on every tick.
    #[test]
    fn the_next_admission_is_the_later_of_the_cooldown_and_the_budget() {
        let t0 = Instant::now();
        let second = Duration::from_secs(1);
        let mut gate = SendGate::new(Some(TxBudget::new(2, 0)), Some(10 * second), Some(second));
        assert_eq!(gate.next_admission(t0, TxClass::RiskReducing), None);
        gate.admit(t0, TxClass::RiskReducing).unwrap();
        gate.admit(t0, TxClass::RiskReducing).unwrap();
        assert_eq!(
            gate.next_admission(t0, TxClass::RiskReducing),
            Some(t0 + WINDOW),
            "spent through: when the first token comes back"
        );

        gate.rate_limited(t0 + 2 * second);
        assert_eq!(
            gate.next_admission(t0 + 4 * second, TxClass::RiskReducing),
            Some(t0 + WINDOW),
            "the probe is due, the budget is not: the budget decides"
        );
        assert_eq!(
            gate.next_admission(t0 + 4 * second, TxClass::Open),
            Some(t0 + WINDOW),
            "and for a quote the later of its cooldown and the budget"
        );

        let mut gate = SendGate::new(Some(TxBudget::new(2, 1)), Some(10 * second), Some(second));
        gate.rate_limited(t0);
        assert_eq!(
            gate.next_admission(t0, TxClass::RiskReducing),
            Some(t0 + second),
            "a token is there, the probe is not due: the probe decides"
        );
        assert_eq!(
            gate.next_admission(t0 + second, TxClass::RiskReducing),
            None
        );
        assert_eq!(
            gate.next_admission(t0 + second, TxClass::Open),
            Some(t0 + 10 * second),
            "a quote waits the cooldown out"
        );
        assert_eq!(
            gate.usage(t0 + second),
            Some((0, 2)),
            "asking spent nothing"
        );
        assert_eq!(
            gate.admit(t0 + second, TxClass::RiskReducing),
            Ok(Admission::Probe),
            "and what it said is what admit does"
        );
    }

    /// Without `rate_limit_cancel_probe_ms` nothing probes: a cooldown holds every class to its
    /// end, as before probes existed.
    #[test]
    fn without_a_probe_every_send_waits_the_cooldown_out() {
        let t0 = Instant::now();
        let mut gate = SendGate::new(None, Some(Duration::from_secs(60)), None);
        gate.rate_limited(t0);
        assert_eq!(
            gate.cooling_for(t0 + Duration::from_secs(59), TxClass::RiskReducing),
            Some(t0 + Duration::from_secs(60))
        );
    }
}
