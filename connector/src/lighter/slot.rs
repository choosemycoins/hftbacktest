//! The slot owner's **decisions**, factored out of the async task so the money-path policy
//! is unit-tested without a socket, a signer subprocess or a clock (design note
//! [`docs/design-lighter-connector.md`](../../../docs/design-lighter-connector.md) §3.3,
//! §4.11).
//!
//! A slot owns one api-key's outgoing queue: reserve a nonce, sign, `sendTx`, then decide two
//! things from the response.
//!
//! 1. **What the outcome means for the nonce** ([`nonce_action`]). A clean 200 spent it
//!    (advance); anything else leaves its fate unknown, so the slot hard-refreshes from
//!    `nextNonce` rather than guess the counter forward (§3.3, fail closed).
//! 2. **Whether an order actually resulted** ([`placement_after_deadline`]) — the §4.11
//!    centrepiece. A 200 is *not* that answer: acceptance is a private-channel fact. So the
//!    slot arms a confirmation deadline, and if the order has not appeared on
//!    `account_all_orders` by then it asks `GET /tx` for the verdict in `event_info.ae`. The
//!    duplicate-`ClientOrderIndex` transaction is the reproducible instance: HTTP 200, nonce
//!    spent, no order, and the only legible rejection is `ae = 21728`. The connector must
//!    then expire the order and report the error to the bot — never leave it believing a
//!    one-sided quote is resting (`AGENTS.md` §1.1).

use std::time::{Duration, Instant};

use tracing::warn;

use crate::lighter::{
    private_msg::AccountOrder,
    rest::{SendOutcome, TxVerdict},
};

/// What a `sendTx` outcome means for the slot's nonce counter (§3.3, §2.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonceAction {
    /// The sequencer consumed the nonce (HTTP 200, whatever the order's fate — §4.11). Advance
    /// the counter by one.
    Consumed,
    /// The fate is unknown or the counter drifted (a `21104`, a 5xx, an unreadable body).
    /// Invalidate and hard-refresh from `nextNonce` — never guess forward (§3.3).
    Resync,
    /// The fate is unknown and the venue is refusing the account (a 429/405 under a cooldown,
    /// §4.16): invalidate, but do **not** ask `nextNonce` now — that is another request into
    /// the refusal. The next `reserve` re-reads it, and the cooldown keeps the next send until
    /// the venue answers again.
    Invalidate,
}

/// The nonce action a `sendTx` outcome calls for.
pub fn nonce_action(outcome: &SendOutcome) -> NonceAction {
    match outcome {
        SendOutcome::Accepted(_) => NonceAction::Consumed,
        SendOutcome::InvalidNonce { .. } | SendOutcome::Ambiguous { .. } => NonceAction::Resync,
        SendOutcome::RateLimited { .. } => NonceAction::Invalidate,
    }
}

/// What a cancel's `sendTx` outcome leaves for the bot (§3.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelFollowUp {
    /// HTTP 200: the venue took the transaction; the channel's `canceled` (or a fill) closes
    /// the order. Nothing to tell the bot yet.
    AwaitChannel,
    /// The cancel did not take, or may not have: clear the order's `req = Canceled` and
    /// report why, so the bot can cancel again instead of waiting for an answer that will
    /// never come.
    HandBack(String),
    /// Rate-limited (429/405 under a cooldown, §4.16): it may or may not have taken, and a
    /// cancel sent again now would only be refused. Keep the order's `req = Canceled` and send
    /// it again once the cooldown is over — unless the channel has closed the order by then.
    AfterCooldown,
}

/// The follow-up a cancel's `sendTx` outcome calls for.
///
/// A `21104` means the cancel was rejected on nonce grounds — it did not take. An ambiguous
/// outcome (a 429/405 throttle, a 5xx, a 200 without a hash) may or may not have taken; the
/// order stays tracked either way, so handing the cancel back is safe in both cases: if it
/// took, the channel's `canceled` removes the order; if not, the bot's next cancel is the
/// retry. Leaving `req = Canceled` set — what the slot did before — left the quote standing on
/// the venue with the bot believing a cancel was in flight.
pub fn cancel_after_send(outcome: &SendOutcome) -> CancelFollowUp {
    match outcome {
        SendOutcome::Accepted(_) => CancelFollowUp::AwaitChannel,
        SendOutcome::InvalidNonce { .. } => CancelFollowUp::HandBack(
            "the venue rejected the cancel's nonce (21104); the cancel did not take".to_string(),
        ),
        SendOutcome::Ambiguous { status, detail } => CancelFollowUp::HandBack(format!(
            "the cancel's sendTx was ambiguous (HTTP {status}: {detail}); it may not have \
             taken — cancel again"
        )),
        SendOutcome::RateLimited { .. } => CancelFollowUp::AfterCooldown,
    }
}

/// How many times one market's `CancelAllOrders` is sent before the sweep is declared failed.
pub const SWEEP_ATTEMPTS: u32 = 3;

/// The wait before the second attempt; the third waits twice as long.
pub const SWEEP_RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// What one `CancelAllOrders` attempt leaves for the slot (§3.4).
///
/// A sweep is what stands between a dead or stopping bot and its quotes trading on
/// unattended — on this venue orders outlive the process (§2.7). It used to be sent once and
/// logged "Sent" whatever came back, so a `21104` or a 429 — the answer most likely exactly
/// when a rate-limit storm is what killed the bot — left the market's orders resting under a
/// line that said they had been swept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SweepFollowUp {
    /// HTTP 200: the venue took the sweep; its `canceled` deltas arrive on the channel.
    Done,
    /// It did not take (`21104`), or may not have (ambiguous, no answer, not even built):
    /// send it again after `after`. A sweep is idempotent — it cancels whatever rests on the
    /// market when it executes — so a second one behind a first that did take costs one
    /// `sendTx` and nothing else.
    Retry { after: Duration },
    /// Out of attempts: the sweep FAILED and orders may still rest on that market.
    Failed(String),
}

/// The follow-up to a sweep's `attempt`-th try (1-based). `outcome` is the venue's answer,
/// or — `Err` — why there was none: the nonce could not be seeded, the signer refused, the
/// transport failed.
pub fn sweep_follow_up(outcome: Result<&SendOutcome, &str>, attempt: u32) -> SweepFollowUp {
    let (reason, after) = match outcome {
        Ok(SendOutcome::Accepted(_)) => return SweepFollowUp::Done,
        // Refused before it executed, and `apply_nonce` has already resynced the counter:
        // nothing to wait for.
        Ok(SendOutcome::InvalidNonce { message }) => (
            format!("the venue rejected the sweep's nonce (21104: {message})"),
            Duration::ZERO,
        ),
        Ok(SendOutcome::Ambiguous { status, detail }) => (
            format!("the sweep's sendTx was ambiguous (HTTP {status}: {detail})"),
            SWEEP_RETRY_BACKOFF * attempt,
        ),
        // The cooldown it started is the wait: the next attempt's admission holds it until the
        // gate opens, or ends the sweep if that is past its deadline.
        Ok(SendOutcome::RateLimited { status, detail }) => (
            format!("the sweep's sendTx was rate-limited (HTTP {status}: {detail})"),
            Duration::ZERO,
        ),
        Err(reason) => (reason.to_string(), SWEEP_RETRY_BACKOFF * attempt),
    };
    if attempt >= SWEEP_ATTEMPTS {
        return SweepFollowUp::Failed(format!(
            "{reason}; gave up after {attempt} attempts — orders may still be resting on this \
             market"
        ));
    }
    SweepFollowUp::Retry { after }
}

/// One reserve-sign-send round of a `CancelAllOrders` sweep: the slot's, or a scripted venue
/// in a test. `Err` is why nothing was sent or nothing came back.
pub trait SweepSend {
    /// Waits until the send gate admits this sweep's next attempt (`crate::lighter::gate`), or
    /// says why it will not before the sweep's deadline — a reason that ends the sweep unsent.
    fn admit(&mut self) -> impl Future<Output = Result<(), String>> + Send;
    fn send_once(&mut self) -> impl Future<Output = Result<SendOutcome, String>> + Send;
}

/// Sends one market's sweep until it takes or [`SWEEP_ATTEMPTS`] are spent, as
/// [`sweep_follow_up`] decides. `Ok` carries the attempts it took; `Err` is the failure, to
/// be reported loudly by the caller.
///
/// Every attempt is admitted by the send gate first ([`SweepSend::admit`]). One the gate will
/// not admit before the sweep's deadline ends the sweep there, unsent: waiting longer would
/// answer a waiter who has already given up, and retrying sooner would only be refused again.
pub async fn sweep_with_retries(sender: &mut impl SweepSend) -> Result<u32, String> {
    let mut attempt = 1;
    loop {
        if let Err(reason) = sender.admit().await {
            return Err(format!(
                "{reason}; attempt {attempt} not sent — orders may still be resting on this market"
            ));
        }
        let outcome = sender.send_once().await;
        match sweep_follow_up(outcome.as_ref().map_err(String::as_str), attempt) {
            SweepFollowUp::Done => return Ok(attempt),
            SweepFollowUp::Retry { after } => {
                warn!(
                    attempt,
                    ?after,
                    outcome = ?outcome,
                    "A Lighter CancelAllOrders sweep did not take; sending it again."
                );
                tokio::time::sleep(after).await;
                attempt += 1;
            }
            SweepFollowUp::Failed(reason) => return Err(reason),
        }
    }
}

/// What became of an order whose confirmation deadline has now been examined (§4.11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    /// The order appeared on `account_all_orders` — it is resting, and the order manager has
    /// already published it. Nothing more to do.
    Confirmed,
    /// The order will never exist: `event_info.ae` carried an application error (e.g. the
    /// duplicate-COI `21728`). Expire the order back to the bot and report the error (§4.11).
    Rejected { code: i64, message: String },
    /// Still unknown — the verdict is `Accepted` (accepted by the app but not yet seen, i.e.
    /// network lag) or `Pending`. Keep waiting within the budget; do **not** expire, and above
    /// all do not silently treat it as live.
    Unresolved,
}

/// Decides an order's fate once its confirmation deadline is up.
///
/// `confirmed` is whether `account_all_orders` has listed the order (the acceptance signal,
/// §4.11); `verdict` is what `GET /tx` said about `event_info.ae`. A confirmed order is
/// `Confirmed` regardless of anything else — the private channel is the truth. Only an
/// unconfirmed order with an explicit application error in `ae` is `Rejected`; everything else
/// stays `Unresolved` rather than risk a false expire or a false accept.
pub fn placement_after_deadline(confirmed: bool, verdict: &TxVerdict) -> Placement {
    if confirmed {
        return Placement::Confirmed;
    }
    match verdict {
        TxVerdict::Rejected { code, message } => Placement::Rejected {
            code: *code,
            message: message.clone(),
        },
        TxVerdict::Accepted | TxVerdict::Pending => Placement::Unresolved,
    }
}

/// What an **ambiguous** `sendTx` outcome (a 5xx / 408 / unreadable-200, or a reqwest transport
/// error — the 10 s `ORDER_REST_TIMEOUT` firing after the sequencer may already have processed
/// the tx) resolves to once the slot has checked the order against the venue's authoritative
/// open-order list (§3.3). An ambiguous send left the order's fate **unknown** — it may already
/// rest on the venue — so it must never be expired blind: that orphans a resting order into a
/// silent one-sided quote (`AGENTS.md` §1.1). The slot hard-refreshes its nonce and reads the
/// fate off `accountActiveOrders`, exactly what §3.3 prescribes ("check the order fate via
/// `account_all_orders` before building the next tx").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmbiguousFate {
    /// The order is on the venue's active-order list — it **landed**. Adopt it (feed the frame
    /// through the order manager as the private channel would); do **not** re-send it, so the
    /// §5 gate's "reconcile WITHOUT a duplicate" holds.
    Landed,
    /// The order is **absent** from a list the venue positively returned — it never landed.
    /// Expire it back to the bot so the id is freed and the bot re-quotes.
    DidNotLand,
    /// The check itself could not run (the auth mint, the token, or the GET failed). Fail
    /// closed: keep the order tracked as unconfirmed and let the confirmation deadline /
    /// reconnect reconciliation resolve it — **never** expire (would orphan a possibly-resting
    /// order) and never assume live.
    Unknown,
}

/// Reads an ambiguous order's fate out of the venue's active-order list (§3.3). `active` is the
/// `accountActiveOrders` result — `Ok(list)` when the venue answered (even if empty), `Err(())`
/// when the check could not be performed. Presence of our `coi` is the only signal; an empty or
/// COI-absent list that the venue positively returned means it never landed.
pub fn fate_of_ambiguous(coi: i64, active: Result<&[AccountOrder], ()>) -> AmbiguousFate {
    match active {
        Ok(orders) if orders.iter().any(|order| order.client_order_index == coi) => {
            AmbiguousFate::Landed
        }
        Ok(_) => AmbiguousFate::DidNotLand,
        Err(()) => AmbiguousFate::Unknown,
    }
}

/// How the confirmation deadline resolves an order once its wait is up (§4.11, §3.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadlineAction {
    /// The order is confirmed on the channel, or no longer tracked — stop waiting.
    Drop,
    /// Unconfirmed, and the send returned a `tx_hash` (a clean HTTP 200): ask `GET /tx` for the
    /// `event_info.ae` verdict — the §4.11 duplicate-COI path.
    ViaTxVerdict,
    /// Unconfirmed, and the send returned **no** `tx_hash` (an ambiguous / transport-failed
    /// send that was kept tracked rather than orphaned): there is nothing to `GET /tx`, so the
    /// fate is read off `accountActiveOrders` instead (§3.3).
    ViaFateCheck,
}

/// Decides how a lapsed confirmation deadline resolves, from whether the order is still tracked
/// and unconfirmed ([`crate::lighter::order::OrderManager::is_confirmed`]) and whether its send
/// produced a `tx_hash`. A confirmed order (`Some(true)`) or one already gone (`None`) is
/// dropped; an unconfirmed one splits on the `tx_hash`, which is the only thing distinguishing a
/// clean-200 send (has a hash → `GET /tx`) from an ambiguous one (no hash → fate check).
pub fn deadline_action(confirmed: Option<bool>, has_tx_hash: bool) -> DeadlineAction {
    match confirmed {
        Some(true) | None => DeadlineAction::Drop,
        Some(false) if has_tx_hash => DeadlineAction::ViaTxVerdict,
        Some(false) => DeadlineAction::ViaFateCheck,
    }
}

/// The clock marks one placement collects on its way out, opened where the connector first
/// sees the order and carried to the slot on its command.
///
/// **Why the outbound path is instrumented at all.** MEASURED on the Lighter testnet
/// 2026-08-09 over 60_719 order responses: the venue answers in a median 135 ms, but an order
/// takes a median 913 ms to reach it — all of the latency is outbound, and the connector could
/// not say which of its own stages spent it. On that path sit a queue shared with cancels, a
/// nonce that may need a REST refresh, and a Python signer sidecar in its own process. Whether
/// those 913 ms are ours or the venue's decides whether a signal with a 450–800 ms lead can be
/// traded here at all, so it is worth one log line per placement.
///
/// The spans it yields are all differences of marks taken on **one** clock. That matters:
/// entry latency as measured downstream is `venue stamp − our stamp`, so any host/venue clock
/// offset moves silently between the two legs. These do not have that weakness.
pub struct Outbound {
    /// `Order::local_timestamp` — stamped in the **bot** process before iceoryx (nanoseconds).
    pub requested_ns: i64,
    /// `Utc::now()` when the connector took the order (nanoseconds). Same host and same clock
    /// as `requested_ns`, so their difference is the hand-off and nothing else.
    pub received_ns: i64,
    /// When the order was queued for the slot task.
    pub queued: Instant,
}

/// Where one placement's outbound microseconds went. Every field is a span, not a timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboundSpans {
    /// Bot stamp → connector: the iceoryx hand-off. `-1` when the bot did not stamp the
    /// request (see [`Outbound::spans`]).
    pub ipc_us: i64,
    /// Connector → slot task: the head-of-line wait behind whatever the slot was already
    /// sending. Submits and cancels share one queue and each `.await`s a full REST round trip,
    /// so a burst of requotes shows up here and nowhere else.
    pub queue_us: i64,
    /// Reserving the nonce. Normally in-memory and ~0; an unseeded slot spends a `GET
    /// /nextNonce` round trip here rather than inflating `sign_us` with it.
    pub nonce_us: i64,
    /// The signer sidecar round trip — a line to its stdin, a `ctypes` call into the Go
    /// c-archive, a line back. Local: the sidecar never talks to the venue.
    pub sign_us: i64,
    /// `sendTx`: connection setup, the POST, and the sequencer's answer.
    pub send_us: i64,
}

impl Outbound {
    /// Charges each span to the stage that spent it.
    ///
    /// `ipc_us` is `-1` — an impossible span, so unmistakable — when `requested_ns` is not a
    /// positive stamp. `LiveBot::submit_order` always sets one, but a `0` from any other
    /// producer would contribute ~1.8e15 us to a percentile and quietly ruin the measurement
    /// this exists for.
    pub fn spans(
        &self,
        dequeued: Instant,
        reserved: Instant,
        signed: Instant,
        sent: Instant,
    ) -> OutboundSpans {
        OutboundSpans {
            ipc_us: if self.requested_ns > 0 {
                (self.received_ns - self.requested_ns) / 1_000
            } else {
                -1
            },
            queue_us: (dequeued - self.queued).as_micros() as i64,
            nonce_us: (reserved - dequeued).as_micros() as i64,
            sign_us: (signed - reserved).as_micros() as i64,
            send_us: (sent - signed).as_micros() as i64,
        }
    }
}

/// Test doubles for the sweep, shared by this module's tests and `order_path`'s.
#[cfg(test)]
pub(crate) mod testing {
    use std::collections::VecDeque;

    use super::SweepSend;
    use crate::lighter::rest::SendOutcome;

    /// A venue that answers each sweep attempt from a script, in order, and counts the sends.
    /// Its gate admits every attempt unless `admissions` says otherwise, one entry an attempt.
    pub(crate) struct ScriptedVenue {
        pub(crate) answers: VecDeque<Result<SendOutcome, String>>,
        pub(crate) admissions: VecDeque<Result<(), String>>,
        pub(crate) sends: u32,
    }

    impl ScriptedVenue {
        pub(crate) fn new(answers: Vec<Result<SendOutcome, String>>) -> Self {
            Self {
                answers: answers.into(),
                admissions: VecDeque::new(),
                sends: 0,
            }
        }
    }

    impl SweepSend for ScriptedVenue {
        async fn admit(&mut self) -> Result<(), String> {
            self.admissions.pop_front().unwrap_or(Ok(()))
        }

        async fn send_once(&mut self) -> Result<SendOutcome, String> {
            self.sends += 1;
            self.answers
                .pop_front()
                .unwrap_or_else(|| Err("the script ran out".to_string()))
        }
    }

    /// The venue's throttle answer to a `sendTx`.
    pub(crate) fn throttled() -> SendOutcome {
        SendOutcome::Ambiguous {
            status: 429,
            detail: "Too Many Requests".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{
        AmbiguousFate,
        CancelFollowUp,
        DeadlineAction,
        NonceAction,
        Outbound,
        OutboundSpans,
        Placement,
        SWEEP_ATTEMPTS,
        SweepFollowUp,
        cancel_after_send,
        deadline_action,
        fate_of_ambiguous,
        nonce_action,
        placement_after_deadline,
        sweep_follow_up,
        sweep_with_retries,
    };
    use crate::lighter::{
        private_msg::AccountOrder,
        rest::{SendOutcome, SendTxAck, TxVerdict},
        slot::testing::{ScriptedVenue, throttled},
    };

    fn ack() -> SendTxAck {
        SendTxAck {
            tx_hash: "ab".to_string(),
            ratelimit: Some("Ratelimit is off".to_string()),
            predicted_execution_time_ms: None,
        }
    }

    // ---- the CancelAllOrders sweep ----------------------------------------------------

    /// A sweep the venue took is done, on whichever attempt it took.
    #[test]
    fn a_sweep_the_venue_took_is_done() {
        for attempt in 1..=SWEEP_ATTEMPTS {
            assert_eq!(
                sweep_follow_up(Ok(&SendOutcome::Accepted(ack())), attempt),
                SweepFollowUp::Done
            );
        }
    }

    /// **A sweep that did not take is sent again, and then declared failed — never "sent".**
    /// A `21104` was refused before it executed and the nonce is already resynced, so it goes
    /// again at once; a 429, any other ambiguous answer, and no answer at all wait 1 s then
    /// 2 s. The last attempt's failure names what came back and that orders may be resting.
    #[test]
    fn a_sweep_that_did_not_take_is_retried_then_declared_failed() {
        assert_eq!(
            sweep_follow_up(
                Ok(&SendOutcome::InvalidNonce {
                    message: "invalid nonce".to_string()
                }),
                1
            ),
            SweepFollowUp::Retry {
                after: Duration::ZERO
            }
        );
        assert_eq!(
            sweep_follow_up(Ok(&throttled()), 1),
            SweepFollowUp::Retry {
                after: Duration::from_secs(1)
            }
        );
        assert_eq!(
            sweep_follow_up(Err("the cancel-all sendTx failed: timed out"), 2),
            SweepFollowUp::Retry {
                after: Duration::from_secs(2)
            }
        );
        let SweepFollowUp::Failed(reason) = sweep_follow_up(Ok(&throttled()), SWEEP_ATTEMPTS)
        else {
            panic!("the last attempt's failure is a failed sweep");
        };
        assert!(reason.contains("429"), "{reason}");
        assert!(reason.contains("resting"), "{reason}");
        let SweepFollowUp::Failed(reason) =
            sweep_follow_up(Err("couldn't seed the nonce"), SWEEP_ATTEMPTS)
        else {
            panic!("no answer on the last attempt is a failed sweep");
        };
        assert!(reason.contains("couldn't seed the nonce"), "{reason}");
    }

    /// The driver, against a scripted venue: two 429s and then a 200 is a sweep that took on
    /// the third attempt, after the 1 s and 2 s waits.
    #[tokio::test(start_paused = true)]
    async fn a_throttled_sweep_takes_on_a_later_attempt() {
        let mut venue = ScriptedVenue::new(vec![
            Ok(throttled()),
            Ok(throttled()),
            Ok(SendOutcome::Accepted(ack())),
        ]);
        let started = tokio::time::Instant::now();
        let result = sweep_with_retries(&mut venue).await;
        assert_eq!(result, Ok(3));
        assert_eq!(venue.sends, 3);
        assert!(
            started.elapsed() >= Duration::from_secs(3),
            "waited 1 s + 2 s"
        );
    }

    /// **An attempt the send gate will not admit before the sweep's deadline ends the sweep,
    /// unsent** — at once, not after the retries' waits, and naming why. Here the first attempt
    /// is throttled and retried, and the gate then stays shut past the deadline.
    #[tokio::test(start_paused = true)]
    async fn a_sweep_the_gate_will_not_admit_in_time_fails_unsent() {
        let mut venue = ScriptedVenue::new(vec![Ok(throttled()), Ok(throttled())]);
        venue.admissions = vec![
            Ok(()),
            Err("the send gate is shut past the sweep's deadline".to_string()),
        ]
        .into();
        let result = sweep_with_retries(&mut venue).await;
        assert_eq!(
            venue.sends, 1,
            "the refused attempt never reached the venue"
        );
        let Err(reason) = result else {
            panic!("a sweep that could not be sent has failed");
        };
        assert!(reason.contains("send gate"), "{reason}");
        assert!(reason.contains("not sent"), "{reason}");
        assert!(reason.contains("resting"), "{reason}");
    }

    /// ...and a venue that never takes it is a failed sweep after exactly
    /// [`SWEEP_ATTEMPTS`] sends, not a silent success and not a loop.
    #[tokio::test(start_paused = true)]
    async fn a_sweep_the_venue_never_takes_fails_after_the_last_attempt() {
        let mut venue = ScriptedVenue::new((0..10).map(|_| Ok(throttled())).collect());
        let result = sweep_with_retries(&mut venue).await;
        assert_eq!(venue.sends, SWEEP_ATTEMPTS);
        let Err(reason) = result else {
            panic!("a sweep that never took must fail");
        };
        assert!(reason.contains("429"), "{reason}");
    }

    /// **A cancel the venue did not take is handed back to the bot.** Before, a cancel that got
    /// a `21104` or an ambiguous answer (a 429 among them) only resynced the nonce: the order
    /// kept `req = Canceled`, the bot believed a cancel was in flight and never sent another,
    /// and the quote stood on the venue. A clean 200 waits for the channel's `canceled`; the
    /// other two clear the request, so the bot can cancel again — which is safe even when an
    /// ambiguous cancel did take (the channel's `canceled` then removes the order, and a
    /// second cancel of a gone order is refused harmlessly).
    #[test]
    fn a_cancel_that_did_not_take_is_handed_back() {
        assert_eq!(
            cancel_after_send(&SendOutcome::Accepted(ack())),
            CancelFollowUp::AwaitChannel
        );
        for outcome in [
            SendOutcome::InvalidNonce {
                message: "invalid nonce".to_string(),
            },
            SendOutcome::Ambiguous {
                status: 429,
                detail: "Too Many Requests".to_string(),
            },
            SendOutcome::Ambiguous {
                status: 200,
                detail: "no tx_hash".to_string(),
            },
        ] {
            let CancelFollowUp::HandBack(reason) = cancel_after_send(&outcome) else {
                panic!("{outcome:?} must hand the cancel back");
            };
            assert!(reason.contains("cancel"), "{reason}");
            assert!(!reason.contains("no live order"), "{reason}");
        }
    }

    /// **A 429/405 leaves the nonce untrusted but unread**: whether the tx was processed is not
    /// known, so the counter cannot be advanced — and `nextNonce` is a request to the venue
    /// that is refusing this account, so it is not asked now. `reserve` re-reads it before the
    /// next send, which the cooldown keeps until the venue will answer (§4.16).
    #[test]
    fn a_rate_limited_send_invalidates_the_nonce_without_reading_it() {
        assert_eq!(
            nonce_action(&SendOutcome::RateLimited {
                status: 429,
                detail: "Too Many Requests".to_string()
            }),
            NonceAction::Invalidate
        );
    }

    /// **A rate-limited cancel waits out the cooldown rather than going back to the bot.**
    /// Handed back, the bot would cancel again into the same cooldown — each refusal another
    /// error toward its storm breaker — while the order stood; held, it goes out the moment the
    /// venue will take it, even if the bot has died meanwhile (§4.16).
    #[test]
    fn a_rate_limited_cancel_waits_for_the_cooldown() {
        assert_eq!(
            cancel_after_send(&SendOutcome::RateLimited {
                status: 405,
                detail: String::new()
            }),
            CancelFollowUp::AfterCooldown
        );
    }

    /// A rate-limited sweep attempt is tried again with no wait of its own: the cooldown it
    /// started is the wait, and the send gate decides whether it ends before the sweep's
    /// deadline. The last attempt's failure still names the status.
    #[test]
    fn a_rate_limited_sweep_attempt_leaves_the_wait_to_the_gate() {
        let limited = SendOutcome::RateLimited {
            status: 429,
            detail: "Too Many Requests".to_string(),
        };
        assert_eq!(
            sweep_follow_up(Ok(&limited), 1),
            SweepFollowUp::Retry {
                after: Duration::ZERO
            }
        );
        let SweepFollowUp::Failed(reason) = sweep_follow_up(Ok(&limited), SWEEP_ATTEMPTS) else {
            panic!("the last attempt's failure is a failed sweep");
        };
        assert!(reason.contains("429"), "{reason}");
        assert!(reason.contains("rate-limited"), "{reason}");
    }

    /// A clean 200 spends the nonce; a `21104` and any ambiguous outcome resync it — the slot
    /// never advances the counter on an uncertain send (§3.3).
    #[test]
    fn the_nonce_advances_only_on_a_clean_two_hundred() {
        assert_eq!(
            nonce_action(&SendOutcome::Accepted(ack())),
            NonceAction::Consumed
        );
        assert_eq!(
            nonce_action(&SendOutcome::InvalidNonce {
                message: "invalid nonce".to_string()
            }),
            NonceAction::Resync
        );
        assert_eq!(
            nonce_action(&SendOutcome::Ambiguous {
                status: 503,
                detail: "upstream".to_string()
            }),
            NonceAction::Resync
        );
    }

    /// **The §4.11 gate, socketless: a duplicate-COI order gets a 200 (nonce spent), never
    /// appears, and `GET /tx` returns `ae = 21728` → the placement is `Rejected`, so the
    /// connector expires it and reports — it does not silently believe it is live.**
    ///
    /// This is the exact chain the Phase-0 artifacts recorded (step5 B): the 200 is
    /// [`SendOutcome::Accepted`], and the verdict that the private channel never contradicted
    /// is the rejection in `event_info.ae`.
    #[test]
    fn a_duplicate_coi_that_got_two_hundred_but_never_appeared_is_rejected_not_live() {
        let verdict = TxVerdict::Rejected {
            code: 21728,
            message: "client order index already exists".to_string(),
        };
        match placement_after_deadline(false, &verdict) {
            Placement::Rejected { code, message } => {
                assert_eq!(code, 21728);
                assert!(message.contains("client order index"));
            }
            other => panic!("a rejected-ae order must be Rejected, got {other:?}"),
        }
    }

    /// A confirmed order is `Confirmed` no matter what `GET /tx` says — the private channel is
    /// the source of truth, and a race where the verdict lags the confirmation must not expire
    /// a resting order.
    #[test]
    fn a_confirmed_order_is_confirmed_regardless_of_the_verdict() {
        assert_eq!(
            placement_after_deadline(true, &TxVerdict::Pending),
            Placement::Confirmed
        );
        assert_eq!(
            placement_after_deadline(
                true,
                &TxVerdict::Rejected {
                    code: 21728,
                    message: "stale".to_string()
                }
            ),
            Placement::Confirmed
        );
    }

    /// An unconfirmed order whose verdict is not an explicit rejection stays `Unresolved` — a
    /// deadline that lapsed on network lag must neither expire a possibly-live order nor
    /// declare it live. Fail closed both ways.
    #[test]
    fn an_unconfirmed_order_without_a_rejection_stays_unresolved() {
        assert_eq!(
            placement_after_deadline(false, &TxVerdict::Pending),
            Placement::Unresolved
        );
        assert_eq!(
            placement_after_deadline(false, &TxVerdict::Accepted),
            Placement::Unresolved
        );
    }

    fn active(coi: i64) -> AccountOrder {
        AccountOrder {
            market_index: 1,
            client_order_index: coi,
            order_index: 844424914280000 + coi,
            is_ask: false,
            status: "open".to_string(),
            price: 58300.0,
            initial_base_amount: 0.001,
            remaining_base_amount: 0.001,
            filled_base_amount: 0.0,
            transaction_time_us: 1785431774184833,
        }
    }

    /// **The §3.3 ambiguity resolution, socketless: an order whose `sendTx` was ambiguous (a
    /// 5xx / unreadable 200 / transport error) has its fate read off the venue's authoritative
    /// open-order list, never guessed.** Present ⇒ it landed and is adopted (so it is never
    /// re-sent — the §5 gate's "reconcile WITHOUT a duplicate"); absent ⇒ it never landed and is
    /// expired; a check that could not run is `Unknown` and must NOT expire (that would orphan a
    /// possibly-resting order into a silent one-sided quote, §1.1 — the bug this path fixes).
    #[test]
    fn an_ambiguous_send_is_resolved_against_the_active_order_list_not_guessed() {
        // Our COI is on the venue's list → it landed. Adopt it; do not re-send.
        let landed = vec![active(900_000_000_002), active(424242424242)];
        assert_eq!(
            fate_of_ambiguous(424242424242, Ok(&landed)),
            AmbiguousFate::Landed
        );

        // The list holds other orders but not ours → it never landed. Expire it.
        assert_eq!(
            fate_of_ambiguous(555, Ok(&landed)),
            AmbiguousFate::DidNotLand
        );

        // A flat account (empty list) → absent → it never landed.
        assert_eq!(fate_of_ambiguous(555, Ok(&[])), AmbiguousFate::DidNotLand);

        // The check itself could not run (mint/token/transport failed) → Unknown. NEVER expire:
        // keep the order tracked as unconfirmed and let the deadline / reconnect resolve it.
        assert_eq!(fate_of_ambiguous(555, Err(())), AmbiguousFate::Unknown);
    }

    /// **The confirmation deadline dispatches on whether a `tx_hash` exists** (§4.11 vs §3.3). A
    /// confirmed order (or one no longer tracked) is dropped; an unconfirmed one with a
    /// `tx_hash` (a clean HTTP-200 send) asks `GET /tx` for the `event_info.ae` verdict; an
    /// unconfirmed one with NO `tx_hash` (an ambiguous / transport-failed send left it tracked
    /// but unarmed) is resolved against `accountActiveOrders` — the only way to learn the fate
    /// of a send that produced no hash.
    #[test]
    fn the_deadline_dispatches_on_whether_a_tx_hash_exists() {
        assert_eq!(deadline_action(Some(true), true), DeadlineAction::Drop);
        assert_eq!(deadline_action(Some(true), false), DeadlineAction::Drop);
        assert_eq!(deadline_action(None, false), DeadlineAction::Drop);
        assert_eq!(
            deadline_action(Some(false), true),
            DeadlineAction::ViaTxVerdict
        );
        assert_eq!(
            deadline_action(Some(false), false),
            DeadlineAction::ViaFateCheck
        );
    }

    /// **Each span is charged to the stage that actually spent it.**
    ///
    /// This is the whole value of the instrumentation, so it is the thing pinned. MEASURED on
    /// the Lighter testnet 2026-08-09, n = 60_719: the outbound leg costs a median 913 ms and
    /// the return leg 135 ms, and nothing in the connector could say which of its own stages
    /// spent the 913. Two candidate answers lead to opposite decisions — if it is the venue,
    /// Lighter cannot carry a signal whose lead is 450–800 ms; if it is our own queue or our
    /// own Python signer sidecar, it is ours to fix. An off-by-one mark that charged the queue
    /// wait to signing (or to the venue) would answer the question backwards while looking
    /// perfectly plausible.
    ///
    /// The stages are laid out with distinct durations so no two can be swapped undetected.
    #[test]
    fn every_outbound_span_is_charged_to_the_stage_that_spent_it() {
        let queued = Instant::now();
        let marks = Outbound {
            // The bot stamped it 3_000 us before the connector picked it up.
            requested_ns: 1_000_000_000,
            received_ns: 1_003_000_000,
            queued,
        };

        let spans = marks.spans(
            queued + Duration::from_micros(70_000), // dequeued: 70 ms behind the queue
            queued + Duration::from_micros(75_000), // reserved: 5 ms on the nonce
            queued + Duration::from_micros(79_000), // signed:   4 ms in the sidecar
            queued + Duration::from_micros(529_000), // sent:     450 ms on the wire
        );

        assert_eq!(
            spans,
            OutboundSpans {
                ipc_us: 3_000,
                queue_us: 70_000,
                nonce_us: 5_000,
                sign_us: 4_000,
                send_us: 450_000,
            }
        );
    }

    /// **A request the bot never stamped is reported as unknown, not as a 56-year hand-off.**
    /// `Order::local_timestamp` is always set by `LiveBot::submit_order`, but a `0` from any
    /// other producer would otherwise contribute ~1.8e15 us to a percentile and quietly ruin
    /// the very measurement this exists for. The other four spans are local and stay real.
    #[test]
    fn an_unstamped_request_reports_an_unknown_hand_off_rather_than_a_nonsense_one() {
        let queued = Instant::now();
        let marks = Outbound {
            requested_ns: 0,
            received_ns: 1_786_302_295_222_600_652,
            queued,
        };

        let spans = marks.spans(
            queued + Duration::from_micros(1),
            queued + Duration::from_micros(2),
            queued + Duration::from_micros(3),
            queued + Duration::from_micros(4),
        );

        assert_eq!(spans.ipc_us, -1, "the sentinel, not a 56-year hand-off");
        assert_eq!(spans.send_us, 1, "the local spans are unaffected");
    }
}
