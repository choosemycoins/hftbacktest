//! What the venue acknowledged, what it refused, and what to do about it.
//!
//! # The failure this exists for, and the diagnosis that was wrong first
//!
//! Twice in seven days a Lighter reconnect left part of the roster unserved on
//! a socket that stayed perfectly healthy: 2026-08-12, ten markets of
//! twenty-three silent for 5.185h after the 00:40:48Z reconnect; 2026-08-18,
//! thirteen of twenty-three for 1.485h. The markets the venue *did* serve kept
//! [`IDLE_TIMEOUT`](super::http::IDLE_TIMEOUT) fed, so nothing detected it, and
//! both holes were closed only by the next reconnect, which happened for an
//! unrelated reason.
//!
//! The first reading of that was "the venue drops a subscribe and says
//! nothing". **It is wrong, and the recording says so.** The venue answers
//! every refused subscribe with `{"error":{"code":30009,"message":"Too Many
//! Websocket Messages!"}}` — 41 of them on 12.08, 53 on 18.08, one per refused
//! frame, arriving in bursts of eight 246ms apart, which is exactly this
//! collector's own [`SUBSCRIBE_CHUNK`](super::http::SUBSCRIBE_CHUNK) shape
//! coming back at it. The refusals begin **1.01 seconds after `connected` in
//! both incidents, after exactly 38 accepted frames**.
//!
//! So the cause is not silence, it is **rate limiting of our own subscribe
//! burst**, and that inverts the cure. Answering it with more messages — a
//! resend, or worse a reconnect, which puts the whole 92-frame set back into
//! the same limiter — is how a five-hour hole becomes a permanent one. Two
//! rules follow, and they are the reason this module exists rather than a
//! flag in the connection loop:
//!
//! 1. **A rate-limited connection is never abandoned.** Dropping it is the one
//!    action guaranteed to re-trigger the limiter.
//! 2. **A resend goes out at the sustained rate**, not at the burst rate that
//!    caused the refusal.
//!
//! The ledger is still needed alongside the error, because the error does not
//! say *which* channel it refused — `30009` carries no channel at all. The
//! error gives the cause in one second; the ledger gives the identity.

use std::{collections::HashSet, time::Duration};

use serde_json::Value;

/// How long after the last subscribe frame every subscription must be acked.
///
/// A minute against a measured acknowledgement latency of 267ms is a margin of
/// 225×. It is that wide for a second reason: a full set is the venue's whole
/// per-minute subscribe budget, so a resend may not share a minute with the set
/// it repeats (`a_resend_cannot_exceed_the_subscribe_budget`).
pub const ACK_GRACE: Duration = Duration::from_secs(60);

/// How many connections may be abandoned for a set the venue silently ignores.
///
/// Spent, not free. Dropping the connection is the right cure for a batch the
/// venue lost, and the wrong one for a market it will never serve — there the
/// ledger never closes and the collector would reconnect for ever, costing
/// every other market its stream every two minutes to repair one that cannot be
/// repaired. Recording twenty-two markets of twenty-three with a loud alarm
/// beats recording none of them quietly.
///
/// The budget is **consecutive**: a connection that reaches a complete ledger
/// gives it back (see [`Subscriptions::is_complete`] and its caller). A running
/// total would mean that three separate, successfully repaired episodes over a
/// month of uptime leave the fourth one with no repair at all.
pub const MAX_UNACKED_ABANDONS: u32 = 3;

/// The frames the venue admitted before it began refusing, in both incidents.
///
/// Measured, not documented: 38 accepted then refusals from the 39th, at
/// +1.01s, on 2026-08-12 and 2026-08-18 alike. Recorded here because it is the
/// only number that says what the burst may actually be, and because the
/// venue's published figure (200 client messages a minute) does not describe
/// it — 92 frames is well inside 200, and was refused anyway.
pub const ADMITTED_BEFORE_REFUSAL: usize = 38;

/// What one received frame told us.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observed {
    /// `subscribed/<channel>` — this channel is live.
    Acknowledged(String),
    /// `30003 Already Subscribed` — the venue holds it, whatever we think.
    AlreadyHeld(String),
    /// `30009 Too Many Websocket Messages` — our own burst was refused.
    Refused,
    /// Anything else, which is almost every frame.
    Nothing,
}

/// What to do about an incomplete ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Complete, still going out, or the grace has not run out.
    Wait,
    /// Ask again for these, and see [`Subscriptions::throttled`] for the pace.
    Resend(Vec<Value>),
    /// Report the loss and keep recording what is served.
    Report(Vec<String>),
    /// The venue is ignoring us without saying so; drop the connection.
    Abandon(Vec<String>),
}

/// The response spelling of a request channel: `order_book/0` -> `order_book:0`.
///
/// The two differ in one separator and the venue is strict about which belongs
/// where. The mapping lives here only, so "what was asked for" and "what is
/// waited for" cannot drift apart.
fn response_spelling(request: &str) -> String {
    request.replacen('/', ":", 1)
}

/// One connection's subscribe ledger.
pub struct Subscriptions {
    catalogue: Vec<Value>,
    awaiting: HashSet<String>,
    throttled: bool,
    resends: u32,
}

impl Subscriptions {
    /// Built from the subscribe frames themselves, so the ledger is by
    /// construction exactly the set that goes on the wire.
    pub fn new(frames: &[Value]) -> Self {
        Self {
            awaiting: frames
                .iter()
                .filter_map(|frame| frame.get("channel").and_then(|c| c.as_str()))
                .map(response_spelling)
                .collect(),
            catalogue: frames.to_vec(),
            throttled: false,
            resends: 0,
        }
    }

    pub fn is_complete(&self) -> bool {
        self.awaiting.is_empty()
    }

    /// True once the venue has refused a frame for being too many.
    pub fn throttled(&self) -> bool {
        self.throttled
    }

    pub fn outstanding(&self) -> Vec<String> {
        let mut out: Vec<String> = self.awaiting.iter().cloned().collect();
        out.sort();
        out
    }

    /// Read one received frame and update the ledger.
    ///
    /// The caller asks only while the ledger is incomplete, so in the steady
    /// state of a feed that peaks in the thousands of frames a second this is
    /// never entered at all.
    pub fn observe(&mut self, text: &str) -> Observed {
        let observed = classify(text);
        match &observed {
            Observed::Acknowledged(channel) | Observed::AlreadyHeld(channel) => {
                self.awaiting.remove(channel);
            }
            Observed::Refused => self.throttled = true,
            Observed::Nothing => {}
        }
        observed
    }

    /// What to do, given how long the last frame went out ago.
    ///
    /// Order is fail-closed in the direction that costs data least: a set that
    /// is still going out is not a set the venue has ignored; a missing
    /// acknowledgement is asked for once before anything drastic; and the
    /// drastic thing is refused outright to a connection the venue has told us
    /// it is throttling.
    pub fn review(
        &mut self,
        still_sending: bool,
        since_last_send: Duration,
        abandons_spent: u32,
    ) -> Verdict {
        if self.is_complete() || still_sending || since_last_send < ACK_GRACE {
            return Verdict::Wait;
        }
        if self.resends == 0 {
            self.resends += 1;
            return Verdict::Resend(self.missing_frames());
        }
        // Measured: a reconnect re-sends the whole set into the same limiter,
        // which is what refused it in the first place.
        if self.throttled || abandons_spent >= MAX_UNACKED_ABANDONS {
            Verdict::Report(self.outstanding())
        } else {
            Verdict::Abandon(self.outstanding())
        }
    }

    fn missing_frames(&self) -> Vec<Value> {
        self.catalogue
            .iter()
            .filter(|frame| {
                frame
                    .get("channel")
                    .and_then(|c| c.as_str())
                    .is_some_and(|c| self.awaiting.contains(&response_spelling(c)))
            })
            .cloned()
            .collect()
    }
}

/// One idle tick's whole decision, budget included.
///
/// The connection loop calls this and does what it says. Everything that used
/// to be spelled out at the call site — whether the set is still going out,
/// whether a drop is spent, what the budget becomes — lives here instead, so
/// it is pinned by tests rather than by reading the loop.
pub fn on_idle_tick(
    ledger: &mut Subscriptions,
    queued: usize,
    since_last_send: Duration,
    abandons_spent: u32,
) -> (Verdict, u32) {
    let verdict = ledger.review(queued > 0, since_last_send, abandons_spent);
    let spent = match verdict {
        Verdict::Abandon(_) => abandons_spent + 1,
        _ => abandons_spent,
    };
    (verdict, spent)
}

/// The consecutive-abandon budget, given what the ledger now knows.
///
/// A connection that reached a complete ledger gives the budget back: the
/// budget is for a set the venue will never serve, not a running total of every
/// episode this process has ever repaired successfully.
pub fn abandon_budget(spent: u32, ledger: &Subscriptions) -> u32 {
    if ledger.is_complete() { 0 } else { spent }
}

/// Read one frame without touching any state.
///
/// Two frames must not be mistaken for one: `update/order_book` is data, and
/// the `30003` refusal of a *duplicate* subscribe carries neither `type` nor
/// `channel` at the top level — its channel is inside the prose. `type` and
/// `channel` are cross-checked against each other so a frame that merely
/// mentions the word cannot mark a subscription live.
fn classify(text: &str) -> Observed {
    if text.contains("\"code\":30009") {
        return Observed::Refused;
    }
    if text.contains("\"code\":30003")
        && let Some(channel) = text.split("Already Subscribed to : ").nth(1)
    {
        let channel = channel.trim_end_matches(['"', '}', ' ']);
        if channel.contains(':') {
            return Observed::AlreadyHeld(channel.to_string());
        }
    }
    if !text.contains("\"subscribed/") {
        return Observed::Nothing;
    }
    let Ok(frame) = serde_json::from_str::<Value>(text) else {
        return Observed::Nothing;
    };
    let named = frame
        .get("type")
        .and_then(|t| t.as_str())
        .and_then(|t| t.strip_prefix("subscribed/"));
    let channel = frame.get("channel").and_then(|c| c.as_str());
    match (named, channel) {
        (Some(kind), Some(channel)) if channel.split_once(':').is_some_and(|(c, _)| c == kind) => {
            Observed::Acknowledged(channel.to_string())
        }
        _ => Observed::Nothing,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lighter::{CHANNELS, MarketInfo, SubscriptionSpec, fixtures::*};

    fn frames(ids: &[i64]) -> Vec<Value> {
        ids.iter()
            .flat_map(|id| {
                CHANNELS.into_iter().map(move |channel| {
                    serde_json::json!({
                        "type": "subscribe",
                        "channel": SubscriptionSpec::plain(channel).topic(*id),
                    })
                })
            })
            .collect()
    }

    fn ack_of(channel: &str, id: i64) -> String {
        format!(r#"{{"channel":"{channel}:{id}","type":"subscribed/{channel}"}}"#)
    }

    fn late() -> Duration {
        ACK_GRACE + Duration::from_secs(1)
    }

    /// The one frame that says a subscription is live, told apart from every
    /// frame that merely looks like it. Real wire fixtures throughout.
    #[test]
    fn an_acknowledgement_names_the_channel_it_confirms() {
        assert_eq!(
            classify(ORDER_BOOK_SNAPSHOT_ETH),
            Observed::Acknowledged("order_book:0".to_string()),
            "the snapshot IS the acknowledgement"
        );
        for (name, frame) in [
            ("the data that follows it", ORDER_BOOK_AFTER_SNAPSHOT_ETH),
            ("the unsubscribe ack", UNSUBSCRIBED_ETH),
            ("a pong", PONG),
            ("the session handshake", CONNECTED),
            ("a channel we do not subscribe to", MARKET_STATS_ALL),
            ("an unknown market", ERROR_UNKNOWN_MARKET),
            ("an unknown channel", ERROR_UNKNOWN_CHANNEL),
        ] {
            assert_eq!(
                classify(frame),
                Observed::Nothing,
                "{name} confirms nothing"
            );
        }
    }

    /// The frame the first version of this module claimed did not exist.
    ///
    /// 41 of these on 2026-08-12 and 53 on 2026-08-18, one per refused
    /// subscribe. Reading it is what turns a five-hour hole into a one-second
    /// diagnosis — and, more importantly, what stops the cure from being a
    /// reconnect that re-sends the whole set into the same limiter.
    #[test]
    fn the_venue_says_out_loud_that_it_refused_our_burst() {
        assert_eq!(classify(ERROR_TOO_MANY_MESSAGES), Observed::Refused);
    }

    /// The duplicate-subscribe refusal names its channel in prose, and that is
    /// still the venue telling us it holds the subscription.
    #[test]
    fn already_subscribed_is_read_as_held_and_names_its_channel() {
        assert_eq!(
            classify(ERROR_ALREADY_SUBSCRIBED),
            Observed::AlreadyHeld("order_book:0".to_string())
        );
    }

    /// A frame that says `subscribed/x` about channel `y` confirms neither.
    #[test]
    fn an_acknowledgement_whose_type_and_channel_disagree_confirms_nothing() {
        assert_eq!(
            classify(r#"{"channel":"ticker:7","type":"subscribed/order_book"}"#),
            Observed::Nothing
        );
        assert_eq!(
            classify(r#"{"channel":"order_book","type":"subscribed/order_book"}"#),
            Observed::Nothing,
            "a channel with no market id names no market"
        );
    }

    /// The cheap `contains` must not be what makes the check correct.
    #[test]
    fn only_the_subscribed_prefix_confirms_not_a_frame_that_merely_carries_it() {
        assert_eq!(
            classify(
                r#"{"channel":"order_book:0","type":"update/order_book","echo":"subscribed/order_book"}"#
            ),
            Observed::Nothing
        );
    }

    /// The ledger waits for exactly the set that went out, market for market.
    #[test]
    fn the_ledger_waits_for_exactly_what_was_asked_for() {
        let ledger = Subscriptions::new(&frames(&[24, 42]));
        assert_eq!(ledger.outstanding().len(), 8);
        for id in [24, 42] {
            for channel in CHANNELS {
                assert!(ledger.outstanding().contains(&format!("{channel}:{id}")));
            }
        }
    }

    /// The request spelling and the acknowledged spelling are one channel.
    /// Pinned against the wire fixture, not against another constant.
    #[test]
    fn a_request_and_its_acknowledgement_are_one_channel_spelled_two_ways() {
        let asked = SubscriptionSpec::plain("order_book").topic(0);
        assert_eq!(asked, "order_book/0");
        assert_eq!(
            Observed::Acknowledged(response_spelling(&asked)),
            classify(ORDER_BOOK_SNAPSHOT_ETH)
        );
    }

    /// The regression: the market the venue dropped is the one still held.
    ///
    /// Shaped on the measured incident of 2026-08-12 — the reconnect at
    /// 00:40:48Z acknowledged AERO and never acknowledged HYPE, and HYPE's file
    /// has literally zero records in UTC hours 01 through 04 while AERO's has
    /// twenty-four non-empty hours.
    #[test]
    fn the_market_the_venue_dropped_is_the_one_left_outstanding() {
        let mut ledger = Subscriptions::new(&frames(&[24, 42]));
        for channel in CHANNELS {
            ledger.observe(&ack_of(channel, 42));
        }
        assert_eq!(
            ledger.outstanding(),
            vec!["market_stats:24", "order_book:24", "ticker:24", "trade:24"]
        );
        assert!(!ledger.is_complete());
    }

    /// A resend asks for the missing channels and for nothing else.
    #[test]
    fn a_resend_carries_exactly_the_missing_channels() {
        let mut ledger = Subscriptions::new(&frames(&[24, 42]));
        for channel in CHANNELS {
            ledger.observe(&ack_of(channel, 42));
        }
        let Verdict::Resend(again) = ledger.review(false, late(), 0) else {
            panic!("the first review after the grace must ask again");
        };
        let asked: Vec<&str> = again
            .iter()
            .map(|f| f["channel"].as_str().unwrap())
            .collect();
        assert_eq!(asked.len(), 4, "one frame per missing channel, no more");
        assert!(
            asked.iter().all(|c| c.ends_with("/24")),
            "the served market is not asked for again: {asked:?}"
        );
    }

    /// **The correction.** A connection the venue is rate-limiting is never
    /// dropped, because dropping it re-sends the whole set into the same
    /// limiter — which is what refused it in the first place.
    #[test]
    fn a_rate_limited_connection_is_reported_and_never_abandoned() {
        let mut ledger = Subscriptions::new(&frames(&[24, 42]));
        for channel in CHANNELS {
            ledger.observe(&ack_of(channel, 42));
        }
        ledger.observe(ERROR_TOO_MANY_MESSAGES);
        assert!(ledger.throttled());

        assert!(matches!(
            ledger.review(false, late(), 0),
            Verdict::Resend(_)
        ));
        for spent in 0..MAX_UNACKED_ABANDONS + 2 {
            assert!(
                matches!(ledger.review(false, late(), spent), Verdict::Report(_)),
                "a throttled connection must never be abandoned (budget {spent})"
            );
        }
    }

    /// Silence, on the other hand, is still answered by dropping the
    /// connection — once the resend has been tried.
    #[test]
    fn a_silently_unserved_set_is_asked_once_then_the_connection_is_dropped() {
        let mut ledger = Subscriptions::new(&frames(&[24]));
        assert!(matches!(
            ledger.review(false, late(), 0),
            Verdict::Resend(_)
        ));
        assert!(matches!(
            ledger.review(false, late(), 0),
            Verdict::Abandon(_)
        ));
    }

    /// And dropping is spent, not free.
    #[test]
    fn abandoning_is_bounded_so_one_dead_market_cannot_cost_every_other_one() {
        let mut ledger = Subscriptions::new(&frames(&[24]));
        let _ = ledger.review(false, late(), 0);
        for spent in 0..MAX_UNACKED_ABANDONS {
            assert!(matches!(
                ledger.review(false, late(), spent),
                Verdict::Abandon(_)
            ));
        }
        assert!(matches!(
            ledger.review(false, late(), MAX_UNACKED_ABANDONS),
            Verdict::Report(_)
        ));
    }

    /// The budget is CONSECUTIVE: a connection that completed its ledger gives
    /// it back. A running total would leave the fourth episode of a month's
    /// uptime with no repair at all, however well the first three went.
    #[test]
    fn a_completed_ledger_gives_the_abandon_budget_back() {
        let mut ledger = Subscriptions::new(&frames(&[24]));
        assert_eq!(
            abandon_budget(MAX_UNACKED_ABANDONS, &ledger),
            MAX_UNACKED_ABANDONS
        );
        for channel in CHANNELS {
            ledger.observe(&ack_of(channel, 24));
        }
        assert!(ledger.is_complete());
        assert_eq!(abandon_budget(MAX_UNACKED_ABANDONS, &ledger), 0);
    }

    /// One tick decides, and spends the budget, in one place.
    #[test]
    fn a_drop_costs_one_unit_of_budget_and_nothing_else_does() {
        let mut ledger = Subscriptions::new(&frames(&[24]));
        let (v, spent) = on_idle_tick(&mut ledger, 0, late(), 0);
        assert!(matches!(v, Verdict::Resend(_)));
        assert_eq!(spent, 0, "asking again is free");

        let (v, spent) = on_idle_tick(&mut ledger, 0, late(), 0);
        assert!(matches!(v, Verdict::Abandon(_)));
        assert_eq!(spent, 1, "a drop is spent");

        let (v, spent) = on_idle_tick(&mut ledger, 0, late(), MAX_UNACKED_ABANDONS);
        assert!(matches!(v, Verdict::Report(_)));
        assert_eq!(spent, MAX_UNACKED_ABANDONS, "reporting costs nothing");
    }

    /// Half a set is not an ignored set, and the grace is not spent early.
    #[test]
    fn nothing_happens_while_the_set_is_still_going_out_or_before_the_grace() {
        let mut ledger = Subscriptions::new(&frames(&[24]));
        assert_eq!(
            on_idle_tick(&mut ledger, 7, ACK_GRACE * 10, 0).0,
            Verdict::Wait,
            "seven frames are still queued"
        );
        assert_eq!(
            on_idle_tick(&mut ledger, 0, ACK_GRACE - Duration::from_millis(1), 0).0,
            Verdict::Wait
        );
    }

    /// A complete ledger never speaks.
    #[test]
    fn a_complete_ledger_never_asks_again() {
        let mut ledger = Subscriptions::new(&frames(&[24]));
        for channel in CHANNELS {
            ledger.observe(&ack_of(channel, 24));
        }
        assert_eq!(
            on_idle_tick(&mut ledger, 0, ACK_GRACE * 10, 0).0,
            Verdict::Wait
        );
    }

    /// Our own burst overshoots the measured admission limit, which is the
    /// entire reason the throttled pace exists.
    ///
    /// 38 frames were admitted and the 39th refused, at +1.01s, in both
    /// incidents. The normal pacer puts 32 frames out in that first second and
    /// 92 out in under three, so on a connection whose bucket is already partly
    /// spent the refusal is not bad luck — it is arithmetic.
    #[test]
    fn our_burst_overshoots_the_measured_admission_limit() {
        let first_second = crate::lighter::http::SUBSCRIBE_CHUNK * 4;
        assert!(
            first_second + crate::lighter::http::SUBSCRIBE_CHUNK > ADMITTED_BEFORE_REFUSAL,
            "if the first second's burst fitted inside the admission limit with a \
             chunk to spare, the refusal would need another explanation"
        );
    }
}
