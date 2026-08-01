//! The transaction-nonce owner for one api-key slot (design note
//! [`docs/design-lighter-connector.md`](../../../docs/design-lighter-connector.md) §3.3,
//! measured live in §2.7).
//!
//! The venue's nonce is a **strictly sequential counter per `(account_index, api_key_index)`
//! slot**: the next accepted transaction must carry exactly `old + 1`. There is **no
//! window** — a repeat and a skip are both refused with the same `21104 invalid nonce`, and
//! neither moves the counter (§2.7). Parallelism comes from provisioning more key slots, not
//! from pipelining one, so a slot serialises: **one transaction in flight at a time** (§4.12,
//! the real constraint, not the 350 ms folklore in the SDK docstring).
//!
//! This type owns that counter for a single slot. It is pure state — no I/O, no clock — so
//! the whole of the measured semantics is unit-tested here without a socket. The async slot
//! task that drives the signer and REST is the only caller; it holds exactly one of these per
//! slot and never shares it (single-writer, `AGENTS.md` §1.4).
//!
//! ## The four measured outcomes it has to encode (§2.7)
//!
//! 1. **Accepted** (`sendTx` → HTTP 200, sequencer took it): the nonce is consumed →
//!    [`NonceOwner::consumed`] advances the counter by exactly one.
//! 2. **Application-rejected at HTTP 200** — the §4.11 trap: a duplicate `ClientOrderIndex`
//!    gets 200, produces no order, and **still spends the nonce**. So this is *also*
//!    [`NonceOwner::consumed`]: acceptance of the *order* is a private-channel question, but
//!    consumption of the *nonce* is settled the moment the sequencer answered 200.
//! 3. **`21104` on a fresh send** (a real skip/repeat, or the counter drifted from the
//!    venue's): the nonce was **not** consumed, and the local counter can no longer be
//!    trusted → [`NonceOwner::invalidated`] forces a re-seed from `GET /nextNonce` before the
//!    next reserve. Fail closed: never guess the counter forward on a rejection (§3.3).
//! 4. **Ambiguous transport** (5xx / 408 / an unreadable 200 body): the fate of the
//!    in-flight tx is unknown. §3.3 gives two correct resolutions, and this type expresses
//!    both — [`NonceOwner::released`] to **retry the same nonce** (cheap: `21104` on the
//!    retry then means "the first one landed", itself a verdict), or [`NonceOwner::invalidated`]
//!    to hard-refresh from `nextNonce`. What it must **never** do is increment locally on an
//!    ambiguous outcome, which would desync the counter silently.

use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum NonceError {
    /// The slot has no trusted counter value — it was never seeded, or an ambiguous/rejected
    /// outcome invalidated it. The caller must `GET /nextNonce` and [`NonceOwner::seed`]
    /// before it can build another transaction. Fail closed rather than guess (§3.3).
    #[error("the slot's nonce is not seeded; refresh from GET /nextNonce first")]
    NotSeeded,
    /// A transaction is already in flight on this slot. The venue accepts one at a time per
    /// key slot (§4.12); the caller must resolve the outstanding one first.
    #[error("a transaction is already in flight on this api-key slot (§4.12)")]
    InFlight,
}

/// Owns the sequential tx-nonce for one api-key slot.
#[derive(Debug)]
pub struct NonceOwner {
    api_key_index: u8,
    /// The value [`Self::reserve`] hands out next, or `None` when the counter is not trusted.
    next: Option<i64>,
    /// The value currently in flight, `Some` between [`Self::reserve`] and its resolution.
    reserved: Option<i64>,
}

impl NonceOwner {
    /// A fresh owner for `api_key_index`. Unseeded: the first thing the slot task does is
    /// `GET /nextNonce` and [`Self::seed`], so construction performs no I/O.
    pub fn new(api_key_index: u8) -> Self {
        Self {
            api_key_index,
            next: None,
            reserved: None,
        }
    }

    /// The slot this owner is for.
    pub fn api_key_index(&self) -> u8 {
        self.api_key_index
    }

    /// Sets the counter from an authoritative `GET /nextNonce`, clearing any in-flight
    /// reservation.
    ///
    /// This is both the initial seed and the hard-refresh of §3.3: after an ambiguous
    /// outcome the slot re-reads the venue's `nextNonce` and calls this, which is why it
    /// drops `reserved` — the refresh *is* the reconciliation, and the previous in-flight
    /// value is no longer the truth.
    pub fn seed(&mut self, next_nonce: i64) {
        self.next = Some(next_nonce);
        self.reserved = None;
    }

    /// Whether the counter is trusted (seeded and not invalidated).
    pub fn is_seeded(&self) -> bool {
        self.next.is_some()
    }

    /// Whether a transaction is in flight.
    pub fn in_flight(&self) -> bool {
        self.reserved.is_some()
    }

    /// The nonce [`Self::reserve`] would hand out, without reserving it. `None` when unseeded.
    pub fn peek(&self) -> Option<i64> {
        self.next
    }

    /// Reserves the next nonce for a transaction about to be signed and sent.
    ///
    /// Fails with [`NonceError::InFlight`] if one is already out (§4.12), and with
    /// [`NonceError::NotSeeded`] if the counter is not trusted. On success the slot is
    /// in flight until [`Self::consumed`], [`Self::released`] or [`Self::invalidated`].
    pub fn reserve(&mut self) -> Result<i64, NonceError> {
        if self.reserved.is_some() {
            return Err(NonceError::InFlight);
        }
        let n = self.next.ok_or(NonceError::NotSeeded)?;
        self.reserved = Some(n);
        Ok(n)
    }

    /// The sequencer consumed the reserved nonce (HTTP 200, whatever the *order*'s fate — see
    /// outcomes 1 and 2 above). Advances the counter by exactly one and clears the in-flight
    /// reservation. A no-op if nothing was reserved.
    pub fn consumed(&mut self) {
        if let Some(reserved) = self.reserved.take() {
            self.next = Some(reserved + 1);
        }
    }

    /// The reserved nonce was **not** consumed and the same value is safe to retry (outcome
    /// 4, the cheap resolution): clears the in-flight reservation and leaves the counter
    /// where it was, so the next [`Self::reserve`] hands out the same nonce again.
    pub fn released(&mut self) {
        self.reserved = None;
    }

    /// The counter can no longer be trusted (a fresh `21104`, outcome 3; or the hard-refresh
    /// arm of an ambiguous outcome, 4): drops both the reservation and the counter, so
    /// [`Self::reserve`] refuses with [`NonceError::NotSeeded`] until the next [`Self::seed`].
    pub fn invalidated(&mut self) {
        self.reserved = None;
        self.next = None;
    }
}

#[cfg(test)]
mod tests {
    use super::{NonceError, NonceOwner};

    /// A slot's counter advances by **exactly one** per consumed transaction — the venue's
    /// `new = old + 1` rule (§3.3). Anything else is refused `21104`, so an owner that
    /// jumped by two would wedge the slot on its next send.
    #[test]
    fn a_consumed_transaction_advances_the_counter_by_exactly_one() {
        let mut slot = NonceOwner::new(5);
        slot.seed(1);
        assert_eq!(slot.reserve().unwrap(), 1);
        slot.consumed();
        assert_eq!(slot.peek(), Some(2));
        assert_eq!(slot.reserve().unwrap(), 2);
        slot.consumed();
        assert_eq!(slot.peek(), Some(3));
    }

    /// **One transaction in flight per slot** (§4.12): the second reserve is refused until the
    /// first resolves. This is the constraint that makes a slot serialise; parallelism is
    /// more slots, not a window.
    #[test]
    fn only_one_transaction_is_in_flight_per_slot() {
        let mut slot = NonceOwner::new(5);
        slot.seed(5);
        assert_eq!(slot.reserve().unwrap(), 5);
        assert_eq!(slot.reserve(), Err(NonceError::InFlight));
        slot.consumed();
        assert_eq!(slot.reserve().unwrap(), 6);
    }

    /// **A `21104` rejection costs nothing and the same nonce retries** (§2.7 note 2): a
    /// repeat or a skip is refused without moving the counter, so retrying the identical
    /// transaction is safe and idempotent at the counter level.
    #[test]
    fn a_released_nonce_is_unchanged_and_retries_with_the_same_value() {
        let mut slot = NonceOwner::new(5);
        slot.seed(7);
        assert_eq!(slot.reserve().unwrap(), 7);
        slot.released();
        assert!(!slot.in_flight());
        assert_eq!(
            slot.peek(),
            Some(7),
            "a rejected nonce did not move the counter"
        );
        assert_eq!(slot.reserve().unwrap(), 7, "the same nonce is retried");
    }

    /// **An application rejection at HTTP 200 still spends the nonce** — the §4.11 trap. A
    /// duplicate `ClientOrderIndex` gets 200, produces no order, and moves the counter 4 → 5
    /// (§2.7). Acceptance of the order is a separate, private-channel question; the nonce is
    /// spent regardless.
    #[test]
    fn an_application_rejection_at_http_200_still_consumes_the_nonce() {
        let mut slot = NonceOwner::new(5);
        slot.seed(4);
        assert_eq!(slot.reserve().unwrap(), 4);
        // The sequencer answered 200 — the nonce is consumed even though no order resulted.
        slot.consumed();
        assert_eq!(slot.peek(), Some(5));
    }

    /// **An ambiguous outcome forces a re-seed and never guesses the counter forward** (§3.3,
    /// fail closed). After it, a reserve is refused until the slot has hard-refreshed from
    /// `nextNonce`.
    #[test]
    fn an_ambiguous_outcome_invalidates_the_counter_until_reseeded() {
        let mut slot = NonceOwner::new(5);
        slot.seed(9);
        assert_eq!(slot.reserve().unwrap(), 9);
        slot.invalidated();
        assert!(!slot.is_seeded());
        assert_eq!(slot.reserve(), Err(NonceError::NotSeeded));
        // The hard-refresh from GET /nextNonce puts it back in business.
        slot.seed(9);
        assert_eq!(slot.reserve().unwrap(), 9);
    }

    /// Reserving before the first `nextNonce` fetch is refused — the counter has no trusted
    /// value, and inventing one would desync the slot from the first transaction.
    #[test]
    fn reserving_before_seeding_is_refused() {
        let mut slot = NonceOwner::new(4);
        assert!(!slot.is_seeded());
        assert_eq!(slot.reserve(), Err(NonceError::NotSeeded));
        assert_eq!(slot.api_key_index(), 4);
    }
}
