//! `POST /exchange`: the actions this backend signs, and what the venue's answer means.
//!
//! # The field order in this file is protocol, not style
//!
//! The signature covers `keccak(msgpack(action) ‖ …)`, and `rmp_serde::to_vec_named`
//! serialises a struct as a map in **declaration order**. Moving a field, renaming one, or
//! adding `#[serde(...)]` that reorders them changes the hash — and nothing fails to
//! compile, no test that only checks JSON notices, and the venue reports it as
//! `"User or API Wallet 0x… does not exist"`, which reads like a credentials problem.
//! `msgpack_bytes_match_the_official_sdk` pins the exact bytes for that reason.
//!
//! Two omissions are also protocol: an order without a client id must **omit** `c` rather
//! than send `null`, and a cancel's `f` flag must be omitted rather than sent as `false` —
//! the venue rejects an action hashed with `f: false`. Both cancel actions carry an `f`
//! field guarded by `skip_serializing_if`, so it appears on the wire only when set to
//! `true`; the default `new()` leaves it false and therefore absent, byte-for-byte the
//! shape the signature fixtures pin. `f: true` is fast-cancel — future mempool
//! prioritisation, with **no effect today**, and it is **dormant: no production path sets
//! it.** The [`CancelByCloidAction::fast`] builder and its serialisation test keep the wire
//! shape correct for the day it is activated, but activating it on the live cancel path is
//! gated on a testnet direct-signing capture proving the venue accepts an `f: true` cancel
//! (or a primary venue contract). Until then the live path is not merely "left off" but
//! **sealed**: it signs a [`PlainCancelByCloid`], a wrapper with no `.fast()`, so no money-path
//! call site can set `f` — the reason being that the official API docs and python-SDK describe
//! cancel without `f`, so an unverified field on the money path could get every cancel
//! rejected — leaving the order live — for a flag that buys nothing today. The `.fast()`
//! builder stays reachable only from the capture tool (`smoke.rs`) that would earn that proof.
//! The sweep's `cancel` never sets it either, and additionally must not, because `f: true` on a
//! trigger order is rejected and a sweep can name a human's UI trigger.
//!
//! # What a 200 means
//!
//! Almost nothing on its own. `HTTP 200` with `{"status":"ok"}` says the venue parsed and
//! accepted the *request*; the per-order verdicts are nested inside, and one of them can be
//! `{"error": …}` while the envelope still says `ok`. A backend that stops at the HTTP
//! status reports every rejected order as a successful submission and then waits forever
//! for a fill. [`parse_exchange_response`] is where that is unpacked, and it is a pure
//! function so the shapes can be pinned without a network.

use std::sync::Arc;

use serde::Serialize;
use tracing::debug;

use crate::hyperliquid::{
    HyperliquidError,
    signing::{NonceSource, Signature, Signer, hashing_string},
};

/// One order, in the venue's own field order: `a, b, p, s, r, t, c`.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct OrderWire {
    /// Asset index — the position in `meta.universe`, which **differs between mainnet and
    /// testnet** and is reshuffled by delistings. Always resolved at run time.
    pub a: u32,
    /// Is buy.
    pub b: bool,
    /// Price, as a canonical string ([`hashing_string`]).
    pub p: String,
    /// Size, as a canonical string.
    pub s: String,
    /// Reduce only.
    pub r: bool,
    pub t: OrderKind,
    /// Client order id. Omitted entirely when absent — `null` is a different msgpack byte
    /// and therefore a different signature.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct OrderKind {
    pub limit: LimitKind,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct LimitKind {
    /// `Gtc`, `Ioc` or `Alo` (post-only). Capitalised exactly like this.
    pub tif: &'static str,
}

/// `{"type":"order","orders":[…],"grouping":"na"}`.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct OrderAction {
    #[serde(rename = "type")]
    pub action_type: &'static str,
    pub orders: Vec<OrderWire>,
    pub grouping: &'static str,
}

impl OrderAction {
    pub fn new(orders: Vec<OrderWire>) -> Self {
        Self {
            action_type: "order",
            orders,
            grouping: "na",
        }
    }
}

/// One cancel keyed by the venue's own order id: `{"a": asset, "o": oid}`.
///
/// The **short** field names, unlike [`CancelByCloidWire`] two structs down — the venue uses
/// different names for the two cancel actions and the signature covers the difference.
///
/// This is the shape a sweep needs. A client id can only cancel what this process minted,
/// and the orders a restart has to clear are by definition from a run that no longer exists;
/// the venue's open-order list answers in `oid`.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct CancelWire {
    pub a: u32,
    pub o: u64,
}

/// serde `skip_serializing_if` predicate: omit a `bool` when it is false.
///
/// A plain `bool` cannot express the forbidden `Some(false)` an `Option<bool>` could, and
/// its default is the safe (omitted) value. serde's derive pre-counts the non-skipped
/// fields, so `rmp_serde` still emits the correct fixmap header — `0x82` with `f` skipped,
/// `0x83` with it present — which is exactly why skipping preserves the signature.
fn is_false(value: &bool) -> bool {
    !*value
}

/// `{"type":"cancel","cancels":[…]}`, plus an optional `f` when fast-cancel is set.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct CancelAction {
    #[serde(rename = "type")]
    pub action_type: &'static str,
    pub cancels: Vec<CancelWire>,
    /// Fast-cancel flag — see the module docs and [`CancelByCloidAction::f`]. The sweep
    /// builds this action and **never** sets it: a sweep cancels whatever the account
    /// holds on a coin, which can include a human's UI **trigger** order, and `f: true` on
    /// a trigger is rejected. It therefore stays false and is omitted, keeping the sweep's
    /// signed bytes byte-identical. Field order is protocol: `f` is last, after `cancels`.
    #[serde(skip_serializing_if = "is_false")]
    pub f: bool,
}

impl CancelAction {
    pub fn new(cancels: Vec<CancelWire>) -> Self {
        Self {
            action_type: "cancel",
            cancels,
            f: false,
        }
    }
}

/// One cancel keyed by client id. Note the **long** field names here: `cancelByCloid` uses
/// `asset`/`cloid` where the order action uses `a`/`c`. Copying the short ones across is a
/// silent signature change.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct CancelByCloidWire {
    pub asset: u32,
    pub cloid: String,
}

/// `{"type":"cancelByCloid","cancels":[…]}`, plus an optional `f` when fast-cancel is set.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct CancelByCloidAction {
    #[serde(rename = "type")]
    pub action_type: &'static str,
    pub cancels: Vec<CancelByCloidWire>,
    /// Fast-cancel: future mempool prioritisation, with **no effect today** per the venue's
    /// docs — a cancel with `f: true` is scheduled ahead of others *iff* a coming network
    /// upgrade ships it, and behaves identically until then. It MUST be omitted rather than
    /// sent as `false`: an action hashed with `f: false` is rejected, and the signature
    /// covers `msgpack(action)`, so [`is_false`] skips it when unset. Field order is
    /// protocol — `f` is last, after `cancels`.
    ///
    /// **Dormant: no production path sets it.** [`Self::fast`] can, and the serialisation
    /// test pins that the wire would be correct if it did, but the live cancel path is
    /// **sealed** against it: `private_stream::cancel_action_for_order` returns
    /// [`PlainCancelByCloid`], which wraps a plain `new()` action and exposes no `.fast()`, so
    /// no money-path call site can set `f`. Two reasons, in order of weight: (1) we have no
    /// evidence the venue accepts an `f: true` direct-signed cancel — the docs and python-SDK
    /// omit `f` — so enabling it on every user cancel risks the venue rejecting the request and
    /// the order staying live, for zero benefit today; activation is gated on a testnet capture
    /// that proves acceptance. (2) It must never name a **trigger** order (the venue rejects
    /// `f: true` on triggers); the per-order path cancels only this backend's own
    /// `Gtc`/`Alo`/`Ioc` limits — `tif_of` admits no trigger kind — so that hazard alone would
    /// not have blocked it.
    #[serde(skip_serializing_if = "is_false")]
    pub f: bool,
}

impl CancelByCloidAction {
    pub fn new(cancels: Vec<CancelByCloidWire>) -> Self {
        Self {
            action_type: "cancelByCloid",
            cancels,
            f: false,
        }
    }

    /// Opts this cancel into fast-cancel (`f: true`) — see the [`f`](Self::f) field.
    ///
    /// **No production caller today.** Kept, with its serialisation test, so the wire is
    /// ready the day fast-cancel is activated; activation is gated on a testnet capture
    /// proving the venue accepts an `f: true` direct-signed cancel. The capture tool that
    /// would produce such proof (`smoke.rs`) builds a `CancelByCloidAction` directly and may
    /// call this; the live cancel path deliberately cannot — it goes through
    /// [`PlainCancelByCloid`], which has no `.fast()`. Only ever safe on a cancel that cannot
    /// name a trigger order.
    pub fn fast(mut self) -> Self {
        self.f = true;
        self
    }
}

/// A `cancelByCloid` action **sealed against fast-cancel**, for the live money path.
///
/// It wraps a [`CancelByCloidAction`] built by `new()` (so `f` is unset) and serialises
/// byte-for-byte identically — `#[serde(transparent)]` forwards `Serialize` straight to the
/// inner action — so the msgpack the signature covers is unchanged
/// (`the_sealed_live_cancel_is_byte_identical_and_carries_no_f`). What it deliberately does
/// **not** offer is any way to set `f`: the inner action is private to this module, and there
/// is no `.fast()`. The live per-order cancel (`private_stream::cancel_action_for_order`)
/// returns this type and `private_stream::cancel_order` posts it, so **no call site on the
/// money path can chain `.fast()` onto the action it signs** — re-adding it, the review's
/// named mutation, is a compile error rather than a silently-green one. The invariant
/// "`cancel_order` posts the built action verbatim" is thus enforced by the compiler, not by
/// review.
///
/// This is not a general seal on fast-cancel: the dormant [`CancelByCloidAction::fast`]
/// builder stays available so the testnet capture tool (`smoke.rs`) can probe whether the
/// venue accepts an `f: true` cancel. Until such a capture proves acceptance, the live path
/// stays sealed — an unverified field on the money path could get every cancel rejected,
/// leaving the order live, for a flag that has no effect today. See the module docs.
#[derive(Debug, Serialize)]
#[serde(transparent)]
pub struct PlainCancelByCloid(CancelByCloidAction);

impl PlainCancelByCloid {
    /// Builds the sealed live-cancel action from its cancels. `f` is unset and, by the type's
    /// construction, cannot be set — see the type docs.
    pub fn new(cancels: Vec<CancelByCloidWire>) -> Self {
        Self(CancelByCloidAction::new(cancels))
    }
}

/// The time-in-force string for one of the venue's three limit behaviours.
///
/// `FOK` has no Hyperliquid equivalent and is refused rather than silently downgraded to
/// `Ioc`: the two differ exactly when it matters, on a partial fill.
pub fn tif_of(
    time_in_force: hftbacktest::types::TimeInForce,
) -> Result<&'static str, HyperliquidError> {
    use hftbacktest::types::TimeInForce::*;
    match time_in_force {
        GTC => Ok("Gtc"),
        GTX => Ok("Alo"),
        IOC => Ok("Ioc"),
        FOK => Err(HyperliquidError::InvalidOrder(
            "Hyperliquid has no fill-or-kill; Ioc would leave a partial fill resting-free \
             rather than cancelling the whole order"
                .into(),
        )),
        Unsupported => Err(HyperliquidError::InvalidOrder(
            "an unsupported time-in-force".into(),
        )),
    }
}

/// Renders a price and a size as the canonical strings the action carries.
pub fn wire_numbers(price: f64, size: f64) -> Result<(String, String), HyperliquidError> {
    Ok((hashing_string(price)?, hashing_string(size)?))
}

/// What the venue said about **one** order or cancel inside an accepted request.
#[derive(Clone, Debug, PartialEq)]
pub enum ActionOutcome {
    /// The order is on the book. `oid` is the venue's own id.
    Resting { oid: u64, cloid: Option<String> },
    /// The order matched immediately, in whole or in part.
    Filled {
        oid: u64,
        cloid: Option<String>,
        total_sz: f64,
        avg_px: f64,
    },
    /// A cancel the venue carried out.
    Success,
    /// This one was refused, with the venue's reason. **Can appear under a `200 ok`.**
    Error(String),
}

impl ActionOutcome {
    pub fn error(&self) -> Option<&str> {
        match self {
            ActionOutcome::Error(reason) => Some(reason),
            _ => None,
        }
    }
}

/// Whether an HTTP status says the venue did *not* decide — as opposed to deciding "no".
///
/// **The distinction is the order's safety, not a nicety.** A rejection is definitive and
/// expires the order back to the bot; an ambiguous answer must keep it, because the order
/// may be resting. A `5xx` from the edge in front of this venue — which drops sockets about
/// nine times a day — is the ambiguous case by definition: the request may well have been
/// executed and only the answer lost. Same for `408` and `425`.
///
/// `429` is deliberately **not** here. A throttled request is refused before it reaches the
/// matching engine, so nothing rests; treating it as ambiguous would leave the bot holding a
/// `Status::New` order that exists nowhere and burn that `order_id`.
fn status_is_ambiguous(status: u16) -> bool {
    status >= 500 || status == 408 || status == 425
}

/// Turns one `/exchange` answer into a verdict per submitted order.
///
/// Four distinct answers hide behind two HTTP status codes, and three of them have been
/// mistaken for success by some client or other:
///
/// * **non-2xx** — the reason is in the body, and at `422` that body is **plain text**
///   (`Failed to deserialize the JSON body into the target type`), not JSON. A strict
///   `serde_json` error path fails on the error itself and reports a parse failure instead
///   of the real cause. Whether it is a *verdict* depends on the status:
///   [`status_is_ambiguous`].
/// * **`{"status":"err", …}` under a 200** — a venue rejection. The `response` field is the
///   reason, and it is a bare string, not an object.
/// * **`{"status":"ok"}` with `{"error": …}` inside `statuses`** — the request was accepted
///   and *this order* was not. Stopping at the envelope reports it as a live order that
///   will never fill and never be cancellable.
/// * **`{"status":"ok"}` with `resting`/`filled`** — the only real acceptance, and even
///   then the authoritative lifecycle comes over `orderUpdates`.
pub fn parse_exchange_response(
    status: u16,
    body: &str,
) -> Result<Vec<ActionOutcome>, HyperliquidError> {
    if !(200..300).contains(&status) {
        // The body is the diagnosis; 429 and 422 in particular carry it in prose.
        let message = format!("HTTP {status}: {}", body.trim());
        return Err(if status_is_ambiguous(status) {
            HyperliquidError::ExchangeUnavailable(message)
        } else {
            HyperliquidError::ExchangeRejected(message)
        });
    }

    let value: serde_json::Value = serde_json::from_str(body).map_err(|error| {
        // A `200` this backend cannot read is not a refusal — the venue may have carried the
        // action out and answered in a shape (or from a proxy) that is not its own.
        HyperliquidError::ExchangeUnavailable(format!(
            "HTTP {status} with a body that is not JSON ({error}): {}",
            body.trim()
        ))
    })?;

    match value.get("status").and_then(|s| s.as_str()) {
        Some("ok") => {}
        _ => {
            let reason = value
                .get("response")
                .map(render_reason)
                .unwrap_or_else(|| body.trim().to_string());
            return Err(HyperliquidError::ExchangeRejected(reason));
        }
    }

    let statuses = value
        .pointer("/response/data/statuses")
        .and_then(|s| s.as_array());
    let Some(statuses) = statuses else {
        // Some actions answer `ok` with no per-item data. Nothing was refused, so nothing
        // is reported; the caller learns the real outcome from `orderUpdates`.
        return Ok(Vec::new());
    };

    Ok(statuses.iter().map(parse_status).collect())
}

fn parse_status(status: &serde_json::Value) -> ActionOutcome {
    if let Some(text) = status.as_str() {
        return if text == "success" {
            ActionOutcome::Success
        } else {
            // Unrecognised, and therefore not claimed as a success.
            ActionOutcome::Error(text.to_string())
        };
    }
    if let Some(error) = status.get("error") {
        return ActionOutcome::Error(render_reason(error));
    }
    if let Some(resting) = status.get("resting") {
        return ActionOutcome::Resting {
            oid: resting.get("oid").and_then(|o| o.as_u64()).unwrap_or(0),
            cloid: resting
                .get("cloid")
                .and_then(|c| c.as_str())
                .map(str::to_string),
        };
    }
    if let Some(filled) = status.get("filled") {
        return ActionOutcome::Filled {
            oid: filled.get("oid").and_then(|o| o.as_u64()).unwrap_or(0),
            cloid: filled
                .get("cloid")
                .and_then(|c| c.as_str())
                .map(str::to_string),
            total_sz: number_or_string(filled.get("totalSz")),
            avg_px: number_or_string(filled.get("avgPx")),
        };
    }
    // A shape this backend has never seen is **not** an acceptance. The venue adds
    // statuses over time and treating an unknown one as "resting" leaves the bot holding
    // an order that may not exist.
    ActionOutcome::Error(format!("an unrecognised order status: {status}"))
}

fn number_or_string(value: Option<&serde_json::Value>) -> f64 {
    match value {
        Some(serde_json::Value::String(text)) => text.parse().unwrap_or(0.0),
        Some(serde_json::Value::Number(number)) => number.as_f64().unwrap_or(0.0),
        _ => 0.0,
    }
}

/// A venue reason as text: a bare string stays a string, anything else is rendered.
fn render_reason(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// Signs and posts actions to `/exchange`.
pub struct ExchangeClient {
    client: reqwest::Client,
    rest_url: String,
    signer: Arc<Signer>,
    nonces: Arc<NonceSource>,
}

/// A request that has not answered by now will not answer usefully: the order it carries
/// is stale, and holding the caller blocks nothing useful.
const EXCHANGE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Serialize)]
struct ExchangeRequest<'a, A: Serialize> {
    action: &'a A,
    nonce: u64,
    signature: Signature,
}

impl ExchangeClient {
    pub fn new(rest_url: &str, signer: Arc<Signer>, nonces: Arc<NonceSource>) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(EXCHANGE_TIMEOUT)
                .build()
                .unwrap_or_default(),
            rest_url: rest_url.to_string(),
            signer,
            nonces,
        }
    }

    /// Signs `action`, posts it, and returns one verdict per item.
    ///
    /// The action is serialised to msgpack **once** and both hashed and — via its JSON
    /// rendering — sent; the two must describe the same thing, which is why the struct is
    /// the single source rather than a hand-built JSON value.
    pub async fn post<A: Serialize>(
        &self,
        action: &A,
    ) -> Result<Vec<ActionOutcome>, HyperliquidError> {
        let msgpack = rmp_serde::to_vec_named(action)
            .map_err(|error| HyperliquidError::Signing(format!("msgpack: {error}")))?;
        let nonce = self.nonces.next();
        let signature = self.signer.sign_action(&msgpack, nonce)?;

        // Logged on every submit, at debug: when the venue rejects with its one
        // undifferentiated message, the only way to tell two runs apart is to diff the
        // bytes that were actually hashed.
        debug!(
            nonce,
            action_bytes = msgpack.len(),
            signer = %self.signer.address(),
            "Signing a Hyperliquid action."
        );

        let response = self
            .client
            .post(format!("{}/exchange", self.rest_url))
            .json(&ExchangeRequest {
                action,
                nonce,
                signature,
            })
            .send()
            .await?;
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        parse_exchange_response(status, &body)
    }
}

#[cfg(test)]
mod tests {
    use crate::hyperliquid::exchange::{
        ActionOutcome,
        CancelAction,
        CancelByCloidAction,
        CancelByCloidWire,
        CancelWire,
        LimitKind,
        OrderAction,
        OrderKind,
        OrderWire,
        PlainCancelByCloid,
        parse_exchange_response,
        tif_of,
    };

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// **The one test that pins the wire format.** These are the bytes the official
    /// `hyperliquid-python-sdk` produces for the same two actions (`msgpack.packb` of
    /// `order_wires_to_order_action`), and they are what the signature covers.
    ///
    /// A field reordered, renamed, or serialised as `null` instead of omitted changes them,
    /// and every other signal is silent: the JSON still looks right, the request is still
    /// well-formed, and the venue answers that the API wallet does not exist.
    #[test]
    fn msgpack_bytes_match_the_official_sdk() {
        // Split only to stay inside the line width; the concatenation is exact.
        let order_alo_cloid = concat!(
            "83a474797065a56f72646572a66f72646572739187a16103a162c3a170a53330303030a173a5302e303031a172c2a17481a56c696d697481a3",
            "746966a3416c6fa163d92230786131623230303030303030303030303030303030303030303030303064656164a867726f7570696e67a26e61"
        );
        let order_gtc_no_cloid = concat!(
            "83a474797065a56f72646572a66f72646572739186a16101a162c2a170a6323030302e35a1",
            "73a3312e35a172c3a17481a56c696d697481a3746966a3477463a867726f7570696e67a26e61"
        );
        let cancel_by_cloid = concat!(
            "82a474797065ad63616e63656c4279436c6f6964a763616e63656c739182a5617373657403a563",
            "6c6f6964d92230786131623230303030303030303030303030303030303030303030303064656164"
        );

        let order = OrderAction::new(vec![OrderWire {
            a: 3,
            b: true,
            p: "30000".into(),
            s: "0.001".into(),
            r: false,
            t: OrderKind {
                limit: LimitKind { tif: "Alo" },
            },
            c: Some("0xa1b2000000000000000000000000dead".into()),
        }]);
        assert_eq!(
            hex(&rmp_serde::to_vec_named(&order).unwrap()),
            order_alo_cloid
        );

        // Without a client id the `c` key is absent, not null: six pairs, not seven.
        let no_cloid = OrderAction::new(vec![OrderWire {
            a: 1,
            b: false,
            p: "2000.5".into(),
            s: "1.5".into(),
            r: true,
            t: OrderKind {
                limit: LimitKind { tif: "Gtc" },
            },
            c: None,
        }]);
        assert_eq!(
            hex(&rmp_serde::to_vec_named(&no_cloid).unwrap()),
            order_gtc_no_cloid
        );

        // `cancelByCloid` uses the long field names. Reusing the order action's `a`/`c`
        // here signs perfectly well and recovers to nobody.
        let cancel = CancelByCloidAction::new(vec![CancelByCloidWire {
            asset: 3,
            cloid: "0xa1b2000000000000000000000000dead".into(),
        }]);
        assert_eq!(
            hex(&rmp_serde::to_vec_named(&cancel).unwrap()),
            cancel_by_cloid
        );
    }

    /// The request body around the action: `{"action":…, "nonce":…, "signature":{r,s,v}}`.
    #[test]
    fn the_request_body_carries_the_action_nonce_and_signature() {
        use crate::hyperliquid::signing::{NonceSource, Signer};

        let signer = Signer::from_hex(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            false,
        )
        .unwrap();
        let action = CancelByCloidAction::new(vec![CancelByCloidWire {
            asset: 3,
            cloid: "0xa1b2000000000000000000000000dead".into(),
        }]);
        let msgpack = rmp_serde::to_vec_named(&action).unwrap();
        let nonce = NonceSource::new().next_from(1_700_000_000_001);
        let signature = signer.sign_action(&msgpack, nonce).unwrap();

        let body = serde_json::json!({
            "action": action, "nonce": nonce, "signature": signature,
        });
        assert_eq!(body["action"]["type"], "cancelByCloid");
        assert_eq!(body["action"]["cancels"][0]["asset"], 3);
        assert_eq!(body["nonce"], 1_700_000_000_001u64);
        assert_eq!(
            body["signature"]["r"],
            "0xf236230145c3fbe6a944174af468aed28430978ccb2d587fccf7cc4206248207"
        );
        assert_eq!(body["signature"]["v"], 27);
    }

    /// Post-only maps to `Alo`, and fill-or-kill is refused rather than quietly demoted to
    /// `Ioc` — the two differ precisely on a partial fill, which is where the difference is
    /// a position the bot did not ask for.
    #[test]
    fn time_in_force_maps_to_the_venues_three_and_refuses_the_fourth() {
        use hftbacktest::types::TimeInForce;
        assert_eq!(tif_of(TimeInForce::GTC).unwrap(), "Gtc");
        assert_eq!(tif_of(TimeInForce::GTX).unwrap(), "Alo");
        assert_eq!(tif_of(TimeInForce::IOC).unwrap(), "Ioc");
        assert!(tif_of(TimeInForce::FOK).is_err());
        assert!(tif_of(TimeInForce::Unsupported).is_err());
    }

    /// A resting order: the only answer that means the order is on the book.
    #[test]
    fn an_accepted_order_reports_its_venue_id() {
        let outcomes = parse_exchange_response(
            200,
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":77738308,"cloid":"0xa1b2000000000000000000000000dead"}}]}}}"#,
        )
        .unwrap();
        assert_eq!(
            outcomes,
            vec![ActionOutcome::Resting {
                oid: 77738308,
                cloid: Some("0xa1b2000000000000000000000000dead".into()),
            }]
        );
    }

    /// **A 200 is not an acceptance.** The envelope says `ok` while this order was refused;
    /// a backend that stops at the HTTP status hands the bot a live order that does not
    /// exist and cannot be cancelled, and waits for a fill for ever.
    #[test]
    fn an_error_nested_under_a_200_ok_is_a_rejection() {
        let outcomes = parse_exchange_response(
            200,
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Order could not immediately match against any resting orders. asset=3"}]}}}"#,
        )
        .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(
            outcomes[0]
                .error()
                .unwrap()
                .contains("could not immediately match"),
            "{outcomes:?}"
        );
    }

    /// A mixed batch: the venue answers per order, and one refusal must not condemn the
    /// others or be lost among them.
    #[test]
    fn each_order_in_a_batch_gets_its_own_verdict() {
        let outcomes = parse_exchange_response(
            200,
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[
                {"resting":{"oid":1}},
                {"error":"Insufficient margin to place order."},
                {"filled":{"totalSz":"0.001","avgPx":"63000.0","oid":2}}
            ]}}}"#,
        )
        .unwrap();
        assert_eq!(outcomes.len(), 3);
        assert!(matches!(outcomes[0], ActionOutcome::Resting { oid: 1, .. }));
        assert!(outcomes[1].error().unwrap().contains("Insufficient margin"));
        assert_eq!(
            outcomes[2],
            ActionOutcome::Filled {
                oid: 2,
                cloid: None,
                total_sz: 0.001,
                avg_px: 63000.0
            }
        );
    }

    /// `{"status":"err"}` under a 200: the venue refused the whole request, and its reason
    /// is a bare string rather than an object.
    #[test]
    fn a_venue_rejection_surfaces_the_venues_own_words() {
        let error = parse_exchange_response(
            200,
            r#"{"status":"err","response":"User or API Wallet 0xf39f does not exist."}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("does not exist"), "{error}");
    }

    /// A cancel answers with the bare string `"success"`, not an object.
    #[test]
    fn a_cancel_answers_with_a_bare_string() {
        let outcomes = parse_exchange_response(
            200,
            r#"{"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}"#,
        )
        .unwrap();
        assert_eq!(outcomes, vec![ActionOutcome::Success]);

        // …and a cancel the venue refused says why, in the same slot.
        let outcomes = parse_exchange_response(
            200,
            r#"{"status":"ok","response":{"type":"cancel","data":{"statuses":[{"error":"Order was never placed, already canceled, or filled."}]}}}"#,
        )
        .unwrap();
        assert!(outcomes[0].error().unwrap().contains("never placed"));
    }

    /// **`422` answers in plain text, not JSON.** A strict `serde_json` error path fails on
    /// the error itself and reports a parse problem, hiding the real one — which is usually
    /// a malformed `cloid`.
    #[test]
    fn a_non_json_error_body_is_still_reported_verbatim() {
        let error = parse_exchange_response(
            422,
            "Failed to deserialize the JSON body into the target type: action.orders[0].c: \
             invalid length 30, expected a 0x-prefixed 32 hex character string",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("422"), "{error}");
        assert!(error.contains("32 hex character"), "{error}");
    }

    /// Throttling is an HTTP status, and the body's wording is not documented — so the
    /// status has to survive into the message rather than being swallowed by a parse.
    #[test]
    fn a_rate_limit_is_reported_as_itself() {
        let error = parse_exchange_response(429, "Too many requests").unwrap_err();
        assert!(error.to_string().contains("429"), "{error}");
        // A throttled request never reached the matching engine, so nothing rests and the
        // order is expired back to the bot rather than left as a phantom.
        assert!(error.is_venue_verdict(), "{error}");
    }

    /// **A `5xx` is not a rejection.** The venue sits behind an edge that drops about nine
    /// sockets a day; a `502` means the request may have been executed and the answer lost.
    /// Treating it as a verdict expires an order that is resting, and the bot re-quotes on
    /// top of it — the one failure `submit_order`'s asymmetry exists to prevent.
    #[test]
    fn a_gateway_failure_is_a_transport_failure_and_not_a_verdict() {
        for status in [500, 502, 503, 504, 408, 425] {
            let error = parse_exchange_response(status, "<html>Bad Gateway</html>").unwrap_err();
            assert!(
                !error.is_venue_verdict(),
                "HTTP {status} must not expire an order that may be resting: {error}"
            );
            assert!(error.to_string().contains(&status.to_string()), "{error}");
        }

        // A `200` whose body this backend cannot read is the same kind of unknown.
        let error = parse_exchange_response(200, "<html>Gateway timeout</html>").unwrap_err();
        assert!(!error.is_venue_verdict(), "{error}");

        // The venue's own refusals stay verdicts, at both levels.
        for (status, body) in [
            (400u16, "bad request"),
            (
                422,
                "invalid length 30, expected a 0x-prefixed 32 hex character string",
            ),
        ] {
            assert!(
                parse_exchange_response(status, body)
                    .unwrap_err()
                    .is_venue_verdict()
            );
        }
        assert!(
            parse_exchange_response(
                200,
                r#"{"status":"err","response":"User or API Wallet 0xf39f does not exist."}"#
            )
            .unwrap_err()
            .is_venue_verdict()
        );
    }

    /// **The sweep's action, byte for byte.** `cancel` uses the *short* field names `a`/`o`
    /// where `cancelByCloid` uses `asset`/`cloid`; the bytes below are `msgpack.packb` of
    /// what the official SDK's `cancel` builds, computed independently of this code.
    ///
    /// The failure it guards is the module's opening warning: a mis-serialised action signs
    /// perfectly, recovers to an address that does not exist, and the venue answers that the
    /// API wallet is unknown.
    #[test]
    fn the_cancel_by_oid_action_matches_the_official_sdks_bytes() {
        let action = CancelAction::new(vec![CancelWire {
            a: 3,
            o: 57118842019,
        }]);
        assert_eq!(
            hex(&rmp_serde::to_vec_named(&action).unwrap()),
            "82a474797065a663616e63656ca763616e63656c739182a16103a16fcf0000000d4c8c5ca3"
        );
        // …and the JSON body the same struct produces names the same fields.
        let body = serde_json::to_value(&action).unwrap();
        assert_eq!(body["type"], "cancel");
        assert_eq!(body["cancels"][0]["a"], 3);
        assert_eq!(body["cancels"][0]["o"], 57118842019u64);
    }

    /// **The fast-cancel flag serialises when set.** `f: true` is future mempool
    /// prioritisation with no effect today, but the byte it adds is signed — so the exact
    /// bytes are pinned the same way every other action's are. Mechanically this is the
    /// base `cancelByCloid` fixture with the leading fixmap header `82`→`83` (two entries
    /// become three) and the pair `a166 c3` — key `"f"`, value `true` — appended after
    /// `cancels`.
    ///
    /// **This is a dormant "would-be-correct-if-activated" pin, not the production path.** It
    /// builds `.fast()` on its own object; no production cancel sets `f` today
    /// (`private_stream::the_per_order_cancel_omits_the_fast_flag` pins that). Its value is
    /// that the wire is already correct the day fast-cancel is activated against the venue.
    #[test]
    fn a_fast_cancel_serialises_the_f_flag_true() {
        let cancel = CancelByCloidAction::new(vec![CancelByCloidWire {
            asset: 3,
            cloid: "0xa1b2000000000000000000000000dead".into(),
        }])
        .fast();
        assert_eq!(
            hex(&rmp_serde::to_vec_named(&cancel).unwrap()),
            concat!(
                "83a474797065ad63616e63656c4279436c6f6964a763616e63656c739182a5617373657403a563",
                "6c6f6964d92230786131623230303030303030303030303030303030303030303030303064656164",
                "a166c3"
            )
        );
        // `f` is a sibling of `cancels`, not nested inside a cancel — and it is `true`.
        let body = serde_json::to_value(&cancel).unwrap();
        assert_eq!(body["type"], "cancelByCloid");
        assert_eq!(body["f"], true);
        assert!(body["cancels"][0].get("f").is_none(), "{body}");
    }

    /// **The signing-critical half: a plain cancel omits `f` entirely.** The wire rule is
    /// "omit when false, present only when true" — an action hashed with `f: false` is
    /// rejected, and the signature covers `msgpack(action)`. So the default `new()` must
    /// produce bytes byte-identical to the pre-`f` fixture: leading `82`, no `a166` tail.
    #[test]
    fn a_plain_cancel_omits_the_f_flag() {
        let cancel = CancelByCloidAction::new(vec![CancelByCloidWire {
            asset: 3,
            cloid: "0xa1b2000000000000000000000000dead".into(),
        }]);
        assert_eq!(
            hex(&rmp_serde::to_vec_named(&cancel).unwrap()),
            concat!(
                "82a474797065ad63616e63656c4279436c6f6964a763616e63656c739182a5617373657403a563",
                "6c6f6964d92230786131623230303030303030303030303030303030303030303030303064656164"
            )
        );
        let body = serde_json::to_value(&cancel).unwrap();
        assert!(
            body.get("f").is_none(),
            "a false f must not appear at all: {body}"
        );
    }

    /// **The sealed live-cancel action is byte-identical to a plain `cancelByCloid`, and
    /// carries no `f`.** The live money path signs [`PlainCancelByCloid`], not a bare
    /// [`CancelByCloidAction`], precisely so no call site can chain `.fast()` onto the action
    /// it posts (`private_stream::cancel_action_for_order`). The seal must not change what the
    /// venue signs: these msgpack bytes are exactly what `CancelByCloidAction::new` produces —
    /// the `f`-omitted shape pinned against the SDK by `msgpack_bytes_match_the_official_sdk`
    /// and `a_plain_cancel_omits_the_f_flag` — so the signature is unaffected. If the
    /// `#[serde(transparent)]` seal ever diverged (an added wrapper key, or a `.fast()` folded
    /// into the constructor), the byte comparison and the `f`-absent assertion each fail here.
    #[test]
    fn the_sealed_live_cancel_is_byte_identical_and_carries_no_f() {
        let wires = vec![CancelByCloidWire {
            asset: 3,
            cloid: "0xa1b2000000000000000000000000dead".into(),
        }];
        let sealed = PlainCancelByCloid::new(wires.clone());
        let plain = CancelByCloidAction::new(wires);
        assert_eq!(
            hex(&rmp_serde::to_vec_named(&sealed).unwrap()),
            hex(&rmp_serde::to_vec_named(&plain).unwrap()),
            "the seal must sign byte-for-byte what a plain cancelByCloid signs"
        );
        let body = serde_json::to_value(&sealed).unwrap();
        assert_eq!(body["type"], "cancelByCloid");
        assert!(
            body.get("f").is_none(),
            "the sealed live cancel must omit f: {body}"
        );
    }

    /// An `ok` whose per-item shape this backend has never seen is **not** counted as a
    /// live order. The status vocabulary grows over time, and optimism here means quoting
    /// against orders that may not exist.
    #[test]
    fn an_unrecognised_status_shape_is_not_mistaken_for_success() {
        let outcomes = parse_exchange_response(
            200,
            r#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"waitingForTrigger":{"oid":9}}]}}}"#,
        )
        .unwrap();
        assert!(outcomes[0].error().is_some(), "{outcomes:?}");

        // A bare string that is not "success" is likewise not one.
        let outcomes = parse_exchange_response(
            200,
            r#"{"status":"ok","response":{"type":"cancel","data":{"statuses":["queued"]}}}"#,
        )
        .unwrap();
        assert!(outcomes[0].error().is_some(), "{outcomes:?}");
    }
}
