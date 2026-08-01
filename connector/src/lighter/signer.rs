//! Client to the Lighter signer sidecar (design note
//! [`docs/design-lighter-connector.md`](../../../docs/design-lighter-connector.md) §3.2).
//!
//! The signer is **not** a composition of ready primitives — it is Schnorr over ECgFp5
//! with a Poseidon2-Goldilocks (Plonky2 variant) hash, shipped as a native Go c-archive
//! and a Python SDK that wraps it. §3.2 decided a **sidecar process** over a Rust port:
//! the private key lives ONLY in the sidecar, which reads it from its own config file;
//! it never enters the connector's address space, never rides on argv, and is never
//! logged. This module is the connector's end of that boundary — it spawns the sidecar
//! and speaks line-delimited JSON over its stdin/stdout, one request at a time.
//!
//! One-in-flight is deliberate, not a limitation: §4.12 says the venue accepts one tx
//! per api-key slot in flight, and the nonce owner (§3.3) already serialises a slot's
//! traffic. Parallelism comes from provisioning more slots (more sidecar keys), each
//! with its own [`SignerClient`], not from pipelining one.
//!
//! ## What comes back
//!
//! [`Signed`] carries `tx_type`, `tx_info` (the JSON the connector POSTs to `sendTx`) and
//! `tx_hash`. The hash is a pure function of the tx fields incl. `ExpiredAt`; the
//! signature is non-deterministic and is **not** a stable identifier (§3.2). `ExpiredAt`
//! is set from the clock inside the native library (a 10-minute tx lifetime) and cannot
//! be supplied — it comes back inside `tx_info`.
//!
//! ## What this is NOT
//!
//! A `sendTx` HTTP 200 is not a verdict (§4.11): a rejected tx still gets 200, spends a
//! nonce and produces nothing on the private channel. Acceptance is keyed off
//! `event_info.ae`, not off anything here. This module only signs.

use std::process::Stdio;

use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
};

#[derive(Error, Debug)]
pub enum SignerError {
    /// The sidecar answered `ok:false` — a signing refusal (e.g. a field failed the
    /// native library's validation). Recoverable; the caller decides policy.
    #[error("signer rejected: {0}")]
    Rejected(String),
    /// The response did not answer the request we sent (id mismatch). The sidecar
    /// answers in order, one per request, so this means the stream desynchronised.
    #[error("response id mismatch: expected {expected}, got {got:?}")]
    IdMismatch { expected: u64, got: Option<u64> },
    /// A response line that is not the shape this client can read.
    #[error("malformed response: {0}")]
    Malformed(String),
    /// The sidecar closed its stdout — it exited (fail-closed setup failure, or crash).
    #[error("signer sidecar closed the connection")]
    Closed,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// A signed transaction ready to be POSTed to `sendTx`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signed {
    /// Lighter tx type: 14 CreateOrder, 15 CancelOrder, 16 CancelAllOrders.
    pub tx_type: u8,
    /// The tx JSON to POST verbatim. Carries `ExpiredAt`, `Nonce`, `Sig`.
    pub tx_info: String,
    /// The 40-byte tx hash, hex (80 chars). Pure function of the fields, not the signature.
    pub tx_hash: String,
}

/// A `create_order` request. `nonce` and `api_key_index` are owned by the caller (the
/// nonce owner, §3.3) and supplied per send. Integrator / self-trade knobs default to 0.
#[derive(Debug, Clone)]
pub struct CreateOrder {
    pub market_index: i16,
    pub client_order_index: i64,
    pub base_amount: i64,
    pub price: u32,
    pub is_ask: u8,
    /// 0 LIMIT, 1 MARKET, … (`SignerClient` constants live venue-side).
    pub order_type: u8,
    /// 0 IOC, 1 GTT, 2 POST_ONLY. PostOnly is the ALO-equivalent (§3.5).
    pub time_in_force: u8,
    pub reduce_only: u8,
    pub trigger_price: u32,
    /// Order lifetime in ms, or -1 for the library default (28 days).
    pub order_expiry: i64,
}

impl CreateOrder {
    fn params(&self) -> Value {
        json!({
            "market_index": self.market_index,
            "client_order_index": self.client_order_index,
            "base_amount": self.base_amount,
            "price": self.price,
            "is_ask": self.is_ask,
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "reduce_only": self.reduce_only,
            "trigger_price": self.trigger_price,
            "order_expiry": self.order_expiry,
        })
    }
}

// ---- pure codec (unit-tested without a subprocess) ---------------------------------

/// Build the JSON request line for an op with the given params, id and slot.
fn request_line(
    id: u64,
    op: &str,
    api_key_index: u8,
    nonce: Option<i64>,
    mut params: Value,
) -> String {
    let obj = params.as_object_mut().expect("params is a JSON object");
    obj.insert("id".into(), json!(id));
    obj.insert("op".into(), json!(op));
    obj.insert("api_key_index".into(), json!(api_key_index));
    if let Some(n) = nonce {
        obj.insert("nonce".into(), json!(n));
    }
    let mut s = params.to_string();
    s.push('\n');
    s
}

/// Parse a response envelope: verify it answers `expect_id`, and either return the
/// object (on `ok:true`) or map `ok:false` to [`SignerError::Rejected`].
fn parse_envelope(
    line: &str,
    expect_id: u64,
) -> Result<serde_json::Map<String, Value>, SignerError> {
    let v: Value =
        serde_json::from_str(line.trim()).map_err(|e| SignerError::Malformed(e.to_string()))?;
    let obj = v
        .as_object()
        .ok_or_else(|| SignerError::Malformed("response is not a JSON object".into()))?;
    let got = obj.get("id").and_then(Value::as_u64);
    if got != Some(expect_id) {
        return Err(SignerError::IdMismatch {
            expected: expect_id,
            got,
        });
    }
    match obj.get("ok").and_then(Value::as_bool) {
        Some(true) => Ok(obj.clone()),
        Some(false) => {
            let msg = obj
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            Err(SignerError::Rejected(msg.to_string()))
        }
        None => Err(SignerError::Malformed(
            "response has no boolean `ok`".into(),
        )),
    }
}

fn parse_signed(line: &str, expect_id: u64) -> Result<Signed, SignerError> {
    let obj = parse_envelope(line, expect_id)?;
    let tx_type = obj
        .get("tx_type")
        .and_then(Value::as_u64)
        .ok_or_else(|| SignerError::Malformed("missing tx_type".into()))? as u8;
    let tx_info = obj
        .get("tx_info")
        .and_then(Value::as_str)
        .ok_or_else(|| SignerError::Malformed("missing tx_info".into()))?
        .to_string();
    let tx_hash = obj
        .get("tx_hash")
        .and_then(Value::as_str)
        .ok_or_else(|| SignerError::Malformed("missing tx_hash".into()))?
        .to_string();
    Ok(Signed {
        tx_type,
        tx_info,
        tx_hash,
    })
}

fn parse_auth(line: &str, expect_id: u64) -> Result<String, SignerError> {
    let obj = parse_envelope(line, expect_id)?;
    obj.get("auth")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| SignerError::Malformed("missing auth".into()))
}

// ---- the process client -------------------------------------------------------------

/// Build the [`Command`] that runs the Python sidecar. The config path is the file the
/// sidecar opens to read its key — it is NOT the key. `LIGHTER_SDK_PATH` can point the
/// sidecar at a non-default SDK install (tests); omit in production where the sidecar's
/// venv has `lighter-sdk`.
pub fn python_sidecar_command(python: &str, script: &str, config: &str) -> Command {
    let mut cmd = Command::new(python);
    cmd.arg(script).arg(config);
    cmd
}

/// A running signer sidecar. Owns the child's stdin and a line reader over its stdout.
/// Not `Clone`: one client owns one sidecar owns one api-key slot's signing (§4.12).
pub struct SignerClient {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    next_id: u64,
}

impl SignerClient {
    /// Spawn the sidecar. The command is configured by the caller (see
    /// [`python_sidecar_command`]); this wires up the piped stdio.
    pub async fn spawn(mut command: Command) -> Result<Self, SignerError> {
        command.stdin(Stdio::piped()).stdout(Stdio::piped());
        // stderr is left inherited so the sidecar's operator lines (never keys) show up
        // in the connector's log stream.
        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| SignerError::Malformed("no stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SignerError::Malformed("no stdout".into()))?;
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
            next_id: 1,
        })
    }

    async fn round_trip(&mut self, line: String) -> Result<String, SignerError> {
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.flush().await?;
        match self.stdout.next_line().await? {
            Some(resp) => Ok(resp),
            None => Err(SignerError::Closed),
        }
    }

    fn take_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Liveness probe: `{"op":"ping"}` → `ok`.
    pub async fn ping(&mut self) -> Result<(), SignerError> {
        let id = self.take_id();
        let line = request_line(id, "ping", 0, None, json!({}));
        let resp = self.round_trip(line).await?;
        parse_envelope(&resp, id).map(|_| ())
    }

    pub async fn create_order(
        &mut self,
        order: &CreateOrder,
        nonce: i64,
        api_key_index: u8,
    ) -> Result<Signed, SignerError> {
        let id = self.take_id();
        let line = request_line(
            id,
            "create_order",
            api_key_index,
            Some(nonce),
            order.params(),
        );
        let resp = self.round_trip(line).await?;
        parse_signed(&resp, id)
    }

    pub async fn cancel_order(
        &mut self,
        market_index: i16,
        order_index: i64,
        nonce: i64,
        api_key_index: u8,
    ) -> Result<Signed, SignerError> {
        let id = self.take_id();
        let params = json!({ "market_index": market_index, "order_index": order_index });
        let line = request_line(id, "cancel_order", api_key_index, Some(nonce), params);
        let resp = self.round_trip(line).await?;
        parse_signed(&resp, id)
    }

    /// Cancel-all for a market (or 255 for all markets). `time_in_force` 0 IMMEDIATE
    /// (then `time_ms` must be 0), 1 SCHEDULED, 2 ABORT.
    pub async fn cancel_all(
        &mut self,
        time_in_force: i32,
        time_ms: i64,
        market_index: i32,
        nonce: i64,
        api_key_index: u8,
    ) -> Result<Signed, SignerError> {
        let id = self.take_id();
        let params = json!({
            "time_in_force": time_in_force,
            "time_ms": time_ms,
            "market_index": market_index,
        });
        let line = request_line(id, "cancel_all", api_key_index, Some(nonce), params);
        let resp = self.round_trip(line).await?;
        parse_signed(&resp, id)
    }

    /// Mint a websocket auth token that expires at `deadline` (absolute unix seconds;
    /// server ceiling 8h, refresh before expiry — §3.4).
    pub async fn auth_token(
        &mut self,
        deadline: i64,
        api_key_index: u8,
    ) -> Result<String, SignerError> {
        let id = self.take_id();
        let line = request_line(
            id,
            "auth_token",
            api_key_index,
            None,
            json!({ "deadline": deadline }),
        );
        let resp = self.round_trip(line).await?;
        parse_auth(&resp, id)
    }

    /// Close the sidecar (drop its stdin so it sees EOF, then reap).
    pub async fn shutdown(mut self) -> Result<(), SignerError> {
        drop(self.stdin);
        let _ = self.child.wait().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    fn parse_line(line: &str) -> Value {
        serde_json::from_str(line.trim()).unwrap()
    }

    #[test]
    fn create_order_request_carries_op_id_slot_nonce_and_fields() {
        let o = CreateOrder {
            market_index: 1,
            client_order_index: 123456789,
            base_amount: 1000,
            price: 6500000,
            is_ask: 0,
            order_type: 0,
            time_in_force: 2,
            reduce_only: 0,
            trigger_price: 0,
            order_expiry: 1800000000000,
        };
        let line = request_line(7, "create_order", 5, Some(42), o.params());
        assert!(line.ends_with('\n'), "requests are newline-delimited");
        let v = parse_line(&line);
        assert_eq!(v["op"], "create_order");
        assert_eq!(v["id"], 7);
        assert_eq!(v["api_key_index"], 5);
        assert_eq!(v["nonce"], 42);
        assert_eq!(v["market_index"], 1);
        assert_eq!(v["client_order_index"], 123456789i64);
        assert_eq!(v["price"], 6500000);
        assert_eq!(v["time_in_force"], 2);
        assert_eq!(v["order_expiry"], 1800000000000i64);
    }

    #[test]
    fn cancel_order_request_names_market_order_and_nonce() {
        let v = parse_line(&request_line(
            3,
            "cancel_order",
            5,
            Some(43),
            json!({ "market_index": 1i16, "order_index": 987654321i64 }),
        ));
        assert_eq!(v["op"], "cancel_order");
        assert_eq!(v["id"], 3);
        assert_eq!(v["api_key_index"], 5);
        assert_eq!(v["nonce"], 43);
        assert_eq!(v["order_index"], 987654321i64);
    }

    #[test]
    fn cancel_all_request_carries_tif_time_and_market() {
        let v = parse_line(&request_line(
            9,
            "cancel_all",
            6,
            Some(44),
            json!({ "time_in_force": 0, "time_ms": 0i64, "market_index": 255 }),
        ));
        assert_eq!(v["op"], "cancel_all");
        assert_eq!(v["time_in_force"], 0);
        assert_eq!(v["time_ms"], 0);
        assert_eq!(v["market_index"], 255);
        assert_eq!(v["api_key_index"], 6);
        assert_eq!(v["nonce"], 44);
    }

    #[test]
    fn auth_token_request_carries_deadline_and_has_no_nonce() {
        let v = parse_line(&request_line(
            2,
            "auth_token",
            5,
            None,
            json!({ "deadline": 1785535014i64 }),
        ));
        assert_eq!(v["op"], "auth_token");
        assert_eq!(v["deadline"], 1785535014i64);
        assert_eq!(v["api_key_index"], 5);
        assert!(v.get("nonce").is_none(), "auth tokens do not spend a nonce");
    }

    #[test]
    fn success_response_parses_to_signed() {
        // Shape captured from the live sidecar (fictional key).
        let line = r#"{"ok": true, "tx_type": 14, "tx_info": "{\"Nonce\":42}", "tx_hash": "fb6ecb13d49cddca1722f76e1905f7c8e829dac55a3bee2b99ba685c68854c523515548b9f93916b", "id": 7}"#;
        let s = parse_signed(line, 7).unwrap();
        assert_eq!(s.tx_type, 14);
        assert_eq!(s.tx_info, "{\"Nonce\":42}");
        assert_eq!(s.tx_hash.len(), 80, "hash is 40 bytes = 80 hex chars");
    }

    #[test]
    fn rejection_response_is_rejected_error_not_success() {
        // §4.11's cousin: a duplicate ClientOrderIndex is refused at sign/send time. The
        // signer surfaces refusals as an error the caller must not read as acceptance.
        let line = r#"{"ok": false, "error": "duplicate ClientOrderIndex", "id": 7}"#;
        match parse_signed(line, 7) {
            Err(SignerError::Rejected(m)) => assert!(m.contains("duplicate")),
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn auth_response_yields_the_token() {
        let line = r#"{"ok": true, "auth": "1785535014:1:2:deadbeef", "id": 2}"#;
        assert_eq!(parse_auth(line, 2).unwrap(), "1785535014:1:2:deadbeef");
    }

    #[test]
    fn a_response_for_a_different_id_is_a_mismatch_not_a_result() {
        // Responses are in-order, one per request; a wrong id means desync, never a value.
        let line = r#"{"ok": true, "tx_type": 14, "tx_info": "x", "tx_hash": "y", "id": 99}"#;
        match parse_signed(line, 7) {
            Err(SignerError::IdMismatch {
                expected: 7,
                got: Some(99),
            }) => {}
            other => panic!("expected IdMismatch, got {other:?}"),
        }
    }

    #[test]
    fn ok_false_without_error_field_still_fails_closed() {
        let line = r#"{"ok": false, "id": 7}"#;
        assert!(matches!(
            parse_signed(line, 7),
            Err(SignerError::Rejected(_))
        ));
    }

    #[test]
    fn a_response_missing_ok_is_malformed() {
        let line = r#"{"tx_type": 14, "id": 7}"#;
        assert!(matches!(
            parse_signed(line, 7),
            Err(SignerError::Malformed(_))
        ));
    }

    #[test]
    fn garbage_line_is_malformed_not_a_panic() {
        assert!(matches!(
            parse_signed("not json", 1),
            Err(SignerError::Malformed(_))
        ));
    }

    // Live round-trip against the real sidecar. Ignored by default: needs a Python venv
    // with `lighter-sdk` and the native library. Set the three env vars and run with
    // `--ignored`. Uses the FICTIONAL golden key via LIGHTER_SIGNER_TEST_CONFIG — never a
    // real account.
    #[tokio::test]
    #[ignore]
    async fn roundtrip_against_real_sidecar() {
        let python =
            std::env::var("LIGHTER_SIGNER_TEST_PYTHON").expect("LIGHTER_SIGNER_TEST_PYTHON");
        let script =
            std::env::var("LIGHTER_SIGNER_TEST_SCRIPT").expect("LIGHTER_SIGNER_TEST_SCRIPT");
        let config =
            std::env::var("LIGHTER_SIGNER_TEST_CONFIG").expect("LIGHTER_SIGNER_TEST_CONFIG");
        let cmd = python_sidecar_command(&python, &script, &config);
        let mut client = SignerClient::spawn(cmd).await.unwrap();
        client.ping().await.unwrap();
        let order = CreateOrder {
            market_index: 1,
            client_order_index: 123456789,
            base_amount: 1000,
            price: 6500000,
            is_ask: 0,
            order_type: 0,
            time_in_force: 2,
            reduce_only: 0,
            trigger_price: 0,
            order_expiry: 1800000000000,
        };
        let signed = client.create_order(&order, 42, 2).await.unwrap();
        assert_eq!(signed.tx_type, 14);
        assert_eq!(signed.tx_hash.len(), 80);
        assert!(signed.tx_info.contains("\"Nonce\":42"));
        // A duplicate/invalid slot must surface as an error, never as a silent success.
        let bad = client.create_order(&order, 42, 99).await;
        assert!(matches!(bad, Err(SignerError::Rejected(_))));
        client.shutdown().await.unwrap();
    }
}
