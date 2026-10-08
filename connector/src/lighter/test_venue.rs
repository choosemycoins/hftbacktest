//! Test doubles for driving the order path's slot end to end without a key, a subprocess or
//! the network: an in-process stand-in for the signer sidecar, and a local HTTP server that
//! plays the venue's REST order endpoints from a script and records what reached it.
//!
//! Neither signs anything. The "signed" transaction the stand-in hands back is a JSON note of
//! what was asked (`op`, `nonce`), so the scripted venue can say exactly which transactions
//! reached it and in what order — which is the observable every slot pin is about.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

use crate::lighter::signer::SignerClient;

/// What the stand-in sidecar says it signs for, in its `ping` answer.
pub(crate) struct SidecarIdentity {
    pub(crate) url: String,
    pub(crate) chain_id: u32,
    pub(crate) account_index: i64,
    pub(crate) api_key_index: u8,
}

/// The venue's tx type for each signer op (`CreateOrder` 14, `CancelOrder` 15,
/// `CancelAllOrders` 16).
fn tx_type_of(op: &str) -> u8 {
    match op {
        "create_order" => 14,
        "cancel_order" => 15,
        _ => 16,
    }
}

/// A [`SignerClient`] whose other end is a task in this process: it answers `ping` with
/// `identity`, "signs" a create/cancel/cancel-all as a JSON note of the op and the nonce it was
/// given, and mints a dummy auth token. No key exists anywhere.
pub(crate) fn fake_signer(identity: SidecarIdentity) -> SignerClient {
    let (client_stdin, sidecar_stdin) = tokio::io::duplex(64 * 1024);
    let (mut sidecar_stdout, client_stdout) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let mut requests = BufReader::new(sidecar_stdin).lines();
        while let Ok(Some(line)) = requests.next_line().await {
            let request: Value = serde_json::from_str(&line).expect("the client sends JSON lines");
            let id = request["id"].as_u64().expect("every request has an id");
            let op = request["op"].as_str().unwrap_or_default().to_string();
            let answer = match op.as_str() {
                "ping" => json!({
                    "id": id,
                    "ok": true,
                    "chain_id": identity.chain_id,
                    "url": identity.url,
                    "account_index": identity.account_index,
                    "api_key_slots": [identity.api_key_index],
                }),
                "create_order" | "cancel_order" | "cancel_all" => json!({
                    "id": id,
                    "ok": true,
                    "tx_type": tx_type_of(&op),
                    "tx_info": json!({
                        "op": op,
                        "nonce": request["nonce"],
                        "client_order_index": request["client_order_index"],
                        "market_index": request["market_index"],
                    })
                    .to_string(),
                    "tx_hash": format!("hash-{id}"),
                }),
                "auth_token" => json!({"id": id, "ok": true, "auth": "test-auth-token"}),
                other => json!({"id": id, "ok": false, "error": format!("unknown op {other}")}),
            };
            let mut text = answer.to_string();
            text.push('\n');
            if sidecar_stdout.write_all(text.as_bytes()).await.is_err() {
                break;
            }
        }
    });
    SignerClient::over(client_stdin, client_stdout)
}

/// One request the scripted venue received.
#[derive(Clone, Debug)]
pub(crate) struct Seen {
    pub(crate) path: String,
    /// For a `sendTx`: the `op` and `nonce` the stand-in signer wrote into `tx_info`.
    pub(crate) op: Option<String>,
    pub(crate) nonce: Option<i64>,
}

/// A local HTTP server playing the venue's order-path REST endpoints.
///
/// * `GET /api/v1/nextNonce` — the venue's counter, which a `sendTx` answered 200 advances.
/// * `POST /api/v1/sendTx` — the next scripted answer, or a clean 200 ack once the script is
///   spent.
/// * `GET /api/v1/accountActiveOrders` — an empty list (the venue holds nothing).
/// * `GET /api/v1/tx` — no verdict yet.
///
/// Every request is recorded, in arrival order.
pub(crate) struct ScriptedRestVenue {
    pub(crate) url: String,
    seen: Arc<Mutex<Vec<Seen>>>,
    send_tx_answers: Arc<Mutex<VecDeque<(u16, String)>>>,
}

/// A clean `sendTx` 200, carrying the venue's rate-limit report the way it does (§2.7).
pub(crate) fn send_tx_ack(ratelimit: &str) -> (u16, String) {
    let message = json!({ "ratelimit": ratelimit }).to_string();
    (
        200,
        json!({
            "code": 200,
            "message": message,
            "tx_hash": "e38dd4f2150419f1b407d1aaa4d494c71ac4cb0d",
            "predicted_execution_time_ms": 1_785_431_671_831_i64,
        })
        .to_string(),
    )
}

impl ScriptedRestVenue {
    /// Starts the server. `nonce` is what `nextNonce` answers first.
    pub(crate) async fn start(nonce: i64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let send_tx_answers: Arc<Mutex<VecDeque<(u16, String)>>> = Arc::default();
        let counter = Arc::new(Mutex::new(nonce));
        {
            let seen = seen.clone();
            let send_tx_answers = send_tx_answers.clone();
            tokio::spawn(async move {
                while let Ok((socket, _)) = listener.accept().await {
                    let seen = seen.clone();
                    let send_tx_answers = send_tx_answers.clone();
                    let counter = counter.clone();
                    tokio::spawn(async move {
                        serve_one(socket, seen, send_tx_answers, counter).await;
                    });
                }
            });
        }
        Self {
            url,
            seen,
            send_tx_answers,
        }
    }

    /// Queues the answers the next `sendTx`s get, in order.
    pub(crate) fn answer_send_tx_with(&self, answers: impl IntoIterator<Item = (u16, String)>) {
        self.send_tx_answers.lock().unwrap().extend(answers);
    }

    /// Everything that reached the venue so far.
    pub(crate) fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// The `sendTx`s that reached the venue so far, as `(op, nonce)`.
    pub(crate) fn sent(&self) -> Vec<(String, i64)> {
        self.seen()
            .into_iter()
            .filter(|seen| seen.path == "/api/v1/sendTx")
            .map(|seen| (seen.op.unwrap_or_default(), seen.nonce.unwrap_or(-1)))
            .collect()
    }

    /// How many times `path` was asked for.
    pub(crate) fn count(&self, path: &str) -> usize {
        self.seen().iter().filter(|seen| seen.path == path).count()
    }
}

async fn serve_one(
    mut socket: TcpStream,
    seen: Arc<Mutex<Vec<Seen>>>,
    send_tx_answers: Arc<Mutex<VecDeque<(u16, String)>>>,
    counter: Arc<Mutex<i64>>,
) {
    let Some((method, target, body)) = read_request(&mut socket).await else {
        return;
    };
    let path = target.split('?').next().unwrap_or_default().to_string();
    let (op, nonce) = if path == "/api/v1/sendTx" {
        let form: HashMap<String, String> = serde_qs::from_str(&body).unwrap_or_default();
        let info: Value = form
            .get("tx_info")
            .and_then(|info| serde_json::from_str(info).ok())
            .unwrap_or(Value::Null);
        (
            info["op"].as_str().map(str::to_string),
            info["nonce"].as_i64(),
        )
    } else {
        (None, None)
    };
    seen.lock().unwrap().push(Seen {
        path: path.clone(),
        op,
        nonce,
    });
    let (status, body) = match (method.as_str(), path.as_str()) {
        ("GET", "/api/v1/nextNonce") => {
            let next = *counter.lock().unwrap();
            (200, json!({"code": 200, "nonce": next}).to_string())
        }
        ("POST", "/api/v1/sendTx") => {
            let answer = send_tx_answers
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| send_tx_ack("Ratelimit is off"));
            if answer.0 == 200 {
                *counter.lock().unwrap() += 1;
            }
            answer
        }
        ("GET", "/api/v1/accountActiveOrders") => {
            (200, json!({"code": 200, "orders": []}).to_string())
        }
        ("GET", "/api/v1/tx") => (200, json!({"event_info": ""}).to_string()),
        _ => (404, json!({"code": 404, "message": "not here"}).to_string()),
    };
    let response = format!(
        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.shutdown().await;
}

/// Reads one HTTP/1.1 request: the method, the target and the body (by `Content-Length`).
async fn read_request(socket: &mut TcpStream) -> Option<(String, String, String)> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(at) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break at + 4;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..header_end]).to_string();
    let mut lines = head.lines();
    let mut request_line = lines.next()?.split_whitespace();
    let method = request_line.next()?.to_string();
    let target = request_line.next()?.to_string();
    let length = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    while buffer.len() < header_end + length {
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    let body = String::from_utf8_lossy(&buffer[header_end..]).to_string();
    Some((method, target, body))
}
