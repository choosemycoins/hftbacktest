//! Test doubles for driving the order path's slot end to end without a key, a subprocess or
//! the network: an in-process stand-in for the signer sidecar, and a local HTTP server that
//! plays the venue's REST order endpoints from a script and records what reached it.
//!
//! Neither signs anything. The "signed" transaction the stand-in hands back is a JSON note of
//! what was asked (`op`, `nonce`), so the scripted venue can say exactly which transactions
//! reached it and in what order — which is the observable every slot pin is about.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::Instant,
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
/// `identity`, "signs" a create/cancel/cancel-all as a JSON note of the op, the nonce it was
/// given and the hash it reports, and mints a dummy auth token. No key exists anywhere.
///
/// A `create_order` whose client order index is in `refuse` is refused, as a sidecar that could
/// not sign it would.
pub(crate) fn fake_signer(
    identity: SidecarIdentity,
    refuse: Arc<Mutex<HashSet<i64>>>,
) -> SignerClient {
    let (client_stdin, sidecar_stdin) = tokio::io::duplex(64 * 1024);
    let (mut sidecar_stdout, client_stdout) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let mut requests = BufReader::new(sidecar_stdin).lines();
        while let Ok(Some(line)) = requests.next_line().await {
            let request: Value = serde_json::from_str(&line).expect("the client sends JSON lines");
            let id = request["id"].as_u64().expect("every request has an id");
            let op = request["op"].as_str().unwrap_or_default().to_string();
            let refused = op == "create_order"
                && request["client_order_index"]
                    .as_i64()
                    .is_some_and(|coi| refuse.lock().unwrap().contains(&coi));
            let answer = match op.as_str() {
                _ if refused => json!({"id": id, "ok": false, "error": "scripted refusal"}),
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
                        "hash": format!("hash-{id}"),
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
    /// For a `sendTxBatch`: the `(op, nonce)` of each transaction in it, in order.
    pub(crate) batch: Vec<(String, i64)>,
    /// When it arrived.
    pub(crate) at: Instant,
}

/// A local HTTP server playing the venue's order-path REST endpoints.
///
/// * `GET /api/v1/nextNonce` — the venue's counter, which a `sendTx` answered 200 advances;
///   or, while [`ScriptedRestVenue::answer_next_nonce_with`] has queued statuses, the next one.
/// * `POST /api/v1/sendTx` — the next scripted answer, or a clean 200 ack once the script is
///   spent. A scripted status of [`DROP_CONNECTION`] answers nothing and closes the socket: a
///   transport error on the client's side.
/// * `POST /api/v1/sendTxBatch` — the next scripted [`BatchAnswer`], or [`BatchAnswer::Echo`]
///   once the script is spent.
/// * `GET /api/v1/accountActiveOrders` — an empty list (the venue holds nothing).
/// * `GET /api/v1/tx` — no verdict yet.
///
/// Every request is recorded, in arrival order.
pub(crate) struct ScriptedRestVenue {
    pub(crate) url: String,
    seen: Arc<Mutex<Vec<Seen>>>,
    send_tx_answers: Arc<Mutex<VecDeque<(u16, String)>>>,
    batch_answers: Arc<Mutex<VecDeque<BatchAnswer>>>,
    next_nonce_answers: Arc<Mutex<VecDeque<u16>>>,
}

/// How the scripted venue answers one `sendTxBatch`.
#[derive(Clone, Debug)]
pub(crate) enum BatchAnswer {
    /// A clean 200 echoing every transaction's hash; the counter moves past all of them.
    Echo,
    /// A 200 echoing the first transaction's hash only, as if the venue took that one and not
    /// the rest; the counter moves past the first.
    EchoFirst,
    /// This status and body; the counter does not move. [`DROP_CONNECTION`] answers nothing.
    Fixed(u16, String),
}

/// The scripted "status" that answers a `sendTx` with no response at all: the socket is closed
/// once the request has been read, so the client sees a transport error.
pub(crate) const DROP_CONNECTION: u16 = 0;

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
        let batch_answers: Arc<Mutex<VecDeque<BatchAnswer>>> = Arc::default();
        let next_nonce_answers: Arc<Mutex<VecDeque<u16>>> = Arc::default();
        let counter = Arc::new(Mutex::new(nonce));
        {
            let seen = seen.clone();
            let send_tx_answers = send_tx_answers.clone();
            let batch_answers = batch_answers.clone();
            let next_nonce_answers = next_nonce_answers.clone();
            tokio::spawn(async move {
                while let Ok((socket, _)) = listener.accept().await {
                    let script = Script {
                        seen: seen.clone(),
                        send_tx_answers: send_tx_answers.clone(),
                        batch_answers: batch_answers.clone(),
                        next_nonce_answers: next_nonce_answers.clone(),
                        counter: counter.clone(),
                    };
                    tokio::spawn(async move {
                        serve_one(socket, script).await;
                    });
                }
            });
        }
        Self {
            url,
            seen,
            send_tx_answers,
            batch_answers,
            next_nonce_answers,
        }
    }

    /// Queues the answers the next `sendTxBatch`s get, in order.
    pub(crate) fn answer_batch_with(&self, answers: impl IntoIterator<Item = BatchAnswer>) {
        self.batch_answers.lock().unwrap().extend(answers);
    }

    /// The `sendTxBatch`es that reached the venue so far, each as its `(op, nonce)`s.
    pub(crate) fn sent_batches(&self) -> Vec<Vec<(String, i64)>> {
        self.seen()
            .into_iter()
            .filter(|seen| seen.path == "/api/v1/sendTxBatch")
            .map(|seen| seen.batch)
            .collect()
    }

    /// Queues the answers the next `sendTx`s get, in order.
    pub(crate) fn answer_send_tx_with(&self, answers: impl IntoIterator<Item = (u16, String)>) {
        self.send_tx_answers.lock().unwrap().extend(answers);
    }

    /// Queues the statuses the next `nextNonce` reads get, in order; a status other than 200
    /// is an error answer, and 200 the counter as usual.
    pub(crate) fn answer_next_nonce_with(&self, statuses: impl IntoIterator<Item = u16>) {
        self.next_nonce_answers.lock().unwrap().extend(statuses);
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

    /// The `sendTx`s that reached the venue so far, as `(op, when it arrived)`.
    pub(crate) fn sent_at(&self) -> Vec<(String, Instant)> {
        self.seen()
            .into_iter()
            .filter(|seen| seen.path == "/api/v1/sendTx")
            .map(|seen| (seen.op.unwrap_or_default(), seen.at))
            .collect()
    }

    /// How many times `path` was asked for.
    pub(crate) fn count(&self, path: &str) -> usize {
        self.seen().iter().filter(|seen| seen.path == path).count()
    }
}

/// What one connection to the scripted venue answers from, and records into.
struct Script {
    seen: Arc<Mutex<Vec<Seen>>>,
    send_tx_answers: Arc<Mutex<VecDeque<(u16, String)>>>,
    batch_answers: Arc<Mutex<VecDeque<BatchAnswer>>>,
    next_nonce_answers: Arc<Mutex<VecDeque<u16>>>,
    counter: Arc<Mutex<i64>>,
}

async fn serve_one(mut socket: TcpStream, script: Script) {
    let Script {
        seen,
        send_tx_answers,
        batch_answers,
        next_nonce_answers,
        counter,
    } = script;
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
    // A batch's `tx_infos` is a JSON array of strings, each a `tx_info` (design note §3.7).
    let batch_infos: Vec<Value> = if path == "/api/v1/sendTxBatch" {
        let form: HashMap<String, String> = serde_qs::from_str(&body).unwrap_or_default();
        form.get("tx_infos")
            .and_then(|infos| serde_json::from_str::<Vec<String>>(infos).ok())
            .unwrap_or_default()
            .iter()
            .map(|info| serde_json::from_str(info).unwrap_or(Value::Null))
            .collect()
    } else {
        Vec::new()
    };
    seen.lock().unwrap().push(Seen {
        path: path.clone(),
        op,
        nonce,
        batch: batch_infos
            .iter()
            .map(|info| {
                (
                    info["op"].as_str().unwrap_or_default().to_string(),
                    info["nonce"].as_i64().unwrap_or(-1),
                )
            })
            .collect(),
        at: Instant::now(),
    });
    let (status, body) = match (method.as_str(), path.as_str()) {
        ("GET", "/api/v1/nextNonce") => match next_nonce_answers.lock().unwrap().pop_front() {
            Some(status) if status != 200 => (
                status,
                json!({"code": status, "message": "scripted"}).to_string(),
            ),
            _ => {
                let next = *counter.lock().unwrap();
                (200, json!({"code": 200, "nonce": next}).to_string())
            }
        },
        ("POST", "/api/v1/sendTx") => {
            let answer = send_tx_answers
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| send_tx_ack("Ratelimit is off"));
            if answer.0 == DROP_CONNECTION {
                // Read and recorded, never answered.
                return;
            }
            if answer.0 == 200 {
                *counter.lock().unwrap() += 1;
            }
            answer
        }
        ("POST", "/api/v1/sendTxBatch") => {
            let answer = batch_answers
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(BatchAnswer::Echo);
            let hashes: Vec<&str> = batch_infos
                .iter()
                .map(|info| info["hash"].as_str().unwrap_or_default())
                .collect();
            let echo = |hashes: &[&str]| {
                json!({
                    "code": 200,
                    "message": json!({ "ratelimit": "Ratelimit is off" }).to_string(),
                    "tx_hash": hashes,
                    "predicted_execution_time_ms": 1_785_431_671_831_i64,
                    "volume_quota_remaining": 100,
                })
                .to_string()
            };
            match answer {
                BatchAnswer::Echo => {
                    *counter.lock().unwrap() += hashes.len() as i64;
                    (200, echo(&hashes))
                }
                BatchAnswer::EchoFirst => {
                    *counter.lock().unwrap() += 1;
                    (200, echo(&hashes[..1.min(hashes.len())]))
                }
                BatchAnswer::Fixed(DROP_CONNECTION, _) => return,
                BatchAnswer::Fixed(status, body) => (status, body),
            }
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
