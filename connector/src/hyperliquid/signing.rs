//! Signing an L1 action: a msgpack hash wrapped in an EIP-712 "phantom agent".
//!
//! Every mistake in this file produces the **same** venue answer — a signature that
//! ecrecovers to an address Hyperliquid has never heard of, reported as
//! `"User or API Wallet 0x… does not exist"`. It never says which of the six ways you got
//! there. Each one below cost someone days:
//!
//! 1. **One hash, not two.** `connection_id = keccak(msgpack ‖ nonce_be8 ‖ 0x00)`. Hashing
//!    the msgpack first and concatenating the digest is the natural-looking version and is
//!    wrong.
//! 2. **`chainId` is 1337 on both networks**, and `verifyingContract` is the zero address.
//!    Mainnet and testnet differ *only* in the agent's `source` field, `"a"` / `"b"`.
//! 3. **The domain is `Exchange`**, not `HyperliquidSignTransaction` — that one is for
//!    user-signed actions, not L1 trading.
//! 4. **msgpack key order is part of the protocol.** The hash is over the serialised bytes,
//!    so a reordered struct field changes the hash and nothing fails to compile. See
//!    [`crate::hyperliquid::exchange`], where the action structs carry that warning.
//! 5. **Numbers are canonical strings.** `"108"`, never `"108.0"` — see
//!    [`hashing_string`], which is the single most expensive line in this module's history.
//! 6. **`v` is `27 + parity`**, and `r`/`s` are 32-byte big-endian hex.
//!
//! Everything here is pure and pinned by golden vectors taken from the **official
//! `hyperliquid-python-sdk`** (`utils/signing.py`), generated with the well-known Hardhat
//! test key. `AGENTS.md` §1.3 asks for test-first on anything touching money; a signing bug
//! is not a wrong trade, it is every trade silently refused.
//!
//! Out of scope, deliberately (design note §2): vaults and sub-accounts. The vault flag is
//! therefore the constant byte `0x00` rather than a parameter — see [`connection_id`].

use std::sync::atomic::{AtomicU64, Ordering};

use k256::ecdsa::{RecoveryId, Signature as EcdsaSignature, SigningKey, VerifyingKey};
use serde::Serialize;
use sha3::{Digest, Keccak256};
use tracing::warn;

use crate::hyperliquid::HyperliquidError;

/// How far the stored nonce may fall behind the wall clock before it is worth a warning.
///
/// The acceptance window is two days wide, so an hour of drift is not yet a failure — it is
/// the only chance to see one coming. See [`NonceSource`].
const NONCE_DRIFT_WARN_MS: u64 = 3_600_000;

/// `keccak256`, the hash the whole EIP-712 construction is built from.
fn keccak(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

/// The canonical decimal string for a price or a size, as the venue re-derives it.
///
/// The rendering is the official SDKs': eight decimal places, trailing zeros stripped,
/// trailing dot stripped, `-0` normalised to `0`.
///
/// **This is the trap that hides as a rounding preference.** The number goes into the
/// action as a *string*, so `"108.0"` and `"108"` are different msgpack bytes, a different
/// keccak, a different recovered address — and the venue answers "that wallet does not
/// exist", which reads like a key or approval problem and is neither. A week was spent on
/// that diagnosis once already.
///
/// **Why this returns a `Result` when the Rust SDK's equivalent does not.** The two
/// official SDKs disagree about a value that needs more than eight decimals to write down:
/// the Rust one rounds it silently, the Python one raises. Measured against both, e.g.
/// `0.123456789` → Rust `"0.12345679"`, Python `ValueError`. Rounding silently means the
/// venue rests a *different order* than the bot asked for, with nothing anywhere reporting
/// the difference — which `AGENTS.md` §1.1 rules out, so this takes the Python behaviour.
/// Normalisation (`super::normalize`) puts every price and size on a grid of at most six
/// decimals before this is ever called, so the refusal is a guard on an unreachable case
/// rather than an ordinary outcome; a non-zero count of it means normalisation is wrong.
///
/// The two SDKs also disagree about `-0.0` (Rust `"0"`, Python `"-0"`). This follows the
/// Rust SDK, and the submit path refuses non-positive prices and sizes long before here.
pub fn hashing_string(value: f64) -> Result<String, HyperliquidError> {
    if !value.is_finite() {
        return Err(HyperliquidError::Signing(format!(
            "{value} cannot be written as a Hyperliquid wire number"
        )));
    }
    let mut rendered = format!("{value:.8}");
    while rendered.ends_with('0') {
        rendered.pop();
    }
    if rendered.ends_with('.') {
        rendered.pop();
    }
    if rendered == "-0" {
        rendered = "0".to_string();
    }
    // The venue signs the *string*. If it does not name the value that was asked for, the
    // order that rests is not the order that was requested, and no later event says so.
    let parsed: f64 = rendered.parse().map_err(|_| {
        HyperliquidError::Signing(format!(
            "{value} rendered as {rendered:?}, which is not a number"
        ))
    })?;
    if (parsed - value).abs() >= 1e-12 {
        return Err(HyperliquidError::Signing(format!(
            "{value} needs more than 8 decimal places; it would be sent as {rendered:?}, a \
             different order from the one requested"
        )));
    }
    Ok(rendered)
}

/// `keccak(msgpack(action) ‖ nonce:u64be ‖ 0x00)`.
///
/// **One hash over the concatenation**, not a hash of a hash. The trailing `0x00` is the
/// "no vault" flag; a vault action would carry `0x01 ‖ vault_address`, which this backend
/// does not build (design note §2).
pub fn connection_id(action_msgpack: &[u8], nonce: u64) -> [u8; 32] {
    let mut buffer = Vec::with_capacity(action_msgpack.len() + 9);
    buffer.extend_from_slice(action_msgpack);
    buffer.extend_from_slice(&nonce.to_be_bytes());
    buffer.push(0x00);
    keccak(&buffer)
}

/// The EIP-712 domain separator: `Exchange` / `1` / chainId **1337** / `address(0)`.
///
/// The same for mainnet and testnet — the network lives in the agent's `source` field and
/// nowhere else. A `chainId` "corrected" to 42161 or 998 produces a perfectly well-formed
/// signature for an address that does not exist.
fn domain_separator() -> [u8; 32] {
    let type_hash = keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let mut encoded = Vec::with_capacity(32 * 5);
    encoded.extend_from_slice(&type_hash);
    encoded.extend_from_slice(&keccak(b"Exchange"));
    encoded.extend_from_slice(&keccak(b"1"));
    encoded.extend_from_slice(&u256_be(1337));
    // `verifyingContract` is an address, left-padded to a 32-byte word: the zero address.
    encoded.extend_from_slice(&[0u8; 32]);
    keccak(&encoded)
}

/// A `uint256` as a 32-byte big-endian word.
fn u256_be(value: u64) -> [u8; 32] {
    let mut word = [0u8; 32];
    word[24..].copy_from_slice(&value.to_be_bytes());
    word
}

/// The hash actually signed: EIP-712 over `Agent { string source; bytes32 connectionId; }`.
///
/// `source` is the **only** thing distinguishing mainnet from testnet, which is why
/// [`crate::hyperliquid::Config::is_mainnet`] must agree with the URLs beside it.
pub fn phantom_agent_hash(connection_id: &[u8; 32], is_mainnet: bool) -> [u8; 32] {
    let type_hash = keccak(b"Agent(string source,bytes32 connectionId)");
    let source: &[u8] = if is_mainnet { b"a" } else { b"b" };

    let mut encoded = Vec::with_capacity(32 * 3);
    encoded.extend_from_slice(&type_hash);
    encoded.extend_from_slice(&keccak(source));
    encoded.extend_from_slice(connection_id);
    let struct_hash = keccak(&encoded);

    let mut prefixed = Vec::with_capacity(2 + 64);
    prefixed.extend_from_slice(&[0x19, 0x01]);
    prefixed.extend_from_slice(&domain_separator());
    prefixed.extend_from_slice(&struct_hash);
    keccak(&prefixed)
}

/// The signature as `/exchange` wants it: `{"r":"0x…","s":"0x…","v":27|28}`.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Signature {
    pub r: String,
    pub s: String,
    pub v: u8,
}

/// `0x`-prefixed lowercase hex.
fn hex_0x(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Parses `0x`-prefixed (or bare) hex into exactly `N` bytes.
fn parse_hex<const N: usize>(text: &str, what: &'static str) -> Result<[u8; N], HyperliquidError> {
    let trimmed = text.trim();
    let body = trimmed.strip_prefix("0x").unwrap_or(trimmed);
    if body.len() != N * 2 {
        // Deliberately says the length and not the value: this path parses private keys.
        return Err(HyperliquidError::Credentials(format!(
            "{what} must be {} hex characters, got {}",
            N * 2,
            body.len()
        )));
    }
    let mut out = [0u8; N];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&body[index * 2..index * 2 + 2], 16)
            .map_err(|_| HyperliquidError::Credentials(format!("{what} is not hexadecimal")))?;
    }
    Ok(out)
}

/// The API wallet that signs actions.
///
/// Deliberately has **no `Debug`, no `Clone` of the raw key material and no accessor for
/// it**: the only things that leave are the address and a signature. `AGENTS.md` §1.1 —
/// a key in a log line is a key on disk on someone else's machine.
pub struct Signer {
    key: SigningKey,
    address: [u8; 20],
    is_mainnet: bool,
}

impl Signer {
    /// Builds a signer from a hex private key.
    pub fn from_hex(private_key: &str, is_mainnet: bool) -> Result<Self, HyperliquidError> {
        let bytes = parse_hex::<32>(private_key, "the API wallet private key")?;
        let key = SigningKey::from_slice(&bytes).map_err(|_| {
            HyperliquidError::Credentials(
                "the API wallet private key is not a valid secp256k1 scalar".into(),
            )
        })?;
        let address = address_of(key.verifying_key());
        Ok(Self {
            key,
            address,
            is_mainnet,
        })
    }

    /// The API wallet's own address, `0x`-prefixed lowercase.
    ///
    /// **Not** the address to use for `clearinghouseState`, `openOrders`, `orderUpdates` or
    /// `userFills`: those are keyed by the *master* account, and asking with this one
    /// returns an empty answer rather than an error (design note §5.7).
    pub fn address(&self) -> String {
        hex_0x(&self.address)
    }

    pub fn is_mainnet(&self) -> bool {
        self.is_mainnet
    }

    /// Signs one action, and verifies its own work before the bytes leave the process.
    ///
    /// The local `recover_address_from_prehash` costs tens of microseconds and turns the
    /// venue's single undifferentiated "that wallet does not exist" into two distinct
    /// answers: a mismatch here means the fault is on this side of the wire, and a rejection
    /// *past* here means it is not. Fail closed either way.
    pub fn sign_action(
        &self,
        action_msgpack: &[u8],
        nonce: u64,
    ) -> Result<Signature, HyperliquidError> {
        let hash = phantom_agent_hash(&connection_id(action_msgpack, nonce), self.is_mainnet);
        let (signature, recovery_id) = self
            .key
            .sign_prehash_recoverable(&hash)
            .map_err(|error| HyperliquidError::Signing(format!("could not sign: {error}")))?;

        let recovered = VerifyingKey::recover_from_prehash(&hash, &signature, recovery_id)
            .map_err(|error| HyperliquidError::Signing(format!("could not recover: {error}")))?;
        if address_of(&recovered) != self.address {
            return Err(HyperliquidError::Signing(format!(
                "local ecrecover disagrees with the signing key: recovered {}, expected {}",
                hex_0x(&address_of(&recovered)),
                hex_0x(&self.address)
            )));
        }

        Ok(to_wire(&signature, recovery_id))
    }
}

/// The Ethereum address of a public key: the last 20 bytes of the keccak of its
/// uncompressed encoding, minus the `0x04` tag byte.
fn address_of(key: &VerifyingKey) -> [u8; 20] {
    let point = key.to_encoded_point(false);
    let digest = keccak(&point.as_bytes()[1..]);
    let mut address = [0u8; 20];
    address.copy_from_slice(&digest[12..]);
    address
}

/// `v = 27 + parity`, `r`/`s` as 32-byte big-endian hex.
fn to_wire(signature: &EcdsaSignature, recovery_id: RecoveryId) -> Signature {
    let bytes = signature.to_bytes();
    Signature {
        r: hex_0x(&bytes[..32]),
        s: hex_0x(&bytes[32..]),
        v: 27 + recovery_id.to_byte(),
    }
}

/// Allocates nonces that are both monotonic and anchored to the wall clock.
///
/// Hyperliquid keeps the 100 highest nonces per signer and accepts only values inside
/// `(T − 2 days, T + 1 day)` against **its own** clock. Two failure modes, and each rules
/// out the obvious implementation of the other:
///
/// * A **pure counter** seeded once at startup drifts. A process that submits sparsely for
///   two days is still issuing nonces from around its boot time; every submit then dies
///   with `Invalid nonce: nonce too low` — and that can also surface as the phantom-address
///   error, which sends the diagnosis somewhere else entirely.
/// * A **raw clock read** repeats. More than one action inside the same millisecond
///   collides, and a clock stepping backwards replays into the retained set.
///
/// `max(stored + 1, wall_ms)` satisfies both. The CAS loop only spins when another action
/// beat this one, and its answer is a valid nonce either way.
///
/// Corollary, and it belongs in the deployment notes rather than only here: **one API
/// wallet per connector process**. The retained set is per signer address, so two processes
/// sharing a key fight, and the loser's orders are rejected.
#[derive(Debug)]
pub struct NonceSource {
    last: AtomicU64,
}

impl Default for NonceSource {
    fn default() -> Self {
        Self::new()
    }
}

impl NonceSource {
    pub fn new() -> Self {
        Self {
            last: AtomicU64::new(0),
        }
    }

    /// The next nonce, against the current wall clock.
    pub fn next(&self) -> u64 {
        self.next_from(now_ms())
    }

    /// The next nonce against a supplied clock reading. Split out so the clock can step
    /// backwards in a test without stepping backwards on the machine.
    pub fn next_from(&self, wall_ms: u64) -> u64 {
        loop {
            let stored = self.last.load(Ordering::Acquire);
            let candidate = stored.saturating_add(1).max(wall_ms);
            if self
                .last
                .compare_exchange(stored, candidate, Ordering::Release, Ordering::Acquire)
                .is_ok()
            {
                // The precursor, not the failure: an hour of drift is still accepted, and
                // is the only warning before two days of it is not.
                if stored > 0 && wall_ms.saturating_sub(stored) > NONCE_DRIFT_WARN_MS {
                    warn!(
                        stored_nonce = stored,
                        wall_clock_ms = wall_ms,
                        drift_ms = wall_ms.saturating_sub(stored),
                        "The Hyperliquid nonce counter is falling behind the wall clock; the \
                         venue's two-day acceptance window is at risk."
                    );
                }
                return candidate;
            }
        }
    }
}

fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

#[cfg(test)]
mod tests {
    use crate::hyperliquid::signing::{
        NonceSource,
        Signer,
        connection_id,
        domain_separator,
        hashing_string,
        phantom_agent_hash,
    };

    /// The well-known Hardhat / Anvil account 0. Public by construction; never funded.
    const HARDHAT_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const HARDHAT_ADDRESS: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";

    fn unhex(text: &str) -> Vec<u8> {
        (0..text.len() / 2)
            .map(|i| u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).unwrap())
            .collect()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    // --- Golden vectors -----------------------------------------------------------------
    //
    // Generated with the **official `hyperliquid-python-sdk`** (`utils/signing.py`:
    // `action_hash`, `construct_phantom_agent`, `l1_payload`, `sign_l1_action`) against the
    // Hardhat key above. The msgpack bytes are the SDK's own `msgpack.packb` output, so
    // these pin the field order too — the one part of the protocol a compiler cannot check.

    /// `{"type":"order","orders":[{"a":3,"b":true,"p":"30000","s":"0.001","r":false,
    /// "t":{"limit":{"tif":"Alo"}},"c":"0xa1b2…dead"}],"grouping":"na"}`
    const ORDER_MSGPACK: &str = "83a474797065a56f72646572a66f72646572739187a16103a162c3a170a533\
                                 30303030a173a5302e303031a172c2a17481a56c696d697481a3746966a341\
                                 6c6fa163d9223078613162323030303030303030303030303030303030303\
                                 0303030303064656164a867726f7570696e67a26e61";
    const ORDER_NONCE: u64 = 1_700_000_000_000;
    const ORDER_CONNECTION_ID: &str =
        "2b05cad7218f1799e2c38120bec74dae41a5fe4ebca176d5cbeed213a24e0a1f";
    const ORDER_HASH_MAINNET: &str =
        "7a01af5c16cad3aad6cf37e6bf905279d44144f2ed71b7221888c14d62be41f3";
    const ORDER_HASH_TESTNET: &str =
        "9ddf9e9fcbeb87f4c90563afc3f8a38c08d7e6032a2581a6b13d6bb0e42dc8d3";
    const ORDER_SIG_MAINNET: (&str, &str, u8) = (
        "0x4f19e43d0e8fd35f9b46d97f1232a6da31dcb3183e21504dd986661d59db5561",
        "0x5f8bb4dd7dac895ce58d2f3c9c3742a10364fa4f7091669566117a996f4761d5",
        27,
    );
    const ORDER_SIG_TESTNET: (&str, &str, u8) = (
        "0x7dd89d62cb2fcf0a2a2096ec907343f3684614fe068270ef7a3fb653ad88af46",
        "0x4a7bfbc05120377c9c6049773b0156a04e75a5b2eb952615b132e594b622d0f4",
        28,
    );

    /// `{"type":"cancelByCloid","cancels":[{"asset":3,"cloid":"0xa1b2…dead"}]}`
    const CANCEL_MSGPACK: &str = "82a474797065ad63616e63656c4279436c6f6964a763616e63656c73918\
                                  2a5617373657403a5636c6f6964d92230786131623230303030303030303\
                                  030303030303030303030303030303064656164";
    const CANCEL_NONCE: u64 = 1_700_000_000_001;
    const CANCEL_CONNECTION_ID: &str =
        "5a87ba218da9db9e0ea80f08872108cefefda6c3f1c43048efaa58f30ba1a36c";
    const CANCEL_SIG_TESTNET: (&str, &str, u8) = (
        "0xf236230145c3fbe6a944174af468aed28430978ccb2d587fccf7cc4206248207",
        "0x7b2e643aede4b3751111ecd74dd3c81e2e8e8ff8df5c927873464406829027af",
        27,
    );

    /// The whole reason the number is a *string*: the bytes are what gets hashed, so a
    /// trailing zero is not cosmetic. `"108.0"` and `"108"` recover different addresses,
    /// and the venue reports both as "that wallet does not exist".
    ///
    /// Every case below is the official SDK's own output for the same input.
    #[test]
    fn a_number_renders_exactly_as_the_venue_will_re_render_it() {
        let cases = [
            (0.0, "0"),
            (0.00076, "0.00076"),
            (0.00000001, "0.00000001"),
            (0.12345678, "0.12345678"),
            (87_654_321.12345678, "87654321.12345678"),
            // The failure that cost a week: a whole number must lose its ".0".
            (108.0, "108"),
            (100.0, "100"),
            (30000.0, "30000"),
            (63460.0, "63460"),
            (123460.0, "123460"),
            (105.6, "105.6"),
            (0.11484, "0.11484"),
            (0.12569, "0.12569"),
            (1.5, "1.5"),
        ];
        for (value, expected) in cases {
            assert_eq!(hashing_string(value).unwrap(), expected, "{value}");
        }

        // Negative zero: the two official SDKs disagree (Rust says "0", Python "-0"). This
        // follows the Rust SDK, and the submit path refuses non-positive numbers long
        // before here, so nothing can reach the disagreement.
        assert_eq!(hashing_string(-0.0).unwrap(), "0");
    }

    /// A number that needs more than eight decimals is **refused**, not rounded.
    ///
    /// The official Rust SDK rounds it (`0.123456789` → `"0.12345679"`) and the official
    /// Python SDK raises; both were measured. Rounding here would rest an order at a size
    /// or price the bot never asked for, and no later event would mention it — the exact
    /// shape of failure `AGENTS.md` §1.1 exists to forbid. Normalisation keeps every real
    /// order on a six-decimal grid, so this is a guard, and a hit means normalisation is
    /// wrong rather than that the venue is.
    #[test]
    fn a_number_that_cannot_be_written_exactly_is_refused_rather_than_rounded() {
        for value in [
            0.123456789,
            0.000000005,
            0.999999999,
            1e-9,
            f64::NAN,
            f64::INFINITY,
        ] {
            assert!(
                hashing_string(value).is_err(),
                "{value} was silently rewritten as {:?}",
                hashing_string(value)
            );
        }
        // …while everything on the venue's coarsest legal grid still renders.
        for value in [1e-8, 1e-6, 1e-5, 0.00076, 63460.0, 123460.0] {
            hashing_string(value).unwrap();
        }
    }

    /// A rendered number must survive the round trip back to the value it came from —
    /// otherwise the venue rests an order at a price the connector never asked for, and
    /// nothing anywhere reports a difference.
    #[test]
    fn a_rendered_number_parses_back_to_itself() {
        let mut seed: u64 = 0x5eed_1234_9876_0001;
        for _ in 0..5_000 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            // Exercise the range an order actually carries: 1e-5 sizes to six-digit prices,
            // at the venue's finest legal grids.
            let decimals = (seed % 7) as i32;
            let magnitude = ((seed >> 8) % 9) as i32;
            let raw = ((seed >> 16) % 1_000_000) as f64 * 10f64.powi(magnitude - 4);
            let value = (raw * 10f64.powi(decimals)).round() / 10f64.powi(decimals);
            if !(0.0..1e9).contains(&value) {
                continue;
            }
            let Ok(rendered) = hashing_string(value) else {
                continue;
            };
            let parsed: f64 = rendered.parse().unwrap();
            assert!(
                (parsed - value).abs() <= value.abs() * 1e-12 + 1e-12,
                "{value} rendered as {rendered} and parsed back as {parsed}"
            );
            assert!(
                !rendered.ends_with('0') || !rendered.contains('.'),
                "{rendered}"
            );
            assert!(!rendered.ends_with('.'), "{rendered}");
        }
    }

    /// The domain is fixed and shared by both networks. A `chainId` "corrected" to the real
    /// Hyperliquid chain, or a `verifyingContract` that is not zero, still signs — it just
    /// signs for an address nobody has ever funded.
    #[test]
    fn the_domain_separator_is_the_venues_own() {
        assert_eq!(
            hex(&domain_separator()),
            "d79297fcdf2ffcd4ae223d01edaa2ba214ff8f401d7c9300d995d17c82aa4040"
        );
    }

    /// **One** keccak over `msgpack ‖ nonce ‖ 0x00`. Hashing the msgpack first and
    /// concatenating the digest compiles, runs, and produces a signature for a random
    /// address; this vector is what tells the two apart.
    #[test]
    fn the_connection_id_is_one_hash_over_the_concatenation() {
        assert_eq!(
            hex(&connection_id(&unhex(ORDER_MSGPACK), ORDER_NONCE)),
            ORDER_CONNECTION_ID
        );
        assert_eq!(
            hex(&connection_id(&unhex(CANCEL_MSGPACK), CANCEL_NONCE)),
            CANCEL_CONNECTION_ID
        );

        // The trailing no-vault byte is part of the preimage, not decoration.
        let mut without_flag = unhex(ORDER_MSGPACK);
        without_flag.extend_from_slice(&ORDER_NONCE.to_be_bytes());
        assert_ne!(
            hex(&connection_id(&unhex(ORDER_MSGPACK), ORDER_NONCE)),
            hex(&{
                use sha3::{Digest, Keccak256};
                let mut h = Keccak256::new();
                h.update(&without_flag);
                let out: [u8; 32] = h.finalize().into();
                out
            })
        );
    }

    /// Mainnet and testnet differ **only** in the phantom agent's `source`. Getting it
    /// backwards is the single most likely way to spend an afternoon: the signature is
    /// valid, the venue is the wrong one, and the answer is "that wallet does not exist".
    #[test]
    fn the_network_lives_in_the_agent_source_and_nowhere_else() {
        let id = connection_id(&unhex(ORDER_MSGPACK), ORDER_NONCE);
        assert_eq!(hex(&phantom_agent_hash(&id, true)), ORDER_HASH_MAINNET);
        assert_eq!(hex(&phantom_agent_hash(&id, false)), ORDER_HASH_TESTNET);
        assert_ne!(ORDER_HASH_MAINNET, ORDER_HASH_TESTNET);
    }

    #[test]
    fn the_signer_derives_the_key_s_own_address() {
        let signer = Signer::from_hex(HARDHAT_KEY, false).unwrap();
        assert_eq!(signer.address(), HARDHAT_ADDRESS);
        // A bare key, without the 0x, is the same key.
        let bare = Signer::from_hex(&HARDHAT_KEY[2..], false).unwrap();
        assert_eq!(bare.address(), HARDHAT_ADDRESS);
    }

    /// The end-to-end vector: the same action, nonce and key must produce the official
    /// SDK's `r`, `s` and `v`, byte for byte, on both networks.
    #[test]
    fn a_signed_action_matches_the_official_sdk_byte_for_byte() {
        for (is_mainnet, expected) in [(true, ORDER_SIG_MAINNET), (false, ORDER_SIG_TESTNET)] {
            let signer = Signer::from_hex(HARDHAT_KEY, is_mainnet).unwrap();
            let signature = signer
                .sign_action(&unhex(ORDER_MSGPACK), ORDER_NONCE)
                .unwrap();
            assert_eq!(signature.r, expected.0, "mainnet={is_mainnet}");
            assert_eq!(signature.s, expected.1, "mainnet={is_mainnet}");
            assert_eq!(signature.v, expected.2, "mainnet={is_mainnet}");
        }

        let signer = Signer::from_hex(HARDHAT_KEY, false).unwrap();
        let signature = signer
            .sign_action(&unhex(CANCEL_MSGPACK), CANCEL_NONCE)
            .unwrap();
        assert_eq!(
            (signature.r.as_str(), signature.s.as_str(), signature.v),
            CANCEL_SIG_TESTNET
        );
    }

    /// The wire shape the venue parses: `v` is the Ethereum-legacy 27/28, and `r`/`s` are
    /// `0x` plus exactly 64 hex characters — a stripped leading zero is a rejected request.
    #[test]
    fn the_signature_serialises_as_the_exchange_endpoint_expects() {
        let signer = Signer::from_hex(HARDHAT_KEY, false).unwrap();
        let signature = signer
            .sign_action(&unhex(ORDER_MSGPACK), ORDER_NONCE)
            .unwrap();
        let json = serde_json::to_value(&signature).unwrap();
        assert_eq!(json["v"], 28);
        for field in ["r", "s"] {
            let text = json[field].as_str().unwrap();
            assert_eq!(text.len(), 66, "{field}={text}");
            assert!(text.starts_with("0x"), "{field}={text}");
            assert!(text[2..].chars().all(|c| c.is_ascii_hexdigit()), "{text}");
        }
    }

    /// Signing is deterministic (RFC 6979), which is what makes the vectors above testable
    /// at all — and what makes a mismatch a real signal rather than nonce noise.
    #[test]
    fn signing_the_same_action_twice_gives_the_same_signature() {
        let signer = Signer::from_hex(HARDHAT_KEY, true).unwrap();
        let first = signer.sign_action(&unhex(ORDER_MSGPACK), 42).unwrap();
        let second = signer.sign_action(&unhex(ORDER_MSGPACK), 42).unwrap();
        assert_eq!(first, second);
    }

    /// The self-check has to hold across the whole space it guards, not just the vectors.
    /// A failure here means the bytes about to go out recover to an address the venue has
    /// never seen — and the venue's report of that is indistinguishable from a missing
    /// agent approval, so it must be caught locally.
    #[test]
    fn every_signature_recovers_to_the_signing_key_before_it_leaves() {
        let signer = Signer::from_hex(HARDHAT_KEY, false).unwrap();
        let mut seed: u64 = 0xc0ffee_dead_beef_00;
        for i in 0..256u64 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let action = format!("{{\"type\":\"order\",\"n\":{seed}}}");
            // `sign_action` fails closed if its own recovery disagrees, so reaching `Ok`
            // *is* the assertion.
            signer
                .sign_action(action.as_bytes(), 1_700_000_000_000 + i)
                .unwrap();
        }
    }

    /// A key that is not a key must be refused where it is read, not where it is used —
    /// and the message must never quote the value it rejected.
    #[test]
    fn a_malformed_key_is_refused_without_echoing_it() {
        for bad in [
            "0xdeadbeef",
            "",
            "0xzz0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            &"0".repeat(64),
        ] {
            let Err(error) = Signer::from_hex(bad, false) else {
                panic!("{bad:?} was accepted as a private key");
            };
            let error = error.to_string();
            assert!(!error.contains(bad) || bad.is_empty(), "{error}");
        }
    }

    /// A pure counter drifts out of the venue's window; a raw clock read collides and can
    /// step backwards. `max(stored + 1, wall_ms)` is the only rule that survives both, and
    /// each clause below is one of the two incidents it prevents.
    #[test]
    fn nonces_are_monotonic_and_anchored_to_the_clock() {
        let nonces = NonceSource::new();

        // Anchored: the first nonce is the clock, not zero and not a boot counter.
        assert_eq!(nonces.next_from(1_700_000_000_000), 1_700_000_000_000);

        // A burst inside one millisecond must not repeat — the venue keeps only the 100
        // highest per signer and a repeat is refused.
        let burst: Vec<u64> = (0..200)
            .map(|_| nonces.next_from(1_700_000_000_000))
            .collect();
        assert_eq!(burst[0], 1_700_000_000_001);
        assert_eq!(burst[199], 1_700_000_000_200);
        for pair in burst.windows(2) {
            assert!(pair[1] > pair[0]);
        }

        // A clock that steps backwards — NTP correction, a restart on a skewed host —
        // must not replay into the retained set.
        assert_eq!(nonces.next_from(1_600_000_000_000), 1_700_000_000_201);

        // …and once the clock has moved on, the nonce follows it rather than crawling
        // forward by one per action. This is the two-day-uptime failure: 200 submits over
        // 48 hours left a pure counter 200 ms past boot while the window had moved a day.
        assert_eq!(nonces.next_from(1_700_172_800_000), 1_700_172_800_000);
    }
}
