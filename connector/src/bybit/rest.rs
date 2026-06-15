use chrono::Utc;
use serde::Deserialize;

use crate::{
    bybit::{
        BybitError,
        msg::{OrderCreateResponse, Position, PrivateOrder, RestResponse, WalletBalanceResponse},
    },
    utils::sign_hmac_sha256,
};

/// Hard cap on open-order pagination pages (~20 pages × 50 = 1000 orders). If the cap is hit with a
/// non-empty cursor the reconcile is aborted fail-closed (a truncated set must NOT be presented as
/// complete — design-note §1/Q6).
pub const OPEN_ORDERS_PAGE_CAP: usize = 20;

#[derive(Clone)]
pub struct BybitClient {
    client: reqwest::Client,
    url: String,
    api_key: String,
    secret: String,
}

impl BybitClient {
    pub fn new(url: &str, api_key: &str, secret: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.to_string(),
            api_key: api_key.to_string(),
            secret: secret.to_string(),
        }
    }

    async fn get<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        query: &str,
        api_key: &str,
        secret: &str,
    ) -> Result<T, reqwest::Error> {
        let time = Utc::now().timestamp_millis() - 1000;
        let sign_body = format!("{time}{api_key}5000{query}");
        let signature = sign_hmac_sha256(secret, &sign_body);
        let resp = self
            .client
            .get(format!("{}{}?{}", self.url, path, query))
            .header("Accept", "application/json")
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-API-KEY", api_key)
            .header("X-BAPI-TIMESTAMP", time)
            .header("X-BAPI-RECV-WINDOW", "5000")
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    async fn post<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        body: String,
        api_key: &str,
        secret: &str,
    ) -> Result<T, reqwest::Error> {
        let time = Utc::now().timestamp_millis() - 1000;
        let sign_body = format!("{time}{api_key}5000{body}");
        let signature = sign_hmac_sha256(secret, &sign_body);
        let resp = self
            .client
            .post(format!("{}{}", self.url, path))
            .header("Accept", "application/json")
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-API-KEY", api_key)
            .header("X-BAPI-TIMESTAMP", time)
            .header("X-BAPI-RECV-WINDOW", "5000")
            .body(body)
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    pub async fn cancel_all_orders(&self, category: &str, symbol: &str) -> Result<(), BybitError> {
        let resp: RestResponse = self
            .post(
                "/v5/order/cancel-all",
                format!("{{\"category\":\"{category}\",\"symbol\":\"{symbol}\"}}"),
                &self.api_key,
                &self.secret,
            )
            .await?;
        if resp.result.success != "1" {
            Err(BybitError::OpError(resp.ret_msg))
        } else {
            Ok(())
        }
    }

    pub async fn get_position_information(
        &self,
        category: &str,
        symbol: &str,
    ) -> Result<Vec<Position>, BybitError> {
        let resp: RestResponse = self
            .get(
                "/v5/position/list",
                &format!("category={category}&symbol={symbol}"),
                &self.api_key,
                &self.secret,
            )
            .await?;
        if resp.ret_code != 0 {
            Err(BybitError::OpError(resp.ret_msg))
        } else {
            let position: Vec<Position> = serde_json::from_value(resp.result.list.unwrap())?;
            Ok(position)
        }
    }

    /// Reconcile (R-M1a): fetches all positions for `category` (no `symbol` filter), guarding
    /// `result.list` (Bybit returns `list:null` on errors — never `.unwrap()`, unlike
    /// [`Self::get_position_information`]). Returns the raw envelope `(retCode, retMsg)` on a
    /// non-zero `retCode` so the caller can emit a fail-closed `End` with raw codes preserved.
    pub async fn get_positions_for_reconcile(
        &self,
        category: &str,
    ) -> Result<Vec<Position>, BybitError> {
        let resp: RestResponse = self
            .get(
                "/v5/position/list",
                &format!("category={category}&settleCoin=USDT"),
                &self.api_key,
                &self.secret,
            )
            .await?;
        if resp.ret_code != 0 {
            return Err(BybitError::OpErrorCode {
                code: resp.ret_code,
                msg: resp.ret_msg,
            });
        }
        match resp.result.list {
            Some(list) => Ok(serde_json::from_value(list)?),
            // Guarded: `null` list on a zero-retCode response → treat as empty (fail-soft; the
            // caller still completes the reconcile). A `.unwrap()` here would panic → process exit.
            None => Ok(Vec::new()),
        }
    }

    /// Reconcile (R-M1a): fetches ALL open orders, paginating on `next_page_cursor` up to
    /// [`OPEN_ORDERS_PAGE_CAP`] pages. Returns:
    /// * `Ok(orders)` — full set fetched (cursor drained).
    /// * `Err(BybitError::OpErrorCode{..})` — a page returned a non-zero `retCode`.
    /// * `Err(BybitError::PaginationCapExceeded)` — the cap was hit with a non-empty cursor; the
    ///   caller MUST fail-closed (do NOT emit a truncated set as complete — §Q6).
    pub async fn get_open_orders(
        &self,
        category: &str,
    ) -> Result<Vec<PrivateOrder>, BybitError> {
        let mut orders: Vec<PrivateOrder> = Vec::new();
        let mut cursor = String::new();
        for _ in 0..OPEN_ORDERS_PAGE_CAP {
            let query = if cursor.is_empty() {
                format!("category={category}&limit=50&openOnly=0")
            } else {
                // Bybit cursors are URL-safe tokens echoed back verbatim.
                format!("category={category}&limit=50&openOnly=0&cursor={cursor}")
            };
            let resp: RestResponse = self
                .get("/v5/order/realtime", &query, &self.api_key, &self.secret)
                .await?;
            if resp.ret_code != 0 {
                return Err(BybitError::OpErrorCode {
                    code: resp.ret_code,
                    msg: resp.ret_msg,
                });
            }
            if let Some(list) = resp.result.list {
                let page: Vec<PrivateOrder> = serde_json::from_value(list)?;
                orders.extend(page);
            }
            // (else `list:null` → treat as no orders on this page.)
            cursor = resp.result.next_page_cursor;
            if cursor.is_empty() {
                return Ok(orders);
            }
        }
        // Cap hit while a cursor still remains → fail-closed (do not present truncated set).
        Err(BybitError::PaginationCapExceeded)
    }

    /// Reconcile (R-M1a): fetches the UNIFIED account wallet balance. Uses the dedicated
    /// [`WalletBalanceResponse`] (nested `result.list[0].coin[]`), guarding `list` against `null`.
    pub async fn get_wallet_balance(&self) -> Result<WalletBalanceResponse, BybitError> {
        let resp: WalletBalanceResponse = self
            .get(
                "/v5/account/wallet-balance",
                "accountType=UNIFIED",
                &self.api_key,
                &self.secret,
            )
            .await?;
        Ok(resp)
    }

    /// SpotOrder (B-M1a): places a spot order via `POST /v5/order/create`. `body` is the pre-built
    /// JSON (category=spot hardcoded, see `spot_order::build_create_body`). Returns the parsed
    /// [`OrderCreateResponse`]; the caller checks `ret_code` (Bybit always HTTP 200, so a non-zero
    /// `retCode` is the only failure signal — fail-closed).
    pub async fn spot_create_order(
        &self,
        body: String,
    ) -> Result<OrderCreateResponse, BybitError> {
        let resp: OrderCreateResponse = self
            .post("/v5/order/create", body, &self.api_key, &self.secret)
            .await?;
        Ok(resp)
    }

    /// SpotOrder (B-M1a): cancels a spot order via `POST /v5/order/cancel` (correlated by
    /// `orderLinkId` inside `body`). Returns the parsed [`OrderCreateResponse`] (same envelope shape;
    /// `result` carries orderId/orderLinkId). The caller keys success on `ret_code`.
    pub async fn spot_cancel_order(
        &self,
        body: String,
    ) -> Result<OrderCreateResponse, BybitError> {
        let resp: OrderCreateResponse = self
            .post("/v5/order/cancel", body, &self.api_key, &self.secret)
            .await?;
        Ok(resp)
    }

    /// SpotOrder (B-M1a): queries a spot order's current state via
    /// `GET /v5/order/realtime?category=spot&orderLinkId={...}`. Returns the parsed envelope; the
    /// caller guards `result.list` against `null` (never `.unwrap()`) and keys on `ret_code`.
    pub async fn spot_order_status(
        &self,
        order_link_id: &str,
    ) -> Result<RestResponse, BybitError> {
        let resp: RestResponse = self
            .get(
                "/v5/order/realtime",
                &format!("category=spot&orderLinkId={order_link_id}"),
                &self.api_key,
                &self.secret,
            )
            .await?;
        Ok(resp)
    }
}
