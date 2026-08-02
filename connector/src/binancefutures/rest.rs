use chrono::Utc;
use hftbacktest::types::{OrdType, Side, TimeInForce};
use serde::Deserialize;

use super::msg::{rest, rest::PositionInformationV2};
use crate::{
    binancefutures::{
        BinanceFuturesError,
        msg::{
            rest::{OrderResponse, OrderResponseResult},
            stream::ListenKey,
        },
    },
    utils::sign_hmac_sha256,
};

#[derive(Clone)]
pub struct BinanceFuturesClient {
    client: reqwest::Client,
    url: String,
    api_key: String,
    secret: String,
}

impl BinanceFuturesClient {
    pub fn new(url: &str, api_key: &str, secret: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.to_string(),
            api_key: api_key.to_string(),
            secret: secret.to_string(),
        }
    }

    async fn get_noauth<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        query: String,
    ) -> Result<T, reqwest::Error> {
        let resp = self
            .client
            .get(format!("{}{}?{}", self.url, path, query))
            .header("Accept", "application/json")
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    async fn get<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        mut query: String,
    ) -> Result<T, reqwest::Error> {
        let time = Utc::now().timestamp_millis() - 1000;
        if !query.is_empty() {
            query.push('&');
        }
        query.push_str("recvWindow=5000&timestamp=");
        query.push_str(&time.to_string());
        let signature = sign_hmac_sha256(&self.secret, &query);
        let resp = self
            .client
            .get(format!(
                "{}{}?{}&signature={}",
                self.url, path, query, signature
            ))
            .header("Accept", "application/json")
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    async fn put<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        body: String,
    ) -> Result<T, reqwest::Error> {
        let time = Utc::now().timestamp_millis() - 1000;
        let sign_body = format!("recvWindow=5000&timestamp={time}{body}");
        let signature = sign_hmac_sha256(&self.secret, &sign_body);
        let resp = self
            .client
            .put(format!(
                "{}{}?recvWindow=5000&timestamp={}&signature={}",
                self.url, path, time, signature
            ))
            .header("Accept", "application/json")
            .header("X-MBX-APIKEY", &self.api_key)
            .body(body)
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
    ) -> Result<T, reqwest::Error> {
        let time = Utc::now().timestamp_millis() - 1000;
        let sign_body = format!("recvWindow=5000&timestamp={time}{body}");
        let signature = sign_hmac_sha256(&self.secret, &sign_body);
        let resp = self
            .client
            .post(format!(
                "{}{}?recvWindow=5000&timestamp={}&signature={}",
                self.url, path, time, signature
            ))
            .header("Accept", "application/json")
            .header("X-MBX-APIKEY", &self.api_key)
            .body(body)
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    async fn delete<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        body: String,
    ) -> Result<T, reqwest::Error> {
        let time = Utc::now().timestamp_millis() - 1000;
        let sign_body = format!("recvWindow=5000&timestamp={time}{body}");
        let signature = sign_hmac_sha256(&self.secret, &sign_body);
        let resp = self
            .client
            .delete(format!(
                "{}{}?recvWindow=5000&timestamp={}&signature={}",
                self.url, path, time, signature
            ))
            .header("Accept", "application/json")
            .header("X-MBX-APIKEY", &self.api_key)
            .body(body)
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    pub async fn start_user_data_stream(&self) -> Result<String, reqwest::Error> {
        let resp: Result<ListenKey, _> = self.post("/fapi/v1/listenKey", String::new()).await;
        resp.map(|v| v.listen_key)
    }

    pub async fn keepalive_user_data_stream(&self) -> Result<(), reqwest::Error> {
        let _: serde_json::Value = self.put("/fapi/v1/listenKey", String::new()).await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn submit_order(
        &self,
        client_order_id: &str,
        symbol: &str,
        side: Side,
        price: f64,
        price_prec: usize,
        qty: f64,
        order_type: OrdType,
        time_in_force: TimeInForce,
    ) -> Result<OrderResponse, BinanceFuturesError> {
        let mut body = String::with_capacity(200);
        body.push_str("newClientOrderId=");
        body.push_str(client_order_id);
        body.push_str("&symbol=");
        body.push_str(symbol);
        body.push_str("&side=");
        // The venue boundary: a side that is not a direction is refused here rather than
        // written into a payload — where it used to panic the process (invariant E2).
        body.push_str(
            side.try_resolve()
                .ok_or(BinanceFuturesError::InvalidRequest)?
                .as_str(),
        );
        body.push_str("&price=");
        body.push_str(&format!("{price:.price_prec$}"));
        body.push_str("&quantity=");
        body.push_str(&format!("{qty:.5}"));
        body.push_str("&type=");
        body.push_str(order_type.as_ref());
        body.push_str("&timeInForce=");
        body.push_str(time_in_force.as_ref());

        let resp: OrderResponseResult = self.post("/fapi/v1/order", body).await?;
        match resp {
            OrderResponseResult::Ok(resp) => Ok(resp),
            OrderResponseResult::Err(resp) => Err(BinanceFuturesError::OrderError {
                code: resp.code,
                msg: resp.msg,
            }),
        }
    }

    pub async fn submit_orders(
        &self,
        orders: Vec<(String, String, Side, f64, usize, f64, OrdType, TimeInForce)>,
    ) -> Result<Vec<Result<OrderResponse, BinanceFuturesError>>, BinanceFuturesError> {
        if orders.len() > 5 {
            return Err(BinanceFuturesError::InvalidRequest);
        }
        let mut body = String::with_capacity(2000 * orders.len());
        body.push_str("{\"batchOrders\":[");
        for (i, order) in orders.iter().enumerate() {
            if i > 0 {
                body.push(',');
            }
            body.push_str("{\"newClientOrderId\":\"");
            body.push_str(&order.0);
            body.push_str("\",\"symbol\":\"");
            body.push_str(&order.1);
            body.push_str("\",\"side\":\"");
            // As above: no order in the batch reaches the wire with a sideless side.
            body.push_str(
                order
                    .2
                    .try_resolve()
                    .ok_or(BinanceFuturesError::InvalidRequest)?
                    .as_str(),
            );
            body.push_str("\",\"price\":\"");
            body.push_str(&format!("{:.prec$}", order.3, prec = order.4));
            body.push_str("\",\"quantity\":\"");
            body.push_str(&format!("{:.5}", order.5));
            body.push_str("\",\"type\":\"");
            body.push_str(order.6.as_ref());
            body.push_str("\",\"timeInForce\":\"");
            body.push_str(order.7.as_ref());
            body.push_str("\"}");
        }
        body.push_str("]}");

        let resp: Vec<OrderResponseResult> = self.post("/fapi/v1/batchOrders", body).await?;
        Ok(resp
            .into_iter()
            .map(|resp| match resp {
                OrderResponseResult::Ok(resp) => Ok(resp),
                OrderResponseResult::Err(resp) => Err(BinanceFuturesError::OrderError {
                    code: resp.code,
                    msg: resp.msg,
                }),
            })
            .collect())
    }

    pub async fn modify_order(
        &self,
        client_order_id: &str,
        symbol: &str,
        side: Side,
        price: f64,
        price_prec: usize,
        qty: f64,
    ) -> Result<OrderResponse, BinanceFuturesError> {
        let mut body = String::with_capacity(100);
        body.push_str("symbol=");
        body.push_str(symbol);
        body.push_str("&origClientOrderId=");
        body.push_str(client_order_id);
        body.push_str("&side=");
        // The venue boundary: a side that is not a direction is refused here rather than
        // written into a payload — where it used to panic the process (invariant E2).
        body.push_str(
            side.try_resolve()
                .ok_or(BinanceFuturesError::InvalidRequest)?
                .as_str(),
        );
        body.push_str("&price=");
        body.push_str(&format!("{price:.price_prec$}"));
        body.push_str("&quantity=");
        body.push_str(&format!("{qty:.5}"));

        let resp: OrderResponseResult = self.put("/fapi/v1/order", body).await?;
        match resp {
            OrderResponseResult::Ok(resp) => Ok(resp),
            OrderResponseResult::Err(resp) => Err(BinanceFuturesError::OrderError {
                code: resp.code,
                msg: resp.msg,
            }),
        }
    }

    pub async fn cancel_order(
        &self,
        client_order_id: &str,
        symbol: &str,
    ) -> Result<OrderResponse, BinanceFuturesError> {
        let mut body = String::with_capacity(100);
        body.push_str("symbol=");
        body.push_str(symbol);
        body.push_str("&origClientOrderId=");
        body.push_str(client_order_id);

        let resp: OrderResponseResult = self.delete("/fapi/v1/order", body).await?;
        match resp {
            OrderResponseResult::Ok(resp) => Ok(resp),
            OrderResponseResult::Err(resp) => Err(BinanceFuturesError::OrderError {
                code: resp.code,
                msg: resp.msg,
            }),
        }
    }

    pub async fn cancel_orders(
        &self,
        symbol: &str,
        client_order_ids: Vec<String>,
    ) -> Result<Vec<Result<OrderResponse, BinanceFuturesError>>, BinanceFuturesError> {
        if client_order_ids.len() > 10 {
            return Err(BinanceFuturesError::InvalidRequest);
        }
        let mut body = String::with_capacity(100);
        body.push_str("{\"symbol\":\"");
        body.push_str(symbol);
        body.push_str("\",\"origClientOrderIdList\":[");
        for (i, client_order_id) in client_order_ids.iter().enumerate() {
            if i > 0 {
                body.push(',');
            }
            body.push('\"');
            body.push_str(client_order_id);
            body.push('\"');
        }
        body.push_str("]}");
        let resp: Vec<OrderResponseResult> = self.post("/fapi/v1/batchOrders", body).await?;
        Ok(resp
            .into_iter()
            .map(|resp| match resp {
                OrderResponseResult::Ok(resp) => Ok(resp),
                OrderResponseResult::Err(resp) => Err(BinanceFuturesError::OrderError {
                    code: resp.code,
                    msg: resp.msg,
                }),
            })
            .collect())
    }

    pub async fn cancel_all_orders(&self, symbol: &str) -> Result<(), reqwest::Error> {
        let _: serde_json::Value = self
            .delete("/fapi/v1/allOpenOrders", format!("symbol={symbol}"))
            .await?;
        Ok(())
    }

    pub async fn get_position_information(
        &self,
    ) -> Result<Vec<PositionInformationV2>, reqwest::Error> {
        let resp: Vec<PositionInformationV2> =
            self.get("/fapi/v2/positionRisk", String::new()).await?;
        Ok(resp)
    }

    pub async fn get_depth(&self, symbol: &str) -> Result<rest::Depth, reqwest::Error> {
        let resp: rest::Depth = self
            .get_noauth("/fapi/v1/depth", format!("symbol={symbol}&limit=1000"))
            .await?;
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use hftbacktest::types::{OrdType, Side, TimeInForce};

    use crate::binancefutures::{BinanceFuturesError, rest::BinanceFuturesClient};

    /// The client points at a port nothing listens on: if a request were ever built, the test
    /// would fail with a transport error instead of the refusal it asserts.
    fn offline_client() -> BinanceFuturesClient {
        BinanceFuturesClient::new("http://127.0.0.1:1", "key", "secret")
    }

    /// **A side that is not a direction never reaches the venue payload** (invariant E2).
    ///
    /// `Side` carries `None` and `Unsupported`, and the order body used to be built by
    /// `side.as_ref()`, which panicked on both — in a connector where a panic is process death
    /// (`AGENTS.md` §4.7). It is now refused where the payload is built, which is what the
    /// caller already treats as a rejected submit: the bot is told, the process lives.
    #[tokio::test]
    async fn an_order_whose_side_has_no_sign_is_refused_before_it_reaches_the_venue() {
        for side in [Side::None, Side::Unsupported] {
            let refused = offline_client()
                .submit_order(
                    "cid",
                    "BTCUSDT",
                    side,
                    30_000.0,
                    1,
                    0.001,
                    OrdType::Limit,
                    TimeInForce::GTX,
                )
                .await;
            assert!(
                matches!(refused, Err(BinanceFuturesError::InvalidRequest)),
                "submit {side:?}: {refused:?}"
            );

            let refused = offline_client()
                .modify_order("cid", "BTCUSDT", side, 30_000.0, 1, 0.001)
                .await;
            assert!(
                matches!(refused, Err(BinanceFuturesError::InvalidRequest)),
                "modify {side:?}: {refused:?}"
            );

            let refused = offline_client()
                .submit_orders(vec![(
                    "cid".to_string(),
                    "BTCUSDT".to_string(),
                    side,
                    30_000.0,
                    1,
                    0.001,
                    OrdType::Limit,
                    TimeInForce::GTX,
                )])
                .await;
            assert!(
                matches!(refused, Err(BinanceFuturesError::InvalidRequest)),
                "batch {side:?}: {refused:?}"
            );
        }
    }
}
