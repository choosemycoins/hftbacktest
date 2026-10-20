//! Кто раньше сообщает про изменение тача: `bookTicker` или `depth@0ms`.
//!
//! # Зачем
//!
//! Замер `binance_feed_latency` показал, что КАДР `bookTicker` доезжает
//! свежее диффового: возраст от матча 1.7 мс против 8.7 с хвостом до 28.7.
//! Но из этого ещё не следует, что мы раньше УЗНАЁМ ПРО ТАЧ: диффовый кадр
//! может нести то же самое изменение, просто позже помеченное.
//!
//! # Как связываются два потока — точно, а не приблизительно
//!
//! У `bookTicker` есть `u` — идентификатор обновления книги. У `depthUpdate`
//! диапазон `[U, u]`. Одно и то же событие книги попадает ровно в один
//! диапазон, поэтому пара находится однозначно, без догадок по времени.
//!
//! Считаем `recv(depth) − recv(bookTicker)` для сопоставленных пар:
//! положительное = тач узнали из тикера раньше.
//!
//! Побочно проверяется согласие цен: тач из `bookTicker` против лучших цен,
//! перечисленных в том же диффовом кадре. Расхождение здесь означало бы, что
//! потоки описывают разное, и тогда сравнение времён бессмысленно.
//!
//! Запуск: `binance_ticker_vs_depth <символ> <секунд>`
use std::{
    collections::VecDeque,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const URL: &str = "wss://fstream.binance.com/ws";
/// Сколько диффовых кадров держим для сопоставления. Тикер приходит чаще
/// диффов, так что окна в пару сотен хватает с большим запасом.
const KEEP: usize = 512;

fn now_ns() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64
}

fn pct(v: &mut Vec<f64>, q: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[(((v.len() - 1) as f64) * q).round() as usize]
}

struct Depth {
    first: i64,
    last: i64,
    recv: i64,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
}

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args: Vec<String> = std::env::args().collect();
    let symbol = args.get(1).cloned().unwrap_or_else(|| "hypeusdt".into());
    let secs: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(300);

    let (mut ws, _) = connect_async(URL).await.expect("connect");
    let sub = format!(
        r#"{{"method":"SUBSCRIBE","params":["{symbol}@bookTicker","{symbol}@depth@0ms"],"id":1}}"#
    );
    ws.send(Message::Text(sub.into())).await.expect("subscribe");
    eprintln!("подписан на {symbol}, слушаю {secs} с...");

    let mut depths: VecDeque<Depth> = VecDeque::with_capacity(KEEP);
    let mut lead: Vec<f64> = Vec::new();      // recv(depth) − recv(ticker), мс
    let mut pending: Vec<(i64, i64, f64, f64)> = Vec::new(); // тикеры без пары: (u, recv, bid, ask)
    let (mut matched, mut unmatched, mut price_ok, mut price_bad) = (0u64, 0u64, 0u64, 0u64);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);

    loop {
        let msg = tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            m = ws.next() => match m {
                Some(Ok(m)) => m,
                Some(Err(e)) => { eprintln!("сокет: {e}"); break }
                None => break,
            },
        };
        let Message::Text(txt) = msg else { continue };
        let recv = now_ns();
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) else { continue };
        match v.get("e").and_then(|x| x.as_str()) {
            Some("bookTicker") => {
                let (Some(u), Some(b), Some(a)) = (
                    v.get("u").and_then(|x| x.as_i64()),
                    v.get("b").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()),
                    v.get("a").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()),
                ) else { continue };
                pending.push((u, recv, b, a));
            }
            Some("depthUpdate") => {
                let (Some(first), Some(last)) = (
                    v.get("U").and_then(|x| x.as_i64()),
                    v.get("u").and_then(|x| x.as_i64()),
                ) else { continue };
                let top = |k: &str, hi: bool| -> Option<f64> {
                    let arr = v.get(k)?.as_array()?;
                    arr.iter()
                        .filter_map(|lv| {
                            let p = lv.get(0)?.as_str()?.parse::<f64>().ok()?;
                            let q = lv.get(1)?.as_str()?.parse::<f64>().ok()?;
                            (q > 0.0).then_some(p)
                        })
                        .fold(None, |acc: Option<f64>, p| {
                            Some(match acc {
                                None => p,
                                Some(x) => if hi { x.max(p) } else { x.min(p) },
                            })
                        })
                };
                let d = Depth { first, last, recv, best_bid: top("b", true), best_ask: top("a", false) };
                // Сопоставляем накопившиеся тикеры с этим диапазоном.
                pending.retain(|(u, trecv, tb, ta)| {
                    if *u >= d.first && *u <= d.last {
                        matched += 1;
                        lead.push((d.recv - *trecv) as f64 / 1e6);
                        // согласие цен: тач тикера должен присутствовать среди
                        // уровней диффа, если дифф вообще трогал эту сторону
                        let ok_b = d.best_bid.map_or(true, |p| (p - *tb).abs() < 1e-9 || p < *tb);
                        let ok_a = d.best_ask.map_or(true, |p| (p - *ta).abs() < 1e-9 || p > *ta);
                        if ok_b && ok_a { price_ok += 1 } else { price_bad += 1 }
                        return false;
                    }
                    // тикер старше всего окна — списываем
                    if *u < d.first && depths.len() >= KEEP { unmatched += 1; return false }
                    true
                });
                if depths.len() == KEEP { depths.pop_front(); }
                depths.push_back(d);
            }
            _ => {}
        }
    }

    println!("\nСОПОСТАВЛЕНО ПО ИДЕНТИФИКАТОРУ ОБНОВЛЕНИЯ");
    println!("  пар: {matched}   не нашли пары: {unmatched}   висят: {}", pending.len());
    if matched == 0 { println!("  сопоставить нечего"); return }
    println!("\nrecv(depth) − recv(bookTicker), мс   (плюс = тикер раньше)");
    println!("  p1 {:.1}  p25 {:.1}  p50 {:.1}  p75 {:.1}  p99 {:.1}   среднее {:.1}",
        pct(&mut lead, 0.01), pct(&mut lead, 0.25), pct(&mut lead, 0.50),
        pct(&mut lead, 0.75), pct(&mut lead, 0.99),
        lead.iter().sum::<f64>() / lead.len() as f64);
    let earlier = lead.iter().filter(|x| **x > 0.0).count();
    println!("  тикер пришёл раньше в {:.1}% пар", 100.0 * earlier as f64 / lead.len() as f64);
    println!("\nсогласие цен: сошлось {price_ok}, разошлось {price_bad}");
}
