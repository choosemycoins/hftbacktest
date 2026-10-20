//! Сколько времени кадр Binance USD-M идёт до ЭТОЙ машины.
//!
//! # Зачем
//!
//! Задержку фида мы мерили на токийском СБОРЩИКЕ (2026-08-26, ethfiusdt,
//! 900 тыс. строк): от метки публикации `E` до нас — `depthUpdate` 0.7 мс по
//! медиане, `bookTicker` 1.3, `trade` 1.4; от метки матча `T` — 26.6 / 1.6 / 1.6.
//! Но торгует не сборщик. Бот живёт на другой машине, и её собственный путь до
//! Binance не мерен ни разу — при том, что на стороне действия (HL) у нас
//! намерено всё до десятых: постановка 196.6 мс, отмена 195.4.
//!
//! Асимметрия неприятная: сторона, которой мы бьём, измерена; сторона, которой
//! мы смотрим, — нет. Этот бинарь её закрывает.
//!
//! # Что считает
//!
//! Для каждого кадра две разности:
//!   `now − E` — от момента, когда Binance его ОПУБЛИКОВАЛ;
//!   `now − T` — от момента МАТЧА, то есть возраст самой информации.
//! Разница между ними — внутренняя задержка Binance перед публикацией; на
//! сборщике она составляла 25 мс у диффов стакана и ~0 у тача.
//!
//! # Оговорки, без которых число читать нельзя
//!
//! * `E` и `T` квантованы до миллисекунды — субмиллисекундные значения читать
//!   как «в пределах такта».
//! * Часы машины обязаны быть дисциплинированы. Проверять ДО замера
//!   (`chronyc tracking`): на hl-grid-1 на 2026-08-28 расхождение 14 нс,
//!   RMS 3.9 мкс — на порядки точнее сетки `E`.
//! * Отрицательных значений быть не должно. Появились — часы врут, и весь
//!   замер надо выбросить, а не поправлять.
//!
//! Запуск: `binance_feed_latency <символ> <секунд>`, например `hypeusdt 300`.
use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const URL: &str = "wss://fstream.binance.com/ws";

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn pct(v: &mut Vec<f64>, q: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[(((v.len() - 1) as f64) * q).round() as usize]
}

#[tokio::main]
async fn main() {
    // Тот же явный выбор провайдера, что делает сам коннектор: при двух
    // доступных rustls отказывается угадывать и падает на первом хендшейке.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args: Vec<String> = std::env::args().collect();
    let symbol = args.get(1).cloned().unwrap_or_else(|| "hypeusdt".into());
    let secs: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(300);

    let (mut ws, _) = connect_async(URL).await.expect("connect");
    let sub = format!(
        r#"{{"method":"SUBSCRIBE","params":["{symbol}@bookTicker","{symbol}@trade","{symbol}@depth@0ms"],"id":1}}"#
    );
    ws.send(Message::Text(sub.into())).await.expect("subscribe");
    eprintln!("подписан на {symbol}, слушаю {secs} с...");

    // поток -> (от публикации, от матча)
    let mut from_e: HashMap<String, Vec<f64>> = HashMap::new();
    let mut from_t: HashMap<String, Vec<f64>> = HashMap::new();
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
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) else {
            continue;
        };
        let Some(ev) = v.get("e").and_then(|x| x.as_str()) else {
            continue; // ответ на подписку и прочая служебка
        };
        if let Some(e) = v.get("E").and_then(|x| x.as_i64()) {
            from_e
                .entry(ev.to_string())
                .or_default()
                .push((recv - e * 1_000_000) as f64 / 1e6);
        }
        if let Some(t) = v.get("T").and_then(|x| x.as_i64()) {
            from_t
                .entry(ev.to_string())
                .or_default()
                .push((recv - t * 1_000_000) as f64 / 1e6);
        }
    }

    let report = |title: &str, m: &mut HashMap<String, Vec<f64>>| {
        println!("\n{title}");
        println!(
            "{:14} {:>9} {:>8} {:>8} {:>8} {:>8} {:>8}",
            "поток", "n", "p1", "p25", "p50", "p75", "p99"
        );
        let mut keys: Vec<String> = m.keys().cloned().collect();
        keys.sort();
        for k in keys {
            let v = m.get_mut(&k).unwrap();
            let n = v.len();
            let neg = v.iter().filter(|x| **x < 0.0).count();
            print!(
                "{:14} {:9} {:8.1} {:8.1} {:8.1} {:8.1} {:8.1}",
                k,
                n,
                pct(v, 0.01),
                pct(v, 0.25),
                pct(v, 0.50),
                pct(v, 0.75),
                pct(v, 0.99)
            );
            if neg > 0 {
                print!("   ⚠ отрицательных {neg} — часы врут, замер недействителен");
            }
            println!();
        }
    };
    report("ОТ ПУБЛИКАЦИИ (now − E), мс:", &mut from_e);
    report("ОТ МАТЧА (now − T), мс — возраст информации:", &mut from_t);
}
