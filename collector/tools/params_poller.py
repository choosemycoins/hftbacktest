#!/usr/bin/env python3
"""Поллер администрируемых параметров площадок.

Зачем. Мы измерили, что кросс-венью фандинг-спред — это разность ЛИТЕРАЛОВ в
формулах площадок, а не рыночная величина. Тот же слой шире: тик, минимальный
ноутионал, тиры маржи, максимальное плечо, статус рынка, комиссии, интервал
фандинга — всё это назначается деплоем или голосованием, а не торгуется. Каждое
изменение — датированное событие с известным направлением воздействия, и ряда
этих изменений не ведёт никто, потому что это считается сантехникой.

Из корпуса Lopata: «полик ввёл тик 0.0025 без объявления, за ночь заректило».
То есть изменение параметра двигает деньги — просто никто не смотрит.

Контракт записи (тот же дух, что у коллектора): пишем СНИМКИ, ничего не
интерпретируем и ничего не мержим. Чтобы не копить одинаковое, снимок пишется
целиком только при ИЗМЕНЕНИИ хеша; иначе — однострочный пульс, который
доказывает, что в этот час параметры были те же. Пульс важен не меньше снимка:
без него «не изменилось» и «мы не смотрели» неразличимы.

Запуск: params_poller.py <выходной каталог> [--once]
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

TIMEOUT_S = 20
USER_AGENT = "hft-collector-params/1.0"

# --- источники -------------------------------------------------------------
# Только публичные каталоги без ключей. Если у площадки каталог отдаётся
# несколькими эндпоинтами, берём тот, где живут ПАРАМЕТРЫ, а не котировки.

Source = tuple[str, str]  # (имя, url)

SOURCES: dict[str, list[Source]] = {
    "hyperliquid": [("meta", "https://api.hyperliquid.xyz/info")],  # POST, см. fetch_json
    "lighter": [("orderBooks", "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks")],
    "aster": [("exchangeInfo", "https://fapi.asterdex.com/fapi/v1/exchangeInfo")],
    # У Extended параметры живут на starknet-хосте; голый api.extended.exchange отдаёт 404.
    "extended": [("markets", "https://api.starknet.extended.exchange/api/v1/info/markets")],
    "paradex": [("markets", "https://api.prod.paradex.trade/v1/markets")],
    "bybit": [
        ("instruments", "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"),
        ("risk-limit", "https://api.bybit.com/v5/market/risk-limit?category=linear"),
    ],
    # leverageBracket у Binance — ПОДПИСАННЫЙ эндпоинт (проверено: -2014 без ключа),
    # поэтому тиры маржи оттуда недоступны; фильтры символов есть в exchangeInfo.
    "binanceusdm": [("exchangeInfo", "https://fapi.binance.com/fapi/v1/exchangeInfo")],
}

# Поля, меняющиеся каждый опрос и не несущие информации о ПАРАМЕТРАХ. Их надо
# выбросить до хеширования, иначе «изменилось» будет срабатывать всегда и
# ряд изменений утонет в шуме.
VOLATILE_KEYS = {
    "serverTime", "timestamp", "time", "updated_at", "updatedAt", "ts",
    "openInterest", "volume", "quoteVolume", "lastPrice", "markPrice",
    "indexPrice", "fundingRate", "nextFundingTime", "bid", "ask",
    "best_bid_price", "best_ask_price", "daily_quote_token_volume",
    "daily_base_token_volume", "last_trade_price", "open_interest",
    "dailyVolume", "next_funding_rate", "funding_rate", "cursor",
    # Замерено на живом смоуке 11.08: Extended отдаёт эти поля в каталоге рынков.
    "askPrice", "bidPrice", "dailyPriceChange", "dailyPriceChangePercentage",
    "dailyVolumeBase", "dailyVolumeQuote", "openInterestBase", "lastTradePrice",
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def fetch_json(url: str) -> Any:
    """GET, кроме Hyperliquid — там каталог отдаётся только POST-ом."""
    if "hyperliquid" in url:
        body = json.dumps({"type": "meta"}).encode()
        req = urllib.request.Request(
            url, data=body,
            headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
        )
    else:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
        return json.loads(resp.read().decode())


def strip_volatile(node: Any) -> Any:
    """Рекурсивно выбросить поля, которые дышат сами по себе.

    Консервативно: выбрасываем по ИМЕНИ ключа, а не по типу значения — лучше
    потерять пару полей, чем получить ложное «параметр изменился» каждый час.
    """
    if isinstance(node, dict):
        return {k: strip_volatile(v) for k, v in sorted(node.items()) if k not in VOLATILE_KEYS}
    if isinstance(node, list):
        # Порядок элементов каталога НЕ несёт информации, а площадки его не держат:
        # замерено на живом смоуке 11.08 — Lighter и Paradex вернули те же рынки в
        # другом порядке через 11 секунд, и позиционное сравнение объявило
        # «изменились» min_base_amount, funding_period_hours и ещё 44 параметра.
        # Без канонизации порядка ряд изменений был бы шумом на сто процентов.
        return sorted(
            (strip_volatile(v) for v in node),
            key=lambda x: json.dumps(x, sort_keys=True, separators=(",", ":")),
        )
    return node


def stable_hash(payload: Any) -> str:
    canonical = json.dumps(strip_volatile(payload), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def write_record(path: Path, record: dict[str, Any]) -> None:
    """Одна строка JSON на запись, gzip, дозапись. Формат — как у коллектора."""
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
    with gzip.open(path, "at", encoding="utf-8") as fh:
        fh.write(line)


def last_hashes(state_path: Path) -> dict[str, str]:
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        # Битое состояние читается как «ничего не знаем»: в худшем случае
        # запишем лишний полный снимок. Обратная ошибка — пропустить изменение.
        return {}


def save_hashes(state_path: Path, hashes: dict[str, str]) -> None:
    tmp = state_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(hashes, indent=1), encoding="utf-8")
    tmp.replace(state_path)  # атомарно: недописанное состояние хуже отсутствующего


def poll_once(out_dir: Path, fetch: Callable[[str], Any] = fetch_json) -> dict[str, str]:
    """Один проход по всем источникам. Возвращает сводку исходов по ключам."""
    state_path = out_dir / ".params_state.json"
    known = last_hashes(state_path)
    fresh = dict(known)
    outcomes: dict[str, str] = {}
    stamp = now_utc()
    day = stamp.strftime("%Y%m%d")

    for venue, sources in SOURCES.items():
        for name, url in sources:
            key = f"{venue}/{name}"
            path = out_dir / venue / f"params_{venue}_{name}_{day}.gz"
            try:
                payload = fetch(url)
            except (urllib.error.URLError, OSError, json.JSONDecodeError, TimeoutError) as exc:
                # Недоступность площадки — САМА ПО СЕБЕ наблюдение (см. линию
                # «здоровье площадок»), поэтому она записывается, а не молчит.
                write_record(path, {
                    "ts": stamp.isoformat(), "venue": venue, "source": name,
                    "kind": "error", "error": f"{type(exc).__name__}: {exc}"[:300],
                })
                outcomes[key] = "error"
                continue

            digest = stable_hash(payload)
            if known.get(key) == digest:
                write_record(path, {
                    "ts": stamp.isoformat(), "venue": venue, "source": name,
                    "kind": "pulse", "sha256": digest,
                })
                outcomes[key] = "unchanged"
            else:
                write_record(path, {
                    "ts": stamp.isoformat(), "venue": venue, "source": name,
                    "kind": "snapshot", "sha256": digest,
                    "prev_sha256": known.get(key), "payload": payload,
                })
                outcomes[key] = "changed" if key in known else "first"
            fresh[key] = digest

    save_hashes(state_path, fresh)
    return outcomes


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("out_dir", type=Path)
    ap.add_argument("--once", action="store_true", help="один проход и выход (для таймера)")
    ap.add_argument("--interval-s", type=int, default=3600)
    args = ap.parse_args()

    while True:
        began = time.time()
        outcomes = poll_once(args.out_dir)
        changed = [k for k, v in outcomes.items() if v == "changed"]
        errors = [k for k, v in outcomes.items() if v == "error"]
        print(
            f"{now_utc().isoformat()} источников={len(outcomes)} "
            f"изменилось={len(changed)} ошибок={len(errors)}"
            + (f" ИЗМЕНЕНИЯ: {', '.join(changed)}" if changed else "")
            + (f" ОШИБКИ: {', '.join(errors)}" if errors else ""),
            flush=True,
        )
        if args.once:
            return 0
        time.sleep(max(0.0, args.interval_s - (time.time() - began)))


if __name__ == "__main__":
    sys.exit(main())
