#!/usr/bin/env python3
"""Поллер позиций операторов Hyperliquid.

Зачем. HL публично отдаёт ПОЧАСОВУЮ ПОЗИЦИЮ ЛЮБОГО АДРЕСА: в записях
`userFunding` поле `delta.szi` — знаковый размер позиции на момент начисления.
Это фактически per-address hourly Commitments of Traders, чего в традиционных
рынках нет (у CFTC недельно, агрегированно и с лагом). И это НЕ он-чейн-анализ:
классическое наблюдение за кошельками требует вывода «перевод на биржу ⇒
намерение продать ⇒ сделка», а здесь звеньев нет — видна сама позиция на той
площадке, где мы торгуем.

Почему срочно. Окно `userFunding` ограничено, история укатывается, и никто её не
ведёт. Та же логика, что у поллера параметров: каждый неснятый час потерян
навсегда.

Контракт записи (тот же дух, что у коллектора): пишем как отдаёт площадка,
ничего не мержим и ничего не интерпретируем. Дедупликация — только по времени
последней виденной записи, чтобы ряд не распухал копиями; отсутствие новых
записей оставляет ПУЛЬС, потому что иначе «позиция не менялась» и «мы не
смотрели» неразличимы.

Сабаккаунты. Измерено 2026-08-12: 8 из 12 топ-мейкеров HYPE — `subAccount`,
причём трое под одним мастером. Сабаккаунт не имеет своей истории моста и своего
стейкинга, а тариф наследует. Поэтому мастер разрешается и кладётся РЯДОМ С
КАЖДОЙ записью — чтобы агрегация по оператору не требовала второго джойна.

Запуск: positions_poller.py <выходной каталог> <файл со списком адресов> [--once]
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

INFO_URL = "https://api.hyperliquid.xyz/info"
USER_AGENT = "hft-collector-positions/1.0"
TIMEOUT_S = 25
# Площадка отдаёт не больше этого за один ответ; полный ответ означает «есть ещё».
RECORD_LIMIT = 500
# Предохранитель от бесконечной пагинации, если площадка перестанет продвигать время.
MAX_PAGES = 40
# Пауза между вызовами. /info лимитируется по ВЕСУ, а бэкфилл делает по несколько
# страниц на адрес: первый прогон по 60 адресам с паузой 0.25 с получил 429 на 37 из 60.
SLEEP_S = 1.0
# Ретрай на 429 и 5xx. Без него часовой ряд превращается в решето: каждый прогон
# терял бы часть адресов, и дыры были бы РАЗНЫЕ каждый час — то есть невосстановимые
# даже задним числом.
RETRY_STATUS = (429, 500, 502, 503, 504)
RETRIES = 5
BACKOFF_S = 2.0

Fetch = Callable[..., Any]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def fetch_info(
    kind: str,
    addr: str,
    start: int | None = None,
    *,
    _open: Callable[..., Any] = urllib.request.urlopen,
    _sleep: Callable[[float], None] = time.sleep,
) -> Any:
    """Вызов /info с экспоненциальным отступлением на рейт-лимит.

    Инъекция `_open`/`_sleep` — шов для тестов: сеть в них не трогается.
    Исчерпав попытки, поднимаем последнюю ошибку — её запишет вызывающий как
    наблюдение (аутэдж адреса сам по себе факт), но уже после честных попыток.
    """
    body: dict[str, Any] = {"type": kind, "user": addr}
    if start is not None:
        body["startTime"] = start
    req = urllib.request.Request(
        INFO_URL, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
    )
    last: Exception | None = None
    for attempt in range(RETRIES):
        try:
            with _open(req, timeout=TIMEOUT_S) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code not in RETRY_STATUS:
                raise
            _sleep(BACKOFF_S * (2 ** attempt))
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last = exc
            _sleep(BACKOFF_S * (2 ** attempt))
    raise last if last else RuntimeError("fetch_info: попытки исчерпаны без ошибки")


def resolve_master(role_resp: Any) -> tuple[str | None, str]:
    """(мастер, роль) из ответа userRole. ТОТАЛЬНАЯ: неизвестная форма не валит проход.

    Роли у HL: user (сам себе мастер), subAccount (есть master), vault, agent.
    Отсутствие мастера у сабаккаунта — тоже валидный исход: разрешать нечем,
    и лучше записать это честно, чем угадать.
    """
    if not isinstance(role_resp, dict):
        return None, "unknown"
    role = role_resp.get("role") or "unknown"
    data = role_resp.get("data")
    master = data.get("master") if isinstance(data, dict) else None
    return master, role


def load_state(out_dir: Path) -> dict[str, int]:
    p = out_dir / ".positions_state.json"
    if not p.exists():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        return {k: int(v) for k, v in raw.items()}
    except (json.JSONDecodeError, OSError, TypeError, ValueError):
        # Битое состояние читается как «ничего не знаем»: в худшем случае перекачаем
        # лишнее и отсечём по времени. Обратная ошибка — пропустить часы навсегда.
        return {}


def save_state(out_dir: Path, state: dict[str, int]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / ".positions_state.json"
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1), encoding="utf-8")
    tmp.replace(p)  # атомарно: недописанное состояние хуже отсутствующего


def write_record(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
    with gzip.open(path, "at", encoding="utf-8") as fh:
        fh.write(line)


def fetch_funding_since(fetch: Fetch, addr: str, since: int | None) -> list[dict]:
    """Все записи строго ПОСЛЕ `since`, с пагинацией по времени."""
    out: list[dict] = []
    start = (since + 1) if since is not None else None
    for _ in range(MAX_PAGES):
        page = fetch("userFunding", addr, start)
        if not isinstance(page, list) or not page:
            break
        fresh = [r for r in page if since is None or int(r.get("time", 0)) > since]
        out.extend(fresh)
        if len(page) < RECORD_LIMIT:
            break
        last = max(int(r.get("time", 0)) for r in page)
        if start is not None and last + 1 <= start:
            break  # площадка не продвигает время — выходим, а не крутимся
        start = last + 1
        time.sleep(SLEEP_S)
    out.sort(key=lambda r: int(r.get("time", 0)))
    return out


def poll_once(out_dir: Path, addrs_file: Path, fetch: Fetch = fetch_info) -> dict[str, str]:
    """Один проход по списку адресов. Возвращает сводку исходов по адресам."""
    addrs = json.loads(Path(addrs_file).read_text(encoding="utf-8"))
    state = load_state(out_dir)
    outcomes: dict[str, str] = {}
    stamp = now_utc()
    day = stamp.strftime("%Y%m%d")

    for addr in addrs:
        path = out_dir / addr[:10] / f"positions_{addr[:10]}_{day}.gz"
        base = {"ts": stamp.isoformat(), "user": addr}
        try:
            master, role = resolve_master(fetch("userRole", addr))
            time.sleep(SLEEP_S)
            write_record(path, {**base, "kind": "role", "role": role, "master": master})
            recs = fetch_funding_since(fetch, addr, state.get(addr))
        except (urllib.error.URLError, OSError, json.JSONDecodeError, TimeoutError,
                ValueError, TypeError) as exc:
            # Недоступность адреса — само по себе наблюдение, и она НЕ должна уносить
            # остальной проход (урок #75: чужой 503 убил сбор насовсем).
            write_record(path, {**base, "kind": "error",
                                "error": f"{type(exc).__name__}: {exc}"[:300]})
            outcomes[addr] = "error"
            continue

        if not recs:
            write_record(path, {**base, "kind": "pulse", "master": master, "role": role,
                                "since": state.get(addr)})
            outcomes[addr] = "unchanged"
            continue

        for r in recs:
            write_record(path, {**base, "kind": "funding", "master": master, "role": role,
                                "time": int(r.get("time", 0)), "delta": r.get("delta")})
        state[addr] = max(int(r.get("time", 0)) for r in recs)
        outcomes[addr] = f"new:{len(recs)}"

    save_state(out_dir, state)
    return outcomes


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("out_dir", type=Path)
    ap.add_argument("addrs_file", type=Path, help="JSON-список адресов (мастеров)")
    ap.add_argument("--once", action="store_true", help="один проход и выход (для таймера)")
    ap.add_argument("--interval-s", type=int, default=3600)
    args = ap.parse_args()

    while True:
        began = time.time()
        o = poll_once(args.out_dir, args.addrs_file)
        new = sum(int(v.split(":")[1]) for v in o.values() if v.startswith("new:"))
        errs = [a for a, v in o.items() if v == "error"]
        print(
            f"{now_utc().isoformat()} адресов={len(o)} новых_записей={new} "
            f"ошибок={len(errs)}" + (f" ОШИБКИ: {', '.join(errs)}" if errs else ""),
            flush=True,
        )
        if args.once:
            return 0
        time.sleep(max(0.0, args.interval_s - (time.time() - began)))


if __name__ == "__main__":
    sys.exit(main())
