#!/usr/bin/env python3
"""Собрать вотчлист операторов HL для positions_poller.

Ранжирует адреса по МЕЙКЕРСКОМУ объёму из нашей записи ленты (поле `users` есть у
100% сделок HL) и разрешает каждый через `userRole`.

ВАЖНО, ПОЧЕМУ В СПИСОК ИДУТ ТОРГОВЫЕ АДРЕСА, А НЕ МАСТЕРА: позиции живут на том
адресе, который торгует. Если оператор работает через три сабаккаунта, у мастера
своих позиций может не быть вовсе, и опрос мастера покажет пустоту. Поэтому
опрашиваем торговые адреса, а мастер кладём рядом — по нему потом агрегировать.

Конвенция ленты: users = [покупатель, продавец]; при side == 'A' тейкер — продавец
(значит мейкер — покупатель), при side == 'B' наоборот. Конвенция восстановлена нами
и отдельного пина не имеет — если она неверна, ранжирование перевернётся.

Запуск: build_operator_watchlist.py <каталог записи HL> <выход.json> [--top N] [--no-roles]
"""

from __future__ import annotations

import argparse
import glob
import gzip
import json
import os
import sys
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

INFO_URL = "https://api.hyperliquid.xyz/info"
USER_AGENT = "hft-collector-watchlist/1.0"


def maker_volume(data_dir: Path) -> tuple[dict[str, float], dict[str, int], int]:
    """Мейкерский объём и число филлов по адресу, по всем файлам каталога."""
    vol: dict[str, float] = defaultdict(float)
    fills: dict[str, int] = defaultdict(int)
    seen: set[str] = set()
    n_tr = 0
    files = [f for f in sorted(glob.glob(str(data_dir / "*.gz")))
             if not os.path.basename(f).startswith("_meta")]
    for fp in files:
        try:
            with gzip.open(fp, "rt") as fh:
                for line in fh:
                    if '"trades"' not in line:
                        continue
                    try:
                        obj = json.loads(line.split(" ", 1)[1])
                    except (ValueError, IndexError):
                        continue
                    for tr in obj.get("data", []):
                        tid = tr.get("tid")
                        if tid in seen:
                            continue
                        seen.add(tid)
                        u = tr.get("users")
                        if not u or len(u) != 2:
                            continue
                        n_tr += 1
                        maker = u[0] if tr.get("side") == "A" else u[1]
                        vol[maker] += float(tr["px"]) * float(tr["sz"])
                        fills[maker] += 1
        except (OSError, EOFError):
            continue
    return vol, fills, n_tr


def fetch_role(addr: str) -> dict:
    body = json.dumps({"type": "userRole", "user": addr}).encode()
    req = urllib.request.Request(
        INFO_URL, data=body,
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("data_dir", type=Path)
    ap.add_argument("out_json", type=Path)
    ap.add_argument("--top", type=int, default=60)
    ap.add_argument("--no-roles", action="store_true", help="не ходить в сеть за ролями")
    args = ap.parse_args()

    vol, fills, n_tr = maker_volume(args.data_dir)
    print(f"сделок с users: {n_tr:,}   адресов-мейкеров: {len(vol):,}", flush=True)
    top = sorted(vol.items(), key=lambda kv: -kv[1])[: args.top]
    total = sum(vol.values())

    rows = []
    for addr, v in top:
        row = {"addr": addr, "maker_usd": round(v, 2), "fills": fills[addr],
               "share": round(v / total, 6) if total else 0.0}
        if not args.no_roles:
            try:
                r = fetch_role(addr)
                row["role"] = r.get("role")
                d = r.get("data")
                row["master"] = d.get("master") if isinstance(d, dict) else None
                time.sleep(0.25)
            except Exception as exc:  # noqa: BLE001 — роль необязательна для опроса позиций
                row["role"] = f"error: {type(exc).__name__}"
                row["master"] = None
        rows.append(row)

    args.out_json.write_text(json.dumps(rows, indent=1, ensure_ascii=False), encoding="utf-8")
    addrs_path = args.out_json.with_name(args.out_json.stem + "_addrs.json")
    addrs_path.write_text(json.dumps([r["addr"] for r in rows], indent=1), encoding="utf-8")

    subs = sum(1 for r in rows if r.get("role") == "subAccount")
    masters = {r["master"] for r in rows if r.get("master")}
    print(f"записано {len(rows)} адресов -> {args.out_json}")
    print(f"  сабаккаунтов: {subs}, различных мастеров за ними: {len(masters)}")
    print(f"  доля объёма, покрытая списком: {sum(r['share'] for r in rows):.3f}")
    print(f"  список для поллера: {addrs_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
