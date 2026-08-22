#!/usr/bin/env python3
"""Гейт качества для каталогов поллеров.

Зачем отдельный инструмент. `quality_report.py` построен вокруг записей
БИРЖЕВЫХ СТРИМОВ: у него есть сайдкар `_meta` с `session_start`, набор стримов
на символ, цепочки последовательности, каденция кадров. У поллера нет ничего из
этого: это снимки REST по таймеру. Профиль внутри того инструмента был бы
абстракцией поверх двух непохожих вещей, а гейт обходит инстансы по `etc/*.env`,
которых у поллеров тоже нет, — поэтому три каталога (`funding`, `params`,
`positions`) не проверялись НИЧЕМ. Замерено 2026-08-22.

Что проверяется, и каждая проверка отвечает измеренному отказу:

* **целостность gzip** — файл, дописывавшийся в момент нечистой смерти, теряет
  трейлер члена и читается только до обрыва. Ровно этот класс гейт поймал у
  `bybit/hftusdt` 21.08;
* **число тиков против каденции** — 12.08 таймер params молча не сработал 23
  часа подряд, и единственным следом была тишина. Тик считается по МЕТКЕ
  ВРЕМЕНИ в записях, а не по числу строк: у разных поллеров на один тик
  приходится разное число записей (funding — по эндпоинту, positions — role +
  funding);
* **самый большой разрыв между тиками** — половина суток без опроса при
  формально приличном покрытии;
* **исчезнувшая нога** — площадка/адрес, которые вчера писались, а сегодня нет.
  Без сверки со вчера такой день выглядит идеально чистым: проверять нечего,
  потому что файла нет;
* **записи об ошибках** — все три поллера пишут отказ в ряд (`kind: "error"`),
  а не глотают его. Непрочитанная запись об ошибке ничем не лучше проглоченной.

Использование:
    poller_report.py <каталог> --cadence-s 3600 [--day YYYYMMDD] [--json OUT]

Код возврата: 0 — зелёный или жёлтый, 1 — красный, 2 — проверить не удалось.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

GREEN, YELLOW, RED = "green", "yellow", "red"
_RANK = {GREEN: 0, YELLOW: 1, RED: 2}

DAY_RE = re.compile(r"_(\d{8})\.gz$")

# --- пороги: заморожены здесь, а не разбросаны по коду ------------------------
# Доля ожидаемых тиков, ниже которой день красный. Половина суток без опроса —
# это отказ, а не шум.
COVERAGE_RED = 0.50
# Ниже этого — жёлтый: пропуск-другой бывает от рестарта или джиттера таймера.
COVERAGE_YELLOW = 0.90
# Во сколько раз разрыв между тиками может превысить каденцию, оставаясь жёлтым.
# Три — это два подряд пропущенных тика; больше похоже на остановку.
GAP_YELLOW_FACTOR = 3.0


def parse_ts(record: dict[str, Any]) -> float | None:
    """Секунды эпохи из записи любого из трёх поллеров.

    Две формы, и обе настоящие: `ts` — ISO-8601 с зоной (params, positions),
    `t_local_ns` — наносекунды (funding). Запись без метки времени не тик, а
    мусор, и молча считать её тиком нельзя.
    """
    ts = record.get("ts")
    if isinstance(ts, str):
        try:
            return datetime.fromisoformat(ts).timestamp()
        except ValueError:
            return None
    ns = record.get("t_local_ns")
    if isinstance(ns, (int, float)):
        return float(ns) / 1e9
    return None


@dataclass
class Leg:
    """Одна нога поллера: площадка, адрес или пара «площадка/источник»."""

    name: str
    files: list[Path] = field(default_factory=list)
    ticks: list[float] = field(default_factory=list)
    errors: int = 0
    records: int = 0
    truncated: list[str] = field(default_factory=list)


def leg_name(rel: Path, day: str) -> str:
    """Имя ноги — путь с вырезанным днём.

    `funding_hyperliquid_20260821.gz` -> `funding_hyperliquid`;
    `bybit/params_bybit_instruments_20260821.gz` -> `bybit/params_bybit_instruments`;
    `0xabc/positions_0xabc_20260821.gz` -> `0xabc/positions_0xabc`.
    """
    return rel.as_posix().replace(f"_{day}.gz", "")


def read_leg(path: Path, leg: Leg, tick_tolerance_s: float) -> None:
    """Читает файл до конца, набирая тики, ошибки и признак обрыва."""
    leg.files.append(path)
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                leg.records += 1
                try:
                    record = json.loads(line)
                except ValueError:
                    continue
                if record.get("kind") == "error":
                    leg.errors += 1
                ts = parse_ts(record)
                if ts is None:
                    continue
                # Записи одного тика делят метку с точностью до долей секунды;
                # склеиваем их, иначе «тиков» окажется столько же, сколько строк.
                if not leg.ticks or ts - leg.ticks[-1] > tick_tolerance_s:
                    leg.ticks.append(ts)
    except (OSError, EOFError, gzip.BadGzipFile, zlib.error) as exc:
        leg.truncated.append(f"{path.name}: {type(exc).__name__}: {exc}")



def collect(root: Path, day: str, tick_tolerance_s: float) -> dict[str, Leg]:
    legs: dict[str, Leg] = {}
    for path in sorted(root.rglob(f"*_{day}.gz")):
        rel = path.relative_to(root)
        name = leg_name(rel, day)
        leg = legs.setdefault(name, Leg(name=name))
        read_leg(path, leg, tick_tolerance_s)
    for leg in legs.values():
        leg.ticks.sort()
    return legs


def previous_day(day: str) -> str:
    d = datetime.strptime(day, "%Y%m%d").replace(tzinfo=timezone.utc)
    return (d - timedelta(days=1)).strftime("%Y%m%d")


def legs_present(root: Path, day: str) -> set[str]:
    return {leg_name(p.relative_to(root), day) for p in root.rglob(f"*_{day}.gz")}


def expected_ticks(cadence_s: float, day: str, now: datetime | None = None) -> int:
    """Сколько тиков должно быть за сутки — с поправкой на НЕЗАКОНЧЕННЫЙ день.

    Без поправки сегодняшний день всегда красный: в полдень прошла половина
    ожидаемых тиков, и это не отказ.
    """
    now = now or datetime.now(timezone.utc)
    start = datetime.strptime(day, "%Y%m%d").replace(tzinfo=timezone.utc)
    elapsed = min((now - start).total_seconds(), 86400.0)
    if elapsed <= 0:
        return 0
    return max(1, int(elapsed // cadence_s))


def build_report(
    root: Path, day: str, cadence_s: float, now: datetime | None = None
) -> dict[str, Any]:
    tolerance = max(1.0, cadence_s / 10.0)
    legs = collect(root, day, tolerance)
    yesterday = previous_day(day)
    was = legs_present(root, yesterday)
    now_legs = set(legs)
    vanished = sorted(
        {leg_name(Path(w + f"_{yesterday}.gz"), yesterday) for w in was}
        - {leg_name(Path(n + f"_{day}.gz"), day) for n in now_legs}
    )

    want = expected_ticks(cadence_s, day, now)
    issues: list[dict[str, Any]] = []
    verdict = GREEN

    def add(level: str, kind: str, text: str) -> None:
        nonlocal verdict
        issues.append({"level": level, "kind": kind, "text": text})
        if _RANK[level] > _RANK[verdict]:
            verdict = level

    if not legs:
        add(RED, "no_data", f"нет ни одного файла за {day} в {root}")

    for name in vanished:
        add(RED, "leg_vanished", f"{name}: писалась {yesterday}, за {day} файла нет")

    for name in sorted(legs):
        leg = legs[name]
        for text in leg.truncated:
            add(RED, "gzip_integrity", f"{name}: {text}")
        got = len(leg.ticks)
        coverage = got / want if want else 0.0
        if got == 0:
            add(RED, "no_ticks", f"{name}: ни одного тика с меткой времени за {day}")
        elif coverage < COVERAGE_RED:
            add(RED, "tick_coverage",
                f"{name}: тиков {got} из ожидаемых {want} ({coverage:.0%})")
        elif coverage < COVERAGE_YELLOW:
            add(YELLOW, "tick_coverage",
                f"{name}: тиков {got} из ожидаемых {want} ({coverage:.0%})")
        if got >= 2:
            gaps = [b - a for a, b in zip(leg.ticks, leg.ticks[1:])]
            worst = max(gaps)
            if worst > cadence_s * GAP_YELLOW_FACTOR:
                add(YELLOW, "tick_gap",
                    f"{name}: наибольший разрыв {worst:.0f}s при каденции {cadence_s:.0f}s")
        if leg.errors:
            add(YELLOW, "poller_errors", f"{name}: записей об ошибке {leg.errors}")

    return {
        "schema": "poller-report-v1",
        "dir": str(root),
        "day": day,
        "cadence_s": cadence_s,
        "expected_ticks": want,
        "verdict": verdict,
        "legs": {
            name: {
                "ticks": len(leg.ticks),
                "records": leg.records,
                "errors": leg.errors,
                "files": [p.name for p in leg.files],
            }
            for name, leg in sorted(legs.items())
        },
        "issues": issues,
    }


def render(report: dict[str, Any]) -> str:
    lines = [
        f"poller report  schema={report['schema']}  verdict={report['verdict'].upper()}",
        f"=== {report['dir']}  день {report['day']}  каденция {report['cadence_s']:.0f}s",
        f"    ног {len(report['legs'])}, ожидалось тиков {report['expected_ticks']}",
    ]
    for issue in report["issues"]:
        lines.append(f"     [{issue['level']:6}] {issue['kind']:18} {issue['text']}")
    if not report["issues"]:
        lines.append("     чисто")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Гейт качества каталога поллера.")
    p.add_argument("dir", type=Path)
    p.add_argument("--cadence-s", type=float, required=True,
                   help="ожидаемый интервал между тиками, секунды")
    p.add_argument("--day", help="YYYYMMDD; по умолчанию вчера (UTC)")
    p.add_argument("--json", dest="json_out")
    return p


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    if not args.dir.is_dir():
        print(f"poller_report: {args.dir} не каталог", file=sys.stderr)
        return 2
    day = args.day or (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")
    report = build_report(args.dir, day, args.cadence_s)
    print(render(report))
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(report, indent=1, ensure_ascii=False),
                                       encoding="utf-8")
    return 1 if report["verdict"] == RED else 0


if __name__ == "__main__":
    raise SystemExit(main())
