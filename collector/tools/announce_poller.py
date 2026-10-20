#!/usr/bin/env python3
"""Поллер анонсов «решение биржи»: делистинги, листинги, вселенные площадок, правила
наградных программ. Одно сообщение в Telegram на каждое новое решение.

ЗАЧЕМ. Семейство «решение биржи/календарь» — единственное живое в карте рынка
(`reference_market-map-vs-our-history`): всё остальное закрыто отрицательно, по доступу
или по ёмкости. У этого семейства цена измерена и она вся в первом часе:

  * панель 09.09 (430 делистингов Binance + 474 Bybit за 24 месяца) даёт профиль
    −12.3 % в ЧАС публикации анонса против −3.9 / −1.4 / −1.8 % на следующие сутки.
    Три четверти хода уходит там, где нас нет;
  * наградные окна (Lighter LPP) живут 4–8 недель и затухают с первого дня;
    правила меняются молча — недельный `topic-scan` увидит смену через 3–4 дня.

Поэтому прибор ходит раз в 5 минут, а не раз в неделю, и живёт на машине оператора
под user-таймером, а не в голове.

ЧТО ЭТО НЕ ДЕЛАЕТ. Он не торгует, не считает деньги и ничего не решает: он только
называет решение площадки, время ПЛОЩАДКИ, локальное время наблюдения и ссылку.
Все выводы — снаружи.

УСТРОЙСТВО. Ровно как `day0_poller.py`: чистая `plan_tick` (сырые байты источников +
состояние + локальные часы → план ДАННЫМИ), исполнение — через тонкий `Host`,
подменяемый в тестах. Идемпотентность считается по НАБЛЮДЁННЫМ эффектам: ключ статьи
помечается виденным только после того, как Telegram принял сообщение, а снимок
вселенной сдвигается только когда доставлены все изменения, которые он несёт. Без
этого обрыв между отправкой и записью состояния теряет событие навсегда: страница
анонсов отдаёт только последние 50, назад её не отмотать.

ЧЕТЫРЕ ВЕЩИ, КОТОРЫЕ ЗДЕСЬ ЛЕГКО СДЕЛАТЬ НЕПРАВИЛЬНО.

1. ПУСТОЙ ОТВЕТ — НЕ МИР. Нераспарсенный, пустой или усечённый ответ читается как
   провал ИСТОЧНИКА. Для diff-источников (Hyperliquid, Lighter) цена прямая: пустая
   вселенная, прочитанная как мир, — это «делистнули всё», алерт на каждую монету и
   ложный снимок, после которого настоящий делистинг уже не виден.
2. ЧАСЫ РАЗДЕЛЕНЫ. Метка площадки печатается со своим именем поля (`releaseDate`,
   `publishTime`, `pubDate`), локальные UTC — отдельной строкой. Урок 28.08: «лаг
   площадки» тогда оказался нашей же задержкой приёма.
3. ХЭШ СТРАНИЦЫ — ПО ТЕКСТУ, А НЕ ПО БАЙТАМ. Замерено 2026-09-09 22:00–22:05Z: два
   ответа страницы Kraken за пять минут отличались только каруселью «related posts».
   Хэш сырых байт мигал бы каждый час навсегда. Считается дайджест видимого текста, а
   само изменение объявляется только после ПОДТВЕРЖДЕНИЯ на следующей проверке.

4. ТИХАЯ НЕДОСТАВКА — ТОЖЕ СЛЕПОТА. Отсутствие TG_BOT_TOKEN/TG_CHAT_ID вне `--dry-run`
   фатально ДО тика (код 3): иначе каждое сообщение отвергается, ключи не метятся, а
   код выхода и пульс выглядят здоровыми. Что Telegram отверг уже в тике, считает поле
   `undelivered` в пульсе — по нему и по возрасту самого пульса сторожит внешний таймер.

Запуск:  announce_poller.py --once [--dry-run] [--state PATH] [--sources a,b]
Секреты: TG_BOT_TOKEN / TG_CHAT_ID только из окружения (EnvironmentFile у systemd).
Рядом:   <state>/pulse.json — пульс; <state>/last_fail/<источник>.bin — тело ответа,
         на котором источник провалился (первые 64 КБ, для разбора постфактум).
"""

from __future__ import annotations

import argparse
import copy
import dataclasses
import gzip
import hashlib
import html
import json
import os
import re
import socket
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Protocol

# Версия релиза. Имя файла в проде — `announce_poller-<VERSION>-<sha8>.py`, и до 1.2
# версия жила ТОЛЬКО там: User-Agent отставал на два релиза и врал площадкам о том, кто
# их спрашивает. Теперь она объявлена один раз и едет в UA сама.
VERSION = "1.5"
USER_AGENT = f"hft-announce-poller/{VERSION}"
# Замерено: все зарегистрированные источники, кроме блога Kraken, отвечают 200 на
# честный UA — маскироваться под браузер не нужно и не надо. Блог 2026-09-14 отдавал
# 403 с JS-челленджем ЛЮБОМУ UA (проверено с кипрской машины оператора и с токийского
# хоста сбора), но делает это НЕ ВСЕГДА — челлендж интермиттентный, и маскировка тут
# ничего не решает. Разбор и пороги — в `SOURCES` и у FAIL_ALERT_AFTER_CHALLENGE.
HTTP_TIMEOUT_S = 20

# --- каденция ----------------------------------------------------------------
# API-источники: 5 минут. Час публикации анонса стоит трёх четвертей хода, поэтому
# внутричасовое разрешение — это и есть смысл прибора.
CADENCE_API_S = 300
# Блог Kraken — обычный WordPress-фид на 124 КБ. 15 минут вместо 5 экономят площадке
# две трети трафика и ничего не стоят: анонсы там не минутные.
CADENCE_FEED_S = 900
# Страницы программ меняются неделями, а не минутами.
CADENCE_HASH_S = 3600
# Таймер systemd ходит с RandomizedDelaySec; строгое «прошло ли ровно cadence»
# выкидывало бы каждый второй цикл источника.
DUE_SLACK_S = 60

# --- пороги ------------------------------------------------------------------
# ~30 минут тишины API-источника (~6 часов для часовых). Ниже порога молчим: мигание
# сети не повод будить оператора.
FAIL_ALERT_AFTER = 6
# Бот-защита — особый класс провала, и порог у неё свой. Измерено 2026-09-14 на блоге
# Kraken: челлендж ИНТЕРМИТТЕНТНЫЙ. Прод-поллер прочитал фид в 19:10:10Z, а зонд из
# 20 запросов раз в минуту в 19:36:45–19:55:49Z получил 20 из 20 челленджей (одно и то
# же тело в 6460 байт); токийский хост в 19:36:40Z и 20:04:06Z — то же самое. То есть
# открытые окна реже, чем одно на 20 минут, и этим зондом не характеризуются.
# Арифметика: 8 провалов × 900 с каденции блога = 2 часа; у часовой страницы LP те же
# 8 — это 8 часов. Восемь выбрано как МИНИМУМ, дающий два часа на самом быстром
# источнике, который челлендж вообще отдаёт. Шесть (полтора часа у блога) кричали бы о
# норме этого источника, а канал, в котором кричат о норме, перестают читать.
# ⚠️ Появится челлендж у пятиминутного источника — 8 даст там всего 40 минут, и
# константу придётся выводить заново: измерения для такого источника у нас нет.
FAIL_ALERT_AFTER_CHALLENGE = 8
# ~3 часа. Пока провал длится — напоминание, а не шторм.
FAIL_REALERT_EVERY = 36
# Потолок сообщений за тик. Пачка из 50 анонсов не имеет права стать 50 сообщениями
# подряд (Telegram режет), но и потеряться не имеет права: хвост остаётся непомеченным
# и уезжает следующим тиком — сток, а не потеря.
MAX_ALERTS_PER_TICK = 12
# Страницы, которые мы просим: Binance/Bybit по 50, OKX отдаёт 20 и просить другое
# число не даёт. Ключей помним вдвое больше самой длинной — иначе засев обрезал бы
# собственное окно и первый же рабочий тик отчитался бы историей.
PAGE_SIZE = 50
MAX_SEEN_KEYS = 2 * PAGE_SIZE
# Гвард «схема уехала»: вселенная правдоподобного размера, но не та. Абсолютный пол
# нужен маленьким вселенным (30 активов у builder-dex), доля — большим.
CHURN_FLOOR = 8
CHURN_FRACTION = 0.35
# Столько неподтверждённых изменений подряд, после которых прибор объявляет себя
# сломанным. Молчать нельзя: хэш-источник, чей текст мигает, не измеряет ничего.
UNSTABLE_AFTER = 4
# Заглушка CDN («403 Forbidden») короче любой программы. Прочитанная как мир, она даёт
# ложный алерт об изменении правил и стирает настоящий дайджест.
PAGE_TEXT_FLOOR = 400
# Тело провалившегося ответа кладётся рядом с состоянием: `last_error` называет ЧЕГО не
# хватило, но не то, что площадка реально прислала, а второй такой же ответ придёт
# только через каденцию. Потолок — чтобы сайдкар не стал способом забить диск.
FAIL_BODY_BYTES = 64 * 1024
FAIL_DIR = "last_fail"

SCHEMA = 1
DEFAULT_STATE = "/home/storage/announce-poller/state/seen.json"

# --- адреса ------------------------------------------------------------------
BINANCE_CMS = ("https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
               "?type=1&catalogId={cid}&pageNo=1&pageSize=%d" % PAGE_SIZE)
BINANCE_ARTICLE = "https://www.binance.com/en/support/announcement/detail/{code}"
BYBIT_ANN = ("https://api.bybit.com/v5/announcements/index"
             "?locale=en-US&type={t}&limit=%d&page=1" % PAGE_SIZE)
KRAKEN_FEED = "https://blog.kraken.com/feed"
KRAKEN_LP = "https://blog.kraken.com/product/promotions/kraken-liquidity-provider-program"
# Структурный вход к Kraken, не зависящий от блога: 1449 пар живьём 2026-09-14, ключ —
# имя пары (`XXBTZUSD`), внутри altname/wsname/base/quote/status.
KRAKEN_PAIRS = "https://api.kraken.com/0/public/AssetPairs"
HL_INFO = "https://api.hyperliquid.xyz/info"
LIGHTER_BOOKS = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"
# Lighter под Robinhood — ОТДЕЛЬНАЯ вселенная на том же коде: 84 книги (57 перпов,
# 27 спотов) против 245 у мейна, и SPY/QQQ/XAU/SPCX/OPENAI живут только здесь.
LIGHTER_RH_BOOKS = "https://api.rh.lighter.xyz/api/v1/orderBooks"
LIGHTER_LPP = "https://docs.lighter.xyz/liquidity-partner-program.md"
# Перпы Polymarket: 83 инструмента живьём 2026-09-14, адрес взят из её собственной
# OpenAPI (сервер + путь), а не угадан.
POLYMARKET_INSTRUMENTS = "https://api.perpetuals.polymarket.com/v1/info/instruments"
# Deribit: 121 фьючерс с расчётом в USDC, из них 21 ещё не включён.
DERIBIT_USDC_FUTURES = ("https://www.deribit.com/api/v2/public/get_instruments"
                        "?currency=USDC&kind=future")
# Kraken Futures: 295 инструментов, и единственный вход к площадке, который видит
# перпы на акции (спотовая вселенная `AssetPairs` их не знает).
KRAKEN_FUTURES_INSTRUMENTS = "https://futures.kraken.com/derivatives/api/v3/instruments"
# OKX. Измерено 2026-09-13: `announcements-latest-announcements` из задания площадка не
# знает вовсе (`51000 Parameter annType error`) — список живых типов отдаёт
# /api/v5/support/announcement-types, а «последние по всем типам» — это тот же запрос
# БЕЗ annType. Страница — двадцать записей, попросить другое число нельзя.
OKX_ANN = "https://www.okx.com/api/v5/support/announcements?annType={t}&page=1"
OKX_LATEST = "https://www.okx.com/api/v5/support/announcements?page=1"
OKX_PAGE_SIZE = 20
# Типы, у которых есть СВОЙ источник. Лента «последних» их вычитает: иначе один
# делистинг приезжал бы оператору дважды — ключи у разных источников разные.
OKX_COVERED_ANN_TYPES = ("announcements-delistings", "announcements-new-listings")
# Лента «последних» — это все десять типов сразу, и большая часть её записей не про
# торговлю (розыгрыши, P2P-кампании, обслуживание кошельков). Замерено на 160 записях
# 2026-09-13: словам отвечает 21, и это ровно решения площадки — тик-сайз перпов, тиры
# позиций, формула фандинга, делистинг из Onchain Earn. Фильтр по заголовку в нижнем
# регистре; `perp` намеренно ловит и `perpetual`, и `X-Perps`.
OKX_KEYWORDS = ("perp", "pre-ipo", "tokenized", "delist", "listing")


class SourceError(RuntimeError):
    """Ответ источника нечитаем или неправдоподобен. Тик этого источника — провал."""


# ==============================================================================
# Данные
# ==============================================================================


@dataclass(frozen=True)
class Item:
    """Одно решение площадки, готовое к печати."""

    source: str
    kind: str                  # «делистинг», «листинг», «вселенная», «программа»
    key: str                   # стабильный id: articleId / guid / coin / market_id
    title: str
    url: str
    venue_ts_ms: int | None    # метка ВРЕМЕНИ ПЛОЩАДКИ, если площадка её даёт
    venue_clock: str           # чьи это часы и какое поле
    note: str = ""


@dataclass(frozen=True)
class Source:
    name: str
    venue: str
    kind: str                  # feed | diff | hash
    label: str
    cadence_s: int
    url: str
    parse: Callable[[bytes, "Source"], Any]
    body: dict | None = None   # POST-тело (Hyperliquid)
    floor: int = 1
    entity: str = "запись"
    # Форма ключей снимка. Меняешь способ ключевания в адаптере — бампай: иначе
    # апгрейд поллера сравнит новые ключи со старыми, гвард массовой замены прочтёт
    # это как «схема уехала», и источник молча умрёт до вмешательства оператора.
    shape: str = "1"
    # Чем ответ представляется. 200 с валидной формой, но чужим содержимым (Binance
    # перевесил каталог, Bybit сменил семантику типа) читался бы как мир — то есть как
    # «делистингов больше нет». Пустая строка — «идентичность не проверяем».
    expect: str = ""
    # Сколько записей площадка кладёт на страницу. Объявлено, а не выведено из URL:
    # у Binance/Bybit размер просим мы сами (PAGE_SIZE), у OKX (20) его задаёт сама
    # площадка. Окно `seen` обязано накрывать две таких страницы — иначе засев
    # обрезает собственное окно и часть той же страницы станет «новой».
    page_size: int = PAGE_SIZE
    # Фильтр ленты по заголовку (нижний регистр). Пустой кортеж — берём всё.
    keywords: tuple[str, ...] = ()


@dataclass(frozen=True)
class Message:
    source: str
    key: str | None            # None — служебное сообщение, не про статью
    text: str
    kind: str = "item"         # item | fail | recover | unstable


@dataclass
class SourceState:
    seeded: bool = False
    shape: str = "1"
    last_checked_ms: int | None = None
    fails: int = 0
    alerted: bool = False
    # Алерт о провале СФОРМИРОВАН, но Telegram его ещё не принял. Живёт в состоянии,
    # потому что переживать обязан и перезапуск процесса: без него шестой провал,
    # совпавший с провалом доставки, ждал следующего окна перевызова — три часа у
    # пятиминутных источников, тридцать шесть у часовых.
    fail_alert_pending: bool = False
    # Подряд идущих провалов ИМЕННО по бот-защите. Отдельно от `fails`, потому что порог
    # у них разный (FAIL_ALERT_AFTER_CHALLENGE), и в состоянии, а не в процессе: тик —
    # это отдельный запуск oneshot-юнита, и счётчик, живущий в памяти, всегда был бы 1.
    challenge_fails: int = 0
    last_error: str = ""
    seen: list[str] = field(default_factory=list)
    snapshot: dict | None = None
    digest: str | None = None
    pending_digest: str | None = None
    pending_count: int = 0
    unstable_alerted: bool = False


@dataclass
class State:
    schema: int = SCHEMA
    sources: dict[str, SourceState] = field(default_factory=dict)
    # Не сохраняется: признак того, что СУЩЕСТВОВАВШЕЕ состояние прочитать не удалось.
    # Чистый старт — не потеря, и молчать о нём правильно; потеря — единственное, о чём
    # прибор обязан сказать сам про себя.
    lost: bool = False
    lost_at_ms: int | None = None

    def source(self, name: str) -> SourceState:
        return self.sources.setdefault(name, SourceState())


@dataclass(frozen=True)
class Plan:
    """Решения тика как ДАННЫЕ. Исполняет их `apply_plan`, и только он."""

    messages: list[Message] = field(default_factory=list)
    # Снимок/дайджест сдвигается только после доставки всех сообщений источника.
    commit_snapshot: dict[str, dict] = field(default_factory=dict)
    commit_digest: dict[str, str] = field(default_factory=dict)
    # Тело ответа, на котором источник провалился: имя источника → первые FAIL_BODY_BYTES.
    sidecars: dict[str, bytes] = field(default_factory=dict)
    # Поля пульса БЕЗ учёта доставки: сколько сообщений Telegram отверг, знает только
    # исполнитель, и он же дописывает `undelivered` перед записью.
    pulse: dict | None = None
    counters: dict[str, int] = field(default_factory=dict)


# ==============================================================================
# Часы
# ==============================================================================


def iso_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ==============================================================================
# Адаптеры: лента статей
# ==============================================================================


def _doc(payload: bytes) -> Any:
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SourceError(f"нечитаемый ответ: {type(exc).__name__}") from exc


def _need(cond: bool, what: str) -> None:
    if not cond:
        raise SourceError(what)


def parse_binance_cms(payload: bytes, source: Source) -> list[Item]:
    """Каталог анонсов Binance. Битая запись — провал целиком, а не пропуск одной:
    ей может оказаться сам делистинг, а каждый пропущенный невосполним."""
    doc = _doc(payload)
    _need(isinstance(doc, dict), "ответ не объект")
    data = doc.get("data")
    _need(isinstance(data, dict), "в ответе нет data")
    catalogs = data.get("catalogs")
    _need(isinstance(catalogs, list) and catalogs, "в ответе нет catalogs")
    articles: list[Any] = []
    for cat in catalogs:
        _need(isinstance(cat, dict), "запись catalogs не объект")
        if source.expect:
            got = cat.get("catalogName")
            _need(got == source.expect, f"каталог {got!r} ≠ ожидаемый {source.expect!r}")
        articles += cat.get("articles") or []
    _need(len(articles) >= source.floor, f"статей {len(articles)} < {source.floor}")

    items = []
    for raw in articles:
        _need(isinstance(raw, dict), "статья не объект")
        art_id, code, title, released = (raw.get("id"), raw.get("code"),
                                         raw.get("title"), raw.get("releaseDate"))
        _need(isinstance(art_id, int) and not isinstance(art_id, bool), "у статьи нет id")
        _need(isinstance(code, str) and code, "у статьи нет code")
        _need(isinstance(title, str) and title, "у статьи нет title")
        _need(isinstance(released, int) and not isinstance(released, bool),
              "у статьи нет releaseDate")
        items.append(Item(
            source=source.name, kind=source.label, key=f"{source.name}:{art_id}",
            title=title, url=BINANCE_ARTICLE.format(code=code),
            venue_ts_ms=released, venue_clock="binance releaseDate",
        ))
    return items


# Живьём 2026-09-10 у `delistings` две формы id в ссылке: `--art<hex>` (38 записей из
# 50) и `-blt<hex>` либо `--blt<hex>` (12 записей, id Contentstack, все до июля 2026).
# Регекс на две чёрточки последние не ловил, и ключом становился URL целиком — вместе
# с сегментом локали, то есть ровно то, от чего ключ и защищает.
_BYBIT_SLUG = re.compile(r"-(-)?((?:art|blt)[0-9a-f]+)/?$")
_BYBIT_LOCALE = re.compile(r"^[a-z]{2}(-[A-Za-z]{2,})?$")


def bybit_key(url: str) -> str:
    """Id статьи из ссылки. Фолбэк — сама ссылка без сегмента локали, query и
    фрагмента: ни смена локали, ни utm-хвост не имеют права выглядеть новым
    делистингом даже там, где id из ссылки не выделяется."""
    m = _BYBIT_SLUG.search(url)
    if m:
        return m.group(2)
    parts = urllib.parse.urlsplit(url)
    segs = [seg for seg in parts.path.split("/") if seg]
    if segs and _BYBIT_LOCALE.match(segs[0]):
        segs = segs[1:]
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, "/" + "/".join(segs), "", ""))


def parse_bybit_announcements(payload: bytes, source: Source) -> list[Item]:
    doc = _doc(payload)
    _need(isinstance(doc, dict), "ответ не объект")
    # 200 с retCode != 0 — конверт валиден, содержимого нет (например 10006).
    _need(doc.get("retCode") == 0, f"retCode={doc.get('retCode')} {str(doc.get('retMsg'))[:80]}")
    result = doc.get("result")
    _need(isinstance(result, dict), "в ответе нет result")
    lst = result.get("list")
    _need(isinstance(lst, list) and len(lst) >= source.floor,
          f"статей {0 if not isinstance(lst, list) else len(lst)} < {source.floor}")

    items = []
    for raw in lst:
        _need(isinstance(raw, dict), "статья не объект")
        title, url = raw.get("title"), raw.get("url")
        _need(isinstance(title, str) and title, "у статьи нет title")
        _need(isinstance(url, str) and url, "у статьи нет url")
        if source.expect:
            got = raw.get("type")
            got = got.get("key") if isinstance(got, dict) else None
            _need(got == source.expect, f"тип записи {got!r} ≠ ожидаемый {source.expect!r}")
        # Ключ — id статьи из ссылки, а не URL целиком: смена локали в ссылке не имеет
        # права выглядеть новым делистингом.
        ident = bybit_key(url)
        ts, clock = raw.get("publishTime"), "bybit publishTime"
        if not isinstance(ts, int) or isinstance(ts, bool):
            ts, clock = raw.get("dateTimestamp"), "bybit dateTimestamp"
        if not isinstance(ts, int) or isinstance(ts, bool):
            ts, clock = None, "bybit (метки нет)"
        items.append(Item(
            source=source.name, kind=source.label, key=f"{source.name}:{ident}",
            title=title, url=url, venue_ts_ms=ts, venue_clock=clock,
            note=str(raw.get("description") or "")[:200],
        ))
    return items


def parse_rss(payload: bytes, source: Source) -> list[Item]:
    """RSS блога Kraken. Площадка отдаёт по этому адресу то фид, то 403 с JS-челленджем
    (разбор и измерения — в `SOURCES`); формат при этом не менялся ни разу, поэтому
    адаптер пережил и снятие источника в 1.2, и его возврат в 1.3.

    НЕ фильтруется по категориям: имя категории делистинга у этой
    площадки мы никогда не наблюдали, а фильтр по списку известных выбросил бы ровно
    то, ради чего источник заведён. Три записи в сутки — терпимый шум."""
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        raise SourceError(f"нечитаемый RSS: {exc}") from exc
    raw_items = root.findall("./channel/item")
    _need(len(raw_items) >= source.floor, f"записей {len(raw_items)} < {source.floor}")

    items = []
    for it in raw_items:
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        guid = (it.findtext("guid") or "").strip() or link
        _need(bool(title and guid), "в записи RSS нет title/guid")
        ts, clock = None, "kraken (метки нет)"
        pub = it.findtext("pubDate")
        if pub:
            try:
                ts, clock = int(parsedate_to_datetime(pub).timestamp() * 1000), "kraken pubDate"
            except (TypeError, ValueError):
                ts, clock = None, "kraken pubDate (нечитаема)"
        cats = ", ".join((c.text or "").strip() for c in it.findall("category") if c.text)
        items.append(Item(
            source=source.name, kind=source.label, key=f"{source.name}:{guid}",
            title=title, url=link, venue_ts_ms=ts, venue_clock=clock, note=cats,
        ))
    return items


def okx_key(url: str) -> str:
    """Ключ статьи OKX — слаг ссылки, а не заголовок и не URL целиком.

    Измерено 2026-09-13 на 60 записях: три пары ПОВТОРЯЮЩИХСЯ заголовков («OKX to
    suspend USDC deposit and withdrawal on SOLANA network…», «OKX to adjust tick size
    for perpetual futures») и ни одного повторяющегося слага — повторный анонс получает
    суффикс `-1` либо дату (`-20260413`). Ключ по заголовку проглотил бы второй анонс
    целиком. URL целиком тоже нельзя: он несёт сегмент локали (`/en-eu/`), который
    зависит от того, из какой страны мы спросили, — урок ключа Bybit.
    """
    path = urllib.parse.urlsplit(url).path.rstrip("/")
    return path.rsplit("/", 1)[-1] or url


def parse_okx_announcements(payload: bytes, source: Source) -> list[Item]:
    """Анонсы OKX. Форма живая: `{"code":"0","data":[{"details":[…],"totalPage":N}]}`,
    у записи пять полей, `pTime` — СТРОКА миллисекунд.

    Порог правдоподобия считается по СЫРОЙ странице, до фильтра по ключевым словам:
    страница из двадцати записей, где не совпало ни одно слово, — это норма (чаще всего
    именно так), а не провал источника.
    """
    doc = _doc(payload)
    _need(isinstance(doc, dict), "ответ не объект")
    # 200 с code != "0" — конверт валиден, содержимого нет (51000 при чужом annType).
    _need(doc.get("code") == "0", f"code={doc.get('code')} {str(doc.get('msg'))[:80]}")
    data = doc.get("data")
    _need(isinstance(data, list) and data, "в ответе нет data")
    page = data[0]
    _need(isinstance(page, dict), "страница не объект")
    details = page.get("details")
    _need(isinstance(details, list) and len(details) >= source.floor,
          f"записей {0 if not isinstance(details, list) else len(details)} < {source.floor}")

    items = []
    for raw in details:
        _need(isinstance(raw, dict), "запись не объект")
        title, url, ann, ptime = (raw.get("title"), raw.get("url"),
                                  raw.get("annType"), raw.get("pTime"))
        _need(isinstance(title, str) and title.strip(), "у записи нет title")
        _need(isinstance(url, str) and url, "у записи нет url")
        _need(isinstance(ann, str) and ann, "у записи нет annType")
        if source.expect:
            _need(ann == source.expect, f"тип записи {ann!r} ≠ ожидаемый {source.expect!r}")
        # Метка площадки не имеет права потеряться тихо: без неё сообщение соврало бы о
        # том, КОГДА площадка приняла решение, а это и есть предмет прибора.
        _need(isinstance(ptime, str) and ptime.isdigit(),
              f"у записи нечитаемая метка pTime {ptime!r}")
        if source.keywords:
            if ann in OKX_COVERED_ANN_TYPES:
                continue
            if not any(k in title.lower() for k in source.keywords):
                continue
        items.append(Item(
            source=source.name, kind=source.label, key=f"{source.name}:{okx_key(url)}",
            title=title.strip(), url=url, venue_ts_ms=int(ptime), venue_clock="okx pTime",
        ))
    return items


# ==============================================================================
# Адаптеры: снимок вселенной
# ==============================================================================


def _bool_str(v: Any) -> str:
    return "true" if v is True else "false"


def parse_hl_meta(payload: bytes, source: Source) -> dict:
    """Вселенная перпов Hyperliquid (мейн или builder-dex). Плоские поля — ровно те,
    изменение которых и есть решение площадки."""
    doc = _doc(payload)
    _need(isinstance(doc, dict), "ответ не объект")
    universe = doc.get("universe")
    _need(isinstance(universe, list) and len(universe) >= source.floor,
          f"вселенная {0 if not isinstance(universe, list) else len(universe)} < {source.floor}")
    snap: dict[str, dict] = {}
    for raw in universe:
        _need(isinstance(raw, dict), "запись вселенной не объект")
        name, lev = raw.get("name"), raw.get("maxLeverage")
        _need(isinstance(name, str) and name, "у актива нет name")
        _need(isinstance(lev, int) and not isinstance(lev, bool), f"у {name} нет maxLeverage")
        snap[name] = {"label": name, "maxLeverage": str(lev),
                      "isDelisted": _bool_str(raw.get("isDelisted"))}
    return snap


def parse_hl_outcomes(payload: bytes, source: Source) -> dict:
    """Аутком-рынки HL — СЕМЬЯМИ, а не поштучно.

    Замерено 2026-09-09 22:05→22:26Z: за 21 минуту 13 исходов появилось и 17 исчезло
    из 160, при том что множество семей (шаблон × базовый перп × деплойер) не
    изменилось ни на одну. Поштучный diff давал бы ~2000 сообщений в сутки —
    механическую прокрутку часовых бинарников, а не решение площадки. Решение
    площадки — это НОВАЯ семья: новый шаблон, новый базовый актив, новый деплойер.
    ⚠️ Стабильность семей измерена на получасе, не на неделе: спортивные семьи могут
    ротироваться. Если начнут — это будет видно в чате, и порог придётся поднимать.
    """
    doc = _doc(payload)
    _need(isinstance(doc, dict), "ответ не объект")
    outs = doc.get("outcomes")
    _need(isinstance(outs, list), "в ответе нет outcomes")
    snap: dict[str, dict] = {}
    for raw in outs:
        _need(isinstance(raw, dict), "запись outcomes не объект")
        _need(isinstance(raw.get("outcome"), int), "у исхода нет outcome")
        name = str(raw.get("name") or "")
        fields = dict(kv.split(":", 1) for kv in str(raw.get("description") or "").split("|")
                      if ":" in kv)
        perp = (fields.get("perp") or "?").strip()
        deployer = str(raw.get("venue"))
        snap[f"{name}|{perp}|{deployer}"] = {
            "label": f"{name} · базовый {perp} · деплойер {deployer}"}
    _need(len(snap) >= source.floor, f"семей исходов {len(snap)} < {source.floor}")
    return snap


def parse_lighter_books(payload: bytes, source: Source) -> dict:
    doc = _doc(payload)
    _need(isinstance(doc, dict), "ответ не объект")
    books = doc.get("order_books")
    _need(isinstance(books, list) and len(books) >= source.floor,
          f"рынков {0 if not isinstance(books, list) else len(books)} < {source.floor}")
    snap: dict[str, dict] = {}
    for raw in books:
        _need(isinstance(raw, dict), "запись order_books не объект")
        mid, sym, status = raw.get("market_id"), raw.get("symbol"), raw.get("status")
        _need(isinstance(mid, int) and not isinstance(mid, bool), "у рынка нет market_id")
        _need(isinstance(sym, str) and sym, "у рынка нет symbol")
        _need(isinstance(status, str) and status, f"у {sym} нет status")
        snap[str(mid)] = {"label": f"{sym} (#{mid})", "status": status}
    return snap


# Поля, без которых пара нечитаема. `wsname` сюда НЕ входит: площадка объявляет его
# необязательным («if available»), и требовать его значило бы однажды убить весь
# источник из-за одной экзотической пары. Живьём 2026-09-14 он есть у всех 1449.
KRAKEN_PAIR_FIELDS = ("altname", "base", "quote", "status")


def parse_kraken_assetpairs(payload: bytes, source: Source) -> dict:
    """Вселенная торговых пар Kraken — единственный оставшийся вход к этой площадке.

    Конверт у неё свой, и 200 в нём не значит ничего: неизвестную пару площадка отдаёт
    кодом 200 с непустым `error` и БЕЗ `result` (измерено 2026-09-14:
    `{"error":["EQuery:Unknown asset pair"]}`). Поэтому читается только пустой `error`
    при объекте `result`; всё остальное — провал ИСТОЧНИКА, а не пустой мир: мир из
    такого ответа означал бы «делистнули все 1449 пар».

    СТАТУС ЛЕЖИТ В ЗНАЧЕНИИ, А НЕ В КЛЮЧЕ, и это главное решение адаптера. У Kraken
    делистинг начинается не с исчезновения пары, а со смены её статуса
    (`online` → `cancel_only` / `post_only` / `reduce_only`; живьём 2026-09-14 — 79 пар
    в cancel_only и 17 в post_only против 1353 online). Такой переход уже ловит
    существующий `diff_snapshots`: он сравнивает значения ПОЛЕ ЗА ПОЛЕМ у общих ключей —
    тем же механизмом видны `isDelisted` у Hyperliquid и `status` у Lighter. Значит ни
    расширять `diff_snapshots`, ни вплавлять статус в ключ не нужно: вплавленный статус
    превратил бы один переход в пару «исчезла/появилась» — два сообщения вместо одного,
    и ни одно из них не называет сам переход.

    Битая запись — провал целиком, а не пропуск одной: пропущенной может оказаться
    ровно та пара, ради которой источник заведён.
    """
    doc = _doc(payload)
    _need(isinstance(doc, dict), "ответ не объект")
    err = doc.get("error")
    _need(isinstance(err, list), "в ответе нет списка error")
    _need(not err, f"площадка вернула error={str(err)[:120]}")
    result = doc.get("result")
    _need(isinstance(result, dict), "в ответе нет result")
    _need(len(result) >= source.floor, f"пар {len(result)} < {source.floor}")

    snap: dict[str, dict] = {}
    for key, raw in result.items():
        _need(isinstance(raw, dict), f"запись пары {key} не объект")
        fields = {}
        for f_name in KRAKEN_PAIR_FIELDS:
            value = raw.get(f_name)
            _need(isinstance(value, str) and value, f"у пары {key} нет {f_name}")
            fields[f_name] = value
        wsname = raw.get("wsname") if isinstance(raw.get("wsname"), str) else ""
        snap[str(key)] = {"label": f"{wsname or fields['altname']} ({key})",
                          "wsname": wsname, **fields}
    return snap


def _opt_str(value: Any) -> str:
    """Необязательное строковое поле → строка; пустая, если площадка его не прислала.

    Требовать поле, объявленное необязательным, — это однажды убить весь источник из-за
    одной экзотической записи (урок `wsname` у пар Kraken). Обязательные поля читаются
    строго, через `_need`, и разница между этими двумя способами — решение адаптера.
    """
    return value if isinstance(value, str) else ""


def _amount(value: Any, what: str) -> float:
    """Число, приехавшее СТРОКОЙ (так их шлёт Hyperliquid) → float.

    Нечитаемое значение — провал источника, а не ноль. Ноль здесь означает «тишина»,
    то есть молчание сигналки ровно тогда, когда она и нужна.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise SourceError(what)
    try:
        return float(value)
    except ValueError as exc:
        raise SourceError(what) from exc


def parse_hl_perpdexs(payload: bytes, source: Source) -> dict:
    """Реестр builder-dex Hyperliquid — единственный источник, который видит
    ПОЯВЛЕНИЕ площадки.

    Вселенная конкретного dex (`parse_hl_meta`) ловит изменения внутри известного dex
    и по построению слепа к новому: у нового dex нет источника, пока его не завели
    руками. А новый builder-dex — это новая площадка с собственным листингом (io
    торгует пре-IPO, xyz — акции и сырьё), то есть решение, которое стоит денег.

    Конверт — сам список, и первый его элемент null: так площадка говорит «нулевой
    индекс — не builder-dex, это мейн» (живьём 2026-09-14: одиннадцать элементов,
    десять именованных). Прочитанный как запись, null стал бы либо провалом
    источника, либо dex-ом с пустым именем.

    `name` читается строго — имя и есть ключ; `fullName` и `deployer` терпимо. Поле,
    которое площадка может не прислать, не имеет права убить источник, чья работа как
    раз в том, чтобы объявить незнакомую запись.
    """
    doc = _doc(payload)
    _need(isinstance(doc, list), "ответ не список")
    rows = [raw for raw in doc if raw is not None]
    _need(len(rows) >= source.floor, f"dex-ов {len(rows)} < {source.floor}")

    snap: dict[str, dict] = {}
    for raw in rows:
        _need(isinstance(raw, dict), "запись реестра не объект")
        name = raw.get("name")
        _need(isinstance(name, str) and name, "у dex нет name")
        full = _opt_str(raw.get("fullName"))
        snap[name] = {"label": f"{full or name} (dex {name})", "full_name": full,
                      # Деплойер вправе снять активы с торгов и переставить оракул:
                      # смена адреса под тем же именем — решение, а не косметика.
                      "deployer": _opt_str(raw.get("deployer"))}
    return snap


def parse_hl_dex_activity(payload: bytes, source: Source) -> dict:
    """Сигналка на СПЯЩИЙ builder-dex: торгует ли он хоть чем-нибудь.

    Шесть dex из десяти живьём зарегистрированы и не торгуют ничем — у всех их активов
    и `dayNtlVlm`, и `openInterest` нули (замерено 2026-09-14). Их вселенная не
    двигается месяцами, поэтому diff по вселенной молчит и тогда, когда площадка ожила:
    первая сделка ничего не добавляет и ничего не удаляет. А именно она и есть решение —
    момент, с которого dex перестаёт быть строкой в реестре.

    Ответ `metaAndAssetCtxs` — ПАРА: вселенная и контексты, параллельные ей по ИНДЕКСУ
    (имени актива во второй половине нет вовсе). Поэтому читаются обе половины и
    сверяется длина: разъехавшись, они припишут активность чужому активу, и первая же
    сработка назовёт не тот символ.

    Значение снимка — ОДИН бит на актив, и поле названо словами намеренно: сообщение
    собирает `diff_snapshots` по шаблону «{label} — {поле}: {было} → {стало}», то есть
    имя поля и есть половина фразы, которую прочитает оператор («flx:GOLD на dex flx —
    первая активность: нет → есть»). Бит здесь ровно один потому, что label диффается
    наравне с остальными полями: label, зависящий от того же бита, дал бы ДВА сообщения
    на одно событие.

    Монотонности здесь нет и быть не может: `dayNtlVlm` — скользящие сутки, поэтому
    проснувшийся и снова затихший dex даст обратный переход («есть → нет»). Это шум
    ценой в одно сообщение, а не ложное молчание; латч потребовал бы состояния, которого
    у парсера нет.

    ⚠️ Потолок: если разом оживёт больше CHURN_FLOOR активов, `diff_snapshots` прочитает
    это как уехавшую схему и источник упадёт — громко (красный алерт через полчаса,
    тело в `last_fail`), но без называния активов. Для спящего dex это выбранная
    сторона: пробуждение в один тик всей вселенной неотличимо от подмены ответа.
    """
    dex = (source.body or {}).get("dex") or "?"
    doc = _doc(payload)
    _need(isinstance(doc, list) and len(doc) == 2, "ответ не пара [вселенная, контексты]")
    meta, ctxs = doc
    _need(isinstance(meta, dict), "первая половина ответа не объект")
    universe = meta.get("universe")
    _need(isinstance(universe, list), "в первой половине нет universe")
    _need(isinstance(ctxs, list), "вторая половина ответа не список")
    _need(len(universe) == len(ctxs),
          f"половины ответа разной длины: {len(universe)} ≠ {len(ctxs)}")
    _need(len(universe) >= source.floor, f"активов {len(universe)} < {source.floor}")

    snap: dict[str, dict] = {}
    for raw, ctx in zip(universe, ctxs):
        _need(isinstance(raw, dict), "запись вселенной не объект")
        _need(isinstance(ctx, dict), "запись контекста не объект")
        name = raw.get("name")
        _need(isinstance(name, str) and name, "у актива нет name")
        # «Или», а не «и»: оборот суток — скользящее окно, открытый интерес —
        # мгновенный срез. Позиция, открытая и оставленная, — тоже первая активность.
        vlm = _amount(ctx.get("dayNtlVlm"), f"у {name} нечитаемый dayNtlVlm")
        oi = _amount(ctx.get("openInterest"), f"у {name} нечитаемый openInterest")
        live = vlm > 0 or oi > 0
        snap[name] = {"label": f"{name} на dex {dex}",
                      "первая активность": "есть" if live else "нет"}
    return snap


def parse_lighter_rh_books(payload: bytes, source: Source) -> dict:
    """Вселенная рынков Lighter под Robinhood — ОТДЕЛЬНАЯ площадка на том же коде.

    Адрес другой и вселенная другая: 84 книги (57 перпов, 27 спотов) против 245 у
    мейна, замерено 2026-09-15, и SPY/QQQ/XAU/SPCX/OPENAI живут только здесь.

    Почему не тот же адаптер, что у мейна: снимок здесь несёт ещё `created_at` —
    единственную метку времени, которую эта площадка даёт вообще. Добавить поле в
    снимок РАБОТАЮЩЕГО источника значит сменить форму его ключей, то есть засеять
    `lighter_markets` заново и выбросить его окно. Дубль в двадцать строк дешевле.

    Конверт: код площадки лежит ВНУТРИ тела рядом с HTTP-кодом (200 живьём). 200
    снаружи и 500 внутри, прочитанные как мир, — это «делистнули все 84 рынка».
    """
    doc = _doc(payload)
    _need(isinstance(doc, dict), "ответ не объект")
    _need(doc.get("code") == 200, f"площадка вернула code={doc.get('code')!r}")
    books = doc.get("order_books")
    _need(isinstance(books, list) and len(books) >= source.floor,
          f"рынков {0 if not isinstance(books, list) else len(books)} < {source.floor}")

    snap: dict[str, dict] = {}
    for raw in books:
        _need(isinstance(raw, dict), "запись order_books не объект")
        mid, sym, status = raw.get("market_id"), raw.get("symbol"), raw.get("status")
        _need(isinstance(mid, int) and not isinstance(mid, bool), "у рынка нет market_id")
        _need(isinstance(sym, str) and sym, "у рынка нет symbol")
        _need(isinstance(status, str) and status, f"у {sym} нет status")
        created = raw.get("created_at")
        _need(isinstance(created, (str, int)) and not isinstance(created, bool),
              f"у {sym} нет created_at")
        snap[str(mid)] = {"label": f"{sym} (#{mid})", "status": status,
                          "created_at": str(created)}
    return snap


def parse_polymarket_instruments(payload: bytes, source: Source) -> dict:
    """Инструменты перпов Polymarket.

    Ответ — ГОЛЫЙ список без конверта, а ошибка у этой площадки приезжает объектом
    (`Error400Response`/`Error429Response` её же OpenAPI). Поэтому проверка формы —
    это и есть весь конверт, который здесь бывает: объект, прочитанный как мир,
    означал бы «сняли все 83 инструмента».

    Статуса у этой площадки НЕТ: её собственная схема (снята 2026-09-14) объявляет
    девятнадцать обязательных полей, и ни одно из них не статус. Роль «когда включат»
    играет `ui_live_time` — назначенный час выхода в торговлю. Живьём он приехал
    четырьмя волнами (39 инструментов на 2026-08-01, 13, 15 и 16 на следующие три
    даты), то есть назначается осознанно, и его перестановка — решение площадки.
    """
    doc = _doc(payload)
    _need(isinstance(doc, list), "ответ не список")
    _need(len(doc) >= source.floor, f"инструментов {len(doc)} < {source.floor}")

    snap: dict[str, dict] = {}
    for raw in doc:
        _need(isinstance(raw, dict), "запись инструмента не объект")
        iid, sym, live = raw.get("instrument_id"), raw.get("symbol"), raw.get("ui_live_time")
        _need(isinstance(iid, int) and not isinstance(iid, bool),
              "у инструмента нет instrument_id")
        _need(isinstance(sym, str) and sym, f"у инструмента #{iid} нет symbol")
        _need(isinstance(live, int) and not isinstance(live, bool),
              f"у {sym} нет ui_live_time")
        snap[str(iid)] = {"label": f"{sym} (#{iid})", "symbol": sym,
                          "ui_live_time": str(live)}
    return snap


def parse_deribit_instruments(payload: bytes, source: Source) -> dict:
    """Фьючерсы Deribit с расчётом в USDC.

    Главное, ради чего источник заведён: у этой площадки контракт заводится ЗАРАНЕЕ и
    включается отдельным решением — 21 из 121 инструмента живьём 2026-09-14 стоит с
    `is_active: false`. То есть очередь листингов видна ДО первой сделки, а включение
    приезжает изменением поля (`is_active: false → true`).

    Конверт JSON-RPC: ошибка живёт рядом с результатом, а не вместо него, поэтому
    сначала проверяется `error`, и только потом читается `result`.
    """
    doc = _doc(payload)
    _need(isinstance(doc, dict), "ответ не объект")
    _need(not doc.get("error"), f"площадка вернула error={str(doc.get('error'))[:120]}")
    result = doc.get("result")
    _need(isinstance(result, list) and len(result) >= source.floor,
          f"инструментов {0 if not isinstance(result, list) else len(result)} < {source.floor}")

    snap: dict[str, dict] = {}
    for raw in result:
        _need(isinstance(raw, dict), "запись инструмента не объект")
        name, active = raw.get("instrument_name"), raw.get("is_active")
        period, created = raw.get("settlement_period"), raw.get("creation_timestamp")
        _need(isinstance(name, str) and name, "у инструмента нет instrument_name")
        _need(isinstance(active, bool), f"у {name} нет is_active")
        _need(isinstance(period, str) and period, f"у {name} нет settlement_period")
        _need(isinstance(created, int) and not isinstance(created, bool),
              f"у {name} нет creation_timestamp")
        snap[name] = {"label": f"{name} ({period})", "is_active": _bool_str(active),
                      "settlement_period": period, "creation_timestamp": str(created)}
    return snap


def parse_kraken_futures_instruments(payload: bytes, source: Source) -> dict:
    """Вселенная деривативов Kraken Futures — вход к площадке, не зависящий ни от
    блога, ни от спотовых пар.

    Именно здесь видны перпы на акции (`PF_NBISUSD` открыт 2026-09-03): спотовая
    вселенная `AssetPairs` их не знает вовсе, а блог отвечает челленджем чаще, чем
    содержимым. Живьём 2026-09-14 — 295 инструментов, все торгуемы.

    Делистинг здесь начинается со снятия торгуемости, а не с исчезновения символа, —
    ровно как статус у спотовых пар, и ловится тем же сравнением поле за полем.

    Конверт: отказ приезжает кодом 200 и полем `result: "error"`. Правдоподобный
    размер при таком конверте — самая опасная форма ответа: прочитанный как мир, он
    означает «сняли с торгов все 295 инструментов».
    """
    doc = _doc(payload)
    _need(isinstance(doc, dict), "ответ не объект")
    _need(doc.get("result") == "success",
          f"result={doc.get('result')!r} {str(doc.get('error'))[:80]}")
    insts = doc.get("instruments")
    _need(isinstance(insts, list) and len(insts) >= source.floor,
          f"инструментов {0 if not isinstance(insts, list) else len(insts)} < {source.floor}")

    snap: dict[str, dict] = {}
    for raw in insts:
        _need(isinstance(raw, dict), "запись инструмента не объект")
        sym, typ = raw.get("symbol"), raw.get("type")
        tradeable, opening = raw.get("tradeable"), raw.get("openingDate")
        _need(isinstance(sym, str) and sym, "у инструмента нет symbol")
        _need(isinstance(typ, str) and typ, f"у {sym} нет type")
        _need(isinstance(tradeable, bool), f"у {sym} нет tradeable")
        _need(isinstance(opening, str) and opening, f"у {sym} нет openingDate")
        snap[sym] = {"label": f"{sym} ({typ})", "tradeable": _bool_str(tradeable),
                     "type": typ, "openingDate": opening}
    return snap


def diff_snapshots(prev: dict, cur: dict, source: Source) -> list[Item]:
    """Снимок → снимок. Ключ каждого изменения несёт и старое, и новое значение,
    поэтому следующее изменение того же поля — это новый ключ, а не повтор.

    ИСЧЕЗНОВЕНИЕ КЛЮЧА НЕ ОТПРАВЛЯЕТСЯ — решение оператора 2026-10-06, после того как
    недельный простой bigbox выдал 178 изменений разом. Начало делистинга у этих
    площадок — смена значения (`isDelisted` у HL и его dex-ов, `status` у Lighter и
    спота Kraken, `tradeable` у Kraken Futures), и она по-прежнему идёт сообщением;
    исчезновение — уже конец того же события. Цена: инструмент, снятый БЕЗ смены
    значения перед этим, и любая ушедшая семья аутком-рынков HL теперь уходят молча.

    Но исчезновения СЧИТАЮТСЯ в гварде массовой замены ниже. Без них ответ, усечённый
    выше пола, тихо продвинул бы снимок, а вернувшаяся полная вселенная прочиталась бы
    волной ложных «появился»."""
    items: list[Item] = []

    def add(key: str, kind: str, title: str) -> None:
        # Ссылки нет намеренно: у diff-источника это POST-эндпоинт, который оператор
        # всё равно не откроет. Заголовок сообщения называет площадку и dex.
        items.append(Item(source=source.name, kind=kind, key=f"{source.name}:{key}",
                          title=title, url="", venue_ts_ms=None, venue_clock="", note=""))

    for k in sorted(set(cur) - set(prev)):
        add(f"{k}:new", "новый " + source.entity, f"{cur[k].get('label', k)} — появился")
    gone = set(prev) - set(cur)
    for k in sorted(set(prev) & set(cur)):
        for f_name in sorted(set(prev[k]) | set(cur[k])):
            was, now = prev[k].get(f_name), cur[k].get(f_name)
            if was != now:
                add(f"{k}:{f_name}:{was}->{now}", "изменение",
                    f"{cur[k].get('label', k)} — {f_name}: {was} → {now}")

    # Вселенная правдоподобного размера, но НЕ ТА (переименованное поле, отданный не
    # тот dex). Массовая замена читается как провал источника: fail closed.
    churn = len(items) + len(gone)
    if prev and churn > max(CHURN_FLOOR, CHURN_FRACTION * len(prev)):
        raise SourceError(f"изменений {churn} на {len(prev)} записей — схема уехала?")
    return items


# ==============================================================================
# Адаптер: дайджест страницы
# ==============================================================================

# Всё, что ниже этих маркеров, — не программа, а обвязка страницы. `related-posts`
# измерен как мигающий: два ответа Kraken за 5 минут отличались только им.
CUT_MARKERS = ("related-posts-container", "<footer")
_TAG_BLOCK = re.compile(r"(?is)<(script|style|svg|noscript)\b.*?</\1>")
_TAG = re.compile(r"(?s)<[^>]+>")


def text_digest(payload: bytes, source: Source | None = None) -> str:
    """SHA-256 видимого текста страницы. По сырым байтам считать нельзя: страница
    Kraken меняет их сама по себе несколько раз в час."""
    text = payload.decode("utf-8", "replace")
    for marker in CUT_MARKERS:
        cut = text.find(marker)
        if cut > 0:
            text = text[:cut]
    text = _TAG_BLOCK.sub(" ", text)
    text = _TAG.sub(" ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    floor = source.floor if source is not None else PAGE_TEXT_FLOOR
    _need(len(text) >= floor, f"текст страницы {len(text)} симв. < {floor}")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ==============================================================================
# Реестр источников
# ==============================================================================

# Builder-dex Hyperliquid: реестр `perpDexs` живьём 2026-09-14 знает десять.
# Эти четыре ТОРГУЮТ (xyz 120 активов, para 35, mkts 23, io 10) — у каждого своя
# вселенная, и её движение и есть решение площадки.
HL_LIVE_DEXES = ("xyz", "io", "mkts", "para")
# А эти шесть зарегистрированы и не торгуют НИЧЕМ: и `dayNtlVlm`, и `openInterest`
# нули у всех их активов до единого. За вселенной каждого следим так же, а рядом
# ставим сигналку на первую сделку (`parse_hl_dex_activity`): вселенная спящего dex
# не двигается месяцами, и diff по ней промолчит ровно в тот момент, когда площадка
# оживёт.
HL_DORMANT_DEXES = ("flx", "vntl", "hyna", "km", "cash", "abcd")


def hl_dex_universe(dex: str, floor: int) -> Source:
    """Вселенная одного builder-dex.

    Собирается функцией, а не переписывается руками одиннадцать раз: имя dex едет в
    ТРИ места (имя источника, заголовок сообщения и тело запроса), и разъехавшаяся
    копипаста дала бы источник, который спрашивает один dex, а подписан именем
    другого, — молча и навсегда.
    """
    return Source(f"hl_dex_{dex}", f"Hyperliquid {dex}", "diff", "вселенная",
                  CADENCE_API_S, HL_INFO, parse_hl_meta,
                  body={"type": "meta", "dex": dex}, floor=floor, entity="актив")


def hl_dex_tripwire(dex: str) -> Source:
    """Сигналка на первую активность спящего dex.

    Пол 1, а не «как у торгующих»: у abcd вселенная — РОВНО ОДИН актив, и любой порог
    выше единицы убил бы этот источник навсегда и молча. Пустой ответ ловится тем же
    полом, подмену ответа — гвардом массовой замены в `diff_snapshots`.
    """
    return Source(f"hl_dex_{dex}_activity", f"Hyperliquid {dex} (активность)", "diff",
                  "активность", CADENCE_API_S, HL_INFO, parse_hl_dex_activity,
                  body={"type": "metaAndAssetCtxs", "dex": dex}, floor=1, entity="актив")


SOURCES: tuple[Source, ...] = (
    # `expect` — имя каталога, которое площадка вернула живьём 2026-09-10. Косметическое
    # переименование каталога уронит источник в провал, и это выбранная сторона:
    # громкий отказ через полчаса против молчания о делистингах навсегда.
    Source("binance_delisting", "Binance", "feed", "делистинг", CADENCE_API_S,
           BINANCE_CMS.format(cid=161), parse_binance_cms, floor=1, expect="Delisting"),
    Source("binance_listing", "Binance", "feed", "листинг", CADENCE_API_S,
           BINANCE_CMS.format(cid=48), parse_binance_cms, floor=1,
           expect="New Cryptocurrency Listing"),
    Source("bybit_delistings", "Bybit", "feed", "делистинг", CADENCE_API_S,
           BYBIT_ANN.format(t="delistings"), parse_bybit_announcements, floor=1,
           expect="delistings"),
    Source("bybit_new_crypto", "Bybit", "feed", "листинг", CADENCE_API_S,
           BYBIT_ANN.format(t="new_crypto"), parse_bybit_announcements, floor=1,
           expect="new_crypto"),
    # OKX. Измерено 2026-09-13 с кипрского адреса: площадка отдаёт EEA-сущность
    # (ссылки `/en-eu/`), и переключить её ни заголовком, ни параметром не удалось —
    # то есть это анонсы ЕВРОПЕЙСКОГО юрлица, а не глобальные. Тип записи площадка
    # кладёт в сам ответ (`annType`) — он и служит проверкой идентичности.
    Source("okx_delistings", "OKX", "feed", "делистинг", CADENCE_API_S,
           OKX_ANN.format(t="announcements-delistings"), parse_okx_announcements, floor=1,
           expect="announcements-delistings", page_size=OKX_PAGE_SIZE),
    Source("okx_new_listings", "OKX", "feed", "листинг", CADENCE_API_S,
           OKX_ANN.format(t="announcements-new-listings"), parse_okx_announcements, floor=1,
           expect="announcements-new-listings", page_size=OKX_PAGE_SIZE),
    # Лента всех типов сразу, фильтр по ключевым словам. `expect` здесь пуст намеренно:
    # смешанный тип — это и есть её содержимое, а конверт стережёт `code`.
    Source("okx_latest", "OKX", "feed", "анонс", CADENCE_API_S,
           OKX_LATEST, parse_okx_announcements, floor=1, page_size=OKX_PAGE_SIZE,
           keywords=OKX_KEYWORDS),
    # Мейн-вселенная HL: 234 актива живьём. Пол 50 отсекает усечённый ответ, который
    # как мир означал бы «делистнули почти всё».
    Source("hl_universe", "Hyperliquid", "diff", "вселенная", CADENCE_API_S,
           HL_INFO, parse_hl_meta, body={"type": "meta"}, floor=50, entity="актив"),
    # Реестр builder-dex. Вселенная dex видит движение ВНУТРИ известного dex и слепа
    # к появлению нового: у нового источника нет, пока его не заведут руками. Пол 8
    # при десяти живых — усечённый реестр, ставший снимком, дал бы «появились» на
    # весь список при следующей же проверке.
    Source("hl_perpdexs", "Hyperliquid", "diff", "реестр", CADENCE_API_S,
           HL_INFO, parse_hl_perpdexs, body={"type": "perpDexs"}, floor=8, entity="dex"),
    # Каждый builder-dex — своё решение площадки и свой запрос: один источник на
    # десять dex-ов означал бы, что провал одного глушит остальные девять.
    # Пол у builder-dex НИЗКИЙ намеренно: io живьём — десять активов, и порог «как у
    # мейна» превратил бы делистинг половины dex-а в провал источника, то есть в
    # молчание. Пустой ответ ловит пол, массовую замену — гвард в diff_snapshots.
    *(hl_dex_universe(dex, 3) for dex in HL_LIVE_DEXES),
    # У спящих пол ещё ниже (1): вселенная abcd — один актив.
    *(hl_dex_universe(dex, 1) for dex in HL_DORMANT_DEXES),
    *(hl_dex_tripwire(dex) for dex in HL_DORMANT_DEXES),
    Source("hl_outcomes", "Hyperliquid outcomes", "diff", "вселенная", CADENCE_API_S,
           HL_INFO, parse_hl_outcomes, body={"type": "outcomeMeta"}, floor=5,
           entity="семья исходов", shape="family/1"),
    Source("lighter_markets", "Lighter", "diff", "вселенная", CADENCE_API_S,
           LIGHTER_BOOKS, parse_lighter_books, floor=20, entity="рынок"),
    Source("lighter_lpp_doc", "Lighter", "hash", "программа", CADENCE_HASH_S,
           LIGHTER_LPP, text_digest, floor=PAGE_TEXT_FLOOR),
    # --- Kraken: три входа, и ни один не заменяет другой ---------------------
    # Блог (`kraken_blog`, `kraken_lp_page`) был снят в 1.2 как заблокированный
    # НАВСЕГДА и возвращён в 1.3, потому что это оказалось неверно. Челлендж
    # Cloudflare («Checking your browser», «Javascript required») ИНТЕРМИТТЕНТНЫЙ —
    # измерено 2026-09-14: прод-поллер 1.1 прочитал фид в 19:10:10Z (источников=14,
    # провалов=0), а зонд из 20 запросов раз в минуту в 19:36:45–19:55:49Z получил
    # 20 из 20 челленджей, одним и тем же телом в 6460 байт; токийский хост в 19:36:40Z
    # и 20:04:06Z ответил тем же. Значит открытые окна реже, чем одно на 20 минут, и
    # этим зондом не характеризуются — но они есть, и блог через них проходит.
    # ЗАЧЕМ ДЕРЖАТЬ ИСТОЧНИК, КОТОРЫЙ ЧАЩЕ МОЛЧИТ: когда он проходит, он анонсирует
    # листинги за ДНИ до включения. Такого упреждения по этой площадке не даёт больше
    # ничто: ни вселенная пар, ни support.kraken.com (Zendesk API и atom — 404), ни
    # status.kraken.com (только задержки вывода). Цена ложной тревоги при этом снята
    # порогом: провал по бот-защите не будит оператора раньше двух часов
    # (FAIL_ALERT_AFTER_CHALLENGE), обычный провал — по-прежнему через шесть.
    Source("kraken_blog", "Kraken", "feed", "анонс", CADENCE_FEED_S,
           KRAKEN_FEED, parse_rss, floor=1, page_size=10),
    Source("kraken_lp_page", "Kraken", "hash", "программа", CADENCE_HASH_S,
           KRAKEN_LP, text_digest, floor=PAGE_TEXT_FLOOR),
    # Структурный фолбэк, от блога не зависящий вовсе: вселенная торговых пар видит
    # включение и смену статуса (`online` → `cancel_only`/`post_only`/`reduce_only`),
    # то есть ФАКТ вместо упреждения — решение, которое уже вступило в силу.
    Source("kraken_pairs", "Kraken", "diff", "вселенная", CADENCE_API_S,
           KRAKEN_PAIRS, parse_kraken_assetpairs, floor=500, entity="пара"),
    # Kraken Futures — второй, независимый от блога вход к той же площадке, и
    # единственный, который видит перпы на акции. Пол 100 при 295 живых.
    Source("kraken_futures_instruments", "Kraken Futures", "diff", "вселенная",
           CADENCE_API_S, KRAKEN_FUTURES_INSTRUMENTS, parse_kraken_futures_instruments,
           floor=100, entity="инструмент"),
    # --- площадки, заведённые в 1.4 по разведке 2026-09-14 --------------------
    # Lighter под Robinhood: отдельная вселенная (84 книги против 245 у мейна).
    Source("lighter_rh_markets", "Lighter Robinhood", "diff", "вселенная",
           CADENCE_API_S, LIGHTER_RH_BOOKS, parse_lighter_rh_books,
           floor=20, entity="рынок"),
    # Polymarket Perps: 83 инструмента, час включения назначается заранее.
    Source("polymarket_perps_instruments", "Polymarket Perps", "diff", "вселенная",
           CADENCE_API_S, POLYMARKET_INSTRUMENTS, parse_polymarket_instruments,
           floor=40, entity="инструмент"),
    # Deribit USDC: очередь ещё не включённых контрактов видна на снимке.
    Source("deribit_usdc_instruments", "Deribit USDC", "diff", "вселенная",
           CADENCE_API_S, DERIBIT_USDC_FUTURES, parse_deribit_instruments,
           floor=40, entity="инструмент"),
)

_BY_NAME = {s.name: s for s in SOURCES}


def source_by_name(name: str) -> Source:
    return _BY_NAME[name]


def due_sources(state: State, now_ms: int, sources: tuple[Source, ...] = SOURCES) -> list[Source]:
    out = []
    for src in sources:
        last = state.sources.get(src.name, SourceState()).last_checked_ms
        if last is None or now_ms - last >= (src.cadence_s - DUE_SLACK_S) * 1000:
            out.append(src)
    return out


# ==============================================================================
# Состояние
# ==============================================================================


def load_state(text: str | None) -> State:
    """Битое или чужой схемы состояние читается как «ничего не знаем».

    Плата известна и выбрана осознанно: пропустим анонсы, пришедшие за время простоя.
    Альтернатива — прочитать мир как новый и высыпать в чат 2700 сообщений; после
    такого каналом перестают пользоваться, и следующий настоящий анонс не увидят.
    """
    if not text:
        return State()
    try:
        doc = json.loads(text)
        if not isinstance(doc, dict) or doc.get("schema") != SCHEMA:
            return State(lost=True)
        names = {f.name for f in dataclasses.fields(SourceState)}
        sources = {
            k: SourceState(**{n: v for n, v in raw.items() if n in names})
            for k, raw in (doc.get("sources") or {}).items()
            if isinstance(raw, dict)
        }
        return State(schema=SCHEMA, sources=sources)
    except (json.JSONDecodeError, TypeError, ValueError):
        return State(lost=True)


def dump_state(state: State) -> str:
    return json.dumps(
        {"schema": SCHEMA,
         "sources": {k: dataclasses.asdict(v) for k, v in sorted(state.sources.items())}},
        ensure_ascii=False, indent=1, sort_keys=False,
    )


# ==============================================================================
# Печать сообщений
# ==============================================================================


def format_item(item: Item, source: Source, now_ms: int) -> str:
    lines = [f"\U0001f195 {source.venue} · {item.kind}", item.title]
    if item.note:
        lines.append(item.note)
    if item.venue_ts_ms is not None:
        lines.append(f"площадка ({item.venue_clock}): {iso_ms(item.venue_ts_ms)}")
    else:
        # Подставить локальные часы значило бы соврать о том, когда площадка приняла
        # решение: у diff-источника метки нет вовсе, наблюдение — единственный факт.
        lines.append("метки площадки нет — наблюдено нами")
    if item.url:
        lines.append(item.url)
    lines.append(f"локально {iso_ms(now_ms)}")
    return "\n".join(lines)


# Слова, по которым узнаётся JS-челлендж. Дословно из тела 403 blog.kraken.com
# (2026-09-14): `<title>Checking your browser...</title><noscript><p>Javascript
# required</p></noscript>`. Регистр не важен: сверка идёт по нижнему.
CHALLENGE_MARKERS = ("checking your browser", "javascript required")
# Одна строка и на печать, и на разбор. Два одинаковых литерала в двух местах однажды
# разъедутся, а разъехавшись — тихо: порог для бот-защиты просто перестанет выбираться.
CHALLENGE_REASON = "бот-защита (JS challenge)"


def failure_reason(exc: BaseException) -> str:
    """Исключение источника → причина словами, по которым оператор поймёт, что делать.

    Голый `HTTPError: HTTP Error 403: Forbidden` неотличим от «площадка легла», а
    решения это требует разного: обычный отказ пережидают минутами, челлендж — часами.
    Метку заводили в 1.2 с неверным выводом («бот-защиту не лечит ожидание, источник
    надо менять») и на этом основании блог Kraken сняли. 1.3 измерил обратное: челлендж
    ИНТЕРМИТТЕНТНЫЙ (прод прочитал фид в 19:10:10Z, зонд из 20 запросов подряд получил
    20 челленджей — измерения в `SOURCES`), поэтому источники ОСТАВЛЕНЫ на месте, а их
    провалы по этому классу считаются своим порогом (`FAIL_ALERT_AFTER_CHALLENGE` = 8
    против обычных шести). Метка существует ровно затем, чтобы оператор видел ПРИЧИНУ и
    не принял норму этого источника за поломку — и чтобы порог было по чему выбрать.
    Тело ошибки живёт ТОЛЬКО в самом исключении: сайдкар `last_fail` пишется лишь для
    ответов, дочитанных до байтов, а до 403 дело не доходит.

    Читается утиным способом (`.read()`), а не по классу `HTTPError`: тот же приём
    работает для любого исключения, которое несёт тело, а чужое — молча остаётся голым.
    Сжатое тело распаковывается — см. комментарий ниже: без этого метка не срабатывает
    ровно там, ради чего заведена.
    """
    bare = f"{type(exc).__name__}: {exc}"
    read = getattr(exc, "read", None)
    if not callable(read):
        return bare
    try:
        body = read(FAIL_BODY_BYTES)
    except Exception:  # noqa: BLE001 — диагностика не имеет права уронить тик
        return bare
    if not isinstance(body, (bytes, bytearray)):
        return bare
    raw = bytes(body)
    # Тело ошибки приезжает СЖАТЫМ, если площадка так сказала: urllib распаковывает
    # только успешные ответы (`RealHost.fetch` делает это сам и по тому же заголовку), а
    # до тела HTTPError у него руки не доходят. Замерено 2026-09-14 20:22Z на
    # blog.kraken.com: 3598 байт на проводе против 6877 распакованных — и метка,
    # заведённая в 1.2, искала маркеры в сжатых байтах, то есть не сработала в проде ни
    # разу (в состоянии лежало голое `HTTP Error 403: Forbidden`).
    try:
        encoding = str((exc.headers or {}).get("Content-Encoding") or "").lower()
    except Exception:  # noqa: BLE001 — у чужого исключения заголовков нет вовсе
        encoding = ""
    if "gzip" in encoding:
        try:
            raw = gzip.decompress(raw)
        except (OSError, EOFError):
            # Обрезанное или не то тело: читаем как есть — диагностика не имеет права
            # уронить тик, а маркеров мы в нём просто не найдём.
            pass
    text = raw.decode("utf-8", "replace").lower()
    if not any(marker in text for marker in CHALLENGE_MARKERS):
        return bare
    code = getattr(exc, "code", None)
    return f"{CHALLENGE_REASON}{f', HTTP {code}' if code else ''}"


def is_challenge(reason: str) -> bool:
    """Причина провала — бот-защита? Читается по той же строке, которую печатает
    `failure_reason`, и только по ней: гадать по коду 403 нельзя — обычный отказ
    площадки выглядит так же, а лечится он ожиданием, в отличие от челленджа."""
    return CHALLENGE_REASON in reason


def _fail_text(hostname: str, src: Source, n: int, reason: str) -> str:
    return (f"\U0001f534 {hostname}: announce-poller — источник {src.name} ({src.venue}) "
            f"не читается, {n} провалов подряд. Последняя причина: {reason[:200]}")


def _recover_text(hostname: str, src: Source, n: int, *, challenge: bool = False) -> str:
    # Красная строка сказала «бот-защита» — зелёная обязана сказать, что снялась именно
    # она. Челлендж интермиттентный: он будет сниматься и вставать сам, и отличить это
    # от «источник починили» можно только по тексту.
    tail = f" — {CHALLENGE_REASON} снята" if challenge else ""
    return (f"\U0001f7e2 {hostname}: announce-poller — источник {src.name} ({src.venue}) "
            f"снова читается после {n} провалов подряд{tail}")


def _lost_text(hostname: str, at_ms: int | None, n_sources: int) -> str:
    when = iso_ms(at_ms) if at_ms else "время последней записи неизвестно"
    return (f"\U0001f534 {hostname}: announce-poller — состояние было нечитаемо и "
            f"отброшено ({when}). Мир пересеян молча по {n_sources} источникам: анонсы, "
            f"пришедшие между потерей и пересевом, не увидит никто — страница анонсов "
            f"назад не отматывается")


def _unstable_text(hostname: str, src: Source, n: int) -> str:
    return (f"\U0001f534 {hostname}: announce-poller — {src.name} ({src.url}): дайджест "
            f"страницы нестабилен, {n} неподтверждённых изменений подряд. Прибор ничего "
            f"не измеряет, пока нормализация не поправлена")


# ==============================================================================
# План
# ==============================================================================


def _read(src: Source, payload: Any, ss: SourceState) -> tuple[list[Item], dict | None, str | None]:
    """Байты источника → (изменения, снимок, дайджест). Любая неправдоподобность —
    исключение: провал ИСТОЧНИКА, а не пустой мир."""
    if isinstance(payload, BaseException):
        raise payload
    if src.kind == "feed":
        return src.parse(payload, src), None, None
    if src.kind == "diff":
        cur = src.parse(payload, src)
        if not ss.seeded or ss.shape != src.shape:
            return [], cur, None
        return diff_snapshots(ss.snapshot or {}, cur, src), cur, None
    return [], None, src.parse(payload, src)


def _chrono(item: Item) -> tuple[int, str]:
    return (item.venue_ts_ms if item.venue_ts_ms is not None else 0, item.key)


def plan_tick(
    results: dict[str, Any],
    state: State,
    now_ms: int,
    *,
    hostname: str = "",
    sources: tuple[Source, ...] = SOURCES,
) -> tuple[Plan, State]:
    """Прочитанное + состояние + локальные часы → (план, новое состояние). Чистая."""
    new = copy.deepcopy(state)
    new.schema = SCHEMA
    messages: list[Message] = []
    pending: list[tuple[Source, Item]] = []
    snapshots: dict[str, dict] = {}
    commit_snapshot: dict[str, dict] = {}
    commit_digest: dict[str, str] = {}
    confirmed_digest: dict[str, str] = {}
    sidecars: dict[str, bytes] = {}
    counters = {"read": 0, "failed": 0, "seeded": 0, "changes": 0}

    if state.lost:
        # Сообщение без источника: оно про сам прибор, а не про площадку.
        messages.append(Message("", None, _lost_text(hostname, state.lost_at_ms, len(sources)),
                                "lost"))
        new.lost, new.lost_at_ms = False, None

    for src in sources:
        if src.name not in results:
            continue
        ss = new.source(src.name)
        ss.last_checked_ms = now_ms
        try:
            items, snapshot, digest = _read(src, results[src.name], ss)
        except Exception as exc:  # noqa: BLE001 — любой класс ошибки = провал источника
            counters["failed"] += 1
            ss.fails += 1
            ss.last_error = failure_reason(exc)[:300]
            if isinstance(results[src.name], (bytes, bytearray)):
                # Что площадка ответила НА САМОМ ДЕЛЕ. `last_error` называет только
                # недостающее поле, а разбирать придётся по телу.
                sidecars[src.name] = bytes(results[src.name])[:FAIL_BODY_BYTES]
            n = ss.fails
            # Порог выбирается по КЛАССУ последнего провала. Бот-защита держится
            # часами и снимается сама, поэтому у неё своя, отдельная серия: обычный
            # провал внутри серии челленджей обязан вернуть шкалу к получасовой, иначе
            # настоящая поломка, случившаяся следом за челленджем, получила бы два часа
            # тишины в подарок.
            if is_challenge(ss.last_error):
                ss.challenge_fails += 1
                streak, after = ss.challenge_fails, FAIL_ALERT_AFTER_CHALLENGE
            else:
                ss.challenge_fails = 0
                streak, after = n, FAIL_ALERT_AFTER
            if streak == after or (
                    streak > after and (streak - after) % FAIL_REALERT_EVERY == 0):
                ss.fail_alert_pending = True
            if ss.fail_alert_pending:
                # Пока не доставлено — повторяем каждый тик. Недоставленный алерт,
                # ждущий следующего окна перевызова, — это часы тишины про мёртвый фид.
                ss.alerted = True
                messages.append(Message(src.name, None,
                                        _fail_text(hostname, src, n, ss.last_error), "fail"))
            continue

        counters["read"] += 1
        if ss.alerted:
            messages.append(Message(src.name, None,
                                    _recover_text(hostname, src, ss.fails,
                                                  challenge=ss.challenge_fails > 0),
                                    "recover"))
            ss.alerted = False
        # Недоставленный алерт не имеет права пережить сам провал: иначе первый же
        # следующий сбой отчитается «шесть провалов подряд» на первом же провале.
        ss.fails, ss.last_error, ss.fail_alert_pending = 0, "", False
        ss.challenge_fails = 0

        if not ss.seeded or ss.shape != src.shape:
            # Засев: мир как он есть, ни одного сообщения. 430 делистингов в чат —
            # это не запуск, а инцидент. Смена формы ключей засевает заново по той же
            # причине: сравнивать новые ключи со старыми бессмысленно.
            ss.seeded, ss.shape = True, src.shape
            counters["seeded"] += 1
            if src.kind == "feed":
                # Хронологически, а не в порядке страницы: площадки отдают свежее
                # ПЕРВЫМ, и обрезка по хвосту сохранила бы тогда самое старое.
                ss.seen = [i.key for i in sorted(items, key=_chrono)][-MAX_SEEN_KEYS:]
            elif src.kind == "diff":
                commit_snapshot[src.name] = snapshot or {}
            else:
                commit_digest[src.name] = digest or ""
            continue

        if src.kind == "hash":
            seen_digest = digest or ""
            if seen_digest == ss.digest:
                # Флаг снимается и здесь, иначе один раз объявленная нестабильность
                # глушила бы вторую навсегда: pending_count обнуляется, а alerted нет.
                ss.pending_digest, ss.pending_count = None, 0
                ss.unstable_alerted = False
            elif seen_digest == ss.pending_digest:
                # Подтверждено вторым наблюдением — только теперь это факт. Но факт
                # может быть уже объявленным: у страницы, которую CDN качает между
                # двумя копиями, подтверждается каждый переход, а нового текста в нём
                # нет. Поэтому дайджест сверяется с seen ровно как ключ статьи.
                key = f"{src.name}:digest:{seen_digest[:12]}"
                confirmed_digest[src.name] = seen_digest
                if key not in set(ss.seen):
                    pending.append((src, Item(
                        source=src.name, kind=src.label, key=key,
                        title=f"{src.venue}: содержимое страницы программы изменилось",
                        url=src.url, venue_ts_ms=None, venue_clock="",
                        note="изменение подтверждено двумя проверками подряд",
                    )))
            else:
                ss.pending_digest, ss.pending_count = seen_digest, ss.pending_count + 1
                if ss.pending_count >= UNSTABLE_AFTER and not ss.unstable_alerted:
                    ss.unstable_alerted = True
                    messages.append(Message(src.name, None,
                                            _unstable_text(hostname, src, ss.pending_count),
                                            "unstable"))
            continue

        if src.kind == "diff":
            snapshots[src.name] = snapshot or {}
        seen = set(ss.seen)
        for item in items:
            if item.key not in seen:
                pending.append((src, item))

    # Хронологический сток: сначала самое старое. У изменений вселенной метки площадки
    # нет — они встают по локальному наблюдению, то есть в конец.
    pending.sort(key=lambda si: (si[1].venue_ts_ms if si[1].venue_ts_ms is not None else now_ms,
                                 si[0].name, si[1].key))
    selected = pending[:MAX_ALERTS_PER_TICK]
    deferred = {src.name for src, _ in pending[MAX_ALERTS_PER_TICK:]}
    counters["changes"] = len(pending)
    counters["reported"] = len(selected)
    counters["deferred"] = len(pending) - len(selected)

    for name, snapshot in snapshots.items():
        # Снимок нельзя сдвигать, пока хоть одно его изменение не уехало: сдвинутый
        # снимок делает пропущенное изменение невидимым навсегда.
        if name not in deferred:
            commit_snapshot[name] = snapshot
    for name, digest in confirmed_digest.items():
        # Дайджест — такой же носитель «это уже видели», как снимок: сдвинутый до
        # доставки, он делает изменение правил невидимым навсегда.
        if name not in deferred:
            commit_digest[name] = digest

    messages += [Message(src.name, item.key, format_item(item, src, now_ms), "item")
                 for src, item in selected]

    pulse = None
    if counters["read"]:
        pulse = {"t": now_ms, "iso": iso_ms(now_ms), "ok": counters["read"],
                 "failed": counters["failed"], "seeded": counters["seeded"],
                 "reported": counters["reported"], "deferred": counters["deferred"]}
    return Plan(messages=messages, commit_snapshot=commit_snapshot,
                commit_digest=commit_digest, sidecars=sidecars, pulse=pulse,
                counters=counters), new


# ==============================================================================
# Исполнение
# ==============================================================================


class Host(Protocol):
    state_path: str

    def now_ms(self) -> int: ...
    def fetch(self, source: Source) -> bytes: ...
    def read_file(self, path: str) -> str | None: ...
    def file_mtime_ms(self, path: str) -> int | None: ...
    def write_atomic(self, path: str, text: str) -> None: ...
    def write_bytes(self, path: str, data: bytes) -> None: ...
    def send_telegram(self, text: str) -> bool: ...
    def write_pulse(self, text: str) -> None: ...


def apply_plan(plan: Plan, new_state: State, host: Host, *, hostname: str = "") -> None:
    """Исполнение плана. Исполнитель ЖУРНАЛИРУЕТ СВОИ ЭФФЕКТЫ в сохраняемое состояние:
    ключ становится виденным, а снимок — сдвинутым, только после того, как Telegram
    принял сообщение. Иначе упавшая доставка = навсегда потерянное событие."""
    undelivered: set[str] = set()
    refused = 0
    for msg in plan.messages:
        ok = host.send_telegram(msg.text)
        if not ok:
            refused += 1
        if not msg.source:
            # Сообщение про сам прибор (потерянное состояние), не про источник.
            continue
        ss = new_state.source(msg.source)
        if ok:
            if msg.key:
                ss.seen.append(msg.key)
                del ss.seen[:-MAX_SEEN_KEYS]
            elif msg.kind == "fail":
                ss.fail_alert_pending = False
            continue
        undelivered.add(msg.source)
        # Недоставленное служебное сообщение обязано повториться, а не потеряться.
        if msg.kind == "recover":
            ss.alerted = True
        elif msg.kind == "unstable":
            ss.unstable_alerted = False
        # kind == "fail": перевзведение уже стоит в состоянии и уедет следующим тиком.

    for name, snapshot in plan.commit_snapshot.items():
        if name not in undelivered:
            new_state.source(name).snapshot = snapshot
    for name, digest in plan.commit_digest.items():
        if name not in undelivered:
            ss = new_state.source(name)
            ss.digest = digest
            ss.pending_digest, ss.pending_count = None, 0
            ss.unstable_alerted = False

    fail_dir = Path(host.state_path).parent / FAIL_DIR
    for name, body in plan.sidecars.items():
        host.write_bytes(str(fail_dir / f"{name}.bin"), body)

    host.write_atomic(host.state_path, dump_state(new_state))
    if plan.pulse is not None:
        # Пульс — про живость самого поллера. Тик, где не прочитано НИЧЕГО, пульса не
        # пишет: его тишина и есть второй, независимый от Telegram канал тревоги.
        # `undelivered` — второй его сигнал: свежий файл при недоставке означал бы, что
        # сторож видит живой прибор ровно тогда, когда тот не доставил ни одного слова.
        host.write_pulse(json.dumps({**plan.pulse, "undelivered": refused},
                                    ensure_ascii=False, separators=(",", ":")))


def tick(host: Host, *, hostname: str = "", sources: tuple[Source, ...] = SOURCES) -> str:
    now_ms = host.now_ms()
    state = load_state(host.read_file(host.state_path))
    if state.lost:
        # Начало потерянного окна — это mtime старого файла, и этот же тик его затрёт.
        state.lost_at_ms = host.file_mtime_ms(host.state_path)
    due = due_sources(state, now_ms, sources)
    results: dict[str, Any] = {}
    for src in due:
        # Ровно один запрос на источник за тик — потолок нагрузки на площадку.
        try:
            results[src.name] = host.fetch(src)
        except Exception as exc:  # noqa: BLE001
            results[src.name] = exc
    plan, new_state = plan_tick(results, state, now_ms, hostname=hostname, sources=sources)
    apply_plan(plan, new_state, host, hostname=hostname)
    c = plan.counters
    line = (f"источников={len(due)} прочитано={c['read']} провалов={c['failed']} "
            f"изменений={c['changes']} отправлено={c['reported']} "
            f"отложено={c['deferred']}")
    if c["seeded"]:
        line += f" засев={c['seeded']}"
    return line


class RealHost:
    """Настоящий хост: сеть и диск. Никаких решений — только исполнение."""

    def __init__(self, state_path: str | Path, *, pulse_path: str | Path | None = None,
                 _open: Callable[..., Any] = urllib.request.urlopen,
                 _now_ms: Callable[[], int] | None = None) -> None:
        self.state_path = str(state_path)
        self.pulse_path = str(pulse_path or (Path(self.state_path).parent / "pulse.json"))
        self._open = _open
        self._now_ms = _now_ms or (lambda: int(time.time() * 1000))

    def now_ms(self) -> int:
        return self._now_ms()

    def fetch(self, source: Source) -> bytes:
        data = json.dumps(source.body).encode() if source.body is not None else None
        headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(source.url, data=data, headers=headers)
        with self._open(req, timeout=HTTP_TIMEOUT_S) as resp:
            body = resp.read()
            encoding = (resp.headers.get("Content-Encoding") or "").lower()
        # urllib сам gzip не распаковывает — только по Content-Encoding.
        return gzip.decompress(body) if "gzip" in encoding else body

    def read_file(self, path: str) -> str | None:
        try:
            return Path(path).read_text(encoding="utf-8")
        except OSError:
            return None

    def file_mtime_ms(self, path: str) -> int | None:
        try:
            return int(Path(path).stat().st_mtime * 1000)
        except OSError:
            return None

    def write_bytes(self, path: str, data: bytes) -> None:
        """Сайдкар провала. Без fsync намеренно: это диагностика, а не носитель факта
        «видели» — потерять её при жёстком ребуте дешевле, чем платить fsync за каждый
        сбойный ответ."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.parent / (p.name + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, p)

    def write_atomic(self, path: str, text: str) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.parent / (p.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            # Состояние — ЕДИНСТВЕННЫЙ носитель факта «это мы уже видели»: страница
            # анонсов назад не отматывается. Запись обязана переживать жёсткий ребут.
            os.fsync(fh.fileno())
        os.replace(tmp, p)

    def send_telegram(self, text: str) -> bool:
        token, chat = os.environ.get("TG_BOT_TOKEN"), os.environ.get("TG_CHAT_ID")
        if not token or not chat:
            print("announce_poller: TG не настроен; сообщение только в журнал:\n" + text,
                  flush=True)
            return False
        if re.search(r"\s", token):
            # Урок topic-scan: CRLF из env-файла уезжает в URL и даёт нечитаемую ошибку.
            print("announce_poller: TG_BOT_TOKEN содержит пробельный символ (CRLF в env?)",
                  flush=True)
            return False
        data = urllib.parse.urlencode(
            {"chat_id": chat, "text": text, "disable_web_page_preview": "true"}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=data, headers={"User-Agent": USER_AGENT},
        )
        try:
            with self._open(req, timeout=HTTP_TIMEOUT_S) as resp:
                resp.read()
            return True
        except Exception as exc:  # noqa: BLE001
            # Урок funding_poller: http.client.IncompleteRead не наследует OSError и
            # вылетал наружу, роняя тик целиком. Печатаем ТОЛЬКО класс ошибки: её текст
            # может нести URL, а в URL живёт токен.
            print(f"announce_poller: TG не доставлен ({type(exc).__name__})", flush=True)
            return False

    def write_pulse(self, text: str) -> None:
        self.write_atomic(self.pulse_path, text + "\n")


class DryRunHost:
    """Читает по-настоящему, но наружу не пишет ничего. Для предполётной проверки."""

    def __init__(self, inner: Host) -> None:
        self._inner = inner
        self.state_path = inner.state_path

    def now_ms(self) -> int:
        return self._inner.now_ms()

    def fetch(self, source: Source) -> bytes:
        return self._inner.fetch(source)

    def read_file(self, path: str) -> str | None:
        return self._inner.read_file(path)

    def file_mtime_ms(self, path: str) -> int | None:
        return self._inner.file_mtime_ms(path)

    def write_atomic(self, path: str, text: str) -> None:
        print(f"--- DRY RUN, записал бы {path} ({len(text)} симв.)", flush=True)

    def write_bytes(self, path: str, data: bytes) -> None:
        print(f"--- DRY RUN, записал бы {path} ({len(data)} байт)", flush=True)

    def send_telegram(self, text: str) -> bool:
        print("--- DRY RUN, отправил бы:\n" + text, flush=True)
        return True

    def write_pulse(self, text: str) -> None:
        print(f"--- DRY RUN, пульс: {text}", flush=True)


def credentials_problem(env: Mapping[str, str]) -> str | None:
    """Почему это фатально, а не `return False` на отправке. Учётные данные Telegram —
    конфигурация, а не сбой сети: без них КАЖДОЕ сообщение отвергается, ключи не
    метятся, тик отчитывается нулём и выходит с кодом 0, а пульс остаётся свежим.
    Прибор в этом состоянии слеп и молчит об этом. Ненулевой код до тика поднимает
    OnFailure, у которого учётные данные свои и отдельные."""
    token, chat = env.get("TG_BOT_TOKEN"), env.get("TG_CHAT_ID")
    missing = [name for name in ("TG_BOT_TOKEN", "TG_CHAT_ID") if not env.get(name)]
    if missing:
        return f"нет учётных данных Telegram: {', '.join(missing)}"
    # Урок topic-scan: CRLF из env-файла уезжает в URL и даёт нечитаемую ошибку.
    if re.search(r"\s", token or "") or re.search(r"\s", chat or ""):
        return "TG_BOT_TOKEN/TG_CHAT_ID содержит пробельный символ (CRLF в env-файле?)"
    return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Поллер анонсов «решение биржи»")
    ap.add_argument("--state", default=DEFAULT_STATE, help="файл состояния seen.json")
    ap.add_argument("--pulse", default=None, help="файл пульса (по умолчанию рядом с состоянием)")
    ap.add_argument("--once", action="store_true", help="один тик и выход (для таймера)")
    ap.add_argument("--interval-s", type=int, default=CADENCE_API_S)
    ap.add_argument("--sources", default=None, help="через запятую; по умолчанию все")
    ap.add_argument("--dry-run", action="store_true",
                    help="читать по-настоящему, но ничего не менять и никого не будить")
    args = ap.parse_args(argv)

    chosen = SOURCES
    if args.sources:
        names = [n.strip() for n in args.sources.split(",") if n.strip()]
        unknown = [n for n in names if n not in _BY_NAME]
        if unknown:
            print(f"неизвестные источники: {', '.join(unknown)}", file=sys.stderr)
            return 2
        chosen = tuple(_BY_NAME[n] for n in names)

    if not args.dry_run:
        problem = credentials_problem(os.environ)
        if problem:
            # До тика: сеть не трогаем, состояние не пишем, пульс не освежаем.
            print(f"announce_poller: {problem}; тик не начат", file=sys.stderr)
            return 3

    host: Host = RealHost(args.state, pulse_path=args.pulse)
    if args.dry_run:
        host = DryRunHost(host)
    name = socket.gethostname().split(".")[0]

    while True:
        began = time.time()
        print(f"{iso_ms(host.now_ms())} {tick(host, hostname=name, sources=chosen)}", flush=True)
        if args.once:
            return 0
        time.sleep(max(0.0, args.interval_s - (time.time() - began)))


if __name__ == "__main__":
    sys.exit(main())
