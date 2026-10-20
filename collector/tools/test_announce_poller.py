"""Тесты поллера анонсов «решение биржи». Сеть и диск подменены записывающей подделкой.

Почему пины именно такие. Семейство «решение биржи/календарь» — единственное живое в
карте рынка, и в нём три четверти хода делистинга уходит в ЧАС публикации анонса
(панель 09.09, профиль −12.3 % в час 0 против −3.9 % на следующие сутки). Наградные
окна живут 4–8 недель и затухают с первого дня. Значит цена ошибки несимметрична:
лишнее сообщение стоит секунды внимания, пропущенный анонс — всего события. Отсюда
четыре класса свойств, которые здесь держатся:

1. ДЕТЕКЦИЯ — членство стабильного ключа (articleId / guid / coin / market_id) в
   состоянии, а не «дата свежая». Первый прогон засевает мир молча: 430 делистингов
   в Telegram — это не запуск, а инцидент.
2. FAIL-CLOSED ПО ИСТОЧНИКУ — нераспарсенный, пустой или усечённый ответ читается
   как ПРОВАЛ ИСТОЧНИКА, а не как мир. Для diff-источников цена этой ошибки прямая:
   пустая вселенная, прочитанная как мир, — это «делистнули всё» и алерт на каждую
   монету, а следом ложный снимок, из-за которого настоящий делистинг уже не заметен.
3. ЖУРНАЛ ДОСТАВКИ — ключ помечается виденным только после того, как Telegram принял
   сообщение, а снимок вселенной сдвигается только когда доставлены ВСЕ изменения,
   которые он несёт. Иначе обрыв между отправкой и записью состояния теряет событие
   навсегда, а восстановить его неоткуда: страница анонсов отдаёт только последние 50.
4. ЧАСЫ РАЗДЕЛЕНЫ — метка площадки печатается со своим именем поля, локальная UTC —
   отдельной строкой (урок «одни часы перед сопоставлением»: 28.08 «лаг площадки»
   оказался нашей же задержкой приёма).
"""

from __future__ import annotations

import gzip
import io
import json
import os
import urllib.error
from pathlib import Path

import pytest

import announce_poller as ap
from announce_poller import (
    CHALLENGE_REASON,
    CHURN_FLOOR,
    FAIL_ALERT_AFTER,
    FAIL_ALERT_AFTER_CHALLENGE,
    FAIL_REALERT_EVERY,
    MAX_ALERTS_PER_TICK,
    MAX_SEEN_KEYS,
    SOURCES,
    UNSTABLE_AFTER,
    Plan,
    RealHost,
    SourceError,
    State,
    apply_plan,
    due_sources,
    dump_state,
    load_state,
    plan_tick,
    source_by_name,
    text_digest,
    tick,
)

NOW = 1_788_000_000_000
HOUR_MS = 3_600_000


# =============================================================================
# Фикстуры: записи взяты дословно из живых ответов площадок 2026-09-09/10
# =============================================================================


def cms(*articles: dict, catalog: int = 161, name: str = "Delisting", total: int | None = None) -> bytes:
    return json.dumps({
        "code": "000000", "message": None, "messageDetail": None,
        "data": {"catalogs": [{
            "catalogId": catalog, "parentCatalogId": None, "catalogName": name,
            "catalogType": 1, "total": total if total is not None else len(articles),
            "articles": list(articles),
        }]},
    }).encode()


CMS_DELIST = {
    "id": 282887, "code": "d72915ed7a60473b92f0818d959a227a",
    "title": "Binance Will Delist ICX, SCRT, STORJ on 2026-09-12",
    "type": 1, "releaseDate": 1787500000000,
}
CMS_MARGIN = {
    "id": 284062, "code": "6951f54839734dfb93be3541756c6454",
    "title": "Notice of Removal of Margin Trading Pairs and Collateral - 2026-09-11",
    "type": 1, "releaseDate": 1788863408750,
}
CMS_LIST = {
    "id": 284193, "code": "6133e417dcfe43a8ad20c0db1b53c7e8",
    "title": "Binance Will List 牛来 (牛来) with Seed Tag Applied",
    "type": 1, "releaseDate": 1788953419513,
}


def bybit(*items: dict, ret: int = 0) -> bytes:
    return json.dumps({
        "retCode": ret, "retMsg": "OK" if ret == 0 else "fail",
        "result": {"total": len(items), "list": list(items)},
    }).encode()


BYBIT_DELIST = {
    "title": "Delisting of ASPUSDT Perpetual Contract",
    "description": "Bybit will be delisting the ASPUSDT Perpetual Contract at Sep 5, 2026, 9:00AM UTC.",
    "type": {"key": "delistings", "title": "Delistings"},
    "tags": ["Derivatives"],
    "url": "https://announcements.bybit.com/en-US/article/delisting-of-aspusdt-perpetual-contract--artd07ca3b584f4/",
    "dateTimestamp": 1788426034000, "publishTime": 1788426105000,
}
BYBIT_NEW = {
    "title": "New listing: EURUSDUSDT TradFi Perpetual Contract, with up to 100x leverage",
    "description": "",
    "type": {"key": "new_crypto", "title": "New Listings"},
    "tags": ["Derivatives"],
    "url": "https://announcements.bybit.com/en-US/article/new-listing-eurusdusdt-tradfi-perpetual-contract-with-up-to-100x-leverage--art24098f22b9c7/",
    "dateTimestamp": 1788863043000, "publishTime": 1788863092000,
}


def rss(*items: dict) -> bytes:
    body = "".join(
        "<item><title>{t}</title><link>{l}</link>"
        "<pubDate>{d}</pubDate><guid isPermaLink=\"false\">{g}</guid>"
        "{c}</item>".format(
            t=i["title"], l=i["link"], d=i["date"], g=i["guid"],
            c="".join(f"<category><![CDATA[{c}]]></category>" for c in i.get("cats", [])),
        )
        for i in items
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel>'
        "<title>Kraken Blog</title>" + body + "</channel></rss>"
    ).encode()


# Пины адаптера RSS ниже берут НАСТОЯЩУЮ запись реестра (`source_by_name`), а не
# собранный на месте двойник: двойник продолжал бы зеленеть и после того, как источник
# из реестра уйдёт, — ровно так его снятие в 1.2 и прошло мимо адаптерных пинов.

KRAKEN_LISTING = {
    "title": "TCS is available for trading!",
    "link": "https://blog.kraken.com/product/asset-listings/tcs-is-available-for-trading",
    "date": "Wed, 09 Sep 2026 21:17:41 +0000",
    "guid": "https://blog.kraken.com/?p=36518",
    "cats": ["Asset Listings", "Kraken"],
}
KRAKEN_PROMO = {
    "title": "Kraken Liquidity Provider Program: tier your rebates to your volume share",
    "link": "https://blog.kraken.com/product/promotions/kraken-liquidity-provider-program",
    "date": "Tue, 08 Sep 2026 14:24:01 +0000",
    "guid": "https://blog.kraken.com/?p=36454",
    "cats": ["Promotions"],
}


def hl_meta(*coins: dict) -> bytes:
    return json.dumps({"universe": list(coins), "marginTables": [], "collateralToken": 0}).encode()


def coin(name: str, lev: int = 20, delisted: bool | None = None) -> dict:
    d = {"szDecimals": 2, "name": name, "maxLeverage": lev, "marginTableId": lev}
    if delisted is not None:
        d["isDelisted"] = delisted
    return d


def many_coins(n: int, *, prefix: str = "C") -> list[dict]:
    return [coin(f"{prefix}{i}") for i in range(n)]


def hl_outcomes(*outs: dict) -> bytes:
    return json.dumps({"outcomes": list(outs), "questions": [], "deployers": [],
                       "feeScale": "1.0"}).encode()


def outcome(oid: int, name: str = "template:priceTouch", perp: str = "HYPE",
            venue: str = "out") -> dict:
    """Форма живая: описание — это `k:v|k:v`, откуда и берётся базовый перп."""
    return {"outcome": oid, "name": name,
            "description": f"perp:{perp}|priceDescription:{perp}-USDC perp mark |"
                           f"seconds:1|target:100|time:20261001-0000",
            "sideSpecs": [{"name": "template:Yes"}, {"name": "template:No"}],
            "quoteToken": "USDC", "venue": venue, "deployerFeeScale": "1.0"}


# Восемь семей — примерно столько живых шаблонов у HL живьём (29 семей на 160 исходов).
OUTCOME_FAMILIES = [("template:priceTouch", "BTC", "out"), ("template:priceTouch", "HYPE", "out"),
                    ("template:binaryPrice", "BTC", "skew"), ("template:binaryPrice", "xyz:SP500", "skew"),
                    ("template:sportsContestWinner", "?", "txyz"), ("template fallback", "?", "out"),
                    ("template:policyRateNoChange", "?", "out"), ("Recurring", "?", "None")]


def outcome_world(extra: list[dict] | None = None, *, instances: int = 3) -> bytes:
    """Каждая семья — несколькими экземплярами, как у площадки."""
    outs = []
    oid = 1200
    for name, perp, venue in OUTCOME_FAMILIES:
        for _ in range(instances):
            outs.append(outcome(oid, name=name, perp=perp, venue=venue))
            oid += 1
    return hl_outcomes(*(outs + (extra or [])))


def lighter_books(*books: dict) -> bytes:
    return json.dumps({"code": 200, "order_books": list(books)}).encode()


def book(mid: int, symbol: str, status: str = "active") -> dict:
    return {"symbol": symbol, "market_id": mid, "market_type": "perp", "status": status,
            "taker_fee": "0.0000", "maker_fee": "0.0000", "min_base_amount": "0.0800",
            "supported_size_decimals": 4, "created_at": "1774293435481"}


# Живой класс шума страницы Kraken, замерен 2026-09-09 22:00–22:05Z: два ответа
# отличались ТОЛЬКО блоком «related posts» (карусель соседних статей), то есть сырые
# байты страницы меняются сами по себе несколько раз в час.
PAGE_HEAD = (
    b"<!doctype html><html><head><title>Kraken Liquidity Provider Program</title>"
    b"<style>.a{color:red}</style></head><body><article><h1>Liquidity Provider Program</h1>"
    b"<p>Rebates are tiered to your share of monthly volume. Tier&nbsp;1 pays 0.00%.</p>"
    b"<p>" + b"Programme terms and conditions apply to every participant. " * 12 + b"</p></article>"
)
PAGE_TAIL_A = b'<div class="related-posts-container"><a href="/a">ZKP is available</a></div></body></html>'
PAGE_TAIL_B = b'<div class="related-posts-container"><a href="/b">ONE is available</a></div></body></html>'


def lpp_doc(rate: str = "0.00%", *, noise: bytes = b"") -> bytes:
    """Документ программы Lighter (`…/liquidity-partner-program.md`) — второй хэш-источник
    рядом со страницей LP-программы Kraken (`PAGE_HEAD` выше).

    Это markdown, а не HTML-страница, поэтому шум у него другой: не карусель соседних
    статей, а пробельная раскладка, которую нормализация обязана съесть так же.
    """
    return (b"# Liquidity Partner Program\n" + noise
            + b"Quote reward equals eligible liquidity. " * 30
            + f"Tier 1 pays {rate}.".encode())


# Записи взяты ДОСЛОВНО из живого ответа 2026-09-13. Две последние — измерение, на
# котором держится выбор ключа: заголовок у OKX повторяется (три пары на 60 записей),
# а слаг ссылки — нет, повторный анонс получает суффикс `-1` либо дату.
def okx(*details: dict, code: str = "0", msg: str = "", total_page: int = 18) -> bytes:
    return json.dumps({"code": code, "msg": msg,
                       "data": [{"details": list(details), "totalPage": total_page}]}).encode()


OKX_MIGRATION = {
    "annType": "announcements-delistings",
    "title": "OKX to support AERGO crypto migration",
    "url": "https://www.okx.com/en-eu/help/okx-to-support-aergo-crypto-migration",
    "pTime": "1784023216123", "businessPTime": "1784016000000",
}
OKX_DELIST = {
    "annType": "announcements-delistings",
    "title": "OKX to delist ULTI, GEAR, VRA, DAO, CXT and ELON in EEA",
    "url": "https://www.okx.com/en-eu/help/okx-to-delist-ulti-gear-vra-dao-cxt-and-elon-in-eea",
    "pTime": "1768897266308", "businessPTime": "1768897500000",
}
OKX_LIST = {
    "annType": "announcements-new-listings",
    "title": "OKX to list SLX/USDT (Solstice) for spot trading",
    "url": "https://www.okx.com/en-eu/help/okx-to-list-slx-usdt-solstice-for-spot-trading",
    "pTime": "1783648816124", "businessPTime": "1783648800000",
}
OKX_MCLAREN = {
    "annType": "announcements-others",
    "title": "OKX Card x McLaren Technology Centre Giveaway",
    "url": "https://www.okx.com/en-eu/help/eea-card-mclaren-terms-conditions",
    "pTime": "1788357366699", "businessPTime": "1788336000000",
}
OKX_EXPIRY = {
    "annType": "announcements-trading-updates",
    "title": "OKX to Discontinue Weekly Expiry Dates for Coin-Margined Expiry Futures",
    "url": "https://www.okx.com/en-eu/help/okx-to-discontinue-weekly-expiry-dates-for-coin-margined-expiry-futures",
    "pTime": "1787816875137", "businessPTime": "1785137700000",
}
# Два анонса с ОДНИМ заголовком (второй с висячим пробелом) и разными слагами.
OKX_TICK_APR = {
    "annType": "announcements-trading-updates",
    "title": "OKX to adjust tick size for perpetual futures",
    "url": "https://www.okx.com/en-eu/help/okx-to-adjust-tick-size-for-perpetual-futures-20260413",
    "pTime": "1776061137185", "businessPTime": "1776061200000",
}
OKX_TICK_MAR = {
    "annType": "announcements-trading-updates",
    "title": "OKX to adjust tick size for perpetual futures ",
    "url": "https://www.okx.com/en-eu/help/okx-to-adjust-tick-size-for-perpetual-futures-20260330",
    "pTime": "1774848128134", "businessPTime": "1774846800000",
}


# =============================================================================
# Kraken: вселенная торговых пар (единственный вход к площадке с 2026-09-14)
# =============================================================================

# Обе записи взяты ДОСЛОВНО из живого ответа 2026-09-14 (1449 пар: online 1353,
# cancel_only 79, post_only 17 — статус у этой площадки живёт и меняется, а не
# украшает ответ). Наполнитель ниже несёт только те пять полей, которые читает
# адаптер: остальное живой формы уже запинено этими двумя.
KRAKEN_XBTUSD = {
    "altname": "XBTUSD", "wsname": "XBT/USD", "aclass_base": "currency", "base": "XXBT",
    "aclass_quote": "currency", "quote": "ZUSD", "lot": "unit", "cost_decimals": 5,
    "pair_decimals": 1, "lot_decimals": 8, "lot_multiplier": 1,
    "leverage_buy": [2, 3, 4, 5], "leverage_sell": [2, 3, 4, 5], "fees": [], "fees_maker": [],
    "fee_volume_currency": "ZUSD", "margin_call": 80, "margin_stop": 40, "ordermin": "0.00005",
    "costmin": "0.5", "tick_size": "0.1", "status": "online",
    "execution_venue": "international", "long_position_limit": 350, "short_position_limit": 250,
}
# Живьём уже НЕ online: так у Kraken и выглядит начатый делистинг.
KRAKEN_ACAEUR = {
    "altname": "ACAEUR", "wsname": "ACA/EUR", "aclass_base": "currency", "base": "ACA",
    "aclass_quote": "currency", "quote": "ZEUR", "lot": "unit", "cost_decimals": 5,
    "pair_decimals": 4, "lot_decimals": 8, "lot_multiplier": 1,
    "leverage_buy": [], "leverage_sell": [], "fees": [], "fees_maker": [],
    "fee_volume_currency": "ZUSD", "margin_call": 80, "margin_stop": 40, "ordermin": "40",
    "costmin": "0.45", "tick_size": "0.0001", "status": "cancel_only",
    "execution_venue": "international",
}


def kpair(name: str, quote: str = "ZEUR", status: str = "online") -> dict:
    return {"altname": f"{name}{quote[-3:]}", "wsname": f"{name}/{quote[-3:]}",
            "base": name, "quote": quote, "status": status, "tick_size": "0.001"}


def kraken_world(extra: dict | None = None, *, n: int = 600) -> dict:
    """Правдоподобная вселенная выше пола (пол 500, живьём 1449)."""
    world = {f"FILL{i}EUR": kpair(f"FILL{i}") for i in range(n)}
    world["XXBTZUSD"] = KRAKEN_XBTUSD
    world["ACAEUR"] = KRAKEN_ACAEUR
    world.update(extra or {})
    return world


def kraken_envelope(result, *, error: list | None = None) -> bytes:
    """Конверт этой площадки: {"error": [...], "result": {...}}."""
    return json.dumps({"error": error or [], "result": result}).encode()


# Тело JS-челленджа, ДОСЛОВНО первые 180 байт ответа blog.kraken.com 2026-09-14
# (403 на любой User-Agent, с кипрской машины оператора и с токийского хоста сбора).
CHALLENGE_BODY = (
    b"<!DOCTYPE html><title>Checking your browser...</title><noscript><p>Javascript "
    b"required</p></noscript><meta content=\"width=device-width\"name=viewport>"
    b"<link href=\"data:;base64,=\"rel=icon><script>(()=>{const e=2;let t,n,o,l,r,a,c"
)


def http_error(code: int = 403, body: bytes = CHALLENGE_BODY,
               url: str = "https://blog.kraken.com/feed", *,
               gzipped: bool = False) -> urllib.error.HTTPError:
    """Настоящий HTTPError, а не двойник: тело живёт в нём и читается ровно раз.

    `gzipped=True` — это ЖИВАЯ форма ответа blog.kraken.com (замерено 2026-09-14
    20:22Z: `Content-Encoding: gzip`, 3598 сырых байт против 6877 распакованных).
    """
    headers: dict[str, str] = {}
    if gzipped:
        body, headers = gzip.compress(body), {"Content-Encoding": "gzip"}
    return urllib.error.HTTPError(url, code, "Forbidden", headers, io.BytesIO(body))


# =============================================================================
# 1.4: реестр builder-dex, спящие dex и четыре новые площадки
# =============================================================================
# Все формы ниже измерены 2026-09-14 и лежат целиком в
# ~/hft-research/new-venues-recon-20260914/out (сводка — SUMMARY.md там же).


def perpdex(name: str, full: str, deployer: str) -> dict:
    """Запись реестра `info {"type":"perpDexs"}`. Рядом площадка кладёт ещё шесть
    полей (oracleUpdater, feeRecipient, subDeployers и три таблицы по активам) —
    адаптер их не читает: решение площадки, которое он ловит, — это САМО появление
    строки в реестре."""
    return {"name": name, "fullName": full, "deployer": deployer,
            "oracleUpdater": None, "feeRecipient": None, "subDeployers": [],
            "assetToStreamingOiCap": [], "assetToFundingMultiplier": [],
            "assetToFundingInterestRate": [], "assetToFundingClamp": []}


# Живой ответ 2026-09-14: ОДИННАДЦАТЬ элементов, первый — null. Имени у мейна нет, и
# это не битая запись, а способ площадки сказать «нулевой индекс — не builder-dex».
# km и mkts — один и тот же деплойер (Kinetiq) и два разных dex: имя, а не адрес,
# отвечает за то, чем считается запись.
PERPDEX_ROWS = (
    ("xyz", "XYZ", "0x88806a71d74ad0a510b350545c9ae490912f0888"),
    ("flx", "Felix Exchange", "0x2fab552502a6d45920d5741a2f3ebf4c35536352"),
    ("vntl", "Ventuals", "0x8888888192a4a0593c13532ba48449fc24c3beda"),
    ("hyna", "HyENA", "0x53e655101ea361537124ef814ad4e654b54d0637"),
    ("km", "Markets by Kinetiq", "0x71f0019cc7fa79e4f42587fb7b9a817d8d2429ec"),
    ("abcd", "ABCDEx", "0x372c7f69d0ecc14f73f89bd8a7dfb41bdf29857c"),
    ("cash", "dreamcash", "0xffa8198c62adb1e811629bd54c9b646d726deef7"),
    ("para", "Paragon", "0x8888888c43cbb7e1c4132542e46831bffd866ed3"),
    ("mkts", "Markets By Kinetiq", "0x71f0019cc7fa79e4f42587fb7b9a817d8d2429ec"),
    ("io", "EntropyIO", "0x320c8988e3d1b5198f335802d7bfd2728a8fcac6"),
)


def perpdexs(*rows: tuple, main: bool = True) -> bytes:
    body: list = [None] if main else []
    return json.dumps(body + [perpdex(*r) for r in rows]).encode()


def hl_ctx(vlm: str = "0.0", oi: str = "0.0") -> dict:
    """Контекст актива — ВТОРАЯ половина ответа `metaAndAssetCtxs`, дословно (спящий
    abcd 2026-09-14). Числа у этой площадки приезжают СТРОКАМИ."""
    return {"funding": "0.0", "openInterest": oi, "prevDayPx": "6980.0",
            "dayNtlVlm": vlm, "premium": None, "oraclePx": "6980.0", "markPx": "6980.0",
            "midPx": None, "impactPxs": None, "dayBaseVlm": "0.0"}


def hl_meta_ctxs(*assets: tuple) -> bytes:
    """`metaAndAssetCtxs` — ПАРА [вселенная, контексты], параллельная по ИНДЕКСУ:
    имени актива во второй половине нет вовсе."""
    universe = [coin(name) for name, _, _ in assets]
    ctxs = [hl_ctx(vlm, oi) for _, vlm, oi in assets]
    return json.dumps([{"universe": universe, "marginTables": [], "collateralToken": 0},
                       ctxs]).encode()


def dormant_dex(dex: str, n: int = 3) -> bytes:
    """Спящий dex: ноль оборота и ноль открытого интереса у ВСЕХ активов — ровно так
    живьём выглядят все шесть (flx, vntl, hyna, km, cash, abcd; замерено 2026-09-14,
    ни одного актива с ненулевым dayNtlVlm или openInterest)."""
    return hl_meta_ctxs(*[(f"{dex}:A{i}", "0.0", "0.0") for i in range(n)])


# --- Lighter под Robinhood ----------------------------------------------------
# Та же форма, что у мейна Lighter, и другой адрес: 84 книги (57 перпов, 27 спотов)
# против 245 у мейна, замерено 2026-09-15 00:47Z.


def rh_book(mid: int, symbol: str, status: str = "active", *,
            created: str = "1782499411964", market_type: str = "perp") -> dict:
    return {"symbol": symbol, "market_id": mid, "market_type": market_type,
            "status": status, "taker_fee": "0.0000", "is_taker_fee_enabled": True,
            "maker_fee": "0.0000", "min_base_amount": "0.0100",
            "supported_size_decimals": 4, "created_at": created,
            "multiplier": "1.000000000000000000"}


def rh_books(*books: dict, code: int = 200) -> bytes:
    return json.dumps({"code": code, "order_books": list(books)}).encode()


def rh_world(extra: list | None = None, *, n: int = 60) -> bytes:
    return rh_books(*[rh_book(i, f"RH{i}") for i in range(n)],
                    rh_book(2064, "QQQ/USDG", market_type="spot"), *(extra or []))


# --- Polymarket Perps ---------------------------------------------------------
# Ответ — ГОЛЫЙ список без конверта; ошибка у этой площадки приезжает объектом.
POLY_SP500 = {
    "instrument_id": 1, "instrument_type": "perpetual", "category": "index",
    "isolated_only": False, "symbol": "SP500-USD", "base_asset": "SP500",
    "quote_asset": "pUSD", "funding_interval": "1h", "quantity_decimals": 5,
    "price_decimals": 1, "price_bounds": "0.05", "liquidation_fee": "0.005",
    "max_order_count": 200, "min_notional": "10", "max_market_notional": "1000000",
    "max_limit_notional": "5000000", "max_leverage": 20,
    "risk_tiers": [{"lower_bound": "0", "max_leverage": 20},
                   {"lower_bound": "250000", "max_leverage": 10}],
    "ui_live_time": 1785542400000,
}
POLY_SPCX = dict(POLY_SP500, instrument_id=9, category="equity", symbol="SPCX-USD",
                 base_asset="SPCX", max_leverage=10, ui_live_time=1785542400000)


def poly_instrument(iid: int, symbol: str, *, live_ms: int = 1786966357000,
                    category: str = "crypto") -> dict:
    return dict(POLY_SP500, instrument_id=iid, symbol=symbol, base_asset=symbol.split("-")[0],
                category=category, ui_live_time=live_ms)


def poly_instruments(*rows: dict) -> bytes:
    return json.dumps(list(rows)).encode()


def poly_world(extra: list | None = None, *, n: int = 60) -> bytes:
    return poly_instruments(POLY_SP500, POLY_SPCX,
                            *[poly_instrument(100 + i, f"P{i}-USD") for i in range(n)],
                            *(extra or []))


# --- Deribit USDC -------------------------------------------------------------
# Конверт JSON-RPC. 121 инструмент живьём, из них 21 с `is_active: false`: у этой
# площадки инструмент заводится ЗАРАНЕЕ и включается отдельным решением.
DERIBIT_BONK = {
    "state": "inactive", "price_index": "1000bonk_usdc", "kind": "future",
    "instrument_name": "1000BONK_USDC-PERPETUAL", "max_leverage": 10,
    "maker_commission": 0.00015, "taker_commission": 0.00035,
    "instrument_type": "linear", "instrument_id": 692942,
    "expiration_timestamp": 32503708800000, "underlying_type": "crypto",
    "product_group": "TIER_3", "creation_timestamp": 1789128071000, "is_active": False,
    "contract_size": 1, "tick_size": 1e-06, "counter_currency": "USDC",
    "future_type": "linear", "settlement_currency": "USDC",
    "settlement_period": "perpetual", "base_currency": "1000BONK",
    "quote_currency": "USDC", "min_trade_amount": 1,
}
DERIBIT_PEPE = dict(DERIBIT_BONK, state="open", is_active=True, instrument_id=691765,
                    instrument_name="1000PEPE_USDC-PERPETUAL", base_currency="1000PEPE",
                    price_index="1000pepe_usdc", creation_timestamp=1789027213000,
                    max_leverage=25)


def deribit_instrument(name: str, *, active: bool = True, period: str = "perpetual",
                       created: int = 1789128071000) -> dict:
    return dict(DERIBIT_BONK, instrument_name=name, is_active=active,
                state="open" if active else "inactive", settlement_period=period,
                creation_timestamp=created)


def deribit_envelope(result, **extra) -> bytes:
    doc = {"jsonrpc": "2.0", "result": result, "usIn": 1789414771084985,
           "usOut": 1789414771085284, "usDiff": 299, "testnet": False}
    doc.update(extra)
    return json.dumps(doc).encode()


def deribit_world(extra: list | None = None, *, n: int = 60, bonk_active: bool = False) -> bytes:
    rows = [deribit_instrument(f"D{i}_USDC-PERPETUAL") for i in range(n)]
    rows.append(deribit_instrument(DERIBIT_BONK["instrument_name"], active=bonk_active))
    rows.append(DERIBIT_PEPE)
    return deribit_envelope(rows + (extra or []))


# --- Kraken Futures -----------------------------------------------------------
# 295 инструментов живьём, 283 из них `flexible_futures` (в том числе перпы на акции:
# PF_NBISUSD открыт 2026-09-03). Запись СОКРАЩЕНА: marginLevels, retailMarginLevels,
# marginSchedules, platformsPermitted и tags опущены — адаптер их не читает, а живьём
# они дают по шестьдесят строк на инструмент.
KRAKEN_FUT_NBIS = {
    "symbol": "PF_NBISUSD", "type": "flexible_futures", "tickSize": 0.01,
    "contractSize": 1, "tradeable": True, "impactMidSize": 5.0,
    "maxPositionSize": 12000.0, "maxOpenInterestUsd": 14000000,
    "openingDate": "2026-09-03T11:58:25Z", "fundingRateCoefficient": 8,
    "maxRelativeFundingRate": 0.005, "isin": "GB00BXCBYC14",
    "contractValueTradePrecision": 2, "postOnly": False, "mtf": True,
    "countriesBanned": [], "base": "NBIS", "quote": "USD", "pair": "NBIS:USD",
    "category": "Equity", "tags": [], "tradfi": True, "isExpired": False,
}
KRAKEN_FUT_XBT = dict(KRAKEN_FUT_NBIS, symbol="PI_XBTUSD", type="futures_inverse",
                      openingDate="2018-08-31T00:00:00Z", base="XBT", pair="XBT:USD",
                      category="", tradfi=False, isin="GB00J62YGL67")


def kfut(symbol: str, *, tradeable: bool = True, opening: str = "2026-09-03T11:58:25Z",
         typ: str = "flexible_futures") -> dict:
    return dict(KRAKEN_FUT_NBIS, symbol=symbol, tradeable=tradeable,
                openingDate=opening, type=typ)


def kfut_envelope(instruments, *, result: str = "success", **extra) -> bytes:
    doc = {"result": result, "instruments": instruments,
           "serverTime": "2026-09-14T19:39:31.936Z"}
    doc.update(extra)
    return json.dumps(doc).encode()


def kfut_world(extra: list | None = None, *, n: int = 150) -> bytes:
    rows = [kfut(f"PF_F{i}USD") for i in range(n)]
    return kfut_envelope(rows + [KRAKEN_FUT_NBIS, KRAKEN_FUT_XBT] + (extra or []))


DEFAULT_PAYLOADS: dict[str, bytes] = {
    "binance_delisting": cms(CMS_DELIST, catalog=161, name="Delisting", total=430),
    "binance_listing": cms(CMS_LIST, catalog=48, name="New Cryptocurrency Listing", total=2252),
    "bybit_delistings": bybit(BYBIT_DELIST),
    "bybit_new_crypto": bybit(BYBIT_NEW),
    "kraken_blog": rss(KRAKEN_LISTING, KRAKEN_PROMO),
    "hl_universe": hl_meta(*many_coins(120)),
    "hl_dex_xyz": hl_meta(*many_coins(30, prefix="xyz:S")),
    "hl_dex_io": hl_meta(*many_coins(30, prefix="io:S")),
    "hl_dex_mkts": hl_meta(*many_coins(30, prefix="mkts:S")),
    "hl_outcomes": outcome_world(),
    "lighter_markets": lighter_books(*[book(i, f"SYM{i}") for i in range(240)]),
    "lighter_lpp_doc": lpp_doc(),
    "kraken_lp_page": PAGE_HEAD + PAGE_TAIL_A,
    "kraken_pairs": kraken_envelope(kraken_world()),
    "okx_delistings": okx(OKX_MIGRATION, OKX_DELIST, total_page=2),
    "okx_new_listings": okx(OKX_LIST),
    # Тихая страница «последних»: ни одно ключевое слово не совпало. Живьём это норма —
    # за 160 записей совпало 21, то есть на странице из 20 чаще всего нет ни одной.
    "okx_latest": okx(OKX_MCLAREN, OKX_EXPIRY, total_page=103),
    # --- 1.4 ------------------------------------------------------------------
    "hl_perpdexs": perpdexs(*PERPDEX_ROWS),
    "hl_dex_para": hl_meta(*many_coins(35, prefix="para:S")),
    "hl_dex_flx": hl_meta(*many_coins(16, prefix="flx:S")),
    "hl_dex_vntl": hl_meta(*many_coins(15, prefix="vntl:S")),
    "hl_dex_hyna": hl_meta(*many_coins(25, prefix="hyna:S")),
    "hl_dex_km": hl_meta(*many_coins(23, prefix="km:S")),
    "hl_dex_cash": hl_meta(*many_coins(17, prefix="cash:S")),
    # Живьём у abcd РОВНО ОДИН актив — отсюда и пол 1 у спящих dex.
    "hl_dex_abcd": hl_meta(*many_coins(1, prefix="abcd:S")),
    "hl_dex_flx_activity": dormant_dex("flx", 16),
    "hl_dex_vntl_activity": dormant_dex("vntl", 15),
    "hl_dex_hyna_activity": dormant_dex("hyna", 25),
    "hl_dex_km_activity": dormant_dex("km", 23),
    "hl_dex_cash_activity": dormant_dex("cash", 17),
    "hl_dex_abcd_activity": dormant_dex("abcd", 1),
    "lighter_rh_markets": rh_world(),
    "polymarket_perps_instruments": poly_world(),
    "deribit_usdc_instruments": deribit_world(),
    "kraken_futures_instruments": kfut_world(),
}


class FakeHost:
    """Двойник исполняющего слоя: наружу ни байта, каждый вызов записан."""

    def __init__(self, payloads: dict | None = None, *, now_ms: int = NOW,
                 files: dict | None = None, tg_ok: bool = True):
        self.state_path = "/data/seen.json"
        self.pulse_path = "/data/pulse.json"
        self.payloads = dict(DEFAULT_PAYLOADS)
        self.payloads.update(payloads or {})
        self._now = now_ms
        self.files: dict[str, str] = dict(files or {})
        self.blobs: dict[str, bytes] = {}
        # Время последней записи файла — единственное, чем можно ограничить окно,
        # потерянное вместе с нечитаемым состоянием: этот же тик его затирает.
        self.mtimes: dict[str, int] = {p: now_ms for p in self.files}
        self.fetched: list[str] = []
        self.telegrams: list[str] = []
        self.pulses: list[str] = []
        self.tg_ok = tg_ok

    def now_ms(self) -> int:
        return self._now

    def fetch(self, source) -> bytes:
        self.fetched.append(source.name)
        payload = self.payloads[source.name]
        if isinstance(payload, Exception):
            raise payload
        return payload

    def read_file(self, path: str) -> str | None:
        return self.files.get(path)

    def file_mtime_ms(self, path: str) -> int | None:
        return self.mtimes.get(path)

    def write_atomic(self, path: str, text: str) -> None:
        self.files[path] = text
        self.mtimes[path] = self._now

    def write_bytes(self, path: str, data: bytes) -> None:
        self.blobs[path] = data

    def send_telegram(self, text: str) -> bool:
        if self.tg_ok:
            self.telegrams.append(text)
            return True
        return False

    def write_pulse(self, text: str) -> None:
        self.pulses.append(text)

    # --- удобства теста
    def state(self) -> State:
        return load_state(self.files.get(self.state_path))

    def item_alerts(self) -> list[str]:
        return [t for t in self.telegrams if not t.startswith(("\U0001f534", "\U0001f7e2"))]


def seeded_host(payloads: dict | None = None, **kw) -> FakeHost:
    """Хост, у которого засев уже произошёл: дальше пиннуем только НОВОЕ."""
    host = FakeHost(payloads, **kw)
    tick(host)
    assert host.telegrams == [], host.telegrams
    host.telegrams.clear()
    host.pulses.clear()
    host.fetched.clear()
    return host


def retick(host: FakeHost, payloads: dict | None = None, *, advance_ms: int = 900_000) -> FakeHost:
    """Шаг вперёд на 15 минут: столько нужно, чтобы стал должен самый медленный из
    пятиминутно-четвертьчасовых источников (лента блога Kraken). Сломанный ответ
    подаётся ИМЕННО здесь, а не в засеве: источник обязан сперва стать здоровым,
    иначе тест меряет засев, а не поломку."""
    host.payloads.update(payloads or {})
    host._now += advance_ms
    tick(host)
    return host


# =============================================================================
# ЗАСЕВ И ДЕТЕКЦИЯ
# =============================================================================


def test_the_first_tick_seeds_every_due_source_without_a_single_alert():
    """430 делистингов и 2252 листинга в Telegram на старте — это не запуск, а
    инцидент. Мир запоминается молча, отчитывается только журнал."""
    host = FakeHost()
    line = tick(host)

    assert host.telegrams == []
    st = host.state()
    assert {s.name for s in SOURCES if s.cadence_s <= 300} <= set(st.sources)
    assert st.sources["binance_delisting"].seeded is True
    assert "засев" in line


def test_a_new_article_after_the_seed_is_reported_once_and_only_once():
    host = seeded_host()
    retick(host, {"binance_delisting": cms(CMS_MARGIN, CMS_DELIST, catalog=161, name="Delisting")})

    assert len(host.telegrams) == 1
    assert "Notice of Removal of Margin Trading Pairs" in host.telegrams[0]

    host.telegrams.clear()
    retick(host)  # тот же мир — тишина
    assert host.telegrams == []


def test_a_source_added_later_seeds_on_its_own_without_alerting():
    """Источники засеваются независимо: новый адаптер не имеет права выплюнуть свою
    историю в чат только потому, что остальные давно работают."""
    host = seeded_host()
    st = host.state()
    del st.sources["bybit_delistings"]
    host.files[host.state_path] = dump_state(st)

    retick(host)

    assert host.telegrams == []
    assert host.state().sources["bybit_delistings"].seeded is True


def test_the_key_is_the_venue_id_not_the_title():
    """Заголовок площадка правит задним числом (опечатки, добавленные тикеры).
    Ключ — id статьи, иначе одна правка = второй алерт о том же событии."""
    host = seeded_host()
    edited = dict(CMS_DELIST, title=CMS_DELIST["title"] + " (updated)")
    retick(host, {"binance_delisting": cms(edited, catalog=161, name="Delisting")})

    assert host.telegrams == []


# =============================================================================
# FAIL-CLOSED ПО ИСТОЧНИКУ
# =============================================================================


def test_an_unparsable_response_is_a_source_failure_not_an_empty_world():
    host = seeded_host()
    retick(host, {"bybit_delistings": b"<html>rate limited</html>"})

    st = host.state()
    assert st.sources["bybit_delistings"].fails == 1
    assert st.sources["bybit_delistings"].seen  # засеянное не тронуто
    assert host.telegrams == []  # один провал ещё не повод будить оператора


def test_an_empty_list_is_a_source_failure_not_an_empty_world():
    """Пустой ответ — самая опасная форма: как мир он значит «анонсов больше нет»,
    и следующий настоящий анонс уже не отличить от старого."""
    host = seeded_host()
    retick(host, {"binance_delisting": cms(catalog=161, name="Delisting")})

    assert host.state().sources["binance_delisting"].fails == 1


def test_a_truncated_universe_is_refused_before_it_can_become_a_snapshot():
    """Пол на размере вселенной живёт в разборе, а не в diff-е: усечённый ПЕРВЫЙ ответ
    diff не проходит вовсе, он просто станет снимком — и настоящая вселенная потом
    прочитается как 200 листингов, а настоящий делистинг утонет в них."""
    with pytest.raises(SourceError):
        ap.parse_hl_meta(hl_meta(coin("BTC"), coin("ETH")), source_by_name("hl_universe"))

    plan, new = plan_tick({"hl_universe": hl_meta(coin("BTC"), coin("ETH"))}, State(), NOW)
    assert new.sources["hl_universe"].seeded is False
    assert new.sources["hl_universe"].snapshot is None
    assert plan.messages == []


def test_a_truncated_universe_never_reads_as_a_mass_delisting():
    """Три монеты вместо 234 — это сломанное происхождение, а не делистинг всего.
    Без пола diff выдал бы 231 алерт и записал бы ложный снимок."""
    host = seeded_host()
    retick(host, {"hl_universe": hl_meta(coin("BTC"), coin("ETH"), coin("SOL"))})

    st = host.state()
    assert st.sources["hl_universe"].fails == 1
    assert len(st.sources["hl_universe"].snapshot) == 120
    assert host.telegrams == []


def test_a_wholesale_churn_reads_as_a_schema_move_not_as_the_world():
    """Вселенная правдоподобного размера, но не та (переименование поля, отданный
    не тот dex). Массовая замена читается как провал источника."""
    host = seeded_host()
    retick(host, {"hl_dex_xyz": hl_meta(*many_coins(30, prefix="io:S"))})

    st = host.state()
    assert st.sources["hl_dex_xyz"].fails == 1
    assert host.telegrams == []
    assert next(iter(st.sources["hl_dex_xyz"].snapshot)).startswith("xyz:")


def test_a_change_just_under_the_churn_guard_is_still_reported():
    """Граница жива с обеих сторон: гвард не имеет права глотать настоящую волну."""
    host = seeded_host()
    universe = many_coins(120)
    for i in range(CHURN_FLOOR):
        universe.append(coin(f"NEW{i}"))
    retick(host, {"hl_universe": hl_meta(*universe)})

    assert len(host.item_alerts()) == CHURN_FLOOR
    assert host.state().sources["hl_universe"].fails == 0


def test_disappearances_are_silent_but_still_count_toward_the_churn_guard():
    """Исчезновения не отправляются (2026-10-06), но считаются в гварде массовой
    замены. Без этого ответ, усечённый ВЫШЕ пола, тихо продвинул бы снимок, а
    вернувшаяся полная вселенная прочиталась бы волной ложных «появился» — ровно то,
    от чего гвард стоит. Граница проверена с обеих сторон, по константам, а не по
    числам: засев xyz — 30 активов."""
    limit = int(max(CHURN_FLOOR, ap.CHURN_FRACTION * 30))

    host = seeded_host()
    retick(host, {"hl_dex_xyz": hl_meta(*many_coins(30 - limit, prefix="xyz:S"))})
    st = host.state()
    assert st.sources["hl_dex_xyz"].fails == 0
    assert len(st.sources["hl_dex_xyz"].snapshot) == 30 - limit
    assert host.item_alerts() == []

    host = seeded_host()
    retick(host, {"hl_dex_xyz": hl_meta(*many_coins(30 - limit - 1, prefix="xyz:S"))})
    st = host.state()
    assert st.sources["hl_dex_xyz"].fails == 1
    assert len(st.sources["hl_dex_xyz"].snapshot) == 30
    assert host.item_alerts() == []


def test_one_source_failing_does_not_block_the_others():
    host = seeded_host()
    retick(host, {"hl_universe": TimeoutError("boom"),
                  "binance_delisting": cms(CMS_MARGIN, CMS_DELIST, catalog=161, name="Delisting")})

    assert len(host.item_alerts()) == 1
    st = host.state()
    assert st.sources["hl_universe"].fails == 1
    assert st.sources["binance_delisting"].fails == 0


def test_a_venue_error_code_is_a_failure_even_with_a_valid_envelope():
    """Bybit отвечает 200 с retCode != 0. Конверт валиден, содержимого нет."""
    host = seeded_host()
    # Список НЕПУСТ намеренно: иначе провал ловил бы пол на длине, а не сам retCode,
    # и снятие проверки кода прошло бы незамеченным.
    retick(host, {"bybit_new_crypto": bybit(BYBIT_NEW, ret=10006)})

    assert host.state().sources["bybit_new_crypto"].fails == 1



def test_a_repointed_binance_catalog_reads_as_a_failure_not_as_the_world():
    """200, форма валидна, статьи есть — но каталог уже не тот (Binance перевесил id
    161 на другую рубрику). Прочитанное как мир, это молчание о делистингах навсегда:
    в seen уедут чужие ключи, а настоящие статьи прибор больше никогда не покажет.
    Имя каталога — единственное, чем ответ представляется, поэтому оно и пиннуто."""
    host = seeded_host()
    retick(host, {"binance_delisting": cms(dict(CMS_DELIST, id=999001,
                                                title="Binance Adds FOO Trading Bot"),
                                           catalog=161, name="Trading Bots")})

    st = host.state().sources["binance_delisting"]
    assert st.fails == 1
    assert host.item_alerts() == []
    assert "Trading Bots" in st.last_error


def test_a_bybit_answer_of_another_type_reads_as_a_failure():
    """У Bybit семантику несёт `type.key` КАЖДОЙ записи, а не только параметр запроса:
    ответ листингов на запрос делистингов — это не «делистингов больше нет»."""
    host = seeded_host()
    retick(host, {"bybit_delistings": bybit(BYBIT_NEW)})

    st = host.state().sources["bybit_delistings"]
    assert st.fails == 1
    assert host.item_alerts() == []


def test_every_catalogued_feed_declares_the_identity_it_expects():
    """Пин реестра: источник без ожидаемой идентичности снова читает «200 с чужим
    содержимым» как мир. Значения измерены живьём 2026-09-10."""
    for name, expect in (("binance_delisting", "Delisting"),
                         ("binance_listing", "New Cryptocurrency Listing"),
                         ("bybit_delistings", "delistings"),
                         ("bybit_new_crypto", "new_crypto"),
                         ("okx_delistings", "announcements-delistings"),
                         ("okx_new_listings", "announcements-new-listings")):
        assert source_by_name(name).expect == expect, name


def test_a_failure_keeps_the_body_that_caused_it():
    """Урок сайдкара: `last_error='в ответе нет data'` не говорит, ЧТО ответила
    площадка, а второй такой же ответ придёт только через каденцию. Тело провала
    ложится рядом с состоянием — иначе разбирать нечего."""
    host = seeded_host()
    body = b'{"code":"000000","data":null,"success":true}'
    retick(host, {"binance_delisting": body})

    assert host.blobs["/data/last_fail/binance_delisting.bin"] == body


def test_the_kept_body_is_bounded():
    """Сайдкар не имеет права стать способом забить диск ответом площадки."""
    host = seeded_host()
    retick(host, {"lighter_markets": b"x" * (4 * ap.FAIL_BODY_BYTES)})

    assert len(host.blobs["/data/last_fail/lighter_markets.bin"]) == ap.FAIL_BODY_BYTES


# =============================================================================
# АЛЕРТЫ О ПРОВАЛАХ
# =============================================================================


def test_failures_alert_only_after_the_threshold_and_then_on_the_cadence():
    host = seeded_host()
    retick(host, {"hl_universe": TimeoutError("boom")})
    for _ in range(FAIL_ALERT_AFTER - 2):
        retick(host)
    assert host.telegrams == []

    retick(host)
    assert len(host.telegrams) == 1
    assert "hl_universe" in host.telegrams[0]

    host.telegrams.clear()
    for _ in range(FAIL_REALERT_EVERY - 1):
        retick(host)
    assert host.telegrams == []
    retick(host)
    assert len(host.telegrams) == 1


def test_a_recovered_source_says_so_once():
    host = seeded_host()
    retick(host, {"hl_universe": TimeoutError("boom")})
    for _ in range(FAIL_ALERT_AFTER - 1):
        retick(host)
    host.telegrams.clear()

    retick(host, {"hl_universe": DEFAULT_PAYLOADS["hl_universe"]})
    assert len(host.telegrams) == 1
    assert host.telegrams[0].startswith("\U0001f7e2")

    host.telegrams.clear()
    retick(host)
    assert host.telegrams == []


def test_a_failure_that_never_crossed_the_threshold_stays_silent_on_recovery():
    """Мигание сети не должно давать пару «упал/встал»: молчали — молчим."""
    host = seeded_host()
    retick(host, {"hl_universe": TimeoutError("boom")})
    retick(host, {"hl_universe": DEFAULT_PAYLOADS["hl_universe"]})

    assert host.telegrams == []



def test_an_undelivered_failure_alert_is_repeated_until_it_lands():
    """Шестой провал совпал с провалом Telegram — и алерт терялся до 42-го: три часа
    тишины у пятиминутных источников, тридцать шесть у часовых. Перевзведение живёт
    в состоянии, потому что переживать оно обязано и перезапуск процесса."""
    host = seeded_host(tg_ok=False)
    for _ in range(FAIL_ALERT_AFTER):
        retick(host, {"hl_universe": TimeoutError("boom")})
    assert host.telegrams == []
    assert host.state().sources["hl_universe"].fail_alert_pending is True

    host.tg_ok = True
    retick(host, {"hl_universe": TimeoutError("boom")})

    assert len(host.telegrams) == 1
    assert host.telegrams[0].startswith("\U0001f534") and "hl_universe" in host.telegrams[0]
    assert host.state().sources["hl_universe"].fail_alert_pending is False


def test_a_delivered_failure_alert_is_not_repeated_on_the_next_tick():
    """Обратная сторона: перевзведение обязано сниматься доставкой, иначе один провал
    станет сообщением на каждый тик."""
    host = seeded_host()
    for _ in range(FAIL_ALERT_AFTER):
        retick(host, {"hl_universe": TimeoutError("boom")})
    assert len(host.telegrams) == 1

    retick(host, {"hl_universe": TimeoutError("boom")})

    assert len(host.telegrams) == 1


def test_a_source_that_recovers_before_its_alert_landed_does_not_alert_later():
    """Недоставленный алерт о провале не имеет права пережить сам провал: иначе первый
    же следующий сбой отчитается «шесть провалов подряд» на первом же провале."""
    host = seeded_host(tg_ok=False)
    for _ in range(FAIL_ALERT_AFTER):
        retick(host, {"hl_universe": TimeoutError("boom")})
    host.tg_ok = True
    # Здоровый ответ подаётся ЯВНО: `retick` без payload переиспользует прошлый, и
    # «выздоровление» без этой строки было бы седьмым провалом подряд — пин прошёл бы,
    # ничего не проверив (поймано мутацией).
    retick(host, {"hl_universe": DEFAULT_PAYLOADS["hl_universe"]})

    assert host.state().sources["hl_universe"].fails == 0
    assert host.state().sources["hl_universe"].fail_alert_pending is False
    host.telegrams.clear()
    retick(host, {"hl_universe": TimeoutError("boom")})
    assert host.telegrams == []


# =============================================================================
# ЧАСЫ
# =============================================================================


def test_the_message_names_the_venue_clock_and_the_local_clock_separately():
    """28.08 «лаг площадки» оказался нашей же задержкой приёма. Метка площадки едет
    со своим именем поля, локальные часы — отдельной строкой."""
    host = seeded_host()
    retick(host, {"bybit_delistings": bybit(dict(BYBIT_DELIST, publishTime=1788999999000,
                                                 url=BYBIT_DELIST["url"].replace("d07c", "e11e")))})

    text = host.item_alerts()[0]
    assert "publishTime" in text
    assert "2026-09-10T" in text  # метка площадки
    assert "локально" in text
    assert text.rstrip().endswith("Z")


def test_a_source_without_a_venue_timestamp_says_so_instead_of_borrowing_ours():
    """У diff-источника метки площадки нет вовсе. Подставить локальные часы —
    значит соврать о том, когда площадка приняла решение."""
    host = seeded_host()
    retick(host, {"hl_universe": hl_meta(*many_coins(120), coin("NEWCOIN"))})

    text = host.item_alerts()[0]
    assert "метки площадки нет" in text
    assert "локально" in text


# =============================================================================
# DIFF-ИСТОЧНИКИ
# =============================================================================


def test_a_new_hyperliquid_coin_is_reported_once_with_a_stable_key():
    host = seeded_host()
    retick(host, {"hl_universe": hl_meta(*many_coins(120), coin("NEWCOIN"))})
    assert len(host.item_alerts()) == 1
    assert "NEWCOIN" in host.item_alerts()[0]

    host.telegrams.clear()
    retick(host, {"hl_universe": hl_meta(*many_coins(120), coin("NEWCOIN"))})
    assert host.telegrams == []


def test_an_isdelisted_flip_is_reported_as_a_delisting():
    host = seeded_host({"hl_universe": hl_meta(*many_coins(119), coin("DOOMED", delisted=False))})
    retick(host, {"hl_universe": hl_meta(*many_coins(119), coin("DOOMED", delisted=True))})

    text = host.item_alerts()[0]
    assert "DOOMED" in text and "isDelisted" in text


def test_a_maxleverage_change_is_reported():
    host = seeded_host({"hl_universe": hl_meta(*many_coins(119), coin("HYPE", lev=10))})
    retick(host, {"hl_universe": hl_meta(*many_coins(119), coin("HYPE", lev=5))})

    text = host.item_alerts()[0]
    assert "HYPE" in text and "10" in text and "5" in text


def test_a_disappeared_asset_is_not_reported_but_leaves_the_snapshot():
    """Исчезновение инструмента в Telegram не едет — решение оператора 2026-10-06,
    после того как недельный простой bigbox вывалил 178 изменений разом. Начало
    делистинга — смена значения (`isDelisted` и т. п.), её видно по-прежнему; исчезновение
    ключа — уже конец. Но снимок обязан актив забыть: иначе вернувшийся актив не
    прочитается как новый."""
    host = seeded_host()
    retick(host, {"hl_dex_xyz": hl_meta(*many_coins(29, prefix="xyz:S"))})

    assert host.item_alerts() == []
    st = host.state()
    assert st.sources["hl_dex_xyz"].fails == 0
    assert "xyz:S29" not in st.sources["hl_dex_xyz"].snapshot

    retick(host, {"hl_dex_xyz": hl_meta(*many_coins(30, prefix="xyz:S"))})
    assert len(host.item_alerts()) == 1
    assert "xyz:S29" in host.item_alerts()[0]


def test_a_small_builder_dex_losing_half_its_assets_is_not_a_source_failure():
    """io живьём — девять активов. Пол, взятый «как у мейна», превратил бы потерю
    половины такого dex-а в провал источника: источник болен, снимок не продвинут, и
    следующее настоящее изменение тонет. Сами исчезновения в Telegram не едут
    (2026-10-06), но источник обязан остаться здоровым."""
    host = seeded_host({"hl_dex_io": hl_meta(*many_coins(9, prefix="io:S"))})
    retick(host, {"hl_dex_io": hl_meta(*many_coins(4, prefix="io:S"))})

    assert host.item_alerts() == []
    st = host.state()
    assert st.sources["hl_dex_io"].fails == 0
    assert len(st.sources["hl_dex_io"].snapshot) == 4


def test_changing_a_snapshot_shape_reseeds_instead_of_killing_the_source():
    """Апгрейд адаптера, меняющий способ ключевания (у hl_outcomes — с поштучного на
    семейный), иначе даёт полторы сотни «изменений» на первом же тике: гвард массовой
    замены читает их как провал схемы, и источник умирает до вмешательства оператора —
    громко в чат, но без единого анонса."""
    host = seeded_host()
    st = host.state()
    old = st.sources["hl_outcomes"]
    old.shape = "поштучно/0"
    old.snapshot = {str(i): {"label": f"#{i}"} for i in range(1200, 1356)}
    host.files[host.state_path] = dump_state(st)

    retick(host)

    fresh = host.state().sources["hl_outcomes"]
    assert host.telegrams == []
    assert fresh.fails == 0
    assert fresh.shape == source_by_name("hl_outcomes").shape
    assert "template:priceTouch|BTC|out" in fresh.snapshot


def test_an_all_but_empty_outcome_meta_is_refused():
    """У семейного разбора свой пол: одна семья вместо 29 — это не «HL свернула
    аутком-рынки», это обрезанный ответ, и снимок с неё стал бы источником ложных
    «новых семей» на каждой следующей проверке."""
    with pytest.raises(SourceError):
        ap.parse_hl_outcomes(hl_outcomes(outcome(1)), source_by_name("hl_outcomes"))


def test_an_all_but_empty_builder_dex_answer_is_still_refused():
    """Другая сторона той же границы: один актив вместо девяти — это не делистинг,
    это сломанное происхождение."""
    with pytest.raises(SourceError):
        ap.parse_hl_meta(hl_meta(coin("io:S0")), source_by_name("hl_dex_io"))


def test_a_flapping_page_that_settles_can_be_reported_unstable_again():
    """Флаг нестабильности снимается при возврате к известному дайджесту: иначе одна
    объявленная нестабильность глушила бы вторую навсегда."""
    host = seeded_host()
    for i in range(UNSTABLE_AFTER):
        retick(host, {"lighter_lpp_doc": lpp_doc(f"{i}.11%")}, advance_ms=HOUR_MS)
    assert len(host.telegrams) == 1

    retick(host, {"lighter_lpp_doc": lpp_doc()}, advance_ms=HOUR_MS)
    assert host.state().sources["lighter_lpp_doc"].unstable_alerted is False

    for i in range(UNSTABLE_AFTER):
        retick(host, {"lighter_lpp_doc": lpp_doc(f"{i}.77%")}, advance_ms=HOUR_MS)
    assert len(host.telegrams) == 2


def test_a_lighter_market_going_inactive_is_reported():
    host = seeded_host()
    books = [book(i, f"SYM{i}") for i in range(239)] + [book(239, "SYM239", status="inactive")]
    retick(host, {"lighter_markets": lighter_books(*books)})

    text = host.item_alerts()[0]
    assert "SYM239" in text and "inactive" in text


def test_a_new_kind_of_outcome_market_is_reported():
    """Новая СЕМЬЯ — новый шаблон, новый базовый актив или новый деплойер — это
    решение площадки."""
    host = seeded_host()
    retick(host, {"hl_outcomes": outcome_world([outcome(1900, perp="SOL")])})

    assert len(host.item_alerts()) == 1
    text = host.item_alerts()[0]
    assert "SOL" in text and "priceTouch" in text


def test_the_hourly_roll_of_outcome_markets_is_not_an_announcement():
    """ЗАМЕРЕНО 2026-09-09 22:05→22:26Z: за 21 минуту 13 исходов появилось и 17
    исчезло из 160, а множество семей не изменилось ни на одну. Поштучный diff давал
    бы ~2000 сообщений в сутки и похоронил бы в них единственное, ради чего источник
    заведён."""
    host = seeded_host()
    rolled = outcome_world(instances=5)  # те же семьи, другие и более многочисленные id
    assert rolled != DEFAULT_PAYLOADS["hl_outcomes"]
    retick(host, {"hl_outcomes": rolled})

    assert host.telegrams == []
    assert host.state().sources["hl_outcomes"].fails == 0


def test_every_builder_dex_is_its_own_source_with_one_request_each():
    """dex=xyz/io/mkts — три РАЗНЫХ решения площадки и три разных запроса. Один
    источник на три dex-а означал бы, что провал одного глушит остальные."""
    names = {s.name for s in SOURCES}
    assert {"hl_dex_xyz", "hl_dex_io", "hl_dex_mkts"} <= names
    assert {source_by_name(n).body["dex"] for n in ("hl_dex_xyz", "hl_dex_io", "hl_dex_mkts")} == {
        "xyz", "io", "mkts"}


# =============================================================================
# ЖУРНАЛ ДОСТАВКИ
# =============================================================================


def test_an_undelivered_alert_is_retried_on_the_next_tick():
    """Ключ помечается виденным только тогда, когда Telegram принял сообщение.
    Иначе упавшая доставка = навсегда потерянное событие: страница отдаёт 50 последних."""
    host = seeded_host()
    host.tg_ok = False
    retick(host, {"binance_delisting": cms(CMS_MARGIN, CMS_DELIST, catalog=161, name="Delisting")})
    assert host.telegrams == []

    host.tg_ok = True
    retick(host)
    assert len(host.item_alerts()) == 1
    assert "Notice of Removal" in host.item_alerts()[0]


def test_a_snapshot_only_advances_when_every_change_it_carries_was_delivered():
    host = seeded_host()
    host.tg_ok = False
    retick(host, {"hl_universe": hl_meta(*many_coins(120), coin("NEWCOIN"))})
    assert "NEWCOIN" not in host.state().sources["hl_universe"].snapshot

    host.tg_ok = True
    retick(host)
    assert len(host.item_alerts()) == 1
    assert "NEWCOIN" in host.state().sources["hl_universe"].snapshot


def test_a_burst_is_drained_in_bounded_batches_without_losing_an_item():
    """Пачка из 30 анонсов не имеет права стать 30 сообщениями подряд (Telegram
    режет), но и потеряться не имеет права. Хвост остаётся непомеченным и уезжает
    следующим тиком — сток, а не потеря."""
    host = seeded_host()
    burst = [dict(CMS_DELIST, id=900_000 + i, code=f"{i:032x}",
                  title=f"Binance Will Delist COIN{i}", releaseDate=1787500000000 + i)
             for i in range(MAX_ALERTS_PER_TICK + 3)]
    retick(host, {"binance_delisting": cms(*burst, catalog=161, name="Delisting")})
    assert len(host.item_alerts()) == MAX_ALERTS_PER_TICK

    host.telegrams.clear()
    retick(host)
    assert len(host.item_alerts()) == 3
    reported = " ".join(host.item_alerts())
    assert all(f"COIN{i}" in reported for i in range(MAX_ALERTS_PER_TICK, MAX_ALERTS_PER_TICK + 3))


def test_the_drain_is_chronological():
    """Сначала уезжает самое старое: иначе после простоя оператор читает историю
    задом наперёд и не понимает, что уже случилось, а что только что."""
    host = seeded_host()
    burst = [dict(CMS_DELIST, id=900_000 + i, code=f"{i:032x}",
                  title=f"Binance Will Delist COIN{i}", releaseDate=1787500000000 + i)
             for i in range(MAX_ALERTS_PER_TICK + 3)]
    retick(host, {"binance_delisting": cms(*burst, catalog=161, name="Delisting")})

    order = [t.index("COIN") for t in host.item_alerts()]
    reported = [int(t.split("COIN")[1].split()[0].split("\n")[0]) for t in host.item_alerts()]
    assert order and reported == sorted(reported)
    assert reported[0] == 0


# =============================================================================
# КАДЕНЦИЯ И ВЕЖЛИВОСТЬ
# =============================================================================


def test_a_due_source_is_fetched_at_most_once_per_tick():
    host = FakeHost()
    tick(host)
    assert len(host.fetched) == len(set(host.fetched))


def test_hourly_sources_are_not_fetched_on_every_five_minute_tick():
    host = seeded_host()
    retick(host)
    assert "lighter_lpp_doc" not in host.fetched
    assert "binance_delisting" in host.fetched

    host.fetched.clear()
    retick(host, advance_ms=HOUR_MS)
    assert "lighter_lpp_doc" in host.fetched


def test_a_source_is_due_again_a_minute_early_so_timer_jitter_never_skips_a_cycle():
    """Таймер systemd ходит с RandomizedDelaySec; строгое «прошло ли ровно 300 с»
    выкидывало бы каждый второй цикл источника."""
    st = State()
    st.sources["binance_delisting"] = ap.SourceState(seeded=True, last_checked_ms=NOW)
    assert [s.name for s in due_sources(st, NOW + 299_000)] != []
    assert "binance_delisting" in {s.name for s in due_sources(st, NOW + 299_000)}
    assert "binance_delisting" not in {s.name for s in due_sources(st, NOW + 120_000)}


def test_every_source_declares_a_floor_and_a_cadence():
    for s in SOURCES:
        assert s.cadence_s >= 300, s.name
        assert s.floor >= 1, s.name


# =============================================================================
# ХЭШ-ИСТОЧНИКИ
# =============================================================================


def test_volatile_markup_does_not_move_the_digest():
    """Замерено 2026-09-09: два ответа страницы Kraken за 5 минут отличались только
    каруселью соседних статей. Хэш сырых байт мигал бы каждый час навсегда."""
    assert text_digest(PAGE_HEAD + PAGE_TAIL_A) == text_digest(PAGE_HEAD + PAGE_TAIL_B)
    assert text_digest(PAGE_HEAD + PAGE_TAIL_A) != text_digest(
        PAGE_HEAD.replace(b"0.00%", b"0.01%") + PAGE_TAIL_A)
    # Тот же класс шума у markdown-документа Lighter — пробельная раскладка. На нём
    # держатся пины мигания ниже: «вернулось к прежнему» там именно этот случай.
    assert text_digest(lpp_doc()) == text_digest(lpp_doc(noise=b"\n\n   "))
    assert text_digest(lpp_doc()) != text_digest(lpp_doc("0.01%"))


def test_an_inline_script_body_does_not_move_the_digest():
    """ВЫВЕДЕНО, не измерено: build-id и прочая машинерия страниц живёт во встроенном
    JSON внутри <script>. На наших двух страницах это пока не наблюдалось — гвард
    стоит на будущее и стоит одну строку. Если он однажды съест настоящее изменение,
    признак будет в логе: подтверждённых изменений нет, а страница видимо другая."""
    a = PAGE_HEAD + b'<script>{"buildId":"aaa111"}</script>' + PAGE_TAIL_A
    b = PAGE_HEAD + b'<script>{"buildId":"bbb222"}</script>' + PAGE_TAIL_A
    assert text_digest(a) == text_digest(b)


def test_an_empty_page_body_is_a_failure_not_a_change():
    """Заглушка CDN («403 Forbidden») короче любой программы. Прочитанная как мир,
    она даёт ложный алерт об изменении правил и стирает настоящий хэш."""
    with pytest.raises(SourceError):
        text_digest(b"<html><body>403</body></html>")


def test_a_page_change_is_reported_only_after_it_is_confirmed():
    host = seeded_host()
    changed = lpp_doc("0.05%")
    retick(host, {"lighter_lpp_doc": changed}, advance_ms=HOUR_MS)
    assert host.telegrams == []  # первое наблюдение — ещё не факт

    retick(host, advance_ms=HOUR_MS)
    assert len(host.item_alerts()) == 1
    assert "lighter" in host.item_alerts()[0].lower()

    host.telegrams.clear()
    retick(host, advance_ms=HOUR_MS)
    assert host.telegrams == []


def test_a_one_off_flicker_never_becomes_an_alert():
    host = seeded_host()
    retick(host, {"lighter_lpp_doc": lpp_doc("9.99%")}, advance_ms=HOUR_MS)
    retick(host, {"lighter_lpp_doc": lpp_doc(noise=b"\n\n")}, advance_ms=HOUR_MS)
    retick(host, advance_ms=HOUR_MS)

    assert host.telegrams == []
    assert host.state().sources["lighter_lpp_doc"].pending_count == 0


def test_a_permanently_flapping_page_says_so_once_instead_of_storming():
    """Если нормализация всё же не справилась, прибор обязан сказать, что он сломан,
    а не молчать и не слать по алерту в час."""
    host = seeded_host()
    for i in range(UNSTABLE_AFTER + 4):
        retick(host, {"lighter_lpp_doc": lpp_doc(f"{i}.11%")}, advance_ms=HOUR_MS)

    assert len(host.telegrams) == 1
    assert "\U0001f534" in host.telegrams[0]
    assert "нестабил" in host.telegrams[0]


def test_the_lighter_programme_document_is_watched_by_content_not_by_headers():
    """Last-Modified у обеих страниц двигается сам по себе (замерено), поэтому
    источник истины — содержимое."""
    src = source_by_name("lighter_lpp_doc")
    assert src.kind == "hash"
    assert src.url.endswith(".md")



def test_a_page_that_alternates_between_two_known_digests_reports_each_once():
    """A/B-кэш CDN: страница стабильно отдаёт то A, то B. Каждый переход подтверждается
    вторым наблюдением, то есть проходит гвард мигания, — но это не новость, а текст,
    который уже объявляли. Ключ дайджеста лежит в seen, и сверка с ним отличает
    «страницу переписали» от «CDN качает нас между двумя копиями»."""
    host = seeded_host()
    page_a = lpp_doc()
    page_b = lpp_doc("0.05%")
    for page in (page_b, page_b, page_a, page_a, page_b, page_b, page_a, page_a):
        retick(host, {"lighter_lpp_doc": page}, advance_ms=HOUR_MS)

    assert len(host.item_alerts()) == 2, host.item_alerts()


def test_a_confirmed_page_change_that_was_deferred_does_not_advance_the_digest():
    """Дайджест — такой же носитель «это уже видели», как снимок вселенной: сдвинутый
    до доставки, он делает изменение правил невидимым навсегда."""
    host = seeded_host()
    flood = [dict(CMS_DELIST, id=900_000 + i, code=f"{i:032x}", title=f"n{i}",
                  releaseDate=1787500000000 + i) for i in range(MAX_ALERTS_PER_TICK + 2)]
    changed = lpp_doc("0.07%")
    retick(host, {"lighter_lpp_doc": changed}, advance_ms=HOUR_MS)         # заявка
    retick(host, {"lighter_lpp_doc": changed,
                  "binance_delisting": cms(*flood, catalog=161, name="Delisting")},
           advance_ms=HOUR_MS)                                             # подтверждение под завал

    st = host.state().sources["lighter_lpp_doc"]
    assert not any("страниц" in t for t in host.item_alerts())
    assert st.digest != text_digest(changed)

    host.telegrams.clear()
    retick(host, {"lighter_lpp_doc": changed}, advance_ms=HOUR_MS)
    assert any("страниц" in t for t in host.item_alerts())


# =============================================================================
# СОСТОЯНИЕ
# =============================================================================


def test_a_broken_state_file_reads_as_nothing_known_and_reseeds_silently():
    """Потеря состояния не имеет права стать 2700 сообщениями. Цена — пропуск тех
    анонсов, что пришли за время простоя; цена шторма — потеря доверия к каналу."""
    host = FakeHost(files={"/data/seen.json": "{ это не json"})
    tick(host)

    assert host.item_alerts() == []
    assert host.state().sources["binance_delisting"].seeded is True


def test_state_of_an_unknown_schema_is_dropped_whole():
    host = FakeHost(files={"/data/seen.json": json.dumps({"schema": 999, "sources": {}})})
    tick(host)
    assert host.item_alerts() == []
    assert host.state().schema == ap.SCHEMA


def test_seen_keys_are_bounded_and_drop_the_oldest_first():
    host = seeded_host()
    for batch in range(3):
        arts = [dict(CMS_DELIST, id=1_000_000 + batch * 50 + i, code=f"{batch:02x}{i:030x}",
                     title=f"Delist batch{batch} n{i}", releaseDate=1787500000000 + batch * 50 + i)
                for i in range(50)]
        for _ in range(6):  # сток пачки идёт по MAX_ALERTS_PER_TICK за тик
            retick(host, {"binance_delisting": cms(*arts, catalog=161, name="Delisting")})
    seen = host.state().sources["binance_delisting"].seen
    assert len(seen) == MAX_SEEN_KEYS  # 150 ключей пришло, 100 осталось
    assert seen[-1].endswith(str(1_000_000 + 2 * 50 + 49))


def test_the_seen_bound_covers_two_full_pages_of_every_feed():
    """Ключей помним вдвое больше страницы. Меньше — и засев обрезал бы собственное
    окно: часть статей, лежащих на той же странице, стала бы «новой» на первом же
    рабочем тике, то есть прибор начал бы с ложной тревоги."""
    assert MAX_SEEN_KEYS >= 2 * ap.PAGE_SIZE
    for s in SOURCES:
        if s.kind != "feed":
            continue
        # Размер страницы объявлен у КАЖДОЙ ленты, а не выведен из имени источника:
        # OKX отдаёт 20 записей и попросить другое число не даёт, RSS Kraken — 10.
        assert MAX_SEEN_KEYS >= 2 * s.page_size, s.name
        if s.page_size == ap.PAGE_SIZE:
            # Там, где размер выбираем мы сами, он обязан стоять в самом запросе.
            assert f"={ap.PAGE_SIZE}&" in s.url or s.url.endswith(f"={ap.PAGE_SIZE}"), s.name


def test_the_seed_keeps_the_newest_keys_if_a_page_ever_outgrows_the_bound():
    """Площадки отдают свежее ПЕРВЫМ. Обрезка засева по хвосту без сортировки сохранила
    бы самое старое и выбросила самое свежее — то есть ровно те статьи, ради которых
    прибор заведён."""
    arts = [dict(CMS_DELIST, id=i, code=f"{i:032x}", title=f"n{i}",
                 releaseDate=1787500000000 + i) for i in range(MAX_SEEN_KEYS + 3)]
    plan, new = plan_tick(
        {"binance_delisting": cms(*reversed(arts), catalog=161, name="Delisting")}, State(), NOW)

    seen = new.sources["binance_delisting"].seen
    assert len(seen) == MAX_SEEN_KEYS
    assert seen[-1] == f"binance_delisting:{MAX_SEEN_KEYS + 2}"
    assert "binance_delisting:0" not in seen


def test_a_universe_change_carries_the_dex_in_its_header_and_no_dead_link():
    """Ссылка у diff-источника была бы POST-эндпоинтом, который оператор не откроет;
    зато без имени dex-а «появился актив» неотличимо на четырёх вселенных HL."""
    host = seeded_host()
    retick(host, {"hl_dex_mkts": hl_meta(*many_coins(30, prefix="mkts:S"), coin("mkts:NEW"))})

    text = host.item_alerts()[0]
    assert text.startswith("\U0001f195 Hyperliquid mkts")
    assert "http" not in text


def test_the_state_write_is_atomic_and_fsynced(tmp_path, monkeypatch):
    fsynced: list[int] = []
    monkeypatch.setattr(ap.os, "fsync", lambda fd: fsynced.append(fd))
    host = RealHost(tmp_path / "seen.json")
    host.write_atomic(str(tmp_path / "seen.json"), "{}")

    assert (tmp_path / "seen.json").read_text() == "{}"
    assert not list(tmp_path.glob("*.tmp"))
    assert fsynced



def test_an_unreadable_state_file_says_so_before_reseeding():
    """Пересев после потери состояния молчал: в журнале «засев=13», в чате ничего, а
    окно между потерей и пересевом выброшено — анонсы, пришедшие в нём, не увидит
    никто и никогда, страница назад не отматывается. Это единственное, о чём прибор
    обязан сказать сам про себя."""
    host = FakeHost(files={"/data/seen.json": "{ это не json"})
    tick(host)

    assert len(host.telegrams) == 1
    assert "состояние" in host.telegrams[0].lower()
    assert ap.iso_ms(NOW) in host.telegrams[0]   # с какого момента окно потеряно
    assert host.item_alerts() == []          # мир пересеян молча, историей не сыплем
    assert host.state().sources["binance_delisting"].seeded is True


def test_a_first_run_with_no_state_at_all_stays_silent():
    """Обратная сторона: чистый старт — не потеря. Сообщение о потере на каждом первом
    запуске обесценило бы само сообщение."""
    host = FakeHost()
    tick(host)

    assert host.telegrams == []


# =============================================================================
# ПУЛЬС И СЕКРЕТЫ
# =============================================================================


def test_the_pulse_is_written_only_when_something_was_actually_read():
    """Тишина пульса — второй, независимый от Telegram канал тревоги. Пульс на тике,
    где не прочитано ничего, глушил бы внешний heartbeat при мёртвой сети."""
    host = seeded_host()
    retick(host)
    assert len(host.pulses) == 1

    dead = {s.name: TimeoutError("network down") for s in SOURCES}
    host.pulses.clear()
    retick(host, dead)
    assert host.pulses == []


def test_the_pulse_carries_the_local_clock_and_the_per_source_counters():
    host = seeded_host()
    retick(host)
    doc = json.loads(host.pulses[0])
    assert doc["iso"].endswith("Z")
    assert doc["ok"] >= 1 and doc["failed"] == 0


def test_the_token_never_reaches_the_state_the_pulse_or_the_log(monkeypatch, capsys):
    monkeypatch.setenv("TG_BOT_TOKEN", "123:SECRET-TOKEN")
    monkeypatch.setenv("TG_CHAT_ID", "-100500")
    host = seeded_host()
    retick(host, {"binance_delisting": cms(CMS_MARGIN, CMS_DELIST, catalog=161, name="Delisting")})

    blob = json.dumps(host.files) + " ".join(host.pulses) + " ".join(host.telegrams)
    assert "SECRET-TOKEN" not in blob


def test_a_token_with_whitespace_is_refused_instead_of_being_sent(monkeypatch):
    """Урок topic-scan: CRLF в env-файле уезжает в URL и даёт нечитаемую ошибку."""
    monkeypatch.setenv("TG_BOT_TOKEN", "123:SECRET\n")
    monkeypatch.setenv("TG_CHAT_ID", "-100500")
    sent: list = []
    host = RealHost("/data/seen.json", _open=lambda *a, **k: sent.append(a) or None)

    assert host.send_telegram("hi") is False
    assert sent == []


def test_a_telegram_outage_of_any_class_reports_false_rather_than_dying(monkeypatch):
    """Урок funding_poller: http.client.IncompleteRead не наследует OSError и
    вылетал наружу, роняя тик целиком."""
    monkeypatch.setenv("TG_BOT_TOKEN", "123:ok")
    monkeypatch.setenv("TG_CHAT_ID", "-100500")

    class Weird(Exception):
        pass

    def boom(*a, **k):
        raise Weird("token 123:ok leaked into the message")

    host = RealHost("/data/seen.json", _open=boom)
    assert host.send_telegram("hi") is False



def test_the_pulse_counts_what_telegram_refused():
    """Пульс писался по факту ЧТЕНИЯ и потому не различал «нечего слать» и «не смог
    послать»: сторож видел свежий файл, пока прибор не доставлял ни одного сообщения."""
    host = seeded_host(tg_ok=False)
    retick(host, {"binance_delisting": cms(CMS_MARGIN, CMS_DELIST, catalog=161, name="Delisting")})

    assert json.loads(host.pulses[-1])["undelivered"] == 1

    host.tg_ok = True
    retick(host)

    assert json.loads(host.pulses[-1])["undelivered"] == 0


def test_a_missing_telegram_credential_is_fatal_before_the_first_fetch(monkeypatch):
    """`EnvironmentFile=-` глотает пропавший env-файл, а `send → False` глотает всё
    остальное: тик отчитывался нулём, пульс был свежим, код выхода нулевым, ключи не
    метились — и прибор вечно ретраил в пустоту. Отказ обязан быть ДО тика и с
    ненулевым кодом: тогда его увидит OnFailure, у которого учётные данные свои."""
    monkeypatch.delenv("TG_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TG_CHAT_ID", raising=False)
    host = FakeHost()
    monkeypatch.setattr(ap, "RealHost", lambda *a, **k: host)

    rc = ap.main(["--once"])

    assert rc != 0
    assert host.fetched == []
    assert host.files == {}


def test_a_dry_run_needs_no_credentials(monkeypatch):
    """Предполётная проверка никого не будит и потому не обязана знать токен."""
    monkeypatch.delenv("TG_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TG_CHAT_ID", raising=False)
    host = FakeHost()
    monkeypatch.setattr(ap, "RealHost", lambda *a, **k: host)

    assert ap.main(["--once", "--dry-run"]) == 0
    assert host.fetched


def test_a_whitespace_token_is_fatal_too(monkeypatch):
    """Урок topic-scan: CRLF из env-файла уезжает в URL и даёт нечитаемую ошибку на
    каждой отправке. Это конфигурация, а не сбой сети, — значит место ей в коде
    выхода, а не в бесконечном ретрае."""
    monkeypatch.setenv("TG_CHAT_ID", "-100500")
    monkeypatch.setenv("TG_BOT_TOKEN", "123:abc\n")
    assert ap.credentials_problem(os.environ) is not None

    monkeypatch.setenv("TG_BOT_TOKEN", "123:abc")
    assert ap.credentials_problem(os.environ) is None


# =============================================================================
# ФОРМА ЖИВЫХ ОТВЕТОВ (адаптеры против дословных записей площадок)
# =============================================================================


def test_every_adapter_parses_its_live_shape():
    """По фикстуре на источник: запись каждой площадки взята дословно из живого
    ответа 2026-09-09/10."""
    state = State()
    plan, new = plan_tick({s.name: DEFAULT_PAYLOADS[s.name] for s in SOURCES}, state, NOW)

    assert plan.messages == []
    for s in SOURCES:
        assert new.sources[s.name].seeded is True, s.name
        assert new.sources[s.name].fails == 0, s.name


def test_the_binance_link_points_at_the_article_the_operator_can_open():
    host = seeded_host()
    retick(host, {"binance_delisting": cms(CMS_MARGIN, CMS_DELIST, catalog=161, name="Delisting")})
    assert CMS_MARGIN["code"] in host.item_alerts()[0]
    assert "binance.com" in host.item_alerts()[0]


def test_the_bybit_key_survives_a_locale_prefix_change():
    """Ключ — id статьи из слага (--art…), а не URL целиком: смена локали в ссылке
    не имеет права выглядеть новым делистингом."""
    host = seeded_host()
    moved = dict(BYBIT_DELIST, url=BYBIT_DELIST["url"].replace("en-US", "ru-RU"))
    retick(host, {"bybit_delistings": bybit(moved)})

    assert host.telegrams == []


def test_the_kraken_item_is_keyed_by_guid_and_carries_its_category():
    """Ключ статьи блога — `guid`, а не ссылка и не заголовок. Формат этот пин держал и
    пока источник был снят (1.2): площадка отказывала на своей границе, а не меняла RSS,
    и возврат в 1.3 не потребовал в адаптере ни строчки."""
    fresh = dict(KRAKEN_LISTING, guid="https://blog.kraken.com/?p=99999",
                 title="XYZ is available for trading!", link="https://blog.kraken.com/x")
    item = ap.parse_rss(rss(fresh, KRAKEN_LISTING, KRAKEN_PROMO),
                        source_by_name("kraken_blog"))[0]

    assert item.key == "kraken_blog:https://blog.kraken.com/?p=99999"
    assert item.title == "XYZ is available for trading!"
    assert "Asset Listings" in item.note
    assert item.venue_clock == "kraken pubDate"


def test_the_kraken_feed_is_not_filtered_by_an_unmeasured_category():
    """Имя категории делистинга у Kraken мы никогда не наблюдали. Фильтр по списку
    известных категорий выбросил бы ровно то, ради чего источник заведён."""
    odd = dict(KRAKEN_LISTING, guid="https://blog.kraken.com/?p=1", cats=["Something New"],
               title="Kraken will remove FOO from trading")
    items = ap.parse_rss(rss(odd, KRAKEN_LISTING, KRAKEN_PROMO),
                         source_by_name("kraken_blog"))

    assert "FOO" in items[0].title


# Живьём 2026-09-10: 12 из 50 записей `delistings` несут id Contentstack `-blt<hex>`
# (одна чёрточка), одна — `--blt<hex>` (две). Запись взята дословно.
BYBIT_CONTENTSTACK = {
    "title": "Delisting of IPUSDT Perpetual Contract",
    "description": "",
    "type": {"key": "delistings", "title": "Delistings"},
    "tags": ["Derivatives"],
    "url": "https://announcements.bybit.com/en-US/article/delisting-of-ipusdt-perpetual-contract-blt6ded6acc1447f3ec/",
    "dateTimestamp": 1782000000000, "publishTime": 1782000060000,
}


def test_a_contentstack_keyed_bybit_article_is_not_keyed_by_its_whole_url():
    """Регекс на две чёрточки эти 12 записей не ловил: ключом становился URL целиком,
    вместе с сегментом локали, — то есть ровно то, от чего ключ и защищает."""
    item = ap.parse_bybit_announcements(bybit(BYBIT_CONTENTSTACK),
                                        source_by_name("bybit_delistings"))[0]

    assert item.key == "bybit_delistings:blt6ded6acc1447f3ec"


def test_both_punctuations_of_the_same_contentstack_id_give_one_key():
    """Живьём встречаются обе пунктуации, `-blt…` и `--blt…`. Ключ обязан быть один."""
    double = dict(BYBIT_CONTENTSTACK,
                  url=BYBIT_CONTENTSTACK["url"].replace("-blt", "--blt"))
    single_key = ap.parse_bybit_announcements(
        bybit(BYBIT_CONTENTSTACK), source_by_name("bybit_delistings"))[0].key
    double_key = ap.parse_bybit_announcements(
        bybit(double), source_by_name("bybit_delistings"))[0].key

    assert single_key == double_key


def test_a_bybit_url_without_any_id_still_drops_the_locale():
    """Фолбэк: если id из ссылки не выделяется, ключом остаётся URL — но БЕЗ сегмента
    локали, иначе en-US → ru-RU на той же статье это второй алерт о том же событии."""
    def key(locale: str) -> str:
        url = f"https://announcements.bybit.com/{locale}/article/no-id-in-this-slug"
        return ap.parse_bybit_announcements(bybit(dict(BYBIT_DELIST, url=url)),
                                            source_by_name("bybit_delistings"))[0].key

    assert key("en-US") == key("ru-RU")
    assert "en-US" not in key("en-US")


# =============================================================================
# OKX
# =============================================================================
# Площадка добавлена 2026-09-13. Три источника: делистинги, листинги и «последние»
# с фильтром по ключевым словам. Форма ответа измерена в тот же день:
# `{"code":"0","msg":"","data":[{"details":[...20 записей...],"totalPage":N}]}`,
# у записи ровно пять полей, `pTime` — СТРОКА миллисекунд, ссылка несёт сегмент
# локали (`/en-eu/help/...`), которая зависит от того, из какой страны мы спросили.


def test_the_okx_adapter_reads_its_live_shape_field_by_field():
    """Пин формы: пять полей живой записи → Item. `pTime` приходит СТРОКОЙ, и прочитать
    его как число обязан адаптер: иначе метка площадки потеряется вся."""
    src = source_by_name("okx_delistings")
    items = ap.parse_okx_announcements(okx(OKX_MIGRATION, OKX_DELIST, total_page=2), src)

    assert [i.title for i in items] == [OKX_MIGRATION["title"], OKX_DELIST["title"]]
    assert items[0].url == OKX_MIGRATION["url"]
    assert items[0].venue_ts_ms == 1784023216123
    assert items[0].venue_clock == "okx pTime"
    assert items[0].key == "okx_delistings:okx-to-support-aergo-crypto-migration"


def test_an_okx_announcement_is_keyed_by_its_slug_not_by_its_title():
    """Измерено 2026-09-13: на 60 записях три пары ПОВТОРЯЮЩИХСЯ заголовков и ни одного
    повторяющегося слага — повторный анонс получает суффикс (`-1`, `-20260413`).
    Ключ по заголовку проглотил бы вторую правку тик-сайза целиком."""
    host = seeded_host({"okx_latest": okx(OKX_TICK_MAR, total_page=103)})
    retick(host, {"okx_latest": okx(OKX_TICK_APR, OKX_TICK_MAR, total_page=103)})

    assert len(host.item_alerts()) == 1
    assert "-20260413" in host.item_alerts()[0]


def test_an_okx_locale_prefix_change_is_not_a_new_announcement():
    """Сегмент локали в ссылке зависит от того, откуда мы спросили (живьём с кипрского
    адреса площадка отдаёт `/en-eu/`). Смена сегмента не имеет права выглядеть новым
    делистингом — ровно урок ключа Bybit."""
    host = seeded_host()
    moved = dict(OKX_MIGRATION, url=OKX_MIGRATION["url"].replace("/en-eu/", "/en-us/"))
    retick(host, {"okx_delistings": okx(moved, OKX_DELIST, total_page=2)})

    assert host.telegrams == []


def test_the_first_okx_pass_seeds_silently():
    """Новый источник засевается сам по себе: 18 страниц истории листингов в чат —
    это не запуск, а инцидент. Пин на ТРЁХ источниках сразу, потому что добавляются
    они одной правкой и промолчать обязаны все три."""
    host = seeded_host()
    st = host.state()
    for name in ("okx_delistings", "okx_new_listings", "okx_latest"):
        del st.sources[name]
    host.files[host.state_path] = dump_state(st)

    retick(host)

    assert host.telegrams == []
    for name in ("okx_delistings", "okx_new_listings", "okx_latest"):
        assert host.state().sources[name].seeded is True, name
    assert host.state().sources["okx_delistings"].seen  # мир запомнен, а не потерян


def test_a_new_okx_announcement_carries_its_title_link_and_venue_clock():
    host = seeded_host()
    fresh = dict(OKX_LIST, title="OKX to list FOO/USDT for spot trading",
                 url="https://www.okx.com/en-eu/help/okx-to-list-foo-usdt",
                 pTime="1787990000000")
    retick(host, {"okx_new_listings": okx(fresh, OKX_LIST, total_page=18)})

    text = host.item_alerts()[0]
    assert "OKX to list FOO/USDT for spot trading" in text
    assert "https://www.okx.com/en-eu/help/okx-to-list-foo-usdt" in text
    # Часы площадки — со своим именем поля; наши — отдельной строкой и другим временем.
    assert "площадка (okx pTime): 2026-08-29T07:53:20Z" in text
    assert "локально 2026-08-29T10:55:00Z" in text


def test_an_okx_error_envelope_is_a_failure_not_an_empty_world():
    """200 с `code != "0"` — конверт валиден, содержимого нет (живьём так отвечает
    запрос с неизвестным annType: `51000 Parameter annType error`). Прочитанный как мир,
    он означал бы «делистингов больше нет»."""
    host = seeded_host()
    body = json.dumps({"code": "51000", "msg": "Parameter annType error", "data": []}).encode()
    retick(host, {"okx_delistings": body})

    st = host.state().sources["okx_delistings"]
    assert st.fails == 1 and "51000" in st.last_error
    assert host.item_alerts() == []
    assert host.blobs["/data/last_fail/okx_delistings.bin"] == body


def test_an_empty_okx_page_is_a_failure_not_an_empty_world():
    host = seeded_host()
    retick(host, {"okx_new_listings": okx(total_page=18)})

    assert host.state().sources["okx_new_listings"].fails == 1


def test_an_okx_page_of_another_type_reads_as_a_failure():
    """У каждой записи есть `annType`, и это единственная защита от «200 с чужим
    содержимым»: перевешенный тип отдал бы правдоподобную ленту не про делистинги."""
    host = seeded_host()
    retick(host, {"okx_delistings": okx(OKX_LIST, total_page=18)})

    st = host.state().sources["okx_delistings"]
    assert st.fails == 1 and "announcements-new-listings" in st.last_error
    assert host.item_alerts() == []


def test_an_okx_timestamp_that_is_not_a_number_is_a_failure():
    """`pTime` — строка миллисекунд. Нечисловая метка читается как провал источника, а
    не как «метки нет»: тихо потерянные часы площадки и есть то, ради чего прибор
    заведён (урок «одни часы перед сопоставлением»)."""
    host = seeded_host()
    retick(host, {"okx_delistings": okx(dict(OKX_MIGRATION, pTime="soon"), total_page=2)})

    st = host.state().sources["okx_delistings"]
    assert st.fails == 1
    # Причина обязана НАЗЫВАТЬ поле: без явной проверки `int()` падает сам, источник
    # тоже проваливается — но оператор получает «invalid literal for int()» и идёт
    # читать сайдкар вместо того, чтобы прочитать причину (поймано мутацией).
    assert "pTime" in st.last_error


def test_okx_failures_alert_on_the_same_threshold_as_every_other_source():
    """Никакого своего порога у новой площадки нет: тот же счётчик, тот же алерт,
    тот же пульс."""
    host = seeded_host()
    for _ in range(FAIL_ALERT_AFTER - 1):
        retick(host, {"okx_latest": TimeoutError("boom")})
    assert host.telegrams == []

    retick(host, {"okx_latest": TimeoutError("boom")})

    assert len(host.telegrams) == 1
    assert host.telegrams[0].startswith("\U0001f534") and "okx_latest" in host.telegrams[0]
    assert json.loads(host.pulses[-1])["failed"] == 1


def test_the_okx_latest_feed_reports_only_what_matches_the_keywords():
    """Лента «последних» — это все десять типов сразу, и 87 % её записей нам не про что
    (кампании, розыгрыши, обслуживание кошельков). Измерено на 160 записях: ключевым
    словам отвечает 21, и это ровно решения площадки — тик-сайз перпов, тиры позиций,
    формула фандинга."""
    # Засев ТОЛЬКО совпавшей записью: подай сюда и несовпавшие — и они уедут в `seen`,
    # после чего пин пройдёт даже со снятым фильтром (поймано мутацией).
    host = seeded_host({"okx_latest": okx(OKX_TICK_MAR, total_page=103)})
    retick(host, {"okx_latest": okx(OKX_TICK_APR, OKX_MCLAREN, OKX_EXPIRY, OKX_TICK_MAR,
                                    total_page=103)})

    assert len(host.item_alerts()) == 1
    assert "tick size for perpetual futures" in host.item_alerts()[0]
    assert "McLaren" not in "".join(host.telegrams)
    assert "Weekly Expiry" not in "".join(host.telegrams)


def test_a_quiet_okx_latest_page_is_not_a_source_failure():
    """Порог правдоподобия считается по СЫРОЙ странице, а не по тому, что осталось после
    фильтра: страница из двадцати записей без единого совпадения — это норма (чаще
    всего именно так), а провал источника поднял бы алерт на ровном месте."""
    host = seeded_host({"okx_latest": okx(OKX_TICK_MAR, total_page=103)})
    quiet = [dict(OKX_MCLAREN, url=f"https://www.okx.com/en-eu/help/campaign-{i}")
             for i in range(20)]
    retick(host, {"okx_latest": okx(*quiet, total_page=103)})

    st = host.state().sources["okx_latest"]
    assert st.fails == 0 and st.last_error == ""
    assert host.item_alerts() == []
    assert json.loads(host.pulses[-1])["failed"] == 0


def test_the_okx_latest_feed_does_not_repeat_what_a_dedicated_source_carries():
    """Делистинги и листинги лежат и в «последних» тоже. Без вычета оператор получал бы
    два сообщения об одном решении: у источников разные имена, значит и ключи разные."""
    host = seeded_host()
    fresh = dict(OKX_DELIST, url="https://www.okx.com/en-eu/help/okx-to-delist-foo")
    retick(host, {"okx_latest": okx(fresh, total_page=103)})

    assert host.item_alerts() == []


def test_every_okx_source_declares_the_page_size_the_venue_fixes():
    """OKX отдаёт двадцать записей на страницу и попросить другое число не даёт. Окно
    `seen` обязано накрывать две такие страницы — иначе засев обрежет собственное окно,
    и часть той же страницы станет «новой» на первом же рабочем тике."""
    for name in ("okx_delistings", "okx_new_listings", "okx_latest"):
        s = source_by_name(name)
        assert s.page_size == 20, name
        assert s.cadence_s == ap.CADENCE_API_S, name
        assert MAX_SEEN_KEYS >= 2 * s.page_size, name


# =============================================================================
# KRAKEN: ВСЕЛЕННАЯ ПАР ВМЕСТО БЛОГА
# =============================================================================


def test_kraken_is_watched_by_the_blog_the_lp_page_and_the_pair_universe():
    """ВЕРНУЛИСЬ в 1.3: `kraken_blog` и `kraken_lp_page`. Блокировка blog.kraken.com
    оказалась ИНТЕРМИТТЕНТНОЙ, а не постоянной, и это измерение, а не надежда:
    2026-09-14 прод-поллер 1.1 прочитал фид в 19:10:10Z (источников=14, провалов=0,
    `kraken_blog` fails=0), а зонд из 20 запросов раз в минуту в 19:36:45–19:55:49Z
    получил 20 из 20 челленджей — одно и то же тело в 6460 байт; токийский хост в
    19:36:40Z и 20:04:06Z ответил тем же. Значит открытые окна реже, чем одно на
    20 минут, этим зондом не характеризуются — но существуют.

    Блог держим ради УПРЕЖДЕНИЯ: когда он проходит, он анонсирует листинги за ДНИ до
    включения, и другого такого входа к этой площадке нет. `kraken_pairs` — структурный
    фолбэк, который от блога не зависит вовсе: он видит включение и смену статуса
    (online → cancel_only …), то есть факт вместо упреждения. Одно не заменяет другое,
    поэтому в реестре стоят оба, а не «то или это».
    """
    names = {s.name for s in SOURCES}
    assert {"kraken_blog", "kraken_lp_page", "kraken_pairs"} <= names

    blog = source_by_name("kraken_blog")
    assert (blog.venue, blog.kind, blog.parse) == ("Kraken", "feed", ap.parse_rss)
    assert blog.url == "https://blog.kraken.com/feed"
    assert blog.cadence_s == ap.CADENCE_FEED_S

    page = source_by_name("kraken_lp_page")
    assert (page.venue, page.kind, page.parse) == ("Kraken", "hash", ap.text_digest)
    assert page.url.startswith("https://blog.kraken.com/product/promotions/")
    assert page.cadence_s == ap.CADENCE_HASH_S

    src = source_by_name("kraken_pairs")
    assert (src.venue, src.kind, src.label, src.entity) == ("Kraken", "diff", "вселенная", "пара")
    assert src.url == "https://api.kraken.com/0/public/AssetPairs"
    assert src.cadence_s == ap.CADENCE_API_S


def test_the_kraken_pair_universe_reads_its_live_shape_field_by_field():
    snap = ap.parse_kraken_assetpairs(kraken_envelope(kraken_world()),
                                      source_by_name("kraken_pairs"))

    assert len(snap) == 602
    xbt = snap["XXBTZUSD"]
    assert (xbt["altname"], xbt["wsname"]) == ("XBTUSD", "XBT/USD")
    assert (xbt["base"], xbt["quote"], xbt["status"]) == ("XXBT", "ZUSD", "online")
    assert "XBT/USD" in xbt["label"] and "XXBTZUSD" in xbt["label"]
    # Компактность — не косметика: снимок лежит в состоянии, которое fsync-ается
    # каждые пять минут. Живой ответ площадки — 673 КБ на 1449 пар; сохранённый как
    # есть, он раздул бы seen.json на порядок ради полей, которые никто не читает.
    assert set(xbt) == {"label", "altname", "wsname", "base", "quote", "status"}


def test_a_kraken_error_envelope_is_a_failure_not_an_empty_world():
    """200 у этой площадки не значит ничего: неизвестную пару она отдаёт кодом 200 с
    непустым `error` и БЕЗ `result` (измерено 2026-09-14:
    {"error":["EQuery:Unknown asset pair"]}). Прочитанный как мир, такой ответ — это
    «делистнули все 1449 пар», алерт на каждую и ложный снимок следом."""
    src = source_by_name("kraken_pairs")
    with pytest.raises(SourceError):
        ap.parse_kraken_assetpairs(b'{"error":["EQuery:Unknown asset pair"]}', src)
    with pytest.raises(SourceError):
        # Непустой `error` при правдоподобном `result` — тоже провал: конверт говорит,
        # что площадка запрос не выполнила.
        ap.parse_kraken_assetpairs(kraken_envelope(kraken_world(), error=["EService:Busy"]), src)
    with pytest.raises(SourceError):
        ap.parse_kraken_assetpairs(kraken_envelope([]), src)


def test_a_truncated_kraken_universe_is_refused_before_it_can_become_a_snapshot():
    """Пол 500 при 1449 живых парах. Усечённый ответ, ставший снимком, — это ложный
    делистинг тысячи пар на следующей же проверке."""
    src = source_by_name("kraken_pairs")
    assert src.floor == 500
    with pytest.raises(SourceError):
        ap.parse_kraken_assetpairs(kraken_envelope(kraken_world(n=100)), src)


def test_a_new_kraken_pair_is_reported_once_and_only_once():
    host = seeded_host()
    world = kraken_world({"NEWXEUR": kpair("NEWX")})
    retick(host, {"kraken_pairs": kraken_envelope(world)})

    assert len(host.item_alerts()) == 1
    assert "NEWX/EUR" in host.item_alerts()[0]

    host.telegrams.clear()
    retick(host, {"kraken_pairs": kraken_envelope(world)})
    assert host.telegrams == []


def test_a_kraken_pair_that_disappears_is_not_reported():
    """Делистинг у Kraken начинается со смены статуса (пин ниже), и она едет по-прежнему;
    исчезновение пары — уже его конец и в Telegram не идёт (2026-10-06)."""
    host = seeded_host()
    world = kraken_world()
    del world["ACAEUR"]
    retick(host, {"kraken_pairs": kraken_envelope(world)})

    assert host.item_alerts() == []
    assert host.state().sources["kraken_pairs"].fails == 0


def test_a_kraken_pair_turning_cancel_only_is_reported_as_one_status_change():
    """ТАК у Kraken начинается делистинг: пара не исчезает, у неё меняется статус
    (живьём 2026-09-14 — 79 пар в cancel_only и 17 в post_only против 1353 online).
    Источник, который видит только появление и исчезновение ключей, проспал бы
    начало и сказал бы что-нибудь только в день, когда пару уже снесли.

    Один переход обязан давать РОВНО одно сообщение, и оно называет обе стороны:
    «что-то изменилось» отправляет оператора смотреть руками. Механика — уже
    существующий `diff_snapshots`: он сравнивает значения ПОЛЕ ЗА ПОЛЕМ у общих
    ключей (тем же ловится `isDelisted` у HL и `status` у Lighter), поэтому статус
    достаточно положить в значение снимка."""
    for status in ("cancel_only", "post_only", "reduce_only"):
        host = seeded_host()
        world = kraken_world({"XXBTZUSD": dict(KRAKEN_XBTUSD, status=status)})
        retick(host, {"kraken_pairs": kraken_envelope(world)})

        alerts = host.item_alerts()
        assert len(alerts) == 1, (status, alerts)
        assert "XXBTZUSD" in alerts[0] and "status" in alerts[0], alerts[0]
        assert "online" in alerts[0] and status in alerts[0], alerts[0]


def test_the_preserved_window_of_the_blog_is_reused_when_the_source_returns():
    """Запись снятого источника в `seen.json` не вычищалась намеренно — вот ради чего.

    В проде на момент реза 1.2 запись `kraken_blog` стояла ЗДОРОВОЙ: fails=0,
    alerted=False, last_checked 19:10:10Z — прод-поллер 1.1 успел прочитать фид за
    пять минут до реза, и счётчик обнулился. (Прежняя редакция этого пина описывала её
    как fails=6/alerted=True: так было ЧАСОМ РАНЬШЕ, до последнего удачного чтения, и
    фикстура пинила состояние, которого в момент реза уже не существовало.)

    Вернувшийся источник обязан подобрать это окно и промолчать о том, что в нём уже
    лежит. Пересев молчал бы тоже, но выбросил бы окно; прочитанная как новая история —
    это пачка анонсов недельной давности в чат. И зелёной строки быть не должно:
    красной не было.
    """
    kept = State()
    kept.sources["kraken_blog"] = ap.SourceState(
        seeded=True, shape="1", last_checked_ms=NOW - HOUR_MS, fails=0, alerted=False,
        seen=[f"kraken_blog:{KRAKEN_PROMO['guid']}", f"kraken_blog:{KRAKEN_LISTING['guid']}"])
    fresh = dict(KRAKEN_LISTING, guid="https://blog.kraken.com/?p=36600",
                 title="ZZZ is available for trading!",
                 link="https://blog.kraken.com/product/asset-listings/zzz")
    host = FakeHost({"kraken_blog": rss(fresh, KRAKEN_LISTING, KRAKEN_PROMO)},
                    files={"/data/seen.json": dump_state(kept)})

    tick(host)

    assert host.item_alerts() == host.telegrams          # ни красных, ни зелёных строк
    assert len(host.telegrams) == 1, host.telegrams
    assert "ZZZ is available" in host.telegrams[0]
    assert host.state().sources["kraken_blog"].fails == 0


def test_a_state_entry_of_a_source_no_longer_registered_is_ignored_harmlessly():
    """Обратная сторона той же монеты: снятый когда-то источник оставляет в состоянии
    запись, и тик обязан её просто не заметить — ни исключения, ни сообщения, ни
    попытки сходить по её адресу. Чистить её тоже нельзя: она несёт окно `seen`, стоит
    килобайт, и ровно из такой записи источник возвращается без переобъявления."""
    stale = State()
    stale.sources["kraken_support_zendesk"] = ap.SourceState(
        seeded=True, last_checked_ms=NOW - HOUR_MS, fails=6, alerted=True,
        last_error="HTTPError: HTTP Error 404: Not Found",
        seen=["kraken_support_zendesk:42"])
    host = FakeHost(files={"/data/seen.json": dump_state(stale)})

    tick(host)

    assert host.telegrams == []
    assert "kraken_support_zendesk" not in host.fetched
    kept = host.state().sources["kraken_support_zendesk"]
    assert kept.fails == 6 and kept.seen == ["kraken_support_zendesk:42"]


# =============================================================================
# БОТ-ЗАЩИТА
# =============================================================================


def test_a_js_challenge_is_named_as_bot_protection_not_as_a_bare_http_error():
    """`HTTPError: HTTP Error 403: Forbidden` неотличим от «площадка легла», а решения
    это требует разного: бот-защиту не лечит ни ретрай, ни ожидание. Так и выглядел
    блог Kraken перед резом 1.2: провалы подряд, алерт, и ни слова о том, что именно
    случилось. Тело ошибки есть только у самого исключения — сайдкар пишется лишь для
    ответов, дочитанных до байтов.

    Причина обязана доехать до СОСТОЯНИЯ, а не только до текста сообщения: именно по
    ней порог провалов выбирается ниже."""
    assert "JS challenge" in ap.failure_reason(http_error())
    assert CHALLENGE_REASON in ap.failure_reason(http_error())

    host = seeded_host()
    retick(host, {"kraken_blog": http_error()})

    assert host.telegrams == []          # один челлендж оператора не будит
    assert CHALLENGE_REASON in host.state().sources["kraken_blog"].last_error


def test_the_gzipped_challenge_body_of_the_live_venue_is_still_named_as_bot_protection():
    """ЖИВОЕ ИЗМЕРЕНИЕ 2026-09-14 20:22Z: blog.kraken.com отдаёт челлендж СЖАТЫМ —
    `Content-Encoding: gzip`, 3598 байт на проводе против 6877 распакованных. urllib
    распаковывает только УСПЕШНЫЕ ответы (`RealHost.fetch` делает это сам, по тому же
    заголовку), а тело HTTPError остаётся сжатым, и маркеры в нём не ищутся.

    Поэтому метка, заведённая в 1.2, в проде не срабатывала НИ РАЗУ: в состоянии у
    `kraken_lp_page` так и лежало `HTTPError: HTTP Error 403: Forbidden` — ровно тот
    текст, от которого метка заводилась. А без метки порог для бот-защиты не
    выбирается, то есть вся эта половина 1.3 была бы инертна и молчала бы об этом."""
    assert CHALLENGE_REASON in ap.failure_reason(http_error(gzipped=True))
    # Обратная сторона: распаковка не имеет права превращать в челлендж что попало.
    assert CHALLENGE_REASON not in ap.failure_reason(
        http_error(502, b"<html><body>502 Bad Gateway</body></html>", gzipped=True))

    host = seeded_host()
    retick(host, {"kraken_blog": http_error(gzipped=True)})

    assert CHALLENGE_REASON in host.state().sources["kraken_blog"].last_error
    assert host.state().sources["kraken_blog"].challenge_fails == 1


def test_an_ordinary_http_error_keeps_its_bare_text():
    """Обратная сторона: 500 или 429 — это площадка, а не челлендж, и подменять их
    текст догадкой нельзя."""
    reason = ap.failure_reason(http_error(500, b"<html><body>502 Bad Gateway</body></html>"))

    assert "JS challenge" not in reason
    assert reason.startswith("HTTPError:")
    assert "403" not in reason and "500" in reason or "HTTP Error 500" in reason
    assert ap.failure_reason(TimeoutError("boom")) == "TimeoutError: boom"


def test_the_release_version_is_declared_in_the_file_and_travels_in_the_user_agent():
    """Версия релиза живёт в имени файла (`announce_poller-<версия>-<sha8>.py`). До 1.2
    она была ТОЛЬКО там, а User-Agent отставал на два релиза и врал площадкам о том,
    кто их спрашивает."""
    assert ap.VERSION == "1.5"
    assert ap.USER_AGENT == f"hft-announce-poller/{ap.VERSION}"


# =============================================================================
# ПОРОГ ДЛЯ БОТ-ЗАЩИТЫ
# =============================================================================
# Челлендж блога Kraken ИНТЕРМИТТЕНТНЫЙ — измерения в пине реестра выше. Шесть
# провалов подряд у источника с каденцией 15 минут — это всего полтора часа, а
# замеренная блокировка длилась дольше: 20 из 20 запросов раз в минуту за 19 минут
# подряд, и единственное наблюдённое открытое окно — одно успешное чтение в 19:10:10Z.
# Красный алерт на полутора часах кричал бы о НОРМЕ этого источника, а канал, в котором
# кричат о норме, перестают читать — и тогда теряется настоящий делистинг.


def test_six_challenge_failures_stay_silent_because_the_block_is_intermittent():
    """Прежний порог (шесть) для челленджа больше не работает. Счётчик при этом идёт:
    молчание — это решение, а не незамеченный провал."""
    host = seeded_host()
    for _ in range(FAIL_ALERT_AFTER):
        # Каждый тик — СВОЁ исключение: тело HTTPError читается ровно один раз.
        retick(host, {"kraken_blog": http_error()})

    assert host.telegrams == [], host.telegrams
    assert host.state().sources["kraken_blog"].challenge_fails == FAIL_ALERT_AFTER
    assert host.state().sources["kraken_blog"].fails == FAIL_ALERT_AFTER


def test_a_challenge_lasting_two_hours_alerts_once_and_names_the_bot_protection():
    """Арифметика порога, и она обязана держаться в тесте, а не только в комментарии:
    8 провалов × 900 с каденции блога = 2 часа (у часовой страницы LP те же 8 — это
    8 часов). Восемь — МИНИМУМ, дающий два часа на самом быстром источнике, который
    челлендж вообще отдаёт."""
    blog = source_by_name("kraken_blog")
    assert FAIL_ALERT_AFTER_CHALLENGE * blog.cadence_s >= 2 * 3600
    assert FAIL_ALERT_AFTER_CHALLENGE > FAIL_ALERT_AFTER

    host = seeded_host()
    for _ in range(FAIL_ALERT_AFTER_CHALLENGE):
        retick(host, {"kraken_blog": http_error()})

    assert len(host.telegrams) == 1, host.telegrams
    assert host.telegrams[0].startswith("\U0001f534")
    assert "kraken_blog" in host.telegrams[0]
    assert CHALLENGE_REASON in host.telegrams[0]

    # Ритм напоминаний — прежний: пока провал длится, напоминание, а не шторм.
    host.telegrams.clear()
    for _ in range(FAIL_REALERT_EVERY - 1):
        retick(host, {"kraken_blog": http_error()})
    assert host.telegrams == []
    retick(host, {"kraken_blog": http_error()})
    assert len(host.telegrams) == 1


def test_an_ordinary_403_keeps_the_old_threshold():
    """Обратная сторона: поднять порог для ВСЕХ провалов значило бы купить блогу
    тишину ценой того, что настоящая поломка любого источника молчит два часа вместо
    получаса. Класс провала определяется телом ответа, а не его кодом: 403 без
    челленджа — обычный отказ."""
    host = seeded_host()
    plain = b"<html><head><title>403 Forbidden</title></head><body>nginx</body></html>"
    for _ in range(FAIL_ALERT_AFTER):
        retick(host, {"kraken_blog": http_error(403, plain)})

    assert len(host.telegrams) == 1, host.telegrams
    assert CHALLENGE_REASON not in host.telegrams[0]
    assert host.state().sources["kraken_blog"].challenge_fails == 0


def test_an_ordinary_failure_after_a_challenge_streak_counts_on_the_ordinary_scale():
    """Смешанная серия: пять челленджей, потом настоящий отказ. Шкала обычная, потому
    что считается она по ОБЩЕЙ серии провалов, и шестой провал подряд обязан позвать
    оператора, как звал всегда. Иначе бот-защита, случившаяся перед настоящей
    поломкой, дарила бы этой поломке два часа тишины."""
    host = seeded_host()
    for _ in range(FAIL_ALERT_AFTER - 1):
        retick(host, {"kraken_blog": http_error()})
    assert host.telegrams == []

    retick(host, {"kraken_blog": TimeoutError("boom")})

    assert len(host.telegrams) == 1, host.telegrams
    assert "TimeoutError" in host.telegrams[0]
    assert CHALLENGE_REASON not in host.telegrams[0]
    assert host.state().sources["kraken_blog"].challenge_fails == 0


def test_a_source_that_recovers_from_a_challenge_names_it_in_the_green_line():
    """Красная строка сказала «бот-защита» — зелёная обязана сказать, что снялась
    именно она, а не «что-то починилось само». Челлендж интермиттентный, он будет
    сниматься и вставать сам, и отличить это в канале можно только по тексту."""
    host = seeded_host()
    for _ in range(FAIL_ALERT_AFTER_CHALLENGE):
        retick(host, {"kraken_blog": http_error()})
    host.telegrams.clear()

    retick(host, {"kraken_blog": DEFAULT_PAYLOADS["kraken_blog"]})

    assert len(host.telegrams) == 1, host.telegrams
    assert host.telegrams[0].startswith("\U0001f7e2")
    assert CHALLENGE_REASON in host.telegrams[0]
    assert host.state().sources["kraken_blog"].challenge_fails == 0
    assert host.state().sources["kraken_blog"].fails == 0


def test_state_written_before_the_challenge_counter_existed_still_loads():
    """Первый тик новой версии читает состояние, написанное ПРЕДЫДУЩЕЙ: поля
    `challenge_fails` в нём нет. Запись обязана прочитаться со значением по умолчанию,
    а не отброситься как чужая схема — отброшенное состояние пересеивает мир и теряет
    окно `seen` всех источников разом. Форма взята из живого seen.json 1.2."""
    raw = json.dumps({"schema": 1, "sources": {"kraken_blog": {
        "seeded": True, "shape": "1", "last_checked_ms": NOW - HOUR_MS, "fails": 0,
        "alerted": False, "fail_alert_pending": False, "last_error": "",
        "seen": ["kraken_blog:https://blog.kraken.com/?p=36518"], "snapshot": None,
        "digest": None, "pending_digest": None, "pending_count": 0,
        "unstable_alerted": False}}})

    st = load_state(raw)

    assert st.lost is False
    ss = st.sources["kraken_blog"]
    assert ss.challenge_fails == 0
    assert ss.seeded is True
    assert ss.seen == ["kraken_blog:https://blog.kraken.com/?p=36518"]


# =============================================================================
# 1.4: РЕЕСТР BUILDER-DEX HYPERLIQUID
# =============================================================================
# Вселенная dex ловит изменение ВНУТРИ известного dex. Появление НОВОГО dex она не
# видит вовсе: у него нет источника, пока его не завели руками, — а именно новый dex
# и есть решение площадки, которое стоит денег (io/xyz живьём торгуют пре-IPO и
# акции). Реестр — единственное место, где он объявляется сам.


def test_every_registered_builder_dex_has_its_own_universe_source():
    """Десять dex живьём 2026-09-14, и у каждого свой запрос: один источник на десять
    означал бы, что провал одного глушит остальные девять."""
    names = {s.name for s in SOURCES}
    assert "hl_perpdexs" in names
    assert source_by_name("hl_perpdexs").body == {"type": "perpDexs"}
    assert set(ap.HL_LIVE_DEXES) & set(ap.HL_DORMANT_DEXES) == set()
    for dex in ap.HL_LIVE_DEXES + ap.HL_DORMANT_DEXES:
        src = source_by_name(f"hl_dex_{dex}")
        assert src.body == {"type": "meta", "dex": dex}, dex
        assert src.parse is ap.parse_hl_meta, dex


def test_a_new_builder_dex_in_the_registry_is_announced():
    host = seeded_host()
    retick(host, {"hl_perpdexs": perpdexs(*PERPDEX_ROWS,
                                          ("nu", "Nu Exchange", "0xnu00"))})

    assert len(host.item_alerts()) == 1
    assert "Nu Exchange" in host.item_alerts()[0]
    assert "nu" in host.item_alerts()[0]


def test_a_registry_answer_that_is_not_a_list_is_a_failure_not_an_empty_world():
    """Конверт у этого запроса — сам список. Объект вместо него, прочитанный как мир,
    означал бы «все десять builder-dex закрылись»."""
    host = seeded_host()
    retick(host, {"hl_perpdexs": json.dumps({"dexes": []}).encode()})

    st = host.state()
    assert host.item_alerts() == []
    assert st.sources["hl_perpdexs"].fails == 1
    assert len(st.sources["hl_perpdexs"].snapshot) == len(PERPDEX_ROWS)


def test_a_truncated_registry_is_refused_before_it_can_become_a_snapshot():
    """Пол 8 при десяти живых. Усечённый ответ, ставший снимком, — это ложное
    «появились» на весь реестр при следующей же проверке."""
    src = source_by_name("hl_perpdexs")
    assert src.floor == 8
    with pytest.raises(SourceError):
        ap.parse_hl_perpdexs(perpdexs(*PERPDEX_ROWS[:3]), src)


def test_the_nameless_first_row_of_the_registry_is_not_a_dex():
    """Нулевой элемент ответа — null: это мейн, у которого имени нет. Прочитанный как
    запись, он стал бы либо провалом источника, либо dex-ом с пустым именем."""
    snap = ap.parse_hl_perpdexs(perpdexs(*PERPDEX_ROWS), source_by_name("hl_perpdexs"))

    assert set(snap) == {name for name, _, _ in PERPDEX_ROWS}
    assert snap["io"] == {"label": "EntropyIO (dex io)", "full_name": "EntropyIO",
                          "deployer": "0x320c8988e3d1b5198f335802d7bfd2728a8fcac6"}


def test_a_dex_changing_hands_is_reported_as_one_change():
    """Деплойер — это тот, кто может снять с торгов и переставить оракул. Смена
    адреса под тем же именем — решение площадки, а не косметика."""
    host = seeded_host()
    rows = list(PERPDEX_ROWS)
    rows[-1] = ("io", "EntropyIO", "0xdeadbeef")
    retick(host, {"hl_perpdexs": perpdexs(*rows)})

    assert len(host.item_alerts()) == 1
    assert "deployer" in host.item_alerts()[0]
    assert "0xdeadbeef" in host.item_alerts()[0]


# =============================================================================
# 1.4: СИГНАЛКА НА СПЯЩИХ DEX
# =============================================================================
# Шесть dex из десяти зарегистрированы и не торгуют ничем: у ВСЕХ их активов
# dayNtlVlm и openInterest — нули (замерено 2026-09-14). Вселенная такого dex не
# двигается месяцами, поэтому diff по ней молчит и тогда, когда dex ожил. Первая
# сделка — это и есть решение: площадка, которую до сих пор можно было не считать.


def waking(dex: str, n: int = 16, *, idx: int = 1, vlm: str = "0.0", oi: str = "0.0") -> bytes:
    rows = [(f"{dex}:A{i}", "0.0", "0.0") for i in range(n)]
    rows[idx] = (f"{dex}:A{idx}", vlm, oi)
    return hl_meta_ctxs(*rows)


def test_every_dormant_dex_is_watched_by_a_tripwire_of_its_own():
    for dex in ap.HL_DORMANT_DEXES:
        src = source_by_name(f"hl_dex_{dex}_activity")
        assert src.body == {"type": "metaAndAssetCtxs", "dex": dex}, dex
        assert src.kind == "diff", dex
        # Пол 1, а не «как у торгующих»: у abcd вселенная — РОВНО ОДИН актив, и порог
        # выше единицы убил бы этот источник навсегда, молча.
        assert src.floor == 1, dex


def test_the_first_trade_on_a_dormant_dex_fires_exactly_one_item():
    host = seeded_host()
    retick(host, {"hl_dex_flx_activity": waking("flx", 16, idx=1, vlm="12345.6")})

    alerts = host.item_alerts()
    assert len(alerts) == 1
    assert "flx:A1 на dex flx — первая активность: нет → есть" in alerts[0]


def test_open_interest_alone_wakes_the_tripwire():
    """Оборот суток — скользящее окно, открытый интерес — мгновенный срез. Условие
    «или», а не «и»: позиция, открытая и оставленная, — тоже первая активность."""
    host = seeded_host()
    retick(host, {"hl_dex_km_activity": waking("km", 23, idx=7, oi="5.0")})

    alerts = host.item_alerts()
    assert len(alerts) == 1
    assert "km:A7 на dex km — первая активность: нет → есть" in alerts[0]


def test_a_dex_that_stays_dormant_says_nothing():
    """Сигналка не имеет права шуметь на тишине: шесть источников × 288 тиков в
    сутки — это канал, который перестанут читать."""
    host = seeded_host()
    retick(host)

    assert host.telegrams == []


def test_a_tripwire_answer_without_its_second_half_is_a_failure_not_an_empty_world():
    """`meta` и `metaAndAssetCtxs` отличаются ровно второй половиной. Ответ первой
    формы, прочитанный как мир, дал бы снимок без активности вообще — то есть
    сигналку, которая не сработает никогда и не скажет об этом."""
    host = seeded_host()
    retick(host, {"hl_dex_km_activity": hl_meta(*many_coins(23, prefix="km:S"))})

    assert host.item_alerts() == []
    assert host.state().sources["hl_dex_km_activity"].fails == 1


def test_halves_of_different_length_are_refused_before_they_can_pair_up():
    """Контексты параллельны вселенной по ИНДЕКСУ — имени актива во второй половине
    нет вовсе. Разъехавшиеся половины дали бы снимок, где активность приписана
    чужому активу, и первая же сработка назвала бы не тот символ."""
    doc = json.loads(dormant_dex("km", 23))
    doc[1] = doc[1][:-1]
    host = seeded_host()
    retick(host, {"hl_dex_km_activity": json.dumps(doc).encode()})

    assert host.item_alerts() == []
    assert host.state().sources["hl_dex_km_activity"].fails == 1


def test_an_unreadable_volume_is_a_failure_not_a_silent_dex():
    """Числа приезжают строками. Нечитаемая строка, прочитанная как ноль, — это
    сигналка, которая молчит ровно тогда, когда площадка ожила."""
    doc = json.loads(dormant_dex("cash", 17))
    doc[1][3]["dayNtlVlm"] = None
    with pytest.raises(SourceError):
        ap.parse_hl_dex_activity(json.dumps(doc).encode(),
                                 source_by_name("hl_dex_cash_activity"))


# =============================================================================
# 1.4: LIGHTER ПОД ROBINHOOD
# =============================================================================
# Отдельная площадка на том же коде: 84 книги (57 перпов, 27 спотов) против 245 у
# мейна, и вселенные РАЗНЫЕ — SPY/QQQ/XAU/SPCX/OPENAI живут только здесь.


def test_the_robinhood_lighter_universe_reads_its_live_shape_field_by_field():
    src = source_by_name("lighter_rh_markets")
    snap = ap.parse_lighter_rh_books(rh_world(), src)

    assert len(snap) == 61
    assert snap["2064"] == {"label": "QQQ/USDG (#2064)", "status": "active",
                            "created_at": "1782499411964"}
    assert src.url == "https://api.rh.lighter.xyz/api/v1/orderBooks"
    assert (src.entity, src.floor) == ("рынок", 20)


def test_a_robinhood_lighter_market_going_inactive_is_reported():
    host = seeded_host()
    retick(host, {"lighter_rh_markets": rh_world([rh_book(7, "RH7", status="inactive")])})

    alerts = host.item_alerts()
    assert len(alerts) == 1
    assert "RH7" in alerts[0] and "active → inactive" in alerts[0]


def test_a_new_robinhood_lighter_market_carries_the_venue_time_of_its_creation():
    """`created_at` — единственная метка времени, которую эта площадка даёт: сам diff
    метки не имеет и честно об этом говорит."""
    host = seeded_host()
    retick(host, {"lighter_rh_markets": rh_world([rh_book(900, "SPCX", created="1789000000000")])})

    alerts = host.item_alerts()
    assert len(alerts) == 1
    assert "SPCX" in alerts[0]
    assert host.state().sources["lighter_rh_markets"].snapshot["900"]["created_at"] == "1789000000000"


def test_a_robinhood_lighter_answer_with_a_venue_error_code_is_a_failure():
    """Конверт этой площадки несёт свой код рядом с HTTP-кодом. 200 снаружи и 500
    внутри, прочитанные как мир, — это «делистнули все 84 рынка»."""
    src = source_by_name("lighter_rh_markets")
    books = [rh_book(i, f"RH{i}") for i in range(60)]
    with pytest.raises(SourceError):
        ap.parse_lighter_rh_books(rh_books(*books, code=500), src)


def test_a_truncated_robinhood_lighter_universe_is_refused():
    src = source_by_name("lighter_rh_markets")
    with pytest.raises(SourceError):
        ap.parse_lighter_rh_books(rh_books(rh_book(1, "RH1"), rh_book(2, "RH2")), src)


# =============================================================================
# 1.4: POLYMARKET PERPS
# =============================================================================
# 83 инструмента живьём 2026-09-14, четырьмя волнами включения (39 / 13 / 15 / 16), и
# волна — это решение площадки: `ui_live_time` назначается заранее и переставляется.


def test_the_polymarket_instruments_read_their_live_shape_field_by_field():
    src = source_by_name("polymarket_perps_instruments")
    snap = ap.parse_polymarket_instruments(poly_world(), src)

    assert len(snap) == 62
    assert snap["1"] == {"label": "SP500-USD (#1)", "symbol": "SP500-USD",
                         "ui_live_time": "1785542400000"}
    assert src.url == "https://api.perpetuals.polymarket.com/v1/info/instruments"
    assert (src.entity, src.floor) == ("инструмент", 40)


def test_a_new_polymarket_instrument_is_reported_once_and_only_once():
    host = seeded_host()
    world = poly_world([poly_instrument(777, "NVDA-USD", category="equity")])
    retick(host, {"polymarket_perps_instruments": world})

    assert len(host.item_alerts()) == 1
    assert "NVDA-USD" in host.item_alerts()[0]

    host.telegrams.clear()
    retick(host, {"polymarket_perps_instruments": world})
    assert host.telegrams == []


def test_a_polymarket_go_live_time_that_moves_is_reported():
    """Инструмент заведён заранее, а торговать начнёт в назначенный час. Переставленный
    час — это и есть анонс, и других анонсов у этой площадки нет."""
    host = seeded_host()
    moved = dict(POLY_SPCX, ui_live_time=1790000000000)
    retick(host, {"polymarket_perps_instruments": poly_instruments(
        POLY_SP500, moved, *[poly_instrument(100 + i, f"P{i}-USD") for i in range(60)])})

    alerts = host.item_alerts()
    assert len(alerts) == 1
    assert "SPCX-USD" in alerts[0] and "1790000000000" in alerts[0]


def test_a_polymarket_error_object_is_a_failure_not_an_empty_world():
    """Ответ этой площадки — ГОЛЫЙ список, а ошибка приезжает объектом. Объект,
    прочитанный как мир, — это «сняли все 83 инструмента»."""
    src = source_by_name("polymarket_perps_instruments")
    with pytest.raises(SourceError):
        ap.parse_polymarket_instruments(
            json.dumps({"error": "rate limited", "status": 429}).encode(), src)


def test_a_truncated_polymarket_answer_is_refused():
    src = source_by_name("polymarket_perps_instruments")
    with pytest.raises(SourceError):
        ap.parse_polymarket_instruments(poly_instruments(POLY_SP500, POLY_SPCX), src)


# =============================================================================
# 1.4: DERIBIT USDC
# =============================================================================
# 121 инструмент, из них 21 с `is_active: false`: у этой площадки контракт заводится
# ЗАРАНЕЕ и включается отдельным решением. Очередь видна на снимке — то есть листинг
# известен до того, как по нему пройдёт первая сделка.


def test_the_deribit_usdc_futures_read_their_live_shape_field_by_field():
    src = source_by_name("deribit_usdc_instruments")
    snap = ap.parse_deribit_instruments(deribit_world(), src)

    assert len(snap) == 62
    assert snap["1000BONK_USDC-PERPETUAL"] == {
        "label": "1000BONK_USDC-PERPETUAL (perpetual)", "is_active": "false",
        "settlement_period": "perpetual", "creation_timestamp": "1789128071000"}
    assert src.url == ("https://www.deribit.com/api/v2/public/get_instruments"
                       "?currency=USDC&kind=future")
    assert (src.entity, src.floor) == ("инструмент", 40)


def test_the_inactive_deribit_queue_is_visible_and_its_activation_is_reported():
    host = seeded_host()
    assert host.state().sources["deribit_usdc_instruments"].snapshot[
        "1000BONK_USDC-PERPETUAL"]["is_active"] == "false"

    retick(host, {"deribit_usdc_instruments": deribit_world(bonk_active=True)})

    alerts = host.item_alerts()
    assert len(alerts) == 1
    assert "1000BONK_USDC-PERPETUAL" in alerts[0]
    assert "is_active: false → true" in alerts[0]


def test_a_deribit_error_envelope_is_a_failure_not_an_empty_world():
    """Конверт JSON-RPC: ошибка живёт рядом с результатом, а не вместо него. Ответ с
    `error`, прочитанный как мир, — это «сняли все 121 контракт»."""
    src = source_by_name("deribit_usdc_instruments")
    rows = json.loads(deribit_world())["result"]
    with pytest.raises(SourceError):
        ap.parse_deribit_instruments(
            deribit_envelope(rows, error={"code": 10028, "message": "too_many_requests"}), src)


def test_a_truncated_deribit_answer_is_refused_before_it_can_become_a_snapshot():
    """Пол 40 при 121 живом. Усечённый ответ, ставший снимком, — это ложный делистинг
    восьмидесяти контрактов на следующей проверке."""
    src = source_by_name("deribit_usdc_instruments")
    assert src.floor == 40
    with pytest.raises(SourceError):
        ap.parse_deribit_instruments(deribit_envelope([DERIBIT_PEPE, DERIBIT_BONK]), src)


# =============================================================================
# 1.4: KRAKEN FUTURES
# =============================================================================
# 295 инструментов, и это ЕДИНСТВЕННЫЙ вход к площадке, который видит перпы на акции
# (PF_NBISUSD открыт 2026-09-03): вселенная спотовых пар их не знает, а блог отвечает
# челленджем чаще, чем содержимым.


def test_the_kraken_futures_instruments_read_their_live_shape_field_by_field():
    src = source_by_name("kraken_futures_instruments")
    snap = ap.parse_kraken_futures_instruments(kfut_world(), src)

    assert len(snap) == 152
    assert snap["PF_NBISUSD"] == {"label": "PF_NBISUSD (flexible_futures)",
                                  "tradeable": "true", "type": "flexible_futures",
                                  "openingDate": "2026-09-03T11:58:25Z"}
    assert src.url == "https://futures.kraken.com/derivatives/api/v3/instruments"
    assert (src.entity, src.floor) == ("инструмент", 100)


def test_a_new_kraken_futures_symbol_is_reported_once_and_only_once():
    host = seeded_host()
    world = kfut_world([kfut("PF_SPCXUSD", opening="2026-09-20T00:00:00Z")])
    retick(host, {"kraken_futures_instruments": world})

    assert len(host.item_alerts()) == 1
    assert "PF_SPCXUSD" in host.item_alerts()[0]

    host.telegrams.clear()
    retick(host, {"kraken_futures_instruments": world})
    assert host.telegrams == []


def test_a_kraken_futures_symbol_going_untradeable_is_reported():
    """Делистинг у этой площадки начинается со снятия торгуемости, а не с исчезновения
    символа: живьём 2026-09-14 все 295 торгуемы, и переход виден только здесь."""
    host = seeded_host()
    retick(host, {"kraken_futures_instruments": kfut_world(
        [dict(KRAKEN_FUT_NBIS, symbol="PF_F0USD", tradeable=False)])})

    alerts = host.item_alerts()
    assert len(alerts) == 1
    assert "PF_F0USD" in alerts[0] and "tradeable: true → false" in alerts[0]


def test_a_kraken_futures_error_envelope_is_a_failure_not_an_empty_world():
    """У этой площадки отказ приезжает кодом 200 и полем `result: error`. Правдоподобный
    размер при таком конверте — самая опасная форма: прочитанный как мир, он означает
    «сняли с торгов все 295 инструментов»."""
    src = source_by_name("kraken_futures_instruments")
    rows = json.loads(kfut_world())["instruments"]
    with pytest.raises(SourceError):
        ap.parse_kraken_futures_instruments(
            kfut_envelope(rows, result="error", error="requireAuthentication"), src)


def test_a_truncated_kraken_futures_answer_is_refused():
    src = source_by_name("kraken_futures_instruments")
    with pytest.raises(SourceError):
        ap.parse_kraken_futures_instruments(kfut_world(n=10), src)


# =============================================================================
# 1.4: РЕЕСТР ЧЕТЫРЁХ НОВЫХ ПЛОЩАДОК
# =============================================================================


def test_the_four_new_venues_are_registered_each_with_one_request_of_its_own():
    """Снятый из реестра источник — это молчание без единого признака: адаптер и его
    тесты остаются зелёными (урок 1.2, блог Kraken). Поэтому реестр пиннут отдельно."""
    expected = {
        "lighter_rh_markets": ("https://api.rh.lighter.xyz/api/v1/orderBooks",
                               ap.parse_lighter_rh_books),
        "polymarket_perps_instruments": (
            "https://api.perpetuals.polymarket.com/v1/info/instruments",
            ap.parse_polymarket_instruments),
        "deribit_usdc_instruments": (
            "https://www.deribit.com/api/v2/public/get_instruments?currency=USDC&kind=future",
            ap.parse_deribit_instruments),
        "kraken_futures_instruments": (
            "https://futures.kraken.com/derivatives/api/v3/instruments",
            ap.parse_kraken_futures_instruments),
    }
    for name, (url, parse) in expected.items():
        src = source_by_name(name)
        assert (src.url, src.parse, src.kind) == (url, parse, "diff"), name
        assert src.body is None, name

    host = FakeHost()
    tick(host)
    for name in expected:
        assert host.fetched.count(name) == 1, name


def test_the_challenge_docstring_describes_the_design_that_keeps_the_source():
    """Долг 1.3, оплаченный в 1.4. Docstring `failure_reason` утверждал, что источник
    с бот-защитой «надо менять» и что блог Kraken на этом умер, — а 1.3 как раз
    доказал обратное (челлендж интермиттентный) и источник СОХРАНИЛ, дав ему свой
    порог. Неверная причина в комментарии живёт дольше кода: следующий читатель снял
    бы источник во второй раз."""
    doc = ap.failure_reason.__doc__ or ""

    assert "источник надо менять" not in doc
    assert "FAIL_ALERT_AFTER_CHALLENGE" in doc
    assert "интермиттент" in doc.lower()
