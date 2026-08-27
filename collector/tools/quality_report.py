#!/usr/bin/env python3
"""Offline quality report over raw collector recordings.

Implements **Фаза 2 — offline quality report по сырью** of
`docs/design-multi-venue-collection.md`. It answers the one question the
collector process cannot answer about itself (`collector/README.md`, "Going
silent"): *did we actually get everything we asked for, and is it readable?*

It is a **report**, not enforcement. Enforcement lives in the Phase 3 builder,
which consumes the JSON written by `--json`.

What it checks, per finalized UTC day per venue directory:

1. **Finalized files only.** The live day's gzip member has no trailer until
   rotation or shutdown (`collector/src/file.rs`), so it cannot pass an
   integrity check by construction. `--include-today` overrides this for
   end-to-end testing, and then a truncated *last* member is a warning rather
   than corruption.
2. **gzip integrity** — a full decode through every member (a restart appends
   one; Python's `gzip` reads them transparently).
3. **Expected symbol x stream set** = the `session_start` record x the dataset
   profile. Missing required stream => red; missing optional => warning; missing
   *informational* => nothing at all, only a line in the JSON (see `Expected`:
   a stream added to the collector after a recording was made cannot be
   backfilled, and warning about it would yellow every historical day at once).
   The profile may also contradict the recording outright: mode A trades the
   Hyperliquid book, so a recording configured with no `l2Book` cadence at all
   is red however cleanly it was written.

   `session_start` is written **once per process** (`collector/src/main.rs`)
   while the sidecar rotates at UTC midnight (`collector/src/file.rs`), so the
   configuration for a day is looked up across every sidecar in the directory,
   not just that day's. Otherwise every day after a collector's first would be
   red for a configuration that never changed.
4. **Sequence gaps** — Binance USD-M `pu` chain, Bybit `u` per topic. Hyperliquid
   has no sequence number at all: cadence is the only evidence there.
5. **Cadence gaps** — a hole larger than K x the measured cadence of the channel,
   except where a steadier stream on the same socket was **running across that
   hole** and had none of its own (see `LIVENESS_REFERENCE` and
   `liveness_witness`). Bounded: no reference excuses a hole past
   `MAX_SUPPRESSED_GAP_FACTOR` x the channel's own limit, and none excuses one
   the sidecar already accounts for.
6. **`local_ts` monotonicity, per stream** — with a tolerance for the order two
   streams are interleaved in, allowed only where a second producer exists to
   have raced (a stream with a hand-off of its own, `_SECOND_PRODUCER`, or a
   venue that opens one socket per channel, `FANS_OUT_PER_CHANNEL` in the
   `_TOPOLOGY` registry) and only as far as the hand-offs the *late* row of the
   pair crossed alone can hold it back (`interleave_kind`): the socket hop where
   the REST snapshot went first
   (`CROSS_STREAM_TOLERANCE_NS`),
   that same figure as a reused ceiling where two channel sockets of one runtime
   raced, no bound at all where the `premiumIndex` poll went first, because the
   late row then waited in the writer hop too, and a ceiling of its own where
   the poll is itself the late row (`POLLER_HOP_CEILING_NS`) — its own hand-off
   is all it can have waited in, and a poll stamped anywhere but at receive
   shows up here or nowhere.
7. **Coverage at both ends**, reported **per symbol** as the interval in which
   *every* required stream of that symbol is live (max of the firsts, min of the
   lasts). That — not the venue-wide union — is what Phase 3 must trim to: a
   union lets an on-time `bbo` hide an `l2Book` that started ten minutes late,
   and the run would begin over a window where the traded book does not exist.
   The venue-level number is kept as the union across symbols, for the operator.
8. **`_meta` cross-check** — a gap spanned by a `disconnected` / `dial_failed` /
   restart record is annotated as explained rather than left as a mystery. Only
   the collector's *lifecycle* records may do that (`_EXPLANATORY`): the minutely
   disk gauge lands inside every hole longer than a minute and explains none of
   them.

The sidecar is deliberately NOT checked for monotonicity: `main.rs` writes the
disk gauge and the terminal records straight to the `Writer`, bypassing the
queue, so `_meta` is not ordered by `local_ts` by design (see the Phase 1 status
block in the design doc). Everything here sorts it before use.

**Timestamps are int64 nanoseconds end to end.** ~1.78e18 does not fit a float64
mantissa (2^53 ~ 9e15), so a single implicit float conversion silently corrupts
the coverage window Phase 3 trims on. Nothing in this file lets one become a
float — durations and thresholds are ints too, and only display formatting
divides.

Usage::

    quality_report.py --dir /data/hyperliquid --dir /data/binance \\
        [--day 20260725] [--include-today] \\
        [--json report.json] [--profile mode-a-v1]

Every finalized day present is checked unless `--day` narrows it to one.

Exit codes: 0 green/yellow, 1 red, 2 usage or I/O error.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
import zlib
from bisect import bisect_left
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

SCHEMA = "quality-report-v1"

GREEN = "green"
YELLOW = "yellow"
RED = "red"
_SEVERITY_ORDER = {GREEN: 0, YELLOW: 1, RED: 2}

SEC_NS = 1_000_000_000

#: How many gaps of one (symbol, stream) reach the JSON, and how many are named
#: individually in the issue list. A pathological file must not produce a report
#: nobody can read, but "gaps are listed by name, not summarised away" is the
#: doc's acceptance line — so the cap is generous and the remainder is counted.
MAX_GAPS_RECORDED = 200
MAX_GAP_ISSUES = 10

#: How far outside a gap a lifecycle record may sit and still explain it.
#:
#: Deliberately tiny. A record that accounts for a hole falls strictly inside
#: it: every backend stamps a data frame with `Utc::now()` at receive time, so
#: the disconnect that ended a burst is always stamped after the last frame of
#: that burst, and the `connected` that ended the outage before the first frame
#: of the next one. The margin only absorbs the sub-millisecond skew between two
#: files written by the same process — widen it and a `session_start` at the top
#: of the recording starts "explaining" every gap in the day.
EXPLAIN_MARGIN_NS = SEC_NS // 10

#: The names `collector/src/queue.rs` gives the hand-offs a record crosses on its
#: way into `Writer::write`. A WS frame reaches `WRITER_HOP` only after the
#: socket hop (`WS_HOP`) ahead of it; a second producer can put one there
#: directly, or — the poller, and only the poller — bypass it entirely with a hop
#: of its own.
#:
#: `WS_HOP` is here because a *fan-out* venue's second producer is upstream of
#: it rather than past it: several socket tasks feed this one hop, so it is the
#: first queue their frames share and therefore the first thing that stops
#: holding them apart. See `FANS_OUT_PER_CHANNEL`.
#:
#: Mirrored rather than imported, like the capacities below, and pinned against
#: the Rust by `test_the_poller_still_has_a_hand_off_of_its_own` and
#: `test_the_socket_hop_the_fan_out_shares_is_the_one_the_rust_names` — because
#: which hop a producer uses is now the whole of the interleave model.
WS_HOP = "websocket->parser"
WRITER_HOP = "parser->writer"
POLLER_HOP = "poller->writer"


@dataclass(frozen=True)
class Producer:
    """A *second*, concurrent producer of one stream of a symbol file.

    `hop` is the hand-off it uses to reach the writer, and it is the load-bearing
    field: it says which queues, if any, this producer's row still shares with a
    WebSocket frame, and therefore what can put the two out of `local_ts` order
    and by how much. `mechanism` is that written out for the operator.
    """

    hop: str
    mechanism: str


#: Streams a second, concurrent producer writes.
#:
#: Load-bearing, not decoration: this is the whole reason two streams may be
#: written out of `local_ts` order, so it is also what decides whether an
#: inversion is tolerable at all, and — through `Producer.hop` — how far.
#: Where no entry matches **and the venue does not fan out per channel**
#: (`fans_out_per_channel`), the venue has one WS reader stamping and queueing
#: every frame of a symbol file, write order IS receive order, and a step
#: backwards is a defect at any size — see `scan_symbol_file`, which records
#: where that was verified per venue.
#:
#: Keyed by stream, because on the venues below the second producer is one
#: particular feed. A fan-out venue is the other shape — there the second
#: producer is *every* other stream — and is keyed by exchange instead.
_SECOND_PRODUCER = {
    "depthSnapshot": Producer(
        WRITER_HOP,
        "the REST depth-snapshot fetcher is one such producer: it runs detached "
        "(tokio::spawn, the same shape in all three binance* backends) and hands "
        "its frame straight to the writer hop, skipping the socket hop that WS "
        "frames queue through first",
    ),
    "premiumIndex": Producer(
        POLLER_HOP,
        "the premium-index poller is one such producer, and the only one with a "
        "hand-off of its own: it runs on its own timer beside the socket loop "
        "(binancefuturesum/mod.rs) and hands each element to the writer over "
        "poller->writer, so it queues behind neither the socket hop nor the "
        "writer hop that a WS frame crosses in turn",
    ),
}

#: Said of an inversion between two streams that share the one producer.
#:
#: About the pair rather than the venue, because `scan_symbol_file` holds two
#: shared-chain rows against each other across whatever a second producer wrote
#: between them: on Binance the venue does have second producers, just not on
#: this pair, and the two rows named need not be adjacent lines.
#:
#: Never printed for a fan-out venue: there every cross-stream pair has a second
#: producer by construction, and `second_producer_of` says so before this text
#: can be reached.
_NO_SECOND_PRODUCER = (
    "no second producer stands between these two streams: one WS reader stamps "
    "both at receive and hands them on in that order, through hops that are "
    "FIFO the whole way to the writer, so anything written between them came "
    "off a different hand-off and cannot have reordered either"
)

#: Venues that open one socket per channel instead of multiplexing every channel
#: onto one, so that **every** stream of a symbol file is its own producer.
#:
#: Extended is the one, and the mechanism is per-URL fan-out:
#: `channel_urls` (`collector/src/extended/mod.rs`) gives each recorded stream a
#: WebSocket of its own — the `/orderbooks` firehose, and `/publicTrades/{m}`,
#: `/funding/{m}`, `/prices/mark/{m}` per market — and `keep_connections`
#: (`extended/http.rs`) spawns one `keep_connection_one` task per URL. Each task
#: stamps its own `Utc::now()` at its own read and then sends into the one
#: shared socket hop, so two streams can reach that hop in the other order from
#: the one they were stamped in. That is a second-producer race, not a
#: single-reader defect, and it is bounded — see `interleave_kind`.
#:
#: **One stream class is one socket, which is why the exemption is per pair and
#: not per venue.** Two rows of the *same* class came off the same task, which
#: reads, stamps and enqueues in that order, so a same-class step backwards
#: keeps no tolerance at all — `scan_symbol_file` files it under
#: `monotonic_violation`, red at a nanosecond, and nothing here reaches it.
#:
#: **An unclassifiable frame gets the exemption too, and that is a decision,
#: not a fall-out** — pinned by
#: `test_an_extended_inversion_against_an_unclassified_frame_is_yellow`, because
#: before this exemption existed such a pair was red at a nanosecond and a
#: silent flip is the defect this whole file is written against. Whatever the
#: backend wrote came off one of these same per-channel sockets — recording a
#: channel this report has not been taught is the ordinary way to get here, and
#: on this venue a new channel is a new socket — so the pair really is two tasks
#: racing, unless the frame came off the *other* row's own socket, and one task
#: reads, stamps and enqueues in that order, so a same-socket pair cannot invert
#: at all.
#:
#: The residual is that last clause read backwards: frames of one socket split
#: between this bucket and their own stream share no monotonic cursor, so if
#: such a pair inverts anyway it collects the cross-stream tolerance instead of
#: the red a correctly named pair would get. Accepted, because it cannot arrive
#: quietly. It needs the classifier to be out of date, which is its own finding
#: on the same file and the same day (`unclassified_frame`); misclassification
#: is a different failure from reordering and that finding is its detector; and
#: every defect the red exists for reaches a detector this bucket cannot dull —
#: a clock step lands inside the busiest *classified* stream
#: (`monotonic_violation`, red at a nanosecond), and two recordings in one file
#: disorder every stream against every other and fail `gzip_integrity` first.
#: Failing closed instead — the exemption restricted to two named streams — was
#: tried and rejected: it reds exactly the frames the report has just said it
#: cannot name, which is a hard build refusal earned by this file's own
#: ignorance rather than by anything in the recording, and it prints the
#: `_NO_SECOND_PRODUCER` sentence over a venue that has one.
#:
#: Declared per backend in `_TOPOLOGY` rather than by family: which sockets get
#: opened is a property of the backend and of nothing else. The declaration is a
#: claim about `collector/src/`, so it is pinned to it in both directions —
#: `test_extended_still_opens_one_socket_per_channel` fails if Extended is ever
#: refactored onto one socket (the exemption would then be tolerating a real
#: defect for up to a second, silently), and
#: `test_paradex_still_multiplexes_every_channel_onto_one_socket` fails if a
#: single-reader venue grows a fan-out (its red would then be a false one).
FANS_OUT_PER_CHANNEL = "fans_out_per_channel"

#: One WS reader multiplexes every channel of a symbol file onto one socket.
#:
#: The other topology, and the one the report was written around: one task
#: reads, stamps `Utc::now()` and enqueues in that order, and every hand-off
#: from there to the file is a FIFO — so **write order IS receive order** and a
#: cross-stream step backwards is a defect at a nanosecond. There is no
#: mechanism to tolerate, which is why the tolerances below are granted on the
#: producer and never on the size.
#:
#: A venue may be `SINGLE_READER` and still have named second producers — see
#: `Topology.producers` and `_SECOND_PRODUCER`. That is the Binance base: one
#: multiplexed socket for the WS streams, plus a REST fetcher and/or a poller
#: writing rows of their own beside it.
SINGLE_READER = "single_reader"


@dataclass(frozen=True)
class Topology:
    """How one backend's producers reach the writer.

    `backend` is the directory under `collector/src/` that records this venue —
    two spellings of one backend (`binance`/`binancespot`) name the same one,
    because topology is a property of the sockets a backend opens and not of the
    word the operator typed. It is also where the mirror pin reads the Rust.

    `kind` is `SINGLE_READER` or `FANS_OUT_PER_CHANNEL`, and it is what
    `interleave_kind` turns into a verdict.

    `producers` names the `_SECOND_PRODUCER` entries this backend actually runs.
    A claim about the backend, checked against it in **both** directions by
    `test_the_topology_the_registry_declares_is_the_one_the_backend_has` — a
    backend that grows a poller or a REST fetcher without declaring it here has
    a real race the model has no tolerance for and would call corruption. It is
    deliberately not consulted to gate `_SECOND_PRODUCER`, which stays keyed by
    stream: those two stream names exist only in the Binance frame family, so
    the per-stream key is already exact, and gating would change a verdict
    rather than only declaring one.
    """

    backend: str
    kind: str
    producers: tuple = ()


#: Every venue the collector can record, and the producer topology of each.
#:
#: **The one source of truth for that question, and there is no default.** It
#: exists because the gate learned each venue's topology reactively, one
#: production alert per venue: Extended opens a socket per channel, nothing said
#: so, and the interleave model — which simply assumed a single reader for any
#: venue it had not been told about — went red on an ordinary bounded race
#: (896a51d). Omission is now impossible in both directions: `topology_of`
#: refuses an exchange it has no entry for the way `family_of` does, and
#: `test_every_backend_the_collector_dispatches_has_a_topology` reads the arms
#: of `collector/src/main.rs` and fails on a backend that is not declared here.
#:
#: Keyed by the spelling `session_start.exchange` can carry, exactly as
#: `_FAMILY` is — the aliases are listed rather than resolved so that nothing
#: here depends on `canonical_exchange` having been called first.
_TOPOLOGY = {
    # One WS reader, no producer of any other kind: red at a nanosecond between
    # any two streams of a symbol file.
    "bybit": Topology("bybit", SINGLE_READER),
    "hyperliquid": Topology("hyperliquid", SINGLE_READER),
    "lighter": Topology("lighter", SINGLE_READER),
    "paradex": Topology("paradex", SINGLE_READER),
    # The Binance base: one multiplexed socket, plus the named producers beside
    # it. Which of the two a backend runs is not uniform — COIN-M and spot have
    # the REST snapshot fetcher and no poller, because the venue still serves
    # their mark-price class on the stream — so it is stated per backend rather
    # than inherited from the family.
    "binance": Topology("binance", SINGLE_READER, ("depthSnapshot",)),
    "binancespot": Topology("binance", SINGLE_READER, ("depthSnapshot",)),
    "binancefuturescm": Topology("binancefuturescm", SINGLE_READER, ("depthSnapshot",)),
    "binancefutures": Topology(
        "binancefuturesum", SINGLE_READER, ("depthSnapshot", "premiumIndex")
    ),
    "binancefuturesum": Topology(
        "binancefuturesum", SINGLE_READER, ("depthSnapshot", "premiumIndex")
    ),
    # A Binance USD-M clone down to the poller — see `_FAMILY`, and note that
    # sharing the frame family says nothing about the sockets: this entry is
    # what says the two agree here.
    "aster": Topology("aster", SINGLE_READER, ("depthSnapshot", "premiumIndex")),
    # The one fan-out venue.
    "extended": Topology("extended", FANS_OUT_PER_CHANNEL),
}


def topology_of(exchange: str) -> Topology:
    """The producer topology of a `session_start.exchange` value.

    Refuses rather than assumes, and that refusal is the point of the registry:
    a venue nobody has modelled must not be classified by a default. Same shape
    as `family_of`, because it is the same kind of ignorance — this report has
    been handed a recording from a backend it does not know.
    """
    try:
        return _TOPOLOGY[exchange]
    except KeyError:
        raise ValueError(
            f"unknown exchange {exchange!r}: no producer topology is declared "
            f"for it, so how its streams reach the writer is not known and its "
            f"interleaves cannot be classified. Declare it in _TOPOLOGY; "
            f"known: {', '.join(sorted(_TOPOLOGY))}"
        ) from None


#: What a fan-out venue's second producer is, written out for the operator.
#:
#: `hop` is `WS_HOP` and that is the point: unlike the two producers above, this
#: one shares *every* queue with the row it raced — the socket hop and the
#: writer hop both — so those cancel and what is left is only the window between
#: one task's stamp and its enqueue.
_FAN_OUT_PRODUCER = Producer(
    WS_HOP,
    "this venue opens one socket per channel and spawns a task for each "
    "(extended/http.rs: keep_connections spawns keep_connection_one per URL), "
    "so the other stream IS a second producer: it stamps its own Utc::now() at "
    "its own read and races this one into the shared %s hop, and the two hops "
    "from there to the file are FIFOs that both rows cross" % WS_HOP,
)


def fans_out_per_channel(exchange: str) -> bool:
    """True where each channel of a symbol file arrives on a socket of its own.

    The single place the topology `kind` is compared, so the classification and
    the sentence explaining it cannot come to different answers.

    A membership test would answer `False` for a venue it has never heard of,
    which is the silent single-reader default this registry exists to remove —
    so it goes through `topology_of`, which refuses instead.
    """
    return topology_of(exchange).kind == FANS_OUT_PER_CHANNEL


def second_producer_of(exchange, prev_stream, stream) -> Optional[Producer]:
    """The concurrent producer whose hand-off explains this pair, if any.

    This answers *which mechanism to name*, not *how far it stretches* — the
    bound is direction-dependent and `interleave_kind` decides it. Usually there
    is at most one producer here, a second producer against an ordinary WS
    stream. But both rows can have one (a poll written next to a depth
    snapshot), and then the one to name is the one sharing the **fewest** queues
    with the other row, because that is what is left to hold the two in order.
    `POLLER_HOP` shares none, so it wins, and it is the mechanism in either
    direction: what changes with the direction is which of the two rows waited,
    not which hand-off made waiting possible.

    The venue is consulted **last**, for the same reason: a named stream
    producer shares fewer queues than a sibling channel socket does, so where
    both apply the named one is still what holds the pair apart. No fan-out
    venue has a poller or a REST fetcher today; the order is written down so
    that the day one does, the answer does not depend on which check runs first.
    """
    # The venue is *answered* last, for the reason above — but it is
    # *validated* first, or a pair naming one of the streams below would be
    # classified for a backend nobody has modelled. That is the silent default
    # in its last hiding place; `_TOPOLOGY` is where it stops.
    topology_of(exchange)
    found = [
        _SECOND_PRODUCER[name] for name in (prev_stream, stream) if name in _SECOND_PRODUCER
    ]
    for producer in found:
        if producer.hop == POLLER_HOP:
            return producer
    if found:
        return found[0]
    if fans_out_per_channel(exchange):
        # Every stream of this venue is a socket task of its own, and this pair
        # is two different keys (`scan_symbol_file` sends a same-key step to
        # `monotonic_violation` instead), so it is two tasks by construction.
        # `UNCLASSIFIED` is one of those keys and is answered here too, on
        # purpose: what this report could not name still came off one of these
        # sockets. It is safe because misclassification is a different failure
        # from reordering and has its own finding — `FANS_OUT_PER_CHANNEL`
        # carries the argument, the one residual it does not cover, and the
        # pin.
        return _FAN_OUT_PRODUCER
    return None


def crosses_a_hand_off_of_its_own(stream) -> bool:
    """True where this stream reaches the writer on a hop no one else uses.

    `premiumIndex` over `queue::POLLER_HOP`, and only it. The single place the
    hop is compared, because both the classification and the text it prints turn
    on it.
    """
    producer = _SECOND_PRODUCER.get(stream)
    return producer is not None and producer.hop == POLLER_HOP


#: The two numbers in `collector/src/queue.rs` the interleave bound is derived
#: from — the socket hop's depth (`WS_QUEUE_CAPACITY`) and the burst rate it was
#: sized against (`burst::PEAK_MSG_PER_S`, measured 2026-07-26 22:00 UTC).
#:
#: Mirrored rather than imported: this is a Python tool reading recorded bytes,
#: not the collector. `test_the_interleave_bound_still_covers_the_socket_hop_it_
#: is_derived_from` reads the Rust and fails if either number moves, so raising
#: the capacity re-checks the gate instead of silently reddening burst days.
#: That is exactly what happened on 2026-07-29: the socket hop overflowed on
#: both `binancefuturesum` instances at once and went from 4096 to 16384, which
#: quadrupled the honest overtake below and forced the tolerance up with it.
WS_QUEUE_CAPACITY = 16384
PEAK_MSG_PER_S = 20_000

#: The longest a WS frame can sit behind the REST snapshot that skips it: the
#: whole socket hop, drained at the measured peak. 16384 / 20 000 = 819.2ms.
SOCKET_HOP_NS = WS_QUEUE_CAPACITY * SEC_NS // PEAK_MSG_PER_S

#: How far the *write* order of two different streams may disagree with their
#: `local_ts` order before it stops being an interleave and becomes a defect.
#:
#: A symbol file is written by one queue, but not always by one producer. Every
#: backend stamps `Utc::now()` at its own receive moment, and the Binance
#: backends run a second producer: the REST depth-snapshot fetcher is detached
#: (`tokio::spawn` in `binancefuturesum/mod.rs`) and sends **straight to the
#: writer hop**, while WS frames queue through the socket hop first. A snapshot
#: can therefore be written ahead of market data stamped earlier. Both stamps
#: are honest; only the interleaving is out of order.
#:
#: Sized from that hop rather than from the observation. Measured on ethusdt,
#: 2026-07-26: one inversion per ~5M lines, always at a REST refetch (12 that
#: day), worst 134us — but that was a writer keeping up. The two hops cancel
#: (both frames pass the writer hop), so the honest maximum is exactly
#: `SOCKET_HOP_NS`, and this is that with ~20% of headroom. A bound at the
#: observed 134us, or at the 10ms this shipped with, would go red on precisely
#: the burst days whose data matters most: a burst is also what breaks the `pu`
#: chain the refetch responds to, so a deep socket hop and a REST snapshot
#: co-occur by construction. Red is a hard build refusal in `build_dataset.py`.
#:
#: It was 250ms while the socket hop was 4096. Raising that hop to 16384 on
#: 2026-07-29 raised the honest maximum to 819.2ms, and this bound is a
#: consequence of the hop rather than a judgement of its own — so it follows,
#: to 1s. The cost is real and worth naming: a genuine 900ms inversion is now
#: yellow where it used to be red. That is the price of the deeper hop, not a
#: separate decision, and the alternative is a gate that calls the collector's
#: own design corruption on every burst day.
#:
#: A second pair is held to this same figure, and for a different reason: two
#: channel sockets of a fan-out venue (`FANS_OUT_PER_CHANNEL`) share *both*
#: hops, so what is left between them is a scheduler slice rather than a queue.
#: That is a ceiling reused, not a derivation — `interleave_kind`'s last
#: alternative is where it is argued and its cost named.
#:
#: Three things this bound is NOT. It does not apply within one stream: there is
#: one producer appending in receive order, so a step backwards of any size is
#: red — including on a fan-out venue, where one stream is still one socket
#: task reading, stamping and enqueueing in that order. It does not apply where
#: no second producer exists — see `_SECOND_PRODUCER` and
#: `FANS_OUT_PER_CHANNEL`; a venue with one WS reader gets no tolerance at all,
#: whatever the size, because there is no mechanism to tolerate. And **it does
#: not apply where the `premiumIndex` poll was written first** — that pair has
#: no bound at all, because the row it overtook then waited in the writer hop
#: too; see `interleave_kind`, which is where the arithmetic above runs out.
#: In the other direction, where the poll is itself the late row, the pair does
#: keep a ceiling — its own, `POLLER_HOP_CEILING_NS`, which is this same figure
#: and deliberately not this same constant.
CROSS_STREAM_TOLERANCE_NS = 1_000 * 1_000_000

#: How far a `premiumIndex` row may be written **behind** a frame stamped later
#: before the report stops calling it an interleave.
#:
#: A ceiling, not a derivation. The late row in that pair is the poll, so the
#: only queue it can have waited in is `queue::POLLER_HOP` — one element per
#: recorded symbol per `PREMIUM_INDEX_INTERVAL`, drained from a `select!` arm
#: `main` offers on every iteration, and `queue.rs` says in as many words that
#: this hop cannot fill. Nothing measures how fast it drains, so there is no
#: arithmetic to run here; what there is, is an observation. On 2026-07-29 — the
#: day the other direction reached 1.034730s — this direction's worst across a
#: full symbol-day was 3.128ms (tiausdt, 39 of them) and 1.882867ms (seiusdt,
#: 38). This figure is ~320x the worst thing that hop has ever been seen doing,
#: and the defect it is here to catch, a stamp that is not the receive moment,
#: is a poll interval or more.
#:
#: **Its own number, though it equals `CROSS_STREAM_TOLERANCE_NS` today.** That
#: one is the socket hop's occupancy and follows `WS_QUEUE_CAPACITY`: it has
#: already moved once, 250ms -> 1s, when the hop went 4096 -> 16384, and
#: `test_the_interleave_bound_still_covers_the_socket_hop_it_is_derived_from`
#: exists to make the next capacity change move it again. This pair never
#: crosses the socket hop. Sharing the constant would let a deeper socket hop
#: silently widen a lookahead detector by the same factor, with no test failing
#: and no one deciding it. Narrow this one the day something measures the drain;
#: until then it moves only for a reason of its own.
POLLER_HOP_CEILING_NS = 1_000 * 1_000_000


#: The three findings one out-of-order pair can be, in the order a symbol record
#: lists them. Field of `FileScan.interleave`, key in the per-symbol JSON and
#: `check` in the issue list are all this same string, so a pair cannot be filed
#: under one name and explained under another — which is how an earlier revision
#: printed "within the bound, so nothing is missing" on a finding it was
#: reporting as red. Which of them a record carries a key for when the finding
#: did *not* fire is a separate question — `INTERLEAVE_KEYS_ALWAYS`.
INTERLEAVE_INVERSION = "interleave_inversion"
INTERLEAVE_INVERSION_POLLER = "interleave_inversion_poller"
INTERLEAVE_EXCESS = "interleave_excess"
INTERLEAVE_CHECKS = (INTERLEAVE_INVERSION, INTERLEAVE_INVERSION_POLLER, INTERLEAVE_EXCESS)

#: The severity each of them carries. Two yellows and one red, and the red is
#: the one no mechanism explains.
INTERLEAVE_SEVERITY = {
    INTERLEAVE_INVERSION: YELLOW,
    INTERLEAVE_INVERSION_POLLER: YELLOW,
    INTERLEAVE_EXCESS: RED,
}

#: The two of them whose key a symbol record carries whether or not the finding
#: fired, as `null`: a consumer asking "was there an excess?" must not have to
#: tell an intact recording apart from an older schema.
#:
#: `INTERLEAVE_INVERSION_POLLER` is deliberately not one of them, and this line
#: is what keeps a venue with no poller byte-identical to the report it got
#: before this class existed. Unconditional, it would add one `null` per symbol
#: to Hyperliquid, Bybit and Lighter for a class their recordings cannot
#: produce — a schema change for every venue, carrying no information, in a
#: change whose own invariant is that nothing about the poller reaches a venue
#: without one. Nothing is lost by it either: `.get()` answers `None` for an
#: absent key exactly as it does for a `null`, so the distinction the two keys
#: above are unconditional *for* does not arise here — the key was never in the
#: schema to be missing from it.
INTERLEAVE_KEYS_ALWAYS = (INTERLEAVE_INVERSION, INTERLEAVE_EXCESS)


def interleave_json(found: dict) -> dict:
    """The interleave keys one symbol record carries, in one fixed order.

    `found` maps `INTERLEAVE_CHECKS` names to records — `FileScan.interleave`,
    or `{}` for a symbol whose file is missing entirely. One function because
    both of those go into the same JSON object and a key set that differed
    between them would be a schema that depends on whether the file was there.
    """
    return {
        check: found.get(check)
        for check in INTERLEAVE_CHECKS
        if check in INTERLEAVE_KEYS_ALWAYS or check in found
    }


def interleave_kind(exchange: str, prev_stream: str, stream: str, delta_ns: int) -> str:
    """Which finding an out-of-order pair of streams is, from its mechanism.

    The pair is read in **write order**: `prev_stream` was written first,
    `stream` second — and `stream` is the *late* row, the one whose `local_ts`
    is older than the row already above it in the file. Read the class off the
    hand-offs that late row crossed **alone**, because those are the only queues
    it can have waited in while the other row went past:

    * **Both rows crossed the same hops, off the same reader** — every WS pair
      on the venues with one socket: Hyperliquid, Bybit, Lighter and Paradex.
      The last is worth naming because it looks like the venue below and is not:
      `collector/src/paradex/mod.rs` makes ONE `keep_connection(CHANNELS
      .to_vec(), markets, ws_tx)` to one `WS_URL`, every channel multiplexed
      onto that socket, and its only non-WS writes — the universe catalog and
      the refusal-to-start record — go to `_meta` rather than to a symbol file.
      One reader stamps at receive and hands frames on in that order, and every
      queue after it is a FIFO, so write order IS receive order.
      `INTERLEAVE_EXCESS`, red, at a nanosecond.
    * **Two channel sockets of a fan-out venue** — Extended, and see
      `FANS_OUT_PER_CHANNEL` for why it is the only one. Both rows do cross the
      same socket hop and the same writer hop, so both cancel; the un-shared
      segment is upstream of the first queue, the window between one task's
      `Utc::now()` and its `send`, in which the runtime may poll the sibling
      socket and let it stamp later and enqueue first.
      `CROSS_STREAM_TOLERANCE_NS` bounds it, with the same two verdicts as the
      snapshot pair below — argued at the end of this docstring, because it is
      a reused ceiling rather than a derivation.
    * **The late row is the poll** — it crossed `queue::POLLER_HOP` and nothing
      else, so that hop plus one turn of `main`'s `select!` is the whole of what
      it can have waited in, and `queue.rs` says in as many words that this hop
      cannot fill: one element per recorded symbol per ten seconds, against an
      arm `main` offers on every iteration. Bounded — `POLLER_HOP_CEILING_NS`,
      and see below for why that figure and why a constant of its own. Under it
      `INTERLEAVE_INVERSION`, yellow; over it `INTERLEAVE_EXCESS`, red.
    * **The poll went first, and the late row is anything else** — that row
      waited in the socket hop *and* the writer hop, and the poll queued in
      neither. `INTERLEAVE_INVERSION_POLLER`, yellow, at any magnitude; the
      argument is the section below.
    * **The REST depth snapshot against a WS frame, either way round** — it
      sends on `writer_tx`, the same hop the parser feeds, so the two rows do
      meet in one FIFO and the writer hop cancels: what is left to separate them
      is the socket hop, `CROSS_STREAM_TOLERANCE_NS`, with the same two verdicts
      as above. In the direction where the snapshot is itself the late row the
      honest figure is nearer zero than that; the last of the alternatives below
      says why that is not exploited.

    # Why the direction decides, when the two streams do not change

    An inversion says the row written second was stamped first, i.e. that it
    waited longer than the row that overtook it. Write `t` for the stamps and
    `W` for the moments the two reached `Writer::write`; then `W_first <
    W_late`, `t_first <= W_first`, and the magnitude the report prints is

        delta = t_first - t_late  <  W_late - t_late

    — the **late row's own latency**, and nothing else. What the row ahead of it
    did drops out of the arithmetic entirely. So `premiumIndex` against
    `bookTicker` is two different questions depending on which was written
    first, and answering both with the unbounded yellow granted the exemption to
    a case with no mechanism under it: a poll written *last* was not held up by
    any WS backlog, because `writer_rx` is a FIFO and a backlog delays the rows
    behind it, not a row that is on another hop entirely. Measured on the
    recording this class was built for — tiausdt, 2026-07-29, one symbol-day:
    92 inversions with the poll written first, worst 1.034730s; 39 with the poll
    written last, worst 0.003128s. Two orders of magnitude apart, and only one
    of them has a queue in it.

    Keeping the bound on the poll-written-last direction is also the only
    detector the report has for a `premiumIndex` stamp that is **not** the
    receive moment — stamped at request time, served from a cache, or copied
    from the venue's own `E`. That is a lookahead defect in any dataset built
    from these files: the element would enter the recording ahead of the moment
    it could have been known. A uniform stale offset preserves the poller's own
    monotonicity, its cadence and its coverage, the stream carries no sequence
    chain of its own, and nothing else in `check_day` reads it — so this
    inversion is where it surfaces or nowhere. Today the stamp is `Utc::now()`
    taken after the response body arrives (`binancefuturesum/mod.rs`), so the
    defect is not live; deleting the check that would catch it is a regression
    all the same, and a gate is for the code that has not been written yet.

    # Why the poll-written-first pair has no bound, when every other pair does

    **The arithmetic that produces one has no shared queue left to run on.** A
    bound here is the WS row's own wait before the writer sees it, and the poll
    skips both hops that wait happens in: the two rows meet for the first time
    in `main`'s `select!`, which takes whichever arm is ready. So the figure
    would have to be the socket hop **plus** the writer hop, divided by the rate
    those two actually drain at. Nothing measures that rate.
    Dividing by the arrival peak instead is what `SOCKET_HOP_NS` does, and
    `queue.rs` says in as many words that this yields a floor rather than a
    bound: a hop only fills when its consumer is slower than its producer, so
    during exactly the events that deepen it, residency exceeds depth ÷ arrival.
    The writer hop is worse than unmeasured — it drains at the speed of blocking
    gzip I/O, which was measured **stopping for longer than its own whole depth**
    on 2026-07-26. While it is stopped the only ceiling on a WS frame's wait is
    the stall watchdog (`--stall-timeout-min`, 5 minutes, and `0` is a deployable
    value that disarms it). "Five minutes, or unbounded by configuration" is not
    a gate; it is a formality.

    **And no failure this red exists to catch can present as a poll-written-
    first inversion.** The red's own text names the alternatives, and each has a
    detector the poller cannot mask:

    * *two recordings in one file* — prevented by the directory `flock`
      (`collector/src/lock.rs`); and if it happened anyway it would not present
      here, because two `GzEncoder`s interleaving blocks produce a member that
      does not decode at all, which is `gzip_integrity`, red. Two recordings
      that did decode would disorder every stream against every other, including
      the WS pairs that stay red at a nanosecond — and `scan_symbol_file` holds
      those to their own cursor, so a poll row landing between two of them
      cannot launder the pair — and each stream against itself.
    * *a clock step* — every producer stamps `Utc::now()`, so a step backwards
      lands inside the busiest stream first: `monotonic_violation`, red, at any
      size. The sidecar's `clock` gauge samples the same discipline every minute.
    * *a wedged writer replaying old buffers* — a replay writes rows a second
      time, in an order their own stream has already passed: `monotonic_violation`
      again. There is no code path that replays, either: `Writer::write` is
      called once per dequeued record.
    * *the poller's own rows going backwards against its own timer* — the check
      already exists and is red, because `poll_premium_index` awaits each
      response before the next tick, so two polls cannot overtake each other.
      Pinned by `test_a_premium_index_stream_going_backwards_within_itself_is_red`.
    * *a `premiumIndex` stamp that is not its receive moment* — the one failure
      that **would** have presented here, and the reason this class is only half
      of the pair. It puts the poll on the late side of the inversion, where the
      bound is kept: see the direction section above.

    So the size of a poll-written-first inversion is evidence about the depth of
    the collector's own queues, and about nothing else. It stays a finding —
    yellow, counted, with its worst delta and that occurrence's line in the JSON
    — because that depth is worth seeing: it is the only measurement anything
    makes of what those hops hold at a burst, and a wide one is the last trace
    of a burst the recording only just survived.

    # The alternatives, and why not

    **A larger derived bound (10s poll interval + the two capacities).** The
    poll interval bounds nothing: the inversion is the WS side's wait, and the
    poller's cadence has no term in it. The capacities need the drain rate the
    paragraph above says nobody has, and picking one converts an unmeasured
    number straight into a hard build refusal on burst days — the same failure
    being fixed, moved to a larger magnitude. Revisit it the day the parser's
    and the writer's throughputs are measured; of the first, `queue.rs` says in
    as many words that nothing has.

    **A derived bound for the poll-written-last direction**, in the shape
    `SOCKET_HOP_NS` has: `POLLER_QUEUE_CAPACITY / PEAK_MSG_PER_S` = 51.2ms. It
    has the form of arithmetic and none of the content — that rate is the
    *socket's* arrival peak, and nothing arrives on this hop at 20 000/s; it
    takes a dozen elements once every ten seconds and drains at whatever
    `main`'s loop rate happens to be, which nothing measures either. Minting a
    number nobody measured is the move being reversed here, so that direction
    gets a ceiling read off an observation instead — `POLLER_HOP_CEILING_NS`,
    which is where the observation and the reason for a separate constant are
    written down.

    **Red at any magnitude for the poll-written-last direction**, on the grounds
    that the hop cannot fill. Rejected by measurement: 39 of them in one real
    symbol-day, worst 3.128ms. `select!` picks at random between arms that are
    both ready, so a poll losing a few turns to a draining backlog is ordinary,
    and a gate that fires on it would be back to calling the collector's own
    design corruption.

    **A bounded tolerance in the shape of `MAX_SUPPRESSED_GAP_FACTOR`**, which
    caps how far a cadence gap may be excused. That cap discriminates: a hole
    10x its channel's limit is not a quiet book, so *size* separates the excuse
    from the failure it could hide. Here it does not, per the list above — every
    failure the red is for is caught by a check the poller pair cannot reach,
    at any size. A cap that can only ever fire on a healthy recording is not a
    cap worth having.

    **Reclassifying `depthSnapshot` the same way**, on the grounds that its
    bound divides by the same unmeasured rate. Deliberately not done. Its row
    shares the writer hop, so one queue and one measured rate still describe the
    pair; the bound has ~20% headroom over the derived floor; the fetches are
    throttled to 100/min against the poller's 8640 rows a day per symbol; and
    the worst inversion ever observed on that pair is 134us against the poller's
    1.045s on the first burst it met. Widening a red that has never fired
    falsely, on an argument rather than an observation, is the move this file
    rejects everywhere else. If a snapshot pair does go red on a day whose
    sequence chains are intact, that is the measurement, and the fix is to move
    it into this class.

    **Splitting the snapshot pair by direction too.** The asymmetry is there:
    a snapshot written *last* was enqueued on the writer hop behind the frame it
    is out of order with, and `Utc::now()` and that `send` are a few
    instructions apart, so the honest figure for that direction is nearer zero
    than to the socket hop. Not done, because it can only *tighten* a red, and
    the case for tightening is an argument rather than an observation — the
    worst that direction has ever produced is well inside the bound it already
    has. The poller split earned its complexity by being wrong in production, in
    the direction that costs a detector. This one has not.

    **A constant of its own for the fan-out pair.** Reusing
    `CROSS_STREAM_TOLERANCE_NS` there is a ceiling, not a derivation, and the
    honest figure is smaller: the un-shared segment is a scheduler slice, not a
    queue, and the worst ever recorded is 15.153us (btc-usd, 20260804, against
    489ns on sol-usd the same day). What argues for a figure of this order all
    the same is that the slice has no ceiling of its own — a socket task waits
    between its stamp and its enqueue for as long as the runtime is busy
    elsewhere, and the longest single thing on that runtime is the parser
    draining a full socket hop, which is exactly what `SOCKET_HOP_NS` measures.
    So the number is at least the right order, and it is a number this file
    already measures and already pins to the Rust. Minting a tighter one off a
    single day's observation is the move rejected two paragraphs up; the day
    something measures the runtime's worst poll delay, split it. Until then the
    cost is named: a genuine fan-out inversion between 15us and 1s is yellow.
    It would have to come from a defect that leaves both `monotonic_violation`
    and `gzip_integrity` clean, and none of the three the red exists for does.
    """
    if second_producer_of(exchange, prev_stream, stream) is None:
        # One producer, every hop shared and every hop a FIFO.
        return INTERLEAVE_EXCESS
    if crosses_a_hand_off_of_its_own(stream):
        # The poll is the late row: it waited on its own hop or nowhere.
        return (
            INTERLEAVE_INVERSION if delta_ns <= POLLER_HOP_CEILING_NS else INTERLEAVE_EXCESS
        )
    if crosses_a_hand_off_of_its_own(prev_stream):
        # The poll went first; the late row waited in both hops it skipped.
        return INTERLEAVE_INVERSION_POLLER
    # Two producers that do meet in a shared FIFO: the snapshot and a WS frame
    # either way round (the writer hop cancels, the socket hop is left), or two
    # channel sockets of a fan-out venue (both hops cancel, the scheduler slice
    # is left). One verdict path, because one bound is being applied.
    return INTERLEAVE_INVERSION if delta_ns <= CROSS_STREAM_TOLERANCE_NS else INTERLEAVE_EXCESS


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------


def utc_today() -> str:
    """Today's UTC day as `YYYYMMDD` — the day whose files are still open."""
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def worst(verdicts) -> str:
    """The most severe verdict in `verdicts`; `green` for none at all."""
    out = GREEN
    for v in verdicts:
        if _SEVERITY_ORDER[v] > _SEVERITY_ORDER[out]:
            out = v
    return out


def iso(ts: Optional[int]) -> str:
    """`local_ts` as an ISO instant, keeping all nine digits. Display only."""
    if ts is None:
        return "-"
    whole, nanos = divmod(int(ts), SEC_NS)
    stamp = datetime.fromtimestamp(whole, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{stamp}.{nanos:09d}Z"


def fmt_dur(nanos: int) -> str:
    """A duration as `12.345s`, computed with integers only."""
    whole, rest = divmod(int(nanos), SEC_NS)
    return f"{whole}.{rest // 1_000_000:03d}s"


def fmt_short(nanos: int) -> str:
    """A duration at a scale that survives being small — `134.021us`.

    `fmt_dur` rounds to the millisecond, which renders the whole interleave
    check as `0.000s` and hides the number the operator needs. Integers only,
    and lossless below a second.
    """
    n = int(nanos)
    if n >= SEC_NS:
        return fmt_dur(n)
    if n >= 1_000_000:
        ms, rest = divmod(n, 1_000_000)
        # Three digits normally; all six only when the tail would be lost.
        return f"{ms}.{rest:06d}ms" if rest % 1_000 else f"{ms}.{rest // 1_000:03d}ms"
    if n >= 1_000:
        return f"{n // 1_000}.{n % 1_000:03d}us"
    return f"{n}ns"


# ---------------------------------------------------------------------------
# dataset profile: what a venue is expected to have recorded
# ---------------------------------------------------------------------------

#: Venue families, which decide how a frame is classified into a stream.
HYPERLIQUID = "hyperliquid"
BINANCE = "binance"
BYBIT = "bybit"
LIGHTER = "lighter"
EXTENDED = "extended"
PARADEX = "paradex"

#: `aster` is deliberately NOT a family of its own. The venue is a literal
#: Binance USD-M clone — combined-stream `sym@channel` envelope, the same
#: `data.e` event names, the same `pu` chain, and the same bare elements from
#: its own `GET /fapi/v1/premiumIndex` — verified frame by frame against a real
#: recording (2026-08-04, `/opt/hft-collector/data/aster`: 0 unclassified frames
#: over three symbol-days under the Binance rules). A second family would have
#: been a copy of those rules that could drift from them silently. What Aster
#: does NOT share is its expectations: see `expected_streams`.
_FAMILY = {
    "lighter": LIGHTER,
    "hyperliquid": HYPERLIQUID,
    "binancefutures": BINANCE,
    "binancefuturesum": BINANCE,
    "binancefuturescm": BINANCE,
    "binance": BINANCE,
    "binancespot": BINANCE,
    "aster": BINANCE,
    "bybit": BYBIT,
    "extended": EXTENDED,
    "paradex": PARADEX,
}

#: `collector/src/main.rs` matches `"binancefutures" | "binancefuturesum"` onto
#: the one USD-M backend but stamps the operator's spelling into `session_start`
#: verbatim. Canonicalised here, in one place, so the same recorded bytes are
#: not buildable or unbuildable depending on which word was typed on the command
#: line. The word as recorded stays in the report as `exchange_as_recorded`.
_EXCHANGE_ALIAS = {
    "binancefutures": "binancefuturesum",
}


def canonical_exchange(exchange: str) -> str:
    """The backend name a `session_start.exchange` value denotes."""
    return _EXCHANGE_ALIAS.get(exchange, exchange)

#: Largest hole in a channel that is still the feed working normally, in
#: nanoseconds. Derived from the cadences measured on mainnet 2026-07-25 and
#: recorded in `collector/README.md`:
#:
#:   * `l2Book` slow  5.41s x 10  — a throttled snapshot feed, so K can be tight
#:   * `l2Book` fast  0.54s x 10
#:   * `bbo`          0.14s x 100 — event-driven and bursty; a quiet book simply
#:                    stops changing, so a small K would flag every calm minute
#:   * `trades`       0.60s x 200 — droughts are legal, two minutes is not
#:
#: Binance and Bybit cadences have never been measured in this repository (the
#: README's capacity table covers HL and Bybit volume only), so rather than
#: inventing a cadence they get a flat absolute limit. It is deliberately loose:
#: this check exists to name reconnect-sized holes, not to grade liquidity.
#:
#: The two index/funding feeds are the exception on both counts, because both
#: were measured on 2026-07-28 (`collector/README.md`, "Index, oracle and
#: funding") and both are **periodic**: a frame arrives whether or not anything
#: changed — consecutive ones are frequently byte-identical — so their silence
#: is evidence on its own and needs no liveness witness, exactly like the
#: throttled `l2Book` feeds. They take the same K=10:
#:
#:   * `activeAssetCtx`  1.018s x 10  (n=292 over 300s, Hyperliquid mainnet)
#:   * `markPriceUpdate` 1.000s x 10  (n=298 over 300s, Binance COIN-M. USD-M
#:                       is no longer subscribed to this stream at all — the
#:                       venue stopped serving the whole markPrice class there,
#:                       measured 2026-07-28 from two network paths — but the
#:                       limit still applies to any UM day recorded while it
#:                       was, and to COIN-M, which serves it today)
#:
#: Both are written as a round 10s rather than as the cadence x 10 to the
#: decimal. Five minutes of each measures a median well and a tail not at all
#: (the widest interval anyone has watched for is 1.253s, over 45s of
#: `activeAssetCtx`), so a limit carrying two decimal places would be claiming
#: precision that was never observed; 10x a 1/s heartbeat is eight times that
#: worst interval either way.
#:
#: `premiumIndex` is the third periodic feed and the one exception to "measured":
#: it is not a venue stream at all but the collector's own REST poller, whose
#: 10s period is a constant in `binancefuturesum::PREMIUM_INDEX_INTERVAL`. It
#: takes the same K=10, which is 100s. That K is doing different work here —
#: with the cadence exact by construction the only jitter is a skipped cycle, and
#: a poll that fails is ordinary and deliberately costs one sample (see the error
#: policy on `poll_premium_index`). 100s is nine consecutive failures: past any
#: single venue hiccup, and a third of the 30 failures at which the collector
#: writes `poller_degraded` to the sidecar, so the two signals report in order
#: rather than racing.
#:
#: Note what this makes the periodic feeds: at 10s (and 100s for a 10s poller,
#: the same ten periods) they are the tightest cadence checks on either socket —
#: finer than Binance's 30s guesses and than `bbo`'s 14s. That is the point. A
#: periodic feed is the one channel whose silence means something without a
#: second opinion.
MAX_GAP_NS = {
    (HYPERLIQUID, "l2Book_slow"): 54 * SEC_NS,
    (HYPERLIQUID, "l2Book_fast"): 5_400_000_000,
    (HYPERLIQUID, "bbo"): 14 * SEC_NS,
    (HYPERLIQUID, "trades"): 120 * SEC_NS,
    (HYPERLIQUID, "activeAssetCtx"): 10 * SEC_NS,
    (BINANCE, "bookTicker"): 30 * SEC_NS,
    (BINANCE, "depthUpdate"): 30 * SEC_NS,
    # `trade` has NO entry, for the same reason Paradex's `trades` has none, and
    # now on this venue's own measurement rather than by analogy. A print is an
    # event; a thin symbol simply does not print for minutes at a time, and no
    # number separates that from a stream that died.
    #
    #   * 23 Binance USDC perpetuals, 10.5h of 2026-08-27: 166 holes past the
    #     120s this line used to hold, worst 417.8s, spread over 11 of the 23.
    #     A full day of the same set is ~380 findings nobody can act on.
    #   * DATAIPUSDC, the thinnest symbol of that set, went 823 of 1440 minutes
    #     of 2026-08-26 with no print at all (public 1m klines, `count`), in
    #     runs of up to 18 minutes. A limit that does not flag an 18-minute
    #     drought is not a limit; one that does flags a healthy market daily.
    #
    # A `LIVENESS_REFERENCE` would not save it either: the suppression ceiling
    # is 10x the limit, i.e. 1200s, and the worst legal drought measured is
    # 1080s — a 10% margin is not a check, it is a coin toss. What still catches
    # a dead tape is the stream set: no print at all for a whole day is
    # `missing_optional` (`missing_required` under `book-v1`).
    #
    # What is NOT caught, and is the price of this line: a tape that dies
    # halfway through a day while the socket lives. Nothing here can see that
    # for an event-driven feed, and a limit tuned to pretend otherwise buries
    # the finding among hundreds of quiet-market ones.
    (BINANCE, "markPriceUpdate"): 10 * SEC_NS,
    (BINANCE, "premiumIndex"): 100 * SEC_NS,
    (BYBIT, "orderbook"): 30 * SEC_NS,
    (BYBIT, "publicTrade"): 120 * SEC_NS,
    (LIGHTER, "order_book"): 30 * SEC_NS,
    (LIGHTER, "ticker"): 20 * SEC_NS,
    (LIGHTER, "trade"): 120 * SEC_NS,
    (LIGHTER, "market_stats"): 30 * SEC_NS,
    # Extended cadences have never been measured over a full day in this repo, so
    # like Binance's and Bybit's these are deliberately loose flat limits in the
    # spirit "name reconnect-sized holes, not grade liquidity".
    #
    # `orderbook` is the subtle one. A book delta only arrives when *that* market's
    # book changes, so a thin market goes quiet like an event-driven feed — a 25s
    # smoke on 2026-08-03 measured CHZ-USD and TURBO-USD going ~37s between book
    # frames while BTC-USD ran gaplessly. The floor under that quiet is the venue's
    # full-book **re-snapshot every ~60 s** (README, and the same fact the
    # reconnect loop relies on), so the limit is 90s = 1.5x that floor: it tolerates
    # a quiet market (which still re-snapshots inside a minute) yet a firehose that
    # actually died shows >90s gaps on *every* market including the majors, which
    # then flag. A tighter limit painted the first healthy recording yellow.
    #
    # `trades` droughts are legal; `mark` runs ~0.7 frames/s (measured gapless at
    # ~1.5s); `funding` is roughly one frame a minute (perpetual-only, so a short
    # run may record none). Tighten once a full day of a thin market has been
    # recorded and looked at.
    (EXTENDED, "orderbook"): 90 * SEC_NS,
    (EXTENDED, "trades"): 120 * SEC_NS,
    (EXTENDED, "mark"): 30 * SEC_NS,
    (EXTENDED, "funding"): 300 * SEC_NS,
    # Paradex, measured 2026-08-04 over 8.1h of a live day on four markets
    # (BTC, SOL and the thin NEAR, ONDO), which is the first venue here whose
    # limits were set from its own recording rather than from a smoke test:
    #
    #   * both books   0.20s median on BTC, 1.9s on NEAR; worst hole 173.7s
    #     (thin markets) and 107.9s (BTC). Nominally `@100ms`, but the refresh
    #     is a ceiling on updates, not a heartbeat: a book that does not change
    #     sends nothing. 300s is ~1.7x the worst hole seen, and it flagged
    #     nothing on any of the four markets.
    #   * `funding`    5.00s exactly, worst 18.5s, identical across all four
    #     markets. Periodic — a frame arrives whether or not anything changed —
    #     so, like `activeAssetCtx` and `markPriceUpdate`, its silence is
    #     evidence on its own and it takes a tight K: 60s is 12 periods and
    #     3.2x the worst interval observed.
    #   * `bbo`        event-driven and the noisiest thing here: 13 holes past
    #     300s on NEAR and 20 on ONDO in those 8.1h, worst 41 minutes, every
    #     one of them with both books running gaplessly across it. The limit is
    #     therefore paired with a `LIVENESS_REFERENCE` rather than loosened to
    #     the point of measuring nothing — see there.
    #
    # `trades` has NO entry, on purpose: NEAR printed 15 times in those 8.1h
    # (worst hole 1h33m) and SOL 37 (worst 1h43m). A limit that does not flag
    # those is not a limit; one that does flags a healthy thin market every day.
    # This feed has no cadence, and saying so is more honest than a round number
    # nobody could act on. `gap_limit` returning `None` skips the check.
    (PARADEX, "book_snapshot"): 300 * SEC_NS,
    (PARADEX, "book_interactive"): 300 * SEC_NS,
    (PARADEX, "bbo"): 300 * SEC_NS,
    (PARADEX, "funding"): 60 * SEC_NS,
}

#: Lighter's four limits above are round numbers far above a measurement, and
#: the honesty about which is the point. Measured on mainnet 2026-07-28 over a
#: single 40s window on ETH and BTC — two of the venue's most liquid markets:
#:
#:   * `order_book`    0.050s median, 0.25s worst — diffs batched at ~50ms
#:   * `ticker`        0.0095s median, 0.38s worst — event-driven per engine nonce
#:   * `trade`         0.07-0.18s median, 1.66s worst — per block, ~500ms
#:   * `market_stats`  0.12-0.25s median, 1.77s worst
#:
#: Forty seconds measures a median well and a tail not at all, and nothing here
#: has been watched on an illiquid market or through a quiet hour. So rather
#: than cadence x K — which would claim a precision the window cannot support —
#: these are flat limits an order of magnitude above the worst interval seen,
#: in the spirit the Binance ones are set in: this check names reconnect-sized
#: holes, it does not grade liquidity. Tighten them once a full day of a thin
#: market has been recorded and looked at.

#: For an event-driven channel, the steadier channel on the SAME socket whose
#: silence decides whether its own silence meant anything. First name present in
#: the recording wins; if none was recorded the channel keeps its `MAX_GAP_NS`
#: limit as its only liveness signal.
#:
#: Why `bbo` needs one: it fires on a change of the top of book and on nothing
#: else, so a thin symbol in a quiet hour emits nothing for tens of seconds while
#: the connection is perfectly healthy. Measured 2026-07-26: 26 such holes on ENA
#: alone in half a day (14-37s), every one of them with `l2Book_fast` running
#: gapless across it. A limit alone cannot tell that from an outage, and a gate
#: whose yellows are mostly noise is a gate nobody reads — which the design
#: document's acceptance line rules out.
#:
#: Why `l2Book_fast` first and `l2Book_slow` second: both are throttled snapshot
#: feeds and arrive whether or not the book changed, so their cadence measures
#: the socket rather than the market. `fast` (0.54s) resolves a hole ten times
#: finer than `slow` (5.4s), so it is preferred where it was recorded; `slow` is
#: the fallback for a legal `--hl-l2-modes slow` run.
#:
#: Binance's `bookTicker` is event-driven too, and it IS listed now — the
#: measurement the earlier note said was missing has been made. It used to read
#: "no false positive has been observed; adding a reference before the
#: measurement would be inventing one", which was true when the recorded
#: symbols were majors. It stopped being true the moment thin symbols were
#: recorded:
#:
#:   * five thin USD-M symbols, full day 2026-08-24 (`binancefuturesum-c`):
#:     59 `bookTicker` holes past 30s (0, 1, 14, 19, 25 per symbol), worst
#:     75.3s — and `depthUpdate` ran with **zero** holes past its own 30s limit
#:     on all five, witnessing 59 of the 59;
#:   * 23 Binance USDC perpetuals, 10.5h of 2026-08-27: 141 holes, worst 77.9s,
#:     i.e. ~320 findings a day on one instance.
#:
#: `@depth@0ms` is the reference for the reason Lighter's `order_book` is: it is
#: the densest thing on the same socket (2.6M vs 1.25M frames over those five
#: symbol-days) and it is a diff feed, so it moves on any change anywhere in the
#: book while the touch can sit still. Every hole measured is far inside the
#: 300s suppression ceiling (10x its own limit), so a genuine socket outage —
#: which takes depth with it — still flags.
#:
#: It stays a single-name tuple deliberately. `trade` would be the obvious
#: second, and it is exactly the wrong one: the tape is the sparsest channel
#: here, quiet for minutes on a thin symbol, so it can witness nothing that
#: depth cannot. `depthUpdate` is `optional` under `mode-a-v1`, so a recording
#: without it simply has no witness and keeps its warnings — `liveness_witness`
#: fails closed on an absent reference.
#:
#: The 1/s index feeds would make fine references — they are periodic, they are
#: on the same socket, and `markPriceUpdate` is the first steady channel Binance
#: has here. They are deliberately not listed anyway. A reference can only ever
#: remove a warning, and adding one would loosen a check that has not been shown
#: to be noisy, on the strength of a stream no recording older than 2026-07-28
#: contains. `bbo`'s references are there because 26 false positives were counted
#: first; this one has no such measurement behind it yet.
#: Lighter's `ticker` is the same kind of channel as `bbo` and gets the same
#: treatment: it fires on a change of the touch and on nothing else, so a
#: market whose top of book stops moving emits nothing while the connection is
#: healthy. `order_book` is the reference because it is the steadiest thing on
#: the same socket — the venue batches book diffs on a ~50ms timer, so its
#: cadence measures the socket far more than the market — with `market_stats`
#: behind it, which carries mark price, index price and funding and therefore
#: moves even when the book does not.
#:
#: Unlike `bbo`'s, this pairing is a prediction rather than a response to
#: counted false positives: no full day of this venue has been recorded yet. It
#: is here because the alternative — waiting for the noise — means shipping a
#: gate whose first day is yellow for a reason nobody can act on. Revisit it
#: with a day's evidence.
#:
#: Paradex's `bbo` is the same channel as Hyperliquid's, and it is here for the
#: same reason Hyperliquid's is: counted false positives, not a prediction.
#: Measured 2026-08-04 over 8.1h — 13 holes past the 300s limit on
#: NEAR-USD-PERP and 20 on ONDO-USD-PERP, worst 41 minutes, with both books
#: running across every one of them (their own worst hole on those two markets
#: was 173.7s, inside their limit). Left alone that is ~33 yellows a day per
#: thin market for a top of book that simply stopped moving.
#:
#: Both books are references because both are throttled `@100ms` feeds on the
#: same socket, so their cadence measures the socket rather than the market, and
#: either one running gaplessly settles the question. `book_snapshot` is first
#: only because it is the plain API book: `book_interactive` carries the
#: RPI-inclusive quotes and is the more market-dependent of the two.
LIVENESS_REFERENCE = {
    (BINANCE, "bookTicker"): ("depthUpdate",),
    (HYPERLIQUID, "bbo"): ("l2Book_fast", "l2Book_slow"),
    (LIGHTER, "ticker"): ("order_book", "market_stats"),
    (PARADEX, "bbo"): ("book_snapshot", "book_interactive"),
}


@dataclass(frozen=True)
class Expected:
    """The stream set one symbol of this venue must (or may) contain.

    Three classes, and the third is not a quieter second:

    * `required` — the dataset cannot be built without it. Absent: red.
    * `optional` — recorded on purpose and its absence costs something, but
      nothing mode A reads. Absent: yellow.
    * `informational` — checked exactly as the others are **while it is there**
      (classification, cadence, ordering), and not reported at all when it is
      not. Absent: nothing.

    The third class exists because a stream can be added to the collector after
    recordings have already been made. `@markPrice@1s` and `activeAssetCtx` were
    added on 2026-07-28; every day recorded before then lacks them by
    construction, and no rerun can fix that. Calling those days `missing_optional`
    would put a warning on every recording in existence at once — a gate whose
    yellows are mostly history is a gate nobody reads, which is the outcome the
    design document's acceptance line rules out. It is also a warning nobody can
    act on: Binance acks a stream name it will never serve and reports no error
    for it, so an absent `markPriceUpdate` is not always something the recording
    could have done anything about (`collector/README.md`, "Known limitations").

    What it is NOT is unknown. An unrecognised frame shape is still a yellow
    `unclassified_frame`, and the absence is still stated in the JSON as
    `missing_informational` — a fact the report reports, not a problem it raises.

    `violation` is set when the *profile* contradicts the recording
    configuration rather than the data: a legal recording that can never make
    the dataset the profile describes. It is a property of the day's
    `session_start`, not of any one symbol, and it is red.
    """

    required: tuple
    optional: tuple
    violation: Optional[str] = None
    informational: tuple = ()


def family_of(exchange: str) -> str:
    """The frame-shape family of a `session_start.exchange` value."""
    try:
        return _FAMILY[exchange]
    except KeyError:
        raise ValueError(
            f"unknown exchange {exchange!r} in session_start; "
            f"known: {', '.join(sorted(_FAMILY))}"
        ) from None


def expected_streams(profile: str, exchange: str, config: dict) -> Expected:
    """The expected stream set = recording configuration x dataset profile.

    `config` is the merged `session_start` for the day: `symbols`,
    `hl_l2_modes`, `bybit_depths`.

    Profile `mode-a-v1` is the contract of "Режим A" in the design document:
    Hyperliquid is the traded venue and Binance USD-M is the signal, whose only
    load-bearing stream is `@bookTicker`.

    Profile `book-v1` is `mode-a-v1` with one difference, and it exists because
    an instance can be recorded FOR the book. Under mode A a Binance recording
    that lost `@depth@0ms` is yellow — the dataset it feeds reads the touch and
    nothing else. For an instance whose whole reason to exist is that the venue
    publishes no book anywhere else (Binance USD-M `bookTicker` archives stop in
    2024-04 and no perpetual listed after that has one at all), the same loss is
    total: the recording can never become what it was made for, and a yellow
    the nightly gate does not escalate is how that would be found out a month
    later. So under `book-v1` the Binance families require the whole order flow
    — touch, tape and diffs.

    It changes nothing for any other venue: whatever a run carries besides the
    Binance directory is judged exactly as mode A judges it. The profile is
    per-instance (`gate-run.sh` reads `GATE_PROFILE` from the instance's own env
    file), so declaring it for one directory does not re-grade the others.
    """
    if profile not in ("mode-a-v1", "book-v1"):
        raise ValueError(f"unknown profile {profile!r}")

    exchange = canonical_exchange(exchange)

    if exchange == "hyperliquid":
        # Every declared cadence is required, and only the declared ones are:
        # `--hl-l2-modes fast` is a legal recording and must not go red for the
        # slow frames it never asked for.
        required = ["trades", "bbo"]
        for mode in config.get("hl_l2_modes") or []:
            if mode == "slow":
                required.append("l2Book_slow")
            elif mode == "fast":
                required.append("l2Book_fast")
            # "none" asks for no book at all — see the violation below.
        violation = None
        if not any(s.startswith("l2Book") for s in required):
            # The profile contradicting the recording, which is the other half
            # of "session_start x dataset profile". Mode A's traded asset IS the
            # Hyperliquid book: `hyperliquid.convert` has branches for `trades`
            # and `l2Book` only, so a recording with no cadence at all converts
            # to a feed carrying no depth event, and every backtest step blocks
            # on `no_bid`. Cheaper to say so here than after a conversion.
            violation = (
                "session_start declares hl_l2_modes=%r, i.e. no l2Book cadence "
                "at all. Mode A trades the Hyperliquid book, so this recording "
                "cannot become a mode-A dataset however cleanly it was written: "
                "the converter emits no depth event and every backtest step "
                "would block with no bid. Record with --hl-l2-modes slow or fast."
                % (config.get("hl_l2_modes") or [],)
            )
        # `activeAssetCtx` (`hyperliquid::ALWAYS_ON`) carries `ctx.oraclePx` —
        # Hyperliquid's own spot basket and the direct input to its funding —
        # plus the funding rate itself. Mode A trades the book and does not read
        # either, so it is informational; see `Expected` for why that is not the
        # same as optional.
        return Expected(
            tuple(dict.fromkeys(required)), (), violation, ("activeAssetCtx",)
        )

    if exchange in ("binancefuturesum", "binancefuturescm"):
        # Mode A depends on `@bookTicker` alone. `@trade` and `@depth@0ms` are
        # recorded (open decision 1) and their absence is worth a warning —
        # without depth the recording is permanently unconvertible into a
        # tradable asset and the `pu` check above loses its input — but the
        # backtest itself does not read them.
        #
        # Both venues' index/funding data is informational: it is not order
        # flow, mode A does not read it, and it was added to the collector after
        # recordings had already been made. It reaches the two of them by
        # different routes, which is the whole of the difference below.
        #
        # `markPriceUpdate` is COIN-M's, and still live there (measured
        # 2026-07-28). It is listed for USD-M too even though USD-M no longer
        # subscribes: days recorded while it did exist, the venue could start
        # serving the class again, and the frame routing was kept for exactly
        # that case. Listed, it is checked if it turns up; unlisted, it would be
        # an `unclassified_frame` warning instead.
        #
        # `premiumIndex` is USD-M's, and USD-M's only: the venue stopped serving
        # the markPrice class on fstream entirely, so the collector polls
        # `GET /fapi/v1/premiumIndex` instead. COIN-M has no such poller, and
        # listing it there would say a COIN-M recording could have contained
        # something it never can.
        informational = ("markPriceUpdate",)
        if exchange == "binancefuturesum":
            informational = ("markPriceUpdate", "premiumIndex")
        if profile == "book-v1":
            # See the docstring: for a recording made for the book, losing the
            # book is not a warning about a nice-to-have, it is the recording
            # failing at the one thing it was for. The tape rides with it —
            # a book with no prints against it cannot be marked out, which is
            # the only reason to want the book.
            return Expected(
                ("bookTicker", "trade", "depthUpdate"), (), None, informational
            )
        return Expected(("bookTicker",), ("trade", "depthUpdate"), None, informational)

    if exchange == "bybit":
        # Bybit is not part of the mode-A dataset, so nothing it does can make
        # that dataset red. Its declared topics are still checked, as warnings,
        # because a silently rejected subscribe batch is exactly the failure
        # this report exists to catch.
        optional = [f"orderbook.{d}" for d in config.get("bybit_depths") or []]
        optional.append("publicTrade")
        return Expected((), tuple(optional))

    if exchange == "lighter":
        # Same standing as Bybit, for the same reason: no mode-A dataset reads
        # Lighter, so nothing it does can make one red. Optional rather than
        # informational, though — its four channels are not a stream added late
        # to an old recording, they are the whole subscription set, and every
        # day since the backend existed should carry all four. An absent one is
        # a warning worth raising: this venue answers a subscription to a
        # market it does not know with an error frame and keeps the socket
        # open, so a dropped channel is invisible from the inside.
        #
        # The set is fixed rather than read from `config`, unlike Bybit's
        # depths and Hyperliquid's cadences: `lighter::CHANNELS` is a constant
        # with no flag behind it, so there is no legal recording that asked for
        # fewer.
        return Expected((), ("order_book", "ticker", "trade", "market_stats"))

    if exchange == "extended":
        # Same standing as Bybit and Lighter: no mode-A dataset reads Extended,
        # so nothing it does can make one red. The stream classes are chosen to
        # be checked while low-noise, because Extended's sockets are not uniform
        # across markets or files:
        #
        #   * `orderbook` is optional (absent → yellow). Every Extended symbol
        #     file carries a book — the plain `{market}` file the indicative
        #     one, the `{market}-rfq` sibling the executable one — so a file
        #     with no book at all is a real, actionable warning.
        #   * `trades`, `funding`, `mark` are informational (absent → silent,
        #     checked while present). Each is legitimately absent from some
        #     file: the `{market}-rfq` sibling carries only the book; `funding`
        #     is perpetual-only and roughly one frame a minute, so a spot market
        #     or a short run has none; `mark` is per-market but not on the RFQ
        #     file. Making them optional would paint every RFQ file and every
        #     spot market yellow for streams they never had — the noise a gate
        #     dies of. The per-socket liveness warn
        #     (`extended::SocketLiveness`) is the runtime guard for a feed that
        #     was expected and went silent; this offline check verifies what was
        #     recorded rather than asserting what a market must carry.
        #
        # `orderbook` covers the RFQ book too: the executable and indicative
        # books are the same shape and are separated by file, so both classify
        # as `orderbook` and an RFQ sibling with a book satisfies the optional
        # set on its own.
        return Expected((), ("orderbook",), None, ("trades", "funding", "mark"))

    if exchange == "aster":
        # Same standing as Bybit, Lighter and Extended: collect-only, so nothing
        # it does can make a mode-A dataset red. It shares Binance's FAMILY (the
        # frame shapes are identical, see `_FAMILY`) and nothing else: the
        # branch above requires `bookTicker` because mode A's signal is
        # `binancefuturesum`, and this venue is not that venue.
        #
        #   * `bookTicker` and `depthUpdate` are optional (absent → yellow).
        #     Both are in `aster::STREAMS` for every symbol, so a file carrying
        #     neither is a real, actionable warning — this venue acks a stream
        #     name it will never serve and reports no error, exactly as Binance
        #     does, so a dropped subscription is invisible from the inside.
        #   * `trade` is informational: a thin Aster perp legitimately prints
        #     nothing for hours (measured 2026-08-04: 57 prints in 8.2h on
        #     ETHFIUSDT, 77 on NEARUSDT), so a whole quiet day with none is
        #     ordinary rather than a finding.
        #   * `premiumIndex` is informational for the reason it is on USD-M: it
        #     is the collector's own REST poller, not order flow, and a day
        #     recorded before the poller existed cannot have it.
        #
        # Known and accepted: cadence limits are keyed by FAMILY, so Aster
        # inherits Binance's flat 30s/30s/120s guesses. Its markets are quieter
        # than Binance's — measured 2026-08-04, ETHFIUSDT alone produced 199
        # `bookTicker`, 51 `depthUpdate` and 34 `trade` holes past them in 8.2h
        # of a perfectly healthy recording. Those are yellow, so the gate still
        # exits 0, but they are noise. Fixing it properly means limits keyed by
        # exchange rather than by family; loosening the Binance entries instead
        # would loosen the mode-A signal venue, which is not on the table.
        return Expected((), ("bookTicker", "depthUpdate"), None, ("trade", "premiumIndex"))

    if exchange == "paradex":
        # Same standing again: no mode-A dataset reads Paradex, so nothing it
        # does may be red. The gate exited 2 on this venue before this branch
        # existed — one venue falling through to the raise below fails the whole
        # `gate@all` service, so a venue recording perfectly well took every
        # other venue's report down with it.
        #
        #   * Both books are optional (absent → yellow). Every Paradex symbol
        #     file carries both, and the PAIR is the measurement: `snapshot` is
        #     the plain API book, `interactive` the RPI-inclusive one, and the
        #     gap between them is the retail-flow thesis this venue is recorded
        #     for (`paradex::CHANNELS`). Half the pair is not the dataset.
        #   * `bbo`, `trades` and `funding` are informational — checked while
        #     present, silent when absent. Each is legitimately empty on a thin
        #     market: measured 2026-08-04 over 8.1h, NEAR-USD-PERP printed 15
        #     trades and ONDO-USD-PERP emitted no `bbo` at all for stretches of
        #     41 minutes. Making them optional would paint every thin market
        #     yellow for feeds that behaved exactly as that market trades.
        #
        # The RFQ path does not apply here: Paradex publishes its executable
        # book as a second channel on the same market, not as a sibling file,
        # which is why `book_interactive` is a stream and not a `-rfq` file.
        return Expected(
            (), ("book_snapshot", "book_interactive"), None, ("bbo", "trades", "funding")
        )

    raise ValueError(
        f"profile {profile!r} defines no expected stream set for exchange "
        f"{exchange!r}. Mode A is Hyperliquid (traded) plus Binance USD-M "
        f"(signal); spot has no converter in this repository."
    )


# ---------------------------------------------------------------------------
# reading a recording
# ---------------------------------------------------------------------------


class TruncatedRecording(Exception):
    """The gzip stream ended before its member trailer, or would not decode."""


def parse_line(raw) -> tuple:
    """`b"<local_ts_ns> <raw_venue_json>"` -> `(int, dict)`.

    Split on the FIRST space only: the payload is raw JSON and contains plenty
    of its own. The timestamp is parsed with `int`, which is exact at any
    magnitude — `float` would lose the last two digits of a nanosecond stamp.
    """
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    ts_str, _, payload = raw.partition(" ")
    return int(ts_str), json.loads(payload)


def iter_gz_lines(path) -> Iterator[bytes]:
    """Yields every line of a possibly multi-member gzip file.

    Raises `TruncatedRecording` at the point decoding fails, having already
    yielded everything that decoded — a truncated file is still evidence, and
    the checks that do not need the tail still run over what was read.
    """
    with gzip.open(path, "rb") as f:
        while True:
            try:
                line = f.readline()
            except (EOFError, gzip.BadGzipFile, zlib.error, OSError) as error:
                raise TruncatedRecording(str(error) or type(error).__name__) from error
            if not line:
                return
            yield line


@dataclass
class Gap:
    start_ts: int
    end_ts: int
    duration_ns: int
    #: The lifecycle record that accounts for the hole, if the sidecar has one.
    explained_by: Optional[str] = None
    #: Why this hole is not reportable at all — set when another stream on the
    #: same socket ran across it without one, i.e. nothing was lost. Distinct
    #: from `explained_by`, which says why a real hole happened.
    suppressed_by: Optional[str] = None

    def overlaps(self, other: "Gap") -> bool:
        """Whether the two holes share any instant. Touching ends count.

        Inclusive on purpose: an outage stops both feeds at approximately, not
        exactly, the same moment, and the direction to be wrong in is reporting.
        """
        return self.start_ts <= other.end_ts and other.start_ts <= self.end_ts

    def as_json(self) -> dict:
        return {
            "start_local_ts": int(self.start_ts),
            "end_local_ts": int(self.end_ts),
            "duration_ns": int(self.duration_ns),
            "explained_by": self.explained_by,
            "suppressed_by": self.suppressed_by,
        }


@dataclass
class StreamStat:
    count: int = 0
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None
    gaps: list = field(default_factory=list)
    gap_count: int = 0

    def observe(self, ts: int, max_gap_ns: Optional[int]) -> None:
        if self.first_ts is None:
            self.first_ts = ts
        elif max_gap_ns is not None and self.last_ts is not None:
            delta = ts - self.last_ts
            if delta > max_gap_ns:
                self.gap_count += 1
                if len(self.gaps) < MAX_GAPS_RECORDED:
                    self.gaps.append(Gap(self.last_ts, ts, delta))
        self.last_ts = ts
        self.count += 1

    def gaps_truncated(self) -> bool:
        """Whether `MAX_GAPS_RECORDED` dropped holes this stream really had.

        A truncated list cannot prove another stream's hole does not overlap
        one of the holes it stopped keeping, so it may not suppress anything.
        """
        return self.gap_count > len(self.gaps)

    def suppressed_gap_count(self) -> int:
        return sum(1 for g in self.gaps if g.suppressed_by is not None)

    def as_json(self) -> dict:
        return {
            "count": self.count,
            "first_local_ts": None if self.first_ts is None else int(self.first_ts),
            "last_local_ts": None if self.last_ts is None else int(self.last_ts),
            # The raw count of over-limit holes. `suppressed_gap_count` of them
            # were disproved by another stream and reach no issue; the
            # measurement stays here either way.
            "gap_count": self.gap_count,
            "suppressed_gap_count": self.suppressed_gap_count(),
            "gaps": [g.as_json() for g in self.gaps],
        }


@dataclass
class FileScan:
    path: str
    symbol: str
    exchange: str
    lines: int = 0
    truncated: bool = False
    truncation_error: Optional[str] = None
    malformed: int = 0
    malformed_example: Optional[str] = None
    unclassified: int = 0
    #: `local_ts` went backwards WITHIN one stream: one producer, so this is a
    #: clock step or two recordings in one file. Red.
    monotonic_violation: Optional[dict] = None
    #: Two different streams written out of `local_ts` order, keyed by which of
    #: `INTERLEAVE_CHECKS` the pair is — see `interleave_kind` for the three
    #: classes and for why one of them has no bound. One folded record per class
    #: per file, so a snapshot overtake and a poll overtake in the same file are
    #: reported as the two different things they are; the record names the worst
    #: occurrence of its class, which is the one the verdict is made of
    #: (`_note_inversion`).
    interleave: dict = field(default_factory=dict)
    streams: dict = field(default_factory=dict)
    sequence_breaks: dict = field(default_factory=dict)
    #: stream -> the first few breaks, as the interval frames were lost over.
    sequence_break_gaps: dict = field(default_factory=dict)

    def as_json(self) -> dict:
        return {
            "file": self.path,
            "lines": self.lines,
            "truncated": self.truncated,
            "malformed_lines": self.malformed,
            "unclassified_frames": self.unclassified,
            "monotonic_violation": self.monotonic_violation,
            **interleave_json(self.interleave),
            "sequence_breaks": dict(self.sequence_breaks),
            "sequence_break_examples": {
                stream: [g.as_json() for g in gaps]
                for stream, gaps in sorted(self.sequence_break_gaps.items())
            },
            "streams": {name: s.as_json() for name, s in sorted(self.streams.items())},
        }


#: The keys a `GET /fapi/v1/premiumIndex` element must all carry to be one.
#:
#: Four of the eight the venue sends (captured 2026-07-28: `symbol`, `markPrice`,
#: `indexPrice`, `estimatedSettlePrice`, `lastFundingRate`, `interestRate`,
#: `nextFundingTime`, `time`). Not all eight, so that the venue adding or
#: retiring a field does not silently turn the whole feed into
#: `unclassified_frame`; not one or two, so that no other bare object in these
#: files can collide with it.
_PREMIUM_INDEX_KEYS = frozenset(
    {"symbol", "markPrice", "indexPrice", "lastFundingRate"}
)


def classify(family: str, obj: dict) -> Optional[str]:
    """The stream a recorded frame belongs to, or `None` if unrecognised.

    Frame shapes come from the converters that read these files:
    `hyperliquid.py` (`channel`, and `data.fast` telling the two `l2Book`
    cadences apart) and `binancefutures.py` (combined-stream envelope, `data.e`).
    Bybit's topic string is `orderbook.<depth>.<symbol>` / `publicTrade.<symbol>`
    (`collector/src/bybit/mod.rs` routes on its last segment).

    The two WebSocket index/funding feeds need no rule of their own and
    deliberately do not get one: Hyperliquid's `activeAssetCtx` names itself in
    `channel`, and Binance's `markPriceUpdate` in `data.e`, so both fall out of
    the rules above as streams in their own right — including the dex-prefixed
    `xyz:GOLD` form, whose coin only ever appears in the payload the routing
    already keyed on. Pinned by
    `test_the_index_and_funding_frames_classify_as_their_own_streams` over frames
    captured from mainnet, because "happens to work" is one whitelist away from
    a whole feed being counted as `unclassified_frame`.

    `premiumIndex` is the one that does need a rule, because it answers to
    nothing the existing ones read. USD-M stopped serving the markPrice class of
    public streams (measured 2026-07-28 from two independent network paths), so
    its index and funding data now arrive over REST from the collector's own
    poller and are written as the venue's array elements, verbatim: no
    combined-stream envelope, so no `data` and no `e`, and the symbol under
    `symbol` rather than `s`.

    The discriminator is structural rather than semantic, on purpose.
    `markPriceUpdate` carries a mark price, an index price and a funding rate
    too — the same three quantities under one-letter names — so "looks like
    index data" would relabel the whole COIN-M feed. What actually separates
    them is that a WS frame names its event in `e` and a REST element does not,
    and that the four keys required here are the venue's own spellings, which no
    stream envelope uses.
    """
    if family == HYPERLIQUID:
        channel = obj.get("channel")
        if channel == "l2Book":
            data = obj.get("data") or {}
            return "l2Book_fast" if data.get("fast") else "l2Book_slow"
        return channel if isinstance(channel, str) else None

    if family == BINANCE:
        data = obj.get("data")
        if isinstance(data, dict):
            event = data.get("e")
            return event if isinstance(event, str) else None
        # The REST depth snapshot the collector pulls after a `pu` break is
        # written into the symbol file bare, with no stream envelope
        # (`binancefuturesum/mod.rs`); the converter recognises it the same way.
        if "lastUpdateId" in obj:
            return "depthSnapshot"
        if not _PREMIUM_INDEX_KEYS - obj.keys() and "e" not in obj:
            return "premiumIndex"
        return None

    if family == BYBIT:
        topic = obj.get("topic")
        if isinstance(topic, str):
            parts = topic.split(".")
            return ".".join(parts[:-1]) if len(parts) > 1 else topic
        return None

    if family == LIGHTER:
        # `order_book:0`, `ticker:1`, `trade:0`, `market_stats:0`. The tail is
        # the **market id**, and it must not become part of the stream name:
        # every symbol file would then carry a stream with no cadence limit and
        # no expectation, and one instance's streams would not be comparable
        # with another's. The symbol the frame belongs to is the file it is in
        # — `collector/src/lighter/mod.rs` resolves the id at subscribe time,
        # which is the only place the mapping exists.
        channel = obj.get("channel")
        if isinstance(channel, str):
            return channel.split(":", 1)[0]
        return None

    if family == EXTENDED:
        # The envelope is `{type, data, error, ts, seq}`
        # (`collector/src/extended`). The four streams are told apart
        # structurally, in the order that keeps each unambiguous:
        #   * book — `type` is SNAPSHOT or DELTA, `data` a single object. The
        #     RFQ executable book is byte-identical in shape and kept apart by
        #     *file* (`{market}-rfq`), not by any field, so it classifies as
        #     `orderbook` too — correctly, since the file is what separates them.
        #   * mark — `type` is `MP`.
        #   * trades — no `type`; `data` is an *array* of prints.
        #   * funding — no `type`; `data` is an object carrying the funding
        #     rate `f` (mark's object carries `p`, so `f` is the discriminator).
        typ = obj.get("type")
        if typ in ("SNAPSHOT", "DELTA"):
            return "orderbook"
        if typ == "MP":
            return "mark"
        data = obj.get("data")
        if isinstance(data, list):
            return "trades"
        if isinstance(data, dict) and "f" in data:
            return "funding"
        return None

    if family == PARADEX:
        # JSON-RPC: `{"params": {"channel": "<type>.<MARKET>[.<suffix>]",
        # "data": {...}}}`. The market is the SECOND dot-segment — a Paradex
        # market is `BTC-USD-PERP`, dashes and no dots — which is what
        # `collector/src/paradex/mod.rs::route` keys the file on, and the type
        # is the first. Anything without a `params.channel` is an ack, a venue
        # error or a pong, and the same call is made here as there: not a
        # stream.
        #
        # The mapping is a whitelist rather than the channel name verbatim,
        # because it is not the identity: `funding_data` is shortened, and
        # `order_book` splits in two. Those two ARE different books — `snapshot`
        # is the plain API book, `interactive` the RPI-inclusive one, and the
        # difference between them is the whole reason this venue is recorded
        # (`paradex::CHANNELS`) — so collapsing them would hide either going
        # silent. The feed word is inside the third segment, ahead of the
        # `@15@100ms` depth and refresh.
        #
        # A channel the collector starts recording and this report has not been
        # taught (the full-depth `deltas` feed, reserved for a core set) falls
        # through to `None` and is counted as `unclassified_frame`. That yellow
        # is the signal to teach this function; a name derived from the channel
        # would instead have produced a stream with no cadence limit and no
        # expectation, which is the same absence with nothing to read.
        params = obj.get("params")
        channel = params.get("channel") if isinstance(params, dict) else None
        if not isinstance(channel, str):
            return None
        parts = channel.split(".")
        head = parts[0]
        if head == "order_book":
            feed = parts[2].split("@", 1)[0] if len(parts) > 2 else ""
            return f"book_{feed}" if feed in ("snapshot", "interactive") else None
        if head == "funding_data":
            return "funding"
        return head if head in ("bbo", "trades") else None

    return None


#: The bucket an unrecognised frame is ordered under. It has no stream, but it
#: is still a line in the file, so leaving it out of the ordering check would
#: let a whole unknown feed be written out of order unnoticed. Lumping every
#: unrecognised shape together is deliberate: `unclassified_frames` already
#: reports them, and one bucket cannot be worse than none.
UNCLASSIFIED = "(unclassified)"


def _note_inversion(
    record: Optional[dict],
    lineno: int,
    prev_stream: str,
    stream: str,
    prev_ts: int,
    ts: int,
) -> dict:
    """Folds one out-of-order pair into an O(1) summary of its kind.

    The **worst** occurrence is kept whole — its line, its two streams and both
    stamps — and everything else only moves the counter. It used to be the first
    occurrence, and that was survivable while each magnitude had a class of its
    own: the one pair beyond the bound was alone in `interleave_excess` and so
    was its own anchor. It stopped being survivable when a class started folding
    magnitudes together: on tiausdt 2026-07-29 the record then read
    `worst 1.034s; first at line 741: depthUpdate <ts> -> premiumIndex <ts>`,
    where line 741 is 00:00:15, those two stamps are 119us apart, and the 1.034s
    event the finding exists for — line 1069889, 13:30:03 — appeared nowhere in
    the report. A sentence whose number and whose location come from different
    events is worse than either alone, and the number is the one every consumer
    reads.

    Strictly greater, so the *first* pair to reach the worst magnitude keeps the
    record: two identical deltas must not depend on which was scanned second.
    Onset is what is given up. Nothing printed or consumed claimed it: the
    detail line says "worst", `max_delta_ns` is what decides the verdict, and
    `violations` already says whether this was one event or a file full of them.
    """
    delta = prev_ts - ts
    if record is None:
        # Declared whole, so the JSON keeps one key order whichever occurrence
        # ends up anchoring it. `max_delta_ns` starts below any real delta —
        # every caller here has already established `ts < prev_ts` — so the
        # first occurrence always fills the rest in.
        record = {
            "line": None,
            "previous_stream": None,
            "stream": None,
            "previous_local_ts": None,
            "local_ts": None,
            "violations": 0,
            "max_delta_ns": -1,
        }
    record["violations"] += 1
    if delta > record["max_delta_ns"]:
        record.update(
            line=lineno,
            previous_stream=prev_stream,
            stream=stream,
            previous_local_ts=int(prev_ts),
            local_ts=int(ts),
            max_delta_ns=int(delta),
        )
    return record


def gap_limit(family: str, stream: str) -> Optional[int]:
    """The cadence limit for a channel, or `None` if it has no expectation."""
    if (family, stream) in MAX_GAP_NS:
        return MAX_GAP_NS[(family, stream)]
    # Bybit's stream name carries the depth (`orderbook.50`); the limit does not
    # depend on it.
    head = stream.split(".", 1)[0]
    return MAX_GAP_NS.get((family, head))


#: How many break intervals are kept per chain. They exist to point an
#: investigation at the first few; a file that breaks a million times must not
#: cost a million entries.
MAX_BREAK_EXAMPLES = 10


def _track_sequence(
    scan: FileScan, family: str, stream: str, obj: dict, ts: int, prev: dict
) -> None:
    """Counts breaks in whatever sequence number the venue provides.

    Binance USD-M: `data.pu` of a `depthUpdate` must equal the previous `data.u`
    (`collector/src/binancefuturesum/mod.rs` uses the same rule live, to decide
    when to re-pull a REST snapshot). Bybit: `u` increments by one per topic,
    and a `snapshot` frame restarts the chain. Hyperliquid publishes no sequence
    number at all — for it, cadence is the only evidence there is.

    `prev` is the caller's per-file chain state: `stream -> (last id, its ts)`.
    A break is recorded as the interval between the two frames it sits between,
    so the sidecar can explain it exactly as it explains a cadence gap — the
    first `depthUpdate` after a reconnect breaks the chain by construction.
    """

    def note_break(since: int) -> None:
        scan.sequence_breaks[stream] += 1
        examples = scan.sequence_break_gaps.setdefault(stream, [])
        if len(examples) < MAX_BREAK_EXAMPLES:
            examples.append(Gap(since, ts, ts - since))

    if family == BINANCE and stream == "depthUpdate":
        data = obj.get("data") or {}
        u, pu = data.get("u"), data.get("pu")
        if not isinstance(u, int) or not isinstance(pu, int):
            return
        scan.sequence_breaks.setdefault(stream, 0)
        last = prev.get(stream)
        if last is not None and pu != last[0]:
            note_break(last[1])
        prev[stream] = (u, ts)
        return

    if family == BYBIT and stream.startswith("orderbook"):
        data = obj.get("data") or {}
        u = data.get("u")
        if not isinstance(u, int):
            return
        scan.sequence_breaks.setdefault(stream, 0)
        last = prev.get(stream)
        if obj.get("type") == "snapshot":
            # A snapshot is the venue restarting the chain, not a loss.
            prev[stream] = (u, ts)
            return
        if last is not None and u != last[0] + 1:
            note_break(last[1])
        prev[stream] = (u, ts)
        return

    if family == LIGHTER and stream == "order_book":
        # `begin_nonce(N+1)` must equal `nonce(N)`. Not "the next id": these are
        # matching-engine nonces and jump by tens between batches, so the only
        # thing that can be checked is the explicit link the venue publishes.
        # The same rule the collector applies live (`lighter/mod.rs`), where it
        # also triggers the resubscribe that produces the snapshot below.
        #
        # `offset` is deliberately not read. It is API-server-local and jumps
        # on reconnect, so a chain built on it would report a break for every
        # reconnect and miss the losses that matter.
        book = obj.get("order_book") or {}
        nonce, begin = book.get("nonce"), book.get("begin_nonce")
        if not isinstance(nonce, int) or not isinstance(begin, int):
            return
        scan.sequence_breaks.setdefault(stream, 0)
        last = prev.get(stream)
        if str(obj.get("type", "")).startswith("subscribed/"):
            # A full snapshot: the venue restarting the chain. It carries
            # `begin_nonce: 0`, and it is what the collector's own repair asks
            # for, so counting it as a break would count every recovery twice.
            prev[stream] = (nonce, ts)
            return
        if last is not None and begin != last[0]:
            note_break(last[1])
        prev[stream] = (nonce, ts)


def scan_symbol_file(path, exchange: str) -> FileScan:
    """Reads one `<symbol>_<day>.gz` and accumulates everything the report needs.

    Streaming and aggregate-only: a Bybit day is millions of lines, and holding
    the parsed frames would cost gigabytes for numbers that fit in a handful of
    counters. The ordering check below keeps one stamp per stream, and there are
    at most a handful of streams in a symbol file.

    **Ordering has two different meanings here, and conflating them makes the
    check structurally unsatisfiable.**

    *Within one stream* `local_ts` must never go backwards. Verified for every
    stream this file classifies:

    * Hyperliquid — one WS reader stamps `Utc::now()` and `route`s every frame
      (`hyperliquid/mod.rs`); `trades`, `bbo`, `l2Book_fast`, `l2Book_slow` and
      `activeAssetCtx` all come off that one loop, in receive order. The last is
      one more subscription on the same socket (`hyperliquid::ALWAYS_ON`), not a
      second producer.
    * Bybit — likewise, one reader for `orderbook.*` and `publicTrade`
      (`bybit/mod.rs`); nothing else writes a symbol file.
    * Binance (`binance`, `binancefuturesum`, `binancefuturescm`) — `bookTicker`,
      `depthUpdate`, `trade` and `markPriceUpdate` come off the one WS reader
      through `pump`, so they hold; `@markPrice@1s` is a stream of the same
      combined-stream URL (`binancefutures{um,cm}::STREAMS`), which is also why
      it carries no `pu` chain to check and never enters `_track_sequence`.
      Two exceptions are worth knowing about, and both hold *within* their own
      stream for reasons of their own. `depthSnapshot`: each one is fetched by
      its own detached `tokio::spawn`, so two in flight could in principle be
      stamped and enqueued out of order — the window is the few instructions
      between `Utc::now()` and `send`, and the fetches are throttled to 100/min,
      so it stays red, because at that separation a real step backwards is a
      clock, not a race. `premiumIndex` (USD-M only): one poller awaiting each
      response before the next tick, so two polls cannot overtake each other at
      all.
    * Paradex — one reader again, and it has to be said explicitly because this
      venue looks like the next one and is not: `paradex/mod.rs` makes ONE
      `keep_connection(CHANNELS.to_vec(), markets, ws_tx)` to one `WS_URL`, so
      `bbo`, both books, `trades` and `funding` are multiplexed onto a single
      socket. Lighter is the same shape (one connection, `lighter::CHANNELS`).
    * Extended — **not** one reader, and the one venue here that is not: one
      WebSocket per channel and one spawned task per socket
      (`extended/http.rs::keep_connections`), so each stream of a symbol file
      has a producer of its own. Within a stream it still holds, for a reason
      of its own rather than by inheritance: that stream is one task, which
      reads, stamps `Utc::now()` and enqueues in that order, so a step
      backwards inside it is a clock or two recordings at any size.

    *Between two streams* it holds exactly where the venue has one producer,
    which is everywhere in the list above except `depthSnapshot`,
    `premiumIndex`, and every pair on Extended — so the tolerance is granted on
    the mechanism, not on the venue and not on the size:
    `_SECOND_PRODUCER` has to name one of the two streams, or
    `_TOPOLOGY` the venue `FANS_OUT_PER_CHANNEL`, before any tolerance is
    consulted at all.
    A step backwards between two Hyperliquid cadences, or between `bookTicker`
    and `trade`, is red at a nanosecond, because the one
    reader that stamped them also queued them in that order. Which tolerance —
    the socket hop's occupancy, a ceiling on the poller's own hop, that same
    figure reused for two channel sockets of one runtime, or none at
    all — follows from the hand-offs the *late* row of the pair crossed alone;
    `interleave_kind` is where that is decided and argued.

    The file's write order is compared with the previous line's stamp, which is
    what "the writer put these two the wrong way round" means locally; comparing
    against the maximum seen so far would instead count every frame written
    during the overtake.

    **Two cursors, because "the previous line" is the wrong neighbour for the
    pair that has no tolerance.** Rows that share the whole chain — everything
    not in `_SECOND_PRODUCER` — are also held against *each other*, across
    whatever a second producer wrote between them, because a FIFO cannot
    reorder them however many rows from elsewhere land in the gap. Without that
    cursor a single `premiumIndex` row laundered the pair it straddled: the
    genuine WS↔WS relation was never examined, and the pair that was examined
    got the poller's unbounded yellow. There are ~8640 poll rows per symbol-day
    to land in exactly the wrong place, and they land during bursts, which is
    where a writer or parser defect would surface. Measured to invent nothing on
    real recordings: zero such inversions across the whole of
    `binancefuturesum-b` for 2026-07-29 — 23 symbol files, 136.3M shared-chain
    rows, with 110 677 poll rows and 395 REST snapshots landing among them — on
    the same recording whose adjacent-pair inversions run to 1.046s. The two
    cursors are the same row every time on any venue whose streams are all in
    the shared chain, which is every venue but Binance — Extended included: its
    channels are separate *sockets*, but they all reach the writer through the
    same two hops, so `_SECOND_PRODUCER` names none of them and nothing here
    reaches Hyperliquid, Bybit, Lighter, Extended or Paradex.
    """
    path = Path(path)
    family = family_of(exchange)
    symbol = path.name.rsplit("_", 1)[0]
    scan = FileScan(path=str(path), symbol=symbol, exchange=exchange)
    prev_ts = None
    prev_stream = None
    # The last row that reached the writer through the whole chain, which is not
    # always the last row written — see the docstring.
    prev_shared_ts = None
    prev_shared_stream = None
    last_of_stream: dict = {}
    prev_id: dict = {}

    def pair_kind(earlier_stream, later_stream, delta_ns) -> str:
        """`interleave_kind` for the venue this file was recorded on.

        Bound once, outside the loop, rather than passed at each of the two
        cursors below. Which cursor catches a pair depends on what a second
        producer happened to write between the two rows, so a venue threaded to
        one site and not the other is a model that classifies the same file two
        different ways depending on the traffic — and the site that would be
        left behind is the one no venue exercises today (an adjacent pair whose
        late row is a `_SECOND_PRODUCER` stream, which no fan-out venue has),
        so nothing would fail. One binding, and that cannot be written.
        """
        return interleave_kind(exchange, earlier_stream, later_stream, delta_ns)

    try:
        for lineno, raw in enumerate(iter_gz_lines(path), start=1):
            scan.lines += 1
            try:
                ts, obj = parse_line(raw)
            except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
                scan.malformed += 1
                if scan.malformed_example is None:
                    text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else str(raw)
                    scan.malformed_example = f"line {lineno}: {text[:120].rstrip()}"
                continue

            stream = classify(family, obj) if isinstance(obj, dict) else None
            key = stream if stream is not None else UNCLASSIFIED

            shares_the_whole_chain = key not in _SECOND_PRODUCER
            # Equal stamps are fine throughout — two frames can share a
            # nanosecond, and on a burst many do.
            last_seen = last_of_stream.get(key)
            if last_seen is not None and ts < last_seen:
                scan.monotonic_violation = _note_inversion(
                    scan.monotonic_violation, lineno, key, key, last_seen, ts
                )
            elif (
                shares_the_whole_chain
                and prev_shared_ts is not None
                and ts < prev_shared_ts
            ):
                # Two rows of the one FIFO chain, out of order. Reported against
                # each other and ahead of the adjacent pair below, because this
                # is the relation nothing can explain: what a second producer
                # wrote between them crossed a different hop and cannot have
                # reordered either. Classified through the same function as
                # every other pair — there is one classifier, not two — and what
                # it answers depends on the venue, not on which cursor caught
                # the pair. On a single-reader venue that is `INTERLEAVE_EXCESS`
                # and nothing else. On a fan-out venue
                # (`fans_out_per_channel`) every stream is a socket task of its
                # own, so this is where its cross-stream pairs get the bounded
                # second-producer verdict — and this cursor is the ONLY one they
                # reach, because none of that venue's streams is in
                # `_SECOND_PRODUCER`, so `shares_the_whole_chain` holds for all
                # of them — see `pair_kind` above, and the test named there for
                # when that stops being true.
                kind = pair_kind(prev_shared_stream, key, prev_shared_ts - ts)
                scan.interleave[kind] = _note_inversion(
                    scan.interleave.get(kind),
                    lineno,
                    prev_shared_stream,
                    key,
                    prev_shared_ts,
                    ts,
                )
            elif prev_ts is not None and ts < prev_ts:
                # In order for its own stream but written after a line stamped
                # later. Which of the three findings that is depends on the
                # hand-offs the late row crossed alone, not on the size on its
                # own — see `interleave_kind`, which the explanation reads too,
                # so the two cannot disagree.
                kind = pair_kind(prev_stream, key, prev_ts - ts)
                scan.interleave[kind] = _note_inversion(
                    scan.interleave.get(kind), lineno, prev_stream, key, prev_ts, ts
                )
            last_of_stream[key] = ts
            prev_ts = ts
            prev_stream = key
            if shares_the_whole_chain:
                # Advanced even when this row was the violation, exactly as
                # `prev_ts` is: the cursor tracks where the file is, not where it
                # ought to be, and one defect must not report every later row.
                prev_shared_ts = ts
                prev_shared_stream = key

            if stream is None:
                scan.unclassified += 1
                continue

            stat = scan.streams.get(stream)
            if stat is None:
                stat = scan.streams[stream] = StreamStat()
            stat.observe(ts, gap_limit(family, stream))
            _track_sequence(scan, family, stream, obj, ts, prev_id)
    except TruncatedRecording as error:
        scan.truncated = True
        scan.truncation_error = str(error)

    return scan


#: How many times its own cadence limit a hole may be before no liveness
#: reference is allowed to excuse it.
#:
#: The false positives this check exists for were 14-37s on ENA (measured
#: 2026-07-26), against a 14s `bbo` limit — so 10x is roughly four times the
#: worst of them and cannot reach the failure on the other side. That failure is
#: a silently dropped per-channel subscription: socket up, one channel dead, the
#: `orderbook.500` precedent in `AGENTS.md` §4.1. It has exactly the signature
#: this check suppresses, and at some size "the top of book did not move" stops
#: being a hypothesis about a venue whose `bbo` median is 0.14s. A reference
#: proves the SOCKET was alive; it never proves the channel was.
MAX_SUPPRESSED_GAP_FACTOR = 10


def liveness_witness(streams: dict, reference_names: tuple, gap: Gap) -> Optional[str]:
    """The first reference stream that disproves this hole, or `None`.

    A reference disproves nothing unless it was **running across the hole**.
    Selecting on "was it recorded at all" is fail-open twice over: a stream that
    simply stops leaves no trailing hole of its own (`StreamStat.observe` only
    measures between two frames), so a dead reference satisfies "no overlapping
    gap" vacuously; and picking per stream rather than per gap lets a dead
    preferred reference shadow a live fallback that did report the outage.

    Hence the bracket test, and hence the loop: `LIVENESS_REFERENCE` is a tuple
    in preference order, and a name that cannot witness *this* hole is passed
    over for the next one rather than ending the search.
    """
    for name in reference_names:
        ref = streams.get(name)
        if ref is None or ref.first_ts is None or ref.last_ts is None:
            continue
        if ref.first_ts > gap.start_ts or ref.last_ts < gap.end_ts:
            # Recorded, but not over this interval. Its silence here is its own
            # absence, not evidence about anything.
            continue
        if ref.gaps_truncated():
            # Its hole list is incomplete, so "no overlapping hole" is not a
            # fact about the recording, only about what was kept. Fail closed.
            continue
        # Both lists are capped at MAX_GAPS_RECORDED, so the quadratic pair
        # count is bounded by 200x200 and needs no index.
        if any(gap.overlaps(other) for other in ref.gaps):
            continue
        return name
    return None


def suppress_quiet_book_gaps(family: str, streams: dict) -> None:
    """Marks the cadence gaps another stream on the same socket disproves.

    An event-driven channel emits nothing when nothing happens, so its silence
    is only evidence of a hole if the steady channel beside it went silent too.
    Mutates the `Gap` objects in place; see `LIVENESS_REFERENCE` for the choice
    of reference and the measurement behind it, and `liveness_witness` for what
    a reference has to have done to count as one.

    Three things stop a hole being suppressed, all of them reasons the reference
    cannot settle it:

    * an `explained_by` already set from the sidecar. An outage shorter than the
      reference's own cadence limit leaves no hole in it — a two-second
      reconnect is invisible to a feed allowed 5.4s between frames — and
      dropping a hole the collector itself reported would be the one way this
      check could lose information rather than noise. Call it after
      `explain_gap`, or the guard has nothing to read;
    * the hole being past `MAX_SUPPRESSED_GAP_FACTOR` x its own limit, where the
      quiet-book explanation stops being credible whatever else was running;
    * no reference that was actually live across it.
    """
    for stream, stat in streams.items():
        reference_names = LIVENESS_REFERENCE.get((family, stream))
        if reference_names is None or not stat.gaps:
            continue
        limit = gap_limit(family, stream)
        ceiling = None if limit is None else limit * MAX_SUPPRESSED_GAP_FACTOR

        for gap in stat.gaps:
            if gap.explained_by is not None:
                continue
            if ceiling is not None and gap.duration_ns > ceiling:
                continue
            ref_name = liveness_witness(streams, reference_names, gap)
            if ref_name is None:
                continue
            gap.suppressed_by = (
                f"{ref_name} ran without a gap over the same interval — {stream} "
                f"is event-driven, so this is the top of book not changing, not "
                f"a hole in the recording"
            )


# ---------------------------------------------------------------------------
# the sidecar
# ---------------------------------------------------------------------------


def read_meta(path) -> list:
    """`(local_ts | None, record)` for every readable line of a sidecar.

    Lines carry the same `<local_ts> <json>` prefix as market data
    (`RotatingFile::write`), but the file is plain text, appended by several
    writers, and NOT ordered by `local_ts` — see the module docstring.
    Unreadable lines are skipped rather than fatal: the sidecar is diagnostic,
    and losing the tail of it must not stop the report.
    """
    out = []
    with open(path, "rb") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                ts, obj = parse_line(raw)
            except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
                try:
                    obj, ts = json.loads(raw.decode("utf-8", "replace")), None
                except (ValueError, json.JSONDecodeError):
                    continue
            if isinstance(obj, dict):
                out.append((ts, obj))
    return out


def sidecar_paths(data_dir: Path) -> list:
    """Every sidecar in the directory, oldest name first.

    Deliberately not per day: `session_start` is written once per process
    (`collector/src/main.rs`) while `RotatingFile` opens a new sidecar at every
    UTC midnight, so a day's configuration usually lives in an older file.
    `session_records_for_day` does the time-scoping afterwards.
    """
    return sorted(data_dir.glob("_meta_*.jsonl"))


def day_bounds_ns(day: str) -> tuple:
    """`[start, end)` of a UTC `YYYYMMDD` day, in integer nanoseconds."""
    midnight = datetime.strptime(day, "%Y%m%d").replace(tzinfo=timezone.utc)
    # A whole-second epoch is exact in float64 (1.8e9 << 2^53); the nanosecond
    # scaling that would not be is done in ints.
    start = int(midnight.timestamp()) * SEC_NS
    return start, start + 86_400 * SEC_NS


def session_records_for_day(records, day: str) -> list:
    """The `session_start` records in force during `day`.

    The collector writes one per process (`collector/src/main.rs`, inside the
    startup block) while `RotatingFile` opens a new sidecar at every UTC
    midnight (`collector/src/file.rs`). So a process started on Monday leaves
    the only description of Tuesday's recording in *Monday's* sidecar, and
    looking for it in Tuesday's finds nothing.

    In force during a day means: every `session_start` stamped inside it, plus
    the last one before it — the process that was already running. A later
    restart's configuration is deliberately excluded, so a day that only ever
    ran `slow` is not judged against the `fast` a Wednesday restart added.
    An undated record (a sidecar line without the `<ts> ` prefix) cannot be
    placed in time and is kept for every day rather than dropped.
    """
    start_ns, end_ns = day_bounds_ns(day)
    inside, before, undated = [], None, []
    for ts, rec in records:
        if rec.get("_collector") != "session_start":
            continue
        if ts is None:
            undated.append((ts, rec))
        elif start_ns <= ts < end_ns:
            inside.append((ts, rec))
        elif ts < start_ns and (before is None or ts > before[0]):
            before = (ts, rec)
    out = list(undated)
    if before is not None:
        out.append(before)
    out.extend(sorted(inside, key=lambda pair: pair[0]))
    return out


def depth_repairs_refused(records, day: str) -> dict:
    """`{symbol: [reason, ...]}` for the repairs this day could not make.

    Binance USD-M publishes the book as diffs whose continuity the collector
    checks frame by frame; a break is repaired by refetching the whole book over
    REST, and a refused refetch leaves every later diff applying to a book that
    is missing a batch. Nothing in the recording shows that. The frames on
    either side arrive on time and well formed, `_track_sequence` below counts
    the break itself — and counts it identically whether or not it was repaired.

    So the collector says so in the sidecar (`meta::depth_repair_failed`), and
    this is what reads it. Filtered to the day the way nothing else in the
    sidecar is, because unlike `session_start` this record describes a moment
    rather than a configuration: an undated one (a line written without the
    `<ts> ` prefix) is kept, since dropping it would hide the finding entirely.
    """
    start_ns, end_ns = day_bounds_ns(day)
    out = {}
    for ts, rec in records:
        if rec.get("_collector") != "depth_repair_failed":
            continue
        if ts is not None and not (start_ns <= ts < end_ns):
            continue
        symbol = str(rec.get("symbol", "")).lower() or "unknown"
        out.setdefault(symbol, []).append(str(rec.get("reason", "unknown")))
    return out


def merge_session_config(records) -> Optional[dict]:
    """One recording configuration for the day, from every `session_start`.

    A restart writes another `session_start`, and its configuration may differ.
    The union is the right expectation for a whole-day presence check: a day
    that ran `slow,fast` and then `fast` did record slow frames, and requiring
    them is correct; a day that only ever ran `fast` never asked for them and
    must not go red.
    """
    exchange = None
    symbols, modes, depths = [], [], []
    seen = False
    for _, rec in records:
        if rec.get("_collector") != "session_start":
            continue
        seen = True
        found = rec.get("exchange")
        if exchange is not None and found != exchange:
            # Two instances recorded into one directory. `lock.rs` prevents that
            # happening live, but a directory can also be assembled afterwards
            # for conversion (README, "Output format"), and there the two
            # configurations would silently merge into one expected set.
            raise ValueError(
                f"sidecars for this day name two exchanges ({exchange!r} and "
                f"{found!r}); a report directory must hold one collector "
                f"instance, so split them before checking"
            )
        exchange = exchange or found
        for src, dst in (
            (rec.get("symbols"), symbols),
            (rec.get("hl_l2_modes"), modes),
            (rec.get("bybit_depths"), depths),
        ):
            for item in src or []:
                if item not in dst:
                    dst.append(item)
    if not seen:
        return None
    return {
        "exchange": exchange,
        "symbols": symbols,
        "hl_l2_modes": modes,
        "bybit_depths": depths,
    }


#: The collector's **gauges** — `_collector` records that are measurements
#: rather than events in the recording's life. Four are written on the same
#: one-minute timer (`main.rs`, the `gauges.tick()` arm: `disk`, `clock`, `cpu`,
#: `liveness`) and `universe` once at startup.
#:
#: Named as a set for two reasons. They are known records, so nothing may treat
#: one as unrecognised — a gauge the Rust side starts writing must not turn
#: every recording yellow for the collector doing what it was asked to. And not
#: one of them may ever join `_EXPLANATORY` below; the assertion that they do
#: not is a test, because the rule is easy to break by adding a name to the
#: wrong tuple.
#:
#: `cpu` is the one that most looks like it belongs in the other tuple. It
#: carries `steal_pct`, and steal at 80% really is why the writer fell behind —
#: but it is written every minute whether or not anything was stolen, so it
#: lands inside every hole longer than a minute exactly as `disk` does.
#: Annotating a gap "explained by cpu" would state only that a minute passed.
#: The number in the record is the evidence; reading it is the investigation.
_GAUGES = ("disk", "clock", "cpu", "liveness", "universe")

#: The collector's **lifecycle** records — the ones that say something happened
#: to the recording — most conclusive first. A restart (`session_start`) explains
#: a hole as surely as a `disconnected` does.
#:
#: This tuple is also the whitelist: a `_collector` record whose name is not
#: here explains nothing. That matters because the sidecar also carries the
#: `_GAUGES` above. One `disk` gauge lands inside every hole longer than a
#: minute no matter what caused it, so annotating a gap "explained by disk
#: at ..." states only that a minute passed. It closed investigations that had
#: not happened. The `clock` gauge is the sharpest case of the same rule: an
#: unsynchronised clock makes a hole's *measurement* doubtful, which is the
#: opposite of accounting for the hole.
#:
#: Fail closed by omission: a `_collector` record added to the Rust side later
#: will not explain anything until it is named here, and a new gauge silently
#: cannot.
_EXPLANATORY = (
    "disconnected",
    "dial_failed",
    "stalled",
    "queue_overflow",
    "hand_off_closed",
    "disk_exhausted",
    "symbol_check_failed",
    # The venue refused the WebSocket upgrade from this host, so the collector
    # never started. Distinct from `symbol_check_failed` on purpose: Lighter's
    # `/stream` sits behind a jurisdiction check that refuses the upgrade while
    # REST keeps answering, so every symbol resolved and the recording is empty
    # anyway. Sharing a name would send whoever reads the annotation to check a
    # symbol list that was never wrong.
    "probe_failed",
    "stream_ended",
    "session_start",
    "subscribe",
    "connected",
)

# The two tuples must stay disjoint. A measurement that explains a gap is
# exactly how the minutely disk gauge came to close investigations that had not
# happened, and the tuples are adjacent — putting a name in the wrong one is a
# one-line mistake. Checked on import rather than only in a test, so it also
# holds for anything that imports this module without running the suite.
assert not set(_GAUGES) & set(_EXPLANATORY), (
    f"a gauge is listed as explanatory: {sorted(set(_GAUGES) & set(_EXPLANATORY))}"
)


def lifecycle_events(records) -> list:
    """`(ts, event)` for the collector's lifecycle records, sorted by `local_ts`.

    Filtered to `_EXPLANATORY` here rather than at the point of use, so the
    `(+N more)` count in an explanation is a number of records that bear on the
    hole — not a count of how many timer ticks landed inside it.
    """
    out = [
        (ts, rec["_collector"])
        for ts, rec in records
        if ts is not None and rec.get("_collector") in _EXPLANATORY
    ]
    out.sort(key=lambda pair: pair[0])
    return out


#: Most lifecycle records one gap is described with. A reconnect storm can put
#: thousands inside a single hole (`README.md`, "Going silent": a couple a
#: second while a venue refuses connections), and the report only ever names
#: the most conclusive one and counts the rest.
MAX_EVENTS_PER_GAP = 1000


def explain_gap(gap: Gap, events) -> Optional[str]:
    """Names the lifecycle record that accounts for a gap, if there is one.

    `events` must be sorted by timestamp, which is what `lifecycle_events`
    guarantees — the sidecar itself is not ordered (see the module docstring).
    Located by bisection: this runs once per gap, and a day can hold both many
    gaps and many events.
    """
    lo = gap.start_ts - EXPLAIN_MARGIN_NS
    hi = gap.end_ts + EXPLAIN_MARGIN_NS
    start = bisect_left(events, (lo,))
    inside = []
    for ts, name in events[start : start + MAX_EVENTS_PER_GAP]:
        if ts > hi:
            break
        inside.append((ts, name))
    if not inside:
        return None
    for wanted in _EXPLANATORY:
        for ts, name in inside:
            if name == wanted:
                extra = f" (+{len(inside) - 1} more)" if len(inside) > 1 else ""
                return f"{name} at {iso(ts)}{extra}"
    # Unreachable while `lifecycle_events` filters to `_EXPLANATORY`, and it
    # must stay that way: falling back to "whatever was in there" is how the
    # minutely disk gauge came to explain gaps.
    return None


def clock_summary(records, day: str) -> Optional[dict]:
    """What the `clock` gauge said during one UTC day, or `None` if it said
    nothing.

    The gauge is the kernel's own view of how well `CLOCK_REALTIME` was being
    disciplined, sampled once a minute (`collector/src/clock.rs`). It exists
    because the alternative was finding out at assembly time: on 2026-07-27 a
    host came back from a reboot undisciplined, recorded a full day, and the
    time policy rejected all of it on a 7.04 ms local-exchange skew.

    Scoped to the day being checked even though sidecars are read across the
    whole directory — `session_start` is per process, but a clock reading is
    not, and one bad night must not annotate every day in the directory.

    Three things are deliberately *not* done here.

    A missing `sync` field does not count as unsynchronised: it is what an
    unsupported platform and a failed syscall both write, and "we did not
    measure it" is not "it was wrong". Nor does it count towards `samples`,
    which is the denominator the note quotes — "2 of 4" reads as two healthy
    readings, and a sample nobody took is not one of those.

    No threshold on `max_error_us` is applied — the collector owns that number
    (`clock::MAX_ERROR_WARN_US`) and duplicating it here would give two limits
    that drift apart. A dataset manifest applying its own policy reads the
    `_meta` records directly; what is summarised here is only what the note
    needs.

    And `worst_max_error_us` is scoped to the unsynchronised samples, not to the
    day. `max_error` grows between every poll on a perfectly healthy clock, so a
    day almost always holds a larger one outside the window — quoting that
    inside a sentence about the window would attribute an ordinary excursion to
    the fault.
    """
    start_ns, end_ns = day_bounds_ns(day)
    samples = unsynced = 0
    first_bad = last_bad = None
    worst_max_error = None
    for ts, rec in records:
        # An undated sidecar line cannot be placed in a day at all. Counting one
        # would let a record from any day annotate this one.
        if rec.get("_collector") != "clock" or ts is None:
            continue
        if not (start_ns <= ts < end_ns):
            continue
        sync = rec.get("sync")
        if not isinstance(sync, bool):
            continue
        samples += 1
        if sync:
            continue
        unsynced += 1
        max_error = rec.get("max_error_us")
        if isinstance(max_error, int) and (
            worst_max_error is None or max_error > worst_max_error
        ):
            worst_max_error = max_error
        if first_bad is None or ts < first_bad:
            first_bad = ts
        if last_bad is None or ts > last_bad:
            last_bad = ts
    if samples == 0:
        return None
    return {
        "samples": samples,
        "unsynced_samples": unsynced,
        "first_unsynced_ts": first_bad,
        "last_unsynced_ts": last_bad,
        "worst_max_error_us": worst_max_error,
    }


def clock_detail(clock: dict) -> str:
    """The note an unsynchronised window gets.

    Informational, and it says what it bears on rather than what it proves. The
    recording is not corrupt and the venues' own timestamps are untouched; what
    is in doubt is every `local_ts` stamped inside the window, which is what the
    cadence, monotonicity and coverage checks are all measured in.
    """
    worst = clock["worst_max_error_us"]
    worst_text = "" if worst is None else f", worst max_error in it {worst} us"
    return (
        f"the host clock was unsynchronised (STA_UNSYNC) for "
        f"{clock['unsynced_samples']} of {clock['samples']} measured minutely "
        f"sample(s), "
        f"{iso(clock['first_unsynced_ts'])} .. {iso(clock['last_unsynced_ts'])}"
        f"{worst_text}. local_ts inside that window is the host's own idea of "
        f"the time, so any finding there — a cadence gap, a monotonicity step, "
        f"the coverage bounds — may be the clock rather than the recording. The "
        f"venue timestamps inside the payloads are unaffected"
    )


# ---------------------------------------------------------------------------
# per-day checking
# ---------------------------------------------------------------------------


def issue(severity: str, check: str, detail: str) -> dict:
    return {"severity": severity, "check": check, "detail": detail}


def interleave_detail(exchange: str, name: str, kind: str, record: dict) -> str:
    """Why two streams are out of order, and whether that is still credible.

    `kind` is the class `scan_symbol_file` filed the pair under, passed in
    rather than recomputed: the finding and its explanation have to be the same
    decision, or the report prints "nothing is missing" on a red. `exchange` is
    read for the same reason — `interleave_kind` reads it, and a text that did
    not would have gone on calling a fan-out venue single-reader while the
    classification had stopped doing so.

    Eight verdicts, because the class alone does not say what happened: the
    poller pair is two different findings depending on which of the two rows
    waited, and a text naming the wrong one is the same defect as a bound
    applied to the wrong one. This one printed "what separates them is the WS
    row's own wait" over a pair whose WS row was written first and waited for
    nothing.
    """
    delta = record["max_delta_ns"]
    producer = second_producer_of(exchange, record["previous_stream"], record["stream"])
    mechanism = _NO_SECOND_PRODUCER if producer is None else producer.mechanism
    # Which of the two rows waited decides the class, so it also decides what
    # there is to say — see `interleave_kind`. The poll written last waited on
    # its own hop or on nothing; the poll written first held nobody up at all.
    poll_is_the_late_row = crosses_a_hand_off_of_its_own(record["stream"])
    if producer is None:
        # No hand-off separates these two, so no hand-off can have reordered
        # them, and the size of the step is beside the point.
        verdict = (
            "not something an interleave can produce here: with one producer the "
            "write order IS the receive order, so this is a clock step, a frame "
            "classified into the wrong stream, or two recordings in one file"
        )
    elif kind == INTERLEAVE_INVERSION_POLLER:
        # The one class with no arithmetic behind it, so the text carries the
        # reason instead of a number: what the row skipped, what that leaves to
        # bound it, and where loss would actually show if there were any.
        verdict = (
            "a disagreement no queue bounds: the poll was written first, over "
            "%s, a hand-off of its own, so the row it overtook shares no FIFO "
            "with it and the two meet only in main's select loop. What separates "
            "them is that row's own wait in the socket hop plus the writer hop, "
            "and no bound over those capacities holds — the writer hop drains at "
            "the speed of blocking gzip I/O, measured stopping for longer than "
            "its whole depth (queue.rs, 2026-07-26). Nothing here says a frame is "
            "missing: loss shows in the u/pu chain, which is checked separately "
            "and independently. A large one is worth a look anyway — it is the "
            "depth of the collector's own queues at a burst, so check _meta for a "
            "queue_overflow near this line" % POLLER_HOP
        )
    elif poll_is_the_late_row and kind == INTERLEAVE_EXCESS:
        # The poll went last, so no backlog on the WS side can be the reason:
        # `writer_rx` is a FIFO and delays the rows behind it, not a row on
        # another hop. That leaves the stamp itself, which is a lookahead
        # defect and has no other detector in this report.
        verdict = (
            "further back than the poll's own hand-off could hold it: the poll "
            "was written last, so nothing on the WS side delayed it — %s is the "
            "only queue between its Utc::now() and the file, it carries one "
            "element per symbol per poll interval, and main offers its arm every "
            "iteration (worst observed on a full symbol-day: 3.128ms). Either "
            "this premiumIndex local_ts is not the moment the body arrived — "
            "stamped at request time, served from a cache, or copied from the "
            "venue's own E, any of which puts the element into the recording "
            "ahead of the moment it was knowable — or the file holds two "
            "recordings" % POLLER_HOP
        )
    elif poll_is_the_late_row:
        verdict = (
            "within the %s ceiling on the poll's own hand-off, which is the only "
            "queue between its stamp and the file: the poll was written last, so "
            "nothing on the WS side held it up, and nothing here is missing or "
            "mis-stamped — only the write order and the receive order disagree"
            % fmt_short(POLLER_HOP_CEILING_NS)
        )
    elif fans_out_per_channel(exchange) and kind == INTERLEAVE_EXCESS:
        # Both hops cancel here, so the backlog hypothesis the next branch
        # offers is not available: a deep socket hop delays both of these rows
        # equally. What is left is the pair of failures that reorder a file
        # regardless of any queue.
        verdict = (
            "further apart than two sockets of the same runtime can be: both "
            "rows crossed the same %s and %s hops, both FIFOs, so all that "
            "separates their stamps is the moment between one task's Utc::now() "
            "and its send — %s of scheduling is not that (worst observed on a "
            "real day: 15.153us). Either a clock stepped, which the "
            "monotonicity check reports on the busiest stream first, or the "
            "file holds two recordings"
            % (WS_HOP, WRITER_HOP, fmt_short(CROSS_STREAM_TOLERANCE_NS))
        )
    elif fans_out_per_channel(exchange):
        verdict = (
            "within the %s ceiling these channel sockets are held to, so nothing "
            "is missing and nothing is mis-stamped — two socket tasks stamped "
            "their own reads and reached the shared hop in the other order, "
            "which is what a venue with one socket per channel does"
            % fmt_short(CROSS_STREAM_TOLERANCE_NS)
        )
    elif kind == INTERLEAVE_EXCESS:
        # Two hypotheses, and they need different responses. The backlog one is
        # the reason to look at `_meta` first: a hop holding thousands of frames
        # is one capacity away from the overflow that ends the process, so a
        # wide interleave can be the last trace of a burst the recording only
        # just survived.
        verdict = (
            "beyond the %s interleave bound, which is the whole socket hop at "
            "the measured burst rate — the only queue these two do not share: "
            "either it was deeper or slower than that (check _meta for a burst "
            "or a queue_overflow near this line) or the file holds two "
            "recordings" % fmt_short(CROSS_STREAM_TOLERANCE_NS)
        )
    else:
        verdict = (
            "within the %s interleave bound, so nothing is missing and nothing "
            "is mis-stamped — only the write order and the receive order "
            "disagree" % fmt_short(CROSS_STREAM_TOLERANCE_NS)
        )
    return (
        f"{name}: local_ts goes backwards BETWEEN streams {record['violations']} "
        f"time(s), worst {fmt_short(delta)} at line {record['line']}: "
        f"{record['previous_stream']} {iso(record['previous_local_ts'])} -> "
        f"{record['stream']} {iso(record['local_ts'])}. Write order and local_ts "
        f"order can only disagree where two producers stamp their own receive "
        f"moment and reach the writer by different paths, so the question is "
        # "race into one queue" until 2026-07-30, which the poller's own
        # mechanism sentence then denied in the next clause: its whole point is
        # that it shares no queue with a WS frame.
        f"whether this file has two: {mechanism}. This is {verdict}"
    )


def discover_days(data_dir: Path) -> list:
    """Every day the directory holds a symbol file or a sidecar for.

    A day with a sidecar and no market data is included on purpose: "the
    collector ran and recorded nothing" is precisely the failure this report is
    for, and skipping it would report that day as absent instead of broken.
    """
    days = set()
    for entry in data_dir.iterdir():
        name = entry.name
        if name.endswith(".gz"):
            candidate = name[:-3].rsplit("_", 1)[-1]
        elif name.startswith("_meta_") and name.endswith(".jsonl"):
            candidate = name[: -len(".jsonl")].rsplit("_", 1)[-1]
        else:
            continue
        if len(candidate) == 8 and candidate.isdigit():
            days.add(candidate)
    return sorted(days)


def identify_venue(data_dir: Path) -> str:
    """The venue of a collector directory, from any `session_start` it holds."""
    for path in sidecar_paths(data_dir):
        config = merge_session_config(read_meta(path))
        if config and config.get("exchange"):
            return config["exchange"]
    raise ValueError(
        f"{data_dir}: no `session_start` record in any _meta_*.jsonl sidecar, so "
        f"the venue cannot be identified. A directory without a sidecar cannot "
        f"be checked against the configuration it was recorded with."
    )


def check_day(
    data_dir: Path,
    exchange: str,
    day: str,
    profile: str,
    is_today: bool,
    meta_records=None,
) -> dict:
    """The full report for one venue-day. Returns the JSON `days[<day>]` value.

    `meta_records` is every sidecar record of the directory, across all days —
    `session_start` is per process, not per day. Passed in so a multi-day report
    parses each sidecar once.
    """
    issues = []
    symbols_json = {}
    coverage_first = None
    coverage_last = None

    if meta_records is None:
        meta_records = []
        for path in sidecar_paths(data_dir):
            meta_records.extend(read_meta(path))
    events = lifecycle_events(meta_records)
    config = merge_session_config(session_records_for_day(meta_records, day))

    # Raised first so it is read before the findings it qualifies. Yellow and
    # never red: the recording is not damaged, and which window a skewed clock
    # invalidates is a policy the Phase 3 builder applies, not one this report
    # decides.
    clock = clock_summary(meta_records, day)
    if clock is not None and clock["unsynced_samples"]:
        issues.append(issue(YELLOW, "clock_unsynced", clock_detail(clock)))

    # Yellow, not red, and per symbol: one refused repair damages one book from
    # that moment on, which is a warning about part of a day rather than a day
    # that cannot be used. It is listed before the per-symbol findings because
    # it qualifies them — a `sequence_gap` on a symbol named here is a break
    # that stayed broken.
    refused = depth_repairs_refused(meta_records, day)
    for symbol, reasons in sorted(refused.items()):
        counted = ", ".join(
            f"{reasons.count(r)}x {r}" for r in sorted(set(reasons))
        )
        issues.append(
            issue(
                YELLOW,
                "depth_repair_failed",
                f"{symbol}: {len(reasons)} depth snapshot(s) could not be "
                f"fetched after a break in the incremental feed ({counted}); "
                f"every diff after each one applies to a book that is missing "
                f"updates, and the frames themselves look perfectly healthy",
            )
        )

    expected = None
    if config is None:
        issues.append(
            issue(
                RED,
                "meta_missing",
                f"no session_start record in any _meta_*.jsonl of {data_dir} that "
                f"applies to {day} (one is written per process, so the whole "
                f"directory is searched); the expected symbol x stream set for "
                f"this day is unknown, so completeness cannot be verified",
            )
        )
    else:
        expected = expected_streams(profile, exchange, config)
        if expected.violation:
            issues.append(issue(RED, "profile_unsatisfiable", expected.violation))

    present = {}
    for path in sorted(data_dir.glob(f"*_{day}.gz")):
        present[path.name[:-3].rsplit("_", 1)[0]] = path

    wanted = [s.lower() for s in (config or {}).get("symbols", [])]

    def is_expected_file(name: str) -> bool:
        """Whether a recorded file's symbol was asked for.

        Extended records an RFQ market's executable book beside the plain book
        as a sibling `{market}-rfq` file (`collector/src/extended`). It is
        expected output for a requested RFQ market, not a leftover, so it must
        not read as `unexpected_symbol`.
        """
        if name in wanted:
            return True
        if (
            config is not None
            and family_of(exchange) == EXTENDED
            and name.endswith("-rfq")
        ):
            return name[: -len("-rfq")] in wanted
        return False

    for name in sorted(present):
        if config is not None and not is_expected_file(name):
            issues.append(
                issue(
                    YELLOW,
                    "unexpected_symbol",
                    f"{name}: recorded but not in session_start.symbols "
                    f"({', '.join(wanted) or 'none'}) — a leftover file, or a "
                    f"configuration that changed without the day rolling over",
                )
            )

    for name in wanted + [s for s in sorted(present) if s not in wanted]:
        path = present.get(name)
        if path is None:
            missing = list(expected.required) if expected else []
            if missing:
                issues.append(
                    issue(
                        RED,
                        "missing_required",
                        f"{name}: no {name}_{day}.gz at all — required streams "
                        f"{', '.join(missing)} are absent for the whole day",
                    )
                )
            else:
                issues.append(
                    issue(
                        YELLOW,
                        "missing_optional",
                        f"{name}: no {name}_{day}.gz at all",
                    )
                )
            symbols_json[name] = {
                "file": None,
                "lines": 0,
                "truncated": False,
                "malformed_lines": 0,
                "unclassified_frames": 0,
                "monotonic_violation": None,
                **interleave_json({}),
                "sequence_breaks": {},
                "sequence_break_examples": {},
                "streams": {},
                "missing_required": missing,
                "missing_optional": list(expected.optional) if expected else [],
                "missing_informational": (
                    list(expected.informational) if expected else []
                ),
                "coverage": {
                    "first_local_ts": None,
                    "last_local_ts": None,
                    "required_streams": missing,
                },
            }
            continue

        scan = scan_symbol_file(path, exchange)

        if scan.truncated:
            if is_today:
                # The live member has no trailer until rotation or shutdown
                # (`file.rs`), so this is what a healthy recording in progress
                # looks like. Only the LAST member can be open, and everything
                # before it decoded, so the day is still usable — but it is not
                # finalized, and the report says so rather than staying silent.
                issues.append(
                    issue(
                        YELLOW,
                        "gzip_integrity",
                        f"{name}: last gzip member is unfinished ({scan.truncation_error}) "
                        f"— expected for today's open file; {scan.lines} line(s) decoded",
                    )
                )
            else:
                issues.append(
                    issue(
                        RED,
                        "gzip_integrity",
                        f"{name}: gzip decode failed on a finalized day "
                        f"({scan.truncation_error}) after {scan.lines} line(s) — "
                        f"the member trailer is missing or the file is damaged",
                    )
                )

        if scan.malformed:
            issues.append(
                issue(
                    YELLOW,
                    "malformed_line",
                    f"{name}: {scan.malformed} line(s) could not be parsed; "
                    f"first at {scan.malformed_example}",
                )
            )

        if scan.unclassified:
            issues.append(
                issue(
                    YELLOW,
                    "unclassified_frame",
                    f"{name}: {scan.unclassified} frame(s) matched no known "
                    f"stream of {exchange}",
                )
            )

        if scan.monotonic_violation:
            v = scan.monotonic_violation
            issues.append(
                issue(
                    RED,
                    "monotonicity",
                    f"{name}/{v['stream']}: local_ts goes backwards within the "
                    f"stream {v['violations']} time(s), worst "
                    f"{fmt_short(v['max_delta_ns'])} at line {v['line']}: "
                    f"{iso(v['previous_local_ts'])} -> {iso(v['local_ts'])}. "
                    f"One stream has one producer stamping at receive time, so "
                    f"this is a clock step or two recordings in one file",
                )
            )

        # Red first, so the operator's eye lands on the one finding that refuses
        # the build; the two yellows follow in the order `INTERLEAVE_CHECKS`
        # names them.
        for check in (INTERLEAVE_EXCESS, INTERLEAVE_INVERSION, INTERLEAVE_INVERSION_POLLER):
            record = scan.interleave.get(check)
            if record:
                issues.append(
                    issue(
                        INTERLEAVE_SEVERITY[check],
                        check,
                        interleave_detail(exchange, name, check, record),
                    )
                )

        missing_required, missing_optional, missing_informational = [], [], []
        if expected is not None:
            for stream in expected.required:
                if scan.streams.get(stream, StreamStat()).count == 0:
                    missing_required.append(stream)
            for stream in expected.optional:
                if scan.streams.get(stream, StreamStat()).count == 0:
                    missing_optional.append(stream)
            # Recorded, never raised. An informational stream absent from a day
            # older than the stream itself is the normal case, not a finding —
            # see `Expected`. It reaches the JSON so the question "does this day
            # carry the funding basket?" has an answer, and it deliberately
            # reaches no `issue()` below — which is also why `render_text` never
            # prints it: that view is the operator's issue list, and a fact
            # printed among warnings is read as one.
            for stream in expected.informational:
                if scan.streams.get(stream, StreamStat()).count == 0:
                    missing_informational.append(stream)
            if missing_required:
                issues.append(
                    issue(
                        RED,
                        "missing_required",
                        f"{name}: no frames on required stream(s) "
                        f"{', '.join(missing_required)} "
                        f"(recorded: {', '.join(sorted(scan.streams)) or 'nothing'})",
                    )
                )
            if missing_optional:
                issues.append(
                    issue(
                        YELLOW,
                        "missing_optional",
                        f"{name}: no frames on optional stream(s) "
                        f"{', '.join(missing_optional)}",
                    )
                )

        for stream, count in sorted(scan.sequence_breaks.items()):
            if not count:
                continue
            examples = scan.sequence_break_gaps.get(stream, [])
            for gap in examples:
                gap.explained_by = explain_gap(gap, events)
            shown = []
            for gap in examples[:3]:
                tail = f" [{gap.explained_by}]" if gap.explained_by else ""
                shown.append(f"{iso(gap.end_ts)}{tail}")
            issues.append(
                issue(
                    YELLOW,
                    "sequence_gap",
                    f"{name}/{stream}: {count} sequence break(s), frames lost between "
                    f"consecutive updates; first at {', '.join(shown)}",
                )
            )

        # Ask the sidecar about every hole first, then drop the ones a steadier
        # stream on the same socket disproves — a quiet top of book is not a
        # hole, but one the collector reported on is, whatever else was running.
        for stat in scan.streams.values():
            for gap in stat.gaps:
                gap.explained_by = explain_gap(gap, events)
        suppress_quiet_book_gaps(family_of(exchange), scan.streams)

        for stream, stat in sorted(scan.streams.items()):
            reportable = [gap for gap in stat.gaps if gap.suppressed_by is None]
            named = min(len(reportable), MAX_GAP_ISSUES)
            limit = gap_limit(family_of(exchange), stream)
            for gap in reportable[:named]:
                tail = (
                    f"explained by {gap.explained_by}"
                    if gap.explained_by
                    else "unexplained by _meta"
                )
                issues.append(
                    issue(
                        YELLOW,
                        "cadence_gap",
                        f"{name}/{stream}: {fmt_dur(gap.duration_ns)} gap "
                        f"{iso(gap.start_ts)} -> {iso(gap.end_ts)} "
                        f"(limit {fmt_dur(limit)}); {tail}",
                    )
                )
            # Gaps past `MAX_GAPS_RECORDED` were never examined, so they could
            # not have been suppressed; they are counted with the unnamed ones.
            remainder = (len(reportable) - named) + (stat.gap_count - len(stat.gaps))
            if remainder > 0:
                issues.append(
                    issue(
                        YELLOW,
                        "cadence_gap",
                        f"{name}/{stream}: {remainder} further gap(s) "
                        f"not listed individually; see the JSON report",
                    )
                )

        entry = scan.as_json()
        entry["missing_required"] = missing_required
        entry["missing_optional"] = missing_optional
        entry["missing_informational"] = missing_informational

        if expected is not None:
            # Coverage is measured over the REQUIRED streams: an optional feed
            # that started earlier or ran later must not widen the window Phase
            # 3 trims both venues to. A venue the profile requires nothing of
            # (Bybit under mode-a-v1) would otherwise report a null window
            # despite holding data, so there it falls back to everything
            # recorded — the number is informational for such a venue anyway.
            measured = expected.required or tuple(sorted(scan.streams))
            #: Per symbol, coverage is the interval in which EVERY required
            #: stream is live: max of the firsts, min of the lasts. Phase 3
            #: trims to this. The union would let an on-time bbo hide an l2Book
            #: that started ten minutes late, and the run would begin over a
            #: window whose traded book does not exist yet (§3.1).
            sym_first = sym_last = None
            complete = bool(measured)
            for stream in measured:
                stat = scan.streams.get(stream)
                if stat is None or stat.first_ts is None:
                    complete = False
                    continue
                if sym_first is None or stat.first_ts > sym_first:
                    sym_first = stat.first_ts
                if sym_last is None or stat.last_ts < sym_last:
                    sym_last = stat.last_ts
                # The venue-level number stays the union across symbols and
                # streams: an operator reading it wants "when was this venue
                # recording at all", and Phase 3 no longer builds on it.
                if coverage_first is None or stat.first_ts < coverage_first:
                    coverage_first = stat.first_ts
                if coverage_last is None or stat.last_ts > coverage_last:
                    coverage_last = stat.last_ts
            if not complete or (sym_first is not None and sym_last is not None
                                and sym_first > sym_last):
                # A required stream missing, or two whose live intervals do not
                # overlap at all. Either way there is no interval in which the
                # symbol is fully recorded; `missing_required` already made the
                # first case red.
                sym_first = sym_last = None
            entry["coverage"] = {
                "first_local_ts": None if sym_first is None else int(sym_first),
                "last_local_ts": None if sym_last is None else int(sym_last),
                "required_streams": list(measured),
            }
        else:
            entry["coverage"] = {
                "first_local_ts": None,
                "last_local_ts": None,
                "required_streams": [],
            }

        symbols_json[name] = entry

    return {
        "verdict": worst(i["severity"] for i in issues),
        "issues": issues,
        "symbols": symbols_json,
        # Not part of the JSON contract: stripped by `build_report` after the
        # venue-level coverage has been folded together.
        "_coverage": (coverage_first, coverage_last),
    }


def build_report(dirs, profile: str, day: Optional[str], include_today: bool) -> dict:
    """Runs every check over every directory and returns the report document."""
    today = utc_today()
    venues = {}

    for data_dir in dirs:
        # Resolved, so `data_dir` in the JSON is absolute. The Phase 3 builder
        # resolves a relative one against the *report file's* directory, which
        # is not where this ran — `--dir data/hyperliquid --json out/r.json`
        # would send it looking in `out/data/hyperliquid`.
        data_dir = Path(data_dir).resolve()
        if not data_dir.is_dir():
            raise FileNotFoundError(f"{data_dir} is not a directory")
        recorded_exchange = identify_venue(data_dir)
        exchange = canonical_exchange(recorded_exchange)
        if exchange in venues:
            raise ValueError(
                f"{data_dir} and {venues[exchange]['data_dir']} are both "
                f"{exchange!r}; one venue per report entry, so pass them "
                f"separately or merge the directories first"
            )

        # Parsed once for the whole directory: `session_start` is written per
        # process, so every day's configuration may live in an older sidecar.
        meta_records = []
        for path in sidecar_paths(data_dir):
            meta_records.extend(read_meta(path))

        days = [day] if day else discover_days(data_dir)
        if not include_today:
            days = [d for d in days if d != today]

        day_results = {}
        first_ts = last_ts = None
        for d in sorted(days):
            result = check_day(data_dir, exchange, d, profile, is_today=(d == today),
                               meta_records=meta_records)
            cov_first, cov_last = result.pop("_coverage")
            if cov_first is not None and (first_ts is None or cov_first < first_ts):
                first_ts = cov_first
            if cov_last is not None and (last_ts is None or cov_last > last_ts):
                last_ts = cov_last
            day_results[d] = result

        # No day checked at all is not a pass. Nothing was verified, and a gate
        # that reports green on an empty directory is worse than no gate.
        verdict = worst(r["verdict"] for r in day_results.values()) if day_results else RED

        venues[exchange] = {
            "data_dir": str(data_dir),
            # `binancefutures` and `binancefuturesum` are one backend
            # (`collector/src/main.rs`); the key is canonical, this is the word
            # the operator actually recorded with.
            "exchange_as_recorded": recorded_exchange,
            "verdict": verdict,
            "coverage": {
                "first_local_ts": None if first_ts is None else int(first_ts),
                "last_local_ts": None if last_ts is None else int(last_ts),
                "note": "union over symbols and required streams — the operator's "
                        "view of when this venue was recording. Phase 3 trims to "
                        "days[].symbols[].coverage instead, which is per symbol "
                        "and intersects its required streams.",
            },
            "days": day_results,
        }

    return {
        "schema": SCHEMA,
        "profile": profile,
        "verdict": worst(v["verdict"] for v in venues.values()) if venues else RED,
        "venues": venues,
    }


# ---------------------------------------------------------------------------
# human-readable output
# ---------------------------------------------------------------------------


def render_text(report: dict) -> str:
    """The operator's view: every issue named, never a bare "looks fine"."""
    lines = [
        f"quality report  schema={report['schema']}  profile={report['profile']}  "
        f"verdict={report['verdict'].upper()}"
    ]
    if not report["venues"]:
        lines.append("  (no venue directories were checked)")
        return "\n".join(lines) + "\n"

    for exchange, venue in sorted(report["venues"].items()):
        lines.append("")
        recorded = venue.get("exchange_as_recorded")
        alias = f" (recorded as {recorded})" if recorded and recorded != exchange else ""
        lines.append(
            f"=== {exchange}{alias}  {venue['data_dir']}  [{venue['verdict'].upper()}]"
        )
        cov = venue["coverage"]
        lines.append(
            f"    coverage (union over symbols): {iso(cov['first_local_ts'])} .. "
            f"{iso(cov['last_local_ts'])}"
        )
        if not venue["days"]:
            lines.append("    no finalized day to check — nothing was verified")
            continue

        for day, result in sorted(venue["days"].items()):
            lines.append(f"  -- {day}  [{result['verdict'].upper()}]")
            for name, sym in sorted(result["symbols"].items()):
                if not sym["streams"]:
                    lines.append(f"     {name:<12} (no frames)")
                for stream, stat in sorted(sym["streams"].items()):
                    # `gaps` is the raw count of over-limit holes; the note says
                    # how many of them a reference stream showed to be the feed
                    # simply being quiet, and so reach no issue.
                    quiet = stat.get("suppressed_gap_count") or 0
                    tail = f" ({quiet} quiet, not reported)" if quiet else ""
                    # Width 16 fits the longest stream name any venue produces,
                    # which is `markPriceUpdate` at 15; at 14 that one row broke
                    # the alignment of every column after it.
                    lines.append(
                        f"     {name:<12} {stream:<16} n={stat['count']:<9} "
                        f"{iso(stat['first_local_ts'])} .. {iso(stat['last_local_ts'])} "
                        f"gaps={stat['gap_count']}{tail}"
                    )
                cov = sym.get("coverage") or {}
                if cov.get("required_streams"):
                    lines.append(
                        f"     {name:<12} {'coverage':<16} "
                        f"{iso(cov['first_local_ts'])} .. {iso(cov['last_local_ts'])} "
                        f"(all of: {', '.join(cov['required_streams'])})"
                    )
            if not result["issues"]:
                lines.append("     no issues")
            for i in result["issues"]:
                # 19 is a floor, not a fit: two check names are longer than it
                # (`interleave_inversion` by one, `interleave_inversion_poller`
                # by eight) and overhang the column. Widening it to fit them was
                # tried and reverted — the pad applies to every issue row of
                # every venue, so a cosmetic column on a Binance-only finding
                # rewrote every line of every Hyperliquid, Bybit and Lighter
                # report, which is the one thing this change undertook not to
                # do. Move it only for a reason that is worth that.
                lines.append(f"     [{i['severity']:<6}] {i['check']:<19} {i['detail']}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="quality_report.py",
        description=(
            "Offline quality report over raw collector recordings "
            "(Phase 2 of docs/design-multi-venue-collection.md)."
        ),
    )
    parser.add_argument(
        "--dir",
        dest="dirs",
        action="append",
        required=True,
        metavar="DATA_DIR",
        help="One collector instance directory; repeat for several venues. The "
        "venue is read from session_start.exchange in its _meta sidecar.",
    )
    parser.add_argument(
        "--day",
        metavar="YYYYMMDD",
        help="Check this UTC day only. Default: every finalized day present.",
    )
    parser.add_argument(
        "--include-today",
        action="store_true",
        help="Also check today's UTC day. Its last gzip member is still open, "
        "so it is decoded as far as it goes and the missing trailer is a "
        "warning rather than corruption.",
    )
    parser.add_argument("--json", dest="json_out", metavar="OUT", help="Write the report here.")
    parser.add_argument(
        "--profile",
        default="mode-a-v1",
        choices=("mode-a-v1", "book-v1"),
        help="Dataset profile deciding which streams are required (default: "
        "mode-a-v1). `book-v1` additionally requires the Binance book and tape, "
        "for an instance recorded because the venue publishes no book elsewhere.",
    )
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exit_code:  # argparse already printed the reason
        # `--help` exits 0 and is not a usage error; `code or 2` turned it into
        # one, so every wrapper treating non-zero as failure failed on --help.
        return 2 if exit_code.code is None else int(exit_code.code)

    if args.day is not None:
        if len(args.day) != 8 or not args.day.isdigit():
            print(f"--day expects YYYYMMDD, got {args.day!r}", file=sys.stderr)
            return 2
        if args.day == utc_today() and not args.include_today:
            print(
                f"--day {args.day} is today (UTC) and its gzip member is still "
                f"open; pass --include-today to check an unfinalized recording",
                file=sys.stderr,
            )
            return 2

    try:
        report = build_report(args.dirs, args.profile, args.day, args.include_today)
    except (ValueError, FileNotFoundError, NotADirectoryError, PermissionError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    sys.stdout.write(render_text(report))

    if args.json_out:
        try:
            with open(args.json_out, "w") as f:
                json.dump(report, f, indent=2)
                f.write("\n")
        except OSError as error:
            print(f"error: couldn't write {args.json_out}: {error}", file=sys.stderr)
            return 2

    return 1 if report["verdict"] == RED else 0


if __name__ == "__main__":
    sys.exit(main())
