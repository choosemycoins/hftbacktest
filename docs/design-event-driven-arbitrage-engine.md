# Design: event-driven торговый движок для кросс-биржевого арбитража

Status: **Draft — implementation and capital use are not approved**
Date: 2026-08-11
Scope: event-driven live/replay core поверх существующих connector-процессов и iceoryx2 IPC;
первый milestone — два существующих CLOB-коннектора и один двухногий route
Related: [`design-multi-connector-hardening.md`](design-multi-connector-hardening.md),
[`snapshot-complete-marker.md`](snapshot-complete-marker.md),
[`../AGENTS.md`](../AGENTS.md), downstream
[`rfc-live-bot-resilience-and-order-flow-control-v4.md`](../../myhft/docs/rfc-live-bot-resilience-and-order-flow-control-v4.md)

Проверено по коду `hftbacktest@f528aa5` и `myhft@b6101d6`; последний пока pin-ит fork на
`cf35fd5`. Упомянутые ниже ветки `myhft` — исследовательские источники дизайна, а не описание
текущего production runtime.

---

## 0. Решение в одном абзаце

Новый движок не должен быть `elapse(1_000)` и не должен быть набором Tokio-задач вокруг общего
`Arc<RwLock<State>>`. Он должен быть **single-writer reactor**: один закреплённый thread принимает
нормализованные события из тех же connector-процессов через iceoryx2, применяет целый venue frame,
обслуживает monotonic deadlines, запускает только зависящие от изменения стратегии и пропускает их
actions через единый risk/execution FSM. Цель — не «тик раз в 1 µs», а измеримый локальный путь
`IPC sample visible → coherent state commit → decision → command visible connector-у` порядка
десятков микросекунд. WAN, venue feed cadence, signing и matching engine остаются миллисекундными.

Текущий wire годится для benchmark и shadow. Он **не годится для capital canary**: market data,
fills и control events делят одну overflow-очередь, safe overflow молча теряет oldest sample, sequence
number отсутствует, а успешный `send` не подтверждает, что command принят connector-ом. До денег
нужен versioned transport v2 с явной QoS, gap detection и action correlation.

### Зафиксированные решения

| Вопрос | Решение |
|---|---|
| Где живёт движок | `EventSource`/reducer, route/FSM/risk — сначала в `myhft`; в `hftbacktest` остаются wire types и общие IPC/connector seams; exchange logic остаётся в `connector/src/<exchange>/`. Публичный reducer/API выносится только после второго consumer-а. |
| Модель владения | Один reactor-thread — единственный writer books, positions, orders, reservations, route state и timers. В hot core нет `Arc`/`Mutex`. |
| Что будит стратегию | Coherent `BookCommit`, execution/control event или due deadline. Не периодический 1-µs tick и не каждый уровень внутри venue frame. |
| Как ждём | Первый low-latency профиль — прямой nonblocking receive + busy-spin на выделенном core. Hybrid notification/park допустим только после benchmark и с более слабым cold-wakeup SLO. |
| Как упорядочиваем venues | Один owner/sequencer на lane задаёт `lane_seq` и FIFO; каждый upstream source имеет single-owner и `source_item_seq`, а fusion 0→N сохраняет item boundary через `sub_index/sub_count` или `SourceAdvance`. Control может обгонять market по зафиксированному scheduler priority. Между connector-ами — стабильный fair round-robin. По `exch_ts` события не сортируются. |
| Когда можно торговать | Только после barrier-ов snapshot/resync и causal reconciliation, свежего live frame текущей epoch, валидной двусторонней книги, закрытого batch и непрерывных source-item/subrange/lane sequences на обеих ногах. |
| Как исполняются две ноги | Неатомарная saga с parent `IntentId`, child `ActionId`, worst-case reservation, per-leg deadlines и заранее заданным recovery path. |
| Совместимость | Legacy `LiveBot`/`Bot::elapse` остаётся нетронутым. Новый core получает `LiveIpcSource` и `RecordedSource`, а wire v2 — новые service names. Он не использует optimistic `Bot::submit_order`. |

---

## 1. Что именно сегодня занимает 100 ms

Текущий путь выглядит так:

```text
venue WebSocket
  → venue parser / depth mirror
  → tokio::mpsc::unbounded_channel<PublishEvent>
  → connector publisher task: fusion + bincode
  → iceoryx2 <connector>/ToBot
  → LiveBot receive + bincode decode + state apply
  → возврат из hbt.elapse(100 ms)
  → myhft guards + strategy trade()
  → LiveRequest через <connector>/FromBot
  → connector request loop
  → venue-specific submit/cancel
```

`myhft/src/lib.rs:384,457-648` вызывает `hbt.elapse(100_000_000)`, а затем ровно один раз запускает
startup/staleness guards, recorder, circuit breakers и `trade()`. Внутри этого вызова `LiveBot`
принимает и применяет входящие depth/order/position events непрерывно
(`hftbacktest/src/live/bot.rs:713-825`). Следовательно:

- 100 ms — прежде всего **decision/coalescing cadence**, а не частота чтения shared memory;
- реакция стратегии ждёт примерно 0–100 ms, а при открытом batch может ждать дольше;
- замена `100_000_000` на `1_000` запустит guards, аллокации и полный пересчёт до миллиона раз в
  секунду, но не создаст event-driven semantics;
- tick-based windows поменяют физический смысл. Например, 20 samples volatility guard-а сейчас
  примерно равны двум секундам, а при 1-µs tick превратятся в 20 µs;
- recorder cadence `1_000_000_000 / elapse_duration` и другие tick counters также нельзя переносить
  без явной конверсии в deadlines.

### 1.1 Текущий receive — polling через sleep, не notification

`IceoryxUnifiedChannel::recv_timeout` перед каждым опросом одного connector channel вызывает
`Node::wait(Duration::from_nanos(1))` (`hftbacktest/src/live/ipc/iceoryx.rs:529-550`). В pin-нутом
`iceoryx2 0.6.1` `Node::wait` буквально вызывает POSIX `nanosleep`; он не ждёт появления sample.
Connector request-loop делает то же с 1 µs перед drain-ом commands
(`connector/src/main.rs:161-238`).

То есть нынешний путь сочетает syscall/scheduler jitter с polling. Уменьшение аргумента `elapse`
само по себе это не исправляет. Для low-latency профиля нужен публичный nonblocking `try_recv`,
который вызывает уже существующий `Subscriber::receive()` без `Node::wait`.

### 1.2 Что можно, а что нельзя сделать микросекундным

Полезно разделять границы:

```text
T0  connector socket/frame observed
T1  normalized frame enqueued
T2  last sample / BatchEnd published to iceoryx
T3  sample dequeued by engine
T4  coherent state committed
T5  decision started / finished
T6  LiveRequest published
T7  request dequeued by connector
T8  venue socket write started
T9  venue ack / fill observed
```

Основной локальный SLO — `T2 → T7`, с детализацией `T2→T3`, `T3→T4`, `T4→T6` и `T6→T7`.
`T0→T2` измеряется отдельно для каждого backend: там остаются Tokio scheduling, JSON parsing,
depth fusion и общий publisher hop. `T7→T9` включает signing, TLS/HTTP/WebSocket, WAN и venue;
обещать для него микросекунды нельзя.

`Event.local_ts` для этой задачи недостаточен: это UTC wall-clock timestamp внутри connector
handler-а, а не monotonic publish/receive time; разные backend-ы ставят его в разных местах. Cross-
venue события нельзя переставлять по `exch_ts`: часы, точность и даже семантика поля различаются.

### 1.3 Текущий downstream ещё не multi-venue execution engine

Проверенный `myhft`:

- регистрирует один instrument и запускает один connector;
- использует `asset_no == 0` во всём strategy path;
- пересчитывает desired grid и последовательно делает cancel/submit с `wait=false`;
- не имеет parent intent, резервирования двух ног, hedge deadline и compensation FSM;
- местами делает `unwrap()` на command и recorder paths;
- пишет hot-path INFO/file logs синхронно и создаёт временные `HashMap`/`Vec` при пересчёте.

Ветка `feat/signal-openpermit` уже показывает двухконнекторную регистрацию и origin-aware policy,
но остаётся 100-ms tick-driven; её `OpenPermit` — permission lattice для сетки, не двухногий FSM.
Ветка `risk/m1-reconciler-and-carry` содержит полезный чистый FSM и emergency states, но execution
events там стоят за медленным decision gate: 30-секундный naked-leg timeout может провериться лишь
на следующем 300-секундном decision tick. В новом core execution progress **никогда** не зависит от
cadence alpha.

---

## 2. Scope, goals и non-goals

### 2.1 В scope первого milestone

- Два существующих order-book connector-процесса на одном host.
- Один заранее заданный двухногий route и небольшой фиксированный universe.
- Event-driven market decision, immediate order/fill/error handling и monotonic deadlines.
- Одинаковое deterministic strategy/FSM core в recorded replay и live.
- Exploratory engineering shadow на текущем wire; binding research shadow — после минимального
  sequenced/loss-detecting MD v2; полный transport v2 — до capital canary.
- Измерение tails, burst behavior, queue age, loss и timer lateness.

### 2.2 Не в scope первого milestone

- Переписывание venue adapters или перенос стратегии в connector.
- Универсальный event bus, plugin framework или strategy DSL.
- Общая атомарная транзакция между биржами — её не существует.
- On-chain AMM simulation, bundle builder, bridge/finality и solver integration. Такой leg потребует
  отдельного execution adapter-а и новых state transitions, хотя reactor можно будет переиспользовать.
- Dynamic universe, runtime hot reload и произвольное число legs.
- Zero-copy typed wire до profiling; bincode/String могут оказаться не первым bottleneck.
- Обещание прибыли: быстрый engine уменьшает latency/leg risk, но не создаёт alpha или structural edge.

### 2.3 Definition of “microsecond-class”

Это **локальный latency budget в десятках микросекунд**, а не период loop и не end-to-end venue RTT.
До Phase 0 числа ниже — engineering hypotheses, а не SLA:

| Отрезок под объявленной нагрузкой | Предварительный target |
|---|---:|
| one sample publish-attempt stamp → dequeue, hot-spin | p99 ≤ 10–20 µs |
| outer `BatchEnd` dequeue → commit negotiated max steady-state frame | p99 ≤ 20 µs |
| commit → strategy + risk gate + **последняя обязательная leg** published | p99 ≤ 25 µs |
| полный `T2 → max(T7_leg_a, T7_leg_b)` local path | p99 ≤ 50 µs; p99.9 ≤ 100 µs |
| timer lateness под 10× measured burst | p99 ≤ 50 µs |
| cold hybrid wake, если он будет выбран | p99 ≤ 75–100 µs |

Startup/resync snapshots измеряются отдельно и не входят в steady-state target. Targets
замораживаются только вместе с host CPU, kernel, affinity, connector count, symbols, message rate,
negotiated frame size, burst shape/duration, observation count для p99.9 и strategy fixture. Среднее
значение не является acceptance metric; instrumentation overhead измеряется отдельно.

---

## 3. Safety и determinism invariants

1. **State-before-callback.** Strategy видит уже применённое целое изменение и metadata именно этого
   commit-а.
2. **No torn venue frame.** В capital-capable v2 ни один route не принимает решение по depth между
   `BatchStart` и соответствующим внешним `BatchEnd`. На v1 это недоказуемо из-за silent overflow,
   поэтому его результат всегда помечен `Integrity::Unverified` и остаётся exploratory.
3. **Single writer.** Books, positions, order ledger, reservations, route FSM и timers меняет только
   reactor-thread.
4. **Per-lane FIFO.** Ровно один lane-sequencer линеаризует события и назначает `lane_seq`; события
   одного `(process_epoch, lane)` не переставляются после этой точки. Каждый concurrent upstream
   producer имеет отдельный `source_id`, single-owner queue и свой `source_item_seq`. Fusion может
   превратить item в 0…N samples, но обязана сохранить item boundary через `sub_index/sub_count`; для
   нулевого выхода публикуется `SourceAdvance`. Общего FIFO между market/control lanes нет: их
   precedence задаёт scheduler и пишет journal. Между connector-ами используется записываемый stable
   round-robin, а не попытка восстановить «истинное» время по exchange clocks.
5. **Loss is visible in v2.** Single-owner source назначает `source_item_seq`/loss generation до своей
   первой потенциально lossy boundary, lane-sequencer — `lane_seq` в точке успешной линеаризации.
   `AtomicU64` перед cloned MPSC sender не является sequencer-ом: ticket и enqueue могут
   переставиться. Source/lane gap, epoch change, upstream full, IPC delivery failure или malformed
   frame делает venue invalid до resync/reconciliation. Если reserved control path тоже недоступен,
   connector прекращает принимать actions, делает доступный sweep и завершается.
6. **Control is not market data.** Fill/order/position/error и commands нельзя вытеснить burst-ом
   depth updates или потерять по lossy overflow policy.
7. **No strategy-side send.** Strategy выдаёт bounded intents; только central risk/execution layer
   резервирует exposure и публикует actions.
8. **No blind retry.** Timeout после возможной отправки означает `Unknown`; повтор допустим только
   после reconciliation либо при доказанной venue idempotency с тем же client id.
9. **Execution bypasses alpha cadence.** Ack, reject, fill delta, cancel/fill race, hedge deadline и
   shutdown обрабатываются немедленно, даже если detector рассчитывается раз в секунду.
10. **Timers cannot starve.** Бесконечный feed не может отложить heartbeat, stale check или hedge
    deadline без ограниченной верхней границы scheduler quantum. Каждый непредвычислимый callback
    запрещён; commit/detector/risk/egress имеют измеренный `max_nonpreemptible_ns`.
11. **Bounded memory and work.** Все hot queues, pending batch journals, action tables и telemetry
    rings ограничены. Full capacity запрещает новые entries или инвалидирует source, но не растит heap.
12. **No hot-path blocking.** После warmup в reducer→decision→action path нет file I/O, REST, locks,
    formatting и синхронного structured logging.
13. **Identity is not a symbol string.** Stable key —
    `(connector_instance, venue, market_kind, venue_symbol)`. Startup manifest назначает ему dense
    `AssetId`; mapping и hash manifest-а пишутся в journal. Spot и perp одного символа различны.
14. **Unknown exposure remains reserved.** Reservation освобождается только подтверждённым terminal
    state/reconciliation, не локальным timeout.
15. **Invalid state blocks entries, not recovery.** Stale/gap/open-batch запрещает новые intents, но
    не cancel и не явно разрешённый risk-reducing hedge по conservative recovery policy.
16. **Checked numeric core.** `f64` wire input проходит finite/sign/tick/lot validation и переводится
    в checked integer ticks/lots/fixed quote units. NaN, infinity, overflow и invalid precision
    инвалидируют route; rounding всегда conservative для соответствующей side.

---

## 4. Разделение ответственности

### 4.1 `connector/`: venue adapters остаются прежней границей

Connector по-прежнему отвечает за:

- venue WebSocket/REST, parsing, signing и auth;
- exchange-specific subscription/reconnect/rate limits;
- нормализацию market/order/position events;
- venue order submit/cancel и reconciliation;
- frame boundaries и transport publication.

В connector **не** переезжают opportunity detection, cross-venue state, risk reservations или leg
policy. Generic plumbing отвечает за lanes, envelopes, sequence, bounded delivery и recipient result.
Но ack point, recoverable client-id encoding, venue execution-id dedup и reconnect/reconcile cut
неизбежно подтверждаются тестами каждого backend-а: `Connector::submit/cancel`, которые сейчас
возвращают `()`, не могут честно породить общий `ConnectorAccepted` сразу после вызова.

### 4.2 `hftbacktest`: минимальный reusable IPC seam

Первый engine напрямую переиспользует публичные `IceoryxBuilder`, `IceoryxReceiver<LiveEvent>`,
`IceoryxSender<LiveRequest>` и wire types. Если для unified source потребуется библиотечный
nonblocking seam, он добавляется как новый `TryRecvChannel: Channel`, а не required method в
публичный `Channel`: это сломало бы внешние реализации. `recv_timeout(Duration::ZERO)` не является
корректным default — текущая реализация может вернуть timeout до первого poll.

Event reducer сначала остаётся downstream pure core. Выносить его в `hftbacktest` и создавать новый
публичный trait имеет смысл только после второго реального consumer-а. Никаких `ArbIntent`, hedge
policy или `myhft` config names в библиотечном API.

### 4.3 `myhft`: reactor, route, risk и execution saga

Downstream владеет:

- `LiveIpcSource` и `RecordedSource` с единым attributed-input contract;
- pure coherent reducer, не наследующий optimistic mutation `LiveBot::submit_order`;
- typed instrument/route config и dependency index;
- opportunity detector и executable-price calculation;
- account/inventory limits и worst-case reservations;
- parent/child execution FSM;
- recovery, shutdown и operator policy;
- complete research journal, shadow verdict и product-specific metrics.

---

## 5. Reactor architecture

```text
                         existing connector processes
                    ┌───────────┐       ┌───────────┐
venue A ⇄ adapter A │ MD / CTRL │       │ MD / CTRL │ adapter B ⇄ venue B
                    └─────┬─────┘       └─────┬─────┘
                          │       iceoryx2      │
                          └──────────┬──────────┘
                                     ▼
                     ┌──────────────────────────────┐
                     │ pinned single-writer reactor │
                     │                              │
                     │ ingress → reducer/commits    │
                     │ venue health + timers        │
                     │ action ledger + reservations │
                     └──────────────┬───────────────┘
                                    │ immutable borrow
                                    ▼
                     route detector → risk gate → actions
                                    │
                                    ▼
                         iceoryx2 command lanes

       fixed telemetry ring ───────────────→ non-hot logger/metrics thread
```

### 5.1 Core state

```text
EngineState
├── instruments[AssetId]
│   ├── committed depth / top-K executable prices
│   ├── confirmed position and order map
│   ├── readiness barrier / reconcile generation
│   └── last book version and commit time
├── connectors[connector_no]
│   ├── process/stream epochs, per-source item seq/loss generation and per-lane seq
│   ├── batch depth / age / pending journal
│   └── health, queue age and resync state
├── routes[route_no]
│   ├── dependency asset set
│   ├── eligibility / cooldown
│   └── parent intent FSM
├── actions[action_slot]
│   └── child order state and reserved exposure
└── timers
    ├── non-borrowable system/recovery slots
    └── action slots + indexed preallocated heap `(deadline_mono, slot, generation, kind)`
```

Strategy получает `DecisionView` как immutable borrow, без clone. View содержит book versions,
monotonic ages, health flags, confirmed/reserved/unknown exposure и executable VWAP для заданного
qty. Market values на входе проверяются и переводятся из wire `f64` в typed ticks/lots/fixed quote
units. Wall clock нужен только для audit/correlation.

`TimerId { slot, generation }` даёт O(1) lookup/validation slot-а. Indexed binary heap хранит не
больше одного active node на slot; schedule/reschedule/cancel обновляют/remove-ят его за O(log N),
поэтому repeated reschedule не накапливает stale nodes. Slots делятся на неотчуждаемые partitions:

- system slots заранее выделены под heartbeat, stale/batch ceiling, signal/shutdown и startup;
- recovery slots заранее выделены per connector/account под reconnect, reconcile и worst-case
  recovery fan-out, включая external/manual events;
- action slots доступны новым intents только после admission check всего их будущего timer fan-out.

Action не может занять system/recovery capacity. Его slot и heap capacity резервируются до первой
публикации; activation после известного `T6` не может отказать. Full action partition запрещает entry;
исчерпание system/recovery partition — fail-closed configuration/runtime fault, но уже выделенные
safety deadlines остаются обслуживаемыми. Deadline connector ack отсчитывается от `T6`, venue ack —
от `ConnectorAccepted`, unhedged — от первого подтверждённого fill delta, opportunity TTL — от
первого наблюдения candidate.

### 5.2 Runtime event model

Первый core не расширяет `Bot`. Он разделён на source и pure reducer:

```rust
struct AttributedInput {
    connector_id: ConnectorId,
    asset_id: Option<AssetId>,
    lane: Lane,
    process_epoch: ProcessEpoch,
    stream_epoch: StreamEpoch,
    source_id: SourceId,
    source_item_seq: u64,
    source_sub_index: u16,
    source_sub_count: u16,
    lane_seq: u64,
    loss_generation: u64,
    published_mono_ns: u64,
    payload: WireInput, // v1 LiveEvent adapter либо v2 event
}

enum PollOutcome {
    Input(AttributedInput),
    DeadlineReached,
    EndOfStream,
}

trait EventSource {
    fn poll_until(&mut self, deadline: MonotonicDeadline)
        -> Result<PollOutcome, SourceError>;
}

enum CoreEvent {
    BookCommit { asset_id: AssetId, version: u64, commit_mono_ns: u64 },
    Trade { asset_id: AssetId },
    ExecutionUpdate { action_id: ActionId, ledger_version: u64 },
    PositionUpdate { asset_id: AssetId, reconcile_generation: u64 },
    ReadinessBarrier { connector_id: ConnectorId, generation: u64 },
    ConnectorError { connector_id: ConnectorId, error: LiveError },
    SourceGap {
        connector_id: ConnectorId,
        lane: Lane,
        source_id: SourceId,
        first_lost_source_item_seq: u64,
    },
    BatchTimeout { connector_id: ConnectorId },
    Timer(TimerId),
    Shutdown,
}
```

Это sketch, не утверждённая Rust-сигнатура. Важны свойства: точный asset/origin, state уже применён,
нет clone полного event state, и execution/control cause не теряется за generic `Ok`. Authoritative
`ExecutionUpdate` появляется только после dedup/state filtering и ссылается на immutable ledger
version; raw `OrderRecvHook` использовать нельзя — сейчас он вызывается до stale/final-state filter.

`LiveIpcSource` читает существующие connector services; `RecordedSource` отдаёт тот же
`AttributedInput` по virtual monotonic clock. Source API использует typed `MonotonicDeadline`, а
`PollOutcome` различает deadline, end-of-stream и error — никакого negative-`i64` cast и ambiguous
`Option`. Legacy `LiveBot` и `Bot::elapse/wait_next_feed` в этом engine не участвуют.

`Bot::wait_next_feed()` остаётся полезным diagnostic baseline, но не production seam: он не называет
asset/connector/version, не даёт нужный control-event contract и после batch может вернуть `Ok`.

### 5.3 Coherent commit, batch и Bybit

`BatchStart/BatchEnd` сегодня лишь откладывают возврат стратегии: depth меняется по каждому sample.
Кроме того, любой открытый connector batch задерживает result всех connector-ов, а через 250 ms
ceiling legacy path может вернуть управление на потенциально torn state
(`hftbacktest/src/live/bot.rs:89-110,713-815`). Для нового API это неприемлемо.

Сначала нужен canonical depth contract. Текущий `LiveBot::process_event` применяет только
`LOCAL_*_DEPTH_EVENT` kind 1 и trades, но двигает `last_feed_latency` на любом `Feed`; snapshot,
BBO и clear могут быть проигнорированы. `FusedHashMapMarketDepth` также создаёт synthetic cross-side
delete без `LOCAL_EVENT` (`hftbacktest/src/depth/fuse.rs:159-175` и симметричные ветки), поэтому бот
может не удалить stale opposite level. Новый reducer обязан:

- принимать один явный book protocol: reset/snapshot seed, canonical local kind-1 deltas и commit;
- либо явно нормализовать BBO/snapshot/clear, либо отклонять их — никогда не считать проигнорированный
  event свежестью книги;
- валидировать finite price/qty, side, tick/lot и crossed/one-sided result;
- обновлять readiness/staleness только на успешно применённом coherent `BookCommit`;
- иметь regression tests на delta, snapshot/reset, BBO, clear и synthetic opposite-side delete.

Минимальная корректная реализация — **committed book + bounded pending delta journal per connector**:

1. На внешнем `BatchStart` открыть journal; nested starts увеличивают depth.
2. Market events этого connector-а декодировать, но не применять к committed book.
3. Order/position/error events применять и возвращать reactor-у немедленно.
4. На закрытии внешнего batch применить journal в wire/ingress order, обновить версии затронутых
   assets и выдать `BookCommit` для каждого dirty asset в стабильном порядке.
5. При journal overflow, source-item/subrange/lane gap или timeout discard journal, пометить
   connector invalid и потребовать resync/restart. Никогда не публиковать torn state.

До journal transport обязан сохранить 0→N provenance текущей fusion. Один входной source item может
быть отфильтрован либо породить несколько synthetic events. V2 представляет его как logical item:

- выходы несут одинаковый `source_item_seq`, `sub_index=0..sub_count-1` и общий `sub_count`;
- нулевой выход всё равно публикует компактный `SourceAdvance { sub_count: 0 }`;
- `lane_seq` увеличивается для каждого физического output/advance;
- следующий `source_item_seq` принимается только после всех sub-items предыдущего; missing/duplicate
  sub-index или превышение negotiated `max_sub_count` инвалидирует source до apply/commit.

Такой journal проще, чем clone полного depth на каждый frame. Но commit journal-а — один
непрерываемый участок. До canary его WCET доказывается на negotiated maximum frame. Если он не
укладывается в `max_nonpreemptible_ns`, deltas применяются в staging book по мере приёма с O(1) swap
на `BatchEnd`, либо вводится bounded frame-level payload/double buffer.

Binance, Hyperliquid и Lighter уже обрамляют multi-level frame. Bybit сейчас публикует уровни одного
`orderbook` message без `BatchStart/BatchEnd` (`connector/src/bybit/public_stream.rs:279-357`). До
любого live route Bybit должен получить generic frame boundary; иначе event reactor увидит
полукнигу после первого уровня.

На v1 journal остаётся best-effort: silent overflow может удалить сам `BatchStart`, после чего
уровень выглядит самостоятельным commit-ом. Поэтому Phase-1/2a shadow обязан маркировать integrity
как недоказанную; формальный no-torn contract начинается только с v2 item/lane sequence и loss
generation,
назначенной до всех lossy boundaries.

### 5.4 Scheduler и fairness

Reactor не «ждёт следующий тик». Один turn:

1. Drain execution/control lane до bounded quantum.
2. Обработать все overdue hard deadlines, включая hedge, stale/batch ceiling и heartbeat каждого
   trading connector-а.
3. Poll connector market lanes в стабильном round-robin; на connector — не больше
   `max_events_per_turn` **или** `max_drain_ns`.
4. Закрыть готовые commits, отметить dirty routes через dependency index.
5. Запустить bounded pure detector только для eligible dirty routes.
6. Пропустить proposals через risk gate, зарезервировать exposure, записать actions в preallocated
   buffer и опубликовать их.
7. Проверить async-signal-safe shutdown flag и снять numeric telemetry snapshot, если due.

Начальная benchmark-гипотеза — 32 events или 5–10 µs на connector quantum. Это не production
default до измерения. Hard deadline проверяется после каждого общего quantum; noisy venue не может
starve-ить второй connector или naked-leg timer. Heartbeat — firm safety deadline: send failure
становится origin-aware health transition, а не drop.

Single writer не умеет preempt-ить зависший strategy callback. Поэтому commit, detector, risk и
egress получают отдельные WCET budgets и общий `max_nonpreemptible_ns`; runtime overrun немедленно
запрещает новые entries и переводит engine в configured degraded/fatal state. Проверки deadlines
стоят между routes и actions. Первый milestone с одним frozen route не вызывает user/plugin code в
hedge/recovery path. Будущий дорогой alpha может работать off-thread только на immutable versioned
snapshot; его результат проходит повторную проверку versions, ages и TTL перед risk gate.

Нет искусственного debounce/micro-batch поверх явного venue frame. Вне batch решение принимается на
каждый применимый commit. Coalescing разрешается только после измеренного churn и с сохранением
first-seen time/opportunity TTL.

Deadline capacity резервируется до первой leg. После каждого successful publish reactor фиксирует
`T6` и активирует подготовленный timer до следующего poll, поэтому быстрый ack не может быть
обработан раньше timer registration. Две remote queues атомарно зарезервировать нельзя; failure
второго publish после успешного первого — штатный `OneLegPublished → Recovering`, а не невозможная
ошибка.

### 5.5 Busy-spin, notification и CPU contract

Для первого latency profile reactor напрямую вызывает `try_recv` и busy-spin-ит с
`std::hint::spin_loop()` на выделенном CPU. Connector command loop получает тот же nonblocking path.
Это самый простой способ убрать `nanosleep` jitter и не вводить lost-wakeup protocol.

Цена явная: два connector-а + engine требуют как минимум три зарезервированных hot cores, плюс cores
для async venue/publisher work. Busy request-loop выносится на отдельный OS thread: pin всего process
не должен посадить Tokio runtime на тот же core. В fixture фиксируются physical core и SMT sibling,
CPU affinity, migrations, performance governor, release build и NUMA. `SCHED_FIFO`, `mlockall` и
kernel isolation добавляются только после обычного pinned benchmark и watchdog; realtime scheduling
не является стартовой точкой.

Если deployment не может отдать cores, можно добавить companion iceoryx Event service и WaitSet:

```text
publisher: publish data → notify
consumer: drain → recheck data → timed wait → drain
```

Notification — только hint; correctness держится на queue/sequence, а не на числе wakeups. Wait
ограничен `min(nearest_deadline, max_park_interval)`, чтобы `sample sent → publisher died before
notify` не оставил данные спать секунды. После cold wake reactor может spin-ить `spin_budget_us` для
burst. Hybrid профиль принимается только если он проходит отдельный cold-wakeup SLO. Текущий
`Node::wait(1ns)` не является ни одним из этих двух режимов.

После удаления `Node::wait` SIGTERM/SIGINT доставляется через async-signal-safe bridge — atomic flag
или eventfd — и проверяется каждый bounded turn. Повторный signal или истёкший shutdown deadline
переходит в emergency non-zero exit.

### 5.6 Deterministic replay

Каждому **dequeued** input reactor назначает `ingress_seq`. Complete research journal хранит origin,
epochs/sequences, commit markers, virtual-monotonic delta, выбранный scheduler order, config/manifest
hash, code/binary hashes, clock metadata, record checksum и format version. Replay того же journal
при том же config обязан выдавать одинаковые intents/actions.

Tie-break на одном timestamp:

1. уже dequeued и принятые reducer-ом execution/control events;
2. hard deadline;
3. market commit;
4. soft/telemetry timer;

внутри класса — recorded `ingress_seq`. Event, который физически лежал глубже в queue, но не был
dequeued к deadline, консервативно считается поздним: action становится `Unknown`. Поздний correlated
ack/fill может разрешить `Unknown`, но не стирает timeout audit. Это проще и воспроизводимее, чем
доверять source timestamps и пытаться drain-ить до несуществующего глобального watermark.

Обычный historical dataset без live batch/arrival metadata может использовать тот же strategy core,
но не доказывает latency fidelity. Для parity нужен recorded-ingress replay; grouping по одинаковому
`exch_ts` не заменяет venue frame boundaries.

Research journal и lossy telemetry — разные outputs. Если complete journal queue заполнена, shadow
run помечается invalid и прекращает decision/capture; публиковать его economic verdict нельзя.
Telemetry ring может drop-ать records с counter-ом и никогда не влияет на correctness.

---

## 6. Multi-venue readiness и opportunity contract

### 6.1 Health state per connector/instrument

Для каждой ноги отдельно хранятся:

- connector process epoch и public/private stream session epochs;
- last complete `source_item_seq`/subrange/loss generation каждого source и `lane_seq` каждого lane;
- snapshot/resync barrier watermarks и reconciliation generation;
- двусторонняя валидная book и book version;
- last committed monotonic timestamp и age;
- batch depth/age и journal capacity;
- channel/connector health;
- reconciliation state и unknown actions;
- account-risk epoch, causal watermark, freshness и authoritative balances/equity/available margin,
  borrow limits и inventory, включая явные нулевые значения.

`SnapshotComplete` сам по себе недостаточен: он говорит про flushed cached state, но не гарантирует
готовую bid/ask book, завершённый venue-side sweep или свежую позицию. В v1 `position_observed` может
законно не открыться для flat account; bounded startup timeout заканчивается non-zero, а ноль никогда
не предполагается. V2 обязан уметь выразить явный reconciled `{ qty: 0 }`.

При process restart **или** venue stream reconnect внутри живого connector-а соответствующая session
epoch меняется и запускает mini cold-start независимо для каждого connector-а:

1. немедленно invalid market state; private reset также блокирует action ownership до reconcile;
2. `ResyncBegin(generation)`;
3. применить committed registration/book snapshot;
4. получить хотя бы один fresh live frame текущей session epoch либо backend-specific доказательство
   live subscription с causal watermark;
5. завершить causal order/position reconciliation, включая явный flat state;
6. получить freshness-bounded authoritative risk snapshot текущей account/private-stream epoch:
   balances, equity/available margin, borrow limits и inventory;
7. получить `ResyncComplete/ReconcileComplete` с high-water marks всех участвующих lanes и risk
   source;
8. разрешить entry только после consumption этих watermarks.

Разделение lanes иначе допускает, что control `SnapshotReady` обгонит ещё лежащий в MD lane snapshot.
Для v2 нужен barrier/global publication generation вида
`{ generation, md_epoch, md_lane_seq, ctrl_epoch, ctrl_lane_seq, risk_epoch, risk_watermark }`.
Один invalid/stale leg или risk source запрещает новые entries; открытая exposure переводится в
предопределённый recovery path.

Reconciliation имеет causal contract: `ReconcileStart(generation)` → venue order/position/risk
snapshots с определённым cut → buffer/merge private events после cut → snapshot apply → replay
buffered events → `ReconcileComplete { generation, high_water }`. Поздний REST position или margin
snapshot не имеет права затереть fill, пришедший после начала request-а. Static strategy cap может
быть только дополнительным ceiling над этой account truth, но не заменяет её.

### 6.2 Cross-venue consistency

Одновременного snapshot двух бирж не существует. `DecisionView` фиксирует для каждой ноги её
`book_version`, `commit_mono`, `age` и cross-venue age skew. Detector обязан проверить:

- обе книги coherent и не stale;
- разница commit ages не превышает route limit;
- цены executable для requested qty, а не только BBO;
- tick/lot/contract-multiplier rounding, min notional и доступную глубину записанного top-K;
- venue semantics marketable-limit/IOC/FOK и допустимое поведение partial fill;
- fees с asset/precision, quote/basis conversion и conservative action-delay haircut;
- доступный confirmed inventory/margin минус уже reserved worst-case exposure;
- opportunity TTL остаётся положительным.

Ожидать «барьер, когда обе биржи обновились одновременно», и сортировать по exchange timestamp
нельзя: это создаёт искусственное время и добавляет latency.

### 6.3 Dependency index

Config при startup строит `AssetId → routes[]`. `BookCommit(AssetId)` помечает только эти routes.
Order/fill event шагает execution FSM своего parent intent и не запускает полный scan universe.
Первый milestone ограничивает route двумя legs и фиксированным capacity; универсальный graph engine
не нужен.

Current v1 router адресует `(connector instance, symbol)` и отвергает duplicate symbol внутри одного
instance. Поэтому spot/perp с одинаковым venue symbol до v2 обязаны использовать разные connector
service instances. V2 registration один раз сопоставляет stable `InstrumentKey` с numeric
`InstrumentId`; steady-state payload не маршрутизируется по повторяющемуся `String`.

---

## 7. Execution и risk FSM

### 7.1 Две ноги — saga, не атомарная операция

Даже две back-to-back IOC/FOK команды имеют leg risk. До первой публикации engine:

1. проверяет health и opportunity TTL;
2. нормализует обе legs в exposure units с contract multiplier и conservative lot rounding;
3. перебирает диапазон возможных fill combinations и резервирует per account/venue/asset/currency
   worst-case position, margin, fees, slippage, request budget и отдельный recovery headroom;
4. резервирует parent/child/timer slots обеих ног;
5. создаёт `IntentId { engine_epoch, counter }` и child `ActionId` — это tuple/newtype, не сумма;
6. детерминированно кодирует ActionId в venue client id с проверкой charset/length/collision;
7. публикует legs в заранее заданном порядке и запускает deadlines от точек §5.1.

Порядок/режим не выбирается post hoc. Для конкретного arm заранее фиксируется один из явно
реализованных вариантов, например `ParallelMarketable` или `LeadThenHedge`; первый milestone не
строит универсальный policy DSL и держит максимум один non-terminal parent intent на route.

Reservation ledger хранит не один signed scalar, а диапазон возможных outcomes по каждому account и
asset. Transfer invariant:

```text
proposal reserve → outstanding/unknown child reserve → confirmed inventory
```

Release возможен только по доказанному no-side-effect terminal state или causal reconciliation.
Partial fills, одновременные fills, unequal lot sizes, fees, dust и recovery orders не могут занять
headroom, уже обещанный другому intent-у.

### 7.2 Parent intent states

```text
Proposed
  → RiskReserved
  → Dispatching
  → NoLegPublished | OneLegPublished | BothLegsPublished
  → WorkingBoth | ExposedOneLeg | Recovering
  → EconomicallyHedged
  → AwaitingVenueTerminal
  → Reconciled

Любой non-terminal state
  → Recovering
  → Reconciled | HaltedUnknownExposure
```

`EconomicallyHedged` означает, что normalized net exposure находится внутри заранее заданного
tolerance после lot rounding. Это не означает, что venue orders terminal. Late fill может снова
вывести exposure за tolerance и вернуть parent в `Recovering`.

### 7.3 Child action states

```text
Planned
  ├──→ NotDispatched
  └──→ PublishedLocal
       ├──→ ConnectorRejected
       ├──→ ConnectorAccepted → VenuePending
       │                         ├──→ PartiallyFilled (0..N)
       │                         ├──→ CancelRequested
       │                         └──→ Filled | Canceled | Rejected | Expired
       └──→ Unknown

CancelRequested
  → Canceled | PartiallyFilled | Filled | CancelUnknown
```

`PublishedLocal` означает только, что sample помещён в IPC. `ConnectorAccepted` означает, что
backend локально провалидировал command и успешно передал ownership bounded venue path. Это всё ещё
не venue ack. Generic receive-loop не имеет права послать ack сразу после нынешнего
`Connector::submit/cancel`: backend-ы queue/spawn-ят работу по-разному. Нужен typed backend result
`Accepted(ActionId)|Rejected`, и каждый backend тестирует точную ack point. `ConnectorRejected`
допустим только когда venue write точно не начинался; иначе state — `Unknown`.

Authoritative execution ledger update содержит как минимум `ActionId`, venue order id, venue exec id
или иной dedup key, cumulative и last fill qty, leaves, status, fee asset/amount и ledger version.
External/manual order может иметь `ActionId=None`, но всё равно обновляет account exposure и запускает
reconciliation policy.

Правила:

- partial fill немедленно переводится в normalized exposure; hedge округляется по правилам второй
  venue, а residual/dust остаётся booked и reserved до tolerance/rebalance;
- fill после cancel, duplicate ack и out-of-order terminal update идемпотентны;
- local/connector timeout после возможной отправки → `Unknown`, запрет новых intents на route и
  reconciliation; blind retry запрещён;
- `NotDispatched` разрешён только при доказанном отсутствии side effect;
- successful first publish + failed/full/zero-recipient second publish → `OneLegPublished`, весь
  reserve сохраняется и немедленно запускается recovery;
- overfill, unequal fills обеих legs и late fill после `EconomicallyHedged` пересчитывают exposure и
  могут снова открыть recovery;
- send failure не оставляет optimistic local order без соответствующего `Unknown/Rejected` state;
- action-table full, request budget breach или reservation failure → reject proposal до первой leg;
- reconciliation использует venue truth и только затем освобождает unknown reservation;
- balance/available margin/borrow сейчас не приходят по общему wire. До capital canary обязателен
  отдельный authoritative, freshness-bounded и causally reconciled risk source; его epoch/watermark
  входит в readiness barrier. Static cap остаётся лишь дополнительным strategy ceiling. При
  отсутствующей/stale account truth entry запрещён; положительный static cap её не заменяет.

### 7.4 Shutdown

SIGTERM/SIGINT не должен просто убивать process. Он запрещает entries, но дальнейший порядок зависит
от parent/child state. Слепое «cancel всё, затем hedge» опасно: cancel уже работающей hedge leg может
увеличить naked exposure. До canary нужна transition table как минимум для `NoLegPublished`,
`WorkingBoth`, `ExposedOneLeg`, `EconomicallyHedged`, `Unknown` и venue-down:

1. перестать создавать новые intents, продолжая heartbeat всех trading connectors;
2. сохранить/поставить risk-reducing hedge там, где cancel сначала увеличит exposure;
3. cancel remaining risk-increasing/resting orders;
4. bounded wait на correlated acks и causal reconciliation;
5. сохранить unresolved action/exposure summary;
6. exit `0` только при подтверждённом safe state, иначе non-zero для operator/supervisor.

Существующий wrapper уже делает TERM engine → wait engine → TERM connector; новый path использует
этот порядок и pin-ит integration test плюс `systemd TimeoutStopSec`. Второй signal или timeout —
emergency non-zero path. Если connector умер первым, safe reconciliation может быть недоступна:
engine сохраняет unresolved state, но не объявляет его safe.

Connector liveness sweep — последняя защита только для backend-а, который прошёл capability test
`bot death → venue orders actually canceled`. Иначе первый route использует доказанно non-resting
orders и отдельную recovery гарантию; документированный no-op sweep защитой не считается.

---

## 8. IPC v2: integrity важнее ещё 5 µs

### 8.1 Почему v1 разрешён только для shadow

Сейчас `<connector>/ToBot` смешивает Feed, Order, Position, Error, SnapshotComplete и batch markers;
`<connector>/FromBot` смешивает Order, Register и Heartbeat. Buffer по умолчанию — 10,000 samples
(`hftbacktest/src/live/ipc/config.rs:5-32`). Builder не фиксирует overflow behavior, поэтому
iceoryx2 0.6.1 наследует `enable_safe_overflow=true`: полный buffer заменяет oldest unread sample
конкретного subscriber-а.

Header содержит только `{ id, len }` (`hftbacktest/src/live/ipc/iceoryx.rs:35-40`). Sequence/gap
нет. В результате одинаково молча могут исчезнуть depth delta, `BatchEnd`, fill, error или исходящий
order command. Выключить safe overflow глобально тоже недостаточно: один медленный bot способен
заблокировать общий publisher, а upstream `tokio::mpsc::unbounded_channel` продолжит расти.

Кроме того, connector broadcast-ит union зарегистрированных symbols всем bots; чужой target/sample
декодируется до фильтрации. Latency benchmark должен использовать отдельный connector instance, а
не production service с другими consumers.

### 8.2 Lanes и QoS

V2 вводит новые versioned service names и минимум три логических lane:

| Lane | Semantics | Overflow/full behavior |
|---|---|---|
| Market data → engine | Один engine/subscriber на isolated connector instance; bounded, no silent overwrite | Observable full/delivery failure → `SourceInvalidated`; receiver gap → invalidate before post-gap commit |
| Execution/control → engine | Loss-detecting bounded delivery order/fill/position/error/reconcile; duplicates допустимы | Никакой silent overwrite; corruption/failure → action `Unknown` + reconcile |
| Commands → connector | Loss-detecting bounded command stream + backend-owned ingress ack | Full/zero recipient → `NotDispatched`; ambiguous acceptance → `Unknown` |
| Maintenance | Coalesced heartbeat/health или reserved slot | Не занимает capacity, зарезервированную для order command |

Для первого canary фиксируются один engine, один subscriber и isolated account на connector
instance; multi-bot routing deferred. «Loss-detecting» намеренно не называется exactly-once или
durable: duplicates возможны, а process crash всё равно требует reconciliation. Конкретный iceoryx
primitive — отдельная per-engine pub/sub service или request/response — выбирается Phase-3 spike;
hard contract важнее заранее выбранного API. Нельзя блокировать общий connector publisher
бесконечно.

Loss contract проходит через **все** boundaries:

1. Каждый concurrent venue producer получает стабильный `source_id`, собственную bounded SPSC
   queue и ровно одного owner-а. Этот owner последовательно назначает `source_item_seq` и
   `loss_generation` raw item-у непосредственно перед `try_send`; cloned MPSC senders с общим atomic
   counter запрещены.
2. `try_send(full)` source queue посылает
   `SourceInvalidated { lane, source_id, first_lost_source_item_seq, loss_generation }` по reserved
   control path. Поэтому потеря до fusion/lane merge тоже имеет точный origin.
3. Ровно один lane-sequencer детерминированно drain-ит source queues, выполняет 0→N normalization и
   присваивает каждому output `(source_item_seq, sub_index, sub_count)`. Для N=0 он публикует
   `SourceAdvance`; scalar source sequence без этого marker/subrange запрещена.
4. Lane-sequencer назначает `lane_seq` каждому physical output/advance в точке успешной lane/IPC
   линеаризации. Он один владеет publisher-ом; publication failure инвалидирует lane.
   `source_item_seq + subrange` обнаруживает upstream/fusion loss, `lane_seq` — loss/reorder после
   merge.
5. Capital-profile MD IPC не использует silent safe overflow: один subscriber, safe overflow off,
   nonblocking observable delivery result и обязательная проверка recipient count. Если выбранный
   iceoryx primitive этого не даёт, он не проходит Phase-3 spike.
6. Receiver проверяет contiguous item transitions, полный ordered subrange и `lane_seq` до
   apply/commit. Source-item, subrange или lane gap запрещает первый post-gap commit.
7. Если reserved control path недоступен, connector quiesce-ит actions, выполняет доступный sweep и
   выходит; обычный panic без controlled stop недостаточен.

`safe_overflow=true` допустим только в exploratory v1/v2 shadow. Непрерывная, но отставшая очередь
также опасна: `max_ingress_age` участвует в eligibility. Асинхронную race «loss случился сразу после
health check» устранить нельзя; binding contract формулируется точно: upstream full немедленно
инвалидирует source через control path, receiver gap обнаруживается до commit первого post-gap event,
а окно до наблюдения ограничивается freshness/TTL и учитывается как distributed failure.

Общий внутренний `PublishEvent` path перед lanes становится bounded с отдельной reserved control
queue. Переполнение control queue останавливает connector fail-closed. Увеличение buffer не заменяет
эти правила.

### 8.3 Envelope

Изменять текущий `CustomHeader` на тех же services нельзя: это несовместимый iceoryx type. V2
использует новый fixed-width header, концептуально:

```text
protocol_version
lane
process_epoch       # меняется при restart connector-а/engine
stream_epoch        # меняется при public/private venue reconnect/resync
source_id
source_item_sequence # монотонно на (process_epoch, stream_epoch, lane, source_id)
source_sub_index
source_sub_count    # 0 означает explicit SourceAdvance
lane_sequence       # назначает single owner; монотонно на (process_epoch, lane)
loss_generation
published_mono_ns   # same-host monotonic clock
destination_id
instrument_id       # numeric mapping из startup manifest/registration
frame_id            # одинаковый у Start/data/End одного venue frame
payload_len
flags
```

Process или stream epoch change инвалидирует market state до resync barrier; private stream change —
action/position state до causal reconciliation. Logical source item включает frame markers и все
synthetic outputs; matching `frame_id` не даёт чужому/nested `BatchEnd` закрыть не тот frame.
Snapshot/resync и reconcile messages несут source-item/lane high-water marks и global publication
generation.

Для commands payload несёт `IntentId`, `ActionId` и client order id. Собственный backend считается
поддержанным для canary только если ActionId recoverable через venue client id или persisted mapping;
acks/errors/order updates его повторяют, fill несёт venue execution/dedup id. Stable error envelope
также содержит lane, epochs, ActionId и machine-readable class. Uncorrelated external/manual events
допустимы, но обязаны менять account exposure и запускать policy.

Новые wire variants добавляются только парой connector+consumer; в legacy enum — только в конец.
Предпочтительнее не расширять legacy ABI, а поднять `/v2` services и dual-run в shadow.

### 8.4 Monotonic timestamps

Для same-host stages используется один документированный monotonic source, предпочтительно
`CLOCK_MONOTONIC_RAW` либо измеренно эквивалентный clock wrapper. В payload/header не смешиваются:

- monotonic latency/ordering timestamps;
- UTC audit timestamp;
- venue exchange timestamp.

Обязательные точки: socket observed, publisher enqueue, IPC publish, engine dequeue, commit,
decision start/end, command publish, connector dequeue, socket write, venue ack/fill.

`published_mono_ns` ставится непосредственно перед `sample.send()` и является timestamp publish
attempt, а не доказанным моментом visibility. Recipient result и receiver timestamp хранятся
отдельно; one-way benchmark калибрует разницу.

---

## 9. Observability вне hot path

Hot core пишет только fixed-size numeric records:

```text
TraceRecord {
  mono_ns, ingress_seq, process_epoch, stream_epoch,
  source_id, source_item_seq, source_sub_index, source_sub_count, lane_seq,
  connector_id, asset_id, event_kind,
  intent_id, action_id,
  price_ticks, qty_lots, flags
}
```

Records идут в preallocated SPSC/flight-recorder ring. Telemetry может потерять record только с
инкрементом `telemetry_dropped`; она никогда не backpressure-ит execution. Раз в секунду background
thread получает immutable counter snapshot и делает `tracing`, JSON, wall-clock conversion и disk I/O.

Минимальные метрики:

- per-lane queue depth/high-water и oldest event age;
- process/stream epoch changes, source-item/subrange/lane gaps и resync count;
- batch depth/age/journal high-water;
- timer lateness;
- `T0…T9` histograms p50/p95/p99/p99.9/max;
- reactor turns, events per turn, dirty routes и strategy runtime;
- command full/zero-recipient, connector ack/venue ack latency;
- unknown actions, confirmed/reserved/unknown exposure;
- CPU migrations, context switches, allocations after warmup;
- telemetry drops.

Fatal audit summary может логироваться синхронно уже после запрета новых actions.

Эта lossy flight-recorder ring **не** является complete research journal из §5.6. Journal имеет
собственную bounded writer queue; её overflow останавливает/инвалидирует research run, а не теряет
record и не продолжает считать результат.

---

## 10. Tests и benchmark contract

Все money/state behavior changes делаются TDD.

### 10.1 Deterministic unit tests

- State применён до delivery `CoreEvent`.
- Event содержит правильные asset/connector/version.
- Delta/snapshot-reset/BBO/clear и synthetic cross-side delete дают canonical book result; ignored
  feed не двигает staleness clock.
- Interleaved batches двух connector-ов не освобождают друг друга.
- Nested batch commit выдаётся только на внешнем `BatchEnd`.
- Mismatched/duplicate `frame_id` инвалидирует source, а не закрывает соседний frame.
- Bybit frame даёт один commit; strategy не видит отдельные уровни.
- Lost `BatchEnd`, journal overflow и source-item/subrange/lane gap не публикуют torn book.
- Execution/control event проходит при открытом market batch.
- Market flood не задерживает hard timer и второй connector бесконечно.
- Indexed timer heap держит не больше одного node на slot; long reschedule/cancel soak не расходует
  capacity, а full capacity запрещает entry.
- Action timer saturation не занимает reserved heartbeat/stale/reconcile/recovery/shutdown slots;
  worst-case future timer fan-out проверяется до admission.
- Strategy WCET overrun блокирует entries; heartbeat остаётся bounded в idle/flood/overrun режимах.
- Одинаковый recorded journal даёт одинаковые intents/actions.
- Timeout/event tie-break соблюдает правило §5.6.

Pure reducer/source tests используют deterministic fake queues/clock. Если появится библиотечный
`TryRecvChannel`, существующий `MockChannel` получает отдельную реализацию/pins. Новые behavior pins
должны падать при мутации соответствующей ветки, а не только проверять happy path.

### 10.2 Execution/FSM tests

- partial fills обеих ног, lot/contract mismatch, residual tolerance и hedge по normalized exposure;
- reject первой/второй ноги;
- successful first local publish + failed second publish → `OneLegPublished/Recovering` с сохранённым
  reserve;
- cancel/fill race и fill после cancel;
- duplicate/out-of-order ack/fill/terminal status;
- IPC published, connector ack lost, venue order реально live → `Unknown` без blind retry;
- late ack/fill разрешает `Unknown`, но timeout audit сохраняется;
- unequal simultaneous fills, overfill и late fill после `EconomicallyHedged`;
- restart/reconciliation с unknown actions;
- fill после начала REST reconcile request, чей snapshot response приходит позже fill;
- stale/missing balance, margin или borrow snapshot запрещает entry; static cap не открывает gate;
- shutdown во всех parent/child states;
- SIGTERM под market flood и повторный signal/expired shutdown deadline;
- per-account/asset outcome range, margin и recovery headroom никогда не превышают caps;
- route не открывается при stale/gap/open-batch/one-sided book;
- risk-reducing cancel/hedge остаётся доступен в invalid market state по recovery policy;
- action table/queue full запрещает entry до отправки первой ноги.

Для FSM полезны property/model tests: terminal transitions идемпотентны, уникальный fill delta создаёт
не более одного exposure update, unknown reservation не освобождается без evidence, checked
tick/lot/fixed-quote arithmetic не переполняется и всегда округляется conservative.

### 10.3 IPC/fault injection

- explicit safe-overflow configuration проверяется при create **и** open existing service;
- source-item/subrange/lane gap/loss generation, process/stream epoch reset, corrupt payload, zero
  recipient и full queue;
- forced interleaving concurrent producers не переставляет `lane_seq`: single-owner source queues и
  lane-sequencer сохраняют origin/FIFO; `AtomicU64` + cloned MPSC mutation обязана валить test;
- fusion 0→N сохраняет один `source_item_seq`, полный `sub_index/sub_count`; N=0 даёт
  `SourceAdvance`, missing/duplicate sub-item инвалидирует source до commit;
- bounded source queue full до lane sequence assignment всё равно выдаёт точный
  `SourceInvalidated { source_id, first_lost_source_item_seq }`;
- public/private feed reconnect без process restart;
- snapshot/reconcile barrier не открывается до consumption MD/control high-water marks;
- publisher death между data sample и optional notification;
- notify-before-wait/missed hint для hybrid режима;
- control lane не зависит от flooded market lane;
- bounded upstream queue не растёт при stalled subscriber;
- connector restart, feed reconnect и resnapshot.
- `ConnectorAccepted` каждого backend-а приходит только после validation + successful bounded
  ownership transfer; rejected/failed handoff не маскируется ack-ом.
- engine-safe shutdown завершается до TERM connector-а; backend sweep capability проверяется live
  test-ом либо route объявляет его unsupported.

### 10.4 Benchmarks

Отдельно измеряются:

- bincode decode/encode и allocation count;
- один book update и полный representative frame commit;
- top-K executable VWAP/detector/risk gate;
- one-way IPC и fake-connector round trip;
- 1/2/4 connectors, normal load, 10× и 100× microburst;
- pure polling, dedicated spin и, если реализован, hybrid 0/5/20/50/100 µs;
- quiet first event и sustained burst;
- recorder/logger enabled/disabled;
- CPU/core usage, migrations, context switches и queue age.

Criterion подходит для micro-ops; tails измеряет отдельный preallocated harness. Все benchmark
results сохраняют config, binary/git hashes, host/kernel/CPU/governor/affinity и raw histogram.

### 10.5 Hard acceptance gates перед canary

- zero silent execution/control loss;
- upstream full немедленно инвалидирует source через reserved control path; receiver gap обнаружен
  до commit первого post-gap event; `max_ingress_age` и TTL ограничивают окно distributed race;
- zero hot-path allocations после warmup для v2 fixed-ID path либо явно принятый и измеренный bounded
  остаток. V1 bincode/String этому gate заведомо не соответствует;
- bounded memory в long soak;
- action-timer exhaustion не отнимает non-borrowable system/recovery deadlines;
- no **new entry** на invalid/stale/open-batch state; только заранее определённые risk-reducing actions;
- deterministic replay actions;
- complete research journal без gaps; journal overflow инвалидирует run;
- authoritative account-risk source fresh, causally reconciled и включён в readiness barrier;
- successful restart reconciliation и graceful shutdown;
- frozen p99/p99.9 latency SLO выполнен под declared normal/10× load;
- экономический detector/strategy gate пройден отдельно. Быстрый нулевой edge не допускается к
  capital только потому, что engine быстрый.

---

## 11. Phased implementation plan

### Phase 0 — baseline, без изменения trading behavior

1. Зафиксировать host/load fixture и измерить текущие `T0…T9` настолько, насколько позволяет v1.
2. Добавить raw IPC ping/echo harness и queue/backlog metrics.
3. Сравнить current `Node::wait`, direct nonblocking spin и optional Event/WaitSet spike.
4. Измерить publisher `unbounded_channel` age и representative Bybit/Binance/HL frame cost.
5. После измерений заморозить latency targets и CPU budget.

Deliverable: reproducible benchmark report. Никаких live orders.

### Phase 1 — prerequisite hardening и downstream event core

1. Перевести `myhft` с `cf35fd5` как минимум на fork revision с multi-connector hardening:
   per-connector batch state, origin-aware errors/channel failures, heartbeat fan-out и
   `position_observed`.
2. Зафиксировать startup manifest и multi-instrument config без `asset 0` hardcodes; конфликтующие
   spot/perp symbols временно разводить по connector service instances.
3. Добавить Bybit frame boundaries и canonical depth tests/fixes из §5.3.
4. В `myhft` через TDD реализовать `LiveIpcSource`, `RecordedSource`, pure reducer, bounded batch
   journal, typed monotonic clock и checked numeric normalization. Публичный `Bot` не расширять.
5. Реализовать pure single-writer core, dependency index, indexed timer heap с non-borrowable
   system/recovery partitions и immutable `DecisionView`; egress выключен.
6. Конвертировать tick-count guards/recording в monotonic deadlines или event-defined windows.

Deliverable: deterministic no-order source/reducer API. Pin bump и handler signature changes идут
отдельным small commit; текущие исследовательские ветки не мержатся целиком.

### Phase 2a — exploratory v1 shadow

1. Запустить отдельный data-only connector instance/account, чтобы shadow reader не влиял на
   production bot liveness/ownership.
2. Писать complete recorded-ingress journal и проверять replay identity.
3. Считать counterfactual opportunities/actions, queue age и latency, помечая
   `Integrity::Unverified` из-за v1 silent overflow.

Deliverable: engineering prototype и exploratory report. Он не является binding economic verdict.

### Phase 2b — минимальный loss-detecting MD v2

1. Ввести single-owner bounded queue на source, `source_item_seq` до неё, explicit 0→N
   subrange/advance contract и единственный lane-sequencer с `lane_seq`; сделать bounded MD delivery
   без silent overwrite и проверить recipient result.
2. Добавить process/stream epochs, frame sequence, source-item/subrange/lane invalidation и
   resync/snapshot watermarks.
3. Зафиксировать `max_ingress_age`, complete journal и dual-run v1/v2 comparison.
4. Повторить frozen shadow study на untouched interval.

Deliverable: binding opportunity/capacity verdict. Полный execution transport строится только если
этот gate экономически положителен.

### Phase 3 — control/action v2 и backend capability contracts

1. Loss-detecting control/command/maintenance lanes с explicit QoS и bounded/reserved queues.
2. `IntentId`/`ActionId`, backend-owned accepted/rejected, venue/client-id/fill correlation.
3. Causal `ReconcileStart/Complete`, private stream epoch и high-water barriers.
4. Per-backend tests точки acceptance, idempotency, execution dedup, reconnect и death sweep.
5. Gap/corruption/control failure → `Unknown`/invalidate/reconcile; dual-run до отключения v1.

Deliverable: fault-injected transport и backend capability matrix. До этого capital canary запрещён.

### Phase 4 — execution FSM, risk и restart safety

1. Реализовать одну frozen two-leg policy, полную transition/compensation table и один parent/route.
2. Реализовать per-account/asset outcome reservations и отдельный recovery headroom.
3. Добавить authoritative freshness-bounded balances/equity/margin/borrow/inventory source, causal
   watermark и readiness gate. Static cap — только дополнительный strategy ceiling.
4. Unknown/late fill handling, process/stream-epoch startup gates и causal reconciliation.
5. State-specific shutdown FSM, signal bridge, supervisor integration и backend death capability.
6. Property tests, crash/restart, fault injection и long soak.

Deliverable: canary candidate, но ещё без права на scale.

### Phase 5 — tiny canary и только затем optimization

1. Один route, minimum legal notional, isolated account/limits.
2. Predeclared loss, unhedged-notional/time, request, stale/gap и unknown-action stops.
3. Сверить actual joint fills, fees, latency, leg slippage и recovery с shadow model.
4. Масштабировать symbols/venues только после отдельного economic и reliability verdict.
5. Zero-copy/POD, hybrid spin tuning, realtime scheduling или frame-level payload делать лишь по
   профилю доказанного bottleneck.

---

## 12. Отвергнутые альтернативы

| Альтернатива | Почему отклонена |
|---|---|
| `elapse(1 µs)` | Повторяет полный tick logic до 1M раз/с и остаётся `nanosleep` polling; ломает физический смысл tick windows. |
| Сразу добавить `EventDrivenBot: Bot` | Наследует optimistic submit semantics и позволяет смешать двух consumers (`next_event`/`elapse`); второй consumer ещё не доказан. Downstream `EventSource + PureReducer` проще. |
| Tokio task на venue + shared `Arc<RwLock<State>>` | Нечёткое ordering, contention и возможность torn cross-venue view; нарушает single-writer принцип. |
| Strategy внутри connector | Смешивает exchange plumbing и domain policy, ломает process isolation и не решает non-atomic legs. |
| Глобальная сортировка по `exch_ts` | Venue clocks/semantics несопоставимы; создаёт look-ahead и искусственный порядок. |
| Callback на каждый depth level | Торговля по половине venue frame, excessive churn и batch-dependent bugs. |
| Arbitrary debounce/coalescing | Добавляет скрытую latency и может удалить именно короткую opportunity. |
| Увеличить buffer вместо sequence | Делает overflow реже, но по-прежнему молчаливым и расширяет stale backlog. |
| `safe_overflow=true` + sequence как capital contract | Gap виден лишь на следующем dequeue, publisher не знает об overwrite, а loss до sequence assignment невидим. Нужна end-to-end loss generation и observable delivery failure. |
| Одна overflow policy для всех events | Market data может быть resyncable, fills/commands — нет. |
| Blind retry after timeout | Может удвоить реально принятую, но неподтверждённую заявку. |
| Считать legs атомарными | Ни iceoryx publish, ни два venue API не дают общей транзакции. |
| Синхронный logging/persistence на каждом event | Уничтожает tail latency и может заблокировать risk response. |
| Сразу zero-copy wire rewrite | До baseline неизвестно, доминируют ли bincode/String над parsing, scheduling или venue RTT. |
| Generic strategy graph/plugin bus | Не нужен для первого two-leg route и расширяет correctness surface. |

---

## 13. Открытые решения

Эти вопросы надо закрыть до соответствующей фазы, но они не блокируют Phase 0:

1. Первый venue pair, symbol/market kind, expected peak feed rate и одна execution policy.
2. Конкретный hardware fixture и может ли deployment выделить по core на engine и request loops.
3. Binding latency SLO после baseline: pure spin или relaxed hybrid profile.
4. Exact v2 loss-detecting lane primitive для одного engine/subscriber и переход к multi-bot позже.
5. Resync policy per backend: targeted resubscribe/snapshot или process restart.
6. Пойдёт ли authoritative balance/margin/borrow source через common wire или отдельный
   freshness-bounded reconciled snapshot contract; без одного из них canary запрещён.
7. Размер batch journal, negotiated maximum frame и `max_nonpreemptible_ns` commit-а.
8. Какая одна leg-order/execution policy фиксируется для первого arm.
9. Допустимая устойчивость complete research journal при crash; correctness всё равно должна опираться на
   venue reconciliation, а не на удачный fsync.
10. Когда frame-level payload или typed zero-copy окупает дополнительный protocol surface.
11. Per-backend causal reconciliation cut и точка `ConnectorAccepted`.
12. Max park interval hybrid profile и допустимый CPU budget pure-spin profile.

---

## 14. Итоговый go/no-go

Разработка имеет смысл, но первый продуктовый результат — **детерминированный low-latency shadow
reactor**, а не немедленный live arbitrage bot. Микросекундный local dispatch реалистичен на
выделенных cores; микросекундный end-to-end venue execution — нет.

Последовательность не следует менять местами:

```text
measure → coherent events → exploratory shadow → loss-detecting MD verdict
        → loss-detecting control/action IPC
        → explicit two-leg risk FSM → tiny canary → scale
```

Если Phase 0 покажет, что connector parsing/publisher hop или venue latency доминируют, это не делает
event-driven core бесполезным: immediate fill/hedge handling и deterministic safety всё равно нужны.
Но это может снять требование постоянного busy-spin и не оправдать более глубокую wire-оптимизацию.
