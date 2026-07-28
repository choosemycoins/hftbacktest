# AGENTS.md

> `CLAUDE.md` — симлинк на этот файл. Правь только `AGENTS.md`.

## Назначение

Этот репозиторий — **форк [`nkaz001/hftbacktest`](https://github.com/nkaz001/hftbacktest)** (`origin = git@github-coins:choosemycoins/hftbacktest.git`). Сам `hftbacktest` — библиотека для бэктестинга HFT/market-making стратегий на полных тиковых данных, плюс live-обвязка: `LiveBot` + отдельный процесс-коннектор к биржам через iceoryx2 shared memory.

Форк существует ради **downstream-потребителя** — live grid-бота `myhft` (`/Users/andrew/rust/myhft`), который подключает этот репозиторий как pinned git-зависимость. Изменения здесь делаются, чтобы закрыть недостающую upstream-функциональность, нужную `myhft`.

**Из этого следуют два разных режима работы, и их нельзя путать:**

| Режим | Что это значит |
|---|---|
| **Библиотечный код** (`hftbacktest/`, `connector/`, `py-hftbacktest/`) | Публичный API, который потенциально уедет в upstream PR. Изменения должны быть минимальны, обратно-совместимы и уместны для чужого проекта. Никакой `myhft`-специфики в сигнатурах и именах. |
| **Форк-специфика** (design notes в `docs/`, этот файл) | Здесь можно и нужно фиксировать контекст `myhft`. |

Цель — надёжная, понятная и проверяемая система. Предпочитаем:

- корректность вместо скорости разработки любой ценой;
- простые решения вместо «умных»;
- явные инварианты вместо неявной магии;
- тесты до или вместе с кодом;
- безопасную деградацию вместо рискованного поведения.

---

## 0. Authoritative sources

Этот файл описывает **философию, красные линии и известные ловушки**. Архитектура живёт рядом с кодом.

| Что | Где | Роль |
|---|---|---|
| Единый strategy-facing API | `hftbacktest/src/types.rs:795` (`trait Bot<MD>`) | Один трейт, две реализации: `Backtest` (`backtest/mod.rs:866`) и `LiveBot` (`live/bot.rs:416`). Одна и та же стратегия гоняется и в реплее, и в проде. |
| Wire-типы протокола коннектор↔бот | `hftbacktest/src/types.rs` — `LiveEvent` (:128), `LiveRequest` (:735), `ErrorKind` (:119), `ElapseResult` (:1006) | Общие для обоих процессов. Меняются только парно с коннектором. |
| Битовые флаги событий фида | `hftbacktest/src/types.rs:167–345`, `Event::is()` (:355) | Семантика `ev`. Читать **обязательно** перед любой работой с depth-фидом — см. §4.1. |
| Live-бот | `hftbacktest/src/live/bot.rs`, `live/mod.rs` (`Instrument<MD>`) | Single-writer владелец depth/orders/position. `process_event` (:205) — вся обработка входящих событий. |
| IPC | `hftbacktest/src/live/ipc/iceoryx.rs` | iceoryx2 pub/sub, payload — `bincode` `config::standard()`, роутинг по `symbol → inst_no`. |
| Движок бэктеста | `hftbacktest/src/backtest/` — `mod.rs` (`Backtest`, билдеры), `proc/` (`Processor`, local / no-partial-fill / partial-fill / L3), `models/` (`queue.rs`, `latency.rs`, `fee.rs`), `data/` | Событийная симуляция с очередью в стакане и латентностями. |
| Стаканы | `hftbacktest/src/depth/` — `hashmapmarketdepth.rs`, `btreemarketdepth.rs`, `roivectormarketdepth.rs`, `fuse.rs` | `fuse.rs` (`FusedHashMapMarketDepth`) — слияние нескольких depth-стримов разной глубины/частоты. Самый большой файл ядра. |
| Коннектор | `connector/src/main.rs` (процесс, регистрация, publish-таск), `connector/src/bybit/`, `binancefutures/`, `binancespot/` | Отдельный бинарь. Вся биржевая специфика — только здесь. |
| Snapshot-маркер (форк-фича) | `docs/snapshot-complete-marker.md` | Контракт `LiveEvent::SnapshotComplete` / `Bot::snapshot_ready`. **Внимание: раздел «Known gap» в нём содержит фактическую ошибку — см. §4.4.** |
| Примеры конфигов коннектора | `connector/examples/{bybit,binancefutures,binancespot}.toml` | Любое новое поле конфига обязано появиться здесь. |
| Коллектор: как пользоваться | `collector/README.md` | CLI, наборы стримов по биржам, формат выхода, конвертация в `.npz`, известные ограничения. |
| Коллектор: эксплуатация | `collector/deploy/` | Версионированные релизы, атомарный своп симлинка, откат, шаблонный systemd-юнит. Паттерн взят из `myhft/deploy/`; два осознанных отличия описаны в `README.md`. |
| Коннектор Hyperliquid (проект) | [`docs/design-hyperliquid-connector.md`](docs/design-hyperliquid-connector.md) | Черновик дизайна live-бэкенда HL: 11 решений с отклонёнными альтернативами, план тестов, фазовка, 8 открытых вопросов. Прошёл адверсариальное ревью; часть находок применена, статус — Draft, не Approved. |
| Многобиржевой сбор (проект) | [`docs/design-multi-venue-collection.md`](docs/design-multi-venue-collection.md) | Черновик: как писать несколько бирж так, чтобы данные годились для мультиассетного бэктеста. Содержит **непочиненные баги коллектора** (Фаза 0) — прочитать до любой работы над записью. Прошёл три проверяющих агента, переписан по их итогам. Draft, но **Фазы 1–4 и 5(б) реализованы** — статус по фазам в шапке самого документа; `book_mode='bbo+fast'` (фузия `bbo` + `l2Book fast`) живёт в `py-hftbacktest/hftbacktest/data/utils/hyperliquid.py`. |
| Rust-примеры стратегий | `hftbacktest/examples/` — `gridtrading_backtest.rs`, `gridtrading_backtest_args.rs`, `gridtrading_live.rs` | Точки входа для новичка. **Не все примеры компилируются — см. §5.** |
| Python-слой | `py-hftbacktest/src/` (PyO3), `py-hftbacktest/hftbacktest/` (пакет, конвертеры данных, stats) | |
| Планы upstream | `ROADMAP.md` | |

**Правило чтения:** перед началом любой нетривиальной задачи — открыть **соответствующий модуль напрямую**. Не полагаться на дайджесты, обсуждения и на этот файл. Если этот файл расходится с кодом — побеждает код, а файл надо поправить.

---

## 1. Главные принципы

### 1.1 Safety first, fail closed

Через этот код проходят реальные деньги.

- Лучше **пропустить opportunity**, чем торговать в uncertain state.
- Лучше **остановить вход**, чем продолжить execution при неясной картине.
- Лучше **process exit + supervisor restart**, чем жить с неразобранным state mismatch или stale market data.
- Система должна **fail closed**. Default-значения конфигов и лимитов должны **запрещать**, а не разрешать.
- `ConnectionInterrupted`, stale depth, rate-limit storms — сигналы, на которые нельзя отвечать `Ok(())` и продолжать цикл.

Важно понимать разделение ответственности в текущем дизайне: **`hftbacktest` политику не задаёт**. `LiveBot` отдаёт ошибку в пользовательский `error_handler` (`live/bot.rs`, поле `error_handler`), а решение «фатально/восстановимо» принимает потребитель (`myhft`). Не тащи policy (таймауты, circuit breaker, throttling) в библиотеку — там ей не место, и это ломает upstream-совместимость.

### 1.2 Простота по умолчанию (YAGNI)

Любое усложнение отвечает на три вопроса:

1. Какую конкретную проблему оно решает **сейчас**?
2. Почему более простой вариант недостаточен?
3. Будет ли код понятнее через 3 месяца, чем прямой код без этой абстракции?

Нет ответов — решение преждевременное.

Не добавлять «на вырост»: universal event bus, plugin systems, обобщённые policy engines, DSL-конфиги, лишние crates. **Duplication допустима**, если она дешевле абстракции.

Хороший образец применённого YAGNI в этом форке — выбор `snapshot_ready(asset_no) -> bool` вместо нового варианта `ElapseResult`, и отказ от полей `num_orders`/`position` в payload маркера (обоснование — `docs/snapshot-complete-marker.md`).

### 1.3 TDD обязателен

**Red → Green → Refactor** — реальный порядок работы, не формальность.

1. Сформулировать поведение: вход → выход.
2. Написать тест; убедиться, что он падает **по правильной причине**.
3. Минимальный код до зелёного.
4. Рефакторинг без изменения поведения.
5. Прогнать тесты снова.

Запрещено писать сложную реализацию, а потом «дописывать тесты по факту» для core-логики. Тесты падают — разбираемся, не удаляем и не подгоняем под поведение.

Изменения **маленькие**: один behavior change → один набор тестов → один логический коммит.

**Что обязательно через TDD:** всё, что касается денег, позиции, состояния ордеров, reconnect-а, shutdown-а, и всё, что трогает wire-протокол. Конкретно для этого репозитория: обработка событий в `LiveBot::process_event`, роутинг в `iceoryx.rs`, слияние стаканов в `fuse.rs`, queue-position модели, boundary-условия depth (`INVALID_MIN`/`INVALID_MAX`, нулевой объём), парсинг биржевых сообщений, order-id маппинг в `ordermanager.rs`.

Образец для подражания — тест-модуль в `hftbacktest/src/live/bot.rs` (`mod tests`, ~:669): `MockChannel` реализует трейт `Channel` поверх `VecDeque`, отдаёт `BotError::Timeout` при опустошении — этого достаточно, чтобы детерминированно прогонять `elapse()` без iceoryx. Новые тесты live-слоя пиши через него.

### 1.4 Ownership, а не shared state

- `LiveBot` — единственный владелец состояния бота: `Vec<Instrument<MD>>`, внутри — depth, `orders: HashMap`, `state: StateValues`. В процессе бота **нет ни одного `Arc`/`Mutex`** — не заводи.
- `asset_no` — это просто индекс в `instruments`, назначаемый порядком регистрации. Не изобретай альтернативную адресацию.
- В коннекторе shared state есть и он неизбежен (`Arc<Mutex<OrderManager>>`, `SharedSymbolSet`), но границы уже проведены — не расширяй их.
- Передача immutable snapshot-ов предпочтительнее блокировок в hot path.

Любое отклонение — с явным обоснованием в PR.

### 1.5 Явность важнее cleverness

- Явные enum/state transitions; критические переходы не кодируются набором разрозненных bool-флагов.
- Явные error classes: recoverable и fatal различаются явно.
- Newtypes / типизированные константы для доменных величин там, где это снимает класс ошибок.
- Малые функции с понятными контрактами.

Нежелательно: generic-магия без пользы, macro-heavy решения, скрытая логика, длинные методы с несколькими уровнями ответственности.

---

## 2. Что делать нельзя (red lines)

- Писать core-логику (обработка событий, order state, fill-симуляция, error handling) без тестов.
- **Ломать wire-протокол однобоко.** `LiveEvent` / `LiveRequest` кодируются `bincode` без версионирования: новые варианты добавляются **только в конец enum-а**, иначе декодирование существующих вариантов поедет. Коннектор и бот обновляются **парой**. Жёсткий потолок сообщения — `MAX_PAYLOAD_SIZE = 512` байт (`live/ipc/config.rs:5`, `loan_slice_uninit` в `iceoryx.rs:170`); поле переменной длины (например, длинный `symbol`) может упереться в него.
- **Добавлять обязательные методы в `trait Bot`, не реализовав их в обеих реализациях** (`Backtest` и `LiveBot`) — это компиляционный слом для любого внешнего импла.
- Смешивать domain logic и exchange-specific plumbing: биржевая специфика живёт только в `connector/src/<exchange>/`, ядро о ней не знает.
- Тащить `myhft`-специфику в публичный API `hftbacktest`.
- Прятать критические ошибки за `Ok(())`.
- Использовать глобальный mutable state как shortcut.
- Писать «temporary hack» без явного `// TODO:` со ссылкой на задачу.
- Оптимизировать до появления измерений.
- **Расширять `unsafe`.** В live-слое `process_event` использует `get_unchecked_mut(inst_no)`; корректность держится на том, что `inst_no` пришёл из `symbol_to_inst_no`. Если добавляешь новый источник `inst_no` — либо докажи инвариант, либо используй безопасный доступ.
- Менять поведение reconnect-логики и error-путей, не прочитав §3 и §4 целиком.

---

## 3. Operational mindset

Думай не только о happy path:

- reconnect / `ConnectionInterrupted` (и **что теряется** при реконнекте — см. §4.2);
- stale depth: bid/ask/mid не обновляются, а позиция едет;
- duplicate events и out-of-order updates (`process_event` защищается сравнением `exch_timestamp` и финальными статусами — не сломай это);
- partial fills;
- Bybit rate-limit (`10006` «Too many visits»); в `private_stream.rs` есть незакрытые `// todo: rate-limit throttling.`;
- graceful shutdown и supervisor restart;
- degraded venue mode: односторонний стакан, нулевой объём;
- cold start коннектора: биржевое состояние ещё не подтянуто, а бот уже зарегистрировался.

Если код не отвечает на вопрос «что происходит, когда что-то идёт не так?» — он не готов.

---

## 4. Ловушки этой кодовой базы

Проверено по коду. Каждая из них уже способна тихо сломать live-торговлю.

### 4.1 `LiveBot` игнорирует snapshot- и BBO-события стакана

`Event::is()` (`types.rs:355`) требует **точного** совпадения младшего байта: `self.ev & 0xff == event & 0xff`. Виды событий: `DEPTH_EVENT = 1`, `TRADE_EVENT = 2`, `DEPTH_CLEAR_EVENT = 3`, `DEPTH_SNAPSHOT_EVENT = 4`, `DEPTH_BBO_EVENT = 5`.

`LiveBot::process_event` (`live/bot.rs:213–225`) обрабатывает **только** `LOCAL_BID/ASK_DEPTH_EVENT` (вид 1) и trade-события (вид 2). Следствия:

- **`orderbook.1` от Bybit выбрасывается.** `public_stream.rs:71,89` шлёт его как `LOCAL_*_DEPTH_BBO_EVENT` (вид 5) → бот его не применяет. Полезен он только внутреннему `FusedHashMapMarketDepth` коннектора.
- **Depth-снапшот при регистрации выбрасывается.** `FusedHashMapMarketDepth::snapshot()` (`depth/fuse.rs:594`) генерирует события с `DEPTH_SNAPSHOT_EVENT` (`:605`, `:625`) → `LiveBot` их не применяет. Стакан бота строится **только** из инкрементальных обновлений вида 1, то есть из `orderbook.50` / `.200` / `.500`.
- Отсюда прямое ограничение на конфиг: **`orderbook_depths` обязан содержать хотя бы одну глубину > 1**, иначе стакан `LiveBot` останется пустым навсегда. Если добавляешь валидацию конфига — вот её смысл.
- Смежное, замерено на mainnet 2026-07-25: **Bybit отвергает `orderbook.500`** для BTCUSDT, ETHUSDT и SOLUSDT (`error:handler not found`), хотя документация его обещает; `1`, `50` и `200` принимаются. Один отклонённый топик валит **всю** пачку подписки, и коннектор остаётся подключённым, но не подписанным ни на что. Дефолт `[1, 50]` выбран поэтому. Ровно на это же напоролся коллектор и писал ноль байт.
- Отсюда же: `snapshot_ready == true` **не означает**, что у бота есть bid/ask. Маркер говорит только про `position()` и `orders()`.

### 4.1a Для Hyperliquid §4.1 — не сноска, а центральное ограничение

У Hyperliquid **вообще нет инкрементального канала стакана**. Каждое сообщение
`l2Book` — полный снапшот топ-N без sequence number. Подтверждено измерением:
у пяти подряд идущих сообщений множество цен по биду совпало полностью, чего
диффовый фид никогда бы не переслал.

Значит коннектор обязан **синтезировать дельты сам** и эмитить только вид 1.
Наивный путь «это снапшот, шлём `DEPTH_SNAPSHOT_EVENT`» даёт бота с вечно
пустым стаканом и без единой ошибки в логах. Отсюда же ловушка усечения:
уровень, выпавший из окна топ-N, неотличим от снятого. Разбор — в
`docs/design-hyperliquid-connector.md` §5.2.

Замерено на mainnet 2026-07-25: `bbo` — медиана 0.14 с, `l2Book fast` — 5 уровней
раз в 0.54 с, `l2Book` обычный — 20 уровней раз в 5.4 с. Публичный фид HL
принципиально беднее байбитовского (22 МБ/сутки против 86–159 на символ), и это
свойство биржи, а не недосбор.

### 4.2 Публичные стримы не переподписываются после реконнекта

`Connector::register` (`bybit/mod.rs:304`) шлёт символ в `tokio::sync::broadcast` **ровно один раз** — повторная регистрация того же символа отсекается `HashSet`. При этом `PublicStream::new(..., symbol_tx.subscribe(), ...)` вызывается **внутри** retry-замыкания (`bybit/mod.rs`, `.retry(|| async { ... })`), то есть каждый реконнект создаёт свежий `broadcast::Receiver`, который видит только сообщения, отправленные **после** подписки. Сам стрим `SharedSymbolSet` не читает.

Итог: после реконнекта публичного стрима коннектор подключён, но **не подписан ни на что** — маркет-дата молча прекращается, единственный след в логах — исходный `ConnectionInterrupted`. Аналогично в `binancefutures/market_data_stream.rs` и `binancespot/market_data_stream.rs`.

Приватный стрим Bybit этой проблемы не имеет: он перечитывает `self.symbols` на subscribe-ack (`private_stream.rs:100–151`).

Это upstream-баг, не форк-регрессия. Если чинишь — чини явно и с тестом.

### 4.3 `LiveBot::modify` — это `todo!()`

`live/bot.rs:558–567`: вызов `modify` в live-режиме **паникует**. В live доступны только cancel + submit. В `Backtest` `modify` реализован — то есть стратегия, зелёная в бэктесте, упадёт в проде. Учитывай при написании общего кода под `trait Bot`.

### 4.4 Реальная семантика `SnapshotComplete` — «стакан почищен», а не «состояние подтянуто»

`docs/snapshot-complete-marker.md` в разделе «Known gap» утверждает, что REST-префетч закомментирован и `cancel_all`/`get_all_position` не реализованы. **Это неверно.** Закомментированный блок в `bybit/mod.rs:225–232` — устаревшая альтернативная развязка (методы на `PrivateStream`, которых действительно нет). Рабочий путь живёт в `private_stream.rs` и **активен**:

- на subscribe-ack — по всем уже известным символам (`private_stream.rs:100–151`);
- на каждую новую регистрацию символа (`private_stream.rs:275+`);
- обе ветки делают `cancel_all(...)` → REST `cancel_all_orders` (`rest.rs:81`) + `get_position(...)`.

То есть политика коннектора — **«снести все ордера на бирже и начать с чистого состояния»**, а не «сохранить и отдать существующие». Дубликаты после рестарта `myhft` предотвращаются именно этим.

Настоящая проблема другая — **гонка**. Оба вызова уходят в `tokio::spawn`, а `SnapshotComplete` публикуется publish-тредом сразу после `BatchEnd` (`connector/src/main.rs:186`), синхронно с обработкой `RegisterInstrument`. Регистрация в `run_receive_task` при этом идёт в порядке «сначала отправить `PublishEvent::RegisterInstrument`, потом вызвать `connector.register(symbol)`». Значит `snapshot_ready` может стать `true` **до** того, как REST-раунд-трип завершился.

Если трогаешь эту область: сначала поправь design note, потом код. И не удаляй документацию гонки — она load-bearing.

### 4.5 `Backtest::submit_order` игнорирует сторону ордера

`backtest/mod.rs:988`: перегрузка `submit_order(asset_no, order: OrderRequest, wait)` передаёт в `local.submit_order` жёстко `Side::Sell` (строка 997) и не смотрит на `order.side`. Live-аналог (`live/bot.rs:538`) передаёт `order.side` (строка 553). То есть бэктест-стратегия на `OrderRequest` молча шлёт только продажи, а в live та же стратегия работает как задумано.

**Пока не исправлено — пользуйся `submit_buy_order` / `submit_sell_order`.** Фикс существует в сборке, которую тянет `myhft` (см. §5, расхождение ревизий), но в этом дереве его нет.

### 4.6 Частичные исполнения не доезжают до позиции

`PartialFillExchange::fill` выставляет `Status::PartiallyFilled` (`proc/partialfillexchange.rs:233`), но `Local::process_recv_order_` вызывает `state.apply_fill` только при `status == Status::Filled` (`proc/local.rs:102`, то же в `l3_local.rs:282`). С `ExchangeKind::PartialFillExchange` позиция и PnL стратегии не учитывают частичные филлы. Требует проверки перед тем, как доверять результатам бэктеста на этой модели.

Смежное: `NoPartialFillExchange` исполняет **весь** `leaves_qty` независимо от ликвидности на уровне (признано в doc-комментарии `nopartialfillexchange.rs:58–62`) — FOK/IOC ведут себя как GTC.

### 4.7 Прочее

- **`fuse.rs`**: события удаления уровней из `update_best_bid`/`update_best_ask` собираются без бита `LOCAL_EVENT`/`EXCH_EVENT` (`fuse.rs:167,283,412,439,520,547`). В офлайн-пайплайне биты доставляет `py-hftbacktest/hftbacktest/data/validation.py`; в live их не доставляет никто — см. §4.1.
- **Поле `timestamp` у всех четырёх стаканов инициализируется нулём и никогда не пишется.** Не читай `depth.timestamp` в расчёте на время последнего обновления.
- **`snapshot()` реализован не у всех стаканов**: `todo!()` в `BTreeMarketDepth`, `unimplemented!()` в `ROIVectorMarketDepth`. Коннектору годятся только `HashMap` и `Fused`.
- **Паника = смерть процесса.** `connector/src/main.rs` ставит panic hook с `exit(1)`; по всему коннектору `pub_tx.send(...).unwrap()`. Это де-факто контракт с супервизором, а не небрежность.
- **`panic = "abort"` в release-профиле** (корневой `Cargo.toml`). `catch_unwind` в проде не работает.
- **Python-биндинги глушат live-ошибки**: `build_*_livebot` в `py-hftbacktest/src/live.rs` хардкодят `.error_handler(|_| Ok(()))` и `.order_recv_hook(|_,_| Ok(()))`. Это прямо противоречит §1.1 и §2. Если трогаешь Python live-путь — не тиражируй.
- **Нет `[workspace.dependencies]`.** Версии дублируются по манифестам (`tokio`, `tracing`, `iceoryx2`…). Бампаешь общую зависимость — правь все манифесты.
- **Коллектор ничего не мержит и ничего не выбрасывает — это контракт, а не недоделка.** Фиды разной глубины и частоты пишутся рядом как есть, кадры без символа уходят в `_meta`. Сведение фидов — политика без единственно верного ответа, и зашитая на этапе захвата она сделала бы запись неспособной ответить ни на какой другой вопрос. Смысл в том, чтобы прогонять несколько политик поверх одних и тех же байт и сравнивать. Не добавляй мерж в коллектор.
- **`_meta` не сжимается намеренно.** `GzEncoder::flush()` даёт sync-point дефлейта, но не трейлер члена, поэтому gzip-мета-поток читался бы только после остановки процесса — измерено: 12 минут по 10 байт на диске. Диагностировать живую проблему по нему было бы нельзя.

---

## 5. Рабочий процесс

### Команды

```bash
cargo check --workspace --lib --bins
cargo test  --workspace --lib --bins      # НЕ --all-targets, см. ниже
cargo clippy --workspace --lib --bins
cargo +nightly fmt                        # именно nightly, см. ниже
```

**Почему не `--all-targets`.** Четыре примера в `hftbacktest/examples/` протухли относительно текущего API и **не компилируются**:

| Файл | Ошибка |
|---|---|
| `algo.rs` | `E0601: main function not found` — это модуль-хелпер, собираемый как example |
| `logging_order_latency.rs` | `E0599: no function named builder for LiveBot` |
| `gridtrading_live_bybit.rs` | `E0599: no method named run for LiveBot` |
| `custom_evhandling.rs` | `E0599: no method named process_recv_order2 for Local` (сигнатура сменилась на `process_recv_order`) |

Поскольку `cargo test --workspace` собирает и примеры, он падает на них и **не запускает ни одного теста**. Это ловушка: «тесты не собрались» легко принять за «тесты упали». Всегда сужай до `--lib --bins`, пока примеры не починены.

Это upstream-протухание, а не форк-регрессия. Починить их — хорошая отдельная задача.

- **Форматирование требует nightly.** Все опции в `rustfmt.toml` (`imports_layout`, `imports_granularity`, `group_imports`) — nightly-only. Stable rustfmt их молча игнорирует и выдаёт ~53 ложных диффа на весь репозиторий; `cargo fmt --check` на stable — **бесполезный сигнал**. На nightly реальных расхождений сейчас 5.
- **`-D warnings` в этом репозитории недостижим.** Базовый уровень — ~89 предупреждений clippy/rustc (`result_large_err` ×22, `large_enum_variant` ×5, `dead_code`, `unused_variables` и т. д.). Требование к задаче — **не добавлять новых** предупреждений в тронутых файлах, а не обнулить счётчик.
- `edition = "2024"`, MSRV `1.90` (объявлен только в `hftbacktest/Cargo.toml`). `rust-toolchain.toml` в репозитории **нет**.
- Профиль для профилирования — `release-with-debug`.
- **Бенчмарков в репозитории нет.** `cargo bench` ничего не измеряет — не ссылайся на него в DoD.

### CI: его практически нет

`.github/workflows/` содержит три workflow, и **ни один не собирает и не тестирует Rust**: `codeql.yml` (только `language: python`), `release-python.yml` (ручной сбор колёс), `stale.yml`. Никакого `cargo test`, `cargo clippy`, `cargo fmt --check` на PR.

**Следствие: локальный прогон — единственная защита.** Не «CI поймает».

### Покрытие тестами (по состоянию на сейчас)

| Крейт | Тесты |
|---|---|
| `hftbacktest` | 29 `#[test]` в 8 файлах: `types.rs`, `depth/{hashmap,btree,roivector,fuse}marketdepth.rs`, `backtest/models/queue.rs`, `backtest/mod.rs`, `live/bot.rs` |
| `connector` | 5 `#[test]`, все в `src/utils.rs`. **Ни одного теста на парсинг биржевых сообщений, order manager, стримы.** |
| `collector` | 107 `#[test]`: `file.rs`, `disk.rs`, `hyperliquid/mod.rs`, плюс модули Фазы 1 дизайн-дока многобиржевого сбора — `queue.rs` (ограниченные хенд-оффы), `pump.rs` (владение отправителем, один цикл на все пять бэкендов), `watchdog.rs` (сторож молчания), `lock.rs` (flock директории), `backoff.rs`, `meta.rs` (единый словарь lifecycle-записей `_meta`), `clock.rs` (adjtimex-гейдж дисциплины часов), `liveness.rs` (per-symbol возраст записи), premiumIndex-поллер и по два теста в каждом бэкенде. Крейт **bin-only**: `cargo test -p collector --bins` (`--lib --bins` для него ошибка «no library targets») |
| `collector/tools` (Python) | 318 pytest: `test_quality_report.py` (117), `test_build_dataset.py` (85), `test_backtest_first.py` (116). Запуск: `.venv/bin/pytest collector/tools/` — тулинг Фаз 2–4 `docs/design-multi-venue-collection.md` |
| `py-hftbacktest` (Rust), `hftbacktest-derive` | 0 |
| Python | `py-hftbacktest/tests/test_hyperliquid_converter.py` (9, дедуп трейдов), `test_hyperliquid_bbo_fast.py` (30, фузия `bbo+fast`). `test_hftbacktest.py` **падает всегда** — требует отсутствующего в репозитории `tmp_20240501.npz`. |

Самая большая дыра — `connector/`. Любая новая логика там должна приходить с тестом; это дешевле, чем кажется, — сообщения биржи парсятся из строк, и тест на парсинг не требует сети.

### Git

- Работа ведётся в ветках от `master`; `master` отслеживает upstream.
- Ветки в полёте: `feat/snapshot-marker` (база — snapshot-маркер плюс деплой-тулинг коллектора), `feat/hyperliquid-connector` (ответвлена от неё), `feat/order-id-link`, `bybit-run`, `fix/fix-bybit-live-depth`, `fix/fix-bybit-live--orderbook-depth`.
- `task-brief-connector-snapshot-marker.md` в корне — **не в git**. Это документ из `myhft`, оказавшийся здесь; на него нельзя ссылаться как на общий контекст.

#### ⚠️ Ревизия, которую собирает `myhft`, отсутствует в этом клоне

`myhft/Cargo.toml` пинит `rev = "ed96480e05f77ed8f756bf553e5f1e10559cd978"` (комментарий: `# branch feat/snapshot-marker`), а `git cat-file -t ed96480` здесь → `Not a valid object name`. Ни локальная, ни `origin/feat/snapshot-marker` его не содержат — обе на `5abd7f8`. Версия в `myhft/Cargo.lock` — `0.9.4`, в этом дереве `hftbacktest/Cargo.toml` — `0.9.3`.

Практический смысл: **деплой и это дерево разошлись** (force-push или незапушенная работа). Что именно крутится в проде, видно только в `~/.cargo/git/checkouts/hftbacktest-*/ed96480/`.

Прежде чем что-то менять здесь в расчёте на `myhft` — сначала сверь эти две вещи и приведи их в согласие. Иначе правка либо не доедет, либо затрёт то, что уже работает.

---

## 6. Definition of Done

Задача завершена, если:

- поведение формализовано (PR-описание / design note в `docs/` / doc-комментарий к публичному API);
- `cargo test --workspace --lib --bins` зелёный, включая новые тесты (сейчас база — 92: 29 в `hftbacktest`, 5 в `connector`, 58 в `collector`); для Python-тулинга коллектора — `.venv/bin/pytest collector/tools/` (база — 228);
- `cargo clippy --workspace --lib --bins` **не добавил новых** предупреждений в тронутых файлах (обнулить счётчик нельзя — см. §5);
- `cargo +nightly fmt` применён;
- если менялся wire-протокол или `trait Bot` — обновлены **обе** стороны (`connector/` и `hftbacktest/`) и обе реализации трейта;
- если добавлено поле конфига — оно есть в `connector/examples/*.toml` с комментарием;
- код минимален, сложность оправдана;
- observability добавлена (`tracing`, в стиле существующих call-site-ов);
- fatal-пути ведут к process exit, а не к тихому продолжению;
- документация/design note обновлены, если изменился контракт;
- reviewer поймёт решение без устных пояснений.

---

## 7. При конфликте принципов

1. **Безопасность денег и позиции**
2. **Корректность и ясность состояния**
3. **Простота решения**
4. **Тестируемость**
5. **Upstream-совместимость**
6. **Производительность**
7. **Эстетика архитектуры**

---

## 8. Короткая памятка

Перед тем как писать код:

- Читал ли я нужный модуль **напрямую**, а не пересказ?
- Прочитал ли §4 «Ловушки», если трогаю depth-фид, реконнект или snapshot?
- Как это поведение будет протестировано **сначала**?
- Можно ли сделать проще?
- Кто здесь owner состояния?
- Меняю ли я wire-протокол или `trait Bot`? Обновил ли обе стороны?
- Что будет при `ConnectionInterrupted` / stale depth / rate-limit / duplicate / restart?
- Как оператор и лог увидят проблему?
- Это уместно в upstream, или это `myhft`-специфика, которой здесь не место?

Если ответы неясны — сначала уточни дизайн, потом пиши код.
