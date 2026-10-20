# Alpha и structural edge после отрицательного passive-grid результата

- Статус: **Research decision memo; live-arm и connector work этим документом не разрешены**
- Дата: 2026-08-10
- Scope: downstream `myhft`, Hyperliquid passive market making, Binance→HL signal `#20`, Paradex RPI и Lighter Standard/Premium
- Связано: [`proposal-20260810.md`](proposal-20260810.md), [`criteria-20260810.md`](criteria-20260810.md)
- Метод: синтез August sweep, HYPE live fill-quality, committed signal studies, текущих venue rules и доступных passive captures; новых backtest/live-прогонов не выполнялось

---

## 1. Решение

Отрицательный результат пассивной сетки не означает, что нужно «предсказывать цену вообще». Он
означает, что **безусловная симметричная котировка не имеет положительной экономики на проверенной
HYPE-геометрии**. Выход следует искать в двух независимых направлениях:

1. **Informational alpha:** заранее распознавать токсичный fill и снимать только опасную сторону.
2. **Structural edge:** изменить тип входящего потока, fee/latency trade-off или matching rules.

Третий элемент — **carrier**, то есть symbol/venue с достаточной ёмкостью для уже доказанного edge.
Широкий displayed spread, маленькая очередь или высокая частота сами по себе edge не образуют.

Рекомендуемый портфель ограничен:

- один активный engineering/research slot — `#20`;
- один дешёвый data-only venue scout — Lighter Standard/Premium trade-off на уже записываемом
  ETH-окне;
- Paradex RPI — наиболее сильный чистый structural fallback, пока только passive data gate;
- descriptive HL carrier rerank — background work уже сейчас; frozen-policy scoring — только после
  Stage-0 `#20`, не как поиск ещё одной безусловной сетки.

Если `#20` не закрывает экономическую дыру, нельзя отвечать feature zoo или новым grid sweep. Нужно
менять flow или venue.

---

## 2. Какой edge нужен market maker'у

Правильная единица анализа — не forecast будущей цены и не PnL выбранной backtest-ячейки, а вклад
quote opportunity:

```text
EV(action | x) = P(fill | action, x)
               * E[fill edge | fill, action, x]
               - E[action/request + inventory/funding costs | action, x]
```

Здесь `x` — только информация, доступная до действия. Такая запись не позволяет объявить alpha по
красивому markout observed fills, забыв про потерянные хорошие fills, fill probability, cancel race
и capacity. Все слагаемые в этой формуле должны быть приведены к одной единице — например, USD на
quote opportunity. `bp / filled dollar` используется ниже только для decomposition уже случившихся
fills и не вычитается напрямую из per-opportunity costs.

### 2.1 Измеренная планка

HYPE live session 2026-08-07..09 дала 140 fills / 30 hour clusters, из них 139 maker. На горизонте
5 s:

| Компонент | bp на filled notional |
|---|---:|
| Spread captured | +0.814 |
| Maker fee | -1.520 |
| Adverse move | -2.448 |
| **Realized edge** | **-3.154** |

95% hour-clustered CI realized edge: **[-4.293, -1.518] bp**. Источник — operator artifact
[`TABLES.txt`](../../../prod-artifacts-backup/hl-mainnet-2026-08/fill-quality/TABLES.txt).

Это **5 s mid-relative execution edge**, а не cash PnL и не полный strategy return: в нём нет
request, funding и path-dependent inventory costs. Любой additive rescue именно этой HYPE-геометрии
должен улучшить execution edge более чем на 3.154 bp ещё до дополнительных costs. Structural
альтернатива на другой venue обязана доказать собственный положительный absolute edge и не наследует
HYPE-число как механический hurdle. Сигнал на 0.3–0.5 bp может быть полезной добавкой, но не
самостоятельным спасением этой геометрии.

Механизм отрицательного результата тоже измерен: quote стоял примерно в 27 тиках от touch, рынок за
1 s перед fill проходил к нему в среднем 25.54 тика, а в момент fill медианная дистанция от mid до
нашей цены составляла 0.5 тика. Backtest-метка `depth fill` оказалась экономически run-over, а не
захватом пятибипсового half-spread.

### 2.2 Что не является источником edge

- latency и queue engineering помогают монетизировать уже положительный fill edge;
- dynamic spread и inventory skew управляют экспозицией;
- fee tier уменьшает cost;
- увеличение размера улучшает некоторые ratio-метрики.

Ни одно из этих действий не превращает отрицательный conditional fill edge в alpha автоматически.

---

## 3. Приоритет 1: Binance→HL adverse-side signal `#20`

### 3.1 Почему это лучший текущий alpha prior

Committed offline study использует co-recorded Binance HYPE bookTicker, HL BBO и HL touch-trade
proxies на общем local clock. Результат:

- около 141k HYPE observations;
- adverse-high-z decile около -4.5 bp;
- favourable-low-z decile около +1.0 bp;
- extreme-cohort separation около 5.5 bp;
- монотонность по нескольким EWMA windows и horizons;
- знак подтверждён на INJ;
- 100 ms bot cadence сохраняет 84–106% separation.

Источник — [`myhft/docs/research/README.md`](../../myhft/docs/research/README.md) и committed
[`basis_markout_study.py`](../../myhft/docs/research/basis_markout_study.py).

Это более сильный prior, чем generic imbalance: есть внешний leading venue, фиксированный знак,
несколько horizons, независимый symbol check и проверка реальной частоты расчёта.

### 3.2 Почему 5.5 bp separation ещё не означает прибыль

Пусть:

- `p` — доля fill notional, которую frozen policy блокирует;
- `mu_r` — edge retained cohort;
- `mu_b` — edge blocked cohort;
- `d = mu_r - mu_b`;
- `mu_all,W` — unconditional edge на **том же eligible окне `W`**, из которого получены cohorts.

Тогда:

```text
mu_all,W = (1 - p) * mu_r + p * mu_b
mu_r     = mu_all,W + p * d
```

Identity требует одинаковых окна, eligibility rules и notional weights. Нельзя подставить `p` и `d`
из 36-fill overlap, а `mu_all = -3.154 bp` взять из полной 140-fill session. Stage 0 обязан посчитать
`mu_all,overlap`; полная session используется только для проверки representativeness.

Для оценки масштаба можно сделать заведомо щедрый, но **не estimable** stress check: смешать полный
HYPE deficit 3.154 bp с extreme-low-vs-high proxy gap 5.5 bp и представить, что этот полный gap
получает каждый заблокированный доллар. Тогда для выхода в ноль получилось бы:

```text
p > 3.154 / 5.5 = 57.3%
```

Это не оценка требуемой blocked share: 5.5 bp сравнивает два extreme deciles, а не retained 90% с
blocked 10%, и сами данные относятся к другому proxy population. На proxy-данных, при равных
observation weights и блокировке 10% observations, более корректная децильная арифметика использует
unconditional около -0.7 bp и blocked decile около -4.5 bp:

```text
mu_r,proxy = (-0.7 - 0.10 * -4.5) / 0.90 = -0.278 bp
uplift      = -0.278 - -0.7 = 0.422 bp
```

Этот расчёт тоже только proxy sanity check, но он показывает главный эффект: сильный extreme-cohort
contrast может давать небольшой средний uplift. Binding вопрос — **абсолютная экономика одной frozen
policy на одном eligible окне**, а не наличие rank correlation.

### 3.3 Следующий дешёвый тест

Сначала выполнить existing-data fill join и order-life replay:

1. sign взять из committed study; он зафиксирован однозначно;
2. `half_life`, on/off thresholds и hysteresis выбрать **до outcomes** из одного внешнего источника;
   допустимый вариант — заморозить deployed `60 s / 4.0 / 1.5` из
   [`bot-hype.toml`](../../../myhft-liqguard/deploy/hl-grid/bot-hype.toml), явно назвав его
   post-study recalibration, а не параметром committed study;
3. не использовать текущий `raw_permit`: он загрязнён flip-budget и sticky-`Neither` дефектами;
4. для fill-only diagnostic присоединять последний warm/fresh signal tick до fill и публиковать
   join age;
5. для counterfactual policy восстановить **все** order-life intervals: placement → первое threshold
   crossing → bot tick → cancel request → conservative venue arrival/ACK → fill/cancel;
6. посчитать на одном overlap-окне notional-weighted `mu_all,overlap`, `p`, `d`, `mu_r`, retained
   fills/notional per day и action ledger; отдельно сравнить overlap control с полной 140-fill session;
7. late cancels оставить среди policy losses;
8. uncertainty считать по hour clusters.

Один nearest-pre-fill `z` не воспроизводит cancellation policy и создаёт selection bias: denominator
должен включать все периоды жизни заявок, а не только fills.

Доступное пересечение — 36 fills / 15 hour clusters. Оно может дать только directional triage:
**FAST KILL**, **PARK / INCONCLUSIVE** либо обоснование стоимости clean shadow. Arm оно не разрешает.

### 3.4 Решение после join

- **FAST KILL:** `UCB95` исключает положительный retained edge или economically useful separation;
  либо эффект имеет устойчиво неправильный знак на primary и sensitivity horizons. Простое отсутствие
  монотонности на 36 fills не является kill-доказательством.
- **PARK / INCONCLUSIVE:** знак правдоподобен, но CI, cohort size или data quality не позволяют купить
  clean shadow автоматически.
- **CONTINUE TO SHADOW:** один frozen rule имеет правильный знак, экономически достаточный point
  estimate и приемлемую projected capacity, но uncertainty ещё не позволяет arm.
- **Запрещено:** подобрать threshold по 36 fills, объявить PASS по incremental gap или сразу arm.

Только после `CONTINUE` исправляются signal FSM/latch, запускается новый untouched clean shadow и
применяются formal criteria из [`criteria-20260810.md`](criteria-20260810.md). Armed canary допустим
лишь после положительного **абсолютного** edge и actionability на достаточной выборке.

---

## 4. Structural edge

### 4.1 Paradex RPI — наиболее сильный чистый механизм

RPI меняет состав контрагентов непосредственно в matching engine:

- RPI orders могут исполняться только против Retail Taker;
- сами RPI размещаются через API, но не исполняются против regular API orders;
- Retail Taker имеет 300 ms speed bump и существенно меньшие rate limits;
- RPI обязаны быть post-only;
- RPI имеют последний приоритет на своей цене.

Это причинная сегрегация потока, а не корреляционный proxy. Официальный контракт —
[`Retail Price Improvement`](https://docs.paradex.trade/trading/rpi).

Локальная предварительная запись около 13 часов показывает на 30 s:

- regular `FILL`: около +1.05 bp adverse maker'у;
- RPI: около -0.46 bp;
- RPI-flow примерно на 1.5 bp менее токсичен.

Источник — [`myhft/docs/research/README.md`](../../myhft/docs/research/README.md). Это пока markout
чужих fills, не наша fill probability и не абсолютная прибыль. Кроме того, локальный анализ пока
содержит допущение `side = taker`: относительная разница `RPI - FILL` устойчива к перевороту знака,
но absolute maker edge — нет.

#### Data-only gate

На fixed недельном окне, отдельно для BTC и ETH:

**Blocking precondition:** сначала подтвердить semantics поля `side` по venue schema и нескольким
golden trades либо восстановить aggressor side независимо. Unresolved side означает `NO PASS`.

```text
prospective_maker_edge_h =
    spread captured versus contemporaneous mid
  - current maker fee
  - side-adjusted mid move against the maker through horizon h
```

Эквивалентная прямая проверка — side-adjusted разница между trade price и future mid минус fee;
нельзя одновременно включить обе формы и дважды вычесть один markout.

- binding primary horizon: 5 s; 1 s и 30 s — только sensitivity checks;
- weighting: trade notional;
- uncertainty: hour/day clusters;
- отдельно показать доступный RPI notional и persistence по дням;
- текущая Pro maker fee для perps/spot — 0.3 bp по
  [официальной fee schedule](https://docs.paradex.trade/trading/trading-fees).

BTC и ETH — два заранее объявленных arms. Если PASS любого из них может разрешить first-party probe,
используются simultaneous family-wise 95% intervals: например, two-sided 97.5% CI на каждый arm по
Bonferroni. Нельзя выбрать лучший symbol по двум обычным 95% CI.

- **PASS DATA GATE для symbol:** adjusted `LCB(prospective_edge_5s) > 0` и leave-one-day sensitivity
  не показывает, что результат создан одним днём.
- **KILL для symbol:** adjusted `UCB(prospective_edge_5s) <= 0` либо capacity заведомо недостаточна.
- **PARK / INCONCLUSIVE:** CI пересекает ноль или side/data quality не закрыты к fixed deadline.

PASS на наблюдаемых чужих fills разрешает только один minimum-size first-party RPI probe для
прошедшего symbol: последний приоритет остаётся главным неизвестным по fill rate/capacity.

### 4.2 Lighter Standard/Premium — самый дешёвый venue scout сейчас

Локально уже идёт untouched ETH capture за 2026-08-06..10:
[`lighter-capture`](../../../myhft-signal-run/lighter-capture). Его следует довести до заранее
зафиксированных семи дней и анализировать без geometry sweep.

Текущая venue-механика:

| Account | Maker fee | Дополнительная maker/cancel latency | Основное ограничение |
|---|---:|---:|---|
| Standard | 0 bp | 200 ms | 60 requests/min |
| Premium, 0 LIT | 0.4 bp | 0 ms для post-only/cancel | 4,000 `sendTx`/`sendTxBatch` per min + volume quota |

Источник — [Lighter trading fees](https://docs.lighter.xyz/trading/trading-fees) и
[rate limits](https://apidocs.lighter.xyz/docs/rate-limits).

Premium volume quota нельзя игнорировать. Direct render официальной страницы, полученный
2026-08-10, задаёт:

- новый account начинает с 1,000 transaction credits;
- один новый credit начисляется на каждые **$2 traded volume**;
- `L2CreateOrder`, `L2ModifyOrder`, `L2CreateGroupedOrders` и `L2CancelAllOrders` тратят quota;
- обычный single cancel quota не тратит;
- один `SendTx` каждые 15 s бесплатен;
- стартовый запас не доказывает sustainable economics.

Источник — [Volume Quota](https://apidocs.lighter.xyz/docs/volume-quota-program). При этом search-index
surface того же официального URL на ту же дату всё ещё возвращает старые/отличающиеся `$5/credit` и
другие caps. Поэтому точный quota rate имеет статус **UNVERIFIED EXTERNAL CONTRACT**, несмотря на
live direct render. Data gate обязан:

- показать sensitivity для `$2/credit` и `$5/credit`;
- считать quota feasible только в обоих вариантах либо после отдельной operational verification;
- на experiment freeze сохранить dated raw snapshot и сверить фактический remaining-quota response.

#### Data-only gate

Публичный capture tier-independent, поэтому fixed seven-day ETH window сначала оценивается в двух
заранее заданных сценариях, а не только при Premium fee:

`adverse markout` ниже означает side-adjusted движение contemporaneous mid → future mid. Ему
эквивалентна прямая trade-price → future-mid формула; обе формы одновременно не суммируются.

До outcomes фиксируется одна virtual quote policy: price/size, placement, requote/cancel triggers и
conservative trade-through fill rule. Только она задаёт timestamps, к которым можно применить
Standard `+200 ms`. Без воспроизводимой policy public capture позволяет оценить лишь 200-ms
stale-exposure bound; tier latency тогда остаётся вопросом first-party probe, а не replay result.

- touch-trade proxy и available spread;
- binding 5 s prospective maker proxy edge; 1/30 s sensitivity;
- Premium: `captured spread - 0.4 bp fee - adverse markout` без дополнительной post-only/cancel
  latency;
- Standard: zero-fee economics с консервативным 200 ms place/cancel replay и exact rolling
  60 requests/min ledger, включая bursts;
- hour/day clustered uncertainty;
- fill-event notional/capacity proxy;
- для Premium — точный timestamped quota ledger по venue semantics: grouped order стоит одну quota,
  `SendTxBatch` из `n` transactions стоит `n`; отдельная no-group sensitivity служит conservative
  bound, но не называется exact ledger.

Если PASS любого tier может разрешить first-party probe, для двух scenarios используются
simultaneous family-wise 95% intervals — например, two-sided 97.5% CI на каждый tier по Bonferroni.

- **PASS DATA GATE для tier:** adjusted `LCB(prospective_proxy_edge_5s) > 0`, exact rate/quota ledger
  feasible и результат не создан одним day cluster.
- **KILL для tier:** adjusted `UCB(prospective_proxy_edge_5s) <= 0` либо rate/quota feasibility
  невозможна.
- **PARK / INCONCLUSIVE:** CI пересекает ноль, quota contract не разрешён или virtual-policy replay
  невоспроизводим к fixed deadline.

PASS разрешает лишь minimum-size first-party fill-quality probe или заранее спроектированный paired
tier check; public touch prints не моделируют нашу queue selection и не различают фактическую
latency двух account types.

Standard нельзя заранее объявлять infeasible по одному среднему rate: текущий HYPE campaign имел
около 4.4 order ACK/min в среднем, хотя burst limit ещё надо проверить. Его zero fee может оказаться
ценнее Premium latency; решение должно прийти из 200 ms replay, rolling rate ledger и затем
first-party evidence.

### 4.3 HL carrier selection — только как hybrid с `#20`

Новый symbol не является новым edge. HYPE можно оставить лабораторией, но не считать production
carrier по умолчанию. По уже записанному 16-symbol HL+Binance окну можно сейчас выполнить
descriptive rerank книги, spread/tick geometry и lead-lag. Policy scoring имеет смысл только после
того, как одна frozen `#20` policy определена и пережила Stage-0 triage. Тогда discovery metric:

```text
signal-conditioned proxy edge * proxy retained touch notional
```

В итоговую таблицу также входят spread persistence, measured tick, touch queue, Binance→HL lead,
funding и request/action economics. Предварительные candidates — INJ, JTO, PENDLE и ONDO, но выбор на
этом окне остаётся discovery. Публичные books/trades не доказывают нашу fill probability или queue
attainment: после будущего untouched confirmation всё равно нужен minimum-size first-party fill
probe.

Если ни один carrier не имеет положительного signal-conditioned proxy edge после costs, а
first-party probe не подтверждает положительную absolute economics и capacity, семейство passive HL
market making следует закрыть.

---

## 5. Hyperliquid request economics: поправка к прежнему критерию

Правило `<1 request/$` больше нельзя считать абсолютным техническим ceiling. Hyperliquid по-прежнему
начисляет примерно одну address-based action на $1 cumulative volume и даёт начальный buffer 10,000;
для cancels действует отдельный расширенный allowance. Актуальные правила —
[Rate limits and user limits](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits).

Кроме того, exchange endpoint теперь позволяет резервировать дополнительные request weights по
**0.0005 USDC за weight**, списывая стоимость с Perps balance:
[`reserveRequestWeight`](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint).

Следствие:

```text
1 purchased weight per $1 filled notional ~= 0.0005 * 10,000 = 5 bp
```

Расчёт 5 bp использует USDC≈USD. Платная capacity превращает **address-based cumulative cap** в
economic gate, но не создаёт edge. Она не документирована как обход независимого congestion cap,
который ограничивает address относительно previous-day maker share.

Exact ledger обязан учитывать:

- New, Modify, Cancel, failed/retried и purchased weights раздельно;
- batch из `n` orders/cancels как `n` address-based requests, даже если IP weight у batch другой;
- cancel cap `min(limit + 100000, limit * 2)`;
- opening earned headroom от исторического volume как отдельный исчерпаемый stock;
- marginal allowance, заработанный **retained volume текущего run**;
- initial 10,000 buffer отдельно.

Исторический headroom, initial buffer и купленные weights не могут маскировать отрицательный
steady-state contribution.

Для promotion нужны два отчётных режима:

1. economics без покупки weights — устойчивость на marginal earned/cancel allowances;
2. economics с фактически необходимыми purchased weights — их USDC-стоимость включена в net
   edge и daily PnL.

Эта секция заменяет формулировку «`requests/$ < 1` — hard venue feasibility» в прежней версии
[`criteria-20260810.md`](criteria-20260810.md).

---

## 6. Кандидаты второго порядка

### Quote-approach / local momentum

На 5,809 placement intervals и 1.27M book frames можно оценить fill hazard по скорости приближения
рынка к quote и проиграть реальную cancel delay. Но 97.9% fills с движением к deep quote частично
тавтологичны: без приближения рынка fill невозможен. Нужен denominator всех order-life intervals.

Как самостоятельный rescue сигнал идея слаба: если selector удаляет adverse move, но **не меняет
capture cohort**, даже идеальное удаление текущих 2.448 bp adverse оставляет
`0.814 - 1.520 = -0.706 bp` до прочих costs. Selector теоретически может одновременно улучшить
captured spread, поэтому это аргумент за низкий приоритет, а не доказательство невозможности.
Использовать только как incremental feature/mechanism check рядом с внешним leader signal.

### OFI / multi-level imbalance

Тестировать только как один preregistered incremental term после basis-z. Старый Bybit-era prior к
2025 году давал около 0.44 bp/trade уже с rebate; без него edge был около нуля. См.
[`bybit-era-pycharm-archive.md`](../../myhft/docs/research/bybit-era-pycharm-archive.md). Это не
самостоятельный ответ на дефицит 3.154 bp.

### Carry

Funding/cash-and-carry — отдельный exogenous transfer и отдельная стратегия с legging, margin и basis
risk. Его нельзя считать rescue market-making grid или смешивать с trial count текущего исследования.

---

## 7. Что парковать

- текущую symmetric HYPE/SUI grid geometry;
- новый `half_spread × interval × grid_num` sweep;
- latency/queue rescue до появления положительного fill edge;
- size scaling ради requests/$;
- generic ML/feature mining;
- dynamic spread / Avellaneda–Stoikov без predictor;
- HL fee-tier/rebate farming как seed-alpha;
- live Lighter grid до прохождения tier-comparison data gate;
- fresh listings без отдельного passive observer gate;
- carry как продолжение maker research.

Killed-гипотеза может вернуться только с новым причинным механизмом или новым типом потока, а не под
новым набором параметров.

---

## 8. Минимальный research portfolio и decision tree

### Active primary: `#20`

```text
existing-fill continuous-z join + order-life replay
  -> FAST KILL
  -> либо PARK / INCONCLUSIVE
  -> либо CONTINUE
       -> исправить FSM/latch
       -> untouched clean shadow
       -> KILL / NO ARM
       -> либо minimum-size armed canary
```

### Active data-only scout: Lighter Standard/Premium

```text
fixed seven-day ETH capture
  -> frozen two-tier absolute-edge + latency/rate/quota test
  -> KILL
  -> либо minimum-size first-party tier probe
```

### Passive structural fallback: Paradex RPI

```text
fixed one-week BTC/ETH RPI recording
  -> validate taker-side semantics
  -> prospective absolute edge after current fee
  -> KILL
  -> либо minimum-size first-party RPI fill probe
```

### Portfolio rule

- одновременно допускается только один engineering-heavy experiment;
- data-only scouts имеют собственные absolute PASS/KILL criteria и не конкурируют за «лучший»
  post-hoc результат;
- ни один data-only proxy не разрешает production connector/order-path work автоматически;
- inconclusive к predeclared deadline означает `NO ARM`, а не новый threshold search.

---

## 9. Ближайшие deliverables

1. **`#20 Stage-0 report`:** actual HYPE fills × continuous adverse-z плюс full order-life replay;
   same-window `mu_all,overlap`, `p`, `d`, retained edge/capacity, late-cancel treatment и
   hour-cluster uncertainty.
2. **Lighter tier data gate:** дождаться fixed seven-day ETH window; один Standard/Premium
   absolute-edge + latency/rate/quota report без geometry sweep.
3. **Paradex RPI report:** сначала валидировать taker-side semantics, затем на fixed недельном окне
   посчитать binding 5 s absolute trade-price edge после текущей 0.3 bp fee, отдельно BTC/ETH и по
   дням; 1/30 s оставить sensitivity.
4. **HL conditional carrier table:** descriptive rerank можно выполнить сейчас; frozen-policy scoring
   — только если `#20` переживает Stage 0. Ранжировать по proxy edge × proxy retained notional, затем
   подтвердить на untouched future window и first-party fills.

Главный критерий программы прост: исследование должно сначала доказать существование положительного
conditional fill edge, затем actionability и capacity, и только после этого получать право на live
scale-up.
