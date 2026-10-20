# Состояние кросс-биржевого криптоарбитража: CEX–CEX, CEX–DEX и DEX–DEX

- Статус: **Research memo; документ не разрешает live trading или разработку нового money path**
- Дата среза: 2026-08-10
- Scope: CEX–CEX, CEX–DEX, same-chain DEX–DEX и cross-chain DEX–DEX; практические следствия для `myhft`
- Метод: обзор актуальных правил площадок, официальной execution-документации и последних доступных эмпирических исследований; новых backtest/live-прогонов не выполнялось
- Связано: [`alpha-structural-edge-20260810.md`](alpha-structural-edge-20260810.md), [`criteria-20260810.md`](criteria-20260810.md), [`proposal-20260810.md`](proposal-20260810.md)

---

## 1. Executive summary

Кросс-биржевой арбитраж не исчез. На зрелых ликвидных рынках простая версия бизнеса, в которой
достаточно увидеть публичный spread, больше не имеет положительного prior. Устойчивая рента
сместилась в execution layer:

- индивидуальные fee/rebate terms;
- предфандированный inventory и дешёвый rebalance;
- borrow, margin и банковские/юридические рельсы;
- быстрый market data и корректное исполнение обеих ног;
- private order flow;
- builder, sequencer или block-engine access;
- точная оценка price impact, failure probability и стоимости капитала.

Главное разделение — не `CEX` против `DEX`, а **atomic** против **non-atomic** исполнения:

| Сегмент | Можно атомизировать обе ноги? | Актуальное состояние | Основной moat | Prior для небольшой новой команды |
|---|---:|---|---|---:|
| CEX–CEX | Нет | Tier-1 сильно сжат; периферийные gaps отражают реальные frictions | fee tier, prefunding, latency, borrow, settlement/legal access | Низкий без заранее известного structural advantage |
| AMM DEX–CEX | Нет | Большой, но концентрированный MEV/HFT-бизнес | CEX inventory + builder/sequencer rights + private flow | Низкий на blue-chip Ethereum, умеренный в узкой нише |
| CLOB/perp DEX–CEX | Нет | По механике ближе к CEX–CEX; venue maturity различается | price discovery, fee/flow semantics, inventory, order-entry | Умеренный и лучше всего совпадает с текущим стеком |
| DEX–DEX, одна chain | Условно: только одна atomic transaction или bundle с гарантией all-or-nothing | Leg risk снимается после final inclusion, но рента разыгрывается в inclusion-аукционе | simulation, trigger attribution, private flow, bid/tip | Низкий для generic scanner; умеренный для узкого L2 backrun |
| DEX–DEX, разные chains | Обычно нет | Реален, но капиталоёмок и операционно тяжёл | inventory на обеих chains, finality и rebalance | Умеренный как data-only scout, высокий execution risk |

Практический вывод для `myhft`: нужно разделить два независимых направления. Сигнал `#20` остаётся
**selective market making / fill-selection alpha** на HYPE/Hyperliquid по binding-критериям из
[`criteria-20260810.md`](criteria-20260810.md). Настоящий CEX–DEX arbitrage — отдельная гипотеза:
обе ноги marketable, ex-ante all-in net положителен, а single-leg, partial и status-unknown outcomes
входят в безусловный результат. Frozen maker→hedge policy сам по себе не превращает market making в
арбитраж.

---

## 2. Что считать арбитражем

### 2.1 Рабочая экономика

Displayed mid-to-mid spread не является торговым результатом. Ниже все величины выражены в одном
numeraire, например USD, а `q` — количество base asset:

```text
net_usd(q) = q * [executable sell VWAP(q) - executable buy VWAP(q)]
             - trading_fees_usd(q)
             - gas / builder tip / sequencer payment
             - borrow / funding
             - E[failed leg, revert, residual inventory and status-unknown loss]
             - transfer / rebalance / FX cost
             - capital and venue-risk charge
```

Executable VWAP уже включает book/pool price impact, поэтому slippage нельзя вычитать второй раз.
Latency и hedge impact должны менять distribution фактических execution prices и failure outcomes,
а не добавляться поверх уже реализованного VWAP как дублирующая строка.

Capacity определяется минимумом доступной depth обеих ног, inventory/borrow на каждой площадке,
throughput ребаланса и тем, насколько opportunity переживает реальную задержку. Число alerts в
сканере capacity не измеряет.

### 2.2 Похожие стратегии, которые нельзя смешивать

- `купить → перевести blockchain → продать` — направленная позиция во время transfer window, а не
  locked arbitrage;
- maker на одной площадке + taker hedge на другой — cross-venue market making: maker-fill условен и
  обычно токсичен;
- perp–perp convergence — relative-value trade без гарантированной даты схождения;
- spot–perp funding capture — carry;
- spot–dated-future cash-and-carry ближе к quasi-arbitrage благодаря expiry/settlement;
- cross-venue predictor без второй ноги — alpha, даже если predictor использует цену другой биржи.

---

## 3. CEX–CEX

### 3.1 Tier-1: промышленный low-margin business

На одинаковых ликвидных spot/perp instruments простая схема `два taker IOC` имеет низкий prior.
Публичная комиссия сама часто выше видимого gap. Например, актуальная на дату среза базовая таблица
Bybit указывает для VIP0 `10 bp` spot-taker и `5.5 bp` derivatives-taker: иллюстративный hurdle двух
одинаковых taker-ног составляет соответственно `20 bp` и `11 bp` ещё до slippage. Даже Supreme VIP
оставляет `9 bp` для двух spot-taker ног и `6 bp` для двух derivatives-taker ног; фактическую ставку
нужно брать из account/pair/region endpoint, поскольку private MM terms и промо отличаются.
Источник: [Bybit Trading Fee Structure, updated 2026-07-30](https://www.bybit.com/en/help-center/article/Trading-Fee-Structure).

Latency и leg risk не являются только backtest caveat. Live-исследование на 6.28 млн Bybit orders и
40 тыс. Binance orders обнаружило failures исполнения по увиденному top-of-book и adverse selection:
самые привлекательные taker-сигналы исполнялись хуже. Очевидный Binance→Bybit lead-lag после
реального исполнения и комиссии оказался убыточным, а actual PnL был почти на 100% ниже naive
last-seen-book PnL. Данные относятся к 2023 году, публикация — к 2025-му; это сильное подтверждение
механизма, но не текущая оценка bp/day. Источник: [The Good, the Bad, and Latency](https://doi.org/10.1080/14697688.2025.2515933).

API не даёт атомарности двум площадкам. Даже acknowledgement одной ноги ещё не обязательно означает
финальный order state: например, Bybit прямо называет create-order acknowledgement асинхронным и
требует подтверждать состояние через WebSocket. Источник: [Bybit V5 Create Order](https://bybit-exchange.github.io/docs/v5/order/create-order).

Рабочая схема требует:

1. quote inventory на buy-venue и base/borrow capacity на sell-venue заранее;
2. sweep/VWAP по одинаковому размеру, а не сравнение BBO;
3. IOC/FOK либо явный state machine для partial/unknown fills;
4. measured local-arrival latency и clock discipline;
5. последующий rebalance вне critical path.

### 3.2 Где gaps остаются

**Периферийные и рискованные CEX.** Исследование 16 бирж с данными до апреля 2024 года показывает,
что opportunities живут дольше, когда дороже именно площадка с более высоким default risk;
ликвидность ускоряет convergence, а fees и blockchain congestion тормозят capital flows. Широкий
sell price здесь может быть компенсацией за нежелание держать актив на venue, а не бесплатной
неэффективностью. Источник: [Bitcoin arbitrage and exchange default risk](https://orca.cardiff.ac.uk/id/eprint/173664/).

**Региональные fiat/stablecoin markets.** BIS на данных четырёх stablecoins, 27 fiat currencies и
64 exchanges за 2021–2025 годы обнаружил крупные устойчивые stablecoin/FX parity gaps; они особенно
велики в emerging markets, где balance-sheet constraints сильнее. Это реальный structural edge для
участника с банковским, KYC и legal access, но не воспроизводимый API-only ботом. Источник:
[BIS Working Paper 1340, 2026](https://www.bis.org/publ/work1340.htm).

**Новые листинги, long-tail и stress.** Там gross spread выше, но одновременно меньше depth,
хуже borrow/withdrawal capacity, чаще status-unknown и выше tail risk. Persistent one-direction
premium быстро исчерпывает inventory и возвращает transfer/FX friction.

Крупные институционалы дополнительно уменьшают **collateral fragmentation и transfer friction** через
off-exchange settlement. Copper заявляет для ClearLoop более `$50B` monthly notional и collateral
delegation к нескольким крупным CEX; это vendor-reported figure, а не независимая оценка. Такие rails
не устраняют price/liquidity fragmentation и не атомизируют сделки; они добавляют отдельный
custodian/settlement/legal layer. Источник:
[Copper ClearLoop](https://copper.co/en-li/products/clearloop).

### 3.3 Verdict

- generic Tier-1 two-taker scanner — **низкий prior** без private fee/rebate terms;
- Tier-2 perp dispersion — **умеренный prior**, но это чаще latency/relative-value alpha;
- regional fiat/stablecoin — **сильный structural edge**, если уже есть нужные legal/banking rails;
- spot–dated-future — более чистый institutional carry, но отдельный strategy family.

В рамках обзора не найден современный unbiased dataset с универсальным CEX–CEX `bp/day`: private
fees, rebates, dual fills и потери обычно не видны. Поэтому текущие обещания доходности без
first-party paired execution следует считать недоказанными.

---

## 4. CEX–DEX

### 4.1 AMM DEX–CEX: большой profit pool, высокая концентрация

На Ethereum наиболее подробное найденное landscape measurement охватывает август 2023 — март 2025:

- `7,203,560` идентифицированных arbitrages;
- `$241.7B` on-chain/DEX-side trade volume, а не наблюдаемый two-leg turnover;
- `$233.8M` inferred extracted value после смоделированной CEX taker fee и Ethereum base fee, но до
  builder tips;
- 19 крупных searchers;
- три searcher-а получили примерно три четверти volume и extracted value.

`$233.8M` — оценка исследователей при ненаблюдаемой CEX-leg, а не доступная новичку net profit:
builder tips, инфраструктура, капитал и ребалансировка требуют отдельного учёта. Авторы также
связывают profitability с интеграцией searcher-а и block builder-а. Источник:
[Measuring CEX-DEX Extracted Value and Searcher Profitability](https://arxiv.org/abs/2507.13023).

Механика принципиально non-atomic. Bundle может защитить DEX-leg, но не CEX order. Поэтому searcher
обычно заранее держит inventory/collateral на CEX, добивается on-chain inclusion и затем хеджирует
по доступной CEX depth. Его EV выглядит так:

```text
EV = P(land) * E[DEX execution - CEX hedge - LP fee - gas - inclusion payment | land]
   - P(fail) * E[failure cost | fail]
   - inventory / rebalance / capital cost
```

Реальная выполнимость подтверждалась и live: опубликованная в 2026 году работа о четырёх CEX–DEX
ботах на двух chains сообщила суммарный profit `$20,237.48` и положительные monthly returns в
коротких окнах 2023–2024 годов после operational frictions. Это доказывает, что edge существовал в
тех окнах; маленький и датированный sample не является современной capacity estimate. Источник:
[Arbitrage trading between decentral and central cryptocurrency exchanges](https://www.sciencedirect.com/science/article/pii/S0378426626000956).

### 4.2 Sequencing rights стали частью себестоимости

На Ethereum searchers конкурируют через builders/MEV-Boost, private bundles и tips. Технический
доступ к bundle API permissionless, но opportunity всё равно достаётся лучшей экономической заявке
и наиболее интегрированному execution path. Источник: [Flashbots documentation](https://docs.flashbots.net/).

Более свежая работа 2026 года моделирует переход Ethereum к односекундным subslots на параметрах
Binance/Uniswap V3 за июль–сентябрь 2025 года и показывает высокую чувствительность CEX–DEX activity
к variance включения. Это simulation, а не measurement реализованного searcher PnL, поэтому она
подтверждает важность ordering regime, но не задаёт текущую доходность. Источник:
[Second Thoughts: How 1-second subslots transform CEX-DEX Arbitrage](https://arxiv.org/abs/2601.00738).

На Arbitrum Timeboost свежая версия исследования использует `48.5M` **всех** express-lane
transactions и `494k` auctions за январь–апрель 2026 года:

- три entity выиграли `99.74%` auctions;
- примерно `30%` ускоренных transactions reverted;
- competition и DAO revenue со временем снижались.

Эта выборка доказывает концентрацию sequencing rights, а не непосредственно concentration или
profitability CEX–DEX arbitrage. Часть latency race была монетизирована через аукцион express-lane,
но profitable MEV в исследовании часто кластеризуется поздно в block, а высокий revert rate
сохраняется. Источник: [The Express Lane to Spam and Centralization, revised 2026-07-21](https://arxiv.org/abs/2509.22143).

На Solana аналогичный execution layer предоставляет Jito: bundles до пяти transactions исполняются
последовательно и all-or-nothing, конфликтующие bundles участвуют в локальных аукционах каждые
`50 ms`, а приоритет определяется tip/CU efficiency. Это атомизирует только Solana legs; CEX hedge
остаётся отдельным. Источник: [Jito Low Latency Transaction Send](https://docs.jito.wtf/lowlatencytxnsend/).

### 4.3 CLOB/perp DEX — другой подтип

Hyperliquid, Lighter и Paradex не следует механически оценивать по Ethereum AMM evidence. По
execution-механике они ближе к CEX–CEX:

- price-time/orderbook или venue-specific matching вместо AMM pathfinding;
- отдельный sequencer/order gateway;
- cross-venue legs по-прежнему non-atomic;
- важны request budget, maker/taker semantics, cancel latency и inventory;
- potential edge чаще приходит из price discovery, flow segmentation или fee structure.

Именно здесь существующий `myhft` stack даёт наименьшую цену входа. Но displayed Binance↔DEX gap
нужно проверять через actual joint execution. Maker-fill на DEX и последующий CEX taker hedge
остаётся cross-venue market making даже при predeclared policy: fill условен и несёт adverse-selection
risk. True two-leg arbitrage требует двух marketable legs и учёта всех single-leg outcomes.

Для perp↔perp дополнительно нельзя предполагать identity инструмента. До economics gate нужно
зафиксировать соответствие contract/index/mark, quote и collateral asset, funding interval,
liquidation/ADL rules и settlement semantics. Остаточный basis без гарантированной даты схождения
делает позицию relative-value hedge, а не locked arbitrage.

### 4.4 Verdict

- Ethereum blue-chip AMM race — **рынок доказан, но entry prior низкий** без builder/private-flow moat;
- покупка dedicated sequencing rights — **не первый эксперимент** небольшой команды;
- CLOB/perp DEX↔CEX — **наиболее подходящая research family** для текущего проекта, но selective MM
  и two-marketable-leg arbitrage должны оставаться разными arms;
- solver/RFQ flow — потенциальный structural edge, но требует отдельного competition/capacity audit.

---

## 5. Same-chain DEX–DEX

### 5.1 Атомарность убирает leg risk, но не competition

Если все legs находятся в одной smart-contract transaction либо protocol-specific bundle с реальной
гарантией all-or-nothing, цикл может revert-нуть целиком при нарушении profit invariant. Это снимает
leg risk только conditional on final inclusion. Flash liquidity дополнительно позволяет получить
актив до оплаты при условии возврата внутри той же atomic transaction. Источник:
[Uniswap V2 whitepaper, Flash Swaps](https://docs.uniswap.org/whitepaper.pdf).

Это снимает риск одной исполненной ноги, но не снимает:

- риск не попасть в block/slot;
- builder/sequencer bid и validator tip;
- gas на неудачные попытки там, где revert платный;
- state race с другим searcher-ом;
- reorg/finality failure и smart-contract risk;
- инфраструктурную стоимость симуляции и low-latency feed.

Когда одну opportunity видят многие searchers, значительная часть gross surplus уходит proposer-у
через inclusion auction. Поэтому flash loan увеличивает возможный notional, но сам по себе не создаёт
retained edge.

### 5.2 L2: окно есть только у targeted search

Дешёвый gas не демократизировал broad scanners. Исследование cyclic arbitrage на Base за июнь 2025 —
февраль 2026 года разделяет targeted search и probabilistic on-chain probing. Probabilistic search
дал лишь `23%` arbitrage activity, но произвёл `95%` классифицированного arbitrage spam, а
on-chain-discovery spam потребил около `20%` всего Base gas. Flashblocks отбирали широкие
probabilistic scanners, а более высокие fee floors — bots, чей opportunity flow не окупал повторные
попытки. Источник:
[To Wait or To Probe, revised 2026-08-04](https://arxiv.org/abs/2606.00720).

Следствие: разумная гипотеза — не «просканировать все циклы», а заранее зафиксировать:

- одну chain;
- один source protocol или тип trigger transaction;
- одну route family;
- один bid/priority policy;
- конкретную decision latency.

Base интересен ~`200 ms` Flashblocks с preconfirmed state, но preconfirmation не равна hard finality.
Источники: [Base protocol overview](https://docs.base.org/base-chain/specs/protocol/overview),
[Flashblocks API](https://docs.base.org/base-chain/api-reference/flashblocks-api/flashblocks-api-overview).

### 5.3 Intent/solver markets

Intent protocols превращают часть арбитража в procurement auction, но их механизмы различаются.
CoW использует bonded solver competition и combinatorial batch auctions, выбирая solution по
user-surplus objective и доступной liquidity. Источник:
[CoW Protocol documentation](https://docs.cow.fi/).

UniswapX использует vetted/permissioned RFQ quoters и Exclusive Dutch Auction: выигравший hard quote
получает временную exclusivity, после чего non-exclusive filling остаётся permissionless либо требует
улучшения цены. Поэтому permissionless tail нельзя смешивать с RFQ-access. Источник:
[UniswapX Filling Overview](https://developers.uniswap.org/docs/liquidity/uniswapx/filling/overview).

Это отдельный business model. Structural edge здесь — RFQ/private liquidity, inventory, netting,
gas efficiency и fill reliability. Permissionless доступ к tail orders не означает положительный
rent: этот поток может быть уже adverse-selected после exclusive quoting window.

### 5.4 Verdict

- generic Ethereum L1 triangular/cyclic scanner — **парковать**;
- generic Solana/Jito latency race — **парковать** без protocol-specific trigger;
- targeted Base backrun — **допустимый data-only arm**;
- CoW/UniswapX solver — **сначала shadow competition**, без bond/live filler до доказанного win rate.

---

## 6. Cross-chain DEX–DEX

### 6.1 Два режима исполнения

Разные chains обычно не имеют общей atomic commit. Есть два практических режима:

1. **Pre-positioned inventory:** обе legs исполняются почти одновременно, капитал заранее лежит на
   обеих chains; затем нужен rebalance.
2. **Bridge-dependent:** bridge стоит между ногами; capital footprint меньше, но gap может исчезнуть
   во время finality/relay.

Годовое исследование девяти chains за сентябрь 2023 — август 2024 года идентифицировало:

- `242,535` cross-chain arbitrages;
- `$868.64M` volume;
- `66.96%` trades с pre-positioned inventory;
- медиану `9 s` для inventory-based против `242 s` для bridge-based execution;
- высокую concentration: top-5 addresses провели более половины trades.

Источник: [Cross-Chain Arbitrage: The Next Frontier of MEV](https://arxiv.org/abs/2501.17335).
Reported empirical net profit вычитает swap gas, builder tips и bridge fees, но не оценивает
inventory risk и capital opportunity cost. Поэтому его нельзя считать чистой доходностью total
committed capital.

### 6.2 Bridge latency — не единственный риск

Даже быстрые мосты могут опираться на soft finality и allowances. Для CCTP Fast Circle указывает
примерно `8 s` attestation на перечисленных L2 и Solana и около `20 s` на Ethereum/Starknet; режим
использует global Fast Transfer Allowance для ограничения reorg exposure. CCTP Standard ждёт hard
finality, но timing зависит от source chain: например, Base/Arbitrum/OP — около `15–19 min`, тогда как
другие chains варьируют от секунд до часов. Источники:
[CCTP finality](https://developers.circle.com/cctp/concepts/finality-and-block-confirmations),
[Fast Transfer Allowance](https://developers.circle.com/cctp/concepts/fast-transfer-allowance).

Полная экономика должна включать:

```text
net_xchain_usd(q) = gross execution PnL_usd(q)
           - trading and LP fees on both legs
           - gas and priority payments
           - bridge / relayer fee
           - inventory financing and rebalance
           - E[reorg + destination failure + outage + depeg + allowance exhaustion]
```

Headline average latency недостаточна: нужны `p95/p99`, allowance=`0`, one-chain halt и depeg stress.

### 6.3 Verdict

- bridge-in-the-loop для fleeting opportunity — **парковать**;
- pre-funded cross-chain inventory — **реальный, но капиталоёмкий niche**;
- исследовать имеет смысл устойчивые long-tail/stablecoin dislocations и return on total committed
  inventory, а не gross profit отдельной пары swaps.

---

## 7. Предлагаемый research portfolio для `myhft`

Приоритет ниже означает соответствие текущему стеку и стоимость проверки, а не уже доказанный edge.

### 7.1 Primary: `#20` selective market making, не arbitrage

Активный primary остаётся ровно один: Binance signal → HYPE/Hyperliquid adverse-side selection. Его
binding decision gate уже определён в [`criteria-20260810.md`](criteria-20260810.md). Добавление CEX
hedge не должно использоваться для переименования этой стратегии в arbitrage: passive maker-fill
остаётся условным и несёт adverse selection.

Lighter и Paradex — отдельные venue hypotheses, а не дополнительные arms того же теста. Их нельзя
добавлять post hoc в primary sample; каждый требует отдельной preregistration и multiple-testing
scope.

### 7.2 Отдельный CLOB DEX–CEX arbitrage scout

True-arbitrage hypothesis допускается как отдельный data-only scout только после выбора **одного**
venue/symbol pair и проверки instrument identity. Для него:

- обе ноги marketable и решение принимается ex ante по all-in executable VWAP;
- `attempt` — каждое eligible событие frozen detection policy, даже если одна нога rejected, partial
  или status-unknown;
- результат включает single-leg loss, residual inventory и final reconciliation обеих venues;
- учитываются actual fee tier, request budget, funding/basis и полный rebalance;
- withdrawal/bridge не находится в critical path.

Если инструменты имеют разные index/settlement/collateral semantics, arm классифицируется как
relative-value trade и оценивается отдельно, а не получает arbitrage PASS.

### 7.3 Secondary: один targeted Base shadow arm

Если выделяется второй независимый research slot:

1. зафиксировать один source protocol, одну route family и decision-time algorithm до outcomes;
2. reconstruct только тот pre-trigger state, который реально был доступен с заданной latency;
3. получить binding counterfactual из frozen algorithm;
4. отдельно посчитать hindsight-optimal cycle как oracle upper bound, не как strategy result;
5. найти actual winning arbitrage и дать диапазон retained PnL с учётом наблюдаемых и потенциально
   private inclusion payments;
6. считать все failed attempts и не отправлять live transactions.

Положительный untouched shadow verdict разрешает только проектирование minimum-size canary с
отдельным safety approval; сам по себе он не разрешает live execution.

### 7.4 Passive only: cross-chain inventory study

Собирать quotes/pool states/block timestamps по 2–3 chain pairs и симулировать две заранее
профинансированные legs плюс реальный rebalance. Не использовать bridge как мгновенную вторую ногу.

### 7.5 Park

- generic Tier-1 CEX–CEX two-taker;
- broad Ethereum L1 public-pool race;
- probabilistic/spam scanner на Base/OP;
- generic Solana/Jito latency race;
- bridge-in-the-loop cross-chain execution;
- live solver/filler до shadow win-rate и capacity evidence.

---

## 8. Минимальные decision gates

### 8.1 Общие требования к данным

- local-arrival timestamps и clock-quality report;
- full executable depth обеих ног, а не BBO/mid;
- фактические account/venue fees;
- gas, builder tip, priority fee или sequencing payment;
- все attempts, включая rejects, reverts, partials и status-unknown;
- frozen определение `eligible attempt`, одинаковое для successes и failures;
- final reconciled state обеих ног; acknowledgement не считается финальным исполнением;
- asset/instrument identity: chain ID, contract address, decimals, quote/collateral, index/mark,
  funding и settlement mapping;
- deposit/withdrawal/borrow/bridge status и venue-status history;
- opening inventory, marginal inventory consumption и полный rebalance;
- immutable manifest: code/data/config/fee regime/order-regime hashes;
- untouched chronological forward window.

### 8.2 PASS / KILL / PARK

До просмотра sample фиксируются `delta_econ` в одной экономической единице, capital/loss budget и
minimum useful capacity. Нулевая statistical boundary недостаточна: положительный epsilon может не
окупать новый stack и operations.

**PASS** разрешает только следующий минимальный experiment, если одновременно:

- lower 95% confidence bound безусловного `net per attempt` выше `delta_econ`;
- результат не держится на top 1–2 volatility events;
- effective sample состоит из независимых source events, а не correlated probes одного shock;
- joint execution/failure distribution измерена по фактическому пути
  `decision → land/fill → final reconciled hedge state`, включая `p99` latency;
- capacity после impact и inventory constraints окупает новый stack и operations;
- stress по fee, latency, failed leg и rebalance не меняет знак при заранее заданных допусках.

**KILL**, если upper 95% confidence bound `net per attempt <= delta_econ`, либо structural capacity не
достигает заранее зафиксированного минимально полезного размера.

**PARK / INCONCLUSIVE**, если confidence interval пересекает decision boundary, данных недостаточно
или execution regime изменился внутри окна. Это не разрешение ретюнить threshold/routes на том же
sample.

Статистический gate не заменяет hard risk gates:

- maximum gross venue/bridge exposure и maximum single-attempt/session loss;
- asset allowlist и smart-contract/venue approval;
- one-venue outage, reorg/finality failure, depeg/default и allowance=`0` scenarios;
- проверенный recovery/reconciliation path после partial/status-unknown;
- fail-closed stop при смене fee, ordering, deposit/withdrawal или bridge regime.

### 8.3 Что должно быть primary outcome

| Направление | Primary outcome |
|---|---|
| `#20` selective MM | binding absolute retained-fill economics и capacity по [`criteria-20260810.md`](criteria-20260810.md), без arbitrage label |
| CEX–CEX | unconditional PnL всех eligible attempts, включая single-leg/partial/unknown, после rebalance; return на average и peak prefunded capital |
| CLOB DEX–CEX true arb | unconditional two-marketable-leg net per eligible attempt, residual inventory и retained capacity |
| Same-chain DEX–DEX | unconditional retained PnL frozen decision algorithm после inclusion bids и всех failed-attempt costs |
| Cross-chain | return on total inventory capital после `p99` rebalance/finality stress |
| Solver/intent | shadow auction win rate и net reward после gas/service/bond capital cost |

---

## 9. Ограничения обзора

1. Публичные исследования всегда отстают от live market. Landscape measurement Ethereum CEX–DEX
   заканчивается мартом 2025; более свежая subslot-работа калибрована на июле–сентябре 2025, но
   является simulation. Arbitrum Timeboost измерен до апреля 2026, Base cyclic arbitrage — до февраля
   2026.
2. CEX legs, private rebates, internal transfers и часть infrastructure costs часто ненаблюдаемы.
   Поэтому on-chain extracted value не равен net searcher PnL.
3. Актуальные fee и ordering rules могут меняться; перед experiment их нужно сохранить в manifest и
   не заменять сегодняшней web-страницей задним числом.
4. Этот обзор не запускал first-party paired execution и не доказывает наличие edge у `myhft`.
5. Ранжирование — research priority по prior × cost of information × stack fit, а не разрешение на
   капитал или production connector work.

---

## 10. Итог

Современный cross-venue arbitrage — это преимущественно **structural execution business**. Публичный
top-of-book commoditized, но timely full state и private flow общедоступными не являются. Редкость
создают право исполниться первым, дешёвая вторая нога, inventory в нужном месте и способность
пережить rebalance/tail risk.

Для текущего проекта `#20` следует проверять как selective-MM alpha по уже зафиксированным критериям,
не добавляя ему arbitrage label. Отдельный true-arbitrage scout возможен только на одной
предзарегистрированной паре с двумя marketable legs и безусловным учётом failures. Generic CEX scanner
или новый MEV stack не следует строить до такого data gate. Если нужен независимый scout, наиболее
информативен один targeted same-chain Base shadow experiment, а не широкий перебор routes.
