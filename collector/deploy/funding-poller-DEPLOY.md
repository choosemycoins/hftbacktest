# Выкатка funding-поллера на токийский хост сбора (#88)

Ревизия артефактов: **`55a3e4d`** (ветка `fix/depth-freshness-83`, не пушнута — файлы
едут на хост копированием, не `git pull`).

Тег релиза, который получится: **`fundingpoller-55a3e4d`**.

> ⚠ Ревизия сдвинулась с `8fb6430`. Если у тебя открыта прежняя редакция этого файла или
> уже скопированы файлы в `/tmp/funding-drop` — **доставить заново**, sha256-сверка на
> шаге 1 это поймает. Что приехало: `03f12ac` — провал ноги стал тотальным (прежний
> `_run_leg` ловил перечисление классов, а провод рвётся и тем, что **не** наследует
> `OSError`: `http.client.IncompleteRead`, `EOFError`, `zlib.error` — а gzip получают
> ровно aster и paradex; любой из них уносил весь тик, счётчик провалов не рос, и алерт
> по ноге не наступал никогда). `55a3e4d` — пины на UTC-сутки файла и на разбор
> `alert.env`, поведение не менялось. Тестов у поллера 40 (было 32), сюита
> `collector/tools` — 885 зелёных.

Что едет на хост (четыре файла):

| в релизе | из репозитория |
|---|---|
| `bin/funding_poller.py` | `collector/tools/funding_poller.py` |
| `bin/heartbeat.sh` | `collector/deploy/heartbeat.sh` (в нём строка `funding:…:20`) |
| `etc/hft-funding-poller.service` | `collector/deploy/hft-funding-poller.service` |
| `etc/hft-funding-poller.timer` | `collector/deploy/hft-funding-poller.timer` |

> **Почему релиз собирается руками, копией `current`, а не `build-release.sh` + `install.sh`.**
> Долг #106 (найден на выкатке day0, major): `build-release.sh`/`cross-build-linux.sh` не
> стейджат **ни один** поллер и их юниты, а `install.sh` ставит из фиксированного
> манифеста тарбола, где поллеров нет. При этом `ExecStart` юнита указывает **внутрь**
> атомарно-свопаемого дерева (`/opt/hft-collector/current/bin/funding_poller.py`). Пока
> сборщик не научен, единственный способ не нарушить деплой-контракт — **новый каталог
> релиза** (копия текущего + четыре файла) и **атомарный своп симлинка**. Живое дерево
> `current/` не правится ни разу, `rollback.sh` продолжает работать.
> **Долг #106 этим релизом РАСШИРЯЕТСЯ**: вписать в `build-release.sh` и манифест
> `install.sh` теперь надо day0-поллер И funding-поллер (py-файлы + юниты + heartbeat).
> Каждый штатный релиз до закрытия долга молча снимет с хоста оба.

Отличия от day0-выкатки, которые надо знать: поллер **не трогает systemd** (нет
инстансов захвата, нет таймеров переподписки — только сеть и диск), данные пишет на
волюм данных `/opt/hft-collector/data/funding/`, пульс и состояние — в
`/home/ubuntu/funding-data/`. Идемпотентности в смысле day0 у него нет и быть не
должно: это append-only временной ряд, второй тик обязан **дописать** строку, а не «не
сделать ничего». Пин на шаге 6 проверяет именно это.

---

## ⛔ Прочитать до шага 0: ряд снимается, но сам НЕ доезжает

Панель нашла блокер, и он **не починен** в `55a3e4d`. `offload.sh` разбирает имена
файлов функцией `day_of` и признаёт ровно три формы; наше имя
`<venue>-YYYYMMDD.jsonl.gz` не подходит ни под одну (нужны подчёркивание перед датой и
`.gz` **сразу** после даты). Проверено прогоном самой функции:

```
hyperliquid-20260820.jsonl.gz            NO-MATCH
paradex-20260820.jsonl.gz                NO-MATCH
binancefuturesum_btcusdt_20260819.gz     MATCH 20260819
pulse-20260820.gz                        NO-MATCH
```

Следствия, которые надо принять сознательно:

1. Автодискавери каталогов офлоад **находит** (`find -mindepth 1 -maxdepth 1 -type d`),
   а файлы внутри — нет. В отчёте это выглядит как `  unrecognised names: N, left alone`
   и `funding: nothing to move`. Отказ **тихий**: пульс зелёный, TG молчит, поллер
   рапортует полные вселенные.
2. Правка `--instances` (совет прежней редакции этого документа) **не помогает** —
   режется имя файла, а не каталог.
3. Файлы не только не доезжают, но и **не удаляются** — ~69 МБ/сут копятся на хосте
   вечно.
4. Пульс (`/home/ubuntu/funding-data`) офлоад не забирает **вообще** — он ходит только по
   `/opt/hft-collector/data`. Без пульса рядом с данными дыра в ряде на машине оператора
   неинтерпретируема: аутэдж площадки, мёртвый таймер и забитый диск выглядят одинаково.

**Решение на выкатку — деплоить**, но забирать руками (шаг 9). Обоснование: ценность
#88 в том, что неснятый час потерян навсегда; снятый и лежащий на хосте час
восстановим, а неснятый — нет. Долг (назвать в трекере рядом с #106): привести имя к
форме, которую `day_of` признаёт (`funding.<venue>_YYYYMMDD.gz` — проверено, матчится),
либо научить `day_of` нашей форме; и дублировать строку пульса в `data/funding/` под
распознаваемым именем. **До закрытия долга шаг 9 обязателен после каждого офлоада.**

---

## Шаг 0. Предполётные проверки (всё — чтение, ничего не меняет)

```bash
TOKYO=ubuntu@<хост-сбора>          # тот же, куда ходит offload.sh
ssh "$TOKYO"
```

**0.1 Диск.** Аппетит измерен локально двумя настоящими тиками: **238 597 байт gz на
тик**, при 288 тиках в сутки — **68.7 МБ/сут**: paradex 57.3 (83.4%), aster 5.3 (7.7%),
hyperliquid 4.4 (6.4%), lighter 1.7 (2.5%). Это ~2.1 ГБ/мес и 0.7% суточной записи хоста
(~10 ГБ/сут). На проводе — 396 КБ/тик ≈ 114 МБ/сут (HL и lighter приходят `identity`,
gzip дают только aster и paradex).

**Но с учётом блокера выше файлы с хоста не удаляются офлоадом**, поэтому запас считать
надо не на сутки, а на интервал ручного забора:

```bash
df -BG /opt/hft-collector/data     # warn heartbeat: 15 ГБ, crit: 8 ГБ
sudo du -sm /opt/hft-collector/data/* | sort -n
```

70 МБ/сут — это ~2 ГБ/мес. Если до warn остаётся меньше 4 ГБ, сначала офлоад
маркетдаты, потом выкатка.

**0.2 Хук алертов на месте** (юнит несёт `OnFailure=hft-collector-alert@%n.service`;
без хука systemd молча не разрешит цель, и падения поллера будут тихими):

```bash
systemctl cat hft-collector-alert@.service >/dev/null && echo "хук есть"
sudo test -r /opt/hft-collector/etc/alert.env && echo "alert.env читается"   # root:600
```

**0.3 Python и текущий релиз:**

```bash
/usr/bin/python3 -VV                      # 3.10+; поллер stdlib-only, venv не нужен
readlink -f /opt/hft-collector/current    # запиши: это будет .previous и цель отката
du -sh "$(readlink -f /opt/hft-collector/current)"
ls /opt/hft-collector/data/ | grep -c funding   # ожидается 0: каталога ещё нет
```

Каталог `data/funding` руками **не создаём** — его делает первый пишущий тик
(`append_jsonl` делает `mkdir`). Он не инстанс захвата (нет `etc/funding.env`), поэтому
цикл heartbeat по коллекторам его не проверяет: свежесть сторожится **пульсом**.

**0.4 Сверка блокера доставки — на машине оператора, до выезда** (чтобы шаг 9 не стал
сюрпризом):

```bash
cd /home/andrew/RustroverProjects/hftbacktest
bash -c '
day_of() {
    local name="$1"
    if [[ "${name}" =~ ^[A-Za-z0-9._:-]+_([0-9]{8})\.gz$ ]] \
    || [[ "${name}" =~ ^_meta_[A-Za-z0-9._-]+_([0-9]{8})\.jsonl$ ]] \
    || [[ "${name}" =~ ^gate/([0-9]{8})\.(txt|json)$ ]]; then
        echo "MATCH ${BASH_REMATCH[1]}"; return 0; fi
    echo "NO-MATCH"; return 1; }
day_of hyperliquid-20260820.jsonl.gz'
```

Ожидается `NO-MATCH`. Если однажды здесь будет `MATCH` — значит долг закрыт, и шаг 9
можно не делать руками (проверить это эффектом: файлы появились на машине оператора).

---

## Шаг 1. Доставка файлов (с машины оператора)

```bash
cd /home/andrew/RustroverProjects/hftbacktest
git rev-parse --short HEAD             # должно быть 55a3e4d (или новее — тогда правь TAG ниже)
git status --porcelain collector/tools/funding_poller.py collector/deploy/heartbeat.sh \
    collector/deploy/hft-funding-poller.service collector/deploy/hft-funding-poller.timer
                                       # должно быть пусто

ssh "$TOKYO" mkdir -p /tmp/funding-drop
scp collector/tools/funding_poller.py \
    collector/deploy/heartbeat.sh \
    collector/deploy/hft-funding-poller.service \
    collector/deploy/hft-funding-poller.timer \
    "$TOKYO":/tmp/funding-drop/
```

Проверка целостности доставки (сверить обе стороны глазами):

```bash
sha256sum collector/tools/funding_poller.py collector/deploy/heartbeat.sh \
          collector/deploy/hft-funding-poller.service collector/deploy/hft-funding-poller.timer
ssh "$TOKYO" 'sha256sum /tmp/funding-drop/funding_poller.py /tmp/funding-drop/heartbeat.sh \
          /tmp/funding-drop/hft-funding-poller.service /tmp/funding-drop/hft-funding-poller.timer'
```

Расхождение хоть по одной строке — доставить заново, дальше не идти.

---

## Шаг 2. Новый релиз = копия `current` + четыре файла

Всё дальше — на хосте, от root.

```bash
ssh "$TOKYO"
sudo -i

TAG=fundingpoller-55a3e4d
SRC="$(readlink -f /opt/hft-collector/current)"
NEW="/opt/hft-collector/releases/${TAG}"

test ! -e "$NEW" || { echo "ОСТАНОВ: ${NEW} уже существует"; exit 1; }

cp -a "$SRC" "$NEW"                       # -a: права/владельцы/времена как были

install -m 755 -o root -g root /tmp/funding-drop/funding_poller.py "$NEW/bin/funding_poller.py"
install -m 755 -o root -g root /tmp/funding-drop/heartbeat.sh      "$NEW/bin/heartbeat.sh"
install -m 644 -o root -g root /tmp/funding-drop/hft-funding-poller.service "$NEW/etc/"
install -m 644 -o root -g root /tmp/funding-drop/hft-funding-poller.timer   "$NEW/etc/"

# происхождение — в манифест, чтобы `rollback.sh --list` не врал о содержимом
printf 'funding_poller_from=55a3e4d\nfunding_poller_added_at=%s\n' "$(date -u +%FT%TZ)" >> "$NEW/RELEASE"
```

Проверки до свопа (ни одна ничего не меняет):

```bash
/usr/bin/python3 -m py_compile "$NEW/bin/funding_poller.py" && echo "поллер компилируется"
bash -n "$NEW/bin/heartbeat.sh" && echo "heartbeat синтаксически цел"
grep -n 'funding:' "$NEW/bin/heartbeat.sh"    # строка funding:…/funding-data:…20 на месте
grep -n 'day0:'    "$NEW/bin/heartbeat.sh"    # строка day0 НЕ должна пропасть
diff -r "$SRC" "$NEW" | head                  # ровно четыре новых/изменённых файла + RELEASE
```

⚠ На хосте после выкатки day0 живёт heartbeat со строкой `day0:`. Наш файл из `55a3e4d`
содержит **и** `day0:`, **и** `funding:` — поэтому обе проверки `grep` обязаны пройти.
Если `$SRC/bin/heartbeat.sh` окажется новее git-версии (чего быть не должно) —
**остановиться** и разобраться, не затирается ли чужая строка.

---

## Шаг 3. Атомарный своп `current` и запись `.previous`

Тот же приём, что в `install.sh`/`rollback.sh`: `ln -s` рядом + `mv -T`. Бегущие
коллекторы не затрагиваются (их процесс давно сделал `exec`; путь резолвится только при
старте), рестарта инстансов не требуется — двоичный файл в новом релизе побайтно тот же.

```bash
PREV="$SRC"
ln -snf "$NEW" /opt/hft-collector/current.new
mv -Tf /opt/hft-collector/current.new /opt/hft-collector/current

# .previous — ОБЫЧНЫЙ ФАЙЛ с путём внутри, mode 600 (контракт rollback.sh), не симлинк
printf '%s\n' "$PREV" > /opt/hft-collector/.previous.new
chmod 600 /opt/hft-collector/.previous.new
mv -f /opt/hft-collector/.previous.new /opt/hft-collector/.previous

readlink -f /opt/hft-collector/current            # должен показать $NEW
cat /opt/hft-collector/.previous                  # должен показать $SRC
stat -c '%a %F' /opt/hft-collector/.previous      # 600 regular file
```

С этого момента `hft-heartbeat.service` на следующем получасовом тике возьмёт **новый**
`heartbeat.sh` — тот, что знает про `funding`. Рестартовать ничего не нужно.

---

## Шаг 4. Установка юнитов поллера

```bash
install -m 644 -o root -g root "$NEW/etc/hft-funding-poller.service" /etc/systemd/system/
install -m 644 -o root -g root "$NEW/etc/hft-funding-poller.timer"   /etc/systemd/system/
systemctl daemon-reload

systemd-analyze verify /etc/systemd/system/hft-funding-poller.service \
                       /etc/systemd/system/hft-funding-poller.timer
```

`systemd-analyze verify` обязателен, а не для порядка: класс отказа «`OnFailure` в
секции `[Service]` молча игнорируется» ловится только им (первая редакция day0-юнита
напоролась ровно на это; у нас `OnFailure` и `StartLimitIntervalSec` живут в `[Unit]`).
Прогнан локально на `55a3e4d` — по нашим файлам ноль замечаний; в выводе будет шум по
чужим системным юнитам, его игнорировать.

Каталог пульса (systemd создаёт файл лога, но не каталог):

```bash
install -d -m 755 -o ubuntu -g ubuntu /home/ubuntu/funding-data
```

Именно `/home/ubuntu/funding-data`: туда смотрит `ExecStart` (`--pulse-dir`) и там его
ищет heartbeat (`Environment=POLLER_HOME=/home/ubuntu` в `hft-heartbeat.service`).

Таймер пока **не включаем** — сначала сухой прогон и ручной тик.

---

## Шаг 5. Сухой прогон на хосте — настоящая сеть, ноль записей

```bash
/usr/bin/python3 /opt/hft-collector/current/bin/funding_poller.py --once --dry-run \
    --out-dir /opt/hft-collector/data/funding --pulse-dir /home/ubuntu/funding-data
echo "код выхода: $?"
```

`--dry-run` ходит в сеть по-настоящему, но наружу **не пишет ничего**: подменён весь
исполняющий слой — запись jsonl, состояние, пульс, Telegram.

Что обязано быть в выводе (сверено живым прогоном 2026-08-19T23:04Z против мейннетов):

```
--- DRY RUN, дописал бы hyperliquid-20260819.jsonl.gz: 71533 байт, {"t_local_ns":…,"venue":"hyperliquid","endpoint":"metaAndAssetCtxs","payload":[{"universe":[…
--- DRY RUN, дописал бы hyperliquid-20260819.jsonl.gz: 63508 байт, {…,"endpoint":"predictedFundings","payload":[["0G",[["BinPerp",…
--- DRY RUN, дописал бы lighter-20260819.jsonl.gz: 51628 байт, {…,"endpoint":"funding-rates","payload":{"code":200,"funding_rates":[…
--- DRY RUN, дописал бы aster-20260819.jsonl.gz: 152677 байт, {…,"endpoint":"premiumIndex","payload":[{"symbol":…
--- DRY RUN, дописал бы paradex-20260819.jsonl.gz: 1098367 байт, {…,"endpoint":"markets-summary","payload":{"results":[…
--- DRY RUN, записал бы /home/ubuntu/funding-data/state.json:
{ "schema": 1, "legs": { … пять ног, у каждой "consecutive_failures": 0, "alerted": false } }
--- DRY RUN, пульс: {"t_ns":…,"legs":{"hyperliquid.metaAndAssetCtxs":{"ok":true,"symbols":232,…
<UTC> hyperliquid.metaAndAssetCtxs=232 hyperliquid.predictedFundings=232 lighter.funding-rates=723 aster.premiumIndex=702 paradex.markets-summary=1697
код выхода: 0
```

**Стоп-условия** (не продолжать, разбираться):

* `…=ПРОВАЛ(…)` по любой ноге — площадка недоступна с хоста или форма уехала. Числа
  символов от локальных отличаться **могут** (вселенные растут), `ПРОВАЛ` — нет;
* особое внимание ноге `lighter.funding-rates`: блад-факт «сырой REST Lighter капризен к
  заголовкам» локально **не воспроизвёлся** (5/5 проб), но проверялся не из Токио.
  Этот шаг и есть его перепроверка с хоста;
* ненулевой код выхода — это не сеть (сетевое поллер обрабатывает сам, fail-closed по
  ногам), а баг или отказ записи. Выкатку прекратить.

После прогона каталоги обязаны остаться нетронутыми — наблюдаем эффект, а не намерение:

```bash
ls /opt/hft-collector/data/ | grep funding   # пусто: каталога всё ещё нет
ls -la /home/ubuntu/funding-data             # пусто
```

---

## Шаг 6. Первый настоящий тик, пин вторым тиком, включение таймера

Порядок именно такой: сначала **один ручной тик** — эффекты под контролем, потом таймер.

```bash
systemctl start hft-funding-poller.service
systemctl status hft-funding-poller.service --no-pager   # inactive (dead), result=success
tail -5 /home/ubuntu/funding-data/poller.log
```

Проверка **эффектов** (файлы, а не намерения):

```bash
ls -la /opt/hft-collector/data/funding/    # 4 файла <venue>-YYYYMMDD.jsonl.gz
for f in /opt/hft-collector/data/funding/*.jsonl.gz; do
    echo "$f: $(zcat "$f" | wc -l) строк"; done   # hyperliquid=2 (две ноги), остальные по 1
ls -la /home/ubuntu/funding-data/          # state.json + pulse-YYYYMMDD.gz + poller.log
zcat /home/ubuntu/funding-data/pulse-*.gz | tail -1
```

Пин append-only рядом (второй тик обязан **дописать**, а не переписать):

```bash
systemctl start hft-funding-poller.service
for f in /opt/hft-collector/data/funding/*.jsonl.gz; do
    echo "$f: $(zcat "$f" | wc -l) строк"; done   # hyperliquid=4, остальные по 2
zcat /home/ubuntu/funding-data/pulse-*.gz | wc -l  # 2
```

Если счётчик строк не вырос — ряд не пишется, дальше не идти.

Включение таймера:

```bash
systemctl enable --now hft-funding-poller.timer
systemctl list-timers hft-funding-poller.timer --no-pager
```

**Урок params-поллера, проверка обязательна:** колонка `NEXT` должна показывать время в
пределах ~5 минут, **не `n/a`**. `n/a` — ровно тот отказ, что стоил 23 часов молчания;
юнит специально на `OnCalendar=*:0/5`, но проверить надо глазами.

Через ~11 минут — эффект таймера, а не намерение:

```bash
for f in /opt/hft-collector/data/funding/*.jsonl.gz; do
    echo "$f: $(zcat "$f" | wc -l) строк"; done   # выросло ещё на 2 тика
```

---

## Шаг 7. Пульс виден сторожу

```bash
POLLER_HOME=/home/ubuntu /opt/hft-collector/current/bin/heartbeat.sh --dry-run
```

В выводе `funding` обязан стоять среди **живых**. Проверено на day0: тот же прогон при
отсутствующем каталоге даёт в тревоге строку `STALE funding nodir` — «не задеплоен» не
равно «здоров», и это разные строки.

Порог — 20 минут (3 пропущенных тика × 5 мин + 5 запас), не общие 90: частичные провалы
(одна площадка из четырёх) алертит сам поллер по ногам, а тишина пульса значит «все ноги
мертвы либо мёртв таймер» — это надо видеть за 20 минут.

Честная задержка обнаружения полного отказа: 20 мин порога + сторож раз в 30 мин
(джиттер 90 с) + рейт-лимит сообщений 1 ч ⇒ **до ~50 минут** от последней записи до
сообщения. Это выведено из каденций, а не измерено на живом инциденте.

Сутки спустя стоит убедиться, что суточный дайджест (9:00 UTC) перечисляет `funding`
среди живых источников.

---

## Шаг 8. Что смотреть в первые сутки

```bash
tail -20 /home/ubuntu/funding-data/poller.log        # каждые 5 мин строка с пятью ногами
zcat /home/ubuntu/funding-data/pulse-*.gz | tail -3  # consecutive_failures по ногам
du -sh /opt/hft-collector/data/funding/              # ~69 МБ к концу первых суток
```

* `🔴 … нога <имя> не пишется, 6 провалов подряд` в TG — площадка лежит полчаса;
  остальные ноги при этом пишутся, ряд **не** теряется целиком. Повтор сообщения — раз в
  ~6 часов (72 тика).
* `🟢 … нога <имя> восстановилась` — конец аутэджа, счётчик обнулён.
* Смена суток UTC: старые файлы закрываются сами (день берётся из метки **каждой** ноги),
  новые появляются на первом тике после полуночи. Тик, попавший на полночь, штатно
  разложит свои ноги по двум суточным файлам.

**Проверка целостности после любого нештатного завершения** (ребут, OOM, `systemctl
stop` посреди тика) — обязательна, и вот почему. Обрыв на середине дописываемого
gzip-члена уносит **не последнюю строку, а весь остаток суток этой площадки**: измерено
— читатель отдаёт члены до обрыва и падает `zlib.error: Error -3`, `zcat` печатает то же
и выходит с `rc=1`; все последующие тики этого venue до полуночи не читаются никогда
(до 288 записей). Наибольший риск у paradex — у него самый длинный член (~199 КБ).

```bash
for f in /opt/hft-collector/data/funding/*.jsonl.gz; do
    zcat "$f" > /dev/null 2>&1 || echo "БИТЫЙ ХВОСТ: $f"; done
```

Нашёлся битый — забрать файл на машину оператора (шаг 9) **до полуночи**, спасённые
строки лежат в начале; на хосте файл убрать в сторону (`mv "$f" "$f.broken"`), чтобы
следующие тики писали в новый, читаемый файл. Останавливать поллер ради этого не надо.
Плановые ребуты хоста делать так: `systemctl stop hft-funding-poller.timer`, дождаться
`systemctl is-active hft-funding-poller.service` = `inactive`, и только потом ребут.

---

## Шаг 9. Забор данных — пока РУЧНОЙ (см. блок «⛔» выше)

Штатный офлоад файлы не увидит. Проверить это эффектом на первом же прогоне:

```bash
# с машины оператора, безопасно:
collector/deploy/offload.sh --host "$TOKYO" --target ~/marketdata --dry-run | grep -A2 funding
# ожидается: "  unrecognised names: N, left alone" и "funding: nothing to move"
```

Ручной забор (данные + пульс рядом, чтобы дыры в ряде можно было объяснить):

```bash
mkdir -p ~/marketdata/funding ~/marketdata/funding/_pulse
rsync -av "$TOKYO":/opt/hft-collector/data/funding/ ~/marketdata/funding/
rsync -av "$TOKYO":/home/ubuntu/funding-data/pulse-*.gz ~/marketdata/funding/_pulse/
```

Удаление с хоста — **отдельным решением и только после сверки**, автоматизировать здесь
нечего (офлоад именно этим и занимается — верификацией перед удалением):

```bash
# сверить размеры/суммы обеих сторон по конкретному дню, и только тогда:
ssh "$TOKYO" 'ls -la /opt/hft-collector/data/funding/'
# ssh "$TOKYO" 'rm /opt/hft-collector/data/funding/<venue>-<YYYYMMDD>.jsonl.gz'   # вчерашние и старше
```

Файл сегодняшних суток **не трогать** — в него ещё дописывают.

Периодичность: раз в сутки-двое, пока долг из блока «⛔» не закрыт. 70 МБ/сут терпят
неделю без внимания, месяц — уже 2 ГБ на волюме, который сторожит heartbeat.

---

## Откат

Поллер и таймер:

```bash
systemctl disable --now hft-funding-poller.timer
# данные и пульс остаются лежать; юниты можно удалить из /etc/systemd/system при желании
```

Релиз целиком (штатный путь, `.previous` записан на шаге 3). **Порядок именно такой** —
сначала гасим таймер, потом откатываем:

```bash
systemctl disable --now hft-funding-poller.timer     # ОБЯЗАТЕЛЬНО ПЕРВЫМ
# и тем же — про day0, если он включён:
# systemctl disable --now hft-day0-poller.timer

/opt/hft-collector/current/bin/rollback.sh --list
/opt/hft-collector/current/bin/rollback.sh           # вернётся на .previous
```

Почему первым: `ExecStart` указывает внутрь свопаемого дерева, а `funding_poller.py`
появляется **только** в новом релизе (шаг 2 кладёт его в `$NEW`, копией которого
`$SRC`/`.previous` не является). После отката файла по пути нет, юнит падает с
`203/EXEC`, `OnFailure` шлёт сообщение — и таймер повторяет это **каждые 5 минут**:
288 ложных алертов в сутки, топящих настоящие тревоги. Проверить одной строкой:

```bash
ls "$(cat /opt/hft-collector/.previous)"/bin/funding_poller.py   # ожидается ENOENT
```

Откат релиза возвращает и старый `heartbeat.sh` (без строк `funding:` и `day0:`) — то
есть тревога по пульсу исчезнет вместе с ним. Если поллер при этом оставлен включённым,
его тишина станет ненаблюдаемой сторожем; останется собственный алерт по ногам (после 6
провалов) и `OnFailure`.

---

## Оговорки, которые надо знать до выкатки

1. **Ряд снимается, но сам не доезжает** (панель, blocker; блок «⛔» выше). Деплоим
   осознанно, забираем руками шагом 9, долг записан.
2. **Долг #106 расширен**: `build-release.sh`/`install.sh` не знают ни про day0-, ни про
   funding-поллер. Следующий штатный релиз, собранный сборщиком, молча снимет **оба**.
3. **Обрыв gzip-члена стоит остатка суток этой площадки, а не одной строки** (панель,
   major; измерено — см. шаг 8). SPEC §5 в редакции `55a3e4d` это занижает; читалка на
   стороне потребителя обязана уметь честно сказать «файл оборван», а не молча отдать
   префикс.
4. **Пульс офлоадом не забирается** (панель, minor) — на машине оператора дыра в ряде
   неинтерпретируема без него; поэтому шаг 9 тащит `pulse-*.gz` рядом.
5. **`RealHost.send_telegram` ловит только `(OSError, ValueError)`** (панель, major) —
   тот же класс бага, что `03f12ac` починил в `_run_leg`: `IncompleteRead` из
   `resp.read()` вылетает наружу. Ветка срабатывает ровно во время инцидента и до записи
   состояния и пульса, то есть устойчиво рвущийся путь к `api.telegram.org` даёт
   «данные пишутся, пульс не пишется, сторож кричит про мёртвый поллер». Не починено в
   `55a3e4d`; лечится зеркалом `03f12ac` (тотальный `except Exception` + пин). Паттерн
   унаследован от day0 — там тот же долг.
6. **Доставка алерта не проверяется**: одна попытка, результат отбрасывается, флаг
   `alerted` ставится по факту наступления, а не доставки. Транзиентно потерянный красный
   алерт не повторится до ~6 часов, а зелёное «восстановилась» может прийти за красное,
   которого никто не видел. Осознанный компромисс (шторм при ненастроенном TG дороже),
   но при разборе инцидента источник истины — `poller.log` и пульс, а не Telegram.
7. **Ретрая внутри тика нет намеренно** (тот же выбор, что у day0): цена провала — 5
   минут ряда, серия из 6 алертит сама. Не «чинить» добавлением ретраев.
8. **paradex пишет и опционы** — 1635 записей из 1697, ~199 КБ gz/тик, 83% объёма
   поллера. Это контракт «ничего не выбрасывать»: фильтрация на захвате сделала бы запись
   неспособной ответить на будущие вопросы. Если диск начнёт болеть — решение принимает
   оператор, а не тихий фильтр в коде.
9. **У Lighter нет метки времени venue вообще** — строка датируется только
   `t_local_ns`. Потребителю (#40) это надо знать: ставка Lighter датируется моментом
   снятия, не моментом площадки.
10. **Числа символов в шагах 5–6 — снимок 2026-08-19/20** (232/232/723/702/1697).
    Вселенные растут; критерий здоровья — отсутствие `ПРОВАЛ`, а не равенство числам.
11. **Binance premiumIndex сюда не входит намеренно** — его пишет Rust-коллектор
    инстанса `binancefuturesum` (свой тик, только свои символы). В коде это пиннуто
    (`no binance.com in LEGS`). У aster Rust-бэкенд тоже есть, но venue-wide история
    фандинга aster появляется только с этим поллером.
12. **`payload` проходит `json.loads` → `json.dumps`** — порядок ключей и значения
    сохраняются, но это **не** побайтная копия ответа (в отличие от Rust-коллектора с
    `RawValue`). Частный случай: `1e400` в ответе площадки станет голым токеном
    `Infinity`, который строгие читатели (Rust `serde_json`, Go) не примут; ветки
    fail-closed на это сегодня нет.
