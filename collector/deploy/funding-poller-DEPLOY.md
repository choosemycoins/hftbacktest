# Выкатка funding-поллера на токийский хост сбора (#88)

Ревизия артефактов: **`964d532`** (ветка `fix/depth-freshness-83`, не пушнута — файлы
едут на хост копированием, не `git pull`).

Тег релиза, который получится: **`fundingpoller-964d532`**.

> ⚠ Ревизия сдвинулась с `55a3e4d` (а до того — с `8fb6430`). Если уже скопированы файлы
> в `/tmp/funding-drop` — **доставить заново**, sha256-сверка на шаге 1 это поймает.
> Что приехало (все четыре находки панели закрыты кодом, красным-вперёд):
>
> * `b7bfe75` — `send_telegram` тотален (major): перечисление `(OSError, ValueError)`
>   пропускало `http.client.IncompleteRead` наружу РОВНО во время инцидента и до записи
>   состояния/пульса — устойчивый обрыв пути к Telegram давал «данные пишутся, state
>   замер, сторож кричит про мёртвый поллер».
> * `8a924eb` — **имя файла данных сменилось** (blocker): было `<venue>-YYYYMMDD.jsonl.gz`,
>   стало **`funding_<venue>_YYYYMMDD.gz`** — форма, которую признаёт `day_of` из
>   `offload.sh`. Содержимое прежнее (jsonl.gz). Блок «⛔» ниже переписан: ряд теперь
>   уезжает штатным офлоадом.
> * `99eeea8` — тик ложится на диск одним ПОЛНЫМ gzip-членом (один `os.write` + `fsync`):
>   обрыв процесса посреди дописывания больше не делает нечитаемым остаток суток
>   площадки (major; шаг 8 переписан).
> * `964d532` — юниты обоих поллеров несут `ConditionPathExists` на путь скрипта:
>   откат релиза больше не превращает таймер в шторм 203/EXEC-алертов (major; раздел
>   «Откат» обновлён).
>
> Тестов у поллера 53 (было 40); `03f12ac`/`55a3e4d` (тотальный провал ноги, пины
> UTC-суток и alert.env) — в составе, как и раньше.

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

## ✅ Блокер доставки ЗАКРЫТ в `8a924eb`: имя файла — контракт офлоада

Прежняя редакция несла здесь блокер: `offload.sh` разбирает имена функцией `day_of`,
а поллер писал `<venue>-YYYYMMDD.jsonl.gz` — не подходило ни под одну из трёх
признаваемых форм, и ряд молча копился на хосте вечно («unrecognised names: N, left
alone»). Теперь имя — **`funding_<venue>_YYYYMMDD.gz`** (`data_file_name` в
`funding_poller.py`), и держит его пин
`test_every_data_file_name_is_recognised_and_dated_by_offload`, который гоняет
**настоящую** `day_of`, вырезанную из `offload.sh`: разъедутся — упадёт тест, а не
доставка. Сегодняшний файл офлоад не трогает сам (day >= UTC-сегодня хоста), так что
файлу, в который ещё дописывают, ничего не грозит.

Что осталось руками (шаг 9, теперь маленький):

1. **Пульс** (`/home/ubuntu/funding-data`) офлоад не забирает — он ходит только по
   `/opt/hft-collector/data`. Без пульса рядом с данными дыра в ряде на машине оператора
   неинтерпретируема: аутэдж площадки, мёртвый таймер и забитый диск выглядят одинаково.
2. **Эффект первого офлоада проверить глазами** (файлы `funding_*` появились у
   оператора и исчезли с хоста за вчерашние сутки) — наблюдать эффект, не намерение.
3. Если на хосте успели появиться файлы под СТАРЫМ именем (деплой прежней ревизии) —
   переименовать их руками: `mv <venue>-<день>.jsonl.gz funding_<venue>_<день>.gz`;
   уже записанные под невидимым именем сами видимыми не станут.

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

Финализированные сутки уезжают и удаляются штатным офлоадом (блок «✅» выше), поэтому
на хосте живёт максимум двое суток ряда (~140 МБ):

```bash
df -BG /opt/hft-collector/data     # warn heartbeat: 15 ГБ, crit: 8 ГБ
sudo du -sm /opt/hft-collector/data/* | sort -n
```

Если до warn остаётся меньше 4 ГБ, сначала офлоад маркетдаты, потом выкатка.

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

**0.4 Сверка контракта доставки — на машине оператора, до выезда** (пин
`recognised_and_dated_by_offload` держит это в тестах; здесь — сверка руками той же
функции):

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
day_of funding_hyperliquid_20260820.gz'
```

Ожидается `MATCH 20260820`. `NO-MATCH` — стоп: либо доставляешь старую ревизию поллера,
либо `day_of` в `offload.sh` уехала; в обоих случаях сначала зелёный
`pytest collector/tools/test_funding_poller.py`, потом выкатка.

---

## Шаг 1. Доставка файлов (с машины оператора)

```bash
cd /home/andrew/RustroverProjects/hftbacktest
git rev-parse --short HEAD             # 964d532 или новее (тогда правь TAG ниже; артефакты не менялись после 964d532)
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

TAG=fundingpoller-964d532
SRC="$(readlink -f /opt/hft-collector/current)"
NEW="/opt/hft-collector/releases/${TAG}"

test ! -e "$NEW" || { echo "ОСТАНОВ: ${NEW} уже существует"; exit 1; }

cp -a "$SRC" "$NEW"                       # -a: права/владельцы/времена как были

install -m 755 -o root -g root /tmp/funding-drop/funding_poller.py "$NEW/bin/funding_poller.py"
install -m 755 -o root -g root /tmp/funding-drop/heartbeat.sh      "$NEW/bin/heartbeat.sh"
install -m 644 -o root -g root /tmp/funding-drop/hft-funding-poller.service "$NEW/etc/"
install -m 644 -o root -g root /tmp/funding-drop/hft-funding-poller.timer   "$NEW/etc/"

# происхождение — в манифест, чтобы `rollback.sh --list` не врал о содержимом
printf 'funding_poller_from=964d532\nfunding_poller_added_at=%s\n' "$(date -u +%FT%TZ)" >> "$NEW/RELEASE"
```

Проверки до свопа (ни одна ничего не меняет):

```bash
/usr/bin/python3 -m py_compile "$NEW/bin/funding_poller.py" && echo "поллер компилируется"
bash -n "$NEW/bin/heartbeat.sh" && echo "heartbeat синтаксически цел"
grep -n 'funding:' "$NEW/bin/heartbeat.sh"    # строка funding:…/funding-data:…20 на месте
grep -n 'day0:'    "$NEW/bin/heartbeat.sh"    # строка day0 НЕ должна пропасть
diff -r "$SRC" "$NEW" | head                  # ровно четыре новых/изменённых файла + RELEASE
```

⚠ На хосте после выкатки day0 живёт heartbeat со строкой `day0:`. Наш файл из `964d532`
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
Прогнан локально на `964d532` — по нашим файлам ноль замечаний; в выводе будет шум по
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
--- DRY RUN, дописал бы funding_hyperliquid_20260819.gz: 71533 байт, {"t_local_ns":…,"venue":"hyperliquid","endpoint":"metaAndAssetCtxs","payload":[{"universe":[…
--- DRY RUN, дописал бы funding_hyperliquid_20260819.gz: 63508 байт, {…,"endpoint":"predictedFundings","payload":[["0G",[["BinPerp",…
--- DRY RUN, дописал бы funding_lighter_20260819.gz: 51628 байт, {…,"endpoint":"funding-rates","payload":{"code":200,"funding_rates":[…
--- DRY RUN, дописал бы funding_aster_20260819.gz: 152677 байт, {…,"endpoint":"premiumIndex","payload":[{"symbol":…
--- DRY RUN, дописал бы funding_paradex_20260819.gz: 1098367 байт, {…,"endpoint":"markets-summary","payload":{"results":[…
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
ls -la /opt/hft-collector/data/funding/    # 4 файла funding_<venue>_YYYYMMDD.gz
for f in /opt/hft-collector/data/funding/funding_*.gz; do
    echo "$f: $(zcat "$f" | wc -l) строк"; done   # hyperliquid=2 (две ноги), остальные по 1
ls -la /home/ubuntu/funding-data/          # state.json + pulse-YYYYMMDD.gz + poller.log
zcat /home/ubuntu/funding-data/pulse-*.gz | tail -1
```

Пин append-only рядом (второй тик обязан **дописать**, а не переписать):

```bash
systemctl start hft-funding-poller.service
for f in /opt/hft-collector/data/funding/funding_*.gz; do
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
for f in /opt/hft-collector/data/funding/funding_*.gz; do
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

**Целостность при нештатном завершении — закрыта конструкцией в `99eeea8`, но проверка
после инцидента остаётся.** Каждый тик теперь ложится на диск одним ПОЛНЫМ gzip-членом
за один `os.write` + `fsync` (`_append_gzip_member`): убийство процесса посреди тика
(ребут, OOM, `systemctl stop`) больше **не может** оставить усечённый член — окно
разрыва сжато с серии буферизованных write до одного syscall. Прежняя механика
(буферизованный `gzip.open`) оставляла рваный член, и весь остаток суток площадки
становился нечитаемым — измерено: читатель отдаёт члены до обрыва и падает
`zlib.error: Error -3`, `zcat` выходит с `rc=1`; до 288 записей.

Остаточный риск — разрыв НИЖЕ уровня syscall (отказ питания посреди записи страниц ФС).
На этот случай каждый член самодостаточен: читалка с ресинком по gzip-магии `1f 8b 08`
достаёт всё, что записано **после** обрыва (пин
`ticks_after_a_torn_tail_are_recoverable_by_gzip_magic_resync`). Наивные `zcat`/`gzip`
на таком файле по-прежнему останавливаются на рваном члене — поэтому после любого
нештатного завершения хоста (не процесса):

```bash
for f in /opt/hft-collector/data/funding/funding_*.gz; do
    zcat "$f" > /dev/null 2>&1 || echo "БИТЫЙ ХВОСТ: $f"; done
```

Нашёлся битый — забрать файл на машину оператора **до полуночи** (после неё офлоад
попробует его увезти, а `gzip -t` на верификации его завернёт); строки после обрыва
спасать ресинком по магии. На хосте файл убрать в сторону (`mv "$f" "$f.broken"` —
имя с суффиксом `day_of` не признаёт, офлоад его не тронет), чтобы следующие тики
писали в новый, читаемый насквозь файл. Останавливать поллер ради этого не надо.
Плановые ребуты хоста делать так: `systemctl stop hft-funding-poller.timer`, дождаться
`systemctl is-active hft-funding-poller.service` = `inactive`, и только потом ребут.

---

## Шаг 9. Забор данных: офлоад ШТАТНЫЙ, руками остаётся пульс

Данные уезжают обычным офлоадом (имя файла — его контракт, блок «✅» выше). На первом
прогоне после выкатки проверить **эффект**, а не намерение:

```bash
# с машины оператора, безопасно:
collector/deploy/offload.sh --host "$TOKYO" --target ~/marketdata --dry-run | grep -A3 '=== funding'
# ожидается: "finalized: 4 file(s)" за вчерашние сутки (сегодняшние — "left alone");
# "unrecognised names" по funding-файлам быть НЕ должно
```

После первого настоящего (не dry-run) офлоада: файлы `funding_*_<вчера>.gz` появились в
`~/marketdata/funding/` и исчезли с хоста. Если вместо этого «unrecognised names» —
на хосте старая ревизия поллера либо файлы под старым именем (п. 3 блока «✅»:
переименовать руками).

Пульс офлоад не забирает (он ходит только по `/opt/hft-collector/data`) — его тащить
рядом с данными, чтобы дыры в ряде можно было объяснить:

```bash
mkdir -p ~/marketdata/funding/_pulse
rsync -av "$TOKYO":/home/ubuntu/funding-data/pulse-*.gz ~/marketdata/funding/_pulse/
```

Периодичность пульса — как у офлоада (раз в сутки); он килобайтный, хост от него не
болеет, поэтому просрочка не аварийна — но дыру без него не объяснить.

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
`$SRC`/`.previous` не является). После отката файла по пути нет. С юнитом из `964d532`
это **тихий пропуск**, не авария: `ConditionPathExists` в `[Unit]` — нет файла, юнит
скипается (не failed), `OnFailure` молчит, а тишину записи через 20 минут поднимает
сторож по пульсу — одна честная тревога вместо шторма. Без этой строки (старый юнит)
юнит падал `203/EXEC`, `OnFailure` слал сообщение, и таймер повторял это **каждые
5 минут** — 288 ложных алертов в сутки, топящих настоящие тревоги; поэтому порядок
«таймер первым» остаётся обязательным: он же избавляет от слепого окна, где скип тих,
а откат старого heartbeat уже снял и строку сторожа. Проверить одной строкой:

```bash
ls "$(cat /opt/hft-collector/.previous)"/bin/funding_poller.py   # ожидается ENOENT
```

Откат релиза возвращает и старый `heartbeat.sh` (без строк `funding:` и `day0:`) — то
есть тревога по пульсу исчезнет вместе с ним. Если поллер при этом оставлен включённым,
его тишина станет ненаблюдаемой сторожем; останется собственный алерт по ногам (после 6
провалов) и `OnFailure`.

---

## Оговорки, которые надо знать до выкатки

1. **Доставка ряда — ЗАКРЫТО в `8a924eb`** (был blocker; блок «✅» выше). Имя файла —
   контракт офлоада, держится пином на настоящей `day_of`. Руками остаётся пульс и
   разовая проверка эффекта первого офлоада (шаг 9).
2. **Долг #106 расширен**: `build-release.sh`/`install.sh` не знают ни про day0-, ни про
   funding-поллер. Следующий штатный релиз, собранный сборщиком, молча снимет **оба**.
   `ConditionPathExists` в юнитах превращает это из шторма алертов в тихий пропуск —
   ловить его будет сторож по пульсу, но долг это не закрывает: поллеры со снятого
   релиза не тикают.
3. **Обрыв gzip-члена — ЗАКРЫТО в `99eeea8`** (был major). Тик пишется одним полным
   членом за один `os.write` + `fsync`; остаточный риск — только разрыв ниже уровня
   syscall (питание), и на него члены самодостаточны — ресинк по магии достаёт всё
   после обрыва (см. шаг 8). Читалка потребителя всё равно обязана честно сказать
   «файл оборван», а не молча отдать префикс: наивный `gzip.open` останавливается на
   рваном члене.
4. **Пульс офлоадом не забирается** (панель, minor) — на машине оператора дыра в ряде
   неинтерпретируема без него; поэтому шаг 9 тащит `pulse-*.gz` рядом.
5. **`send_telegram` — ЗАКРЫТО в `b7bfe75`** (был major): тотальный `except Exception`,
   зеркало `03f12ac`; канал тревоги больше не роняет тик до записи состояния и пульса.
   Пин — `send_telegram_reports_false_on_any_transport_failure`. У day0 тот же паттерн
   остаётся долгом (TODO в `day0_poller.py`).
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
