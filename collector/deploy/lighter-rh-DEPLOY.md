# Деплой `lighter-rh` и снимков очков на хост Токио

**Статус: подготовлено, НЕ задеплоено. Выполнять только после ревью оператора.**
Ничего здесь не торгует, не депонирует и не логинится: коллектор читает публичный
WS/REST `api.rh.lighter.xyz`, поллер — публичные `leaderboard` и `pnlLeaderboard`.

## Что едет

| Что | Где в релизе | Эффект на хосте |
|---|---|---|
| бинарь `collector` с бэкендом `lighter-rh` | `bin/collector` | новый инстанс `hft-collector@lighter-rh`; работающие инстансы **не перезапускаются** (держат старый inode до своего следующего рестарта) |
| `lighter-rh.env.example` | `etc/` | копируется в `/opt/hft-collector/etc/lighter-rh.env` |
| `quality_report.py` с алиасом `lighter-rh → lighter` | `tools/` | гейт `@all` понимает новый каталог (без алиаса он падал бы exit 2 на неизвестной бирже) |
| `lighter_rh_points.py` + `hft-lighter-rh-points.{service,timer}` | `tools/`, `etc/` | ежедневный снимок очков в `/opt/hft-collector/data/lighter-rh-points/points_<YYYYMMDD>.gz` |

## Из какой ветки собирать

**Из `release/collector-20261008-lighter-rh`, не из `feat/collector-lighter-rh` и не из `develop`.**
Она = `release/collector-20260909` (@ `ee050f7`, дерево за `releases/day0rotation-ee050f7`)
+ мердж `feat/collector-lighter-rh`. В `develop` нет поллеров, heartbeat и
admission-логики подписок Lighter, которые сейчас в проде; релиз из `develop` повторил
бы дефект 04.09 (`queue-bump`: из `current/bin` пропали `day0_poller.py` и `heartbeat.sh`).
На мердже: `cargo test -p collector --bins` 248 passed, `pytest collector/tools` 846 passed.

Предусловие, проверить первым: на хосте `current → releases/day0rotation-ee050f7`.
Если там другое — **стоп**: значит, после 09.09 был ещё релиз, и базу ветки надо
перенести на его коммит.

## 0. Пуш (оператор, из своей сессии — у Claude пуш заблокирован)

```bash
cd ~/RustroverProjects/hftbacktest
git push origin feat/collector-lighter-rh release/collector-20261008-lighter-rh
```

## 1. Сборка на машине оператора (Linux x86_64)

```bash
cd ~/RustroverProjects/hftbacktest
git worktree add /home/storage/hft-scratch/collector-release-20261008 release/collector-20261008-lighter-rh
cd /home/storage/hft-scratch/collector-release-20261008
git status --short                     # пусто: build-release.sh пишет dirty-флаг в RELEASE
cargo test -p collector --bins         # ожидается 248 passed
~/RustroverProjects/hftbacktest/.venv/bin/pytest -q collector/tools/   # 846 passed
TAG=lighterrh-$(git rev-parse --short HEAD)
collector/deploy/build-release.sh "$TAG"
ls -l /tmp/hft-collector-release-$TAG.tar.gz
tar -tzf /tmp/hft-collector-release-$TAG.tar.gz | grep -E 'lighter_rh_points|lighter-rh|day0_poller|heartbeat'   # все четыре вида должны быть
scp /tmp/hft-collector-release-$TAG.tar.gz hft-collector-tokyo:/tmp/
echo "$TAG"
```

## 2. Предполётная проверка на хосте (только чтение)

```bash
ssh hft-collector-tokyo
readlink /opt/hft-collector/current            # ожидается releases/day0rotation-ee050f7
sudo cat /opt/hft-collector/.previous           # файл mode 600, путь прежнего релиза
systemctl list-units 'hft-collector@*' --no-pager
ls -l /opt/hft-collector/etc/                   # владелец/права существующих *.env — повторить для нового
test -e /opt/hft-collector/etc/lighter-rh.env && echo "УЖЕ ЕСТЬ — стоп"
df -h /opt/hft-collector/data
```

## 3. Установка релиза без рестарта работающих инстансов

`install.sh` перезапускает **все** работающие инстансы по одному (дыра в записи у
каждого). Для добавления одного инстанса — ручной атомарный своп, как 01.08 и
04.08. Права обязательно чинить **до** свопа (урок Extended 04.08: `tar` сохраняет
uid 1000 и mode 700 → `203/EXEC` у каждого инстанса при следующем рестарте).

```bash
TAG=lighterrh-XXXXXXX                           # из шага 1
R=/opt/hft-collector/releases/$TAG
sudo mkdir "$R"
sudo tar -xzf /tmp/hft-collector-release-$TAG.tar.gz -C "$R"   # тарбол собран из "." — без верхнего каталога
ls "$R"                                         # bin etc tools README.md RELEASE
sudo chown -R root:root "$R"
sudo chmod -R u+rwX,go+rX,go-w "$R"
sudo chmod 755 "$R"/bin/*
cat "$R/RELEASE"; "$R/bin/collector" --version  # коммит = HEAD ветки релиза, dirty=clean
ls "$R/bin"                                     # day0_poller.py heartbeat.sh ... на месте
diff /etc/systemd/system/hft-collector@.service "$R/etc/hft-collector@.service" && echo "шаблон не менялся"

# своп как в install.sh: .previous — ФАЙЛ (mode 600) с прежней целью current,
# пишется до свопа через rename; потом атомарная перестановка симлинка
PREV=$(readlink /opt/hft-collector/current)
echo "$PREV"                                    # должен быть путь под /opt/hft-collector/releases/
printf '%s\n' "$PREV" | sudo tee /opt/hft-collector/.previous.new >/dev/null
sudo chmod 600 /opt/hft-collector/.previous.new
sudo mv -f /opt/hft-collector/.previous.new /opt/hft-collector/.previous
sudo ln -snf "$R" /opt/hft-collector/current.new
sudo mv -Tf /opt/hft-collector/current.new /opt/hft-collector/current
readlink /opt/hft-collector/current
```

Если `diff` шаблона юнита показал разницу — `sudo cp "$R/etc/hft-collector@.service" /etc/systemd/system/ && sudo systemctl daemon-reload`
(работающие инстансы это не перезапускает).

## 4. Инстанс `lighter-rh`

```bash
sudo install -m 644 -o root -g root /opt/hft-collector/current/etc/lighter-rh.env.example \
     /opt/hft-collector/etc/lighter-rh.env   # права — как у соседних *.env из шага 2
sudo systemctl enable --now hft-collector@lighter-rh
systemctl status hft-collector@lighter-rh --no-pager
journalctl -u hft-collector@lighter-rh -n 40 --no-pager   # "the venue is reachable", subscriptions=88 markets=22
```

Проверка через 2–3 минуты (**не по размеру .gz** — у тонких символов файл
минутами стоит на 10 байтах в буфере дефлейта; смотреть `_meta`):

```bash
D=/opt/hft-collector/data/lighter-rh
grep -m1 session_start $D/_meta_lighter-rh_*.jsonl | grep -o '"lighter_endpoints":{[^}]*}'
#   → {"rest":"https://api.rh.lighter.xyz","ws":"wss://api.rh.lighter.xyz/stream"}
grep liveness $D/_meta_lighter-rh_$(date -u +%Y%m%d).jsonl | tail -1     # max age ≪ 300
grep -c sequence_gap $D/_meta_lighter-rh_$(date -u +%Y%m%d).jsonl        # ожидается 0
# соседний mainnet-инстанс не пострадал (общий IP, лимит 200/мин может быть per-IP):
grep -E 'sequence_gap|disconnected' /opt/hft-collector/data/lighter/_meta_lighter_$(date -u +%Y%m%d).jsonl | tail
```

## 5. Снимки очков

```bash
sudo cp /opt/hft-collector/current/etc/hft-lighter-rh-points.service /etc/systemd/system/
sudo cp /opt/hft-collector/current/etc/hft-lighter-rh-points.timer   /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start hft-lighter-rh-points.service       # один прогон руками, ~20 с
journalctl -u hft-lighter-rh-points -n 5 --no-pager      # "top10=… accounts=3554/3554 … complete=True"
ls -l /opt/hft-collector/data/lighter-rh-points/
sudo systemctl enable --now hft-lighter-rh-points.timer
systemctl list-timers hft-lighter-rh-points.timer --no-pager
```

Офлоад: `offload.sh` сам подхватит оба каталога (`lighter-rh`, `lighter-rh-points`) —
он берёт все каталоги под `/opt/hft-collector/data`, а имена файлов в формате
`<name>_<YYYYMMDD>.gz`. Гейт `@all` возьмёт `lighter-rh` по `etc/lighter-rh.env`;
у `lighter-rh-points` env нет, гейт его не трогает.

## 6. Откат

```bash
sudo systemctl disable --now hft-lighter-rh-points.timer hft-collector@lighter-rh
# rollback.sh ПЕРЕЗАПУСКАЕТ все инстансы; раз они и так на старом inode,
# достаточно ручного свопа обратно:
sudo ln -snf "$PREV" /opt/hft-collector/current.new
sudo mv -Tf /opt/hft-collector/current.new /opt/hft-collector/current
```

Работающие инстансы ручной откат не задевает: они так и не перезапускались. Данные
в `data/lighter-rh*` не удаляются. Если `.previous` после этого указывает на
откатываемый релиз — не страшно, `rollback.sh --list` покажет оба.

## Объём и диск

Локальный прогон 2026-10-08 09:55–10:04 UTC, ровно этот набор из 22 символов, US-сессия
закрыта: 7.9 МБ gzip / 65.6 МБ сырого за 570 с → **~1.2 ГБ/сут gzip** (~10 ГБ/сут
сырого). Крупнейшие BTC ~113, ETH ~97, SPY ~89 МБ/сут. Закладывать 2 ГБ/сут на
US-часы. 0 sequence_gap, liveness max age 1 с. При офлоаде раз в сутки том 54 ГБ
это не нагружает; локальный архив на bigbox (`/home/storage`) — +~0.5–0.7 ТБ/год. Снимок очков — ~0.4 МБ сырого JSON на прогон,
~0.1 МБ в gzip, 1–2 прогона в сутки.

## Что знать после деплоя

- Существующие инстансы остаются на старом бинаре до своего следующего рестарта;
  после него `lighter` (mainnet) поедет на код, где эндпоинты передаются явно, а не
  через `LazyLock` — поведение то же (переопределение через `LIGHTER_WS_URL` работает
  только для `lighter`).
- Лимит 200 сообщений/мин у Lighter не измерен: per-connection или per-IP. Mainnet-инстанс
  уже тратит 100 подписок, RH — 88, при реконнекте обоих в одну минуту под per-IP-чтением
  это 188 + keepalive. Признак проблемы — молчание сокета (stall-watchdog / liveness), не
  ошибка. Если увидите — первым снимать 3 тонких символа RH.
- Ловушка liveness: для `lighter-rh` порог 300 с (дефолт для незамеренной площадки).
  Каденс `market_stats` акционных перпов вне часов US не мерян.
