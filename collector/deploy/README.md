# Деплой коллектора: полный путь и справочник конфигов

Эта дока — воспроизводимый путь от пустого хоста до пишущего, алертящего и
самопроверяющегося коллектора. Формат данных, наборы стримов и конвертация —
в [`../README.md`](../README.md); здесь только эксплуатация.

Проверено на: Ubuntu 24.04+/x86_64 и arm64, systemd, EC2 с отдельным EBS-томом
под данные. Паттерн версионированных релизов взят из `myhft/deploy/`.

## Компоненты

| Файл | Назначение |
|---|---|
| `bootstrap.sh` | одноразово на хост: пользователь `hftcollector`, дерево `/opt/hft-collector` |
| `build-release.sh` / `cross-build-linux.sh` | сборка релиз-тарбола (локально / кросс с macOS через zig) |
| `install.sh` | установка тарбола, атомарный своп симлинка `current`, последовательный рестарт инстансов |
| `rollback.sh` | откат на предыдущий релиз одним свопом |
| `hft-collector@.service` | шаблонный юнит; один инстанс = один env-файл = одна биржа |
| `collector-run.sh` | обёртка ExecStart: env → флаги CLI |
| `hft-collector-alert@.service` + `alert.sh` | Telegram-алерт по `OnFailure` любого юнита |
| `hft-collector-gate@.service` + `.timer` + `gate-run.sh` | ежедневная валидация вчерашних записей (00:35 UTC) + dead-man-пинг |
| `offload.sh` | вывоз финализированных дней с хоста (запускается С операторской машины) |
| `instance.env.example`, `alert.env.example` | шаблоны двух операторских конфигов |

## Новый хост: пошагово

```bash
# 0. ДАННЫЕ — ОТДЕЛЬНЫЙ ТОМ, И МОНТИРОВАТЬ ЕГО ДО BOOTSTRAP.
#    bootstrap выставляет владельца на директорию данных; примонтируете позже —
#    chown окажется под томом, а записанное до монтирования исчезнет под ним.
sudo mkfs.ext4 -L hftdata /dev/nvme1n1          # только если том пуст! проверьте: sudo file -s /dev/nvme1n1
sudo mkdir -p /opt/hft-collector/data
echo "UUID=$(sudo blkid -s UUID -o value /dev/nvme1n1) /opt/hft-collector/data ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab
sudo mount /opt/hft-collector/data              # nofail в fstab — пара к RequiresMountsFor в юните

# 1. Бутстрап и установка релиза (тарбол собран build-release.sh/cross-build-linux.sh)
sudo ./bootstrap.sh
sudo ./install.sh /tmp/hft-collector-release-<tag>.tar.gz

# 2. Конфиги (справочник ниже)
sudo cp /opt/hft-collector/current/etc/instance.env.example /opt/hft-collector/etc/<instance>.env
sudo $EDITOR /opt/hft-collector/etc/<instance>.env
sudo install -o root -g root -m 600 /opt/hft-collector/current/etc/alert.env.example /opt/hft-collector/etc/alert.env
sudo $EDITOR /opt/hft-collector/etc/alert.env

# 3. Запуск: имя инстанса = имя env-файла
sudo systemctl enable --now hft-collector@<instance>
# Токен таймера — это НАБОР ИНСТАНСОВ, а не расписание: `all` = все инстансы
# хоста, либо имя одного env-файла. Любое другое слово (`daily`) gate-run.sh
# примет за имя инстанса, не найдёт его директорию данных и будет падать с
# кодом 2 каждую ночь.
sudo systemctl enable --now hft-collector-gate@all.timer

# 4. Проверка (см. «Диагностика» ниже)
journalctl -u 'hft-collector@*' -f
```

Вторая биржа = второй env-файл + второй `enable --now`. Инстансы полностью
изолированы: процессы, замки (`flock` на директорию данных), файлы.

## Справочник: `<instance>.env`

Файл читает systemd (`EnvironmentFile=`), НЕ shell: без подстановок и
инлайн-комментариев после значений. Полные пояснения — в самом
`instance.env.example`; здесь сводка.

| Переменная | Обязательна | Значение |
|---|---|---|
| `COLLECTOR_EXCHANGE` | да | `hyperliquid` \| `binancefutures`/`binancefuturesum` \| `binancefuturescm` \| `binance`/`binancespot` \| `bybit` \| `lighter` |
| `COLLECTOR_SYMBOLS` | да | символы через пробел, В НОТАЦИИ БИРЖИ: HL — `BTC`, `xyz:GOLD` (dex-префикс — часть имени); Binance/Bybit — `BTCUSDT`. Каждый символ проверяется на бирже при старте, неизвестный = отказ запуска |
| `RUST_LOG` | нет (`info`) | verbosity; `debug` очень шумный на тиках |
| `COLLECTOR_DATA_DIR` | нет | дефолт `/opt/hft-collector/data/<instance>` — правильный: две записи в одну директорию запрещены и пресекаются замком. Если выносите за `/opt/hft-collector/data` — расширьте `ReadWritePaths` юнита drop-in'ом |
| `COLLECTOR_MIN_FREE_GB` | нет (5) | порог свободного места; пробитие = фатальный выход (и алерт). `0` — выключить |
| `COLLECTOR_STALL_TIMEOUT_MIN` | нет (5) | сторож полного молчания: ни одной маркет-записи за N минут = фатал. Ловит «подключён, но не подписан». `0` — выключить |
| `COLLECTOR_LIVENESS_TIMEOUT_S` | нет (60) | per-symbol гейдж: WARN + запись в `_meta`, когда символ молчит дольше порога (не фатал). `0` — только измерение, без WARN |
| `COLLECTOR_BYBIT_DEPTHS` | только bybit (`1,50`) | глубины стакана. НЕ добавляйте `500`: биржа отвергает его для мажоров, а один отвергнутый топик валит всю пачку подписки |
| `COLLECTOR_HL_L2_MODES` | только HL (`slow,fast`) | какие каденции `l2Book` писать. Дефолт пишет обе — датасет `bbo+fast` (live-parity) требует `fast` |
| `COLLECTOR_NO_SYMBOL_CHECK` | нет (0) | пропустить стартовую валидацию символов. Для `lighter` игнорируется — там каталог и есть адресация |

## Справочник: `alert.env` (root:600, единый на хост)

| Переменная | Значение |
|---|---|
| `TG_BOT_TOKEN` | токен бота из @BotFather |
| `TG_CHAT_ID` | id чата/группы. Группа — отрицательный; достаётся из `curl .../getUpdates` после добавления бота. **Ловушка:** апгрейд группы в supergroup МЕНЯЕТ id (на `-100…`) — алерты молча пропадут, перечитайте id |
| `HEALTHCHECK_PING_URL` | URL check'а healthchecks.io (period=1 day, grace≈2h). Гейт пингует его после успешной валидации и бьёт `<url>/fail` на красном дне. Пусто — dead-man выключен |

Без файла или с плейсхолдерами всё работает, но молча: `alert.sh` логирует
«alert not sent» и выходит нулём — поэтому `OnFailure` вшит в шаблонный юнит
безусловно, и настроенный хост не может «забыть» алертинг.

## Алертинг: устройство и проверка

```
фатал юнита        → systemd failed → OnFailure → alert.sh → Telegram   (секунды)
красный день гейта → gate-юнит failed → тот же путь + <url>/fail        (00:35 UTC)
хост умер молча    → сутки без пинга → healthchecks → Telegram          (period+grace)
```

`alert.sh`: hostname + статус юнита + журнал инцидента (`--since -5min`,
с 2-секундной паузой — journald пишет асинхронно и без неё хвост падения
не успевает попасть в алерт); rate-limit 1 сообщение / 5 мин на юнит
(стемпы в `/run`, сбрасываются ребутом — первый фатал после ребута алертит
всегда); 3 попытки доставки; никогда не выходит с ошибкой — алерт-юнит,
алертящий о собственном падении, был бы петлёй.

Проверка доставки живым учебным падением (безопасно, прод не трогает):

```bash
sudo systemctl reset-failed alert-selftest.service 2>/dev/null
sudo systemd-run --unit=alert-selftest \
  --property=OnFailure=hft-collector-alert@alert-selftest.service \
  /bin/sh -c "echo selftest; exit 1"
# ≈10 секунд спустя в Telegram-группе должно лежать 🔴-сообщение с этим журналом
```

## Ежедневный гейт и вывоз данных

- **Гейт**: `hft-collector-gate@all.timer` в 00:35 UTC гоняет
  `tools/quality_report.py` по вчерашнему дню всех инстансов (nice/ionice —
  не конкурирует с записью), кладёт отчёт в `data/gate/`, на красном дне
  уходит в `failed` (⇒ алерт) и бьёт `/fail` в healthchecks; на зелёном —
  обычный пинг. Один успешный пинг в сутки = «хост жив, том смонтирован,
  запись шла, день валиден».
- **Вывоз**: `offload.sh` запускается С ОПЕРАТОРСКОЙ машины (rsync →
  sha256 с двух сторон → gzip -t → только затем rm на хосте; сегодняшние
  файлы не трогает). Диск после ротации набора 2026-07-29 горит
  ~10–12 ГБ/сутки (мажоры BTC/ETH/SOL — 60% этого) — вывоз ежедневный,
  запас тома ~4 дня. Данные никогда не прунятся сами.
- **Автоматический вывоз (launchd, операторский Mac)**: обёртка
  `offload-daily.sh` + `com.hftbacktest.offload-daily.plist.example` —
  инструкция по установке в комментарии plist-а. Обёртка пишет лог в
  `<target>/offload-logs/`, молчит на успехе, шлёт 🔴 в Telegram на провале
  (креды — `~/.config/hftbacktest-connector/telegram-alert.env`,
  `TG_BOT_TOKEN`/`TG_CHAT_ID`) и 🟡, если после успешного вывоза том хоста
  всё равно заполнен ≥70% (`HFT_OFFLOAD_WARN_PCT`). Семантика расписания:
  Mac спал в назначенное время → запуск при пробуждении; был выключен →
  пропуск. Сторожа на «launchd вообще не стреляет» нет — если нужен,
  заведите второй healthchecks-чек и задайте `HFT_OFFLOAD_PING_URL`
  (успех — пинг, провал — `/fail`, тишина — эскалация сама).
- **Гибридный архив (`archive-rotate.sh`)**: операторская машина держит
  скользящее окно последних `HFT_KEEP_DAYS` (дефолт 2) дней — датасеты и
  бэктесты хотят сырьё локально, — а всё старше уезжает на внешний том
  `HFT_ARCHIVE_DIR` (copy → sha256 с двух сторон → rm; gate-отчёты,
  `reports/` и `dataset-*` не трогаются). Обёртка зовёт ротацию **до**
  вывоза — это освобождает место под него — и шлёт 🟡, когда свободного
  на машине меньше `HFT_LOCAL_FLOOR_GB` (дефолт 15), по какой бы причине
  это ни случилось: том не подключён, ярус не настроен, расход обогнал
  окно. Урок 2026-07-30: полный диск оператора стоил утреннего вывоза
  трёх инстансов; собственный ENOSPC вывоза приходит на сутки позже
  этого предупреждения.

## Обновление и откат

```bash
./collector/deploy/cross-build-linux.sh <tag>     # или build-release.sh на linux-хосте
scp ... && sudo ./install.sh <tarball> -y         # атомарный своп current + последовательный рестарт
sudo /opt/hft-collector/current/bin/rollback.sh   # откат = обратный своп + рестарт
```

Каждый рестарт стоит ~2 с дыры на инстанс; члены gzip финализируются
SIGTERM'ом (проверяйте `gzip -t` только на прошлых UTC-днях — живой файл
не проходит его по построению).

## Диагностика

- `journalctl -u 'hft-collector@*' -f` — connect/disconnect, ротация, фаталы.
- `_meta_*.jsonl` (плейн-текст, читается на живую): `session_start`
  (конфигурация и коммит записи), lifecycle (`connected`/`disconnected`/…),
  минутные гейджи — `disk` (свободное место), `clock` (дисциплина часов;
  `sync:false` после ребута = ждите NTP), `liveness` (возраст последней записи
  по символам), `cpu` (steal на burstable-инстансах).
- **«Файл не растёт» ≠ «не пишет»**: у тонкого символа сжатый поток может
  наполнять 48КБ-буфер gzip дольше 10 минут — mtime стоит, декодер видит только
  сброшенное. Смотрите журнал и liveness-гейдж, не mtime.
- Гейт-отчёты: `data/gate/YYYYMMDD.txt` — построчно, какие дыры и чем объяснены.

## Переезд операторской машины

Операторская машина — это та, что вывозит данные, держит локальное окно записей
и гоняет тулинг/бэктесты. Сервер записи она не трогает: там ничего менять не
надо. Порядок переезда — именно такой; пункт 8 (выключение старого агента)
делайте ДО пункта 5 на новой машине, чтобы два вывоза не гонялись за одними
файлами.

1. **Секреты** (в git их нет, переносятся руками, права 600):
   - `hl_bn_md.pem` — из корня старого клона (gitignored) в корень нового;
   - `~/.ssh/config` — алиас `hft-collector-tokyo` (HostName EC2, User ubuntu,
     IdentityFile → pem);
   - `~/.config/hftbacktest-connector/telegram-alert.env` — `TG_BOT_TOKEN`,
     `TG_CHAT_ID`, `HEALTHCHECK_PING_URL`;
   - `~/.config/hftbacktest-connector/hl-testnet.toml` — ключи HL-тестнета.
2. **Репозиторий**: `git clone git@github-coins:choosemycoins/hftbacktest.git`,
   ветка `feat/hyperliquid-connector`. В `~/.cargo/config.toml` нового
   пользователя — `git-fetch-with-cli = true` (ssh-алиас `github-coins` cargo
   сам не разрешит). Путь клона свободный, но перенос сессий Claude Code
   требует тот же абсолютный путь (см. п. 9).
3. **Rust**: stable ≥ 1.91.1 (MSRV) + nightly для `cargo +nightly fmt`.
   Проверка: `cargo test --workspace --lib --bins` — все крейты зелёные.
4. **Python**: `python3 -m venv .venv && .venv/bin/pip install numpy numba
   pytest maturin`, затем `cd py-hftbacktest && ../.venv/bin/maturin develop
   --release`. Проверка: `.venv/bin/pytest collector/tools/ -q`.
5. **Данные**: перенести `~/hft-data` целиком (окно последних дней + `reports/`
   + `offload-logs/`); правило одно — какие дни лежат локально, такие доступны
   датасетам и бэктестам без внешнего диска.
6. **launchd-вывоз**: `cp collector/deploy/com.hftbacktest.offload-daily.plist.example
   ~/Library/LaunchAgents/com.hftbacktest.offload-daily.plist`, поправить в нём
   оба пути и, при большом диске, окно: `HFT_KEEP_DAYS` можно растянуть (данные
   тогда копятся локально, ярус `HFT_ARCHIVE_DIR` не нужен), а
   `HFT_LOCAL_FLOOR_GB` поднять с запасом на 2–3 дня расхода (~25–35 ГБ).
   `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/...` и пробный
   `launchctl kickstart gui/$(id -u)/com.hftbacktest.offload-daily`.
7. **Смоук всего пути**: `ssh hft-collector-tokyo uptime` →
   `bash collector/deploy/offload.sh --host hft-collector-tokyo --target
   ~/hft-data --dry-run` → лог свежего kickstart-прогона в
   `~/hft-data/offload-logs/`.
8. **Старая машина**: `launchctl bootout gui/$(id -u)/com.hftbacktest.offload-daily`,
   стереть секреты из п. 1. Делается до включения агента на новой.
9. **Сессии Claude Code** (по желанию): `rsync -av
   ~/.claude/projects/-Users-andrew-rust-hftbacktest/` на новую машину в тот же
   путь — это транскрипты и файловая память; репозиторий должен лежать по тому
   же абсолютному пути, иначе `--resume <id>` сессию не найдёт.
10. **Бэктесты на машине с 64 ГБ**: полный день HYPE-класса в `.npz` — единицы
    ГБ, так что многодневные мультисимвольные прогоны помещаются в память
    целиком; `--buffer-size` у `build_dataset.py` можно поднимать смело, а
    датасеты хранить рядом с сырьём (`dataset-*` не трогает ни вывоз, ни
    ротация).
- **Linux-оператор (systemd вместо launchd)**: те же скрипты, юниты —
  `hft-offload-daily.{service,timer}.example` → `~/.config/systemd/user/`,
  поправить пути/Environment, затем `systemctl --user daemon-reload &&
  systemctl --user enable --now hft-offload-daily.timer` и
  `loginctl enable-linger $USER` (иначе таймер живёт только при активной
  сессии). `Persistent=true` — аналог launchd-семантики «проспал — запустись
  при пробуждении». Проверка: `systemctl --user start hft-offload-daily` и
  лог в `<target>/offload-logs/`.
