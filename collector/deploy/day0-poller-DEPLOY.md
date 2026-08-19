# Выкатка day0-поллера на токийский хост сбора (#74)

Ревизия артефактов: **`c5c2a0d`** (ветка `fix/depth-freshness-83`, не пушнута — файлы едут
на хост копированием, не `git pull`).

Тег релиза, который получится: **`day0poller-c5c2a0d`**.

Что едет на хост (пять файлов):

| в релизе | из репозитория |
|---|---|
| `bin/day0_poller.py` | `collector/tools/day0_poller.py` |
| `bin/heartbeat.sh` | `collector/deploy/heartbeat.sh` (в нём строка `day0:…:35`) |
| `etc/hft-day0-poller.service` | `collector/deploy/hft-day0-poller.service` |
| `etc/hft-day0-poller.timer` | `collector/deploy/hft-day0-poller.timer` |
| `etc/binancefuturesum-day0.env.example` | `collector/deploy/binancefuturesum-day0.env.example` |

> **Почему релиз собирается руками, копией `current`, а не `build-release.sh` + `install.sh`.**
> Панель нашла (major): `build-release.sh`/`cross-build-linux.sh` не стейджат ни поллер, ни
> его юниты, а `install.sh` ставит из фиксированного манифеста тарбола, где поллера нет.
> При этом `ExecStart` юнита указывает **внутрь** атомарно-свопаемого дерева
> (`/opt/hft-collector/current/bin/day0_poller.py`). Пока сборщик не научен, единственный
> способ не нарушить деплой-контракт — сделать **новый каталог релиза** (копия текущего +
> пять файлов) и **атомарно перевесить симлинк**. Живое дерево `current/` при этом не
> правится ни разу, `rollback.sh` продолжает работать. Долг: вписать эти пять файлов в
> `build-release.sh` и в манифест `install.sh` — тогда следующая выкатка пойдёт штатно.

---

## Шаг 0. Предполётные проверки (до того, как что-либо трогать)

Всё в этом шаге — **чтение**. Ни одна команда ничего не меняет.

```bash
TOKYO=ubuntu@<хост-сбора>          # тот же, куда ходит offload.sh
ssh "$TOKYO"
```

**0.1 Диск.** Первый же тик поллера — не «пустой прогрев»: bootstrap захватывает **всё,
что моложе 14 суток по onboardDate**. На снапшоте 19.08 это **21 символ** (см. шаг 5,
сухой прогон покажет точный список сегодня). Инстанс стартует автоматически на первом
тике (поллер делает `systemctl restart`, а `restart` неактивного юнита — это старт),
поэтому запас по диску проверяется **до** включения таймера, а не после.

```bash
# сколько занимает уже идущий инстанс дня-0 (шестёрка 14.08) и за сколько суток
sudo du -sm /opt/hft-collector/data/* | sort -n
df -BG /opt/hft-collector/data
```

Оценка: `(МБ шестёрки / суток_записи) × 21/6` — это суточный аппетит нового инстанса.
Порог тревоги heartbeat — 15 ГБ свободного (warn) / 8 ГБ (crit). Если после умножения
до warn остаётся меньше двух суток — **сначала офлоад**, потом выкатка. Пропущенный
листинг невосполним, но и остановка всех инстансов у пола диска — тоже.

**0.2 Хук алертов на месте** (юниты несут `OnFailure=hft-collector-alert@%n.service`;
если хука нет, systemd молча не разрешит цель, и падения поллера будут тихими):

```bash
systemctl cat hft-collector-alert@.service >/dev/null && echo "хук есть"
sudo test -r /opt/hft-collector/etc/alert.env && echo "alert.env читается"   # root:600
```

**0.3 Python и текущий релиз:**

```bash
/usr/bin/python3 -VV                      # ожидается 3.10+, stdlib-only, venv не нужен
readlink -f /opt/hft-collector/current    # запиши: это будет .previous и цель отката
du -sh "$(readlink -f /opt/hft-collector/current)"
ls -la /opt/hft-collector/etc/            # ⚠ binancefuturesum-day0.env НЕ должен существовать
```

`binancefuturesum-day0.env` **не создаём руками**: его целиком пишет поллер на первой
волне (заголовок, символы, `COLLECTOR_STALL_TIMEOUT_MIN=1080`,
`COLLECTOR_LIVENESS_TIMEOUT_S=64800`). Пустой env, положенный заранее, стоил бы
крашлупа (`collector-run.sh` exit 4) и лишних `nodir`-тревог heartbeat: его цикл по
коллекторам ходит по `etc/*.env`, и файл без данных читается как «инстанс есть, записи
нет». `.env.example` кладём только в релиз, как документацию — глоб `*.env` его не ловит.

---

## Шаг 1. Доставка файлов (с машины оператора)

```bash
cd /home/andrew/RustroverProjects/hftbacktest
git rev-parse --short HEAD            # должно быть c5c2a0d
git status --porcelain collector/     # должно быть пусто

scp collector/tools/day0_poller.py \
    collector/deploy/heartbeat.sh \
    collector/deploy/hft-day0-poller.service \
    collector/deploy/hft-day0-poller.timer \
    collector/deploy/binancefuturesum-day0.env.example \
    "$TOKYO":/tmp/day0-drop/
```

(`ssh "$TOKYO" mkdir -p /tmp/day0-drop` — если каталога ещё нет.)

Проверка целостности доставки (сверяем обе стороны):

```bash
sha256sum collector/tools/day0_poller.py collector/deploy/heartbeat.sh
ssh "$TOKYO" 'sha256sum /tmp/day0-drop/day0_poller.py /tmp/day0-drop/heartbeat.sh'
```

---

## Шаг 2. Новый релиз = копия `current` + пять файлов

Всё дальше — на хосте, от root.

```bash
ssh "$TOKYO"
sudo -i

TAG=day0poller-c5c2a0d
SRC="$(readlink -f /opt/hft-collector/current)"
NEW="/opt/hft-collector/releases/${TAG}"

test ! -e "$NEW" || { echo "ОСТАНОВ: ${NEW} уже существует"; exit 1; }

cp -a "$SRC" "$NEW"                       # -a: права/владельцы/времена как были

install -m 755 -o root -g root /tmp/day0-drop/day0_poller.py "$NEW/bin/day0_poller.py"
install -m 755 -o root -g root /tmp/day0-drop/heartbeat.sh   "$NEW/bin/heartbeat.sh"
install -m 644 -o root -g root /tmp/day0-drop/hft-day0-poller.service "$NEW/etc/"
install -m 644 -o root -g root /tmp/day0-drop/hft-day0-poller.timer   "$NEW/etc/"
install -m 644 -o root -g root /tmp/day0-drop/binancefuturesum-day0.env.example "$NEW/etc/"

# происхождение — в манифест, чтобы `rollback.sh --list` не врал о содержимом
printf 'day0_poller_from=c5c2a0d\nday0_poller_added_at=%s\n' "$(date -u +%FT%TZ)" >> "$NEW/RELEASE"
```

Проверки до свопа (ни одна ничего не меняет):

```bash
/usr/bin/python3 -m py_compile "$NEW/bin/day0_poller.py" && echo "поллер компилируется"
bash -n "$NEW/bin/heartbeat.sh" && echo "heartbeat синтаксически цел"
diff -r "$SRC" "$NEW" | head            # ожидаем ровно пять новых файлов + RELEASE
```

---

## Шаг 3. Атомарный своп `current` и запись `.previous`

Тот же приём, что в `install.sh`/`rollback.sh`: `ln -s` рядом + `mv -T`. Коллекторы,
которые уже бегут, не затрагиваются (их процесс давно сделал exec; путь резолвится
только при старте), поэтому рестарта инстансов здесь **не требуется** — двоичный файл
в новом релизе побайтно тот же.

```bash
PREV="$SRC"
ln -snf "$NEW" /opt/hft-collector/current.new
mv -Tf /opt/hft-collector/current.new /opt/hft-collector/current

printf '%s\n' "$PREV" > /opt/hft-collector/.previous.new
chmod 600 /opt/hft-collector/.previous.new
mv -f /opt/hft-collector/.previous.new /opt/hft-collector/.previous

readlink -f /opt/hft-collector/current   # должен показать $NEW
```

С этого момента `hft-heartbeat.service` (oneshot по таймеру) на следующем тике возьмёт
**новый** `heartbeat.sh` — тот, что знает про day0. Рестартовать ничего не нужно.

---

## Шаг 4. Установка юнитов поллера

```bash
install -m 644 -o root -g root "$NEW/etc/hft-day0-poller.service" /etc/systemd/system/
install -m 644 -o root -g root "$NEW/etc/hft-day0-poller.timer"   /etc/systemd/system/
systemctl daemon-reload

systemd-analyze verify /etc/systemd/system/hft-day0-poller.service \
                       /etc/systemd/system/hft-day0-poller.timer
```

`systemd-analyze verify` обязателен, а не для порядка: первая редакция этого юнита несла
`OnFailure=` в секции `[Service]`, где systemd его **молча игнорирует** (тот же класс, что
задокументированный в репозитории `StartLimitIntervalSec`), и поймал это именно verify.

Каталог данных поллера (пульс, состояние, журнал) — systemd создаёт файл, но не каталог:

```bash
install -d -m 755 -o ubuntu -g ubuntu /home/ubuntu/day0-data
```

Каталог обязан быть именно `/home/ubuntu/day0-data`: там его ищет heartbeat
(`Environment=POLLER_HOME=/home/ubuntu` в `hft-heartbeat.service`), и туда же смотрит
`ExecStart` поллера.

---

## Шаг 5. Сухой прогон на хосте — настоящий exchangeInfo, ноль записей

```bash
/usr/bin/python3 /opt/hft-collector/current/bin/day0_poller.py \
    --once --dry-run --data-dir /home/ubuntu/day0-data
echo "код выхода: $?"
```

`--dry-run` читает по-настоящему (сеть, `state.json`, `/opt/hft-collector/etc/…env`), но
наружу **не пишет ничего** и никого не будит: подменён весь исполняющий слой — запись,
`systemctl`, `systemd-run`, Telegram, пульс.

Что обязано быть в выводе (сверено локально 19.08 против мейннета):

* блок `--- DRY RUN, записал бы /opt/hft-collector/etc/binancefuturesum-day0.env:` — и в
  нём `COLLECTOR_SYMBOLS=` с ~21 символом, `COLLECTOR_STALL_TIMEOUT_MIN=1080`,
  `COLLECTOR_LIVENESS_TIMEOUT_S=64800`;
* `--- DRY RUN, выполнил бы: systemctl restart hft-collector@binancefuturesum-day0.service`;
* `--- DRY RUN, отправил бы:` с текстом `🆕 <хост>: волна листингов day-0 — N` и строкой
  времён `локально … · serverTime … (лаг … мин)`;
* `--- DRY RUN, записал бы …/day0-data/state.json`;
* `код выхода: 0`.

**Стоп-условия** (не продолжать, разбираться):

* строка вида `провал #1: …` вместо волны — площадка недоступна или схема поехала
  (битая запись = провал тика, это by design, см. фикс `8dc4e3e`);
* `TG не настроен` — проверить `alert.env` (шаг 0.2); поллер продолжит работать, но
  тревоги будут только в журнале;
* попытка записать **не** `/opt/hft-collector/etc/binancefuturesum-day0.env` — это отказ
  `Day0Refusal`, баг, выкатку прекратить.

После сухого прогона `/home/ubuntu/day0-data` обязан остаться **пустым**:

```bash
ls -la /home/ubuntu/day0-data     # ожидаем пусто
```

---

## Шаг 6. Первый настоящий тик и включение таймера

Порядок именно такой: сначала **один ручной тик**, чтобы увидеть эффекты под контролем,
и только потом таймер.

```bash
systemctl start hft-day0-poller.service
systemctl status hft-day0-poller.service --no-pager    # ожидается inactive (dead), result=success
tail -20 /home/ubuntu/day0-data/poller.log
```

Проверка эффектов первого тика:

```bash
cat /opt/hft-collector/etc/binancefuturesum-day0.env   # непустой COLLECTOR_SYMBOLS
ls -la /home/ubuntu/day0-data/                         # state.json + pulse-YYYYMMDD.gz
zcat /home/ubuntu/day0-data/pulse-*.gz | tail -3
systemctl is-active hft-collector@binancefuturesum-day0.service   # поллер уже поднял его
journalctl -u hft-collector@binancefuturesum-day0 -n 30 --no-pager
```

Идемпотентность (обязательная проверка, второй тик над тем же миром не делает **ничего**,
кроме пульса):

```bash
systemctl start hft-day0-poller.service
tail -2 /home/ubuntu/day0-data/poller.log   # строка без слова ВОЛНА
journalctl -u hft-collector@binancefuturesum-day0 -n 5 --no-pager  # рестарта быть не должно
```

Включаем таймер и делаем инстанс переживающим ребут:

```bash
systemctl enable --now hft-day0-poller.timer
systemctl list-timers hft-day0-poller.timer --no-pager   # NEXT в пределах 10 минут, не n/a

systemctl enable hft-collector@binancefuturesum-day0     # он уже запущен поллером
systemctl is-enabled hft-collector@binancefuturesum-day0
```

`list-timers` со значением `n/a` в колонке NEXT — это ровно тот отказ, который стоил
params-поллеру 23 часов молчания. Юнит специально на `OnCalendar=*:00/10`, от рестартов
не зависящем; если NEXT пустой — не уходить, разбираться.

---

## Шаг 7. Пульс виден сторожу

```bash
POLLER_HOME=/home/ubuntu /opt/hft-collector/current/bin/heartbeat.sh --dry-run
```

В выводе `day0` обязан стоять в списке **живы**. Проверено локально: тот же прогон при
отсутствующем каталоге даёт `day0(nodir)` в тревоге — «не задеплоен» не равно «здоров».

Порог day0 — 35 минут (3 пропущенных тика + запас), а не общие 90: тишина этого поллера
стоит дороже, волна приходит раз в ~2 суток без предупреждения.

Сутки спустя стоит убедиться, что сторож не шумит и что суточный дайджест (9:00 UTC)
перечисляет day0 среди живых источников.

---

## Шаг 8. Что смотреть на первой настоящей волне

Волна приходит сама (темп 29 листингов/30 суток). Признаки:

```bash
grep ВОЛНА /home/ubuntu/day0-data/poller.log
systemctl list-timers 'hft-day0-restart-*' --no-pager    # transient-таймеры на onboard−2мин
```

* Telegram: `🆕 <хост>: волна листингов day-0 — N` со списком, классом контракта и
  `onboard … UTC`.
* Для символа с датой **в будущем** ожидается строка `armed` и transient-таймер
  `hft-day0-restart-*` c `AccuracySec=1s`, стреляющий за 2 минуты до `onboardDate`
  (переподписка после старта торгов — урок day0-start).
* Для символа, который уже торгуется, — немедленный рестарт инстанса.
* `🔴 … эффекты тика не исполнились` — systemd отказал; долг переезжает на следующий тик
  и добирается сам, но причину надо смотреть (`journalctl -u hft-day0-poller`).

Ротация: символ выбывает из env через 14 суток от своего якоря, и только внутри
следующей волны (в алерте — строка «выведено»).

---

## Откат

Поллер и его таймер:

```bash
systemctl disable --now hft-day0-poller.timer
systemctl stop 'hft-day0-restart-*.timer' 2>/dev/null || true
# по желанию — остановить и захват:
systemctl disable --now hft-collector@binancefuturesum-day0
```

Релиз целиком (штатный путь, `.previous` записан на шаге 3). **Порядок именно
такой** — сначала гасим таймер, потом откатываем:

```bash
systemctl disable --now hft-day0-poller.timer        # ОБЯЗАТЕЛЬНО ПЕРВЫМ
/opt/hft-collector/current/bin/rollback.sh --list
/opt/hft-collector/current/bin/rollback.sh          # вернётся на .previous
```

Почему первым: `ExecStart` указывает внутрь свопаемого дерева, а `day0_poller.py`
есть только в релизах, куда его положили руками (долг #106). После отката файла по
пути нет — юнит падает `203/EXEC`, `OnFailure` шлёт сообщение, и таймер повторяет
это **каждые 10 минут**: 144 ложных алерта в сутки, топящих настоящие тревоги
(панель #88). Git-версия юнита несёт `ConditionPathExists` на путь скрипта — с ней
юнит после отката тихо пропускается, не failed. ⚠ Но юнит, УСТАНОВЛЕННЫЙ релизом
`day0poller-c5c2a0d`, этой строки **не имеет**: до переустановки юнита из свежего
дерева гашение таймера перед откатом — единственная защита.

Откат релиза возвращает и старый `heartbeat.sh` (без строки day0) — то есть тревога по
пульсу поллера исчезнет вместе с ним. Если поллер остаётся включённым, а релиз
откатывается, его тишина снова станет ненаблюдаемой сторожем (остаётся собственный
алерт поллера после 6 провалов подряд).

---

## Оговорки, которые надо знать до выкатки

1. **`build-release.sh`/`install.sh` не знают про поллер** (панель, major). Этот документ
   и есть обходной путь; долг — вписать пять файлов в сборщик и манифест.
2. **argv отложенного таймера не проходит через вайтлист** (панель, major).
   `RealHost.systemctl` защищён `assert_allowed_systemctl` (только `restart` своего
   инстанса и `stop` своих `hft-day0-restart-*.timer`), а `systemd_run_timer` получает
   argv литералом из `apply_plan` и проверкой не накрыт. Сегодня безопасно, но регрессия
   проехала бы зелёной — не менять эту строку без пина.
3. **`state.json` растёт монотонно** (~404 КБ на 872 символа, перезапись каждые 10 мин,
   ~58 МБ/сут — 0.5% суточной записи хоста). Ключи исчезнувших символов не выпиливаются.
4. **Первый тик — это сразу захват ~21 символа.** Если запас по диску не проверен на
   шаге 0.1, выкатка сама создаёт проблему, ради предупреждения о которой в heartbeat
   заводился disk-гвард.
5. **Транзиентные таймеры не переживают ребут** (`systemd-run --collect`). Ребут между
   волной и `onboardDate` теряет запланированную переподписку; поллер это заметит только
   как отсутствие подтверждённого таймера и поведёт себя как на флипе (рестарт), то есть
   деградация мягкая, но не нулевая.
6. **Ретрая на 429/5xx нет намеренно.** Вес запроса 1 при бюджете 2400/мин, тик редкий,
   цена провала — 10 минут; серия из шести подряд алертит.
