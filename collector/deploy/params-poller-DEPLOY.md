# Поллер параметров площадок — деплой и миграция

Ряд администрируемых величин (тик, минимальный ноутионал, тиры маржи, максимальное
плечо, статус рынка, комиссии, интервал фандинга) по семи площадкам, раз в час.
Пишет снимок только при изменении хеша, иначе однострочный пульс — без пульса
«не изменилось» и «мы не смотрели» неразличимы.

## Что изменилось 2026-08-22

До этой даты поллер жил **вне всякой дисциплины**: скрипт в `/home/ubuntu/`,
**user-level** таймер пользователя `ubuntu` (виден только через
`systemctl --user`, держится на `Linger=yes`), юнитов не было в git вовсе, а
данные писались в `/home/ubuntu/params-data` — то есть на **корневой** диск
(6.7 ГБ), мимо `offload.sh` и мимо гейта. Замерено: 24 МБ ряда параметров
существовали в одном экземпляре и не были вывезены ни разу.

Теперь это обычный системный юнит наравне с day0- и funding-поллером: скрипт
едет в релизе (`current/bin/params_poller.py`), данные — на том данных
(`/opt/hft-collector/data/params`, забирается вывозом), журнал остаётся в
`/home/ubuntu/params-data/poller.log`, у юнита появились `Wants=network-online`,
`OnFailure=`, `ConditionPathExists=`, `Nice=19` и `StartLimitIntervalSec=0`.

## Разовая миграция (выполняется ОДИН раз, после первого релиза с юнитами)

Порядок важен: сначала гасим старый таймер, потом переносим историю, потом
включаем новый. Иначе два таймера опрашивают каталоги параллельно и пишут в
разные каталоги один и тот же час.

```bash
# 1. Погасить прежний user-таймер (он невидим системному systemctl!)
sudo -u ubuntu XDG_RUNTIME_DIR=/run/user/$(id -u ubuntu) \
     systemctl --user disable --now params-poller.timer
sudo -u ubuntu XDG_RUNTIME_DIR=/run/user/$(id -u ubuntu) \
     systemctl --user list-timers --all | grep params   # должно быть пусто

# 2. Перенести историю на том данных ВМЕСТЕ с .params_state.json —
#    без состояния первый же тик перепишет полные снимки по всем площадкам
#    и в ряду появится ложное «всё изменилось».
sudo mkdir -p /opt/hft-collector/data/params
sudo mv /home/ubuntu/params-data/.params_state.json /opt/hft-collector/data/params/
for v in aster binanceusdm bybit extended hyperliquid lighter paradex; do
    [ -d "/home/ubuntu/params-data/$v" ] && sudo mv "/home/ubuntu/params-data/$v" /opt/hft-collector/data/params/
done
ls /opt/hft-collector/data/params           # семь каталогов + .params_state.json
ls /home/ubuntu/params-data                 # остаётся только poller.log

# 3. Включить системный таймер
sudo systemctl enable --now hft-params-poller.timer
systemctl list-timers hft-params-poller.timer   # NEXT не должен быть n/a

# 4. Проверить один тик руками
sudo systemctl start hft-params-poller.service
tail -3 /home/ubuntu/params-data/poller.log
find /opt/hft-collector/data/params -name '*.gz' -newermt '-5 min' | head
```

## Проверка после обычного релиза

```bash
systemctl list-timers hft-params-poller.timer          # NEXT != n/a
cmp /opt/hft-collector/current/etc/hft-params-poller.service \
    /etc/systemd/system/hft-params-poller.service      # юнит совпал с релизом
```

## Известные ограничения

* `leverageBracket` у Binance — **подписанный** эндпоинт (`-2014` без ключа), так
  что тиры маржи оттуда недоступны; есть только фильтры символов `exchangeInfo`.
* Каталог `params` не имеет профиля в гейте качества: гейт обходит инстансы по
  `etc/*.env`, а у поллера `.env` нет. Тот же пробел, что у каталога `funding`.
