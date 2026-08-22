# Поллер позиций операторов HL — деплой и миграция

Почасовая позиция шестидесяти адресов Hyperliquid: `userFunding` → `delta.szi`,
знаковый размер позиции на момент начисления. Фактически per-address hourly
Commitments of Traders. Окно `userFunding` у площадки конечно — неснятый час
укатывается за его край и не восстанавливается ничем.

## Что изменилось 2026-08-22

До этой даты поллер, как и params, жил вне дисциплины: скрипт в `/home/ubuntu`,
**user-level** таймер пользователя `ubuntu` (системный `systemctl` его не видит,
держится на `Linger=yes`), юниты в git не отслеживались, а данные писались в
`/home/ubuntu/positions-data` — на **корневой** диск (6.7 ГБ), мимо вывоза.
Замерено: **188 МБ** ряда позиций в одном экземпляре, не вывезены ни разу.

Теперь: скрипт едет в релизе, данные — на томе данных
(`/opt/hft-collector/data/positions`), список адресов — операторский конфиг в
`/opt/hft-collector/etc/operators_addrs.json` (релиз его не перезаписывает,
пример едет как `etc/operators_addrs.json.example`), журнал остаётся в
`/home/ubuntu/positions-data/poller.log`.

⚠️ Отдельно понадобилась правка `offload.sh`: поллер пишет в подкаталог адреса
(`0x…/positions_0x…_<день>.gz`), а `day_of` якорилась на `^` и такое имя не
признавала — файл ушёл бы в «unrecognised names, left alone», то есть не доехал
бы до архива и не удалился бы с хоста. Починено и запинено в
`test_positions_poller.py`.

## Разовая миграция (ОДИН раз, после первого релиза с юнитами)

Порядок несущий: сначала гасим старый таймер, потом переносим, потом включаем
новый. Иначе два таймера пишут один и тот же час в два разных каталога.

```bash
# 1. Погасить прежний user-таймер (системному systemctl он не виден!)
sudo -u ubuntu XDG_RUNTIME_DIR=/run/user/$(id -u ubuntu) \
     systemctl --user disable --now hft-positions-poller.timer

# 2. Список адресов — в etc/, рядом с *.env
sudo install -m 644 /home/ubuntu/operators_addrs.json \
     /opt/hft-collector/etc/operators_addrs.json

# 3. Перенести историю ВМЕСТЕ с .positions_state.json. Без состояния поллер
#    перечитает окно userFunding с начала и задвоит записи по каждому адресу.
sudo mkdir -p /opt/hft-collector/data/positions
sudo mv /home/ubuntu/positions-data/.positions_state.json /opt/hft-collector/data/positions/
for d in /home/ubuntu/positions-data/0x*; do
    [ -d "$d" ] && sudo mv "$d" /opt/hft-collector/data/positions/
done
ls /opt/hft-collector/data/positions | wc -l     # 60 каталогов + состояние
ls /home/ubuntu/positions-data                   # остаётся только poller.log

# 4. Включить системный таймер и проверить тик
sudo systemctl enable --now hft-positions-poller.timer
sudo systemctl start hft-positions-poller.service
tail -3 /home/ubuntu/positions-data/poller.log   # адресов=60 … ошибок=0
```

Проверка, ради которой всё делалось: после первого тика в журнале должно быть
`новых_записей` **того же порядка**, что и до миграции (~2250/час). Числа в разы
больше означают, что состояние не переехало и ряд задваивается.

## Известные ограничения

* Каталог `positions` не имеет профиля в гейте качества — гейт обходит инстансы
  по `etc/*.env`, а у поллера `.env` нет. Тот же пробел, что у `funding` и
  `params`.
* Список адресов порождается `collector/tools/build_operator_watchlist.py`;
  провенанс отбора (fills / maker_usd / role / share) лежит рядом в
  `collector/tools/operators_meta.json`.
