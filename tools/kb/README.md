# kb — локальная база знаний

Где это обсуждалось? — транскрипты Claude и `/export`, память, md-доки репо
(hftbacktest, myhft, eventshort, jansen), `~/hft-research`, курс (ASR + конспекты),
Telegram по allowlist, коммиты. Ответ — **указатели** (`kb:<16 hex>`, вид, проект, дата,
путь + строки / время / сообщения / sha) и маскированный сниппет. Открыть источник и
прочитать — до того, как чему-то верить. Пустой ответ ≠ «не обсуждалось» (см. `coverage`).

**БД и матрицы — секрет**: 0600 в каталоге 0700, не коммитить, не синкать, не копировать с
машины; производный артефакт. Всё, что выходит наружу (CLI, MCP), маскируется — это сеть,
не гарантия. Сеть нужна только `kb fetch-model` (разово).

Дизайн и решения — `DESIGN.md` рядом (если он в дереве); конфиг — `~/.config/kb/kb.toml`
(0600, не в git; образец без чатов — `kb.example.toml`).

## Окружение

```bash
export UV_CACHE_DIR=/home/storage/hft-scratch/uv-cache
export UV_PROJECT_ENVIRONMENT=/home/storage/hft-scratch/kb-venv
cd tools/kb
uv sync --frozen --no-editable --extra test --reinstall-package kb   # torch — только CPU-колесо (uv.lock)
/home/storage/hft-scratch/kb-venv/bin/python -m pytest -q    # тесты, офлайн
```

Установка **не editable**: ночной юнит работает с копией кода в venv и не ломается при
смене ветки в checkout. После правок кода — тот же `uv sync` **с `--reinstall-package kb`**:
версия пакета не меняется, и без этого флага uv считает его установленным и оставляет в venv
старую копию (замерено 07.10: `cli.py` и `update.py` в venv отставали от дерева).
Сверка: `cmp` файлов `kb/*.py` с `…/kb-venv/lib/python3.12/site-packages/kb/`.

## Команды

```text
kb update [--embed] [--budget-minutes N] [--max-chunks N] [--allow-mass-delete]
          [--require-offline | --allow-network] [--max-psi P --psi-wait-minutes M]
          [--min-free-gb G] [--lock-timeout S]
kb build --full                 # во временный файл → fsync → атомарная подмена
kb embed [--model KEY] [--budget-minutes N] [--max-chunks N] [--device cpu|cuda]
kb search QUERY [--mode hybrid|fts|semantic] [--kind K[,K]] [--project P] [--since DATE] [--limit N] [--json]
kb read POINTER [--context N] [--json]
kb stats [--check]
kb eval [--golden PATH] [--modes fts_raw_or,fts,semantic,hybrid] [--compare-models e5b,m3 --distractors N]
kb bench-embed --model KEY --n N [--threads T]
kb fetch-model --model KEY      # единственная сетевая команда
kb gc [--drop-model KEY]
kb mcp                          # stdio MCP-сервер
```

Если индекс не читается, ответ говорит, что делать: «busy» — писатель коммитит, повторить через
несколько секунд; «hot journal» — прерванная запись (SIGKILL, питание), лечит `kb update` (откатывает
журнал, векторы на месте); только «not a kb database» / «schema vN» — `kb build --full`. Сама
`kb build --full` сначала откатывает журнал живой БД и отказывается заменять файл, если не смогла
прочитать его векторы (код 3, старый файл не тронут).

Команды, загружающие модель (`update --embed`, `embed`, `mcp`), отказываются работать, если
кроме `lo` виден хоть один сетевой интерфейс: запускать под `unshare -rn` (ночной юнит живёт в
своём сетевом namespace) или, для ручных update/embed, с `--allow-network`.

Обычный запрос стеммируется (ru/en, ё = е); `raw:` — синтаксис FTS5 по сырому тексту
(точные формы, `"фраза"`, `префикс*`, `AND/OR/NOT`, `NEAR`). Префикс — только буквы и сразу
перед `*` (`теря*`, но не `0x7a*`, не `"…"*`): «нашлось / не нашлось» по маскированному тексту
выписывает ключ по символу. Значение из одних букв (`password=…`) этим не закрыто. `tools/kbindex.py` — шим:
`build` → `kb update`, `search Q` → `kb search "raw:Q" --mode fts`.

Коды выхода: 0 — ок; 2 — запрос/использование; 3 — неполный прогон (корень, гард массового
удаления, отказ разбора, пропущенный проход `--embed`: модели нет или она не прошла сверку);
75 — отложено (давление памяти, занятый лок при ожидании).

## Первый запуск (лид)

```bash
kb fetch-model --model e5b ; kb fetch-model --model m3            # сеть, разово
~/bin/hft-run -m 24G -- kb build --full
~/bin/hft-run -m 24G -- kb bench-embed --model e5b --n 2000        # затем m3
~/bin/hft-run -m 24G -- kb eval --compare-models e5b,m3 --distractors 10000
# решение по DESIGN §6.3 → kb.toml [models] active = …   (07.10: m3 — нетто +5, дельта 19 мин; §6.8)
cp tools/kb/units/kb-update.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload && systemctl --user enable --now kb-update.timer
```

Итог ночного прогона: `journalctl --user -u kb-update -n 50`, `systemctl --user status
kb-update`. Любой ненулевой код (3 — неполный прогон, 75 — отложено по давлению памяти, 1/137/203 —
падение, MemoryMax, нет venv) уходит алертом через `OnFailure=signal-alert@%n.service` (Telegram:
состояние юнита и хвост журнала). В журнале только счётчики, id и пути, трейсбеки маскируются.
Если отложенные ночи станут шумом — `SuccessExitStatus=75` в юните делает их молчаливыми.

## MCP (регистрирует лид; здесь не выполнено)

```bash
claude mcp add kb -s user \
  -e HF_HOME=/home/storage/hft-scratch/hf-cache -e HF_HUB_OFFLINE=1 -e TRANSFORMERS_OFFLINE=1 \
  -- /usr/bin/unshare -rn /home/storage/hft-scratch/kb-venv/bin/kb mcp
```

`unshare -rn` (user + net namespace) отрезает сеть целиком: внутри только `lo`. Сервер сам
проверяет `/proc/net/dev` и без этого не стартует. Инструменты: `kb_search(query,
mode=hybrid|fts|semantic, kind, project, since, limit≤50)` и `kb_read(pointer, context 0–5)`;
`kb_read` резолвит только проиндексированные указатели, ответ ≤ 20 000 символов; каждая
строка ответа маскируется. fd 1 процесса уведён в stderr: случайный print не ломает
протокол. Модель грузится в фоне; первый семантический запрос ждёт её до 60 с, дальше —
fts с полем `degraded`. Память: модель + матрица ≈ 1.5–3 ГБ на каждую сессию Claude,
поднявшую сервер.
