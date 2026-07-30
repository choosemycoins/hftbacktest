# Артефакты Phase 0 Lighter-коннектора (тестнет, 2026-07-30)

Сырые выходы онлайн-спайка, на которые ссылается `docs/design-lighter-connector.md`
(§2.7, §6.А-2, source tag `[probe-live]`). Аккаунт — тестнет `account_index 516`,
слот api-ключа 5; ключей и auth-токенов в файлах нет (проверено grep-ом до коммита).
SDK: `lighter-sdk == 1.1.2` (ctypes поверх `lighter-signer-darwin-arm64.dylib`).

| Файл | Что |
|---|---|
| `golden_vector.json`, `golden_out.json`, `golden2_out.json` | golden-вектор хеша транзакции (подпись недетерминирована — фиксируется хеш) и два прогона подписанта |
| `sensitivity.json` | чувствительность хеша к каждому из 15 полей CreateOrder |
| `step3_out.json` | auth-токен: выпуск, приём сервером, отказ 20013 на 9-часовом |
| `step4_out.json` | снапшоты+дельты приватных каналов (`account_all_*`) |
| `step5_out.json` | CreateOrder end-to-end: sendTx → приватный канал, тайминги |
| `step6_out.json`, `step6b_out.json` | nonce-семантика: replay/gap → 21104, счётчик не двигается; дубль COI → 21728; HTTP 200 без ордера (вердикт только в `event_info.ae`) |
| `step7_out.json` | Cancel/CancelAll + выживание ордеров через обрыв сокета |
| `testnet_orderbooks.json` | каталог тестнета: 5 маркетов, наших монет нет |

Эти файлы — будущие фикстуры Phase 1/2 (`connector/src/lighter/`): golden-вектор
пинит Rust-порт или sidecar-подписант, тела каналов — парсеры приватного стрима.
