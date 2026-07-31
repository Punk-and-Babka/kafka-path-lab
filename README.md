# Kafka Path 0.5.0 — Sandbox

Интерактивная песочница Kafka на React, TypeScript и Vinext.

В версии 0.5.0 нет готовых учебных сценариев. Пользователь задаёт входные
данные и наблюдает их фактическое прохождение по рабочей системе:

```text
Producer → Topic / Partition → Leader / Followers → ACK
→ Consumer → Decode → Handler → service_db → Offset commit
```

## Что можно отправить

- event с собственными `topic`, `event name`, `key`, `headers` и `payload`;
- локальный текстовый, JSON, XML или бинарный файл;
- сообщение без key для демонстрации round-robin;
- несколько сообщений подряд для сравнения partitions и offsets.

Файл не загружается на сервер. Текстовые данные до 256 KB читаются локально,
а для крупных или бинарных файлов симулятор формирует metadata record.

## Наблюдение и ручное управление

- пошаговая state machine из 11+ этапов;
- реальные для симуляции `partition`, `offset`, Leader, ISR и ACK;
- Event, Delivery и Lifecycle Inspector;
- ручные `acks`, `replication.factor`, `min.insync.replicas`, retries и
  idempotence;
- остановка Broker, failover Leader, lagging replica и возврат в ISR;
- потеря request или ACK, retry, duplicate и deduplication;
- управление анимацией, скорость, горячие клавиши и режим фокуса.

## Локальный запуск

Требуется Node.js 22 или новее.

```bash
npm ci
npm run dev
```

Откройте адрес, который покажет терминал. Дополнительная русская инструкция:
[`LOCAL_RUN_RU.md`](./LOCAL_RUN_RU.md).

## Проверка production-сборки

Обычная сборка:

```bash
npm run build
```

Статический экспорт для репозитория `Kafka-Path-Lab`:

```bash
GITHUB_PAGES=true GITHUB_PAGES_BASE_PATH=/Kafka-Path-Lab npm run build:pages
```

Готовый статический сайт появится в `dist/client`.

## Публикация на GitHub Pages

Workflow уже находится в `.github/workflows/deploy-pages.yml`.

1. В GitHub откройте `Settings → Pages`.
2. В `Source` выберите `GitHub Actions`.
3. Отправьте изменения в ветку `main`:

```bash
git add -A
git commit -m "Release 0.5.0 sandbox"
git push origin main
```

После push GitHub самостоятельно выполнит `npm ci`, статический экспорт и
публикацию. Папки `node_modules` и `dist` добавлять в репозиторий не нужно.

## Основные файлы

- `app/kafka-path-simulator.tsx` — интерфейс и интерактивная логика;
- `app/simulator-model.ts` — модель Kafka, состояния и вычисления;
- `app/globals.css` — визуальная система и responsive layout;
- `app/page.tsx` — статическая оболочка страницы;
- `scripts/prepare-pages-output.mjs` — подготовка путей GitHub Pages.
