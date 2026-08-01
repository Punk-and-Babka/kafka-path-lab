# Kafka Path 0.7.3 — Topic / Broker UX

Интерактивная лаборатория Kafka на React, TypeScript и Vinext.

Версия 0.7.3 — небольшой UX-релиз поверх correctness-базы 0.7.2. Он делает явной связь между логическим Topic и физическим размещением partition replicas на Brokers, не меняя механику симуляции.

Улучшения 0.7.3:

- над основной цепочкой добавлена динамическая карта `P0–P2 → Broker Leader / Follower`;
- одинаковая partition имеет один цвет в логическом журнале Topic и во всех физических replicas;
- подписи разделены на `ЛОГИЧЕСКИЙ ВИД · TOPIC` и `ФИЗИЧЕСКИЙ ВИД · BROKERS`;
- прямо в интерфейсе объяснено: Topic — логическая структура, Brokers — физическое хранение;
- карта автоматически перестраивается при смене `replication.factor`, Leader и размещения replicas;
- строка partition на карте кликабельна и открывает её подробности;
- добавлена автоматическая проверка нового учебного пояснения.

## Correctness-база 0.7.2

Версия 0.7.2 исправила расхождения, найденные во время полного correctness-review перед демонстрацией ментору. Новых верхнеуровневых режимов нет; крупный UI и полноэкранный режим 0.7.1 сохранены.

Исправления 0.7.2:

- основной Producer и Producer внутри Consumer Lab используют единый Topic и один LEO по каждой partition;
- отдельно моделируются Leader LEO и consumer-visible High Watermark;
- `poll()` двигает fetch position до завершения business processing;
- добавлен отдельный processed offset;
- auto commit выполняется периодически и может опередить processing;
- manual commit сохраняет processed offset;
- commit во время rebalance отклоняется как `RebalanceInProgressException`;
- `Crash` сначала прекращает heartbeat и poll(), а исключение происходит только после `session.timeout.ms`;
- остановка Broker делает данные `UNAVAILABLE / OFFLINE COPY`, но не `LOST`;
- `request lost` при `acks=0` больше не превращается в успешную запись;
- при `acks=1` и недостаточном `min.insync.replicas` Producer может получить ACK, но record остаётся ниже High Watermark;
- preview и фактическая отправка keyless record используют один partitioner cursor;
- явно указаны `group.protocol=classic` и границы учебной модели;
- добавлены поведенческие тесты вычислений, таймеров, commit и rebalance.

Consumer Group Lab остаётся встроенной внутрь свободной песочницы. Records, созданные кнопкой лаборатории, получают реальные offsets общего Topic и отображаются в основной partition log.

В Consumer Group Lab доступны:

- до четырёх Consumer с одним `group.id`;
- состояния группы `EMPTY`, `REBALANCING` и `STABLE`;
- стратегии распределения `Range` и `Round Robin`;
- подключение, корректная остановка, crash, timeout-исключение и восстановление Consumer;
- медленная обработка, остановка `poll()` и потеря heartbeat;
- сжатые учебные таймеры `session.timeout.ms` и `max.poll.interval.ms`;
- `LEO`, `High Watermark`, `fetch position`, `processed offset`, `committed offset` и lag отдельно по P0–P2;
- периодический auto commit и ручной commit processed offsets;
- поток новых records и журнал причин каждого rebalance;
- QA-фокус с проверками назначения, восстановления и повторной обработки.

Справка 0.6.3 также сохранена и расширена для новой лаборатории:

- кнопка **«Подсказка»** во всех трёх режимах;
- подсветка кликабельных элементов;
- добровольное пошаговое объяснение текущего режима;
- без автозапуска, прогресса прохождения, напоминаний и сохранения просмотра.

Расширенный словарь теперь содержит 46 терминов:

- поиск по терминам и русским/английским синонимам;
- фильтрация по пяти категориям;
- разворачиваемые объяснения механики;
- примеры, связанные с Kafka Path;
- отдельный фокус «Что проверить QA».

- **Свободная песочница** — собственный event или локальный файл, topic, key,
  headers, payload и ручные настройки Kafka;
- **Учебные сценарии** — 10 готовых ситуаций из версии 0.4.0.1 с presets,
  объяснениями и ожидаемым результатом;
- **Конструктор** — собственная перетаскиваемая топология из Producer, Topic,
  Broker, Consumer и Database с ручным размещением partition replicas.

Песочница и учебные сценарии используют одну end-to-end цепочку:

```text
Producer → Topic / Partition → Leader / Followers → ACK
→ Consumer → Decode → Handler → service_db → Offset commit
```

## Что можно отправить

- event с собственными `topic`, `event name`, `key`, `headers` и `payload`;
- локальный текстовый, JSON, XML или бинарный файл;
- сообщение без key для демонстрации round-robin;
- несколько сообщений подряд для сравнения partitions и offsets.

## Учебные сценарии

- обычная доставка и последовательность событий с одинаковым key;
- сравнение `acks=0`, `acks=1` и `acks=all`;
- падение Leader, недостаточный ISR и возврат replica;
- потеря ACK с duplicate и с idempotent deduplication.

Preset сценария можно перенести в песочницу кнопкой
**«Изменить в песочнице»** и продолжить эксперимент вручную.

В песочнице четыре лаборатории работают независимо от
сценариев:

- **Sandbox Producer Settings** — `acks`, `replication.factor`,
  `min.insync.replicas`, brokers, retries и idempotence;
- **Network & Retry Lab** — нормальная сеть, потеря request или ACK, повторная
  отправка, duplicate и deduplication;
- **Cluster Resilience Lab** — управление Broker, Leader, ISR, lagging replica
  и failover.
- **Consumer Group Lab** — assignments, rebalance, poll, heartbeat, offsets,
  commit и lag в общей цепочке после Topic.

Файл не загружается на сервер. Текстовые данные до 256 KB читаются локально,
а для крупных или бинарных файлов симулятор формирует metadata record.

## Конструктор топологии

- готовый стенд или полностью пустой холст;
- добавление и перетаскивание Producer, Topic, Broker, Consumer и Database;
- направленные связи между узлами;
- настройка `acks`, retries, idempotence, partition count и
  `min.insync.replicas`;
- размещение Leader/Follower replicas внутри конкретных Broker и управление ISR;
- включение и остановка Broker;
- автоматическая проверка связей, Leader, ISR и Producer config;
- запуск event по фактически собранному маршруту;
- сохранение и загрузка читаемой конфигурации `.txt`.

## Наблюдение и ручное управление

- пошаговая state machine из 11+ этапов;
- реальные для симуляции `partition`, `offset`, Leader, ISR и ACK;
- Event, Delivery и Lifecycle Inspector;
- ручные `acks`, `replication.factor`, `min.insync.replicas`, retries и
  idempotence;
- остановка Broker, failover Leader, lagging replica и возврат в ISR;
- потеря request или ACK, retry, duplicate и deduplication;
- управление анимацией, скорость, горячие клавиши и режим фокуса.
- дополнительная кнопка отправки рядом с окном симуляции;
- FIFO-порядок records отдельно внутри каждой partition;
- визуальное сравнение Leader/Follower replicas, LEO и состава ISR;
- учебный Broker File System: `.log`, `.index` и `.timeindex`.

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
git commit -m "Release 0.7.3 topic broker UX"
git push origin main
```

После push GitHub самостоятельно выполнит `npm ci`, статический экспорт и
публикацию. Папки `node_modules` и `dist` добавлять в репозиторий не нужно.

## Основные файлы

- `app/kafka-path-simulator.tsx` — интерфейс и интерактивная логика;
- `app/consumer-group-lab.tsx` — интерфейс Consumer Group Lab;
- `app/consumer-group-model.ts` — reducer, offsets, таймеры и rebalance;
- `app/contextual-help.tsx` — добровольная справка и подсветка интерфейса;
- `app/topology-constructor.tsx` — холст, topology validation и запуск event;
- `app/simulator-model.ts` — модель Kafka, состояния и вычисления;
- `app/globals.css` — визуальная система и responsive layout;
- `app/page.tsx` — статическая оболочка страницы;
- `scripts/prepare-pages-output.mjs` — подготовка путей GitHub Pages.
