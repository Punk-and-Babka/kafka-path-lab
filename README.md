# Kafka Path 0.7.1 — крупный UI Consumer Group Lab

Интерактивная лаборатория Kafka на React, TypeScript и Vinext.

Версия 0.7.1 делает Consumer Group Lab заметно крупнее и удобнее для чтения без масштабирования браузера. Добавлен полноэкранный режим, увеличены шрифты, элементы управления, значения offsets и lag, карточки Consumer и журнал событий. Функциональность версии 0.7.0 сохранена без изменений.

UI-изменения 0.7.1:

- полноэкранный режим лаборатории с выходом по `Escape`;
- шрифты и ключевые значения увеличены примерно на 30–40%;
- кнопки и поля получили более крупные области нажатия;
- Topic, карточки Consumer и Offset Matrix используют больше полезной ширины;
- журнал rebalance и QA-подсказки читаются без увеличения страницы;
- адаптивная компоновка раньше переходит в вертикальный режим на узких экранах.

Consumer Group Lab по-прежнему встроена внутрь свободной песочницы, не создавая отдельный верхнеуровневый режим. Topic, partitions и записи основной end-to-end цепочки связаны с лабораторией Consumer.

В Consumer Group Lab доступны:

- до четырёх Consumer с одним `group.id`;
- состояния группы `EMPTY`, `REBALANCING` и `STABLE`;
- стратегии распределения `Range` и `Round Robin`;
- подключение, корректная остановка, crash и восстановление Consumer;
- медленная обработка, остановка `poll()` и потеря heartbeat;
- сжатые учебные таймеры `session.timeout.ms` и `max.poll.interval.ms`;
- `LEO`, текущая `position`, `committed offset`, uncommitted records и lag отдельно по P0–P2;
- автоматический и ручной commit;
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
git commit -m "Release 0.7.1 consumer lab UI"
git push origin main
```

После push GitHub самостоятельно выполнит `npm ci`, статический экспорт и
публикацию. Папки `node_modules` и `dist` добавлять в репозиторий не нужно.

## Основные файлы

- `app/kafka-path-simulator.tsx` — интерфейс и интерактивная логика;
- `app/consumer-group-lab.tsx` — модель и интерфейс Consumer Group Lab;
- `app/contextual-help.tsx` — добровольная справка и подсветка интерфейса;
- `app/topology-constructor.tsx` — холст, topology validation и запуск event;
- `app/simulator-model.ts` — модель Kafka, состояния и вычисления;
- `app/globals.css` — визуальная система и responsive layout;
- `app/page.tsx` — статическая оболочка страницы;
- `scripts/prepare-pages-output.mjs` — подготовка путей GitHub Pages.
