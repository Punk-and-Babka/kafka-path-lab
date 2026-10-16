import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { APP_VERSION } from "../app/version.ts";

const developmentPreviewMeta =
  /<meta(?=[^>]*\bname=["']codex-preview["'])(?=[^>]*\bcontent=["']development["'])[^>]*>/i;

test("renders development preview metadata", async () => {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);

  const response = await worker.fetch(
    new Request("http://localhost/", {
      headers: { accept: "text/html" },
    }),
    {
      ASSETS: {
        fetch: async () => new Response("Not found", { status: 404 }),
      },
    },
    {
      waitUntil() {},
      passThroughOnException() {},
    },
  );

  assert.equal(response.status, 200);
  assert.match(
    response.headers.get("content-type") ?? "",
    /^text\/html\b/i,
  );
  assert.match(await response.text(), developmentPreviewMeta);
});

test("renders the sandbox, guided-scenario, and constructor entry points", async () => {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("modes", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);

  const response = await worker.fetch(
    new Request("http://localhost/", { headers: { accept: "text/html" } }),
    { ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) } },
    { waitUntil() {}, passThroughOnException() {} },
  );
  const html = await response.text();

  assert.equal(response.status, 200);
  assert.match(html, new RegExp(`version ${APP_VERSION.replace(/\./g, "\.")}`));
  assert.match(html, /Свободная песочница/);
  assert.match(html, /Учебные сценарии/);
  assert.match(html, /Конструктор/);
  assert.match(html, /10 готовых ситуаций из версии 0\.4\.0\.1/);
  assert.match(html, /SANDBOX LABS/);
  assert.match(html, /Producer Settings/);
  assert.match(html, /Network (?:&|&amp;) Retry/);
  assert.match(html, /Cluster Resilience/);
  assert.match(html, /Consumer Group Lab/);
  assert.match(html, /Consumer Errors/);
  assert.match(html, /Подсказка/);
  // Сами стенды свёрнуты на старте, поэтому в разметке есть только кнопка,
  // которая их раскрывает, а не их содержимое.
  assert.match(html, /Показать лаборатории песочницы/);
  assert.doesNotMatch(html, /CONSUMER ERROR LAB/);
});

test("keeps the consumer error lab and dead letter topic behind the labs toggle", async () => {
  const source = await readFile(
    new URL("../app/kafka-path-simulator.tsx", import.meta.url),
    "utf8",
  );

  assert.match(source, /\{showAdvancedConfig && </);
  assert.match(source, /id="consumer-error-lab"/);
  assert.match(source, /CONSUMER ERROR LAB/);
  assert.match(source, /dead letter topic/);
});

test("explains logical Topic and physical Broker placement", async () => {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("placement-guide", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);

  const response = await worker.fetch(
    new Request("http://localhost/", { headers: { accept: "text/html" } }),
    { ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) } },
    { waitUntil() {}, passThroughOnException() {} },
  );
  const html = await response.text();

  assert.equal(response.status, 200);
  assert.match(html, /Topic — логическая структура, Brokers — физическое хранение/);
  assert.match(html, /Одинаковый цвет означает одну и ту же partition/);
  assert.match(html, /ЛОГИЧЕСКИЙ ВИД · TOPIC/);
  assert.match(html, /ФИЗИЧЕСКИЙ ВИД · BROKERS/);
});

test("includes topology construction, validation, event routing, and txt persistence", async () => {
  const source = await readFile(
    new URL("../app/topology-constructor.tsx", import.meta.url),
    "utf8",
  );

  assert.match(source, /application\/x-kafka-node/);
  assert.match(source, /Leader replica/);
  assert.match(source, /validateTopology/);
  assert.match(source, /simulateEvent/);
  assert.match(source, /kafka-path-topology/);
  assert.match(source, /Сохранить \.txt/);
  assert.match(source, /Загрузить \.txt/);
});

test("keeps constructor and simulator event markers isolated", async () => {
  const [constructorSource, simulatorSource, styles] = await Promise.all([
    readFile(new URL("../app/topology-constructor.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/kafka-path-simulator.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
  ]);

  assert.match(constructorSource, /className="constructor-event-orb"/);
  assert.doesNotMatch(constructorSource, /className="event-orb"/);
  assert.match(simulatorSource, /className={`event-orb stage-/);
  assert.match(styles, /\.constructor-event-orb\s*{[^}]*right:\s*-11px;/s);
  const simulatorOrbRules = styles.match(/\.event-orb\s*{[^}]*}/gs) ?? [];
  assert.ok(simulatorOrbRules.length > 0);
  assert.ok(simulatorOrbRules.every((rule) => !/\bright\s*:/.test(rule)));
});

test("includes the nearby send action and partition storage learning views", async () => {
  const source = await readFile(
    new URL("../app/kafka-path-simulator.tsx", import.meta.url),
    "utf8",
  );

  assert.match(source, /className="map-send-button"/);
  assert.match(source, /FIFO \/ partition/);
  assert.match(source, /Leader → Followers/);
  assert.match(source, /\.log/);
  assert.match(source, /\.index/);
  assert.match(source, /\.timeindex/);
});

test("includes the expanded searchable QA-oriented glossary", async () => {
  const [simulatorSource, glossarySource] = await Promise.all([
    readFile(new URL("../app/kafka-path-simulator.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/glossary-data.ts", import.meta.url), "utf8"),
  ]);

  assert.match(simulatorSource, /Поиск по словарю/);
  assert.match(simulatorSource, /className="glossary-cta"/);
  assert.match(simulatorSource, /Словарь Kafka/);
  assert.match(simulatorSource, /терминов с примерами/);
  assert.match(simulatorSource, /onOpenGlossary/);
  assert.match(simulatorSource, /glossaryCategory/);
  assert.match(simulatorSource, /Развернуть объяснение/);
  assert.match(simulatorSource, /Что проверить QA/);
  assert.match(glossarySource, /qaFocus/);
  assert.match(glossarySource, /Kafka гарантирует порядок records только в пределах одной partition/);
  assert.match(glossarySource, /At-least-once/);
  assert.match(glossarySource, /Log compaction/);
  assert.match(glossarySource, /Heartbeat \/ session timeout/);
  assert.match(glossarySource, /poll\(\) \/ max\.poll\.interval\.ms/);
});

test("keeps the learning copy consistent with the simulated Kafka version", async () => {
  const [simulatorSource, modelSource, glossarySource] = await Promise.all([
    readFile(new URL("../app/kafka-path-simulator.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/simulator-model.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/glossary-data.ts", import.meta.url), "utf8"),
  ]);
  const has = (source, fragment) => assert.ok(
    source.includes(fragment),
    `ожидался фрагмент: ${fragment}`,
  );
  const lacks = (source, fragment) => assert.ok(
    !source.includes(fragment),
    `фрагмент должен был исчезнуть: ${fragment}`,
  );

  // Невидимость record при ISR ниже min.insync.replicas — поведение Kafka 4.x
  // (KIP-966), а не универсальное правило: каждая формулировка это оговаривает.
  has(glossarySource, "KIP-966");
  has(glossarySource, "На кластере 3.x и старше такой record виден сразу");
  has(modelSource, "Лаборатория моделирует Kafka 4.x (KIP-966)");
  has(simulatorSource, "в Kafka 4.x видимость требует ISR ≥ min ISR");

  // Примеры словаря обязаны совпадать с фактическим partitioner лаборатории:
  // hash("order-8421") % 3 даёт P0, а не P1.
  has(glossarySource, "hash(order-8421) % 3 → P0");
  assert.ok(
    !glossarySource.split("\n").some((line) =>
      line.includes("order-8421") && line.includes("P1")),
    "пример с order-8421 больше не должен ссылаться на P1",
  );

  // acks=0 не ждёт ответа, поэтому его результат нельзя называть timeout.
  has(simulatorSource, '"NO ACK"');
  has(simulatorSource, "подтверждение не запрашивалось");

  // Выбор нового Leader уже реализован: обещаний будущих версий быть не должно.
  lacks(simulatorSource, "появится в версии");
});

test("includes the integrated Consumer Group Lab and its failure model", async () => {
  const [simulatorSource, consumerLabSource, consumerModelSource] = await Promise.all([
    readFile(new URL("../app/kafka-path-simulator.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/consumer-group-lab.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/consumer-group-model.ts", import.meta.url), "utf8"),
  ]);

  assert.match(simulatorSource, /<ConsumerGroupLab/);
  assert.match(simulatorSource, /consumerExternalLeo/);
  assert.match(simulatorSource, /consumerExternalHighWatermark/);
  assert.match(simulatorSource, /produceLabRecords/);
  assert.match(simulatorSource, /openSandboxLab\("consumer-group-lab"\)/);
  assert.match(consumerModelSource, /type GroupPhase = "EMPTY" \| "REBALANCING" \| "STABLE"/);
  assert.match(consumerModelSource, /"range" \| "round-robin"/);
  assert.match(consumerModelSource, /sessionTimeout/);
  assert.match(consumerModelSource, /maxPollInterval/);
  assert.match(consumerModelSource, /fetchPosition/);
  assert.match(consumerModelSource, /processed/);
  assert.match(consumerLabSource, /committed/);
  assert.match(consumerModelSource, /Consumer Group готова/);
  assert.match(consumerModelSource, /RebalanceInProgressException/);
  assert.match(consumerLabSource, /Журнал coordinator и commit/);
  assert.match(consumerLabSource, /Границы учебной модели/);
  assert.match(consumerLabSource, /На весь экран/);
  assert.match(consumerLabSource, /is-fullscreen/);
  assert.match(consumerLabSource, /event\.key === "Escape"/);
});

test("keeps contextual help voluntary and available in every mode", async () => {
  const [simulatorSource, constructorSource, helpSource] = await Promise.all([
    readFile(new URL("../app/kafka-path-simulator.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/topology-constructor.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/contextual-help.tsx", import.meta.url), "utf8"),
  ]);

  assert.match(simulatorSource, /mode=\{isGuided \? "guided" : "sandbox"\}/);
  assert.match(simulatorSource, /mode="constructor"/);
  assert.match(constructorSource, /onOpenHelp/);
  assert.match(helpSource, /Показать, где можно нажать/);
  assert.match(helpSource, /Объяснить текущий режим/);
  assert.match(helpSource, /Справка запускается только по вашему запросу/);
  assert.doesNotMatch(helpSource, /localStorage/);
  assert.doesNotMatch(helpSource, /ШАГ \{stepIndex/);
});

test("the interface version matches the published package version", async () => {
  const manifest = JSON.parse(await readFile(
    new URL("../package.json", import.meta.url),
    "utf8",
  ));

  assert.equal(manifest.version, APP_VERSION);
});
