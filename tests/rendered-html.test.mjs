import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

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
  assert.match(html, /version 0\.6\.0/);
  assert.match(html, /Свободная песочница/);
  assert.match(html, /Учебные сценарии/);
  assert.match(html, /Конструктор/);
  assert.match(html, /10 готовых ситуаций из версии 0\.4\.0\.1/);
  assert.match(html, /SANDBOX LABS/);
  assert.match(html, /Producer Settings/);
  assert.match(html, /Network (?:&|&amp;) Retry/);
  assert.match(html, /Cluster Resilience/);
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
