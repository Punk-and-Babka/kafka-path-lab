import assert from "node:assert/strict";
import test from "node:test";

import {
  consumerForPartition,
  consumerGroupsForTopic,
  isSavedTopology,
  presetEdges,
  presetNodes,
  validateTopology,
} from "../app/topology-model.ts";
import { resolvePartition } from "../app/simulator-model.ts";

const consumer = (id, groupId) => ({
  id,
  kind: "consumer",
  label: id,
  x: 0,
  y: 0,
  config: { groupId, autoCommit: false },
});

const subscription = (id) => ({ id: `edge-${id}`, from: "topic-1", to: id });

test("the shipped preset topology validates without errors", () => {
  const issues = validateTopology(presetNodes(), presetEdges());
  assert.deepEqual(issues.filter((issue) => issue.level === "error"), []);
});

test("a leader on a stopped broker is reported as an error", () => {
  const nodes = presetNodes().map((node) => node.id === "broker-1"
    ? { ...node, config: { ...node.config, online: false } }
    : node);
  const errors = validateTopology(nodes, presetEdges())
    .filter((issue) => issue.level === "error");

  assert.ok(errors.some((issue) => /Leader находится на выключенном Broker/.test(issue.message)));
});

test("consumers sharing a group.id form one group", () => {
  const consumers = [consumer("c1", "workers"), consumer("c2", "workers")];
  const groups = consumerGroupsForTopic("topic-1", consumers, consumers.map((node) => subscription(node.id)));

  assert.equal(groups.length, 1);
  assert.equal(groups[0].members.length, 2);
  assert.equal(groups[0].standalone, false);
});

test("each distinct group.id receives its own copy of the record", () => {
  const consumers = [consumer("c1", "billing"), consumer("c2", "analytics")];
  const groups = consumerGroupsForTopic("topic-1", consumers, consumers.map((node) => subscription(node.id)));

  assert.equal(groups.length, 2);
  assert.deepEqual(groups.map((group) => group.groupId), ["billing", "analytics"]);
});

test("a consumer without group.id is treated as standalone", () => {
  const consumers = [consumer("c1", "workers"), consumer("c2", "")];
  const groups = consumerGroupsForTopic("topic-1", consumers, consumers.map((node) => subscription(node.id)));

  assert.equal(groups.length, 2);
  assert.equal(groups[1].standalone, true);
});

test("inside one group a partition belongs to exactly one member", () => {
  const consumers = [consumer("c1", "workers"), consumer("c2", "workers")];
  const [group] = consumerGroupsForTopic("topic-1", consumers, consumers.map((node) => subscription(node.id)));

  assert.equal(consumerForPartition(group, 0).id, "c1");
  assert.equal(consumerForPartition(group, 1).id, "c2");
  assert.equal(consumerForPartition(group, 2).id, "c1");
});

test("a group with more consumers than partitions is flagged", () => {
  const extra = ["c1", "c2", "c3", "c4"].map((id) => consumer(id, "workers"));
  const nodes = [...presetNodes().filter((node) => node.kind !== "consumer"), ...extra];
  const edges = [...presetEdges(), ...extra.map((node) => subscription(node.id))];
  const warnings = validateTopology(nodes, edges).filter((issue) => issue.level === "warning");

  assert.ok(warnings.some((issue) => /4 Consumer на 3 partitions/.test(issue.message)));
});

test("the constructor routes keys through the shared sandbox partitioner", () => {
  assert.equal(resolvePartition("order-8421", 0, 3), resolvePartition("order-8421", 0));
  assert.equal(resolvePartition("", 4, 3), 1);
  assert.equal(resolvePartition("order-8421", 0, 12), resolvePartition("order-8421", 0, 12));
});

test("import rejects files that are not constructor topologies", () => {
  assert.equal(isSavedTopology({ format: "something-else", version: 1, nodes: [], edges: [] }), false);
  assert.equal(isSavedTopology({ format: "kafka-path-topology", version: 2, nodes: [], edges: [] }), false);
  assert.equal(isSavedTopology({ format: "kafka-path-topology", version: 1, nodes: [], edges: [] }), true);
});
