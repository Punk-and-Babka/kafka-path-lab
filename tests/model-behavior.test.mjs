import assert from "node:assert/strict";
import test from "node:test";

import {
  classifyRecordAvailability,
  evaluateDelivery,
  partitionRuntime,
  replicaRoleLabel,
  resolvePartition,
  stepDisposition,
  stepOrderForConfig,
} from "../app/simulator-model.ts";
import {
  assignmentsFor,
  consumerLabReducer,
  createInitialState,
} from "../app/consumer-group-model.ts";

const healthyRuntime = {
  onlineBrokers: [1, 2, 3],
  laggingReplicas: [],
  leaders: [1, 2, 3],
};

test("acks=0 keeps a lost request unconfirmed and writes no record", () => {
  const result = evaluateDelivery({
    acks: "0",
    replicationFactor: 2,
    minInSyncReplicas: 2,
    availableBrokers: 3,
    retries: 5,
    idempotence: false,
  }, 0, healthyRuntime, "request-lost");

  assert.equal(result.producerResult, "unconfirmed");
  assert.equal(result.faultApplied, "request-lost");
  assert.equal(result.leaderAppended, false);
  assert.equal(result.recordsWritten, 0);
  assert.equal(result.attempts, 1);
  assert.equal(result.ambiguousResult, true);
});

test("acks=1 may ACK an append that is still below the consumer-visible watermark", () => {
  const result = evaluateDelivery({
    acks: "1",
    replicationFactor: 1,
    minInSyncReplicas: 2,
    availableBrokers: 1,
    retries: 0,
    idempotence: false,
  }, 0, {
    onlineBrokers: [1],
    laggingReplicas: [],
    leaders: [1, 2, 3],
  });

  assert.equal(result.leaderAppended, true);
  assert.equal(result.producerResult, "ack");
  assert.equal(result.currentIsr, 1);
  assert.equal(result.recordCommitted, false);
});

test("acks=all rejects the same insufficient ISR before append", () => {
  const result = evaluateDelivery({
    acks: "all",
    replicationFactor: 1,
    minInSyncReplicas: 2,
    availableBrokers: 1,
    retries: 0,
    idempotence: false,
  }, 0, {
    onlineBrokers: [1],
    laggingReplicas: [],
    leaders: [1, 2, 3],
  });

  assert.equal(result.leaderAppended, false);
  assert.equal(result.producerResult, "error");
  assert.equal(result.errorCode, "NotEnoughReplicas");
});

test("keyless partition preview and send use the same cursor", () => {
  assert.equal(resolvePartition("", 0), 0);
  assert.equal(resolvePartition("", 1), 1);
  assert.equal(resolvePartition("", 2), 2);
  assert.equal(resolvePartition("", 3), 0);
});

test("an offline replica is unavailable rather than physically lost", () => {
  assert.equal(classifyRecordAvailability(true, 2, 0), "UNAVAILABLE");
  assert.equal(classifyRecordAvailability(true, 2, 1), "AVAILABLE");
  assert.equal(classifyRecordAvailability(true, 0, 0), "LOST");
  assert.equal(classifyRecordAvailability(false, 0, 0), "NOT_APPENDED");
});

test("Range and Round Robin assignments differ for three partitions and two consumers", () => {
  assert.deepEqual(assignmentsFor(["c1", "c2"], "range"), ["c1", "c1", "c2"]);
  assert.deepEqual(assignmentsFor(["c1", "c2"], "round-robin"), ["c1", "c2", "c1"]);
});

test("Crash waits for session timeout before coordinator exclusion", () => {
  let state = createInitialState([10, 10, 10]);
  state = consumerLabReducer(state, {
    type: "SET_MEMBER_STATUS",
    id: "consumer-1",
    status: "crashed",
  });

  assert.equal(state.phase, "STABLE");
  assert.equal(state.rebalances, 0);
  assert.equal(state.members[0].status, "crashed");

  for (let second = 1; second < state.sessionTimeout; second += 1) {
    state = consumerLabReducer(state, { type: "TICK" });
    assert.equal(state.phase, "STABLE");
  }
  state = consumerLabReducer(state, { type: "TICK" });

  assert.equal(state.members[0].status, "excluded");
  assert.equal(state.phase, "EMPTY");
  assert.equal(state.rebalances, 1);
});

test("poll advances fetch position before business processing completes", () => {
  let state = createInitialState([5, 5, 5]);
  state = consumerLabReducer(state, {
    type: "SYNC_TOPIC",
    leo: [11, 5, 5],
    highWatermark: [11, 5, 5],
  });
  state = consumerLabReducer(state, { type: "TICK" });

  assert.equal(state.fetchPosition[0], 7);
  assert.equal(state.processed[0], 5);
  assert.equal(state.committed[0], 5);
});

test("auto commit is periodic and can advance ahead of processed records", () => {
  let state = createInitialState([5, 5, 5]);
  state = consumerLabReducer(state, {
    type: "SYNC_TOPIC",
    leo: [11, 5, 5],
    highWatermark: [11, 5, 5],
  });
  state = consumerLabReducer(state, { type: "TICK" });
  state = consumerLabReducer(state, { type: "TICK" });
  assert.equal(state.committed[0], 5);
  state = consumerLabReducer(state, { type: "TICK" });

  assert.equal(state.fetchPosition[0], 11);
  assert.equal(state.processed[0], 9);
  assert.equal(state.committed[0], 11);
});

test("manual commit stores processed offset and is rejected during rebalance", () => {
  let state = createInitialState([5, 5, 5]);
  state = consumerLabReducer(state, { type: "SET_COMMIT_MODE", mode: "manual" });
  state = consumerLabReducer(state, {
    type: "SYNC_TOPIC",
    leo: [11, 5, 5],
    highWatermark: [11, 5, 5],
  });
  state = consumerLabReducer(state, { type: "TICK" });
  state = consumerLabReducer(state, { type: "TICK" });
  state = consumerLabReducer(state, { type: "COMMIT" });
  assert.equal(state.committed[0], state.processed[0]);
  assert.equal(state.committed[0], 7);

  state = consumerLabReducer(state, { type: "ADD_CONSUMER" });
  const committedBeforeRejectedCall = [...state.committed];
  state = consumerLabReducer(state, { type: "COMMIT" });
  assert.deepEqual(state.committed, committedBeforeRejectedCall);
  assert.match(state.log[0].detail, /RebalanceInProgressException/);
});

test("topic synchronization is authoritative and can reset LEO instead of taking max", () => {
  let state = createInitialState([20, 20, 20]);
  state = consumerLabReducer(state, {
    type: "SYNC_TOPIC",
    leo: [8, 9, 10],
    highWatermark: [8, 9, 10],
  });

  assert.deepEqual(state.leo, [8, 9, 10]);
  assert.deepEqual(state.highWatermark, [8, 9, 10]);
  assert.deepEqual(state.fetchPosition, [8, 9, 10]);
  assert.deepEqual(state.committed, [8, 9, 10]);
});

test("a partition without a live leader labels no replica as Leader", () => {
  const dead = partitionRuntime(0, 2, {
    onlineBrokers: [],
    laggingReplicas: [],
    leaders: [1, 2, 3],
  });

  assert.equal(dead.leaderOnline, false);
  assert.deepEqual(
    dead.assignedReplicas.map((broker) => replicaRoleLabel(dead, broker)),
    ["Offline", "Offline"],
  );
});

test("a lagging replica stays online but loses the Follower role in ISR terms", () => {
  const lagging = partitionRuntime(0, 3, {
    onlineBrokers: [1, 2, 3],
    laggingReplicas: ["0:2"],
    leaders: [1, 2, 3],
  });

  assert.equal(replicaRoleLabel(lagging, 1), "Leader");
  assert.equal(replicaRoleLabel(lagging, 2), "Follower");
  assert.deepEqual(lagging.isrBrokers, [1, 3]);
});

test("slow processing eventually breaches max.poll.interval.ms", () => {
  let state = createInitialState([5, 5, 5]);
  state = consumerLabReducer(state, {
    type: "SYNC_TOPIC",
    leo: [60, 60, 60],
    highWatermark: [60, 60, 60],
  });
  state = consumerLabReducer(state, { type: "SET_MAX_POLL", value: 5 });
  state = consumerLabReducer(state, {
    type: "SET_MEMBER_STATUS",
    id: "consumer-1",
    status: "slow",
  });

  // Heartbeat продолжает идти, поэтому session.timeout.ms не срабатывает:
  // участника исключает именно незавершённая обработка batch.
  for (let tick = 0; tick < state.maxPollInterval + 2; tick += 1) {
    state = consumerLabReducer(state, { type: "TICK" });
  }

  assert.equal(state.members[0].status, "excluded");
  assert.equal(state.phase, "EMPTY");
  assert.match(state.log[0].detail, /max\.poll\.interval\.ms/);
  assert.match(state.log[0].detail, /handler не завершил предыдущий batch/);
});

test("a slow consumer that caught up keeps polling and stays in the group", () => {
  let state = createInitialState([5, 5, 5]);
  state = consumerLabReducer(state, {
    type: "SET_MEMBER_STATUS",
    id: "consumer-1",
    status: "slow",
  });

  // Новых records нет: backlog пуст, poll() продолжает вызываться.
  for (let tick = 0; tick < state.maxPollInterval + 3; tick += 1) {
    state = consumerLabReducer(state, { type: "TICK" });
  }

  assert.equal(state.members[0].status, "slow");
  assert.equal(state.phase, "STABLE");
});

test("auto commit only stores offsets of assigned partitions", () => {
  let state = createInitialState([5, 5, 5]);
  state = consumerLabReducer(state, {
    type: "SYNC_TOPIC",
    leo: [11, 11, 11],
    highWatermark: [11, 11, 11],
  });
  state = { ...state, assignments: ["consumer-1", null, "consumer-1"] };

  for (let tick = 0; tick < state.autoCommitInterval; tick += 1) {
    state = consumerLabReducer(state, { type: "TICK" });
  }

  assert.ok(state.committed[0] > 5);
  assert.equal(state.committed[1], 5);
  assert.ok(state.committed[2] > 5);
});

const deliveryConfig = (overrides = {}) => ({
  acks: "all",
  replicationFactor: 2,
  minInSyncReplicas: 2,
  availableBrokers: 3,
  retries: 3,
  idempotence: true,
  ...overrides,
});

/** Собирает event так же, как это делает песочница при отправке. */
const eventFor = (config, faultMode = "none", runtime = healthyRuntime, partition = 0) => ({
  partition,
  delivery: config,
  faultMode,
  stage: 0,
  stepOrder: stepOrderForConfig(config, faultMode),
  result: evaluateDelivery(config, partition, runtime, faultMode),
});

const orderIndex = (order, step) => order.indexOf(step);

test("acks=1 acknowledges before replication, acks=all only after commit", () => {
  const leaderOnly = stepOrderForConfig(deliveryConfig({ acks: "1", idempotence: false }));
  const fullIsr = stepOrderForConfig(deliveryConfig());

  assert.ok(orderIndex(leaderOnly, "producerAck") < orderIndex(leaderOnly, "replication"));
  assert.ok(orderIndex(leaderOnly, "producerAck") < orderIndex(leaderOnly, "committed"));
  assert.ok(orderIndex(fullIsr, "committed") < orderIndex(fullIsr, "producerAck"));
});

test("a lost request without retries never reaches the consumer", () => {
  const order = stepOrderForConfig(
    deliveryConfig({ retries: 0, idempotence: false }),
    "request-lost",
  );

  assert.deepEqual(order, ["producerSend", "partitioning", "networkTimeout", "producerAck"]);
  assert.equal(order.includes("leaderAppend"), false);
  assert.equal(order.includes("consumerFetch"), false);
});

test("a lost request with retries appends only after the retry", () => {
  const order = stepOrderForConfig(deliveryConfig(), "request-lost");

  assert.ok(orderIndex(order, "networkTimeout") < orderIndex(order, "retrySend"));
  assert.ok(orderIndex(order, "retrySend") < orderIndex(order, "leaderAppend"));
  assert.ok(orderIndex(order, "leaderAppend") < orderIndex(order, "offsetCommit"));
});

test("a lost ACK happens after the record is already committed", () => {
  const order = stepOrderForConfig(deliveryConfig(), "ack-lost");

  assert.ok(orderIndex(order, "committed") < orderIndex(order, "networkTimeout"));
  assert.ok(orderIndex(order, "networkTimeout") < orderIndex(order, "retryResolution"));
  assert.ok(orderIndex(order, "retryResolution") < orderIndex(order, "producerAck"));
});

test("acks=0 ignores a lost ACK because no ACK is awaited", () => {
  const config = deliveryConfig({ acks: "0", idempotence: false });

  assert.deepEqual(
    stepOrderForConfig(config, "ack-lost"),
    stepOrderForConfig(config, "none"),
  );
  assert.equal(evaluateDelivery(config, 0, healthyRuntime, "ack-lost").faultApplied, "none");
});

test("an invalid producer config fails at send and skips every later step", () => {
  const event = eventFor(deliveryConfig({ acks: "1" }));

  assert.equal(event.result.configValid, false);
  assert.equal(stepDisposition(event, "producerSend"), "failed");
  assert.equal(stepDisposition(event, "leaderAppend"), "skipped");
  assert.equal(stepDisposition(event, "offsetCommit"), "skipped");
});

test("acks=0 skips the ACK step instead of failing it", () => {
  const event = eventFor(deliveryConfig({ acks: "0", idempotence: false }));

  assert.equal(stepDisposition(event, "producerAck"), "skipped");
  assert.equal(stepDisposition(event, "leaderAppend"), "success");
});

test("an append below the watermark succeeds but stops the consumer steps", () => {
  const event = eventFor(
    deliveryConfig({ acks: "1", replicationFactor: 1, minInSyncReplicas: 2, idempotence: false }),
    "none",
    { onlineBrokers: [1], laggingReplicas: [], leaders: [1, 2, 3] },
  );

  assert.equal(event.result.leaderAppended, true);
  assert.equal(event.result.recordCommitted, false);
  assert.equal(stepDisposition(event, "leaderAppend"), "success");
  assert.equal(stepDisposition(event, "committed"), "skipped");
  assert.equal(stepDisposition(event, "consumerFetch"), "skipped");
  assert.equal(stepDisposition(event, "sinkWrite"), "skipped");
});

test("the dedup step only runs when a retry actually produced or suppressed a copy", () => {
  const duplicating = eventFor(
    deliveryConfig({ acks: "1", idempotence: false }),
    "ack-lost",
  );
  const clean = eventFor(deliveryConfig());

  assert.equal(duplicating.result.duplicateWritten, true);
  assert.equal(stepDisposition(duplicating, "retryResolution"), "success");
  assert.equal(stepDisposition(clean, "retryResolution"), "skipped");
});
