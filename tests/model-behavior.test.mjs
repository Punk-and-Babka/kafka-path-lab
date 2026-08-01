import assert from "node:assert/strict";
import test from "node:test";

import {
  classifyRecordAvailability,
  evaluateDelivery,
  resolvePartition,
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
