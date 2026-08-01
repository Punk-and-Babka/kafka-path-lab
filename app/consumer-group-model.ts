export type AssignmentStrategy = "range" | "round-robin";
export type CommitMode = "auto" | "manual";
export type GroupPhase = "EMPTY" | "REBALANCING" | "STABLE";
export type MemberStatus =
  | "active"
  | "slow"
  | "poll-paused"
  | "heartbeat-lost"
  | "crashed"
  | "stopped"
  | "excluded";

export type ConsumerMember = {
  id: string;
  name: string;
  status: MemberStatus;
  lastHeartbeat: number;
  lastPoll: number;
};

export type LabLogEntry = {
  id: number;
  time: number;
  tone: "info" | "success" | "warning";
  title: string;
  detail: string;
};

export type ConsumerLabState = {
  time: number;
  phase: GroupPhase;
  strategy: AssignmentStrategy;
  commitMode: CommitMode;
  members: ConsumerMember[];
  assignments: Array<string | null>;
  leo: number[];
  highWatermark: number[];
  fetchPosition: number[];
  processed: number[];
  committed: number[];
  sessionTimeout: number;
  maxPollInterval: number;
  autoCommitInterval: number;
  lastAutoCommit: number;
  rebalances: number;
  logSequence: number;
  log: LabLogEntry[];
};

export type ConsumerLabAction =
  | { type: "ADD_CONSUMER" }
  | { type: "SET_MEMBER_STATUS"; id: string; status: MemberStatus }
  | { type: "SET_STRATEGY"; strategy: AssignmentStrategy }
  | { type: "SET_COMMIT_MODE"; mode: CommitMode }
  | { type: "SET_SESSION_TIMEOUT"; value: number }
  | { type: "SET_MAX_POLL"; value: number }
  | { type: "TICK" }
  | { type: "COMMIT" }
  | { type: "FINISH_REBALANCE" }
  | { type: "SYNC_TOPIC"; leo: number[]; highWatermark: number[] }
  | { type: "RESET"; leo: number[]; highWatermark: number[] };

export const PARTITIONS = [0, 1, 2] as const;

export function isMemberOfGroup(member: ConsumerMember) {
  return member.status !== "stopped" && member.status !== "excluded";
}

export function assignmentsFor(
  memberIds: string[],
  strategy: AssignmentStrategy,
): Array<string | null> {
  if (!memberIds.length) return PARTITIONS.map(() => null);
  if (strategy === "round-robin") {
    return PARTITIONS.map((partition) => memberIds[partition % memberIds.length]);
  }

  const result: Array<string | null> = PARTITIONS.map(() => null);
  const base = Math.floor(PARTITIONS.length / memberIds.length);
  const remainder = PARTITIONS.length % memberIds.length;
  let partition = 0;
  memberIds.forEach((memberId, memberIndex) => {
    const count = base + (memberIndex < remainder ? 1 : 0);
    for (let index = 0; index < count; index += 1) {
      if (partition < PARTITIONS.length) result[partition] = memberId;
      partition += 1;
    }
  });
  return result;
}

function normalizeOffsets(offsets: number[]) {
  return PARTITIONS.map((partition) => Math.max(0, offsets[partition] ?? 0));
}

function appendLog(
  state: ConsumerLabState,
  tone: LabLogEntry["tone"],
  title: string,
  detail: string,
): ConsumerLabState {
  const nextSequence = state.logSequence + 1;
  return {
    ...state,
    logSequence: nextSequence,
    log: [{ id: nextSequence, time: state.time, tone, title, detail }, ...state.log].slice(0, 22),
  };
}

function beginRebalance(state: ConsumerLabState, reason: string): ConsumerLabState {
  const memberIds = state.members.filter(isMemberOfGroup).map((member) => member.id);
  const assignments = assignmentsFor(memberIds, state.strategy);
  const restart = state.committed.map((offset, partition) =>
    Math.min(offset, state.highWatermark[partition]));
  const phase: GroupPhase = memberIds.length ? "REBALANCING" : "EMPTY";
  return appendLog({
    ...state,
    phase,
    assignments,
    fetchPosition: [...restart],
    processed: [...restart],
    rebalances: state.rebalances + 1,
  }, "warning", memberIds.length ? "Начался eager rebalance" : "Группа стала EMPTY",
  `${reason} ${memberIds.length
    ? "Classic coordinator отзывает assignments; чтение возобновится с committed offsets."
    : "Ни одного участника группы не осталось."}`);
}

export function createInitialState(
  leoOffsets: number[],
  highWatermarkOffsets: number[] = leoOffsets,
): ConsumerLabState {
  const leo = normalizeOffsets(leoOffsets);
  const highWatermark = normalizeOffsets(highWatermarkOffsets).map((value, partition) =>
    Math.min(value, leo[partition]));
  return {
    time: 0,
    phase: "STABLE",
    strategy: "range",
    commitMode: "auto",
    members: [{ id: "consumer-1", name: "Consumer 1", status: "active", lastHeartbeat: 0, lastPoll: 0 }],
    assignments: ["consumer-1", "consumer-1", "consumer-1"],
    leo,
    highWatermark,
    fetchPosition: [...highWatermark],
    processed: [...highWatermark],
    committed: [...highWatermark],
    sessionTimeout: 5,
    maxPollInterval: 7,
    autoCommitInterval: 3,
    lastAutoCommit: 0,
    rebalances: 0,
    logSequence: 1,
    log: [{
      id: 1,
      time: 0,
      tone: "success",
      title: "Consumer Group готова",
      detail: "Consumer 1 получил P0–P2. Fetch position, processed и committed начинаются с consumer-visible High Watermark.",
    }],
  };
}

export function consumerLabReducer(
  state: ConsumerLabState,
  action: ConsumerLabAction,
): ConsumerLabState {
  if (action.type === "RESET") {
    return createInitialState(action.leo, action.highWatermark);
  }

  if (action.type === "SYNC_TOPIC") {
    const leo = normalizeOffsets(action.leo);
    const highWatermark = normalizeOffsets(action.highWatermark).map((value, partition) =>
      Math.min(value, leo[partition]));
    const topicReset = leo.some((value, partition) => value < state.leo[partition]);
    const added = leo.reduce((total, value, partition) =>
      total + Math.max(0, value - state.leo[partition]), 0);
    const visibilityAdvanced = highWatermark.reduce((total, value, partition) =>
      total + Math.max(0, value - state.highWatermark[partition]), 0);
    if (!topicReset && !added && !visibilityAdvanced) return state;

    const clamp = (offsets: number[]) => offsets.map((value, partition) =>
      Math.min(value, highWatermark[partition]));
    const next = {
      ...state,
      leo,
      highWatermark,
      fetchPosition: clamp(state.fetchPosition),
      processed: clamp(state.processed),
      committed: clamp(state.committed),
    };
    if (topicReset) {
      return appendLog(next, "warning", "Topic сброшен",
        "LEO и High Watermark синхронизированы с основной песочницей; локальные offsets безопасно ограничены новым концом журнала.");
    }
    return appendLog(next, "info", `${added} новых record в общем Topic`,
      `LEO обновлён; High Watermark продвинулся на ${visibilityAdvanced}. Consumer читает только до HW.`);
  }

  if (action.type === "ADD_CONSUMER") {
    if (state.members.length >= 4) return state;
    const index = state.members.length + 1;
    const member: ConsumerMember = {
      id: `consumer-${index}`,
      name: `Consumer ${index}`,
      status: "active",
      lastHeartbeat: state.time,
      lastPoll: state.time,
    };
    return beginRebalance({ ...state, members: [...state.members, member] },
      `${member.name} присоединился к group coordinator.`);
  }

  if (action.type === "SET_MEMBER_STATUS") {
    const current = state.members.find((member) => member.id === action.id);
    if (!current || current.status === action.status) return state;
    const wasMember = isMemberOfGroup(current);
    const members = state.members.map((member) => member.id === action.id ? {
      ...member,
      status: action.status,
      lastHeartbeat: action.status === "active" ? state.time : member.lastHeartbeat,
      lastPoll: action.status === "active" ? state.time : member.lastPoll,
    } : member);
    const nextMember = members.find((member) => member.id === action.id)!;
    const remainsMember = isMemberOfGroup(nextMember);
    const next = { ...state, members };
    if (wasMember !== remainsMember) {
      return beginRebalance(next, remainsMember
        ? `${nextMember.name} снова присоединился к группе.`
        : `${nextMember.name} ${action.status === "stopped" ? "выполнил корректный LeaveGroup" : "исключён coordinator"}.`);
    }
    if (action.status === "crashed") {
      return appendLog(next, "warning", `${nextMember.name}: PROCESS CRASHED`,
        `Heartbeat и poll() прекращены. Coordinator пока считает участника членом группы и исключит его после session.timeout.ms=${state.sessionTimeout} с.`);
    }
    return appendLog(next, action.status === "active" ? "success" : "warning",
      `${nextMember.name}: ${action.status.toUpperCase()}`,
      action.status === "poll-paused"
        ? "Heartbeat продолжается, но следующий poll() не вызывается."
        : action.status === "heartbeat-lost"
          ? "poll() продолжается, но heartbeat не достигает coordinator."
          : action.status === "slow"
            ? "Приложение обрабатывает уже полученные records медленнее."
            : "Consumer снова активен.");
  }

  if (action.type === "SET_STRATEGY") {
    if (state.strategy === action.strategy) return state;
    return beginRebalance({ ...state, strategy: action.strategy },
      `Assignor изменён на ${action.strategy === "range" ? "Range" : "Round Robin"}.`);
  }

  if (action.type === "SET_COMMIT_MODE") {
    if (state.commitMode === action.mode) return state;
    return appendLog({
      ...state,
      commitMode: action.mode,
      lastAutoCommit: state.time,
    }, "info", action.mode === "auto" ? "Включён auto commit" : "Включён manual commit",
    action.mode === "auto"
      ? `Auto commit срабатывает раз в ${state.autoCommitInterval} с и сохраняет fetch position; он может опередить бизнес-обработку.`
      : "Ручной commit сохраняет только processed offset после завершённой бизнес-обработки.");
  }

  if (action.type === "SET_SESSION_TIMEOUT") {
    return { ...state, sessionTimeout: action.value };
  }
  if (action.type === "SET_MAX_POLL") {
    return { ...state, maxPollInterval: action.value };
  }

  if (action.type === "COMMIT") {
    if (state.phase !== "STABLE") {
      return appendLog(state, "warning", "Commit отклонён",
        "RebalanceInProgressException: сначала завершите rebalance вызовом poll(), затем повторите commit.");
    }
    const committed = state.committed.map((current, partition) =>
      state.assignments[partition] ? state.processed[partition] : current);
    const changed = committed.some((value, partition) => value !== state.committed[partition]);
    return appendLog({ ...state, committed }, changed ? "success" : "info",
      changed ? "Processed offsets сохранены" : "Нечего сохранять",
      changed
        ? `__consumer_offsets: P0=${committed[0]}, P1=${committed[1]}, P2=${committed[2]}.`
        : "Committed offset уже совпадает с processed offset.");
  }

  if (action.type === "FINISH_REBALANCE") {
    if (state.phase !== "REBALANCING") return state;
    const assignmentText = PARTITIONS.map((partition) =>
      `P${partition}→${state.assignments[partition]?.replace("consumer-", "C") ?? "—"}`).join(" · ");
    return appendLog({ ...state, phase: "STABLE" }, "success", "Группа снова STABLE",
      `${assignmentText}. Новая fetch position начинается с committed offset.`);
  }

  if (action.type === "TICK") {
    if (state.phase !== "STABLE") return state;
    const time = state.time + 1;
    const exclusionReasons: string[] = [];
    const members = state.members.map((member) => {
      if (!isMemberOfGroup(member)) return member;
      let next = { ...member };
      if (!["heartbeat-lost", "crashed"].includes(member.status)) next.lastHeartbeat = time;
      if (!["poll-paused", "crashed"].includes(member.status)) next.lastPoll = time;
      if ((member.status === "heartbeat-lost" || member.status === "crashed")
        && time - member.lastHeartbeat >= state.sessionTimeout) {
        next = { ...next, status: "excluded" };
        exclusionReasons.push(`${member.name}: session.timeout.ms=${state.sessionTimeout} с`);
      } else if (member.status === "poll-paused"
        && time - member.lastPoll >= state.maxPollInterval) {
        next = { ...next, status: "excluded" };
        exclusionReasons.push(`${member.name}: max.poll.interval.ms=${state.maxPollInterval} с`);
      }
      return next;
    });

    if (exclusionReasons.length) {
      return beginRebalance({ ...state, time, members },
        `Coordinator исключил участника (${exclusionReasons.join("; ")}).`);
    }

    // Сначала приложение завершает часть records из предыдущего poll(), затем
    // poll() получает следующий batch. Поэтому fetch position может быть впереди
    // processed offset даже в одном учебном такте.
    const processed = state.processed.map((current, partition) => {
      const owner = members.find((member) => member.id === state.assignments[partition]);
      if (!owner || !isMemberOfGroup(owner) || owner.status === "crashed") return current;
      const rate = owner.status === "slow" ? (time % 2 === 0 ? 1 : 0) : 2;
      return Math.min(state.fetchPosition[partition], current + rate);
    });

    const fetchPosition = state.fetchPosition.map((current, partition) => {
      const owner = members.find((member) => member.id === state.assignments[partition]);
      if (!owner || !isMemberOfGroup(owner)
        || owner.status === "poll-paused" || owner.status === "crashed") return current;
      const rate = owner.status === "slow" ? 1 : 2;
      return Math.min(state.highWatermark[partition], current + rate);
    });

    const autoCommitDue = state.commitMode === "auto"
      && time - state.lastAutoCommit >= state.autoCommitInterval;
    const committed = autoCommitDue ? [...fetchPosition] : state.committed;
    const next = {
      ...state,
      time,
      members,
      processed,
      fetchPosition,
      committed,
      lastAutoCommit: autoCommitDue ? time : state.lastAutoCommit,
    };
    return autoCommitDue
      ? appendLog(next, "info", "Сработал auto commit",
        `enable.auto.commit сохранил fetch position по интервалу ${state.autoCommitInterval} с; сравните её с processed.`)
      : next;
  }

  return state;
}
