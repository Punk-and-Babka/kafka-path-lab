"use client";

import {
  Activity, AlertTriangle, ArrowRight, Check, ChevronRight, CirclePause,
  CirclePlay, Clock3, Gauge, HeartPulse, Info, Pause, Play, Plus, Power,
  RefreshCw, RotateCcw, Save, ServerCrash, SlidersHorizontal, TimerReset,
  Users, WifiOff, X,
} from "lucide-react";
import { useEffect, useMemo, useReducer, useState } from "react";

type AssignmentStrategy = "range" | "round-robin";
type CommitMode = "auto" | "manual";
type GroupPhase = "EMPTY" | "REBALANCING" | "STABLE";
type MemberStatus =
  | "active"
  | "slow"
  | "poll-paused"
  | "heartbeat-lost"
  | "stopped"
  | "crashed";

type ConsumerMember = {
  id: string;
  name: string;
  status: MemberStatus;
  lastHeartbeat: number;
  lastPoll: number;
};

type LabLogEntry = {
  id: number;
  time: number;
  tone: "info" | "success" | "warning";
  title: string;
  detail: string;
};

type ConsumerLabState = {
  time: number;
  phase: GroupPhase;
  strategy: AssignmentStrategy;
  commitMode: CommitMode;
  members: ConsumerMember[];
  assignments: Array<string | null>;
  leo: number[];
  position: number[];
  committed: number[];
  sessionTimeout: number;
  maxPollInterval: number;
  generatedCursor: number;
  rebalances: number;
  logSequence: number;
  log: LabLogEntry[];
};

type ConsumerLabAction =
  | { type: "ADD_CONSUMER" }
  | { type: "SET_MEMBER_STATUS"; id: string; status: MemberStatus }
  | { type: "SET_STRATEGY"; strategy: AssignmentStrategy }
  | { type: "SET_COMMIT_MODE"; mode: CommitMode }
  | { type: "SET_SESSION_TIMEOUT"; value: number }
  | { type: "SET_MAX_POLL"; value: number }
  | { type: "GENERATE"; count: number }
  | { type: "TICK" }
  | { type: "COMMIT" }
  | { type: "FINISH_REBALANCE" }
  | { type: "SYNC_EXTERNAL"; offsets: number[] }
  | { type: "RESET"; offsets: number[] };

const PARTITIONS = [0, 1, 2] as const;
const MEMBER_COLORS = ["violet", "mint", "amber", "cyan"] as const;

const statusCopy: Record<MemberStatus, { label: string; hint: string }> = {
  active: { label: "ACTIVE", hint: "heartbeat и poll() работают" },
  slow: { label: "SLOW", hint: "обработка ограничена" },
  "poll-paused": { label: "POLL PAUSED", hint: "heartbeat есть, poll() остановлен" },
  "heartbeat-lost": { label: "NO HEARTBEAT", hint: "poll() идёт, coordinator не видит heartbeat" },
  stopped: { label: "STOPPED", hint: "корректно вышел из группы" },
  crashed: { label: "EXCLUDED", hint: "исключён после сбоя или timeout" },
};

function isMemberOfGroup(member: ConsumerMember) {
  return member.status !== "stopped" && member.status !== "crashed";
}

function assignmentsFor(
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
    log: [{ id: nextSequence, time: state.time, tone, title, detail }, ...state.log].slice(0, 18),
  };
}

function beginRebalance(state: ConsumerLabState, reason: string): ConsumerLabState {
  const memberIds = state.members.filter(isMemberOfGroup).map((member) => member.id);
  const assignments = assignmentsFor(memberIds, state.strategy);
  const position = state.position.map((current, partition) =>
    state.assignments[partition] !== assignments[partition]
      ? state.committed[partition]
      : current);
  const phase: GroupPhase = memberIds.length ? "REBALANCING" : "EMPTY";
  return appendLog({
    ...state,
    phase,
    assignments,
    position,
    rebalances: state.rebalances + 1,
  }, "warning", memberIds.length ? "Начался rebalance" : "Группа стала EMPTY",
  `${reason} ${memberIds.length
    ? "Обработка приостановлена, partitions будут переназначены."
    : "Ни одного активного Consumer не осталось."}`);
}

function createInitialState(offsets: number[]): ConsumerLabState {
  const normalized = PARTITIONS.map((partition) => offsets[partition] ?? 0);
  return {
    time: 0,
    phase: "STABLE",
    strategy: "range",
    commitMode: "auto",
    members: [{ id: "consumer-1", name: "Consumer 1", status: "active", lastHeartbeat: 0, lastPoll: 0 }],
    assignments: ["consumer-1", "consumer-1", "consumer-1"],
    leo: normalized,
    position: [...normalized],
    committed: [...normalized],
    sessionTimeout: 5,
    maxPollInterval: 7,
    generatedCursor: 0,
    rebalances: 0,
    logSequence: 1,
    log: [{
      id: 1,
      time: 0,
      tone: "success",
      title: "Consumer Group готова",
      detail: "Consumer 1 получил P0, P1 и P2. Начальные position и committed offset совпадают с LEO.",
    }],
  };
}

function reducer(state: ConsumerLabState, action: ConsumerLabAction): ConsumerLabState {
  if (action.type === "RESET") return createInitialState(action.offsets);

  if (action.type === "SYNC_EXTERNAL") {
    const leo = state.leo.map((current, partition) =>
      Math.max(current, action.offsets[partition] ?? current));
    const added = leo.reduce((total, value, partition) =>
      total + Math.max(0, value - state.leo[partition]), 0);
    if (!added) return state;
    return appendLog({ ...state, leo }, "info", `${added} record из основной цепочки`,
      "LEO обновлён: записи, отправленные через Producer песочницы, теперь видны Consumer Group.");
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
        ? `${nextMember.name} вернулся в группу.`
        : `${nextMember.name} ${action.status === "stopped" ? "корректно остановлен" : "аварийно отключён"}.`);
    }
    return appendLog(next, action.status === "active" ? "success" : "warning",
      `${nextMember.name}: ${statusCopy[action.status].label}`,
      statusCopy[action.status].hint);
  }

  if (action.type === "SET_STRATEGY") {
    if (state.strategy === action.strategy) return state;
    return beginRebalance({ ...state, strategy: action.strategy },
      `Assignor изменён на ${action.strategy === "range" ? "Range" : "Round Robin"}.`);
  }

  if (action.type === "SET_COMMIT_MODE") {
    if (state.commitMode === action.mode) return state;
    return appendLog({ ...state, commitMode: action.mode }, "info",
      action.mode === "auto" ? "Включён auto commit" : "Включён manual commit",
      action.mode === "auto"
        ? "После каждого такта committed offset догоняет текущую position."
        : "Position будет двигаться отдельно; offset сохранится только по кнопке Commit.");
  }

  if (action.type === "SET_SESSION_TIMEOUT") {
    return { ...state, sessionTimeout: action.value };
  }
  if (action.type === "SET_MAX_POLL") {
    return { ...state, maxPollInterval: action.value };
  }

  if (action.type === "GENERATE") {
    const leo = [...state.leo];
    for (let index = 0; index < action.count; index += 1) {
      const partition = (state.generatedCursor + index) % PARTITIONS.length;
      leo[partition] += 1;
    }
    return appendLog({
      ...state,
      leo,
      generatedCursor: (state.generatedCursor + action.count) % PARTITIONS.length,
    }, "info", `Producer добавил ${action.count} records`,
    `LEO: P0=${leo[0]}, P1=${leo[1]}, P2=${leo[2]}. Lag растёт до обработки и commit.`);
  }

  if (action.type === "COMMIT") {
    const committed = state.committed.map((current, partition) =>
      state.assignments[partition] ? state.position[partition] : current);
    const changed = committed.some((value, partition) => value !== state.committed[partition]);
    return appendLog({ ...state, committed }, changed ? "success" : "info",
      changed ? "Offsets сохранены" : "Нечего сохранять",
      changed
        ? `__consumer_offsets: P0=${committed[0]}, P1=${committed[1]}, P2=${committed[2]}.`
        : "Committed offset уже совпадает с текущей position.");
  }

  if (action.type === "FINISH_REBALANCE") {
    if (state.phase !== "REBALANCING") return state;
    const assignmentText = PARTITIONS.map((partition) =>
      `P${partition}→${state.assignments[partition]?.replace("consumer-", "C") ?? "—"}`).join(" · ");
    return appendLog({ ...state, phase: "STABLE" }, "success", "Группа снова STABLE",
      `${assignmentText}. Новые владельцы начинают с committed offset.`);
  }

  if (action.type === "TICK") {
    if (state.phase !== "STABLE") return state;
    const time = state.time + 1;
    let excludedReason = "";
    const members = state.members.map((member) => {
      if (!isMemberOfGroup(member)) return member;
      let next = { ...member };
      if (member.status !== "heartbeat-lost") next.lastHeartbeat = time;
      if (member.status !== "poll-paused") next.lastPoll = time;
      if (member.status === "heartbeat-lost"
        && time - member.lastHeartbeat >= state.sessionTimeout) {
        next = { ...next, status: "crashed" };
        excludedReason = `${member.name} исключён: session.timeout.ms=${state.sessionTimeout} с превышен.`;
      }
      if (member.status === "poll-paused"
        && time - member.lastPoll >= state.maxPollInterval) {
        next = { ...next, status: "crashed" };
        excludedReason = `${member.name} исключён: max.poll.interval.ms=${state.maxPollInterval} с превышен.`;
      }
      return next;
    });

    const position = state.position.map((current, partition) => {
      const owner = members.find((member) => member.id === state.assignments[partition]);
      if (!owner || owner.status === "poll-paused" || !isMemberOfGroup(owner)) return current;
      const rate = owner.status === "slow" ? (time % 2 === 0 ? 1 : 0) : 2;
      return Math.min(state.leo[partition], current + rate);
    });
    const committed = state.commitMode === "auto" ? [...position] : state.committed;
    const next = { ...state, time, members, position, committed };
    return excludedReason ? beginRebalance(next, excludedReason) : next;
  }

  return state;
}

function formatLabTime(seconds: number) {
  return `T+${String(seconds).padStart(2, "0")}s`;
}

export default function ConsumerGroupLab({
  topicName,
  externalOffsets,
  expanded,
  onExpandedChange,
}: {
  topicName: string;
  externalOffsets: number[];
  expanded: boolean;
  onExpandedChange: (expanded: boolean) => void;
}) {
  const [state, dispatch] = useReducer(reducer, externalOffsets, createInitialState);
  const [autoFlow, setAutoFlow] = useState(false);
  const externalKey = externalOffsets.join(":");

  useEffect(() => {
    dispatch({ type: "SYNC_EXTERNAL", offsets: externalOffsets });
  }, [externalKey]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (state.phase !== "REBALANCING") return;
    const timer = window.setTimeout(() => dispatch({ type: "FINISH_REBALANCE" }), 850);
    return () => window.clearTimeout(timer);
  }, [state.phase, state.rebalances]);

  useEffect(() => {
    if (!autoFlow) return;
    const timer = window.setInterval(() => {
      dispatch({ type: "GENERATE", count: 2 });
      dispatch({ type: "TICK" });
    }, 1100);
    return () => window.clearInterval(timer);
  }, [autoFlow]);

  const activeMembers = state.members.filter(isMemberOfGroup);
  const totalLag = state.leo.reduce((total, leo, partition) =>
    total + Math.max(0, leo - state.committed[partition]), 0);
  const uncommitted = state.position.reduce((total, position, partition) =>
    total + Math.max(0, position - state.committed[partition]), 0);
  const assignmentByMember = useMemo(() => Object.fromEntries(state.members.map((member) => [
    member.id,
    PARTITIONS.filter((partition) => state.assignments[partition] === member.id),
  ])), [state.assignments, state.members]);

  if (!expanded) {
    return <section id="consumer-group-lab" className="consumer-lab-collapsed" aria-label="Consumer Group Lab">
      <div className="consumer-lab-launch-icon"><Users size={23} /></div>
      <div className="consumer-lab-launch-copy">
        <span>ВСТРОЕНА В ОСНОВНУЮ ЦЕПОЧКУ · 0.7.0</span>
        <h2>Consumer Group Lab</h2>
        <p>Откройте группу, чтобы управлять Consumer, rebalance, poll(), heartbeat, offsets и lag.</p>
      </div>
      <div className="consumer-lab-mini-state">
        <span className={`group-phase ${state.phase.toLowerCase()}`}>{state.phase}</span>
        <strong>{activeMembers.length} Consumer · lag {totalLag}</strong>
        <small>{PARTITIONS.map((partition) => `P${partition}→${state.assignments[partition]?.replace("consumer-", "C") ?? "—"}`).join(" · ")}</small>
      </div>
      <button className="consumer-lab-open" onClick={() => onExpandedChange(true)}>
        Развернуть лабораторию <ChevronRight size={17} />
      </button>
    </section>;
  }

  return <section id="consumer-group-lab" className="consumer-group-lab" aria-labelledby="consumer-lab-title">
    <header className="consumer-lab-header">
      <div>
        <span><Users size={16} /> CONSUMER GROUP LAB · 0.7.0</span>
        <h2 id="consumer-lab-title">Управляйте чтением после Topic</h2>
        <p>Topic и offsets общие с песочницей. Здесь видно, кто читает partition, когда возникает rebalance и почему position не равна committed offset.</p>
      </div>
      <button className="consumer-lab-collapse" onClick={() => onExpandedChange(false)}><X size={16} /> Свернуть</button>
    </header>

    <div className="consumer-lab-summary">
      <article><span>GROUP STATE</span><strong className={state.phase.toLowerCase()}>{state.phase}</strong><small>{state.phase === "REBALANCING" ? "poll() временно остановлен" : state.phase === "EMPTY" ? "нет участников" : "назначения стабильны"}</small></article>
      <article><span>MEMBERS</span><strong>{activeMembers.length} / {state.members.length}</strong><small>до 4 Consumer в одной группе</small></article>
      <article><span>TOTAL LAG</span><strong className={totalLag > 5 ? "warning" : ""}>{totalLag}</strong><small>LEO − committed offset</small></article>
      <article><span>UNCOMMITTED</span><strong>{uncommitted}</strong><small>position − committed</small></article>
      <article><span>REBALANCES</span><strong>{state.rebalances}</strong><small>с момента сброса</small></article>
      <article><span>LAB CLOCK</span><strong>{formatLabTime(state.time)}</strong><small>таймеры сжаты до секунд</small></article>
    </div>

    <div className="consumer-lab-controls">
      <label><span>Partition assignor</span><select value={state.strategy} onChange={(event) => dispatch({ type: "SET_STRATEGY", strategy: event.target.value as AssignmentStrategy })}>
        <option value="range">Range</option><option value="round-robin">Round Robin</option>
      </select></label>
      <label><span>Offset commit</span><select value={state.commitMode} onChange={(event) => dispatch({ type: "SET_COMMIT_MODE", mode: event.target.value as CommitMode })}>
        <option value="auto">Auto commit</option><option value="manual">Manual commit</option>
      </select></label>
      <label><span>session.timeout.ms</span><select value={state.sessionTimeout} onChange={(event) => dispatch({ type: "SET_SESSION_TIMEOUT", value: Number(event.target.value) })}>
        {[3, 5, 8].map((value) => <option key={value} value={value}>{value} с</option>)}
      </select></label>
      <label><span>max.poll.interval.ms</span><select value={state.maxPollInterval} onChange={(event) => dispatch({ type: "SET_MAX_POLL", value: Number(event.target.value) })}>
        {[5, 7, 10].map((value) => <option key={value} value={value}>{value} с</option>)}
      </select></label>
    </div>

    <div className="consumer-lab-actions">
      <button className="primary" disabled={state.members.length >= 4 || state.phase === "REBALANCING"} onClick={() => dispatch({ type: "ADD_CONSUMER" })}><Plus size={15} /> Добавить Consumer</button>
      <button onClick={() => dispatch({ type: "GENERATE", count: 6 })}><Play size={15} /> Добавить 6 records</button>
      <button disabled={state.phase !== "STABLE"} onClick={() => dispatch({ type: "TICK" })}><Clock3 size={15} /> Обработать 1 секунду</button>
      <button className={autoFlow ? "active" : ""} onClick={() => setAutoFlow((value) => !value)}>{autoFlow ? <CirclePause size={15} /> : <CirclePlay size={15} />} {autoFlow ? "Остановить поток" : "Запустить поток"}</button>
      <button disabled={state.commitMode !== "manual"} onClick={() => dispatch({ type: "COMMIT" })}><Save size={15} /> Commit offsets</button>
      <button onClick={() => { setAutoFlow(false); dispatch({ type: "RESET", offsets: externalOffsets }); }}><RotateCcw size={15} /> Сбросить Lab</button>
    </div>

    <div className="consumer-lab-stage">
      <section className="consumer-topic-panel">
        <div className="consumer-stage-title"><span><Activity size={15} /> TOPIC</span><strong>{topicName}</strong><small>records ожидают poll()</small></div>
        <div className="consumer-partition-queues">
          {PARTITIONS.map((partition) => {
            const lag = Math.max(0, state.leo[partition] - state.committed[partition]);
            const queued = Math.max(0, state.leo[partition] - state.position[partition]);
            return <article key={partition}>
              <div><strong>P{partition}</strong><span>LEO {state.leo[partition]}</span></div>
              <div className="queue-dots" aria-label={`${queued} records ожидают обработки`}>
                {Array.from({ length: Math.min(8, queued) }, (_, index) => <i key={index} />)}
                {!queued && <em><Check size={13} /> caught up</em>}
                {queued > 8 && <b>+{queued - 8}</b>}
              </div>
              <footer><span>owner</span><strong>{state.assignments[partition]?.replace("consumer-", "Consumer ") ?? "—"}</strong><small>lag {lag}</small></footer>
            </article>;
          })}
        </div>
      </section>

      <div className={`rebalance-bridge ${state.phase.toLowerCase()}`}>
        <ArrowRight size={24} />
        <span>{state.phase === "REBALANCING" ? <RefreshCw size={16} /> : state.phase === "EMPTY" ? <WifiOff size={16} /> : <Check size={16} />}</span>
        <strong>{state.phase === "REBALANCING" ? "GROUP COORDINATOR" : state.phase}</strong>
        <small>{state.phase === "REBALANCING" ? "пересчитывает assignment" : state.strategy === "range" ? "Range assignor" : "Round Robin assignor"}</small>
      </div>

      <section className="consumer-members-panel">
        <div className="consumer-stage-title"><span><Users size={15} /> GROUP</span><strong>sandbox-cg</strong><small>group.id одинаковый у всех участников</small></div>
        <div className="consumer-member-grid">
          {state.members.map((member, memberIndex) => {
            const assignments = assignmentByMember[member.id] ?? [];
            const memberLag = assignments.reduce((total: number, partition: number) =>
              total + Math.max(0, state.leo[partition] - state.committed[partition]), 0);
            return <article key={member.id} className={`consumer-member-card ${member.status} ${MEMBER_COLORS[memberIndex]}`}>
              <header><span><Users size={16} /></span><div><strong>{member.name}</strong><small>{statusCopy[member.status].hint}</small></div><b>{statusCopy[member.status].label}</b></header>
              <div className="member-assignment"><span>ASSIGNED</span><div>{assignments.length ? assignments.map((partition: number) => <b key={partition}>P{partition}</b>) : <em>idle</em>}</div><small>lag {memberLag}</small></div>
              <div className="member-signals">
                <span className={member.status === "heartbeat-lost" ? "lost" : ""}><HeartPulse size={13} /> heartbeat {state.time - member.lastHeartbeat}s</span>
                <span className={member.status === "poll-paused" ? "lost" : ""}><RefreshCw size={13} /> poll {state.time - member.lastPoll}s</span>
              </div>
              <div className="member-actions">
                {member.status === "stopped" || member.status === "crashed" ? <button onClick={() => dispatch({ type: "SET_MEMBER_STATUS", id: member.id, status: "active" })}><Power size={13} /> Вернуть</button> : <>
                  <button className={member.status === "slow" ? "active" : ""} onClick={() => dispatch({ type: "SET_MEMBER_STATUS", id: member.id, status: member.status === "slow" ? "active" : "slow" })}><Gauge size={13} /> Медленно</button>
                  <button className={member.status === "poll-paused" ? "active warning" : ""} onClick={() => dispatch({ type: "SET_MEMBER_STATUS", id: member.id, status: member.status === "poll-paused" ? "active" : "poll-paused" })}><Pause size={13} /> poll()</button>
                  <button className={member.status === "heartbeat-lost" ? "active danger" : ""} onClick={() => dispatch({ type: "SET_MEMBER_STATUS", id: member.id, status: member.status === "heartbeat-lost" ? "active" : "heartbeat-lost" })}><HeartPulse size={13} /> Heartbeat</button>
                  <button onClick={() => dispatch({ type: "SET_MEMBER_STATUS", id: member.id, status: "stopped" })}><Power size={13} /> Stop</button>
                  <button className="danger" onClick={() => dispatch({ type: "SET_MEMBER_STATUS", id: member.id, status: "crashed" })}><ServerCrash size={13} /> Crash</button>
                </>}
              </div>
            </article>;
          })}
        </div>
      </section>
    </div>

    <div className="consumer-offset-section">
      <div className="consumer-offset-heading"><div><span><SlidersHorizontal size={15} /> OFFSET MATRIX</span><strong>Три разных указателя, три разных факта</strong></div><p><b>LEO</b> — следующий offset в Broker; <b>position</b> — откуда Consumer продолжит poll в текущей сессии; <b>committed</b> — точка восстановления после rebalance.</p></div>
      <div className="consumer-offset-table" role="table" aria-label="Offsets и lag по партициям">
        <div className="offset-row header" role="row"><span>Partition</span><span>Owner</span><span>LEO</span><span>Position</span><span>Committed</span><span>Lag</span><span>Состояние</span></div>
        {PARTITIONS.map((partition) => {
          const lag = Math.max(0, state.leo[partition] - state.committed[partition]);
          const progress = state.leo[partition] === 0 ? 100 : Math.max(8, 100 - Math.min(100, lag * 12));
          return <div className="offset-row" role="row" key={partition}>
            <strong>P{partition}</strong><span>{state.assignments[partition]?.replace("consumer-", "C") ?? "—"}</span><b>{state.leo[partition]}</b><b>{state.position[partition]}</b><b>{state.committed[partition]}</b><strong className={lag > 5 ? "lag-warning" : ""}>{lag}</strong><div className="lag-track"><i style={{ width: `${progress}%` }} /><small>{lag ? "есть backlog" : "caught up"}</small></div>
          </div>;
        })}
      </div>
    </div>

    <div className="consumer-lab-footer">
      <section className="consumer-cause-panel">
        <div className="consumer-footer-title"><TimerReset size={16} /><div><strong>Почему произошёл rebalance?</strong><small>Coordinator фиксирует причину, а не только новое распределение</small></div></div>
        <div className="consumer-log-list">
          {state.log.map((entry) => <article key={entry.id} className={entry.tone}>
            <span>{entry.tone === "warning" ? <AlertTriangle size={14} /> : entry.tone === "success" ? <Check size={14} /> : <Info size={14} />}</span>
            <div><strong>{entry.title}</strong><p>{entry.detail}</p></div><time>{formatLabTime(entry.time)}</time>
          </article>)}
        </div>
      </section>
      <aside className="consumer-qa-panel">
        <span><Gauge size={16} /> QA FOCUS</span>
        <h3>Что проверять в эксперименте</h3>
        <ul>
          <li>Каждая partition назначена не более чем одному Consumer внутри группы.</li>
          <li>При смене владельца чтение начинается с <b>committed offset</b>, а не с прежней position.</li>
          <li>Пауза poll() и потеря heartbeat приводят к исключению по разным таймерам.</li>
          <li>Consumer сверх числа partitions остаётся участником группы, но не получает assignment.</li>
          <li>В manual commit обработанные records остаются uncommitted до явного сохранения.</li>
        </ul>
      </aside>
    </div>
  </section>;
}
