"use client";

import {
  Activity, AlertTriangle, ArrowRight, Check, ChevronRight, CirclePause,
  CirclePlay, Clock3, Gauge, HeartPulse, Info, Pause, Play, Plus, Power,
  Maximize2, Minimize2, RefreshCw, RotateCcw, Save, ServerCrash,
  SlidersHorizontal, TimerReset, Users, WifiOff, X,
} from "lucide-react";
import { useEffect, useMemo, useReducer, useState } from "react";
import {
  AssignmentStrategy, CommitMode, consumerLabReducer, createInitialState,
  isMemberOfGroup, MemberStatus, PARTITIONS,
} from "./consumer-group-model";

const MEMBER_COLORS = ["violet", "mint", "amber", "cyan"] as const;

const statusCopy: Record<MemberStatus, { label: string; hint: string }> = {
  active: { label: "ACTIVE", hint: "heartbeat и poll() работают" },
  slow: { label: "SLOW", hint: "business processing замедлен" },
  "poll-paused": { label: "POLL PAUSED", hint: "heartbeat есть, poll() остановлен" },
  "heartbeat-lost": { label: "NO HEARTBEAT", hint: "poll() идёт, coordinator не видит heartbeat" },
  crashed: { label: "CRASHED", hint: "process не отвечает; coordinator ждёт timeout" },
  stopped: { label: "STOPPED", hint: "корректно вышел через LeaveGroup" },
  excluded: { label: "EXCLUDED", hint: "исключён coordinator после timeout" },
};

function formatLabTime(seconds: number) {
  return `T+${String(seconds).padStart(2, "0")}s`;
}

export default function ConsumerGroupLab({
  topicName,
  externalLeo,
  externalHighWatermark,
  expanded,
  producerLocked,
  onProduceRecords,
  onExpandedChange,
}: {
  topicName: string;
  externalLeo: number[];
  externalHighWatermark: number[];
  expanded: boolean;
  producerLocked: boolean;
  onProduceRecords: (count: number) => void;
  onExpandedChange: (expanded: boolean) => void;
}) {
  const [state, dispatch] = useReducer(
    consumerLabReducer,
    undefined,
    () => createInitialState(externalLeo, externalHighWatermark),
  );
  const [autoFlow, setAutoFlow] = useState(false);
  const [fullscreen, setFullscreen] = useState(false);
  const externalKey = `${externalLeo.join(":")}|${externalHighWatermark.join(":")}`;

  useEffect(() => {
    if (!fullscreen) return;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setFullscreen(false);
    };
    window.addEventListener("keydown", closeOnEscape);
    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener("keydown", closeOnEscape);
    };
  }, [fullscreen]);

  useEffect(() => {
    dispatch({
      type: "SYNC_TOPIC",
      leo: externalLeo,
      highWatermark: externalHighWatermark,
    });
  }, [externalKey]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (state.phase !== "REBALANCING") return;
    const timer = window.setTimeout(() => dispatch({ type: "FINISH_REBALANCE" }), 850);
    return () => window.clearTimeout(timer);
  }, [state.phase, state.rebalances]);

  useEffect(() => {
    if (!autoFlow) return;
    const timer = window.setInterval(() => {
      if (!producerLocked) onProduceRecords(2);
      dispatch({ type: "TICK" });
    }, 1100);
    return () => window.clearInterval(timer);
  }, [autoFlow, onProduceRecords, producerLocked]);

  const activeMembers = state.members.filter(isMemberOfGroup);
  const totalLag = state.highWatermark.reduce((total, highWatermark, partition) =>
    total + Math.max(0, highWatermark - state.committed[partition]), 0);
  const processingBacklog = state.fetchPosition.reduce((total, position, partition) =>
    total + Math.max(0, position - state.processed[partition]), 0);
  const readyToCommit = state.processed.reduce((total, processed, partition) =>
    total + Math.max(0, processed - state.committed[partition]), 0);
  const committedAhead = state.committed.reduce((total, committed, partition) =>
    total + Math.max(0, committed - state.processed[partition]), 0);
  const assignmentByMember = useMemo(() => Object.fromEntries(state.members.map((member) => [
    member.id,
    PARTITIONS.filter((partition) => state.assignments[partition] === member.id),
  ])), [state.assignments, state.members]);

  if (!expanded) {
    return <section id="consumer-group-lab" className="consumer-lab-collapsed" aria-label="Consumer Group Lab">
      <div className="consumer-lab-launch-icon"><Users size={23} /></div>
      <div className="consumer-lab-launch-copy">
        <span>ВСТРОЕНА В ОСНОВНУЮ ЦЕПОЧКУ · 0.7.3</span>
        <h2>Consumer Group Lab</h2>
        <p>Откройте группу, чтобы управлять Consumer, rebalance, poll(), processing, heartbeat, offsets и lag.</p>
      </div>
      <div className="consumer-lab-mini-state">
        <span className={`group-phase ${state.phase.toLowerCase()}`}>{state.phase}</span>
        <strong>{activeMembers.length} Consumer · lag {totalLag}</strong>
        <small>Classic protocol · HW-based lag</small>
      </div>
      <button className="consumer-lab-open" onClick={() => onExpandedChange(true)}>
        Развернуть лабораторию <ChevronRight size={17} />
      </button>
    </section>;
  }

  return <section id="consumer-group-lab" className={`consumer-group-lab ${fullscreen ? "is-fullscreen" : ""}`} aria-labelledby="consumer-lab-title">
    <header className="consumer-lab-header">
      <div>
        <span><Users size={18} /> CONSUMER GROUP LAB · 0.7.3</span>
        <h2 id="consumer-lab-title">От poll() до business processing и commit</h2>
        <p>Topic действительно общий с песочницей. LEO показывает конец Leader log, HW — границу видимости Consumer, fetch position двигается при poll(), processed — после handler, committed — после сохранения offset.</p>
      </div>
      <div className="consumer-lab-view-actions">
        <button className="consumer-lab-fullscreen" onClick={() => setFullscreen((value) => !value)} aria-pressed={fullscreen}>
          {fullscreen ? <Minimize2 size={18} /> : <Maximize2 size={18} />}
          {fullscreen ? "Обычный вид" : "На весь экран"}
        </button>
        <button className="consumer-lab-collapse" onClick={() => { setFullscreen(false); onExpandedChange(false); }}><X size={18} /> Свернуть</button>
      </div>
    </header>

    <div className="consumer-protocol-note"><Info size={15} /><span><b>group.protocol=classic</b> · eager rebalance · один Topic · таймеры сжаты до секунд</span></div>

    <div className="consumer-lab-summary">
      <article><span>GROUP STATE</span><strong className={state.phase.toLowerCase()}>{state.phase}</strong><small>{state.rebalances} rebalance с момента сброса</small></article>
      <article><span>MEMBERS</span><strong>{activeMembers.length} / {state.members.length}</strong><small>crashed остаётся member до timeout</small></article>
      <article><span>CONSUMER LAG</span><strong className={totalLag > 5 ? "warning" : ""}>{totalLag}</strong><small>High Watermark − committed</small></article>
      <article><span>PROCESSING</span><strong>{processingBacklog}</strong><small>fetched, но handler не завершён</small></article>
      <article><span>{state.commitMode === "manual" ? "READY TO COMMIT" : "COMMITTED AHEAD"}</span><strong className={committedAhead ? "warning" : ""}>{state.commitMode === "manual" ? readyToCommit : committedAhead}</strong><small>{state.commitMode === "manual" ? "processed − committed" : "риск пропуска side effect"}</small></article>
      <article><span>LAB CLOCK</span><strong>{formatLabTime(state.time)}</strong><small>auto commit каждые {state.autoCommitInterval} с</small></article>
    </div>

    <div className="consumer-lab-controls">
      <label><span>Partition assignor</span><select value={state.strategy} onChange={(event) => dispatch({ type: "SET_STRATEGY", strategy: event.target.value as AssignmentStrategy })}>
        <option value="range">Range</option><option value="round-robin">Round Robin</option>
      </select></label>
      <label><span>Offset commit</span><select value={state.commitMode} onChange={(event) => dispatch({ type: "SET_COMMIT_MODE", mode: event.target.value as CommitMode })}>
        <option value="auto">Auto commit</option><option value="manual">Manual commit</option>
      </select></label>
      <label><span>auto.commit.interval.ms</span><output>{state.autoCommitInterval} с · сжато</output></label>
      <label><span>session.timeout.ms</span><select value={state.sessionTimeout} onChange={(event) => dispatch({ type: "SET_SESSION_TIMEOUT", value: Number(event.target.value) })}>
        {[3, 5, 8].map((value) => <option key={value} value={value}>{value} с</option>)}
      </select></label>
      <label><span>max.poll.interval.ms</span><select value={state.maxPollInterval} onChange={(event) => dispatch({ type: "SET_MAX_POLL", value: Number(event.target.value) })}>
        {[5, 7, 10].map((value) => <option key={value} value={value}>{value} с</option>)}
      </select></label>
    </div>

    <div className="consumer-lab-actions">
      <button className="primary" disabled={state.members.length >= 4 || state.phase === "REBALANCING"} onClick={() => dispatch({ type: "ADD_CONSUMER" })}><Plus size={15} /> Добавить Consumer</button>
      <button disabled={producerLocked} title={producerLocked ? "Завершите текущий Produce event" : undefined} onClick={() => onProduceRecords(6)}><Play size={15} /> Добавить 6 records</button>
      <button disabled={state.phase !== "STABLE"} onClick={() => dispatch({ type: "TICK" })}><Clock3 size={15} /> +1 секунда</button>
      <button className={autoFlow ? "active" : ""} onClick={() => setAutoFlow((value) => !value)}>{autoFlow ? <CirclePause size={15} /> : <CirclePlay size={15} />} {autoFlow ? "Остановить поток" : "Запустить поток"}</button>
      <button disabled={state.commitMode !== "manual" || state.phase !== "STABLE"} onClick={() => dispatch({ type: "COMMIT" })}><Save size={15} /> Commit processed</button>
      <button onClick={() => { setAutoFlow(false); dispatch({ type: "RESET", leo: externalLeo, highWatermark: externalHighWatermark }); }}><RotateCcw size={15} /> Сбросить Group</button>
    </div>

    <div className="consumer-lab-stage">
      <section className="consumer-topic-panel">
        <div className="consumer-stage-title"><span><Activity size={15} /> TOPIC</span><strong>{topicName}</strong><small>Consumer fetch ограничен High Watermark</small></div>
        <div className="consumer-partition-queues">
          {PARTITIONS.map((partition) => {
            const lag = Math.max(0, state.highWatermark[partition] - state.committed[partition]);
            const queued = Math.max(0, state.highWatermark[partition] - state.fetchPosition[partition]);
            const notVisible = Math.max(0, state.leo[partition] - state.highWatermark[partition]);
            return <article key={partition}>
              <div><strong>P{partition}</strong><span>LEO {state.leo[partition]} · HW {state.highWatermark[partition]}</span></div>
              <div className="queue-dots" aria-label={`${queued} records ожидают poll`}>
                {Array.from({ length: Math.min(8, queued) }, (_, index) => <i key={index} />)}
                {!queued && <em><Check size={13} /> fetch caught up</em>}
                {queued > 8 && <b>+{queued - 8}</b>}
              </div>
              <footer><span>owner</span><strong>{state.assignments[partition]?.replace("consumer-", "Consumer ") ?? "—"}</strong><small>lag {lag}{notVisible ? ` · ${notVisible} ещё не видим` : ""}</small></footer>
            </article>;
          })}
        </div>
      </section>

      <div className={`rebalance-bridge ${state.phase.toLowerCase()}`}>
        <ArrowRight size={24} />
        <span>{state.phase === "REBALANCING" ? <RefreshCw size={16} /> : state.phase === "EMPTY" ? <WifiOff size={16} /> : <Check size={16} />}</span>
        <strong>{state.phase === "REBALANCING" ? "GROUP COORDINATOR" : state.phase}</strong>
        <small>{state.phase === "REBALANCING" ? "отзывает и назначает partitions" : state.strategy === "range" ? "Range assignor" : "Round Robin assignor"}</small>
      </div>

      <section className="consumer-members-panel">
        <div className="consumer-stage-title"><span><Users size={15} /> GROUP</span><strong>sandbox-cg</strong><small>одинаковый group.id · Classic protocol</small></div>
        <div className="consumer-member-grid">
          {state.members.map((member, memberIndex) => {
            const assignments = assignmentByMember[member.id] ?? [];
            const memberLag = assignments.reduce((total: number, partition: number) =>
              total + Math.max(0, state.highWatermark[partition] - state.committed[partition]), 0);
            return <article key={member.id} className={`consumer-member-card ${member.status} ${MEMBER_COLORS[memberIndex]}`}>
              <header><span><Users size={16} /></span><div><strong>{member.name}</strong><small>{statusCopy[member.status].hint}</small></div><b>{statusCopy[member.status].label}</b></header>
              <div className="member-assignment"><span>ASSIGNED</span><div>{assignments.length ? assignments.map((partition: number) => <b key={partition}>P{partition}</b>) : <em>idle</em>}</div><small>lag {memberLag}</small></div>
              <div className="member-signals">
                <span className={["heartbeat-lost", "crashed"].includes(member.status) ? "lost" : ""}><HeartPulse size={13} /> heartbeat {state.time - member.lastHeartbeat}s</span>
                <span className={["poll-paused", "crashed"].includes(member.status) ? "lost" : ""}><RefreshCw size={13} /> poll {state.time - member.lastPoll}s</span>
              </div>
              <div className="member-actions">
                {member.status === "stopped" || member.status === "excluded" ? <button onClick={() => dispatch({ type: "SET_MEMBER_STATUS", id: member.id, status: "active" })}><Power size={13} /> Вернуть</button>
                  : member.status === "crashed" ? <button disabled><Clock3 size={13} /> Ждём session timeout</button> : <>
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
      <div className="consumer-offset-heading"><div><span><SlidersHorizontal size={15} /> OFFSET MATRIX</span><strong>Пять указателей — пять разных фактов</strong></div><p><b>LEO</b> — конец Leader log; <b>HW</b> — видимая граница; <b>fetch</b> двигается при poll(); <b>processed</b> — после handler; <b>committed</b> — восстановление группы.</p></div>
      <div className="consumer-offset-table" role="table" aria-label="Offsets и lag по партициям">
        <div className="offset-row header" role="row"><span>Partition</span><span>Owner</span><span>LEO</span><span>HW</span><span>Fetch</span><span>Processed</span><span>Committed</span><span>Lag</span><span>Состояние</span></div>
        {PARTITIONS.map((partition) => {
          const lag = Math.max(0, state.highWatermark[partition] - state.committed[partition]);
          const processing = Math.max(0, state.fetchPosition[partition] - state.processed[partition]);
          const atRisk = Math.max(0, state.committed[partition] - state.processed[partition]);
          const ready = Math.max(0, state.processed[partition] - state.committed[partition]);
          const progress = state.highWatermark[partition] === 0 ? 100 : Math.max(8, 100 - Math.min(100, lag * 12));
          const stateLabel = atRisk ? `${atRisk} committed до processing` : processing ? `${processing} in processing` : ready ? `${ready} ждут commit` : lag ? "есть backlog" : "caught up";
          return <div className="offset-row" role="row" key={partition}>
            <strong>P{partition}</strong><span>{state.assignments[partition]?.replace("consumer-", "C") ?? "—"}</span><b>{state.leo[partition]}</b><b>{state.highWatermark[partition]}</b><b>{state.fetchPosition[partition]}</b><b>{state.processed[partition]}</b><b>{state.committed[partition]}</b><strong className={lag > 5 ? "lag-warning" : ""}>{lag}</strong><div className={`lag-track ${atRisk ? "risk" : ""}`}><i style={{ width: `${progress}%` }} /><small>{stateLabel}</small></div>
          </div>;
        })}
      </div>
    </div>

    <div className="consumer-lab-footer">
      <section className="consumer-cause-panel">
        <div className="consumer-footer-title"><TimerReset size={16} /><div><strong>Журнал coordinator и commit</strong><small>Причина каждого перехода и периодического auto commit</small></div></div>
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
          <li>Одна partition назначена не более чем одному Consumer внутри группы.</li>
          <li><b>Crash</b> не вызывает мгновенный rebalance: сначала истекает session timeout.</li>
          <li>Fetch position может опережать processed, а auto commit — зафиксировать ещё не обработанный batch.</li>
          <li>Manual commit сохраняет processed offset; после rebalance возможна повторная обработка uncommitted records.</li>
          <li>Consumer lag считается от High Watermark, а records между HW и LEO ещё не выдаются poll().</li>
        </ul>
      </aside>
    </div>

    <aside className="consumer-model-boundaries">
      <div><Info size={18} /><span><strong>Границы учебной модели</strong><small>Это намеренные упрощения, а не поведение «по умолчанию» для любой Kafka.</small></span></div>
      <ul>
        <li>Consumer Group моделирует <b>Classic protocol</b> и eager rebalance; новый Consumer protocol и cooperative assignors не включены.</li>
        <li>Один Topic и три partitions; Range и Round Robin показаны для одного subscription.</li>
        <li>Время сжато до секунд, а размеры batches и скорость processing условны.</li>
        <li>Keyless Producer использует учебный round-robin; современный default partitioner обычно применяет sticky batching.</li>
        <li>Hash по key детерминирован для лаборатории, но не воспроизводит байт-в-байт стандартный Kafka partitioner.</li>
      </ul>
    </aside>
  </section>;
}
