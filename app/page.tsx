"use client";

import {
  Activity, AlertTriangle, ArrowRight, BookOpen, Boxes, Braces, Check,
  ChevronRight, CirclePause, CirclePlay, CircleX, Database, Gauge,
  GitBranch, Info, Layers3, Maximize2, Minimize2, Network,
  Power, Radio, RefreshCw, RotateCcw, Send, Server, Settings2, ShieldCheck,
  SkipBack, SkipForward, Sparkles, TimerReset, Users, WifiOff, X, Zap,
} from "lucide-react";
import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  AcksMode, BASE_OFFSETS, BROKER_COUNT, clusterRuntimeForScenario,
  DeliveryConfig, evaluateDelivery, EventRecord, GLOSSARY, hasProducerResult,
  isConsumed, isFollowerReplicated, isLogVisible, isOffsetCommitted, isRetryResolved,
  isRecordCommitted, lifecycleForEvent, LifecycleStatus, PARTITION_COUNT,
  NetworkFaultMode, partitionRuntime, replicaKey, resolvePartition,
  SAME_KEY_VALUES, SCENARIOS, ScenarioId, SimulationStep, STEP_BY_ID,
  stepDisposition, stepOrderForConfig, TOPIC_NAME,
} from "./simulator-model";

const PARTITION_Y = [31, 50, 69] as const;
const BROKER_Y = [28, 50, 72] as const;
const lifecycleLabels = {
  producerSend: ["Producer send()", "Запрос сформирован"],
  partitioning: ["Partition selected", "Маршрут вычислен"],
  leaderAppend: ["Leader append", "Запись в журнале"],
  replication: ["Follower sync", "Копия создана"],
  committed: ["Record committed", "Запись надёжна"],
  networkTimeout: ["Network timeout", "Ответ потерян"],
  retrySend: ["Producer retry", "Повторный запрос"],
  retryResolution: ["Broker dedup", "Дубль или подавление"],
  producerAck: ["Producer ACK", "Ответ получен"],
  consumerFetch: ["Consumer fetch", "Event прочитан"],
  offsetCommit: ["Offset commit", "Позиция сохранена"],
} as const;
const statusText: Record<LifecycleStatus, string> = {
  waiting: "ожидает",
  active: "сейчас",
  done: "готово",
  skipped: "пропущено",
  failed: "ошибка",
};
const faultOptions: {
  id: NetworkFaultMode;
  title: string;
  description: string;
}[] = [
  { id: "none", title: "Сеть работает", description: "Запрос и ACK доходят" },
  { id: "request-lost", title: "Потерять request", description: "Сбой до записи Broker" },
  { id: "ack-lost", title: "Потерять ACK", description: "Сбой после append" },
];

function formatTime(date: Date) {
  return date.toLocaleTimeString("ru-RU", {
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

function copyForStep(event: EventRecord | null, step: SimulationStep | null) {
  if (!event || !step) {
    return {
      title: "Готово к запуску",
      description: "Настройте доставку и отправьте event. Здесь появится объяснение каждого перехода.",
      technical: "Предварительный прогноз выше показывает ожидаемый итог до запуска state machine.",
    };
  }

  const { result, delivery } = event;
  if (step.id === "producerSend" && !result.configValid) {
    return {
      title: "Producer отклонил конфигурацию",
      description: result.configErrors.join(". "),
      technical: "ConfigException возникает до сетевого запроса: Kafka ещё не получила event.",
    };
  }
  if (step.id === "leaderAppend" && !result.leaderAppended) {
    const reason = event.faultMode === "request-lost" && result.errorCode === "TimeoutException"
      ? "Produce request не дошёл до Leader, а retries=0."
      : result.errorCode === "LeaderNotAvailable"
        ? `Broker ${result.leaderBroker} с Leader недоступен.`
        : `Текущий ISR=${result.currentIsr}, а min.insync.replicas=${delivery.minInSyncReplicas}.`;
    return {
      title: "Leader не добавил record",
      description: `${reason} Produce request завершится ошибкой ${result.errorCode ?? "ConfigException"}.`,
      technical: delivery.acks === "all"
        ? "При acks=all Broker проверяет min.insync.replicas и может отклонить запись до append."
        : "Без доступного Leader append невозможен. Выбор нового Leader появится в версии 0.3.2.",
    };
  }
  if (step.id === "replication") {
    if (!result.leaderAppended) {
      return {
        title: "Репликация не началась",
        description: "Leader не сохранил record, поэтому Followers нечего копировать.",
        technical: "Следующие этапы помечены как skipped, а не как успешно выполненные.",
      };
    }
    if (delivery.replicationFactor === 1) {
      return {
        title: "Follower не предусмотрен",
        description: "replication.factor=1 означает единственную копию на Leader.",
        technical: "Такая запись не переживёт потерю Broker с Leader.",
      };
    }
    if (result.followerCopies === 0) {
      return {
        title: "Followers недоступны",
        description: "Record остался только на Leader: доступных follower-replicas сейчас нет.",
        technical: "При acks=0/1 min.insync.replicas не блокирует такую запись, но риск потери остаётся.",
      };
    }
    return {
      title: `${result.followerCopies} follower ${result.followerCopies === 1 ? "создал копию" : "создали копии"}`,
      description: `Record хранится на ${result.totalCopies} Brokers из RF=${delivery.replicationFactor}.`,
      technical: `Текущий ISR=[${result.onlineReplicaBrokers.map((broker) => `B${broker}`).join(", ")}].`,
    };
  }
  if (step.id === "committed" && result.recordCommitted) {
    return result.survivesLeaderFailure
      ? {
          title: "Record committed и имеет резервную копию",
          description: `Все ${result.currentIsr} реплики текущего ISR синхронизированы.`,
          technical: "Потеря текущего Leader не уничтожит record: другая replica уже содержит данные.",
        }
      : {
          title: "Record committed, но хранится в одной копии",
          description: "ISR состоит только из Leader, поэтому high watermark может продвинуться без резервной копии.",
          technical: "Committed не всегда означает «переживёт сбой»: отдельно проверяйте размер ISR и число копий.",
        };
  }
  if (step.id === "networkTimeout") {
    return event.faultMode === "ack-lost"
      ? {
          title: "ACK потерян после успешной записи",
          description: `Broker уже создал record с offset ${event.offset}, но ответ не дошёл до Producer.`,
          technical: "Timeout оставляет результат неоднозначным: Producer не может отличить «Broker не записал» от «Broker записал, ACK потерян».",
        }
      : {
          title: "Produce request потерян до Broker",
          description: "Первая попытка не достигла Leader, поэтому record и offset ещё не появились.",
          technical: "В этом случае retry безопасен сам по себе: первая попытка физически ничего не записала.",
        };
  }
  if (step.id === "retrySend") {
    return {
      title: `Producer отправляет attempt ${result.attempts}`,
      description: `Retry использует producerId=${event.producerId} и sequence=${event.producerSequence}.`,
      technical: event.delivery.idempotence
        ? "Тот же producerId + sequence позволяет Broker распознать сетевой повтор."
        : "Без idempotence повторный запрос неотличим от новой записи.",
    };
  }
  if (step.id === "retryResolution") {
    return result.duplicateSuppressed
      ? {
          title: "Broker подавил повторную запись",
          description: "Запросов было два, но в partition остался один record и один offset.",
          technical: `Broker уже видел sequence=${event.producerSequence} от ${event.producerId}, поэтому вернул результат без нового append.`,
        }
      : {
          title: "Retry создал duplicate record",
          description: `Второй record получил новый offset ${event.retryOffset}, хотя eventId и payload не изменились.`,
          technical: "Kafka хранит две записи. Дедупликация по бизнес-ключу теперь остаётся задачей приложения или Consumer.",
        };
  }
  if (step.id === "producerAck") {
    if (delivery.acks === "0") {
      return {
        title: "Producer не ждёт ACK",
        description: `Фактически record ${result.leaderAppended ? "появился" : "не появился"} в Kafka, но Producer не получает подтверждение.`,
        technical: "Без ответа обычные retries не могут отреагировать на ошибку Broker.",
      };
    }
    if (result.ambiguousResult) {
      return {
        title: "Retries закончились: результат неизвестен",
        description: "Producer вернул TimeoutException, хотя первый record уже находится в Kafka.",
        technical: "Ошибка на стороне Producer не доказывает отсутствие event. QA должен проверить topic по eventId или бизнес-ключу.",
      };
    }
    if (result.producerResult === "error") {
      return {
        title: `Producer получил ${result.errorCode}`,
        description: `Всего попыток: ${result.attempts}. Без изменения состояния кластера все retries дают тот же результат.`,
        technical: "Retries повторяют запрос, но сами по себе не восстанавливают ISR и не запускают Broker.",
      };
    }
    if (result.faultApplied !== "none" && result.attempts > 1) {
      return {
        title: "Producer получил ACK после retry",
        description: result.duplicateSuppressed
          ? "Повтор завершился успехом без второго record."
          : result.duplicateWritten
            ? "Повтор завершился успехом, но Kafka сохранила duplicate."
            : "Вторая попытка достигла Broker и завершилась успешно.",
        technical: `Producer видит SUCCESS после ${result.attempts} attempts; records в partition: ${result.recordsWritten}.`,
      };
    }
    return delivery.acks === "1"
      ? {
          title: "ACK получен сразу после Leader append",
          description: "Producer видит успех до шага replication. На этот момент record может существовать в одной копии.",
          technical: "Это ключевое отличие acks=1 от acks=all.",
        }
      : {
          title: "ACK получен после выполнения условий ISR",
          description: `Kafka подтвердила запись при ISR=${result.currentIsr} и min ISR=${delivery.minInSyncReplicas}.`,
          technical: "acks=all подтверждает запись на всём текущем ISR, а min.insync.replicas задаёт минимально допустимый размер ISR.",
        };
  }

  return {
    title: step.title,
    description: step.description,
    technical: step.technical,
  };
}

export default function Home() {
  const [scenarioId, setScenarioId] = useState<ScenarioId>("happy-path");
  const scenario = SCENARIOS.find((item) => item.id === scenarioId) ?? SCENARIOS[0];
  const [learningMode, setLearningMode] = useState<"guided" | "sandbox">("guided");
  const [eventKey, setEventKey] = useState(scenario.defaultKey);
  const [eventValue, setEventValue] = useState(scenario.defaultValue);
  const [deliveryConfig, setDeliveryConfig] = useState<DeliveryConfig>({ ...scenario.config });
  const [faultMode, setFaultMode] = useState<NetworkFaultMode>(scenario.faultMode);
  const [configErrorAccepted, setConfigErrorAccepted] = useState(false);
  const [events, setEvents] = useState<EventRecord[]>([]);
  const [activeEventId, setActiveEventId] = useState<string | null>(null);
  const [selectedEventId, setSelectedEventId] = useState<string | null>(null);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(1);
  const [showGlossary, setShowGlossary] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [showClusterFocus, setShowClusterFocus] = useState(false);
  const [selectedPartition, setSelectedPartition] = useState<number | null>(null);
  const [selectedBroker, setSelectedBroker] = useState<number | null>(null);
  const [focusedPartition, setFocusedPartition] = useState(scenario.focusPartition ?? 0);
  const [onlineBrokers, setOnlineBrokers] = useState<number[]>(
    () => clusterRuntimeForScenario(scenario).onlineBrokers,
  );
  const [laggingReplicas, setLaggingReplicas] = useState<string[]>(
    () => clusterRuntimeForScenario(scenario).laggingReplicas,
  );
  const [leaders, setLeaders] = useState<number[]>(
    () => clusterRuntimeForScenario(scenario).leaders,
  );
  const [clusterNotice, setClusterNotice] = useState({
    tone: "info" as "info" | "success" | "warning",
    title: "Кластер готов",
    text: "Выберите partition и управляйте Broker после завершения отправки.",
  });
  const [inspectorTab, setInspectorTab] = useState<"event" | "delivery" | "lifecycle">("delivery");
  const [copied, setCopied] = useState(false);
  const keylessCounter = useRef(0);
  const eventCounter = useRef(1);

  const activeEvent = events.find((event) => event.id === activeEventId) ?? null;
  const selectedEvent = events.find((event) => event.id === selectedEventId) ?? activeEvent;
  const activeStep = activeEvent
    ? STEP_BY_ID[activeEvent.stepOrder[activeEvent.stage]]
    : null;
  const activeStepCopy = copyForStep(activeEvent, activeStep);
  const selectedLifecycle = lifecycleForEvent(selectedEvent);
  const selectedStepOrder = selectedEvent?.stepOrder ?? stepOrderForConfig(deliveryConfig, faultMode);
  const sameKeyCount = events.filter((event) => event.scenarioId === "same-key").length;
  const canSend = (!activeEvent || activeEvent.stage === activeEvent.stepOrder.length - 1)
    && !(scenarioId === "same-key" && sameKeyCount >= SAME_KEY_VALUES.length);
  const nextKey = scenarioId === "same-key" ? scenario.defaultKey : eventKey;
  const previewPartition = resolvePartition(nextKey, events.length);
  const clusterRuntime = useMemo(() => ({
    onlineBrokers,
    laggingReplicas,
    leaders,
  }), [laggingReplicas, leaders, onlineBrokers]);
  const partitionStates = useMemo(() =>
    Array.from({ length: PARTITION_COUNT }, (_, partition) =>
      partitionRuntime(partition, deliveryConfig.replicationFactor, clusterRuntime)),
  [clusterRuntime, deliveryConfig.replicationFactor]);
  const focusedPartitionState = partitionStates[focusedPartition];
  const previewResult = evaluateDelivery(
    deliveryConfig,
    previewPartition,
    clusterRuntime,
    faultMode,
  );
  const focusedDeliveryResult = evaluateDelivery(
    deliveryConfig,
    focusedPartition,
    clusterRuntime,
  );
  const focusedFollower = focusedPartitionState.assignedReplicas.find((broker) =>
    broker !== focusedPartitionState.leaderBroker) ?? null;
  const previewStepOrder = stepOrderForConfig(deliveryConfig, faultMode);
  const inspectorDelivery = selectedEvent?.delivery ?? deliveryConfig;
  const inspectorResult = selectedEvent?.result ?? previewResult;
  const inspectorPartition = selectedEvent?.partition ?? previewPartition;
  const inspectorPartitionState = partitionStates[inspectorPartition];
  const availableCopiesForEvent = (event: EventRecord) => {
    if (!isLogVisible(event)) return 0;
    const survivingOriginalCopies = event.result.onlineReplicaBrokers.filter((broker) =>
      onlineBrokers.includes(broker));
    if (!survivingOriginalCopies.length) return 0;
    if (isRecordCommitted(event)) {
      return partitionStates[event.partition].isrBrokers.length;
    }
    return survivingOriginalCopies.length;
  };
  const inspectorCopies = selectedEvent
    ? availableCopiesForEvent(selectedEvent)
    : inspectorResult.totalCopies;
  const inspectorLost = Boolean(selectedEvent && isLogVisible(selectedEvent) && inspectorCopies === 0);
  const inspectorCommitted = selectedEvent
    ? isRecordCommitted(selectedEvent)
    : inspectorResult.recordCommitted;
  const inspectorProducerReady = selectedEvent
    ? selectedEvent.delivery.acks === "0" || hasProducerResult(selectedEvent)
    : true;
  const activeDisposition = activeEvent && activeStep
    ? stepDisposition(activeEvent, activeStep.id)
    : null;
  const isGuided = learningMode === "guided";
  const retryResult = selectedEvent?.result ?? previewResult;
  const retryFault = selectedEvent?.faultMode ?? faultMode;
  const retryStageReached = Boolean(selectedEvent && isRetryResolved(selectedEvent));

  const eventsByPartition = useMemo(() =>
    Array.from({ length: PARTITION_COUNT }, (_, partition) =>
      events.filter((event) => event.partition === partition && isLogVisible(event))),
  [events]);
  const writtenRecordCount = (partition: number) =>
    events
      .filter((event) => event.partition === partition)
      .reduce((total, event) => total + event.result.recordsWritten, 0);
  const visibleRecordCount = (partition: number) =>
    events
      .filter((event) => event.partition === partition && isLogVisible(event))
      .reduce((total, event) =>
        total + 1 + (event.result.duplicateWritten && isRetryResolved(event) ? 1 : 0), 0);
  const partitionDetails = selectedPartition === null
    ? null
    : eventsByPartition[selectedPartition];
  const selectedPartitionReplicas = selectedPartition === null
    ? []
    : partitionStates[selectedPartition].assignedReplicas;
  const selectedPartitionState = selectedPartition === null
    ? null
    : partitionStates[selectedPartition];

  const brokerReplicas = useMemo(() =>
    Array.from({ length: BROKER_COUNT }, (_, index) => {
      const broker = index + 1;
      return partitionStates.map((partitionState) => {
        const replicaIndex = partitionState.assignedReplicas.indexOf(broker);
        if (replicaIndex < 0) return null;
        return {
          partition: partitionState.partition,
          role: partitionState.leaderOnline && partitionState.leaderBroker === broker
            ? "L"
            : "F",
          status: !onlineBrokers.includes(broker)
            ? "offline"
            : partitionState.laggingReplicaBrokers.includes(broker)
              ? "lagging"
              : partitionState.isrBrokers.includes(broker) ? "isr" : "online",
        };
      }).filter((replica): replica is {
        partition: number;
        role: string;
        status: string;
      } => replica !== null);
    }),
  [onlineBrokers, partitionStates]);
  const selectedBrokerReplicas = selectedBroker === null
    ? []
    : brokerReplicas[selectedBroker - 1];

  const goNext = useCallback(() => {
    if (!activeEventId) return;
    setEvents((current) => current.map((event) => {
      if (event.id !== activeEventId) return event;
      if (event.stage >= event.stepOrder.length - 1) {
        setPlaying(false);
        return event;
      }
      const stage = event.stage + 1;
      if (stage === event.stepOrder.length - 1) {
        window.setTimeout(() => setPlaying(false), 450);
      }
      return { ...event, stage };
    }));
  }, [activeEventId]);

  const goPrevious = useCallback(() => {
    if (!activeEventId) return;
    setPlaying(false);
    setEvents((current) => current.map((event) =>
      event.id === activeEventId
        ? { ...event, stage: Math.max(0, event.stage - 1) }
        : event));
  }, [activeEventId]);

  const replayActive = useCallback(() => {
    if (!activeEventId) return;
    setEvents((current) => current.map((event) =>
      event.id === activeEventId ? { ...event, stage: 0 } : event));
    setPlaying(true);
  }, [activeEventId]);

  useEffect(() => {
    if (!playing || !activeEvent) return;
    const timer = window.setInterval(goNext, 1550 / speed);
    return () => window.clearInterval(timer);
  }, [activeEvent, goNext, playing, speed]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      const field = event.target instanceof HTMLInputElement
        || event.target instanceof HTMLSelectElement
        || event.target instanceof HTMLTextAreaElement;
      if (event.code === "Space" && !field && activeEvent) {
        event.preventDefault();
        setPlaying((value) => !value);
      }
      if (event.code === "ArrowRight" && !field && activeEvent) {
        event.preventDefault();
        goNext();
      }
      if (event.code === "ArrowLeft" && !field && activeEvent) {
        event.preventDefault();
        goPrevious();
      }
      if (event.code === "KeyR" && !field && activeEvent) {
        event.preventDefault();
        replayActive();
      }
      if (event.code === "Escape") {
        if (selectedPartition !== null) setSelectedPartition(null);
        else if (selectedBroker !== null) setSelectedBroker(null);
        else if (showGlossary) setShowGlossary(false);
        else if (showClusterFocus) setShowClusterFocus(false);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [
    activeEvent,
    goNext,
    goPrevious,
    replayActive,
    selectedBroker,
    selectedPartition,
    showClusterFocus,
    showGlossary,
  ]);

  useEffect(() => {
    if (!showClusterFocus && selectedPartition === null && selectedBroker === null && !showGlossary) return;
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => { document.body.style.overflow = previous; };
  }, [selectedBroker, selectedPartition, showClusterFocus, showGlossary]);

  const applyScenarioCluster = (nextScenario: typeof scenario) => {
    const runtime = clusterRuntimeForScenario(nextScenario);
    setOnlineBrokers(runtime.onlineBrokers);
    setLaggingReplicas(runtime.laggingReplicas);
    setLeaders(runtime.leaders);
    setFocusedPartition(nextScenario.focusPartition ?? resolvePartition(nextScenario.defaultKey, 0));
    setSelectedBroker(null);
    setClusterNotice({
      tone: nextScenario.group === "resilience" ? "warning" : "info",
      title: nextScenario.group === "resilience" ? "Сценарий отказоустойчивости загружен" : "Кластер готов",
      text: nextScenario.group === "resilience"
        ? "Следуйте подсказке сценария и наблюдайте Leader, ISR и доступность записи."
        : "Все изменения Broker выполняются в Cluster Resilience Lab.",
    });
  };

  const chooseScenario = (id: ScenarioId) => {
    const next = SCENARIOS.find((item) => item.id === id) ?? SCENARIOS[0];
    setScenarioId(id); setEventKey(next.defaultKey); setEventValue(next.defaultValue);
    setDeliveryConfig({ ...next.config });
    setFaultMode(next.faultMode);
    setConfigErrorAccepted(false);
    setEvents([]); setActiveEventId(null); setSelectedEventId(null);
    setPlaying(false); setShowSettings(false); setInspectorTab("delivery");
    applyScenarioCluster(next);
    keylessCounter.current = 0;
  };

  const chooseLearningMode = (mode: "guided" | "sandbox") => {
    if (mode === learningMode) return;
    setLearningMode(mode);
    setDeliveryConfig({ ...scenario.config });
    setFaultMode(scenario.faultMode);
    setConfigErrorAccepted(false);
    setEvents([]); setActiveEventId(null); setSelectedEventId(null);
    setPlaying(false); setShowSettings(false); setInspectorTab("delivery");
    setEventKey(scenario.defaultKey); setEventValue(scenario.defaultValue);
    applyScenarioCluster(scenario);
    keylessCounter.current = 0;
  };

  const sendEvent = () => {
    if (!canSend) return;
    const sequence = scenarioId === "same-key" ? sameKeyCount : events.length;
    const key = scenarioId === "same-key" ? scenario.defaultKey : eventKey;
    const value = scenarioId === "same-key"
      ? SAME_KEY_VALUES[Math.min(sequence, SAME_KEY_VALUES.length - 1)] : eventValue;
    const partition = resolvePartition(key, keylessCounter.current);
    if (!key.trim()) keylessCounter.current += 1;
    const offset = BASE_OFFSETS[partition] + writtenRecordCount(partition) + 1;
    const id = `evt_${String(eventCounter.current++).padStart(3, "0")}`;
    const delivery = { ...deliveryConfig };
    const result = evaluateDelivery(delivery, partition, clusterRuntime, faultMode);
    const nextEvent: EventRecord = {
      id, key, value, partition, offset, stage: 0, scenarioId, sequence,
      createdAt: formatTime(new Date()), delivery, result,
      stepOrder: stepOrderForConfig(delivery, faultMode),
      faultMode,
      producerId: "producer-7f31",
      producerEpoch: 0,
      producerSequence: eventCounter.current - 2,
      retryOffset: result.duplicateWritten ? offset + 1 : null,
    };
    setEvents((current) => [...current, nextEvent]);
    setActiveEventId(id); setSelectedEventId(id); setPlaying(true);
    setInspectorTab("delivery");
    setCopied(false);
    if (scenarioId === "same-key" && sequence + 1 < SAME_KEY_VALUES.length) {
      setEventValue(SAME_KEY_VALUES[sequence + 1]);
    }
  };

  const resetScenario = () => {
    setEvents([]); setActiveEventId(null); setSelectedEventId(null);
    setPlaying(false); setEventKey(scenario.defaultKey); setEventValue(scenario.defaultValue);
    setShowSettings(false); setCopied(false); keylessCounter.current = 0;
  };

  const restoreScenarioConfig = () => {
    setDeliveryConfig({ ...scenario.config });
    setFaultMode(scenario.faultMode);
    setConfigErrorAccepted(false);
    applyScenarioCluster(scenario);
    resetScenario();
  };

  const autoFixConfig = () => {
    setDeliveryConfig((current) => ({
      ...current,
      acks: current.idempotence ? "all" : current.acks,
      retries: current.idempotence ? Math.max(1, current.retries) : current.retries,
    }));
    setConfigErrorAccepted(false);
    resetScenario();
  };

  const selectStep = (stage: number) => {
    if (!activeEvent) return;
    setPlaying(false);
    setEvents((current) => current.map((event) =>
      event.id === activeEvent.id ? { ...event, stage } : event));
  };

  const selectFaultMode = (mode: NetworkFaultMode) => {
    if (isGuided || mode === faultMode) return;
    setFaultMode(mode);
    setConfigErrorAccepted(false);
    resetScenario();
  };

  const compareRetryMode = () => {
    chooseScenario(
      scenarioId === "ack-lost-idempotent"
        ? "ack-lost-duplicate"
        : "ack-lost-idempotent",
    );
  };

  const copyPayload = async () => {
    const payload = selectedEvent?.value ?? eventValue;
    try {
      await navigator.clipboard.writeText(payload);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1400);
    } catch {
      setCopied(false);
    }
  };

  const partition = activeEvent?.partition ?? previewPartition;
  const leaderBroker = activeEvent?.result.leaderBroker
    ?? partitionStates[partition].leaderBroker;
  const followerBroker = activeEvent?.result.onlineReplicaBrokers.find((broker) =>
    broker !== leaderBroker)
    ?? partitionStates[partition].assignedReplicas.find((broker) => broker !== leaderBroker)
    ?? leaderBroker;
  const partitionY = PARTITION_Y[partition];
  const leaderY = BROKER_Y[leaderBroker - 1];
  const followerY = BROKER_Y[followerBroker - 1];
  const orb = useMemo(() => {
    switch (activeStep?.node) {
      case "partition": return { x: 51, y: partitionY };
      case "leader": case "committed": return { x: 69, y: leaderY };
      case "follower": return { x: 69, y: followerY };
      case "ack": return { x: 8.5, y: 42 };
      case "timeout": return { x: 37, y: 13 };
      case "retry": return { x: 31, y: 50 };
      case "consumer": case "offset": return { x: 91, y: 50 };
      default: return { x: 8.5, y: 50 };
    }
  }, [activeStep, followerY, leaderY, partitionY]);

  const ackReached = activeEvent
    ? hasProducerResult(activeEvent) && activeEvent.delivery.acks !== "0"
    : false;
  const routeReached = activeEvent
    ? activeEvent.stage >= activeEvent.stepOrder.indexOf("partitioning")
    : false;
  const appendReached = activeEvent
    ? activeEvent.stage >= activeEvent.stepOrder.indexOf("leaderAppend")
    : false;
  const replicationReached = activeEvent
    ? isFollowerReplicated(activeEvent)
    : false;

  const setConfig = <K extends keyof DeliveryConfig>(
    key: K,
    value: DeliveryConfig[K],
  ) => {
    if (isGuided) return;
    setDeliveryConfig((current) => ({ ...current, [key]: value }));
    setConfigErrorAccepted(false);
    if (events.length) resetScenario();
  };

  const clusterActionsLocked = Boolean(
    activeEvent && activeEvent.stage < activeEvent.stepOrder.length - 1,
  );

  const toggleBroker = (broker: number) => {
    if (clusterActionsLocked) return;
    const stopping = onlineBrokers.includes(broker);
    const nextOnline = stopping
      ? onlineBrokers.filter((item) => item !== broker)
      : [...onlineBrokers, broker].sort((a, b) => a - b);
    const affectedReplicaKeys = partitionStates
      .filter((state) => state.assignedReplicas.includes(broker))
      .map((state) => replicaKey(state.partition, broker));
    const nextLagging = Array.from(new Set([
      ...laggingReplicas,
      ...affectedReplicaKeys,
    ]));
    const nextRuntime = {
      onlineBrokers: nextOnline,
      laggingReplicas: nextLagging,
      leaders,
    };
    const nextPartitionStates = Array.from({ length: PARTITION_COUNT }, (_, partition) =>
      partitionRuntime(partition, deliveryConfig.replicationFactor, nextRuntime));
    const nextLeaders = nextPartitionStates.map((state, partition) =>
      state.leaderOnline ? state.leaderBroker : leaders[partition]);
    const focusBefore = partitionStates[focusedPartition];
    const focusAfter = nextPartitionStates[focusedPartition];

    setOnlineBrokers(nextOnline);
    setLaggingReplicas(nextLagging);
    setLeaders(nextLeaders);
    setDeliveryConfig((current) => ({ ...current, availableBrokers: nextOnline.length }));
    setClusterNotice(stopping
      ? broker === focusBefore.leaderBroker
        ? {
            tone: "warning",
            title: `Leader B${broker} остановлен`,
            text: focusAfter.leaderOnline
              ? `Kafka выбрала B${focusAfter.leaderBroker} новым Leader P${focusedPartition}. ISR теперь [${focusAfter.isrBrokers.map((item) => `B${item}`).join(", ")}].`
              : `У P${focusedPartition} не осталось синхронной replica: Leader недоступен.`
          }
        : {
            tone: "warning",
            title: `Broker ${broker} остановлен`,
            text: `Его replicas вышли из ISR. Для P${focusedPartition}: ISR=[${focusAfter.isrBrokers.map((item) => `B${item}`).join(", ") || "empty"}].`,
          }
      : {
          tone: "info",
          title: `Broker ${broker} снова ONLINE`,
          text: "Его replicas пока CATCHING UP и не входят в ISR. Запустите синхронизацию нужной partition.",
        });
  };

  const recoverReplica = (partition: number, broker: number) => {
    if (clusterActionsLocked || !onlineBrokers.includes(broker)) return;
    const key = replicaKey(partition, broker);
    const nextLagging = laggingReplicas.filter((item) => item !== key);
    const nextRuntime = {
      onlineBrokers,
      laggingReplicas: nextLagging,
      leaders,
    };
    const nextState = partitionRuntime(
      partition,
      deliveryConfig.replicationFactor,
      nextRuntime,
    );
    setLaggingReplicas(nextLagging);
    setClusterNotice({
      tone: "success",
      title: `P${partition} на B${broker} синхронизирована`,
      text: `Replica вернулась в ISR. Текущий ISR=[${nextState.isrBrokers.map((item) => `B${item}`).join(", ")}].`,
    });
  };

  const restorePreferredLeader = () => {
    if (clusterActionsLocked) return;
    const preferred = focusedPartitionState.preferredLeaderBroker;
    if (!focusedPartitionState.isrBrokers.includes(preferred)) {
      setClusterNotice({
        tone: "warning",
        title: "Preferred Leader ещё недоступен",
        text: `Сначала запустите и синхронизируйте P${focusedPartition} на B${preferred}.`,
      });
      return;
    }
    setLeaders((current) => current.map((leader, partition) =>
      partition === focusedPartition ? preferred : leader));
    setClusterNotice({
      tone: "success",
      title: `Preferred Leader восстановлен`,
      text: `B${preferred} снова принимает записи P${focusedPartition}.`,
    });
  };

  const canLaunch = canSend
    && (isGuided || previewResult.configValid || configErrorAccepted);
  const selectedHasReached = (stepId: keyof typeof lifecycleLabels) => {
    if (!selectedEvent) return false;
    const position = selectedEvent.stepOrder.indexOf(stepId);
    return position >= 0 && selectedEvent.stage >= position;
  };
  const currentFaultLabel = faultOptions.find((option) => option.id === retryFault)
    ?? faultOptions[0];
  const timelineSteps = activeEvent?.stepOrder ?? previewStepOrder;
  const timelineInset = 50 / timelineSteps.length;
  const timelineProgress = activeEvent && activeEvent.stepOrder.length > 1
    ? (activeEvent.stage / (activeEvent.stepOrder.length - 1)) * (100 - timelineInset * 2)
    : 0;

  return (
    <main className="app-shell">
      <header className="topbar">
        <a className="brand" href="#" aria-label="Kafka Path — главная">
          <span className="brand-mark"><Network size={19} /></span>
          <span>Kafka Path</span><span className="version">version 0.3.3</span>
        </a>
        <div className="header-actions">
          <button className="header-link" onClick={() => setShowGlossary(true)}>
            <BookOpen size={16} /> Словарь
          </button>
          <span className={`mode-pill ${isGuided ? "" : "sandbox"}`}>
            {isGuided ? <Sparkles size={14} /> : <Settings2 size={14} />}
            {isGuided ? "Учебный сценарий" : "Песочница"}
          </span>
        </div>
      </header>

      <section className="workspace">
        <div className="intro-row">
          <div>
            <p className="eyebrow"><Activity size={14} /> Producer delivery lab</p>
            <h1>Что происходит, когда ACK теряется после записи в Kafka</h1>
            <p>Запускайте timeout и retry, сравнивайте дубли с idempotence и управляйте анимацией пошагово. Все возможности Broker, ISR и partition из 0.3.2 сохранены.</p>
          </div>
          <div className="cluster-summary">
            <span><Server size={15} /> {onlineBrokers.length}/3 online</span>
            <span><Boxes size={15} /> 3 partitions</span>
            <span><Database size={15} /> RF = {deliveryConfig.replicationFactor}</span>
            <span><ShieldCheck size={15} /> min ISR = {deliveryConfig.minInSyncReplicas}</span>
          </div>
        </div>

        <section className="learning-mode-switch" aria-label="Режим работы симулятора">
          <button
            className={isGuided ? "active" : ""}
            aria-pressed={isGuided}
            onClick={() => chooseLearningMode("guided")}
          >
            <span><Sparkles size={19} /></span>
            <div>
              <strong>Учебные сценарии</strong>
              <small>Готовые корректные настройки — можно сразу запускать</small>
            </div>
            {isGuided && <Check size={18} />}
          </button>
          <button
            className={!isGuided ? "active sandbox" : "sandbox"}
            aria-pressed={!isGuided}
            onClick={() => chooseLearningMode("sandbox")}
          >
            <span><Settings2 size={19} /></span>
            <div>
              <strong>Свободная песочница</strong>
              <small>Меняйте всё и изучайте ошибки конфигурации</small>
            </div>
            {!isGuided && <Check size={18} />}
          </button>
          <div className={`mode-explanation ${isGuided ? "guided" : "sandbox"}`}>
            <strong>{isGuided ? "Сценарий защищён от случайных изменений" : "Вы управляете конфигурацией"}</strong>
            <span>{isGuided
              ? "При выборе карточки Kafka Path автоматически применяет подходящий preset."
              : "Карточка сценария задаёт стартовый preset, после чего его можно изменять вручную."}</span>
          </div>
        </section>

        <div className="scenario-labels">
          <span>{isGuided ? "Основы 0.2" : "Базовые presets"}</span>
          <span>{isGuided ? "Producer reliability 0.3.1" : "Presets доставки"}</span>
          <span>{isGuided ? "Cluster resilience 0.3.2" : "Presets отказов"}</span>
          <span>{isGuided ? "Retries 0.3.3" : "Presets сети"}</span>
        </div>
        <nav className="scenario-switcher" aria-label={isGuided ? "Учебные сценарии" : "Стартовые presets"}>
          {SCENARIOS.map((item, index) => (
            <button
              key={item.id}
              className={`${scenarioId === item.id ? "active" : ""} ${item.group}`}
              onClick={() => chooseScenario(item.id)}
            >
              <span>{String(index + 1).padStart(2, "0")}</span>
              <div><strong>{item.title}</strong><small>{item.description}</small></div>
              {scenarioId === item.id && <Check size={16} />}
            </button>
          ))}
        </nav>

        <section className="scenario-brief">
          <div className="brief-icon"><GitBranch size={18} /></div>
          <div><span>{scenario.badge}</span><strong>{scenario.lesson}</strong></div>
          {scenarioId === "same-key" && <div className="scenario-progress">
            {SAME_KEY_VALUES.map((_, index) => <i key={index} className={index < sameKeyCount ? "complete" : ""}>
              {index < sameKeyCount ? <Check size={10} /> : index + 1}
            </i>)}
          </div>}
        </section>

        <section className="delivery-lab" aria-label="Настройки доставки Producer">
          <header className="delivery-lab-heading">
            <div>
              <span>{isGuided ? <ShieldCheck size={16} /> : <Settings2 size={16} />}
                {isGuided ? "SCENARIO PRESET" : "DELIVERY SETTINGS"}</span>
              <h2>{isGuided
                ? "Настройки применены автоматически"
                : "Измените конфигурацию и сравните результат"}</h2>
            </div>
            <div className={`config-health ${isGuided || previewResult.configValid ? "valid" : "invalid"}`}>
              {isGuided || previewResult.configValid ? <Check size={16} /> : <CircleX size={16} />}
              {isGuided
                ? "Preset сценария активен"
                : previewResult.configValid ? "Конфигурация совместима" : "ConfigException"}
            </div>
          </header>

          <div className={`config-grid ${isGuided ? "locked" : ""}`}>
            <div className="config-control acks-control">
              <label>acks <small>Когда вернуть результат</small></label>
              <div className="segmented-control">
                {(["0", "1", "all"] as AcksMode[]).map((acks) => (
                  <button
                    key={acks}
                    aria-pressed={deliveryConfig.acks === acks}
                    className={deliveryConfig.acks === acks ? "active" : ""}
                    disabled={isGuided}
                    onClick={() => setConfig("acks", acks)}
                  >{acks}</button>
                ))}
              </div>
            </div>

            <div className="config-control">
              <label htmlFor="rf-select">replication.factor <small>Желаемое число копий</small></label>
              <select
                id="rf-select"
                value={deliveryConfig.replicationFactor}
                disabled={isGuided}
                onChange={(event) => setConfig("replicationFactor", Number(event.target.value))}
              >
                {[1, 2, 3].map((value) => <option key={value} value={value}>{value}</option>)}
              </select>
            </div>

            <div className="config-control">
              <label htmlFor="min-isr-select">min.insync.replicas <small>Порог для acks=all</small></label>
              <select
                id="min-isr-select"
                value={deliveryConfig.minInSyncReplicas}
                disabled={isGuided}
                onChange={(event) => setConfig("minInSyncReplicas", Number(event.target.value))}
              >
                {[1, 2, 3].map((value) => <option key={value} value={value}>{value}</option>)}
              </select>
            </div>

            <div className="config-control cluster-config-summary">
              <label>cluster.brokers <small>Управление каждым Broker ниже</small></label>
              <div><Server size={15} /><strong>{onlineBrokers.length} / 3 ONLINE</strong></div>
            </div>

            <div className="config-control">
              <label htmlFor="retries-select">retries <small>Повторы после ошибки</small></label>
              <select
                id="retries-select"
                value={deliveryConfig.retries}
                disabled={isGuided}
                onChange={(event) => setConfig("retries", Number(event.target.value))}
              >
                {[0, 1, 3, 5].map((value) => <option key={value} value={value}>{value}</option>)}
              </select>
            </div>

            <div className="config-control idempotence-control">
              <label>enable.idempotence <small>Защита retry от дубля</small></label>
              <button
                role="switch"
                aria-checked={deliveryConfig.idempotence}
                className={`toggle-control ${deliveryConfig.idempotence ? "active" : ""}`}
                disabled={isGuided}
                onClick={() => setConfig("idempotence", !deliveryConfig.idempotence)}
              >
                <i><span /></i>{deliveryConfig.idempotence ? "true" : "false"}
              </button>
            </div>
          </div>

          {isGuided ? (
            <div className="guided-config-note">
              <ShieldCheck size={18} />
              <div>
                <strong>Параметры соответствуют сценарию «{scenario.title}»</strong>
                <span>Они доступны для просмотра, но не изменяются в учебном режиме.</span>
              </div>
              <button onClick={() => chooseLearningMode("sandbox")}>Открыть песочницу</button>
            </div>
          ) : (
            <div className="sandbox-config-actions">
              <div>
                <Settings2 size={17} />
                <span>Ручной режим: изменения относятся только к текущему эксперименту.</span>
              </div>
              <button onClick={restoreScenarioConfig}><RotateCcw size={15} /> Восстановить preset</button>
            </div>
          )}

          {!isGuided && !previewResult.configValid && (
            <div className={`config-errors ${configErrorAccepted ? "accepted" : ""}`}>
              <AlertTriangle size={17} />
              <div>
                <strong>{configErrorAccepted
                  ? "Ошибка оставлена для учебного эксперимента"
                  : "Конфликт настроек Producer"}</strong>
                {previewResult.configErrors.map((error) => <span key={error}>{error}</span>)}
                {!configErrorAccepted && <div className="config-error-actions">
                  <button onClick={autoFixConfig}><Check size={14} /> Исправить автоматически</button>
                  <button onClick={() => setConfigErrorAccepted(true)}>
                    <AlertTriangle size={14} /> Оставить и изучить ошибку
                  </button>
                </div>}
              </div>
            </div>
          )}

          <div className="delivery-forecast">
            <article className={
              previewResult.producerResult === "ack" ? "good"
                : previewResult.producerResult === "error" || previewResult.producerResult === "config-error" ? "bad"
                  : "warn"
            }>
              <span>PRODUCER ЗНАЕТ</span>
              <strong>{
                previewResult.producerResult === "ack" ? "ACK · успех"
                  : previewResult.producerResult === "unconfirmed" ? "Ничего · ACK нет"
                    : previewResult.errorCode
              }</strong>
              <small>{deliveryConfig.acks === "0" ? "fire-and-forget" : `${previewResult.attempts} attempt(s)`}</small>
            </article>
            <article className={previewResult.leaderAppended ? "good" : "bad"}>
              <span>ФАКТ В KAFKA</span>
              <strong>{previewResult.leaderAppended
                ? `${previewResult.recordsWritten} record(s) в P${previewPartition}`
                : "Record не записан"}</strong>
              <small>{previewResult.duplicateWritten
                ? "duplicate получил новый offset"
                : `Leader · Broker ${previewResult.leaderBroker}`}</small>
            </article>
            <article className={previewResult.currentIsr >= deliveryConfig.minInSyncReplicas ? "good" : "warn"}>
              <span>ISR / КОПИИ</span>
              <strong>{previewResult.currentIsr} ISR · {previewResult.totalCopies} copies</strong>
              <small>RF={deliveryConfig.replicationFactor}, min ISR={deliveryConfig.minInSyncReplicas}</small>
            </article>
            <article className={
              !previewResult.leaderAppended ? "neutral"
                : previewResult.survivesLeaderFailure ? "good" : "bad"
            }>
              <span>ЕСЛИ LEADER УПАДЁТ</span>
              <strong>{!previewResult.leaderAppended ? "Проверять нечего"
                : previewResult.survivesLeaderFailure ? "Record сохранится" : "Record потеряется"}</strong>
              <small>{previewResult.survivesLeaderFailure ? "есть резервная replica" : "резервной копии нет"}</small>
            </article>
          </div>
        </section>

        <section className="retry-lab" aria-label="Сетевые сбои, retries и idempotence">
          <header className="retry-lab-heading">
            <div>
              <span><Radio size={16} /> NETWORK & RETRY LAB · 0.3.3</span>
              <h2>Выберите, где оборвётся первая попытка</h2>
              <p>В учебном режиме используется безопасный preset сценария. В песочнице сетевой сбой можно менять отдельно от конфигурации Producer.</p>
            </div>
            <div className="retry-heading-actions">
              {(scenarioId === "ack-lost-duplicate" || scenarioId === "ack-lost-idempotent") && (
                <button className="compare-button" onClick={compareRetryMode}>
                  <RefreshCw size={15} />
                  {scenarioId === "ack-lost-idempotent"
                    ? "Сравнить без idempotence"
                    : "Сравнить с idempotence"}
                </button>
              )}
              <button className="replay-button" onClick={replayActive} disabled={!activeEvent}>
                <TimerReset size={15} /> Повторить анимацию
              </button>
            </div>
          </header>

          <div className={`fault-selector ${isGuided ? "locked" : ""}`}>
            {faultOptions.map((option) => (
              <button
                key={option.id}
                className={faultMode === option.id ? "active" : ""}
                aria-pressed={faultMode === option.id}
                disabled={isGuided}
                onClick={() => selectFaultMode(option.id)}
              >
                <i>{option.id === "none"
                  ? <Check size={16} />
                  : option.id === "request-lost"
                    ? <WifiOff size={16} />
                    : <Radio size={16} />}</i>
                <span><strong>{option.title}</strong><small>{option.description}</small></span>
              </button>
            ))}
            {isGuided && (
              <div className="fault-lock">
                <ShieldCheck size={16} />
                <span>Preset: <strong>{currentFaultLabel.title}</strong></span>
              </div>
            )}
          </div>

          <div className="attempt-summary">
            <article>
              <span>PRODUCE REQUESTS</span>
              <strong>{retryResult.attempts}</strong>
              <small>{retryResult.attempts > 1 ? "первая + retry" : "одна попытка"}</small>
            </article>
            <article className={retryResult.recordsWritten > 1 ? "danger" : "good"}>
              <span>RECORDS В KAFKA</span>
              <strong>{retryResult.recordsWritten}</strong>
              <small>{retryResult.duplicateWritten ? "обнаружен duplicate" : "нового дубля нет"}</small>
            </article>
            <article className={retryResult.duplicateSuppressed ? "good" : retryResult.duplicateWritten ? "danger" : ""}>
              <span>RETRY OUTCOME</span>
              <strong>{retryResult.duplicateSuppressed
                ? "DEDUPLICATED"
                : retryResult.duplicateWritten
                  ? "DUPLICATE"
                  : retryResult.ambiguousResult
                    ? "AMBIGUOUS"
                    : retryFault === "request-lost" && retryResult.attempts > 1
                      ? "RECOVERED"
                      : "NOT USED"}</strong>
              <small>idempotence={String(inspectorDelivery.idempotence)}</small>
            </article>
            <article className={retryResult.ambiguousResult ? "warning" : "good"}>
              <span>PRODUCER RESULT</span>
              <strong>{retryResult.ambiguousResult
                ? "TIMEOUT"
                : retryResult.producerResult === "ack"
                  ? "SUCCESS"
                  : retryResult.producerResult.toUpperCase()}</strong>
              <small>{retryResult.ambiguousResult ? "record мог сохраниться" : "финальный статус известен"}</small>
            </article>
          </div>

          <div className={`attempt-rail fault-${retryFault}`}>
            <div className={`attempt-node ${selectedEvent ? "done" : "preview"}`}>
              <i>1</i><span><strong>Attempt 1</strong><small>produce request</small></span>
            </div>
            <ArrowRight size={18} />
            <div className={`attempt-node ${selectedHasReached("leaderAppend") ? "done" : ""}`}>
              <i>{retryFault === "request-lost" ? <WifiOff size={13} /> : <Database size={13} />}</i>
              <span><strong>{retryFault === "request-lost" ? "Request lost" : "Leader append"}</strong><small>{retryFault === "request-lost" ? "до Broker" : "offset создан"}</small></span>
            </div>
            <ArrowRight size={18} />
            <div className={`attempt-node timeout ${selectedHasReached("networkTimeout") ? "active" : ""}`}>
              <i>!</i><span><strong>{retryFault === "none" ? "ACK" : "Timeout"}</strong><small>{retryFault === "ack-lost" ? "ACK потерян" : retryFault === "request-lost" ? "ответа нет" : "ответ доставлен"}</small></span>
            </div>
            <ArrowRight size={18} />
            <div className={`attempt-node retry ${selectedHasReached("retrySend") ? "active" : ""}`}>
              <i>2</i><span><strong>{retryResult.attempts > 1 ? "Retry" : "Без retry"}</strong><small>{retryResult.attempts > 1 ? `sequence ${selectedEvent?.producerSequence ?? "n"}` : "retries не нужен"}</small></span>
            </div>
            <ArrowRight size={18} />
            <div className={`attempt-node outcome ${retryStageReached ? "active" : ""} ${retryResult.duplicateWritten ? "danger" : "success"}`}>
              <i>{retryResult.duplicateWritten ? "2×" : <Check size={13} />}</i>
              <span><strong>{retryResult.duplicateWritten ? "Два offsets" : retryResult.duplicateSuppressed ? "Dedup" : "Готово"}</strong><small>{retryResult.recordsWritten} record(s)</small></span>
            </div>
          </div>

          <footer className="retry-tip">
            <Info size={17} />
            <p><strong>Ключевая проверка QA:</strong> число вызовов Producer, число ACK и число records в topic — три разных факта. Ошибка или timeout у Producer не доказывает отсутствие event.</p>
          </footer>
        </section>

        <section className="resilience-lab" aria-label="Управление отказоустойчивостью кластера">
          <header className="resilience-heading">
            <div>
              <span><Network size={16} /> CLUSTER RESILIENCE LAB · 0.3.2</span>
              <h2>Остановите Broker — Kafka пересчитает Leader и ISR</h2>
              <p>Действия доступны после завершения текущей отправки, чтобы состояние event оставалось однозначным.</p>
            </div>
            <div className="partition-focus-tabs" aria-label="Partition для наблюдения">
              {partitionStates.map((state) => (
                <button
                  key={state.partition}
                  className={focusedPartition === state.partition ? "active" : ""}
                  aria-pressed={focusedPartition === state.partition}
                  onClick={() => setFocusedPartition(state.partition)}
                >
                  P{state.partition}
                </button>
              ))}
            </div>
          </header>

          <div className="resilience-status-grid">
            <article>
              <span>CURRENT LEADER · P{focusedPartition}</span>
              <strong>{focusedPartitionState.leaderOnline
                ? `Broker ${focusedPartitionState.leaderBroker}`
                : "NO LEADER"}</strong>
              <small>{focusedPartitionState.leaderElected
                ? `elected · preferred B${focusedPartitionState.preferredLeaderBroker}`
                : "preferred leader"}</small>
            </article>
            <article>
              <span>ISR</span>
              <strong>[{focusedPartitionState.isrBrokers.map((broker) => `B${broker}`).join(", ") || "empty"}]</strong>
              <small>{focusedPartitionState.laggingReplicaBrokers.length
                ? `catching up: ${focusedPartitionState.laggingReplicaBrokers.map((broker) => `B${broker}`).join(", ")}`
                : "все online replicas синхронны"}</small>
            </article>
            <article className={focusedDeliveryResult.leaderAppended ? "available" : "blocked"}>
              <span>WRITE AVAILABILITY</span>
              <strong>{focusedDeliveryResult.leaderAppended ? "AVAILABLE" : "BLOCKED"}</strong>
              <small>{focusedDeliveryResult.leaderAppended
                ? `acks=${deliveryConfig.acks} · ISR=${focusedDeliveryResult.currentIsr}`
                : focusedDeliveryResult.errorCode ?? "ожидается recovery"}</small>
            </article>
            <article>
              <span>ASSIGNMENT</span>
              <strong>{focusedPartitionState.assignedReplicas.map((broker) => `B${broker}`).join(" → ")}</strong>
              <small>RF={deliveryConfig.replicationFactor} · первый Broker preferred</small>
            </article>
          </div>

          <div className="broker-control-grid">
            {brokerReplicas.map((replicas, index) => {
              const broker = index + 1;
              const isOnline = onlineBrokers.includes(broker);
              const focusReplica = replicas.find((replica) =>
                replica.partition === focusedPartition);
              return (
                <article key={broker} className={`broker-control-card ${isOnline ? "online" : "offline"}`}>
                  <div className="broker-control-title">
                    <span className="mini-rack"><i /><i /><i /></span>
                    <div>
                      <strong>Broker {broker}</strong>
                      <small>{isOnline ? "ONLINE" : "OFFLINE"}</small>
                    </div>
                    <i className={`health-light ${isOnline ? "online" : "offline"}`} />
                  </div>
                  <div className={`focus-replica ${focusReplica?.status ?? "none"}`}>
                    <span>P{focusedPartition} replica</span>
                    <strong>{focusReplica
                      ? `${focusReplica.role === "L" ? "LEADER" : "FOLLOWER"} · ${
                          focusReplica.status === "isr" ? "IN ISR"
                            : focusReplica.status === "lagging" ? "CATCHING UP"
                              : focusReplica.status.toUpperCase()
                        }`
                      : "NOT ASSIGNED"}</strong>
                  </div>
                  <div className="broker-replica-mini-list">
                    {replicas.map((replica) => (
                      <span key={`${replica.partition}-${replica.role}`} className={replica.status}>
                        P{replica.partition} {replica.role} · {replica.status}
                      </span>
                    ))}
                  </div>
                  <div className="broker-control-actions">
                    <button
                      className={isOnline ? "stop" : "start"}
                      disabled={clusterActionsLocked}
                      onClick={() => toggleBroker(broker)}
                    >
                      {isOnline ? <WifiOff size={14} /> : <Power size={14} />}
                      {isOnline ? "Остановить" : "Запустить"}
                    </button>
                    {isOnline && focusReplica?.status === "lagging" && (
                      <button
                        className="recover"
                        disabled={clusterActionsLocked}
                        onClick={() => recoverReplica(focusedPartition, broker)}
                      >
                        <RefreshCw size={14} /> Синхронизировать
                      </button>
                    )}
                  </div>
                </article>
              );
            })}
          </div>

          <footer className={`cluster-notice ${clusterNotice.tone}`}>
            {clusterNotice.tone === "success"
              ? <Check size={18} />
              : clusterNotice.tone === "warning" ? <AlertTriangle size={18} /> : <Info size={18} />}
            <div><strong>{clusterNotice.title}</strong><span>{clusterNotice.text}</span></div>
            <div className="cluster-quick-actions">
              <button
                disabled={clusterActionsLocked || !focusedPartitionState.leaderOnline}
                onClick={() => toggleBroker(focusedPartitionState.leaderBroker)}
              >
                <WifiOff size={14} /> Остановить Leader
              </button>
              {focusedPartitionState.leaderElected && (
                <button disabled={clusterActionsLocked} onClick={restorePreferredLeader}>
                  <RefreshCw size={14} /> Вернуть preferred
                </button>
              )}
              {focusedFollower && onlineBrokers.includes(focusedFollower)
                && laggingReplicas.includes(replicaKey(focusedPartition, focusedFollower)) && (
                <button
                  disabled={clusterActionsLocked}
                  onClick={() => recoverReplica(focusedPartition, focusedFollower)}
                >
                  <RefreshCw size={14} /> Вернуть Follower в ISR
                </button>
              )}
            </div>
          </footer>
          {clusterActionsLocked && (
            <p className="cluster-lock-note">
              <CirclePause size={14} /> Завершите текущую state machine — затем действия Broker разблокируются.
            </p>
          )}
        </section>

        <section className="composer-card" aria-label="Создание события">
          <div className="field-group"><label htmlFor="event-key">Event key</label>
            <input id="event-key" value={eventKey} onChange={(e) => setEventKey(e.target.value)} readOnly={scenarioId === "same-key"} />
          </div>
          <div className="field-group value-field"><label htmlFor="event-value">Value (JSON)</label>
            <input id="event-value" value={eventValue} onChange={(e) => setEventValue(e.target.value)} readOnly={scenarioId === "same-key"} />
          </div>
          <div className="delivery-target">
            <span>{faultMode === "none" ? "Маршрут" : "Инъекция сбоя"}</span>
            <strong>P{previewPartition} → Broker {previewResult.leaderBroker}</strong>
            {faultMode !== "none" && <small>{faultMode === "ack-lost" ? "ACK LOST" : "REQUEST LOST"}</small>}
          </div>
          <button className="send-button" onClick={sendEvent} disabled={!canLaunch}>
            <Send size={17} />{
              !canSend && scenarioId === "same-key" && sameKeyCount === 3
                ? "Сценарий завершён"
                : !isGuided && !previewResult.configValid && configErrorAccepted
                  ? "Изучить ConfigException"
                  : scenario.sendLabel
            }
          </button>
        </section>

        <div className="content-grid">
          <section className={`simulator-card ${showClusterFocus ? "focus-mode" : ""}`}>
            <div className="card-heading">
              <div><span>ФИЗИЧЕСКИЙ КЛАСТЕР + ЛОГИЧЕСКИЙ ЖУРНАЛ</span><h2>{TOPIC_NAME}</h2></div>
              <div className="sim-controls">
                <button className="icon-button" onClick={goPrevious} disabled={!activeEvent || activeEvent.stage === 0} aria-label="Предыдущий шаг" title="Предыдущий шаг · ←"><SkipBack size={18} /></button>
                <button className="icon-button primary-control" onClick={() => activeEvent && setPlaying((v) => !v)} disabled={!activeEvent} aria-label={playing ? "Пауза" : "Продолжить"} title="Пауза / продолжить · Space">{playing ? <CirclePause size={20} /> : <CirclePlay size={20} />}</button>
                <button className="icon-button" onClick={goNext} disabled={!activeEvent || activeEvent.stage === activeEvent.stepOrder.length - 1} aria-label="Следующий шаг" title="Следующий шаг · →"><SkipForward size={19} /></button>
                <button className="icon-button" onClick={replayActive} disabled={!activeEvent} aria-label="Повторить анимацию" title="Повторить анимацию · R"><TimerReset size={18} /></button>
                <button className="icon-button" onClick={resetScenario} disabled={!events.length} aria-label="Очистить события" title="Очистить события"><RotateCcw size={18} /></button>
                <button className={`icon-button ${showSettings ? "selected" : ""}`} onClick={() => setShowSettings((v) => !v)} aria-label="Скорость"><Settings2 size={18} /></button>
                <button className="focus-button" onClick={() => setShowClusterFocus((value) => !value)} aria-label={showClusterFocus ? "Выйти из режима фокуса" : "Развернуть схему"}>
                  {showClusterFocus ? <Minimize2 size={17} /> : <Maximize2 size={17} />}
                  <span>{showClusterFocus ? "Свернуть" : "Режим фокуса"}</span>
                </button>
                <span className="speed-badge">{speed}×</span>
                {showSettings && <div className="speed-popover"><span>Скорость анимации</span>{[0.5, 1, 2].map((value) =>
                  <button key={value} className={speed === value ? "active" : ""} onClick={() => setSpeed(value)}>{value}×</button>)}</div>}
              </div>
            </div>

            <div className="cluster-map">
              <svg className="connectors" viewBox="0 0 1000 520" preserveAspectRatio="none" aria-hidden="true">
                <defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto"><path d="M0,0 L0,6 L7,3 z" /></marker></defs>
                <path className={routeReached ? "route active" : "route"} d={`M140 260 C180 260 195 ${partitionY * 5.2} 500 ${partitionY * 5.2}`} markerEnd="url(#arrow)" />
                <path className={appendReached ? `route active ${activeEvent?.result.leaderAppended ? "" : "failed"}` : "route"} d={`M510 ${partitionY * 5.2} C560 ${partitionY * 5.2} 600 ${leaderY * 5.2} 640 ${leaderY * 5.2}`} markerEnd="url(#arrow)" />
                <path className={replicationReached ? "route replication active" : "route replication"} d={`M700 ${leaderY * 5.2} C790 ${leaderY * 5.2} 790 ${followerY * 5.2} 700 ${followerY * 5.2}`} markerEnd="url(#arrow)" />
                <path className={activeEvent && isConsumed(activeEvent) ? "route consume active" : "route consume"} d={`M750 ${leaderY * 5.2} C810 ${leaderY * 5.2} 825 260 865 260`} markerEnd="url(#arrow)" />
                <path className={ackReached ? `route ack active ${activeEvent?.result.producerResult === "error" ? "failed" : ""}` : "route ack"} d={`M640 ${leaderY * 5.2} C520 55 220 55 100 220`} markerEnd="url(#arrow)" />
              </svg>
              <span className="map-label producer-label">PRODUCER</span><span className="map-label brokers-label">BROKERS / REPLICAS</span><span className="map-label consumer-label">CONSUMER GROUP</span>
              {activeStep?.id === "networkTimeout" && (
                <div className={`network-fault-burst ${activeEvent?.faultMode ?? faultMode}`}>
                  <span><WifiOff size={18} /></span>
                  <strong>{activeEvent?.faultMode === "ack-lost" ? "ACK LOST" : "REQUEST LOST"}</strong>
                  <small>Producer ждёт до timeout</small>
                </div>
              )}
              {activeStep?.id === "retrySend" && (
                <div className="retry-wave"><RefreshCw size={16} /><span>ATTEMPT 2</span></div>
              )}
              <div className={`map-node producer-node ${activeEvent?.stage === 0 ? "current" : ""}`}><span className="node-icon violet"><Braces size={21} /></span><div><strong>Order API</strong><small>Producer</small></div></div>

              <section className="topic-journal">
                <header><div><span>LOGICAL VIEW · TOPIC</span><strong>{TOPIC_NAME}</strong></div><b><Layers3 size={13} /> append-only logs</b></header>
                <div className="partition-logs">
                  {eventsByPartition.map((partitionEvents, p) => <div key={p} className={`partition-log ${activeEvent?.partition === p && activeEvent.stage >= 1 ? "selected" : ""}`}>
                    <button className="partition-name" onClick={() => setSelectedPartition(p)} aria-label={`Открыть детали partition P${p}`}>
                      <span><strong>P{p}</strong><small>next {BASE_OFFSETS[p] + visibleRecordCount(p) + 1}</small></span>
                      <ChevronRight size={14} />
                    </button>
                    <div className="log-track"><span className="base-segment">… {BASE_OFFSETS[p]}</span>
                      {partitionEvents.map((event) => {
                        const eventLost = availableCopiesForEvent(event) === 0;
                        const duplicateVisible = event.result.duplicateWritten && isRetryResolved(event);
                        return <Fragment key={event.id}>
                          <button
                            className={`log-event original ${selectedEvent?.id === event.id ? "selected-event" : ""} ${eventLost ? "lost" : isRecordCommitted(event) ? "committed" : ""}`}
                            onClick={() => setSelectedEventId(event.id)}
                            title={`${event.id}, offset=${event.offset}${eventLost ? ", lost" : ""}`}
                          ><i /><b>{event.offset}</b><small>{eventLost ? "LOST" : "ORIG"}</small></button>
                          {duplicateVisible && <button
                            className={`log-event duplicate ${selectedEvent?.id === event.id ? "selected-event" : ""}`}
                            onClick={() => setSelectedEventId(event.id)}
                            title={`${event.id}, retry duplicate, offset=${event.retryOffset}`}
                          ><i /><b>{event.retryOffset}</b><small>DUP</small></button>}
                        </Fragment>;
                      })}
                      {!partitionEvents.length && <span className="empty-log">новых записей нет</span>}
                    </div>
                  </div>)}
                </div>
                <footer><span><i className="dot appended" /> appended</span><span><i className="dot committed" /> committed</span><span>Нажмите P0–P2 для деталей</span></footer>
              </section>

              {brokerReplicas.map((replicas, index) => {
                const broker = index + 1;
                const brokerOnline = onlineBrokers.includes(broker);
                const leaderCurrent = activeStep?.node === "leader" && broker === leaderBroker;
                const followerCurrent = activeStep?.node === "follower"
                  && activeEvent?.result.onlineReplicaBrokers.filter((item) =>
                    item !== activeEvent.result.leaderBroker).includes(broker);
                return <button
                  type="button"
                  key={broker}
                  onClick={() => setSelectedBroker(broker)}
                  aria-label={`Открыть Broker ${broker}`}
                  className={`broker-node b${broker} ${brokerOnline ? "" : "offline"} ${leaderCurrent ? "current" : ""} ${followerCurrent ? "current replica-current" : ""}`}
                >
                  <span className="server-rack"><i /><i /><i /></span><div><strong>Broker {broker} <em className={brokerOnline ? "online" : "offline"}>{brokerOnline ? "ONLINE" : "OFFLINE"}</em></strong><div className="replica-list">
                    {replicas.map((replica) => {
                      const match = activeEvent?.partition === replica.partition;
                      const replicated = match
                        && replica.role === "F"
                        && activeEvent?.result.onlineReplicaBrokers.includes(broker)
                        && isFollowerReplicated(activeEvent);
                      return <span
                        key={`${replica.partition}-${replica.role}`}
                        className={`${match ? `highlight role-${replica.role.toLowerCase()}` : ""} ${replica.status}`}
                      >
                        P{replica.partition} {replica.role}
                        {replica.status === "lagging" ? " ↻" : replicated && <Check size={9} />}
                      </span>;
                    })}
                  </div></div>
                </button>;
              })}
              <div className={`map-node consumer-node ${activeEvent && isConsumed(activeEvent) ? "current mint-current" : ""}`}><span className="node-icon mint"><Users size={21} /></span><div><strong>analytics-cg</strong><small>Consumer 1</small></div></div>
              {activeEvent && activeStep && <div className={`event-orb stage-${activeEvent.stage} ${activeDisposition ?? ""}`} style={{ left: `${orb.x}%`, top: `${orb.y}%` }}><b>{
                activeStep.node === "ack"
                  ? activeEvent.delivery.acks === "0" ? "∅" : activeEvent.result.producerResult === "error" ? "ERR" : "ACK"
                  : activeStep.id === "networkTimeout" ? "TIME"
                    : activeStep.id === "retrySend" ? "R2"
                      : activeStep.id === "retryResolution"
                        ? activeEvent.result.duplicateWritten ? "DUP" : "OK"
                  : activeDisposition === "failed" ? "!" : activeDisposition === "skipped" ? "—" : "E"
              }</b></div>}
              {!activeEvent && <div className="empty-map"><span><Send size={20} /></span><strong>Сценарий готов</strong><small>Отправьте event, чтобы запустить state machine</small></div>}
            </div>

            <div
              className="timeline"
              style={{ gridTemplateColumns: `repeat(${timelineSteps.length}, minmax(62px, 1fr))` }}
            >
              {timelineSteps.map((stepId, index) => {
                const item = STEP_BY_ID[stepId];
                const disposition = activeEvent ? stepDisposition(activeEvent, stepId) : "success";
                const reached = Boolean(activeEvent && index <= activeEvent.stage);
                return <button
                  key={item.id}
                  className={`${index === activeEvent?.stage ? "active" : ""} ${reached ? disposition : ""} ${activeEvent && index < activeEvent.stage && disposition === "success" ? "done" : ""}`}
                  onClick={() => selectStep(index)}
                  disabled={!activeEvent}
                >
                  <span>{reached && disposition === "failed" ? <CircleX size={13} />
                    : reached && disposition === "skipped" ? "—"
                      : activeEvent && index < activeEvent.stage ? <Check size={12} /> : index + 1}</span>
                  <small>{item.short}</small>
                </button>;
              })}
              <i className="timeline-track" style={{ left: `${timelineInset}%`, right: `${timelineInset}%` }} />
              {activeEvent && <i
                className="timeline-progress"
                style={{ left: `${timelineInset}%`, width: `${timelineProgress}%` }}
              />}
            </div>
          </section>

          <aside className="inspector">
            <section className="inspector-card explanation-card">
              <div className="step-counter"><span>ШАГ {activeEvent ? activeEvent.stage + 1 : "—"} ИЗ {activeEvent?.stepOrder.length ?? timelineSteps.length}</span>{playing && <i><span /> в процессе</i>}</div>
              <div className="explanation-icon">{activeStep?.node === "follower" ? <Database size={22} /> : activeStep?.node === "consumer" || activeStep?.node === "offset" ? <Users size={22} /> : <Info size={22} />}</div>
              <h2>{activeStepCopy.title}</h2><p>{activeStepCopy.description}</p>
              <div className="tech-note"><span>Технически</span><p>{activeStepCopy.technical}</p></div>
              <div className="keyboard-help"><span><kbd>Space</kbd> пауза</span><span><kbd>←</kbd><kbd>→</kbd> шаг</span><span><kbd>R</kbd> повтор</span></div>
            </section>

            <div className="inspector-details">
              <div className="inspector-tabs" role="tablist" aria-label="Детали события">
                <button
                  type="button"
                  role="tab"
                  aria-selected={inspectorTab === "event"}
                  className={inspectorTab === "event" ? "active" : ""}
                  onClick={() => setInspectorTab("event")}
                >
                  <Braces size={16} /> Event
                </button>
                <button
                  type="button"
                  role="tab"
                  aria-selected={inspectorTab === "delivery"}
                  className={inspectorTab === "delivery" ? "active" : ""}
                  onClick={() => setInspectorTab("delivery")}
                >
                  <ShieldCheck size={16} /> Delivery
                </button>
                <button
                  type="button"
                  role="tab"
                  aria-selected={inspectorTab === "lifecycle"}
                  className={inspectorTab === "lifecycle" ? "active" : ""}
                  onClick={() => setInspectorTab("lifecycle")}
                >
                  <Gauge size={16} /> Lifecycle
                </button>
              </div>

              {inspectorTab === "event" ? (
                <section className="inspector-card event-card" role="tabpanel">
                  <div className="section-title"><span><Braces size={18} /> Event Inspector</span>{selectedEvent && <b>{selectedEvent.id === activeEvent?.id ? "CURRENT" : "HISTORY"}</b>}</div>
                  <dl>
                    <div><dt>eventId</dt><dd>{selectedEvent?.id ?? "—"}</dd></div><div><dt>created</dt><dd>{selectedEvent?.createdAt ?? "—"}</dd></div>
                    <div><dt>topic</dt><dd>{TOPIC_NAME}</dd></div><div><dt>key</dt><dd>{selectedEvent ? selectedEvent.key || "null" : "—"}</dd></div>
                    <div><dt>partition</dt><dd>{selectedEvent ? `P${selectedEvent.partition}` : "—"}</dd></div><div><dt>offset</dt><dd>{selectedEvent && isLogVisible(selectedEvent) ? selectedEvent.offset : "—"}</dd></div>
                    <div><dt>leader at send</dt><dd>{selectedEvent ? `Broker ${selectedEvent.result.leaderBroker}` : "—"}</dd></div>
                    <div><dt>followers</dt><dd>{selectedEvent ? selectedEvent.result.onlineReplicaBrokers.filter((broker) => broker !== selectedEvent.result.leaderBroker).map((broker) => `B${broker}`).join(", ") || "none" : "—"}</dd></div>
                    <div><dt>acks</dt><dd>{selectedEvent?.delivery.acks ?? "—"}</dd></div>
                    <div><dt>copies now</dt><dd>{selectedEvent ? inspectorCopies : "—"}</dd></div>
                    <div><dt>producerId</dt><dd>{selectedEvent?.producerId ?? "—"}</dd></div>
                    <div><dt>epoch / sequence</dt><dd>{selectedEvent ? `${selectedEvent.producerEpoch} / ${selectedEvent.producerSequence}` : "—"}</dd></div>
                    <div><dt>attempts</dt><dd>{selectedEvent?.result.attempts ?? "—"}</dd></div>
                    <div><dt>retry offset</dt><dd>{selectedEvent?.retryOffset ?? "—"}</dd></div>
                  </dl>
                  <div className="event-flags"><span className={selectedEvent && isLogVisible(selectedEvent) ? "complete" : ""}>appended</span><span className={selectedEvent && isRecordCommitted(selectedEvent) ? "complete" : ""}>committed</span><span className={selectedEvent?.result.duplicateWritten ? "duplicate" : selectedEvent?.result.duplicateSuppressed ? "complete" : ""}>{selectedEvent?.result.duplicateWritten ? "duplicate" : selectedEvent?.result.duplicateSuppressed ? "deduplicated" : "no retry"}</span><span className={inspectorLost ? "lost" : ""}>{inspectorLost ? "record lost" : "data online"}</span><span className={selectedEvent && isOffsetCommitted(selectedEvent) ? "complete" : ""}>offset saved</span></div>
                  <div className="payload-preview">
                    <span>payload <button onClick={copyPayload}>{copied ? <Check size={12} /> : <Braces size={12} />}{copied ? "Скопировано" : "Копировать"}</button></span>
                    <code>{selectedEvent?.value ?? eventValue}</code>
                  </div>
                </section>
              ) : inspectorTab === "delivery" ? (
                <section className="inspector-card delivery-card" role="tabpanel">
                  <div className="section-title">
                    <span><ShieldCheck size={18} /> Delivery Result</span>
                    <b>{selectedEvent?.id ?? "FORECAST"}</b>
                  </div>
                  <div className={`producer-verdict ${inspectorProducerReady ? inspectorResult.producerResult : "waiting"}`}>
                    <span>Producer result</span>
                    <strong>{!inspectorProducerReady ? "WAITING"
                      : inspectorResult.producerResult === "ack" ? "ACK · SUCCESS"
                        : inspectorResult.producerResult === "unconfirmed" ? "NO ACK · UNKNOWN"
                          : inspectorResult.errorCode
                    }</strong>
                    <small>acks={inspectorDelivery.acks} · retries={inspectorDelivery.retries}</small>
                  </div>
                  <div className="delivery-facts">
                    <div><span>Kafka append</span><b>{selectedEvent
                      ? inspectorLost ? "record lost after failure"
                        : isLogVisible(selectedEvent) ? `P${inspectorPartition} / offset ${selectedEvent.offset}` : "ещё не выполнен"
                      : inspectorResult.leaderAppended ? `P${inspectorPartition} / offset next` : "нет record"}</b></div>
                    <div><span>Current Leader</span><b>{inspectorPartitionState.leaderOnline ? `B${inspectorPartitionState.leaderBroker}` : "NO LEADER"}</b></div>
                    <div><span>Current ISR</span><b>[{inspectorPartitionState.isrBrokers.map((broker) => `B${broker}`).join(", ") || "empty"}]</b></div>
                    <div><span>Physical copies now</span><b>{inspectorCopies} / RF {inspectorDelivery.replicationFactor}</b></div>
                    <div><span>Produce requests</span><b>{inspectorResult.attempts}</b></div>
                    <div><span>Records written</span><b>{inspectorResult.recordsWritten}</b></div>
                    <div><span>Record state</span><b>{inspectorLost ? "LOST" : inspectorCommitted ? "committed" : "not committed"}</b></div>
                    <div><span>Next Leader failure</span><b>{inspectorCopies === 0 ? "нет record" : inspectorCopies >= 2 ? "переживёт" : "может потеряться"}</b></div>
                    <div><span>Idempotence</span><b>{inspectorDelivery.idempotence ? "enabled" : "disabled"}</b></div>
                  </div>
                  {selectedEvent && selectedEvent.faultMode !== "none" && (
                    <div className="attempt-inspector">
                      <div><span>01</span><p><strong>Original request</strong><small>{selectedEvent.faultMode === "ack-lost" ? `record offset ${selectedEvent.offset}, ACK lost` : "request lost before append"}</small></p><b>DONE</b></div>
                      {selectedEvent.result.attempts > 1 && <div><span>02</span><p><strong>Retry request</strong><small>same eventId · sequence {selectedEvent.producerSequence}</small></p><b>{selectedEvent.result.duplicateWritten ? "DUP" : selectedEvent.result.duplicateSuppressed ? "DEDUP" : "ACK"}</b></div>}
                    </div>
                  )}
                  <div className="qa-note">
                    <Zap size={17} />
                    <p><strong>Что проверяет QA</strong>{
                      inspectorResult.duplicateWritten
                        ? "Найдите два offsets с одинаковыми eventId, key и payload: это сетевой retry, создавший физический дубль."
                        : inspectorResult.duplicateSuppressed
                          ? "Проверьте две попытки в логах Producer и только один record в partition: retry был подавлен Broker."
                          : inspectorResult.ambiguousResult
                            ? "После TimeoutException обязательно проверьте topic: record мог быть записан до потери ACK."
                            : inspectorDelivery.acks === "0"
                        ? "Нельзя подтверждать доставку только по успешному вызову send(): ACK вообще не ожидается."
                        : inspectorDelivery.acks === "1"
                          ? "Разделяйте успешный ACK Leader и наличие резервной копии на Follower."
                          : "Проверяйте вместе acks=all, размер ISR и min.insync.replicas."
                    }</p>
                  </div>
                </section>
              ) : (
                <section className="inspector-card lifecycle-card" role="tabpanel">
                  <div className="section-title"><span><Gauge size={18} /> Event Lifecycle</span><b>{selectedEvent?.id ?? "NO EVENT"}</b></div>
                  <div className="lifecycle-list">{selectedStepOrder.map((stepId) => {
                    const step = STEP_BY_ID[stepId];
                    const status = selectedLifecycle[step.id];
                    return <div key={step.id} className={`lifecycle-row ${status}`}>
                      <i>{status === "done" ? <Check size={12} /> : status === "failed" ? <CircleX size={12} /> : status === "skipped" ? "—" : null}</i>
                      <div><strong>{lifecycleLabels[step.id][0]}</strong><small>{lifecycleLabels[step.id][1]}</small></div>
                      <b>{statusText[status]}</b>
                    </div>;
                  })}</div>
                </section>
              )}
            </div>
          </aside>
        </div>

        <section className="lesson-strip"><div><span className="lesson-icon"><Zap size={20} /></span><div><strong>Что значит «event дошёл»?</strong><p>Сравните append, committed, fetch и offset commit — это разные проверяемые факты.</p></div></div><button onClick={() => setShowGlossary(true)}>Разобрать термины <ChevronRight size={16} /></button></section>
      </section>

      {selectedBroker !== null && <div className="partition-backdrop" onMouseDown={() => setSelectedBroker(null)}>
        <section className="broker-modal" role="dialog" aria-modal="true" aria-labelledby="broker-title" onMouseDown={(event) => event.stopPropagation()}>
          <header className="broker-modal-header">
            <div>
              <span>BROKER INSPECTOR · PHYSICAL CLUSTER</span>
              <h2 id="broker-title">Broker {selectedBroker}</h2>
              <p>Broker может быть ONLINE, но отдельная replica всё ещё догонять Leader и не входить в ISR.</p>
            </div>
            <button className="icon-button" onClick={() => setSelectedBroker(null)} aria-label="Закрыть Broker Inspector"><X size={23} /></button>
          </header>

          <div className={`broker-modal-health ${onlineBrokers.includes(selectedBroker) ? "online" : "offline"}`}>
            <span className="server-rack"><i /><i /><i /></span>
            <div>
              <span>PROCESS STATE</span>
              <strong>{onlineBrokers.includes(selectedBroker) ? "ONLINE" : "OFFLINE"}</strong>
            </div>
            <button disabled={clusterActionsLocked} onClick={() => toggleBroker(selectedBroker)}>
              {onlineBrokers.includes(selectedBroker) ? <WifiOff size={16} /> : <Power size={16} />}
              {onlineBrokers.includes(selectedBroker) ? "Остановить Broker" : "Запустить Broker"}
            </button>
          </div>

          <div className="broker-modal-replicas">
            <div className="broker-modal-table-head">
              <span>REPLICA</span><span>ROLE</span><span>SYNC STATE</span><span>ACTION</span>
            </div>
            {selectedBrokerReplicas.map((replica) => {
              const state = partitionStates[replica.partition];
              return <div key={replica.partition} className={`broker-modal-replica ${replica.status}`}>
                <strong>P{replica.partition}</strong>
                <span>{state.leaderOnline && state.leaderBroker === selectedBroker
                  ? "LEADER"
                  : "FOLLOWER"}</span>
                <span>{replica.status === "isr" ? "IN ISR"
                  : replica.status === "lagging" ? "CATCHING UP"
                    : replica.status.toUpperCase()}</span>
                {replica.status === "lagging" ? (
                  <button
                    disabled={clusterActionsLocked || !onlineBrokers.includes(selectedBroker)}
                    onClick={() => recoverReplica(replica.partition, selectedBroker)}
                  >
                    <RefreshCw size={14} /> Синхронизировать
                  </button>
                ) : (
                  <small>{replica.status === "isr" ? `ISR=[${state.isrBrokers.map((broker) => `B${broker}`).join(", ")}]` : "запустите Broker"}</small>
                )}
              </div>;
            })}
          </div>

          <footer className="broker-modal-note">
            <Info size={18} />
            <p><strong>Важно:</strong> запуск процесса Broker не возвращает replica в ISR мгновенно. Сначала она должна скачать недостающие records и сравняться с Leader.</p>
          </footer>
        </section>
      </div>}

      {selectedPartition !== null && <div className="partition-backdrop" onMouseDown={() => setSelectedPartition(null)}>
        <section className="partition-modal" role="dialog" aria-modal="true" aria-labelledby="partition-title" onMouseDown={(event) => event.stopPropagation()}>
          <header className="partition-modal-header">
            <div>
              <span>PARTITION INSPECTOR · {TOPIC_NAME}</span>
              <h2 id="partition-title">Partition P{selectedPartition}</h2>
              <p>Один упорядоченный append-only журнал и его физические replicas при RF={deliveryConfig.replicationFactor}.</p>
            </div>
            <button className="icon-button" onClick={() => setSelectedPartition(null)} aria-label="Закрыть детали partition"><X size={23} /></button>
          </header>

          <div className="partition-stats">
            <article><span>Следующий offset</span><strong>{BASE_OFFSETS[selectedPartition] + visibleRecordCount(selectedPartition) + 1}</strong></article>
            <article><span>Новых records</span><strong>{visibleRecordCount(selectedPartition)}</strong></article>
            <article><span>Current Leader</span><strong>{selectedPartitionState?.leaderOnline ? `Broker ${selectedPartitionState.leaderBroker}` : "NO LEADER"}</strong></article>
            <article><span>Current ISR</span><strong>[{selectedPartitionState?.isrBrokers.map((broker) => `B${broker}`).join(", ") || "empty"}]</strong></article>
          </div>

          <div className="partition-topology" aria-label="Связь логического журнала и физических реплик">
            <div><Layers3 size={20} /><span>Логический журнал</span><strong>P{selectedPartition}</strong></div>
            <ArrowRight size={20} />
            <div className="leader-copy"><Server size={20} /><span>Current Leader · принимает запись</span><strong>{selectedPartitionState?.leaderOnline ? `Broker ${selectedPartitionState.leaderBroker}` : "недоступен"}</strong></div>
            <ArrowRight size={20} />
            <div className="follower-copy"><Database size={20} />
              <span>{selectedPartitionReplicas.length > 1 ? "Followers · копируют журнал" : "Follower не предусмотрен"}</span>
              <strong>{selectedPartitionReplicas.filter((broker) => broker !== selectedPartitionState?.leaderBroker).map((broker) => `Broker ${broker}`).join(" + ") || "RF = 1"}</strong>
            </div>
          </div>

          <div className="partition-records">
            <div className="records-heading"><div><strong>Records</strong><span>Offset уникален только внутри P{selectedPartition}</span></div><b>RF = {deliveryConfig.replicationFactor}</b></div>
            <div className="records-table" role="table" aria-label={`Records partition P${selectedPartition}`}>
              <div className="record-row record-head" role="row">
                <span>OFFSET</span><span>EVENT / KEY</span><span>RECORD</span><span>REPLICAS</span><span>CONSUMER</span>
              </div>
              <div className="record-row base-record" role="row">
                <strong>… {BASE_OFFSETS[selectedPartition]}</strong><span>Предыдущие записи</span><span>committed</span><span>история</span><span>—</span>
              </div>
              {partitionDetails?.map((event) => {
                const copiesNow = availableCopiesForEvent(event);
                const lost = copiesNow === 0;
                const openEvent = () => {
                  setSelectedEventId(event.id);
                  setInspectorTab("event");
                  setSelectedPartition(null);
                };
                return <Fragment key={event.id}>
                  <button className={`record-row ${selectedEvent?.id === event.id ? "selected" : ""} ${lost ? "lost" : ""}`} role="row" onClick={openEvent}>
                    <strong>{event.offset}</strong>
                    <span><b>{event.id}</b><small>ORIGINAL · key: {event.key || "null"}</small></span>
                    <span className={lost ? "state-lost" : isRecordCommitted(event) ? "state-ok" : "state-warn"}>{lost ? "lost" : isRecordCommitted(event) ? "committed" : "appended"}</span>
                    <span>{lost ? "0 copies" : `${copiesNow} copies`}</span>
                    <span className={isOffsetCommitted(event) ? "state-ok" : ""}>{isOffsetCommitted(event) ? "offset saved" : isConsumed(event) ? "fetched" : "ожидает"}</span>
                  </button>
                  {event.result.duplicateWritten && isRetryResolved(event) && (
                    <button className={`record-row duplicate-record ${selectedEvent?.id === event.id ? "selected" : ""}`} role="row" onClick={openEvent}>
                      <strong>{event.retryOffset}</strong>
                      <span><b>{event.id}</b><small>DUPLICATE · same key / payload</small></span>
                      <span className="state-duplicate">duplicate</span>
                      <span>{copiesNow} copies</span>
                      <span>отдельный offset</span>
                    </button>
                  )}
                </Fragment>;
              })}
              {!partitionDetails?.length && <div className="partition-empty">
                <Send size={22} /><div><strong>Новых records пока нет</strong><p>Запустите сценарий и вернитесь сюда после шага Append.</p></div>
              </div>}
            </div>
          </div>

          <footer className="partition-hint"><Info size={18} /><p><strong>Важно:</strong> P{selectedPartition} в Topic и P{selectedPartition} L/F на Brokers — не разные очереди, а логический журнал и его физические копии.</p></footer>
        </section>
      </div>}

      {showGlossary && <div className="drawer-backdrop" onMouseDown={() => setShowGlossary(false)}><section className="glossary-modal" role="dialog" aria-modal="true" aria-labelledby="glossary-title" onMouseDown={(event) => event.stopPropagation()}>
        <div className="drawer-header"><div><span>Учебный словарь · 0.3.3</span><h2 id="glossary-title">Термины Kafka</h2></div><button className="icon-button" onClick={() => setShowGlossary(false)} aria-label="Закрыть словарь"><X size={24} /></button></div>
        <p className="drawer-intro">Определения привязаны к тому, что можно увидеть и проверить прямо в симуляции.</p>
        <div className="glossary-list">{GLOSSARY.map(([term, definition], index) => <article key={term}><span>{String(index + 1).padStart(2, "0")}</span><div><h3>{term}</h3><p>{definition}</p></div></article>)}</div>
        <div className="next-version"><Sparkles size={18} /><div><strong>Далее · версия 0.4</strong><p>Consumer group, lag, commit и rebalance.</p></div><ArrowRight size={17} /></div>
      </section></div>}
    </main>
  );
}
