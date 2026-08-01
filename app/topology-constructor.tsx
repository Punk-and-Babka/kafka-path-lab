"use client";

import {
  AlertTriangle, ArrowLeft, ArrowRight, BookOpen, Boxes, Check, ChevronRight,
  CirclePause, CirclePlay, CircleX, Database, Download, Grip, HelpCircle,
  Info, Link2, MousePointer2, Network, Play, Plus, Radio,
  RotateCcw, Send, Server, Settings2, ShieldCheck, Sparkles,
  Trash2, Unlink, Upload, Users, Workflow, X, Zap,
} from "lucide-react";
import {
  ChangeEvent, DragEvent, PointerEvent as ReactPointerEvent,
  useEffect, useMemo, useRef, useState,
} from "react";
import { GLOSSARY } from "./glossary-data";

type LearningMode = "sandbox" | "guided" | "constructor";
type NodeKind = "producer" | "topic" | "broker" | "consumer" | "database";
type AcksMode = "0" | "1" | "all";
type ReplicaRole = "leader" | "follower";

type Replica = {
  topicId: string;
  partition: number;
  role: ReplicaRole;
  inIsr: boolean;
};

type NodeConfig = {
  acks?: AcksMode;
  retries?: number;
  idempotence?: boolean;
  topicName?: string;
  partitions?: number;
  minIsr?: number;
  brokerId?: number;
  online?: boolean;
  replicas?: Replica[];
  groupId?: string;
  autoCommit?: boolean;
  tableName?: string;
};

type TopologyNode = {
  id: string;
  kind: NodeKind;
  label: string;
  x: number;
  y: number;
  config: NodeConfig;
};

type TopologyEdge = {
  id: string;
  from: string;
  to: string;
};

type ValidationIssue = {
  level: "error" | "warning";
  message: string;
  nodeId?: string;
};

type EventStep = {
  nodeId: string;
  title: string;
  detail: string;
  state: "success" | "warning" | "error";
};

type EventRun = {
  id: string;
  topicId: string;
  partition: number | null;
  key: string;
  payload: string;
  offset: number | null;
  steps: EventStep[];
  currentStep: number;
  finalState: "success" | "warning" | "error";
};

type SavedTopology = {
  format: "kafka-path-topology";
  version: 1;
  savedAt: string;
  nodes: TopologyNode[];
  edges: TopologyEdge[];
};

const CANVAS_WIDTH = 1320;
const CANVAS_HEIGHT = 670;
const NODE_WIDTH = 176;
const NODE_HEIGHT = 104;

const nodeMeta: Record<NodeKind, {
  title: string;
  description: string;
  className: string;
}> = {
  producer: { title: "Producer", description: "создаёт records", className: "producer" },
  topic: { title: "Topic", description: "логические partitions", className: "topic" },
  broker: { title: "Broker", description: "хранит replicas", className: "broker" },
  consumer: { title: "Consumer", description: "читает partition", className: "consumer" },
  database: { title: "Database", description: "принимает результат", className: "database" },
};

const kindIcon = (kind: NodeKind, size = 18) => {
  if (kind === "producer") return <Radio size={size} />;
  if (kind === "topic") return <Boxes size={size} />;
  if (kind === "broker") return <Server size={size} />;
  if (kind === "consumer") return <Users size={size} />;
  return <Database size={size} />;
};

const nodeDefaults = (kind: NodeKind, index: number): NodeConfig => {
  if (kind === "producer") return { acks: "all", retries: 3, idempotence: true };
  if (kind === "topic") return { topicName: `events.topic.${index}`, partitions: 3, minIsr: 2 };
  if (kind === "broker") return { brokerId: index, online: true, replicas: [] };
  if (kind === "consumer") return { groupId: `workers-${index}`, autoCommit: false };
  return { tableName: `service_events_${index}` };
};

const presetNodes = (): TopologyNode[] => [
  { id: "producer-1", kind: "producer", label: "orders-api", x: 48, y: 274, config: { acks: "all", retries: 3, idempotence: true } },
  { id: "topic-1", kind: "topic", label: "orders.events", x: 290, y: 274, config: { topicName: "orders.events", partitions: 3, minIsr: 2 } },
  { id: "broker-1", kind: "broker", label: "Broker 1", x: 550, y: 70, config: { brokerId: 1, online: true, replicas: [
    { topicId: "topic-1", partition: 0, role: "leader", inIsr: true },
    { topicId: "topic-1", partition: 2, role: "follower", inIsr: true },
  ] } },
  { id: "broker-2", kind: "broker", label: "Broker 2", x: 550, y: 274, config: { brokerId: 2, online: true, replicas: [
    { topicId: "topic-1", partition: 0, role: "follower", inIsr: true },
    { topicId: "topic-1", partition: 1, role: "leader", inIsr: true },
  ] } },
  { id: "broker-3", kind: "broker", label: "Broker 3", x: 550, y: 478, config: { brokerId: 3, online: true, replicas: [
    { topicId: "topic-1", partition: 1, role: "follower", inIsr: true },
    { topicId: "topic-1", partition: 2, role: "leader", inIsr: true },
  ] } },
  { id: "consumer-1", kind: "consumer", label: "orders-worker-1", x: 825, y: 205, config: { groupId: "orders-workers", autoCommit: false } },
  { id: "consumer-2", kind: "consumer", label: "orders-worker-2", x: 825, y: 360, config: { groupId: "orders-workers", autoCommit: false } },
  { id: "database-1", kind: "database", label: "service_db", x: 1080, y: 282, config: { tableName: "processed_orders" } },
];

const presetEdges = (): TopologyEdge[] => [
  { id: "edge-producer-topic", from: "producer-1", to: "topic-1" },
  { id: "edge-topic-b1", from: "topic-1", to: "broker-1" },
  { id: "edge-topic-b2", from: "topic-1", to: "broker-2" },
  { id: "edge-topic-b3", from: "topic-1", to: "broker-3" },
  { id: "edge-topic-c1", from: "topic-1", to: "consumer-1" },
  { id: "edge-topic-c2", from: "topic-1", to: "consumer-2" },
  { id: "edge-c1-db", from: "consumer-1", to: "database-1" },
  { id: "edge-c2-db", from: "consumer-2", to: "database-1" },
];

function hashKey(value: string) {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = ((hash << 5) - hash + value.charCodeAt(index)) | 0;
  }
  return Math.abs(hash);
}

function nextId(kind: NodeKind, nodes: TopologyNode[]) {
  const used = new Set(nodes.map((node) => node.id));
  let index = 1;
  while (used.has(`${kind}-${index}`)) index += 1;
  return `${kind}-${index}`;
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}

function nodeCenter(node: TopologyNode) {
  return { x: node.x + NODE_WIDTH / 2, y: node.y + NODE_HEIGHT / 2 };
}

function validateTopology(nodes: TopologyNode[], edges: TopologyEdge[]): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const producers = nodes.filter((node) => node.kind === "producer");
  const topics = nodes.filter((node) => node.kind === "topic");
  const brokers = nodes.filter((node) => node.kind === "broker");
  const consumers = nodes.filter((node) => node.kind === "consumer");

  if (!producers.length) issues.push({ level: "error", message: "Добавьте хотя бы один Producer." });
  if (!topics.length) issues.push({ level: "error", message: "Добавьте хотя бы один Topic." });
  if (!brokers.length) issues.push({ level: "error", message: "Добавьте хотя бы один Broker." });
  if (!consumers.length) issues.push({ level: "warning", message: "Consumer отсутствует: records останутся в Kafka." });

  producers.forEach((producer) => {
    const connectedTopic = edges.some((edge) => edge.from === producer.id
      && topics.some((topic) => topic.id === edge.to));
    if (!connectedTopic) issues.push({ level: "error", nodeId: producer.id, message: `${producer.label}: нет исходящей связи с Topic.` });
    if (producer.config.idempotence && producer.config.acks !== "all") {
      issues.push({ level: "error", nodeId: producer.id, message: `${producer.label}: enable.idempotence=true требует acks=all.` });
    }
    if (producer.config.idempotence && (producer.config.retries ?? 0) < 1) {
      issues.push({ level: "error", nodeId: producer.id, message: `${producer.label}: idempotence требует retries > 0.` });
    }
  });

  topics.forEach((topic) => {
    const count = clamp(topic.config.partitions ?? 1, 1, 12);
    const linkedBrokers = new Set(edges.filter((edge) => edge.from === topic.id).map((edge) => edge.to));
    for (let partition = 0; partition < count; partition += 1) {
      const replicas = brokers.flatMap((broker) => (broker.config.replicas ?? [])
        .filter((replica) => replica.topicId === topic.id && replica.partition === partition)
        .map((replica) => ({ broker, replica })));
      const leaders = replicas.filter(({ replica }) => replica.role === "leader");
      if (!replicas.length) issues.push({ level: "error", nodeId: topic.id, message: `${topic.label} · P${partition}: не размещена ни одна replica.` });
      if (leaders.length === 0) issues.push({ level: "error", nodeId: topic.id, message: `${topic.label} · P${partition}: Leader не назначен.` });
      if (leaders.length > 1) issues.push({ level: "error", nodeId: topic.id, message: `${topic.label} · P${partition}: назначено несколько Leader.` });
      if (leaders[0] && leaders[0].broker.config.online === false) {
        issues.push({ level: "error", nodeId: leaders[0].broker.id, message: `${topic.label} · P${partition}: Leader находится на выключенном Broker.` });
      }
      if (leaders[0] && !leaders[0].replica.inIsr) {
        issues.push({ level: "error", nodeId: leaders[0].broker.id, message: `${topic.label} · P${partition}: Leader должен входить в ISR.` });
      }
      replicas.forEach(({ broker }) => {
        if (!linkedBrokers.has(broker.id)) issues.push({ level: "warning", nodeId: broker.id, message: `${broker.label}: replica P${partition} существует, но Topic не соединён с Broker на холсте.` });
      });
    }
    const invalidReplicas = brokers.flatMap((broker) => (broker.config.replicas ?? [])
      .filter((replica) => replica.topicId === topic.id && replica.partition >= count)
      .map((replica) => ({ broker, replica })));
    invalidReplicas.forEach(({ broker, replica }) => issues.push({ level: "error", nodeId: broker.id, message: `${broker.label}: P${replica.partition} выходит за пределы ${count} partitions Topic.` }));
    const hasConsumer = edges.some((edge) => edge.from === topic.id
      && consumers.some((consumer) => consumer.id === edge.to));
    if (!hasConsumer) issues.push({ level: "warning", nodeId: topic.id, message: `${topic.label}: Consumer не подключён.` });
  });

  consumers.forEach((consumer) => {
    const hasInput = edges.some((edge) => edge.to === consumer.id
      && topics.some((topic) => topic.id === edge.from));
    if (!hasInput) issues.push({ level: "warning", nodeId: consumer.id, message: `${consumer.label}: нет входящей связи от Topic.` });
  });

  return issues;
}

function isSavedTopology(value: unknown): value is SavedTopology {
  if (!value || typeof value !== "object") return false;
  const candidate = value as Partial<SavedTopology>;
  return candidate.format === "kafka-path-topology"
    && candidate.version === 1
    && Array.isArray(candidate.nodes)
    && Array.isArray(candidate.edges);
}

export default function TopologyConstructor({
  onModeChange,
  onOpenGlossary,
  onOpenHelp,
}: {
  onModeChange: (mode: LearningMode) => void;
  onOpenGlossary: () => void;
  onOpenHelp: () => void;
}) {
  const [nodes, setNodes] = useState<TopologyNode[]>(presetNodes);
  const [edges, setEdges] = useState<TopologyEdge[]>(presetEdges);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>("topic-1");
  const [connectionTarget, setConnectionTarget] = useState("");
  const [replicaTopicId, setReplicaTopicId] = useState("topic-1");
  const [replicaPartition, setReplicaPartition] = useState(0);
  const [replicaRole, setReplicaRole] = useState<ReplicaRole>("follower");
  const [eventProducerId, setEventProducerId] = useState("producer-1");
  const [eventTopicId, setEventTopicId] = useState("topic-1");
  const [eventKey, setEventKey] = useState("order-8421");
  const [eventPayload, setEventPayload] = useState('{"orderId":8421,"status":"CREATED"}');
  const [eventRun, setEventRun] = useState<EventRun | null>(null);
  const [playing, setPlaying] = useState(false);
  const [validationOpen, setValidationOpen] = useState(true);
  const [importMessage, setImportMessage] = useState("");
  const [roundRobin, setRoundRobin] = useState(0);
  const [offsets, setOffsets] = useState<Record<string, number>>({});
  const [dragState, setDragState] = useState<{ id: string; offsetX: number; offsetY: number } | null>(null);
  const canvasRef = useRef<HTMLDivElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const edgeCounter = useRef(1);
  const eventCounter = useRef(1);

  const selectedNode = nodes.find((node) => node.id === selectedNodeId) ?? null;
  const topics = nodes.filter((node) => node.kind === "topic");
  const producers = nodes.filter((node) => node.kind === "producer");
  const brokers = nodes.filter((node) => node.kind === "broker");
  const consumers = nodes.filter((node) => node.kind === "consumer");
  const issues = useMemo(() => validateTopology(nodes, edges), [edges, nodes]);
  const errors = issues.filter((issue) => issue.level === "error");
  const warnings = issues.filter((issue) => issue.level === "warning");
  const effectiveProducerId = producers.some((node) => node.id === eventProducerId)
    ? eventProducerId : producers[0]?.id ?? "";
  const effectiveTopicId = topics.some((node) => node.id === eventTopicId)
    ? eventTopicId : topics[0]?.id ?? "";

  useEffect(() => {
    if (!dragState) return;
    const move = (event: PointerEvent) => {
      const canvas = canvasRef.current;
      if (!canvas) return;
      const rect = canvas.getBoundingClientRect();
      const parent = canvas.parentElement;
      const x = event.clientX - rect.left + (parent?.scrollLeft ?? 0) - dragState.offsetX;
      const y = event.clientY - rect.top + (parent?.scrollTop ?? 0) - dragState.offsetY;
      setNodes((current) => current.map((node) => node.id === dragState.id
        ? { ...node, x: clamp(x, 10, CANVAS_WIDTH - NODE_WIDTH - 10), y: clamp(y, 10, CANVAS_HEIGHT - NODE_HEIGHT - 10) }
        : node));
    };
    const stop = () => setDragState(null);
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", stop, { once: true });
    return () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", stop);
    };
  }, [dragState]);

  useEffect(() => {
    if (!playing || !eventRun) return;
    if (eventRun.currentStep >= eventRun.steps.length - 1) return;
    const timer = window.setTimeout(() => {
      setEventRun((current) => {
        if (!current) return current;
        const nextStep = Math.min(current.steps.length - 1, current.currentStep + 1);
        if (nextStep === current.steps.length - 1) setPlaying(false);
        return { ...current, currentStep: nextStep };
      });
    }, 850);
    return () => window.clearTimeout(timer);
  }, [eventRun, playing]);

  const updateNode = (id: string, updater: (node: TopologyNode) => TopologyNode) => {
    setNodes((current) => current.map((node) => node.id === id ? updater(node) : node));
  };

  const updateConfig = <K extends keyof NodeConfig>(key: K, value: NodeConfig[K]) => {
    if (!selectedNode) return;
    updateNode(selectedNode.id, (node) => ({ ...node, config: { ...node.config, [key]: value } }));
  };

  const addNode = (kind: NodeKind, x = 80, y = 80) => {
    const id = nextId(kind, nodes);
    const index = Number(id.split("-").pop()) || nodes.length + 1;
    const node: TopologyNode = {
      id,
      kind,
      label: kind === "broker" ? `Broker ${index}` : `${nodeMeta[kind].title.toLowerCase()}-${index}`,
      x: clamp(x, 10, CANVAS_WIDTH - NODE_WIDTH - 10),
      y: clamp(y, 10, CANVAS_HEIGHT - NODE_HEIGHT - 10),
      config: nodeDefaults(kind, index),
    };
    setNodes((current) => [...current, node]);
    setSelectedNodeId(id);
    if (kind === "topic") {
      setEventTopicId(id);
      setReplicaTopicId(id);
    }
    if (kind === "producer") setEventProducerId(id);
  };

  const handlePaletteDrag = (event: DragEvent<HTMLDivElement>, kind: NodeKind) => {
    event.dataTransfer.setData("application/x-kafka-node", kind);
    event.dataTransfer.effectAllowed = "copy";
  };

  const handleCanvasDrop = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    const kind = event.dataTransfer.getData("application/x-kafka-node") as NodeKind;
    if (!nodeMeta[kind]) return;
    const rect = event.currentTarget.getBoundingClientRect();
    const parent = event.currentTarget.parentElement;
    addNode(
      kind,
      event.clientX - rect.left + (parent?.scrollLeft ?? 0) - NODE_WIDTH / 2,
      event.clientY - rect.top + (parent?.scrollTop ?? 0) - NODE_HEIGHT / 2,
    );
  };

  const startNodeDrag = (event: ReactPointerEvent<HTMLButtonElement>, node: TopologyNode) => {
    if (event.button !== 0) return;
    const rect = event.currentTarget.parentElement?.getBoundingClientRect();
    setSelectedNodeId(node.id);
    setDragState({
      id: node.id,
      offsetX: rect ? event.clientX - rect.left : NODE_WIDTH / 2,
      offsetY: rect ? event.clientY - rect.top : 18,
    });
    event.preventDefault();
  };

  const addConnection = (direction: "out" | "in") => {
    if (!selectedNode || !connectionTarget || selectedNode.id === connectionTarget) return;
    const from = direction === "out" ? selectedNode.id : connectionTarget;
    const to = direction === "out" ? connectionTarget : selectedNode.id;
    if (edges.some((edge) => edge.from === from && edge.to === to)) return;
    setEdges((current) => [...current, { id: `edge-user-${edgeCounter.current++}`, from, to }]);
  };

  const deleteNode = (id: string) => {
    setNodes((current) => current.filter((node) => node.id !== id).map((node) => ({
      ...node,
      config: node.kind === "broker" ? {
        ...node.config,
        replicas: (node.config.replicas ?? []).filter((replica) => replica.topicId !== id),
      } : node.config,
    })));
    setEdges((current) => current.filter((edge) => edge.from !== id && edge.to !== id));
    setSelectedNodeId(null);
  };

  const addReplica = () => {
    if (!selectedNode || selectedNode.kind !== "broker" || !replicaTopicId) return;
    const duplicate = (selectedNode.config.replicas ?? []).some((replica) =>
      replica.topicId === replicaTopicId && replica.partition === replicaPartition);
    if (duplicate) return;
    updateConfig("replicas", [...(selectedNode.config.replicas ?? []), {
      topicId: replicaTopicId,
      partition: replicaPartition,
      role: replicaRole,
      inIsr: true,
    }]);
  };

  const updateReplica = (index: number, patch: Partial<Replica>) => {
    if (!selectedNode || selectedNode.kind !== "broker") return;
    updateConfig("replicas", (selectedNode.config.replicas ?? []).map((replica, replicaIndex) =>
      replicaIndex === index ? { ...replica, ...patch } : replica));
  };

  const removeReplica = (index: number) => {
    if (!selectedNode || selectedNode.kind !== "broker") return;
    updateConfig("replicas", (selectedNode.config.replicas ?? []).filter((_, replicaIndex) => replicaIndex !== index));
  };

  const loadPreset = () => {
    setNodes(presetNodes());
    setEdges(presetEdges());
    setSelectedNodeId("topic-1");
    setEventProducerId("producer-1");
    setEventTopicId("topic-1");
    setReplicaTopicId("topic-1");
    setEventRun(null);
    setPlaying(false);
    setOffsets({});
    setImportMessage("Готовый стенд загружен.");
  };

  const clearCanvas = () => {
    if (nodes.length && !window.confirm("Очистить холст и удалить текущую топологию? Сохраните её в .txt, если она нужна.")) return;
    setNodes([]);
    setEdges([]);
    setSelectedNodeId(null);
    setEventRun(null);
    setPlaying(false);
    setOffsets({});
    setImportMessage("Пустой холст готов.");
  };

  const exportTopology = () => {
    const payload: SavedTopology = {
      format: "kafka-path-topology",
      version: 1,
      savedAt: new Date().toISOString(),
      nodes,
      edges,
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `kafka-topology-${new Date().toISOString().slice(0, 10)}.txt`;
    anchor.click();
    URL.revokeObjectURL(url);
    setImportMessage("Топология сохранена в читаемый .txt файл.");
  };

  const importTopology = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;
    try {
      const parsed: unknown = JSON.parse(await file.text());
      if (!isSavedTopology(parsed)) throw new Error("unsupported");
      const nodeIds = new Set(parsed.nodes.map((node) => node.id));
      const safeNodes = parsed.nodes.map((node) => ({
        ...node,
        x: clamp(Number(node.x) || 10, 10, CANVAS_WIDTH - NODE_WIDTH - 10),
        y: clamp(Number(node.y) || 10, 10, CANVAS_HEIGHT - NODE_HEIGHT - 10),
      }));
      const safeEdges = parsed.edges.filter((edge) => nodeIds.has(edge.from) && nodeIds.has(edge.to));
      setNodes(safeNodes);
      setEdges(safeEdges);
      setSelectedNodeId(safeNodes[0]?.id ?? null);
      setEventRun(null);
      setPlaying(false);
      setOffsets({});
      setImportMessage(`Загружено: ${file.name}. Проверка схемы обновлена.`);
    } catch {
      setImportMessage("Не удалось загрузить файл: ожидается .txt из конструктора Kafka Path 0.6–0.7.");
    } finally {
      event.target.value = "";
    }
  };

  const simulateEvent = () => {
    const producer = nodes.find((node) => node.id === effectiveProducerId && node.kind === "producer");
    const topic = nodes.find((node) => node.id === effectiveTopicId && node.kind === "topic");
    const steps: EventStep[] = [];
    let finalState: EventRun["finalState"] = "success";
    let partition: number | null = null;
    let offset: number | null = null;

    if (!producer || !topic) {
      const fallback = producer ?? topic ?? nodes[0];
      steps.push({ nodeId: fallback?.id ?? "", title: "Запуск невозможен", detail: "Выберите существующие Producer и Topic.", state: "error" });
      finalState = "error";
    } else {
      steps.push({ nodeId: producer.id, title: `${producer.label}: send()`, detail: "Producer сериализует payload и headers.", state: "success" });
      if (producer.config.idempotence && producer.config.acks !== "all") {
        steps.push({ nodeId: producer.id, title: "ConfigException", detail: "enable.idempotence=true требует acks=all. Запрос не покинул Producer.", state: "error" });
        finalState = "error";
      } else if (producer.config.idempotence && (producer.config.retries ?? 0) < 1) {
        steps.push({ nodeId: producer.id, title: "ConfigException", detail: "Idempotent Producer требует retries > 0.", state: "error" });
        finalState = "error";
      } else if (!edges.some((edge) => edge.from === producer.id && edge.to === topic.id)) {
        steps.push({ nodeId: producer.id, title: "Нет маршрута к Topic", detail: "Соедините Producer → Topic на холсте.", state: "error" });
        finalState = "error";
      } else {
        const partitionCount = clamp(topic.config.partitions ?? 1, 1, 12);
        partition = eventKey.trim() ? hashKey(eventKey) % partitionCount : roundRobin % partitionCount;
        if (!eventKey.trim()) setRoundRobin((value) => value + 1);
        steps.push({ nodeId: topic.id, title: `${topic.label}: P${partition}`, detail: eventKey.trim() ? `key «${eventKey}» → stable hash → P${partition}` : `key=null → round-robin → P${partition}`, state: "success" });
        const replicas = brokers.flatMap((broker) => (broker.config.replicas ?? [])
          .filter((replica) => replica.topicId === topic.id && replica.partition === partition)
          .map((replica) => ({ broker, replica })));
        const leaders = replicas.filter(({ replica }) => replica.role === "leader");
        if (leaders.length !== 1) {
          steps.push({ nodeId: topic.id, title: "Leader недоступен", detail: leaders.length ? "Для partition назначено несколько Leader." : "Назначьте Leader replica на одном Broker.", state: "error" });
          finalState = "error";
        } else {
          const leader = leaders[0];
          if (leader.broker.config.online === false) {
            steps.push({ nodeId: leader.broker.id, title: `${leader.broker.label}: OFFLINE`, detail: `Leader P${partition} недоступен. Нужен failover или запуск Broker.`, state: "error" });
            finalState = "error";
          } else if (!leader.replica.inIsr) {
            steps.push({ nodeId: leader.broker.id, title: "Некорректный Leader", detail: `Leader P${partition} не входит в ISR.`, state: "error" });
            finalState = "error";
          } else {
            const isrReplicas = replicas.filter(({ broker, replica }) => broker.config.online !== false && replica.inIsr);
            if (producer.config.acks === "all" && isrReplicas.length < (topic.config.minIsr ?? 1)) {
              steps.push({ nodeId: leader.broker.id, title: "NotEnoughReplicas", detail: `ISR=${isrReplicas.length}, min.insync.replicas=${topic.config.minIsr ?? 1}. Append отклонён.`, state: "error" });
              finalState = "error";
            } else {
              const offsetKey = `${topic.id}:${partition}`;
              offset = (offsets[offsetKey] ?? 300) + 1;
              setOffsets((current) => ({ ...current, [offsetKey]: offset as number }));
              steps.push({ nodeId: leader.broker.id, title: `${leader.broker.label}: append`, detail: `Record получил P${partition} / offset ${offset}.`, state: "success" });
              isrReplicas.filter(({ broker }) => broker.id !== leader.broker.id).forEach(({ broker }) => {
                steps.push({ nodeId: broker.id, title: `${broker.label}: follower sync`, detail: `Replica P${partition} скопировала offset ${offset} и остаётся в ISR.`, state: "success" });
              });
              steps.push({ nodeId: producer.id, title: producer.config.acks === "0" ? "Producer не ждёт ACK" : `ACK ${producer.config.acks}`, detail: producer.config.acks === "0" ? "Запись произошла, но Producer не получает подтверждение." : `Подтверждение после append${producer.config.acks === "all" ? " и ISR replication" : " на Leader"}.`, state: producer.config.acks === "0" ? "warning" : "success" });

              const connectedConsumers = consumers.filter((consumer) => edges.some((edge) => edge.from === topic.id && edge.to === consumer.id));
              if (!connectedConsumers.length) {
                steps.push({ nodeId: topic.id, title: "Record ждёт Consumer", detail: "Kafka сохранила запись, но к Topic не подключён Consumer.", state: "warning" });
                finalState = "warning";
              } else {
                const consumer = connectedConsumers[partition % connectedConsumers.length];
                steps.push({ nodeId: consumer.id, title: `${consumer.label}: fetch`, detail: `${consumer.config.groupId ?? "consumer-group"} получает P${partition} / offset ${offset}.`, state: "success" });
                const database = nodes.find((node) => node.kind === "database" && edges.some((edge) => edge.from === consumer.id && edge.to === node.id));
                if (database) {
                  steps.push({ nodeId: database.id, title: `${database.label}: write`, detail: `Результат сохранён в ${database.config.tableName ?? "table"}; Consumer может commit offset ${offset}.`, state: "success" });
                } else {
                  steps.push({ nodeId: consumer.id, title: "Обработано без sink", detail: "Consumer получил event, но Database не подключена.", state: "warning" });
                  finalState = "warning";
                }
              }
            }
          }
        }
      }
    }

    const run: EventRun = {
      id: `evt_${String(eventCounter.current++).padStart(3, "0")}`,
      topicId: effectiveTopicId,
      partition,
      key: eventKey,
      payload: eventPayload,
      offset,
      steps,
      currentStep: 0,
      finalState,
    };
    setEventRun(run);
    setPlaying(steps.length > 1);
  };

  const currentStep = eventRun?.steps[eventRun.currentStep] ?? null;
  const reachedNodeIds = new Set(eventRun?.steps.slice(0, (eventRun?.currentStep ?? -1) + 1).map((step) => step.nodeId) ?? []);
  const activeEdgeIds = new Set(edges.filter((edge) => reachedNodeIds.has(edge.from) && reachedNodeIds.has(edge.to)).map((edge) => edge.id));
  const selectedTopicForReplica = topics.find((topic) => topic.id === replicaTopicId) ?? topics[0];

  return (
    <main className="app-shell constructor-shell">
      <header className="topbar">
        <a className="brand" href="#" aria-label="Kafka Path — главная">
          <span className="brand-mark"><Network size={19} /></span>
          <span>Kafka Path</span><span className="version">version 0.7.3</span>
        </a>
        <div className="header-actions">
          <span className="mode-pill constructor"><Workflow size={14} /> Конструктор</span>
          <button className="help-cta" onClick={onOpenHelp} aria-haspopup="dialog">
            <HelpCircle size={18} /><span><strong>Подсказка</strong><small>Что здесь доступно</small></span>
          </button>
          <button className="glossary-cta" onClick={onOpenGlossary} aria-haspopup="dialog">
            <span className="glossary-cta-icon"><BookOpen size={19} /></span>
            <span className="glossary-cta-copy"><strong>Словарь Kafka</strong><small>{GLOSSARY.length} терминов с примерами</small></span>
            <span className="glossary-cta-badge" aria-hidden="true">{GLOSSARY.length}</span>
          </button>
        </div>
      </header>

      <section className="workspace constructor-workspace">
        <div className="intro-row constructor-intro">
          <div>
            <p className="eyebrow"><Workflow size={14} /> Kafka topology builder</p>
            <h1>Соберите собственную Kafka-систему</h1>
            <p>Размещайте элементы на холсте, назначайте replicas и Leader, соединяйте сервисы и наблюдайте путь event по вашей топологии.</p>
          </div>
          <div className="cluster-summary">
            <span><Boxes size={15} /> {nodes.length} nodes</span>
            <span><Link2 size={15} /> {edges.length} connections</span>
            <span><Server size={15} /> {brokers.filter((broker) => broker.config.online !== false).length}/{brokers.length} online</span>
            <span className={errors.length ? "summary-error" : "summary-ok"}>{errors.length ? <CircleX size={15} /> : <ShieldCheck size={15} />} {errors.length ? `${errors.length} errors` : "valid"}</span>
          </div>
        </div>

        <section className="learning-mode-switch three-modes" aria-label="Режим работы симулятора" data-help="Переключение между тремя форматами работы">
          <button className="sandbox" onClick={() => onModeChange("sandbox")}>
            <span><Settings2 size={19} /></span><div><strong>Свободная песочница</strong><small>Готовый стенд и ручные эксперименты</small></div>
          </button>
          <button onClick={() => onModeChange("guided")}>
            <span><Sparkles size={19} /></span><div><strong>Учебные сценарии</strong><small>10 готовых ситуаций и объяснения</small></div>
          </button>
          <button className="constructor active" aria-pressed="true">
            <span><Workflow size={19} /></span><div><strong>Конструктор</strong><small>Своя топология, replicas и цепочка</small></div><Check size={18} />
          </button>
        </section>

        <section className="constructor-toolbar" aria-label="Инструменты топологии" data-help="Preset, очистка, сохранение и загрузка схемы">
          <div className="constructor-presets">
            <button className="primary" onClick={loadPreset}><Play size={15} /> Готовый стенд</button>
            <button onClick={clearCanvas}><RotateCcw size={15} /> Пустой холст</button>
          </div>
          <div className="constructor-file-actions">
            <button onClick={exportTopology}><Download size={15} /> Сохранить .txt</button>
            <button onClick={() => fileInputRef.current?.click()}><Upload size={15} /> Загрузить .txt</button>
            <input ref={fileInputRef} type="file" accept=".txt,application/json,text/plain" onChange={(event) => void importTopology(event)} />
          </div>
          <button className={`validation-trigger ${errors.length ? "invalid" : "valid"}`} onClick={() => setValidationOpen((value) => !value)}>
            {errors.length ? <AlertTriangle size={16} /> : <Check size={16} />}
            {errors.length ? `Проверка: ${errors.length} ошибок` : `Схема корректна${warnings.length ? ` · ${warnings.length} предупреждений` : ""}`}
            <ChevronRight size={15} />
          </button>
        </section>

        {importMessage && <div className="constructor-toast"><Info size={15} /><span>{importMessage}</span><button onClick={() => setImportMessage("")} aria-label="Закрыть"><X size={14} /></button></div>}

        {validationOpen && <section className={`topology-validation ${errors.length ? "invalid" : "valid"}`}>
          <header><span>{errors.length ? <AlertTriangle size={17} /> : <ShieldCheck size={17} />}</span><div><strong>{errors.length ? "Топология требует исправлений" : "Топология готова к запуску"}</strong><small>Проверяются связи, Leader, ISR и Producer config</small></div></header>
          <div className="validation-issues">
            {!issues.length && <span className="validation-ok"><Check size={14} /> Ошибок и предупреждений нет.</span>}
            {issues.slice(0, 8).map((issue, index) => <button key={`${issue.message}-${index}`} className={issue.level} onClick={() => issue.nodeId && setSelectedNodeId(issue.nodeId)}>
              {issue.level === "error" ? <CircleX size={13} /> : <AlertTriangle size={13} />}<span>{issue.message}</span>
            </button>)}
            {issues.length > 8 && <span className="validation-more">ещё {issues.length - 8}</span>}
          </div>
        </section>}

        <div className="constructor-layout">
          <aside className="node-palette" data-help="Добавьте элементы на холст">
            <header><span>ЭЛЕМЕНТЫ</span><strong>Перетащите на холст</strong></header>
            <div className="palette-list">
              {(Object.keys(nodeMeta) as NodeKind[]).map((kind) => <div key={kind} className={`palette-item ${kind}`} draggable onDragStart={(event) => handlePaletteDrag(event, kind)}>
                <span>{kindIcon(kind, 19)}</span><div><strong>{nodeMeta[kind].title}</strong><small>{nodeMeta[kind].description}</small></div><Grip size={16} />
                <button onClick={() => addNode(kind, 100 + nodes.length * 18, 90 + nodes.length * 14)} aria-label={`Добавить ${nodeMeta[kind].title}`}><Plus size={14} /></button>
              </div>)}
            </div>
            <div className="palette-hint"><MousePointer2 size={16} /><p><strong>Как собирать</strong><span>Добавьте узлы, соедините их в инспекторе справа, затем назначьте replicas внутри Broker.</span></p></div>
          </aside>

          <section className="topology-stage" aria-label="Холст конструктора" data-help="Перемещайте узлы и наблюдайте связи">
            <header className="stage-heading">
              <div><span>TOPOLOGY CANVAS</span><strong>{nodes.length ? "Перетаскивайте узлы за верхнюю панель" : "Холст пуст — добавьте первый элемент"}</strong></div>
              <div className="stage-legend"><span><i className="leader" /> Leader</span><span><i className="follower" /> Follower</span><span><i className="isr" /> ISR</span></div>
            </header>
            <div className="canvas-scroll">
              <div
                ref={canvasRef}
                className={`topology-canvas ${nodes.length ? "" : "empty"}`}
                style={{ width: CANVAS_WIDTH, height: CANVAS_HEIGHT }}
                onDragOver={(event) => { event.preventDefault(); event.dataTransfer.dropEffect = "copy"; }}
                onDrop={handleCanvasDrop}
              >
                <svg className="topology-connections" width={CANVAS_WIDTH} height={CANVAS_HEIGHT} aria-hidden="true">
                  <defs><marker id="topology-arrow" markerWidth="9" markerHeight="9" refX="8" refY="4.5" orient="auto"><path d="M0,0 L9,4.5 L0,9 z" /></marker></defs>
                  {edges.map((edge) => {
                    const from = nodes.find((node) => node.id === edge.from);
                    const to = nodes.find((node) => node.id === edge.to);
                    if (!from || !to) return null;
                    const start = nodeCenter(from);
                    const end = nodeCenter(to);
                    const bend = Math.max(55, Math.abs(end.x - start.x) * 0.42);
                    return <path key={edge.id} className={activeEdgeIds.has(edge.id) ? "active" : ""} d={`M ${start.x} ${start.y} C ${start.x + bend} ${start.y}, ${end.x - bend} ${end.y}, ${end.x} ${end.y}`} markerEnd="url(#topology-arrow)" />;
                  })}
                </svg>

                {!nodes.length && <div className="canvas-empty-state"><Workflow size={35} /><strong>Начните с Producer или загрузите готовый стенд</strong><span>Элементы можно добавить кнопкой слева или перетащить на сетку.</span><button onClick={loadPreset}><Play size={15} /> Загрузить preset</button></div>}

                {nodes.map((node) => {
                  const selected = selectedNodeId === node.id;
                  const active = currentStep?.nodeId === node.id;
                  const reached = reachedNodeIds.has(node.id);
                  const nodeReplicas = node.kind === "broker" ? node.config.replicas ?? [] : [];
                  return <article
                    key={node.id}
                    className={`topology-node ${nodeMeta[node.kind].className} ${selected ? "selected" : ""} ${active ? "event-active" : reached ? "event-reached" : ""} ${node.kind === "broker" && node.config.online === false ? "offline" : ""}`}
                    style={{ transform: `translate(${node.x}px, ${node.y}px)` }}
                    onClick={() => setSelectedNodeId(node.id)}
                  >
                    <button className="node-drag-handle" onPointerDown={(event) => startNodeDrag(event, node)} aria-label={`Переместить ${node.label}`}><Grip size={14} /><span>{node.id}</span>{node.kind === "broker" && <i className={node.config.online === false ? "offline" : "online"} />}</button>
                    <div className="node-main"><span className="node-icon">{kindIcon(node.kind, 20)}</span><div><strong>{node.label}</strong><small>{node.kind === "topic" ? `${node.config.partitions ?? 1} partitions · min ISR ${node.config.minIsr ?? 1}` : node.kind === "broker" ? `${nodeReplicas.length} replicas · ${node.config.online === false ? "OFFLINE" : "ONLINE"}` : node.kind === "producer" ? `acks=${node.config.acks} · retries=${node.config.retries}` : node.kind === "consumer" ? node.config.groupId : node.config.tableName}</small></div></div>
                    {node.kind === "topic" && <div className="topic-partitions">{Array.from({ length: clamp(node.config.partitions ?? 1, 1, 12) }, (_, partition) => <span key={partition}>P{partition}</span>)}</div>}
                    {node.kind === "broker" && <div className="node-replicas">{nodeReplicas.slice(0, 4).map((replica, index) => <span key={`${replica.topicId}-${replica.partition}-${index}`} className={`${replica.role} ${replica.inIsr ? "isr" : "out"}`}>P{replica.partition} {replica.role === "leader" ? "L" : "F"}</span>)}{nodeReplicas.length > 4 && <span>+{nodeReplicas.length - 4}</span>}</div>}
                    {active && <span className="constructor-event-orb"><Zap size={11} /></span>}
                  </article>;
                })}
              </div>
            </div>
          </section>

          <aside className="node-inspector" data-help="Настройки выбранного узла">
            <header><span>INSPECTOR</span><strong>{selectedNode ? nodeMeta[selectedNode.kind].title : "Элемент не выбран"}</strong></header>
            {!selectedNode ? <div className="inspector-empty"><MousePointer2 size={27} /><p>Выберите узел на холсте, чтобы изменить его настройки и связи.</p></div> : <>
              <div className={`inspector-node-title ${selectedNode.kind}`}><span>{kindIcon(selectedNode.kind, 21)}</span><input value={selectedNode.label} onChange={(event) => updateNode(selectedNode.id, (node) => ({ ...node, label: event.target.value }))} aria-label="Название элемента" /></div>

              {selectedNode.kind === "producer" && <div className="property-section">
                <h3>Producer config</h3>
                <label>acks<select value={selectedNode.config.acks} onChange={(event) => updateConfig("acks", event.target.value as AcksMode)}><option value="0">0</option><option value="1">1</option><option value="all">all</option></select></label>
                <label>retries<input type="number" min="0" max="20" value={selectedNode.config.retries ?? 0} onChange={(event) => updateConfig("retries", Number(event.target.value))} /></label>
                <button className={`property-toggle ${selectedNode.config.idempotence ? "active" : ""}`} onClick={() => updateConfig("idempotence", !selectedNode.config.idempotence)}><span><strong>enable.idempotence</strong><small>dedup для retry</small></span><i><b /></i></button>
              </div>}

              {selectedNode.kind === "topic" && <div className="property-section">
                <h3>Topic config</h3>
                <label>topic.name<input value={selectedNode.config.topicName ?? ""} onChange={(event) => { updateConfig("topicName", event.target.value); updateNode(selectedNode.id, (node) => ({ ...node, label: event.target.value || node.label })); }} /></label>
                <label>partitions<input type="number" min="1" max="12" value={selectedNode.config.partitions ?? 1} onChange={(event) => updateConfig("partitions", clamp(Number(event.target.value), 1, 12))} /></label>
                <label>min.insync.replicas<input type="number" min="1" max="12" value={selectedNode.config.minIsr ?? 1} onChange={(event) => updateConfig("minIsr", clamp(Number(event.target.value), 1, 12))} /></label>
              </div>}

              {selectedNode.kind === "broker" && <div className="property-section broker-properties">
                <h3>Broker state</h3>
                <label>broker.id<input type="number" min="1" value={selectedNode.config.brokerId ?? 1} onChange={(event) => updateConfig("brokerId", Number(event.target.value))} /></label>
                <button className={`broker-power ${selectedNode.config.online === false ? "offline" : "online"}`} onClick={() => updateConfig("online", selectedNode.config.online === false)}><Server size={15} /><span>{selectedNode.config.online === false ? "OFFLINE · запустить" : "ONLINE · остановить"}</span></button>
                <h3>Replicas на Broker</h3>
                <div className="replica-editor-list">{(selectedNode.config.replicas ?? []).map((replica, index) => {
                  const topic = topics.find((item) => item.id === replica.topicId);
                  return <div key={`${replica.topicId}-${replica.partition}-${index}`} className={`replica-editor-row ${replica.role}`}>
                    <span><strong>{topic?.label ?? "missing topic"} · P{replica.partition}</strong><small>{replica.inIsr ? "IN ISR" : "OUT OF ISR"}</small></span>
                    <select value={replica.role} onChange={(event) => updateReplica(index, { role: event.target.value as ReplicaRole })}><option value="leader">Leader</option><option value="follower">Follower</option></select>
                    <button className={replica.inIsr ? "in-isr" : ""} onClick={() => updateReplica(index, { inIsr: !replica.inIsr })}>ISR</button>
                    <button onClick={() => removeReplica(index)} aria-label="Удалить replica"><X size={13} /></button>
                  </div>;
                })}{!(selectedNode.config.replicas ?? []).length && <p className="empty-property">Replica ещё не назначены.</p>}</div>
                <div className="replica-add-row">
                  <select value={replicaTopicId} onChange={(event) => { setReplicaTopicId(event.target.value); setReplicaPartition(0); }}><option value="">Topic</option>{topics.map((topic) => <option key={topic.id} value={topic.id}>{topic.label}</option>)}</select>
                  <select value={replicaPartition} onChange={(event) => setReplicaPartition(Number(event.target.value))}>{Array.from({ length: clamp(selectedTopicForReplica?.config.partitions ?? 1, 1, 12) }, (_, partition) => <option key={partition} value={partition}>P{partition}</option>)}</select>
                  <select value={replicaRole} onChange={(event) => setReplicaRole(event.target.value as ReplicaRole)}><option value="leader">Leader</option><option value="follower">Follower</option></select>
                  <button onClick={addReplica}><Plus size={13} /> Replica</button>
                </div>
              </div>}

              {selectedNode.kind === "consumer" && <div className="property-section">
                <h3>Consumer config</h3>
                <label>group.id<input value={selectedNode.config.groupId ?? ""} onChange={(event) => updateConfig("groupId", event.target.value)} /></label>
                <button className={`property-toggle ${selectedNode.config.autoCommit ? "active" : ""}`} onClick={() => updateConfig("autoCommit", !selectedNode.config.autoCommit)}><span><strong>enable.auto.commit</strong><small>{selectedNode.config.autoCommit ? "автоматически" : "вручную после обработки"}</small></span><i><b /></i></button>
              </div>}

              {selectedNode.kind === "database" && <div className="property-section">
                <h3>Sink config</h3>
                <label>table / collection<input value={selectedNode.config.tableName ?? ""} onChange={(event) => updateConfig("tableName", event.target.value)} /></label>
              </div>}

              <div className="property-section connection-editor">
                <h3>Связи</h3>
                <select value={connectionTarget} onChange={(event) => setConnectionTarget(event.target.value)}><option value="">Выберите узел</option>{nodes.filter((node) => node.id !== selectedNode.id).map((node) => <option key={node.id} value={node.id}>{node.label} · {nodeMeta[node.kind].title}</option>)}</select>
                <div><button onClick={() => addConnection("out")} disabled={!connectionTarget}><Link2 size={13} /> Этот → выбранный</button><button onClick={() => addConnection("in")} disabled={!connectionTarget}><Link2 size={13} /> Выбранный → этот</button></div>
                <div className="connection-list">{edges.filter((edge) => edge.from === selectedNode.id || edge.to === selectedNode.id).map((edge) => {
                  const other = nodes.find((node) => node.id === (edge.from === selectedNode.id ? edge.to : edge.from));
                  return <span key={edge.id}><b>{edge.from === selectedNode.id ? "OUT" : "IN"}</b>{other?.label ?? "missing"}<button onClick={() => setEdges((current) => current.filter((item) => item.id !== edge.id))}><Unlink size={12} /></button></span>;
                })}</div>
              </div>
              <button className="delete-node" onClick={() => deleteNode(selectedNode.id)}><Trash2 size={14} /> Удалить элемент</button>
            </>}
          </aside>
        </div>

        <section className="constructor-event-lab" aria-label="Запуск event по собственной топологии" data-help="Запустите event по собранной схеме">
          <header><div><span><Send size={15} /> EVENT RUNNER</span><h2>Проверьте путь по собранной схеме</h2><p>Симулятор вычислит partition, найдёт Leader и ISR, затем определит Consumer и sink по вашим связям.</p></div><button className="constructor-send" onClick={simulateEvent} disabled={!effectiveProducerId || !effectiveTopicId || !eventPayload.trim()}><Send size={17} /> Отправить event</button></header>
          <div className="event-runner-grid">
            <div className="runner-inputs">
              <label>Producer<select value={effectiveProducerId} onChange={(event) => setEventProducerId(event.target.value)}><option value="">—</option>{producers.map((producer) => <option key={producer.id} value={producer.id}>{producer.label}</option>)}</select></label>
              <label>Topic<select value={effectiveTopicId} onChange={(event) => setEventTopicId(event.target.value)}><option value="">—</option>{topics.map((topic) => <option key={topic.id} value={topic.id}>{topic.label}</option>)}</select></label>
              <label>Key · optional<input value={eventKey} onChange={(event) => setEventKey(event.target.value)} placeholder="null → round-robin" /></label>
              <label className="runner-payload">Payload<textarea value={eventPayload} onChange={(event) => setEventPayload(event.target.value)} spellCheck={false} /></label>
            </div>
            <div className={`run-status ${eventRun?.finalState ?? "idle"}`}>
              {!eventRun ? <div className="run-empty"><Zap size={28} /><strong>Event ещё не отправлен</strong><span>Настройте схему и нажмите «Отправить event».</span></div> : <>
                <div className="run-status-heading"><span><strong>{eventRun.id}</strong><small>{eventRun.partition === null ? "route unavailable" : `P${eventRun.partition} · offset ${eventRun.offset ?? "—"}`}</small></span><div><button onClick={() => setEventRun((current) => current ? { ...current, currentStep: Math.max(0, current.currentStep - 1) } : current)} disabled={eventRun.currentStep === 0}><ArrowLeft size={14} /></button><button onClick={() => setPlaying((value) => !value)} disabled={eventRun.currentStep === eventRun.steps.length - 1}>{playing ? <CirclePause size={15} /> : <CirclePlay size={15} />}</button><button onClick={() => setEventRun((current) => current ? { ...current, currentStep: Math.min(current.steps.length - 1, current.currentStep + 1) } : current)} disabled={eventRun.currentStep === eventRun.steps.length - 1}><ArrowRight size={14} /></button></div></div>
                <div className="run-current-step">{currentStep?.state === "error" ? <CircleX size={20} /> : currentStep?.state === "warning" ? <AlertTriangle size={20} /> : <Zap size={20} />}<span><strong>{currentStep?.title}</strong><small>{currentStep?.detail}</small></span></div>
                <div className="run-progress">{eventRun.steps.map((step, index) => <button key={`${step.title}-${index}`} className={`${index < eventRun.currentStep ? "done" : index === eventRun.currentStep ? "active" : ""} ${step.state}`} onClick={() => { setPlaying(false); setEventRun((current) => current ? { ...current, currentStep: index } : current); }} title={step.title}><i>{index < eventRun.currentStep ? <Check size={10} /> : index + 1}</i><span>{step.title}</span></button>)}</div>
              </>}
            </div>
          </div>
          <footer><Info size={16} /><p><strong>Модель 0.7:</strong> Topic остаётся логическим объектом, а Leader/Follower replicas физически размещаются внутри Broker. Event пишет только Leader выбранной partition.</p></footer>
        </section>
      </section>
    </main>
  );
}
