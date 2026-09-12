// Чистая модель конструктора топологии: типы, пресеты, проверка схемы и правила
// доставки. Логика вынесена из React-компонента, чтобы её можно было проверять
// тестами без рендера.

export type NodeKind = "producer" | "topic" | "broker" | "consumer" | "database";
export type AcksMode = "0" | "1" | "all";
export type ReplicaRole = "leader" | "follower";

export type Replica = {
  topicId: string;
  partition: number;
  role: ReplicaRole;
  inIsr: boolean;
};

export type NodeConfig = {
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

export type TopologyNode = {
  id: string;
  kind: NodeKind;
  label: string;
  x: number;
  y: number;
  config: NodeConfig;
};

export type TopologyEdge = {
  id: string;
  from: string;
  to: string;
};

export type ValidationIssue = {
  level: "error" | "warning";
  message: string;
  nodeId?: string;
};

export type EventStep = {
  nodeId: string;
  title: string;
  detail: string;
  state: "success" | "warning" | "error";
};

export type EventRun = {
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

export type SavedTopology = {
  format: "kafka-path-topology";
  version: 1;
  savedAt: string;
  nodes: TopologyNode[];
  edges: TopologyEdge[];
};

export const CANVAS_WIDTH = 1320;
export const CANVAS_HEIGHT = 670;
export const NODE_WIDTH = 176;
export const NODE_HEIGHT = 104;

export const nodeMeta: Record<NodeKind, {
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

export const nodeDefaults = (kind: NodeKind, index: number): NodeConfig => {
  if (kind === "producer") return { acks: "all", retries: 3, idempotence: true };
  if (kind === "topic") return { topicName: `events.topic.${index}`, partitions: 3, minIsr: 2 };
  if (kind === "broker") return { brokerId: index, online: true, replicas: [] };
  if (kind === "consumer") return { groupId: `workers-${index}`, autoCommit: false };
  return { tableName: `service_events_${index}` };
};

export const presetNodes = (): TopologyNode[] => [
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

export const presetEdges = (): TopologyEdge[] => [
  { id: "edge-producer-topic", from: "producer-1", to: "topic-1" },
  { id: "edge-topic-b1", from: "topic-1", to: "broker-1" },
  { id: "edge-topic-b2", from: "topic-1", to: "broker-2" },
  { id: "edge-topic-b3", from: "topic-1", to: "broker-3" },
  { id: "edge-topic-c1", from: "topic-1", to: "consumer-1" },
  { id: "edge-topic-c2", from: "topic-1", to: "consumer-2" },
  { id: "edge-c1-db", from: "consumer-1", to: "database-1" },
  { id: "edge-c2-db", from: "consumer-2", to: "database-1" },
];

export function nextId(kind: NodeKind, nodes: TopologyNode[]) {
  const used = new Set(nodes.map((node) => node.id));
  let index = 1;
  while (used.has(`${kind}-${index}`)) index += 1;
  return `${kind}-${index}`;
}

export function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}

export function nodeCenter(node: TopologyNode) {
  return { x: node.x + NODE_WIDTH / 2, y: node.y + NODE_HEIGHT / 2 };
}

export function validateTopology(nodes: TopologyNode[], edges: TopologyEdge[]): ValidationIssue[] {
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
    const groups = consumerGroupsForTopic(topic.id, consumers, edges);
    if (!groups.length) issues.push({ level: "warning", nodeId: topic.id, message: `${topic.label}: Consumer не подключён.` });
    groups.forEach((group) => {
      if (group.members.length > count) {
        issues.push({
          level: "warning",
          nodeId: group.members[count].id,
          message: `${topic.label}: в группе ${group.groupId} ${group.members.length} Consumer на ${count} partitions — лишние останутся без assignment.`,
        });
      }
    });
  });

  consumers.forEach((consumer) => {
    const hasInput = edges.some((edge) => edge.to === consumer.id
      && topics.some((topic) => topic.id === edge.from));
    if (!hasInput) issues.push({ level: "warning", nodeId: consumer.id, message: `${consumer.label}: нет входящей связи от Topic.` });
  });

  return issues;
}

export function isSavedTopology(value: unknown): value is SavedTopology {
  if (!value || typeof value !== "object") return false;
  const candidate = value as Partial<SavedTopology>;
  return candidate.format === "kafka-path-topology"
    && candidate.version === 1
    && Array.isArray(candidate.nodes)
    && Array.isArray(candidate.edges);
}

export type ConsumerGroup = {
  key: string;
  groupId: string;
  standalone: boolean;
  members: TopologyNode[];
};

/**
 * Consumer группируются по `group.id`: каждая группа получает собственную копию
 * record, а внутри группы partition достаётся ровно одному участнику. Consumer
 * без заполненного `group.id` считается одиночным — в Kafka такой клиент
 * использует assign() вместо subscribe() и ни с кем не делит partitions.
 */
export function consumerGroupsForTopic(
  topicId: string,
  consumers: TopologyNode[],
  edges: TopologyEdge[],
): ConsumerGroup[] {
  const groups: ConsumerGroup[] = [];
  consumers
    .filter((consumer) => edges.some((edge) => edge.from === topicId && edge.to === consumer.id))
    .forEach((consumer) => {
      const groupId = (consumer.config.groupId ?? "").trim();
      const key = groupId ? `group:${groupId}` : `standalone:${consumer.id}`;
      const existing = groups.find((group) => group.key === key);
      if (existing) {
        existing.members.push(consumer);
        return;
      }
      groups.push({
        key,
        groupId: groupId || consumer.label,
        standalone: !groupId,
        members: [consumer],
      });
    });
  return groups;
}

export function consumerForPartition(group: ConsumerGroup, partition: number) {
  return group.members[partition % group.members.length];
}
