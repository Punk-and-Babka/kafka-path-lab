export const TOPIC_NAME = "orders.events";
export const PARTITION_COUNT = 3;
export const BROKER_COUNT = 3;
export const BASE_OFFSETS = [18, 42, 7] as const;

export type ScenarioId =
  | "happy-path"
  | "same-key"
  | "acks-zero"
  | "acks-one"
  | "acks-all"
  | "leader-failover"
  | "isr-loss"
  | "replica-recovery"
  | "ack-lost-duplicate"
  | "ack-lost-idempotent";
export type AcksMode = "0" | "1" | "all";
export type NetworkFaultMode = "none" | "request-lost" | "ack-lost";
export type SimulationNode =
  | "producer"
  | "partition"
  | "leader"
  | "follower"
  | "committed"
  | "ack"
  | "timeout"
  | "retry"
  | "consumer"
  | "deserializer"
  | "processor"
  | "sink"
  | "offset";
export type LifecycleKey =
  | "producerSend"
  | "partitioning"
  | "leaderAppend"
  | "replication"
  | "committed"
  | "networkTimeout"
  | "retrySend"
  | "retryResolution"
  | "producerAck"
  | "consumerFetch"
  | "deserialization"
  | "businessProcessing"
  | "sinkWrite"
  | "offsetCommit";
export type LifecycleStatus = "waiting" | "active" | "done" | "skipped" | "failed";
export type StepDisposition = "success" | "skipped" | "failed";

export type DeliveryConfig = {
  acks: AcksMode;
  replicationFactor: number;
  minInSyncReplicas: number;
  availableBrokers: number;
  retries: number;
  idempotence: boolean;
};

export type ClusterRuntime = {
  onlineBrokers: number[];
  laggingReplicas: string[];
  leaders: number[];
};

export type PartitionRuntime = {
  partition: number;
  assignedReplicas: number[];
  onlineReplicaBrokers: number[];
  isrBrokers: number[];
  laggingReplicaBrokers: number[];
  preferredLeaderBroker: number;
  leaderBroker: number;
  leaderOnline: boolean;
  leaderElected: boolean;
};

export type DeliveryResult = {
  configValid: boolean;
  configErrors: string[];
  leaderBroker: number;
  replicaBrokers: number[];
  onlineReplicaBrokers: number[];
  currentIsr: number;
  leaderOnline: boolean;
  leaderAppended: boolean;
  followerCopies: number;
  totalCopies: number;
  recordCommitted: boolean;
  producerResult: "ack" | "unconfirmed" | "error" | "config-error";
  errorCode: string | null;
  attempts: number;
  survivesLeaderFailure: boolean;
  preferredLeaderBroker: number;
  leaderElected: boolean;
  laggingReplicaBrokers: number[];
  faultApplied: NetworkFaultMode;
  recordsWritten: number;
  duplicateWritten: boolean;
  duplicateSuppressed: boolean;
  ambiguousResult: boolean;
};

export type SimulationStep = {
  id: LifecycleKey;
  short: string;
  title: string;
  description: string;
  technical: string;
  node: SimulationNode;
};

export type Scenario = {
  id: ScenarioId;
  group: "foundation" | "delivery" | "resilience" | "retry";
  title: string;
  badge: string;
  description: string;
  lesson: string;
  defaultKey: string;
  defaultValue: string;
  sendLabel: string;
  config: DeliveryConfig;
  faultMode: NetworkFaultMode;
  focusPartition?: number;
  cluster: ClusterRuntime;
};

export type EventRecord = {
  id: string;
  topic: string;
  name: string;
  kind: "event" | "file";
  headers: string;
  fileName: string | null;
  mimeType: string | null;
  fileSize: number | null;
  key: string;
  value: string;
  partition: number;
  offset: number;
  stage: number;
  scenarioId: ScenarioId | "sandbox";
  sequence: number;
  createdAt: string;
  delivery: DeliveryConfig;
  result: DeliveryResult;
  stepOrder: LifecycleKey[];
  faultMode: NetworkFaultMode;
  producerId: string;
  producerEpoch: number;
  producerSequence: number;
  retryOffset: number | null;
};

export const STEPS: SimulationStep[] = [
  {
    id: "producerSend",
    short: "Send",
    title: "Producer подготовил event",
    description: "Приложение сформировало key, payload и настройки доставки. Пока event находится на стороне Producer.",
    technical: "Вызов send() ещё не доказывает, что Broker получил или сохранил запись.",
    node: "producer",
  },
  {
    id: "partitioning",
    short: "Route",
    title: "Выбрана partition",
    description: "Partitioner вычислил маршрут. Topic — логическое имя потока, а partition — конкретный упорядоченный журнал.",
    technical: "Для key используется детерминированный hash. Одинаковый key в этой конфигурации попадает в одну partition.",
    node: "partition",
  },
  {
    id: "leaderAppend",
    short: "Append",
    title: "Leader обрабатывает Produce request",
    description: "Producer отправил запрос на Broker с leader-replica выбранной partition.",
    technical: "При успешном append event получает offset. При недоступном Leader или недостаточном ISR запись может быть отклонена.",
    node: "leader",
  },
  {
    id: "replication",
    short: "Replicate",
    title: "Follower синхронизирует запись",
    description: "Доступные follower-replicas забирают event у Leader и копируют его в свои журналы.",
    technical: "Replication factor задаёт число копий, но фактически создать их можно только на доступных Brokers.",
    node: "follower",
  },
  {
    id: "committed",
    short: "Commit",
    title: "Kafka обновляет committed state",
    description: "Event записан на всех репликах текущего ISR и становится доступен для обычного чтения.",
    technical: "Committed record и надёжность при потере Leader — не одно и то же: при ISR=1 committed record имеет только одну копию.",
    node: "committed",
  },
  {
    id: "networkTimeout",
    short: "Timeout",
    title: "Producer не получил ожидаемый ответ",
    description: "Сетевой сбой может произойти до записи или уже после успешного append на Broker.",
    technical: "По одному timeout Producer не знает, находится ли record в Kafka. Это неопределённый результат.",
    node: "timeout",
  },
  {
    id: "retrySend",
    short: "Retry",
    title: "Producer повторяет Produce request",
    description: "После timeout Producer отправляет ещё одну попытку в пределах настройки retries.",
    technical: "Повторная отправка может создать второй record, если Broker не умеет распознать повтор.",
    node: "retry",
  },
  {
    id: "retryResolution",
    short: "Dedup",
    title: "Broker обрабатывает повторную попытку",
    description: "С idempotence Broker проверяет producerId и sequence; без неё retry выглядит как новая запись.",
    technical: "Idempotence защищает от сетевых повторов Producer, но не от двух отдельных бизнес-команд приложения.",
    node: "leader",
  },
  {
    id: "producerAck",
    short: "ACK",
    title: "Producer получает результат",
    description: "Поведение зависит от acks: подтверждение после Leader, после ISR или отсутствие ожидания ответа.",
    technical: "ACK описывает знания Producer, а не просто физическое наличие record в Kafka.",
    node: "ack",
  },
  {
    id: "consumerFetch",
    short: "Fetch",
    title: "Consumer получил event",
    description: "Consumer группы sandbox-cg прочитал committed record и может начать бизнес-обработку.",
    technical: "Получение event ещё не означает успешную обработку или сохранение результата во внешней системе.",
    node: "consumer",
  },
  {
    id: "deserialization",
    short: "Decode",
    title: "Payload десериализован",
    description: "Consumer преобразовал bytes из record в объект или метаданные файла и проверил ожидаемый формат.",
    technical: "На этом шаге проявляются ошибки формата, несовместимая schema и неожиданные обязательные поля.",
    node: "deserializer",
  },
  {
    id: "businessProcessing",
    short: "Process",
    title: "Запущена бизнес-обработка",
    description: "Handler применяет правила приложения к полученным данным и подготавливает результат для внешней системы.",
    technical: "Kafka считает record прочитанным, но бизнес-операция ещё может завершиться ошибкой или быть выполнена повторно.",
    node: "processor",
  },
  {
    id: "sinkWrite",
    short: "DB write",
    title: "Результат сохранён во внешней БД",
    description: "Consumer записал результат обработки в service_db — появился наблюдаемый side effect.",
    technical: "Для QA важно связать eventId, partition и offset с записью в БД и проверить идемпотентность этой операции.",
    node: "sink",
  },
  {
    id: "offsetCommit",
    short: "Offset",
    title: "Consumer зафиксировал offset",
    description: "После успешной обработки Consumer сохранил позицию, с которой группа продолжит чтение.",
    technical: "Consumer position и committed offset могут различаться. Здесь offset фиксируется после обработки.",
    node: "offset",
  },
];

export const STEP_BY_ID = Object.fromEntries(
  STEPS.map((step) => [step.id, step]),
) as Record<LifecycleKey, SimulationStep>;

const reliableConfig: DeliveryConfig = {
  acks: "all",
  replicationFactor: 2,
  minInSyncReplicas: 2,
  availableBrokers: 3,
  retries: 3,
  idempotence: true,
};

const healthyCluster: ClusterRuntime = {
  onlineBrokers: [1, 2, 3],
  laggingReplicas: [],
  leaders: [1, 2, 3],
};

export const SCENARIOS: Scenario[] = [
  {
    id: "happy-path",
    group: "foundation",
    title: "Обычная доставка",
    badge: "Сценарий 01 · Основы",
    description: "Полный путь event при RF=2, min ISR=2 и acks=all.",
    lesson: "Проследите одиннадцать независимых фактов: от send() и append до записи в БД и commit consumer offset.",
    defaultKey: "user-123",
    defaultValue: '{"orderId":8421,"status":"created"}',
    sendLabel: "Отправить event",
    config: reliableConfig,
    faultMode: "none",
    cluster: healthyCluster,
  },
  {
    id: "same-key",
    group: "foundation",
    title: "Одинаковый key",
    badge: "Сценарий 02 · Основы",
    description: "Три изменения заказа попадают в одну partition.",
    lesson: "Отправьте created → paid → shipped и сравните partition и возрастающие offsets.",
    defaultKey: "order-8421",
    defaultValue: '{"orderId":8421,"status":"created"}',
    sendLabel: "Отправить следующий",
    config: reliableConfig,
    faultMode: "none",
    cluster: healthyCluster,
  },
  {
    id: "acks-zero",
    group: "delivery",
    title: "acks=0",
    badge: "Сценарий 03 · Доставка",
    description: "Producer отправляет запрос и не ждёт подтверждения.",
    lesson: "Сравните отсутствие ACK с реальным append: Producer не знает, сохранила ли Kafka event.",
    defaultKey: "delivery-demo",
    defaultValue: '{"orderId":9100,"status":"created"}',
    sendLabel: "Отправить без ACK",
    config: {
      acks: "0",
      replicationFactor: 2,
      minInSyncReplicas: 2,
      availableBrokers: 1,
      retries: 0,
      idempotence: false,
    },
    faultMode: "none",
    cluster: {
      onlineBrokers: [1],
      laggingReplicas: ["0:2", "1:2", "1:3", "2:3"],
      leaders: [1, 2, 3],
    },
  },
  {
    id: "acks-one",
    group: "delivery",
    title: "acks=1",
    badge: "Сценарий 04 · Доставка",
    description: "Leader подтверждает append до синхронизации Follower.",
    lesson: "Producer видит успех, хотя единственная копия record пока находится только на Leader.",
    defaultKey: "delivery-demo",
    defaultValue: '{"orderId":9101,"status":"paid"}',
    sendLabel: "Отправить в Leader",
    config: {
      acks: "1",
      replicationFactor: 2,
      minInSyncReplicas: 2,
      availableBrokers: 1,
      retries: 3,
      idempotence: false,
    },
    faultMode: "none",
    cluster: {
      onlineBrokers: [1],
      laggingReplicas: ["0:2", "1:2", "1:3", "2:3"],
      leaders: [1, 2, 3],
    },
  },
  {
    id: "acks-all",
    group: "delivery",
    title: "acks=all",
    badge: "Сценарий 05 · Доставка",
    description: "ACK приходит после выполнения условий текущего ISR.",
    lesson: "Проверьте связку acks=all + min.insync.replicas: одного параметра acks недостаточно.",
    defaultKey: "delivery-demo",
    defaultValue: '{"orderId":9102,"status":"shipped"}',
    sendLabel: "Отправить надёжно",
    config: {
      ...reliableConfig,
      availableBrokers: 2,
    },
    faultMode: "none",
    cluster: {
      onlineBrokers: [1, 2],
      laggingReplicas: ["1:3", "2:3"],
      leaders: [1, 2, 3],
    },
  },
  {
    id: "leader-failover",
    group: "resilience",
    title: "Падение Leader",
    badge: "Сценарий 06 · Отказоустойчивость",
    description: "Остановите Leader и наблюдайте election на Follower.",
    lesson: "Сначала отправьте record, затем остановите Leader P0: B2 станет новым Leader, но ISR сократится и следующая запись будет заблокирована.",
    defaultKey: "cluster-demo",
    defaultValue: '{"orderId":9200,"status":"created"}',
    sendLabel: "Отправить в текущий Leader",
    config: reliableConfig,
    faultMode: "none",
    focusPartition: 0,
    cluster: healthyCluster,
  },
  {
    id: "isr-loss",
    group: "resilience",
    title: "Недостаточно ISR",
    badge: "Сценарий 07 · Отказоустойчивость",
    description: "Follower отстаёт, а acks=all требует две ISR.",
    lesson: "P0 имеет Leader B1, но B2 находится в CATCHING UP. При min ISR=2 Kafka отклонит Produce request.",
    defaultKey: "cluster-demo",
    defaultValue: '{"orderId":9201,"status":"paid"}',
    sendLabel: "Проверить запись",
    config: reliableConfig,
    faultMode: "none",
    focusPartition: 0,
    cluster: {
      onlineBrokers: [1, 2, 3],
      laggingReplicas: ["0:2"],
      leaders: [1, 2, 3],
    },
  },
  {
    id: "replica-recovery",
    group: "resilience",
    title: "Возврат replica",
    badge: "Сценарий 08 · Отказоустойчивость",
    description: "Запустите Broker и верните replica в ISR.",
    lesson: "B2 сначала OFFLINE. Запустите его, дождитесь CATCHING UP, синхронизируйте P0 и убедитесь, что запись снова разрешена.",
    defaultKey: "cluster-demo",
    defaultValue: '{"orderId":9202,"status":"shipped"}',
    sendLabel: "Отправить после recovery",
    config: {
      ...reliableConfig,
      availableBrokers: 2,
    },
    faultMode: "none",
    focusPartition: 0,
    cluster: {
      onlineBrokers: [1, 3],
      laggingReplicas: ["0:2", "1:2"],
      leaders: [1, 2, 3],
    },
  },
  {
    id: "ack-lost-duplicate",
    group: "retry",
    title: "ACK потерян: дубль",
    badge: "Сценарий 09 · Retry",
    description: "Broker записал event, но retry создаёт второй record.",
    lesson: "Проследите две попытки с одним eventId: без idempotence в partition появятся два offsets.",
    defaultKey: "retry-demo",
    defaultValue: '{"orderId":9300,"status":"paid"}',
    sendLabel: "Потерять ACK и повторить",
    config: {
      ...reliableConfig,
      retries: 1,
      idempotence: false,
    },
    faultMode: "ack-lost",
    cluster: healthyCluster,
  },
  {
    id: "ack-lost-idempotent",
    group: "retry",
    title: "ACK потерян: dedup",
    badge: "Сценарий 10 · Retry",
    description: "Тот же retry подавляется idempotent Producer.",
    lesson: "Сравните число запросов и records: попыток две, но offset создаётся только один.",
    defaultKey: "retry-demo",
    defaultValue: '{"orderId":9300,"status":"paid"}',
    sendLabel: "Проверить idempotence",
    config: {
      ...reliableConfig,
      retries: 1,
      idempotence: true,
    },
    faultMode: "ack-lost",
    cluster: healthyCluster,
  },
];

export const SAME_KEY_VALUES = [
  '{"orderId":8421,"status":"created"}',
  '{"orderId":8421,"status":"paid"}',
  '{"orderId":8421,"status":"shipped"}',
] as const;

export function hashKey(value: string) {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = (hash * 31 + value.charCodeAt(index)) >>> 0;
  }
  return hash;
}

export function resolvePartition(key: string, keylessCounter: number) {
  const normalized = key.trim();
  return normalized
    ? hashKey(normalized) % PARTITION_COUNT
    : keylessCounter % PARTITION_COUNT;
}

export function leaderForPartition(partition: number) {
  return partition + 1;
}

export function replicaBrokersForPartition(partition: number, replicationFactor: number) {
  return Array.from(
    { length: Math.min(BROKER_COUNT, Math.max(1, replicationFactor)) },
    (_, index) => ((partition + index) % BROKER_COUNT) + 1,
  );
}

export function followerForPartition(partition: number) {
  return replicaBrokersForPartition(partition, 2)[1];
}

export function replicaKey(partition: number, broker: number) {
  return `${partition}:${broker}`;
}

export function clusterRuntimeForScenario(scenario: Scenario): ClusterRuntime {
  return {
    onlineBrokers: [...scenario.cluster.onlineBrokers],
    laggingReplicas: [...scenario.cluster.laggingReplicas],
    leaders: [...scenario.cluster.leaders],
  };
}

export function partitionRuntime(
  partition: number,
  replicationFactor: number,
  runtime: ClusterRuntime,
): PartitionRuntime {
  const assignedReplicas = replicaBrokersForPartition(partition, replicationFactor);
  const onlineReplicaBrokers = assignedReplicas.filter((broker) =>
    runtime.onlineBrokers.includes(broker));
  const laggingReplicaBrokers = onlineReplicaBrokers.filter((broker) =>
    runtime.laggingReplicas.includes(replicaKey(partition, broker)));
  const isrBrokers = onlineReplicaBrokers.filter((broker) =>
    !laggingReplicaBrokers.includes(broker));
  const preferredLeaderBroker = assignedReplicas[0];
  const requestedLeader = runtime.leaders[partition] ?? preferredLeaderBroker;
  const leaderBroker = isrBrokers.includes(requestedLeader)
    ? requestedLeader
    : isrBrokers[0] ?? requestedLeader;
  return {
    partition,
    assignedReplicas,
    onlineReplicaBrokers,
    isrBrokers,
    laggingReplicaBrokers,
    preferredLeaderBroker,
    leaderBroker,
    leaderOnline: isrBrokers.length > 0,
    leaderElected: isrBrokers.length > 0 && leaderBroker !== preferredLeaderBroker,
  };
}

export function stepOrderForConfig(
  config: DeliveryConfig,
  faultMode: NetworkFaultMode = "none",
): LifecycleKey[] {
  const afterAppend = config.acks === "1"
    ? ["producerAck", "replication", "committed"] as LifecycleKey[]
    : ["replication", "committed", "producerAck"] as LifecycleKey[];
  const finish = [
    "consumerFetch",
    "deserialization",
    "businessProcessing",
    "sinkWrite",
    "offsetCommit",
  ] as LifecycleKey[];

  if (faultMode === "request-lost" && config.acks !== "0") {
    if (config.retries === 0) {
      return [
        "producerSend",
        "partitioning",
        "networkTimeout",
        "producerAck",
      ];
    }
    return [
      "producerSend",
      "partitioning",
      "networkTimeout",
      "retrySend",
      "leaderAppend",
      ...afterAppend,
      ...finish,
    ];
  }

  if (faultMode === "ack-lost" && config.acks !== "0") {
    const beforeTimeout = config.acks === "1"
      ? ["producerSend", "partitioning", "leaderAppend"] as LifecycleKey[]
      : ["producerSend", "partitioning", "leaderAppend", "replication", "committed"] as LifecycleKey[];
    const afterTimeout = [
      "networkTimeout",
      ...(config.retries > 0
        ? ["retrySend", "retryResolution"] as LifecycleKey[]
        : []),
      "producerAck",
    ] as LifecycleKey[];
    const afterAck = config.acks === "1"
      ? ["replication", "committed", ...finish] as LifecycleKey[]
      : finish;
    return [...beforeTimeout, ...afterTimeout, ...afterAck];
  }

  return [
    "producerSend",
    "partitioning",
    "leaderAppend",
    ...afterAppend,
    ...finish,
  ];
}

export function evaluateDelivery(
  config: DeliveryConfig,
  partition: number,
  runtime?: ClusterRuntime,
  faultMode: NetworkFaultMode = "none",
): DeliveryResult {
  const configErrors: string[] = [];
  if (config.idempotence && config.acks !== "all") {
    configErrors.push("enable.idempotence=true требует acks=all");
  }
  if (config.idempotence && config.retries < 1) {
    configErrors.push("enable.idempotence=true требует retries > 0");
  }

  const configValid = configErrors.length === 0;
  const fallbackRuntime: ClusterRuntime = {
    onlineBrokers: Array.from(
      { length: config.availableBrokers },
      (_, index) => index + 1,
    ),
    laggingReplicas: [],
    leaders: [1, 2, 3],
  };
  const partitionState = partitionRuntime(
    partition,
    config.replicationFactor,
    runtime ?? fallbackRuntime,
  );
  const replicaBrokers = partitionState.assignedReplicas;
  const onlineReplicaBrokers = partitionState.isrBrokers;
  const leaderBroker = partitionState.leaderBroker;
  const leaderOnline = partitionState.leaderOnline;
  const currentIsr = leaderOnline ? partitionState.isrBrokers.length : 0;
  const insufficientIsr =
    config.acks === "all" && currentIsr < config.minInSyncReplicas;
  const baseLeaderAppended =
    configValid && leaderOnline && !insufficientIsr;
  const effectiveFault = config.acks === "0" ? "none" : faultMode;
  const requestLostWithoutRetry =
    effectiveFault === "request-lost" && config.retries === 0 && baseLeaderAppended;
  const leaderAppended = baseLeaderAppended && !requestLostWithoutRetry;
  const ackLostAfterWrite = effectiveFault === "ack-lost" && baseLeaderAppended;
  const duplicateWritten =
    ackLostAfterWrite && config.retries > 0 && !config.idempotence;
  const duplicateSuppressed =
    ackLostAfterWrite && config.retries > 0 && config.idempotence;
  const recordsWritten = leaderAppended ? 1 + (duplicateWritten ? 1 : 0) : 0;
  const totalCopies = leaderAppended ? Math.max(1, onlineReplicaBrokers.length) : 0;
  const followerCopies = Math.max(0, totalCopies - 1);
  const recordCommitted = leaderAppended && totalCopies === currentIsr;

  let producerResult: DeliveryResult["producerResult"];
  let errorCode: string | null = null;
  if (!configValid) {
    producerResult = "config-error";
    errorCode = "ConfigException";
  } else if (config.acks === "0") {
    producerResult = "unconfirmed";
  } else if (requestLostWithoutRetry) {
    producerResult = "error";
    errorCode = "TimeoutException";
  } else if (!leaderOnline) {
    producerResult = "error";
    errorCode = "LeaderNotAvailable";
  } else if (insufficientIsr) {
    producerResult = "error";
    errorCode = "NotEnoughReplicas";
  } else if (ackLostAfterWrite && config.retries === 0) {
    producerResult = "error";
    errorCode = "TimeoutException";
  } else {
    producerResult = "ack";
  }

  return {
    configValid,
    configErrors,
    leaderBroker,
    replicaBrokers,
    onlineReplicaBrokers,
    currentIsr,
    leaderOnline,
    leaderAppended,
    followerCopies,
    totalCopies,
    recordCommitted,
    producerResult,
    errorCode,
    attempts: !configValid ? 0
      : effectiveFault !== "none" && baseLeaderAppended
        ? Math.min(2, config.retries + 1)
        : producerResult === "error" ? config.retries + 1 : 1,
    survivesLeaderFailure: totalCopies >= 2,
    preferredLeaderBroker: partitionState.preferredLeaderBroker,
    leaderElected: partitionState.leaderElected,
    laggingReplicaBrokers: partitionState.laggingReplicaBrokers,
    faultApplied: baseLeaderAppended ? effectiveFault : "none",
    recordsWritten,
    duplicateWritten,
    duplicateSuppressed,
    ambiguousResult: ackLostAfterWrite && config.retries === 0,
  };
}

export function stepDisposition(
  event: EventRecord,
  stepId: LifecycleKey,
): StepDisposition {
  if (!event.result.configValid) {
    return stepId === "producerSend" ? "failed" : "skipped";
  }

  switch (stepId) {
    case "producerSend":
    case "partitioning":
      return "success";
    case "leaderAppend":
      return event.result.leaderAppended ? "success" : "failed";
    case "replication":
      if (!event.result.leaderAppended || event.delivery.replicationFactor === 1) {
        return "skipped";
      }
      return event.result.followerCopies > 0 ? "success" : "skipped";
    case "committed":
      return event.result.recordCommitted ? "success" : "skipped";
    case "networkTimeout":
      return event.result.faultApplied !== "none" ? "success" : "skipped";
    case "retrySend":
      return event.result.attempts > 1 ? "success" : "skipped";
    case "retryResolution":
      return event.result.duplicateWritten || event.result.duplicateSuppressed
        ? "success"
        : "skipped";
    case "producerAck":
      if (event.delivery.acks === "0") return "skipped";
      return event.result.producerResult === "ack" ? "success" : "failed";
    case "consumerFetch":
    case "deserialization":
    case "businessProcessing":
    case "sinkWrite":
    case "offsetCommit":
      return event.result.recordCommitted ? "success" : "skipped";
  }
}

export function lifecycleForEvent(event: EventRecord | null) {
  return STEPS.reduce<Record<LifecycleKey, LifecycleStatus>>((result, step) => {
    if (!event) {
      result[step.id] = "waiting";
      return result;
    }
    const position = event.stepOrder.indexOf(step.id);
    if (position > event.stage) {
      result[step.id] = "waiting";
      return result;
    }
    const disposition = stepDisposition(event, step.id);
    if (disposition === "failed") result[step.id] = "failed";
    else if (disposition === "skipped") result[step.id] = "skipped";
    else result[step.id] = position === event.stage ? "active" : "done";
    return result;
  }, {} as Record<LifecycleKey, LifecycleStatus>);
}

function reached(event: EventRecord, stepId: LifecycleKey) {
  return event.stage >= event.stepOrder.indexOf(stepId);
}

export function isLogVisible(event: EventRecord) {
  return event.result.leaderAppended && reached(event, "leaderAppend");
}

export function isFollowerReplicated(event: EventRecord) {
  return event.result.followerCopies > 0 && reached(event, "replication");
}

export function isRecordCommitted(event: EventRecord) {
  return event.result.recordCommitted && reached(event, "committed");
}

export function hasProducerResult(event: EventRecord) {
  return reached(event, "producerAck");
}

export function isRetryResolved(event: EventRecord) {
  return event.stepOrder.includes("retryResolution")
    && reached(event, "retryResolution");
}

export function isConsumed(event: EventRecord) {
  return event.result.recordCommitted && reached(event, "consumerFetch");
}

export function isDeserialized(event: EventRecord) {
  return event.result.recordCommitted
    && event.stepOrder.includes("deserialization")
    && reached(event, "deserialization");
}

export function isProcessed(event: EventRecord) {
  return event.result.recordCommitted
    && event.stepOrder.includes("businessProcessing")
    && reached(event, "businessProcessing");
}

export function isSinkWritten(event: EventRecord) {
  return event.result.recordCommitted
    && event.stepOrder.includes("sinkWrite")
    && reached(event, "sinkWrite");
}

export function isOffsetCommitted(event: EventRecord) {
  return event.result.recordCommitted && reached(event, "offsetCommit");
}
