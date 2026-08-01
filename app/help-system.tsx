"use client";

import {
  ArrowLeft, ArrowRight, BookOpenCheck, CircleHelp, Eye, MousePointerClick,
  Route, X,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";

export type HelpMode = "sandbox" | "guided" | "constructor";

type HelpView = "menu" | "discovery" | "tour";

type TourStep = {
  target: string;
  title: string;
  description: string;
  qaNote?: string;
};

type TargetRect = {
  top: number;
  left: number;
  width: number;
  height: number;
};

const MODE_COPY: Record<HelpMode, {
  label: string;
  description: string;
  actions: string[];
}> = {
  sandbox: {
    label: "Свободная песочница",
    description: "Ручной эксперимент с event, настройками Producer, сетевыми сбоями и состоянием Kafka-кластера.",
    actions: [
      "создать event или выбрать локальный файл",
      "менять acks, retries, idempotence, Broker и ISR",
      "отправить данные и проверить факты доставки в инспекторе",
    ],
  },
  guided: {
    label: "Учебные сценарии",
    description: "Готовые ситуации с заранее настроенными условиями, ошибками и ожидаемым поведением Kafka.",
    actions: [
      "выбрать один из десяти сценариев",
      "отправить preset-event и управлять шагами симуляции",
      "сопоставить результат Producer, Kafka, Consumer и БД",
    ],
  },
  constructor: {
    label: "Конструктор",
    description: "Свободная схема из Producer, Topic, Broker, Consumer и Database с собственными связями и replicas.",
    actions: [
      "добавлять и перетаскивать узлы на холсте",
      "назначать Leader, Follower, ISR и соединения",
      "проверять топологию, запускать event и сохранять схему в .txt",
    ],
  },
};

const TOUR_STEPS: Record<HelpMode, TourStep[]> = {
  sandbox: [
    {
      target: "learning-modes",
      title: "Три независимых режима",
      description: "Переключайтесь между свободным стендом, готовыми сценариями и конструктором топологии. Каждый режим сохраняет свою учебную задачу.",
    },
    {
      target: "sandbox-input",
      title: "Конструктор входных данных",
      description: "Задайте topic, имя event, key, headers и payload либо выберите локальный файл. Предварительный маршрут показывает ожидаемую partition.",
      qaNote: "Проверяйте обязательные поля, пустой key, формат payload и сохранение headers.",
    },
    {
      target: "sandbox-labs",
      title: "Быстрый переход к лабораториям",
      description: "Эти кнопки прокручивают страницу к настройкам Producer, сетевым сбоям/retry и управлению Broker/ISR.",
    },
    {
      target: "producer-settings",
      title: "Producer Settings",
      description: "Здесь меняются acks, replication factor, min.insync.replicas, retries и idempotence. Допустимо собрать ошибочную комбинацию и увидеть ConfigException при запуске.",
    },
    {
      target: "network-retry",
      title: "Network & Retry Lab",
      description: "Смоделируйте потерю request или ACK и сравните повторную запись, duplicate и подавление дубля при idempotence.",
    },
    {
      target: "cluster-resilience",
      title: "Cluster Resilience Lab",
      description: "Останавливайте Broker, меняйте Leader, выводите replica из ISR и наблюдайте, когда запись остаётся доступной или отклоняется.",
    },
    {
      target: "simulation-map",
      title: "Карта прохождения event",
      description: "Кнопка отправки запускает event, а элементы управления позволяют поставить анимацию на паузу, перейти по шагам или повторить маршрут.",
      qaNote: "Не считайте успешный ACK доказательством записи в БД — проверяйте каждый этап отдельно.",
    },
    {
      target: "event-inspector",
      title: "Инспектор результата",
      description: "Справа отображаются объяснение текущего шага, фактическая конфигурация доставки и полный lifecycle выбранного event.",
    },
    {
      target: "glossary-button",
      title: "Словарь Kafka",
      description: "Откройте справочник, если нужно уточнить термин, механику, пример или список QA-проверок.",
    },
  ],
  guided: [
    {
      target: "learning-modes",
      title: "Выбор режима",
      description: "Учебные сценарии используют готовые presets. В песочнице параметры свободны, а в конструкторе вы собираете собственную топологию.",
    },
    {
      target: "scenario-picker",
      title: "Каталог сценариев",
      description: "Выберите ситуацию: обычная доставка, разные ACK, падение Leader, недостаточный ISR, потеря ACK, duplicate или idempotent retry.",
    },
    {
      target: "guided-input",
      title: "Запуск preset-event",
      description: "Поля показывают данные текущего сценария и рассчитанный маршрут. Нажмите отправку, чтобы увидеть фактический результат.",
    },
    {
      target: "producer-settings",
      title: "Почему параметры заблокированы",
      description: "В сценарии preset защищён от случайного изменения, чтобы результат соответствовал условию. Его можно перенести в песочницу и продолжить эксперимент вручную.",
    },
    {
      target: "simulation-map",
      title: "Пошаговая симуляция",
      description: "Следите за event от Producer до offset commit. Управление в заголовке позволяет остановиться на любом техническом этапе.",
    },
    {
      target: "event-inspector",
      title: "Объяснение наблюдаемого результата",
      description: "Инспектор разделяет результат Producer, состояние записи в Kafka, обработку Consumer и side effect в БД.",
      qaNote: "Сравнивайте ожидаемый результат сценария с фактически достигнутым этапом.",
    },
    {
      target: "glossary-button",
      title: "Словарь Kafka",
      description: "В словаре собраны подробные определения, примеры из лаборатории и отдельные подсказки для QA.",
    },
  ],
  constructor: [
    {
      target: "learning-modes",
      title: "Конструктор — отдельный режим",
      description: "Здесь маршрут не задан заранее: его определяют размещённые узлы, связи, replicas и их состояния.",
    },
    {
      target: "constructor-toolbar",
      title: "Preset, пустой холст и .txt",
      description: "Начните с готовой схемы или очистите холст. Конфигурацию можно сохранить в читаемый .txt и позднее загрузить обратно.",
    },
    {
      target: "node-palette",
      title: "Палитра элементов",
      description: "Перетащите Producer, Topic, Broker, Consumer или Database на холст либо добавьте элемент кнопкой «+».",
    },
    {
      target: "topology-canvas",
      title: "Холст топологии",
      description: "Перемещайте узлы за верхнюю панель. Стрелки показывают направленные связи, а Broker отображают назначенные Leader/Follower replicas.",
    },
    {
      target: "node-inspector",
      title: "Настройки выбранного узла",
      description: "Выберите элемент на холсте, затем измените его config, создайте входящие или исходящие связи и назначьте replicas на Broker.",
    },
    {
      target: "event-runner",
      title: "Event Runner",
      description: "После проверки схемы выберите Producer и Topic, задайте key/payload и отправьте event по фактически собранному маршруту.",
      qaNote: "Ошибочная схема не маскируется: runner объяснит отсутствие Leader, ISR, связей или доступного Broker.",
    },
    {
      target: "glossary-button",
      title: "Словарь Kafka",
      description: "Справочник помогает уточнить различия между логической partition, физической replica, Leader, Follower и ISR.",
    },
  ],
};

function rectForTarget(target: string): TargetRect | null {
  const element = document.querySelector<HTMLElement>(`[data-tour="${target}"]`);
  if (!element) return null;
  const rect = element.getBoundingClientRect();
  if (rect.width <= 0 || rect.height <= 0) return null;
  const padding = 8;
  return {
    top: Math.max(8, rect.top - padding),
    left: Math.max(8, rect.left - padding),
    width: Math.min(window.innerWidth - Math.max(8, rect.left - padding) - 8, rect.width + padding * 2),
    height: Math.min(window.innerHeight - Math.max(8, rect.top - padding) - 8, rect.height + padding * 2),
  };
}

export default function HelpSystem({
  open,
  mode,
  onClose,
}: {
  open: boolean;
  mode: HelpMode;
  onClose: () => void;
}) {
  const [view, setView] = useState<HelpView>("menu");
  const [stepIndex, setStepIndex] = useState(0);
  const [targetRect, setTargetRect] = useState<TargetRect | null>(null);
  const steps = TOUR_STEPS[mode];
  const currentStep = steps[stepIndex] ?? steps[0];
  const modeCopy = MODE_COPY[mode];

  const close = useCallback(() => {
    document.body.classList.remove("help-discovery-mode");
    setView("menu");
    setStepIndex(0);
    setTargetRect(null);
    onClose();
  }, [onClose]);

  const startTour = useCallback(() => {
    document.body.classList.remove("help-discovery-mode");
    setStepIndex(0);
    setView("tour");
  }, []);

  const startDiscovery = useCallback(() => {
    setView("discovery");
  }, []);

  const updateTargetRect = useCallback(() => {
    if (!currentStep) return;
    setTargetRect(rectForTarget(currentStep.target));
  }, [currentStep]);

  useEffect(() => {
    if (!open) return;
    document.body.classList.toggle("help-discovery-mode", view === "discovery");
    return () => document.body.classList.remove("help-discovery-mode");
  }, [open, view]);

  useEffect(() => {
    if (!open || view !== "menu") return;
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => { document.body.style.overflow = previous; };
  }, [open, view]);

  useEffect(() => {
    if (!open || view !== "tour" || !currentStep) return;
    const element = document.querySelector<HTMLElement>(`[data-tour="${currentStep.target}"]`);
    const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    element?.scrollIntoView({ behavior: reducedMotion ? "auto" : "smooth", block: "center", inline: "nearest" });
    const timer = window.setTimeout(updateTargetRect, reducedMotion ? 0 : 360);
    const refresh = () => updateTargetRect();
    window.addEventListener("resize", refresh);
    window.addEventListener("scroll", refresh, true);
    return () => {
      window.clearTimeout(timer);
      window.removeEventListener("resize", refresh);
      window.removeEventListener("scroll", refresh, true);
    };
  }, [currentStep, open, updateTargetRect, view]);

  useEffect(() => {
    if (!open) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      event.preventDefault();
      event.stopImmediatePropagation();
      close();
    };
    window.addEventListener("keydown", onKeyDown, true);
    return () => window.removeEventListener("keydown", onKeyDown, true);
  }, [close, open]);

  const actions = useMemo(() => modeCopy.actions.map((action) => (
    <li key={action}><BookOpenCheck size={15} /><span>{action}</span></li>
  )), [modeCopy.actions]);

  if (!open) return null;

  if (view === "discovery") {
    return <aside className="help-discovery-toolbar" aria-live="polite">
      <span className="help-toolbar-icon"><Eye size={20} /></span>
      <div>
        <strong>Кликабельные элементы подсвечены</strong>
        <small>Голубая рамка — отдельное действие. Фиолетовая — целая интерактивная зона; наведите на неё для пояснения.</small>
      </div>
      <button type="button" onClick={startTour}><Route size={16} /> Объяснить режим</button>
      <button type="button" className="help-toolbar-close" onClick={close} aria-label="Закрыть подсветку"><X size={19} /></button>
    </aside>;
  }

  if (view === "tour") {
    const isLast = stepIndex === steps.length - 1;
    return <div className="help-tour-layer" role="dialog" aria-modal="true" aria-labelledby="help-tour-title">
      {targetRect && <div className="help-tour-spotlight" style={targetRect} aria-hidden="true" />}
      <section className="help-tour-card">
        <header>
          <span>{modeCopy.label}</span>
          <button type="button" onClick={close} aria-label="Закрыть объяснение"><X size={20} /></button>
        </header>
        <div className="help-tour-icon"><MousePointerClick size={21} /></div>
        <h2 id="help-tour-title">{currentStep.title}</h2>
        <p>{currentStep.description}</p>
        {currentStep.qaNote && <aside><strong>QA-фокус</strong><span>{currentStep.qaNote}</span></aside>}
        <footer>
          <button type="button" onClick={() => setStepIndex((index) => Math.max(0, index - 1))} disabled={stepIndex === 0}><ArrowLeft size={16} /> Назад</button>
          <button type="button" className="primary" onClick={() => isLast ? close() : setStepIndex((index) => Math.min(steps.length - 1, index + 1))}>
            {isLast ? "Закрыть" : "Далее"}{!isLast && <ArrowRight size={16} />}
          </button>
        </footer>
      </section>
    </div>;
  }

  return <div className="help-dialog-backdrop" onMouseDown={close}>
    <section className="help-dialog" role="dialog" aria-modal="true" aria-labelledby="help-dialog-title" onMouseDown={(event) => event.stopPropagation()}>
      <header>
        <div className="help-dialog-heading"><span><CircleHelp size={21} /></span><div><small>КАК ПОЛЬЗОВАТЬСЯ</small><h2 id="help-dialog-title">Справка по Kafka Path</h2></div></div>
        <button type="button" onClick={close} aria-label="Закрыть справку"><X size={22} /></button>
      </header>
      <div className="help-mode-summary">
        <span>{modeCopy.label}</span>
        <p>{modeCopy.description}</p>
        <ul>{actions}</ul>
      </div>
      <div className="help-choice-grid">
        <button type="button" onClick={startDiscovery}>
          <span className="help-choice-icon discovery"><Eye size={24} /></span>
          <strong>Показать, где можно нажать</strong>
          <small>Подсветить все видимые кнопки, поля и основные интерактивные зоны.</small>
          <ArrowRight size={18} />
        </button>
        <button type="button" onClick={startTour}>
          <span className="help-choice-icon tour"><Route size={24} /></span>
          <strong>Объяснить текущий режим</strong>
          <small>Последовательно показать главные элементы и рассказать, для чего они нужны.</small>
          <ArrowRight size={18} />
        </button>
      </div>
      <footer><CircleHelp size={15} /><span>Справка ничего не запускает автоматически и не сохраняет отметки о просмотре.</span></footer>
    </section>
  </div>;
}
