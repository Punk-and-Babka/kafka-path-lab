"use client";

import {
  ArrowLeft, ArrowRight, BookOpen, Eye, HelpCircle, Info, MousePointer2,
  Sparkles, X,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";

export type HelpMode = "sandbox" | "guided" | "constructor";

type HelpStep = {
  selector: string;
  title: string;
  description: string;
};

const modeCopy: Record<HelpMode, {
  eyebrow: string;
  title: string;
  intro: string;
  bullets: string[];
  steps: HelpStep[];
}> = {
  sandbox: {
    eyebrow: "СВОБОДНАЯ ПЕСОЧНИЦА",
    title: "Как экспериментировать с системой",
    intro: "Вы задаёте собственные входные данные, конфигурацию и сбои. Симулятор показывает не только итог, но и отдельные проверяемые факты.",
    bullets: [
      "Создайте event или выберите локальный файл и отправьте его в Topic.",
      "Меняйте acks, retries, idempotence, Broker и ISR в лабораториях.",
      "Откройте Consumer Group Lab, чтобы изучить rebalance, offsets и lag.",
      "Нажимайте partitions, Brokers и timeline для подробного инспектора.",
    ],
    steps: [
      { selector: ".learning-mode-switch", title: "Режимы работы", description: "Песочница, готовые учебные сценарии и конструктор используют одну Kafka-модель, но дают разную степень свободы." },
      { selector: ".sandbox-composer", title: "Входные данные", description: "Здесь задаются Topic, Event name, key, headers и payload. Кнопка отправки запускает общую state machine." },
      { selector: ".sandbox-lab-index", title: "Лаборатории Producer и Broker", description: "Быстрые ссылки открывают настройки доставки, сетевые ошибки, отказоустойчивость и Consumer Group." },
      { selector: "#consumer-group-lab", title: "Consumer Group Lab", description: "Группа встроена после Topic. Управляйте Consumer, assignor, poll(), heartbeat, commit и наблюдайте lag." },
      { selector: ".simulator-card", title: "End-to-end цепочка", description: "Event движется от Producer до БД и __consumer_offsets. Схему можно прокручивать, раскрывать и проходить вручную." },
      { selector: ".inspector", title: "Инспектор результата", description: "Здесь объясняется активный шаг, конфигурация доставки и состояние выбранного event." },
    ],
  },
  guided: {
    eyebrow: "УЧЕБНЫЕ СЦЕНАРИИ",
    title: "Как разобрать готовую ситуацию",
    intro: "Сценарий фиксирует параметры и ожидаемую проблему. Вы запускаете event и проходите цепочку по шагам без риска случайно изменить preset.",
    bullets: [
      "Выберите карточку сценария и прочитайте его учебную цель.",
      "Отправьте event и управляйте шагами кнопками или стрелками клавиатуры.",
      "Сравнивайте append, ACK, replication, fetch, запись в БД и offset commit.",
      "При необходимости перенесите preset в песочницу и меняйте параметры вручную.",
    ],
    steps: [
      { selector: ".learning-mode-switch", title: "Выбор формата", description: "Вернуться в песочницу или открыть конструктор можно в любой момент." },
      { selector: ".scenario-switcher", title: "Готовые сценарии", description: "Карточки сгруппированы по основам, доставке, отказоустойчивости и retries." },
      { selector: ".scenario-brief", title: "Учебная цель", description: "Перед запуском здесь описано, на какой факт нужно обратить внимание." },
      { selector: ".guided-composer", title: "Запуск preset", description: "Параметры сценария уже применены. Отправьте event и наблюдайте заранее подготовленную ситуацию." },
      { selector: ".simulator-card", title: "Пошаговая цепочка", description: "Timeline позволяет остановиться на любом этапе и увидеть объяснение." },
      { selector: ".inspector", title: "Доказательства", description: "Инспектор разделяет данные event, результат доставки и lifecycle." },
    ],
  },
  constructor: {
    eyebrow: "КОНСТРУКТОР",
    title: "Как собрать собственную топологию",
    intro: "Добавляйте узлы, размещайте их на холсте, создавайте связи и запускайте event по собранной Kafka-системе.",
    bullets: [
      "Перетащите элементы из палитры или добавьте их кнопкой +.",
      "Выберите узел и настройте параметры и связи в правом инспекторе.",
      "Назначьте Leader/Follower replicas внутри Brokers и проверьте схему.",
      "Сохраните топологию в .txt или загрузите её снова позднее.",
    ],
    steps: [
      { selector: ".learning-mode-switch", title: "Режимы работы", description: "Конструктор не заменяет песочницу: он нужен для проверки собственной структуры системы." },
      { selector: ".constructor-toolbar", title: "Preset и файлы", description: "Загрузите готовый стенд, очистите холст или сохраните топологию в переносимый .txt." },
      { selector: ".node-palette", title: "Палитра элементов", description: "Producer, Topic, Broker, Consumer и Database добавляются на холст отсюда." },
      { selector: ".topology-stage", title: "Холст", description: "Узлы можно перемещать. Линии показывают связи, а роли replicas видны внутри Broker." },
      { selector: ".node-inspector", title: "Инспектор узла", description: "Настройки выбранного элемента, connections и replicas редактируются здесь." },
      { selector: ".constructor-event-lab", title: "Event Runner", description: "После проверки схемы отправьте event и посмотрите вычисленный маршрут и итог." },
    ],
  },
};

export default function ContextualHelp({
  mode,
  open,
  onClose,
}: {
  mode: HelpMode;
  open: boolean;
  onClose: () => void;
}) {
  const [surface, setSurface] = useState<"menu" | "discovery" | "tour">("menu");
  const [stepIndex, setStepIndex] = useState(0);
  const [rect, setRect] = useState<DOMRect | null>(null);
  const copy = modeCopy[mode];
  const currentStep = copy.steps[Math.min(stepIndex, copy.steps.length - 1)];

  useEffect(() => {
    if (!open || surface !== "discovery") return;
    document.body.classList.add("help-discovery-active");
    return () => document.body.classList.remove("help-discovery-active");
  }, [open, surface]);

  useEffect(() => {
    if (!open || surface !== "tour") return;
    const element = document.querySelector(currentStep.selector) as HTMLElement | null;
    if (!element) {
      const missingTimer = window.setTimeout(() => setRect(null), 0);
      return () => window.clearTimeout(missingTimer);
    }
    element.scrollIntoView({ behavior: "smooth", block: "center", inline: "nearest" });
    const update = () => setRect(element.getBoundingClientRect());
    const timer = window.setTimeout(update, 360);
    window.addEventListener("resize", update);
    window.addEventListener("scroll", update, true);
    return () => {
      window.clearTimeout(timer);
      window.removeEventListener("resize", update);
      window.removeEventListener("scroll", update, true);
    };
  }, [currentStep.selector, open, surface]);

  useEffect(() => {
    if (!open) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        if (surface === "menu") onClose();
        else setSurface("menu");
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose, open, surface]);

  const tooltipStyle = useMemo(() => {
    if (!rect) return undefined;
    const width = Math.min(390, window.innerWidth - 32);
    const left = Math.max(16, Math.min(window.innerWidth - width - 16, rect.left));
    const preferredTop = rect.bottom + 16;
    const top = preferredTop + 230 < window.innerHeight
      ? preferredTop
      : Math.max(16, rect.top - 230);
    return { left, top, width };
  }, [rect]);

  if (!open) return null;

  if (surface === "discovery") {
    return <div className="help-discovery-banner" role="status">
      <span><Eye size={18} /></span>
      <div><strong>Кликабельные элементы подсвечены</strong><small>Наведите на крупную область с подписью или просто попробуйте выделенные кнопки и поля.</small></div>
      <button onClick={() => setSurface("menu")}><X size={15} /> Закрыть подсветку</button>
    </div>;
  }

  if (surface === "tour") {
    return <div className="help-tour-layer" aria-live="polite">
      {rect && <div className="help-tour-highlight" style={{ left: rect.left - 6, top: rect.top - 6, width: rect.width + 12, height: rect.height + 12 }} />}
      <section className="help-tour-tooltip" style={tooltipStyle}>
        <div className="help-tour-title"><span><MousePointer2 size={17} /></span><div><small>{copy.eyebrow}</small><h3>{currentStep.title}</h3></div><button onClick={() => setSurface("menu")} aria-label="Закрыть объяснение"><X size={17} /></button></div>
        <p>{currentStep.description}</p>
        <footer>
          <button disabled={stepIndex === 0} onClick={() => setStepIndex((index) => Math.max(0, index - 1))}><ArrowLeft size={15} /> Назад</button>
          {stepIndex < copy.steps.length - 1
            ? <button className="primary" onClick={() => setStepIndex((index) => Math.min(copy.steps.length - 1, index + 1))}>Далее <ArrowRight size={15} /></button>
            : <button className="primary" onClick={() => setSurface("menu")}>Закрыть <X size={15} /></button>}
        </footer>
      </section>
    </div>;
  }

  return <div className="help-modal-backdrop" onMouseDown={onClose}>
    <section className="help-modal" role="dialog" aria-modal="true" aria-labelledby="help-title" onMouseDown={(event) => event.stopPropagation()}>
      <header><div><span><HelpCircle size={16} /> {copy.eyebrow}</span><h2 id="help-title">{copy.title}</h2></div><button onClick={onClose} aria-label="Закрыть подсказку"><X size={21} /></button></header>
      <p className="help-intro">{copy.intro}</p>
      <div className="help-mode-notes">
        {copy.bullets.map((bullet) => <p key={bullet}><CheckMark /><span>{bullet}</span></p>)}
      </div>
      <div className="help-actions">
        <button onClick={() => setSurface("discovery")}><span><Eye size={20} /></span><div><strong>Показать, где можно нажать</strong><small>Подсветить реальные кнопки, поля и интерактивные области текущего экрана.</small></div><ArrowRight size={17} /></button>
        <button className="primary" onClick={() => { setStepIndex(0); setSurface("tour"); }}><span><Sparkles size={20} /></span><div><strong>Объяснить текущий режим</strong><small>Последовательно показать главные части интерфейса без прогресса и обязательного прохождения.</small></div><ArrowRight size={17} /></button>
      </div>
      <footer><Info size={15} /><p>Справка запускается только по вашему запросу. Сайт не сохраняет просмотр и не показывает напоминания.</p></footer>
    </section>
  </div>;
}

function CheckMark() {
  return <span className="help-check"><BookOpen size={14} /></span>;
}
