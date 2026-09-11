import { COMBAT_EFFECTS_KEY, COMBAT_TYPES, EFFECT_LIMITS, combatFeedback, effectPriority, effectTarget, magnitude, nextEffectIndex } from "./combat-effects-model.js";

let sessionMode = null;
const COLORS = { damage: "#ff6655", death: "#d72f4c", raze: "#ff923d", disable: "#b3a0ff", enable: "#94f8e4", heal: "#8af3ad", repair: "#e9d887", "shield-hit": "#a9e8ff", "shield-break": "#dbf8ff", "shield-gain": "#71c8ff", combat: "#ff8897", trade: "#f0d671" };
const center = rect => ({ x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 });
const rectOf = element => {
  if (!element?.isConnected) return null;
  const r = element.getBoundingClientRect();
  if (!r.width || !r.height || r.bottom < 0 || r.top > innerHeight || r.right < 0 || r.left > innerWidth) return null;
  if (getComputedStyle(element).visibility === "hidden") return null;
  const clip = element.closest(".attackTargetsScroller,.ownCards,.handRow");
  if (clip) {
    const c = clip.getBoundingClientRect();
    if (r.right <= c.left || r.left >= c.right || r.bottom <= c.top || r.top >= c.bottom) return null;
  }
  return { left: r.left, top: r.top, width: r.width, height: r.height };
};

export function combatEffectsReduced() {
  try {
    return (sessionMode || localStorage.getItem(COMBAT_EFFECTS_KEY)) === "reduced" || localStorage.getItem("warRealms.reduceMotion") === "1" || globalThis.matchMedia?.("(prefers-reduced-motion: reduce)").matches === true;
  } catch { return sessionMode === "reduced" || globalThis.matchMedia?.("(prefers-reduced-motion: reduce)").matches === true; }
}
export function setCombatEffectsMode(mode) {
  sessionMode = mode === "reduced" ? "reduced" : "full";
  try { localStorage.setItem(COMBAT_EFFECTS_KEY, mode === "reduced" ? "reduced" : "full"); } catch { /* Session settings still work without storage. */ }
  document.documentElement.classList.toggle("wrReducedCombat", mode === "reduced");
  globalThis.dispatchEvent(new CustomEvent("warrealms:effects-settings"));
}

export function createCombatEffects({ legacy = async () => {}, makeCard = () => null } = {}) {
  if (typeof document === "undefined") return null;
  const link = document.createElement("link");
  link.rel = "stylesheet";
  link.href = new URL("./combat-effects.css", import.meta.url).href;
  if (!document.querySelector('link[data-wr-combat-styles]')) { link.dataset.wrCombatStyles = ""; document.head.append(link); }
  const layer = document.createElement("div");
  layer.id = "wrCombatLayer";
  layer.setAttribute("aria-hidden", "true");
  layer.inert = true;
  document.body.append(layer);
  let queue = [], nodes = new Map(), animations = new Map(), lanes = [], seen = new Set();
  let frame = 0, nextAt = 0, busy = false, epoch = 0, initialized = false, playerId = "", prepared = [];
  const themes = new Map();
  const now = () => performance.now();
  const reduced = () => combatEffectsReduced();
  const mobile = () => innerWidth <= 700;
  const cap = () => mobile() ? EFFECT_LIMITS.mobileNodes : EFFECT_LIMITS.nodes;
  const findVisible = selector => [...document.querySelectorAll(selector)].find(node => !node.closest("#wrCombatLayer") && rectOf(node));
  const exactCard = id => id ? findVisible(`.gameCard[data-instance-id="${CSS.escape(String(id))}"]`) : null;
  function ownerAnchor(id, shield = false) {
    if (id && id === playerId) return (shield && rectOf(document.getElementById("commanderShield")) ? document.getElementById("commanderShield") : document.getElementById("commanderAuthority"));
    const safe = CSS.escape(String(id || ""));
    return findVisible(`.enemySnapshot[data-target-id="${safe}"]`) || findVisible(`[data-player-id="${safe}"]`) || findVisible(`[data-target-owner-group="${safe}"]`);
  }
  function targetFor(event) {
    if (event.type === "stat-gain" && event.targetId === playerId) return document.getElementById(event.stat === "combat" ? "commanderCombat" : "commanderTrade");
    return exactCard(event.instanceId) || ownerAnchor(event.targetId, event.type === "shield-gain");
  }
  function sourceFor(event) { return exactCard(event.sourceInstanceId) || ownerAnchor(event.actorId || event.sourceId); }
  function schedule() { if (!frame && !document.hidden) frame = requestAnimationFrame(tick); }
  function remove(node) { nodes.delete(node); node.remove(); }
  function add(node, duration, essential = false) {
    if (nodes.size >= cap()) {
      const victim = [...nodes].find(([, item]) => !item.essential);
      if (victim) remove(victim[0]);
      else return null;
    }
    layer.append(node);
    nodes.set(node, { until: now() + duration, essential });
    schedule();
    return node;
  }
  function nodeAt(className, point, color, duration, essential = false) {
    const node = document.createElement("div");
    node.className = className;
    node.style.left = `${point.x}px`; node.style.top = `${point.y}px`;
    node.style.setProperty("--wr-color", color);
    return add(node, duration, essential);
  }
  function animate(element, frames, duration) {
    if (!element?.isConnected || typeof element.animate !== "function") return;
    const old = animations.get(element);
    old?.animation.cancel();
    const animation = element.animate(frames, { duration, easing: "ease-out" });
    animations.set(element, { animation, until: now() + duration });
    schedule();
  }
  function ghostFor(event, target, rect) {
    if ([...nodes.keys()].filter(n => n.classList.contains("wrRazeGhost")).length >= EFFECT_LIMITS.ghosts) return null;
    const ghost = document.createElement("div");
    ghost.className = "gameCard wrRazeGhost";
    ghost.style.left = `${rect.left}px`; ghost.style.top = `${rect.top}px`;
    ghost.style.width = `${rect.width}px`; ghost.style.height = `${rect.height}px`;
    if (target) ghost.style.setProperty("--faction", getComputedStyle(target).getPropertyValue("--faction"));
    // Clone art only, never live actions, instance IDs, status trays, or listeners.
    const art = target?.querySelector(".battleCardVisual")?.cloneNode(true) || makeCard(event);
    if (art) {
      art.removeAttribute("id");
      art.removeAttribute("data-instance-id");
      art.querySelectorAll("[id],[data-instance-id],button,a,input").forEach(n => n.remove());
      ghost.append(art);
    }
    return add(ghost, 4500, true);
  }
  function capture(events, owner) {
    playerId = String(owner || "");
    if (document.hidden) { reset(); events.forEach(e => seen.add(e.id)); initialized = true; return; }
    if (!initialized) {
      events.forEach(e => seen.add(e.id)); initialized = true; return;
    }
    prepared = [];
    const destroys = events.filter(e => e.type === "card-destroy" && !seen.has(e.id));
    for (const original of events) {
      if (!original?.id || seen.has(original.id)) continue;
      seen.add(original.id);
      while (seen.size > EFFECT_LIMITS.seen) seen.delete(seen.values().next().value);
      const event = { ...original, strong: original.strong || (original.type === "card-destroy" && destroys.length > 1) };
      const target = targetFor(event), source = sourceFor(event);
      const rect = rectOf(target), sourceRect = rectOf(source);
      const ghost = event.type === "card-destroy" && rect ? ghostFor(event, exactCard(event.instanceId), rect) : null;
      prepared.push({ event, rect, sourceRect, ghost, queuedAt: now() });
    }
  }
  function enqueue(item) {
    if (queue.length >= 8 && ["health-loss", "base-damage", "health-gain", "base-repair", "shield-gain", "stat-gain"].includes(item.event.type)) {
      const previous = queue.findLast(other => effectTarget(other.event) === effectTarget(item.event));
      if (previous?.event.type === item.event.type && previous.event.stat === item.event.stat) {
        previous.event = { ...previous.event,
          amount: (Number(previous.event.amount) || 0) + (Number(item.event.amount) || 0),
          absorbed: (Number(previous.event.absorbed) || 0) + (Number(item.event.absorbed) || 0),
          shieldBroken: previous.event.shieldBroken || item.event.shieldBroken,
          lethal: previous.event.lethal || item.event.lethal,
          hits: (previous.event.hits || 1) + (item.event.hits || 1)
        };
        return;
      }
    }
    if (queue.length >= EFFECT_LIMITS.queue) {
      const victim = queue.findIndex(other => effectPriority(other.event) < effectPriority(item.event));
      if (victim < 0) { if (item.ghost) remove(item.ghost); return; }
      const [dropped] = queue.splice(victim, 1);
      if (dropped.ghost) remove(dropped.ghost);
    }
    queue.push(item);
  }
  function flush() {
    for (const item of prepared) enqueue(item);
    prepared = [];
    document.documentElement.classList.toggle("wrReducedCombat", combatEffectsReduced());
    layer.classList.toggle("wrFxReduced", reduced());
    schedule();
  }
  function floating(point, label, kind, amount) {
    lanes = lanes.filter(l => l.until > now());
    let x = Math.max(64, Math.min(innerWidth - 64, point.x));
    let y = Math.max(32, Math.min(innerHeight - 28, point.y));
    // Reserve screen-space lanes across targets as well as within one target.
    for (let i = 0; i < 12 && lanes.some(l => Math.abs(l.x - x) < 110 && Math.abs(l.y - y) < 28); i++) {
      y -= 30;
      if (y < 24) { y = Math.max(32, point.y); x = Math.max(64, Math.min(innerWidth - 64, x + (x > innerWidth / 2 ? -120 : 120))); }
    }
    lanes.push({ x, y, until: now() + 860 });
    if (lanes.length > 40) lanes.shift();
    const node = nodeAt(`wrCombatNumber ${kind} ${magnitude(amount)}`, { x, y }, COLORS[kind], 950, true);
    if (node) node.textContent = label;
  }
  function particles(point, kind, color, heavy) {
    if (reduced()) return;
    const count = Math.min(mobile() ? 6 : 14, heavy ? 14 : 7);
    for (let i = 0; i < count; i++) {
      const angle = i * 2.39996;
      const distance = 20 + (i % 5) * 12;
      const inward = kind === "repair";
      const upward = ["raze", "heal", "enable"].includes(kind);
      const dx = Math.cos(angle) * distance, dy = upward ? -35 - i * 7 : Math.sin(angle) * distance;
      const node = nodeAt(`wrCombatParticle ${kind}`, point, color, 1050);
      if (!node) break;
      node.style.setProperty("--dx", `${dx}px`); node.style.setProperty("--dy", `${dy}px`);
      node.style.setProperty("--start-x", inward ? `${dx}px` : "0px");
      node.style.setProperty("--start-y", inward ? `${dy}px` : "0px");
      if (inward) { node.style.setProperty("--dx", "0px"); node.style.setProperty("--dy", "0px"); }
      node.style.animationDelay = `${i * 12}ms`;
    }
  }
  function impact(item, feedback, index) {
    const { event } = item;
    const target = targetFor(event);
    const liveCard = exactCard(event.instanceId);
    const rect = (event.instanceId ? rectOf(liveCard) || item.rect : rectOf(target) || item.rect) || { left: innerWidth / 2 - 45, top: innerHeight * .45, width: 90, height: 110 };
    const point = center(rect), kind = feedback.kind;
    const heavy = magnitude(feedback.amount) === "heavy";
    if (event.type === "stat-gain" && !reduced()) {
      const source = exactCard(event.sourceInstanceId) || (event.sourceCardId ? findVisible(`.gameCard[data-card-id="${CSS.escape(event.sourceCardId)}"]`) : null);
      const sourceRect = rectOf(source);
      if (sourceRect) {
        const origin = center(sourceRect);
        for (let particle = 0; particle < 5; particle++) {
          const node = nodeAt("wrResourceTransfer", origin, COLORS[kind] || "#e4c65c", 650);
          if (!node) break;
          node.style.setProperty("--dx", `${point.x - origin.x}px`);
          node.style.setProperty("--dy", `${point.y - origin.y}px`);
          node.style.animationDelay = `${particle * 35}ms`;
        }
      }
    }
    const color = themes.get(event.faction)?.[kind] || COLORS[kind];
    // Numbers occupy the top edge, leaving rules and controls readable.
    floating({ x: point.x, y: rect.top - 8 - index * 28 }, feedback.label, kind, feedback.amount);
    nodeAt(`wrCombatBloom ${kind} ${heavy ? "heavy" : ""}`, point, color, 720);
    particles(point, kind, color, heavy);
    if (kind === "raze") {
      const ghost = (item.ghost?.isConnected ? item.ghost : null) || ghostFor(event, exactCard(event.instanceId), rect);
      if (ghost) {
        ghost.classList.add("burning");
        nodes.get(ghost).until = now() + 1150;
        const cracks = document.createElement("i"); cracks.className = "wrBurnCracks"; ghost.append(cracks);
      }
    } else if (kind === "repair") {
      const seal = nodeAt("wrRepairSeal", point, color, 680);
      if (seal) { seal.style.width = `${rect.width}px`; seal.style.height = `${rect.height}px`; }
    } else if (["disable", "enable"].includes(kind)) {
      const sweep = nodeAt(`wrDisableSweep ${kind}`, point, color, 680);
      if (sweep) { sweep.style.width = `${rect.width}px`; sweep.style.height = `${rect.height}px`; }
    } else if (kind.startsWith("shield")) {
      const barrier = nodeAt(`wrCombatBarrier ${kind}`, point, color, 680);
      if (barrier) { barrier.style.width = `${Math.min(220, rect.width + 22)}px`; barrier.style.height = `${Math.min(240, rect.height + 16)}px`; }
    }
    const stationary = reduced();
    const dx = heavy ? 6 : magnitude(feedback.amount) === "medium" ? 3 : 1;
    const damage = ["damage", "death"].includes(kind);
    const frames = damage && !stationary
      ? [{ transform: "translateX(0)", filter: "brightness(1)" }, { transform: `translateX(-${dx}px) scale(.98)`, filter: "brightness(1.5)" }, { transform: `translateX(${dx}px)`, filter: "brightness(1.1)" }, { transform: "none", filter: "none" }]
      : [{ opacity: 1 }, { opacity: .65 }, { opacity: 1 }];
    animate(target, frames, stationary ? 380 : 440);
    const portrait = event.targetId === playerId ? document.getElementById("commanderHudPortrait") : findVisible(`[data-commander-player="${CSS.escape(String(event.targetId || ""))}"]`);
    if (!event.instanceId && portrait) animate(portrait, frames, 440);
    if (!stationary && ((damage && heavy) || (kind === "raze" && event.strong))) {
      animate(document.querySelector(".battleShell"), [{ transform: "translate(0)" }, { transform: "translate(-2px,1px)" }, { transform: "translate(2px,-1px)" }, { transform: "translate(0)" }], 190);
    }
    if ((damage && !event.instanceId) || (kind === "raze" && event.strong)) {
      const edge = document.createElement("div"); edge.className = "wrCombatEdge"; edge.style.setProperty("--wr-color", color); add(edge, 600);
    }
  }
  function start(item, stagger = 0) {
    const { event } = item;
    const feedback = combatFeedback(event);
    if (!COMBAT_TYPES.has(event.type)) {
      busy = true;
      const token = epoch;
      Promise.resolve().then(() => legacy(event)).catch(console.warn).finally(() => {
        if (token !== epoch) return;
        busy = false; schedule();
      });
      return;
    }
    const sourceRect = item.sourceRect || rectOf(sourceFor(event));
    const targetRect = (event.instanceId ? rectOf(exactCard(event.instanceId)) || item.rect : rectOf(targetFor(event)) || item.rect);
    const attack = ["health-loss", "base-damage", "card-disable"].includes(event.type) || (event.type === "card-destroy" && event.method !== "combat");
    let travel = 0;
    if (attack && sourceRect && targetRect && !reduced()) {
      const a = center(sourceRect), b = center(targetRect), distance = Math.hypot(b.x - a.x, b.y - a.y);
      if (distance > 24) {
        const projectile = nodeAt("wrCombatProjectile", a, themes.get(event.faction)?.projectile || COLORS[feedback[0]?.kind] || COLORS.damage, 280);
        if (projectile) { projectile.style.setProperty("--dx", `${b.x - a.x}px`); projectile.style.setProperty("--dy", `${b.y - a.y}px`); }
        animate(sourceFor(event), [{ opacity: 1 }, { opacity: .55 }, { opacity: 1 }], 220);
        travel = 220;
      }
    }
    item.impacts = feedback.map((fx, i) => ({ fx, at: now() + stagger + travel + i * 140, index: i }));
    active.push(item);
    const catchingUp = queue.length > 8 || now() - item.queuedAt > 1800;
    nextAt = now() + stagger + (event.type === "card-destroy" ? 1120 + travel : travel + Math.max(180, feedback.length * 140) + (catchingUp ? 0 : 100));
  }
  let active = [];
  function tick(time) {
    frame = 0;
    for (const [node, item] of nodes) if (time >= item.until) remove(node);
    for (const [element, item] of animations) if (time >= item.until || !element.isConnected) { item.animation.cancel(); animations.delete(element); }
    for (const item of active) {
      while (item.impacts.length && item.impacts[0].at <= time) { const part = item.impacts.shift(); impact(item, part.fx, part.index); }
    }
    active = active.filter(item => item.impacts.length);
    if (!busy && queue.length && time >= nextAt) {
      const [item] = queue.splice(nextEffectIndex(queue), 1);
      start(item);
      if (item.event.type === "card-destroy") {
        // Up to three bases burn in one wave, with one major board reaction.
        for (let count = 1; count < 3; count++) {
          const index = nextEffectIndex(queue);
          if (index < 0 || queue[index].event.type !== "card-destroy") break;
          const [next] = queue.splice(index, 1);
          next.event = { ...next.event, strong: false };
          start(next, count * 90);
        }
      }
    }
    if ((!busy && queue.length) || nodes.size || active.length || animations.size) schedule();
  }
  function reset(preserveHistory = false) {
    epoch++; busy = false; nextAt = 0;
    if (preserveHistory !== true) { initialized = false; seen.clear(); }
    queue = []; active = []; prepared = []; lanes = [];
    cancelAnimationFrame(frame); frame = 0;
    for (const { animation } of animations.values()) animation.cancel();
    animations.clear(); nodes.clear(); layer.replaceChildren();
  }
  const onVisibility = () => { if (document.hidden) reset(); };
  const onGeometry = () => { reset(true); };
  const onScroll = event => {
    if (event.target === document || event.target?.closest?.("#battleView")) reset(true);
  };
  const onSettings = () => { layer.classList.toggle("wrFxReduced", reduced()); };
  document.addEventListener("visibilitychange", onVisibility);
  document.addEventListener("wheel", onScroll, { capture: true, passive: true });
  document.addEventListener("touchmove", onScroll, { capture: true, passive: true });
  globalThis.addEventListener("pagehide", reset);
  globalThis.addEventListener("resize", onGeometry);
  globalThis.addEventListener("warrealms:effects-settings", onSettings);
  return {
    capture, flush, reset,
    registerTheme: (faction, theme) => themes.set(faction, { ...theme }),
    debug: () => ({ queued: queue.length, nodes: layer.querySelectorAll("*").length, tracked: nodes.size, seen: seen.size, animations: animations.size, active: active.length, running: !!frame, busy }),
    dispose() {
      reset(); layer.remove();
      document.removeEventListener("visibilitychange", onVisibility);
      document.removeEventListener("wheel", onScroll, true);
      document.removeEventListener("touchmove", onScroll, true);
      globalThis.removeEventListener("pagehide", reset);
      globalThis.removeEventListener("resize", onGeometry);
      globalThis.removeEventListener("warrealms:effects-settings", onSettings);
    }
  };
}
