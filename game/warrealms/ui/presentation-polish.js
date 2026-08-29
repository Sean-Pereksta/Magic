const WARREALMS_EVENT = "warrealms:game-event";
const STYLE_ID = "warrealmsPresentationPolishStyles";
const LAYER_ID = "warrealmsPresentationLayer";
const CHAIN_ID = "warrealmsComboChain";

const chainState = new Map();
const hudValues = new Map();
let lastSourceElement = null;
let lastSourceAt = 0;
let previewCard = null;

const EFFECT_PRESENTATION = Object.freeze({
  CARD_PLAYED: { className: "wrFxPlay", label: "PLAYED", duration: 440 },
  CARD_DRAWN: { className: "wrFxDraw", label: "DRAW", duration: 520 },
  CARD_DISCARDED: { className: "wrFxDiscard", label: "DISCARD", duration: 520 },
  CARD_PURGED: { className: "wrFxPurge", label: "PURGED", duration: 720 },
  CARD_SACRIFICED: { className: "wrFxSacrifice", label: "SACRIFICE", duration: 700 },
  CARD_DESTROYED: { className: "wrFxDestroy", label: "DESTROYED", duration: 680 },
  CARD_RESURRECTED: { className: "wrFxResurrect", label: "RESURRECT", duration: 720 },
  TOKEN_CREATED: { className: "wrFxToken", label: "TOKEN", duration: 520 },
  TOKEN_PLAYED: { className: "wrFxPlay", label: "TOKEN PLAYED", duration: 440 },
  TOKEN_SACRIFICED: { className: "wrFxSacrifice", label: "TOKEN SACRIFICED", duration: 650 },
  BASE_PLAYED: { className: "wrFxBaseLand", label: "BASE DEPLOYED", duration: 620 },
  BASE_DAMAGED: { className: "wrFxDamage", label: "BASE HIT", duration: 520 },
  BASE_REPAIRED: { className: "wrFxRepair", label: "REPAIR", duration: 620 },
  BASE_WOULD_BE_DESTROYED: { className: "wrFxWarning", label: "BASE THREATENED", duration: 600 },
  BASE_DESTROYED: { className: "wrFxDestroy", label: "BASE DESTROYED", duration: 760 },
  BASE_CONSTRUCTION_ADVANCED: { className: "wrFxBuild", label: "CONSTRUCTION", duration: 600 },
  BASE_CONSTRUCTION_COMPLETED: { className: "wrFxBuildComplete", label: "CONSTRUCTION COMPLETE", duration: 900 },
  ATTACHMENT_ATTACHED: { className: "wrFxAttach", label: "ATTACHED", duration: 560 },
  ATTACHMENT_REMOVED: { className: "wrFxDiscard", label: "DETACHED", duration: 500 },
  HEAT_GAINED: { className: "wrFxHeat", label: "HEAT", duration: 620 },
  HEAT_SPENT: { className: "wrFxHeatSpend", label: "HEAT SPENT", duration: 600 },
  HEAT_OVERLOADED: { className: "wrFxOverload", label: "OVERLOAD", duration: 820 },
  CARD_TRANSFORMED: { className: "wrFxTransform", label: "TRANSFORM", duration: 820 },
  SHIELD_GAINED: { className: "wrFxShield", label: "SHIELD", duration: 620 },
  ALLY_TRIGGERED: { className: "wrFxAlly", label: "ALLY", duration: 650 },
  DOUBLE_ALLY_TRIGGERED: { className: "wrFxDoubleAlly", label: "DOUBLE ALLY", duration: 800 },
  AUTHORITY_GAINED: { className: "wrFxHeal", label: "AUTHORITY", duration: 580 },
  AUTHORITY_LOST: { className: "wrFxDamage", label: "AUTHORITY", duration: 580 }
});

function hasDocument() {
  return typeof document !== "undefined" && !!document.documentElement;
}

function isVisible(element) {
  if (!(element instanceof Element)) return false;
  const rect = element.getBoundingClientRect();
  return rect.width > 0 && rect.height > 0 && getComputedStyle(element).visibility !== "hidden";
}

function selectorEscape(value) {
  const string = String(value || "");
  if (globalThis.CSS?.escape) return globalThis.CSS.escape(string);
  return string.replace(/\\/g, "\\\\").replace(/"/g, "\\\"");
}

function injectStyles() {
  if (!hasDocument() || document.getElementById(STYLE_ID)) return;
  const style = document.createElement("style");
  style.id = STYLE_ID;
  style.textContent = `
    #${LAYER_ID}{position:fixed;inset:0;z-index:126;overflow:hidden;pointer-events:none}
    .wrFloatText{--wr-fx:#f4f7fb;position:fixed;left:0;top:0;transform:translate(-50%,-50%);padding:5px 8px;border:1px solid color-mix(in srgb,var(--wr-fx) 72%,#fff);border-radius:999px;background:rgba(6,10,15,.9);box-shadow:0 8px 24px rgba(0,0,0,.42),0 0 18px color-mix(in srgb,var(--wr-fx) 25%,transparent);color:var(--wr-fx);font-size:9px;font-weight:1000;letter-spacing:.08em;white-space:nowrap;text-transform:uppercase;animation:wrFloatUp .86s cubic-bezier(.2,.75,.2,1) forwards}
    .wrFloatText.gain{--wr-fx:#68d491}.wrFloatText.loss{--wr-fx:#ff7384}.wrFloatText.trade{--wr-fx:#e4c94e}.wrFloatText.combat{--wr-fx:#ef7181}.wrFloatText.draw{--wr-fx:#72baf0}.wrFloatText.heat{--wr-fx:#ff9b4b}.wrFloatText.special{--wr-fx:#d394ff}
    .wrFxBeam{--wr-fx:#f4f7fb;position:fixed;left:0;top:0;height:3px;border-radius:999px;transform-origin:0 50%;background:linear-gradient(90deg,transparent,var(--wr-fx),#fff);box-shadow:0 0 12px color-mix(in srgb,var(--wr-fx) 70%,transparent);animation:wrBeam .46s ease-out forwards}
    #${CHAIN_ID}{position:fixed;z-index:127;right:14px;top:calc(72px + var(--safeTop,0px));width:min(230px,42vw);pointer-events:none;display:grid;gap:5px}
    .wrChainPanel{border:1px solid #46576b;border-radius:11px;background:rgba(8,13,20,.92);box-shadow:0 15px 36px rgba(0,0,0,.45);padding:7px 8px;backdrop-filter:blur(8px);animation:wrChainIn .18s ease-out}
    .wrChainHead{display:flex;align-items:center;justify-content:space-between;gap:8px;color:#e7cb57;font-size:7px;font-weight:1000;letter-spacing:.12em;text-transform:uppercase}.wrChainRows{display:grid;gap:3px;margin-top:5px}.wrChainRow{display:flex;align-items:center;gap:6px;color:#ced7e2;font-size:7px;line-height:1.25}.wrChainRow::before{content:"›";color:#718399}.wrChainRow b{margin-left:auto;color:#fff;font-size:7px}
    .gameCard{transition:transform .14s ease,filter .14s ease,box-shadow .14s ease,border-color .14s ease}.gameCard.wrHoveredSource{transform:translateY(-4px);filter:brightness(1.07);box-shadow:0 12px 28px rgba(0,0,0,.36),0 0 0 1px rgba(255,255,255,.2)}
    .commanderHudStat.wrPreviewTarget{filter:brightness(1.14);box-shadow:0 0 0 1px rgba(255,255,255,.35),0 0 18px rgba(255,255,255,.16)}
    .commanderHudStat.trade.wrPreviewTarget{box-shadow:0 0 0 1px #d6bb3d,0 0 20px rgba(214,187,61,.3)}.commanderHudStat.combat.wrPreviewTarget{box-shadow:0 0 0 1px #e36b7b,0 0 20px rgba(227,107,123,.3)}.commanderHudStat.authority.wrPreviewTarget{box-shadow:0 0 0 1px #68d491,0 0 20px rgba(104,212,145,.25)}
    .gameCard.wrFxPlay{animation:wrCardPlay .44s cubic-bezier(.2,.8,.2,1)}.gameCard.wrFxDraw{animation:wrCardDraw .52s ease-out}.gameCard.wrFxDiscard{animation:wrCardDiscard .52s ease-out}.gameCard.wrFxPurge{animation:wrCardPurge .72s ease-in}.gameCard.wrFxSacrifice{animation:wrCardSacrifice .7s ease-in-out}.gameCard.wrFxDestroy{animation:wrCardDestroy .68s ease-in}.gameCard.wrFxResurrect{animation:wrCardResurrect .72s ease-out}.gameCard.wrFxToken{animation:wrToken .52s ease-out}.gameCard.wrFxBaseLand{animation:wrBaseLand .62s cubic-bezier(.18,.8,.22,1)}.gameCard.wrFxDamage{animation:wrDamage .52s ease-out}.gameCard.wrFxRepair{animation:wrRepair .62s ease-out}.gameCard.wrFxWarning{animation:wrWarning .6s ease-in-out}.gameCard.wrFxBuild{animation:wrBuild .6s ease-out}.gameCard.wrFxBuildComplete{animation:wrBuildComplete .9s ease-out}.gameCard.wrFxAttach{animation:wrAttach .56s ease-out}.gameCard.wrFxHeat{animation:wrHeat .62s ease-out}.gameCard.wrFxHeatSpend{animation:wrHeatSpend .6s ease-out}.gameCard.wrFxOverload{animation:wrOverload .82s ease-in-out}.gameCard.wrFxTransform{animation:wrTransform .82s ease-in-out}.gameCard.wrFxShield{animation:wrShield .62s ease-out}.gameCard.wrFxAlly{animation:wrAlly .65s ease-out}.gameCard.wrFxDoubleAlly{animation:wrDoubleAlly .8s ease-out}.gameCard.wrFxHeal{animation:wrHeal .58s ease-out}
    @keyframes wrFloatUp{0%{opacity:0;transform:translate(-50%,-35%) scale(.82)}18%{opacity:1;transform:translate(-50%,-55%) scale(1.08)}100%{opacity:0;transform:translate(-50%,-145%) scale(.96)}}
    @keyframes wrBeam{0%{opacity:0;transform:rotate(var(--wr-angle)) scaleX(0)}18%{opacity:1}75%{opacity:1;transform:rotate(var(--wr-angle)) scaleX(1)}100%{opacity:0;transform:rotate(var(--wr-angle)) scaleX(1)}}
    @keyframes wrChainIn{from{opacity:0;transform:translateX(12px)}to{opacity:1;transform:none}}
    @keyframes wrCardPlay{0%{transform:translateY(8px) scale(.94);filter:brightness(.8)}55%{transform:translateY(-8px) scale(1.06);filter:brightness(1.28)}100%{transform:none;filter:none}}
    @keyframes wrCardDraw{0%{transform:translateY(-14px) rotate(-2deg);opacity:.35}100%{transform:none;opacity:1}}
    @keyframes wrCardDiscard{50%{transform:translateY(7px) rotate(2deg);filter:grayscale(.65) brightness(.7)}}
    @keyframes wrCardPurge{0%{filter:none}55%{filter:brightness(1.35) saturate(.3);box-shadow:0 0 26px #ff765e}100%{filter:brightness(.2) saturate(0);transform:scale(.86)}}
    @keyframes wrCardSacrifice{0%,100%{transform:none}45%{transform:scale(.88);filter:brightness(.65) saturate(1.6);box-shadow:0 0 32px #c2465d}70%{transform:scale(1.05)}}
    @keyframes wrCardDestroy{35%{transform:translateX(-4px) rotate(-1deg);filter:brightness(1.35)}55%{transform:translateX(5px) rotate(1deg);box-shadow:0 0 28px #ff6f7f}100%{filter:brightness(.45) saturate(.25)}}
    @keyframes wrCardResurrect{0%{opacity:.3;transform:translateY(14px) scale(.9);filter:brightness(.4)}55%{filter:brightness(1.5);box-shadow:0 0 30px #9be5c0}100%{opacity:1;transform:none}}
    @keyframes wrToken{0%{opacity:.25;transform:scale(.65)}65%{opacity:1;transform:scale(1.08);box-shadow:0 0 24px #72baf0}100%{transform:none}}
    @keyframes wrBaseLand{0%{transform:translateY(-11px) scale(1.06)}48%{transform:translateY(5px) scale(.98);box-shadow:0 13px 32px rgba(0,0,0,.65),0 0 24px #d6bb3d}100%{transform:none}}
    @keyframes wrDamage{20%,60%{transform:translateX(-4px);filter:brightness(1.45)}40%,80%{transform:translateX(4px)}100%{transform:none;filter:none}}
    @keyframes wrRepair{0%{filter:brightness(.75)}55%{filter:brightness(1.35);box-shadow:0 0 28px #68d491}100%{filter:none}}
    @keyframes wrWarning{0%,100%{box-shadow:none}50%{box-shadow:0 0 0 2px #ff7384,0 0 30px rgba(255,115,132,.55)}}
    @keyframes wrBuild{0%{filter:brightness(.8)}50%{filter:brightness(1.35);box-shadow:inset 0 0 22px #e4c94e,0 0 20px rgba(228,201,78,.35)}100%{filter:none}}
    @keyframes wrBuildComplete{0%{transform:scale(.94);filter:brightness(.7)}38%{transform:scale(1.07);filter:brightness(1.7);box-shadow:0 0 38px #e4c94e}100%{transform:none;filter:none}}
    @keyframes wrAttach{0%{transform:translateX(-10px);opacity:.45}60%{transform:translateX(3px);opacity:1;box-shadow:0 0 22px #72baf0}100%{transform:none}}
    @keyframes wrHeat{50%{filter:brightness(1.32) saturate(1.6);box-shadow:inset 0 0 26px #ff8b3d,0 0 24px rgba(255,107,48,.42)}}
    @keyframes wrHeatSpend{45%{filter:brightness(1.25);box-shadow:0 0 26px #ff9b4b}100%{filter:none}}
    @keyframes wrOverload{20%,60%{transform:translateX(-5px) rotate(-1deg);filter:brightness(1.6)}40%,80%{transform:translateX(5px) rotate(1deg)}55%{box-shadow:0 0 38px #ff683d}100%{transform:none;filter:none}}
    @keyframes wrTransform{0%{transform:rotateY(0) scale(1);filter:none}45%{transform:rotateY(85deg) scale(.94);filter:brightness(2);box-shadow:0 0 34px #d394ff}55%{transform:rotateY(95deg) scale(.94)}100%{transform:rotateY(180deg) scale(1);filter:none}}
    @keyframes wrShield{50%{box-shadow:0 0 0 4px rgba(114,186,240,.55),0 0 30px rgba(114,186,240,.48);filter:brightness(1.2)}}
    @keyframes wrAlly{50%{box-shadow:0 0 0 2px #e4c94e,0 0 28px rgba(228,201,78,.5);filter:brightness(1.25)}}
    @keyframes wrDoubleAlly{25%,70%{box-shadow:0 0 0 2px #72baf0,0 0 34px rgba(114,186,240,.55);filter:brightness(1.4)}50%{box-shadow:0 0 0 2px #e4c94e,0 0 38px rgba(228,201,78,.6)}}
    @keyframes wrHeal{50%{filter:brightness(1.3);box-shadow:0 0 28px #68d491}}
    @media(max-width:620px){#${CHAIN_ID}{right:7px;top:calc(61px + var(--safeTop,0px));width:min(205px,58vw)}.wrChainPanel{padding:6px}.wrChainRow{font-size:6px}.wrFloatText{font-size:8px}}
    @media(prefers-reduced-motion:reduce){.wrFloatText,.wrFxBeam,.gameCard[class*="wrFx"],.wrChainPanel{animation-duration:.01ms!important;animation-iteration-count:1!important}.gameCard{transition:none!important}}
  `;
  document.head.appendChild(style);
}

function ensureLayer() {
  if (!hasDocument()) return null;
  let layer = document.getElementById(LAYER_ID);
  if (!layer) {
    layer = document.createElement("div");
    layer.id = LAYER_ID;
    layer.setAttribute("aria-hidden", "true");
    document.body.appendChild(layer);
  }
  return layer;
}

function ensureChainRoot() {
  if (!hasDocument()) return null;
  let root = document.getElementById(CHAIN_ID);
  if (!root) {
    root = document.createElement("div");
    root.id = CHAIN_ID;
    root.setAttribute("aria-hidden", "true");
    document.body.appendChild(root);
  }
  return root;
}

function centerOf(element) {
  if (!(element instanceof Element)) return null;
  const rect = element.getBoundingClientRect();
  if (!rect.width && !rect.height) return null;
  return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
}

function sourceColor(element) {
  if (!(element instanceof Element)) return "#f4f7fb";
  const style = getComputedStyle(element);
  const faction = style.getPropertyValue("--faction").trim();
  return faction || "#f4f7fb";
}

function showFloat(anchor, text, tone = "special", color = "") {
  const layer = ensureLayer();
  if (!layer || !(anchor instanceof Element) || !isVisible(anchor) || !text) return;
  const point = centerOf(anchor);
  if (!point) return;
  const node = document.createElement("div");
  node.className = `wrFloatText ${tone}`;
  if (color) node.style.setProperty("--wr-fx", color);
  node.textContent = text;
  node.style.left = `${point.x}px`;
  node.style.top = `${point.y}px`;
  layer.appendChild(node);
  node.addEventListener("animationend", () => node.remove(), { once: true });
  setTimeout(() => node.remove(), 1100);
}

function showBeam(from, to, color = "#f4f7fb") {
  const layer = ensureLayer();
  if (!layer || !(from instanceof Element) || !(to instanceof Element) || !isVisible(from) || !isVisible(to)) return;
  const a = centerOf(from);
  const b = centerOf(to);
  if (!a || !b) return;
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const distance = Math.hypot(dx, dy);
  if (distance < 12) return;
  const beam = document.createElement("div");
  beam.className = "wrFxBeam";
  beam.style.left = `${a.x}px`;
  beam.style.top = `${a.y}px`;
  beam.style.width = `${distance}px`;
  beam.style.setProperty("--wr-angle", `${Math.atan2(dy, dx)}rad`);
  beam.style.setProperty("--wr-fx", color);
  layer.appendChild(beam);
  beam.addEventListener("animationend", () => beam.remove(), { once: true });
  setTimeout(() => beam.remove(), 650);
}

function findCardForEvent(event = {}) {
  const instanceId = String(event.instanceId || event.sourceInstanceId || "").trim();
  const cardId = String(event.cardId || event.sourceCardId || "").trim();
  const selectors = [];
  if (instanceId) selectors.push(`.gameCard[data-instance-id="${selectorEscape(instanceId)}"]`);
  if (cardId) selectors.push(`.gameCard[data-card-id="${selectorEscape(cardId)}"]`);
  for (const selector of selectors) {
    const candidates = [...document.querySelectorAll(selector)];
    const visible = candidates.find(isVisible);
    if (visible) return visible;
  }
  return null;
}

function animateClass(element, className, duration = 600) {
  if (!(element instanceof Element) || !className) return;
  element.classList.remove(className);
  void element.offsetWidth;
  element.classList.add(className);
  setTimeout(() => element.classList.remove(className), duration + 80);
}

export function describePresentationEvent(event = {}) {
  const type = String(event.type || "");
  const config = EFFECT_PRESENTATION[type];
  if (!config) return null;
  const amount = Math.abs(Number(event.amount) || 0);
  let label = config.label;
  if (amount > 0) {
    if (["BASE_DAMAGED", "AUTHORITY_LOST", "HEAT_SPENT"].includes(type)) label += ` −${amount}`;
    else if (["BASE_REPAIRED", "HEAT_GAINED", "SHIELD_GAINED", "AUTHORITY_GAINED", "TOKEN_CREATED", "BASE_CONSTRUCTION_ADVANCED"].includes(type)) label += ` +${amount}`;
  }
  return { ...config, label };
}

function readableEventName(event = {}) {
  const described = describePresentationEvent(event);
  if (described) return described.label;
  return String(event.type || "EVENT").replaceAll("_", " ");
}

function updateChain(event = {}) {
  const chainId = String(event.chainId || "").trim();
  if (!chainId) return;
  const root = ensureChainRoot();
  if (!root) return;
  const current = chainState.get(chainId) || { events: [], timer: 0 };
  current.events.push({ type: event.type, label: readableEventName(event), amount: Number(event.amount) || 0 });
  current.events = current.events.slice(-6);
  clearTimeout(current.timer);
  current.timer = setTimeout(() => {
    chainState.delete(chainId);
    renderChains();
  }, 2200);
  chainState.set(chainId, current);
  renderChains();
}

function renderChains() {
  const root = ensureChainRoot();
  if (!root) return;
  const active = [...chainState.entries()].filter(([, chain]) => chain.events.length > 1).slice(-2);
  root.innerHTML = active.map(([id, chain]) => `
    <section class="wrChainPanel" data-chain-id="${selectorEscape(id)}">
      <div class="wrChainHead"><span>Effect chain</span><b>×${chain.events.length}</b></div>
      <div class="wrChainRows">${chain.events.slice(-5).map(item => `<div class="wrChainRow"><span>${item.label}</span></div>`).join("")}</div>
    </section>`).join("");
}

function handleGameEvent(customEvent) {
  const event = customEvent?.detail;
  if (!event || typeof event !== "object") return;
  const presentation = describePresentationEvent(event);
  const card = findCardForEvent(event);
  if (event.type === "CARD_PLAYED" && card) {
    lastSourceElement = card;
    lastSourceAt = performance.now();
  }
  if (presentation && card) {
    animateClass(card, presentation.className, presentation.duration);
    if (!["CARD_PLAYED", "AUTHORITY_GAINED", "AUTHORITY_LOST"].includes(event.type)) {
      const tone = event.type.includes("HEAT") ? "heat" : event.type.includes("DAMAGE") || event.type.includes("DESTROY") ? "loss" : "special";
      showFloat(card, presentation.label, tone, sourceColor(card));
    }
  }
  updateChain(event);
}

function clearPreview() {
  if (previewCard) previewCard.classList.remove("wrHoveredSource");
  previewCard = null;
  document.querySelectorAll(".wrPreviewTarget").forEach(node => node.classList.remove("wrPreviewTarget"));
}

function previewCardEffects(card) {
  clearPreview();
  if (!(card instanceof Element) || !document.body.classList.contains("battleMode")) return;
  previewCard = card;
  card.classList.add("wrHoveredSource");
  const labels = [...card.querySelectorAll(".abilityGlyph")]
    .map(node => `${node.getAttribute("title") || ""} ${node.getAttribute("aria-label") || ""}`)
    .join(" ")
    .toLowerCase();
  const targets = [];
  if (labels.includes("trade")) targets.push("#commanderTrade");
  if (labels.includes("combat")) targets.push("#commanderCombat");
  if (labels.includes("authority") || labels.includes("heal")) targets.push("#commanderAuthority");
  if (labels.includes("control")) targets.push("#commanderControl");
  if (labels.includes("purge") || labels.includes("scrap own")) targets.push("#commanderPurge");
  targets.forEach(selector => document.querySelector(selector)?.classList.add("wrPreviewTarget"));
}

function installHoverPreviews() {
  document.addEventListener("pointerover", event => {
    const card = event.target instanceof Element ? event.target.closest(".gameCard") : null;
    if (!card || card === previewCard) return;
    previewCardEffects(card);
  }, { passive: true });
  document.addEventListener("pointerout", event => {
    if (!previewCard) return;
    const next = event.relatedTarget instanceof Element ? event.relatedTarget.closest(".gameCard") : null;
    if (next === previewCard) return;
    clearPreview();
  }, { passive: true });
}

function hudTone(key, delta) {
  if (delta < 0) return "loss";
  if (key === "trade") return "trade";
  if (key === "combat") return "combat";
  if (key === "authority") return "gain";
  if (key === "control") return "draw";
  return "special";
}

function hudLabel(key, delta) {
  const names = { authority: "Authority", trade: "Trade", combat: "Combat", control: "Control", purge: "Purge" };
  return `${names[key] || key} ${delta > 0 ? "+" : "−"}${Math.abs(delta)}`;
}

function observeHudValue(key, valueId, statId) {
  const value = document.getElementById(valueId);
  const stat = document.getElementById(statId);
  if (!value || !stat) return;
  hudValues.set(key, Number(value.textContent) || 0);
  const observer = new MutationObserver(() => {
    const next = Number(value.textContent) || 0;
    const previous = hudValues.get(key);
    hudValues.set(key, next);
    if (!Number.isFinite(previous) || next === previous) return;
    const delta = next - previous;
    showFloat(stat, hudLabel(key, delta), hudTone(key, delta));
    if (lastSourceElement && performance.now() - lastSourceAt < 900 && isVisible(lastSourceElement)) {
      const color = key === "trade" ? "#e4c94e" : key === "combat" ? "#ef7181" : key === "authority" ? "#68d491" : "#72baf0";
      showBeam(lastSourceElement, stat, color);
    }
  });
  observer.observe(value, { childList: true, characterData: true, subtree: true });
}

function installHudObservers() {
  observeHudValue("authority", "commanderAuthorityValue", "commanderAuthority");
  observeHudValue("trade", "commanderTradeValue", "commanderTrade");
  observeHudValue("combat", "commanderCombatValue", "commanderCombat");
  observeHudValue("control", "commanderControlValue", "commanderControl");
  observeHudValue("purge", "commanderPurgeValue", "commanderPurge");
}

function replaceButtonCopy(button, icon, label) {
  if (!(button instanceof HTMLButtonElement)) return;
  button.innerHTML = `<span aria-hidden="true">${icon}</span> ${label}`;
}

function installMenuCleanup() {
  const multiplayer = document.querySelector('#moreMenu [data-primary-destination="multiplayer"]');
  const publicDecks = document.querySelector('#moreMenu [data-primary-destination="public"]');
  const help = document.getElementById("helpBtn");
  const botRules = document.getElementById("singleplayerRulesMenuBtn");
  const lobby = document.getElementById("lobbyBtn");
  replaceButtonCopy(multiplayer, "◎", "Online Battle");
  replaceButtonCopy(publicDecks, "▦", "Community Decks");
  replaceButtonCopy(help, "?", "Codex & Rules");
  replaceButtonCopy(botRules, "⚙", "Bot Rules");
  replaceButtonCopy(lobby, "⌂", "Game Hub");
  const more = document.getElementById("moreMenuBtn");
  if (more) {
    more.setAttribute("aria-label", "More War Realms options");
    more.title = "More options";
  }
  const navHints = {
    battle: "Play — start or resume a battle",
    store: "Armory — cards and collection",
    deck: "Decks — build, analyze, and select decks",
    campaign: "Campaign — continue your current run"
  };
  Object.entries(navHints).forEach(([destination, hint]) => {
    const button = document.querySelector(`.primaryNavBtn[data-primary-destination="${destination}"]`);
    if (!button) return;
    button.title = hint;
    button.setAttribute("aria-label", hint);
  });
}

function firstVisibleClose() {
  const selectors = [
    `#warrealmsPlayLauncher:not([hidden]) [data-wr-play-close]`,
    "#closeAdvancedFiltersBtn",
    ".choiceModal .modalClose",
    ".modal:not(.hidden) .modalClose",
    "#summaryDrawerClose"
  ];
  for (const selector of selectors) {
    const candidate = [...document.querySelectorAll(selector)].find(isVisible);
    if (candidate) return candidate;
  }
  return null;
}

function installEscapeNavigation() {
  document.addEventListener("keydown", event => {
    if (event.key !== "Escape" || event.defaultPrevented) return;
    const close = firstVisibleClose();
    if (close instanceof HTMLElement) {
      event.preventDefault();
      close.click();
      return;
    }
    const moreMenu = document.getElementById("moreMenu");
    if (moreMenu && !moreMenu.classList.contains("hidden")) {
      event.preventDefault();
      document.getElementById("moreMenuBtn")?.click();
    }
  });
}

function install() {
  if (!hasDocument()) return;
  injectStyles();
  ensureLayer();
  ensureChainRoot();
  installMenuCleanup();
  installEscapeNavigation();
  installHoverPreviews();
  installHudObservers();
  globalThis.addEventListener?.(WARREALMS_EVENT, handleGameEvent);
}

if (hasDocument()) {
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", install, { once: true });
  else install();
}
