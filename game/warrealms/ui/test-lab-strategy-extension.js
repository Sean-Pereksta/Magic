import {
  TEST_LAB_STRATEGIES,
  getTestLabCard,
  getTestLabCards,
  simulateTestLabGame as simulateCoreTestLabGame
} from "./test-lab-simulator.js";
import {
  EXPANDED_BOT_STRATEGIES,
  expandedStrategyById,
  expandedStrategyFit
} from "./bot-strategy-expansion.js";

const CORE_STRATEGY_IDS = new Set(TEST_LAB_STRATEGIES.map(strategy => strategy.id));
const NAMED_EXTENDED_STRATEGIES = Object.freeze([
  ...TEST_LAB_STRATEGIES.filter(strategy => strategy.id !== "random"),
  ...EXPANDED_BOT_STRATEGIES.map(strategy => ({
    id: strategy.id,
    name: strategy.name,
    primary: [...strategy.primary],
    support: [...strategy.support],
    description: strategy.description
  }))
]);

export const EXTENDED_TEST_LAB_STRATEGIES = Object.freeze([
  TEST_LAB_STRATEGIES.find(strategy => strategy.id === "random"),
  ...NAMED_EXTENDED_STRATEGIES
].filter(Boolean));

const CORE_FALLBACK = Object.freeze({
  reactor: "engine",
  sacrifice: "attrition",
  architect: "bastion",
  overcharger: "engine",
  legion: "vanguard",
  arsenal: "bastion"
});
const TEST_LAB_CARDS = Object.freeze(getTestLabCards());
const RANKING_CACHE = new Map();

function number(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function seededUnit(seed) {
  let state = Math.max(1, Number(seed) >>> 0);
  state += 0x6D2B79F5;
  let result = state;
  result = Math.imul(result ^ result >>> 15, result | 1);
  result ^= result + Math.imul(result ^ result >>> 7, result | 61);
  return ((result ^ result >>> 14) >>> 0) / 4294967296;
}

function resolvedStrategy(requested, seed, gameIndex, side) {
  if (requested && requested !== "random" && EXTENDED_TEST_LAB_STRATEGIES.some(strategy => strategy.id === requested)) return requested;
  const sideSalt = side === "a" ? 0x9E3779B9 : 0x85EBCA6B;
  const roll = seededUnit((Math.max(1, number(seed) || 24681357) + Math.imul(gameIndex + 1, 2654435761) + sideSalt) >>> 0);
  return NAMED_EXTENDED_STRATEGIES[Math.min(NAMED_EXTENDED_STRATEGIES.length - 1, Math.floor(roll * NAMED_EXTENDED_STRATEGIES.length))]?.id || "vanguard";
}

function mappedCoreStrategy(strategyId) {
  if (CORE_STRATEGY_IDS.has(strategyId)) return strategyId;
  return CORE_FALLBACK[strategyId] || "vanguard";
}

function legionFaction(seed, gameIndex, side) {
  const salt = side === "a" ? 0x27D4EB2D : 0x165667B1;
  return seededUnit((Math.max(1, number(seed) || 24681357) + Math.imul(gameIndex + 7, 2246822519) + salt) >>> 0) < .5 ? "green" : "red";
}

function rankedStrategyCards(strategyId, options = {}) {
  if (!expandedStrategyById(strategyId)) return [];
  const wantedFaction = strategyId === "legion" ? options.legionFaction : "";
  const cacheKey = `${strategyId}:${wantedFaction || "all"}`;
  if (RANKING_CACHE.has(cacheKey)) return RANKING_CACHE.get(cacheKey);
  const ranked = TEST_LAB_CARDS
    .map(card => ({ card, definition: getTestLabCard(card.id) }))
    .filter(entry => entry.definition && (!wantedFaction || entry.definition.faction === wantedFaction))
    .map(entry => ({ id: entry.card.id, score: expandedStrategyFit(entry.definition, strategyId), cost: number(entry.card.cost) }))
    .filter(entry => entry.score > 2.5)
    .sort((left, right) => right.score - left.score || left.cost - right.cost || left.id.localeCompare(right.id));
  const cached = Object.freeze(ranked);
  RANKING_CACHE.set(cacheKey, cached);
  return cached;
}

export function testLabExtendedStrategyFit(cardOrId, strategyId) {
  const card = typeof cardOrId === "string" ? getTestLabCard(cardOrId) : cardOrId;
  return card ? expandedStrategyFit(card, strategyId) : 0;
}

function strategyAutoPriority(strategyId, seed, gameIndex, side) {
  if (!expandedStrategyById(strategyId)) return [];
  const faction = strategyId === "legion" ? legionFaction(seed, gameIndex, side) : "";
  return rankedStrategyCards(strategyId, { legionFaction: faction }).slice(0, 8).map(entry => entry.id);
}

function strategyMarketCards(strategyId, seed, gameIndex, side) {
  if (!expandedStrategyById(strategyId)) return [];
  const faction = strategyId === "legion" ? legionFaction(seed, gameIndex, side) : "";
  return rankedStrategyCards(strategyId, { legionFaction: faction }).slice(0, 4).map(entry => entry.id);
}

function manualPriority(options, side) {
  const suffix = side === "a" ? "A" : "B";
  return {
    enabled: options[`priority${suffix}Enabled`] === true,
    ids: new Set(Array.isArray(options[`priorityCards${suffix}`]) ? options[`priorityCards${suffix}`] : []),
    mode: options[`priorityMode${suffix}`] || "prefer"
  };
}

function applyInternalPriority(options, side, strategyId, seed, gameIndex) {
  const suffix = side === "a" ? "A" : "B";
  const manual = manualPriority(options, side);
  if (manual.enabled || !expandedStrategyById(strategyId)) return;
  const ids = strategyAutoPriority(strategyId, seed, gameIndex, side);
  if (!ids.length) return;
  options[`priority${suffix}Enabled`] = true;
  options[`priorityCards${suffix}`] = ids;
  options[`priorityMode${suffix}`] = "strong";
}

function restoreVisiblePriority(result, originalOptions) {
  for (const side of ["a", "b"]) {
    const manual = manualPriority(originalOptions, side);
    result.priorities[side] = {
      enabled: manual.enabled && manual.ids.size > 0,
      mode: manual.mode,
      cards: [...manual.ids]
    };
    for (const purchase of result.purchases || []) {
      if (purchase.playerId !== side) continue;
      purchase.priority = manual.enabled && manual.ids.has(purchase.cardId);
    }
  }
}

export function simulateExtendedTestLabGame(options = {}, gameIndex = 0) {
  const seed = Math.max(1, number(options.seed) || 24681357);
  const strategyA = resolvedStrategy(options.strategyA || "random", seed, gameIndex, "a");
  const strategyB = resolvedStrategy(options.strategyB || "random", seed, gameIndex, "b");
  const internal = {
    ...options,
    strategyA: mappedCoreStrategy(strategyA),
    strategyB: mappedCoreStrategy(strategyB)
  };

  applyInternalPriority(internal, "a", strategyA, seed, gameIndex);
  applyInternalPriority(internal, "b", strategyB, seed, gameIndex);

  const injected = new Set(Array.isArray(options.experimentalCardIds) ? options.experimentalCardIds : []);
  strategyMarketCards(strategyA, seed, gameIndex, "a").forEach(cardId => injected.add(cardId));
  strategyMarketCards(strategyB, seed, gameIndex, "b").forEach(cardId => injected.add(cardId));
  internal.experimentalCardIds = [...injected];
  internal.experimentalCopies = Math.max(2, Math.min(4, number(options.experimentalCopies) || 2));

  const result = simulateCoreTestLabGame(internal, gameIndex);
  result.strategies = { a: strategyA, b: strategyB };
  restoreVisiblePriority(result, options);
  return result;
}

function appendExpandedOptions() {
  if (typeof document === "undefined") return;
  for (const selectId of ["strategyA", "strategyB"]) {
    const select = document.getElementById(selectId);
    if (!select) continue;
    const selected = select.value;
    for (const strategy of EXPANDED_BOT_STRATEGIES) {
      if (select.querySelector(`option[value="${strategy.id}"]`)) continue;
      const option = document.createElement("option");
      option.value = strategy.id;
      option.textContent = strategy.name;
      option.title = strategy.description;
      select.appendChild(option);
    }
    if (selected) select.value = selected;
  }
}

function installTestLabStrategyOptions() {
  if (typeof document === "undefined") return;
  const schedule = () => queueMicrotask(appendExpandedOptions);
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", schedule, { once: true });
  schedule();
  const observer = new MutationObserver(() => appendExpandedOptions());
  const startObserver = () => document.body && observer.observe(document.body, { childList: true, subtree: true });
  if (document.body) startObserver();
  else document.addEventListener("DOMContentLoaded", startObserver, { once: true });
}

installTestLabStrategyOptions();
