import {
  TEST_LAB_STRATEGIES,
  getTestLabCard,
  getTestLabCards,
  testLabStrategyFit
} from "./test-lab-simulator.js";
import {
  EXPANDED_BOT_STRATEGIES,
  expandedStrategyById,
  expandedStrategyFit
} from "./bot-strategy-expansion.js";

export const TEST_LAB_PRESET_PREFIX = "deck:";
export const TEST_LAB_PRESET_DECK_SIZE = 50;

const CORE_BY_ID = new Map(TEST_LAB_STRATEGIES.map(strategy => [strategy.id, strategy]));
const EXPANDED_BY_ID = new Map(EXPANDED_BOT_STRATEGIES.map(strategy => [strategy.id, strategy]));
const TEST_CARDS = Object.freeze(getTestLabCards());

const PRESET_DEFINITIONS = Object.freeze([
  ...TEST_LAB_STRATEGIES
    .filter(strategy => strategy.id !== "random")
    .map(strategy => Object.freeze({
      id: strategy.id,
      strategyId: strategy.id,
      name: `${strategy.name} Preset Deck`,
      primary: Object.freeze([...(strategy.primary || [])]),
      support: Object.freeze([...(strategy.support || [])]),
      description: strategy.description || ""
    })),
  ...EXPANDED_BOT_STRATEGIES
    .filter(strategy => strategy.id !== "legion")
    .map(strategy => Object.freeze({
      id: strategy.id,
      strategyId: strategy.id,
      name: `${strategy.name} Preset Deck`,
      primary: Object.freeze([...(strategy.primary || [])]),
      support: Object.freeze([...(strategy.support || [])]),
      description: strategy.description || ""
    })),
  Object.freeze({
    id: "legion-gorak",
    strategyId: "legion",
    name: "Legion Preset Deck — Gorak",
    primary: Object.freeze(["green"]),
    support: Object.freeze([]),
    description: "Faction-pure Gorak Legion deck built to trigger Ally and Double Ally effects as consistently as possible."
  }),
  Object.freeze({
    id: "legion-umbral",
    strategyId: "legion",
    name: "Legion Preset Deck — Umbral",
    primary: Object.freeze(["red"]),
    support: Object.freeze([]),
    description: "Faction-pure Umbral Legion deck built to trigger Ally and Double Ally effects as consistently as possible."
  })
]);

function number(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function strategyFit(card, definition) {
  const fullCard = getTestLabCard(card.id);
  if (!fullCard) return -Infinity;
  const strategyId = definition.strategyId;
  const expanded = expandedStrategyById(strategyId);
  const mechanical = expanded
    ? expandedStrategyFit(fullCard, strategyId)
    : testLabStrategyFit(fullCard, strategyId);
  let score = number(mechanical);
  if (definition.primary.includes(card.faction)) score += 6;
  else if (definition.support.includes(card.faction)) score += 1.75;
  else if (card.faction && card.faction !== "neutral") score -= 1.25;
  if (card.type === "base" && ["stronghold", "bastion", "architect", "arsenal"].includes(strategyId)) score += 2.25;
  if (card.type === "attachment" && strategyId === "arsenal") score += 3;
  score += Math.max(0, 8 - number(card.cost)) * .025;
  return score;
}

function rankedCards(definition) {
  return TEST_CARDS
    .map(card => ({ card, score: strategyFit(card, definition) }))
    .filter(entry => Number.isFinite(entry.score))
    .sort((left, right) => right.score - left.score || number(left.card.cost) - number(right.card.cost) || left.card.id.localeCompare(right.card.id));
}

function addBand(deck, copies, ranked, predicate, targetSize) {
  const candidates = ranked.filter(entry => predicate(number(entry.card.cost)));
  for (let pass = 0; pass < 4 && deck.length < targetSize; pass += 1) {
    for (const entry of candidates) {
      if (deck.length >= targetSize) break;
      const count = copies.get(entry.card.id) || 0;
      if (count >= 4) continue;
      deck.push(entry.card.id);
      copies.set(entry.card.id, count + 1);
    }
  }
}

function buildPresetDeck(definition) {
  const ranked = rankedCards(definition);
  const deck = [];
  const copies = new Map();

  // Match the normal WarRealms 50-card command-deck curve so the preset is a legal,
  // representative player contribution rather than a pile of only signature cards.
  addBand(deck, copies, ranked, cost => cost >= 1 && cost <= 3, 25);
  addBand(deck, copies, ranked, cost => cost === 4, 40);
  addBand(deck, copies, ranked, cost => cost >= 5, TEST_LAB_PRESET_DECK_SIZE);

  if (deck.length < TEST_LAB_PRESET_DECK_SIZE) {
    addBand(deck, copies, ranked, () => true, TEST_LAB_PRESET_DECK_SIZE);
  }
  return Object.freeze(deck.slice(0, TEST_LAB_PRESET_DECK_SIZE));
}

export const TEST_LAB_DECK_PRESETS = Object.freeze(PRESET_DEFINITIONS.map(definition => Object.freeze({
  ...definition,
  value: `${TEST_LAB_PRESET_PREFIX}${definition.id}`,
  cardIds: buildPresetDeck(definition)
})));

const PRESET_BY_VALUE = new Map(TEST_LAB_DECK_PRESETS.flatMap(preset => [
  [preset.value, preset],
  [preset.id, preset]
]));

export function getTestLabDeckPreset(value) {
  if (!value) return null;
  return PRESET_BY_VALUE.get(String(value)) || null;
}

export function isTestLabDeckPreset(value) {
  return !!getTestLabDeckPreset(value);
}

export function testLabDeckPresetSummary(presetOrValue) {
  const preset = typeof presetOrValue === "string" ? getTestLabDeckPreset(presetOrValue) : presetOrValue;
  if (!preset) return null;
  const counts = new Map();
  let low = 0;
  let four = 0;
  let high = 0;
  for (const cardId of preset.cardIds) {
    const card = getTestLabCard(cardId);
    const cost = number(card?.cost);
    if (cost <= 3) low += 1;
    else if (cost === 4) four += 1;
    else high += 1;
    counts.set(cardId, (counts.get(cardId) || 0) + 1);
  }
  return {
    id: preset.id,
    name: preset.name,
    strategyId: preset.strategyId,
    cards: preset.cardIds.length,
    lowCostCards: low,
    fourCostCards: four,
    highCostCards: high,
    maxCopies: Math.max(0, ...counts.values()),
    uniqueCards: counts.size
  };
}

export function coreStrategyForPreset(presetOrValue) {
  const preset = typeof presetOrValue === "string" ? getTestLabDeckPreset(presetOrValue) : presetOrValue;
  if (!preset) return null;
  return CORE_BY_ID.get(preset.strategyId) || EXPANDED_BY_ID.get(preset.strategyId) || null;
}
