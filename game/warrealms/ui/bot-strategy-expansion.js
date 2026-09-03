const freeze = Object.freeze;

export const BASE_NATIVE_STRATEGY_IDS = freeze([
  "vanguard", "engine", "stronghold", "cycle", "siege", "attrition", "marketeer",
  "summoning", "ascendents", "bastion", "fleet"
]);

export const EXPANDED_BOT_STRATEGIES = freeze([
  freeze({
    id: "reactor",
    name: "Reactor",
    icon: "♨",
    color: "#49a7e8",
    secondary: "#c2465d",
    factions: "Blue + Red",
    primary: freeze(["blue", "red"]),
    support: freeze(["yellow"]),
    deck: "Heat · thresholds · cooling",
    description: "Builds Heat deliberately, hits payoff thresholds, then cools or spends it for explosive turns."
  }),
  freeze({
    id: "sacrifice",
    name: "Sacrifice",
    icon: "†",
    color: "#c2465d",
    secondary: "#67b64c",
    factions: "Red + Green",
    primary: freeze(["red", "green"]),
    support: freeze(["yellow"]),
    deck: "Sacrifice · token fuel · death payoffs",
    description: "Feeds expendable cards and Tokens into sacrifice engines, then cashes in the death-trigger payoffs."
  }),
  freeze({
    id: "architect",
    name: "Architect",
    icon: "⌂",
    color: "#49a7e8",
    secondary: "#67b64c",
    factions: "Blue + Green",
    primary: freeze(["blue", "green"]),
    support: freeze(["yellow"]),
    deck: "Construction · acceleration · repair",
    description: "Prioritizes Construction projects and the cards that accelerate, repair, and protect massive structures."
  }),
  freeze({
    id: "overcharger",
    name: "Overcharger",
    icon: "⚡",
    color: "#49a7e8",
    secondary: "#d6bb3d",
    factions: "Blue + Yellow",
    primary: freeze(["blue", "yellow"]),
    support: freeze(["red"]),
    deck: "Charge · activations · burst",
    description: "Builds Charge engines and hunts cards whose stored Charge unlocks repeated high-impact actions."
  }),
  freeze({
    id: "legion",
    name: "Legion",
    icon: "⚑",
    color: "#67b64c",
    secondary: "#c2465d",
    factions: "Green + Red",
    primary: freeze(["green", "red"]),
    support: freeze([]),
    deck: "Ally · Double Ally · faction purity",
    description: "Commits hard to one faction so Ally and Double Ally abilities trigger as reliably as possible."
  }),
  freeze({
    id: "arsenal",
    name: "Arsenal",
    icon: "⚒",
    color: "#49a7e8",
    secondary: "#d6bb3d",
    factions: "Blue + Yellow",
    primary: freeze(["blue", "yellow"]),
    support: freeze(["green"]),
    deck: "Attachments · fitted Bases · upgrades",
    description: "Builds upgrade-ready Bases and aggressively pairs them with Attachments, armor, and repair support."
  })
]);

export const EXPANDED_BOT_STRATEGY_IDS = freeze(EXPANDED_BOT_STRATEGIES.map(strategy => strategy.id));
export const NATIVE_STRATEGY_ORDER = freeze([...BASE_NATIVE_STRATEGY_IDS, ...EXPANDED_BOT_STRATEGY_IDS]);

const native = (definition) => freeze({
  ...definition,
  preferredFactions: freeze([...(definition.preferredFactions || [])]),
  weights: freeze({ ...(definition.weights || {}) }),
  coreAbilities: freeze([...(definition.coreAbilities || [])]),
  secondaryAbilities: freeze([...(definition.secondaryAbilities || [])]),
  preferredTypes: freeze([...(definition.preferredTypes || [])]),
  costProfile: freeze({ ...(definition.costProfile || {}) }),
  factionTargets: freeze({ ...(definition.factionTargets || {}) }),
  minimumAbilities: freeze((definition.minimumAbilities || []).map(requirement => freeze({
    ...requirement,
    ...(requirement.abilities ? { abilities: freeze([...requirement.abilities]) } : {}),
    ...(requirement.types ? { types: freeze([...requirement.types]) } : {})
  }))),
  playPriorities: freeze([...(definition.playPriorities || [])])
});

export const NATIVE_BOT_STRATEGY_EXTENSIONS = freeze({
  reactor: native({
    name: "Reactor",
    preferredFactions: ["blue", "red"],
    weights: { addHeat: 2.25, coolHeat: 1.7, combat: .64, draw: .72, shield: .28, trade: .2 },
    coreAbilities: ["addHeat", "coolHeat"],
    secondaryAbilities: ["draw", "combat"],
    preferredTypes: ["unit", "base"],
    costProfile: { idealMin: 2, idealMax: 7, expensiveTolerance: .6, signatureChance: .66 },
    factionTargets: { primary: .58, support: .26 },
    minimumAbilities: [{ abilities: ["addHeat", "coolHeat"], min: 4 }],
    playPriorities: ["addHeat", "coolHeat", "draw", "combat"],
    baseTarget: 5,
    baseBias: .16,
    defenseWeight: .07,
    drawPriority: true
  }),
  sacrifice: native({
    name: "Sacrifice",
    preferredFactions: ["red", "green"],
    weights: { createToken: 1.7, combat: .82, draw: .52, scrapOwn: .82, reclaim: .76, redeploy: .54, trade: .16 },
    coreAbilities: ["createToken", "combat"],
    secondaryAbilities: ["reclaim", "scrapOwn"],
    preferredTypes: ["unit", "base"],
    costProfile: { idealMin: 1, idealMax: 6, expensiveTolerance: .48, signatureChance: .5 },
    factionTargets: { primary: .61, support: .24 },
    minimumAbilities: [{ abilities: ["createToken"], min: 3 }],
    playPriorities: ["createToken", "combat", "reclaim"],
    baseTarget: 4,
    baseBias: .1,
    defenseWeight: .05,
    drawPriority: false
  }),
  architect: native({
    name: "Architect",
    preferredFactions: ["blue", "green"],
    weights: { advanceConstruction: 3.1, repair: 1.8, shield: .88, tradePerBase: .82, combatPerBase: .58, drawPerBase: .76, healPerBase: .62, trade: .16 },
    coreAbilities: ["advanceConstruction", "repair"],
    secondaryAbilities: ["shield", "tradePerBase", "drawPerBase"],
    preferredTypes: ["base", "attachment"],
    costProfile: { idealMin: 3, idealMax: 9, expensiveTolerance: .8, signatureChance: .82 },
    factionTargets: { primary: .59, support: .28 },
    minimumAbilities: [{ types: ["base"], min: 10 }, { abilities: ["advanceConstruction", "repair"], min: 3 }],
    playPriorities: ["base", "advanceConstruction", "repair", "shield"],
    baseTarget: 14,
    baseBias: 1.55,
    defenseWeight: .2,
    drawPriority: false
  }),
  overcharger: native({
    name: "Overcharger",
    preferredFactions: ["blue", "yellow"],
    weights: { gainCharge: 2.2, combat: .92, draw: 1.02, shield: .58, trade: .48, stun: .58, destroyBase: .7 },
    coreAbilities: ["draw", "combat"],
    secondaryAbilities: ["shield", "trade"],
    preferredTypes: ["unit", "base"],
    costProfile: { idealMin: 2, idealMax: 7, expensiveTolerance: .62, signatureChance: .66 },
    factionTargets: { primary: .58, support: .27 },
    minimumAbilities: [{ abilities: ["draw", "combat"], min: 5 }],
    playPriorities: ["draw", "combat", "shield", "trade"],
    baseTarget: 5,
    baseBias: .18,
    defenseWeight: .07,
    drawPriority: true
  }),
  legion: native({
    name: "Legion",
    preferredFactions: ["green", "red"],
    weights: { combat: .82, draw: .7, trade: .38, shield: .32, heal: .3, createToken: .5 },
    coreAbilities: ["combat", "draw"],
    secondaryAbilities: ["trade", "createToken"],
    preferredTypes: ["unit"],
    costProfile: { idealMin: 1, idealMax: 6, expensiveTolerance: .42, signatureChance: .44 },
    factionTargets: { primary: .74, support: .14 },
    minimumAbilities: [{ abilities: ["combat", "draw"], min: 5 }],
    playPriorities: ["combat", "draw", "createToken"],
    baseTarget: 3,
    baseBias: .05,
    defenseWeight: .03,
    drawPriority: true
  }),
  arsenal: native({
    name: "Arsenal",
    preferredFactions: ["blue", "yellow"],
    weights: { armor: 1.45, repair: 1.3, shield: .88, combatPerBase: .94, tradePerBase: .86, drawPerBase: .9, combat: .35, trade: .32 },
    coreAbilities: ["shield", "repair"],
    secondaryAbilities: ["combatPerBase", "tradePerBase", "drawPerBase"],
    preferredTypes: ["attachment", "base"],
    costProfile: { idealMin: 2, idealMax: 8, expensiveTolerance: .68, signatureChance: .72 },
    factionTargets: { primary: .59, support: .27 },
    minimumAbilities: [{ types: ["base"], min: 7 }, { types: ["attachment"], min: 4 }],
    playPriorities: ["base", "shield", "repair"],
    baseTarget: 10,
    baseBias: .88,
    defenseWeight: .16,
    drawPriority: false
  })
});

function number(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function walk(value, visit, seen = new WeakSet()) {
  if (!value || typeof value !== "object" || seen.has(value)) return;
  seen.add(value);
  if (Array.isArray(value)) {
    value.forEach(child => walk(child, visit, seen));
    return;
  }
  for (const [key, child] of Object.entries(value)) {
    visit(key, child, value);
    walk(child, visit, seen);
  }
}

function hasAnyKey(value, keys) {
  const wanted = new Set(keys);
  let found = false;
  walk(value, key => { if (wanted.has(key)) found = true; });
  return found;
}

function sumKey(value, wantedKey) {
  let total = 0;
  walk(value, (key, child) => {
    if (key !== wantedKey) return;
    if (typeof child === "number") total += Math.max(0, child);
    else if (child && typeof child === "object") total += Math.max(0, number(child.amount ?? child.count ?? child.value));
  });
  return total;
}

function countKey(value, wantedKey) {
  let total = 0;
  walk(value, key => { if (key === wantedKey) total += 1; });
  return total;
}

function includesEvent(value, eventName) {
  let found = false;
  walk(value, (key, child) => {
    if (key === "event" && String(child || "").toUpperCase() === eventName) found = true;
  });
  return found;
}

function createdTokenCount(value) {
  let total = 0;
  walk(value, (key, child) => {
    if (key !== "createToken" || !child || typeof child !== "object") return;
    total += Math.max(1, number(child.count ?? child.amount) || 1);
  });
  return total;
}

function attachmentSlots(card) {
  if (!card || card.type !== "base") return 0;
  return Math.max(0, number(card.attachmentSlots ?? card.attachmentCapacity ?? (card.expansion ? 2 : 0)));
}

export function expandedStrategyMechanicScore(card, strategyId) {
  if (!card || !strategyId) return 0;
  switch (strategyId) {
    case "reactor": {
      const heatActions = Array.isArray(card.heat?.actions) ? card.heat.actions.length : (card.heat?.action ? 1 : 0);
      const heatThresholds = Array.isArray(card.heat?.thresholds) ? card.heat.thresholds.length : 0;
      return (card.heat ? 5 : 0) + heatActions * 2.2 + heatThresholds * 1.35 +
        sumKey(card, "addHeat") * 2.3 + sumKey(card, "coolHeat") * 1.9 +
        (hasAnyKey(card, ["moveHeat", "harvestHeat", "heatAura"]) ? 3 : 0) +
        (card.transform?.trigger === "heat" ? 3.5 : 0);
    }
    case "sacrifice":
      return (card.sacrifice ? 6 : 0) + (card.sacrificeTrigger ? 5 : 0) + (card.tokenSacrificeTrigger ? 5.5 : 0) +
        (card.sacrificeThresholds ? 3.5 : 0) + (card.tokenSacrificeThresholds ? 4 : 0) +
        (hasAnyKey(card, ["sacrificeRequired", "optionalTokenSacrificeChain"]) ? 3 : 0) +
        (includesEvent(card, "TOKEN_SACRIFICED") || includesEvent(card, "CARD_SACRIFICED") ? 3.5 : 0) +
        createdTokenCount(card) * 1.35;
    case "architect":
      return (card.type === "base" && (card.expansion || number(card.construction) > 0) ? 8 : 0) +
        sumKey(card, "advanceConstruction") * 3.25 + sumKey(card, "repair") * 1.55 +
        (card.type === "base" ? 1 : 0);
    case "overcharger": {
      const actions = Array.isArray(card.charge?.actions) ? card.charge.actions.length : 0;
      return (card.charge ? 5 : 0) + actions * 3 + countKey(card, "gainCharge") * 3 + countKey(card, "charges") * 1.6 +
        (card.charge?.trigger || card.charge?.triggers ? 2.5 : 0);
    }
    case "legion":
      return (card.ally && Object.keys(card.ally).length ? 4.5 : 0) +
        ((card.doubleAlly || card.double_ally || card.ally2) ? 7 : 0) +
        (card.factionScaling || card.factionThresholds ? 2 : 0) +
        (card.faction && card.faction !== "neutral" ? .5 : 0);
    case "arsenal":
      return (card.type === "attachment" ? 9 : 0) + attachmentSlots(card) * 2.8 +
        (hasAnyKey(card, ["armor"]) ? 2.8 : 0) + sumKey(card, "repair") * .9 +
        (card.type === "base" ? 1.2 : 0);
    default:
      return 0;
  }
}

export function expandedStrategyFit(card, strategyId) {
  const strategy = EXPANDED_BOT_STRATEGIES.find(candidate => candidate.id === strategyId);
  if (!strategy || !card) return 0;
  let value = expandedStrategyMechanicScore(card, strategyId);
  if (strategy.primary.includes(card.faction)) value += 2.8;
  else if (strategy.support.includes(card.faction)) value += 1.1;
  else if (card.faction && card.faction !== "neutral") value -= .35;
  return value;
}

export function expandedStrategyById(strategyId) {
  return EXPANDED_BOT_STRATEGIES.find(strategy => strategy.id === strategyId) || null;
}

export function extendNativeBotStrategyRegistry(registry) {
  if (!registry || typeof registry !== "object" || Array.isArray(registry)) return registry;
  const result = { ...registry };
  for (const strategyId of EXPANDED_BOT_STRATEGY_IDS) {
    if (!Object.prototype.hasOwnProperty.call(result, strategyId)) result[strategyId] = NATIVE_BOT_STRATEGY_EXTENSIONS[strategyId];
  }
  return freeze(result);
}

function looksLikeNativeWarRealmsRegistry(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  if (EXPANDED_BOT_STRATEGY_IDS.some(id => Object.prototype.hasOwnProperty.call(value, id))) return false;
  if (!BASE_NATIVE_STRATEGY_IDS.every(id => Object.prototype.hasOwnProperty.call(value, id))) return false;
  return value.vanguard?.name === "Vanguard" && value.engine?.name === "Engine" && value.fleet?.name === "Fleet";
}

let bridgeInstalled = false;

export function installNativeBotStrategyBridge() {
  if (bridgeInstalled || typeof Object.freeze !== "function") return;
  bridgeInstalled = true;
  const originalFreeze = Object.freeze;
  let active = true;

  function bridgedFreeze(value) {
    if (active && looksLikeNativeWarRealmsRegistry(value)) {
      active = false;
      if (Object.freeze === bridgedFreeze) Object.freeze = originalFreeze;
      return extendNativeBotStrategyRegistry(value);
    }
    return originalFreeze(value);
  }

  Object.freeze = bridgedFreeze;
  queueMicrotask(() => {
    active = false;
    if (Object.freeze === bridgedFreeze) Object.freeze = originalFreeze;
  });
}
