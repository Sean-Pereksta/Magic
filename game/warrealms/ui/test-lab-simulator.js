import {
  CARDS,
  STARTER_CARDS,
  COMMAND_DECK_SIZE
} from "../../warrealms-pack/warrealms-cards.js?v=20";

const ALL_CARDS = Object.freeze([
  ...Object.values(STARTER_CARDS || {}),
  ...(Array.isArray(CARDS) ? CARDS : [])
]);
const CARD_BY_ID = new Map(ALL_CARDS.map(card => [card.id, card]));
const ELIGIBLE_CARDS = Object.freeze((Array.isArray(CARDS) ? CARDS : []).filter(card =>
  card?.id &&
  Number(card.cost) > 0 &&
  card.token !== true &&
  !card.marketOnlyPermanent &&
  !card.hiredLooter
));

export const TEST_LAB_STRATEGIES = Object.freeze([
  { id: "random", name: "Random", primary: [], support: [], description: "Randomly rotates through the available bot plans each game." },
  { id: "vanguard", name: "Vanguard", primary: ["green", "red"], support: ["blue"], description: "Combat pressure, base breaking, and recovery." },
  { id: "engine", name: "Engine", primary: ["yellow", "red"], support: ["blue"], description: "Trade, draw, cycling, and deck thinning." },
  { id: "stronghold", name: "Stronghold", primary: ["blue", "green"], support: ["yellow"], description: "Bases, Shield, durability, and scaling." },
  { id: "cycle", name: "Cycle", primary: ["blue", "yellow"], support: ["red"], description: "Draw, purge, and repeated high-value turns." },
  { id: "siege", name: "Siege", primary: ["green", "yellow"], support: ["red"], description: "Combat, structural pressure, disable, and raze." },
  { id: "attrition", name: "Attrition", primary: ["red", "blue"], support: ["yellow"], description: "Discard pressure, sustain, and thinning." },
  { id: "marketeer", name: "Marketeer", primary: ["yellow", "red"], support: ["blue"], description: "Trade, market control, and card flow." }
]);

export const TEST_LAB_DIFFICULTIES = Object.freeze([
  { id: "easy", name: "Easy", errorRate: .34, noise: .42 },
  { id: "medium", name: "Medium", errorRate: .16, noise: .24 },
  { id: "hard", name: "Hard", errorRate: .06, noise: .12 },
  { id: "impossible", name: "Impossible", errorRate: .015, noise: .045 }
]);

const STRATEGY_BY_ID = new Map(TEST_LAB_STRATEGIES.map(strategy => [strategy.id, strategy]));
const DIFFICULTY_BY_ID = new Map(TEST_LAB_DIFFICULTIES.map(difficulty => [difficulty.id, difficulty]));

const DIRECT_EFFECT_KEYS = new Set([
  "trade", "combat", "draw", "heal", "authority", "shield", "scrapOwn", "purge",
  "scrapMarket", "marketErase", "stun", "disable", "destroyBase", "repair", "createToken"
]);

function number(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function clamp(value, minimum, maximum) {
  return Math.max(minimum, Math.min(maximum, value));
}

function mulberry32(seed) {
  let value = Math.max(1, Number(seed) >>> 0);
  return () => {
    value += 0x6D2B79F5;
    let result = value;
    result = Math.imul(result ^ result >>> 15, result | 1);
    result ^= result + Math.imul(result ^ result >>> 7, result | 61);
    return ((result ^ result >>> 14) >>> 0) / 4294967296;
  };
}

function shuffle(values, random) {
  const result = [...values];
  for (let index = result.length - 1; index > 0; index -= 1) {
    const swap = Math.floor(random() * (index + 1));
    [result[index], result[swap]] = [result[swap], result[index]];
  }
  return result;
}

function pick(values, random) {
  return values[Math.floor(random() * values.length)];
}

function sumKey(value, wantedKey, seen = new WeakSet()) {
  if (!value || typeof value !== "object") return 0;
  if (seen.has(value)) return 0;
  seen.add(value);
  if (Array.isArray(value)) return value.reduce((total, child) => total + sumKey(child, wantedKey, seen), 0);
  let total = 0;
  for (const [key, child] of Object.entries(value)) {
    if (key === wantedKey && typeof child === "number") total += Math.max(0, child);
    else if (key === wantedKey && child && typeof child === "object") {
      if (typeof child.amount === "number") total += Math.max(0, child.amount);
      else if (typeof child.count === "number") total += Math.max(0, child.count);
    }
    total += sumKey(child, wantedKey, seen);
  }
  return total;
}

function hasKey(value, wantedKeys, seen = new WeakSet()) {
  if (!value || typeof value !== "object") return false;
  if (seen.has(value)) return false;
  seen.add(value);
  if (Array.isArray(value)) return value.some(child => hasKey(child, wantedKeys, seen));
  for (const [key, child] of Object.entries(value)) {
    if (wantedKeys.has(key)) return true;
    if (hasKey(child, wantedKeys, seen)) return true;
  }
  return false;
}

function directEffect(effect = {}) {
  const result = {
    trade: 0,
    combat: 0,
    draw: 0,
    heal: 0,
    shield: 0,
    purge: 0,
    marketErase: 0,
    disable: 0,
    destroyBase: 0,
    repair: 0,
    tokens: 0
  };
  if (!effect || typeof effect !== "object") return result;
  result.trade += Math.max(0, number(effect.trade));
  result.combat += Math.max(0, number(effect.combat));
  result.draw += Math.max(0, number(effect.draw));
  result.heal += Math.max(0, number(effect.heal)) + Math.max(0, number(effect.authority));
  result.shield += Math.max(0, number(effect.shield));
  result.purge += Math.max(0, number(effect.scrapOwn)) + Math.max(0, number(effect.purge));
  result.marketErase += Math.max(0, number(effect.scrapMarket)) + Math.max(0, number(effect.marketErase));
  result.disable += Math.max(0, number(effect.stun)) + Math.max(0, number(effect.disable));
  result.destroyBase += Math.max(0, number(effect.destroyBase));
  result.repair += Math.max(0, number(effect.repair?.amount ?? effect.repair));
  result.tokens += Math.max(0, number(effect.createToken?.count ?? effect.createToken?.amount ?? effect.createToken));
  return result;
}

function addEffects(target, source, multiplier = 1) {
  for (const key of Object.keys(target)) target[key] += number(source[key]) * multiplier;
  return target;
}

function emptyOutput() {
  return { trade: 0, combat: 0, draw: 0, heal: 0, shield: 0, purge: 0, marketErase: 0, disable: 0, destroyBase: 0, repair: 0, tokens: 0 };
}

function factionCount(cards) {
  return cards.reduce((counts, card) => {
    if (card?.faction && card.faction !== "neutral") counts[card.faction] = (counts[card.faction] || 0) + 1;
    return counts;
  }, {});
}

function cardOutput(card, handFactionCounts = {}, persistent = false) {
  const output = emptyOutput();
  addEffects(output, directEffect(card?.effect || {}));
  const sameFaction = Math.max(0, number(handFactionCounts[card?.faction]) - 1);
  if (card?.faction && card.faction !== "neutral" && sameFaction >= 1) addEffects(output, directEffect(card.ally || {}));
  if (card?.faction && card.faction !== "neutral" && sameFaction >= 2) addEffects(output, directEffect(card.doubleAlly || card.double_ally || card.ally2 || {}));

  // Heat, Charge, sacrifice, transformation, and trigger engines are intentionally
  // discounted rather than treated as free every turn. This keeps rapid simulations
  // from over-crediting conditional text while still recognizing those card systems.
  const conditional = emptyOutput();
  if (card?.heat) addEffects(conditional, directEffect(card.heat), .18);
  if (card?.charge) addEffects(conditional, directEffect(card.charge), .2);
  if (card?.trigger || card?.triggers) addEffects(conditional, directEffect({ trigger: card.trigger, triggers: card.triggers }), .22);
  if (card?.transform) addEffects(conditional, directEffect(card.transform), .12);
  if (card?.sacrifice || card?.sacrificeTrigger || card?.tokenSacrificeTrigger) {
    conditional.combat += 1.05;
    conditional.trade += .3;
  }
  if (hasKey(card, new Set(["addHeat", "moveHeat", "coolHeat", "harvestHeat"]))) conditional.combat += .65;
  if (hasKey(card, new Set(["discard", "oppDiscard", "opponentDiscard", "pendingDiscard"]))) conditional.combat += .55;
  if (hasKey(card, new Set(["combatPerBase", "tradePerBase", "drawPerBase"]))) {
    conditional.combat += sumKey(card, "combatPerBase") * 1.35;
    conditional.trade += sumKey(card, "tradePerBase") * 1.15;
    conditional.draw += sumKey(card, "drawPerBase") * .8;
  }
  if (persistent && card?.type === "base") {
    conditional.combat += .15;
    conditional.trade += .1;
  }
  addEffects(output, conditional);
  return output;
}

function strategicTags(card) {
  return {
    economy: sumKey(card, "trade") + sumKey(card, "draw") * 1.2 + (hasKey(card, new Set(["scrapOwn", "purge", "purgeAndDraw"])) ? 1.4 : 0),
    offense: sumKey(card, "combat") + (hasKey(card, new Set(["destroyBase", "combatAgainstBases", "damageAll"] )) ? 2 : 0),
    defense: number(card?.health || card?.defense) * (card?.type === "base" ? .25 : 0) + sumKey(card, "shield") + sumKey(card, "heal"),
    control: (hasKey(card, new Set(["stun", "disable", "discard", "oppDiscard", "scrapMarket", "marketErase"])) ? 2.5 : 0),
    engine: (hasKey(card, new Set(["addHeat", "charge", "createToken", "sacrifice", "transform", "construction", "attachment"])) ? 2.1 : 0)
  };
}

function strategyBonus(card, strategyId) {
  const strategy = STRATEGY_BY_ID.get(strategyId) || STRATEGY_BY_ID.get("vanguard");
  const tags = strategicTags(card);
  let bonus = 0;
  if (strategy.primary.includes(card.faction)) bonus += 3.1;
  else if (strategy.support.includes(card.faction)) bonus += 1.15;
  switch (strategyId) {
    case "vanguard": bonus += tags.offense * .72 + tags.defense * .14; break;
    case "engine": bonus += tags.economy * .68 + tags.engine * .42; break;
    case "stronghold": bonus += tags.defense * .72 + (card.type === "base" ? 2.6 : 0) + tags.economy * .18; break;
    case "cycle": bonus += tags.economy * .62 + sumKey(card, "draw") * .85; break;
    case "siege": bonus += tags.offense * .78 + tags.control * .38; break;
    case "attrition": bonus += tags.control * .72 + tags.defense * .28 + tags.engine * .22; break;
    case "marketeer": bonus += tags.economy * .72 + tags.control * .5; break;
    default: bonus += (tags.economy + tags.offense + tags.defense + tags.control + tags.engine) * .16;
  }
  return bonus;
}

function intrinsicCardValue(card) {
  const output = cardOutput(card, { [card?.faction]: 2 });
  const recurring = card?.type === "base" ? 1.38 : 1;
  const defense = card?.type === "base" ? Math.max(0, number(card.health || card.defense)) * .3 : 0;
  return (
    output.trade * 1.15 +
    output.combat * 1.05 +
    output.draw * 2 +
    output.heal * .52 +
    output.shield * .55 +
    output.purge * 1.8 +
    output.marketErase * .7 +
    output.disable * 1.15 +
    output.destroyBase * 3.2 +
    output.tokens * .8 +
    defense
  ) * recurring;
}

function purchaseScore(card, bot, difficulty, random) {
  const cost = Math.max(1, number(card?.cost));
  let score = intrinsicCardValue(card) + strategyBonus(card, bot.strategy);
  score += Math.min(3.5, cost * .18);
  if (bot.factionCounts[card.faction]) score += Math.min(2.6, bot.factionCounts[card.faction] * .13);
  if (difficulty.noise) score += (random() - .5) * difficulty.noise * 10;
  if (random() < difficulty.errorRate) score *= .45 + random() * .65;
  return score;
}

function resolveStrategy(strategyId, random) {
  if (strategyId && strategyId !== "random" && STRATEGY_BY_ID.has(strategyId)) return strategyId;
  return pick(TEST_LAB_STRATEGIES.filter(strategy => strategy.id !== "random"), random).id;
}

function weightedCommandDeck(strategyId, random) {
  const strategy = STRATEGY_BY_ID.get(strategyId) || STRATEGY_BY_ID.get("vanguard");
  const weighted = ELIGIBLE_CARDS.map(card => {
    const cost = Math.max(1, number(card.cost));
    let weight = 1;
    if (strategy.primary.includes(card.faction)) weight *= 3.1;
    else if (strategy.support.includes(card.faction)) weight *= 1.45;
    if (cost <= 3) weight *= 1.15;
    if (cost >= 8) weight *= .62;
    weight *= clamp(1 + strategyBonus(card, strategyId) * .055, .7, 2.6);
    return { card, weight };
  });
  const result = [];
  const copies = new Map();
  while (result.length < COMMAND_DECK_SIZE && weighted.length) {
    const total = weighted.reduce((sum, entry) => sum + entry.weight, 0);
    let roll = random() * total;
    let selected = weighted[weighted.length - 1];
    for (const entry of weighted) {
      roll -= entry.weight;
      if (roll <= 0) { selected = entry; break; }
    }
    const count = copies.get(selected.card.id) || 0;
    if (count < 4) {
      result.push(selected.card.id);
      copies.set(selected.card.id, count + 1);
    } else {
      const index = weighted.indexOf(selected);
      if (index >= 0) weighted.splice(index, 1);
    }
  }
  return result;
}

function createStarterDeck(random) {
  return shuffle([
    ...Array.from({ length: 8 }, () => "starter_coin"),
    ...Array.from({ length: 2 }, () => "starter_blade")
  ], random);
}

function createBot(id, label, strategy, difficulty, random) {
  return {
    id,
    label,
    strategy,
    difficulty,
    authority: 50,
    shield: 0,
    draw: createStarterDeck(random),
    discard: [],
    hand: [],
    bases: [],
    turn: 0,
    purchases: [],
    factionCounts: { yellow: 0, blue: 0, green: 0, red: 0 }
  };
}

function drawOne(bot, random) {
  if (!bot.draw.length && bot.discard.length) {
    bot.draw = shuffle(bot.discard, random);
    bot.discard = [];
  }
  return bot.draw.pop() || null;
}

function drawHand(bot, random, count = 5) {
  bot.hand = [];
  for (let index = 0; index < count; index += 1) {
    const cardId = drawOne(bot, random);
    if (!cardId) break;
    bot.hand.push(cardId);
  }
}

function deployAndResolveHand(bot, random) {
  const cards = bot.hand.map(id => CARD_BY_ID.get(id)).filter(Boolean);
  const factions = factionCount(cards);
  const output = emptyOutput();
  const playedShips = [];
  const baseIds = [];

  for (const card of cards) {
    if (card.type === "base") {
      const health = Math.max(1, number(card.health || card.defense || 5));
      bot.bases.push({ cardId: card.id, hp: health, maxHp: health, outpost: card.outpost === true });
      baseIds.push(card.id);
    } else {
      addEffects(output, cardOutput(card, factions));
      playedShips.push(card.id);
    }
  }
  const baseCards = bot.bases.map(base => CARD_BY_ID.get(base.cardId)).filter(Boolean);
  const baseFactions = factionCount([...cards, ...baseCards]);
  for (const base of bot.bases) {
    const card = CARD_BY_ID.get(base.cardId);
    if (card) addEffects(output, cardOutput(card, baseFactions, true));
  }

  const extraDraws = Math.min(5, Math.floor(output.draw));
  for (let index = 0; index < extraDraws; index += 1) {
    const cardId = drawOne(bot, random);
    if (!cardId) break;
    const card = CARD_BY_ID.get(cardId);
    if (!card) continue;
    if (card.type === "base") {
      const health = Math.max(1, number(card.health || card.defense || 5));
      bot.bases.push({ cardId: card.id, hp: health, maxHp: health, outpost: card.outpost === true });
      baseIds.push(card.id);
    } else {
      addEffects(output, cardOutput(card, baseFactions));
      playedShips.push(card.id);
    }
  }

  bot.hand = [];
  bot.discard.push(...playedShips);
  bot.authority = Math.min(70, bot.authority + output.heal);
  bot.shield += output.shield;
  return output;
}

function purgeWeakCard(bot, count, random) {
  let remaining = Math.max(0, Math.floor(count));
  while (remaining > 0) {
    const zones = [bot.discard, bot.draw];
    let removed = false;
    for (const zone of zones) {
      const bladeIndex = zone.indexOf("starter_blade");
      const coinIndex = zone.indexOf("starter_coin");
      const index = bladeIndex >= 0 ? bladeIndex : coinIndex;
      if (index >= 0) {
        zone.splice(index, 1);
        removed = true;
        break;
      }
    }
    if (!removed) {
      const zone = bot.discard.length ? bot.discard : bot.draw;
      if (!zone.length) break;
      let weakestIndex = 0;
      let weakestValue = Infinity;
      zone.forEach((cardId, index) => {
        const card = CARD_BY_ID.get(cardId);
        const value = card ? intrinsicCardValue(card) : 0;
        if (value < weakestValue) { weakestValue = value; weakestIndex = index; }
      });
      zone.splice(weakestIndex, 1);
    }
    remaining -= 1;
  }
  if (bot.draw.length > 1 && random() < .02) bot.draw = shuffle(bot.draw, random);
}

function attackBases(attackerCombat, defender) {
  let combat = Math.max(0, attackerCombat);
  if (!combat || !defender.bases.length) return combat;
  const ordered = [...defender.bases].sort((a, b) => {
    if (a.outpost !== b.outpost) return a.outpost ? -1 : 1;
    const aValue = intrinsicCardValue(CARD_BY_ID.get(a.cardId) || {});
    const bValue = intrinsicCardValue(CARD_BY_ID.get(b.cardId) || {});
    return bValue - aValue;
  });
  for (const target of ordered) {
    if (combat <= 0) break;
    if (target.outpost || combat >= target.hp) {
      const damage = Math.min(combat, target.hp);
      target.hp -= damage;
      combat -= damage;
      if (target.hp <= 0) {
        const index = defender.bases.indexOf(target);
        if (index >= 0) defender.bases.splice(index, 1);
      }
      if (target.outpost && target.hp > 0) return 0;
    }
  }
  return combat;
}

function hitAuthority(defender, amount) {
  let damage = Math.max(0, amount);
  if (defender.shield > 0) {
    const absorbed = Math.min(defender.shield, damage);
    defender.shield -= absorbed;
    damage -= absorbed;
  }
  defender.authority -= damage;
}

function refillMarket(market, marketDeck) {
  while (market.length < 5 && marketDeck.length) market.push(marketDeck.pop());
}

function buyCards(bot, trade, market, marketDeck, difficulty, random, purchases) {
  let remainingTrade = Math.max(0, trade);
  let safety = 12;
  while (safety-- > 0) {
    const affordable = market
      .map((cardId, index) => ({ cardId, index, card: CARD_BY_ID.get(cardId) }))
      .filter(entry => entry.card && number(entry.card.cost) <= remainingTrade);
    if (!affordable.length) break;
    affordable.forEach(entry => { entry.score = purchaseScore(entry.card, bot, difficulty, random); });
    affordable.sort((a, b) => b.score - a.score);
    const choice = random() < difficulty.errorRate && affordable.length > 1
      ? affordable[Math.floor(random() * Math.min(3, affordable.length))]
      : affordable[0];
    if (!choice?.card) break;
    remainingTrade -= Math.max(0, number(choice.card.cost));
    bot.discard.push(choice.card.id);
    bot.purchases.push({ cardId: choice.card.id, turn: bot.turn });
    purchases.push({ playerId: bot.id, cardId: choice.card.id, turn: bot.turn });
    if (choice.card.faction && choice.card.faction !== "neutral") bot.factionCounts[choice.card.faction] = (bot.factionCounts[choice.card.faction] || 0) + 1;
    market.splice(choice.index, 1);
    refillMarket(market, marketDeck);
    if (remainingTrade < 1) break;
  }
}

function eraseMarketCards(count, market, marketDeck, bot, difficulty, random) {
  let remaining = Math.min(3, Math.floor(count));
  while (remaining-- > 0 && market.length) {
    let worstIndex = 0;
    let worstScore = Infinity;
    market.forEach((cardId, index) => {
      const card = CARD_BY_ID.get(cardId);
      const score = card ? purchaseScore(card, bot, difficulty, random) : 0;
      if (score < worstScore) { worstScore = score; worstIndex = index; }
    });
    market.splice(worstIndex, 1);
    refillMarket(market, marketDeck);
  }
}

function injectExperimentalCards(pool, experimentalCardIds, copies, random) {
  const valid = [...new Set(experimentalCardIds || [])].filter(id => CARD_BY_ID.has(id));
  if (!valid.length) return pool;
  const result = [...pool];
  for (const cardId of valid) {
    for (let copy = 0; copy < copies; copy += 1) {
      const replaceIndex = Math.floor(random() * Math.max(1, result.length));
      if (result.length) result[replaceIndex] = cardId;
      else result.push(cardId);
    }
  }
  return result;
}

function gameMarketPool(strategyA, strategyB, experimentalCardIds, experimentalCopies, random) {
  const sideA = weightedCommandDeck(strategyA, random);
  const sideB = weightedCommandDeck(strategyB, random);
  const combined = [...sideA, ...sideB];
  return shuffle(injectExperimentalCards(combined, experimentalCardIds, experimentalCopies, random), random);
}

export function simulateTestLabGame(options = {}, gameIndex = 0) {
  const seedBase = Math.max(1, Number(options.seed) || 24681357);
  const random = mulberry32((seedBase + gameIndex * 2654435761) >>> 0);
  const strategyA = resolveStrategy(options.strategyA || "random", random);
  const strategyB = resolveStrategy(options.strategyB || "random", random);
  const difficultyA = DIFFICULTY_BY_ID.get(options.difficultyA || "hard") || DIFFICULTY_BY_ID.get("hard");
  const difficultyB = DIFFICULTY_BY_ID.get(options.difficultyB || "hard") || DIFFICULTY_BY_ID.get("hard");
  const botA = createBot("a", "Bot A", strategyA, difficultyA.id, random);
  const botB = createBot("b", "Bot B", strategyB, difficultyB.id, random);
  const bots = gameIndex % 2 === 0 ? [botA, botB] : [botB, botA];
  const marketDeck = gameMarketPool(strategyA, strategyB, options.experimentalCardIds, clamp(number(options.experimentalCopies) || 2, 1, 4), random);
  const market = [];
  refillMarket(market, marketDeck);
  const purchases = [];
  const maxTurns = clamp(Math.floor(number(options.maxTurns) || 80), 20, 200);
  let totalTurns = 0;
  let winnerId = "";

  while (!winnerId && totalTurns < maxTurns) {
    const bot = bots[totalTurns % 2];
    const enemy = bot.id === botA.id ? botB : botA;
    bot.turn += 1;
    totalTurns += 1;
    drawHand(bot, random, 5);
    const output = deployAndResolveHand(bot, random);
    if (output.purge > 0) purgeWeakCard(bot, output.purge, random);
    if (output.marketErase > 0) eraseMarketCards(output.marketErase, market, marketDeck, bot, DIFFICULTY_BY_ID.get(bot.difficulty), random);
    let combat = output.combat;
    if (output.destroyBase > 0 && enemy.bases.length) {
      const targets = [...enemy.bases].sort((a, b) => intrinsicCardValue(CARD_BY_ID.get(b.cardId) || {}) - intrinsicCardValue(CARD_BY_ID.get(a.cardId) || {}));
      const removes = Math.min(targets.length, Math.floor(output.destroyBase));
      for (let index = 0; index < removes; index += 1) {
        const targetIndex = enemy.bases.indexOf(targets[index]);
        if (targetIndex >= 0) enemy.bases.splice(targetIndex, 1);
      }
    }
    combat = attackBases(combat, enemy);
    hitAuthority(enemy, combat);
    if (enemy.authority <= 0) {
      winnerId = bot.id;
      break;
    }
    buyCards(bot, output.trade, market, marketDeck, DIFFICULTY_BY_ID.get(bot.difficulty), random, purchases);
  }

  return {
    winnerId,
    draw: !winnerId,
    totalTurns,
    rounds: Math.ceil(totalTurns / 2),
    strategies: { a: strategyA, b: strategyB },
    authority: { a: Math.max(0, botA.authority), b: Math.max(0, botB.authority) },
    purchases
  };
}

export function createTestLabAccumulator() {
  return {
    games: 0,
    winsA: 0,
    winsB: 0,
    draws: 0,
    totalTurns: 0,
    startedAt: performance?.now?.() ?? Date.now(),
    cardStats: new Map(),
    strategyStats: new Map()
  };
}

function ensureCardStat(accumulator, cardId) {
  if (!accumulator.cardStats.has(cardId)) {
    accumulator.cardStats.set(cardId, {
      cardId,
      purchases: 0,
      wins: 0,
      losses: 0,
      draws: 0,
      turnTotal: 0,
      winningTurnTotal: 0,
      winningPurchases: 0,
      losingTurnTotal: 0,
      losingPurchases: 0,
      games: new Set()
    });
  }
  return accumulator.cardStats.get(cardId);
}

export function recordTestLabGame(accumulator, result, gameIndex = accumulator.games) {
  accumulator.games += 1;
  accumulator.totalTurns += number(result.totalTurns);
  if (result.winnerId === "a") accumulator.winsA += 1;
  else if (result.winnerId === "b") accumulator.winsB += 1;
  else accumulator.draws += 1;

  const strategyKey = `${result.strategies?.a || "?"} vs ${result.strategies?.b || "?"}`;
  const strategyStat = accumulator.strategyStats.get(strategyKey) || { games: 0, winsA: 0, winsB: 0, draws: 0 };
  strategyStat.games += 1;
  if (result.winnerId === "a") strategyStat.winsA += 1;
  else if (result.winnerId === "b") strategyStat.winsB += 1;
  else strategyStat.draws += 1;
  accumulator.strategyStats.set(strategyKey, strategyStat);

  for (const purchase of result.purchases || []) {
    const stat = ensureCardStat(accumulator, purchase.cardId);
    stat.purchases += 1;
    stat.turnTotal += number(purchase.turn);
    stat.games.add(gameIndex);
    if (!result.winnerId) stat.draws += 1;
    else if (result.winnerId === purchase.playerId) {
      stat.wins += 1;
      stat.winningPurchases += 1;
      stat.winningTurnTotal += number(purchase.turn);
    } else {
      stat.losses += 1;
      stat.losingPurchases += 1;
      stat.losingTurnTotal += number(purchase.turn);
    }
  }
  return accumulator;
}

function rounded(value, digits = 1) {
  const scale = 10 ** digits;
  return Math.round(number(value) * scale) / scale;
}

export function testLabCardRows(accumulator) {
  return [...accumulator.cardStats.values()].map(stat => {
    const card = CARD_BY_ID.get(stat.cardId);
    const decisions = stat.wins + stat.losses;
    return {
      cardId: stat.cardId,
      name: card?.name || stat.cardId,
      faction: card?.faction || "neutral",
      cost: number(card?.cost),
      purchases: stat.purchases,
      gamesPurchased: stat.games.size,
      wins: stat.wins,
      losses: stat.losses,
      draws: stat.draws,
      winRate: decisions ? rounded(stat.wins / decisions * 100, 1) : 0,
      avgBuyTurn: stat.purchases ? rounded(stat.turnTotal / stat.purchases, 2) : 0,
      avgWinningBuyTurn: stat.winningPurchases ? rounded(stat.winningTurnTotal / stat.winningPurchases, 2) : 0,
      avgLosingBuyTurn: stat.losingPurchases ? rounded(stat.losingTurnTotal / stat.losingPurchases, 2) : 0
    };
  });
}

export function testLabSummary(accumulator) {
  const elapsedMs = Math.max(1, (performance?.now?.() ?? Date.now()) - accumulator.startedAt);
  const decided = accumulator.winsA + accumulator.winsB;
  return {
    games: accumulator.games,
    winsA: accumulator.winsA,
    winsB: accumulator.winsB,
    draws: accumulator.draws,
    winRateA: decided ? rounded(accumulator.winsA / decided * 100, 1) : 0,
    winRateB: decided ? rounded(accumulator.winsB / decided * 100, 1) : 0,
    averageTurns: accumulator.games ? rounded(accumulator.totalTurns / accumulator.games, 1) : 0,
    gamesPerSecond: rounded(accumulator.games / elapsedMs * 1000, 1)
  };
}

export async function runTestLabSimulation(options = {}, hooks = {}) {
  const games = clamp(Math.floor(number(options.games) || 1000), 1, 50000);
  const batchSize = clamp(Math.floor(number(options.batchSize) || 25), 1, 250);
  const accumulator = createTestLabAccumulator();
  for (let index = 0; index < games; index += 1) {
    if (hooks.signal?.aborted) break;
    const result = simulateTestLabGame(options, index);
    recordTestLabGame(accumulator, result, index);
    if ((index + 1) % batchSize === 0 || index + 1 === games) {
      hooks.onProgress?.({
        accumulator,
        summary: testLabSummary(accumulator),
        rows: testLabCardRows(accumulator),
        completed: index + 1,
        requested: games
      });
      await new Promise(resolve => setTimeout(resolve, 0));
    }
  }
  return {
    accumulator,
    summary: testLabSummary(accumulator),
    rows: testLabCardRows(accumulator)
  };
}

export function getTestLabCards() {
  return ELIGIBLE_CARDS.map(card => ({
    id: card.id,
    name: card.name,
    faction: card.faction || "neutral",
    cost: number(card.cost),
    type: card.type || "card"
  })).sort((a, b) => a.name.localeCompare(b.name));
}

export function getTestLabCard(cardId) {
  return CARD_BY_ID.get(cardId) || null;
}
