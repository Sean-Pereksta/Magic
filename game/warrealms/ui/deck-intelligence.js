const SYSTEM_KEYS = Object.freeze([
  "trade", "combat", "draw", "authority", "shield", "purge", "marketErase", "disable", "raze",
  "sacrificeCards", "sacrificePayoffs", "tokenProducers", "tokenPayoffs", "heatCards", "heatConsumers",
  "transforms", "constructionBases", "constructionSupport", "attachments", "attachmentTargets",
  "chargeProducers", "chargeConsumers", "ally", "doubleAlly"
]);

function number(value) {
  const result = Number(value);
  return Number.isFinite(result) ? result : 0;
}

function clamp(value, minimum = 0, maximum = 10) {
  return Math.max(minimum, Math.min(maximum, value));
}

function nonEmpty(value) {
  return value && typeof value === "object" && Object.keys(value).length > 0;
}

function walk(value, visit, seen = new WeakSet()) {
  if (!value || typeof value !== "object") return;
  if (seen.has(value)) return;
  seen.add(value);
  if (Array.isArray(value)) {
    value.forEach(entry => walk(entry, visit, seen));
    return;
  }
  for (const [key, child] of Object.entries(value)) {
    visit(key, child, value);
    walk(child, visit, seen);
  }
}

function hasAnyKey(card, keys) {
  let found = false;
  const wanted = new Set(keys);
  walk(card, key => {
    if (wanted.has(key)) found = true;
  });
  return found;
}

function sumKeys(card, keys) {
  let total = 0;
  const wanted = new Set(keys);
  walk(card, (key, value) => {
    if (wanted.has(key) && typeof value === "number") total += Math.max(0, value);
  });
  return total;
}

function effectCardCount(cards, keys) {
  return cards.filter(card => hasAnyKey(card, keys)).length;
}

function transformTargets(card) {
  if (Array.isArray(card?.transform?.choose)) return card.transform.choose.length;
  return card?.transform?.into ? 1 : 0;
}

function triggerEvents(card) {
  return [card?.trigger, ...(Array.isArray(card?.triggers) ? card.triggers : [])]
    .filter(Boolean)
    .map(trigger => trigger.event);
}

function countType(cards, type) {
  if (type === "outpost") return cards.filter(card => card.type === "base" && card.outpost).length;
  return cards.filter(card => card.type === type).length;
}

function resolveCards(values, getCard) {
  return (values || []).map(value => typeof value === "object" && value ? value : getCard?.(value)).filter(Boolean);
}

function factionCounts(cards) {
  return cards.reduce((counts, card) => {
    counts[card.faction || "neutral"] = (counts[card.faction || "neutral"] || 0) + 1;
    return counts;
  }, {});
}

function score(value) {
  return Math.round(clamp(value) * 10) / 10;
}

function buildSynergies(metrics, factions, deckSize) {
  const notes = [];
  if (metrics.tokenProducers) notes.push(`${metrics.tokenProducers} cards create Tokens and ${metrics.tokenPayoffs} reward Token play or sacrifice.`);
  if (metrics.heatCards) notes.push(`${metrics.heatCards} Heat cards pair with ${metrics.heatConsumers} Heat-spending or cooling abilities.`);
  if (metrics.sacrificeCards || metrics.sacrificePayoffs) notes.push(`${metrics.sacrificeCards} sacrifice enablers connect to ${metrics.sacrificePayoffs} sacrifice-triggered payoffs.`);
  if (metrics.constructionBases || metrics.constructionSupport) notes.push(`${metrics.constructionBases} Construction Bases have ${metrics.constructionSupport} acceleration or repair supports.`);
  if (metrics.attachments || metrics.attachmentTargets) notes.push(`${metrics.attachmentTargets} potential Base targets support ${metrics.attachments} Attachments.`);
  const orderedFactions = Object.entries(factions).filter(([faction]) => faction !== "neutral").sort((a, b) => b[1] - a[1]);
  const [dominantFaction, dominantCount] = orderedFactions[0] || ["neutral", 0];
  if (metrics.ally || metrics.doubleAlly) notes.push(`${dominantCount} ${dominantFaction} cards support ${metrics.ally} Ally and ${metrics.doubleAlly} Double Ally cards.`);
  if (deckSize >= 20 && metrics.trade < Math.max(4, Math.floor(deckSize * .1))) notes.push(`Only ${metrics.trade} Trade sources may make the high end of the curve slower to reach.`);
  if (!notes.length) notes.push("No concentrated engine is visible yet; the current list reads as a flexible hybrid shell.");
  return notes;
}

function archetypes(metrics, factions, scores) {
  const tags = [];
  const dominant = Object.entries(factions).filter(([faction]) => faction !== "neutral").sort((a, b) => b[1] - a[1])[0]?.[0];
  const deckSize = Object.values(factions).reduce((sum, count) => sum + number(count), 0);
  if (dominant === "green" && (scores.combat >= 5 || metrics.combat >= Math.max(2, deckSize * .3))) tags.push("Gorak Siege");
  if (dominant === "yellow" && (metrics.disable + metrics.marketErase) >= 3) tags.push("Xythe Control");
  if (dominant === "blue" && scores.base >= 4) tags.push("Azure Fortress");
  if (dominant === "red" && scores.sacrifice >= 4) tags.push("Umbral Sacrifice");
  if (scores.heat >= 4) tags.push("Heat Engine");
  if (scores.token >= 4) tags.push("Token Swarm");
  if (metrics.constructionBases >= 2) tags.push("Construction Engine");
  if (metrics.transforms >= 3) tags.push("Transformation Deck");
  if (!tags.length || (tags.length === 1 && Object.keys(factions).filter(faction => faction !== "neutral" && factions[faction] > 0).length >= 3)) tags.push("Hybrid");
  return [...new Set(tags)];
}

export function analyzeDeck(values = [], options = {}) {
  const cards = resolveCards(values, options.getCard);
  const deckSize = cards.length;
  const costs = cards.map(card => Math.max(0, number(card.cost)));
  const curve = { low: 0, mid: 0, high: 0, apex: 0 };
  costs.forEach(cost => {
    curve[cost <= 2 ? "low" : cost <= 4 ? "mid" : cost <= 6 ? "high" : "apex"] += 1;
  });
  const events = cards.map(triggerEvents);
  const metrics = {
    trade: effectCardCount(cards, ["trade", "tradePerBase"]),
    combat: effectCardCount(cards, ["combat", "combatPerBase", "combatAgainstBases"]),
    draw: effectCardCount(cards, ["draw", "drawPerBase", "drawFromDrawPile"]),
    authority: effectCardCount(cards, ["heal", "authority", "healPerBase"]),
    shield: effectCardCount(cards, ["shield"]),
    purge: effectCardCount(cards, ["scrapOwn", "purge", "purgeAndDraw"]),
    marketErase: effectCardCount(cards, ["scrapMarket", "marketErase"]),
    disable: effectCardCount(cards, ["stun", "disable"]),
    raze: cards.filter(card => hasAnyKey(card, ["destroyBase", "damageAll", "combatAgainstBases"]) || /\braze\b/i.test(card.text || "")).length,
    sacrificeCards: cards.filter(card => nonEmpty(card.sacrifice) || hasAnyKey(card, ["sacrificeRequired", "optionalTokenSacrificeChain"])).length,
    sacrificePayoffs: cards.filter((card, index) => events[index].some(event => ["CARD_SACRIFICED", "TOKEN_SACRIFICED"].includes(event)) || card.sacrificeTrigger || card.tokenSacrificeTrigger || card.sacrificeThresholds || card.tokenSacrificeThresholds).length,
    tokenProducers: effectCardCount(cards, ["createToken"]),
    tokenPayoffs: cards.filter((card, index) => events[index].some(event => ["TOKEN_CREATED", "TOKEN_PLAYED", "TOKEN_SACRIFICED"].includes(event)) || card.tokenSacrificeTrigger || card.tokenSacrificeThresholds || hasAnyKey(card, ["optionalTokenSacrificeChain", "tokenCombo"])).length,
    heatCards: cards.filter(card => card.heat || card.transform?.trigger === "heat" || hasAnyKey(card, ["addHeat", "harvestHeat", "moveHeat", "coolHeat"])).length,
    heatConsumers: cards.filter(card => card.heat?.actions?.length || card.heat?.action || hasAnyKey(card, ["harvestHeat", "moveHeat", "coolHeat"])).length,
    transforms: cards.filter(card => transformTargets(card) > 0).length,
    constructionBases: cards.filter(card => card.type === "base" && (card.expansion || number(card.construction) > 0)).length,
    constructionSupport: effectCardCount(cards, ["advanceConstruction", "repair"]),
    attachments: countType(cards, "attachment"),
    attachmentTargets: cards.filter(card => card.type === "base" && number(card.attachmentSlots ?? card.attachmentCapacity ?? (card.expansion ? 2 : 1)) > 0).length,
    chargeProducers: cards.filter(card => card.charge?.trigger || card.charge?.triggers || hasAnyKey(card, ["gainCharge", "charges"])).length,
    chargeConsumers: cards.filter(card => Array.isArray(card.charge?.actions) && card.charge.actions.length > 0).length,
    ally: cards.filter(card => nonEmpty(card.ally)).length,
    doubleAlly: cards.filter(card => nonEmpty(card.doubleAlly || card.double_ally || card.ally2)).length
  };
  for (const key of SYSTEM_KEYS) metrics[key] = Math.max(0, number(metrics[key]));

  const factions = factionCounts(cards);
  const dominantFactionCount = Math.max(0, ...Object.entries(factions).filter(([faction]) => faction !== "neutral").map(([, count]) => count), 0);
  const sourceTotal = key => cards.reduce((total, card) => total + sumKeys(card, key), 0);
  const scorecards = {
    economy: score(metrics.trade * .65 + sourceTotal(["trade", "tradePerBase"]) * .18),
    combat: score(metrics.combat * .55 + sourceTotal(["combat", "combatPerBase"]) * .12),
    cardFlow: score(metrics.draw * .8 + metrics.purge * .45),
    faction: score((deckSize ? dominantFactionCount / deckSize : 0) * 7 + metrics.ally * .16 + metrics.doubleAlly * .28),
    sacrifice: score(metrics.sacrificeCards * .55 + metrics.sacrificePayoffs * .8),
    base: score((countType(cards, "base") + countType(cards, "attachment")) * .4 + metrics.constructionSupport * .6 + metrics.chargeConsumers * .45),
    token: score(metrics.tokenProducers * .65 + metrics.tokenPayoffs * .9),
    heat: score(metrics.heatCards * .45 + metrics.heatConsumers * .7)
  };

  return {
    cards,
    deckSize,
    averageBattleCost: deckSize ? costs.reduce((sum, cost) => sum + cost, 0) / deckSize : 0,
    curve,
    types: {
      ships: countType(cards, "ship"),
      bases: countType(cards, "base"),
      outposts: countType(cards, "outpost"),
      attachments: countType(cards, "attachment")
    },
    factions,
    metrics,
    scorecards,
    synergies: buildSynergies(metrics, factions, deckSize),
    archetypes: archetypes(metrics, factions, scorecards)
  };
}

function seededRandom(seed = 1) {
  let state = Math.max(1, Math.floor(number(seed)) || 1) >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 4294967296;
  };
}

function shuffle(values, random) {
  const result = [...values];
  for (let index = result.length - 1; index > 0; index -= 1) {
    const other = Math.floor(random() * (index + 1));
    [result[index], result[other]] = [result[other], result[index]];
  }
  return result;
}

function cardDrawValue(card) {
  return Math.min(3, Math.floor(sumKeys(card, ["draw", "drawPerBase"])));
}

function cardTokenValue(card) {
  let total = 0;
  walk(card, (key, value) => {
    if (key !== "createToken" || !value || typeof value !== "object") return;
    total += Math.max(1, number(value.count) || 1);
  });
  return total;
}

export function simulateDeck(values = [], options = {}) {
  const cards = resolveCards(values, options.getCard);
  const iterations = Math.max(100, Math.min(5000, Math.floor(number(options.iterations) || 750)));
  if (!cards.length) return {
    iterations,
    averageOpeningTrade: 0,
    averageOpeningCombat: 0,
    chanceFourTradeByTurnTwo: 0,
    chanceSixTradeByTurnFour: 0,
    expectedFirstReshuffleTurn: 0,
    allyActivationRate: 0,
    doubleAllyActivationRate: 0,
    averageCardsDrawnPerTurn: 0,
    averagePurgesBeforeReshuffle: 0,
    estimatedHeatGeneration: 0,
    estimatedTokenGeneration: 0,
    estimatedBaseDensity: 0
  };
  const random = seededRandom(options.seed ?? cards.reduce((seed, card) => seed + String(card.id || card.name || "").split("").reduce((sum, char) => sum + char.charCodeAt(0), 0), 17));
  const totals = {
    openingTrade: 0,
    openingCombat: 0,
    fourTradeTurnTwo: 0,
    sixTradeTurnFour: 0,
    reshuffleTurn: 0,
    allyCards: 0,
    allyHits: 0,
    doubleCards: 0,
    doubleHits: 0,
    cardsDrawn: 0,
    turns: 0,
    purges: 0,
    heat: 0,
    tokens: 0,
    bases: 0
  };

  for (let iteration = 0; iteration < iterations; iteration += 1) {
    let drawPile = shuffle(cards, random);
    let discard = [];
    let reshuffleTurn = 0;
    let cumulativeTrade = 0;
    const drawCards = count => {
      const hand = [];
      for (let index = 0; index < count; index += 1) {
        if (!drawPile.length && discard.length) {
          drawPile = shuffle(discard, random);
          discard = [];
        }
        if (!drawPile.length) break;
        hand.push(drawPile.pop());
      }
      return hand;
    };

    for (let turn = 1; turn <= 12; turn += 1) {
      if (!reshuffleTurn && drawPile.length < 5 && discard.length) reshuffleTurn = turn;
      const hand = drawCards(5);
      const extraDraws = Math.min(5, hand.reduce((sum, card) => sum + cardDrawValue(card), 0));
      hand.push(...drawCards(extraDraws));
      const trade = hand.reduce((sum, card) => sum + sumKeys(card, ["trade", "tradePerBase"]), 0);
      const combat = hand.reduce((sum, card) => sum + sumKeys(card, ["combat", "combatPerBase"]), 0);
      if (turn === 1) {
        totals.openingTrade += trade;
        totals.openingCombat += combat;
      }
      cumulativeTrade += trade;
      if (turn === 2 && cumulativeTrade >= 4) totals.fourTradeTurnTwo += 1;
      if (turn === 4 && cumulativeTrade >= 6) totals.sixTradeTurnFour += 1;

      const factionInHand = factionCounts(hand);
      for (const card of hand) {
        const sameFaction = Math.max(0, number(factionInHand[card.faction]) - 1);
        if (nonEmpty(card.ally)) {
          totals.allyCards += 1;
          if (sameFaction >= 1) totals.allyHits += 1;
        }
        if (nonEmpty(card.doubleAlly || card.double_ally || card.ally2)) {
          totals.doubleCards += 1;
          if (sameFaction >= 2) totals.doubleHits += 1;
        }
      }
      totals.cardsDrawn += hand.length;
      totals.turns += 1;
      totals.purges += hand.filter(card => hasAnyKey(card, ["scrapOwn", "purge", "purgeAndDraw"])).length;
      totals.heat += hand.reduce((sum, card) => sum + sumKeys(card, ["addHeat"]), 0) + hand.filter(card => card.heat?.onPlay).length;
      totals.tokens += hand.reduce((sum, card) => sum + cardTokenValue(card), 0);
      totals.bases += hand.filter(card => card.type === "base").length;
      discard.push(...hand);
      if (reshuffleTurn && turn >= 4) break;
    }
    totals.reshuffleTurn += reshuffleTurn || 12;
  }

  const percent = value => Math.round(value / iterations * 1000) / 10;
  const average = value => Math.round(value / iterations * 100) / 100;
  return {
    iterations,
    averageOpeningTrade: average(totals.openingTrade),
    averageOpeningCombat: average(totals.openingCombat),
    chanceFourTradeByTurnTwo: percent(totals.fourTradeTurnTwo),
    chanceSixTradeByTurnFour: percent(totals.sixTradeTurnFour),
    expectedFirstReshuffleTurn: average(totals.reshuffleTurn),
    allyActivationRate: totals.allyCards ? Math.round(totals.allyHits / totals.allyCards * 1000) / 10 : 0,
    doubleAllyActivationRate: totals.doubleCards ? Math.round(totals.doubleHits / totals.doubleCards * 1000) / 10 : 0,
    averageCardsDrawnPerTurn: totals.turns ? Math.round(totals.cardsDrawn / totals.turns * 100) / 100 : 0,
    averagePurgesBeforeReshuffle: average(totals.purges),
    estimatedHeatGeneration: average(totals.heat),
    estimatedTokenGeneration: average(totals.tokens),
    estimatedBaseDensity: totals.cardsDrawn ? Math.round(totals.bases / totals.cardsDrawn * 1000) / 10 : 0
  };
}
