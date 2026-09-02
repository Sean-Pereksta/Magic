export const BATTLE_TELEMETRY_VERSION = 1;

const COUNTER_KEYS = Object.freeze([
  "turnsStarted",
  "cardsPlayed",
  "cardsPurchased",
  "cardsDrawn",
  "tradeGenerated",
  "combatGenerated",
  "authorityGained",
  "authorityDamage",
  "shieldGenerated",
  "basesConstructed",
  "basesDamaged",
  "basesDestroyed",
  "baseDamage",
  "tokensCreated",
  "cardsSacrificed",
  "heatGenerated",
  "heatSpent",
  "allyActivations",
  "doubleAllyActivations",
  "combatSpentOnBases",
  "combatSpentOnAuthority"
]);

function whole(value) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : 0;
}

function counters(source = {}) {
  return Object.fromEntries(COUNTER_KEYS.map(key => [key, whole(source[key])]));
}

function object(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

export function ensureBattleTelemetry(game) {
  if (!game || typeof game !== "object") return null;
  const existing = object(game.telemetry);
  game.telemetry = {
    version: BATTLE_TELEMETRY_VERSION,
    startedAtMs: whole(existing.startedAtMs) || whole(game.createdAtMs) || Date.now(),
    players: object(existing.players)
  };
  for (const [playerId, raw] of Object.entries(game.telemetry.players)) {
    game.telemetry.players[playerId] = {
      ...counters(raw),
      cardContributions: object(raw?.cardContributions)
    };
  }
  return game.telemetry;
}

export function resetBattleTelemetry(game) {
  if (!game || typeof game !== "object") return null;
  game.telemetry = { version: BATTLE_TELEMETRY_VERSION, startedAtMs: Date.now(), players: {} };
  return game.telemetry;
}

export function playerBattleTelemetry(game, playerId) {
  const telemetry = ensureBattleTelemetry(game);
  if (!telemetry || !playerId) return null;
  const id = String(playerId);
  const raw = object(telemetry.players[id]);
  telemetry.players[id] = {
    ...counters(raw),
    cardContributions: object(raw.cardContributions)
  };
  return telemetry.players[id];
}

export function recordBattleMetrics(game, playerId, changes = {}, context = {}) {
  const player = playerBattleTelemetry(game, playerId);
  if (!player) return null;
  const applied = {};
  for (const key of COUNTER_KEYS) {
    const amount = whole(changes[key]);
    if (!amount) continue;
    player[key] += amount;
    applied[key] = amount;
  }
  const cardId = String(context.cardId || "");
  if (cardId && Object.keys(applied).length) {
    const existing = object(player.cardContributions[cardId]);
    const contribution = { ...counters(existing), triggers: whole(existing.triggers) + whole(context.triggers) };
    for (const [key, amount] of Object.entries(applied)) contribution[key] += amount;
    if (!whole(context.triggers)) contribution.triggers += 1;
    player.cardContributions[cardId] = contribution;
  }
  return player;
}

export function recordBattleEvent(game, event = {}) {
  const actorId = String(event.actorId || event.playerId || "");
  const amount = Math.max(1, whole(event.amount) || 1);
  const sourceCardId = String(event.sourceCardId || "");
  const context = { cardId: sourceCardId, triggers: 1 };
  const changes = {};
  switch (event.type) {
    case "TURN_STARTED": changes.turnsStarted = 1; break;
    case "CARD_PLAYED": changes.cardsPlayed = 1; context.cardId = String(event.cardId || ""); break;
    case "CARD_ACQUIRED": changes.cardsPurchased = 1; context.cardId = String(event.cardId || ""); break;
    case "CARD_DRAWN": changes.cardsDrawn = amount; break;
    case "AUTHORITY_GAINED": changes.authorityGained = amount; context.cardId = sourceCardId || String(event.cardId || ""); break;
    case "AUTHORITY_LOST":
      if (actorId && actorId !== String(event.playerId || event.ownerId || "")) changes.authorityDamage = amount;
      break;
    case "SHIELD_GAINED": changes.shieldGenerated = amount; context.cardId = sourceCardId || String(event.cardId || ""); break;
    case "BASE_PLAYED": changes.basesConstructed = 1; context.cardId = String(event.cardId || ""); break;
    case "BASE_DAMAGED": changes.basesDamaged = 1; changes.baseDamage = whole(event.damage) || amount; break;
    case "BASE_DESTROYED": changes.basesDestroyed = 1; break;
    case "TOKEN_CREATED": changes.tokensCreated = amount; break;
    case "CARD_SACRIFICED": changes.cardsSacrificed = amount; break;
    case "HEAT_GAINED": changes.heatGenerated = amount; context.cardId = sourceCardId || String(event.cardId || ""); break;
    case "HEAT_SPENT": changes.heatSpent = amount; context.cardId = sourceCardId || String(event.cardId || ""); break;
    case "ALLY_TRIGGERED": changes.allyActivations = 1; context.cardId = String(event.cardId || ""); break;
    case "DOUBLE_ALLY_TRIGGERED": changes.doubleAllyActivations = 1; context.cardId = String(event.cardId || ""); break;
    default: return null;
  }
  if (!Object.keys(changes).length) return null;
  return recordBattleMetrics(game, actorId, changes, context);
}

function contributionScore(item) {
  return item.tradeGenerated * 1.1
    + item.combatGenerated
    + item.authorityGained * .65
    + item.shieldGenerated * .55
    + item.cardsDrawn * 2.8
    + item.tokensCreated * 1.4
    + item.baseDamage * .75
    + item.basesDestroyed * 4
    + item.allyActivations * 1.5
    + item.doubleAllyActivations * 2.5
    + item.triggers * .2;
}

function topCardContribution(player, getCard) {
  const ranked = Object.entries(player.cardContributions || {}).map(([cardId, raw]) => {
    const item = { cardId, ...counters(raw), triggers: whole(raw.triggers) };
    return { ...item, score: contributionScore(item) };
  }).sort((left, right) => right.score - left.score || right.triggers - left.triggers);
  const top = ranked[0];
  if (!top || top.score <= 0) return null;
  const generated = [];
  if (top.combatGenerated) generated.push(`${top.combatGenerated} Combat`);
  if (top.tradeGenerated) generated.push(`${top.tradeGenerated} Trade`);
  if (top.cardsDrawn) generated.push(`${top.cardsDrawn} cards drawn`);
  if (top.shieldGenerated) generated.push(`${top.shieldGenerated} Shield`);
  if (top.authorityGained) generated.push(`${top.authorityGained} Authority`);
  if (top.tokensCreated) generated.push(`${top.tokensCreated} Tokens`);
  if (top.baseDamage) generated.push(`${top.baseDamage} Base damage`);
  return {
    ...top,
    name: getCard?.(top.cardId)?.name || top.cardId,
    generated: generated.slice(0, 3)
  };
}

function battleObservations(player, analysis = {}) {
  const observations = [];
  const turns = Math.max(1, player.turnsStarted);
  if (player.tradeGenerated / turns < 3 && turns >= 3) {
    observations.push({ tone: "warning", title: "Your economy struggled early.", text: `Only ${(player.tradeGenerated / turns).toFixed(1)} Trade was generated per turn.` });
  }
  const spentCombat = player.combatSpentOnBases + player.combatSpentOnAuthority;
  if (player.combatSpentOnBases >= 10 && player.combatSpentOnBases / Math.max(1, spentCombat) >= .55) {
    observations.push({ tone: "strength", title: "Your siege engine dominated.", text: `${Math.round(player.combatSpentOnBases / Math.max(1, spentCombat) * 100)}% of committed Combat was directed at Bases.` });
  }
  if (player.tokensCreated >= 6) {
    observations.push({ tone: "strength", title: "Your Token engine carried momentum.", text: `${player.tokensCreated} Tokens entered play during the battle.` });
  }
  const allyCards = whole(analysis?.metrics?.ally) + whole(analysis?.metrics?.doubleAlly);
  if (allyCards >= 4 && player.allyActivations + player.doubleAllyActivations < Math.max(2, turns / 2)) {
    observations.push({ tone: "warning", title: "Your Ally engine was inconsistent.", text: `Only ${player.allyActivations + player.doubleAllyActivations} Ally effects triggered across ${turns} turns.` });
  }
  if (player.shieldGenerated + player.authorityGained >= 18) {
    observations.push({ tone: "strength", title: "Your defenses sustained the campaign.", text: `${player.shieldGenerated} Shield and ${player.authorityGained} Authority were generated.` });
  }
  if (!observations.length) observations.push({ tone: "neutral", title: "Your plan stayed balanced.", text: "No single engine overwhelmed the rest of the deck in this battle." });
  return observations.slice(0, 2);
}

export function buildBattleReport(game, playerId, options = {}) {
  const player = playerBattleTelemetry(game, playerId) || { ...counters(), cardContributions: {} };
  const seat = (game?.players || []).find(candidate => candidate.id === playerId) || {};
  const report = {
    version: BATTLE_TELEMETRY_VERSION,
    battleId: String(game?.id || game?.campaign?.nodeId || game?.createdAtMs || "battle"),
    createdAtMs: Date.now(),
    won: game?.winnerId === playerId,
    winnerName: String(game?.winnerName || ""),
    enemyName: String((game?.players || []).find(candidate => candidate.id !== playerId)?.name || "Enemy Commander"),
    turns: player.turnsStarted,
    authorityRemaining: whole(seat.health),
    maximumAuthority: Math.max(1, whole(seat.maxAuthority) || 1),
    metrics: counters(player),
    mvp: topCardContribution(player, options.getCard),
    observations: battleObservations(player, options.analysis)
  };
  return report;
}
