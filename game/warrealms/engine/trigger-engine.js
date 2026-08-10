import { GAME_EVENT_TYPES } from "./event-system.js";

export function normalizePersistentCardTriggers(card) {
  if (!card) return [];
  const triggers = [];
  if (card.trigger && typeof card.trigger === "object") triggers.push({ ...card.trigger, schema: "trigger" });
  if (Array.isArray(card.triggers)) triggers.push(...card.triggers.filter(Boolean).map(trigger => ({ ...trigger, schema: "triggers" })));
  if (card.sacrificeTrigger?.effect) {
    triggers.push({
      event: GAME_EVENT_TYPES.CARD_SACRIFICED,
      condition: { cardId: card.sacrificeTrigger.sacrificedId || "" },
      effect: card.sacrificeTrigger.effect,
      oncePerTurn: card.sacrificeTrigger.oncePerTurn === true,
      perTurnCap: card.sacrificeTrigger.perTurnCap,
      schema: "sacrificeTrigger"
    });
  }
  if (card.tokenSacrificeTrigger?.effect) {
    triggers.push({
      event: GAME_EVENT_TYPES.TOKEN_SACRIFICED,
      condition: { cardId: card.tokenSacrificeTrigger.tokenId || "", token: true },
      effect: card.tokenSacrificeTrigger.effect,
      ownedFactionBonus: card.tokenSacrificeTrigger.ownedFactionBonus,
      oncePerTurn: card.tokenSacrificeTrigger.oncePerTurn === true,
      perTurnCap: card.tokenSacrificeTrigger.perTurnCap,
      schema: "tokenSacrificeTrigger"
    });
  }
  if (card.heatAura?.effect && card.heatAura.trigger === "shipReachesHeat") {
    triggers.push({
      event: GAME_EVENT_TYPES.HEAT_GAINED,
      condition: { reachesHeat: Math.max(1, Number(card.heatAura.at) || 1) },
      effect: card.heatAura.effect,
      oncePerTurn: card.heatAura.firstTimeEachTurn === true,
      schema: "heatAura"
    });
  }
  return triggers;
}

export function eventMatchesPersistentTrigger(event, condition = {}, sourceEntry = null) {
  if (!condition || typeof condition !== "object") return true;
  if (condition.cardId && String(condition.cardId) !== String(event.cardId || "")) return false;
  if (condition.sacrificedId && String(condition.sacrificedId) !== String(event.cardId || "")) return false;
  if (condition.faction && String(condition.faction) !== String(event.faction || "")) return false;
  if (condition.method && String(condition.method) !== String(event.method || "")) return false;
  if (condition.token !== undefined && Boolean(condition.token) !== Boolean(event.token)) return false;
  if (condition.tag && !(event.tags || []).includes(condition.tag)) return false;
  if (Array.isArray(condition.tags) && !condition.tags.every(tag => (event.tags || []).includes(tag))) return false;
  if (Number.isFinite(Number(condition.minimumAmount)) && Number(event.amount || 0) < Number(condition.minimumAmount)) return false;
  if (Number.isFinite(Number(condition.reachesHeat))) {
    const threshold = Number(condition.reachesHeat);
    if (!(Number(event.before) < threshold && Number(event.after) >= threshold)) return false;
  }
  if (condition.sourceNotSelf && String(event.instanceId || "") === String(sourceEntry?.instanceId || "")) return false;
  return true;
}

export function persistentTriggerScopeMatches(trigger, sourceOwnerId, event) {
  const eventPlayerId = String(event.playerId || event.actorId || event.ownerId || "");
  const scope = String(trigger?.scope || "self");
  if (scope === "any") return true;
  if (scope === "opponent") return String(sourceOwnerId || "") !== eventPlayerId;
  return String(sourceOwnerId || "") === eventPlayerId;
}

export function consumePersistentTriggerAllowance(game, entry, trigger, triggerKey) {
  entry.triggerUsage = entry.triggerUsage && typeof entry.triggerUsage === "object" ? entry.triggerUsage : {};
  const usage = entry.triggerUsage[triggerKey] && typeof entry.triggerUsage[triggerKey] === "object"
    ? entry.triggerUsage[triggerKey]
    : { turnSerial: 0, turnCount: 0, round: 0, gameCount: 0 };
  if (usage.turnSerial !== game.turnSerial) {
    usage.turnSerial = game.turnSerial;
    usage.turnCount = 0;
  }
  const perTurnCap = trigger.oncePerTurn
    ? 1
    : Number.isFinite(Number(trigger.perTurnCap))
      ? Math.max(0, Math.floor(Number(trigger.perTurnCap)))
      : Infinity;
  if (usage.turnCount >= perTurnCap) return false;
  if (trigger.oncePerRound && usage.round === game.round) return false;
  if (trigger.oncePerGame && usage.gameCount > 0) return false;
  usage.turnCount += 1;
  usage.round = game.round;
  usage.gameCount += 1;
  entry.triggerUsage[triggerKey] = usage;
  return true;
}

export function matchingPersistentTriggers(game, event, sources, getCard, claim = () => true) {
  const matches = [];
  for (const source of sources || []) {
    const card = getCard(source.entry);
    for (const [index, trigger] of normalizePersistentCardTriggers(card).entries()) {
      if (trigger.event !== event.type) continue;
      if (!persistentTriggerScopeMatches(trigger, source.owner?.id, event)) continue;
      if (!eventMatchesPersistentTrigger(event, trigger.condition || {}, source.entry)) continue;
      const triggerKey = `${source.entry.instanceId}:${trigger.schema || "trigger"}:${index}`;
      if (!claim(triggerKey)) continue;
      if (!consumePersistentTriggerAllowance(game, source.entry, trigger, triggerKey)) continue;
      matches.push({ source, card, trigger, triggerKey });
    }
  }
  return matches;
}
