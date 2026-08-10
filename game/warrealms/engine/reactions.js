import { eventMatchesPersistentTrigger } from "./trigger-engine.js";

export function normalizeCardReactions(card) {
  return [card?.reaction, ...(Array.isArray(card?.reactions) ? card.reactions : [])]
    .filter(reaction => reaction && typeof reaction === "object")
    .map((reaction, index) => ({ ...reaction, reactionIndex: index }));
}

function reactionScopeMatches(reaction, sourceOwnerId, event) {
  const scope = String(reaction.scope || "owner");
  const actorId = String(event.actorId || event.playerId || "");
  const ownerId = String(event.ownerId || event.targetId || event.playerId || "");
  if (scope === "any") return true;
  if (scope === "actor" || scope === "self") return String(sourceOwnerId) === actorId;
  if (scope === "opponent") return String(sourceOwnerId) !== actorId;
  return String(sourceOwnerId) === ownerId;
}

function consumeReactionAllowance(game, entry, reaction, key) {
  entry.reactionUsage = entry.reactionUsage && typeof entry.reactionUsage === "object" ? entry.reactionUsage : {};
  const usage = entry.reactionUsage[key] && typeof entry.reactionUsage[key] === "object"
    ? entry.reactionUsage[key]
    : { turnSerial: 0, turnCount: 0, round: 0, gameCount: 0 };
  if (usage.turnSerial !== game.turnSerial) {
    usage.turnSerial = game.turnSerial;
    usage.turnCount = 0;
  }
  const cap = reaction.oncePerTurn
    ? 1
    : Number.isFinite(Number(reaction.perTurnCap))
      ? Math.max(0, Math.floor(Number(reaction.perTurnCap)))
      : Infinity;
  if (usage.turnCount >= cap) return false;
  if (reaction.oncePerRound && usage.round === game.round) return false;
  if (reaction.oncePerGame && usage.gameCount > 0) return false;
  usage.turnCount += 1;
  usage.round = game.round;
  usage.gameCount += 1;
  entry.reactionUsage[key] = usage;
  return true;
}

export function matchingReactions(game, event, sources, getCard, claim = () => true) {
  const matches = [];
  for (const source of sources || []) {
    const card = getCard(source.entry);
    for (const reaction of normalizeCardReactions(card)) {
      if (reaction.event !== event.type) continue;
      if (!reactionScopeMatches(reaction, source.owner?.id, event)) continue;
      if (!eventMatchesPersistentTrigger(event, reaction.condition || {}, source.entry)) continue;
      const key = `${source.entry.instanceId}:reaction:${reaction.reactionIndex}`;
      if (!claim(key) || !consumeReactionAllowance(game, source.entry, reaction, key)) continue;
      matches.push({ source, card, reaction, reactionKey: key });
    }
  }
  return matches;
}
