import { GAME_EVENT_TYPES } from "./event-system.js";

export function sacrificeEventPayload(player, entry, card, options = {}) {
  return {
    actorId: String(options.actorId || player?.id || ""),
    playerId: String(player?.id || ""),
    ownerId: String(player?.id || ""),
    cardId: String(card?.id || entry?.id || ""),
    instanceId: String(entry?.instanceId || ""),
    faction: String(card?.faction || ""),
    tags: [...new Set((options.tags || []).map(String).filter(Boolean))],
    token: card?.token === true,
    amount: Math.max(1, Math.floor(Number(options.amount) || 1)),
    method: String(options.method || "sacrifice")
  };
}

export function sacrificeEventTypes(card) {
  return card?.token
    ? [GAME_EVENT_TYPES.CARD_SACRIFICED, GAME_EVENT_TYPES.TOKEN_SACRIFICED]
    : [GAME_EVENT_TYPES.CARD_SACRIFICED];
}

export function emitSacrificeEvents(game, player, entry, card, emit, options = {}) {
  if (typeof emit !== "function") throw new TypeError("A sacrifice event emitter is required.");
  const payload = sacrificeEventPayload(player, entry, card, options);
  return sacrificeEventTypes(card).map(type => emit(game, { type, ...payload }));
}
