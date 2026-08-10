import { activeResolutionItem, enqueueResolution } from "./resolution-queue.js";

export const RESOLUTION_KINDS = Object.freeze({
  EFFECT: "EFFECT",
  GAME_EVENT: "GAME_EVENT"
});

export function queueEffectResolution(game, playerId, effect = {}, context = {}, options = {}) {
  if (!game || !playerId || !effect || typeof effect !== "object") return null;
  const active = activeResolutionItem(game);
  const parentEvent = active?.kind === RESOLUTION_KINDS.GAME_EVENT
    ? active.event
    : active?.parentEvent || null;
  return enqueueResolution(game, {
    kind: RESOLUTION_KINDS.EFFECT,
    playerId: String(playerId),
    effect,
    context,
    source: String(context.source || "queued-effect"),
    parentEvent
  }, options);
}

export function isEffectResolution(item) {
  return item?.kind === RESOLUTION_KINDS.EFFECT && !!item.playerId && item.effect && typeof item.effect === "object";
}
