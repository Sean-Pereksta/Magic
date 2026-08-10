import { GAME_EVENT_TYPES } from "./event-system.js";

export const BASE_EVOLUTION_TRIGGERS = Object.freeze([
  "ownerTurnsElapsed",
  "constructionCompleted",
  "chargesReached",
  "damageTaken",
  "repairsReceived",
  "attachmentsReached",
  "friendlyBaseDestroyed",
  "enemyBaseDestroyed"
]);

const supportedTriggers = new Set(BASE_EVOLUTION_TRIGGERS);

function whole(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : fallback;
}

function same(value, other) {
  return String(value || "") === String(other || "");
}

export function isBaseEvolutionTrigger(trigger) {
  return supportedTriggers.has(String(trigger || ""));
}

/**
 * Returns an event-driven progress update for a Base transform. `source` is a
 * small engine-facing descriptor so this module remains independent of the UI.
 */
export function baseEvolutionEventUpdate(transform = {}, source = {}, event = {}, current = 0) {
  const trigger = String(transform.trigger || "");
  const ownerId = String(source.ownerId || "");
  const instanceId = String(source.instanceId || "");
  const progress = whole(current);
  let matched = false;
  let nextProgress = progress;

  if (trigger === "constructionCompleted") {
    matched = event.type === GAME_EVENT_TYPES.BASE_CONSTRUCTION_COMPLETED
      && same(event.ownerId, ownerId)
      && same(event.instanceId, instanceId);
    if (matched) nextProgress = progress + 1;
  } else if (trigger === "damageTaken") {
    const damage = whole(event.amount ?? event.damage);
    matched = event.type === GAME_EVENT_TYPES.BASE_DAMAGED
      && damage > 0
      && same(event.ownerId, ownerId)
      && same(event.instanceId, instanceId);
    if (matched) nextProgress = progress + damage;
  } else if (trigger === "repairsReceived") {
    const repaired = whole(event.amount);
    matched = event.type === GAME_EVENT_TYPES.BASE_REPAIRED
      && repaired > 0
      && same(event.ownerId, ownerId)
      && same(event.instanceId, instanceId);
    if (matched) nextProgress = progress + repaired;
  } else if (trigger === "attachmentsReached") {
    matched = event.type === GAME_EVENT_TYPES.ATTACHMENT_ATTACHED
      && same(event.ownerId, ownerId)
      && same(event.baseInstanceId, instanceId);
    if (matched) nextProgress = whole(source.attachmentCount, progress + 1);
  } else if (trigger === "friendlyBaseDestroyed") {
    matched = event.type === GAME_EVENT_TYPES.BASE_DESTROYED
      && same(event.ownerId, ownerId)
      && !same(event.instanceId, instanceId);
    if (matched) nextProgress = progress + 1;
  } else if (trigger === "enemyBaseDestroyed") {
    matched = event.type === GAME_EVENT_TYPES.BASE_DESTROYED
      && same(event.actorId, ownerId)
      && !same(event.ownerId, ownerId);
    if (matched) nextProgress = progress + 1;
  }

  return {
    matched,
    progress: nextProgress,
    required: Math.max(1, whole(transform.required, 1)),
    trigger
  };
}

export function baseEvolutionChargeUpdate(transform = {}, charges = 0, current = 0) {
  const trigger = String(transform.trigger || "");
  const progress = trigger === "chargesReached" ? whole(charges) : whole(current);
  return {
    matched: trigger === "chargesReached" && progress !== whole(current),
    progress,
    required: Math.max(1, whole(transform.required, 1)),
    trigger
  };
}

export function baseEvolutionIsReady(update = {}) {
  return update.matched === true && whole(update.progress) >= Math.max(1, whole(update.required, 1));
}
