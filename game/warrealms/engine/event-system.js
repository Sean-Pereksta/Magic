import {
  DEFAULT_MAX_RESOLUTION_DEPTH,
  DEFAULT_MAX_RESOLUTION_STEPS,
  activeResolutionItem,
  drainResolutionQueue,
  enqueueResolution,
  ensureResolutionState
} from "./resolution-queue.js";

export const GAME_EVENT_TYPES = Object.freeze({
  CARD_PLAYED: "CARD_PLAYED",
  CARD_ACQUIRED: "CARD_ACQUIRED",
  CARD_DRAWN: "CARD_DRAWN",
  CARD_DISCARDED: "CARD_DISCARDED",
  CARD_PURGED: "CARD_PURGED",
  CARD_SACRIFICED: "CARD_SACRIFICED",
  CARD_DESTROYED: "CARD_DESTROYED",
  CARD_RESURRECTED: "CARD_RESURRECTED",
  TOKEN_CREATED: "TOKEN_CREATED",
  TOKEN_PLAYED: "TOKEN_PLAYED",
  TOKEN_SACRIFICED: "TOKEN_SACRIFICED",
  BASE_PLAYED: "BASE_PLAYED",
  BASE_DAMAGED: "BASE_DAMAGED",
  BASE_REPAIRED: "BASE_REPAIRED",
  BASE_WOULD_BE_DESTROYED: "BASE_WOULD_BE_DESTROYED",
  BASE_DESTROYED: "BASE_DESTROYED",
  BASE_CONSTRUCTION_ADVANCED: "BASE_CONSTRUCTION_ADVANCED",
  BASE_CONSTRUCTION_COMPLETED: "BASE_CONSTRUCTION_COMPLETED",
  ATTACHMENT_ATTACHED: "ATTACHMENT_ATTACHED",
  ATTACHMENT_REMOVED: "ATTACHMENT_REMOVED",
  HEAT_GAINED: "HEAT_GAINED",
  HEAT_SPENT: "HEAT_SPENT",
  HEAT_OVERLOADED: "HEAT_OVERLOADED",
  CARD_TRANSFORMED: "CARD_TRANSFORMED",
  AUTHORITY_GAINED: "AUTHORITY_GAINED",
  AUTHORITY_LOST: "AUTHORITY_LOST",
  SHIELD_GAINED: "SHIELD_GAINED",
  TURN_STARTED: "TURN_STARTED",
  TURN_ENDED: "TURN_ENDED",
  ALLY_TRIGGERED: "ALLY_TRIGGERED",
  DOUBLE_ALLY_TRIGGERED: "DOUBLE_ALLY_TRIGGERED"
});

export const SUPPORTED_GAME_EVENTS = Object.freeze(Object.values(GAME_EVENT_TYPES));
export const MAX_TRIGGER_DEPTH = DEFAULT_MAX_RESOLUTION_DEPTH;
export const MAX_TRIGGER_STEPS = DEFAULT_MAX_RESOLUTION_STEPS;

const supportedEvents = new Set(SUPPORTED_GAME_EVENTS);
const PRESENTATION_EVENT_NAME = "warrealms:game-event";

function nonNegativeInteger(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : fallback;
}

export function ensureGameEventState(game) {
  ensureResolutionState(game);
  game.eventSequence = nonNegativeInteger(game.eventSequence);
  game.eventHistory = Array.isArray(game.eventHistory)
    ? game.eventHistory.filter(event => event && typeof event === "object").slice(-80)
    : [];
  game.eventWarnings = Array.isArray(game.eventWarnings)
    ? game.eventWarnings.map(String).slice(-24)
    : [];
  return game;
}

function parentEventFor(game, explicitParent) {
  if (explicitParent?.type) return explicitParent;
  const active = activeResolutionItem(game);
  if (active?.kind === "GAME_EVENT") return active.event || null;
  return active?.parentEvent?.type ? active.parentEvent : null;
}

export function createGameEvent(game, input = {}, explicitParent = null) {
  ensureGameEventState(game);
  const type = String(input.type || "").trim();
  if (!supportedEvents.has(type)) {
    throw new TypeError(`Unsupported War Realms game event: ${type || "(missing)"}`);
  }
  const parent = parentEventFor(game, explicitParent);
  game.eventSequence += 1;
  const id = String(input.id || `event_${game.turnSerial || 0}_${game.eventSequence}`);
  const chainId = String(input.chainId || parent?.chainId || `chain_${game.turnSerial || 0}_${game.eventSequence}`);
  const depth = nonNegativeInteger(input.depth, parent ? nonNegativeInteger(parent.depth) + 1 : 0);
  return {
    ...input,
    id,
    type,
    chainId,
    depth,
    parentEventId: String(input.parentEventId || parent?.id || ""),
    turnSerial: nonNegativeInteger(input.turnSerial, nonNegativeInteger(game.turnSerial)),
    round: Math.max(1, nonNegativeInteger(input.round, Math.max(1, nonNegativeInteger(game.round, 1)))),
    sequence: game.eventSequence,
    handledTriggerKeys: Array.isArray(input.handledTriggerKeys)
      ? [...new Set(input.handledTriggerKeys.map(String))]
      : []
  };
}

export function queueGameEvent(game, input = {}, explicitParent = null) {
  const event = createGameEvent(game, input, explicitParent);
  enqueueResolution(game, { kind: "GAME_EVENT", event, depth: event.depth });
  return event;
}

export function claimEventTrigger(event, triggerKey) {
  if (!event || !triggerKey) return false;
  event.handledTriggerKeys = Array.isArray(event.handledTriggerKeys) ? event.handledTriggerKeys : [];
  const key = String(triggerKey);
  if (event.handledTriggerKeys.includes(key)) return false;
  event.handledTriggerKeys.push(key);
  return true;
}

function presentationPayload(event) {
  if (!event || typeof event !== "object") return null;
  return {
    id: String(event.id || ""),
    type: String(event.type || ""),
    chainId: String(event.chainId || ""),
    depth: nonNegativeInteger(event.depth),
    parentEventId: String(event.parentEventId || ""),
    turnSerial: nonNegativeInteger(event.turnSerial),
    round: Math.max(1, nonNegativeInteger(event.round, 1)),
    sequence: nonNegativeInteger(event.sequence),
    actorId: String(event.actorId || event.playerId || ""),
    ownerId: String(event.ownerId || ""),
    playerId: String(event.playerId || ""),
    cardId: String(event.cardId || ""),
    sourceCardId: String(event.sourceCardId || ""),
    instanceId: String(event.instanceId || ""),
    sourceInstanceId: String(event.sourceInstanceId || ""),
    amount: Number(event.amount) || 0,
    method: String(event.method || "")
  };
}

function dispatchPresentationEvent(event) {
  const payload = presentationPayload(event);
  if (!payload) return;
  const target = globalThis;
  if (typeof target?.dispatchEvent !== "function" || typeof target?.CustomEvent !== "function") return;
  try {
    target.dispatchEvent(new target.CustomEvent(PRESENTATION_EVENT_NAME, { detail: payload }));
  } catch {
    // Presentation is optional and must never interrupt deterministic game resolution.
  }
}

function recordEventHistory(game, event) {
  game.eventHistory.push({
    id: event.id,
    type: event.type,
    chainId: event.chainId,
    depth: event.depth,
    parentEventId: event.parentEventId,
    turnSerial: event.turnSerial,
    round: event.round,
    actorId: String(event.actorId || event.playerId || ""),
    ownerId: String(event.ownerId || ""),
    cardId: String(event.cardId || ""),
    instanceId: String(event.instanceId || ""),
    amount: Number(event.amount) || 0,
    method: String(event.method || "")
  });
  game.eventHistory = game.eventHistory.slice(-80);
  dispatchPresentationEvent(event);
}

function warn(game, message, onWarning) {
  const warning = String(message);
  game.eventWarnings.push(warning);
  game.eventWarnings = game.eventWarnings.slice(-24);
  if (typeof onWarning === "function") onWarning(warning);
}

export function drainGameEvents(game, handlers = {}, options = {}) {
  ensureGameEventState(game);
  const maxDepth = Math.max(1, nonNegativeInteger(options.maxDepth, MAX_TRIGGER_DEPTH));
  return drainResolutionQueue(game, (item, currentGame) => {
    if (item.kind !== "GAME_EVENT") {
      return typeof handlers.resolveItem === "function"
        ? handlers.resolveItem(item, currentGame)
        : undefined;
    }
    const event = item.event;
    if (!event || !supportedEvents.has(event.type)) return undefined;
    if (event.depth > maxDepth) {
      warn(currentGame, `Event chain ${event.chainId} exceeded trigger depth ${maxDepth} at ${event.type}.`, options.onWarning);
      return undefined;
    }
    recordEventHistory(currentGame, event);
    if (typeof handlers.resolveEvent === "function") {
      return handlers.resolveEvent(event, {
        claim: key => claimEventTrigger(event, key),
        emit: next => queueGameEvent(currentGame, next, event)
      });
    }
    return undefined;
  }, {
    maxDepth,
    maxSteps: options.maxSteps || MAX_TRIGGER_STEPS,
    shouldPause: options.shouldPause,
    onWarning: message => warn(game, message, options.onWarning)
  });
}

export function emitGameEvent(game, input = {}, handlers = {}, options = {}) {
  const event = queueGameEvent(game, input);
  const resolution = drainGameEvents(game, handlers, options);
  return { event, resolution };
}

export function isSupportedGameEvent(type) {
  return supportedEvents.has(String(type || ""));
}
