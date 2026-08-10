export const DEFAULT_MAX_RESOLUTION_STEPS = 256;
export const DEFAULT_MAX_RESOLUTION_DEPTH = 24;

const activeGames = new WeakSet();
const activeItems = new WeakMap();

function asNonNegativeInteger(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : fallback;
}

export function ensureResolutionState(game) {
  if (!game || typeof game !== "object") {
    throw new TypeError("A mutable game object is required.");
  }
  game.resolutionQueue = Array.isArray(game.resolutionQueue)
    ? game.resolutionQueue.filter(item => item && typeof item === "object")
    : [];
  game.resolutionSequence = asNonNegativeInteger(game.resolutionSequence);
  game.resolutionWarnings = Array.isArray(game.resolutionWarnings)
    ? game.resolutionWarnings.slice(-24)
    : [];
  return game.resolutionQueue;
}

export function activeResolutionItem(game) {
  return activeItems.get(game) || null;
}

export function enqueueResolution(game, item, options = {}) {
  const queue = ensureResolutionState(game);
  const active = activeResolutionItem(game);
  const depth = asNonNegativeInteger(
    item?.depth,
    active ? asNonNegativeInteger(active.depth) + 1 : 0
  );
  game.resolutionSequence += 1;
  const normalized = {
    ...item,
    id: String(item?.id || `resolution_${game.turnSerial || 0}_${game.resolutionSequence}`),
    kind: String(item?.kind || "EFFECT"),
    depth,
    sequence: game.resolutionSequence
  };
  if (options.front === true) queue.unshift(normalized);
  else queue.push(normalized);
  return normalized;
}

function recordResolutionWarning(game, message, onWarning) {
  ensureResolutionState(game);
  const warning = String(message || "War Realms resolution warning");
  game.resolutionWarnings.push(warning);
  game.resolutionWarnings = game.resolutionWarnings.slice(-24);
  if (typeof onWarning === "function") onWarning(warning);
}

export function drainResolutionQueue(game, resolver, options = {}) {
  ensureResolutionState(game);
  if (activeGames.has(game)) {
    return { processed: 0, pending: game.resolutionQueue.length, active: true, halted: false };
  }
  if (typeof resolver !== "function") {
    throw new TypeError("A resolution item resolver is required.");
  }

  const maxSteps = Math.max(1, asNonNegativeInteger(options.maxSteps, DEFAULT_MAX_RESOLUTION_STEPS));
  const maxDepth = Math.max(1, asNonNegativeInteger(options.maxDepth, DEFAULT_MAX_RESOLUTION_DEPTH));
  let processed = 0;
  let halted = false;

  activeGames.add(game);
  try {
    while (game.resolutionQueue.length && processed < maxSteps) {
      if (typeof options.shouldPause === "function" && options.shouldPause(game)) {
        halted = true;
        break;
      }
      const item = game.resolutionQueue.shift();
      if (!item) continue;
      if (asNonNegativeInteger(item.depth) > maxDepth) {
        recordResolutionWarning(
          game,
          `Resolution ${item.id || "unknown"} exceeded depth ${maxDepth} and was skipped.`,
          options.onWarning
        );
        continue;
      }
      activeItems.set(game, item);
      const result = resolver(item, game);
      processed += 1;
      if (result?.pause === true) {
        halted = true;
        break;
      }
    }
  } finally {
    activeItems.delete(game);
    activeGames.delete(game);
  }

  if (processed >= maxSteps && game.resolutionQueue.length) {
    recordResolutionWarning(
      game,
      `Resolution queue exceeded ${maxSteps} steps; ${game.resolutionQueue.length} item(s) were left pending.`,
      options.onWarning
    );
    halted = true;
  }

  return {
    processed,
    pending: game.resolutionQueue.length,
    active: false,
    halted
  };
}

export function clearResolutionQueue(game, reason = "cleared") {
  ensureResolutionState(game);
  const removed = game.resolutionQueue.length;
  game.resolutionQueue = [];
  if (removed) recordResolutionWarning(game, `${removed} pending resolution item(s) were ${reason}.`);
  return removed;
}
