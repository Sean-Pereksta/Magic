import { UPGRADES } from './combat.mjs?v=3';
import { TOTAL_STAGES } from './campaign.mjs?v=3';
import { DEFAULT_LOADOUT, normalizeLoadout } from './classes.mjs?v=3';

/** Only durable campaign progress is saved. Battles always begin afresh. */
export const KEY = 'matchwars-runeborn-v2';
export const LEGACY_KEY = 'colorclash-autosave-v1';
const MAX_GOLD = 1_000_000;
const LEGACY_LAST_STAGE = 12;
const owns = (object, key) => Object.prototype.hasOwnProperty.call(object, key);
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const bounded = (value, low, high, fallback) => typeof value === 'number' && Number.isFinite(value)
  ? Math.min(high, Math.max(low, Math.floor(value))) : fallback;

function freshProgress() {
  return {
    version: 2,
    gold: 120,
    unlocked: 1,
    stage: 1,
    completed: [],
    upgrades: {},
    loadout: { ...DEFAULT_LOADOUT },
    settings: { sound: true, reducedMotion: false },
    seenIntro: false,
    legacyMigrated: false,
  };
}

/** Copy and whitelist every field; neither prototypes nor unknown keys survive. */
export function sanitizeProgress(input) {
  const result = freshProgress();
  if (!record(input) || input.version !== 2) return result;
  result.gold = bounded(input.gold, 0, MAX_GOLD, result.gold);
  result.unlocked = bounded(input.unlocked, 1, TOTAL_STAGES, 1);
  if (Array.isArray(input.completed)) {
    result.completed = [...new Set(input.completed.filter(value => Number.isInteger(value)
      && value >= 1 && value <= TOTAL_STAGES))].sort((a, b) => a - b);
    // Completion is authoritative when an older save capped its unlock at 12.
    // Repair access only: never grant currency, upgrades, or completion rewards.
    const nextStage = Math.min(TOTAL_STAGES, (result.completed.at(-1) || 0) + 1);
    result.unlocked = Math.max(result.unlocked, nextStage);
  }
  result.stage = bounded(input.stage, 1, result.unlocked, 1);
  result.loadout = normalizeLoadout(record(input.loadout) ? input.loadout : {}, result.unlocked);
  if (record(input.upgrades)) {
    for (const definition of UPGRADES) {
      if (!owns(input.upgrades, definition.id)) continue;
      const rank = bounded(input.upgrades[definition.id], 0, definition.max, 0);
      if (rank > 0) result.upgrades[definition.id] = rank;
    }
  }
  if (record(input.settings)) {
    for (const setting of ['sound', 'reducedMotion']) {
      if (typeof input.settings[setting] === 'boolean') result.settings[setting] = input.settings[setting];
    }
  }
  result.seenIntro = input.seenIntro === true;
  result.legacyMigrated = input.legacyMigrated === true;
  return result;
}

function migrateLegacy(input) {
  if (!record(input) || input.v !== 1) return null;
  const result = freshProgress();
  result.gold = bounded(input.gold, 0, MAX_GOLD, 120);
  // The pre-Runeborn save format only had the original campaign progression.
  result.stage = bounded(input.level, 1, LEGACY_LAST_STAGE, 1);
  result.unlocked = result.stage;
  result.completed = Array.from({ length: result.stage - 1 }, (_, index) => index + 1);
  result.legacyMigrated = true;
  const mappings = {
    red: ['ember-splash', 'ember-burn'], blue: ['tide-range', 'tide-slow'],
    green: ['grove-heal', 'grove-poison'], pink: ['sun-shield', 'sun-armor'],
    purple: ['void-drain', 'void-execute'], yellow: ['storm-speed', 'storm-chain'],
  };
  if (record(input.upgrades)) {
    for (const [color, ids] of Object.entries(mappings)) {
      const stats = owns(input.upgrades, color) ? input.upgrades[color] : null;
      if (!record(stats)) continue;
      const physical = Math.max(...['attack', 'speed', 'health'].map(stat => bounded(stats[stat], 1, 99, 1)));
      const ability = bounded(stats.ability, 1, 99, 1);
      for (const [index, value] of [physical, ability].entries()) {
        const rank = Math.min(3, Math.ceil((value - 1) / 2));
        if (rank > 0) result.upgrades[ids[index]] = rank;
      }
    }
  }
  return result;
}

function getStorage(storage) {
  return storage === undefined ? globalThis.localStorage : storage;
}

/** Reads without modifying either key. The caller decides when to save migration. */
export function loadProgress(storage) {
  try {
    const target = getStorage(storage);
    if (!target || typeof target.getItem !== 'function') return freshProgress();
    const current = target.getItem(KEY);
    if (current) {
      try {
        const parsed = JSON.parse(current);
        if (record(parsed) && parsed.version === 2) return sanitizeProgress(parsed);
      } catch { /* A corrupt new save can still recover the older campaign. */ }
    }
    const legacy = target.getItem(LEGACY_KEY);
    if (legacy) {
      try { return migrateLegacy(JSON.parse(legacy)) || freshProgress(); }
      catch { return freshProgress(); }
    }
  } catch { /* Disabled browser storage and privacy restrictions are harmless. */ }
  return freshProgress();
}

/** Returns false if storage is unavailable or full; never deletes legacy saves. */
export function saveProgress(progress, storage) {
  try {
    const target = getStorage(storage);
    if (!target || typeof target.setItem !== 'function') return false;
    target.setItem(KEY, JSON.stringify(sanitizeProgress(progress)));
    return true;
  } catch { return false; }
}
