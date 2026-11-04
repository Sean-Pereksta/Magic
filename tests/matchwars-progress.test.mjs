import test from 'node:test';
import assert from 'node:assert/strict';
import { KEY, LEGACY_KEY, loadProgress, saveProgress, sanitizeProgress } from '../game/matchwars/progress.mjs';
import { TOTAL_STAGES } from '../game/matchwars/campaign.mjs';

function memoryStorage(initial = {}) {
  const data = new Map(Object.entries(initial));
  return { data, getItem: key => data.get(key) ?? null, setItem: (key, value) => data.set(key, value) };
}

test('fresh campaigns receive 120 gold and independent mutable values', () => {
  const first = loadProgress(memoryStorage());
  const second = loadProgress(memoryStorage());
  assert.deepEqual(first, {
    version: 2, gold: 120, unlocked: 1, stage: 1, completed: [], upgrades: {},
    settings: { sound: true, reducedMotion: false }, seenIntro: false, legacyMigrated: false,
  });
  first.completed.push(1);
  first.settings.sound = false;
  assert.deepEqual(second.completed, []);
  assert.equal(second.settings.sound, true);
});

test('corrupt JSON and invalid save shapes safely return defaults', () => {
  for (const value of ['{bad', 'null', '[]', '123', '{"version":99}', '{"version":1}']) {
    const storage = memoryStorage({ [KEY]: value });
    assert.equal(loadProgress(storage).gold, 120);
    assert.equal(storage.data.size, 1, 'reading does not rewrite a save');
  }
});

test('sanitization bounds stages, currency, ranks and settings without unknown keys', () => {
  const input = JSON.parse('{"version":2,"gold":-5,"unlocked":999,"stage":999,"completed":[1,1,3,12,13,-1,"2",2.5],"upgrades":{"ember-burn":999,"sun-shield":2.9,"bounty":-4,"combo":"3","__proto__":{"polluted":true},"unknown":3},"settings":{"sound":false,"reducedMotion":"yes","extra":1},"seenIntro":true,"junk":true}');
  const result = sanitizeProgress(input);
  assert.equal(result.gold, 0);
  assert.equal(result.unlocked, TOTAL_STAGES);
  assert.equal(result.stage, TOTAL_STAGES);
  assert.deepEqual(result.completed, [1, 3, 12, 13]);
  assert.deepEqual(result.upgrades, { 'ember-burn': 3, 'sun-shield': 2 });
  assert.deepEqual(result.settings, { sound: false, reducedMotion: false });
  assert.equal(result.junk, undefined);
  assert.equal({}.polluted, undefined);
  assert.equal(sanitizeProgress({ version: 2, gold: Infinity }).gold, 120);
  assert.equal(sanitizeProgress({ version: 2, gold: 5e20 }).gold, 1_000_000);
  assert.equal(sanitizeProgress({ version: 2, unlocked: 2, stage: 8 }).stage, 2);
});

test('upgrade data must be a record with own valid rank properties', () => {
  for (const upgrades of [null, [], true, 'bad']) {
    assert.deepEqual(sanitizeProgress({ version: 2, upgrades }).upgrades, {});
  }
  const upgrades = Object.create({ 'ember-burn': 3 });
  upgrades['tide-range'] = NaN;
  assert.deepEqual(sanitizeProgress({ version: 2, upgrades }).upgrades, {});
});

test('legacy campaigns migrate currency, progress and class investment, preserving original key', () => {
  const legacy = JSON.stringify({ v: 1, gold: 543.7, level: 5, grid: [],
    upgrades: { red: { attack: 4, speed: 1, health: 1, ability: 2 }, blue: { ability: 99 },
      pink: { attack: 1 }, yellow: { speed: 3 }, purple: { ability: 5 }, green: { health: 2 } } });
  const storage = memoryStorage({ [LEGACY_KEY]: legacy });
  const result = loadProgress(storage);
  assert.equal(result.gold, 543);
  assert.equal(result.unlocked, 5);
  assert.equal(result.stage, 5);
  assert.deepEqual(result.completed, [1, 2, 3, 4]);
  assert.equal(result.legacyMigrated, true);
  assert.equal(result.seenIntro, false);
  assert.deepEqual(result.upgrades, {
    'ember-splash': 2, 'ember-burn': 1, 'tide-slow': 3, 'grove-heal': 1,
    'void-execute': 2, 'storm-speed': 1,
  });
  assert.equal(storage.getItem(KEY), null);
  assert.equal(saveProgress(result, storage), true);
  assert.equal(storage.getItem(LEGACY_KEY), legacy);
  assert.deepEqual(loadProgress(storage), result);
});

test('new valid saves take precedence; a corrupt new save can recover legacy', () => {
  const storage = memoryStorage({ [KEY]: '{broken', [LEGACY_KEY]: JSON.stringify({ v: 1, gold: 200, level: 90 }) });
  assert.equal(loadProgress(storage).stage, 12);
  assert.equal(saveProgress({ version: 2, gold: 40 }, storage), true);
  assert.equal(loadProgress(storage).gold, 40);
  assert.equal(loadProgress(storage).legacyMigrated, false);
});

test('completed original campaigns unlock stage 13 without regranting rewards', () => {
  const original = {
    version: 2, gold: 371, unlocked: 12, stage: 12,
    completed: Array.from({ length: 12 }, (_, index) => index + 1),
    upgrades: { 'ember-burn': 2, 'sun-shield': 3, bounty: 1 },
    settings: { sound: false, reducedMotion: true }, seenIntro: true, legacyMigrated: false,
  };
  const storage = memoryStorage({ [KEY]: JSON.stringify(original) });
  const result = loadProgress(storage);
  assert.deepEqual(result, { ...original, unlocked: 13 });
  assert.equal(JSON.parse(storage.getItem(KEY)).unlocked, 12, 'load does not mutate stored data');
  assert.equal(saveProgress(result, storage), true);
  assert.deepEqual(loadProgress(storage), result, 'repair is idempotent and does not add rewards');
});

test('valid completion repairs stale unlocks and preserves sparse campaign history', () => {
  assert.equal(sanitizeProgress({ version: 2, unlocked: 1, completed: [1] }).unlocked, 2);
  assert.equal(sanitizeProgress({ version: 2, unlocked: 1, completed: [4, 2, 4] }).unlocked, 5);
  assert.deepEqual(sanitizeProgress({ version: 2, unlocked: 1, completed: [4, 2, 4] }).completed, [2, 4]);
  const corrupt = sanitizeProgress({ version: 2, unlocked: 1, stage: 100,
    completed: [100, 49, 0, -1, '48', 12.5, null] });
  assert.equal(corrupt.unlocked, 1);
  assert.equal(corrupt.stage, 1);
  assert.deepEqual(corrupt.completed, []);
});

test('stage 48 and complete endgame saves round-trip without unlocking stage 49', () => {
  assert.equal(TOTAL_STAGES, 48);
  const endgame = {
    ...loadProgress(memoryStorage()), gold: 942, unlocked: 48, stage: 48,
    completed: Array.from({ length: 48 }, (_, index) => index + 1),
    upgrades: { 'tide-range': 3, 'storm-chain': 2 },
  };
  const storage = memoryStorage();
  assert.equal(saveProgress(endgame, storage), true);
  assert.deepEqual(loadProgress(storage), endgame);
  assert.equal(sanitizeProgress({ ...endgame, unlocked: 100, stage: 49 }).stage, 48);
});

test('original legacy migration remains capped at stage 12 after campaign expansion', () => {
  const storage = memoryStorage({ [LEGACY_KEY]: JSON.stringify({ v: 1, level: 90, gold: 180 }) });
  const migrated = loadProgress(storage);
  assert.equal(migrated.stage, 12);
  assert.equal(migrated.unlocked, 12);
  assert.deepEqual(migrated.completed, Array.from({ length: 11 }, (_, index) => index + 1));
  assert.equal(saveProgress(migrated, storage), true);
  assert.equal(loadProgress(storage).unlocked, 12);
});

test('restricted and quota-exceeded storage never crashes campaign play', () => {
  const denied = { getItem() { throw new Error('denied'); }, setItem() { throw new Error('quota'); } };
  assert.equal(loadProgress(denied).gold, 120);
  assert.equal(saveProgress({ version: 2 }, denied), false);
  assert.equal(saveProgress({ version: 2 }, null), false);
  assert.equal(loadProgress(null).stage, 1);
  assert.equal(loadProgress().stage, 1, 'Node and unavailable browser storage are supported');
});
