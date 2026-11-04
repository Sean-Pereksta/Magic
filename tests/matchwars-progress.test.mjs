import test from 'node:test';
import assert from 'node:assert/strict';
import { KEY, LEGACY_KEY, loadProgress, saveProgress, sanitizeProgress } from '../game/matchwars/progress.mjs';

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
  assert.equal(result.unlocked, 12);
  assert.equal(result.stage, 12);
  assert.deepEqual(result.completed, [1, 3, 12]);
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

test('restricted and quota-exceeded storage never crashes campaign play', () => {
  const denied = { getItem() { throw new Error('denied'); }, setItem() { throw new Error('quota'); } };
  assert.equal(loadProgress(denied).gold, 120);
  assert.equal(saveProgress({ version: 2 }, denied), false);
  assert.equal(saveProgress({ version: 2 }, null), false);
  assert.equal(loadProgress(null).stage, 1);
  assert.equal(loadProgress().stage, 1, 'Node and unavailable browser storage are supported');
});
