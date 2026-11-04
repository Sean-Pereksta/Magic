import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { TOTAL_STAGES, CHAPTERS, STAGES, getChapter, getStage } from '../game/matchwars/campaign.mjs';
import { difficultyForStage } from '../game/matchwars/combat.mjs';

const originalStages = [
  ['The Verdant Gate', 'Meet the raiders at the forest border.', 'THE AWAKENING'],
  ['River of Glass', 'Frost snipers gather by the river.', 'THE AWAKENING'],
  ['The Tangled Watch', 'Break the healers behind their living shields.', 'THE AWAKENING'],
  ['The Root King', 'A regenerating monarch holds the middle lane.', 'THE AWAKENING'],
  ['Stormbreak Ridge', 'Bring lightning hunters down from the skies.', 'THE FROSTBOUND MARCH'],
  ['The Golden Bastion', 'Crack the sun guards with Void Reapers.', 'THE FROSTBOUND MARCH'],
  ['A Hollow Promise', 'Summoners hide behind their shades.', 'THE FROSTBOUND MARCH'],
  ['Frostbound Queen', 'A frost volley threatens every formation.', 'THE FROSTBOUND MARCH'],
  ['Ashes at Dawn', 'Fire, forest, and storm collide.', 'THE ECLIPSE'],
  ['Prismfall', 'Ranged-resistant sentinels demand a new plan.', 'THE ECLIPSE'],
  ['The Last Lantern', 'Keep your healers alive through the darkness.', 'THE ECLIPSE'],
  ['The Eclipse Lord', 'Break the ward. End the invasion.', 'THE ECLIPSE'],
];

test('the 48-stage campaign preserves the original twelve names, descriptions, and chapter titles', () => {
  assert.equal(TOTAL_STAGES, 48);
  assert.equal(STAGES.length, TOTAL_STAGES);
  assert.equal(CHAPTERS.length, 12);
  assert.deepEqual(STAGES.slice(0, 12), originalStages);
  assert.equal(new Set(STAGES.map(([name]) => name)).size, TOTAL_STAGES);
  for (const tuple of STAGES) assert.ok(tuple.length === 3 && tuple.every(value => typeof value === 'string' && value.length > 0));
});

test('each four-battle chapter has a valid element roster, theme, and named final boss', () => {
  const elements = new Set(['ember', 'tide', 'grove', 'storm', 'sun', 'void']);
  const bosses = new Set(['rootking', 'frostqueen', 'eclipselord']);
  assert.equal(new Set(CHAPTERS.map(chapter => chapter.title)).size, 12);
  assert.equal(new Set(CHAPTERS.map(chapter => chapter.bossName)).size, 12);
  for (const [index, chapter] of CHAPTERS.entries()) {
    assert.equal(chapter.number, index + 1);
    assert.ok(chapter.description.length > 20);
    assert.match(chapter.theme, /^(forest|frost|ash|storm|sun|void)$/);
    assert.match(chapter.color, /^#[\da-f]{6}$/i);
    assert.equal(chapter.elements.length, 3);
    assert.equal(new Set(chapter.elements).size, 3);
    assert.ok(chapter.elements.every(element => elements.has(element)));
    assert.ok(bosses.has(chapter.bossKey));
    for (let offset = 1; offset <= 4; offset++) {
      const number = index * 4 + offset;
      const stage = getStage(number);
      assert.equal(stage.number, number);
      assert.equal(stage.chapter, chapter);
      assert.equal(getChapter(number), chapter);
      assert.equal(stage.boss, offset === 4);
      assert.equal(stage.bossKey, chapter.bossKey);
      assert.equal(stage.bossName, chapter.bossName);
      assert.equal(STAGES[number - 1][2], chapter.title);
    }
    assert.equal(getStage((index + 1) * 4).name, chapter.bossName);
  }
});

test('stage helpers clamp bounds and reject invalid input without unsafe indexing', () => {
  for (const value of [-Infinity, Infinity, NaN, undefined, null, '48', {}, []]) {
    assert.equal(getStage(value).number, 1);
    assert.equal(getChapter(value), CHAPTERS[0]);
  }
  for (const value of [-99, -1, 0, .9]) assert.equal(getStage(value).number, 1);
  assert.equal(getStage(16.9).number, 16);
  assert.equal(getChapter(16.9), CHAPTERS[3]);
  for (const value of [49, 999, Number.MAX_SAFE_INTEGER]) {
    assert.equal(getStage(value).number, 48);
    assert.equal(getChapter(value), CHAPTERS[11]);
  }
});

test('campaign constants and returned metadata cannot be mutated by UI or combat code', () => {
  assert.ok(Object.isFrozen(CHAPTERS) && Object.isFrozen(STAGES));
  for (const chapter of CHAPTERS) assert.ok(Object.isFrozen(chapter) && Object.isFrozen(chapter.elements));
  for (const tuple of STAGES) assert.ok(Object.isFrozen(tuple));
  const stage = getStage(48);
  assert.ok(Object.isFrozen(stage));
  assert.throws(() => { stage.number = 1; }, TypeError);
  assert.throws(() => { stage.chapter.elements[0] = 'sun'; }, TypeError);
  assert.throws(() => { STAGES[47][0] = 'changed'; }, TypeError);
});

test('new frontiers provide nine distinct bosses and strategic encounter descriptions', () => {
  const expansion = STAGES.slice(12);
  assert.equal(expansion.length, 36);
  assert.equal(new Set(expansion.map(([, description]) => description)).size, 36);
  assert.deepEqual(CHAPTERS.slice(0, 3).map(chapter => chapter.bossName), ['The Root King', 'Frostbound Queen', 'The Eclipse Lord']);
  assert.equal(getStage(48).name, 'The Runeless Emperor');
  assert.match(expansion.map(([, description]) => description).join(' '), /healers/);
  assert.match(expansion.map(([, description]) => description).join(' '), /Flying|flying/);
  assert.match(expansion.map(([, description]) => description).join(' '), /different lanes|three-lane|outer lanes/);
  assert.match(expansion.map(([, description]) => description).join(' '), /special rune|chained special/);
});

// Execute the actual presentation functions with a small host, so chapter and
// victory-boundary regressions are covered without starting a browser or timers.
function appFunction(name, next, context) {
  const source = readFileSync(new URL('../game/matchwars/app.mjs', import.meta.url), 'utf8');
  const start = source.indexOf(`function ${name}(`);
  const end = source.indexOf(`function ${next}(`, start);
  assert.ok(start >= 0 && end > start, `presentation function ${name} must exist`);
  const sandbox = vm.createContext(context);
  vm.runInContext(`${source.slice(start, end)}\nglobalThis.run = ${name};`, sandbox);
  return sandbox;
}

test('the app unlocks battle 13 at the former finale and caps only at battle 48', () => {
  for (const stage of [12, 48]) {
    let result;
    const host = appFunction('finishBattle', 'cast', {
      progress: { stage, completed: [], unlocked: stage, gold: 0 },
      battle: { status: 'won', reward: () => 100, kills: 20 },
      resolved: false, busy: false, bestChain: 4,
      TOTAL_STAGES, STAGES, getStage, escape: String,
      persist() {}, sound() {}, openModal: (...args) => { result = args; },
    });
    host.run();
    assert.equal(host.progress.unlocked, stage === 12 ? 13 : 48);
    assert.ok(host.progress.completed.includes(stage));
    assert.equal(host.progress.gold, 135);
    assert.match(result[1], new RegExp(`data-next="${stage === 12 ? 13 : 48}"`));
    if (stage === 12) {
      assert.match(result[1], /Battle 13 is now unlocked/);
      assert.doesNotMatch(result[1], /completed all 48/);
    } else {
      assert.match(result[1], /The Runeless Emperor/);
      assert.match(result[1], /completed all 48 battles/);
      assert.doesNotMatch(result[1], /data-next="49"/);
    }
  }
});

test('the app pages the active chapter into four encounters and displays late-wave counts', () => {
  let result;
  const host = appFunction('showCampaign', 'confirmRestart', {
    progress: { stage: 48, completed: [45, 46], unlocked: 48 },
    TOTAL_STAGES, CHAPTERS, STAGES, getChapter, difficultyForStage,
    clamp: (value, low, high) => Math.min(high, Math.max(low, value)),
    roman: String, difficultyName: () => 'Mythic', escape: String,
    openModal: (...args) => { result = args; },
  });
  host.run();
  const html = result[1];
  assert.match(html, /THE LAST RIFT/);
  assert.match(html, /The Runeless Emperor/);
  assert.deepEqual([...html.matchAll(/data-stage="(\d+)"/g)].slice(0, 4).map(match => Number(match[1])), [45, 46, 47, 48]);
  assert.equal([...html.matchAll(/class="stage-card/g)].length, 4);
  assert.equal([...html.matchAll(/5 waves/g)].length, 4);
  assert.doesNotMatch(html, /The Verdant Gate/);
  host.run(4);
  assert.deepEqual([...result[1].matchAll(/data-stage="(\d+)"/g)].slice(0, 4).map(match => Number(match[1])), [13, 14, 15, 16]);
  assert.equal([...result[1].matchAll(/4 waves/g)].length, 4);
});

test('the live wave display accepts the fifth wave instead of a stale three-wave denominator', () => {
  const nodes = new Map();
  const node = id => {
    if (!nodes.has(id)) nodes.set(id, { style: {}, textContent: '', innerHTML: '', disabled: false, setAttribute() {}, querySelector: () => ({ style: {}, textContent: '' }) });
    return nodes.get(id);
  };
  const host = appFunction('updateUI', 'combatEvent', {
    $: node,
    battle: { playerHp: 180, playerMax: 180, enemyHp: 400, enemyMax: 400, enemyBarrier: false,
      wave: 5, totalWaves: 5, time: 80, forecast: () => [], cooldowns: {}, spellDurations: {}, status: 'playing' },
    document: { querySelector: () => ({ classList: { toggle() {} } }) },
    overdrive: 0, charge: 0, busy: false, started: true,
  });
  host.run();
  assert.equal(nodes.get('wave').textContent, 'WAVE 5 / 5');
});
