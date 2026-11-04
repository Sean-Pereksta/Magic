import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import vm from 'node:vm';
import { TOTAL_STAGES, CHAPTERS, STAGES, getChapter, getStage } from '../game/matchwars/campaign.mjs';
import { difficultyForStage, ELEMENT_INFO } from '../game/matchwars/combat.mjs';
import { ELEMENTS } from '../game/matchwars/board.mjs';
import { CLASSES, CLASS_CHOICES, DEFAULT_LOADOUT, COMBOS, isClassUnlocked, normalizeLoadout } from '../game/matchwars/classes.mjs';

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

test('the 84-stage campaign preserves the original twelve names, descriptions, and chapter titles', () => {
  assert.equal(TOTAL_STAGES, 84);
  assert.equal(STAGES.length, TOTAL_STAGES);
  assert.equal(CHAPTERS.length, 21);
  assert.deepEqual(STAGES.slice(0, 12), originalStages);
  assert.equal(new Set(STAGES.map(([name]) => name)).size, TOTAL_STAGES);
  for (const tuple of STAGES) assert.ok(tuple.length === 3 && tuple.every(value => typeof value === 'string' && value.length > 0));
});

test('all first 48 stage tuples and twelve chapter records remain exactly compatible with the previous release', () => {
  const fingerprint = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
  assert.equal(fingerprint(STAGES.slice(0, 48)), '2a23d53080e1bcff9ae273fbeef14ff6f0b50a48f57bf99b303cc3fb1c453de6');
  assert.equal(fingerprint(CHAPTERS.slice(0, 12)), '6ba3f0c9af502ade96aebec10d9da1cf3809030801afca9b7b11124227136aa5');
});

test('each four-battle chapter has a valid element roster, theme, and named final boss', () => {
  const elements = new Set(['ember', 'tide', 'grove', 'storm', 'sun', 'void']);
  const bosses = new Set(['rootking', 'frostqueen', 'eclipselord']);
  assert.equal(new Set(CHAPTERS.map(chapter => chapter.title)).size, 21);
  assert.equal(new Set(CHAPTERS.map(chapter => chapter.bossName)).size, 21);
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
  for (const value of [-Infinity, Infinity, NaN, undefined, null, '84', {}, []]) {
    assert.equal(getStage(value).number, 1);
    assert.equal(getChapter(value), CHAPTERS[0]);
  }
  for (const value of [-99, -1, 0, .9]) assert.equal(getStage(value).number, 1);
  assert.equal(getStage(16.9).number, 16);
  assert.equal(getChapter(16.9), CHAPTERS[3]);
  for (const value of [85, 999, Number.MAX_SAFE_INTEGER]) {
    assert.equal(getStage(value).number, 84);
    assert.equal(getChapter(value), CHAPTERS[20]);
  }
});

test('campaign constants and returned metadata cannot be mutated by UI or combat code', () => {
  assert.ok(Object.isFrozen(CHAPTERS) && Object.isFrozen(STAGES));
  for (const chapter of CHAPTERS) assert.ok(Object.isFrozen(chapter) && Object.isFrozen(chapter.elements));
  for (const tuple of STAGES) assert.ok(Object.isFrozen(tuple));
  const stage = getStage(84);
  assert.ok(Object.isFrozen(stage));
  assert.throws(() => { stage.number = 1; }, TypeError);
  assert.throws(() => { stage.chapter.elements[0] = 'sun'; }, TypeError);
  assert.throws(() => { STAGES[83][0] = 'changed'; }, TypeError);
});

test('new frontiers provide nine distinct bosses and strategic encounter descriptions', () => {
  const expansion = STAGES.slice(48);
  assert.equal(expansion.length, 36);
  assert.equal(new Set(expansion.map(([, description]) => description)).size, 36);
  assert.deepEqual(CHAPTERS.slice(0, 3).map(chapter => chapter.bossName), ['The Root King', 'Frostbound Queen', 'The Eclipse Lord']);
  assert.equal(getStage(48).name, 'The Runeless Emperor');
  assert.equal(getStage(72).name, 'The Runebound Avatar');
  assert.equal(getStage(84).name, 'The Worldbreaker');
  assert.match(expansion.map(([, description]) => description).join(' '), /healers/);
  assert.match(expansion.map(([, description]) => description).join(' '), /Flying|flying/);
  assert.match(expansion.map(([, description]) => description).join(' '), /every lane|three roads|outer lanes/);
  assert.match(expansion.map(([, description]) => description).join(' '), /special chain|special rune/);
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

test('the app advances beyond both former finales and caps only at battle 84', () => {
  for (const stage of [12, 48, 72, 84]) {
    let result;
    const host = appFunction('finishBattle', 'cast', {
      progress: { stage, completed: [], unlocked: stage, gold: 0 },
      battle: { status: 'won', reward: () => 100, kills: 20 },
      resolved: false, busy: false, bestChain: 4,
      TOTAL_STAGES, STAGES, getStage, CLASSES, escape: String,
      persist() {}, sound() {}, openModal: (...args) => { result = args; },
    });
    host.run();
    const next = Math.min(TOTAL_STAGES, stage + 1);
    assert.equal(host.progress.unlocked, next);
    assert.ok(host.progress.completed.includes(stage));
    assert.equal(host.progress.gold, 135);
    assert.match(result[1], new RegExp(`data-next="${next}"`));
    if (stage === 12) {
      assert.match(result[1], /Battle 13 is now unlocked/);
      assert.doesNotMatch(result[1], /completed all 84/);
    } else if (stage < TOTAL_STAGES) {
      assert.doesNotMatch(result[1], /completed all 84 battles/);
    } else {
      assert.match(result[1], /The Worldbreaker/);
      assert.match(result[1], /completed all 84 battles/);
      assert.doesNotMatch(result[1], /data-next="85"/);
    }
  }
});

test('the app pages the active chapter into four encounters and displays late-wave counts', () => {
  let result;
  const host = appFunction('showCampaign', 'confirmRestart', {
    progress: { stage: 84, completed: [81, 82], unlocked: 84 },
    TOTAL_STAGES, CHAPTERS, STAGES, getChapter, difficultyForStage,
    clamp: (value, low, high) => Math.min(high, Math.max(low, value)),
    roman: String, difficultyName: () => 'Mythic', escape: String,
    openModal: (...args) => { result = args; },
  });
  host.run();
  const html = result[1];
  assert.match(html, /THE WORLD’S LAST WARD/);
  assert.match(html, /The Worldbreaker/);
  assert.deepEqual([...html.matchAll(/data-stage="(\d+)"/g)].slice(0, 4).map(match => Number(match[1])), [81, 82, 83, 84]);
  assert.equal([...html.matchAll(/class="stage-card/g)].length, 4);
  assert.equal([...html.matchAll(new RegExp(`${difficultyForStage(84).totalWaves} waves`, 'g'))].length, 4);
  assert.doesNotMatch(html, /The Verdant Gate/);
  host.run(4);
  assert.deepEqual([...result[1].matchAll(/data-stage="(\d+)"/g)].slice(0, 4).map(match => Number(match[1])), [13, 14, 15, 16]);
  assert.equal([...result[1].matchAll(/4 waves/g)].length, 4);
});

test('the live wave display accepts the sixth wave instead of an earlier campaign denominator', () => {
  const nodes = new Map();
  const node = id => {
    if (!nodes.has(id)) nodes.set(id, { style: {}, textContent: '', innerHTML: '', disabled: false, setAttribute() {}, querySelector: () => ({ style: {}, textContent: '' }) });
    return nodes.get(id);
  };
  const host = appFunction('updateUI', 'combatEvent', {
    $: node,
    battle: { playerHp: 180, playerMax: 180, enemyHp: 400, enemyMax: 400, enemyBarrier: false,
      wave: 6, totalWaves: 6, time: 120, forecast: () => [], cooldowns: {}, spellDurations: {}, status: 'playing' },
    document: { querySelector: () => ({ classList: { toggle() {} } }) },
    overdrive: 0, charge: 0, busy: false, started: true, lane: 1,
  });
  host.run();
  assert.equal(nodes.get('wave').textContent, 'WAVE 6 / 6');
});

test('the class armory presents all eighteen choices and gates them at actual campaign milestones', () => {
  let result;
  const host = appFunction('showArmory', 'equipClass', {
    CLASSES, CLASS_CHOICES, ELEMENTS, ELEMENT_INFO, COMBOS, isClassUnlocked,
    progress: { unlocked: 1, loadout: { ...DEFAULT_LOADOUT } },
    battle: { status: 'playing', loadout: { ...DEFAULT_LOADOUT } }, started: false,
    mobileScreen: { matches: false },
    colors: Object.fromEntries(ELEMENTS.map(key => [key, ELEMENT_INFO[key].color])),
    classPortrait: def => `<canvas data-class="${def.id}"></canvas>`,
    openModal: (...args) => { result = args; },
  });
  host.run('ember');
  assert.match(result[1], /6 \/ 18/);
  assert.equal([...result[1].matchAll(/<article class="class-card/g)].length, 3);
  assert.match(result[1], /data-equip="ember-knight" disabled/);
  assert.match(result[1], /data-equip="phoenix-adept" disabled/);
  host.progress.unlocked = 4;
  host.run('ember');
  assert.match(result[1], /7 \/ 18/);
  assert.doesNotMatch(result[1], /data-equip="ember-knight" disabled/);
  assert.match(result[1], /data-equip="phoenix-adept" disabled/);
  host.progress.unlocked = 84;
  const rendered = new Set();
  for (const element of ELEMENTS) {
    host.run(element);
    assert.match(result[1], /18 \/ 18/);
    assert.doesNotMatch(result[1], /class-card[^"\n]*locked/);
    for (const match of result[1].matchAll(/data-equip="([^"]+)"/g)) rendered.add(match[1]);
  }
  assert.deepEqual([...rendered].sort(), Object.keys(CLASSES).sort());
});

test('equipping an unlocked class during combat saves a pending choice without mutating the current army', () => {
  let saved = 0, restarted = 0, shown;
  const host = appFunction('equipClass', 'showBonds', {
    CLASSES, isClassUnlocked, normalizeLoadout,
    progress: { stage: 4, unlocked: 4, loadout: { ...DEFAULT_LOADOUT } },
    battle: { time: 10, status: 'playing', loadout: { ...DEFAULT_LOADOUT } }, started: true,
    persist() { saved++; }, startBattle() { restarted++; },
    showArmory: key => { shown = key; }, sound() {},
  });
  host.run('ember-knight');
  assert.equal(host.progress.loadout.ember, 'ember-knight');
  assert.equal(host.battle.loadout.ember, 'ember-mage');
  assert.equal(saved, 1);
  assert.equal(restarted, 0);
  assert.equal(shown, 'ember');
  host.run('phoenix-adept');
  assert.equal(host.progress.loadout.ember, 'ember-knight');
  assert.equal(saved, 1, 'a locked class must not overwrite the saved choice');
  host.started = false; host.battle.time = 0;
  host.run('ember-mage');
  assert.equal(restarted, 1, 'an army chosen before starting should be installed immediately');
});

test('the armory explains when a changed class is waiting for the next battle', () => {
  let result;
  const host = appFunction('showArmory', 'equipClass', {
    CLASSES, CLASS_CHOICES, ELEMENTS, ELEMENT_INFO, COMBOS, isClassUnlocked,
    progress: { unlocked: 4, loadout: { ...DEFAULT_LOADOUT, ember: 'ember-knight' } },
    battle: { status: 'playing', loadout: { ...DEFAULT_LOADOUT } }, started: true,
    mobileScreen: { matches: false },
    colors: Object.fromEntries(ELEMENTS.map(key => [key, ELEMENT_INFO[key].color])),
    classPortrait: def => `<canvas data-class="${def.id}"></canvas>`,
    openModal: (...args) => { result = args; },
  });
  host.run('ember');
  assert.match(result[1], /New class equipped for the next battle/);
  assert.match(result[1], /Restart with this army/);
  host.battle.loadout.ember = 'ember-knight';
  host.run('ember');
  assert.doesNotMatch(result[1], /New class equipped for the next battle/);
});

test('winning a milestone battle announces only newly earned classes, including the first and final unlock', () => {
  for (const { stage, unlocked, status, expected } of [
    { stage: 3, unlocked: 3, status: 'won', expected: ['ember-knight'] },
    { stage: 3, unlocked: 4, status: 'won', expected: [] },
    { stage: 3, unlocked: 3, status: 'lost', expected: [] },
    { stage: 79, unlocked: 79, status: 'won', expected: ['hexweaver'] },
  ]) {
    const announced = [];
    const host = appFunction('finishBattle', 'cast', {
      progress: { stage, completed: [], unlocked, gold: 0 },
      battle: { status, reward: () => 100, kills: 3 },
      resolved: false, busy: false, bestChain: 2,
      TOTAL_STAGES, STAGES, getStage, CLASSES, escape: String,
      persist() {}, sound() {}, openModal() {},
      showClassUnlocks: definitions => { announced.push(...definitions.map(def => def.id)); },
    });
    host.run();
    assert.deepEqual(announced, expected);
  }
});

test('the earned-class result panel names the class and links to the correct elemental armory', () => {
  let panel;
  const host = appFunction('showClassUnlocks', 'finishBattle', {
    document: { createElement: () => ({ innerHTML: '' }) },
    content: { prepend: element => { panel = element; } },
    classPortrait: def => `<canvas data-class="${def.id}"></canvas>`,
    drawPortraits() {},
  });
  host.run([CLASSES['ember-knight']]);
  assert.match(panel.innerHTML, /NEW CLASS UNLOCKED/);
  assert.match(panel.innerHTML, /Ember Knight/);
  assert.match(panel.innerHTML, /Cinder Charge/);
  assert.match(panel.innerHTML, /data-army="ember"/);
});
