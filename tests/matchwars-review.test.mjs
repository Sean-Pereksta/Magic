import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { CLASSES, CLASS_CHOICES, DEFAULT_LOADOUT, COMBOS, isClassUnlocked, getTroop } from '../game/matchwars/classes.mjs';
import { ELEMENTS } from '../game/matchwars/board.mjs';
import { ELEMENT_INFO } from '../game/matchwars/combat.mjs';
import { SPECIALIZATIONS } from '../game/matchwars/mastery.mjs';
import { createDaily } from '../game/matchwars/daily.mjs';

const app = () => readFileSync(new URL('../game/matchwars/app.mjs', import.meta.url), 'utf8');

function clickHandler(context) {
  const source = app();
  const start = source.indexOf("content.addEventListener('click',");
  const end = source.indexOf("content.addEventListener('change',", start);
  let handler;
  const host = vm.createContext({ ...context, content: { addEventListener: (type, fn) => { handler = fn; } } });
  vm.runInContext(source.slice(start, end), host);
  return action => handler({ target: { closest: () => ({ dataset: { action }, disabled: false }) } });
}

test('returning from an unstarted daily briefing resumes the current campaign instead of restarting it', () => {
  let resets = 0;
  const click = clickHandler({ dailyConfig: null, started: true, progress: { stage: 18 },
    battle: { status: 'playing', time: 42 },
    startBattle() { resets++; }, begin() {}, closeModal() {}, dismissModal() {},
  });
  click('campaign-return');
  assert.equal(resets, 0);
});

test('returning from an actual daily run restores the selected campaign battle', () => {
  const resets = [];
  const click = clickHandler({ dailyConfig: { date: '2026-11-04' }, started: true, progress: { stage: 18 },
    battle: { status: 'playing', time: 42 },
    startBattle(stage) { resets.push(stage); }, begin() {}, closeModal() {}, dismissModal() {},
  });
  click('campaign-return');
  assert.deepEqual(resets, [18]);
});

test('rare-event rewards and reinforcements wait when the final cascade animation is paused', async () => {
  const source = app(), start = source.indexOf('async function swap('), end = source.indexOf('function burst(', start);
  const grid = [[{ id: 1, key: 'ember' }, { id: 2, key: 'tide' }]];
  let paused = false, resume, applied = 0, reachedFinal;
  const finalDelay = new Promise(resolve => { reachedFinal = resolve; });
  const node = () => ({ textContent: '', offsetWidth: 1 });
  const host = vm.createContext({
    board: { snapshot: () => grid, trySwap: () => ({ valid: true, reshuffled: false,
      events: [{ type: 'curse', action: 'reinforcement', key: 'ember' }],
      steps: [{ before: grid, after: grid, chain: 1, groups: [], cleared: [], created: [] }] }) },
    boardEl: { contains: () => false, setAttribute() {} }, tileNodes: new Map(),
    battle: { status: 'playing', upgrades: {}, summon: () => [] },
    document: { activeElement: null, querySelector: () => ({ classList: { remove() {}, add() {} } }) },
    $: node, inputReady: () => true,
    session: 1, lane: 1, busy: false, selected: null, matches: 0, bestChain: 1, charge: 0, overdrive: 0, summonCount: 0,
    markSelection() {}, renderBoard() {}, sound() {}, burst() {}, updateUI() {}, finishBattle() {},
    applyBoardEvents() { applied++; },
    delay: async ms => { if (ms === 240) { paused = true; reachedFinal(); } },
    waitForPlay: async () => !paused || new Promise(resolve => { resume = resolve; }),
  });
  vm.runInContext(`${source.slice(start, end)}\nglobalThis.run = swap;`, host);
  const ongoing = host.run({ r: 0, c: 0 }, { r: 0, c: 1 });
  await finalDelay;
  await new Promise(resolve => setImmediate(resolve));
  try { assert.equal(applied, 0, 'opening a dialog during the last animation must pause event consequences'); }
  finally { paused = false; resume?.(true); }
  await ongoing;
  assert.equal(applied, 1);
});

test('the daily armory explains its fixed army and does not promise a restart with campaign class changes', () => {
  const source = app(), start = source.indexOf('function showArmory('), end = source.indexOf('function equipClass(', start);
  const dailyStart = source.indexOf('function showDailyArmy('), dailyEnd = source.indexOf('function showGuide(', dailyStart);
  const challenge = createDaily('2026-11-04', '2026-11-04');
  let result;
  const host = vm.createContext({
    CLASSES, CLASS_CHOICES, ELEMENTS, ELEMENT_INFO, COMBOS, isClassUnlocked, getTroop, SPECIALIZATIONS,
    progress: { unlocked: 84, loadout: { ...DEFAULT_LOADOUT }, mastery: {} },
    battle: { status: 'playing', loadout: challenge.loadout, troopFor: key => getTroop(key, challenge.loadout[key]) }, started: true,
    dailyConfig: challenge, escape: String,
    colors: Object.fromEntries(ELEMENTS.map(key => [key, ELEMENT_INFO[key].color])),
    classPortrait: def => `<canvas data-class="${def.id}"></canvas>`, masteryPanel: () => '',
    mobileScreen: { matches: false }, openModal: (...args) => { result = args; },
  });
  vm.runInContext(`${source.slice(dailyStart, dailyEnd)}\n${source.slice(start, end)}\nglobalThis.run = showArmory;`, host);
  host.run('ember');
  assert.doesNotMatch(result[1], /Restart with this army/);
  assert.match(result[1], /daily[\s\S]{0,100}fixed|fixed[\s\S]{0,100}daily/i);
});
