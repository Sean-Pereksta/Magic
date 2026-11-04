import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import * as engine from '../game/splinters/engine.mjs';

const source = readFileSync(new URL('../game/splinters/main.mjs', import.meta.url), 'utf8')
  .replace(/^import .*;\r?\n/gm, '');

// Exercise the registered controls and real game rules without a WebGL/browser dependency.
function mount() {
  class Node {
    constructor() {
      this.children = []; this.listeners = new Map(); this.dataset = {}; this.hidden = false;
      this.style = { setProperty() {} }; this.attributes = {};
      this.classList = { toggle() {} };
    }
    append(...nodes) { this.children.push(...nodes); }
    replaceChildren(...nodes) { this.children = nodes; }
    setAttribute(name, value) { this.attributes[name] = value; }
    addEventListener(type, fn) { const list = this.listeners.get(type) || []; list.push(fn); this.listeners.set(type, list); }
    async dispatch(type, event = {}) {
      await Promise.all((this.listeners.get(type) || []).map(fn => fn({ preventDefault() {}, ...event })));
    }
  }
  const nodes = new Map(), node = id => { if (!nodes.has(id)) nodes.set(id, new Node()); return nodes.get(id); };
  const levels = ['easy', 'normal', 'hard'].map(difficulty => { const n = new Node(); n.dataset.difficulty = difficulty; return n; });
  const document = new Node();
  document.getElementById = node; document.createElement = () => new Node();
  document.querySelectorAll = () => levels; document.body = new Node(); document.documentElement = new Node();
  document.exitPointerLock = () => { document.pointerLockElement = null; };
  node('table').requestPointerLock = () => Promise.resolve();
  let fullscreenRequests = 0, fullscreenExits = 0;
  document.documentElement.requestFullscreen = async () => { fullscreenRequests++; document.fullscreenElement = document.documentElement; };
  document.exitFullscreen = async () => { fullscreenExits++; document.fullscreenElement = null; };
  const window = new Node(); window.THREE = {}; window.innerWidth = 390; window.matchMedia = () => ({ matches: true });
  let now = 100000, frameTime = 0, timerId = 0;
  const timers = new Map(), cleared = [], errors = [], sceneCalls = { taps: [], looks: [], resizes: 0 };
  class Clock extends Date { static now() { return now; } }
  let sceneOptions, hitAt = () => null;
  const view = {
    sync() {}, tick() {}, setLooking() {}, setHandHints() {},
    resize() { sceneCalls.resizes++; }, look(...args) { sceneCalls.looks.push(args); },
    interactAt(x, y, button) { sceneCalls.taps.push([x, y, button]); sceneOptions.onInteract(hitAt(x, y), button); },
  };
  const context = vm.createContext({
    ...engine, document, window, Date: Clock,
    cardImage: type => `drawn:${type}`, backImage: () => 'drawn:back',
    createScene(_canvas, options) { sceneOptions = options; return view; },
    requestAnimationFrame() {},
    setTimeout(fn, delay) { const id = ++timerId; timers.set(id, { fn, due: now + delay }); return id; },
    clearTimeout(id) { if (id != null) cleared.push(id); timers.delete(id); },
    console: { error(error) { errors.push(error); } },
  });
  vm.runInContext(`${source}\n globalThis.ui = {
    get state(){return state}, get targetRecipe(){return targetRecipe}, get overlay(){return overlay},
    get paused(){return paused}, get resultsTimer(){return resultsTimer},
    startGame,newGame,act,showPanel,beginCast,cancelTarget,
    forceBotsDue(){nextBots=[0,0,0,0]}, frame,
  };`, context, { filename: 'splinters/main.mjs' });
  assert.deepEqual(errors, [], 'main module initializes and registers controls');
  return {
    ui: context.ui, node, document, sceneCalls, timers, cleared,
    fullscreenCounts: () => [fullscreenRequests, fullscreenExits],
    resolveHit(fn) { hitAt = fn; }, hover(hit) { sceneOptions.onHover(hit); },
    elapse(ms) { now += ms; },
    tick(ms = 25) { now += ms; frameTime += Math.max(25, ms); context.ui.frame(frameTime); },
    runTimers() { for (const [id, timer] of [...timers]) if (timer.due <= now) { timers.delete(id); timer.fn(); } },
  };
}

function putRecipe(state, id) {
  const recipe = engine.RECIPES.find(value => value.id === id);
  state.players[0].hand.forEach((card, i) => { card.type = recipe.cards[i] || 'moon'; });
  return recipe;
}

test('registered touch controls hit fresh tap/hold coordinates and suppress taps after dragging', async () => {
  const app = mount(); app.ui.startGame();
  const stale = app.ui.state.center[0], tapped = app.ui.state.center[1];
  app.hover({ kind: 'center', id: stale.id });
  app.resolveHit((x, y) => x === 111 && y === 222 ? { kind: 'center', id: tapped.id } : null);
  await app.node('table').dispatch('touchstart', { touches: [{ clientX: 111, clientY: 222 }] });
  app.elapse(80); await app.node('table').dispatch('touchend');
  assert.deepEqual(app.sceneCalls.taps, [[111, 222, 0]]);
  assert.equal(stale.faceUntil, 0, 'stale hover card is not flipped');
  assert.ok(tapped.faceUntil > 0, 'tap reaches the real flip action');
  await app.node('table').dispatch('touchstart', { touches: [{ clientX: 111, clientY: 222 }] });
  app.elapse(421); await app.node('table').dispatch('touchend');
  assert.deepEqual(app.sceneCalls.taps[1], [111, 222, 2]);
  assert.equal(app.ui.state.pendingGrab.cardId, tapped.id, 'hold reaches the real grab action');
  await app.node('table').dispatch('touchstart', { touches: [{ clientX: 10, clientY: 20 }] });
  await app.node('table').dispatch('touchmove', { touches: [{ clientX: 40, clientY: 60 }] });
  await app.node('table').dispatch('touchend');
  assert.deepEqual(app.sceneCalls.looks, [[21, 28]]);
  assert.equal(app.sceneCalls.taps.length, 2, 'dragging never also flips or takes a card');
});

test('AI scheduler waits through pause, unfinished exchange, and target selection', () => {
  const app = mount(); app.ui.startGame();
  const state = app.ui.state;
  app.ui.showPanel('pause'); app.ui.forceBotsDue(); app.tick();
  assert.equal(state.botTurns, 0);
  app.ui.showPanel(null);
  engine.grabCard(state, state.center[0].id); app.ui.forceBotsDue(); app.tick();
  assert.equal(state.botTurns, 0, 'rivals cannot steal the card in an unfinished exchange');
  engine.cancelGrab(state);
  app.ui.beginCast(putRecipe(state, 'dust-to-dust')); app.ui.forceBotsDue(); app.tick();
  assert.equal(state.botTurns, 0, 'target selection is atomic relative to rivals');
  app.ui.cancelTarget(); app.ui.forceBotsDue(); app.tick();
  assert.equal(state.botTurns, 3, 'each rival resumes one scheduled turn');
});

test('registered cast shortcut cannot burn another recipe while choosing a target', async () => {
  const app = mount(); app.ui.startGame();
  const state = app.ui.state, targeted = putRecipe(state, 'cinderstorm');
  app.ui.beginCast(targeted);
  // Make a different recipe ready to catch a missing guard independently of target validity.
  const hearthfire = putRecipe(state, 'hearthfire'), before = JSON.stringify(state.players[0]);
  await app.document.dispatch('keydown', { key: 'r', repeat: false });
  app.ui.beginCast(hearthfire);
  assert.equal(app.ui.targetRecipe.id, targeted.id);
  assert.equal(JSON.stringify(state.players[0]), before, 'no cards or points change before selecting the intended target');
  assert.equal(app.node('targetPanel').hidden, false);
});

test('restarting cancels the delayed results callback and stale callbacks cannot replace the intro/new table', () => {
  const app = mount(); app.ui.startGame();
  const state = app.ui.state; state.goal = 10; putRecipe(state, 'hearthfire');
  app.ui.act(engine.lockRecipe(state, 'hearthfire'));
  const timer = app.ui.resultsTimer, staleCallback = app.timers.get(timer).fn;
  assert.ok(timer);
  app.ui.showPanel('pause'); app.ui.newGame(true);
  assert.ok(app.cleared.includes(timer));
  assert.equal(app.ui.overlay, 'intro');
  app.elapse(2000); app.runTimers();
  assert.equal(app.ui.overlay, 'intro', 'old results timer cannot override difficulty selection');
  app.ui.startGame(); const fresh = app.ui.state;
  staleCallback();
  assert.equal(app.ui.state, fresh);
  assert.equal(app.ui.overlay, null, 'state identity guard also rejects an already queued callback');
  assert.equal(fresh.status, 'playing');
});

test('fullscreen button requests and exits fullscreen and updates its accessible state', async () => {
  const app = mount();
  await app.node('fullscreenBtn').dispatch('click');
  assert.deepEqual(app.fullscreenCounts(), [1, 0]);
  await app.document.dispatch('fullscreenchange');
  assert.equal(app.node('fullscreenBtn').textContent, 'Exit fullscreen');
  assert.equal(app.node('fullscreenBtn').attributes['aria-pressed'], 'true');
  await app.node('fullscreenBtn').dispatch('click');
  await app.document.dispatch('fullscreenchange');
  assert.deepEqual(app.fullscreenCounts(), [1, 1]);
  assert.equal(app.node('fullscreenBtn').textContent, 'Fullscreen');
  assert.equal(app.node('fullscreenBtn').attributes['aria-pressed'], 'false');
  assert.equal(app.sceneCalls.resizes, 2);
});
