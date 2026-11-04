'use strict';
// Run: node tests/command-ui-test.cjs [alternate-project-root]
// Exercises the shipped app's registered handlers and real BattleEngine without a browser.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');

class EventSurface {
  constructor() { this.listeners = new Map(); }
  addEventListener(name, callback) { if (!this.listeners.has(name)) this.listeners.set(name, []); this.listeners.get(name).push(callback); }
  dispatch(name, props = {}) {
    const event = { target: this, type: name, button: 0, pointerId: 1, pointerType: 'mouse', clientX: 0, clientY: 0,
      shiftKey: false, altKey: false, ctrlKey: false, metaKey: false, repeat: false, defaultPrevented: false,
      preventDefault() { this.defaultPrevented = true; }, stopPropagation() {}, ...props };
    for (const callback of this.listeners.get(name) || []) callback(event);
    return event;
  }
  async dispatchAsync(name, props = {}) {
    const event = { target: this, type: name, button: 0, pointerId: 1, pointerType: 'mouse', clientX: 0, clientY: 0,
      shiftKey: false, altKey: false, ctrlKey: false, metaKey: false, repeat: false, defaultPrevented: false,
      preventDefault() { this.defaultPrevented = true; }, stopPropagation() {}, ...props };
    await Promise.all((this.listeners.get(name) || []).map(callback => callback(event)));
    return event;
  }
}
class Element extends EventSurface {
  constructor(document, tag = 'div', attrs = {}) {
    super(); this.ownerDocument = document; this.tagName = tag.toUpperCase(); this.attributes = new Map(); this.dataset = {};
    this.children = []; this.parentElement = null; this.style = {}; this.hidden = false; this.disabled = false; this.open = false;
    this.value = ''; this.textContent = ''; this._html = ''; this.width = 80; this.height = 80;
    const classes = new Set();
    this.classList = { add: (...names) => names.forEach(n => classes.add(n)), remove: (...names) => names.forEach(n => classes.delete(n)),
      contains: name => classes.has(name), toggle(name, force) { const on = force === undefined ? !classes.has(name) : !!force; if (on) classes.add(name); else classes.delete(name); return on; } };
    for (const [name, value] of Object.entries(attrs)) this.setAttribute(name, value);
  }
  setAttribute(name, value) {
    this.attributes.set(name, String(value));
    if (name === 'id') this.id = value;
    if (name === 'class') for (const c of String(value).split(/\s+/)) if (c) this.classList.add(c);
    if (name.startsWith('data-')) this.dataset[name.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] = String(value);
    if (['value', 'type', 'title'].includes(name)) this[name] = String(value);
    if (['hidden', 'disabled', 'open'].includes(name)) this[name] = true;
  }
  getAttribute(name) { return this.attributes.has(name) ? this.attributes.get(name) : null; }
  removeAttribute(name) { this.attributes.delete(name); if (['hidden', 'disabled', 'open'].includes(name)) this[name] = false; }
  set innerHTML(value) { this._html = String(value); this.children = this.ownerDocument.parse(this._html, this); }
  get innerHTML() { return this._html; }
  appendChild(child) { child.parentElement = this; this.children.push(child); this.ownerDocument.register(child); return child; }
  replaceChildren(...children) { this.children = []; for (const c of children) this.appendChild(c); }
  get firstElementChild() { return this.children[0] || null; }
  remove() { if (this.parentElement) this.parentElement.children = this.parentElement.children.filter(c => c !== this); this.removed = true; }
  contains(node) { for (let n = node; n; n = n.parentElement) if (n === this) return true; return false; }
  focus() { this.ownerDocument.activeElement = this; }
  click() { if (!this.disabled) this.dispatch('click'); }
  setPointerCapture() {} releasePointerCapture() {}
  getBoundingClientRect() { return { left: 0, top: 0, right: 6400, bottom: 4400, width: 6400, height: 4400 }; }
  querySelector(selector) {
    if (selector === 'span:nth-child(2)') return this.children.filter(c => c.tagName === 'SPAN')[1] || this.appendChild(new Element(this.ownerDocument, 'span'));
    return this.children.find(e => this.ownerDocument.matches(e, selector)) || null;
  }
  querySelectorAll(selector) { return this.children.filter(e => this.ownerDocument.matches(e, selector)); }
  closest(selector) { for (let n = this; n; n = n.parentElement) if (this.ownerDocument.matches(n, selector)) return n; return null; }
  showModal() { this.open = true; }
  close() { this.open = false; }
}
class Document extends EventSurface {
  constructor(html) {
    super(); this.elements = []; this.byId = new Map(); this.hidden = false; this.activeElement = null;
    this.body = new Element(this, 'body'); this.documentElement = new Element(this, 'html'); this.parse(html, this.body);
  }
  register(element) { if (!this.elements.includes(element)) this.elements.push(element); if (element.id) this.byId.set(element.id, element); return element; }
  parse(html, parent) {
    const created = [];
    for (const match of html.matchAll(/<([a-z][a-z0-9-]*)\b([^>]*?)>/gi)) {
      const attrs = {};
      for (const a of match[2].matchAll(/([^\s=/>]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+)))?/g)) attrs[a[1]] = a[2] ?? a[3] ?? a[4] ?? '';
      const el = new Element(this, match[1], attrs); el.parentElement = parent; this.register(el); created.push(el);
    }
    return created;
  }
  createElement(tag) { return new Element(this, tag); }
  getElementById(id) { const found = this.byId.get(id); if (!found) throw new Error('Missing actual HTML element #' + id); return found; }
  matches(el, selector) {
    if (el.removed) return false;
    if (selector.includes(',')) return selector.split(',').some(s => this.matches(el, s.trim()));
    if (selector[0] === '#') return el.id === selector.slice(1);
    const m = selector.match(/^([a-z0-9-]+)?(?:\[([^=\]]+)(?:=["']?([^"'\]]+)["']?)?\])?$/i);
    if (!m) return false;
    if (m[1] && el.tagName !== m[1].toUpperCase()) return false;
    if (m[2]) { if (m[2] === 'open') return el.open; if (!el.attributes.has(m[2])) return false; if (m[3] !== undefined && el.getAttribute(m[2]) !== m[3]) return false; }
    return true;
  }
  querySelectorAll(selector) { return this.elements.filter(e => this.matches(e, selector)); }
  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
}

function bootUI(root, options = {}) {
  const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
  const document = new Document(html), window = new EventSurface();
  let nextTimer = 0, frame = null;
  const errors = [], storage = new Map(), timeouts = new Map();
  if(options.preferences)storage.set('ironfield.preferences.v1',JSON.stringify(options.preferences));
  class Renderer {
    constructor(canvas, engine, preferences) { this.canvas = canvas; this.engine = engine; this.options = { ...preferences }; this.camera = { x: 3200, y: 2200, zoom: 1 }; }
    setEngine(e) { this.engine = e; } resize() {} fit() {} drawUnitIcon() {} render() {this.renderCalls=(this.renderCalls||0)+1;} renderMinimap() {this.minimapCalls=(this.minimapCalls||0)+1;}
    screenToWorld(x, y) { return { x, y }; } focus(x, y, zoom) { this.camera.x = x; this.camera.y = y; if (zoom) this.camera.zoom = zoom; }
    pan(x, y) { this.camera.x += x; this.camera.y += y; } zoomAt(factor) { this.camera.zoom *= factor; }
  }
  Object.assign(window, { document, innerWidth: 1400, devicePixelRatio: 1, BattleRenderer: Renderer });
  const context = { window, document, console: { ...console, error: (...args) => errors.push(args.join(' ')) },
    performance: { now: () => 0 }, localStorage: { getItem: k => storage.get(k) || null, setItem: (k, v) => storage.set(k, String(v)) },
    setTimeout(callback) { const id = ++nextTimer; timeouts.set(id, callback); return id; }, clearTimeout(id) { timeouts.delete(id); },
    requestAnimationFrame(callback) { frame = callback; }, location: { reload() {} }, BattleRenderer: Renderer };
  window.setTimeout = context.setTimeout; window.clearTimeout = context.clearTimeout;
  const sandbox = vm.createContext(context);
  const scriptPath = name => fs.existsSync(path.join(root, 'src', name)) ? path.join(root, 'src', name) : path.join(root, name);
  vm.runInContext(fs.readFileSync(scriptPath('data.js'), 'utf8'), sandbox, { filename: 'data.js' });
  context.GameData = window.GameData;
  if (fs.existsSync(scriptPath('tactics.js'))) {
    vm.runInContext(fs.readFileSync(scriptPath('tactics.js'), 'utf8'), sandbox, { filename: 'tactics.js' });
    context.BattleTactics = window.BattleTactics;
  }
  for (const [name, symbol] of [['impacts.js', 'BattleImpacts'], ['siege.js', 'BattleSiege']]) {
    if (fs.existsSync(scriptPath(name))) {
      vm.runInContext(fs.readFileSync(scriptPath(name), 'utf8'), sandbox, { filename: name });
      context[symbol] = window[symbol];
    }
  }
  vm.runInContext(fs.readFileSync(scriptPath('engine.js'), 'utf8'), sandbox, { filename: 'engine.js' });
  context.BattleEngine = window.BattleEngine;
  vm.runInContext(fs.readFileSync(scriptPath('app.js'), 'utf8'), sandbox, { filename: 'app.js' });
  assert(window.IRONFIELD, errors.join('\n') || 'Real app boot failed');
  assert.equal(errors.length, 0, errors.join('\n'));
  // Existing command tests begin in deployment through the real new menu.
  // Menu-specific tests can keep startup on screen with {menu:true}.
  if(!options.menu)document.getElementById('menuDeploy').click();
  const canvas = document.getElementById('battlefield');
  const key = (key, props = {}) => window.dispatch('keydown', { key, target: canvas, ...props });
  const point = (x, y, props = {}) => { canvas.dispatch('pointerdown', { clientX: x, clientY: y, ...props }); canvas.dispatch('pointerup', { clientX: x, clientY: y, ...props }); };
  const select = (regiment, additive = false) => { const u = window.IRONFIELD.engine.units[regiment.unitIds[0]]; point(u.x, u.y, { shiftKey: additive }); };
  const engine = window.IRONFIELD.engine, calls = [], acceptedOrders = [], rawCommand = engine.command.bind(engine);
  engine.command = (...args) => { calls.push(args); const accepted = rawCommand(...args); acceptedOrders.push(accepted); return accepted; };
  const flushTimers = () => {
    let rounds = 0;
    while (timeouts.size) {
      assert(++rounds <= 100, 'Interface timers failed to settle');
      const pending = [...timeouts]; timeouts.clear();
      for (const [, callback] of pending) callback();
    }
  };
  return { document, window, get engine() { return window.IRONFIELD.engine; }, calls, acceptedOrders, key, point, select,
    storage, flushTimers, frame: now => frame(now), toast: () => document.getElementById('toastArea').children.map(e => e.textContent),
    button: command => document.querySelector(`[data-command="${command}"]`) };
}

// Real interface handlers run against the real engine and the actual HTML IDs.
// Canvas drawing and browser layout are deliberately outside this input test.
const ROOT = process.argv[2] || path.resolve(__dirname, '..');
const tests = [];
const pendingTests = [];
function test(name, body) {
  if (process.env.IRONFIELD_UI_FILTER && !name.toLowerCase().includes(process.env.IRONFIELD_UI_FILTER.toLowerCase())) return;
  const success = result => { tests.push({ name, passed: true, result }); console.log('PASS ' + name); };
  const failure = error => { tests.push({ name, passed: false, error: error.message }); console.log('FAIL ' + name + ': ' + error.message); process.exitCode = 1; };
  try { const result = body(); if (result && typeof result.then === 'function') pendingTests.push(result.then(success, failure)); else success(result); }
  catch (error) { failure(error); }
}
function battle() {
  const ui = bootUI(ROOT);
  const selected = ui.engine.regiments.find(r => r.team === 0 && r.type === 'swordsman');
  ui.select(selected); ui.key(' ');
  assert.equal(ui.engine.phase, 'running');
  ui.selected = selected;
  return ui;
}
function lastMessage(ui) { return ui.toast().at(-1) || ''; }
function lastOrder(ui) { assert(ui.calls.length, 'No real engine command was dispatched'); return ui.calls.at(-1); }
function orderText(ui) { return ui.document.getElementById('selectedOrder').textContent; }
function control(ui, id, value) { const el = ui.document.getElementById(id); el.value = value; el.dispatch('change'); return el; }
function paint(ui, type, x1, y1, x2 = x1, y2 = y1) {
  const tool = ui.document.querySelector(`[data-terrain-tool="${type}"]`), canvas = ui.document.getElementById('battlefield');
  assert(tool, 'Missing actual terrain tool ' + type); tool.click();
  canvas.dispatch('pointerdown', { clientX: x1, clientY: y1 });
  if (x2 !== x1 || y2 !== y1) canvas.dispatch('pointermove', { clientX: x2, clientY: y2 });
  canvas.dispatch('pointerup', { clientX: x2, clientY: y2 });
}
function placeCompany(ui, team, type, count, x, y) {
  ui.document.getElementById(team ? 'editRed' : 'editBlue').click();
  control(ui, 'placeTroop', type); ui.document.getElementById('placeCount').value = String(count);
  ui.document.getElementById('placeFormationButton').click(); ui.point(x, y);
}
function rosterCount(ui, type, count) {
  const list = ui.document.getElementById('armyList'), input = list.querySelector(`[data-roster="${type}"]`);
  assert(input, 'Missing actual roster control ' + type); input.value = String(count); list.dispatch('change', { target: input });
}
function storedScenarios(ui) {
  for (const [key, value] of ui.storage) {
    let parsed; try { parsed = JSON.parse(value); } catch (_) { continue; }
    if (Array.isArray(parsed) && parsed.some(s => s.format === 'ironfield-scenario')) return { key, scenarios: parsed };
  }
  throw new Error('The real Save handler did not store any scenario');
}
function actualFormationGeometry(ui) {
  return ui.engine.regiments.map(r => ({ team: r.team, type: r.type,
    positions: r.unitIds.map(id => ({ x: ui.engine.units[id].x, y: ui.engine.units[id].y })) }));
}
function assertFormationGeometry(ui, expected, message, allowAdditional = false) {
  const actual = actualFormationGeometry(ui);
  if (allowAdditional) assert(actual.length >= expected.length, message + ': existing groups disappeared');
  else assert.equal(actual.length, expected.length, message + ': group count changed');
  expected.forEach((group, i) => {
    assert.equal(actual[i].team, group.team); assert.equal(actual[i].type, group.type);
    assert.equal(actual[i].positions.length, group.positions.length, message + ': existing membership changed');
    group.positions.forEach((p, j) => assert(Math.hypot(p.x - actual[i].positions[j].x, p.y - actual[i].positions[j].y) < 1e-6,
      `${message}: group ${i}, soldier ${j} moved during reconstruction`));
  });
}
function assertSeparatedBodies(ui, message) {
  const units = ui.engine.units.filter(u => u.alive && !u.routed), types = ui.window.GameData.TYPES;
  let minimum = Infinity;
  for (const u of units) {
    assert(Number.isFinite(u.x) && Number.isFinite(u.y), message + ': invalid coordinates');
    assert.equal(ui.engine.terrainAt(u.x, u.y).blocked, false, message + ': troop inside blocked terrain');
  }
  for (let i = 0; i < units.length; i++) for (let j = i + 1; j < units.length; j++) {
    const a = units[i], b = units[j];
    minimum = Math.min(minimum, Math.hypot(a.x - b.x, a.y - b.y) / (types[a.type].radius + types[b.type].radius));
  }
  assert(minimum >= .90, `${message}: bodies overlap (${minimum.toFixed(3)} of combined radii)`);
  return minimum;
}

if (require.main === module) {
  test('Troop category tabs filter the roster and placement picker while preserving both armies', () => {
    const ui=bootUI(ROOT),doc=ui.document;
    doc.getElementById('newEncounter').click();
    const list=doc.getElementById('armyList'),picker=doc.getElementById('placeTroop');
    const cards=()=>list.querySelectorAll('[data-roster]').map(el=>el.dataset.roster);
    const tabs=doc.querySelectorAll('[data-troop-category]');
    const seen=new Set(),total=ui.window.GameData.TYPE_ORDER.length;
    assert.equal(cards().length,total);
    for(const tab of tabs.filter(t=>t.dataset.troopCategory!=='all')){
      tab.click(); const ids=cards();
      assert(ids.length>0&&ids.length<total);
      assert.deepEqual(picker.querySelectorAll('option').map(el=>el.value),ids);
      assert.equal(list.getAttribute('aria-labelledby'),tab.id);
      const required={infantry:'berserker',ranged:'musketeer',cavalry:'horseArcher',siege:'trebuchet'};
      assert(ids.includes(required[tab.dataset.troopCategory]));
      for(const id of ids){assert(!seen.has(id));seen.add(id);}
    }
    assert.equal(seen.size,total); assert(cards().includes('cannon'));
    rosterCount(ui,'cannon',3); ui.flushTimers();
    doc.getElementById('editRed').click(); rosterCount(ui,'trebuchet',1); ui.flushTimers();
    doc.getElementById('troopTabRanged').click(); rosterCount(ui,'musketeer',20); ui.flushTimers();
    const cfg=ui.window.IRONFIELD.config;
    assert.equal(cfg.armies[0].roster.cannon,3); assert.equal(cfg.armies[1].roster.trebuchet,1);
    assert.equal(cfg.armies[1].roster.musketeer,20); assert.equal(doc.getElementById('armyCount').textContent,'21');
    const tab=doc.getElementById('troopTabRanged'); tab.dispatch('keydown',{key:'ArrowRight'});
    assert.equal(doc.activeElement.id,'troopTabSiege'); assert(cards().includes('cannon'));
    doc.getElementById('troopTabAll').click(); assert.equal(cards().length,total);
    return {categories:4,uniqueTypes:total,blueCannons:3,redMusketeers:20};
  });

  test('Thunder at Stonekeep loads all four weapons and shows real battery reload time', () => {
    const ui=bootUI(ROOT,{menu:true}),doc=ui.document;
    const gallery=doc.getElementById('menuMapGrid');
    gallery.dispatch('click',{target:gallery.querySelector('[data-menu-map="stonekeep"]')});
    control(ui,'menuScenarioSelect','thunderAtStonekeep');
    doc.getElementById('menuQuick').click();
    const e=ui.engine;
    assert.equal(e.config.map,'stonekeep'); assert.equal(e.phase,'running');
    for(const type of ['berserker','musketeer','cannon','trebuchet'])assert(e.units.some(u=>u.type===type));
    const battery=e.regiments.find(r=>r.type==='trebuchet'),engines=battery.unitIds.map(id=>e.units[id]);
    assert.equal(engines.length,2); engines[0]._cooldown=0; engines[1]._cooldown=12.3;
    ui.select(battery);
    const label=doc.getElementById('selectedReload');assert(!label.hidden);assert.equal(label.textContent,'1/2 loaded · Next ready in 12.3 s');
    ui.select(e.regiments.find(r=>r.type==='berserker'));assert(label.hidden);
    return {map:e.map.id,newWeapons:4};
  });

  test('An armed Move survives a right-click destination', () => {
    const ui = battle(); ui.key('m');
    assert.equal(ui.document.getElementById('commandHint').hidden, false);
    ui.point(2000, 2300, { button: 2 });
    const call = lastOrder(ui);
    assert.equal(call[1], 'move', 'The armed Move was silently changed into Attack');
    assert.equal(call[2], 2000); assert.equal(call[3], 2300);
    assert.equal(ui.selected.order, 'move');
    assert.match(lastMessage(ui), /move|moving/i, 'A successful point order had no useful acknowledgement');
    assert.match(orderText(ui), /move|moving/i, 'The selected order did not update immediately');
    assert.doesNotMatch(orderText(ui), /following (?:the )?(?:army )?battle plan/i, 'A manual Move still displays the automatic order status');
    return { dispatchedOrder: call[1], selectedOrder: orderText(ui) };
  });

  test('Right-click marches to open ground and attacks at a hostile position; A still attack-moves', () => {
    const ui = battle(); ui.point(2000, 2300, { button: 2 });
    assert.equal(lastOrder(ui)[1], 'move');
    assert.equal(ui.selected.order, 'move');
    const enemy = ui.engine.units.find(u => u.team !== ui.selected.team);
    ui.point(enemy.x, enemy.y, { button: 2 });
    assert.equal(lastOrder(ui)[1], 'attack');
    assert.equal(ui.selected.order, 'attack');
    ui.key('a'); ui.point(2000, 2300, { button: 2 });
    assert.equal(lastOrder(ui)[1], 'attack');
    return { dispatchedOrder: lastOrder(ui)[1] };
  });

  test('C charges immediately and retains visible command feedback', () => {
    const ui = battle(), before = ui.calls.length;
    ui.key('c');
    assert.equal(ui.calls.length, before + 1, 'Charge required an extra destination click');
    assert.equal(lastOrder(ui)[1], 'charge');
    assert.equal(ui.selected.order, 'charge');
    assert(ui.selected.commandTargetId >= 0, 'Charge did not expose a committed enemy target');
    assert.match(orderText(ui), /charg/i);
    assert.equal(ui.document.getElementById('selectedOrder').hidden, false, 'The accepted order feedback is hidden');
    assert.doesNotMatch(orderText(ui), /following (?:the )?(?:army )?battle plan/i, 'A manual Charge still displays the automatic order status');
    assert(orderText(ui).includes(ui.selected.commandTargetName), 'Selected order omits the committed target');
    assert.equal(ui.button('charge').classList.contains('active'), true, 'Charge button lost its active state');
    assert.equal(ui.document.getElementById('commandHint').hidden, true, 'Ordinary Charge left a target picker armed');
    return { target: ui.selected.commandTargetName, selectedOrder: orderText(ui) };
  });

  test('Orders issued while paused are acknowledged without advancing troops', () => {
    const ui = battle(); ui.key(' ');
    assert.equal(ui.engine.phase, 'paused');
    const positions = ui.engine.units.map(u => [u.x, u.y]), time = ui.engine.time;
    ui.key('c');
    assert.equal(ui.selected.order, 'charge');
    assert.equal(ui.engine.phase, 'paused'); assert.equal(ui.engine.time, time);
    assert.equal(JSON.stringify(ui.engine.units.map(u => [u.x, u.y])), JSON.stringify(positions));
    assert.match(orderText(ui) + ' ' + lastMessage(ui), /paused|resume/i, 'The UI did not explain when a paused order would execute');
    assert.match(orderText(ui), /charg/i);
    assert.doesNotMatch(orderText(ui), /following (?:the )?(?:army )?battle plan/i, 'Paused command feedback was not synchronized with the accepted order');
    return { phase: ui.engine.phase, selectedOrder: orderText(ui) };
  });

  test('Shift+C explicitly targets a clicked hostile formation', () => {
    const ui = battle(), before = ui.calls.length;
    const enemy = ui.engine.regiments.find(r => r.team === 1 && r.type === 'archer');
    const target = ui.engine.units[enemy.unitIds[0]];
    ui.key('C', { shiftKey: true });
    assert.equal(ui.calls.length, before, 'The explicit target shortcut issued a charge before a target was chosen');
    assert.equal(ui.document.getElementById('commandHint').hidden, false);
    assert.equal(ui.document.getElementById('targetChargeButton').classList.contains('active'), true);
    ui.point(target.x, target.y);
    const call = lastOrder(ui);
    assert.equal(call[1], 'charge');
    assert.equal(call[4].targetRegimentId, enemy.id, 'The clicked enemy was not sent to the real engine');
    assert.equal(ui.selected.commandTargetId, enemy.id);
    assert.equal(ui.document.getElementById('commandHint').hidden, true);
    assert(orderText(ui).includes(enemy.name));
    return { clickedEnemy: enemy.name, targetId: ui.selected.commandTargetId };
  });

  test('The Target button rejects friendly formations without losing the pending order', () => {
    const ui = battle(), before = ui.calls.length;
    const friend = ui.engine.regiments.find(r => r.team === 0 && r.type === 'archer');
    const target = ui.engine.units[friend.unitIds[0]];
    ui.document.getElementById('targetChargeButton').click();
    assert.equal(ui.document.getElementById('commandHint').hidden, false, 'The Target button did not arm the target picker');
    ui.point(target.x, target.y);
    assert.equal(ui.calls.length, before, 'A friendly click dispatched an attack order');
    assert.equal(ui.document.getElementById('commandHint').hidden, false, 'Invalid targeting silently discarded the target picker');
    assert.match(lastMessage(ui), /enemy|hostile/i);
    return { acknowledgement: lastMessage(ui) };
  });

  test('Escape cancels a pending destination while preserving the selection', () => {
    const ui = battle(); ui.key('m'); ui.key('Escape');
    assert.equal(ui.document.getElementById('commandHint').hidden, true);
    assert.equal(ui.document.getElementById('selectionPanel').hidden, false, 'Cancelling the picker also discarded the selection');
    ui.key('c'); assert.equal(lastOrder(ui)[1], 'charge');
    ui.key('Escape'); assert.equal(ui.document.getElementById('selectionPanel').hidden, true);
    return { cancelledModeKeptSelection: true, secondEscapeDeselected: true };
  });

  test('Mixed ready and retreating selections report partial acceptance', () => {
    const ui = battle(); ui.key('r');
    assert.equal(ui.selected.fallingBack, true);
    const ready = ui.engine.regiments.find(r => r.team === 0 && r.type === 'archer');
    ui.select(ready, true); ui.key('c');
    assert.equal(ui.selected.fallingBack, true);
    assert.equal(ready.order, 'charge');
    assert.match(lastMessage(ui), /1\s*(?:\/|of)\s*2/i, 'Acknowledgement implied every selected formation accepted the order');
    assert.match(lastMessage(ui), /regroup|recover|withdraw|falling|ready/i);
    return { acknowledgement: lastMessage(ui) };
  });

  test('Changing formation preserves the order and returns keyboard control to the battlefield', () => {
    const ui = battle(), select = ui.document.getElementById('formationSelect');
    ui.key('m'); ui.point(2000, 2300);
    assert.equal(ui.selected.order, 'move');
    select.focus(); select.value = 'wedge'; select.dispatch('change');
    assert.equal(ui.selected.formation, 'wedge');
    assert.equal(ui.selected.order, 'move', 'Changing formation discarded the current movement order');
    assert.match(orderText(ui), /move|moving/i, 'Changing formation hid or replaced the displayed order');
    assert.doesNotMatch(orderText(ui), /following (?:the )?(?:army )?battle plan/i);
    assert.equal(ui.document.activeElement && ui.document.activeElement.id, 'battlefield', 'Formation selector retained focus and swallowed combat hotkeys');
    ui.key('c', { target: ui.document.activeElement });
    assert.equal(lastOrder(ui)[1], 'charge');
    assert.equal(ui.selected.order, 'charge');
    return { formation: ui.selected.formation, resultingOrder: ui.selected.order };
  });

  test('Retreat starts moving on the next step and repeated orders preserve the withdrawal', () => {
    const ui=battle(), r=ui.selected, positions=ui.engine.units.map(u=>[u.x,u.y,u.angle]);
    ui.key('r');
    assert.equal(lastOrder(ui)[1],'retreat'); assert.equal(ui.acceptedOrders.at(-1),1);
    assert.equal(r.order,'retreat'); assert.equal(r.commandStatus,'withdrawing'); assert.equal(r.commandTurnRemaining,0);
    assert.deepEqual(ui.engine.units.map(u=>[u.x,u.y,u.angle]),positions);
    assert.match(orderText(ui),/withdraw/i); assert(ui.button('retreat').classList.contains('active'));
    const epoch=r._commandEpoch;
    ui.engine.step(1/30);
    const moved=r.unitIds.filter(id=>{
      const u=ui.engine.units[id],p=positions[id];
      return (u.x-p[0])*r._retreatNX+(u.y-p[1])*r._retreatNY>.03;
    }).length;
    assert(moved>=r.alive*.8,'The formation waited for a turn before stepping away');
    ui.button('retreat').click(); ui.key('r');
    assert.equal(r._commandEpoch,epoch); assert.equal(r.commandStatus,'withdrawing');
    assert.equal(r.commandTurnRemaining,0);
    assert.doesNotMatch(lastMessage(ui),/could not|must recover|no formation/i);
    return { movedFirstStep:moved, soldiers:r.alive };
  });

  test('Paused Retreat holds all troops still and starts withdrawing on resume', () => {
    const ui=battle(); ui.key(' ');
    const positions=ui.engine.units.map(u=>[u.x,u.y,u.angle]),time=ui.engine.time;
    ui.key('r'); assert.match(orderText(ui),/paused/i); assert.match(orderText(ui),/withdraw/i);
    assert.match(lastMessage(ui),/queued|resume/i);
    for(let i=0;i<30;i++)ui.engine.step(1/30);
    assert.equal(ui.engine.time,time); assert.deepEqual(ui.engine.units.map(u=>[u.x,u.y,u.angle]),positions);
    ui.key('r'); assert.equal(ui.acceptedOrders.at(-1),1);
    ui.key(' '); ui.engine.step(1/30);
    assert.equal(ui.selected.commandStatus,'withdrawing');
    assert(ui.selected.unitIds.some(id=>Math.hypot(ui.engine.units[id].x-positions[id][0],ui.engine.units[id].y-positions[id][1])>.05));
    return { resumedOrder:orderText(ui) };
  });

  test('Retreat overrides a real commander-rally regrouping phase through the R handler', () => {
    const ui = bootUI(ROOT), troops = ui.engine.regiments.find(r => r.team === 0 && r.type === 'swordsman');
    const commander = ui.engine.regiments.find(r => r.team === 0 && r.type === 'commander');
    ui.engine.relocateRegiments([troops.id], 2800, 2200);
    ui.engine.relocateRegiments([commander.id], 2600, 2200);
    ui.engine.command([commander.id], 'hold');
    ui.select(troops); ui.key(' '); ui.key('r');
    for (let i = 0; i < 100; i++) ui.engine.step(1 / 30);
    assert.equal(troops.fallingBack, true, 'The selected troops did not remain in their initial withdrawal');
    assert.equal(ui.engine.command([commander.id], 'rally'), 1, 'The public commander rally did not activate');
    assert.equal(troops.commandStatus, 'regrouping', 'The fixture did not enter actual commander-supported regrouping');
    ui.frame(250);
    assert.match(orderText(ui), /regroup/i);
    ui.key('r');
    assert.equal(lastOrder(ui)[1], 'retreat');
    assert.equal(ui.acceptedOrders.at(-1), 1, 'Regrouping incorrectly prevented the new Retreat');
    assert.equal(troops.fallingBack, true);
    assert.equal(troops.order, 'retreat');
    assert.equal(troops.commandStatus, 'withdrawing');
    assert.match(orderText(ui), /withdraw/i);
    for (let i = 0; i < 30; i++) ui.engine.step(1 / 30);
    assert.equal(troops.commandStatus, 'withdrawing', 'The old regrouping phase overrode the accepted withdrawal');
    return { retreatAcceptedDuringRegrouping: true, finalStatus: troops.commandStatus };
  });

  test('Custom encounter selectors clear both armies, all terrain and a pending roster rebuild', () => {
    const outcomes = [];
    for (const entry of ['preset', 'map', 'button']) {
      const ui = bootUI(ROOT); assert(ui.engine.units.length > 0 && ui.engine.map.regions.length > 0);
      rosterCount(ui, 'swordsman', 900); // Leave its debounced rebuild pending.
      if (entry === 'preset') control(ui, 'presetSelect', 'custom');
      else if (entry === 'map') control(ui, 'mapSelect', 'custom');
      else ui.document.getElementById('newEncounter').click();
      ui.flushTimers();
      assert.equal(ui.engine.map.id, 'custom'); assert.equal(ui.engine.units.length, 0);
      assert.equal(ui.engine.regiments.length, 0); assert.equal(ui.engine.map.regions.length, 0);
      assert(ui.engine.getStats().teams.every(s => s.initial === 0 && s.alive === 0));
      assert.equal(ui.document.getElementById('encounterEditor').hidden, false);
      assert.equal(ui.document.getElementById('controlSelect').value, 'both');
      ui.key(' '); assert.equal(ui.engine.phase, 'deployment', 'The empty encounter started and immediately declared a victory');
      outcomes.push({ entry, troops: ui.engine.units.length, terrain: ui.engine.map.regions.length });
    }
    const ui = battle(); ui.engine.step(1 / 30); ui.document.getElementById('newEncounter').click();
    assert.equal(ui.engine.phase, 'deployment'); assert.equal(ui.engine.time, 0);
    assert.equal(ui.engine.units.length + ui.engine.projectiles.length + ui.engine.effects.length, 0);
    return { selectors: outcomes, liveBattleCleared: true };
  });

  test('Custom terrain tools paint, undo, erase and create a traversable bridge', () => {
    const ui = bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    control(ui, 'terrainBiome', 'woodland'); assert.equal(ui.engine.map.biome, 'woodland');
    paint(ui, 'forest', 1000, 1000, 1400, 1300);
    assert.equal(ui.engine.map.regions.length, 1); assert.equal(ui.engine.terrainAt(1200, 1150).type, 'forest');
    const forest = ui.engine.map.regions[0]; assert.deepEqual([forest.x, forest.y, forest.w, forest.h], [1000, 1000, 400, 300]);
    paint(ui, 'rock', 2000, 1500, 2200, 1700); assert.equal(ui.engine.terrainAt(2100, 1600).blocked, true);
    ui.document.getElementById('undoTerrain').click(); assert.equal(ui.engine.map.regions.length, 1);
    paint(ui, 'bridge', 3120, 2200); assert.equal(ui.engine.map.regions.length, 1, 'A floating bridge was accepted without water');
    paint(ui, 'water', 3000, 400, 3240, 3600); assert.equal(ui.engine.terrainAt(3120, 2200).blocked, true);
    paint(ui, 'bridge', 3120, 2200); assert.equal(ui.engine.terrainAt(3120, 2200).type, 'bridge');
    assert.equal(ui.engine.terrainAt(3120, 2200).blocked, false);
    assert.equal(ui.engine.terrainAt(3120, 1200).blocked, true);
    placeCompany(ui, 0, 'swordsman', 1, 3120, 2200);
    assert.equal(ui.engine.units.length, 1);
    const bridgeSoldier = ui.engine.units[0], bridgePosition = [bridgeSoldier.x, bridgeSoldier.y];
    for (let attempt = 0; attempt < 2; attempt++) {
      ui.document.getElementById('undoTerrain').click();
      assert.equal(ui.engine.map.regions.length, 3, 'Undo removed terrain beneath a deployed soldier or lost its history after refusal');
      assert.equal(ui.engine.terrainAt(...bridgePosition).type, 'bridge');
      assert.equal(ui.engine.units[0], bridgeSoldier, 'Refused terrain change rebuilt the army');
      assert.deepEqual([bridgeSoldier.x, bridgeSoldier.y], bridgePosition, 'Removing a bridge silently moved the soldier to shore');
      assert.match(lastMessage(ui), /move.*clear/i);
    }
    paint(ui, 'erase', 1200, 1150); assert(!ui.engine.map.regions.some(r => r.type === 'forest'));
    ui.document.getElementById('undoTerrain').click(); assert(ui.engine.map.regions.some(r => r.type === 'forest'));
    ui.document.getElementById('clearTerrain').click(); assert.equal(ui.engine.map.regions.length, 0);
    ui.document.getElementById('undoTerrain').click(); assert.equal(ui.engine.map.regions.length, 3);
    ui.document.getElementById('finishTerrain').click(); assert.equal(ui.document.getElementById('commandHint').hidden, true);
    return { restoredAreas: ui.engine.map.regions.length, rectangularPaint: true, bridgePassable: true, occupiedBridgeUndoRefused: true };
  });

  test('The Castle tool builds an owned enclosure in one drag and undoes the whole placement', () => {
    const ui=bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    control(ui,'terrainDefender','0'); placeCompany(ui,0,'swordsman',12,2500,2100);
    paint(ui,'castle',2000,1500,3200,2700);
    const structures=ui.engine.structures, gate=structures.find(s=>s.type==='gate');
    assert.equal(structures.filter(s=>s.castleTower).length,4);
    assert.equal(gate.x,2000); assert(structures.every(s=>s.team===0));
    assert(!ui.engine.terrainAt(2500,2100).blocked);
    for(const [x,y] of [[2500,1510],[2500,2690],[3190,2100],[2010,2100]])assert(ui.engine.terrainAt(x,y).blocked);
    const count=structures.length;
    paint(ui,'castle',2000,1500,3200,2700);
    assert.equal(ui.engine.structures.length,count,'Overlapping castle walls were accepted');
    ui.document.getElementById('undoTerrain').click();
    assert.equal(ui.engine.structures.length,0); assert.equal(ui.engine.units.length,12);
    paint(ui,'castle',4400,3000);
    assert.equal(ui.engine.structures.filter(s=>s.castleTower).length,4);
    ui.document.getElementById('saveScenario').click();
    const scenario=storedScenarios(ui).scenarios[0];
    assert.equal(scenario.config.customMap.regions.filter(s=>s.castleTower).length,4,'Save stripped corner towers');
    return { cornerTowers:4,dragAndClick:true,saved:true };
  });

  test('Defenders operate castle gates using the battlefield controls', () => {
    const ui=bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    control(ui,'terrainDefender','0'); paint(ui,'castle',3000,2200);
    placeCompany(ui,0,'guardian',8,3000,2200);
    placeCompany(ui,1,'militia',8,1600,2200);
    const guard=ui.engine.regiments.find(r=>r.team===0);
    ui.select(guard); ui.key(' '); ui.engine.command([guard.id],'hold');
    const gate=ui.engine.structures.find(s=>s.castleGate),x=gate.x+gate.w/2,y=gate.y+gate.h/2;
    ui.point(x,y,{button:2}); assert(gate.open); assert(!ui.engine.terrainAt(x,y).blocked);
    assert.match(lastMessage(ui),/opened/i); assert.equal(guard.order,'hold');
    ui.point(x,y,{pointerType:'touch'}); assert(!gate.open); assert(ui.engine.terrainAt(x,y).blocked);
    return { openedByMouse:true,closedByTouch:true };
  });

  test('Encounter placement adds the chosen army and refuses occupied or blocked ground', () => {
    const ui = bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    paint(ui, 'rock', 3000, 1800, 3400, 2200);
    placeCompany(ui, 0, 'elephant', 1, 2988, 2000);
    assert.equal(ui.engine.units.length, 0, 'A large body was placed outside a rock center but intersecting its edge');
    placeCompany(ui, 0, 'elephant', 1, 2960, 2000);
    assert.equal(ui.engine.units.length, 1, 'A large troop with full body clearance was rejected');
    assert.equal(ui.engine.units[0].type, 'elephant');
    ui.document.getElementById('newEncounter').click(); paint(ui, 'rock', 3000, 1800, 3400, 2200);
    placeCompany(ui, 0, 'javelin', 80, 3200, 2000); assert.equal(ui.engine.units.length, 0, 'Troops were inserted into solid rocks');
    placeCompany(ui, 0, 'javelin', 80, 2400, 2400);
    assert.equal(ui.engine.units.length, 80); assert.equal(ui.engine.regiments.length, 1);
    assert.equal(ui.engine.regiments[0].type, 'javelin'); assert.equal(ui.engine.regiments[0].team, 0);
    placeCompany(ui, 1, 'crossbow', 48, 2400, 2400); assert.equal(ui.engine.units.length, 80, 'A second army overlapped the first formation');
    placeCompany(ui, 1, 'crossbow', 48, 3800, 2400);
    assert.equal(ui.engine.units.length, 128); assert.equal(ui.engine.getStats().teams[1].initial, 48);
    const before = ui.engine.map.regions.length;
    paint(ui, 'water', 2300, 2250, 2500, 2550);
    assert.equal(ui.engine.map.regions.length, before, 'Blocking terrain was painted through living soldiers');
    ui.key('Escape'); ui.key(' '); assert.equal(ui.engine.phase, 'running');
    return { azure: ui.engine.getStats().teams[0].initial, ember: ui.engine.getStats().teams[1].initial, blockedPlacementsRefused: true, fullBodyClearanceChecked: true };
  });

  test('Split button and B create independent groups while preserving soldiers and roster edits', () => {
    const ui = bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    placeCompany(ui, 0, 'swordsman', 96, 2500, 2200); placeCompany(ui, 1, 'guardian', 96, 3500, 2200);
    const original = ui.engine.regiments.find(r => r.team === 0), before = ui.engine.units.map(u => ({ ref: u, x: u.x, y: u.y, hp: u.hp }));
    ui.select(original); ui.document.getElementById('splitFormation').click();
    assert.equal(ui.engine.regiments.length, 3); assert.equal(ui.engine.units.length, 192);
    const halves = ui.engine.regiments.filter(r => r.team === 0);
    assert(halves.every(r => r.alive === 48));
    for (const u of ui.engine.units) { const p = before[u.id]; assert.equal(u, p.ref); assert.deepEqual([u.x, u.y, u.hp], [p.x, p.y, p.hp]); }
    const stationary = halves[0], moving = halves[1], oldStationary = stationary.unitIds.map(id => [ui.engine.units[id].x, ui.engine.units[id].y]);
    ui.select(moving); ui.point(2500, 2700, { button: 2 });
    assert.deepEqual(stationary.unitIds.map(id => [ui.engine.units[id].x, ui.engine.units[id].y]), oldStationary, 'Repositioning one split half moved the other');
    assert(Math.abs(moving.y - 2700) < 1); const detachedY = moving.y;
    ui.key('b'); assert.equal(ui.engine.regiments.length, 4); assert.equal(ui.engine.units.length, 192);
    const arrangedGeometry = actualFormationGeometry(ui);
    ui.document.getElementById('editBlue').click(); rosterCount(ui, 'swordsman', 116); ui.flushTimers();
    assert.equal(ui.engine.getStats().teams[0].initial, 116);
    const arranged = ui.engine.regiments.filter(r => r.team === 0 && r.type === 'swordsman');
    assert(arranged.filter(r => Math.abs(r.y - detachedY) < 100).length >= 2, 'A roster edit discarded the detached split groups');
    assert(arranged.some(r => r.alive === 48 && Math.abs(r.y - stationary.y) < 100), 'A roster edit collapsed the untouched split half');
    assertFormationGeometry(ui, arrangedGeometry, 'Roster increase moved existing split soldiers', true);
    const rosterRatio = assertSeparatedBodies(ui, 'Roster increase stacked new recruits onto existing split formations');
    const scaled = bootUI(ROOT); scaled.document.getElementById('newEncounter').click();
    placeCompany(scaled, 0, 'swordsman', 160, 2432, 1720);
    const originalGeometry = actualFormationGeometry(scaled); control(scaled, 'scaleSelect', '2');
    assert.equal(scaled.engine.units.length, 320, 'Quick scale did not double the custom army');
    assertFormationGeometry(scaled, originalGeometry, 'Quick scale moved the existing soldiers', true);
    const scaleRatio = assertSeparatedBodies(scaled, 'Quick scale stacked its surplus group on the original default-grid location');
    return { preservedTroopsAtSplit: 192, azureAfterEdit: 116, independentlyPlacedGroups: arranged.length,
      scaledSoldiers: 320, minimumRosterBodyRatio: Math.round(rosterRatio * 1000) / 1000, minimumScaleBodyRatio: Math.round(scaleRatio * 1000) / 1000 };
  });

  test('Saved encounters restore custom terrain and split formations through load and reset', () => {
    const ui = bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    paint(ui, 'forest', 1000, 900, 1500, 1400); control(ui, 'terrainBiome', 'woodland');
    placeCompany(ui, 0, 'swordsman', 160, 2800, 2200); placeCompany(ui, 1, 'guardian', 96, 3800, 2200);
    ui.select(ui.engine.regiments.find(r => r.team === 0)); ui.key('b');
    const positions = actualFormationGeometry(ui);
    assertSeparatedBodies(ui, 'Immediate split changed its physical footprint');
    const layout = JSON.stringify(ui.engine.captureFormations()), terrain = JSON.stringify(ui.engine.map.regions);
    ui.document.getElementById('scenarioName').value = 'Saved split encounter'; ui.document.getElementById('saveScenario').click();
    const stored = storedScenarios(ui); assert.equal(stored.scenarios[0].version, 2);
    assert.equal(stored.scenarios[0].config.formations.length, 3); assert.equal(stored.scenarios[0].config.customMap.regions.length, 1);
    ui.document.getElementById('newEncounter').click(); assert.equal(ui.engine.units.length, 0);
    const saved = ui.document.getElementById('savedScenarios'), load = saved.querySelector('[data-load-scenario="0"]');
    assert(load, 'The saved encounter has no Load button'); saved.dispatch('click', { target: load });
    assert.equal(ui.engine.units.length, 256); assert.equal(JSON.stringify(ui.engine.captureFormations()), layout);
    assert.equal(JSON.stringify(ui.engine.map.regions), terrain); assert.equal(ui.engine.map.biome, 'woodland');
    assertFormationGeometry(ui, positions, 'Load changed the individual split footprint');
    const reloadRatio = assertSeparatedBodies(ui, 'Load re-expanded adjacent split halves into overlapping blocks');
    ui.key(' '); ui.engine.step(1 / 30); ui.document.getElementById('resetButton').click();
    assert.equal(ui.engine.phase, 'deployment'); assert.equal(ui.engine.time, 0);
    assert.equal(JSON.stringify(ui.engine.captureFormations()), layout); assert.equal(JSON.stringify(ui.engine.map.regions), terrain);
    assertFormationGeometry(ui, positions, 'Reset changed the individual split footprint');
    assertSeparatedBodies(ui, 'Reset re-expanded split formations into overlapping blocks');
    return { restoredSoldiers: ui.engine.units.length, restoredGroups: ui.engine.regiments.length, customTerrainPreserved: true,
      exactWorldPositionsPreserved: true, minimumReloadBodyRatio: Math.round(reloadRatio * 1000) / 1000 };
  });

  test('A live split stays responsive while Save and Reset preserve the original encounter setup', () => {
    const ui = bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    placeCompany(ui, 0, 'swordsman', 64, 2500, 2200); placeCompany(ui, 1, 'guardian', 64, 3800, 2200);
    const setup = JSON.stringify(ui.engine.captureFormations());
    ui.key(' '); const troops = ui.engine.regiments.find(r => r.team === 0); ui.select(troops);
    ui.key('m'); ui.point(2100, 2200); assert.equal(troops.order, 'move');
    const positions = ui.engine.units.map(u => [u.x, u.y, u.hp]); ui.key('b');
    const halves = ui.engine.regiments.filter(r => r.team === 0);
    assert.equal(halves.length, 2); assert(halves.every(r => r.order === 'move'));
    assert.deepEqual(ui.engine.units.map(u => [u.x, u.y, u.hp]), positions);
    for (let tick = 0; tick < 30; tick++) ui.engine.step(1 / 30);
    for (const r of halves) {
      const mean = r.unitIds.reduce((sum, id) => sum + positions[id][0] - ui.engine.units[id].x, 0) / r.unitIds.length;
      assert(mean > 10, 'A split half stopped obeying its active Move');
    }
    ui.document.getElementById('scenarioName').value = 'Original setup after live split'; ui.document.getElementById('saveScenario').click();
    assert.equal(storedScenarios(ui).scenarios[0].config.formations.length, 2, 'A midbattle split silently replaced the saved starting encounter');
    ui.document.getElementById('resetButton').click();
    assert.equal(JSON.stringify(ui.engine.captureFormations()), setup); assert.equal(ui.engine.units.length, 128);
    return { liveGroups: 3, restoredSetupGroups: ui.engine.regiments.length, activeMovePreserved: true };
  });

  test('Existing version-one scenarios still load through the saved-battle interface', () => {
    const ui = bootUI(ROOT); ui.document.getElementById('scenarioName').value = 'Storage key seed';
    ui.document.getElementById('saveScenario').click(); const { key } = storedScenarios(ui);
    const legacy = { format: 'ironfield-scenario', version: 1, name: 'Legacy deployment', config: { name: 'Legacy deployment', map: 'meadow', seed: 4719,
      armies: [{ name: 'Old Azure', doctrine: 'balanced', roster: { swordsman: 16 } }, { name: 'Old Ember', doctrine: 'balanced', roster: { guardian: 16 } }] },
      deployment: [{ id: 0, x: 2200, y: 2300, formation: 'shieldwall' }, { id: 1, x: 4000, y: 2300, formation: 'line' }] };
    ui.storage.set(key, JSON.stringify([legacy])); ui.document.getElementById('scenariosButton').click();
    const panel = ui.document.getElementById('savedScenarios'), load = panel.querySelector('[data-load-scenario="0"]');
    assert(load); panel.dispatch('click', { target: load });
    assert.equal(ui.engine.phase, 'deployment'); assert.equal(ui.engine.units.length, 32); assert.equal(ui.engine.map.id, 'meadow');
    assert.equal(ui.engine.regiments[0].x, 2200); assert.equal(ui.engine.regiments[0].y, 2300);
    assert.equal(ui.engine.regiments[0].formation, 'shieldwall'); assert.equal(ui.engine.regiments[1].x, 4000);
    // Optional facing also works in the legacy deployment fallback. Incomplete
    // rows have a physical centroid different from the descriptor's anchor;
    // placing blue at red's old spawn additionally requires relocating every
    // company before testing the resulting rotations for body clearance.
    const partialConfig = { name: 'Legacy facing fallback', map: 'custom', seed: 4719,
      customMap: { biome: 'grassland', regions: [] },
      armies: [{ name: 'Partial blue', doctrine: 'balanced', roster: { swordsman: 5 } }, { name: 'Partial red', doctrine: 'balanced', roster: { guardian: 7 } }] };
    const defaults = new ui.window.BattleEngine(partialConfig), oldRed = defaults.regiments.find(r => r.team === 1);
    const oriented = { format: 'ironfield-scenario', version: 1, name: 'Legacy facing fallback', config: partialConfig,
      deployment: [{ id: 0, x: oldRed.x, y: oldRed.y, formation: 'line', facing: Math.PI / 2 },
        { id: 1, x: 5500, y: 3200, formation: 'line', facing: 0 }] };
    ui.storage.set(key, JSON.stringify([oriented])); ui.document.getElementById('scenariosButton').click();
    const orientedPanel = ui.document.getElementById('savedScenarios'), orientedLoad = orientedPanel.querySelector('[data-load-scenario="0"]');
    assert(orientedLoad); orientedPanel.dispatch('click', { target: orientedLoad });
    assert.equal(ui.engine.units.length, 12);
    for (const r of ui.engine.regiments) {
      const wanted = r.team === 0 ? Math.PI / 2 : 0;
      assert(Math.abs(Math.atan2(Math.sin(r.facing - wanted), Math.cos(r.facing - wanted))) < 1e-6,
        'Legacy optional facing used the deployment anchor instead of the actual member centroid, or a temporary default company blocked it');
      assert(r.unitIds.every(id => Math.abs(Math.atan2(Math.sin(ui.engine.units[id].angle - wanted), Math.cos(ui.engine.units[id].angle - wanted))) < 1e-6));
    }
    assertSeparatedBodies(ui, 'Legacy incomplete-row facing overlapped another company');
    return { legacyVersionAccepted: 1, restoredTroops: 32, originalDeploymentPreserved: true, incompleteRowFacingTroops: 12,
      legacyZeroAndNorthFacingPreserved: true, temporaryDefaultOccupancyAvoided: true };
  });

  test('Face through T or its button uses a direction point and rotates real deployed ranks in place', () => {
    const ui = bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    placeCompany(ui, 0, 'swordsman', 32, 2400, 2200); placeCompany(ui, 1, 'guardian', 32, 4400, 2200);
    paint(ui, 'rock', 2500, 3000, 2700, 3200);
    ui.document.getElementById('finishTerrain').click();
    const r = ui.engine.regiments.find(r => r.team === 0), before = r.unitIds.map(id => ({ id, x: ui.engine.units[id].x, y: ui.engine.units[id].y }));
    const c = { x: before.reduce((s, p) => s + p.x, 0) / before.length, y: before.reduce((s, p) => s + p.y, 0) / before.length };
    ui.select(r); ui.key('t'); assert.match(ui.document.getElementById('commandHint').textContent, /face.*direction/i);
    assert.equal(ui.engine.terrainAt(2600, 3100).blocked, true);
    ui.point(2600, 3100, { button: 2 });
    const angle = Math.atan2(3100 - c.y, 2600 - c.x);
    assert.equal(ui.engine.phase, 'deployment'); assert.equal(r.order, 'auto');
    for (const p of before) {
      const u = ui.engine.units[p.id], dx = p.x - c.x, dy = p.y - c.y;
      assert(Math.hypot(u.x - (c.x + Math.cos(angle) * dx - Math.sin(angle) * dy), u.y - (c.y + Math.sin(angle) * dx + Math.cos(angle) * dy)) < 1e-6,
        'T and right-click treated the direction as a movement destination or regenerated the footprint');
      assert(Math.abs(Math.atan2(Math.sin(u.angle - angle), Math.cos(u.angle - angle))) < 1e-6);
    }
    assert.match(ui.document.getElementById('selectedDetails').textContent, /Facing S/);
    const outside = { x: -1200, y: c.y + 300 }, outsideAngle = Math.atan2(outside.y - c.y, outside.x - c.x);
    ui.button('face').click(); ui.point(outside.x, outside.y);
    assert(Math.abs(Math.atan2(Math.sin(r.facing - outsideAngle), Math.cos(r.facing - outsideAngle))) < 1e-6,
      'A direction point outside the world was incorrectly clamped as a movement destination');
    assert.match(ui.document.getElementById('selectedDetails').textContent, /Facing W/);
    assert.equal(ui.document.getElementById('commandHint').hidden, true);
    assertSeparatedBodies(ui, 'Direction-only Face overlapped soldiers');
    return { soldiersRotated: before.length, blockedDirectionPointAccepted: true, outsideWorldDirectionPreserved: true, buttonAndHotkey: true };
  });

  test('A blocked deployment Face leaves soldiers unchanged and gives useful feedback', () => {
    const ui = bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    placeCompany(ui, 0, 'swordsman', 32, 2400, 2200);
    const original = ui.engine.regiments[0], points = original.unitIds.map(id => ui.engine.units[id]);
    const c = { x: points.reduce((s, u) => s + u.x, 0) / points.length, y: points.reduce((s, u) => s + u.y, 0) / points.length };
    const q = points.map(u => ({ x: c.x - (u.y - c.y), y: c.y + (u.x - c.x) })).reduce((a, b) => b.x > a.x ? b : a);
    paint(ui, 'rock', q.x - 3, q.y - 3, q.x + 57, q.y + 57);
    assert.equal(ui.engine.map.regions.length, 1, 'The blocking-rock fixture was not painted');
    ui.document.getElementById('finishTerrain').click();
    const r = ui.engine.regiments[0], before = JSON.stringify(ui.engine.units.map(u => [u.x, u.y, u.angle, u.hp]));
    ui.select(r); ui.key('t'); ui.point(c.x, c.y + 1000);
    assert.equal(JSON.stringify(ui.engine.units.map(u => [u.x, u.y, u.angle, u.hp])), before, 'Refused Face changed part of the formation');
    assert.match(lastMessage(ui), /clear space|obstacle|room/i);
    assert.equal(r.order, 'auto'); assert.equal(r.facing, 0);
    return { blockedRotationRefused: true, physicalStatePreserved: true };
  });

  test('Paused Face is stored without motion and a fresh Move interrupts its visible turn', () => {
    const ui = bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    placeCompany(ui, 0, 'swordsman', 32, 2400, 2200); placeCompany(ui, 1, 'guardian', 32, 5000, 2200);
    const r = ui.engine.regiments.find(r => r.team === 0); ui.select(r); ui.key(' '); ui.key(' ');
    assert.equal(ui.engine.phase, 'paused');
    const time = ui.engine.time, before = ui.engine.units.map(u => [u.x, u.y, u.angle, u.hp]);
    ui.key('t'); ui.point(2400, 3200);
    assert.equal(r.order, 'face'); assert.equal(r.commandStatus, 'facing'); assert.match(orderText(ui), /fac/i);
    for (let tick = 0; tick < 30; tick++) ui.engine.step(1 / 30);
    assert.equal(ui.engine.time, time); assert.deepEqual(ui.engine.units.map(u => [u.x, u.y, u.angle, u.hp]), before);
    ui.key(' '); for (let tick = 0; tick < 3; tick++) ui.engine.step(1 / 30);
    assert(r.unitIds.some(id => Math.abs(ui.engine.units[id].angle - before[id][2]) > .01), 'The accepted Face never begins a visible turn');
    assert(r.unitIds.every(id => Math.hypot(ui.engine.units[id].x - before[id][0], ui.engine.units[id].y - before[id][1]) < .01));
    const start = r.unitIds.map(id => ({ id, x: ui.engine.units[id].x }));
    ui.key('m'); ui.point(3000, 2200); assert.equal(r.order, 'move');
    for (let tick = 0; tick < 60; tick++) ui.engine.step(1 / 30);
    const advance = start.reduce((s, p) => s + ui.engine.units[p.id].x - p.x, 0) / start.length;
    assert(advance > 30, 'The old facing maneuver kept suppressing the new Move');
    return { storedWhilePaused: true, turnVisibleAfterResume: true, meanMoveAfterCancellation: Math.round(advance) };
  });

  test('Saved split formations retain world positions and explicit facing including zero', () => {
    const ui = bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    placeCompany(ui, 0, 'swordsman', 32, 2400, 2200); placeCompany(ui, 1, 'guardian', 32, 4400, 2200);
    const blue = ui.engine.regiments.find(r => r.team === 0), red = ui.engine.regiments.find(r => r.team === 1);
    ui.select(blue); ui.key('t'); ui.point(2400, 3200);
    ui.select(red); ui.button('face').click(); ui.point(5400, 2200);
    assert.equal(red.facing, 0, 'An eastward facing of zero was dropped');
    ui.select(blue); ui.key('b');
    const geometry = actualFormationGeometry(ui), angles = ui.engine.regiments.map(r => ({ facing: r.facing, soldiers: r.unitIds.map(id => ui.engine.units[id].angle) }));
    ui.document.getElementById('scenarioName').value = 'Oriented split encounter'; ui.document.getElementById('saveScenario').click();
    const saved = storedScenarios(ui).scenarios[0];
    assert(saved.config.formations.every(f => Number.isFinite(f.facing)));
    assert.equal(saved.config.formations.find(f => f.team === 1).facing, 0);
    ui.document.getElementById('newEncounter').click();
    const panel = ui.document.getElementById('savedScenarios'); panel.dispatch('click', { target: panel.querySelector('[data-load-scenario="0"]') });
    const verify = () => {
      assertFormationGeometry(ui, geometry, 'Facing save changed the split footprint');
      ui.engine.regiments.forEach((r, i) => {
        assert(Math.abs(Math.atan2(Math.sin(r.facing - angles[i].facing), Math.cos(r.facing - angles[i].facing))) < 1e-6);
        r.unitIds.forEach((id, j) => assert(Math.abs(Math.atan2(Math.sin(ui.engine.units[id].angle - angles[i].soldiers[j]), Math.cos(ui.engine.units[id].angle - angles[i].soldiers[j]))) < 1e-6));
      });
      assertSeparatedBodies(ui, 'Restoring facing overlapped the split ranks');
    };
    verify(); ui.key(' '); ui.engine.step(1 / 30); ui.document.getElementById('resetButton').click(); verify();
    return { savedAndRestoredGroups: 3, preservedWorldPositions: 64, zeroFacingPreserved: true };
  });

  test('The matchup guide exposes all shared armor factors and real selectors without issuing an order', () => {
    const ui = battle(), data = ui.window.GameData, doc = ui.document;
    const orders = ui.calls.length, initialTime = ui.engine.time, physical = ui.engine.units.map(u => [u.x, u.y, u.hp, u.angle]), accepted = ui.selected.order;
    doc.getElementById('selectedMatchups').click();
    assert.equal(doc.getElementById('helpDialog').open, true); assert.equal(ui.engine.phase, 'paused');
    assert.equal(doc.getElementById('matchupAttacker').value, ui.selected.type);
    const rows = [...doc.getElementById('armorMatchupTable').innerHTML.matchAll(/<tr data-matchup-row="([^"]+)">([\s\S]*?)<\/tr>/g)];
    assert.equal(rows.length, data.TYPE_ORDER.length); assert.equal(data.TYPE_ORDER.length, 23);
    for (const id of data.TYPE_ORDER) {
      const row = rows.find(r => r[1] === id); assert(row, id + ' is missing from the armor table');
      assert(row[2].includes('armor-' + data.TYPES[id].armorClass), id + ' has the wrong displayed protection class');
      for (const armor of ['light', 'medium', 'heavy']) {
        const cell = row[2].match(new RegExp('data-vs-armor="' + armor + '">×([0-9.]+)<'));
        assert(cell && cell[1] === data.armorMultiplier(id, armor).toFixed(2), id + ' has a display-only armor modifier');
      }
      assert(doc.getElementById('armyList').querySelector(`[data-counter-type="${id}"]`), id + ' has no roster counter button');
      assert(doc.getElementById('manualRoster').querySelector(`[data-counter-type="${id}"]`), id + ' has no manual counter button');
    }
    assert.equal(doc.getElementById('matchupAttacker').querySelectorAll('option').length, data.TYPE_ORDER.length);
    assert.equal(doc.getElementById('matchupDefender').querySelectorAll('option').length, data.TYPE_ORDER.length);
    for (const id of data.TYPE_ORDER) {
      control(ui, 'matchupAttacker', id); control(ui, 'matchupDefender', 'lancer');
      const result = doc.getElementById('matchupResult').innerHTML, matchup = data.getMatchup(id, 'lancer');
      assert(result.includes('×' + matchup.armorMultiplier.toFixed(2)), id + ' selector ignored the shared armor factor');
      assert(result.includes('Pierces ' + Math.round(matchup.armorPierce * 100) + '% of armor points'), id + ' hides or conflates armor penetration');
      if (matchup.antiMountedBonus) assert(result.includes('+' + matchup.antiMountedBonus + ' base damage against this mounted target'));
      else assert(!result.includes('base damage against this mounted target'));
    }
    control(ui, 'matchupAttacker', 'militia'); control(ui, 'matchupDefender', 'archer');
    const both = doc.getElementById('matchupResult').innerHTML;
    assert(both.includes('<strong>Good target:</strong>') && both.includes('<strong>Threat:</strong>'), 'A contextual pair lost one of its two legitimate counter explanations');
    assert(both.includes(data.TYPES.militia.matchups.strongAgainst.find(x => x.type === 'archer').reason));
    assert(both.includes(data.TYPES.militia.matchups.weakAgainst.find(x => x.type === 'archer').reason));
    const roster = doc.getElementById('armyList'); roster.dispatch('click', { target: roster.querySelector('[data-counter-type="hoplite"]') });
    assert.equal(doc.getElementById('matchupAttacker').value, 'hoplite');
    assert.equal(doc.getElementById('matchupDefender').value, data.TYPES.hoplite.matchups.strongAgainst[0].type);
    const manual = doc.getElementById('manualRoster'); manual.dispatch('click', { target: manual.querySelector('[data-counter-type="pikeman"]') });
    assert.equal(doc.getElementById('matchupAttacker').value, 'pikeman');
    assert.equal(doc.getElementById('matchupDefender').value, 'cavalry');
    ui.engine.step(1 / 30);
    assert.equal(ui.calls.length, orders); assert.equal(ui.selected.order, accepted); assert.equal(ui.engine.time, initialTime);
    assert.deepEqual(ui.engine.units.map(u => [u.x, u.y, u.hp, u.angle]), physical, 'Reading counters advanced or changed the paused battle');
    return { sharedTableRows: rows.length, exactArmorCells: rows.length * 3, selectorTypes: data.TYPE_ORDER.length, contextIncludesOpportunityAndThreat: true, unintendedCommands: ui.calls.length - orders };
  });

  test('Hoplites can be placed split faced and saved through the actual encounter interface', () => {
    const ui = bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    placeCompany(ui, 0, 'hoplite', 64, 2400, 2200); placeCompany(ui, 1, 'swordsman', 32, 4200, 2200);
    assert.equal(ui.engine.getStats().teams[0].initial, 64); assert.equal(ui.engine.getStats().teams[1].initial, 32);
    const original = ui.engine.regiments.find(r => r.team === 0); assert.equal(original.type, 'hoplite');
    ui.select(original); control(ui, 'formationSelect', 'shieldwall');
    ui.key('t'); ui.point(2400, 3200);
    const before = ui.engine.units.map(u => ({ ref: u, x: u.x, y: u.y, hp: u.hp, angle: u.angle }));
    ui.document.getElementById('splitFormation').click();
    const halves = ui.engine.regiments.filter(r => r.team === 0);
    assert.equal(halves.length, 2); assert(halves.every(r => r.type === 'hoplite' && r.alive === 32 && r.formation === 'shieldwall'));
    for (const u of ui.engine.units) {
      const prior = before[u.id]; assert.equal(u, prior.ref);
      assert.deepEqual([u.x, u.y, u.hp, u.angle], [prior.x, prior.y, prior.hp, prior.angle], 'Splitting hoplites changed their physical bodies');
    }
    const geometry = actualFormationGeometry(ui), facings = ui.engine.regiments.map(r => r.facing), layout = JSON.stringify(ui.engine.captureFormations());
    ui.document.getElementById('scenarioName').value = 'Hoplite shield encounter'; ui.document.getElementById('saveScenario').click();
    const saved = storedScenarios(ui).scenarios[0];
    assert.equal(saved.config.armies[0].roster.hoplite, 64); assert.equal(saved.config.formations.filter(f => f.type === 'hoplite').length, 2);
    ui.document.getElementById('newEncounter').click();
    const panel = ui.document.getElementById('savedScenarios'); panel.dispatch('click', { target: panel.querySelector('[data-load-scenario="0"]') });
    assertFormationGeometry(ui, geometry, 'Loading the new troop changed its split geometry');
    assert.equal(JSON.stringify(ui.engine.captureFormations()), layout);
    ui.engine.regiments.forEach((r, i) => assert(Math.abs(r.facing - facings[i]) < 1e-6));
    const separation = assertSeparatedBodies(ui, 'Loading hoplite ranks overlapped their bodies');
    assert.equal(ui.engine.units.filter(u => u.type === 'hoplite').length, 64);
    const moving = ui.engine.regiments.find(r => r.team === 0), stationary = ui.engine.regiments.filter(r => r.team === 0)[1];
    ui.select(moving); ui.key(' '); ui.key('h');
    const heldPositions = stationary.unitIds.map(id => ({ id, x: ui.engine.units[id].x, y: ui.engine.units[id].y }));
    const direction = moving.x < stationary.x ? -1 : 1;
    ui.select(stationary); ui.key('h'); ui.select(moving); ui.key('m'); ui.point(moving.x + direction * 400, moving.y);
    const start = moving.unitIds.map(id => ({ id, x: ui.engine.units[id].x }));
    for (let tick = 0; tick < 60; tick++) ui.engine.step(1 / 30);
    const advance = start.reduce((s, p) => s + (ui.engine.units[p.id].x - p.x) * direction, 0) / start.length;
    assert(advance > 25, 'An independently commanded saved hoplite half ignored Move');
    assert(heldPositions.every(p => Math.hypot(ui.engine.units[p.id].x - p.x, ui.engine.units[p.id].y - p.y) < .01), 'Commanding one hoplite half dragged the held half');
    return { hoplites: 64, exactBodiesRestored: 96, groups: 3, minimumBodyRatio: Math.round(separation * 1000) / 1000, independentMove: Math.round(advance) };
  });

  test('Battle plans can change in a paused battle without resetting troops or replacing manual orders', () => {
    const ui = battle(); ui.key('h');
    const original = ui.engine, chosen = ui.selected, epoch = chosen._commandEpoch;
    for (let i = 0; i < 30; i++) original.step(1 / 30);
    const time = original.time, positions = JSON.stringify(original.units.map(u => [u.x, u.y, u.angle, u.hp]));
    ui.document.getElementById('battlePlansButton').click();
    assert.equal(original.phase, 'paused');
    control(ui, 'plan0Select', 'flank');
    assert.equal(ui.document.getElementById('plan0Description').textContent, ui.window.GameData.DOCTRINES.flank.description);
    ui.document.getElementById('applyPlan0').click();
    assert.equal(ui.engine, original, 'Changing the battle plan constructed another battle');
    assert.equal(original.time, time);
    assert.equal(JSON.stringify(original.units.map(u => [u.x, u.y, u.angle, u.hp])), positions, 'A paused plan changed physical troops');
    assert.equal(chosen.order, 'hold'); assert.equal(chosen._commandEpoch, epoch);
    assert.equal(original.config.armies[0].doctrine, 'flank');
    assert(original.regiments.some(r => r.team === 0 && r.type === 'cavalry' && r.order === 'auto' && r._tactic && r._tactic.transit));
    assert(/direct orders/i.test(ui.document.getElementById('plan0Status').textContent));
    assert(/Encircle/.test(lastMessage(ui)));
    ui.document.getElementById('battlePlansDialog').close(); ui.key('g');
    assert.equal(chosen.order, 'auto');
    original.resume(); original.step(1 / 30);
    assert.equal(chosen._tactic.plan, 'flank', 'Auto did not join the newly chosen plan');
    return { pausedStatePreserved: true, manualHoldPreserved: true, cavalryHasOuterRoute: true };
  });

  test('Live battle plans respect the controlled army and spectator mode', () => {
    const ui = battle(); ui.document.getElementById('battlePlansButton').click();
    assert.equal(ui.document.getElementById('applyPlan0').disabled, false);
    assert.equal(ui.document.getElementById('applyPlan1').disabled, true);
    const redPlan = ui.engine.config.armies[1].doctrine;
    control(ui, 'plan1Select', 'hammer'); ui.document.getElementById('applyPlan1').click();
    assert.equal(ui.engine.config.armies[1].doctrine, redPlan);
    ui.document.getElementById('battlePlansDialog').close(); control(ui, 'controlSelect', 'both');
    ui.document.getElementById('battlePlansButton').click();
    assert.equal(ui.document.getElementById('applyPlan1').disabled, false);
    control(ui, 'plan1Select', 'hammer'); ui.document.getElementById('applyPlan1').click();
    assert.equal(ui.engine.config.armies[1].doctrine, 'hammer');
    ui.document.getElementById('battlePlansDialog').close(); control(ui, 'controlSelect', 'spectate');
    ui.document.getElementById('battlePlansButton').click();
    assert([0, 1].every(team => ui.document.getElementById('applyPlan' + team).disabled));
    return { opposingPlanProtected: true, commandBothAccepted: true, spectatingReadOnly: true };
  });

  test('Every battle plan round-trips through real scenario Save and Load with its deployment intact', () => {
    const ui = bootUI(ROOT);
    const choose = (team, id) => {
      ui.document.getElementById('battlePlansButton').click(); control(ui, 'plan' + team + 'Select', id);
      ui.document.getElementById('applyPlan' + team).click(); ui.document.getElementById('battlePlansDialog').close();
    };
    choose(0, 'flank'); choose(1, 'hammer'); control(ui, 'scaleSelect', '2'); ui.flushTimers();
    assert.equal(ui.engine.units.length, 4800);
    assert.equal(ui.engine.config.armies[0].doctrine, 'flank'); assert.equal(ui.engine.config.armies[1].doctrine, 'hammer');
    ui.document.getElementById('newEncounter').click();
    placeCompany(ui, 0, 'cavalry', 32, 2100, 1700); placeCompany(ui, 1, 'pikeman', 32, 4100, 2200);
    const customGeometry = actualFormationGeometry(ui);
    control(ui, 'scaleSelect', '2'); ui.flushTimers();
    choose(0, 'hammer'); choose(1, 'skirmish'); control(ui, 'scaleSelect', '1'); ui.flushTimers();
    assert.equal(ui.engine.units.length, 64);
    assert.equal(ui.engine.config.armies[0].doctrine, 'hammer'); assert.equal(ui.engine.config.armies[1].doctrine, 'skirmish');
    assertFormationGeometry(ui, customGeometry, 'Restoring the custom scale changed its cached initial deployment');
    const geometry = actualFormationGeometry(ui), original = ui.engine, plans = Object.keys(ui.window.GameData.DOCTRINES);
    assert.equal(plans.length, 14);
    for (const id of plans) {
      ui.document.getElementById('battlePlansButton').click(); control(ui, 'plan0Select', id); ui.document.getElementById('applyPlan0').click();
      assert.equal(ui.engine, original); assertFormationGeometry(ui, geometry, 'Choosing a deployment plan moved the existing bodies');
      ui.document.getElementById('battlePlansDialog').close();
      ui.document.getElementById('scenarioName').value = 'Saved plan ' + id; ui.document.getElementById('saveScenario').click();
    }
    const saved = storedScenarios(ui).scenarios;
    assert.equal(new Set(saved.map(s => s.config.armies[0].doctrine)).size, plans.length);
    for (const id of plans) {
      const index = saved.findIndex(s => s.config.armies[0].doctrine === id);
      ui.document.getElementById('newEncounter').click(); ui.document.getElementById('scenariosButton').click();
      const panel = ui.document.getElementById('savedScenarios'), load = panel.querySelector('[data-load-scenario="' + index + '"]');
      assert(load); panel.dispatch('click', { target: load });
      assert.equal(ui.engine.config.armies[0].doctrine, id, 'A saved new plan was normalized away');
      assertFormationGeometry(ui, geometry, 'Loading a plan changed deployment ranks');
    }
    return { savedPlans: plans, restoredTroops: 64, exactDeploymentPreserved: true, builtInAndCachedCustomScaleRetainPlans: true };
  });

  test('Custom siege walls and gates carve a usable opening and preserve defenders, health and equipment through Save Load and Import', async () => {
    const ui = bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    control(ui, 'terrainDefender', '1'); paint(ui, 'wall', 3200, 1000, 3280, 3200);
    paint(ui, 'gate', 3160, 2080, 3320, 2320);
    control(ui, 'terrainDefender', '0'); paint(ui, 'wall', 4500, 1100, 4580, 1600);
    const gate = ui.engine.structures.find(s => s.type === 'gate');
    assert(gate && gate.team === 1 && gate.hp === 1000);
    assert.equal(ui.engine.structures.filter(s => s.type === 'wall').length, 3, 'Painting a gate failed to split the original wall');
    assert(ui.engine.structures.some(s => s.type === 'wall' && s.team === 0), 'The defender selector was ignored');
    const overlaps = (a, b) => a.x < b.x + b.w && a.x + a.w > b.x && a.y < b.y + b.h && a.y + a.h > b.y;
    assert(ui.engine.structures.filter(s => s.type === 'wall').every(s => !overlaps(s, gate)), 'Hidden masonry remained beneath the painted gate');
    assert.equal(ui.engine.terrainAt(3240, 2200).blocked, true, 'An intact custom gate starts open');
    placeCompany(ui, 0, 'batteringRam', 8, 2870, 2200);
    placeCompany(ui, 0, 'siegeTower', 7, 2870, 1700);
    assert(ui.engine.regiments.filter(r => ['batteringRam', 'siegeTower'].includes(r.type)).every(r => r.initial === 1), 'A placement click bundled equipment into an overlapping formation');
    placeCompany(ui, 0, 'swordsman', 24, 2600, 2200); placeCompany(ui, 1, 'guardian', 24, 3900, 2200);
    const geometry = actualFormationGeometry(ui), beforeAreas = ui.engine.map.regions.length;
    paint(ui, 'wall', 2500, 2100, 2700, 2300);
    assert.equal(ui.engine.map.regions.length, beforeAreas, 'The fortification brush painted through deployed soldiers');
    ui.key('Escape'); ui.document.getElementById('scenarioName').value = 'Saved custom siege'; ui.document.getElementById('saveScenario').click();
    const snapshot = storedScenarios(ui).scenarios[0];
    assert.equal(snapshot.config.customMap.regions.filter(r => r.type === 'wall' || r.type === 'gate').length, 4);
    ui.document.getElementById('newEncounter').click(); ui.document.getElementById('scenariosButton').click();
    const panel = ui.document.getElementById('savedScenarios'), load = panel.querySelector('[data-load-scenario="0"]');
    panel.dispatch('click', { target: load });
    assertFormationGeometry(ui, geometry, 'Saved siege equipment or infantry moved during Load');
    assert.equal(ui.engine.structures.length, 4); assert.equal(ui.engine.structures.find(s => s.type === 'gate').maxHp, 1000);
    const imported = JSON.parse(JSON.stringify(snapshot)); imported.name = 'Imported siege with authored durability';
    imported.config.customMap.regions.find(r => r.type === 'gate').hp = 1456;
    imported.config.customMap.regions.find(r => r.type === 'wall' && r.team === 0).hp = 3456;
    const payload = JSON.stringify(imported), file = ui.document.getElementById('scenarioFile');
    file.files = [{ size: Buffer.byteLength(payload), text: async () => payload }];
    await file.dispatchAsync('change');
    assert.equal(ui.document.getElementById('scenarioName').value, imported.name, 'The actual Import handler did not load the selected JSON file');
    assert.equal(ui.engine.structures.find(s => s.type === 'gate').maxHp, 1456);
    const blueWall = ui.engine.structures.find(s => s.type === 'wall' && s.team === 0);
    assert(blueWall && blueWall.maxHp === 3456, 'Import stripped fortification ownership or durability');
    assertFormationGeometry(ui, geometry, 'Import moved the saved physical deployment');
    assertSeparatedBodies(ui, 'Imported siege');
    return { structures: 4, equipmentPlaced: 2, gateDurability: 1456, blueWallDurability: 3456, actualImportHandler: true, savedPositionsExact: true };
  });

  test('Right-clicking an enemy fortification assigns a real siege target and fresh Move or Hold overrides the job', () => {
    const ui = bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    control(ui, 'terrainDefender', '1'); paint(ui, 'gate', 3200, 2000, 3280, 2400);
    placeCompany(ui, 0, 'batteringRam', 1, 2700, 2200); placeCompany(ui, 1, 'guardian', 1, 5400, 3900);
    const ram = ui.engine.regiments.find(r => r.type === 'batteringRam'), u = ui.engine.units[ram.unitIds[0]], gate = ui.engine.structures[0];
    ui.select(ram); ui.key(' '); ui.point(3240, 2200, { button: 2 });
    assert.equal(ram._siegeManualTarget, gate.id, 'The right-click became a generic unreachable troop target');
    assert(ram._siegeJob && ram._siegeJob.structureId === gate.id);
    const siegeText = ui.document.getElementById('selectedSiege');
    assert.equal(siegeText.hidden, false); assert.match(siegeText.textContent, /approach|gate|siege/i);
    const start = { x: u.x, y: u.y };
    for (let i = 0; i < 90; i++) ui.engine.step(1 / 30);
    assert(u.x - start.x > 30, 'The displayed siege order did not move its physical equipment');
    ui.select(ram); ui.key('m'); ui.point(2300, 2600, { button: 2 });
    assert.equal(ram.order, 'move'); assert.equal(ram._siegeJob, null, 'Fresh Move retained the old siege job');
    const beforeMove = Math.hypot(u.x - 2300, u.y - 2600);
    for (let i = 0; i < 90; i++) ui.engine.step(1 / 30);
    assert(Math.hypot(u.x - 2300, u.y - 2600) < beforeMove - 25, 'Manual Move did not divert the ram from the gate');
    ui.select(ram); ui.button('hold').click(); const held = { x: u.x, y: u.y }, hp = gate.hp;
    for (let i = 0; i < 60; i++) ui.engine.step(1 / 30);
    assert.equal(ram.order, 'hold'); assert.equal(ram._siegeJob, null);
    assert(Math.hypot(u.x - held.x, u.y - held.y) < .01 && gate.hp === hp, 'Held equipment continued its old siege work');
    return { explicitTarget: gate.id, actualApproach: true, manualMoveOverrides: true, holdStationary: true };
  });

  test('Selected mounted speed feedback follows real acceleration and clears after a fresh Hold', () => {
    const ui = bootUI(ROOT); ui.document.getElementById('newEncounter').click();
    placeCompany(ui, 0, 'cavalry', 1, 1500, 2200); placeCompany(ui, 1, 'guardian', 1, 5600, 2200);
    const horse = ui.engine.regiments.find(r => r.type === 'cavalry'), enemy = ui.engine.regiments.find(r => r.type === 'guardian');
    ui.engine.command([enemy.id], 'hold'); ui.select(horse); ui.key(' '); ui.key('m'); ui.point(4000, 2200, { button: 2 });
    const momentum = ui.document.getElementById('selectedMomentum'), bar = ui.document.getElementById('selectedMomentumBar');
    assert.equal(momentum.hidden, false); assert.equal(bar.hidden, false);
    assert.equal(Number(bar.getAttribute('aria-valuenow')), 0, 'A stationary horse claimed a full charge');
    for (let i = 0; i < 75; i++) ui.engine.step(1 / 30);
    ui.select(horse);
    const percent = Number(bar.getAttribute('aria-valuenow'));
    assert(percent >= 65 && percent <= 100, 'Visible momentum did not follow the actual run-up');
    assert.equal(ui.document.getElementById('selectedMomentumFill').style.width, percent + '%');
    ui.button('hold').click(); ui.engine.step(1 / 30); ui.select(horse);
    assert.equal(Number(bar.getAttribute('aria-valuenow')), 0, 'Held cavalry kept obsolete charge feedback');
    return { afterRunUpPercent: percent, holdPercent: 0, accessibleMeter: true };
  });

  Promise.all(pendingTests).then(() => console.log(JSON.stringify({ passed: tests.filter(t => t.passed).length, failed: tests.filter(t => !t.passed).length, tests }, null, 2)));
}
module.exports = { bootUI };
