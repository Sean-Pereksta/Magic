import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import fs from 'node:fs';

const source = fs.readFileSync(new URL('./mobile-travel.js', import.meta.url), 'utf8');

function harness({touch = true, narrow = false, slots = 3, missingUI = false} = {}) {
  function element() {
    const classes = new Set();
    return {
      attrs: {}, children: [], events: {}, properties: {}, slotCount: slots,
      classList: {contains: name => classes.has(name), toggle(name, on) { if (on) classes.add(name); else classes.delete(name); }},
      style: {setProperty(name, value) { this[name] = value; }},
      appendChild(child) { this.children.push(child); },
      setAttribute(name, value) { this.attrs[name] = value; },
      addEventListener(name, fn) { this.events[name] = fn; },
      querySelectorAll() { return Array(this.slotCount).fill({}); },
      focus() { this.focused = true; }
    };
  }
  const root = element(), hud = element(), tools = element(), spells = element();
  const nodes = missingUI ? {} : {hud, buttons: tools, spells};
  const events = {};
  const query = {matches: narrow, addEventListener(name, fn) { events[name] = fn; }};
  const observations = [];
  const camera = {
    zoom: 1, target: 1, kick: 0, ticks: 0,
    tick(dt) { this.ticks++; this.zoom += (this.target + this.kick - this.zoom) * (1 - Math.exp(-dt * 6)); return this.ticks; },
    project(x) { return x * this.zoom; }
  };
  const mounts = {horse: {speed: 1.42}, elk: {speed: 1.52, terrain: ['forest']}, wolf: {speed: 1.56}, stormstag: {speed: 1.65}, arcanebeast: {speed: 1.75}, extra: {speed: 2.1}, invalid: {speed: NaN}};
  const document = {documentElement: root, events: {}, getElementById: id => nodes[id] || null, createElement: element,
    addEventListener(name, fn) { this.events[name] = fn; }};
  const context = vm.createContext({
    document, isTouch: touch, matchMedia: () => query,
    AWCampaignData: {mounts}, AWPresentation: {camera},
    MutationObserver: class { constructor(fn) { this.fn = fn; } observe(target, options) { observations.push({target, options, fn: this.fn}); } }
  });
  context.window = context;
  vm.runInContext(source, context);
  return {context, root, camera, mounts, query, events, document, hud, tools, spells, observations,
    get toggle() { return hud.children[0]; }, runAgain() { vm.runInContext(source, context); }};
}

test('every current or regional mount gets 60% faster without losing terrain metadata', () => {
  const h = harness();
  for (const [id, before] of Object.entries({horse: 1.42, elk: 1.52, wolf: 1.56, stormstag: 1.65, arcanebeast: 1.75, extra: 2.1})) {
    assert.equal(h.mounts[id].speed, Math.round(before * 1.6 * 1000) / 1000);
    assert.ok(h.mounts[id].speed > 2.2);
  }
  assert.deepEqual(h.mounts.elk.terrain, ['forest']);
  assert.ok(Number.isNaN(h.mounts.invalid.speed));
});

test('desktop also receives faster mounts, but no HUD or camera change', () => {
  const h = harness({touch: false});
  assert.equal(h.mounts.horse.speed, 2.272);
  assert.equal(h.context.AWMobileTravel.compact, false);
  assert.equal(h.camera.zoom, 1);
  h.camera.tick(.1);
  assert.equal(h.camera.zoom, 1);
  h.context.AWMobileTravel.setMenuOpen(true);
  assert.equal(h.root.classList.contains('aw-mobile-tools-open'), false);
});

test('mobile starts 14% wider and does not compound zoom over 600 frames', () => {
  const h = harness();
  assert.equal(h.camera.zoom, .86);
  for (let i = 0; i < 600; i++) h.camera.tick(1 / 60);
  assert.ok(Math.abs(h.camera.zoom - .86) < 1e-12);
  assert.equal(h.camera.project(100), 86);
  assert.equal(h.camera.ticks, 600);
});

test('riding camera and combat zoom still smoothly compose with mobile framing', () => {
  const h = harness();
  for (const target of [.88, .93, .96, 1]) {
    h.camera.target = target;
    for (let i = 0; i < 300; i++) h.camera.tick(1 / 60);
    assert.ok(Math.abs(h.camera.zoom - target * .86) < 1e-10);
  }
});

test('camera kick and tick return value are preserved', () => {
  const h = harness();
  h.camera.kick = .04;
  assert.equal(h.camera.tick(.1), 1);
  assert.ok(Math.abs(h.camera.zoom - (1 + .04 * (1 - Math.exp(-.6))) * .86) < 1e-12);
});

test('resizing into and out of compact mode restores exact desktop zoom', () => {
  const h = harness({touch: false});
  for (let i = 0; i < 10; i++) {
    h.query.matches = true; h.events.change();
    assert.equal(h.camera.zoom, .86);
    assert.equal(h.context.AWMobileTravel.compact, true);
    h.query.matches = false; h.events.change();
    assert.equal(h.camera.zoom, 1);
    assert.equal(h.context.AWMobileTravel.compact, false);
  }
});

test('touch devices remain compact at wide landscape/tablet widths', () => {
  const h = harness({touch: true, narrow: false});
  assert.ok(h.root.classList.contains('aw-mobile-compact'));
  h.events.change();
  assert.equal(h.camera.zoom, .86);
});

test('fourth and fifth spell slots reserve a second row; loadout shrink releases it', () => {
  const h = harness();
  for (const [count, rows] of [[3, '1'], [4, '2'], [5, '2'], [3, '1']]) {
    h.spells.slotCount = count;
    h.observations[0].fn();
    assert.equal(h.root.style['--aw-mobile-spell-rows'], rows);
  }
  assert.equal(h.observations.length, 1);
  assert.equal(h.observations[0].target, h.spells);
  assert.equal(h.observations[0].options.subtree, undefined);
});

test('compact tools toggle is accessible and keeps the original tools intact', () => {
  const h = harness();
  assert.equal(h.toggle.attrs['aria-controls'], 'buttons');
  assert.equal(h.toggle.attrs['aria-expanded'], 'false');
  h.toggle.events.click();
  assert.equal(h.toggle.attrs['aria-expanded'], 'true');
  assert.ok(h.root.classList.contains('aw-mobile-tools-open'));
  h.tools.events.click({target: {closest: () => ({})}});
  assert.equal(h.toggle.attrs['aria-expanded'], 'false');
});

test('Escape closes tools and returns focus to the menu toggle', () => {
  const h = harness();
  h.toggle.events.click();
  h.document.events.keydown({key: 'Escape'});
  assert.equal(h.toggle.attrs['aria-expanded'], 'false');
  assert.equal(h.toggle.focused, true);
});

test('loading tuning twice never doubles mount speed, controls or camera scaling', () => {
  const h = harness();
  h.runAgain();
  assert.equal(h.mounts.horse.speed, 2.272);
  assert.equal(h.hud.children.length, 1);
  assert.equal(h.observations.length, 1);
  assert.equal(h.camera.zoom, .86);
});

test('headless or missing HUD elements do not break travel tuning', () => {
  const h = harness({missingUI: true});
  assert.equal(h.hud.children.length, 0);
  assert.equal(h.observations.length, 0);
  assert.equal(h.mounts.horse.speed, 2.272);
  assert.equal(h.camera.zoom, .86);
});
