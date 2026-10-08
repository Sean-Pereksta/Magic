'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = name => fs.readFileSync(path.join(__dirname, '..', name + '.js'), 'utf8');

// Exercise the renderer directly without mounting DOM controls. Record painted
// paths and actual text extents, rather than trusting label metadata alone.
function canvasContext() {
  const shapes = [], texts = [], rectangles = [], stack = [];
  let tx = 0, ty = 0, current = {};
  const c = {
    font: '10px system-ui', textAlign: 'left',
    save() { stack.push({ tx, ty, font: this.font, textAlign: this.textAlign }); },
    restore() { const s = stack.pop(); assert.ok(s, 'balanced canvas save/restore'); tx = s.tx; ty = s.ty; this.font = s.font; this.textAlign = s.textAlign; },
    translate(x, y) { tx += x; ty += y; }, rotate() {},
    beginPath() { current = { points: [], rectangle: false }; },
    moveTo(x, y) { current.points.push([x, y]); },
    lineTo(x, y) { current.points.push([x, y]); }, closePath() {},
    roundRect(x, y, width, height) { current.rectangle = true; rectangles.push({ x: x + tx, y: y + ty, width, height }); },
    fill() { if (!current.rectangle && current.points.length) shapes.push({ points: current.points.slice(), x: tx, y: ty }); },
    stroke() {},
    measureText(value) {
      const size = Number(this.font.match(/([\d.]+)px/)?.[1] || 10);
      return { width: [...String(value)].reduce((n, ch) => n + (/\s/.test(ch) ? .3 : /[MW◆]/.test(ch) ? .88 : .56) * size, 0) };
    },
    fillText(text, x, y, maxWidth) { texts.push({ text, x: x + tx, y: y + ty, width: Math.min(this.measureText(text).width, maxWidth ?? Infinity), height: Number(this.font.match(/([\d.]+)px/)?.[1] || 10) }); }
  };
  return { c, shapes, texts, rectangles, stack };
}
function renderer(width, height, markers) {
  const drawing = canvasContext();
  class Renderer {
    drawSettlementGuides() { this.legacyCalls = (this.legacyCalls || 0) + 1; }
    settlementIndicators() { return markers; }
  }
  const context = vm.createContext({ ATSRenderer: Renderer, window: {} });
  vm.runInContext(source('interface-art'), context);
  const r = new Renderer(); Object.assign(r, { ctx: drawing.c, w: width, h: height, findSettlements: true });
  return { r, ...drawing };
}
function marker(name, distance, x, y, edge, attack = false, near = false) {
  return Object.freeze({ settlement: Object.freeze({ name, attack }), distance, x, y, edge, angle: .4, near });
}
function overlap(a, b) {
  return a.x < b.x + b.width && a.x + a.width > b.x && a.y < b.y + b.height && a.y + a.height > b.y;
}
function verifyLayout(h, markers) {
  const labels = h.r.settlementFinderLabels;
  assert.equal(h.shapes.length, markers.length, 'every destination retains its painted arrow or nearby-house marker');
  assert.equal(h.r.legacyCalls || 0, 0, 'legacy attacked-name rendering cannot duplicate the new cards');
  assert.equal(h.rectangles.length, labels.length, 'metadata describes the cards actually painted');
  assert.equal(h.stack.length, 0);
  for (let i = 0; i < labels.length; i++) {
    const box = labels[i];
    assert.ok(box.x >= 0 && box.y >= 0 && box.x + box.width <= h.r.w && box.y + box.height <= h.r.h,
      'card stays within viewport: ' + JSON.stringify(box));
    for (let j = i + 1; j < labels.length; j++) assert.ok(!overlap(box, labels[j]), 'cards do not overlap: ' + box.name + ' / ' + labels[j].name);
  }
  for (const text of h.texts) {
    assert.ok(labels.some(box => text.x >= box.x && text.x + text.width <= box.x + box.width + .001 && text.y - text.height >= box.y && text.y <= box.y + box.height),
      'name and status text fits a card: ' + JSON.stringify(text));
  }
}

test('a nearest attacked village keeps its name, nearest status and alarm in one card, even with a short name', () => {
  const markers = [marker('Far grove', 850, 760, 110, 'top'), marker('A', 70, 720, 110, 'top', true), marker('Other home', 300, 680, 110, 'top')];
  const h = renderer(1440, 900, markers), before = JSON.stringify(markers);
  h.r.drawSettlementGuides({}); verifyLayout(h, markers);
  const nearest = h.r.settlementFinderLabels.filter(label => label.nearest);
  assert.equal(nearest.length, 1); assert.equal(nearest[0].name, 'A'); assert.equal(nearest[0].attacked, true);
  assert.equal(h.r.settlementFinderLabels[0].name, 'A', 'nearest receives placement priority');
  assert.equal(h.texts.filter(t => t.text === '◆ A').length, 1);
  assert.equal(h.texts.filter(t => t.text === 'NEAREST VILLAGE').length, 1);
  assert.equal(h.texts.filter(t => t.text === 'UNDER ATTACK').length, 1);
  assert.equal(JSON.stringify(markers), before, 'presentation never mutates settlement or bearing data');
});

for (const [width, height] of [[1440, 900], [390, 844]]) {
  for (const edge of ['top', 'bottom', 'left', 'right']) {
    test('crowded ' + edge + ' village cards fit without collisions at ' + width + 'px', () => {
      const markers = Array.from({ length: 28 }, (_, i) => {
        const x = edge === 'left' ? 22 : edge === 'right' ? width - 22 : width / 2 + i % 5 * 3;
        const y = edge === 'top' ? 104 : edge === 'bottom' ? height - 78 : height / 2 + i % 5 * 3;
        return marker(i === 19 ? 'Oak' : 'The very long name of jungle settlement ' + i, i === 19 ? 5 : 150 + i * 31, x, y, edge, true);
      });
      const h = renderer(width, height, markers); h.r.drawSettlementGuides({}); verifyLayout(h, markers);
      assert.ok(h.r.settlementFinderLabels.some(label => label.name === 'Oak' && label.nearest && label.attacked));
      assert.ok(h.r.settlementFinderLabels.length < markers.length, 'crowding can omit distant text while retaining every arrow');
    });
  }
  test('simultaneously crowded four-edge labels and nearby markers coexist at ' + width + 'px', () => {
    const markers = [];
    for (const edge of ['top', 'bottom', 'left', 'right']) for (let i = 0; i < 18; i++) {
      const x = edge === 'left' ? 22 : edge === 'right' ? width - 22 : width / 2 + (i % 3 - 1) * 8;
      const y = edge === 'top' ? 104 : edge === 'bottom' ? height - 78 : height / 2 + (i % 3 - 1) * 8;
      markers.push(marker(edge + ' woodland home ' + i, 100 + markers.length * 11, x, y, edge, i % 3 === 0));
    }
    markers.push(marker('Nearest threatened haven', 1, width / 2, height / 2, null, true, true));
    const h = renderer(width, height, markers); h.r.drawSettlementGuides({}); verifyLayout(h, markers);
    assert.equal(h.r.settlementFinderLabels[0].name, 'Nearest threatened haven');
  });
}

test('finder clears stale card metadata after the visible village set becomes empty', () => {
  const markers = [marker('Old home', 10, 120, 104, 'top')], h = renderer(390, 844, markers);
  h.r.drawSettlementGuides({}); assert.equal(h.r.settlementFinderLabels.length, 1);
  markers.length = 0; h.r.drawSettlementGuides({}); assert.equal(h.r.settlementFinderLabels.length, 0);
});

test('a throwing artwork progress subscriber cannot block decode, failure, timeout or the ready promise', async () => {
  const images = [], timers = new Set(), errors = [];
  class Image {
    constructor() { images.push(this); }
    set src(value) { this.source = value; }
  }
  const context = vm.createContext({ Image, Promise, Object, console: { error: (...args) => errors.push(args) },
    setTimeout(fn) { timers.add(fn); return fn; }, clearTimeout(fn) { timers.delete(fn); } });
  context.window = context;
  context.ATS_VISUAL_BUNDLE = { manifest: {}, sources: { good: 'good.png', missing: 'missing.png', stalled: 'stalled.png' } };
  vm.runInContext(source('visual-assets'), context);
  const assets = context.ATSVisualAssets, progress = [];
  let unsubscribe;
  assert.doesNotThrow(() => { unsubscribe = assets.onProgress(() => { throw new Error('HUD subscriber failure'); }); });
  assets.onProgress(value => progress.push({ completed: value.completed, progress: value.progress, status: value.status }));
  assert.doesNotThrow(() => { images[0].naturalWidth = 256; images[0].naturalHeight = 256; images[0].onload(); });
  assert.doesNotThrow(() => images[1].onerror());
  assert.doesNotThrow(() => { for (const expire of [...timers]) expire(); });
  let ready = false; assets.ready.then(() => { ready = true; });
  for (let i = 0; i < 8; i++) await Promise.resolve();
  assert.equal(ready, true, 'settlement finishes despite exceptions in every progress notification');
  assert.equal(await assets.ready, assets); assert.equal(assets.status, 'degraded');
  assert.equal(assets.completed, 3); assert.equal(assets.loaded, 1); assert.equal(assets.progress, 1);
  assert.deepEqual(Array.from(assets.failures).sort(), ['missing', 'stalled']);
  assert.ok(assets.get('good')); assert.equal(timers.size, 0);
  assert.equal(progress.at(-1).status, 'degraded', 'healthy listeners receive final completion after another listener throws');
  for (let i = 1; i < progress.length; i++) assert.ok(progress[i].progress >= progress[i - 1].progress);
  assert.ok(errors.length >= 4, 'subscriber failures remain diagnosable');
  assert.equal(context.ATS_VISUAL_BUNDLE, undefined); assert.doesNotThrow(unsubscribe);
});

test('shared coat atlases stay bounded at four even with malformed legacy variant values', () => {
  const manifest = JSON.parse(fs.readFileSync(path.join(__dirname, '../assets/visual/characters/manifest.json'), 'utf8'));
  const originals = new Map(manifest.atlases.map(a => [a.id, { naturalWidth: a.width, naturalHeight: a.height }]));
  let canvases = 0, painted = 0;
  const paint = () => new Proxy({ drawImage() { painted++; } }, { get(target, key) { return key in target ? target[key] : () => {}; } });
  class Renderer {}
  const context = vm.createContext({ ATSRenderer: Renderer,
    window: { ATSVisualAssets: { manifest: { characters: manifest }, get: id => originals.get(id) || null } },
    document: { createElement(tag) { assert.equal(tag, 'canvas'); canvases++; return { getContext: paint }; } } });
  vm.runInContext(source('character-art'), context);
  const r = new Renderer(); Object.assign(r, { time: .1, quality: 'high', reducedMotion: true, detailLevel: 3 });
  const variants = [-100, -1, -.3, 0, .5, .99, 1, 1.9, 2, 7, Infinity, NaN, 'bad'];
  for (const coatVariant of variants) for (const attacking of [false, true]) {
    const a = Object.freeze({ id: 'coat-limit', species: 'gorilla', hp: 100, maxHp: 100, coatVariant, _atlas: true,
      ...(attacking ? { animation: Object.freeze({ kind: 'slam', start: 0, duration: 1 }) } : {}) });
    r.drawPrimateGeometry(paint(), a, { mini: true });
  }
  assert.equal(r.characterCoats.size, 4); assert.equal(canvases, 4, 'two shades shared by ordinary and action sheets only');
  assert.deepEqual(Array.from(r.characterCoats.keys()).sort(), ['actions:1', 'actions:2', 'apes:1', 'apes:2']);
  assert.ok(painted >= variants.length * 2, 'legacy variants continue drawing normally');
  const low = new Renderer(); Object.assign(low, { time: .1, quality: 'low', reducedMotion: true, detailLevel: 3 });
  low.drawPrimateGeometry(paint(), { id: 'low', species: 'gorilla', hp: 100, maxHp: 100, coatVariant: 2, _atlas: true }, { mini: true });
  assert.equal(low.characterCoats, undefined, 'low quality allocates no coat textures');
});
