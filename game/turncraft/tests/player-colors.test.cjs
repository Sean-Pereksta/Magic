const {test} = require('node:test');
const assert = require('node:assert/strict');
const {SpriteTintCache, SpriteRuntime} = require('../animation.js');
const {simulation} = require('./helpers/expansion-harness.cjs');

function canvasFactory() {
  const canvases = [];
  const createCanvas = () => {
    const operations = [], context = {globalAlpha: 1, globalCompositeOperation: 'source-over',
      drawImage(...args) {operations.push(['draw', ...args])},
      fillRect(...args) {operations.push(['fill', this.globalCompositeOperation, this.globalAlpha, this.fillStyle, ...args])}};
    const canvas = {width: 0, height: 0, operations, getContext: () => context};
    canvases.push(canvas);
    return canvas;
  };
  return {canvases, createCanvas};
}

test('same-faction players have obvious ownership colors with allies distinct from opponents', () => {
  for (const faction of ['frontier', 'ascendant', 'brood']) {
    const run = simulation(faction, {factions: [faction, faction, faction]});
    assert.deepEqual(run.plain('[playerTintFor(0),playerTintFor(1),playerTintFor(2),playerTintFor(-1),playerTintFor(null)]'),
      [null, '#ff334f', '#ff334f', null, null]);
    assert.equal(run('playerOwnerColor(0)'), run('FACTIONS[players[0].factionId].color'));
    run('players[0].teamId=0;players[1].teamId=0;players[2].teamId=1');
    assert.deepEqual(run.plain('[playerTintFor(0),playerTintFor(1),playerTintFor(2)]'), [null, '#35ddff', '#ff334f']);
    run('PLAYER=2');
    assert.deepEqual(run.plain('[playerTintFor(0),playerTintFor(1),playerTintFor(2)]'), ['#ff334f', '#ff334f', null]);
  }
});

test('minimap uses player ownership colors even when all players chose the same faction', () => {
  const run = simulation('ascendant', {factions: ['ascendant', 'ascendant', 'ascendant']});
  run("players[0].teamId=0;players[1].teamId=0;players[2].teamId=1;[0,1,2].forEach(slot=>addEntity('unit','probe',slot,10+slot,10));mini.offsetParent={};window.dotColors=[];mctx.fill=()=>window.dotColors.push(mctx.fillStyle);drawMinimap()");
  assert.deepEqual(run.plain('window.dotColors'), [run('FACTIONS.ascendant.color'), '#35ddff', '#ff334f']);
});

test('tinting caches frame crops, retains detail and transparency, and skips original own artwork', () => {
  const {canvases, createCanvas} = canvasFactory(), cache = new SpriteTintCache({createCanvas}), image = {width: 512, height: 512};
  assert.equal(cache.frame(image), null);
  assert.equal(canvases.length, 0);
  const options = {sx: 128, sy: 256, sw: 128, sh: 128, tint: '#ff334f'}, red = cache.frame(image, options);
  assert.equal(cache.frame(image, options), red, 'all troops sharing a pose reuse its tinted crop');
  assert.equal(canvases.length, 1);
  assert.deepEqual(red.operations[0], ['draw', image, 127, 255, 130, 130, 0, 0, 130, 130]);
  assert.deepEqual(red.spriteTintCrop, [1, 1, 128, 128]);
  assert.deepEqual(red.operations[1], ['fill', 'source-atop', .72, '#ff334f', 0, 0, 130, 130]);
  assert.notEqual(cache.frame(image, {...options, tint: '#35ddff'}), red);
  assert.notEqual(cache.frame(image, {...options, sx: 256}), red);
  assert.notEqual(cache.frame({...image}, options), red, 'a repaired or reloaded source cannot retain stale color crops');
  assert.equal(cache.stats().frames, 4);
});

test('tinted-frame memory is bounded and reuses recently used poses', () => {
  const {createCanvas} = canvasFactory(), cache = new SpriteTintCache({createCanvas, maxBytes: 800}), image = {width: 24, height: 8};
  const first = cache.frame(image, {sw: 8, sh: 8, tint: '#ff334f'}), second = cache.frame(image, {sx: 8, sw: 8, sh: 8, tint: '#ff334f'});
  assert.equal(cache.frame(image, {sw: 8, sh: 8, tint: '#ff334f'}), first);
  cache.frame(image, {sx: 16, sw: 8, sh: 8, tint: '#ff334f'});
  assert.equal(second.width, 0, 'the older unused frame is released');
  assert.equal(first.width, 10);
  assert.ok(cache.stats().bytes <= 800);
  cache.setBudget(256);
  assert.ok(cache.stats().bytes <= 256);
  cache.clear();
  assert.equal(cache.stats().bytes, 0);
  assert.equal(cache.stats().frames, 0);
});

test('animated, mirrored and death poses use the same ownership tint without changing bounds or timers', () => {
  const {createCanvas} = canvasFactory(), tintCache = new SpriteTintCache({createCanvas}), image = {width: 256, height: 128},
    cache = {get: () => image, setBudget() {}, setResolution() {}, sourceScale: () => 1},
    state = {sheet: 'pose.webp', frames: 2, fps: 4, frameWidth: 128, frameHeight: 128, directions: ['S', 'E'], rows: {S: 0, E: 0}, directionFlipX: {E: true}},
    runtime = new SpriteRuntime({cache, tintCache, manifest: {assets: {probe: {states: {idle: state, death: {...state, loop: false}}}}}}),
    draws = [], context = {globalAlpha: 1, save() {}, restore() {}, translate() {}, scale() {}, drawImage(...args) {draws.push(args)}},
    unit = {id: 1, kind: 'unit', type: 'probe', faction: 1, x: 10, y: 10, moveReadyAt: 4000};
  const before = {...unit}, bounds = runtime.bounds(unit, {size: 48});
  runtime.draw(context, unit, 1000, {tint: '#ff334f'});
  assert.notEqual(draws[0][0], image);
  runtime.drawState(context, 'probe', 'idle', {direction: 'E', tint: '#ff334f'});
  assert.notEqual(draws[1][0], image);
  runtime.kill(unit, 1000, true);
  runtime.drawGhosts(context, 1100, {worldToScreen: (x, y) => ({x, y}), tintFor: () => '#ff334f'});
  assert.notEqual(draws[2][0], image);
  assert.deepEqual(runtime.bounds(unit, {size: 48}), bounds);
  assert.deepEqual(unit, before);
});
