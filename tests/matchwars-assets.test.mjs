import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { inflateSync } from 'node:zlib';
import { CLASSES, DEFAULT_LOADOUT } from '../game/matchwars/classes.mjs?v=3';
import { ENEMIES } from '../game/matchwars/combat.mjs?v=3';
import { drawSpellEffect, drawCastingRune } from '../game/matchwars/vfx.mjs?v=3';

const DIRECTORY = new URL('../game/matchwars/assets/', import.meta.url);
const ATLAS_NAMES = ['troops', 'classes', 'enemies'];

// Decode just enough of an 8-bit RGBA PNG to verify the real alpha channel.
// This also detects corrupt/truncated image streams without image dependencies.
function inspectAtlas(name) {
  const bytes = readFileSync(new URL(`${name}.png`, DIRECTORY));
  assert.equal(bytes.subarray(0, 8).toString('hex'), '89504e470d0a1a0a', `${name}: PNG signature`);
  assert.equal(bytes.toString('ascii', 12, 16), 'IHDR');
  assert.equal(bytes[24], 8, `${name}: 8-bit channels`);
  assert.equal(bytes[25], 6, `${name}: RGBA color type`);
  assert.equal(bytes[28], 0, `${name}: noninterlaced atlas`);
  const width = bytes.readUInt32BE(16), height = bytes.readUInt32BE(20);
  assert.ok(width > 0 && height > 0);
  assert.equal(width % 8, 0, `${name}: eight complete columns`);
  assert.equal(height % 6, 0, `${name}: six complete rows`);
  assert.equal(width / 8, height / 6, `${name}: square cells`);
  const chunks = [];
  let cursor = 8, ended = false;
  while (cursor < bytes.length) {
    assert.ok(cursor + 12 <= bytes.length, `${name}: complete chunk header`);
    const length = bytes.readUInt32BE(cursor), type = bytes.toString('ascii', cursor + 4, cursor + 8);
    assert.ok(cursor + length + 12 <= bytes.length, `${name}: complete ${type} chunk`);
    if (type === 'IDAT') chunks.push(bytes.subarray(cursor + 8, cursor + length + 8));
    cursor += length + 12;
    if (type === 'IEND') { ended = true; break; }
  }
  assert.ok(ended && chunks.length, `${name}: complete image stream`);
  const raw = inflateSync(Buffer.concat(chunks)), stride = width * 4;
  assert.equal(raw.length, (stride + 1) * height, `${name}: complete pixel data`);
  const previous = new Uint8Array(stride), row = new Uint8Array(stride);
  let offset = 0, transparent = 0, visible = 0;
  const paeth = (a, b, c) => {
    const predicted = a + b - c;
    const pa = Math.abs(predicted - a), pb = Math.abs(predicted - b), pc = Math.abs(predicted - c);
    return pa <= pb && pa <= pc ? a : pb <= pc ? b : c;
  };
  for (let y = 0; y < height; y++) {
    const filter = raw[offset++];
    assert.ok(filter <= 4, `${name}: valid PNG filter`);
    for (let x = 0; x < stride; x++) {
      const left = x >= 4 ? row[x - 4] : 0, up = previous[x], upperLeft = x >= 4 ? previous[x - 4] : 0;
      const predictor = filter === 0 ? 0 : filter === 1 ? left : filter === 2 ? up
        : filter === 3 ? Math.floor((left + up) / 2) : paeth(left, up, upperLeft);
      row[x] = (raw[offset++] + predictor) & 255;
    }
    for (let x = 3; x < stride; x += 4) {
      if (row[x] === 0) transparent++;
      if (row[x] >= 200) visible++;
    }
    previous.set(row);
  }
  assert.ok(transparent > width * height * .01, `${name}: genuine transparent spacing`);
  assert.ok(visible > width * height * .05, `${name}: visible character art`);
  return { name, width, height, columns: 8, rows: 6, cell: width / 8 };
}

const atlases = new Map();
for (const name of ATLAS_NAMES) {
  test(`${name} sprite atlas is a complete transparent RGBA sheet with square animation cells`, () => {
    atlases.set(name, inspectAtlas(name));
  });
}

function assertAnimationBlock(definition) {
  const atlas = atlases.get(definition.spriteAtlas) || inspectAtlas(definition.spriteAtlas);
  assert.ok(Number.isInteger(definition.spriteRow) && definition.spriteRow >= 0
    && definition.spriteRow < atlas.rows, `${definition.name}: row exists in ${atlas.name}`);
  assert.ok(Number.isInteger(definition.spriteColumn) && definition.spriteColumn >= 0
    && definition.spriteColumn + 3 < atlas.columns, `${definition.name}: four animation frames fit`);
  assert.equal(definition.spriteColumn % 4, 0, `${definition.name}: starts at its own character block`);
  return `${atlas.name}:${definition.spriteRow}:${definition.spriteColumn}`;
}

test('all eighteen classes map to distinct, complete character animation blocks', () => {
  assert.equal(Object.keys(CLASSES).length, 18);
  const blocks = Object.values(CLASSES).map(assertAnimationBlock);
  assert.equal(new Set(blocks).size, 18, 'classes must have their own art instead of sharing recolors');
  for (const [element, id] of Object.entries(DEFAULT_LOADOUT)) {
    assert.equal(CLASSES[id].element, element);
    assert.equal(CLASSES[id].spriteAtlas, 'troops');
  }
});

test('all twelve new enemy species select their own correctly assigned animation blocks', () => {
  const rows = [
    ['emberhound', 'ashwitch'], ['tidecrab', 'siren'], ['briarspider', 'sporealchemist'],
    ['stormbeetle', 'zephyrdjinn'], ['solarchimera', 'inquisitor'], ['graveknight', 'soulleech'],
  ];
  const blocks = [];
  rows.forEach((ids, row) => ids.forEach((id, half) => {
    const definition = ENEMIES[id];
    assert.ok(definition, `${id}: enemy definition exists`);
    assert.equal(definition.spriteAtlas, 'enemies', `${id}: new species artwork`);
    assert.equal(definition.spriteRow, row, `${id}: correct species row`);
    assert.equal(definition.spriteColumn, half * 4, `${id}: correct species half`);
    blocks.push(assertAnimationBlock(definition));
  }));
  assert.equal(new Set(blocks).size, 12, 'new species cannot accidentally reuse another species');
  for (const definition of Object.values(ENEMIES)) assertAnimationBlock(definition);
});

function checkedCanvas() {
  let depth = 0;
  const calls = [];
  const state = { globalAlpha: 1, strokeStyle: '#000000', fillStyle: '#000000', lineWidth: 1,
    shadowBlur: 0, shadowColor: '#000000' };
  const geometry = new Set(['beginPath', 'closePath', 'moveTo', 'lineTo', 'stroke', 'fill', 'arc',
    'ellipse', 'quadraticCurveTo', 'bezierCurveTo', 'fillRect']);
  const finite = args => args.forEach(value => assert.ok(typeof value === 'number' && Number.isFinite(value),
    `canvas argument must be finite: ${String(value)}`));
  const context = new Proxy(state, {
    get(target, key) {
      if (Object.hasOwn(target, key)) return target[key];
      if (key === 'save') return () => { depth++; };
      if (key === 'restore') return () => { assert.ok(depth > 0, 'restore has matching save'); depth--; };
      if (key === 'createRadialGradient' || key === 'createLinearGradient') return (...args) => {
        finite(args);
        return { addColorStop(offset, color) {
          assert.ok(Number.isFinite(offset) && offset >= 0 && offset <= 1);
          assert.equal(typeof color, 'string');
        } };
      };
      if (geometry.has(key)) return (...args) => { finite(args); calls.push({ method: key, args }); };
      throw new Error(`Unsupported canvas operation: ${String(key)}`);
    },
    set(target, key, value) {
      assert.ok(Object.hasOwn(target, key), `recognized canvas property: ${String(key)}`);
      if (typeof target[key] === 'number') assert.ok(typeof value === 'number' && Number.isFinite(value));
      target[key] = value; return true;
    },
  });
  return { context, calls, depth: () => depth };
}

test('every class and enemy spell type has valid normal and reduced-motion artwork', () => {
  const types = new Set([...Object.values(CLASSES), ...Object.values(ENEMIES)]
    .filter(definition => definition.spell).map(definition => definition.spell.type));
  types.add('combo');
  for (const type of types) {
    for (const reduced of [false, true]) {
      const canvas = checkedCanvas();
      assert.equal(drawSpellEffect(canvas.context, { type, x: 250, toX: 500, color: '#87c8a6',
        age: .35, maxLife: .75 }, 220, 1, reduced), true, `${type}: supported spell artwork`);
      assert.ok(canvas.calls.length > 0, `${type}: produces visible geometry`);
      assert.equal(canvas.depth(), 0, `${type}: restores canvas state`);
    }
  }
});

test('casting runes use the supplied world position once and leave idle units unmarked', () => {
  const idle = checkedCanvas();
  drawCastingRune(idle.context, { x: 312, casting: 0 }, 220, 1);
  assert.equal(idle.calls.length, 0);
  const casting = checkedCanvas();
  drawCastingRune(casting.context, { x: 312, casting: .45, color: '#b79aef' }, 220, 1);
  const ring = casting.calls.find(call => call.method === 'ellipse');
  assert.equal(ring.args[0], 312, 'ring is anchored at the unit world x');
  assert.ok(Math.abs(ring.args[1] - 220) <= 10, 'ring sits at the supplied lane baseline');
  assert.equal(casting.depth(), 0);
});
