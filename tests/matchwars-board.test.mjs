import test from 'node:test';
import assert from 'node:assert/strict';
import { Board, ELEMENTS } from '../game/matchwars/board.mjs';

function rng(seed = 1) {
  return () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; };
}
function fixture(size = 5) {
  const board = new Board({ size, rng: rng(87) });
  board.restore(Array.from({ length: size }, (_, r) => Array.from({ length: size }, (_, c) => ({
    id: r * size + c + 1000, key: ELEMENTS[(r + c) % ELEMENTS.length], special: null, relic: null,
  }))));
  return board;
}
function place(board, r, c, key, extra = {}) { Object.assign(board.grid[r][c], { key, ...extra }); }
const coords = points => new Set(points.map(({ r, c }) => `${r},${c}`));

test('generated boards start match-free, playable, and have unique stable IDs, including constant RNG', () => {
  for (let seed = 0; seed < 100; seed++) {
    const board = new Board({ rng: seed === 0 ? () => 0 : rng(seed) });
    assert.equal(board._matches().length, 0);
    assert.ok(board.hint());
    assert.equal(new Set(board.grid.flat().map(tile => tile.id)).size, 49);
  }
});

test('invalid, non-adjacent, out-of-bounds, and unproductive swaps preserve the board', () => {
  const board = fixture();
  const before = board.snapshot();
  for (const [a, b] of [[{ r: 0, c: 0 }, { r: 4, c: 4 }], [{ r: -1, c: 0 }, { r: 0, c: 0 }], [{ r: 0, c: 0 }, { r: 0, c: 1 }]]) {
    assert.equal(board.trySwap(a, b).valid, false);
    assert.deepEqual(board.snapshot(), before);
  }
});

test('a three-match collects a relic once and gravity moves surviving IDs downward', () => {
  const board = fixture();
  place(board, 3, 0, 'ember', { relic: 'fury' });
  place(board, 3, 1, 'tide');
  place(board, 3, 2, 'ember');
  place(board, 3, 3, 'grove');
  place(board, 2, 1, 'ember');
  const result = board.trySwap({ r: 2, c: 1 }, { r: 3, c: 1 });
  assert.equal(result.valid, true);
  const step = result.steps[0];
  assert.deepEqual(step.groups.find(group => group.key === 'ember').buffs, ['fury']);
  assert.equal(step.groups.find(group => group.key === 'ember').count, 3);
  assert.equal(step.after[1][0].id, step.before[0][0].id);
  for (const { tile } of step.cleared) assert.ok(!step.after.flat().some(other => other.id === tile.id));
  assert.equal(board._matches().length, 0);
  assert.ok(board.hint());
});

test('four-matches create directional specials at the destination and five-matches create a wild', () => {
  for (const length of [4, 5]) {
    const board = fixture();
    for (let c = 0; c < length; c++) place(board, 0, c, c === 2 ? 'tide' : 'ember');
    if (length === 4) place(board, 0, 4, 'grove');
    place(board, 1, 2, 'ember');
    const { steps } = board.trySwap({ r: 1, c: 2 }, { r: 0, c: 2 });
    assert.equal(steps[0].created.length, 1);
    const creation = steps[0].created[0];
    assert.equal(creation.tile.special, length === 4 ? 'row' : 'wild');
    assert.deepEqual({ r: creation.r, c: creation.c }, { r: 0, c: 2 });
    assert.ok(!steps[0].cleared.some(cell => cell.tile.id === creation.tile.id));
    assert.ok(steps[0].after.flat().some(tile => tile.id === creation.tile.id && tile.special === creation.tile.special));
  }
  const board = fixture();
  for (let r = 0; r < 4; r++) place(board, r, 0, r === 2 ? 'tide' : 'ember');
  place(board, 4, 0, 'grove');
  place(board, 2, 1, 'ember');
  const { steps } = board.trySwap({ r: 2, c: 1 }, { r: 2, c: 0 });
  assert.equal(steps[0].created[0].tile.special, 'column');
});

test('intersecting lines form a nova and count the intersection only once', () => {
  const board = fixture();
  for (const [r, c] of [[2, 1], [2, 3], [2, 4], [1, 2], [3, 2]]) place(board, r, c, 'ember');
  place(board, 2, 2, 'tide');
  place(board, 4, 2, 'sun');
  const { steps } = board.trySwap({ r: 2, c: 1 }, { r: 2, c: 2 });
  const first = steps[0];
  assert.equal(first.created[0].tile.special, 'nova');
  assert.equal(first.groups.find(group => group.key === 'ember').count, 5);
  assert.equal(coords(first.groups.flatMap(group => group.cells)).size, first.groups.reduce((sum, group) => sum + group.count, 0));
});

test('swapped row specials detonate intersecting column specials exactly once', () => {
  const board = fixture();
  board.grid[2][2].special = 'row';
  board.grid[2][0].special = 'column';
  const { steps } = board.trySwap({ r: 2, c: 2 }, { r: 2, c: 3 });
  const first = steps[0];
  assert.equal(first.cleared.length, 9);
  const cleared = coords(first.cleared);
  for (let i = 0; i < 5; i++) { assert.ok(cleared.has(`2,${i}`)); assert.ok(cleared.has(`${i},0`)); }
});

test('wild swaps clear the partner element, while double wild clears the full board', () => {
  const board = fixture();
  board.grid[0][0].special = 'wild';
  const otherEmberId = board.grid[2][4].id;
  const result = board.trySwap({ r: 0, c: 0 }, { r: 0, c: 1 });
  assert.equal(result.valid, true);
  assert.ok(result.steps[0].cleared.some(cell => cell.tile.special === 'wild'));
  assert.ok(result.steps[0].cleared.some(cell => cell.tile.key === 'tide'));
  assert.ok(!result.steps[0].cleared.some(cell => cell.tile.id === otherEmberId));
  const double = fixture();
  double.grid[0][0].special = double.grid[0][1].special = 'wild';
  assert.equal(double.trySwap({ r: 0, c: 0 }, { r: 0, c: 1 }).steps[0].cleared.length, 25);
});

test('natural refills cascade with increasing chain numbers and end in a playable stable board', () => {
  let cascaded = false;
  for (let seed = 1; seed <= 30 && !cascaded; seed++) {
    const board = new Board({ rng: rng(seed) });
    const move = board.hint();
    const { steps } = board.trySwap(move.from, move.to);
    if (steps.length > 1) {
      cascaded = true;
      assert.deepEqual(steps.map(step => step.chain), steps.map((_, i) => i + 1));
      for (let i = 1; i < steps.length; i++) assert.deepEqual(steps[i].before, steps[i - 1].after);
      assert.equal(board._matches().length, 0);
      assert.ok(board.hint());
    }
  }
  assert.equal(cascaded, true, 'test seeds should include a naturally cascading move');
});

test('dead boards are detected and shuffling restores a match-free legal move', () => {
  const board = fixture();
  assert.equal(board.hint(), null);
  board.shuffle();
  assert.equal(board._matches().length, 0);
  assert.ok(board.hint());
  const constant = new Board({ rng: () => 0 });
  constant.shuffle();
  assert.ok(constant.hint());
});

test('snapshot/restore is detached, validates contents, and avoids ID reuse', () => {
  const board = fixture();
  const snap = board.snapshot();
  snap[0][0].key = 'void';
  assert.equal(board.grid[0][0].key, 'ember');
  const duplicate = board.snapshot(); duplicate[0][1].id = duplicate[0][0].id;
  assert.throws(() => board.restore(duplicate), /duplicate/);
  const bad = board.snapshot(); bad[0][0].special = 'unknown';
  assert.throws(() => board.restore(bad), /Invalid/);
  assert.throws(() => board.restore([]), /dimensions/);
  const restored = fixture();
  const oldIds = new Set(restored.grid.flat().map(tile => tile.id));
  const fresh = restored._tile();
  assert.ok(!oldIds.has(fresh.id));
});

test('long sessions and pathological refill randomness stay finite, stable, and playable', () => {
  for (const random of [rng(912), () => 0]) {
    const board = new Board({ rng: random });
    for (let move = 0; move < 100; move++) {
      const hint = board.hint();
      const result = board.trySwap(hint.from, hint.to);
      assert.equal(result.valid, true);
      assert.ok(result.steps.length >= 1 && result.steps.length <= 24);
      assert.equal(board._matches().length, 0);
      assert.ok(board.hint());
      assert.equal(new Set(board.grid.flat().map(tile => tile.id)).size, board.size ** 2);
    }
  }
});
