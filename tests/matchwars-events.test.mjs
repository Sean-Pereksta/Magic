import test from 'node:test';
import assert from 'node:assert/strict';
import { Board, ELEMENTS } from '../game/matchwars/board.mjs';
import { BOARD_EVENT_RATE, BOARD_EVENT_COOLDOWN, CURSE_FUSE } from '../game/matchwars/events.mjs';

const rng = (seed = 87) => () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; };
function authored(board = new Board({ events: true, stage: 5, rng: rng() }), carry = null) {
  const grid = Array.from({ length: 7 }, (_, r) => Array.from({ length: 7 }, (_, c) => ({
    id: 1000 + r * 7 + c, key: ELEMENTS[(r + c) % 6], special: null, relic: null, event: null,
  })));
  grid[0][0].key = grid[0][2].key = grid[1][1].key = 'ember';
  grid[0][1].key = 'tide';
  if (carry) grid[6][6] = { ...carry, event: carry.event ? { ...carry.event } : null };
  board.restore(grid);
  return board;
}
const move = board => board.trySwap({ r: 1, c: 1 }, { r: 0, c: 1 });
const active = board => board.grid.flat().filter(tile => tile.event);
const action = (result, name) => result.events.filter(event => event.action === name);

test('events are opt-in, stage-five gated, and absent from every initial/tutorial board', () => {
  for (const options of [{}, { events: true, stage: 4 }, { events: false, stage: 50 }, { events: true, stage: 5 }]) {
    const board = new Board({ ...options, rng: () => 0 });
    assert.equal(active(board).length, 0);
    if (!options.events || options.stage < 5) {
      board._refilling = true;
      assert.equal(board._tile('ember', false).event, null);
    }
  }
  assert.equal(BOARD_EVENT_RATE, .006);
  assert.equal(BOARD_EVENT_COOLDOWN, 8);
  assert.equal(CURSE_FUSE, 4);
});

test('actual refills observe the 0.6 percent event threshold and can create only one active event', () => {
  for (const [chance, expected] of [[.0059, 1], [.006, 0]]) {
    const board = authored();
    const draws = [0, .99, chance, ...(expected ? [0] : []), .2, .99, .99, .4, .99, .99];
    board.rng = () => draws.shift() ?? .5;
    const result = move(board);
    assert.equal(result.valid, true);
    assert.equal(action(result, 'spawned').length, expected);
    assert.equal(active(board).length, expected);
    assert.deepEqual(result.events, result.steps.flatMap(step => step.events));
  }
});

test('matching a supply cache awards one stable twenty-gold record while preserving relic rewards', () => {
  const board = authored();
  board.grid[0][0].event = { type: 'supply' };
  board.grid[0][0].relic = 'fury';
  const tileId = board.grid[0][0].id;
  const result = move(board);
  assert.deepEqual(action(result, 'collected').map(event => [event.tileId, event.gold]), [[tileId, 20]]);
  assert.ok(result.steps[0].groups.find(group => group.key === 'ember').buffs.includes('fury'));
  assert.equal(result.steps[0].cleared.find(cell => cell.tile.id === tileId).tile.event.type, 'supply');
  assert.equal(board.eventCooldown, 8);
});

test('frost rejects direct swaps and hints omit both directions involving the frozen rune', () => {
  const board = authored();
  board.grid[0][0].event = { type: 'frost' };
  const before = board.snapshot();
  const result = board.trySwap({ r: 0, c: 0 }, { r: 0, c: 1 });
  assert.equal(result.valid, false);
  assert.equal(result.reason, 'frozen');
  assert.deepEqual(board.snapshot(), before);
  assert.equal(board.successfulMoves, 0);
  assert.ok(board.legalMoves().every(({ from, to }) => !(from.r === 0 && from.c === 0) && !(to.r === 0 && to.c === 0)));
});

test('a color match thaws and clears a frozen rune without requiring it to be swapped', () => {
  const board = authored();
  board.grid[0][0].event = { type: 'frost' };
  const result = move(board);
  assert.equal(result.valid, true);
  assert.equal(result.thawed.length, 1);
  assert.equal(result.thawed[0].reason, 'match');
  assert.equal(action(result, 'thawed').length, 1);
  assert.ok(!board.grid.flat().some(tile => tile.id === result.thawed[0].tileId));
});

test('an adjacent clear thaws the event while keeping its underlying rune alive', () => {
  const board = authored();
  board.grid[1][0].event = { type: 'frost' };
  const tileId = board.grid[1][0].id;
  const result = move(board);
  assert.equal(result.thawed[0].tileId, tileId);
  assert.equal(result.thawed[0].reason, 'adjacent');
  assert.equal(board.grid.flat().find(tile => tile.id === tileId).event, null);
  assert.ok(!result.steps[0].cleared.some(cell => cell.tile.id === tileId));
});

test('special-chain blasts collect, purify, or thaw events outside the original match', () => {
  for (const type of ['supply', 'curse', 'frost']) {
    const board = authored();
    board.grid[2][2].special = 'row';
    board.grid[2][6].event = type === 'curse' ? { type, fuse: 1 } : { type };
    const result = board.trySwap({ r: 2, c: 2 }, { r: 2, c: 3 });
    const expected = type === 'supply' ? 'collected' : type === 'curse' ? 'purified' : 'thawed';
    assert.equal(action(result, expected).length, 1);
    assert.equal(action(result, 'reinforcement').length, 0);
    if (type === 'frost') assert.equal(result.thawed[0].reason, 'blast');
  }
});

test('a curse expires once after four successful decisions, never on invalid moves or cascade counts', () => {
  const board = authored();
  board.grid[6][6].event = { type: 'curse', fuse: 4 };
  let carry = board.grid[6][6];
  const before = board.snapshot();
  assert.equal(board.trySwap({ r: -1, c: 0 }, { r: 0, c: 0 }).valid, false);
  assert.deepEqual(board.snapshot(), before);
  let reinforcements = 0;
  for (let decision = 1; decision <= 4; decision++) {
    authored(board, carry);
    board.rng = rng(100 + decision);
    const result = move(board);
    reinforcements += action(result, 'reinforcement').length;
    carry = board.grid.flat().find(tile => tile.id === carry.id);
    assert.equal(carry.event?.fuse ?? 0, 4 - decision);
    assert.equal(reinforcements, decision === 4 ? 1 : 0);
  }
  assert.equal(board.successfulMoves, 4);
  assert.equal(carry.event, null);
  const result = move(authored(board, carry));
  assert.equal(action(result, 'reinforcement').length, 0);
});

test('matching a curse on its last available move purifies it before its fuse expires', () => {
  const board = authored();
  board.grid[0][0].event = { type: 'curse', fuse: 1 };
  const result = move(board);
  assert.equal(action(result, 'purified').length, 1);
  assert.equal(action(result, 'reinforcement').length, 0);
});

test('a curse born during a refill keeps its four-move fuse for the entire birth move', () => {
  const board = authored();
  const draws = [0, .99, .001, .9, .2, .99, .4, .99];
  board.rng = () => draws.shift() ?? .5;
  const result = move(board);
  const spawned = action(result, 'spawned');
  assert.equal(spawned.length, 1);
  assert.equal(spawned[0].type, 'curse');
  assert.equal(active(board)[0].event.fuse, 4);
  assert.equal(board.successfulMoves, 1);
});

test('eight successful moves separate resolved events, and invalid attempts do not advance the cooldown', () => {
  const board = authored();
  board.grid[0][0].event = { type: 'supply' };
  move(board);
  assert.equal(board.eventCooldown, 8);
  board.trySwap({ r: -1, c: 0 }, { r: 0, c: 0 });
  assert.equal(board.eventCooldown, 8);
  for (let decision = 1; decision <= 8; decision++) {
    authored(board);
    board.rng = () => 0;
    const result = move(board);
    assert.equal(action(result, 'spawned').length, decision === 8 ? 1 : 0);
  }
});

test('event snapshots are detached, validated, and retained through a pathological reshuffle', () => {
  const board = authored();
  board.grid[6][6].event = { type: 'curse', fuse: 2 };
  const tileId = board.grid[6][6].id;
  const snapshot = board.snapshot();
  snapshot[6][6].event.fuse = 1;
  assert.equal(board.grid[6][6].event.fuse, 2);
  const bad = board.snapshot(); bad[1][0].event = { type: 'frost' };
  assert.throws(() => board.restore(bad), /multiple/);
  const invalid = board.snapshot(); invalid[6][6].event.fuse = 0;
  assert.throws(() => board.restore(invalid), /Invalid/);
  board.rng = () => 0;
  board.shuffle();
  assert.equal(active(board).length, 1);
  assert.equal(active(board)[0].id, tileId);
  assert.equal(active(board)[0].event.fuse, 2);
  assert.equal(board._matches().length, 0);
  assert.ok(board.hint());
});

test('long seeded sessions stay playable with one event at most and no duplicate reward records', () => {
  for (let seed = 1; seed <= 4; seed++) {
    const board = new Board({ events: true, stage: 40, rng: rng(seed) });
    let lastSpawn = -100;
    for (let decision = 1; decision <= 60; decision++) {
      const hint = board.hint();
      assert.ok(hint);
      const result = board.trySwap(hint.from, hint.to);
      assert.equal(result.valid, true);
      assert.ok(active(board).length <= 1);
      assert.equal(new Set(result.events.map(event => `${event.tileId}:${event.action}`)).size, result.events.length);
      if (action(result, 'spawned').length) { assert.ok(decision - lastSpawn >= 8); lastSpawn = decision; }
      assert.equal(board._matches().length, 0);
      assert.ok(board.hint());
    }
  }
});
