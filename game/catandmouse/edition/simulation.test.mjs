import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  MAPS, STARTER_LOADOUT, STEP_MS, createSimulation, isCompleteLoadout, normalizeLoadout,
} from './simulation.mjs';
import { buildEnemyField, interceptPoint, movingCircleHit, terrainHit, tileAt } from './navigation.mjs';

const mill = MAPS['flour-mill'];
const mouse = { id: 'solo', name: 'Mill Mouse' };

function tick(simulation, count) {
  for (let i = 0; i < count; i++) simulation.step();
}

test('loadouts preserve legal slots, fill vacancies and reject repeated towers', () => {
  const partial = ['turret', 'turret', null, '__proto__', 'flea'];
  const loadout = normalizeLoadout(partial);
  assert.equal(isCompleteLoadout(partial), false);
  assert.equal(isCompleteLoadout(loadout), true);
  assert.equal(loadout[0], 'turret');
  assert.equal(loadout[4], 'flea');
  assert.deepEqual(normalizeLoadout(), STARTER_LOADOUT);
  const sparse = STARTER_LOADOUT.slice();
  delete sparse[3];
  assert.equal(isCompleteLoadout(sparse), false);
  assert.deepEqual(normalizeLoadout(sparse), STARTER_LOADOUT);
});

test('commands authenticate actors and guard resources, placement and structure ownership', () => {
  const sim = createSimulation({ matchId: 'ownership', players: [mouse, { id: 'guest' }] });
  const build = { type: 'build', slot: 1, x: 24, y: 24 };
  assert.equal(sim.command('unknown', build).code, 'invalid-command');
  assert.equal(sim.command(mouse.id, { ...build, slot: 100 }).code, 'invalid-slot');
  assert.equal(sim.command(mouse.id, { ...build, x: 25.3 }).code, 'invalid-cell');
  assert.equal(sim.command(mouse.id, { ...build, x: 2, y: 2 }).code, 'out-of-reach');
  assert.equal(sim.command(mouse.id, { ...build, x: 23, y: 23 }).code, 'blocked');
  assert.equal(sim.command(mouse.id, build).ok, true);
  assert.equal(sim.command(mouse.id, build).code, 'blocked');
  const upgrade = { type: 'upgrade', x: build.x, y: build.y, branch: 'marksman' };
  assert.equal(sim.command(mouse.id, upgrade).code, 'under-construction');
  tick(sim, 700 / STEP_MS);
  assert.equal(sim.command('guest', upgrade).code, 'not-your-structure');
  assert.equal(sim.command(mouse.id, upgrade).ok, true);
  assert.equal(sim.command(mouse.id, upgrade).code, 'already-upgraded');
  assert.equal(sim.snapshot().players[0].cheese, 14);
  assert.equal(sim.snapshot().players[1].cheese, 26);
  assert.equal(sim.snapshot().structures[0].branch, 'marksman');
});

test('click movement avoids structures, gathers cheese and preserves last idle facing', () => {
  const sim = createSimulation({ matchId: 'movement', players: [mouse] });
  assert.equal(sim.command(mouse.id, { type: 'build', slot: 0, x: 22, y: 26 }).ok, true);
  assert.equal(sim.command(mouse.id, { type: 'move', dx: Infinity, dy: 0 }).code, 'invalid-movement');
  assert.equal(sim.command(mouse.id, { type: 'move', x: 22, y: 26 }).code, 'blocked');
  assert.equal(sim.command(mouse.id, { type: 'move', x: 21, y: 25 }).ok, true);
  let lastHeading;
  for (let i = 0; i < 27; i++) {
    sim.step();
    const { x, y, vx, vy, facing } = sim.snapshot().players[0];
    assert.notDeepEqual([Math.round(x), Math.round(y)], [22, 26]);
    if (vx || vy) lastHeading = facing;
  }
  const player = sim.snapshot().players[0];
  assert.deepEqual([player.x, player.y, player.vx, player.vy, player.facing], [21, 25, 0, 0, lastHeading]);
  assert.equal(player.cheese, 28);
  assert.equal(sim.command(mouse.id, { type: 'move', dx: 1, dy: -1 }).ok, true);
  sim.step();
  const moving = sim.snapshot().players[0];
  assert.ok(Math.abs(Math.hypot(moving.vx, moving.vy) - 4.1) < 1e-9);
  assert.equal(moving.facing, -Math.PI / 4);
});

test('all entrances retain routes through dense defense rings to a breachable objective', () => {
  const structures = new Map();
  const objectives = mill.objectives.map(def => ({ ...def, hp: def.maxHp }));
  const goals = new Set(objectives.map(goal => goal.y * mill.size + goal.x));
  for (const goal of objectives) {
    for (const [dx, dy] of [[0, 1], [0, -1], [1, 0], [-1, 0]]) {
      const x = goal.x + dx;
      const y = goal.y + dy;
      structures.set(y * mill.size + x, { x, y, maxHp: 88 });
    }
  }
  const field = buildEnemyField(mill, objectives, structures);
  for (const spawn of mill.spawns) {
    const visited = new Set();
    let cell = spawn.y * mill.size + spawn.x;
    while (!goals.has(cell)) {
      assert.ok(cell >= 0 && !visited.has(cell));
      assert.notEqual(tileAt(mill, cell % mill.size, Math.floor(cell / mill.size)), '#');
      visited.add(cell);
      cell = field[cell];
    }
    assert.ok([...visited].some(routeCell => structures.has(routeCell)));
  }
});

test('projectiles sweep through moving targets and collide with the leading terrain boundary', () => {
  const upcrossing = { previousX: 5, previousY: 1, x: 5, y: -1 };
  assert.ok(movingCircleHit(4, 0, 2, 0, upcrossing, 0.01) < 0.5);
  assert.equal(movingCircleHit(4, 0, 2, 0, { ...upcrossing, previousX: 8, x: 8 }, 0.1), null);
  assert.equal(terrainHit(mill, 16, 8, 19, 8), 1 / 6);
  assert.equal(terrainHit(mill, 20, 24, 32, 24), null);
  const predicted = interceptPoint({ x: 0, y: 0 }, { x: 3, y: 0, vx: 1, vy: 0 }, 4, 1800);
  assert.deepEqual(predicted, { x: 4, y: 0 });
});

test('a seeded offline battle can be won and each shot and result happen once', () => {
  const sim = createSimulation({ matchId: 'wintry', difficulty: 'normal', players: [mouse] });
  const plan = [
    [2, 21, 26], [2, 24, 27], [2, 22, 28],
    [1, 21, 24], [1, 25, 24], [1, 23, 27], [1, 26, 25], [1, 23, 22],
    [1, 24, 22], [1, 22, 23], [1, 25, 23], [1, 26, 24], [1, 24, 25], [1, 21, 28],
    [2, 23, 28], [2, 24, 28], [2, 25, 28],
    [3, 22, 26], [3, 25, 26], [3, 24, 24], [3, 22, 25],
    [4, 24, 26], [4, 20, 26], [4, 26, 27], [5, 23, 25], [5, 22, 22], [6, 22, 27], [6, 26, 23],
    [3, 23, 24], [3, 26, 28], [3, 28, 26], [3, 20, 24],
  ];
  const launches = new Map();
  const impacts = new Map();
  const simultaneousHits = new Map();
  const defeatingHits = new Set();
  const waveSizes = new Map();
  let seenLiveFlight = false;
  let latest = 0;
  let results = 0;
  let state = sim.snapshot();
  for (let step = 0; step < 4700 && state.status === 'active'; step++) {
    if (step % 20 === 0) {
      for (const [slot, x, y] of plan) {
        if (state.structures.some(structure => structure.x === x && structure.y === y)) continue;
        const built = sim.command(mouse.id, { type: 'build', slot, x, y });
        if (built.code === 'not-enough-cheese') break;
      }
      if (step > 2400) {
        for (const [slot, x, y] of plan) {
          if (slot === 1 || slot === 3) sim.command(mouse.id, { type: 'upgrade', x, y, branch: slot === 1 ? 'marksman' : 'cluster' });
        }
      }
    }
    sim.step();
    state = sim.snapshot();
    for (const projectile of state.projectiles) {
      const previous = launches.get(projectile.id);
      if (previous && (previous.x !== projectile.x || previous.y !== projectile.y)) seenLiveFlight = true;
    }
    for (const event of state.events) {
      if (event.id <= latest) continue;
      latest = event.id;
      if (event.type === 'shot') launches.set(event.projectileId, event);
      if (event.type === 'impact') {
        assert.equal(impacts.has(event.projectileId), false);
        const launch = launches.get(event.projectileId);
        assert.equal(launch.shooterId, event.shooterId);
        assert.ok(launch.time < event.time);
        impacts.set(event.projectileId, event);
        if (event.targetId) {
          const key = `${event.tick}:${event.targetId}`;
          const shooters = simultaneousHits.get(key) || new Set();
          shooters.add(event.shooterId);
          simultaneousHits.set(key, shooters);
        }
      }
      if (event.type === 'defeat' && event.entityId.startsWith('enemy-')) {
        assert.equal(defeatingHits.has(event.entityId), false);
        defeatingHits.add(event.entityId);
      }
      if (event.type === 'wave') waveSizes.set(event.wave, event.count);
      if (event.type === 'result') results++;
    }
  }
  assert.equal(state.status, 'won');
  assert.equal(state.wave, 7);
  assert.ok(waveSizes.get(7) >= 24);
  assert.ok(results === 1 && seenLiveFlight && launches.size > 50);
  assert.ok([...impacts.values()].some(impact => impact.reason === 'target'));
  assert.ok([...simultaneousHits.values()].some(shooters => shooters.size > 1));
  const finished = sim.snapshot();
  tick(sim, 5);
  assert.deepEqual(sim.snapshot(), finished);
});

test('an ignored match defeats the team and produces identical state for the same seed', () => {
  const first = createSimulation({ matchId: 'bare', players: [mouse] });
  const second = createSimulation({ matchId: 'bare', players: [mouse] });
  tick(first, 1400);
  tick(second, 1400);
  assert.equal(first.snapshot().status, 'lost');
  assert.deepEqual(first.snapshot(), second.snapshot());
  assert.ok(first.snapshot().objectives.every(objective => objective.hp === 0));
});
