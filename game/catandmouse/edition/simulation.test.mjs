import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  MAPS, STARTER_LOADOUT, STEP_MS, createSimulation, getPlacementError, isCompleteLoadout, normalizeLoadout,
} from './simulation.mjs';
import { buildEnemyField, distanceSquared, interceptPoint, movingCircleHit, terrainHit, tileAt } from './navigation.mjs';

const mill = MAPS['flour-mill'];
const mouse = { id: 'solo', name: 'Mill Mouse' };
const plan = [
  ...[[22, 25], [23, 24], [24, 23], [26, 23], [27, 25], [27, 27], [25, 29], [23, 29], [21, 27], [21, 25]]
    .map(([x, y]) => [0, x, y]),
  [2, 21, 26], [2, 24, 27], [2, 22, 28],
  [1, 21, 24], [1, 25, 24], [1, 23, 27], [1, 26, 25], [1, 23, 22],
  [1, 24, 22], [1, 22, 23], [1, 25, 23], [1, 26, 24], [1, 24, 25], [1, 21, 28],
  [2, 23, 28], [2, 24, 28], [2, 25, 28],
  [3, 22, 26], [3, 25, 26], [3, 24, 24], [3, 22, 25],
  [4, 24, 26], [4, 20, 26], [4, 26, 27], [5, 23, 25], [5, 22, 22], [6, 22, 27], [6, 26, 23],
  [3, 23, 24], [3, 26, 28], [3, 28, 26], [3, 20, 24],
];

function tick(simulation, count) {
  for (let i = 0; i < count; i++) simulation.step();
}

function reinforce(sim, state, step) {
  if (step % 20 !== 0) return;
  for (const [slot, x, y] of plan) {
    if (state.structures.some(structure => structure.x === x && structure.y === y)) continue;
    if (sim.command(mouse.id, { type: 'build', slot, x, y }).code === 'not-enough-cheese') break;
  }
  if (step <= 2400) return;
  for (const [slot, x, y] of plan) {
    if (slot === 1 || slot === 3) sim.command(mouse.id, { type: 'upgrade', x, y, branch: slot === 1 ? 'marksman' : 'cluster' });
  }
}

function dodgeCat(sim, state, step) {
  if (step % 4 !== 0) return;
  const player = state.players[0];
  const cat = state.enemies.find(enemy => enemy.type === 'cat');
  const windup = cat.windupUntil > state.time;
  const danger = windup ? { x: cat.attackX, y: cat.attackY } : cat;
  const base = { x: 23.5, y: 25.5 };
  const threatened = windup || distanceSquared(player, cat) < 36;
  let bestScore = -Infinity;
  let destination;
  for (const dx of [-1, 0, 1]) {
    for (const dy of [-1, 0, 1]) {
      const length = Math.max(1, Math.hypot(dx, dy));
      const point = { x: player.x + dx / length * 2.4, y: player.y + dy / length * 2.4 };
      if (['#', '~'].includes(tileAt(mill, point.x, point.y))) continue;
      const escape = Math.min(10, Math.sqrt(distanceSquared(point, cat)));
      const margin = windup ? Math.min(7, Math.sqrt(distanceSquared(point, danger))) : 0;
      const score = (threatened ? escape * 0.83 + margin * 1.55 : 0) - Math.sqrt(distanceSquared(point, base)) * 0.74;
      if (score > bestScore) {
        bestScore = score;
        destination = { x: Math.round(point.x), y: Math.round(point.y) };
      }
    }
  }
  if (destination) sim.command(mouse.id, { type: 'move', ...destination });
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
  for (const [x, y, code] of [[25.3, 24, 'invalid-cell'], [2, 2, 'out-of-reach'], [23, 26, 'spawn-protected']]) {
    assert.equal(getPlacementError(sim.snapshot(), mouse.id, 'turret', x, y), code);
    assert.equal(sim.command(mouse.id, { ...build, x, y }).code, code);
  }
  assert.equal(getPlacementError(sim.snapshot(), mouse.id, 'turret', build.x, build.y), null);
  assert.equal(sim.command(mouse.id, build).ok, true);
  assert.equal(sim.command(mouse.id, build).code, 'occupied');
  assert.equal(getPlacementError(sim.snapshot(), mouse.id, 'turret', build.x, build.y), 'occupied');
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

test('wall axes persist in snapshots and both directions occupy one tile', () => {
  const loadout = normalizeLoadout(['wall', 'barricade', 'turret']);
  const sim = createSimulation({ matchId: 'directional-walls', players: [{ ...mouse, loadout }] });
  const place = (slot, x, y, axis) => sim.command(mouse.id, { type: 'build', slot, x, y, axis });
  assert.equal(place(0, 21, 26, 'diagonal').code, 'invalid-axis');
  assert.equal(place(0, 21, 26).ok, true);
  assert.equal(place(1, 22, 26, 'y').ok, true);
  assert.equal(place(2, 24, 24, 'y').ok, true);
  assert.deepEqual(sim.snapshot().structures.map(({ axis }) => axis), ['x', 'y', undefined]);
  for (const x of [21, 22]) assert.equal(sim.command(mouse.id, { type: 'move', x, y: 26 }).ok, true);
  assert.equal(sim.command(mouse.id, { type: 'move', x: 24, y: 24 }).ok, true);
  assert.equal(sim.command(mouse.id, { type: 'move', x: 22, y: 25 }).ok, true);
});

test('mouse movement passes through owned walls while respecting teammate walls', () => {
  const sim = createSimulation({ matchId: 'movement', players: [mouse, { id: 'guest' }] });
  assert.equal(sim.command(mouse.id, { type: 'build', slot: 0, x: 22, y: 26 }).ok, true);
  assert.equal(sim.command('guest', { type: 'build', slot: 0, x: 24, y: 26 }).ok, true);
  assert.equal(sim.command(mouse.id, { type: 'move', dx: Infinity, dy: 0 }).code, 'invalid-movement');
  assert.equal(sim.command(mouse.id, { type: 'move', x: 24, y: 26 }).code, 'blocked');
  assert.equal(sim.command('guest', { type: 'move', x: 22, y: 26 }).code, 'blocked');
  assert.equal(sim.command(mouse.id, { type: 'move', dx: -1, dy: 0 }).ok, true);
  tick(sim, 5);
  assert.equal(Math.round(sim.snapshot().players[0].x), 22);
  assert.equal(sim.command(mouse.id, { type: 'move', x: 22, y: 26 }).ok, true);
  tick(sim, 2);
  assert.equal(sim.snapshot().players[0].x, 22);
  assert.equal(sim.command(mouse.id, { type: 'move', x: 21, y: 25 }).ok, true);
  let lastHeading;
  for (let i = 0; i < 27; i++) {
    sim.step();
    const { x, y, vx, vy, facing } = sim.snapshot().players[0];
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

test('all entrances retain routes through dense defense rings to the mice', () => {
  const structures = new Map();
  const targets = mill.playerSpawns;
  const goals = new Set(targets.map(goal => goal.y * mill.size + goal.x));
  for (const goal of targets) {
    for (const [dx, dy] of [[0, 1], [0, -1], [1, 0], [-1, 0]]) {
      const x = goal.x + dx;
      const y = goal.y + dy;
      structures.set(y * mill.size + x, { x, y, maxHp: 88 });
    }
  }
  const field = buildEnemyField(mill, targets, structures);
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

test('a seeded offline battle can be won with the same Cat still hunting throughout', () => {
  const sim = createSimulation({ matchId: 'wintry', difficulty: 'normal', players: [mouse] });
  const launches = new Map();
  const impacts = new Map();
  const simultaneousHits = new Map();
  const defeatingHits = new Set();
  const waveSizes = new Map();
  const lateEnemies = new Set();
  let seenLiveFlight = false;
  let latest = 0;
  let results = 0;
  let state = sim.snapshot();
  const cat = state.enemies.find(enemy => enemy.type === 'cat');
  assert.ok(cat && distanceSquared(cat, state.players[0]) > 18 ** 2);
  assert.equal(state.objectives.length, 0);
  let catStaggered = false;
  for (let step = 0; step < 4700 && state.status === 'active'; step++) {
    dodgeCat(sim, state, step);
    reinforce(sim, state, step);
    sim.step();
    state = sim.snapshot();
    const pursuingCat = state.enemies.find(enemy => enemy.type === 'cat');
    assert.equal(pursuingCat?.id, cat.id);
    assert.ok(pursuingCat.hp > 0);
    if (pursuingCat.trapUntil > state.time) catStaggered = true;
    for (const projectile of state.projectiles) {
      const previous = launches.get(projectile.id);
      if (previous && (previous.x !== projectile.x || previous.y !== projectile.y)) seenLiveFlight = true;
    }
    for (const event of state.events) {
      if (event.id <= latest) continue;
      latest = event.id;
      assert.notEqual(event.kind, 'tail-sweep');
      assert.notEqual(event.kind, 'cat-charge');
      if (event.type === 'spawn' && (event.kind === 'sapper' || event.kind === 'mender')) {
        assert.ok(state.wave >= 6);
        lateEnemies.add(event.kind);
      }
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
  assert.deepEqual(state.enemies.map(enemy => enemy.type), ['cat']);
  assert.ok(catStaggered);
  assert.deepEqual([...lateEnemies].sort(), ['mender', 'sapper']);
  assert.ok(waveSizes.get(7) >= 24);
  assert.ok(results === 1 && seenLiveFlight && launches.size > 50);
  assert.ok([...impacts.values()].some(impact => impact.reason === 'target'));
  assert.ok([...simultaneousHits.values()].some(shooters => shooters.size > 1));
  const finished = sim.snapshot();
  tick(sim, 5);
  assert.deepEqual(sim.snapshot(), finished);
});

test('harder battles telegraph real special attacks and support actions', () => {
  const skills = new Set(['powder-blast', 'mend', 'tail-sweep', 'cat-charge']);
  for (const difficulty of ['hard', 'nightmare']) {
    const sim = createSimulation({ matchId: 'combat-balance', difficulty, players: [mouse] });
    const warnings = new Map();
    const resolved = new Set();
    const interrupted = new Set();
    const effects = new Set();
    const chargedTargets = new Set();
    let latest = 0;
    let traversed = false;
    let state = sim.snapshot();
    for (let step = 0; step < 4700 && state.status === 'active'; step++) {
      if (step >= 260) dodgeCat(sim, state, step);
      reinforce(sim, state, step);
      sim.step();
      state = sim.snapshot();
      for (const { type, chargeUntil, vx, vy } of state.enemies) {
        if (type === 'cat' && chargeUntil > state.time && Math.hypot(vx, vy) > 4) traversed = true;
      }
      for (const event of state.events) {
        if (event.id <= latest) continue;
        latest = event.id;
        if (!skills.has(event.kind)) continue;
        if (difficulty === 'hard') assert.notEqual(event.kind, 'cat-charge');
        if (event.type === 'windup') {
          assert.ok(event.duration >= 700 && event.until > event.time);
          assert.ok(event.radius > 0 && Number.isFinite(event.targetX) && Number.isFinite(event.targetY));
          warnings.set(event.entityId, event);
        } else if (event.type === 'attack') {
          const warning = warnings.get(event.entityId);
          assert.equal(event.kind, warning?.kind);
          assert.ok(event.time >= warning.until && event.time < warning.until + STEP_MS);
          assert.equal(event.radius, warning.radius);
          resolved.add(event.kind);
          if (event.kind === 'cat-charge') chargedTargets.clear();
        } else if (event.type === 'interrupt') {
          const warning = warnings.get(event.entityId);
          assert.equal(event.kind, warning?.kind);
          interrupted.add(event.kind);
        } else if (event.type === 'damage' || event.type === 'heal') {
          assert.ok(resolved.has(event.kind));
          assert.ok(event.value > 0);
          effects.add(event.kind);
          if (event.kind === 'mend') assert.ok(event.value <= 10);
          if (event.kind === 'cat-charge') {
            assert.equal(chargedTargets.has(event.entityId), false);
            chargedTargets.add(event.entityId);
          }
        }
      }
    }
    assert.equal(state.status, 'won');
    assert.ok(effects.has('tail-sweep') || interrupted.has('tail-sweep'));
    if (difficulty === 'nightmare') {
      assert.ok(traversed);
      assert.deepEqual([...effects].sort(), [...skills].sort());
    }
  }
});

test('an ignored match defeats the team and produces identical state for the same seed', () => {
  const first = createSimulation({ matchId: 'bare', players: [mouse] });
  const second = createSimulation({ matchId: 'bare', players: [mouse] });
  tick(first, 1400);
  tick(second, 1400);
  assert.equal(first.snapshot().status, 'lost');
  assert.deepEqual(first.snapshot(), second.snapshot());
  assert.equal(first.snapshot().players[0].hp, 0);
  assert.equal(first.snapshot().enemies[0].type, 'cat');
  assert.ok(first.snapshot().events.some(event => event.type === 'damage' && event.entityId === mouse.id));
});

test('the Cat can breach a wall its owner walks through', () => {
  const sim = createSimulation({ matchId: 'wall-pursuit', players: [mouse] });
  const wall = sim.command(mouse.id, { type: 'build', slot: 0, x: 22, y: 26 });
  assert.equal(wall.ok, true);
  assert.equal(sim.command(mouse.id, { type: 'move', x: 22, y: 26 }).ok, true);
  tick(sim, 240);
  const state = sim.snapshot();
  const cat = state.enemies.find(enemy => enemy.type === 'cat');
  const attack = state.events.find(event => event.type === 'damage' && event.sourceId === cat.id);
  assert.equal(attack?.entityId, wall.id);
  assert.equal(state.players[0].x, 22);
});
