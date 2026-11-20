import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  ENEMIES, MAPS, STARTER_LOADOUT, STARTING_WAVES, STEP_MS, TOWERS, createSimulation, getPlacementError, isCompleteLoadout, normalizeLoadout,
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

function reinforce(sim, state, step, buildOrder = plan) {
  if (step % 20 !== 0) return;
  for (const [slot, x, y] of buildOrder) {
    if (state.structures.some(structure => structure.x === x && structure.y === y)) continue;
    if (sim.command(mouse.id, { type: 'build', slot, x, y }).code === 'not-enough-cheese') break;
  }
  if (step <= 2400) return;
  for (const [slot, x, y] of buildOrder) {
    if (slot === 1 || slot === 3) sim.command(mouse.id, { type: 'upgrade', x, y, branch: slot === 1 ? 'marksman' : 'cluster' });
  }
}

function dodgeCat(sim, state, step) {
  if (step % 4 !== 0) return;
  const player = state.players[0];
  const cat = state.enemies.find(enemy => enemy.type === 'cat');
  if (!cat) return;
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

function runPerimeter(sim, ticks, onStep) {
  const route = [{ x: 24, y: 4 }, { x: 42, y: 4 }, { x: 44, y: 21 }, { x: 43, y: 35 }, { x: 43, y: 44 },
    { x: 26, y: 44 }, { x: 9, y: 44 }, { x: 9, y: 25 }, { x: 4, y: 22 }, { x: 18, y: 22 }];
  let state = sim.snapshot();
  let waypoint = 0;
  let lastEvent = state.events.at(-1)?.id ?? 0;
  for (let step = 0; step < ticks && state.status === 'active'; step++) {
    if (step % 400 === 0 || distanceSquared(state.players[0], route[waypoint]) < 4) {
      waypoint = (waypoint + 1) % route.length;
      sim.command(mouse.id, { type: 'move', ...route[waypoint] });
    }
    sim.step();
    state = sim.snapshot();
    const recent = state.events.filter(event => event.id > lastEvent);
    if (recent.length) lastEvent = recent.at(-1).id;
    onStep(state, recent);
  }
  return state;
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
  assert.equal(sim.command(mouse.id, upgrade).code, 'not-enough-cheese');
  assert.equal(sim.command(mouse.id, { type: 'move', x: 21, y: 25 }).ok, true);
  tick(sim, 25);
  assert.equal(sim.command(mouse.id, upgrade).ok, true);
  assert.equal(sim.command(mouse.id, upgrade).code, 'already-upgraded');
  assert.equal(sim.snapshot().players[0].cheese, 1);
  assert.equal(sim.snapshot().players[1].cheese, 10);
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
  assert.equal(player.cheese, 12);
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

test('the opening Cat hunts until a normal battle is won by dealing the killing blow', () => {
  const loadout = ['wall', 'turret', 'generator', 'rocket', 'repair', 'ultra-wall', 'shield', 'laser'];
  const sim = createSimulation({ matchId: 'wintry', difficulty: 'normal', players: [{ ...mouse, loadout }] });
  const economy = [[21, 26], [24, 27], [22, 28], [23, 28], [24, 28], [25, 28], [24, 26],
    [25, 26], [23, 27], [26, 27], [26, 26], [24, 29]];
  const buildOrder = [
    ...economy.slice(0, 7).map(([x, y]) => [2, x, y]),
    ...plan.filter(([, x, y]) => !economy.some(([ex, ey]) => ex === x && ey === y))
      .map(([slot, x, y]) => [slot === 0 && (x === 22 || x === 27) ? 5 : slot, x, y]),
  ];
  const launches = new Map();
  const impacts = new Map();
  const simultaneousHits = new Map();
  const defeatingHits = new Set();
  let seenLiveFlight = false;
  let latest = 0;
  let results = 0;
  let state = sim.snapshot();
  const cat = state.enemies.find(enemy => enemy.type === 'cat');
  assert.ok(cat && distanceSquared(cat, state.players[0]) > 18 ** 2);
  assert.equal(state.objectives.length, 0);
  for (let step = 0; step < 5800 && state.status === 'active'; step++) {
    dodgeCat(sim, state, step);
    reinforce(sim, state, step, buildOrder);
    sim.step();
    state = sim.snapshot();
    const pursuingCat = state.enemies.find(enemy => enemy.type === 'cat');
    if (state.status === 'active') {
      assert.equal(pursuingCat?.id, cat.id);
      assert.ok(pursuingCat.hp > 0);
    }
    for (const projectile of state.projectiles) {
      const previous = launches.get(projectile.id);
      if (previous && (previous.x !== projectile.x || previous.y !== projectile.y)) seenLiveFlight = true;
    }
    for (const event of state.events) {
      if (event.id <= latest) continue;
      latest = event.id;
      assert.notEqual(event.kind, 'tail-sweep');
      assert.notEqual(event.kind, 'cat-charge');
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
      if (event.type === 'result') results++;
    }
  }
  assert.equal(state.status, 'won');
  assert.ok(state.resultAt >= 120_000, 'the Cat withstands the opening defense before the eventual victory');
  assert.ok(state.wave >= 3);
  assert.ok(state.nextWaveAt > state.time);
  assert.equal(state.enemies.some(enemy => enemy.type === 'cat'), false);
  assert.ok(defeatingHits.has(cat.id));
  assert.ok(results === 1 && seenLiveFlight && launches.size > 50);
  assert.ok([...impacts.values()].some(impact => impact.reason === 'target'));
  assert.ok([...simultaneousHits.values()].some(shooters => shooters.size > 1));
  const finished = sim.snapshot();
  tick(sim, 5);
  assert.deepEqual(sim.snapshot(), finished);
});

test('rats from opposite entrances attack their nearest structure and leave a closer mouse alone', () => {
  const sim = createSimulation({ matchId: 'closest-2', ranked: true, players: [mouse, { id: 'bait' }] });
  const west = sim.command(mouse.id, { type: 'build', slot: 0, x: 18, y: 23 });
  const east = sim.command(mouse.id, { type: 'build', slot: 0, x: 27, y: 24 });
  assert.ok(west.ok && east.ok);
  assert.ok(sim.command('bait', { type: 'move', x: 11, y: 23 }).ok);
  tick(sim, 25_000 / STEP_MS);
  const state = sim.snapshot();
  for (const [entrance, structure] of [['west', west], ['east', east]]) {
    const rat = state.events.find(event => event.type === 'spawn' && event.entrance === entrance);
    const attack = state.events.find(event => event.type === 'attack' && event.entityId === rat?.entityId);
    assert.equal(attack?.targetId, structure.id);
  }
  assert.equal(state.players.find(player => player.id === 'bait').hp, 62);
});

test('the Cat patrols independently of distant mouse positions', () => {
  const stay = createSimulation({ matchId: 'distant-cat', players: [mouse] });
  const flee = createSimulation({ matchId: 'distant-cat', players: [mouse] });
  assert.ok(flee.command(mouse.id, { type: 'move', x: 23, y: 35 }).ok);
  tick(stay, 5_000 / STEP_MS);
  tick(flee, 5_000 / STEP_MS);
  assert.deepEqual(stay.snapshot().enemies[0], flee.snapshot().enemies[0]);
});

test('a dense late wave keeps individual enemies moving through the central lanes', () => {
  const escape = [[12, 23], [20, 18], [33, 23], [24, 38]];
  const players = escape.map((_, index) => ({ id: `mouse-${index}` }));
  const sim = createSimulation({ matchId: 'spread', players, startingWave: 10 });
  for (const [index, [x, y]] of escape.entries()) {
    assert.ok(sim.command(players[index].id, { type: 'move', x, y }).ok);
  }
  tick(sim, 50_000 / STEP_MS);
  const enemies = sim.snapshot().enemies.filter(enemy => enemy.type !== 'cat');
  assert.ok(enemies.length > 50);
  let neighbors = 0;
  let overlapping = 0;
  for (const [index, enemy] of enemies.entries()) {
    for (const other of enemies.slice(index + 1)) {
      const squared = distanceSquared(enemy, other);
      if (squared < 1.2 ** 2) neighbors++;
      if (squared < 0.36 ** 2) overlapping++;
    }
  }
  assert.ok(neighbors > 100 && overlapping < neighbors / 10);
});

test('harder ranked battles keep their late Cat attack telegraphs', () => {
  const skills = new Set(['tail-sweep', 'cat-charge']);
  for (const difficulty of ['hard', 'nightmare']) {
    const sim = createSimulation({ matchId: 'wintry', difficulty, ranked: true, players: [mouse] });
    const warnings = new Map();
    const resolved = new Set();
    const effects = new Set();
    const chargedTargets = new Set();
    let latest = 0;
    let traversed = false;
    let state = sim.snapshot();
    for (let step = 0; step < 4700 && state.status === 'active'; step++) {
      if (state.wave < 7) reinforce(sim, state, step);
      sim.step();
      state = sim.snapshot();
      for (const { type, chargeUntil, vx, vy } of state.enemies) {
        if (type === 'cat' && chargeUntil > state.time && Math.hypot(vx, vy) > 4) traversed = true;
      }
      for (const event of state.events) {
        if (event.id <= latest) continue;
        latest = event.id;
        if (event.type === 'wave' && event.wave === 7) {
          const x = Math.max(3, Math.min(44, event.x));
          const y = Math.max(3, Math.min(44, event.y));
          assert.equal(sim.command(mouse.id, { type: 'move', x, y }).ok, true);
        }
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
        } else if (event.type === 'damage' || event.type === 'heal') {
          assert.ok(resolved.has(event.kind));
          assert.ok(event.value > 0);
          effects.add(event.kind);
          if (event.kind === 'cat-charge') {
            assert.equal(chargedTargets.has(event.entityId), false);
            chargedTargets.add(event.entityId);
          }
        }
      }
    }
    assert.equal(state.wave, 7);
    assert.ok(effects.has(difficulty === 'hard' ? 'tail-sweep' : 'cat-charge'));
    if (difficulty === 'nightmare') assert.ok(traversed);
  }
});

test('later starts give the Cat more initial health and schedule stronger rats beyond wave seven', () => {
  const create = startingWave => createSimulation({ matchId: 'start-tier', startingWave, players: [mouse] });
  for (const invalid of [0, 7, 11, '5', null, NaN]) assert.throws(() => create(invalid), TypeError);
  const ratHealth = [];
  const bossHealth = [];
  for (const startingWave of STARTING_WAVES) {
    const sim = create(startingWave);
    const before = sim.snapshot();
    assert.equal(before.startingWave, startingWave);
    assert.equal(before.wave, startingWave - 1);
    assert.equal(before.players[0].cheese, 10);
    assert.equal(before.nextWaveAt, 30_000);
    const cat = before.enemies.find(enemy => enemy.type === 'cat');
    assert.equal(cat.hp, cat.maxHp);
    bossHealth.push(cat.maxHp);
    const after = runPerimeter(sim, before.nextWaveAt / STEP_MS, () => {});
    assert.equal(after.status, 'active');
    assert.equal(after.wave, startingWave);
    assert.equal(after.nextWaveAt - after.time, startingWave === 1 ? 45_000 : 30_000);
    assert.ok(after.events.find(event => event.type === 'wave' && event.count > 0));
    ratHealth.push(after.enemies.find(enemy => enemy.type === 'rat').maxHp);
  }
  assert.deepEqual(bossHealth, [8_250, 13_613, 20_213]);
  assert.equal(ratHealth[0], ENEMIES.rat.hp);
  assert.ok(ratHealth[0] < ratHealth[1] && ratHealth[1] < ratHealth[2]);
});

test('the opening Cat stalks slowly and claws without special attacks even on a high-wave Nightmare start', () => {
  const hp = ['easy', 'normal', 'hard', 'nightmare'].map(difficulty => {
    const sim = createSimulation({ matchId: 'slow-opening', difficulty, players: [mouse] });
    return sim.snapshot().enemies[0].maxHp;
  });
  assert.deepEqual(hp, [4_510, 8_250, 14_278, 22_440]);
  for (const startingWave of STARTING_WAVES) {
    const sim = createSimulation({ matchId: 'east', startingWave, difficulty: 'nightmare', players: [mouse] });
    tick(sim, 1_000 / STEP_MS);
    const cat = sim.snapshot().enemies[0];
    assert.ok(Math.hypot(cat.vx, cat.vy) > 0 && Math.hypot(cat.vx, cat.vy) < 1.25);
    tick(sim, 26_000 / STEP_MS);
    const attacks = sim.snapshot().events.filter(event => event.type === 'windup' && event.entityId === cat.id);
    assert.ok(attacks.length > 0);
    assert.ok(attacks.every(event => event.kind === 'claw'));
  }
});

test('an active Cat picks up speed, gains health faster, and recovers small amounts after a lull', () => {
  const sim = createSimulation({ matchId: 'combat-balance', players: [mouse] });
  assert.equal(sim.command(mouse.id, { type: 'build', slot: 1, x: 24, y: 24 }).ok, true);
  const firstCat = sim.snapshot().enemies[0];
  const recoveries = [];
  const growths = [];
  let openingSpeed = 0;
  let lateSpeed = 0;
  let lastDamage = -Infinity;
  const state = runPerimeter(sim, 96_000 / STEP_MS, (snapshot, events) => {
    const cat = snapshot.enemies.find(enemy => enemy.id === firstCat.id);
    const speed = Math.hypot(cat.vx, cat.vy);
    if (snapshot.time < 5_000) openingSpeed = Math.max(openingSpeed, speed);
    if (snapshot.time > 60_000) lateSpeed = Math.max(lateSpeed, speed);
    for (const event of events) {
      if (event.entityId !== firstCat.id) continue;
      if (event.type === 'damage') lastDamage = event.time;
      if (event.type === 'heal') {
        assert.equal(event.kind, 'cat-recover');
        assert.ok(event.time - lastDamage >= 14_000);
        assert.ok(event.time - (recoveries.at(-1)?.time ?? -Infinity) >= 10_000);
        assert.ok(event.value > 0 && event.value <= 8);
        recoveries.push(event);
      }
      if (event.type === 'cat-growth') growths.push(event);
    }
  });
  assert.equal(state.status, 'active');
  assert.ok(openingSpeed > 1 && lateSpeed > openingSpeed + 0.75);
  assert.ok(recoveries.length > 1);
  assert.deepEqual(growths.map(growth => growth.time), [60_000]);
  const current = state.enemies.find(enemy => enemy.type === 'cat');
  assert.equal(current.id, firstCat.id);
  assert.equal(current.maxHp, Math.ceil(firstCat.maxHp * 1.06));
  assert.ok(current.hp > 0 && current.hp <= current.maxHp);
});

test('nearby Menders can heal a wounded Cat a little during a high-wave fight', () => {
  const sim = createSimulation({ matchId: 'heals0', difficulty: 'easy', startingWave: 10, players: [mouse] });
  const opening = [[1, 21, 24], [2, 21, 26], [0, 22, 25], [0, 23, 24], [1, 25, 24], [1, 23, 27], [2, 24, 27]];
  const buildOrder = [...opening, ...plan.filter(slot => !opening.some(preferred => preferred.join() === slot.join()))];
  const heals = [];
  let state = sim.snapshot();
  const cat = state.enemies[0];
  let latest = 0;
  for (let step = 0; step < 60_000 / STEP_MS && state.status === 'active'; step++) {
    reinforce(sim, state, step, buildOrder);
    dodgeCat(sim, state, step);
    sim.step();
    state = sim.snapshot();
    for (const event of state.events) {
      if (event.id <= latest) continue;
      latest = event.id;
      if (event.type === 'heal' && event.kind === 'mend' && event.entityId === cat.id) heals.push(event);
    }
  }
  assert.equal(heals.length, 1);
  assert.ok(heals[0].value > 0 && heals[0].value <= 5 && heals[0].sourceId !== cat.id);
});

test('ranked survival keeps the fixed late Cat and scores completed waves and match time', () => {
  const create = options => createSimulation({ matchId: 'ranked-survival', ranked: true, players: [mouse], ...options });
  assert.throws(() => create({ startingWave: 5 }), TypeError);
  const sim = create();
  let state = sim.snapshot();
  assert.equal(state.enemies.length, 0);
  assert.equal(state.players[0].cheese, 26);
  assert.equal(state.nextWaveAt, 12_500);
  assert.deepEqual(sim.rankedScore, { waves: 0, timeMs: 0 });
  let bossAt;
  for (let i = 0; i < 4700 && sim.status === 'active'; i++) {
    reinforce(sim, state, i);
    dodgeCat(sim, state, i);
    sim.step();
    state = sim.snapshot();
    const cat = state.enemies.find(enemy => enemy.type === 'cat');
    if (cat && !bossAt) {
      bossAt = state.wave;
      assert.equal(state.nextWaveAt, null);
      assert.equal(cat.maxHp, 288);
    }
    assert.ok(sim.rankedScore.waves <= state.wave);
  }
  assert.equal(bossAt, 7);
  assert.equal(state.status, 'won');
  assert.equal(sim.rankedScore.timeMs, state.resultAt);
  assert.equal(sim.rankedScore.waves, 5);
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
  tick(sim, 535);
  const state = sim.snapshot();
  const cat = state.enemies.find(enemy => enemy.type === 'cat');
  const attack = state.events.find(event => event.type === 'damage' && event.sourceId === cat.id);
  assert.equal(attack?.entityId, wall.id);
  assert.equal(attack?.kind, 'claw');
  assert.equal(state.players[0].x, 22);
});

test('a surviving teammate keeps co-op active while the Cat changes targets', () => {
  const sim = createSimulation({ matchId: 'east', players: [mouse, { id: 'guest' }] });
  assert.equal(sim.command('guest', { type: 'move', x: 2, y: 24 }).ok, true);
  let caught;
  let revived;
  for (let step = 0; step < 1200 && !revived; step++) {
    sim.step();
    const state = sim.snapshot();
    if (state.players[0].hp === 0 && !caught) caught = state;
    if (caught && state.players[0].hp > 0) revived = state;
  }
  assert.ok(caught && revived);
  assert.equal(caught.status, 'active');
  assert.equal(revived.status, 'active');
  assert.ok(revived.players[1].hp > 0);
  const caughtCat = caught.enemies.find(enemy => enemy.type === 'cat');
  const revivedCat = revived.enemies.find(enemy => enemy.type === 'cat');
  assert.ok(distanceSquared(revivedCat, revived.players[1]) < distanceSquared(caughtCat, caught.players[1]));
});

test('land and cheese govern placement even with hundreds of the same building', () => {
  const loadout = normalizeLoadout(['generator', 'farm']);
  const sim = createSimulation({ matchId: 'large-fortress', players: [{ ...mouse, loadout }] });
  const state = sim.snapshot();
  state.structures = Array.from({ length: 421 }, (_, index) => ({
    x: 1 + index % 46, y: 1 + Math.floor(index / 46), ownerId: mouse.id,
    type: index % 2 ? 'generator' : 'farm',
  }));
  assert.equal(getPlacementError(state, mouse.id, 'generator', 22, 26), null);
  assert.equal(getPlacementError(state, mouse.id, 'farm', 22, 26), null);
  state.players[0].cheese = 0;
  assert.equal(getPlacementError(state, mouse.id, 'farm', 22, 26), 'not-enough-cheese');
});

test('the Ultra Wall uses wall placement rules and has the most health', () => {
  const type = 'ultra-wall';
  const loadout = normalizeLoadout([type]);
  const sim = createSimulation({ matchId: 'ultra-wall', players: [{ ...mouse, loadout }] });
  const build = axis => sim.command(mouse.id, { type: 'build', slot: 0, x: 22, y: 26, axis });
  assert.equal(build('x').code, 'not-enough-cheese');
  assert.equal(sim.command(mouse.id, { type: 'move', x: 21, y: 25 }).ok, true);
  tick(sim, 20);
  assert.equal(build('diagonal').code, 'invalid-axis');
  assert.equal(build('y').ok, true);
  const { hp, axis } = sim.snapshot().structures[0];
  assert.equal(axis, 'y');
  assert.equal(hp, TOWERS[type].hp);
  assert.ok(hp > TOWERS.barricade.hp && hp > TOWERS.wall.hp);
  assert.equal(sim.command(mouse.id, { type: 'move', x: 22, y: 26 }).ok, true);
});

test('map cheese is spread across valid tiles and replenishes after collection', () => {
  const sim = createSimulation({ matchId: 'cheese-supply', players: [mouse] });
  const opening = sim.snapshot().pickups;
  assert.ok(opening.length >= 24);
  for (const spot of [...opening, ...mill.cheeseRefills]) {
    assert.ok(!['#', '~'].includes(tileAt(mill, spot.x, spot.y)));
  }
  for (const [x, y] of [[21, 25], [19, 23]]) {
    assert.equal(sim.command(mouse.id, { type: 'move', x, y }).ok, true);
    tick(sim, 20);
  }
  assert.equal(sim.snapshot().pickups.length, opening.length - 2);
  tick(sim, 60);
  const refilled = sim.snapshot().pickups;
  assert.equal(refilled.length, opening.length);
  assert.equal(new Set(refilled.map(({ x, y }) => `${x},${y}`)).size, refilled.length);
  assert.equal(refilled.filter(entry => !opening.some(pickup => pickup.id === entry.id)).length, 2);
});
