import test from 'node:test';
import assert from 'node:assert/strict';
import {Game, TYPES, MAPS} from '../engine.js';

function advance(game, seconds, inspect = () => {}) {
  for (let elapsed = 0; elapsed < seconds - 1e-9;) {
    const dt = Math.min(.08, seconds - elapsed);
    game.update(dt);
    inspect(game);
    elapsed += dt;
  }
}
function own(game, type, team = 0) {
  return game.entities.filter(e => e.team === team && e.type === type);
}
// Small isolated arenas retain both headquarters so victory does not end a test.
function arena(map = 'desktop', players = 2) {
  const game = new Game({map, players});
  game.entities = game.entities.filter(e => TYPES[e.type].building);
  game._think = Infinity;
  return game;
}
function spawn(game, type, team, x, y) {
  return game._spawn(type, team, x, y);
}

test('workers exclusively harvest one can, carry five, and deposit at a desk', () => {
  const deposits = [];
  const game = new Game({onEvent: e => {if (e.type === 'deposit' && e.team === 0) deposits.push(e);}});
  game._think = Infinity;
  const workers = own(game, 'twig');
  const can = game.entities.find(e => e.type === 'trash');
  const amount = can.amount;
  game.command(workers.slice(0, 2).map(e => e.id), {type:'gather', targetId:can.id});
  const observed = new Set();
  advance(game, 25, () => {
    const collecting = workers.filter(e => e.resourceId === can.id && e.workState === 'collecting');
    assert(collecting.length <= 1, 'a trash can has at most one active collector');
    if (collecting.length) observed.add(collecting[0].id);
    assert(workers.every(e => e.carrying === 0 || e.carrying === 5));
  });
  assert(observed.size > 0);
  assert(can.amount < amount);
  assert(deposits.length > 5, 'workers complete repeated return trips');
  for (const deposit of deposits) {
    assert.equal(deposit.amount, 5);
    const desk = own(game, 'desk').find(d => Math.hypot(d.x - deposit.x, d.y - deposit.y) <= d.r + TYPES.twig.r + 11);
    assert(desk, 'trash is credited only when a worker reaches a desk');
  }
  assert.equal(game.teams[0].trash, 300 + deposits.length * 5);
});

test('twigs cost 25, every troop costs 50, and queues reserve supply', () => {
  const game = new Game();
  game._think = Infinity;
  const desk = own(game, 'desk')[0], printer = own(game, 'printer')[0];
  assert(game.train(desk.id, 'twig').ok);
  assert.equal(game.teams[0].trash, 275);
  for (const type of ['rock', 'paper', 'scissors']) {
    const before = game.teams[0].trash;
    assert(game.train(printer.id, type).ok);
    assert.equal(before - game.teams[0].trash, 50);
  }
  assert.deepEqual(game.getSupply(0), {used:12, cap:20, reserved:4, units:8});
  assert(!game.train(desk.id, 'rock').ok);
  assert(!game.train(printer.id, 'twig').ok);
  advance(game, 5.1);
  assert.equal(own(game, 'twig').length, 6);
  assert.equal(own(game, 'rock').length, 2);
  assert.equal(own(game, 'paper').length, 1, 'a printer trains its queue serially');
});

test('queued units cannot exceed the supply cap', () => {
  const game = new Game(); game._think = Infinity; game.teams[0].trash = 10000;
  const desk = own(game, 'desk')[0], printer = own(game, 'printer')[0];
  for (let i = 0; i < 6; i++) assert(game.train(desk.id, 'twig').ok);
  for (let i = 0; i < 6; i++) assert(game.train(printer.id, 'rock').ok);
  assert.equal(game.getSupply(0).used, game.getSupply(0).cap);
  const secondPrinter = spawn(game, 'printer', 0, 600, 1100);
  const result = game.train(secondPrinter.id, 'paper');
  assert.equal(result.ok, false);
  assert.match(result.reason, /supply/i);
});

test('desk rallies send new twigs to a chosen trash can or movement destination', () => {
  const game = new Game(); game._think = Infinity;
  const desk = own(game, 'desk')[0], can = game.entities.find(e => e.type === 'trash');
  desk.rally = {x:can.x, y:can.y, targetId:can.id};
  assert(game.train(desk.id, 'twig').ok);
  advance(game, 4.1);
  const first = own(game, 'twig').at(-1);
  assert.equal(first.order.type, 'gather');
  assert.equal(first.resourceId, can.id);
  desk.rally = {x:700, y:1100, targetId:null};
  assert(game.train(desk.id, 'twig').ok);
  advance(game, 4.1);
  const second = own(game, 'twig').at(-1);
  assert.deepEqual(second.order, {type:'move', x:700, y:1100});
  advance(game, 6);
  assert(Math.hypot(second.x - 700, second.y - 1100) < 15);
});

for (const [winner, loser] of [['rock','scissors'], ['scissors','paper'], ['paper','rock']]) {
  test(`${winner} defeats ${loser}; the losing type cannot damage its counter`, () => {
    const game = arena();
    const a = spawn(game, winner, 0, 900, 800), b = spawn(game, loser, 1, 940, 800);
    advance(game, 2);
    assert(!game.getEntity(b.id));
    assert.equal(a.hp, a.maxHp);
    assert(game.stats.destroyed[0] >= 1);
  });
}

test('every troop type can damage buildings and defeat vulnerable twigs', () => {
  for (const type of ['rock','paper','scissors']) {
    const game = arena();
    const unit = spawn(game, type, 0, 1800, 800);
    const cabinet = own(game, 'cabinet', 1)[0];
    unit.x = cabinet.x - cabinet.r - 25; unit.y = cabinet.y;
    const hp = cabinet.hp;
    game.command([unit.id], {type:'attack', targetId:cabinet.id});
    advance(game, 1);
    assert(cabinet.hp < hp, `${type} damages a building`);
    const twig = spawn(game, 'twig', 1, unit.x + 20, unit.y);
    game.command([unit.id], {type:'attack', targetId:twig.id});
    advance(game, 2);
    assert(!game.getEntity(twig.id), `${type} defeats a twig`);
  }
});

test('a twig walks to a construction site; cabinets add supply only when complete', () => {
  const game = new Game();
  game._think = Infinity;
  const worker = own(game, 'twig')[0];
  const initial = game.getSupply(0).cap;
  assert(!game.build([worker.id], 'cabinet', 330, 800).ok, 'cannot overlap the desk');
  const result = game.build([worker.id], 'cabinet', 600, 1000);
  assert(result.ok);
  const building = game.getEntity(result.entityId);
  assert.equal(game.getSupply(0).cap, initial);
  advance(game, .2);
  assert.equal(building.constructing, building.buildTotal, 'construction waits for the builder to arrive');
  advance(game, 15);
  assert.equal(building.constructing, 0);
  assert.equal(game.getSupply(0).cap, initial + 8);
  assert(worker.order.type === 'gather', 'the builder resumes the economy');
  game.teams[0].trash = 0;
  assert(!game.build([worker.id], 'printer', 750, 900).ok);
});

test('printer research costs 150, takes time, and blocks duplicate research', () => {
  const game = new Game();
  game._think = Infinity;
  const printer = own(game, 'printer')[0];
  assert(!game.ability(own(game, 'paper').map(e => e.id), 900, 800).ok);
  assert(game.research(printer.id, 'paper').ok);
  assert.equal(game.teams[0].trash, 150);
  assert(!game.research(printer.id, 'paper').ok);
  advance(game, 13.9);
  assert.equal(game.teams[0].upgrades.paper, false);
  advance(game, .2);
  assert.equal(game.teams[0].upgrades.paper, true);
  assert(!game.research(printer.id, 'paper').ok);
});

test('crumple gives exactly three seconds of invulnerability and prevents attacks', () => {
  const game = arena();
  game.teams[0].upgrades.paper = true;
  const paper = spawn(game, 'paper', 0, 900, 800);
  const rock = spawn(game, 'rock', 1, 930, 800);
  spawn(game, 'scissors', 1, 930, 830);
  game.command([paper.id], {type:'attack', targetId:rock.id});
  assert(game.ability([paper.id], 900, 800).ok);
  assert.equal(paper.paperInvulnerable, 3);
  advance(game, 2.96);
  assert(paper.paperInvulnerable > 0 && paper.paperInvulnerable < .05);
  assert.equal(paper.hp, 100, 'scissors cannot hurt crumpled paper');
  assert.equal(rock.hp, 100, 'crumpled paper cannot attack');
  advance(game, .08);
  assert.equal(paper.paperInvulnerable, 0);
  assert(rock.hp < 100, 'attacks resume after the invulnerability period');
  advance(game, 1);
  assert(paper.hp < 100, 'paper becomes vulnerable again');
  assert(!game.ability([paper.id], 900, 800).ok, 'ability has a cooldown');
});

test('crumpled paper can move faster while its attacks are disabled', () => {
  const game = arena(); game.teams[0].upgrades.paper = true;
  const fast = spawn(game, 'paper', 0, 650, 800), normal = spawn(game, 'paper', 0, 650, 1100);
  assert(game.ability([fast.id], 1000, 800).ok);
  game.command([fast.id], {type:'move', x:1000, y:800});
  game.command([normal.id], {type:'move', x:1000, y:1100});
  advance(game, 1);
  assert(fast.x - 650 > (normal.x - 650) * 1.7);
});

test('scissors sweep kills paper in the chosen forward arc only', () => {
  const game = arena(); game.teams[0].upgrades.scissors = true;
  const scissors = spawn(game, 'scissors', 0, 900, 800);
  const front1 = spawn(game, 'paper', 1, 990, 800);
  const front2 = spawn(game, 'paper', 1, 970, 845);
  const behind = spawn(game, 'paper', 1, 820, 800);
  const distant = spawn(game, 'paper', 1, 1060, 800);
  const rock = spawn(game, 'rock', 1, 980, 770);
  const friendly = spawn(game, 'paper', 0, 950, 800);
  const invulnerable = spawn(game, 'paper', 1, 960, 780); invulnerable.paperInvulnerable = 3;
  assert(game.ability([scissors.id], 1200, 800).ok);
  assert(front1.hp <= 0 && front2.hp <= 0);
  for (const unit of [behind, distant, rock, friendly, invulnerable]) assert.equal(unit.hp, unit.maxHp);
  assert(game.effects.some(e => e.type === 'sweep' && e.angle === 0));
});

test('grouped rocks land in separate nearby positions, respecting impassable scenery', () => {
  const game = arena('breakroom'); game.teams[0].upgrades.rock = true;
  const rocks = [spawn(game, 'rock', 0, 800, 700), spawn(game, 'rock', 0, 830, 700), spawn(game, 'rock', 0, 860, 700)];
  assert(game.ability(rocks.map(e => e.id), 1200, 700).ok);
  const destinations = rocks.map(e => ({x:e.leap.toX, y:e.leap.toY}));
  assert.equal(new Set(destinations.map(p => `${p.x},${p.y}`)).size, rocks.length);
  for (const p of destinations) assert(Math.hypot(p.x - 1200, p.y - 700) < 100);
  advance(game, .7);
  assert(rocks.every(e => !e.leap));
  rocks.forEach((e, i) => assert(Math.hypot(e.x - destinations[i].x, e.y - destinations[i].y) < 5));
  const rock = rocks[0]; rock.abilityCooldown = 0;
  assert(game.ability([rock.id], 1200, 520).ok, 'targeting scenery finds a nearby valid landing');
  const landing = {x:rock.leap.toX, y:rock.leap.toY};
  for (const o of game.map.obstacles) assert(!(landing.x > o.x - rock.r && landing.x < o.x + o.w + rock.r && landing.y > o.y - rock.r && landing.y < o.y + o.h + rock.r));
});

test('move commands navigate around scenery on both maps', () => {
  for (const map of Object.keys(MAPS)) {
    const game = arena(map);
    const unit = spawn(game, 'rock', 0, 850, map === 'breakroom' ? 520 : 590);
    const target = {x:1800, y:unit.y};
    game.command([unit.id], {type:'move', ...target});
    advance(game, 17, () => {
      for (const o of game.map.obstacles) assert(!(unit.x > o.x - unit.r + .5 && unit.x < o.x + o.w + unit.r - .5 && unit.y > o.y - unit.r + .5 && unit.y < o.y + o.h + unit.r - .5));
    });
    assert(Math.hypot(unit.x - target.x, unit.y - target.y) < 18, `${map} route reaches its destination`);
  }
});

test('nearby destinations on opposite sides of a building corner take a real detour', () => {
  const game = arena();
  game.entities = game.entities.filter(e => e.type === 'desk');
  spawn(game, 'desk', 0, 140, 100);
  const unit = spawn(game, 'rock', 0, 66, 65);
  assert(!game._blocked(66, 65, unit.r));
  assert(!game._blocked(66, 127, unit.r));
  assert(!game._lineFree(66, 65, 66, 127, unit.r));
  game.command([unit.id], {type:'move', x:66, y:127});
  advance(game, 4);
  assert(Math.hypot(unit.x - 66, unit.y - 127) < 12);
});

test('placing a blueprint over ground units clears its footprint immediately', () => {
  const game = new Game(); game._think = Infinity;
  const worker = own(game, 'twig')[0];
  const troop = spawn(game, 'rock', 0, 650, 1000);
  const result = game.build([worker.id], 'printer', 650, 1000);
  assert(result.ok);
  const building = game.getEntity(result.entityId);
  assert(Math.hypot(troop.x - building.x, troop.y - building.y) >= troop.r + building.r + 2);
  assert(!game._blocked(troop.x, troop.y, troop.r));
  game.command([troop.id], {type:'move', x:850, y:1000});
  advance(game, 4);
  assert(Math.hypot(troop.x - 850, troop.y - 1000) < 15);
});

test('fifty troops push apart, travel through a corridor, and retain separate footprints at rest', t => {
  const game = arena('breakroom');
  const squad = Array.from({length:50}, (_, i) => spawn(game, 'rock', 0, 720 + i % 5 * 4, 780 + Math.floor(i / 5) * 4));
  const minDistance = () => {
    let min = Infinity;
    for (let i = 0; i < squad.length; i++) for (let j = i + 1; j < squad.length; j++) min = Math.min(min, Math.hypot(squad[i].x - squad[j].x, squad[i].y - squad[j].y));
    return min;
  };
  advance(game, 1);
  assert(minDistance() >= 36, 'an initially packed squad separates into physical footprints');
  game.command(squad.map(e => e.id), {type:'move', x:1660, y:800});
  let minimumMoving = Infinity;
  advance(game, 18, () => {
    minimumMoving = Math.min(minimumMoving, minDistance());
    for (const unit of squad) {
      for (const o of game.map.obstacles) assert(!(unit.x > o.x - unit.r + .5 && unit.x < o.x + o.w + unit.r - .5 && unit.y > o.y - unit.r + .5 && unit.y < o.y + o.h + unit.r - .5), 'pushback cannot place a troop inside scenery');
      for (const building of game.entities.filter(e => TYPES[e.type].building)) assert(Math.hypot(unit.x - building.x, unit.y - building.y) >= unit.r + building.r - .5, 'pushback cannot place a troop inside a building');
    }
  });
  assert(minimumMoving >= 35.9, 'troop bodies stay apart while the group moves');
  assert(minDistance() >= 40.8, 'resting troops keep a visible gap');
  assert(squad.every(e => e.x > 1400), 'the whole squad clears the corridor');
  assert.equal(new Set(squad.map(e => `${Math.round(e.x)},${Math.round(e.y)}`)).size, 50);
  t.diagnostic(`50 troops: minimum moving spacing ${minimumMoving.toFixed(2)}, resting spacing ${minDistance().toFixed(2)} world pixels (36-pixel bodies).`);
});

test('opposing ground units push symmetrically, pass head-on traffic, and exclude airborne rocks', () => {
  const game = arena();
  const a = spawn(game, 'twig', 0, 900, 800), b = spawn(game, 'twig', 1, 900, 800);
  advance(game, .1);
  assert(Math.hypot(a.x - b.x, a.y - b.y) >= 30.9);
  assert(Math.abs((a.x + b.x) / 2 - 900) < .01);
  assert(Math.abs((a.y + b.y) / 2 - 800) < .01);
  a.x = 800; a.y = 800; b.x = 1000; b.y = 800;
  game.command([a.id], {type:'move', x:1100, y:800});
  game.command([b.id], {type:'move', x:700, y:800});
  advance(game, 4, () => assert(Math.hypot(a.x - b.x, a.y - b.y) >= 25.9));
  assert(a.x > b.x, 'opposite traffic walks around rather than through each other');
  const ground = spawn(game, 'rock', 0, 1400, 800), flying = spawn(game, 'rock', 1, 1400, 800);
  flying.leap = {fromX:1400, fromY:800, toX:1550, toY:800, remaining:.65, total:.65};
  const before = {x:ground.x, y:ground.y};
  game._separate(.08);
  assert.equal(ground.x, before.x); assert.equal(ground.y, before.y);
});

test('all six map layouts have equal starting economies, safe camps and strategic routes', () => {
  for (const map of Object.keys(MAPS)) for (const players of [2, 3, 4]) {
    const game = new Game({map, players});
    assert.equal(game.teams.length, players);
    assert.equal(game.stats.collected.length, players);
    assert(game.map.lanes.length >= 3);
    assert(game.map.expansions.length >= 5);
    for (let team = 0; team < players; team++) {
      assert.equal(game.teams[team].trash, 300);
      assert.equal(game.teams[team].eliminated, false);
      assert.equal(own(game, 'desk', team).length, 1);
      assert.equal(own(game, 'printer', team).length, 1);
      assert.equal(own(game, 'cabinet', team).length, 1);
      assert.equal(own(game, 'twig', team).length, 5);
      for (const type of ['rock', 'paper', 'scissors']) assert.equal(own(game, type, team).length, 1);
      assert.equal(game.entities.filter(e => e.type === 'trash' && e.homeTeam === team).length, 6);
      for (const e of game.entities.filter(e => e.team === team)) assert(!game._blocked(e.x, e.y, e.r, e.id), `${map}/${players}: starting ${e.type} has safe ground`);
    }
    for (const can of game.entities.filter(e => e.type === 'trash')) {
      assert.equal(can.maxAmount, can.amount);
      assert(!game._blocked(can.x, can.y, can.r), `${map}/${players}: trash can is accessible`);
    }
  }
});

test('sizeable squads can cross every map layout without terrain or formation goals trapping them', t => {
  const timings = [];
  for (const map of Object.keys(MAPS)) for (const players of [2, 3, 4]) {
    const game = arena(map, players), start = game.starts[0], enemy = game.starts[1];
    for (const e of game.entities) e.hp = 1e8;
    const len = Math.hypot(1200 - start.x, 800 - start.y), fx = (1200 - start.x) / len, fy = (800 - start.y) / len;
    const reserved = [], squad = [];
    for (let i = 0; i < 50; i++) {
      const p = game._freePoint(start.x + fx * 300 + (i % 10 - 4.5) * 44, start.y + fy * 300 + (Math.floor(i / 10) - 2) * 44, 18, reserved);
      reserved.push({...p, r:18}); squad.push(spawn(game, 'rock', 0, p.x, p.y));
    }
    const elen = Math.hypot(1200 - enemy.x, 800 - enemy.y);
    game.command(squad.map(e => e.id), {type:'move', x:enemy.x + (1200 - enemy.x) / elen * 250, y:enemy.y + (800 - enemy.y) / elen * 250});
    const goals = squad.map(e => ({x:e.order.x, y:e.order.y, arrival:e.order.arrivalRadius||10}));
    for (let i = 0; i < squad.length; i++) assert(!game._blocked(goals[i].x, goals[i].y, squad[i].r));
    const reached = new Set(); let simulated = 0;
    while (simulated < 40 && reached.size < 50) {
      game.update(.08); simulated += .08;
      squad.forEach((e, i) => {
        const remaining = Math.hypot(e.x - goals[i].x, e.y - goals[i].y);
        const fightingAtDestination = e.order.type === 'idle' && e.combatTarget && game.getEntity(e.combatTarget)?.team !== 0 && Math.hypot(e.x - enemy.x, e.y - enemy.y) < 350;
        if (remaining <= goals[i].arrival + 8 || e.order.type === 'idle' && remaining < Math.min(180, goals[i].arrival * 3) || fightingAtDestination) reached.add(e.id);
        for (const o of game.map.obstacles) assert(!(e.x > o.x - e.r + .5 && e.x < o.x + o.w + e.r - .5 && e.y > o.y - e.r + .5 && e.y < o.y + o.h + e.r - .5), 'a squad cannot be pushed through impassable terrain');
      });
    }
    assert.equal(reached.size, 50, `${map}/${players}: every troop reaches its goal or joins the nearby squad around occupied combat slots`);
    timings.push(`${map}/${players}: ${simulated.toFixed(1)}s`);
  }
  t.diagnostic(`50-unit crossing simulations: ${timings.join(', ')}.`);
});

test('commands aimed inside a terrain obstacle project the entire formation onto reachable ground', () => {
  const game = arena();
  const squad = Array.from({length:24}, (_, i) => spawn(game, 'rock', 0, 700 + i % 6 * 44, 650 + Math.floor(i / 6) * 44));
  game.command(squad.map(e => e.id), {type:'move', x:1200, y:590});
  const goals = squad.map(e => ({x:e.order.x, y:e.order.y}));
  for (let i = 0; i < squad.length; i++) assert(!game._blocked(goals[i].x, goals[i].y, squad[i].r));
  advance(game, 35);
  assert(squad.every((e, i) => Math.hypot(e.x - goals[i].x, e.y - goals[i].y) < 150));
  assert(squad.every(e => e.order.type !== 'move'), 'troops settle near their separate projected goals rather than trying to walk through scenery or each other');
});

for (const players of [3, 4]) {
  test(`${players}-player free-for-all bots gather, fight each other, and obey separate economic ledgers`, t => {
    let botHits = 0;
    const game = new Game({players, difficulty:'hard', onEvent:e => {if (e.type === 'hit' && e.source.team > 0 && e.target.team > 0 && e.source.team !== e.target.team) botHits++;}});
    for (const desk of game.entities.filter(e => e.type === 'desk')) desk.hp = 1e8;
    const spent = Array(players).fill(0);
    for (const method of ['train', 'research', 'build']) {
      const original = game[method].bind(game);
      game[method] = (...args) => {
        const team = method === 'build' ? game.getEntity(args[0][0])?.team : game.getEntity(args[0])?.team;
        const before = team == null ? 0 : game.teams[team].trash;
        const result = original(...args);
        if (team > 0 && result.ok) {
          const expected = method === 'research' ? 150 : TYPES[args[1]].cost;
          assert.equal(before - game.teams[team].trash, expected); spent[team] += expected;
        }
        return result;
      };
    }
    const started = performance.now();
    advance(game, players === 4 ? 180 : 110, () => {
      for (let team = 1; team < players; team++) assert.equal(game.teams[team].trash, 300 + game.stats.collected[team] - spent[team], 'each bot pays from its own gathered income');
    });
    for (let team = 1; team < players; team++) {
      assert(game.stats.collected[team] > 400, `bot ${team} runs its economy`);
      assert(game.stats.trained[team] > 5, `bot ${team} produces units`);
    }
    assert(botHits > 0, 'bots choose and attack other bots as enemies');
    assert(game.entities.filter(e => e.type === 'desk').length > players, 'FFA bots construct expansions');
    t.diagnostic(`${players} players: ${game.time.toFixed(0)} simulated seconds in ${((performance.now() - started) / 1000).toFixed(2)} real seconds; ${botHits} bot-vs-bot hits.`);
  });
}

test('elimination removes a defeated team while a free-for-all continues to its last survivor', () => {
  const game = new Game({players:3}); game._think = Infinity;
  own(game, 'desk')[0].hp = 0;
  advance(game, .1);
  assert.equal(game.teams[0].eliminated, true);
  assert.equal(game.winner, null, 'the match still has two surviving rivals');
  assert(game.entities.filter(e => e.team === 0).every(e => e.hp <= 0));
  own(game, 'desk', 1)[0].hp = 0;
  advance(game, 1.1);
  assert.equal(game.teams[1].eliminated, true);
  assert.equal(game.winner, 2);
});

for (const difficulty of ['easy', 'normal', 'hard']) {
  test(`${difficulty} bot gathers, spends real trash, grows its economy and expands`, () => {
    const game = new Game({difficulty});
    // Keep this unattended simulation running after the first wave reaches home.
    own(game, 'desk')[0].hp = 1e8;
    let spent = 0;
    for (const method of ['train', 'research', 'build']) {
      const original = game[method].bind(game);
      game[method] = (...args) => {
        const team = method === 'build' ? game.getEntity(args[0][0])?.team : game.getEntity(args[0])?.team;
        const before = game.teams[1].trash;
        const result = original(...args);
        if (team === 1 && result.ok) {
          const expected = method === 'research' ? 150 : TYPES[args[1]].cost;
          assert.equal(before - game.teams[1].trash, expected);
          spent += expected;
        }
        return result;
      };
    }
    let sawAttack = false;
    advance(game, difficulty === 'easy' ? 210 : 175, () => {
      assert(game.teams[1].trash >= 0, 'AI cannot overspend');
      assert.equal(game.teams[1].trash, 300 + game.stats.collected[1] - spent, 'AI receives no free resources');
      if (game.entities.some(e => e.team === 1 && ['rock','paper','scissors'].includes(e.type) && e.x < 1100)) sawAttack = true;
    });
    assert(game.stats.collected[1] > 1000);
    assert(game.stats.trained[1] > 10);
    assert(own(game, 'twig', 1).length >= (difficulty === 'easy' ? 8 : 10));
    assert(own(game, 'desk', 1).length >= 2, 'bot constructs an expansion');
    assert(own(game, 'printer', 1).length >= 2, 'bot grows troop production');
    assert(sawAttack, 'bot launches a wave across the map');
  });
}
