import test from "node:test";
import assert from "node:assert/strict";
import { STEP, UNITS } from "../data.mjs";
import { createBattle, battleMap, issueOrder, tickBattle, battleView, hydrateView } from "../battle.mjs";

function battlefield(armyA, armyB, options = {}) {
  const b = createBattle({ armyA, armyB, seed: 113, ...options });
  const map = battleMap(b);
  map.costs.fill(1);
  map.tiles.fill(0);
  map.elevation.fill(0);
  map.version++;
  return b;
}
function place(u, x, y, heading = u.team ? Math.PI : 0) {
  Object.assign(u, { x, y, px: x, py: y, heading, facing: Math.cos(heading) < 0 ? -1 : 1, cooldown: 999, order: { kind: "hold", x, y, anchorX: x, anchorY: y } });
}
function run(b, seconds) {
  for (let i = 0; i < Math.ceil(seconds / STEP) && !b.finished; i++) tickBattle(b);
}
function arrow(b, from, target, extra = {}) {
  b.projectiles.push({ id: b.nextProjectile++, x: from.x, y: from.y, fromX: from.x, fromY: from.y, tx: target.x, ty: target.y, target: target.id, team: from.team, kind: "arrow", damage: 30, life: 0, ...extra });
}

test("Attack Nearest assigns reachable distributed targets immediately and replaces dead targets", () => {
  const b = battlefield({ footmen: 4 }, { recruits: 3 });
  const own = b.units.filter((u) => u.team === 0), foes = b.units.filter((u) => u.team === 1);
  own.forEach((u, i) => place(u, 10, 20 + i * 0.2));
  place(foes[0], 13, 20); place(foes[1], 13, 20.8); place(foes[2], 20, 20);
  const map = battleMap(b);
  for (let y = 0; y < 40; y++) map.costs[y * 64 + 15] = 0;
  map.version++;
  assert.equal(issueOrder(b, 0, own.map((u) => u.id), "attackNearest"), 4);
  assert.equal(new Set(own.map((u) => u.target)).size, 2);
  assert.ok(own.every((u) => u.target !== foes[2].id));
  foes[0].hp = 0;
  tickBattle(b);
  assert.ok(own.every((u) => u.target === foes[1].id));
  foes[1].hp = 0;
  assert.equal(issueOrder(b, 0, own.map((u) => u.id), "attackNearest"), 0);
  assert.match(b.notice, /No reachable enemy/);
});

test("a shield wall intercepts projectiles aimed at an ally and loses condition", () => {
  const b = battlefield({ footmen: 1, archers: 1 }, { recruits: 1 });
  const shield = b.units.find((u) => u.key === "footmen"), archer = b.units.find((u) => u.key === "archers"), enemy = b.units.find((u) => u.team === 1);
  place(shield, 18, 20, Math.PI); place(archer, 21, 20, Math.PI); place(enemy, 8, 20);
  issueOrder(b, 0, [shield.id], "hold", {}, "shieldwall");
  const condition = shield.shield;
  arrow(b, enemy, archer);
  run(b, 1);
  assert.equal(archer.hp, archer.maxHp);
  assert.ok(shield.hp < shield.maxHp);
  assert.ok(shield.shield < condition);
  assert.equal(b.projectiles.length, 0);
});

test("frontal shields reduce damage while rear attacks bypass their protection", () => {
  const scenario = (front) => {
    const b = battlefield({ towerWardens: 1 }, { recruits: 1 });
    const defender = b.units[0], enemy = b.units[1];
    place(defender, 20, 20, 0); place(enemy, front ? 27 : 13, 20);
    // A shield's current facing is independent of the projectile's impact direction.
    enemy.order.kind = "move";
    defender.order.kind = "move";
    defender.order.x = defender.x; defender.order.y = defender.y;
    arrow(b, enemy, defender);
    run(b, 0.4);
    return defender.maxHp - defender.hp;
  };
  assert.ok(scenario(false) > scenario(true) * 1.3);
});

test("walls obstruct arrows and splash has distance falloff without passing through cover", () => {
  const b = battlefield({ footmen: 1 }, { recruits: 3 });
  const shooter = b.units[0], [near, edge, covered] = b.units.slice(1);
  place(shooter, 5, 20); place(near, 10, 20); place(edge, 10, 21.6); place(covered, 12.2, 20);
  const map = battleMap(b);
  for (let y = 18; y <= 22; y++) map.costs[y * 64 + 11] = 0;
  map.version++;
  arrow(b, shooter, near, { splashRadius: 3, splashFalloff: 0.8, damage: 20, kind: "magic", friendlyFire: 0 });
  run(b, 0.6);
  const nearDamage = near.maxHp - near.hp, edgeDamage = edge.maxHp - edge.hp;
  assert.ok(nearDamage > edgeDamage && edgeDamage > 0);
  assert.equal(covered.hp, covered.maxHp);
  assert.equal(shooter.hp, shooter.maxHp);
  const stopped = battlefield({ footmen: 1 }, { recruits: 1 });
  place(stopped.units[0], 5, 20); place(stopped.units[1], 10, 20);
  battleMap(stopped).costs[20 * 64 + 8] = 0;
  battleMap(stopped).version++;
  arrow(stopped, stopped.units[0], stopped.units[1]);
  run(stopped, 0.6);
  assert.equal(stopped.units[1].hp, stopped.units[1].maxHp);
  assert.equal(stopped.projectiles.length, 0);
});

test("braced pikes punish frontal cavalry momentum and charges require recovery", () => {
  const scenario = (brace) => {
    const b = battlefield({ cavalry: 1 }, { condottieriPikes: 1 });
    const horse = b.units[0], pike = b.units[1];
    place(horse, 10, 20); place(pike, 20, 20);
    horse.cooldown = 0;
    issueOrder(b, 0, [horse.id], "charge");
    issueOrder(b, 1, [pike.id], brace ? "brace" : "hold", {}, "shieldwall");
    run(b, 1.8);
    return { b, horse, pike };
  };
  const braced = scenario(true), unbraced = scenario(false);
  assert.ok(braced.horse.hp < unbraced.horse.hp);
  assert.ok(braced.pike.hp > unbraced.pike.hp);
  assert.ok(unbraced.horse.chargeRecovery > 0);
  assert.ok(unbraced.horse.regroupUntil > unbraced.b.time);
});

test("horse archers shoot, withdraw toward their fallback and consume ammunition and stamina", () => {
  const b = battlefield({ falconHorseArchers: 1 }, { footmen: 1 });
  const horse = b.units[0], enemy = b.units[1];
  place(horse, 12, 20); place(enemy, 18, 20);
  horse.cooldown = 0;
  issueOrder(b, 0, [horse.id], "skirmish", { x: 5, y: 20 });
  run(b, 0.7);
  assert.ok(horse.x < 11);
  assert.equal(horse.ammo, UNITS.falconHorseArchers.ammo - 1);
  assert.ok(horse.stamina < 100);
  assert.ok(enemy.hp < enemy.maxHp || b.projectiles.length);
});

test("forest and marsh ground reduce cavalry movement and exhausted missiles cannot fire", () => {
  const movement = (terrain) => {
    const b = battlefield({ cavalry: 1 }, { recruits: 1 }, { terrain });
    place(b.units[0], 10, 20); place(b.units[1], 60, 20);
    issueOrder(b, 0, [0], "move", { x: 20, y: 20 });
    run(b, 1);
    return b.units[0].x - 10;
  };
  assert.ok(movement("plains") > movement("forest") * 1.4);
  assert.ok(movement("forest") > movement("marsh"));
  const b = battlefield({ falconHorseArchers: 1 }, { recruits: 1 });
  place(b.units[0], 10, 20); place(b.units[1], 16, 20);
  b.units[0].ammo = 0; b.units[0].cooldown = 0;
  issueOrder(b, 0, [0], "skirmish", { x: 5, y: 20 });
  run(b, 0.5);
  assert.equal(b.projectiles.length, 0);
  assert.equal(b.units[0].ammo, 0);
});

test("defend holds a screen, fallback preserves facing, regroup converges, and flank uses a side route", () => {
  const b = battlefield({ footmen: 2 }, { recruits: 1 });
  place(b.units[0], 10, 20); place(b.units[1], 10, 24); place(b.units[2], 18, 20);
  issueOrder(b, 0, [0], "defend", { x: 10, y: 20 }, "shieldwall");
  run(b, 0.5);
  assert.ok(b.units[0].x < 10.8);
  issueOrder(b, 0, [0], "fallback", { x: 5, y: 20 });
  run(b, 0.5);
  assert.ok(b.units[0].x < 9.5 && Math.cos(b.units[0].heading) > 0);
  issueOrder(b, 0, [0, 1], "regroup", { x: 12, y: 22 });
  const before = Math.hypot(b.units[0].x - 12, b.units[0].y - 22);
  run(b, 0.5);
  assert.ok(Math.hypot(b.units[0].x - 12, b.units[0].y - 22) < before);
  issueOrder(b, 0, [0], "flank", { x: 18, y: 30, side: 1 });
  const y = b.units[0].y;
  run(b, 0.5);
  assert.ok(b.units[0].y > y);
});

test("doctrine commanders stage screens, reserves and horse harassment on bounded staggered plans", () => {
  const b = battlefield({ footmen: 12, archers: 4, cavalry: 2 }, { falconHorseArchers: 5, khaganLancers: 2 }, { commanders: [true, true], factions: ["crown", "steppe"] });
  run(b, 1);
  assert.equal(b.commanders[0].doctrine, "defensive");
  assert.equal(b.commanders[1].phase, "harass");
  assert.ok(b.units.some((u) => u.team === 0 && u.order.kind === "defend"));
  assert.ok(b.units.some((u) => u.team === 0 && u.order.kind === "regroup"));
  assert.ok(b.units.filter((u) => u.key === "falconHorseArchers").every((u) => u.order.kind === "skirmish"));
  assert.notEqual(b.commanders[0].nextPlan, b.commanders[1].nextPlan);
  run(b, 5);
  assert.ok(b.commanders.every((c) => c.plans <= 3));
});

test("tactical views retain the ten-value troop tuple and old checkpoints initialize missing fields", () => {
  const b = battlefield({ footmen: 2 }, { recruits: 2 });
  const restored = JSON.parse(JSON.stringify(b));
  for (const u of restored.units) for (const key of ["stamina", "morale", "shield", "heading", "cohesion", "ammo", "braceTime", "chargeRecovery", "regroupUntil", "suppressed"]) delete u[key];
  delete restored.commanders;
  tickBattle(restored);
  assert.ok(restored.units.every((u) => Number.isFinite(u.stamina + u.morale + u.heading + u.cohesion)));
  const view = battleView(restored), hydrated = hydrateView(view);
  assert.ok(view.units.every((u) => u.length === 10));
  assert.equal(hydrated.units[0].stamina, view.tactical[0][3]);
  assert.ok(Number.isFinite(hydrated.units[0].heading));
  assert.ok(Number.isFinite(hydrateView({ ...view, tactical: undefined }).units[0].cohesion));
});

test("defensive objectives reward holding until relief and fail when the position is overrun", () => {
  const held = battlefield({ footmen: 1 }, { recruits: 1 }, { encounterType: "defensive", objective: { type: "defensive", x: 12, y: 20, duration: 1 } });
  place(held.units[0], 12, 20); place(held.units[1], 55, 20);
  run(held, 1.1);
  assert.equal(held.winner, 0);
  assert.equal(held.objective.completed, true);
  assert.ok(held.units.every((u) => u.hp > 0));
  const overrun = battlefield({ footmen: 1 }, { recruits: 2 }, { encounterType: "defensive", objective: { x: 20, y: 20, radius: 3, duration: 20 } });
  place(overrun.units[0], 5, 20); place(overrun.units[1], 20, 20); place(overrun.units[2], 21, 20);
  run(overrun, 8.2);
  assert.equal(overrun.winner, 1);
  assert.match(overrun.objective.reason, /overrun/);
});

test("defensive and relief timers only advance while allied troops actually hold the zone", () => {
  for (const type of ["defensive", "reinforcement"]) {
    const b = battlefield({ footmen: 1 }, { recruits: 1 }, { encounterType: type, objective: { type, x: 20, y: 20, radius: 3, duration: 1 } });
    place(b.units[0], 5, 20); place(b.units[1], 55, 20);
    run(b, 1.2);
    assert.equal(b.finished, false);
    assert.equal(b.objective.heldFor, 0);
    assert.equal(b.objective.progress, 0);
    place(b.units[0], 20, 20);
    run(b, 0.5);
    assert.equal(b.finished, false);
    assert.ok(b.objective.heldFor > 0 && b.objective.heldFor < 1);
    assert.ok(b.objective.progress > 0 && b.objective.progress < 1);
    run(b, 0.6);
    assert.equal(b.winner, 0);
    assert.equal(b.objective.progress, 1);
  }
});

test("caravan escort and supply raids can win while enemy soldiers survive", () => {
  const caravan = battlefield({ footmen: 1 }, { recruits: 1 }, { encounterType: "caravanDefense", objective: { x: 10, y: 20, targetX: 12, targetY: 20, speed: 2, duration: 10 } });
  place(caravan.units[0], 10, 20); place(caravan.units[1], 55, 20);
  run(caravan, 1.2);
  assert.equal(caravan.winner, 0);
  assert.match(caravan.objective.reason, /reached safety/);
  const raid = battlefield({ footmen: 1 }, { recruits: 1 }, { encounterType: "raid", objective: { x: 20, y: 20, hp: 6, radius: 2, duration: 10 } });
  place(raid.units[0], 20, 20); place(raid.units[1], 55, 20);
  run(raid, 2.2);
  assert.equal(raid.winner, 0);
  assert.equal(raid.objective.hp, 0);
  assert.ok(raid.units[1].hp > 0);
});

test("rearguards preserve escaped survivors and reinforcement waves join once without permanent troops", () => {
  const rear = battlefield({ footmen: 1 }, { recruits: 1 }, { encounterType: "rearguard", objective: { required: 1, delay: 0, duration: 10 } });
  place(rear.units[0], 2, 20); place(rear.units[1], 55, 20);
  issueOrder(rear, 0, [0], "retreat");
  run(rear, 2);
  assert.equal(rear.winner, 0);
  assert.equal(rear.objective.escaped, 1);
  assert.ok(rear.units[0].routed && rear.units[0].hp > 0);
  const relief = battlefield({ footmen: 1 }, { recruits: 1 }, { encounterType: "reinforcement", objective: { duration: 1, waves: [{ at: 0.3, team: 0, army: { archers: 2 }, spawn: { x: 6, y: 20 } }] } });
  place(relief.units[0], 12, 20); place(relief.units[1], 55, 20);
  run(relief, 1.1);
  assert.equal(relief.winner, 0);
  assert.equal(relief.units.filter((u) => u.temporary).length, 2);
  assert.equal(new Set(relief.units.map((u) => u.id)).size, 4);
  assert.equal(relief.objective.waves[0].delivered, true);
  assert.equal(battleView(relief).objective.type, "reinforcement");
});

test("ambush encounters deploy opposing wings close to the road instead of a distant line", () => {
  const b = createBattle({ armyA: { footmen: 4 }, armyB: { recruits: 8 }, encounterType: "ambush" });
  const enemies = b.units.filter((u) => u.team === 1);
  assert.ok(enemies.every((u) => u.x < 36));
  assert.ok(enemies.some((u) => u.y < 15));
  assert.ok(enemies.some((u) => u.y > 25));
  assert.equal(b.objective, null);
});
