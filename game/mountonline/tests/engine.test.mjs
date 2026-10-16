import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync, statSync } from "node:fs";
import {
  UNITS,
  UNIT_KEYS,
  STEP,
  emptyArmy,
  newProfile,
  migrateLegacy,
  armySize,
  clone,
  addXP,
} from "../data.mjs";
import { Navigator } from "../navigation.mjs";
import { createWorld, battleArmy } from "../world.mjs";
import {
  createBattle,
  tickBattle,
  issueOrder,
  useAbility,
  formationSlots,
  battleMap,
  battleView,
  hydrateView,
} from "../battle.mjs";
import {
  applyAction,
  applyBattleResult,
  tickEconomy,
} from "../progression.mjs";
import { Realm } from "../realm.mjs";
import { parseCampaign, serializeCampaign } from "../storage.mjs";
const army = (key, n) => ({ ...emptyArmy(), [key]: n });
test("world routes cross rivers only at bridges and never pass through tree or cliff props", () => {
  const w = createWorld();
  for (const end of [
    { x: 110, y: 44 },
    { x: 56, y: 22 },
    { x: 91, y: 18 },
  ]) {
    const path = w.nav.path({ x: 15, y: 44 }, end);
    assert.ok(path.length > 5);
    for (const p of path) {
      const i = Math.floor(p.y) * w.width + Math.floor(p.x);
      assert.ok(w.costs[i] > 0);
      assert.notEqual(w.tiles[i], 3);
    }
  }
  for (const p of w.props) {
    assert.equal(w.costs[Math.floor(p.y) * w.width + Math.floor(p.x)], 0);
  }
});
test("navigation forbids diagonal corner cuts and handles unreachable endpoints", () => {
  const costs = new Float32Array([1, 0, 1, 0, 1, 0, 1, 0, 1]),
    nav = new Navigator({ width: 3, height: 3, costs });
  assert.deepEqual(nav.path({ x: 0, y: 0 }, { x: 2, y: 2 }), []);
  assert.deepEqual(nav.path({ x: 0, y: 0 }, { x: 0, y: 0 }), []);
  assert.equal(nav.walkable(-1, 0), false);
});
test("formations keep 600 distinct slots inside the battlefield, including edge placements", () => {
  for (const shape of ["line", "shieldwall", "square", "wedge", "loose"]) {
    const slots = formationSlots(600, shape, { x: 2, y: 2 }, 1);
    assert.equal(
      new Set(slots.map((s) => s.x.toFixed(4) + ":" + s.y.toFixed(4))).size,
      600,
    );
    assert.ok(
      slots.every(
        (s) => s.x >= 1.2 && s.x <= 62.8 && s.y >= 1.2 && s.y <= 38.8,
      ),
    );
  }
});
test("command ownership, finite coordinates, target allegiance and self-follow are enforced", () => {
  const b = createBattle({
    armyA: army("footmen", 4),
    armyB: army("footmen", 4),
  });
  assert.throws(() => issueOrder(b, 0, [4], "charge"), /own active/);
  assert.throws(
    () => issueOrder(b, 0, [0], "move", { x: NaN, y: 2 }),
    /destination/,
  );
  assert.throws(() => issueOrder(b, 0, [0], "follow", { target: 4 }), /allied/);
  assert.throws(() => issueOrder(b, 0, [0], "attack", { target: 1 }), /enemy/);
  issueOrder(b, 0, [0, 1], "follow", { target: 0 });
  assert.equal(b.units[0].order.kind, "hold");
  assert.equal(b.units[1].order.target, 0);
});
test("all nine orders can be issued, and hold does not advance toward a distant enemy", () => {
  const b = createBattle({
    armyA: army("footmen", 4),
    armyB: army("footmen", 4),
    seed: 2,
  });
  for (const order of [
    "move",
    "attack",
    "charge",
    "hold",
    "defend",
    "follow",
    "retreat",
    "patrol",
    "auto",
  ]) {
    issueOrder(
      b,
      0,
      [0, 1],
      order,
      { x: 15, y: 20, ...(order === "follow" ? { target: 2 } : {}) },
      "shieldwall",
    );
    assert.equal(b.units[0].order.kind, order);
  }
  issueOrder(b, 0, [0, 1, 2, 3], "hold", { x: 10, y: 20 }, "square");
  for (let i = 0; i < 40; i++) tickBattle(b);
  assert.ok(b.units.slice(0, 4).every((u) => u.x < 15));
});
test("move orders detour around obstacles and finish without exhausted-waypoint crashes", () => {
  const b = createBattle({
    armyA: army("footmen", 10),
    armyB: army("footmen", 1),
    seed: 9,
    siege: true,
  });
  issueOrder(b, 1, [10], "hold", { x: 60, y: 30 });
  b.gate.hp = 0;
  const m = battleMap(b);
  m.costs[19 * 64 + 42] = m.costs[20 * 64 + 42] = 1;
  m.version++;
  issueOrder(
    b,
    0,
    b.units.filter((u) => u.team === 0).map((u) => u.id),
    "move",
    { x: 52, y: 25 },
  );
  for (let i = 0; i < 1200; i++) tickBattle(b);
  assert.ok(b.units.slice(0, 10).filter((u) => u.x > 45).length >= 8);
  assert.ok(b.units.every((u) => Number.isFinite(u.x + u.y)));
});
test("seeded battles resolve deterministically and ranged attacks create damaging projectiles", () => {
  const args = {
    armyA: { ...army("footmen", 12), archers: 8, monks: 2 },
    armyB: { ...army("recruits", 12), archers: 8 },
    seed: 719,
  };
  const a = createBattle(args),
    b = createBattle(args);
  let arrows = false;
  for (let i = 0; i < 2000 && !a.finished; i++) {
    tickBattle(a);
    tickBattle(b);
    arrows ||= a.projectiles.some((p) => p.kind === "arrow");
  }
  assert.equal(a.finished, true);
  assert.ok(arrows);
  assert.deepEqual(battleView(a), battleView(b));
  assert.ok(a.units.some((u) => u.hp <= 0));
});
test("siege gate blocks the wall, breaches under attack, then allows entry and victory", () => {
  const b = createBattle({
    armyA: { ...army("footmen", 22), archers: 8, monks: 2 },
    armyB: army("recruits", 18),
    seed: 4,
    siege: true,
  });
  assert.equal(battleMap(b).costs[20 * 64 + 42], 0);
  for (let i = 0; i < 3000 && !b.finished; i++) tickBattle(b);
  assert.equal(b.gate.hp, 0);
  assert.equal(battleMap(b).costs[20 * 64 + 42], 1);
  assert.equal(b.finished, true);
  assert.equal(b.winner, 0);
  assert.ok(b.units.some((u) => u.team === 0 && u.x > 43));
});
test("healing, commander abilities and cooldowns cannot be repeatedly invoked", () => {
  const b = createBattle({
    armyA: { ...army("footmen", 2), monks: 1 },
    armyB: army("recruits", 1),
  });
  b.units[0].hp = 10;
  useAbility(b, 0, "rally", b.units[0]);
  assert.ok(b.units[0].hp > 10);
  assert.throws(() => useAbility(b, 0, "rally", b.units[0]), /recharging/);
  assert.throws(
    () => useAbility(b, 0, "volley", { x: Infinity, y: 1 }),
    /location/,
  );
  for (let i = 0; i < 20; i++) tickBattle(b);
  assert.ok(b.units[0].hp > 20);
});
test("retreat moves units to their own edge and preserves withdrawn survivors", () => {
  const b = createBattle({
    armyA: army("footmen", 8),
    armyB: army("recruits", 8),
    seed: 1,
  });
  issueOrder(
    b,
    0,
    b.units.filter((u) => u.team === 0).map((u) => u.id),
    "retreat",
  );
  for (let i = 0; i < 500 && !b.finished; i++) tickBattle(b);
  assert.equal(b.finished, true);
  assert.equal(b.winner, 1);
  const p = newProfile();
  applyBattleResult(p, b, 0);
  assert.equal(p.army.footmen, 8);
  assert.equal(p.stats.wins, 0);
});
test("invalid purchases do not mutate gold or troops; contracts and standards reward only once", () => {
  const p = newProfile(),
    w = createWorld();
  p.x = 13.5;
  p.y = 44.5;
  const before = clone(p);
  assert.throws(() => applyAction(p, w, "train", { unit: "dragonKnights" }));
  assert.deepEqual(p, before);
  applyAction(p, w, "recruit", { settlement: "hearthford", count: 5 });
  assert.equal(p.gold, 200);
  assert.equal(p.army.recruits, 17);
  applyAction(p, w, "contract", { id: "first-blood" });
  p.stats.wins = 1;
  applyAction(p, w, "contract", { id: "first-blood" });
  const gold = p.gold;
  assert.throws(
    () => applyAction(p, w, "contract", { id: "first-blood" }),
    /already/,
  );
  assert.equal(p.gold, gold);
  assert.ok(p.inventory.banners.includes("wayfarer"));
});
test("migration preserves all legacy troop counts through reserves and retains inventory", () => {
  const old = {
    user: "Captain",
    gold: 4351,
    level: 3,
    xp: 40,
    x: 179,
    y: 119,
    army: Object.fromEntries(UNIT_KEYS.map((k) => [k, 41])),
    inventory: { banners: [{ name: "Old standard", bonus: 4 }], equipped: [0] },
    allegianceId: "old-ally",
  };
  const p = migrateLegacy(old);
  for (const key of UNIT_KEYS) assert.equal(p.army[key] + p.reserve[key], 41);
  assert.equal(p.gold, 4351);
  assert.deepEqual(p.legacyInventory, old.inventory);
  assert.equal(p.legacyAllegiance, "old-ally");
  assert.ok(armySize(p.army) <= 84);
});
test("income and supplies derive from owned holdings, and deposits remove field troops", () => {
  const p = newProfile();
  p.uid = "local";
  const w = createWorld(),
    c = w.settlements.find((s) => s.id === "dawnkeep");
  p.x = c.x;
  p.y = c.y;
  c.owner = "local";
  c.level = 2;
  const gold = p.gold;
  tickEconomy(p, w, 120);
  assert.equal(p.gold, gold + 48 - 1);
  assert.ok(p.supplies < 100);
  const before = p.army.footmen;
  applyAction(p, w, "garrison", { settlement: c.id, unit: "footmen" });
  assert.equal(p.army.footmen, before - 1);
  assert.equal(c.garrisonArmy.footmen, 1);
});
test("campaign export restores unfinished battle, inventory and ownership, and rejects invalid troops", () => {
  const r = new Realm(),
    p = r.enter("local", newProfile("Captain"));
  p.x = 35.5;
  p.y = 44.5;
  const c = r.world.settlements.find((s) => s.id === "dawnkeep");
  r.startBattle("local", "siege", c.id);
  for (let i = 0; i < 35; i++) r.tick(STEP);
  const text = serializeCampaign(r, "local"),
    restored = parseCampaign(text);
  assert.equal(restored.player("local").activeBattle, p.activeBattle);
  assert.deepEqual(
    battleView(restored.battles.get(p.activeBattle)),
    battleView(r.battles.get(p.activeBattle)),
  );
  const broken = JSON.parse(text);
  broken.battles[0].units[0].key = "intruder";
  assert.throws(() => parseCampaign(JSON.stringify(broken)), /troop/);
  for (let i = 0; i < 1500; i++) restored.tick(STEP);
  assert.equal(restored.battles.get(p.activeBattle)?.finished ?? true, true);
});
test("castle capture and battle settlement apply only once", () => {
  const r = new Realm(),
    p = r.enter("local", newProfile());
  p.x = 35.5;
  p.y = 44.5;
  const b = r.startBattle("local", "siege", "dawnkeep");
  b.finished = true;
  b.winner = 0;
  r.settle(b);
  const gold = p.gold;
  r.settle(b);
  assert.equal(p.stats.wins, 1);
  assert.equal(p.gold, gold);
  assert.equal(
    r.world.settlements.find((s) => s.id === "dawnkeep").owner,
    "local",
  );
  assert.equal(p.activeBattle, null);
});
test("a nearby castle owner commands their actual garrison without duplicating it into their field army", () => {
  const r = new Realm(),
    a = r.enter("attacker", newProfile("A")),
    d = r.enter("defender", newProfile("D")),
    c = r.world.settlements.find((s) => s.id === "dawnkeep");
  a.x = 35.5;
  a.y = 44.5;
  d.x = 38.5;
  d.y = 44.5;
  c.owner = "defender";
  c.garrisonArmy = { dragonKnights: 2 };
  const original = clone(d.army),
    b = r.startBattle("attacker", "siege", c.id);
  assert.equal(b.participants[1], "defender");
  assert.equal(b.seatRoles[1], "garrison");
  assert.equal(
    b.units.filter((u) => u.team === 1 && u.key === "dragonKnights").length,
    2,
  );
  r.command("defender", {
    ids: b.units.filter((u) => u.team === 1).map((u) => u.id),
    order: "hold",
    point: { x: 47, y: 20 },
    formation: "shieldwall",
  });
  b.finished = true;
  b.winner = 0;
  r.settle(b);
  assert.deepEqual(d.army, original);
  assert.equal(d.activeBattle, null);
  assert.equal(c.owner, "attacker");
  assert.equal(c.garrison, 12);
  assert.deepEqual(c.garrisonArmy, {});
});
test("caravans sell supplies and peace stops faction pressure against owned holdings", () => {
  const r = new Realm(),
    p = r.enter("local", newProfile()),
    n = r.npcs.find((n) => n.kind === "merchant");
  p.x = n.x;
  p.y = n.y;
  p.supplies = 90;
  const gold = p.gold;
  r.action("local", "caravan", { target: n.id });
  assert.equal(p.gold, gold - 20);
  assert.equal(p.supplies, 140);
  const c = r.world.settlements.find((s) => s.id === "dawnkeep");
  c.owner = "local";
  c.ownerReputation = -30;
  p.relations.crown = -30;
  r.action("local", "diplomacy", { faction: "crown", peace: true });
  assert.equal(c.ownerReputation, 15);
});
test("repeated hold orders preserve positions and the level cap remains save-compatible", () => {
  const b = createBattle({
      armyA: army("footmen", 8),
      armyB: army("recruits", 4),
    }),
    before = b.units.slice(0, 8).map((u) => [u.x, u.y]);
  for (let i = 0; i < 5; i++) {
    issueOrder(
      b,
      0,
      b.units.slice(0, 8).map((u) => u.id),
      "hold",
    );
    for (let j = 0; j < 5; j++) tickBattle(b);
  }
  assert.deepEqual(
    b.units.slice(0, 8).map((u) => [u.x, u.y]),
    before,
  );
  const p = newProfile();
  p.level = 99;
  addXP(p, 20000);
  assert.equal(p.level, 100);
  const points = p.skillPoints;
  addXP(p, 1e8);
  assert.equal(p.level, 100);
  assert.equal(p.skillPoints, points);
  assert.ok(p.xp < 10000);
});
test("patrol attrition removes assigned garrison troops and captured AI keeps clear old rosters", () => {
  const r = new Realm(),
    p = r.enter("local", newProfile()),
    c = r.world.settlements.find((s) => s.id === "dawnkeep"),
    n = r.npcs.find((n) => n.kind === "patrol");
  p.x = 15.5;
  p.y = 44.5;
  c.owner = "local";
  c.ownerReputation = -30;
  c.garrison = 15;
  c.garrisonArmy = { dragonKnights: 10 };
  c.raidPressure = 19.9;
  n.faction = "crown";
  n.x = c.x;
  n.y = c.y;
  n.decision = 100;
  n.route = [];
  n.army = army("footmen", 40);
  r.tick(0.2);
  assert.equal(c.garrison, 7);
  assert.equal(c.garrisonArmy.dragonKnights, 7);
  c.raidPressure = 19.9;
  r.tick(0.2);
  assert.equal(c.owner, null);
  assert.equal(c.garrison, 16);
  assert.deepEqual(c.garrisonArmy, {});
});
test("online battle view omits routes and hydrated sprites preserve class and ownership", () => {
  const b = createBattle({
    armyA: army("dragonKnights", 2),
    armyB: army("mages", 2),
  });
  const v = battleView(b),
    hydrated = hydrateView(v);
  assert.equal(hydrated.units[0].key, "dragonKnights");
  assert.equal(hydrated.units[3].team, 1);
  assert.ok(v.units.every((u) => Array.isArray(u) && u.length === 10));
});
test("every troop has an eight-frame atlas and total essential artwork stays below 2.5 MB", () => {
  const root = new URL("../../../graphics/mountonline/", import.meta.url),
    manifest = JSON.parse(readFileSync(new URL("manifest.json", root)));
  for (const u of Object.values(UNITS)) {
    assert.ok(u.sheet < 2 && u.row < 7);
    assert.equal(manifest.atlases[u.sheet].columns, 8);
  }
  for (const a of manifest.atlases) {
    assert.equal(statSync(new URL(a.file, root)).size, a.bytes);
  }
  assert.ok(manifest.atlases.reduce((n, a) => n + a.bytes, 0) < 2500000);
});
