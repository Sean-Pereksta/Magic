import test from "node:test";
import assert from "node:assert/strict";
import { WORLD, VERSION, UNITS, UNIT_KEYS, FACTIONS, ORDERS, newProfile, armySize } from "../data.mjs";
import { createWorld, createNPCs, tickNPCs, SETTLEMENTS, BIOMES, battleArmy } from "../world.mjs";

const legacy = [
  ["hearthford", 13.5, 44.5], ["oakwatch", 27.5, 27.5], ["dawnkeep", 38.5, 44.5],
  ["bracken", 48.5, 66.5], ["greenhold", 56.5, 22.5], ["rivercross", 75.5, 44.5],
  ["redmill", 92.5, 66.5], ["embergate", 104.5, 44.5], ["highcrown", 112.5, 19.5], ["frostmere", 91.5, 18.5],
];
test("expanded roster preserves original schema and provides ten distinct signature doctrines", () => {
  assert.equal(VERSION, 2);
  assert.equal(UNIT_KEYS.length, 34);
  assert.deepEqual(UNIT_KEYS.slice(0, 14), ["recruits", "archers", "footmen", "monks", "veteranArchers", "crossbowmen", "swordsmen", "cavalry", "priests", "mages", "romanSergeants", "romanChampions", "dragonKnights", "venetianSaints"]);
  assert.equal(FACTIONS.length, 10);
  assert.equal(new Set(FACTIONS.map((f) => f.doctrine)).size, 10);
  for (const f of FACTIONS) {
    assert.equal(f.signatureUnits.length, 2);
    assert.ok(f.leader && f.personality && f.economy.length);
    for (const key of f.signatureUnits) {
      assert.equal(UNITS[key].faction, f.id);
      assert.equal(UNITS[key].sheet, 3);
    }
    const army = battleArmy(70, f.id);
    assert.equal(armySize(army), 70);
    assert.ok(f.signatureUnits.every((key) => army[key] > 0));
  }
  const p = newProfile();
  assert.equal(p.worldVersion, 2);
  assert.equal(Object.keys(p.relations).length, 10);
  for (const order of ["attack", "attackNearest", "skirmish", "flank", "fallback", "focus", "regroup", "brace"]) assert.ok(ORDERS.includes(order));
});
test("continent has exact settlement distribution and preserves every legacy location", () => {
  assert.equal(WORLD.width, 320); assert.equal(WORLD.height, 224);
  assert.equal(SETTLEMENTS.length, 80);
  assert.equal(new Set(SETTLEMENTS.map((s) => s.id)).size, 80);
  assert.equal(new Set(SETTLEMENTS.map((s) => `${s.x}:${s.y}`)).size, 80);
  assert.deepEqual(SETTLEMENTS.reduce((result, s) => (result[s.kind] = (result[s.kind] || 0) + 1, result), {}), { village: 30, castle: 20, capital: 10, town: 20 });
  for (const [id, x, y] of legacy) {
    const settlement = SETTLEMENTS.find((s) => s.id === id);
    assert.equal(settlement.x, x); assert.equal(settlement.y, y);
  }
  for (const f of FACTIONS) assert.equal(SETTLEMENTS.filter((s) => s.faction === f.id).length, 8);
});
test("seeded broad biomes and connected roads give every settlement a safe route", () => {
  const w = createWorld(), copy = createWorld(), other = createWorld(WORLD.seed + 1);
  assert.deepEqual(w.tiles, copy.tiles);
  assert.deepEqual(w.costs, copy.costs);
  assert.deepEqual(w.props, copy.props);
  assert.notDeepEqual(w.tiles, other.tiles);
  for (const biome of Object.values(BIOMES)) assert.ok(w.biomes.includes(biome), `biome ${biome}`);
  assert.ok(w.tiles.includes(7), "bridges span rivers");
  for (const settlement of w.settlements) {
    assert.ok(w.nav.walkable(Math.floor(settlement.x), Math.floor(settlement.y)));
    const route = w.nav.path({ x: 15.5, y: 44.5 }, settlement);
    assert.ok(route.length, settlement.id);
    let previous = { x: 15.5, y: 44.5 };
    for (const p of route) {
      const x = Math.floor(p.x), y = Math.floor(p.y);
      assert.ok(w.nav.walkable(x, y), settlement.id);
      assert.notEqual(w.tiles[y * w.width + x], 3);
      assert.ok(Math.abs(p.x - previous.x) <= 1 && Math.abs(p.y - previous.y) <= 1, `continuous route to ${settlement.id}`);
      if (p.x !== previous.x && p.y !== previous.y) {
        assert.ok(w.nav.walkable(Math.floor(previous.x), y));
        assert.ok(w.nav.walkable(x, Math.floor(previous.y)));
      }
      previous = p;
    }
    assert.equal(previous.x, settlement.x); assert.equal(previous.y, settlement.y);
    assert.ok(w.nav.lastVisits <= 26640, "search remains bounded independently of continent size");
  }
  for (const prop of w.props) assert.equal(w.costs[Math.floor(prop.y) * w.width + Math.floor(prop.x)], 0);
  const from = w.settlements[0], to = w.settlements.at(-1), route = w.nav.path(from, to);
  const expected = route[0].x; route[0].x = -99;
  assert.equal(w.nav.path(from, to)[0].x, expected, "cached paths cannot be mutated by travelers");
});
test("named lords and caravans preserve saved NPC IDs while far planning stays bounded", () => {
  const w = createWorld(), npcs = createNPCs(w);
  assert.equal(npcs.length, 74);
  assert.deepEqual(npcs, createNPCs(w));
  assert.equal(new Set(npcs.map((n) => n.id)).size, npcs.length);
  for (let i = 0; i < 28; i++) assert.ok(npcs.some((n) => n.id === `${i < 13 ? "bandit" : i < 20 ? "patrol" : "merchant"}-${i}`));
  for (const f of FACTIONS) {
    const lords = npcs.filter((n) => n.faction === f.id && n.lord);
    assert.equal(lords.length, 3);
    assert.equal(new Set(lords.map((n) => n.name)).size, 3);
    assert.ok(lords.every((n) => f.signatureUnits.some((key) => n.army[key] > 0)));
  }
  let searches = 0; const original = w.nav.path.bind(w.nav);
  w.nav.path = (...args) => { searches++; return original(...args); };
  for (const n of npcs) n.decision = 0;
  tickNPCs(w, npcs, [], .2, 1);
  assert.ok(searches <= 4);
  assert.ok(npcs.every((n) => n.simulation === "strategic"));
});
test("caravan arrival funds a real town and bandit raids damage production", () => {
  const w = createWorld(), npcs = createNPCs(w), merchant = npcs.find((n) => n.kind === "merchant");
  const town = w.settlements.find((s) => s.kind === "town");
  Object.assign(merchant, { x: town.x, y: town.y, target: town.id, decision: 0, cargo: 40 });
  tickNPCs(w, [merchant], [], .2, 10);
  assert.equal(merchant.deliveryCount, 1); assert.equal(merchant.deliveredAt, 10);
  assert.equal(town.tradeIncome, 40); assert.ok(town.prosperity > 1);
  assert.notEqual(merchant.target, town.id);
  const bandit = npcs.find((n) => n.kind === "bandit"), village = w.settlements.find((s) => s.kind === "village");
  Object.assign(bandit, { x: village.x, y: village.y, target: village.id, decision: 0, route: [] });
  tickNPCs(w, [bandit], [], .2, 120);
  assert.ok(village.production < 1); assert.ok(village.food < 90);
});
test("outlaws attack vulnerable caravans and lords pursue declared kingdom enemies", () => {
  const w = createWorld(), npcs = createNPCs(w), bandit = npcs.find((n) => n.kind === "bandit"), merchant = npcs.find((n) => n.kind === "merchant");
  Object.assign(bandit, { x: merchant.x, y: merchant.y, decision: 0 }); merchant.decision = 100;
  tickNPCs(w, [bandit, merchant], [], .2, 20);
  assert.equal(bandit.loot, 40); assert.equal(merchant.cargo, 0); assert.equal(merchant.respawn, 80);
  const lord = npcs.find((n) => n.lord && n.faction === "crown"); lord.decision = 0;
  w.kingdoms = { relations: { "crown:ember": { war: true } } };
  tickNPCs(w, [lord], [], .2, 22);
  assert.equal(w.settlements.find((s) => s.id === lord.target).currentFaction, "ember");
});
test("legacy settlements appended after navigation creation remain routable without graph index errors", () => {
  const w = createWorld(), site = { ...w.settlements.at(-1), id: "legacy-imported" };
  w.settlements.push(site);
  assert.ok(w.nav.path(w.settlements[0], site).length);
});
test("captured commanders cannot move before the strategic release turn", () => {
  const w = createWorld(), n = createNPCs(w).find((n) => n.lord);
  Object.assign(n, { capturedBy: "captain", capturedUntil: 10, respawn: 10, decision: 0 });
  const previous = { x: n.x, y: n.y };
  tickNPCs(w, [n], [], 1, 11);
  assert.equal(n.x, previous.x); assert.equal(n.y, previous.y);
});
