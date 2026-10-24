import test from "node:test";
import assert from "node:assert/strict";
import { Realm } from "../realm.mjs";
import { newProfile, clone, UNITS, WORLD, emptyArmy } from "../data.mjs";
import { tickKingdoms, playerTreaty, kingdomHostile } from "../kingdoms.mjs";
import { signatureRecruitment, tickEconomy } from "../progression.mjs";
import { serializeCampaign, parseCampaign } from "../storage.mjs";
import { BIOMES, tickNPCs } from "../world.mjs";

const company = () => { const r = new Realm(); const p = r.enter("local", newProfile("Envoy")); p.gold = 10000; return { r, p }; };
const advance = (r, seconds = 12) => { r.time += seconds; r.world.time = r.time; tickKingdoms(r, seconds); };
const agree = (r, p, faction, action) => { p.relations[faction] = 90; p.diplomacy.trust[faction] = 90; r.action(p.uid, "diplomacy", { faction, action }); };

test("negotiations produce reasons and finite counteroffers; accepting charges exactly once", () => {
  const { r, p } = company(), before = p.gold;
  p.relations.stone = 30;
  r.action("local", "diplomacy", { faction: "stone", action: "militaryAlliance" });
  assert.equal(p.diplomacy.lastDecision.status, "countered");
  assert.equal(p.gold, before);
  const o = p.diplomacy.offers.at(-1);
  assert.ok(o.gold > 0 && o.reasons.length && o.expires > r.time);
  r.action("local", "diplomacy", { faction: "stone", action: "accept", offerId: o.id });
  assert.equal(p.gold, before - o.gold);
  assert.ok(playerTreaty(p, "stone", "militaryAlliance", r.time));
  const acceptedGold = p.gold;
  assert.throws(() => r.action("local", "diplomacy", { faction: "stone", action: "accept", offerId: o.id }), /already|expired/);
  assert.equal(p.gold, acceptedGold);
});

test("diplomacy validates faction, target, whole gold amounts, and envoy ownership before transfer", () => {
  const { r, p } = company(), before = p.gold;
  for (const input of [
    { faction: "unknown", action: "gift" }, { faction: "crown", action: "gift", gold: -1 },
    { faction: "crown", action: "gift", gold: 0.5 }, { faction: "crown", action: "gift", gold: Infinity },
    { faction: "crown", action: "aid", target: "missing" }, { faction: "crown", action: "accept", offerId: "o-9999999999" },
  ]) assert.throws(() => r.action("local", "diplomacy", input));
  assert.equal(p.gold, before);
});

test("treaties prevent attacks, expire, and breaking them damages trust across kingdoms", () => {
  const { r, p } = company(); agree(r, p, "crown", "pact");
  const c = r.world.settlements.find((s) => s.id === "dawnkeep"); p.x = c.x; p.y = c.y;
  assert.throws(() => r.startBattle("local", "siege", c.id), /agreement/);
  const trust = p.diplomacy.trust.crown, externalTrust = p.diplomacy.trust.vale;
  r.action("local", "diplomacy", { faction: "crown", action: "break" });
  assert.ok(p.diplomacy.trust.crown < trust && p.diplomacy.trust.vale < externalTrust);
  agree(r, p, "crown", "passage");
  const until = p.diplomacy.treaties.find((t) => t.type === "passage").until;
  r.time = until + 1; tickKingdoms(r, 12);
  assert.equal(playerTreaty(p, "crown", "passage", r.time), false);
});

test("kingdoms declare wars and sign agreements without players; strategic turns remain deterministic", () => {
  const a = new Realm(), b = new Realm();
  for (let i = 0; i < 7; i++) { advance(a); advance(b); }
  assert.ok(a.kingdoms.wars.length > 0);
  assert.ok(Object.values(a.kingdoms.relations).some((r) => r.treaties.length));
  assert.equal(kingdomHostile(a.kingdoms, "crown", "ember"), true);
  assert.deepEqual(a.kingdoms, b.kingdoms);
});

test("AI lord campaigns capture poorly defended enemy settlements and affect war score and exhaustion", () => {
  const r = new Realm(); advance(r);
  const n = r.npcs.find((n) => n.kind === "patrol" && n.faction === "crown"), target = r.world.settlements.find((s) => s.faction === "ember" && s.kind === "castle");
  n.x = target.x; n.y = target.y; n.respawn = 0; target.garrison = 1;
  advance(r);
  assert.equal(target.currentFaction, "crown");
  assert.equal(target.faction, "crown");
  assert.equal(target.baseFaction, "ember");
  assert.ok(r.kingdoms.wars[0].score > 0 && r.kingdoms.factions.ember.exhaustion > 0);
  assert.ok(target.loyalty < 50 && target.damage > 0);
});

test("player captures score territory and losses make peace negotiations more favorable", () => {
  const { r, p } = company(), c = r.world.settlements.find((s) => s.id === "dawnkeep");
  p.x = c.x; p.y = c.y;
  const b = r.startBattle("local", "siege", c.id); b.finished = true; b.winner = 0; r.settle(b);
  assert.equal(c.owner, "local");
  assert.ok(p.diplomacy.wars.crown.score >= 25);
  p.diplomacy.wars.crown.exhaustion = 70; r.kingdoms.factions.crown.exhaustion = 60;
  r.action("local", "diplomacy", { faction: "crown", action: "peace" });
  assert.equal(p.diplomacy.lastDecision.status, "accepted");
  assert.equal(p.diplomacy.wars.crown, undefined);
  assert.ok(playerTreaty(p, "crown", "peace", r.time));
});

test("captured named lords are unavailable, can be ransomed, and return with a rebuilding army", () => {
  const { r, p } = company(), n = r.npcs.find((n) => n.lord); p.x = n.x; p.y = n.y;
  const b = r.startBattle("local", "npc", n.id); b.finished = true; b.winner = 0;
  for (const u of b.units) if (u.team === 1) u.hp = 0;
  r.settle(b);
  assert.equal(n.capturedBy, "local"); assert.ok(p.diplomacy.prisoners.some((l) => l.id === n.id));
  n.respawn = 0;
  assert.throws(() => r.startBattle("local", "npc", n.id), /moved on/);
  const gold = p.gold;
  r.action("local", "diplomacy", { faction: n.faction, action: "prisoners" });
  assert.ok(p.gold > gold); assert.equal(n.capturedBy, null); assert.equal(p.diplomacy.prisoners.length, 0);
  advance(r, 24); assert.ok(n.army.recruits >= 8);
});

test("trade income follows completed caravan deliveries and is not paid again on later turns", () => {
  const { r, p } = company(), n = r.npcs.find((n) => n.kind === "merchant"); agree(r, p, n.faction, "trade");
  const gold = p.gold; n.deliveryCount = (n.deliveryCount || 0) + 1;
  advance(r); assert.equal(p.gold, gold + 5);
  advance(r); assert.equal(p.gold, gold + 5);
});

test("signature recruitment depends on the homeland, relations, rank, and recruitment buildings", () => {
  const { r, p } = company(), home = r.world.settlements.find((s) => s.faction === "crown" && s.kind === "capital"), foreign = r.world.settlements.find((s) => s.faction === "vale" && s.kind === "village");
  p.level = 3; p.x = home.x; p.y = home.y;
  assert.equal(signatureRecruitment(p, r.world, home, "royalCaptains").allowed, false);
  p.relations.crown = 40;
  assert.equal(signatureRecruitment(p, r.world, home, "royalCaptains").allowed, true);
  assert.equal(signatureRecruitment(p, r.world, foreign, "royalCaptains").allowed, false);
  r.action("local", "recruit", { settlement: home.id, unit: "royalCaptains", count: 2 });
  assert.equal(p.army.royalCaptains, 2);
  home.recruitment = 1;
  assert.equal(signatureRecruitment(p, r.world, home, "royalCaptains").allowed, false);
});

test("raid damage reduces income and deep hostile desert travel consumes additional supplies", () => {
  const { r, p } = company(), c = r.world.settlements.find((s) => s.id === "dawnkeep"); c.owner = "local"; c.damage = 60;
  const normal = clone(p), damaged = clone(p); c.damage = 0; tickEconomy(normal, r.world, 120); c.damage = 60; tickEconomy(damaged, r.world, 120);
  assert.ok(normal.gold > damaged.gold);
  const s = r.world.settlements.find((s) => s.faction === "desert"); p.x = s.x; p.y = s.y;
  r.world.biomes[Math.floor(p.y) * WORLD.width + Math.floor(p.x)] = BIOMES.desert;
  p.relations.desert = -60; const before = p.supplies; tickEconomy(p, r.world, 120);
  assert.ok(before - p.supplies > 3);
});

test("new and old campaigns preserve strategic state while regenerating the expanded continent", () => {
  const { r, p } = company(); agree(r, p, "vale", "trade"); advance(r);
  const far = r.world.settlements.find((s) => s.x > 128); far.owner = "local"; p.x = far.x; p.y = far.y;
  const restored = parseCampaign(serializeCampaign(r, "local"));
  assert.equal(restored.world.settlements.length, 80); assert.equal(restored.npcs.length, 74);
  assert.equal(restored.world.settlements.find((s) => s.id === far.id).owner, "local");
  assert.ok(playerTreaty(restored.player("local"), "vale", "trade", restored.time));
  assert.deepEqual(restored.kingdoms.wars, r.kingdoms.wars);
  const old = JSON.parse(serializeCampaign(r, "local")); old.profile.x = 15.5; old.profile.y = 44.5; delete old.profile.diplomacy;
  old.state.settlements = old.state.settlements.slice(0, 10); old.state.npcs = old.state.npcs.slice(0, 28); delete old.state.kingdoms;
  const migrated = parseCampaign(JSON.stringify(old)); assert.equal(migrated.world.settlements.length, 80); assert.equal(migrated.npcs.length, 74);
  assert.ok(Object.keys(migrated.player("local").army).length >= 34);
});

test("kingdom checkpoint roundtrips through Realm without terrain payloads or losing online alliances", () => {
  const { r } = company(); r.alliances = [{ id: "old-alliance", members: ["local"] }]; advance(r);
  const checkpoint = r.checkpoint(), restored = new Realm(checkpoint);
  assert.deepEqual(restored.kingdoms.wars, r.kingdoms.wars); assert.deepEqual(restored.alliances, r.alliances);
  assert.equal(checkpoint.tiles, undefined); assert.equal(checkpoint.biomes, undefined); assert.equal(checkpoint.nav, undefined);
  assert.ok(JSON.stringify(checkpoint).length < 200000);
});

test("diplomatic histories and offers remain bounded during sustained negotiations", () => {
  const { r, p } = company(); p.gold = 100000;
  p.relations.stone = 30;
  for (let i = 0; i < 70; i++) r.action("local", "diplomacy", { faction: "stone", action: "militaryAlliance" });
  assert.equal(p.diplomacy.offers.length, 24);
  for (let i = 0; i < 70; i++) r.action("local", "diplomacy", { faction: "crown", action: "gift", gold: 1 });
  assert.equal(p.diplomacy.history.length, 40); assert.equal(r.kingdoms.history.length, 60);
});

test("campaign village raids have a supply objective, damage the economy, and settle loot only once", () => {
  const { r, p } = company(), village = r.world.settlements.find((s) => s.id === "hearthford"); p.x = village.x; p.y = village.y; p.supplies = 70;
  const b = r.startBattle("local", "raid", village.id);
  assert.equal(b.objective.type, "raid"); assert.equal(b.siege, false);
  b.finished = true; b.winner = 0; r.settle(b);
  const gold = p.gold;
  assert.ok(village.damage >= 40 && village.recruitStock < 30 && village.production < 1 && p.supplies > 70);
  assert.equal(p.diplomacy.wars.crown.score, 8);
  r.settle(b); assert.equal(p.gold, gold);
  assert.throws(() => r.startBattle("local", "raid", village.id), /recovering/);
});

test("campaign caravan defense rewards protection once and leaves route completion to the merchant simulation", () => {
  const { r, p } = company(), merchant = r.npcs.find((n) => n.kind === "merchant"), bandit = r.npcs.find((n) => n.kind === "bandit");
  p.x = merchant.x; p.y = merchant.y; bandit.x = merchant.x + 2; bandit.y = merchant.y;
  const b = r.startBattle("local", "caravanDefense", merchant.id);
  assert.equal(b.objective.type, "caravanDefense"); assert.equal(merchant.battleId, b.id); assert.equal(b.encounter.targetId, bandit.id);
  b.finished = true; b.winner = 0; r.settle(b);
  assert.equal(merchant.battleId, null); assert.equal(merchant.deliveryCount || 0, 0);
  const gold = p.gold; r.settle(b); assert.equal(p.gold, gold); assert.equal(merchant.deliveryCount || 0, 0);
});

test("campaign reinforcement battles commit allied lord troops without adding them to the player roster", () => {
  const { r, p } = company(), enemy = r.npcs.find((n) => n.kind === "bandit"), ally = r.npcs.find((n) => n.lord && n.faction === "crown");
  agree(r, p, "crown", "defensiveAlliance"); p.x = enemy.x; p.y = enemy.y; ally.x = p.x + 2; ally.y = p.y;
  const before = clone(p.army), b = r.startBattle("local", "reinforcement", enemy.id);
  assert.equal(b.objective.type, "reinforcement"); assert.equal(ally.battleId, b.id);
  b.objective.waves[0].delivered = true;
  b.units.push({ id: 5000, team: 0, key: "royalCaptains", hp: 10, temporary: true });
  b.finished = true; b.winner = 0; r.settle(b);
  assert.deepEqual(p.army, before); assert.equal(ally.battleId, null); assert.equal(ally.army.royalCaptains, 1);
});

test("tribute obligations pay on their due turn and defensive treaties mobilize AI support", () => {
  const { r, p } = company(); agree(r, p, "suncoast", "tribute");
  const gold = p.gold; advance(r, 120); assert.equal(p.gold, gold - 12);
  advance(r, 12); assert.equal(p.gold, gold - 12);
  agree(r, p, "vale", "defensiveAlliance");
  const attacker = r.npcs.find((n) => n.kind === "patrol" && n.faction === "ember");
  p.x = attacker.x; p.y = attacker.y; p.relations.ember = -60; p.readyAt = 0; p.battleCooldown = 0; attacker.route = []; attacker.decision = 100; attacker.respawn = 0;
  r.tick(0.05);
  assert.equal(kingdomHostile(r.kingdoms, "vale", "ember"), true);
});

test("expired captive timers cannot intercept players or pressure holdings before the strategic release turn", () => {
  const { r, p } = company(), n = r.npcs.find((n) => n.lord && n.faction === "crown"), c = r.world.settlements.find((s) => s.id === "dawnkeep");
  r.time = 2; n.capturedBy = "local"; n.capturedUntil = 1; n.respawn = 1; n.x = c.x; n.y = c.y;
  p.x = c.x; p.y = c.y; p.relations.crown = -50; p.readyAt = 0; p.battleCooldown = 0;
  c.owner = "local"; c.ownerReputation = -30; c.raidPressure = 19.9;
  p.diplomacy.prisoners.push({ id: n.id, name: n.name, faction: n.faction, until: 1 });
  assert.doesNotThrow(() => r.tick(0.2));
  assert.equal(p.activeBattle, null); assert.equal(c.raidPressure, 19.9);
  assert.equal(r.snapshot("local").npcs.some((npc) => npc.id === n.id), false);
  advance(r, 12);
  assert.equal(n.capturedBy, null); assert.equal(p.diplomacy.prisoners.some((l) => l.id === n.id), false);
  assert.ok(n.respawn > r.time);
});

test("an escort defended at its destination produces exactly one actual delivery and one trade payment", () => {
  const { r, p } = company(), merchant = r.npcs.find((n) => n.kind === "merchant"), bandit = r.npcs.find((n) => n.kind === "bandit"), town = r.world.settlements.find((s) => s.kind === "town");
  agree(r, p, merchant.faction, "trade");
  merchant.x = town.x; merchant.y = town.y; merchant.target = town.id; merchant.cargo = 40; merchant.route = []; merchant.decision = 0;
  p.x = town.x; p.y = town.y; bandit.x = town.x + 2; bandit.y = town.y;
  const b = r.startBattle("local", "caravanDefense", merchant.id); b.finished = true; b.winner = 0; r.settle(b);
  assert.equal(merchant.deliveryCount || 0, 0);
  const afterEscort = p.gold;
  tickNPCs(r.world, [merchant], [], 0.2, r.time + 0.2);
  assert.equal(merchant.deliveryCount, 1);
  advance(r); assert.equal(p.gold, afterEscort + 5);
  advance(r); assert.equal(p.gold, afterEscort + 5);
});

test("a successful rearguard escape does not capture a lord whose pursuing army remains alive", () => {
  const { r, p } = company(), lord = r.npcs.find((n) => n.lord); p.x = lord.x; p.y = lord.y;
  const army = clone(lord.army), b = r.startBattle("local", "rearguard", lord.id);
  b.finished = true; b.winner = 0; b.objective.completed = true;
  assert.ok(b.units.some((u) => u.team === 1 && u.hp > 0 && !u.routed));
  r.settle(b);
  assert.equal(lord.capturedBy || null, null); assert.equal(p.diplomacy.prisoners.length, 0); assert.deepEqual(lord.army, army);
});
