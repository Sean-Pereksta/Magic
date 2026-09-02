import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { loadCardPack } from "./test-utils.mjs";

const pack = await loadCardPack();
const byId = new Map(pack.CARDS.map(card => [card.id, card]));

test("Heat payoff mini-pack card definitions are present", () => {
  const imbuer = byId.get("sanctum_core_imbuer");
  assert.ok(imbuer);
  assert.equal(imbuer.faction, "blue");
  assert.equal(imbuer.cost, 7);
  assert.deepEqual(imbuer.effect.addHeat, { amount: 2, target: "friendlyHeatCard", excludeSelf: true });
  assert.equal(imbuer.ally.shield, 2);
  assert.equal(imbuer.doubleAlly.or[0].effect.repair.amount, 5);
  assert.equal(imbuer.doubleAlly.or[1].effect.shield, 5);

  const turret = byId.get("heavenlance_thermal_turret");
  assert.ok(turret);
  assert.equal(turret.type, "base");
  assert.equal(turret.defense, 8);
  assert.equal(turret.heat.max, 6);
  assert.equal(turret.heat.overload.at, 6);
  assert.equal(turret.heat.overload.effect.combat, 15);
  assert.equal(turret.heat.overload.reset, 0);

  const ark = byId.get("eightfold_drone_ark");
  assert.ok(ark);
  assert.equal(ark.faction, "yellow");
  assert.equal(ark.heat.max, 8);
  assert.equal(ark.heat.overload.at, 8);
  assert.deepEqual(ark.heat.overload.effect.createToken, { id: "drone", count: 4, zone: "discard" });
});

test("live battle resolver supports adding Heat to active Heat cards", async () => {
  const html = await fs.readFile(new URL("../../warrealms.html", import.meta.url), "utf8");
  assert.match(html, /function queueAddHeatTargetChoice\(/);
  assert.match(html, /const addHeatConfig = normalized\.addHeat;/);
  assert.match(html, /queueAddHeatTargetChoice\(game, player, addHeatConfig, context\)/);
  assert.match(html, /targetRule === "friendlyHeatShip" && targetCard\.type !== "ship"/);
  assert.match(html, /heatOverloadReady\(card\.heat, entry\.heat\)/);
});
