import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import vm from "node:vm";
import { magnitude, combatFeedback, nextEffectIndex, effectPriority } from "../ui/combat-effects-model.js";
import { bridgeCombatEvent } from "../engine/combat-visual-events.js";
import { emitGameEvent, GAME_EVENT_TYPES } from "../engine/event-system.js";

const html = await readFile(new URL("../../warrealms.html", import.meta.url), "utf8");
function liveFunction(name) {
  const start = html.indexOf(`  function ${name}(`);
  assert.ok(start >= 0, `live ${name} exists`);
  const end = html.indexOf("\n  function ", start + 5);
  return html.slice(start, end);
}
function liveContext() {
  let id = 0;
  const context = vm.createContext({
    GAME_EVENT_TYPES, STARTING_AUTHORITY: 50, CARD_MAP: {},
    makeId: () => `fx_${++id}`, emitGameEvent,
    addLog() {}, finishGameIfNeeded() {}, resolveCampaignBossPhases() {},
    advanceOwnerTurnTransforms() {}, triggerFactionAllies() {}, drawCards() {}
  });
  for (const name of ["pushVisualEvent", "damageAuthority", "healPlayer", "beginTurn"]) vm.runInContext(liveFunction(name), context);
  return context;
}
const player = (id, overrides = {}) => ({ id, name: id, health: 30, maxAuthority: 50, shield: 0, bases: [], hand: [], played: [], ...overrides });

test("damage magnitude and feedback distinguish shield-only, split, break, lethal, and healing", () => {
  assert.deepEqual([1, 4, 5, 9, 10, 50].map(magnitude), ["minor", "minor", "medium", "medium", "heavy", "heavy"]);
  assert.deepEqual(combatFeedback({ type: "health-loss", absorbed: 4, amount: 0 }).map(x => x.kind), ["shield-hit"]);
  assert.deepEqual(combatFeedback({ type: "health-loss", absorbed: 4, amount: 8, shieldBroken: true }).map(x => x.kind), ["shield-hit", "shield-break", "damage"]);
  assert.equal(combatFeedback({ type: "health-gain", amount: 6 })[0].label, "+6");
  assert.equal(combatFeedback({ type: "shield-gain", amount: 8 })[0].label, "🛡 +8");
  assert.equal(combatFeedback({ type: "health-loss", amount: 20, lethal: true }).at(-1).kind, "death");
  assert.deepEqual(combatFeedback({ type: "base-repair", amount: 0 }), []);
});

test("priority preserves each target's damage/destruction order and promotes transformation", () => {
  const queue = [
    { event: { type: "stat-gain", targetId: "me" } },
    { event: { type: "base-damage", instanceId: "base" } },
    { event: { type: "card-destroy", instanceId: "base" } },
    { event: { type: "card-transform", instanceId: "egg" } }
  ];
  assert.equal(nextEffectIndex(queue), 3);
  queue.splice(3, 1);
  assert.equal(nextEffectIndex(queue), 1);
  queue.splice(1, 1);
  assert.equal(nextEffectIndex(queue), 1);
  assert.ok(effectPriority({ type: "health-loss", shieldBroken: true }) > effectPriority({ type: "health-loss", amount: 12 }));
});

test("live health damage resolves immediately and emits exact split Shield/Health feedback", () => {
  const c = liveContext();
  const target = player("enemy", { shield: 8 });
  const game = { players: [target], turnSerial: 1 };
  assert.equal(c.damageAuthority(game, target, 3, "attack", "me"), 0);
  assert.equal(target.shield, 5);
  assert.equal(target.health, 30);
  assert.equal(game.visualEvents.at(-1).absorbed, 3);
  assert.equal(game.visualEvents.at(-1).shieldBroken, false);
  assert.equal(c.damageAuthority(game, target, 12, "attack", "me"), 7);
  assert.equal(target.health, 23);
  assert.equal(target.shield, 0);
  assert.equal(game.visualEvents.at(-1).amount, 7);
  assert.equal(game.visualEvents.at(-1).absorbed, 5);
  assert.equal(game.visualEvents.at(-1).shieldBroken, true);
  c.damageAuthority(game, target, 99, "attack", "me");
  assert.equal(target.health, 0);
  assert.equal(target.eliminated, true);
  assert.equal(game.visualEvents.at(-1).lethal, true);
});

test("live healing is capped, produces one event, and round-trips to a remote client", () => {
  const c = liveContext(), target = player("me", { health: 48 });
  const game = { players: [target], turnSerial: 1 };
  assert.equal(c.healPlayer(game, target, 10), 2);
  assert.equal(game.visualEvents.length, 1);
  assert.equal(game.visualEvents[0].type, "health-gain");
  assert.equal(game.visualEvents[0].amount, 2);
  assert.equal(c.healPlayer(game, target, 5), 0);
  assert.equal(game.visualEvents.length, 1);
  const remote = JSON.parse(JSON.stringify(game));
  assert.deepEqual(combatFeedback(remote.visualEvents[0]), combatFeedback(game.visualEvents[0]));
});

test("base and Shield feedback bridges resolve the owner and keep card sources separate", () => {
  const damaged = bridgeCombatEvent({ type: "BASE_DAMAGED", id: "hit", actorId: "me", playerId: "me", ownerId: "enemy", instanceId: "fort", amount: 2, absorbed: 4 });
  assert.equal(damaged.targetId, "enemy");
  assert.equal(damaged.instanceId, "fort");
  const shield = bridgeCombatEvent({ type: "SHIELD_GAINED", id: "shield", ownerId: "me", instanceId: "caster", amount: 8 });
  assert.equal(shield.instanceId, "");
  assert.equal(shield.sourceInstanceId, "caster");
  assert.equal(shield.type, "shield-gain");
  assert.equal(bridgeCombatEvent({ type: "CARD_DESTROYED", id: "x" }), null);
});

test("visual history remains bounded under repeated engine events", () => {
  const game = { players: [], turnSerial: 1 };
  for (let i = 0; i < 400; i++) emitGameEvent(game, { type: "BASE_REPAIRED", ownerId: "me", instanceId: "fort", amount: 1 });
  assert.equal(game.visualEvents.length, 80);
  assert.equal(new Set(game.visualEvents.map(e => e.id)).size, 80);
  assert.equal(game.eventHistory.length, 80);
});

test("live Disable persists through the skipped owner turn, then emits one restoration", () => {
  const c = liveContext();
  const base = { id: "fort", instanceId: "fort_1", disabledTurn: 0, stunTurns: 0 };
  const actor = player("me", { stun: 2 }), target = player("enemy", { bases: [base] });
  const game = { players: [actor, target], turnSerial: 1, round: 1 };
  const start = html.indexOf('      case "STUN_BASE": {');
  const end = html.indexOf('      case "DESTROY_BASE":', start);
  vm.runInContext(`function stun(game, actor, action) { switch (action.type) { ${html.slice(start, end)} } }`, c);
  c.stun(game, actor, { type: "STUN_BASE", targetId: "enemy", instanceId: "fort_1" });
  assert.equal(base.stunTurns, 1);
  assert.equal(game.visualEvents.filter(e => e.type === "card-disable").length, 1);
  c.stun(game, actor, { type: "STUN_BASE", targetId: "enemy", instanceId: "fort_1" });
  assert.equal(game.visualEvents.filter(e => e.type === "card-disable").length, 1);
  c.beginTurn(game, target);
  assert.equal(base.stunTurns, 0);
  assert.equal(base.disabledTurn, game.turnSerial);
  assert.equal(game.visualEvents.filter(e => e.type === "card-enable").length, 0);
  c.beginTurn(game, actor);
  assert.notEqual(base.disabledTurn, game.turnSerial);
  assert.equal(game.visualEvents.filter(e => e.type === "card-enable").length, 1);
  c.beginTurn(game, target);
  assert.equal(game.visualEvents.filter(e => e.type === "card-enable").length, 1);
});
