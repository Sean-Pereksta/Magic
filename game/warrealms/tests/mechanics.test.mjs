import test from "node:test";
import assert from "node:assert/strict";

import { allyActivationState } from "../engine/allies.js";
import { chargeActionSpend, resolveChargeGain } from "../engine/charges.js";
import {
  advanceConstruction,
  constructionHealthCap,
  repairConstructionHealth
} from "../engine/construction.js";
import {
  heatOverloadReady,
  heatThresholdsReached,
  heatTransformReady,
  resolveHeatValue
} from "../engine/heat.js";
import {
  emitSacrificeEvents,
  sacrificeEventTypes
} from "../engine/sacrifice.js";
import { GAME_EVENT_TYPES } from "../engine/event-system.js";

test("Heat clamps, reaches thresholds, overloads, and transforms at exactly 3", () => {
  const card = {
    heat: { max: 5, thresholds: [{ at: 2 }, { at: 4 }], overload: { at: 5 } },
    transform: { trigger: "heat", required: 3, into: "greater_form" }
  };
  const update = resolveHeatValue(card.heat, 2, 1);
  assert.deepEqual(update, { before: 2, after: 3, maximum: 5, changed: true, gained: 1, spent: 0 });
  assert.equal(heatTransformReady(card, 2), false);
  assert.equal(heatTransformReady(card, 3), true);
  assert.deepEqual(heatThresholdsReached(card.heat, 3).map(threshold => threshold.at), [2]);
  assert.equal(heatOverloadReady(card.heat, 4), false);
  assert.equal(heatOverloadReady(card.heat, 5), true);
  assert.equal(resolveHeatValue(card.heat, 1, -5).after, 0);
});

test("token sacrifice emits both card and token events while normal sacrifice emits only card sacrifice", () => {
  assert.deepEqual(sacrificeEventTypes({ token: true }), [GAME_EVENT_TYPES.CARD_SACRIFICED, GAME_EVENT_TYPES.TOKEN_SACRIFICED]);
  assert.deepEqual(sacrificeEventTypes({ token: false }), [GAME_EVENT_TYPES.CARD_SACRIFICED]);
  const emitted = [];
  emitSacrificeEvents({}, { id: "p1" }, { id: "spawn", instanceId: "spawn_1" }, { id: "spawn", faction: "green", token: true }, (_game, event) => emitted.push(event), { tags: ["spawn"] });
  assert.deepEqual(emitted.map(event => event.type), [GAME_EVENT_TYPES.CARD_SACRIFICED, GAME_EVENT_TYPES.TOKEN_SACRIFICED]);
  assert.equal(emitted[0].method, "sacrifice");
  assert.equal(emitted[0].token, true);
});

test("Construction raises the Health ceiling in steps and Repair respects the current cap", () => {
  assert.equal(constructionHealthCap(12, 3, 3), 4);
  assert.equal(constructionHealthCap(12, 3, 2), 8);
  assert.equal(constructionHealthCap(12, 3, 1), 12);
  const advanced = advanceConstruction(2, 2);
  assert.deepEqual(advanced, { before: 2, after: 0, advanced: 2, completed: true });
  assert.deepEqual(repairConstructionHealth(5, 8, 9), { before: 5, after: 8, repaired: 3 });
});

test("Charge gain obeys maximum and per-turn cap", () => {
  const first = resolveChargeGain(2, { max: 5, gain: 3, perTurnCap: 2 }, 1);
  assert.equal(first.after, 3);
  assert.equal(first.gained, 1);
  const capped = resolveChargeGain(5, { max: 5, gain: 2 }, 0);
  assert.equal(capped.gained, 0);
  assert.deepEqual(chargeActionSpend(4, { cost: "all", minimum: 3 }), { allowed: true, spend: 4, available: 4 });
  assert.equal(chargeActionSpend(2, { cost: 3 }).allowed, false);
});

test("Ally and Double Ally activation levels remain distinct", () => {
  assert.deepEqual(allyActivationState(1), { active: 1, ally: false, doubleAlly: false, additionalFactionCards: 0 });
  assert.equal(allyActivationState(2).ally, true);
  assert.equal(allyActivationState(2).doubleAlly, false);
  assert.equal(allyActivationState(3).doubleAlly, true);
});
