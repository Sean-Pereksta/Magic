import test from "node:test";
import assert from "node:assert/strict";

import { GAME_EVENT_TYPES } from "../engine/event-system.js";
import {
  baseEvolutionChargeUpdate,
  baseEvolutionEventUpdate,
  baseEvolutionIsReady
} from "../engine/base-evolution.js";
import {
  baseTransformDestination,
  inheritedTransformState,
  transformPreservesAttachments
} from "../engine/transforms.js";

test("Base evolution preserves configured state and scales Health ratio", () => {
  const source = {
    currentHealth: 5,
    maxHealth: 10,
    armor: 3,
    armorExpiresTurn: 8,
    charges: 4,
    chargeUsedTurn: 6,
    disabledTurn: 7,
    stunTurns: 1,
    turnsSurvived: 3
  };
  const result = inheritedTransformState(source, { health: 10 }, { health: 20, charge: { max: 5 } }, {
    healthRatio: true,
    armor: true,
    charges: true,
    disableState: true,
    turnsSurvived: true
  });
  assert.equal(result.currentHealth, 10);
  assert.equal(result.maxHealth, 20);
  assert.equal(result.armor, 3);
  assert.equal(result.charges, 4);
  assert.equal(result.disabledTurn, 7);
  assert.equal(result.turnsSurvived, 3);
});

test("Base transforms default in place and attachment persistence is configurable", () => {
  assert.equal(baseTransformDestination({ type: "base" }, {}, {}), "");
  assert.equal(transformPreservesAttachments({ preserve: { attachments: true } }), true);
  assert.equal(transformPreservesAttachments({ preserve: { attachments: false } }), false);
});

test("event-driven Base evolution tracks construction, damage, repair, attachment, and destruction events", () => {
  const source = { ownerId: "p1", instanceId: "base_1", attachmentCount: 2 };
  const construction = baseEvolutionEventUpdate({ trigger: "constructionCompleted", required: 1 }, source, {
    type: GAME_EVENT_TYPES.BASE_CONSTRUCTION_COMPLETED,
    ownerId: "p1",
    instanceId: "base_1"
  }, 0);
  assert.equal(baseEvolutionIsReady(construction), true);

  const damage = baseEvolutionEventUpdate({ trigger: "damageTaken", required: 5 }, source, {
    type: GAME_EVENT_TYPES.BASE_DAMAGED,
    ownerId: "p1",
    instanceId: "base_1",
    amount: 3
  }, 2);
  assert.equal(baseEvolutionIsReady(damage), true);

  const repair = baseEvolutionEventUpdate({ trigger: "repairsReceived", required: 4 }, source, {
    type: GAME_EVENT_TYPES.BASE_REPAIRED,
    ownerId: "p1",
    instanceId: "base_1",
    amount: 2
  }, 2);
  assert.equal(baseEvolutionIsReady(repair), true);

  const attachments = baseEvolutionEventUpdate({ trigger: "attachmentsReached", required: 2 }, source, {
    type: GAME_EVENT_TYPES.ATTACHMENT_ATTACHED,
    ownerId: "p1",
    baseInstanceId: "base_1"
  }, 0);
  assert.equal(attachments.progress, 2);

  const friendlyDestroyed = baseEvolutionEventUpdate({ trigger: "friendlyBaseDestroyed", required: 1 }, source, {
    type: GAME_EVENT_TYPES.BASE_DESTROYED,
    ownerId: "p1",
    actorId: "p2",
    instanceId: "other_base"
  }, 0);
  assert.equal(baseEvolutionIsReady(friendlyDestroyed), true);

  const enemyDestroyed = baseEvolutionEventUpdate({ trigger: "enemyBaseDestroyed", required: 1 }, source, {
    type: GAME_EVENT_TYPES.BASE_DESTROYED,
    ownerId: "p2",
    actorId: "p1",
    instanceId: "enemy_base"
  }, 0);
  assert.equal(baseEvolutionIsReady(enemyDestroyed), true);
});

test("chargesReached evolution uses the Base's current Charge total", () => {
  const update = baseEvolutionChargeUpdate({ trigger: "chargesReached", required: 4 }, 4, 3);
  assert.equal(update.progress, 4);
  assert.equal(baseEvolutionIsReady(update), true);
});
