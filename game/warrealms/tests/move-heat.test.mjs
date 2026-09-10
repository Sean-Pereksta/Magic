import test from "node:test";
import assert from "node:assert/strict";

import { emitGameEvent, GAME_EVENT_TYPES } from "../engine/event-system.js";
import { resolveHeatValue } from "../engine/heat.js";
import {
  clearMoveHeatTransferIntents,
  encodeMoveHeatAmount,
  installMoveHeatRuntimeAdapter
} from "../engine/heat-transfer.js";

function makeGame(player) {
  return {
    turnSerial: 4,
    round: 2,
    players: [player],
    eventHistory: [],
    eventWarnings: [],
    visualEvents: []
  };
}

function makePlayer(overrides = {}) {
  return {
    id: "p1",
    pendingChoices: [],
    draw: [],
    discard: [],
    hand: [],
    played: [],
    bases: [],
    attachments: [],
    ...overrides
  };
}

test("moveHeat stays authored as moveHeat while runtime cloning gains the transfer spend", () => {
  const effect = {
    moveHeat: {
      amount: 2,
      from: "friendlyHeatCard",
      to: "anotherFriendlyHeatCard"
    }
  };
  installMoveHeatRuntimeAdapter(effect);

  assert.deepEqual(Object.keys(effect), ["moveHeat"]);
  const runtime = JSON.parse(JSON.stringify(effect));
  assert.deepEqual(runtime.moveHeat, effect.moveHeat);
  assert.equal(runtime.coolHeat.amount, encodeMoveHeatAmount(2));
  assert.equal(runtime.coolHeat.targets, 1);
});

test("encoded moveHeat spends only the requested Heat", () => {
  clearMoveHeatTransferIntents();
  const update = resolveHeatValue({ max: 7 }, 3, -encodeMoveHeatAmount(2));
  assert.deepEqual(update, {
    before: 3,
    after: 1,
    maximum: 7,
    changed: true,
    gained: 0,
    spent: 2
  });
  clearMoveHeatTransferIntents();
});

test("moveHeat spend queues another active Heat card as the destination", () => {
  clearMoveHeatTransferIntents();
  resolveHeatValue({ max: 7 }, 4, -encodeMoveHeatAmount(2));

  const player = makePlayer({
    played: [
      { id: "eightfold_drone_ark", instanceId: "source", heat: 2 },
      { id: "eightfold_drone_ark", instanceId: "target", heat: 3 }
    ],
    bases: [
      { id: "heavenlance_thermal_turret", instanceId: "nearly-full", heat: 5 }
    ]
  });
  const game = makeGame(player);

  emitGameEvent(game, {
    type: GAME_EVENT_TYPES.HEAT_SPENT,
    actorId: "p1",
    playerId: "p1",
    ownerId: "p1",
    cardId: "eightfold_drone_ark",
    instanceId: "source",
    amount: 2,
    before: 4,
    after: 2,
    method: "coolHeat-choice"
  });

  assert.equal(player.pendingChoices.length, 1);
  const choice = player.pendingChoices[0];
  assert.equal(choice.source, "moveHeat");
  assert.equal(choice.title, "Move Heat");
  assert.equal(choice.options.length, 1);
  assert.equal(choice.options[0].instanceId, "target");
  assert.deepEqual(choice.options[0].effect, {
    __heatChange: { instanceId: "target", delta: 2 }
  });
});

test("moveHeat never offers its source as its own destination", () => {
  clearMoveHeatTransferIntents();
  resolveHeatValue({ max: 7 }, 4, -encodeMoveHeatAmount(1));

  const player = makePlayer({
    played: [
      { id: "eightfold_drone_ark", instanceId: "source", heat: 3 },
      { id: "eightfold_drone_ark", instanceId: "target", heat: 1 }
    ]
  });
  const game = makeGame(player);

  emitGameEvent(game, {
    type: GAME_EVENT_TYPES.HEAT_SPENT,
    actorId: "p1",
    playerId: "p1",
    ownerId: "p1",
    cardId: "eightfold_drone_ark",
    instanceId: "source",
    amount: 1,
    before: 4,
    after: 3,
    method: "coolHeat-choice"
  });

  assert.deepEqual(player.pendingChoices[0].options.map(option => option.instanceId), ["target"]);
});

test("moveHeat refunds the source when no destination can hold the whole transfer", () => {
  clearMoveHeatTransferIntents();
  resolveHeatValue({ max: 7 }, 4, -encodeMoveHeatAmount(2));

  const source = { id: "eightfold_drone_ark", instanceId: "source", heat: 2 };
  const player = makePlayer({
    played: [source],
    bases: [
      { id: "heavenlance_thermal_turret", instanceId: "full", heat: 6 }
    ]
  });
  const game = makeGame(player);

  const result = emitGameEvent(game, {
    type: GAME_EVENT_TYPES.HEAT_SPENT,
    actorId: "p1",
    playerId: "p1",
    ownerId: "p1",
    cardId: "eightfold_drone_ark",
    instanceId: "source",
    amount: 2,
    before: 4,
    after: 2,
    method: "coolHeat-choice"
  });

  assert.equal(source.heat, 4);
  assert.equal(player.pendingChoices.length, 0);
  assert.equal(result.event, null);
  assert.equal(game.eventHistory.length, 0);
});
