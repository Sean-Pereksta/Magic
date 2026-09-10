import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";

import { resolveHeatValue } from "../engine/heat.js";
import {
  clearMoveHeatTransferIntents,
  clearRegisteredHeatCards,
  encodeMoveHeatAmount,
  installMoveHeatRuntimeAdapter,
  prepareMoveHeatDestination
} from "../engine/heat-transfer.js";

function makeGame(player) {
  return {
    turnSerial: 4,
    eventSequence: 0,
    players: [player]
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

function registerTestHeatCards() {
  clearRegisteredHeatCards();
  installMoveHeatRuntimeAdapter([
    { id: "source_heat", name: "Source Heat", heat: { max: 7 } },
    { id: "target_heat", name: "Target Heat", heat: { max: 7 } },
    { id: "tight_heat", name: "Tight Heat", heat: { max: 6 } }
  ]);
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
  registerTestHeatCards();
  clearMoveHeatTransferIntents();
  resolveHeatValue({ max: 7 }, 4, -encodeMoveHeatAmount(2));

  const player = makePlayer({
    played: [
      { id: "source_heat", instanceId: "source", heat: 2 },
      { id: "target_heat", instanceId: "target", heat: 3 }
    ],
    bases: [
      { id: "tight_heat", instanceId: "nearly-full", heat: 5 }
    ]
  });
  const game = makeGame(player);
  const event = {
    type: "HEAT_SPENT",
    actorId: "p1",
    playerId: "p1",
    ownerId: "p1",
    cardId: "source_heat",
    instanceId: "source",
    amount: 2,
    before: 4,
    after: 2,
    method: "coolHeat-choice"
  };

  const result = prepareMoveHeatDestination(game, event);

  assert.deepEqual(result, { transfer: true, refunded: false });
  assert.equal(event.method, "moveHeat-source");
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
  registerTestHeatCards();
  clearMoveHeatTransferIntents();
  resolveHeatValue({ max: 7 }, 4, -encodeMoveHeatAmount(1));

  const player = makePlayer({
    played: [
      { id: "source_heat", instanceId: "source", heat: 3 },
      { id: "target_heat", instanceId: "target", heat: 1 }
    ]
  });

  prepareMoveHeatDestination(makeGame(player), {
    type: "HEAT_SPENT",
    actorId: "p1",
    playerId: "p1",
    ownerId: "p1",
    cardId: "source_heat",
    instanceId: "source",
    amount: 1,
    before: 4,
    after: 3,
    method: "coolHeat-choice"
  });

  assert.deepEqual(player.pendingChoices[0].options.map(option => option.instanceId), ["target"]);
});

test("moveHeat refunds the source when no destination can hold the whole transfer", () => {
  registerTestHeatCards();
  clearMoveHeatTransferIntents();
  resolveHeatValue({ max: 7 }, 4, -encodeMoveHeatAmount(2));

  const source = { id: "source_heat", instanceId: "source", heat: 2 };
  const player = makePlayer({
    played: [source],
    bases: [
      { id: "tight_heat", instanceId: "full", heat: 6 }
    ]
  });

  const result = prepareMoveHeatDestination(makeGame(player), {
    type: "HEAT_SPENT",
    actorId: "p1",
    playerId: "p1",
    ownerId: "p1",
    cardId: "source_heat",
    instanceId: "source",
    amount: 2,
    before: 4,
    after: 2,
    method: "coolHeat-choice"
  });

  assert.deepEqual(result, { transfer: true, refunded: true });
  assert.equal(source.heat, 4);
  assert.equal(player.pendingChoices.length, 0);
});

test("event system hooks HEAT_SPENT into moveHeat destination resolution", async () => {
  const source = await fs.readFile(new URL("../engine/event-system.js", import.meta.url), "utf8");
  assert.match(source, /prepareMoveHeatDestination\(game, input\)/);
  assert.match(source, /if \(moveHeat\.refunded\)/);
  assert.match(source, /return \{ event: null, resolution \}/);
});
