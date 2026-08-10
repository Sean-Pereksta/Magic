import test from "node:test";
import assert from "node:assert/strict";

import {
  GAME_EVENT_TYPES,
  emitGameEvent
} from "../engine/event-system.js";
import { eventGame } from "./test-utils.mjs";
import { isEffectResolution, queueEffectResolution } from "../engine/effects.js";

test("nested events resolve deterministically in one chain", () => {
  const game = eventGame();
  const resolved = [];
  emitGameEvent(game, { type: GAME_EVENT_TYPES.CARD_PLAYED, playerId: "p1", cardId: "ship" }, {
    resolveEvent(event, controls) {
      resolved.push(event.type);
      if (event.type === GAME_EVENT_TYPES.CARD_PLAYED) controls.emit({ type: GAME_EVENT_TYPES.AUTHORITY_GAINED, playerId: "p1", amount: 1 });
      if (event.type === GAME_EVENT_TYPES.AUTHORITY_GAINED) controls.emit({ type: GAME_EVENT_TYPES.SHIELD_GAINED, playerId: "p1", amount: 1 });
    }
  });

  assert.deepEqual(resolved, [
    GAME_EVENT_TYPES.CARD_PLAYED,
    GAME_EVENT_TYPES.AUTHORITY_GAINED,
    GAME_EVENT_TYPES.SHIELD_GAINED
  ]);
  assert.equal(new Set(game.eventHistory.map(event => event.chainId)).size, 1);
  assert.deepEqual(game.eventHistory.map(event => event.depth), [0, 1, 2]);
});

test("a trigger can claim the exact event only once", () => {
  const game = eventGame();
  let first;
  let second;
  emitGameEvent(game, { type: GAME_EVENT_TYPES.CARD_ACQUIRED, playerId: "p1", cardId: "card" }, {
    resolveEvent(_event, controls) {
      first = controls.claim("base_1:trigger:0");
      second = controls.claim("base_1:trigger:0");
    }
  });
  assert.equal(first, true);
  assert.equal(second, false);
});

test("queued trigger effects resolve in order and preserve their parent event chain", () => {
  const game = eventGame({ players: [{ id: "p1" }] });
  const order = [];
  const handlers = {
    resolveEvent(event) {
      order.push(event.type);
      if (event.type === GAME_EVENT_TYPES.CARD_PLAYED) queueEffectResolution(game, "p1", { heal: 1 }, { source: "test-trigger" });
    },
    resolveItem(item) {
      if (!isEffectResolution(item)) return;
      order.push("EFFECT");
      emitGameEvent(game, { type: GAME_EVENT_TYPES.AUTHORITY_GAINED, playerId: item.playerId, amount: 1 }, handlers);
    }
  };
  emitGameEvent(game, { type: GAME_EVENT_TYPES.CARD_PLAYED, playerId: "p1", cardId: "ship" }, handlers);
  assert.deepEqual(order, [GAME_EVENT_TYPES.CARD_PLAYED, "EFFECT", GAME_EVENT_TYPES.AUTHORITY_GAINED]);
  assert.equal(game.eventHistory[0].chainId, game.eventHistory[1].chainId);
  assert.equal(game.eventHistory[1].parentEventId, game.eventHistory[0].id);
  assert.equal(game.eventHistory[1].depth, 1);
});

test("Authority and Shield recursion halts at the depth guard", () => {
  const game = eventGame();
  emitGameEvent(game, { type: GAME_EVENT_TYPES.AUTHORITY_GAINED, playerId: "p1", amount: 1 }, {
    resolveEvent(event, controls) {
      if (event.type === GAME_EVENT_TYPES.AUTHORITY_GAINED) controls.emit({ type: GAME_EVENT_TYPES.SHIELD_GAINED, playerId: "p1", amount: 1 });
      if (event.type === GAME_EVENT_TYPES.SHIELD_GAINED) controls.emit({ type: GAME_EVENT_TYPES.AUTHORITY_GAINED, playerId: "p1", amount: 1 });
    }
  }, { maxDepth: 5, maxSteps: 50 });

  assert.ok(game.eventHistory.length <= 6);
  assert.ok(game.eventWarnings.some(message => message.includes("exceeded depth")));
  assert.equal(game.resolutionQueue.length, 0);
});
