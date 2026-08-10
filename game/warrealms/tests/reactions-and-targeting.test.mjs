import test from "node:test";
import assert from "node:assert/strict";

import { GAME_EVENT_TYPES } from "../engine/event-system.js";
import { matchingReactions } from "../engine/reactions.js";
import { resolveOpponentTargets } from "../engine/targeting.js";
import { eventGame } from "./test-utils.mjs";

test("automatic pre-destruction Reaction matches once per turn", () => {
  const reactionCard = {
    id: "reactive_base",
    reaction: {
      event: GAME_EVENT_TYPES.BASE_WOULD_BE_DESTROYED,
      oncePerTurn: true,
      automatic: true,
      effect: { armor: 3 }
    }
  };
  const entry = { id: reactionCard.id, instanceId: "reactive_1", reactionUsage: {} };
  const source = { owner: { id: "p1" }, entry, zone: "bases" };
  const game = eventGame();
  const event = { type: GAME_EVENT_TYPES.BASE_WOULD_BE_DESTROYED, id: "event_1", ownerId: "p1", instanceId: "reactive_1" };
  const first = matchingReactions(game, event, [source], () => reactionCard, () => true);
  const second = matchingReactions(game, { ...event, id: "event_2" }, [source], () => reactionCard, () => true);
  assert.equal(first.length, 1);
  assert.equal(second.length, 0);
  game.turnSerial += 1;
  const third = matchingReactions(game, { ...event, id: "event_3" }, [source], () => reactionCard, () => true);
  assert.equal(third.length, 1);
});

test("opponent target rules preserve next-player behavior and support explicit choice", () => {
  const game = {
    players: [
      { id: "p1", health: 50, hand: [], bases: [] },
      { id: "p2", health: 15, hand: [{}, {}, {}], bases: [{}] },
      { id: "p3", health: 80, hand: [{}], bases: [{}, {}, {}] }
    ]
  };
  assert.deepEqual(resolveOpponentTargets(game, "p1").map(player => player.id), ["p2"]);
  assert.deepEqual(resolveOpponentTargets(game, "p1", "chooseOpponent", { targetId: "p3" }).map(player => player.id), ["p3"]);
  assert.deepEqual(resolveOpponentTargets(game, "p1", "allOpponents").map(player => player.id), ["p2", "p3"]);
  assert.deepEqual(resolveOpponentTargets(game, "p1", "lowestAuthorityOpponent").map(player => player.id), ["p2"]);
  assert.deepEqual(resolveOpponentTargets(game, "p1", "mostBasesOpponent").map(player => player.id), ["p3"]);
  assert.deepEqual(resolveOpponentTargets(game, "p1", "chooseOpponent", { isBot: true, purpose: "discard" }).map(player => player.id), ["p2"]);
});
