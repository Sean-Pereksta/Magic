import test from "node:test";
import assert from "node:assert/strict";

import { GAME_EVENT_TYPES } from "../engine/event-system.js";
import { matchingPersistentTriggers } from "../engine/trigger-engine.js";
import { eventGame, loadCardPack } from "./test-utils.mjs";

function matchFor(game, event, source, getCard) {
  const claims = new Set();
  return matchingPersistentTriggers(game, event, [source], getCard, key => {
    if (claims.has(key)) return false;
    claims.add(key);
    return true;
  });
}

test("Broodheart Idol responds to a sacrificed Spawn from the real card pack", async () => {
  const pack = await loadCardPack();
  const card = pack.ALL_CARD_MAP.broodheart_idol;
  assert.equal(card.text, "Whenever you sacrifice a Spawn, gain 2 Authority.");
  const owner = { id: "p1" };
  const source = { owner, entry: { id: card.id, instanceId: "idol_1", triggerUsage: {} }, zone: "bases" };
  const game = eventGame();
  const getCard = entry => pack.ALL_CARD_MAP[entry.id];
  const event = {
    type: GAME_EVENT_TYPES.CARD_SACRIFICED,
    id: "event_1",
    playerId: "p1",
    ownerId: "p1",
    cardId: "spawn",
    instanceId: "spawn_1",
    tags: ["spawn"],
    token: true,
    amount: 1
  };

  const matches = matchFor(game, event, source, getCard);
  assert.equal(matches.length, 1);
  assert.deepEqual(matches[0].trigger.effect, { heal: 2 });
});

test("Broodheart Idol distinguishes sacrifice from discard, purge, and destruction", async () => {
  const pack = await loadCardPack();
  const source = {
    owner: { id: "p1" },
    entry: { id: "broodheart_idol", instanceId: "idol_1", triggerUsage: {} },
    zone: "bases"
  };
  const getCard = entry => pack.ALL_CARD_MAP[entry.id];
  for (const type of [GAME_EVENT_TYPES.CARD_DISCARDED, GAME_EVENT_TYPES.CARD_PURGED, GAME_EVENT_TYPES.CARD_DESTROYED]) {
    const matches = matchFor(eventGame(), {
      type,
      id: `event_${type}`,
      playerId: "p1",
      ownerId: "p1",
      cardId: "spawn",
      instanceId: "spawn_1",
      tags: ["spawn"],
      token: true,
      amount: 1
    }, source, getCard);
    assert.equal(matches.length, 0, `${type} must not count as sacrifice`);
  }
});
