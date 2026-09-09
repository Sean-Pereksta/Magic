import test from "node:test";
import assert from "node:assert/strict";
import { GAME_EVENT_TYPES, emitGameEvent } from "../engine/event-system.js";
import { startOfTurnHeatGain } from "../engine/heat-passives.js";

function heavenlanceGame(heat = 0) {
  return {
    turnSerial: 1,
    round: 1,
    players: [
      {
        id: "player_1",
        name: "Azure Tester",
        combat: 0,
        bases: [
          {
            id: "heavenlance_thermal_turret",
            instanceId: "heavenlance_1",
            heat,
            stunTurns: 0,
            constructionRemaining: 0
          }
        ]
      }
    ]
  };
}

function resolveQueuedCombat(game) {
  return {
    resolveItem(item) {
      if (item?.kind !== "EFFECT") return;
      const player = game.players.find(candidate => candidate.id === item.playerId);
      if (player) player.combat += Math.max(0, Number(item.effect?.combat) || 0);
    }
  };
}

function beginOwnerTurn(game) {
  return emitGameEvent(game, {
    type: GAME_EVENT_TYPES.TURN_STARTED,
    actorId: "player_1",
    playerId: "player_1",
    ownerId: "player_1",
    amount: 1,
    method: "turn"
  }, resolveQueuedCombat(game));
}

test("Heavenlance gains 1 Heat at the start of its owner's turn", () => {
  assert.equal(startOfTurnHeatGain("heavenlance_thermal_turret"), 1);

  const game = heavenlanceGame(2);
  beginOwnerTurn(game);

  assert.equal(game.players[0].bases[0].heat, 3);
  assert.equal(game.players[0].combat, 0);
  assert.ok(game.eventHistory.some(event =>
    event.type === GAME_EVENT_TYPES.HEAT_GAINED
    && event.cardId === "heavenlance_thermal_turret"
    && event.amount === 1
    && event.method === "start-of-turn"
  ));
});

test("Heavenlance overloads automatically when its turn-start Heat reaches 6", () => {
  const game = heavenlanceGame(5);
  beginOwnerTurn(game);

  assert.equal(game.players[0].bases[0].heat, 0);
  assert.equal(game.players[0].combat, 15);
  assert.ok(game.eventHistory.some(event =>
    event.type === GAME_EVENT_TYPES.HEAT_OVERLOADED
    && event.cardId === "heavenlance_thermal_turret"
    && event.amount === 6
    && event.method === "start-of-turn-overload"
  ));
  assert.ok(game.eventHistory.some(event =>
    event.type === GAME_EVENT_TYPES.HEAT_SPENT
    && event.cardId === "heavenlance_thermal_turret"
    && event.amount === 6
    && event.method === "start-of-turn-overload-reset"
  ));
});

test("a stunned Heavenlance does not advance passive Heat", () => {
  const game = heavenlanceGame(3);
  game.players[0].bases[0].stunTurns = 1;

  beginOwnerTurn(game);

  assert.equal(game.players[0].bases[0].heat, 3);
  assert.equal(game.players[0].combat, 0);
});
