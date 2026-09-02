import test from "node:test";
import assert from "node:assert/strict";

import {
  buildBattleReport,
  ensureBattleTelemetry,
  recordBattleEvent,
  recordBattleMetrics,
  resetBattleTelemetry
} from "../ui/battle-telemetry.js";

function game() {
  return {
    id: "battle_1",
    createdAtMs: 10,
    winnerId: "human",
    winnerName: "Varek",
    players: [
      { id: "human", name: "Varek", health: 24, maxAuthority: 60 },
      { id: "enemy", name: "Siegebreaker Host", health: 0, maxAuthority: 45 }
    ]
  };
}

test("battle telemetry tracks relevant performance and produces an MVP", () => {
  const state = game();
  ensureBattleTelemetry(state);
  recordBattleEvent(state, { type: "TURN_STARTED", actorId: "human" });
  recordBattleMetrics(state, "human", { tradeGenerated: 7, combatGenerated: 12, cardsDrawn: 2 }, { cardId: "war_drum_nest" });
  recordBattleEvent(state, { type: "TOKEN_CREATED", actorId: "human", sourceCardId: "war_drum_nest", amount: 2 });
  recordBattleEvent(state, { type: "BASE_DAMAGED", actorId: "human", playerId: "human", ownerId: "enemy", cardId: "enemy_base", damage: 6, amount: 4 });
  recordBattleMetrics(state, "human", { combatSpentOnBases: 8, combatSpentOnAuthority: 4 });
  const report = buildBattleReport(state, "human", {
    getCard: cardId => ({ name: cardId === "war_drum_nest" ? "War-Drum Nest" : cardId }),
    analysis: { metrics: { ally: 0, doubleAlly: 0 } }
  });
  assert.equal(report.won, true);
  assert.equal(report.turns, 1);
  assert.equal(report.metrics.tradeGenerated, 7);
  assert.equal(report.metrics.baseDamage, 6);
  assert.equal(report.mvp.name, "War-Drum Nest");
  assert.ok(report.observations.length >= 1 && report.observations.length <= 2);
});

test("telemetry resets between matches without leaking previous values", () => {
  const state = game();
  recordBattleMetrics(state, "human", { combatGenerated: 30, tokensCreated: 5 }, { cardId: "old_card" });
  resetBattleTelemetry(state);
  const report = buildBattleReport(state, "human");
  assert.equal(report.metrics.combatGenerated, 0);
  assert.equal(report.metrics.tokensCreated, 0);
  assert.equal(report.mvp, null);
});

test("authority damage credits the attacker but not self-damage", () => {
  const state = game();
  recordBattleEvent(state, { type: "AUTHORITY_LOST", actorId: "human", playerId: "enemy", amount: 9, sourceCardId: "siege_card" });
  recordBattleEvent(state, { type: "AUTHORITY_LOST", actorId: "human", playerId: "human", amount: 3, sourceCardId: "overload_card" });
  const report = buildBattleReport(state, "human", { getCard: id => ({ name: id }) });
  assert.equal(report.metrics.authorityDamage, 9);
});

test("reports preserve distinct victory and defeat states", () => {
  const victory = game();
  const victoryReport = buildBattleReport(victory, "human");
  assert.equal(victoryReport.won, true);
  assert.equal(victoryReport.authorityRemaining, 24);

  const defeat = game();
  defeat.winnerId = "enemy";
  defeat.winnerName = "Siegebreaker Host";
  defeat.players[0].health = 0;
  const defeatReport = buildBattleReport(defeat, "human");
  assert.equal(defeatReport.won, false);
  assert.equal(defeatReport.authorityRemaining, 0);
});
