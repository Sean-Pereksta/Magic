import test from "node:test";
import assert from "node:assert/strict";

import {
  simulateTestLabGame
} from "../ui/test-lab-simulator.js";
import {
  createStrategyAnalytics,
  recordStrategyGame,
  strategyCardRows,
  strategyRankingRows
} from "../ui/test-lab-strategy-analytics.js";

test("strategy analytics rank strategies by seat-level win rate", () => {
  const analytics = createStrategyAnalytics();
  recordStrategyGame(analytics, {
    winnerId: "a",
    totalTurns: 20,
    strategies: { a: "vanguard", b: "engine" },
    purchases: [
      { playerId: "a", cardId: "sanctum_core_imbuer", turn: 5 },
      { playerId: "b", cardId: "heavenlance_thermal_turret", turn: 6 }
    ]
  }, 0);
  recordStrategyGame(analytics, {
    winnerId: "a",
    totalTurns: 18,
    strategies: { a: "vanguard", b: "cycle" },
    purchases: [
      { playerId: "a", cardId: "sanctum_core_imbuer", turn: 4 },
      { playerId: "b", cardId: "heavenlance_thermal_turret", turn: 7 }
    ]
  }, 1);

  const rows = strategyRankingRows(analytics);
  assert.equal(rows[0].strategyId, "vanguard");
  assert.equal(rows[0].games, 2);
  assert.equal(rows[0].wins, 2);
  assert.equal(rows[0].winRate, 100);
});

test("strategy card profiles return most common purchases with timing and win association", () => {
  const analytics = createStrategyAnalytics();
  recordStrategyGame(analytics, {
    winnerId: "a",
    totalTurns: 22,
    strategies: { a: "siege", b: "engine" },
    purchases: [
      { playerId: "a", cardId: "sanctum_core_imbuer", turn: 5 },
      { playerId: "a", cardId: "sanctum_core_imbuer", turn: 8 },
      { playerId: "a", cardId: "heavenlance_thermal_turret", turn: 7 }
    ]
  }, 0);
  recordStrategyGame(analytics, {
    winnerId: "b",
    totalTurns: 24,
    strategies: { a: "siege", b: "engine" },
    purchases: [
      { playerId: "a", cardId: "sanctum_core_imbuer", turn: 9 }
    ]
  }, 1);

  const profiles = strategyCardRows(analytics, 50);
  assert.ok(Array.isArray(profiles.siege));
  assert.equal(profiles.siege[0].cardId, "sanctum_core_imbuer");
  assert.equal(profiles.siege[0].purchases, 3);
  assert.equal(profiles.siege[0].wins, 2);
  assert.equal(profiles.siege[0].losses, 1);
  assert.equal(profiles.siege[0].avgBuyTurn, 7.33);
});

test("bot-specific priority settings are preserved by the simulation", () => {
  const result = simulateTestLabGame({
    seed: 24680,
    strategyA: "vanguard",
    strategyB: "engine",
    difficultyA: "hard",
    difficultyB: "hard",
    priorityAEnabled: true,
    priorityCardsA: ["sanctum_core_imbuer", "heavenlance_thermal_turret"],
    priorityModeA: "force",
    priorityBEnabled: false,
    experimentalCardIds: ["sanctum_core_imbuer", "heavenlance_thermal_turret"],
    experimentalCopies: 4,
    maxTurns: 80
  }, 3);

  assert.equal(result.priorities.a.enabled, true);
  assert.equal(result.priorities.a.mode, "force");
  assert.deepEqual(result.priorities.a.cards, ["sanctum_core_imbuer", "heavenlance_thermal_turret"]);
  assert.equal(result.priorities.b.enabled, false);
});
