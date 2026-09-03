import test from "node:test";
import assert from "node:assert/strict";

import {
  createTestLabAccumulator,
  getTestLabCards,
  recordTestLabGame,
  simulateTestLabGame,
  testLabCardRows,
  testLabSummary
} from "../ui/test-lab-simulator.js";

test("Test Lab exposes the live collectible card pool", () => {
  const cards = getTestLabCards();
  assert.ok(cards.length > 20);
  assert.ok(cards.every(card => card.id && card.name));
  assert.ok(cards.some(card => card.faction === "blue"));
  assert.ok(cards.some(card => card.faction === "green"));
});

test("headless matches are deterministic for the same seed and game index", () => {
  const options = {
    seed: 123456,
    strategyA: "vanguard",
    strategyB: "engine",
    difficultyA: "hard",
    difficultyB: "hard",
    maxTurns: 80
  };
  const first = simulateTestLabGame(options, 7);
  const second = simulateTestLabGame(options, 7);
  assert.deepEqual(second, first);
  assert.ok(first.totalTurns > 0);
  assert.ok(["a", "b", ""].includes(first.winnerId));
});

test("card telemetry separates winning and losing purchase timing", () => {
  const accumulator = createTestLabAccumulator();
  recordTestLabGame(accumulator, {
    winnerId: "a",
    totalTurns: 18,
    strategies: { a: "vanguard", b: "engine" },
    purchases: [
      { playerId: "a", cardId: "sanctum_core_imbuer", turn: 4 },
      { playerId: "b", cardId: "sanctum_core_imbuer", turn: 7 }
    ]
  }, 0);
  recordTestLabGame(accumulator, {
    winnerId: "b",
    totalTurns: 22,
    strategies: { a: "vanguard", b: "engine" },
    purchases: [
      { playerId: "b", cardId: "sanctum_core_imbuer", turn: 5 },
      { playerId: "a", cardId: "sanctum_core_imbuer", turn: 9 }
    ]
  }, 1);

  const row = testLabCardRows(accumulator).find(entry => entry.cardId === "sanctum_core_imbuer");
  assert.ok(row);
  assert.equal(row.purchases, 4);
  assert.equal(row.wins, 2);
  assert.equal(row.losses, 2);
  assert.equal(row.winRate, 50);
  assert.equal(row.avgBuyTurn, 6.25);
  assert.equal(row.avgWinningBuyTurn, 4.5);
  assert.equal(row.avgLosingBuyTurn, 8);
});

test("multi-game simulations produce purchases and aggregate win totals", () => {
  const accumulator = createTestLabAccumulator();
  for (let index = 0; index < 30; index += 1) {
    const result = simulateTestLabGame({
      seed: 7654321,
      strategyA: "random",
      strategyB: "random",
      difficultyA: "impossible",
      difficultyB: "impossible",
      experimentalCardIds: ["heavenlance_thermal_turret"],
      experimentalCopies: 3,
      maxTurns: 80
    }, index);
    recordTestLabGame(accumulator, result, index);
  }
  const summary = testLabSummary(accumulator);
  const rows = testLabCardRows(accumulator);
  assert.equal(summary.games, 30);
  assert.equal(summary.winsA + summary.winsB + summary.draws, 30);
  assert.ok(rows.length > 0);
  assert.ok(rows.some(row => row.purchases > 0));
});
