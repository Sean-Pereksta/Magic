import assert from "node:assert/strict";
import test from "node:test";

import {
  TEST_LAB_DECK_PRESETS,
  getTestLabDeckPreset,
  testLabDeckPresetSummary
} from "../ui/test-lab-deck-presets.js";
import {
  normalizeTestLabCommandDeck
} from "../ui/test-lab-simulator.js";
import {
  simulateExtendedTestLabGame
} from "../ui/test-lab-strategy-extension.js";

test("Test Lab exposes legal fixed 50-card preset decks for every strategy family", () => {
  assert.ok(TEST_LAB_DECK_PRESETS.length >= 18);
  const values = new Set();

  for (const preset of TEST_LAB_DECK_PRESETS) {
    assert.ok(preset.value.startsWith("deck:"));
    assert.equal(values.has(preset.value), false, `duplicate preset value ${preset.value}`);
    values.add(preset.value);

    const summary = testLabDeckPresetSummary(preset);
    assert.equal(summary.cards, 50, `${preset.name} should contain 50 cards`);
    assert.equal(summary.lowCostCards, 25, `${preset.name} should contain 25 cards costing 1-3`);
    assert.equal(summary.fourCostCards, 15, `${preset.name} should contain 15 cards costing 4`);
    assert.equal(summary.highCostCards, 10, `${preset.name} should contain 10 cards costing 5+`);
    assert.ok(summary.maxCopies <= 4, `${preset.name} exceeds the four-copy limit`);
    assert.deepEqual(normalizeTestLabCommandDeck(preset.cardIds), preset.cardIds);
  }
});

test("preset selection contributes the exact selected command decks to the Test Lab market", () => {
  const vanguard = getTestLabDeckPreset("deck:vanguard");
  const engine = getTestLabDeckPreset("deck:engine");
  assert.ok(vanguard);
  assert.ok(engine);

  const result = simulateExtendedTestLabGame({
    strategyA: vanguard.value,
    strategyB: engine.value,
    difficultyA: "impossible",
    difficultyB: "impossible",
    maxTurns: 120,
    seed: 9142026
  }, 0);

  assert.equal(result.strategies.a, "vanguard");
  assert.equal(result.strategies.b, "engine");
  assert.deepEqual(result.deckPresets, { a: "vanguard", b: "engine" });
  assert.deepEqual(result.commandDecks.a, vanguard.cardIds);
  assert.deepEqual(result.commandDecks.b, engine.cardIds);

  const allowedMarketCards = new Set([...vanguard.cardIds, ...engine.cardIds]);
  assert.ok(result.purchases.length > 0, "the simulated match should make purchases");
  assert.ok(result.purchases.every(purchase => allowedMarketCards.has(purchase.cardId)), "preset match purchased a card outside the selected decks");
});

test("Legion offers separate faction-pure Gorak and Umbral deck presets", () => {
  const gorak = getTestLabDeckPreset("deck:legion-gorak");
  const umbral = getTestLabDeckPreset("deck:legion-umbral");
  assert.ok(gorak);
  assert.ok(umbral);
  assert.equal(gorak.strategyId, "legion");
  assert.equal(umbral.strategyId, "legion");
  assert.notDeepEqual(gorak.cardIds, umbral.cardIds);
});
