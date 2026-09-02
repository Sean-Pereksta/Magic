import test from "node:test";
import assert from "node:assert/strict";

import { analyzeDeck, simulateDeck } from "../ui/deck-intelligence.js";

test("Deck Intelligence counts systems, relationships, scorecards, and archetypes", () => {
  const cards = [
    { id: "green_attack", faction: "green", type: "ship", cost: 2, effect: { combat: 4, createToken: { id: "spawn", count: 1 } }, ally: { combat: 2 } },
    { id: "green_heat", faction: "green", type: "ship", cost: 3, effect: { combat: 2, addHeat: 1 }, heat: { max: 4, actions: [{ cost: 2, effect: { combat: 3 } }] }, transform: { trigger: "heat", required: 3, into: "greater" } },
    { id: "green_idol", faction: "green", type: "base", cost: 4, defense: 5, attachmentSlots: 2, effect: {}, triggers: [{ event: "TOKEN_SACRIFICED", effect: { heal: 2 } }] },
    { id: "red_sacrifice", faction: "red", type: "ship", cost: 2, sacrifice: { draw: 1 }, effect: { sacrificeRequired: 1, purge: 1 } },
    { id: "attachment", faction: "blue", type: "attachment", cost: 3, attachment: { defense: 2 }, effect: {} }
  ];
  const analysis = analyzeDeck(cards);
  assert.equal(analysis.averageBattleCost, 2.8);
  assert.equal(analysis.metrics.combat, 2);
  assert.equal(analysis.metrics.tokenProducers, 1);
  assert.equal(analysis.metrics.tokenPayoffs, 1);
  assert.equal(analysis.metrics.heatCards, 1);
  assert.equal(analysis.metrics.sacrificeCards, 1);
  assert.equal(analysis.metrics.attachments, 1);
  assert.equal(analysis.metrics.attachmentTargets, 1);
  assert.ok(analysis.scorecards.combat > 0);
  assert.ok(analysis.synergies.some(note => note.includes("Tokens")));
  assert.ok(analysis.archetypes.includes("Gorak Siege"));
});

test("deck simulation is deterministic and estimates a six-coin/four-blade opening", () => {
  const coins = Array.from({ length: 6 }, (_, index) => ({ id: `coin_${index}`, faction: "neutral", type: "ship", cost: 0, effect: { trade: 1 } }));
  const blades = Array.from({ length: 4 }, (_, index) => ({ id: `blade_${index}`, faction: "neutral", type: "ship", cost: 0, effect: { combat: 1 } }));
  const first = simulateDeck([...coins, ...blades], { iterations: 500, seed: 42 });
  const second = simulateDeck([...coins, ...blades], { iterations: 500, seed: 42 });
  assert.deepEqual(first, second);
  assert.ok(first.averageOpeningTrade > 2.7 && first.averageOpeningTrade < 3.3);
  assert.ok(first.averageOpeningCombat > 1.7 && first.averageOpeningCombat < 2.3);
  assert.equal(first.estimatedBaseDensity, 0);
});

test("Deck Intelligence handles empty, faction, Token, Heat, Base, Sacrifice, and Ally deck shapes", () => {
  const empty = analyzeDeck([]);
  assert.equal(empty.deckSize, 0);
  assert.deepEqual(empty.factions, {});
  assert.ok(empty.warnings.length);

  const cards = [
    { id: "token_one", faction: "green", type: "ship", cost: 2, effect: { createToken: { id: "spawn", count: 2 }, combat: 2 }, ally: { combat: 1 } },
    { id: "token_payoff", faction: "green", type: "ship", cost: 3, triggers: [{ event: "TOKEN_CREATED", effect: { combat: 1 } }], doubleAlly: { draw: 1 } },
    { id: "heat_one", faction: "red", type: "ship", cost: 3, heat: { max: 3, actions: [{ cost: 2, effect: { combat: 4 } }] }, effect: { addHeat: 1 } },
    { id: "sacrifice_one", faction: "red", type: "ship", cost: 2, sacrifice: { draw: 1 }, effect: { sacrificeRequired: 1 } },
    { id: "sacrifice_payoff", faction: "red", type: "base", cost: 4, defense: 5, triggers: [{ event: "CARD_SACRIFICED", effect: { heal: 1 } }] },
    { id: "construction_base", faction: "blue", type: "base", cost: 4, health: 6, construction: 2, expansion: true, attachmentSlots: 2 },
    { id: "base_support", faction: "blue", type: "attachment", cost: 2, effect: { repair: { amount: 2 } } },
    { id: "yellow_trade", faction: "yellow", type: "ship", cost: 2, effect: { trade: 2 } }
  ];
  const analysis = analyzeDeck(cards);
  assert.equal(Object.keys(analysis.factions).filter(faction => faction !== "neutral").length, 4);
  assert.equal(analysis.metrics.tokenProducers, 1);
  assert.equal(analysis.metrics.tokenPayoffs, 1);
  assert.equal(analysis.metrics.heatCards, 1);
  assert.equal(analysis.metrics.heatConsumers, 1);
  assert.equal(analysis.metrics.sacrificeCards, 1);
  assert.equal(analysis.metrics.sacrificePayoffs, 1);
  assert.equal(analysis.metrics.constructionBases, 1);
  assert.equal(analysis.metrics.attachments, 1);
  assert.equal(analysis.metrics.ally, 1);
  assert.equal(analysis.metrics.doubleAlly, 1);
  assert.ok(analysis.strengths.length);
  assert.ok(analysis.warnings.length);
});
