import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

import { TEST_LAB_STRATEGIES, testLabStrategyFit } from "../ui/test-lab-simulator.js";
import {
  EXPANDED_BOT_STRATEGY_IDS,
  NATIVE_STRATEGY_ORDER,
  extendNativeBotStrategyRegistry,
  expandedStrategyFit
} from "../ui/bot-strategy-expansion.js";
import {
  EXTENDED_TEST_LAB_STRATEGIES,
  testLabExtendedStrategyFit
} from "../ui/test-lab-strategy-extension.js";

const here = dirname(fileURLToPath(import.meta.url));
const firstWaveIds = ["summoning", "ascendents", "bastion", "fleet"];
const secondWaveIds = ["reactor", "sacrifice", "architect", "overcharger", "legion", "arsenal"];

test("first-wave strategy archetypes remain registered and mechanically specialized", () => {
  const ids = TEST_LAB_STRATEGIES.map(strategy => strategy.id);
  firstWaveIds.forEach(id => assert.ok(ids.includes(id), `${id} should remain available in Test Lab`));

  const tokenCard = { faction: "green", type: "ship", cost: 4, effect: { createToken: { id: "spawn", count: 2 } } };
  const transformCard = { faction: "red", type: "ship", cost: 5, transform: { trigger: "turns", required: 2, into: "greater_form" } };
  const bastionCard = { faction: "blue", type: "base", cost: 5, health: 8, effect: { shield: 4, repair: { amount: 3 } } };
  const fleetCard = { faction: "blue", type: "ship", cost: 5, effect: { createToken: { id: "drone", count: 2 } } };

  assert.ok(testLabStrategyFit(tokenCard, "summoning") > testLabStrategyFit(tokenCard, "marketeer"));
  assert.ok(testLabStrategyFit(transformCard, "ascendents") > testLabStrategyFit(transformCard, "vanguard"));
  assert.ok(testLabStrategyFit(bastionCard, "bastion") > testLabStrategyFit(bastionCard, "cycle"));
  assert.ok(testLabStrategyFit(fleetCard, "fleet") > testLabStrategyFit(fleetCard, "summoning"));
});

test("six mechanic-focused strategies are registered in the extended Test Lab", () => {
  const ids = EXTENDED_TEST_LAB_STRATEGIES.map(strategy => strategy.id);
  secondWaveIds.forEach(id => assert.ok(ids.includes(id), `${id} should be available in the extended Test Lab`));
  assert.deepEqual(EXPANDED_BOT_STRATEGY_IDS, secondWaveIds);

  const reactorCard = { faction: "blue", type: "ship", cost: 4, heat: { max: 6, actions: [{ cost: 3, effect: { combat: 8 } }] }, effect: { addHeat: 2, coolHeat: 1 } };
  const sacrificeCard = { faction: "red", type: "ship", cost: 4, effect: { createToken: { id: "spawn", count: 1 } }, sacrifice: { combat: 6 }, tokenSacrificeTrigger: { combat: 3 } };
  const architectCard = { faction: "blue", type: "base", cost: 7, expansion: true, construction: 4, effect: { advanceConstruction: { amount: 2 }, repair: { amount: 3 } } };
  const chargeCard = { faction: "blue", type: "ship", cost: 5, effect: { gainCharge: 2 }, charge: { trigger: "cardPlayed", actions: [{ cost: 2, effect: { draw: 2 } }] } };
  const legionCard = { faction: "green", type: "ship", cost: 4, effect: { combat: 2 }, ally: { combat: 3 }, doubleAlly: { draw: 2 } };
  const arsenalCard = { faction: "blue", type: "attachment", cost: 4, effect: { armor: { amount: 2 }, repair: { amount: 1 } } };

  assert.ok(testLabExtendedStrategyFit(reactorCard, "reactor") > testLabExtendedStrategyFit(reactorCard, "legion"));
  assert.ok(testLabExtendedStrategyFit(sacrificeCard, "sacrifice") > testLabExtendedStrategyFit(sacrificeCard, "reactor"));
  assert.ok(testLabExtendedStrategyFit(architectCard, "architect") > testLabExtendedStrategyFit(architectCard, "arsenal"));
  assert.ok(testLabExtendedStrategyFit(chargeCard, "overcharger") > testLabExtendedStrategyFit(chargeCard, "legion"));
  assert.ok(testLabExtendedStrategyFit(legionCard, "legion") > testLabExtendedStrategyFit(legionCard, "architect"));
  assert.ok(testLabExtendedStrategyFit(arsenalCard, "arsenal") > testLabExtendedStrategyFit(arsenalCard, "reactor"));
  assert.ok(expandedStrategyFit(reactorCard, "reactor") > 5);
});

test("Quick Play and the native Warbot keep the original strategy indices in lockstep", () => {
  const launcher = readFileSync(resolve(here, "../ui/play-launcher-core.js"), "utf8");
  const nativeGame = readFileSync(resolve(here, "../../warrealms.html"), "utf8");

  const launcherStart = launcher.indexOf("const BOT_STRATEGIES = Object.freeze([");
  const launcherEnd = launcher.indexOf("\n]);", launcherStart);
  const nativeStart = nativeGame.indexOf("  const BOT_STRATEGIES = Object.freeze({");
  const nativeEnd = nativeGame.indexOf("\n  });", nativeStart);
  assert.ok(launcherStart >= 0 && launcherEnd > launcherStart, "Quick Play strategy block should be present");
  assert.ok(nativeStart >= 0 && nativeEnd > nativeStart, "Native Warbot strategy block should be present");

  const launcherBlock = launcher.slice(launcherStart, launcherEnd);
  const nativeBlock = nativeGame.slice(nativeStart, nativeEnd);
  const launcherOrder = [...launcherBlock.matchAll(/id: "([a-z_]+)", name:/g)].map(match => match[1]);
  const nativeOrder = [...nativeBlock.matchAll(/^    ([a-z_]+): Object\.freeze\(\{/gm)].map(match => match[1]);

  assert.deepEqual(nativeOrder, launcherOrder, "Quick Play forcing depends on identical native/menu strategy order");
  firstWaveIds.forEach(id => assert.ok(nativeOrder.includes(id), `${id} should exist in native Warbot strategies`));
  assert.deepEqual(NATIVE_STRATEGY_ORDER, [...nativeOrder, ...secondWaveIds]);
});

test("native strategy extension appends all six new strategies without replacing existing definitions", () => {
  const base = Object.freeze(Object.fromEntries(NATIVE_STRATEGY_ORDER.slice(0, -secondWaveIds.length).map(id => [id, { name: id }])));
  const extended = extendNativeBotStrategyRegistry(base);
  assert.deepEqual(Object.keys(extended), NATIVE_STRATEGY_ORDER);
  secondWaveIds.forEach(id => assert.ok(extended[id], `${id} should be appended to the native registry`));
  assert.equal(extended.vanguard, base.vanguard);

  const wrapper = readFileSync(resolve(here, "../ui/play-launcher.js"), "utf8");
  assert.match(wrapper, /installNativeBotStrategyBridge\(\)/, "the launcher should install the native registry bridge before battle setup");
  assert.match(wrapper, /NATIVE_STRATEGY_ORDER\.length/, "Quick Play forcing should use the full expanded strategy count");
});
