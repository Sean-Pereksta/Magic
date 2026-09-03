import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

import { TEST_LAB_STRATEGIES, testLabStrategyFit } from "../ui/test-lab-simulator.js";

const here = dirname(fileURLToPath(import.meta.url));
const newIds = ["summoning", "ascendents", "bastion", "fleet"];

test("new strategy archetypes are registered and mechanically specialized", () => {
  const ids = TEST_LAB_STRATEGIES.map(strategy => strategy.id);
  newIds.forEach(id => assert.ok(ids.includes(id), `${id} should be available in Test Lab`));

  const tokenCard = { faction: "green", type: "ship", cost: 4, effect: { createToken: { id: "spawn", count: 2 } } };
  const transformCard = { faction: "red", type: "ship", cost: 5, transform: { trigger: "turns", required: 2, into: "greater_form" } };
  const bastionCard = { faction: "blue", type: "base", cost: 5, health: 8, effect: { shield: 4, repair: { amount: 3 } } };
  const fleetCard = { faction: "blue", type: "ship", cost: 5, effect: { createToken: { id: "drone", count: 2 } } };

  assert.ok(testLabStrategyFit(tokenCard, "summoning") > testLabStrategyFit(tokenCard, "marketeer"));
  assert.ok(testLabStrategyFit(transformCard, "ascendents") > testLabStrategyFit(transformCard, "vanguard"));
  assert.ok(testLabStrategyFit(bastionCard, "bastion") > testLabStrategyFit(bastionCard, "cycle"));
  assert.ok(testLabStrategyFit(fleetCard, "fleet") > testLabStrategyFit(fleetCard, "summoning"));
});

test("Quick Play and the native Warbot keep strategy indices in lockstep", () => {
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
  newIds.forEach(id => assert.ok(nativeOrder.includes(id), `${id} should exist in native Warbot strategies`));
});
