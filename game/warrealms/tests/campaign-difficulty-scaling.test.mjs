import test from "node:test";
import assert from "node:assert/strict";

import { campaignRegionScaling } from "../campaign/campaign-map.js";
import { currentCampaignEncounter } from "../campaign/campaign.js";

function encounterAt(region, pathId = "vanguard") {
  return currentCampaignEncounter({
    region,
    runId: `difficulty_${region}_${pathId}`,
    nodeIndex: 1,
    pathChoices: {
      [`region_${region}_node_1`]: pathId
    }
  });
}

test("campaign pressure begins scaling materially by regions four and five", () => {
  const region1 = campaignRegionScaling(1);
  const region4 = campaignRegionScaling(4);
  const region5 = campaignRegionScaling(5);
  const region9 = campaignRegionScaling(9);
  const region10 = campaignRegionScaling(10);

  assert.equal(region1.enemyBonusCombat, 0);
  assert.equal(region1.enemyHandSize, 5);
  assert.equal(region1.enemyAuthorityMultiplier, 1);

  assert.equal(region4.enemyBonusCombat, 1);
  assert.equal(region4.modifiers.length, 1);

  assert.equal(region5.enemyStartingBases, 1);
  assert.equal(region5.enemyBonusTrade, 1);
  assert.equal(region5.enemyHandSize, 6);
  assert.ok(region5.enemyAuthorityMultiplier > 1);

  assert.equal(region9.enemyHandSize, 7);
  assert.equal(region10.enemyBonusCombat, 3);
  assert.ok(region10.enemyBaseHealthMultiplier > region5.enemyBaseHealthMultiplier);
});

test("campaign AI difficulty tiers advance earlier with region progression", () => {
  assert.equal(encounterAt(1, "vanguard").difficulty, "easy");
  assert.equal(encounterAt(1, "rift_gambit").difficulty, "medium");
  assert.equal(encounterAt(3, "rift_gambit").difficulty, "hard");
  assert.equal(encounterAt(6, "siegebreaker").difficulty, "hard");
  assert.equal(encounterAt(10, "vanguard").difficulty, "hard");
  assert.equal(encounterAt(15, "siegebreaker").difficulty, "impossible");
  assert.equal(encounterAt(19, "vanguard").difficulty, "impossible");
  assert.ok(encounterAt(10, "vanguard").enemyAggression > encounterAt(1, "vanguard").enemyAggression);
});
