import test from "node:test";
import assert from "node:assert/strict";

import {
  createCampaignProfile,
  loadCampaignProfile,
  saveCampaignProfile
} from "../campaign/campaign-state.js";
import {
  advanceCampaignNode,
  campaignNodeAt,
  campaignRegionScaling,
  generateCampaignRegion
} from "../campaign/campaign-map.js";
import {
  applyCampaignReward,
  generateCampaignRewards
} from "../campaign/campaign-rewards.js";
import {
  advanceBossTurn,
  applyBossPhaseState,
  bossAbilityStatus,
  getCampaignBoss,
  pendingBossPhases
} from "../campaign/campaign-bosses.js";
import {
  currentCampaignEncounter,
  finishCampaignRewardNode,
  recordCampaignBattleResult
} from "../campaign/campaign.js";
import { evaluateCampaignUnlocks } from "../campaign/campaign-unlocks.js";
import { resolveCampaignEventChoice } from "../campaign/campaign-events.js";
import {
  buyCampaignShopCard,
  generateCampaignShop,
  removeCampaignDeckCard
} from "../campaign/campaign-shop.js";
import { loadCardPack } from "./test-utils.mjs";

function memoryStorage() {
  const values = new Map();
  return {
    getItem: key => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, String(value)),
    removeItem: key => values.delete(key)
  };
}

test("campaign starts with six Coins and four Blades and saves separately", () => {
  const profile = createCampaignProfile({ runId: "test_run", now: 100, random: 0 });
  assert.equal(profile.deck.length, 10);
  assert.equal(profile.deck.filter(cardId => cardId === "starter_coin").length, 6);
  assert.equal(profile.deck.filter(cardId => cardId === "starter_blade").length, 4);
  const storage = memoryStorage();
  assert.equal(saveCampaignProfile(profile, storage, 200), true);
  assert.deepEqual(loadCampaignProfile(storage), profile);
});

test("campaign map demonstrates battle/reward/elite/boss and scales without a maximum region", () => {
  const nodes = generateCampaignRegion(1, "test_run");
  assert.deepEqual(nodes.map(node => node.type), ["battle", "reward", "battle", "reward", "elite", "reward", "boss", "reward"]);
  const region200 = campaignRegionScaling(200);
  assert.equal(region200.region, 200);
  assert.ok(region200.enemyAuthorityMultiplier > campaignRegionScaling(20).enemyAuthorityMultiplier);
  assert.ok(region200.modifiers.length > 0);
  assert.ok(region200.modifiers.some(modifier => modifier.rank > 1));
});

test("victory advances into a deterministic three-card reward and then the next battle", async () => {
  const pack = await loadCardPack();
  const profile = createCampaignProfile({ runId: "reward_run", now: 100, random: 0 });
  const encounter = currentCampaignEncounter(profile);
  assert.equal(encounter.node.type, "battle");
  recordCampaignBattleResult(profile, { won: true, authorityRemaining: 37, now: 200 });
  assert.equal(campaignNodeAt(profile).type, "reward");
  const first = generateCampaignRewards(pack.CARDS, profile, campaignNodeAt(profile), { count: 3 });
  const second = generateCampaignRewards(pack.CARDS, profile, campaignNodeAt(profile), { count: 3 });
  assert.deepEqual(first, second);
  assert.equal(first.length, 3);
  const deckSize = profile.deck.length;
  applyCampaignReward(profile, first[0], { now: 300 });
  finishCampaignRewardNode(profile, { now: 301 });
  assert.equal(profile.deck.length, deckSize + 1);
  assert.equal(campaignNodeAt(profile).type, "battle");
});

test("World Eater has starting Bases, a two-turn ability, and a Phase II at 60 Authority", () => {
  const boss = getCampaignBoss("world_eater");
  assert.equal(boss.authority, 120);
  assert.deepEqual(boss.startingBases, ["worldheart_core", "rift_maw"]);
  let state = { turns: 0, triggeredPhases: [], frequencyReduction: 0 };
  let advanced = advanceBossTurn(boss, state);
  state = advanced.state;
  assert.equal(advanced.ability.due, false);
  advanced = advanceBossTurn(boss, state);
  state = advanced.state;
  assert.equal(advanced.ability.due, true);
  const phases = pendingBossPhases(boss, 60, state);
  assert.equal(phases[0].createBase, "rift_maw");
  state = applyBossPhaseState(state, phases[0]);
  assert.equal(bossAbilityStatus(boss, state).every, 1);
});

test("the first boss reward exposes the campaign-only evolving Frontier Fort", async () => {
  const pack = await loadCardPack();
  const profile = createCampaignProfile({ runId: "boss_reward_run", now: 100, random: 0 });
  profile.nodeIndex = 7;
  const rewards = generateCampaignRewards(pack.CARDS, profile, campaignNodeAt(profile), { count: 3 });
  assert.ok(rewards.some(reward => reward.cardId === "frontier_fort"));
});

test("campaign unlock rules are data-driven", async () => {
  const pack = await loadCardPack();
  const profile = createCampaignProfile({ runId: "unlock_run", now: 100, random: 0 });
  profile.stats.heatSpent = 25;
  const unlocked = evaluateCampaignUnlocks(profile, pack.CARDS);
  assert.ok(unlocked.some(result => result.ruleId === "heat_specialists"));
  assert.ok(profile.unlockedCards.some(cardId => pack.ALL_CARD_MAP[cardId]?.heat || pack.ALL_CARD_MAP[cardId]?.transform?.trigger === "heat"));
});

test("advancing past the boss reward generates the next region", () => {
  const profile = createCampaignProfile({ runId: "endless_run", now: 100, random: 0 });
  const length = generateCampaignRegion(1, profile.runId).length;
  for (let index = 0; index < length; index += 1) advanceCampaignNode(profile);
  assert.equal(profile.region, 2);
  assert.equal(profile.nodeIndex, 0);
});

test("campaign events and shops resolve from data without touching normal profile currency", async () => {
  const pack = await loadCardPack();
  const profile = createCampaignProfile({ runId: "event_shop_run", now: 100, random: 0 });
  profile.currency = 100;
  const event = resolveCampaignEventChoice(profile, "ruined_observatory", "dismantle", { now: 200 });
  assert.equal(event.ok, true);
  assert.equal(profile.currency, 140);
  const offers = generateCampaignShop(pack.CARDS, profile, { seed: 12 }, { count: 3 });
  assert.equal(offers.length, 3);
  const beforeDeck = profile.deck.length;
  const bought = buyCampaignShopCard(profile, { ...offers[0], price: 20 });
  assert.equal(bought.ok, true);
  assert.equal(profile.deck.length, beforeDeck + 1);
  const removed = removeCampaignDeckCard(profile, offers[0].cardId, 10);
  assert.equal(removed.ok, true);
  assert.equal(profile.deck.length, beforeDeck);
});
