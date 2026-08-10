import test from "node:test";
import assert from "node:assert/strict";

import {
  createCampaignProfile,
  loadCampaignProfile,
  saveCampaignProfile,
  skipCampaignStarterRemoval,
  useCampaignStarterRemoval
} from "../campaign/campaign-state.js";
import {
  advanceCampaignNode,
  campaignNodeAt,
  campaignPathOptions,
  campaignRegionScaling,
  chooseCampaignPath,
  generateCampaignRegion
} from "../campaign/campaign-map.js";
import {
  applyCampaignReward,
  generateCampaignRewards,
  rerollCampaignRewardOptions
} from "../campaign/campaign-rewards.js";
import {
  BEGINNER_CAMPAIGN_BOSSES,
  CAMPAIGN_BOSSES,
  EXISTING_CAMPAIGN_BOSSES,
  advanceBossTurn,
  applyBossPhaseState,
  bossAbilityStatus,
  campaignBossForRegion,
  getCampaignBoss,
  pendingBossPhases
} from "../campaign/campaign-bosses.js";
import {
  campaignEnemyStartingBaseCount,
  currentCampaignEncounter,
  finishCampaignRewardNode,
  recordCampaignBattleResult
} from "../campaign/campaign.js";
import { evaluateCampaignUnlocks } from "../campaign/campaign-unlocks.js";
import { resolveCampaignEventChoice } from "../campaign/campaign-events.js";
import {
  buyCampaignWarCampOption,
  buyCampaignShopCard,
  campaignWarCampOptions,
  finishCampaignWarCamp,
  generateCampaignShop,
  removeCampaignDeckCard
} from "../campaign/campaign-shop.js";
import {
  campaignRestChoices,
  resolveCampaignRestChoice
} from "../campaign/campaign-rest.js";
import { loadCardPack } from "./test-utils.mjs";

function memoryStorage() {
  const values = new Map();
  return {
    getItem: key => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, String(value)),
    removeItem: key => values.delete(key)
  };
}

test("campaign specialization replaces exactly one starter and saves new progression fields", () => {
  const profile = createCampaignProfile({ runId: "test_run", now: 100, random: 0, specialization: "warrior" });
  assert.equal(profile.deck.length, 10);
  assert.equal(profile.deck.filter(cardId => cardId === "starter_coin").length, 6);
  assert.equal(profile.deck.filter(cardId => cardId === "starter_blade").length, 3);
  assert.equal(profile.deck.filter(cardId => cardId === "knucklebone_rusher").length, 1);
  assert.equal(profile.specialization, "warrior");
  assert.equal(profile.nextBattleShield, 0);
  assert.deepEqual(profile.pathChoices, {});
  const storage = memoryStorage();
  assert.equal(saveCampaignProfile(profile, storage, 200), true);
  assert.deepEqual(loadCampaignProfile(storage), profile);
});

test("each region includes two routes, a War Camp, an elite, Rest, and a rotating boss", () => {
  const nodes = generateCampaignRegion(1, "test_run");
  assert.deepEqual(nodes.map(node => node.type), ["path", "battle", "reward", "path", "battle", "reward", "shop", "elite", "reward", "rest", "boss", "reward"]);
  assert.equal(nodes[10].bossId, CAMPAIGN_BOSSES[0].id);
  assert.equal(campaignRegionScaling(1).baseEnemyAuthority, 30);
  assert.equal(campaignRegionScaling(6).baseEnemyAuthority, 55);
  assert.equal(campaignRegionScaling(1).enemyBonusCombat, 0);
  const region200 = campaignRegionScaling(200);
  assert.equal(region200.region, 200);
  assert.ok(region200.enemyAuthorityMultiplier > campaignRegionScaling(20).enemyAuthorityMultiplier);
  assert.ok(region200.modifiers.some(modifier => modifier.rank > 1));
});

test("path nodes offer three materially different risks and persist the selected branch", () => {
  const profile = createCampaignProfile({ runId: "path_run", now: 100, random: 0 });
  const paths = campaignPathOptions(profile);
  assert.deepEqual(paths.map(path => path.id), ["vanguard", "siegebreaker", "rift_gambit"]);
  const selected = chooseCampaignPath(profile, "rift_gambit", { now: 150 });
  assert.equal(selected.ok, true);
  assert.equal(campaignNodeAt(profile).type, "elite");
  assert.equal(campaignNodeAt(profile).pathId, "rift_gambit");
  const encounter = currentCampaignEncounter(profile);
  assert.ok(encounter.enemyAuthority >= 35 && encounter.enemyAuthority <= 40);
  assert.equal(encounter.difficulty, "medium");
  assert.equal(encounter.enemyBonusCombat, 1);
  assert.equal(campaignEnemyStartingBaseCount(encounter), 1);
  assert.equal(encounter.currencyBonus, 35);
});

test("victory advances into a deterministic choice among three different Trade Deck cards", async () => {
  const pack = await loadCardPack();
  const profile = createCampaignProfile({ runId: "reward_run", now: 100, random: 0 });
  chooseCampaignPath(profile, "siegebreaker", { now: 150 });
  const encounter = currentCampaignEncounter(profile);
  assert.equal(encounter.node.type, "elite");
  recordCampaignBattleResult(profile, { won: true, authorityRemaining: 37, now: 200 });
  assert.equal(campaignNodeAt(profile).type, "reward");
  const first = generateCampaignRewards(pack.CARDS, profile, campaignNodeAt(profile), { count: 3 });
  const second = generateCampaignRewards(pack.CARDS, profile, campaignNodeAt(profile), { count: 3 });
  assert.deepEqual(first, second);
  assert.equal(first.length, 3);
  assert.equal(new Set(first.map(reward => reward.cardId)).size, 3);
  assert.ok(first.every(reward => pack.ALL_CARD_MAP[reward.cardId].cost >= 4 && pack.ALL_CARD_MAP[reward.cardId].cost <= 5));
  const deckSize = profile.deck.length;
  applyCampaignReward(profile, first[0], { now: 300 });
  finishCampaignRewardNode(profile, { now: 301 });
  assert.equal(profile.deck.length, deckSize + 1);
  assert.equal(campaignNodeAt(profile).type, "path");
});

test("five beginner bosses lead into all twenty-one existing bosses before endless repeats", () => {
  assert.equal(BEGINNER_CAMPAIGN_BOSSES.length, 5);
  assert.equal(EXISTING_CAMPAIGN_BOSSES.length, 21);
  assert.equal(CAMPAIGN_BOSSES.length, 26);
  assert.equal(new Set(CAMPAIGN_BOSSES.map(boss => boss.id)).size, 26);
  assert.deepEqual(BEGINNER_CAMPAIGN_BOSSES.map(boss => boss.authority), [30, 35, 40, 45, 55]);
  assert.ok(BEGINNER_CAMPAIGN_BOSSES.every(boss => boss.difficulty === "easy" && boss.startingBases.length <= 1));
  assert.equal(campaignBossForRegion(1).id, BEGINNER_CAMPAIGN_BOSSES[0].id);
  assert.equal(campaignBossForRegion(5).id, BEGINNER_CAMPAIGN_BOSSES[4].id);
  assert.equal(campaignBossForRegion(6).id, EXISTING_CAMPAIGN_BOSSES[0].id);
  assert.equal(campaignBossForRegion(26).id, "world_eater");
  assert.equal(campaignBossForRegion(27).id, BEGINNER_CAMPAIGN_BOSSES[0].id);
  for (const boss of EXISTING_CAMPAIGN_BOSSES) {
    assert.ok(boss.startingDeck.length >= 10);
    assert.ok(boss.startingDeck.filter(cardId => cardId === "starter_coin").length <= 4);
    assert.ok(boss.economy.tradeLimit >= 1 && boss.economy.tradeLimit <= 6);
  }
});

test("World Eater has a fixed attack deck, limited Trade, starting Bases, and a Phase II", () => {
  const boss = getCampaignBoss("world_eater");
  assert.equal(boss.authority, 190);
  assert.deepEqual(boss.startingBases, ["worldheart_core", "rift_maw"]);
  assert.equal(boss.startingDeck.length, 10);
  assert.equal(boss.economy.tradeLimit, 4);
  let state = { turns: 0, triggeredPhases: [], frequencyReduction: 0 };
  let advanced = advanceBossTurn(boss, state);
  state = advanced.state;
  assert.equal(advanced.ability.due, false);
  advanced = advanceBossTurn(boss, state);
  state = advanced.state;
  assert.equal(advanced.ability.due, true);
  const phases = pendingBossPhases(boss, 95, state);
  assert.equal(phases[0].createBase, "rift_maw");
  state = applyBossPhaseState(state, phases[0]);
  assert.equal(bossAbilityStatus(boss, state).every, 1);
});

test("beginner boss rewards are strong but bounded and the first grants an early relic", async () => {
  const pack = await loadCardPack();
  const profile = createCampaignProfile({ runId: "boss_reward_run", now: 100, random: 0 });
  profile.nodeIndex = 11;
  const rewards = generateCampaignRewards(pack.CARDS, profile, campaignNodeAt(profile), { count: 3 });
  assert.equal(rewards.length, 3);
  assert.ok(rewards.every(reward => pack.ALL_CARD_MAP[reward.cardId].cost >= 3 && pack.ALL_CARD_MAP[reward.cardId].cost <= 5));
  finishCampaignRewardNode(profile, { skipped: true, now: 200 });
  assert.ok(profile.relics.includes("Vanguard Standard"));
  assert.equal(profile.nextBattleShield, 5);
});

test("route reward promises override early regional caps instead of downgrading", async () => {
  const pack = await loadCardPack();
  const expectations = { vanguard: [1, 3], siegebreaker: [4, 5], rift_gambit: [5, 7] };
  for (const [pathId, [minimum, maximum]] of Object.entries(expectations)) {
    const profile = createCampaignProfile({ runId: `risk_${pathId}`, now: 100, random: 0 });
    chooseCampaignPath(profile, pathId);
    profile.nodeIndex = 2;
    const rewards = generateCampaignRewards(pack.CARDS, profile, campaignNodeAt(profile), { count: 3 });
    assert.equal(rewards.length, 3);
    assert.ok(rewards.every(reward => pack.ALL_CARD_MAP[reward.cardId].cost >= minimum && pack.ALL_CARD_MAP[reward.cardId].cost <= maximum));
  }
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

test("victory recovery restores at least 15 and elites guarantee 70 percent Authority", () => {
  const normal = createCampaignProfile({ runId: "normal_recovery", now: 100, random: 0 });
  chooseCampaignPath(normal, "vanguard");
  recordCampaignBattleResult(normal, { won: true, authorityRemaining: 1, now: 200 });
  assert.equal(normal.authority, 16);
  assert.ok(normal.authority <= normal.maxAuthority);

  const elite = createCampaignProfile({ runId: "elite_recovery", now: 100, random: 0 });
  chooseCampaignPath(elite, "rift_gambit");
  recordCampaignBattleResult(elite, { won: true, authorityRemaining: 1, now: 200 });
  assert.equal(elite.authority, 42);
  assert.ok(elite.authority <= elite.maxAuthority);
});

test("early elites receive exactly the configured total of one starting Base", () => {
  const routeElite = createCampaignProfile({ runId: "base_total_route", now: 100, random: 0 });
  chooseCampaignPath(routeElite, "siegebreaker");
  assert.equal(campaignEnemyStartingBaseCount(currentCampaignEncounter(routeElite)), 1);

  const gateElite = createCampaignProfile({ runId: "base_total_gate", now: 100, random: 0 });
  gateElite.nodeIndex = 7;
  const encounter = currentCampaignEncounter(gateElite);
  assert.equal(encounter.node.type, "elite");
  assert.equal(encounter.difficulty, "easy");
  assert.equal(campaignEnemyStartingBaseCount(encounter), 1);
});

test("Rest offers Recover, Purge, and Prepare as one clean choice", () => {
  const recover = createCampaignProfile({ runId: "rest_recover", now: 100, random: 0 });
  recover.nodeIndex = 9;
  recover.authority = 10;
  assert.deepEqual(campaignRestChoices(recover).map(choice => choice.id), ["recover", "purge", "prepare"]);
  const recovered = resolveCampaignRestChoice(recover, "recover", { now: 200 });
  assert.equal(recovered.ok, true);
  assert.ok(recovered.healed >= 15);
  assert.equal(campaignNodeAt(recover).type, "boss");

  const prepare = createCampaignProfile({ runId: "rest_prepare", now: 100, random: 0 });
  prepare.nodeIndex = 9;
  assert.equal(resolveCampaignRestChoice(prepare, "prepare").shield, 5);
  assert.equal(prepare.nextBattleShield, 5);

  const purge = createCampaignProfile({ runId: "rest_purge", now: 100, random: 0 });
  purge.nodeIndex = 9;
  assert.equal(resolveCampaignRestChoice(purge, "purge_coin").ok, true);
  assert.equal(purge.deck.filter(cardId => cardId === "starter_coin").length, 5);
});

test("War Camp spends campaign currency on focused one-time services", async () => {
  const pack = await loadCardPack();
  const profile = createCampaignProfile({ runId: "war_camp", now: 100, random: 0, specialization: "merchant" });
  profile.nodeIndex = 6;
  profile.currency = 300;
  profile.authority = 40;
  assert.equal(campaignWarCampOptions(profile).length, 5);
  assert.equal(buyCampaignWarCampOption(profile, "recover", { cards: pack.CARDS }).healed, 10);
  assert.equal(buyCampaignWarCampOption(profile, "reward_reroll", { cards: pack.CARDS }).ok, true);
  assert.equal(profile.rewardRerolls, 1);
  const deckBeforeRecruit = profile.deck.length;
  const recruit = buyCampaignWarCampOption(profile, "recruit", { cards: pack.CARDS });
  assert.equal(recruit.ok, true);
  assert.equal(profile.deck.length, deckBeforeRecruit + 1);
  assert.equal(buyCampaignWarCampOption(profile, "purge", { cards: pack.CARDS, starterId: "starter_coin" }).ok, true);
  assert.equal(buyCampaignWarCampOption(profile, "recover", { cards: pack.CARDS }).ok, false);
  assert.equal(finishCampaignWarCamp(profile).ok, true);
  assert.equal(campaignNodeAt(profile).type, "elite");
});

test("free starter cleanup and reward rerolls persist safely", async () => {
  const pack = await loadCardPack();
  const profile = createCampaignProfile({ runId: "cleanup", now: 100, random: 0 });
  profile.starterRemovalsAvailable = 1;
  assert.equal(useCampaignStarterRemoval(profile, "starter_blade").ok, true);
  assert.equal(profile.deck.filter(cardId => cardId === "starter_blade").length, 3);
  profile.rewardRerolls = 1;
  profile.nodeIndex = 2;
  const before = generateCampaignRewards(pack.CARDS, profile, campaignNodeAt(profile), { count: 3 });
  assert.equal(rerollCampaignRewardOptions(profile).ok, true);
  const after = generateCampaignRewards(pack.CARDS, profile, campaignNodeAt(profile), { count: 3 });
  assert.equal(profile.rewardRerolls, 0);
  assert.equal(profile.rewardSeedOffset, 1);
  assert.notDeepEqual(after, before);
  profile.starterRemovalsAvailable = 1;
  assert.equal(skipCampaignStarterRemoval(profile), true);
});

test("a complete branched region uses War Camp and Rest before reaching the next region", () => {
  const profile = createCampaignProfile({ runId: "full_region_run", now: 100, random: 0 });
  chooseCampaignPath(profile, "vanguard");
  recordCampaignBattleResult(profile, { won: true, authorityRemaining: 55 });
  finishCampaignRewardNode(profile, { skipped: true });
  skipCampaignStarterRemoval(profile);
  chooseCampaignPath(profile, "rift_gambit");
  recordCampaignBattleResult(profile, { won: true, authorityRemaining: 48 });
  finishCampaignRewardNode(profile, { skipped: true });
  assert.equal(campaignNodeAt(profile).type, "shop");
  finishCampaignWarCamp(profile);
  assert.equal(campaignNodeAt(profile).type, "elite");
  recordCampaignBattleResult(profile, { won: true, authorityRemaining: 41 });
  finishCampaignRewardNode(profile, { skipped: true });
  skipCampaignStarterRemoval(profile);
  assert.equal(campaignNodeAt(profile).type, "rest");
  resolveCampaignRestChoice(profile, "prepare");
  assert.equal(currentCampaignEncounter(profile).boss.id, campaignBossForRegion(1).id);
  recordCampaignBattleResult(profile, { won: true, authorityRemaining: 33 });
  finishCampaignRewardNode(profile, { skipped: true });
  assert.equal(profile.region, 2);
  assert.equal(campaignNodeAt(profile).type, "path");
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
