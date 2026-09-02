import { campaignNodeAt, campaignRegionScaling, advanceCampaignNode } from "./campaign-map.js";
import { campaignBossForRegion, getCampaignBoss } from "./campaign-bosses.js";
import { commanderBattleRules } from "./campaign-commanders.js";

function whole(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : fallback;
}

function appendHistory(profile, entry) {
  profile.history = Array.isArray(profile.history) ? profile.history : [];
  profile.history.push(entry);
  profile.history = profile.history.slice(-200);
}

export function currentCampaignEncounter(profile) {
  const node = campaignNodeAt(profile);
  if (!node) return null;
  const scaling = campaignRegionScaling(profile.region);
  const boss = node.type === "boss" ? getCampaignBoss(node.bossId) : null;
  const routeMultiplier = Math.max(.75, Number(node.enemyAuthorityMultiplier) || 1);
  const difficulty = boss
    ? (boss.difficulty === "mythic" ? "impossible" : boss.difficulty)
    : profile.region <= 2
      ? (node.pathId === "rift_gambit" ? "medium" : "easy")
      : profile.region <= 5
        ? (node.pathId === "rift_gambit" ? "hard" : "medium")
        : profile.region <= 9
          ? (node.type === "elite" ? "hard" : "medium")
          : profile.region <= 14
            ? (node.pathId === "rift_gambit" ? "impossible" : "hard")
            : profile.region <= 18
              ? (node.type === "elite" ? "impossible" : "hard")
              : "impossible";
  const regionalShield = scaling.modifiers.filter(modifier => modifier.id === "armored_front").reduce((sum, modifier) => sum + modifier.rank * 3, 0);
  const configuredBaseTotal = Math.max(whole(node.enemyStartingBases), whole(scaling.enemyStartingBases));
  const regionalAggression = 1 + Math.min(.2, Math.max(0, Number(profile.region) - 2) * .015);
  return {
    node,
    scaling,
    boss,
    difficulty,
    enemyAuthority: boss
      ? Math.round(boss.authority * scaling.enemyAuthorityMultiplier * routeMultiplier)
      : Math.round(scaling.baseEnemyAuthority * scaling.enemyAuthorityMultiplier * routeMultiplier),
    enemyShield: boss
      ? whole(boss.startingShield) + regionalShield + whole(node.enemyShield)
      : regionalShield + whole(node.enemyShield),
    enemyHandSize: Math.max(2, Math.min(7, scaling.enemyHandSize + whole(node.enemyHandSize))),
    startingBases: boss ? [...boss.startingBases] : [],
    startingDeck: boss ? [...boss.startingDeck] : [],
    enemyStartingBases: boss ? boss.startingBases.length : configuredBaseTotal,
    enemyBonusTrade: scaling.enemyBonusTrade + whole(node.enemyBonusTrade) + whole(boss?.economy?.bonusTrade),
    enemyBonusCombat: scaling.enemyBonusCombat + whole(node.enemyBonusCombat),
    enemyTradeLimit: boss ? Math.max(1, whole(boss.economy?.tradeLimit, 6)) : 99,
    enemyAggression: Math.max(.75, (Number(boss?.aggression) || (node.type === "elite" ? 1.08 : 1)) * regionalAggression),
    primaryFaction: boss?.faction || "",
    supportFaction: boss?.supportFaction || "",
    currencyBonus: whole(node.currencyBonus),
    bossCard: boss?.bossCard || ""
  };
}

export function campaignEnemyStartingBaseCount(encounter) {
  if (!encounter) return 0;
  return encounter.boss ? (encounter.startingBases || []).length : whole(encounter.enemyStartingBases);
}

export function recordCampaignBattleResult(profile, result = {}) {
  if (!profile) return null;
  const encounter = currentCampaignEncounter(profile);
  if (!encounter || !["battle", "elite", "boss"].includes(encounter.node.type)) return profile;
  const now = Number(result.now) || Date.now();
  if (!result.won) {
    profile.status = "defeated";
    profile.authority = 0;
    appendHistory(profile, { atMs: now, type: "CAMPAIGN_DEFEAT", nodeId: encounter.node.id, region: profile.region });
    return profile;
  }

  profile.battlesWon = whole(profile.battlesWon) + 1;
  profile.level = Math.max(whole(profile.level, 1), profile.battlesWon + 1);
  profile.commanderLevel = Math.max(whole(profile.commanderLevel, 1), profile.level);
  const remaining = Math.max(1, whole(result.authorityRemaining, profile.authority || profile.maxAuthority));
  const maximum = Math.max(1, whole(profile.maxAuthority, 60));
  const standardRecovery = Math.max(15, Math.floor(maximum * .25));
  let recoveredAuthority = Math.min(maximum, remaining + standardRecovery);
  if (encounter.node.type === "elite") recoveredAuthority = Math.max(recoveredAuthority, Math.ceil(maximum * .7));
  profile.authority = Math.min(maximum, recoveredAuthority);
  const commanderRules = commanderBattleRules(profile);
  const baseCurrency = (encounter.node.type === "boss" ? 75 : encounter.node.type === "elite" ? 40 : 20)
    + Math.max(0, profile.region - 1) * 5
    + whole(encounter.currencyBonus);
  const commanderCurrencyBonus = Math.floor(baseCurrency * whole(commanderRules.currencyBonusPercent) / 100);
  profile.currency = whole(profile.currency) + baseCurrency + commanderCurrencyBonus;
  if (encounter.node.type === "boss" && whole(commanderRules.rewardRerolls)) {
    profile.rewardRerolls = whole(profile.rewardRerolls) + whole(commanderRules.rewardRerolls);
  }
  if (encounter.boss) profile.bossesDefeated = [...new Set([...(profile.bossesDefeated || []), encounter.boss.id])];
  appendHistory(profile, {
    atMs: now,
    type: "BATTLE_WON",
    nodeId: encounter.node.id,
    nodeType: encounter.node.type,
    bossId: encounter.boss?.id || "",
    pathId: encounter.node.pathId || "",
    region: profile.region,
    authorityRemaining: remaining,
    authorityRecovered: profile.authority - remaining,
    authorityAfterRecovery: profile.authority,
    commanderLevel: profile.commanderLevel,
    commanderCurrencyBonus
  });
  advanceCampaignNode(profile);
  return profile;
}

export function finishCampaignRewardNode(profile, result = {}) {
  const node = campaignNodeAt(profile);
  if (!profile || node?.type !== "reward") return profile;
  if (whole(node.starterPurge) > 0) profile.starterRemovalsAvailable = whole(profile.starterRemovalsAvailable) + whole(node.starterPurge);
  if (node.grantRelic) {
    profile.relics = [...new Set([...(profile.relics || []), String(node.grantRelic)])];
    profile.nextBattleShield = Math.max(whole(profile.nextBattleShield), 5);
  }
  appendHistory(profile, {
    atMs: Number(result.now) || Date.now(),
    type: result.skipped ? "CARD_REWARD_SKIPPED" : "REWARD_NODE_COMPLETED",
    region: profile.region,
    starterPurgeGranted: whole(node.starterPurge),
    relicGranted: node.grantRelic || ""
  });
  profile.pendingReward = null;
  advanceCampaignNode(profile);
  return profile;
}

export function campaignProgressSummary(profile) {
  const node = campaignNodeAt(profile);
  return {
    region: Math.max(1, whole(profile?.region, 1)),
    level: Math.max(1, whole(profile?.level, 1)),
    battlesWon: whole(profile?.battlesWon),
    deckSize: Array.isArray(profile?.deck) ? profile.deck.length : 0,
    collectionSize: Object.values(profile?.collection || {}).reduce((sum, value) => sum + whole(value), 0),
    currency: whole(profile?.currency),
    authority: whole(profile?.authority),
    maxAuthority: Math.max(1, whole(profile?.maxAuthority, 60)),
    starterCardsRemaining: (profile?.deck || []).filter(cardId => cardId === "starter_coin" || cardId === "starter_blade").length,
    bossesDefeated: (profile?.bossesDefeated || []).length,
    nextBoss: campaignBossForRegion(Math.max(1, whole(profile?.region, 1))),
    node
  };
}
