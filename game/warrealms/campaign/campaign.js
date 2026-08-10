import { campaignNodeAt, campaignRegionScaling, advanceCampaignNode } from "./campaign-map.js";
import { getCampaignBoss } from "./campaign-bosses.js";

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
  const eliteMultiplier = node.type === "elite" ? 1.25 : 1;
  const routeMultiplier = Math.max(.75, Number(node.enemyAuthorityMultiplier) || 1);
  const difficulty = boss
    ? (boss.difficulty === "mythic" ? "impossible" : boss.difficulty)
    : node.type === "elite"
      ? (profile.region >= 8 || node.pathId === "rift_gambit" ? "impossible" : "hard")
      : profile.region >= 7 ? "hard" : profile.region >= 3 ? "medium" : "easy";
  const regionalShield = scaling.modifiers.filter(modifier => modifier.id === "armored_front").reduce((sum, modifier) => sum + modifier.rank * 3, 0);
  return {
    node,
    scaling,
    boss,
    difficulty,
    enemyAuthority: boss
      ? Math.round(boss.authority * scaling.enemyAuthorityMultiplier * routeMultiplier)
      : Math.round(60 * scaling.enemyAuthorityMultiplier * eliteMultiplier * routeMultiplier),
    enemyShield: boss
      ? whole(boss.startingShield) + regionalShield + whole(node.enemyShield)
      : (node.type === "elite" ? 5 : 0) + regionalShield + whole(node.enemyShield),
    enemyHandSize: Math.max(2, Math.min(7, scaling.enemyHandSize + whole(node.enemyHandSize))),
    startingBases: boss ? [...boss.startingBases] : [],
    startingDeck: boss ? [...boss.startingDeck] : [],
    enemyStartingBases: whole(node.enemyStartingBases),
    enemyBonusTrade: scaling.enemyBonusTrade + whole(node.enemyBonusTrade) + whole(boss?.economy?.bonusTrade),
    enemyBonusCombat: scaling.enemyBonusCombat + whole(node.enemyBonusCombat),
    enemyTradeLimit: boss ? Math.max(1, whole(boss.economy?.tradeLimit, 6)) : 99,
    enemyAggression: Math.max(1, Number(boss?.aggression) || (node.type === "elite" ? 1.15 : 1)),
    primaryFaction: boss?.faction || "",
    supportFaction: boss?.supportFaction || "",
    currencyBonus: whole(node.currencyBonus),
    bossCard: boss?.bossCard || ""
  };
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
  const remaining = Math.max(1, whole(result.authorityRemaining, profile.authority || profile.maxAuthority));
  profile.authority = Math.min(whole(profile.maxAuthority, 60), remaining + Math.max(5, Math.floor(profile.maxAuthority * .15)));
  profile.currency = whole(profile.currency) + (encounter.node.type === "boss" ? 75 : encounter.node.type === "elite" ? 40 : 20) + Math.max(0, profile.region - 1) * 5 + whole(encounter.currencyBonus);
  if (encounter.boss) profile.bossesDefeated = [...new Set([...(profile.bossesDefeated || []), encounter.boss.id])];
  appendHistory(profile, {
    atMs: now,
    type: "BATTLE_WON",
    nodeId: encounter.node.id,
    nodeType: encounter.node.type,
    bossId: encounter.boss?.id || "",
    pathId: encounter.node.pathId || "",
    region: profile.region,
    authorityRemaining: remaining
  });
  advanceCampaignNode(profile);
  return profile;
}

export function finishCampaignRewardNode(profile, result = {}) {
  if (!profile || campaignNodeAt(profile)?.type !== "reward") return profile;
  appendHistory(profile, {
    atMs: Number(result.now) || Date.now(),
    type: result.skipped ? "CARD_REWARD_SKIPPED" : "REWARD_NODE_COMPLETED",
    region: profile.region
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
    node
  };
}
