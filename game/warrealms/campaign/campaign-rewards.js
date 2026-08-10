import { addCampaignCard } from "./campaign-state.js";

export const CAMPAIGN_RARITIES = Object.freeze({
  common: Object.freeze({ minCost: 1, maxCost: 3, weight: 55 }),
  uncommon: Object.freeze({ minCost: 3, maxCost: 5, weight: 30 }),
  rare: Object.freeze({ minCost: 5, maxCost: 7, weight: 12 }),
  boss: Object.freeze({ minCost: 6, maxCost: 99, weight: 3 })
});

function hash(text) {
  let value = 2166136261;
  for (const char of String(text || "")) {
    value ^= char.charCodeAt(0);
    value = Math.imul(value, 16777619);
  }
  return value >>> 0;
}

function randomFromSeed(seed) {
  let state = (Number(seed) || 1) >>> 0;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 4294967296;
  };
}

export function campaignCardRarity(card) {
  const cost = Math.max(0, Number(card?.cost) || 0);
  if (cost >= 7) return "boss";
  if (cost >= 5) return "rare";
  if (cost >= 3) return "uncommon";
  return "common";
}

export function campaignRewardPool(cards = [], profile = {}, options = {}) {
  const region = Math.max(1, Number(profile.region) || 1);
  const maximumUnlockedCost = Math.min(8, 2 + Math.floor((region + 1) / 2) + Math.floor((profile.bossesDefeated || []).length / 2));
  const explicitUnlocks = new Set(profile.unlockedCards || []);
  return cards.filter(card => {
    const campaignExclusiveReward = card?.campaignOnly === true && card?.campaignReward === true;
    if (!card?.id || (card.collectible === false && !campaignExclusiveReward) || card.token || card.transformedFrom || (card.campaignOnly && !campaignExclusiveReward) || card.permanentMarket) return false;
    if (!Number.isFinite(Number(card.cost)) || Number(card.cost) < 1) return false;
    if (campaignExclusiveReward && options.rarity !== "boss") return false;
    if (options.rarity && options.rarity !== "any" && campaignCardRarity(card) !== options.rarity) return false;
    return campaignExclusiveReward || Number(card.cost) <= maximumUnlockedCost || explicitUnlocks.has(card.id);
  });
}

export function generateCampaignRewards(cards = [], profile = {}, node = {}, options = {}) {
  const count = Math.max(1, Math.min(5, Math.floor(Number(options.count) || 3)));
  const requestedRarity = String(options.rarity || node.rarity || "common");
  let pool = campaignRewardPool(cards, profile, { rarity: requestedRarity });
  const preferredIds = new Set(pool.map(card => card.id));
  if (pool.length < count) {
    const seen = new Set(pool.map(card => card.id));
    pool = [...pool, ...campaignRewardPool(cards, profile, { rarity: "any" }).filter(card => !seen.has(card.id))];
  }
  const random = randomFromSeed(node.seed || hash(`${profile.runId}:${profile.region}:${profile.nodeIndex}:${requestedRarity}`));
  const ranked = pool.map(card => ({ card, roll: random() + (preferredIds.has(card.id) ? 2 : 0) + Math.max(0, Number(card.cost) || 0) * Math.min(.06, (Number(profile.region) || 1) * .003) }))
    .sort((left, right) => right.roll - left.roll || String(left.card.id).localeCompare(String(right.card.id)));
  return ranked.slice(0, count).map(({ card }) => ({
    cardId: card.id,
    rarity: campaignCardRarity(card),
    destination: profile.rewardDestination === "collection" ? "collection" : "deck"
  }));
}

export function applyCampaignReward(profile, reward, options = {}) {
  if (!profile || !reward?.cardId) return profile;
  addCampaignCard(profile, reward.cardId, options.destination || reward.destination || profile.rewardDestination);
  profile.pendingReward = null;
  profile.history = Array.isArray(profile.history) ? profile.history : [];
  profile.history.push({
    atMs: Number(options.now) || Date.now(),
    type: "CARD_REWARD_SELECTED",
    cardId: reward.cardId,
    rarity: reward.rarity || "common",
    region: profile.region
  });
  profile.history = profile.history.slice(-200);
  return profile;
}
