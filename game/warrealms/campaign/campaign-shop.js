import { addCampaignCard } from "./campaign-state.js";
import { generateCampaignRewards } from "./campaign-rewards.js";

export function campaignShopRerollCost(profile) {
  return 10 + Math.min(40, Math.max(0, Number(profile?.shopRerolls) || 0) * 5);
}

export function generateCampaignShop(cards = [], profile = {}, node = {}, options = {}) {
  const rewards = generateCampaignRewards(cards, profile, { ...node, seed: (Number(node.seed) || 1) + Math.max(0, Number(profile.shopRerolls) || 0) }, {
    count: Math.max(1, Math.min(5, Number(options.count) || 3)),
    rarity: options.rarity || "any"
  });
  return rewards.map(reward => {
    const card = cards.find(candidate => candidate.id === reward.cardId);
    const cost = Math.max(20, 18 + Math.max(1, Number(card?.cost) || 1) * 11 + Math.max(0, Number(profile.region) || 1) * 2);
    return { ...reward, price: cost, destination: profile.rewardDestination || "deck" };
  });
}

export function buyCampaignShopCard(profile, offer, options = {}) {
  if (!profile || !offer?.cardId) return { ok: false, reason: "Invalid campaign shop offer." };
  const price = Math.max(0, Number(offer.price) || 0);
  if ((Number(profile.currency) || 0) < price) return { ok: false, reason: "Not enough campaign currency." };
  profile.currency -= price;
  addCampaignCard(profile, offer.cardId, options.destination || offer.destination || profile.rewardDestination);
  return { ok: true, cardId: offer.cardId, price };
}

export function removeCampaignDeckCard(profile, cardId, price = 60) {
  if (!profile || !cardId) return { ok: false, reason: "Choose a campaign card to remove." };
  const cost = Math.max(0, Number(price) || 0);
  if ((Number(profile.currency) || 0) < cost) return { ok: false, reason: "Not enough campaign currency." };
  const index = (profile.deck || []).indexOf(cardId);
  if (index < 0) return { ok: false, reason: "That card is not in the campaign deck." };
  if ((profile.deck || []).length <= 5) return { ok: false, reason: "The campaign deck cannot contain fewer than five cards." };
  profile.currency -= cost;
  profile.deck.splice(index, 1);
  return { ok: true, cardId, price: cost };
}

export function rerollCampaignShop(profile) {
  const cost = campaignShopRerollCost(profile);
  if (!profile || (Number(profile.currency) || 0) < cost) return { ok: false, reason: "Not enough campaign currency." };
  profile.currency -= cost;
  profile.shopRerolls = Math.max(0, Number(profile.shopRerolls) || 0) + 1;
  return { ok: true, price: cost, rerolls: profile.shopRerolls };
}
