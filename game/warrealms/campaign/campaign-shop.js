import { addCampaignCard, getCampaignSpecialization, removeCampaignDeckCardCopy } from "./campaign-state.js";
import { generateCampaignRewards } from "./campaign-rewards.js";
import { advanceCampaignNode, campaignNodeAt } from "./campaign-map.js";

export const WAR_CAMP_OPTIONS = Object.freeze([
  Object.freeze({ id: "recover", label: "Field Medicine", price: 20, description: "Restore 10 Authority." }),
  Object.freeze({ id: "purge", label: "Strip the Baggage", price: 30, description: "Remove one Realm Coin or Militia Blade." }),
  Object.freeze({ id: "reward_reroll", label: "Scout Better Spoils", price: 35, description: "Gain one reroll for a future three-card reward." }),
  Object.freeze({ id: "recruit", label: "Recruit a Veteran", price: 50, description: "Gain a deterministic random cost 3–4 card." }),
  Object.freeze({ id: "upgrade", label: "Faction Training", price: 75, description: "Replace one starter with a cost 3–4 faction card matching your specialty." })
]);

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
  const removed = removeCampaignDeckCardCopy(profile, cardId, { method: "shop" });
  if (!removed.ok) return removed;
  profile.currency -= cost;
  return { ok: true, cardId, price: cost };
}

export function rerollCampaignShop(profile) {
  const cost = campaignShopRerollCost(profile);
  if (!profile || (Number(profile.currency) || 0) < cost) return { ok: false, reason: "Not enough campaign currency." };
  profile.currency -= cost;
  profile.shopRerolls = Math.max(0, Number(profile.shopRerolls) || 0) + 1;
  return { ok: true, price: cost, rerolls: profile.shopRerolls };
}

function hash(text) {
  let value = 2166136261;
  for (const char of String(text || "")) {
    value ^= char.charCodeAt(0);
    value = Math.imul(value, 16777619);
  }
  return value >>> 0;
}

function legalWarCampCards(cards, minimumCost, maximumCost, faction = "") {
  return cards.filter(card => card?.id && card.collectible !== false && !card.token && !card.transformedFrom && !card.campaignOnly && !card.permanentMarket && Number(card.cost) >= minimumCost && Number(card.cost) <= maximumCost && (!faction || card.faction === faction));
}

function deterministicWarCampCard(cards, profile, node, purpose, faction = "") {
  const pool = legalWarCampCards(cards, 3, 4, faction).sort((left, right) => String(left.id).localeCompare(String(right.id)));
  if (!pool.length && faction) return deterministicWarCampCard(cards, profile, node, purpose, "");
  if (!pool.length) return null;
  const purchaseCount = Object.values(profile.warCampPurchases || {}).reduce((sum, ids) => sum + (Array.isArray(ids) ? ids.length : 0), 0);
  return pool[hash(`${profile.runId}:${node.id}:${purpose}:${purchaseCount}`) % pool.length];
}

export function campaignWarCampOptions(profile) {
  const node = campaignNodeAt(profile);
  const purchased = new Set(profile?.warCampPurchases?.[node?.id] || []);
  const deck = Array.isArray(profile?.deck) ? profile.deck : [];
  return WAR_CAMP_OPTIONS.map(option => ({
    ...option,
    purchased: purchased.has(option.id),
    enabled: !purchased.has(option.id) && (Number(profile?.currency) || 0) >= option.price &&
      (option.id !== "recover" || (Number(profile?.authority) || 0) < (Number(profile?.maxAuthority) || 60)) &&
      (!["purge", "upgrade"].includes(option.id) || deck.includes("starter_coin") || deck.includes("starter_blade"))
  }));
}

export function buyCampaignWarCampOption(profile, optionId, context = {}) {
  const node = campaignNodeAt(profile);
  const option = WAR_CAMP_OPTIONS.find(candidate => candidate.id === optionId);
  if (!profile || node?.type !== "shop" || !option) return { ok: false, reason: "Choose a valid War Camp purchase." };
  const purchased = new Set(profile.warCampPurchases?.[node.id] || []);
  if (purchased.has(option.id)) return { ok: false, reason: "That War Camp service has already been used here." };
  if ((Number(profile.currency) || 0) < option.price) return { ok: false, reason: "Not enough campaign currency." };

  const starterId = String(context.starterId || "");
  if (["purge", "upgrade"].includes(option.id) && !["starter_coin", "starter_blade"].includes(starterId)) {
    return { ok: false, reason: "Choose a Realm Coin or Militia Blade." };
  }
  if (["purge", "upgrade"].includes(option.id) && !(profile.deck || []).includes(starterId)) {
    return { ok: false, reason: "That starter is no longer in the campaign deck." };
  }

  const specializationFaction = getCampaignSpecialization(profile.specialization)?.faction || "";
  const gainedCard = option.id === "recruit"
    ? deterministicWarCampCard(context.cards || [], profile, node, option.id)
    : option.id === "upgrade"
      ? deterministicWarCampCard(context.cards || [], profile, node, option.id, specializationFaction)
      : null;
  if (["recruit", "upgrade"].includes(option.id) && !gainedCard) return { ok: false, reason: "No eligible War Camp card was available." };

  let result = {};
  if (option.id === "recover") {
    const maximum = Math.max(1, Number(profile.maxAuthority) || 60);
    const before = Math.max(0, Number(profile.authority) || 0);
    if (before >= maximum) return { ok: false, reason: "Authority is already full." };
    profile.authority = Math.min(maximum, before + 10);
    result.healed = profile.authority - before;
  } else if (option.id === "purge") {
    const removed = removeCampaignDeckCardCopy(profile, starterId, { now: context.now, method: "war-camp" });
    if (!removed.ok) return removed;
    result.cardId = starterId;
  } else if (option.id === "reward_reroll") {
    profile.rewardRerolls = Math.max(0, Number(profile.rewardRerolls) || 0) + 1;
    result.rewardRerolls = profile.rewardRerolls;
  } else if (option.id === "recruit") {
    addCampaignCard(profile, gainedCard.id, "deck");
    result.gainedCardId = gainedCard.id;
  } else if (option.id === "upgrade") {
    const removed = removeCampaignDeckCardCopy(profile, starterId, { now: context.now, method: "war-camp-upgrade" });
    if (!removed.ok) return removed;
    addCampaignCard(profile, gainedCard.id, "deck");
    result.cardId = starterId;
    result.gainedCardId = gainedCard.id;
  }

  profile.currency = Math.max(0, Number(profile.currency) || 0) - option.price;
  profile.warCampPurchases = profile.warCampPurchases && typeof profile.warCampPurchases === "object" ? profile.warCampPurchases : {};
  profile.warCampPurchases[node.id] = [...purchased, option.id];
  profile.history = Array.isArray(profile.history) ? profile.history : [];
  profile.history.push({ atMs: Number(context.now) || Date.now(), type: "WAR_CAMP_PURCHASED", nodeId: node.id, optionId: option.id, price: option.price, ...result });
  profile.history = profile.history.slice(-200);
  return { ok: true, optionId: option.id, price: option.price, ...result };
}

export function finishCampaignWarCamp(profile, options = {}) {
  const node = campaignNodeAt(profile);
  if (!profile || node?.type !== "shop") return { ok: false, reason: "The campaign is not at a War Camp." };
  profile.history = Array.isArray(profile.history) ? profile.history : [];
  profile.history.push({ atMs: Number(options.now) || Date.now(), type: "WAR_CAMP_LEFT", nodeId: node.id });
  profile.history = profile.history.slice(-200);
  advanceCampaignNode(profile);
  return { ok: true, nextNode: campaignNodeAt(profile) };
}
