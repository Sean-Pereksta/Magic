export const CAMPAIGN_UNLOCK_RULES = Object.freeze([
  Object.freeze({
    id: "gorak_tier_two",
    condition: Object.freeze({ path: "bossesDefeated.length", atLeast: 1 }),
    cardFilter: Object.freeze({ faction: "green", maxCost: 5 })
  }),
  Object.freeze({
    id: "heat_specialists",
    condition: Object.freeze({ path: "stats.heatSpent", atLeast: 25 }),
    cardFilter: Object.freeze({ mechanic: "heat" })
  }),
  Object.freeze({
    id: "advanced_construction",
    condition: Object.freeze({ path: "stats.basesConstructed", atLeast: 10 }),
    cardFilter: Object.freeze({ mechanic: "construction" })
  }),
  Object.freeze({
    id: "advanced_evolution",
    condition: Object.freeze({ path: "stats.cardsTransformed", atLeast: 10 }),
    cardFilter: Object.freeze({ mechanic: "transform" })
  })
]);

function valueAtPath(object, path) {
  return String(path || "").split(".").filter(Boolean).reduce((value, key) => {
    if (key === "length" && Array.isArray(value)) return value.length;
    return value && typeof value === "object" ? value[key] : undefined;
  }, object);
}

export function campaignUnlockConditionMet(profile, condition = {}) {
  const value = valueAtPath(profile, condition.path);
  if (condition.equals !== undefined) return value === condition.equals;
  if (condition.includes !== undefined) return Array.isArray(value) && value.includes(condition.includes);
  return Number(value) >= Math.max(0, Number(condition.atLeast) || 0);
}

function cardHasMechanic(card, mechanic) {
  if (mechanic === "heat") return !!(card.heat || card.transform?.trigger === "heat" || /\bHeat\b/i.test(card.text || ""));
  if (mechanic === "construction") return !!(card.expansion || Number(card.construction) > 0 || /Construction/i.test(card.text || ""));
  if (mechanic === "transform") return !!card.transform;
  return true;
}

export function cardMatchesCampaignUnlock(card, filter = {}) {
  if (!card?.id || card.collectible === false || card.token || card.transformedFrom || card.campaignOnly) return false;
  if (filter.faction && card.faction !== filter.faction) return false;
  if (Number.isFinite(Number(filter.minCost)) && Number(card.cost) < Number(filter.minCost)) return false;
  if (Number.isFinite(Number(filter.maxCost)) && Number(card.cost) > Number(filter.maxCost)) return false;
  if (filter.mechanic && !cardHasMechanic(card, filter.mechanic)) return false;
  return true;
}

export function evaluateCampaignUnlocks(profile, cards = [], rules = CAMPAIGN_UNLOCK_RULES) {
  if (!profile) return [];
  const completed = new Set(profile.unlocks || []);
  const unlockedCards = new Set(profile.unlockedCards || []);
  const unlocked = [];
  for (const rule of rules || []) {
    if (!rule?.id || completed.has(rule.id) || !campaignUnlockConditionMet(profile, rule.condition)) continue;
    const cardIds = cards.filter(card => cardMatchesCampaignUnlock(card, rule.cardFilter)).map(card => card.id);
    cardIds.forEach(cardId => unlockedCards.add(cardId));
    completed.add(rule.id);
    unlocked.push({ ruleId: rule.id, cardIds });
  }
  profile.unlocks = [...completed];
  profile.unlockedCards = [...unlockedCards];
  return unlocked;
}
