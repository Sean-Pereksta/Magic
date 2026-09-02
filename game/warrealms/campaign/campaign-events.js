import { commanderBattleRules, commanderEventRequirementMet, getCampaignCommander } from "./campaign-commanders.js";
import { addCampaignCard, removeCampaignDeckCardCopy } from "./campaign-state.js";

function freezeChoice(choice) {
  return Object.freeze({
    cost: 0,
    requirement: null,
    ...choice,
    outcome: Object.freeze({ ...(choice.outcome || {}) }),
    requirement: choice.requirement ? Object.freeze({ ...choice.requirement }) : null
  });
}

function freezeEvent(event) {
  return Object.freeze({
    ...event,
    choices: Object.freeze((event.choices || []).map(freezeChoice))
  });
}

export const CAMPAIGN_EVENTS = Object.freeze([
  freezeEvent({
    id: "broken_caravan",
    title: "Broken Caravan",
    text: "A shattered supply caravan blocks the road. Survivors guard what remains while your scouts watch the ridgeline.",
    choices: [
      { id: "recruit", label: "Recruit Survivors", description: "Add two random Gorak cards to the campaign deck.", outcome: { randomFactionCard: "green", cardCount: 2 } },
      { id: "take_supplies", label: "Take the Supplies", description: "Gain 55 War Currency but lose 8 Authority.", outcome: { currency: 55, authority: -8 } },
      { id: "repair", label: "Repair Their Wagons", description: "Spend 25 Currency and gain a card aligned with your commander.", cost: 25, outcome: { randomCommanderFactionCard: true, cardCount: 1, minCost: 3 } },
      { id: "quartermaster_terms", label: "Quartermaster Terms", description: "Xythe option: negotiate supplies and a recruit for only 10 Currency.", cost: 10, requirement: { faction: "yellow" }, outcome: { currency: 35, randomCommanderFactionCard: true, cardCount: 1, minCost: 2 }, special: true }
    ]
  }),
  freezeEvent({
    id: "ancient_forge",
    title: "Ancient Forge",
    text: "The forge is cold, but the channels beneath its anvil still carry a dangerous ember-glow.",
    choices: [
      { id: "temper", label: "Temper a Starter", description: "Remove one starter card and replace it with a commander-faction card.", outcome: { upgradeStarter: true, randomCommanderFactionCard: true, minCost: 3 } },
      { id: "strip", label: "Strip the Forge", description: "Salvage the mechanisms for 45 War Currency.", outcome: { currency: 45 } },
      { id: "reignite", label: "Reignite the Furnace", description: "Lose 6 Authority to claim a powerful cost 5+ Umbral card.", outcome: { authority: -6, randomFactionCard: "red", cardCount: 1, minCost: 5, stat: { path: "heatGained", amount: 3 } } },
      { id: "engineer_restore", label: "Restore the Bellows", description: "Azure option: spend 10 Currency to recover a Base without the Authority risk.", cost: 10, requirement: { faction: "blue" }, outcome: { randomFactionCard: "blue", cardCount: 1, type: "base", minCost: 3 }, special: true }
    ]
  }),
  freezeEvent({
    id: "fallen_fortress",
    title: "Fallen Fortress",
    text: "A ruined fortress still commands the valley. Its surviving garrison offers access to one part of the stronghold.",
    choices: [
      { id: "recover_base", label: "Recover a Base", description: "Add a random Azure Base to your campaign deck.", outcome: { randomFactionCard: "blue", cardCount: 1, type: "base", minCost: 2 } },
      { id: "clear_barracks", label: "Clear the Barracks", description: "Remove one Realm Coin or Militia Blade from the deck.", outcome: { removeStarter: true } },
      { id: "raise_wards", label: "Raise the Wards", description: "Begin the next battle with 7 additional Shield.", outcome: { nextBattleShield: 7 } },
      { id: "challenge_garrison", label: "Challenge the Garrison", description: "Gorak option: lose 10 Authority for a cost 5+ Gorak card and 25 Currency.", requirement: { faction: "green" }, outcome: { authority: -10, currency: 25, randomFactionCard: "green", cardCount: 1, minCost: 5 }, special: true }
    ]
  }),
  freezeEvent({
    id: "ruined_observatory",
    title: "Ruined Xythe Observatory",
    text: "A silent observatory still charts futures for masters who never returned.",
    choices: [
      { id: "search", label: "Search It", description: "Add a random Xythe card.", outcome: { randomFactionCard: "yellow", cardCount: 1 } },
      { id: "dismantle", label: "Dismantle It", description: "Gain 40 War Currency.", outcome: { currency: 40 } },
      { id: "read_threads", label: "Read the Threads", description: "Xythe option: gain a reward reroll and 15 Currency.", requirement: { faction: "yellow" }, outcome: { rewardRerolls: 1, currency: 15 }, special: true },
      { id: "leave", label: "Leave", description: "Continue without changing the campaign.", outcome: {} }
    ]
  }),
  freezeEvent({
    id: "wounded_grukkin",
    title: "Wounded Grukkin",
    text: "A young ember-beast follows the army at a careful distance.",
    choices: [
      { id: "take", label: "Take It", description: "Add Grukkin Embercub to the campaign deck.", outcome: { addCard: "grukkin_embercub" } },
      { id: "feed", label: "Feed It", description: "Spend 25 Currency, gain the card, and record two Heat toward unlocks.", cost: 25, outcome: { addCard: "grukkin_embercub", stat: { path: "heatGained", amount: 2 } } },
      { id: "umbral_bond", label: "Bind the Ember", description: "Umbral option: lose 4 Authority and gain the card without paying Currency.", requirement: { faction: "red" }, outcome: { addCard: "grukkin_embercub", authority: -4 }, special: true },
      { id: "leave", label: "Leave", description: "Continue without changing the campaign.", outcome: {} }
    ]
  })
]);

const eventMap = new Map(CAMPAIGN_EVENTS.map(event => [event.id, event]));

function whole(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : fallback;
}

function strings(value) {
  return Array.isArray(value) ? value.map(String).filter(Boolean) : [];
}

export function getCampaignEvent(eventId) {
  return eventMap.get(String(eventId || "")) || null;
}

export function campaignEventForNode(node) {
  if (!node || node.type !== "event" || !CAMPAIGN_EVENTS.length) return null;
  const seed = whole(node.seed);
  return CAMPAIGN_EVENTS[seed % CAMPAIGN_EVENTS.length];
}

function requirementReason(profile, choice) {
  if (choice.requirement && !commanderEventRequirementMet(profile, choice.requirement)) {
    if (choice.requirement.faction) return `Requires a commander with ${choice.requirement.faction} faction identity.`;
    if (choice.requirement.perk) return "Requires a specific commander perk.";
    return "This commander cannot use that option.";
  }
  if (whole(choice.cost) > whole(profile?.currency)) return `Requires ${whole(choice.cost)} War Currency.`;
  if ((choice.outcome?.removeStarter || choice.outcome?.upgradeStarter)
    && !(profile?.deck || []).some(cardId => cardId === "starter_coin" || cardId === "starter_blade")) return "No starter card remains to change.";
  return "";
}

export function campaignEventChoiceOptions(profile, eventOrId) {
  const event = typeof eventOrId === "object" ? eventOrId : getCampaignEvent(eventOrId);
  return (event?.choices || []).map(choice => {
    const lockedReason = requirementReason(profile, choice);
    return { ...choice, available: !lockedReason, lockedReason };
  });
}

function randomOutcomeCards(profile, outcome, context = {}) {
  if (outcome.addCard) return [String(outcome.addCard)];
  const requested = Math.max(1, whole(outcome.cardCount, 1));
  const supplied = strings(context.randomFactionCardIds);
  if (supplied.length) return supplied.slice(0, requested);
  if (context.randomFactionCardId) return [String(context.randomFactionCardId)];
  if (typeof context.pickCard === "function") {
    const commander = getCampaignCommander(profile?.commanderId);
    const faction = outcome.randomFactionCard
      || (outcome.randomCommanderFactionCard ? commander?.factions?.[0] : "");
    const cards = [];
    for (let index = 0; index < requested; index += 1) {
      const cardId = context.pickCard({
        faction,
        factions: outcome.randomCommanderFactionCard ? [...(commander?.factions || [])] : faction ? [faction] : [],
        type: outcome.type || "",
        minCost: whole(outcome.minCost),
        exclude: cards
      });
      if (cardId) cards.push(String(cardId));
    }
    return cards;
  }
  return [];
}

export function resolveCampaignEventChoice(profile, eventId, choiceId, context = {}) {
  const event = getCampaignEvent(eventId);
  const choice = event?.choices?.find(candidate => candidate.id === choiceId);
  if (!profile || !event || !choice) return { ok: false, reason: "Invalid campaign event choice." };
  const lockedReason = requirementReason(profile, choice);
  if (lockedReason) return { ok: false, reason: lockedReason };

  const cost = whole(choice.cost);
  const rules = commanderBattleRules(profile);
  const currencyGain = Math.max(0, Number(choice.outcome?.currency) || 0);
  const currencyBonus = Math.floor(currencyGain * Math.max(0, Number(rules.currencyBonusPercent) || 0) / 100);
  profile.currency = Math.max(0, whole(profile.currency) - cost + currencyGain + currencyBonus);
  const authorityDelta = Number(choice.outcome?.authority) || 0;
  profile.authority = Math.max(1, Math.min(whole(profile.maxAuthority, 60), whole(profile.authority, 1) + authorityDelta));
  profile.nextBattleShield = whole(profile.nextBattleShield) + whole(choice.outcome?.nextBattleShield);
  profile.rewardRerolls = whole(profile.rewardRerolls) + whole(choice.outcome?.rewardRerolls);

  let removedCardId = "";
  if (choice.outcome?.removeStarter || choice.outcome?.upgradeStarter) {
    removedCardId = (profile.deck || []).includes("starter_coin") ? "starter_coin" : "starter_blade";
    const removed = removeCampaignDeckCardCopy(profile, removedCardId, { now: context.now, method: choice.outcome.upgradeStarter ? "event-upgrade" : "event" });
    if (!removed.ok) return removed;
  }

  const cardIds = randomOutcomeCards(profile, choice.outcome || {}, context);
  for (const cardId of cardIds) addCampaignCard(profile, cardId, choice.outcome?.destination || profile.rewardDestination);
  if (choice.outcome?.relic) profile.relics = [...new Set([...(profile.relics || []), choice.outcome.relic])];
  if (choice.outcome?.stat?.path) {
    profile.stats = profile.stats || {};
    const key = choice.outcome.stat.path;
    profile.stats[key] = whole(profile.stats[key]) + (Number(choice.outcome.stat.amount) || 0);
  }
  profile.history = Array.isArray(profile.history) ? profile.history : [];
  profile.history.push({
    atMs: Number(context.now) || Date.now(),
    type: "CAMPAIGN_EVENT_CHOICE",
    eventId,
    choiceId,
    commanderId: String(profile.commanderId || ""),
    cardIds,
    removedCardId,
    currencyBonus
  });
  profile.history = profile.history.slice(-200);
  return { ok: true, event, choice, cardIds, cardId: cardIds[0] || "", removedCardId, currencyBonus };
}
