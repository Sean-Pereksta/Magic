import { addCampaignCard } from "./campaign-state.js";

export const CAMPAIGN_EVENTS = Object.freeze([
  Object.freeze({
    id: "ruined_observatory",
    title: "Ruined Xythe Observatory",
    text: "A silent observatory still charts futures for masters who never returned.",
    choices: Object.freeze([
      Object.freeze({ id: "search", label: "Search it", outcome: Object.freeze({ randomFactionCard: "yellow" }) }),
      Object.freeze({ id: "dismantle", label: "Dismantle it", outcome: Object.freeze({ currency: 40 }) }),
      Object.freeze({ id: "leave", label: "Leave", outcome: Object.freeze({}) })
    ])
  }),
  Object.freeze({
    id: "wounded_grukkin",
    title: "Wounded Grukkin",
    text: "A young ember-beast follows the army at a careful distance.",
    choices: Object.freeze([
      Object.freeze({ id: "take", label: "Take it", outcome: Object.freeze({ addCard: "grukkin_embercub" }) }),
      Object.freeze({ id: "feed", label: "Feed it", cost: 25, outcome: Object.freeze({ addCard: "grukkin_embercub", stat: Object.freeze({ path: "heatGained", amount: 2 }) }) }),
      Object.freeze({ id: "leave", label: "Leave", outcome: Object.freeze({}) })
    ])
  })
]);

const eventMap = new Map(CAMPAIGN_EVENTS.map(event => [event.id, event]));

export function getCampaignEvent(eventId) {
  return eventMap.get(String(eventId || "")) || null;
}

export function resolveCampaignEventChoice(profile, eventId, choiceId, context = {}) {
  const event = getCampaignEvent(eventId);
  const choice = event?.choices?.find(candidate => candidate.id === choiceId);
  if (!profile || !event || !choice) return { ok: false, reason: "Invalid campaign event choice." };
  const cost = Math.max(0, Number(choice.cost) || 0);
  if ((Number(profile.currency) || 0) < cost) return { ok: false, reason: "Not enough campaign currency." };
  profile.currency = Math.max(0, Number(profile.currency) || 0) - cost + (Number(choice.outcome?.currency) || 0);
  const cardId = choice.outcome?.addCard || (choice.outcome?.randomFactionCard ? context.randomFactionCardId : "");
  if (cardId) addCampaignCard(profile, cardId, choice.outcome?.destination || profile.rewardDestination);
  if (choice.outcome?.relic) profile.relics = [...new Set([...(profile.relics || []), choice.outcome.relic])];
  if (choice.outcome?.stat?.path) {
    profile.stats = profile.stats || {};
    const key = choice.outcome.stat.path;
    profile.stats[key] = Math.max(0, Number(profile.stats[key]) || 0) + (Number(choice.outcome.stat.amount) || 0);
  }
  profile.history = Array.isArray(profile.history) ? profile.history : [];
  profile.history.push({ atMs: Number(context.now) || Date.now(), type: "CAMPAIGN_EVENT_CHOICE", eventId, choiceId, cardId: cardId || "" });
  profile.history = profile.history.slice(-200);
  return { ok: true, event, choice, cardId: cardId || "" };
}
