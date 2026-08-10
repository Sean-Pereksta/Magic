import { advanceCampaignNode, campaignNodeAt } from "./campaign-map.js";
import { removeCampaignDeckCardCopy } from "./campaign-state.js";

function whole(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : fallback;
}

export function campaignRestRecovery(profile) {
  const maximum = Math.max(1, whole(profile?.maxAuthority, 60));
  const authority = Math.min(maximum, whole(profile?.authority));
  const normal = Math.max(15, Math.floor(maximum * .3));
  return authority <= maximum * .35 ? Math.max(normal, Math.ceil(maximum * .45)) : normal;
}

export function campaignRestChoices(profile) {
  const deck = Array.isArray(profile?.deck) ? profile.deck : [];
  return [
    {
      id: "recover",
      label: "Recover",
      description: `Restore up to ${campaignRestRecovery(profile)} Authority${whole(profile?.authority) <= whole(profile?.maxAuthority, 60) * .35 ? " (low-Authority bonus)" : ""}.`,
      enabled: whole(profile?.authority) < whole(profile?.maxAuthority, 60)
    },
    {
      id: "purge",
      label: "Purge",
      description: "Remove one Realm Coin or Militia Blade from the campaign deck.",
      enabled: deck.includes("starter_coin") || deck.includes("starter_blade")
    },
    {
      id: "prepare",
      label: "Prepare",
      description: "Begin the next battle with 5 additional Shield.",
      enabled: whole(profile?.nextBattleShield) < 15
    }
  ];
}

export function resolveCampaignRestChoice(profile, choiceId, options = {}) {
  if (!profile || campaignNodeAt(profile)?.type !== "rest") return { ok: false, reason: "The campaign is not at a Rest node." };
  const id = String(choiceId || "");
  const now = Number(options.now) || Date.now();
  let result = {};
  if (id === "recover") {
    const maximum = Math.max(1, whole(profile.maxAuthority, 60));
    const before = Math.min(maximum, whole(profile.authority));
    profile.authority = Math.min(maximum, before + campaignRestRecovery(profile));
    result = { healed: profile.authority - before };
  } else if (id === "prepare") {
    const before = whole(profile.nextBattleShield);
    profile.nextBattleShield = Math.min(15, before + 5);
    result = { shield: profile.nextBattleShield - before };
  } else if (id === "purge_coin" || id === "purge_blade") {
    const cardId = id === "purge_coin" ? "starter_coin" : "starter_blade";
    const removed = removeCampaignDeckCardCopy(profile, cardId, { now, method: "rest" });
    if (!removed.ok) return removed;
    result = { cardId };
  } else {
    return { ok: false, reason: "Choose Recover, Purge, or Prepare." };
  }
  profile.history = Array.isArray(profile.history) ? profile.history : [];
  profile.history.push({ atMs: now, type: "CAMPAIGN_REST_RESOLVED", choiceId: id, ...result });
  profile.history = profile.history.slice(-200);
  advanceCampaignNode(profile);
  return { ok: true, choiceId: id, ...result, nextNode: campaignNodeAt(profile) };
}
