import { CARDS } from "../../warrealms-pack/warrealms-cards.js";
import { queueEffectResolution } from "./effects.js";
import { heatOverloadReady, resolveHeatValue } from "./heat.js";

const CARD_MAP = new Map(CARDS.map(card => [card.id, card]));

// Runtime card rules that need owner-turn state rather than on-play resolution.
// Keep these values on the card object so the UI and resolver read the same rule.
const heavenlance = CARD_MAP.get("heavenlance_thermal_turret");
if (heavenlance?.heat) {
  heavenlance.heat.startOfTurn = 1;
  heavenlance.text = "At the start of your turn, gain 1 Heat. This Base produces no Combat normally.";
  heavenlance.heatText = "At 6 Heat, automatically spend all 6 Heat: gain 15 Combat, then reset to 0 Heat.";
}

function whole(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : fallback;
}

function turnOwner(game, event = {}) {
  const playerId = String(event.playerId || event.actorId || event.ownerId || "");
  return (game?.players || []).find(player => String(player?.id || "") === playerId) || null;
}

function automaticOverload(card, entry) {
  const overload = card?.heat?.overload;
  if (!overload || overload.optional === true) return null;
  return heatOverloadReady(card.heat, entry.heat) ? overload : null;
}

/**
 * Queue passive Heat gains that occur at the start of the owner's turn.
 *
 * This intentionally runs only on persistent Bases. Ships still use the normal
 * on-play Heat resolver, and external Heat effects continue to use the targeted
 * Heat flow in the battle resolver.
 */
export function queueStartOfTurnHeat(game, event = {}, queueEvent) {
  if (typeof queueEvent !== "function") return 0;
  const player = turnOwner(game, event);
  if (!player) return 0;

  let changed = 0;
  for (const entry of player.bases || []) {
    const card = CARD_MAP.get(entry?.id);
    const gain = whole(card?.heat?.startOfTurn);
    if (!card?.heat || gain <= 0) continue;

    // Match the normal Base activation rule: a stunned Base does not fire its
    // start-of-turn engine while it is disabled.
    if (whole(entry.stunTurns) > 0) continue;
    if (whole(entry.constructionRemaining) > 0) continue;

    const update = resolveHeatValue(card.heat, entry.heat, gain);
    if (!update.changed || update.gained <= 0) continue;

    entry.heat = update.after;
    changed += 1;
    queueEvent(game, {
      type: "HEAT_GAINED",
      actorId: player.id,
      playerId: player.id,
      ownerId: player.id,
      cardId: card.id,
      instanceId: entry.instanceId,
      faction: card.faction,
      amount: update.gained,
      before: update.before,
      after: update.after,
      method: "start-of-turn"
    }, event);

    const overload = automaticOverload(card, entry);
    if (!overload) continue;

    queueEffectResolution(game, player.id, overload.effect || {}, {
      cardId: card.id,
      instanceId: entry.instanceId,
      source: "heat-overload:start-of-turn",
      eventId: event.id,
      eventChainId: event.chainId
    });

    const heatBeforeReset = whole(entry.heat);
    entry.heat = whole(overload.reset);
    const spent = Math.max(0, heatBeforeReset - entry.heat);

    queueEvent(game, {
      type: "HEAT_OVERLOADED",
      actorId: player.id,
      playerId: player.id,
      ownerId: player.id,
      cardId: card.id,
      instanceId: entry.instanceId,
      faction: card.faction,
      amount: spent,
      before: heatBeforeReset,
      after: entry.heat,
      method: "start-of-turn-overload"
    }, event);

    if (spent > 0) {
      queueEvent(game, {
        type: "HEAT_SPENT",
        actorId: player.id,
        playerId: player.id,
        ownerId: player.id,
        cardId: card.id,
        instanceId: entry.instanceId,
        faction: card.faction,
        amount: spent,
        before: heatBeforeReset,
        after: entry.heat,
        method: "start-of-turn-overload-reset"
      }, event);
    }
  }

  return changed;
}

export function startOfTurnHeatGain(cardId) {
  return whole(CARD_MAP.get(String(cardId || ""))?.heat?.startOfTurn);
}
