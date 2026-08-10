function whole(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : fallback;
}

export function inheritedTransformState(sourceEntry, sourceCard, targetCard, preserve = {}) {
  const state = {};
  if (!sourceEntry || !targetCard) return state;

  if (preserve.healthRatio && Number(sourceEntry.maxHealth) > 0 && Number(targetCard.health) > 0) {
    const ratio = Math.max(0, Math.min(1, Number(sourceEntry.currentHealth) / Number(sourceEntry.maxHealth)));
    state.maxHealth = Math.max(1, whole(targetCard.health, 1));
    state.currentHealth = Math.max(1, Math.min(state.maxHealth, Math.round(state.maxHealth * ratio)));
  } else if (preserve.health && Number(targetCard.health) > 0) {
    state.maxHealth = Math.max(1, whole(targetCard.health, 1));
    state.currentHealth = Math.max(1, Math.min(state.maxHealth, whole(sourceEntry.currentHealth, state.maxHealth)));
  } else if (preserve.fullHeal && Number(targetCard.health) > 0) {
    state.maxHealth = Math.max(1, whole(targetCard.health, 1));
    state.currentHealth = state.maxHealth;
  }

  if (preserve.armor) {
    state.armor = whole(sourceEntry.armor);
    state.armorExpiresTurn = whole(sourceEntry.armorExpiresTurn);
  }
  if (preserve.charges) {
    const maximum = Number(targetCard.charge?.max);
    state.charges = Number.isFinite(maximum)
      ? Math.min(whole(maximum), whole(sourceEntry.charges))
      : whole(sourceEntry.charges);
    state.chargeUsedTurn = whole(sourceEntry.chargeUsedTurn);
    state.chargeGainTurn = whole(sourceEntry.chargeGainTurn);
    state.chargeGainedThisTurn = whole(sourceEntry.chargeGainedThisTurn);
  }
  if (preserve.heat) state.heat = Math.min(whole(targetCard.heat?.max, 99), whole(sourceEntry.heat));
  if (preserve.construction) {
    const targetConstruction = whole(targetCard.construction);
    state.constructionRemaining = Math.min(targetConstruction, whole(sourceEntry.constructionRemaining));
  }
  if (preserve.disableState) {
    state.disabledTurn = whole(sourceEntry.disabledTurn);
    state.stunTurns = whole(sourceEntry.stunTurns);
  }
  if (preserve.turnsSurvived) {
    state.turnsSurvived = whole(sourceEntry.turnsSurvived);
    state.recurringTurns = whole(sourceEntry.recurringTurns);
  }
  return state;
}

export function transformPreservesAttachments(transform = {}, option = {}) {
  const preserve = option.preserve || transform.preserve || {};
  return preserve.attachments === true;
}

export function baseTransformDestination(card, transform = {}, option = {}) {
  const explicit = option.destination ?? transform.destination;
  if (explicit !== undefined && explicit !== null && String(explicit).trim()) return String(explicit).trim();
  return card?.type === "base" ? "" : null;
}
