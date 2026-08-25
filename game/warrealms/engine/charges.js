function whole(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : fallback;
}

function effectCreatesToken(effect, tokenId, seen = new Set()) {
  if (!effect || typeof effect !== "object") return false;
  if (seen.has(effect)) return false;
  seen.add(effect);

  if (Array.isArray(effect)) {
    return effect.some(value => effectCreatesToken(value, tokenId, seen));
  }

  const createToken = effect.createToken;
  if (createToken && typeof createToken === "object" && String(createToken.id || "") === tokenId) {
    return true;
  }

  return Object.values(effect).some(value => effectCreatesToken(value, tokenId, seen));
}

export function resolveChargePerTurnCap(charge = {}) {
  if (Number.isFinite(Number(charge.perTurnCap))) return whole(charge.perTurnCap);

  const tokenId = String(charge.tokenId || "");
  const actions = Array.isArray(charge.actions) ? charge.actions : [];
  const selfFeedsTriggeredToken = charge.trigger === "tokenPlayed"
    && tokenId
    && actions.some(action => effectCreatesToken(action?.effect, tokenId));

  // A self-feeding token charge engine may gain at most one full meter per turn.
  // This keeps recursive token builds powerful while guaranteeing the loop ends.
  return selfFeedsTriggeredToken ? whole(charge.max, 99) : Infinity;
}

export function resolveChargeGain(current, charge = {}, gainedThisTurn = 0) {
  const before = whole(current);
  const maximum = whole(charge.max, 99);
  const perTurnCap = resolveChargePerTurnCap(charge);
  const roomThisTurn = Math.max(0, perTurnCap - whole(gainedThisTurn));
  const requested = Math.min(roomThisTurn, whole(charge.gain));
  const after = Math.min(maximum, before + requested);
  return { before, after, gained: after - before, maximum, roomThisTurn };
}

export function chargeActionSpend(charges, action = {}) {
  const available = whole(charges);
  if (action.cost === "all") {
    const minimum = Math.max(1, whole(action.minimum, 1));
    return { allowed: available >= minimum, spend: available, available };
  }
  const spend = Math.max(1, whole(action.cost, 1));
  return { allowed: available >= spend, spend, available };
}
