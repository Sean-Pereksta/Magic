function whole(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : fallback;
}

export function resolveChargeGain(current, charge = {}, gainedThisTurn = 0) {
  const before = whole(current);
  const maximum = whole(charge.max, 99);
  const perTurnCap = Number.isFinite(Number(charge.perTurnCap)) ? whole(charge.perTurnCap) : Infinity;
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
