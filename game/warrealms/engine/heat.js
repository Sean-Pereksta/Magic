function number(value, fallback = 0) {
  const result = Number(value);
  return Number.isFinite(result) ? result : fallback;
}

export function resolveHeatValue(heat = {}, current = 0, delta = 0) {
  const maximum = Math.max(0, Math.floor(number(heat.max, 99)));
  const before = Math.max(0, Math.min(maximum, Math.floor(number(current))));
  const after = Math.max(0, Math.min(maximum, before + Math.floor(number(delta))));
  return {
    before,
    after,
    maximum,
    changed: before !== after,
    gained: Math.max(0, after - before),
    spent: Math.max(0, before - after)
  };
}

export function heatTransformReady(card, heatValue) {
  if (card?.transform?.trigger !== "heat") return false;
  const required = Math.max(1, Math.floor(number(card.transform.required, 1)));
  return Math.max(0, number(heatValue)) >= required;
}

export function heatThresholdsReached(heat = {}, heatValue = 0) {
  return (Array.isArray(heat.thresholds) ? heat.thresholds : []).filter(threshold => Math.max(0, number(heatValue)) >= Math.max(0, number(threshold.at)));
}

export function heatOverloadReady(heat = {}, heatValue = 0) {
  return !!heat.overload && Math.max(0, number(heatValue)) >= Math.max(1, number(heat.overload.at, 1));
}
