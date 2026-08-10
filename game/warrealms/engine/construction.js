function whole(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : fallback;
}

export function constructionHealthCap(maxHealth, totalConstruction, remainingConstruction) {
  const finalMax = Math.max(1, whole(maxHealth, 1));
  const total = whole(totalConstruction);
  if (!total) return finalMax;
  const remaining = Math.max(0, Math.min(total, whole(remainingConstruction)));
  if (!remaining) return finalMax;
  const builtSteps = Math.max(1, Math.min(total, total - remaining + 1));
  return Math.max(1, Math.min(finalMax, Math.floor((finalMax * builtSteps) / total)));
}

export function advanceConstruction(remainingConstruction, amount = 1) {
  const before = whole(remainingConstruction);
  const after = Math.max(0, before - whole(amount));
  return { before, after, advanced: before - after, completed: before > 0 && after === 0 };
}

export function repairConstructionHealth(currentHealth, healthCap, amount = 0) {
  const before = Math.max(1, whole(currentHealth, 1));
  const after = Math.min(Math.max(1, whole(healthCap, 1)), before + whole(amount));
  return { before, after, repaired: Math.max(0, after - before) };
}
