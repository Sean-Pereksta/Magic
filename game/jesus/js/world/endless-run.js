// One meter is ten world units, matching the historical Endless score scale.
export const WORLD_UNITS_PER_METER = 10;

export function createEndlessRun(previousBest = 0) {
  return { distance: 0, furthestX: 0, segment: 0, regionsReached: 1,
    enemiesDefeated: 0, defeated: new Set(), previousBest: Math.max(0, Number(previousBest) || 0) };
}

// A region or guardian arena has its own coordinates. Starting a segment adds
// no distance: only actual new forward ground reached by the player counts.
export function beginEndlessSegment(run, playerX) {
  run.segment++;
  run.defeated.clear();
  run.furthestX = Number.isFinite(playerX) ? playerX : 0;
}

export function advanceEndlessRun(run, playerX, world = {}) {
  if (!run || !Number.isFinite(playerX)) return 0;
  const x = Math.max(world.bounds?.left ?? 0, Math.min(world.bounds?.right ?? world.width ?? Infinity, playerX));
  const gained = Math.max(0, x - run.furthestX);
  run.distance += gained;
  run.furthestX = Math.max(run.furthestX, x);
  return gained;
}

export function recordEndlessDefeats(run, world) {
  if (!run) return;
  for (const enemy of world.enemies || []) {
    // Enemies falling out of the level are not combat victories.
    if (!enemy.dead || !(enemy.hp <= 0)) continue;
    const key = `${run.segment}:${enemy.id}`;
    if (!run.defeated.has(key)) { run.defeated.add(key); run.enemiesDefeated++; }
  }
}

export function endlessMeters(run) { return Math.max(0, (run?.distance || 0) / WORLD_UNITS_PER_METER); }
export function formatMeters(value) { return `${Math.max(0, Math.floor(Number(value) || 0)).toLocaleString("en-US")} m`; }
