export const CHECKPOINT_KEY = 'turncraft.shattered-front.checkpoint.v1';
const empty = () => ({
  campaign: { completedMissions: {} },
  survival: { recordsByMap: {} },
  achievements: {}
});
export function readProgress(key) {
  try {
    const saved = JSON.parse(localStorage.getItem(key) || '{}');
    const progress = mergeProgress(empty(), saved);
    progress.pendingResults = Array.isArray(saved?.pendingResults)
      ? saved.pendingResults.filter((result) => {
          if (!result || typeof result !== 'object' || Array.isArray(result))
            return false;
          if (result.mode === 'campaign')
            return (
              result.won !== false &&
              typeof result.missionId === 'string' &&
              result.missionId.length > 0
            );
          return (
            (result.mode === 'survival' || result.mode === 'endless') &&
            typeof result.arenaId === 'string' &&
            result.arenaId.length > 0
          );
        })
      : [];
    return progress;
  } catch {
    return empty();
  }
}
export function mergeProgress(local, remote = {}) {
  const next = structuredClone(local || empty());
  next.campaign ||= { completedMissions: {} };
  next.campaign.completedMissions ||= {};
  for (const [id, record] of Object.entries(
    remote.campaign?.completedMissions || {}
  )) {
    const old = next.campaign.completedMissions[id];
    const ratingsByDifficulty = { ...old?.ratingsByDifficulty };
    for (const [difficulty, stars] of Object.entries(
      record.ratingsByDifficulty || {}
    ))
      ratingsByDifficulty[difficulty] = Math.max(
        ratingsByDifficulty[difficulty] || 0,
        stars || 0
      );
    next.campaign.completedMissions[id] = {
      ...old,
      ...record,
      ratingsByDifficulty,
      optionalObjectives: [
        ...new Set([
          ...(old?.optionalObjectives || []),
          ...(record.optionalObjectives || [])
        ])
      ],
      stars: Math.max(Number(old?.stars) || 0, Number(record?.stars) || 0)
    };
  }
  next.survival ||= { recordsByMap: {} };
  next.survival.recordsByMap ||= {};
  for (const [id, record] of Object.entries(
    remote.survival?.recordsByMap || {}
  )) {
    const key =
      record.arenaId && record.difficulty
        ? `${record.arenaId}:${record.difficulty}`
        : id;
    if (
      (record?.wave || record?.highestWave || 0) >
      (next.survival.recordsByMap[key]?.wave ||
        next.survival.recordsByMap[key]?.highestWave ||
        0)
    )
      next.survival.recordsByMap[key] = record;
  }
  next.achievements = { ...next.achievements, ...remote.achievements };
  return next;
}
export function recordResult(progress, result) {
  const next = mergeProgress(progress);
  if (result.mode === 'campaign' && result.won) {
    const old = next.campaign.completedMissions[result.missionId] || {};
    const ratingsByDifficulty = {
      ...old.ratingsByDifficulty,
      [result.difficulty || 'medium']: Math.max(
        old.ratingsByDifficulty?.[result.difficulty || 'medium'] || 0,
        result.stars || 1
      )
    };
    next.campaign.completedMissions[result.missionId] = {
      ...result,
      ratingsByDifficulty,
      optionalObjectives: [
        ...new Set([
          ...(old.optionalObjectives || []),
          ...(result.optionalObjectives || [])
        ])
      ],
      completedAt: new Date().toISOString(),
      stars: Math.max(old.stars || 0, result.stars || 1)
    };
    if (result.missionId.endsWith('-4'))
      next.achievements[`${result.regionId}-liberator`] = true;
    const stars = Object.values(next.campaign.completedMissions).reduce(
      (sum, record) => sum + (record.stars || 0),
      0
    );
    for (const threshold of [12, 24, 48, 72])
      if (stars >= threshold)
        next.achievements[`campaign-${threshold}-stars`] = true;
  }
  if (result.mode === 'survival' || result.mode === 'endless') {
    const id = result.arenaId || result.regionId;
    const key = `${id}:${result.difficulty || 'medium'}`;
    const old = next.survival.recordsByMap[key] || {};
    if ((result.wave || 0) > (old.wave || 0))
      next.survival.recordsByMap[key] = {
        ...result,
        achievedAt: new Date().toISOString()
      };
    for (const wave of [5, 10, 15, 20, 30, 50, 75, 100])
      if (result.wave >= wave) next.achievements[`survival-${wave}`] = true;
  }
  return next;
}
export function missionUnlocked(mission, catalog, progress) {
  const i = catalog.missions.findIndex((item) => item.id === mission.id);
  return (
    i <= 0 ||
    Boolean(progress.campaign.completedMissions[catalog.missions[i - 1].id])
  );
}
export function arenaUnlocked(arena, catalog, progress) {
  const i = catalog.regions.findIndex(
    (region) => region.id === (arena.regionId || arena.id)
  );
  if (i <= 0) return true;
  const previous = catalog.missions
    .filter((mission) => mission.regionId === catalog.regions[i - 1].id)
    .at(-1);
  return Boolean(previous && progress.campaign.completedMissions[previous.id]);
}
