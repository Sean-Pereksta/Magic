import { MISSIONS, REGIONS, ARENAS } from './campaign.mjs';

const difficulties = ['easy', 'medium', 'hard', 'pro'];
const factions = ['frontier', 'ascendant', 'brood'];
const integer = (value, max) => Math.min(max, Math.max(0, Math.floor(Number(value) || 0)));
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);

// These are personal saves, never evidence for competitive ratings.
export function mergePersonalResult(previous = {}, mode, input, now = new Date().toISOString()) {
  const next = structuredClone(previous);
  next.campaign ||= { highestCompletedMission: 0, completedMissions: {}, unlockedRegions: [REGIONS[0].id] };
  next.campaign.completedMissions ||= {};
  next.survival ||= { highestWave: 0, recordsByMap: {} };
  next.survival.recordsByMap ||= {};
  next.achievements ||= {};
  const difficulty = input.difficulty || 'medium';
  if (!difficulties.includes(difficulty)) throw new Error('Choose an existing difficulty.');
  if (mode === 'campaign') {
    const index = MISSIONS.findIndex((mission) => mission.id === input.missionId);
    if (index < 0) throw new Error('This campaign mission does not exist.');
    if (input.won === false) return previous;
    const mission = MISSIONS[index], old = next.campaign.completedMissions[mission.id] || {};
    const stars = Math.max(1, integer(input.stars || 1, 3));
    const validObjectives = new Set(mission.objectives.map((objective) => objective.id));
    const optional = (input.optionalObjectives || []).filter((id) => typeof id === 'string' && validObjectives.has(id));
    next.campaign.completedMissions[mission.id] = {
      ...old,
      stars: Math.max(stars, old.stars || 0),
      ratingsByDifficulty: { ...old.ratingsByDifficulty, [difficulty]: Math.max(stars, old.ratingsByDifficulty?.[difficulty] || 0) },
      optionalObjectives: [...new Set([...(old.optionalObjectives || []), ...optional])],
      completedAt: old.completedAt || now
    };
    next.campaign.highestCompletedMission = Math.max(next.campaign.highestCompletedMission || 0, index + 1);
    next.campaign.unlockedRegions = REGIONS.filter((_, i) => i === 0 || next.campaign.completedMissions[MISSIONS[i * 4 - 1].id]).map((region) => region.id);
    if ((index + 1) % 4 === 0) next.achievements[`${mission.regionId}-liberator`] = true;
    const totalStars = Object.values(next.campaign.completedMissions).reduce((sum, record) => sum + (record.stars || 0), 0);
    for (const threshold of [12, 24, 48, 72]) if (totalStars >= threshold) next.achievements[`campaign-${threshold}-stars`] = true;
    if (Object.keys(next.campaign.completedMissions).length === MISSIONS.length) next.achievements['shattered-front-complete'] = true;
  } else if (mode === 'survival') {
    if (!ARENAS.some((arena) => arena.id === input.arenaId) || !factions.includes(input.factionId)) throw new Error('Choose an existing survival arena and faction.');
    const key = `${input.arenaId}:${difficulty}`;
    const legacyKey = `${input.arenaId}_${difficulty}`;
    const old = next.survival.recordsByMap[key] || next.survival.recordsByMap[legacyKey];
    const record = {
      arenaId: input.arenaId, difficulty, factionId: input.factionId,
      wave: integer(input.wave ?? input.highestWave, 100000),
      durationSeconds: integer(input.durationSeconds, 604800),
      enemiesDefeated: integer(input.enemiesDefeated, 10000000),
      date: now, personal: true, verified: false
    };
    if (!old || record.wave > old.wave || (record.wave === old.wave && record.durationSeconds > (old.durationSeconds || 0))) {
      delete next.survival.recordsByMap[legacyKey];
      next.survival.recordsByMap[key] = record;
    }
    next.survival.highestWave = Math.max(next.survival.highestWave || 0, record.wave);
    for (const milestone of [5, 10, 15, 20, 30, 50, 75, 100]) if (record.wave >= milestone) next.achievements[`survival-${milestone}`] = true;
  } else throw new Error('Unsupported personal save.');
  return next;
}

export function createProfileStore({ db, api, isCurrentAccount = () => true }) {
  return {
    async save(username, mode, result) {
      if (!username || username.includes('/')) throw new Error('Sign into your CatnMice account to sync progress.');
      const ref = api.doc(db, 'users', username);
      return api.runTransaction(db, async (transaction) => {
        const snapshot = await transaction.get(ref);
        if (!snapshot.exists()) throw new Error('Sign into your existing CatnMice account to sync progress.');
        const account = snapshot.data(), previous = account.gameStats?.turncraft || {};
        const historyRef = result.resultId && /^[A-Za-z0-9_-]{1,100}$/.test(result.resultId)
          ? api.doc(db, 'users', username, 'turncraftSoloHistory', result.resultId) : null;
        const receipt = historyRef ? await transaction.get(historyRef) : null;
        if (!isCurrentAccount()) throw Object.assign(new Error('Your account changed. This run remains queued for the profile that played it.'), {code: 'ACCOUNT_CHANGED'});
        if (receipt?.exists()) return {account, profile: previous, wrote: false};
        const next = mergePersonalResult(previous, mode, result);
        const patch = {};
        for (const field of ['campaign', 'survival', 'achievements']) {
          if (!same(previous[field], next[field])) patch[`gameStats.turncraft.${field}`] = next[field];
        }
        // Replaying a queued result that is already saved performs no write.
        if (Object.keys(patch).length) transaction.update(ref, patch);
        if (historyRef) transaction.set(historyRef, {
          id: result.resultId, username, mode,
          outcome: result.won === false ? 'loss' : 'win',
          missionId: result.missionId || null, arenaId: result.arenaId || null,
          factionId: result.factionId || 'frontier', difficulty: result.difficulty || 'medium',
          stars: integer(result.stars, 3), wave: integer(result.wave ?? result.highestWave, 100000),
          durationSeconds: integer(result.durationSeconds, 604800), completedAt: Number(result.completedAt) || Date.now()
        });
        return { account, profile: next, wrote: Object.keys(patch).length > 0 || Boolean(historyRef) };
      });
    }
  };
}
