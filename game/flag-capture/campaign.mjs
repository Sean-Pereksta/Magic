/* A compact faction tour; progress is local and independent of battle state. */
export const CAMPAIGN = Object.freeze([
  {
    id: 'first-light', title: 'First Light', subtitle: '01 · The Lumen landing',
    briefing: 'The Brood has found a glade full of sleeping beacons. Send your Skitterglows ahead, claim the lights, and use groves and Bloomcall to keep the expedition alive.',
    playerFaction: 'bloom', aiFaction: 'chaos', map: 'glade', difficulty: 'story',
    mode: 'beacons', width: 7, height: 5, terrain: 'sparse', goal: 12, duration: 150,
    rewardText: 'The Carnival route opens. Your light has caught something strange\u2019s attention.'
  },
  {
    id: 'crooked-ground', title: 'Crooked Ground', subtitle: '02 · An impossible shortcut',
    briefing: 'Now lead the Crooked Carnival. Spread your beasts across the battlefield, bend the front with Sideways Bite and Foldhorn, and hold ground long enough to score.',
    playerFaction: 'chaos', aiFaction: 'bloom', map: 'fracture', difficulty: 'easy',
    mode: 'control', width: 9, height: 5, terrain: 'sparse', goal: 18, duration: 180,
    rewardText: 'The Choir answers. An army that fights together is waiting beyond the rift.'
  },
  {
    id: 'choir-practice', title: 'Choir Practice', subtitle: '03 · A little harmony',
    briefing: 'Lead the Aureate Choir against the returning Carnival. Keep allies close for Resonance, shield your front with Sanctuary, and use Concordance to restore your formation\u2019s readiness.',
    playerFaction: 'choir', aiFaction: 'chaos', map: 'sanctuary', difficulty: 'normal',
    mode: 'beacons', width: 9, height: 7, terrain: 'balanced', goal: 24, duration: 180,
    rewardText: 'The wild frontier opens. Take what you have learned into a larger battle.'
  },
  {
    id: 'wild-frontier', title: 'Wild Frontier', subtitle: '04 · Life finds a flank',
    briefing: 'Return to the Lumen Brood on a wider, wilder battlefield. Use Sporehop to reach distant ground, heal in groves, and disrupt the Choir\u2019s close formations with Gravity Belch and Sporeburst.',
    playerFaction: 'bloom', aiFaction: 'choir', map: 'fracture', difficulty: 'normal',
    mode: 'control', width: 11, height: 7, terrain: 'wild', goal: 24, duration: 210,
    rewardText: 'The final rift opens. One last army stands in your way.'
  },
  {
    id: 'last-rift', title: 'The Last Rift', subtitle: '05 · The final encore',
    briefing: 'Take the Carnival into the final battle against a hardened Choir. Defeat their army or finish with more surviving health. Summon little allies, isolate their healers, and turn every strange ability into an opening.',
    playerFaction: 'chaos', aiFaction: 'choir', map: 'sanctuary', difficulty: 'hard',
    mode: 'elimination', width: 11, height: 9, terrain: 'wild', duration: 240,
    rewardText: 'Riftbound campaign complete. All five missions are yours to replay.'
  }
].map(level => Object.freeze(level)));

const INITIAL_PROGRESS = Object.freeze({version: 1, unlocked: 1, completed: Object.freeze([])});
const levelIndex = id => CAMPAIGN.findIndex(level => level.id === id);

/** Accept localStorage JSON or an object; always return a fresh, bounded value. */
export function readProgress(value) {
  let source = value;
  if (typeof source === 'string') {
    try { source = JSON.parse(source); } catch { source = null; }
  }
  if (!source || typeof source !== 'object' || Array.isArray(source)) source = INITIAL_PROGRESS;
  const requested = Number.isInteger(source.unlocked) ? source.unlocked : 1;
  let unlocked = Math.max(1, Math.min(CAMPAIGN.length, requested));
  const wins = new Set(Array.isArray(source.completed) ? source.completed.filter(id => typeof id === 'string' && levelIndex(id) >= 0) : []);
  // Earlier saved victories recover an unlock if its counter was missing.
  let consecutive = 0;
  while (consecutive < CAMPAIGN.length && wins.has(CAMPAIGN[consecutive].id)) consecutive++;
  unlocked = Math.max(unlocked, Math.min(CAMPAIGN.length, consecutive + 1));
  const completed = CAMPAIGN.filter((level, index) => index < unlocked && wins.has(level.id)).map(level => level.id);
  return {version: 1, unlocked, completed};
}

/** Call after a victory. Locked/unknown missions and replays cannot skip levels. */
export function completeLevel(progress, levelId) {
  const current = readProgress(progress);
  const index = levelIndex(levelId);
  if (index < 0 || index >= current.unlocked) return current;
  return readProgress({
    version: 1,
    unlocked: Math.max(current.unlocked, Math.min(CAMPAIGN.length, index + 2)),
    completed: [...new Set([...current.completed, levelId])]
  });
}

/** Only supported battle options leave the campaign module. */
export function createCampaignBattle(levelId) {
  const index = levelIndex(levelId);
  if (index < 0) throw new RangeError('Choose an existing campaign mission.');
  const {playerFaction, aiFaction, map, difficulty, mode, width, height, terrain, goal, duration} = CAMPAIGN[index];
  return {playerFaction, aiFaction, map, difficulty, mode, width, height, terrain, goal, duration, seed: 3401 + index * 997};
}
