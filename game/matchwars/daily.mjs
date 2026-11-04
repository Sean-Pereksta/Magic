/** Shared UTC daily challenges. Pure configuration, scoring, and personal records. */
import { CLASSES, normalizeLoadout } from './classes.mjs?v=4';
import { UPGRADES } from './combat.mjs?v=4';
import { normalizeMastery, SPECIALIZATIONS } from './mastery.mjs?v=4';

const DAY = 86_400_000;
const HISTORY_DAYS = 30;
const DAILY_REWARD = 200;
const MAX_SCORE = 1_000_000;
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const finite = (value, low, high, fallback = low) => typeof value === 'number' && Number.isFinite(value)
  ? Math.min(high, Math.max(low, value)) : fallback;

export function utcToday(now = new Date()) {
  try { return now.toISOString().slice(0, 10); } catch { return ''; }
}

function dayNumber(date) {
  if (typeof date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(date)) return NaN;
  const parsed = new Date(`${date}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) && utcToday(parsed) === date ? parsed.getTime() / DAY : NaN;
}

export function isDailyDate(date, today = utcToday()) {
  const candidate = dayNumber(date), current = dayNumber(today), age = current - candidate;
  return Number.isFinite(age) && age >= 0 && age < HISTORY_DAYS;
}

function hashSeed(seed) {
  let hash = 2166136261;
  for (const character of String(seed)) { hash ^= character.charCodeAt(0); hash = Math.imul(hash, 16777619); }
  return hash >>> 0;
}

/** Each call creates a fresh independent stream. Do not share one RNG across systems. */
export function seededRng(seed) {
  let state = hashSeed(seed);
  return () => {
    state = (state + 0x6D2B79F5) >>> 0;
    let mixed = Math.imul(state ^ state >>> 15, 1 | state);
    mixed ^= mixed + Math.imul(mixed ^ mixed >>> 7, 61 | mixed);
    return ((mixed ^ mixed >>> 14) >>> 0) / 4294967296;
  };
}

const ARMIES = [
  ['The First Covenant', ['ember-mage', 'tide-ranger', 'grove-warden', 'storm-lancer', 'sun-paladin', 'void-reaper']],
  ['The Spellbound Host', ['ember-knight', 'cryomancer', 'bloom-druid', 'tempest-mage', 'dawn-priest', 'necromancer']],
  ['The Twilight Guard', ['phoenix-adept', 'tide-oracle', 'thorn-guardian', 'windcaller', 'sun-crusader', 'hexweaver']],
  ['The Frostfire Pact', ['ember-mage', 'cryomancer', 'bloom-druid', 'storm-lancer', 'sun-paladin', 'necromancer']],
  ['The Dawn Expedition', ['ember-knight', 'tide-ranger', 'grove-warden', 'tempest-mage', 'dawn-priest', 'hexweaver']],
  ['The Phoenix Vanguard', ['phoenix-adept', 'tide-oracle', 'thorn-guardian', 'storm-lancer', 'sun-crusader', 'void-reaper']],
];

/** Returns null for malformed, future, or more than 29-days-old share-link dates. */
export function createDaily(date = utcToday(), today = utcToday()) {
  if (!isDailyDate(date, today)) return null;
  const seed = `matchwars-daily-v1:${date}`, choices = seededRng(`${seed}:configuration`);
  const [armyName, army] = ARMIES[Math.floor(choices() * ARMIES.length)];
  const stage = [12, 24, 36][Math.floor(choices() * 3)];
  const loadout = normalizeLoadout(Object.fromEntries(army.map(id => [CLASSES[id].element, id])));
  const upgrades = Object.fromEntries(UPGRADES.map(definition => [definition.id, Math.min(2, definition.max)]));
  const mastery = normalizeMastery(Object.fromEntries(Object.keys(CLASSES).map(id => [id, {
    xp: 180, specialization: SPECIALIZATIONS[id][Math.floor(choices() * 2)].id,
  }])));
  const medalTargets = stage === 12 ? { gold: 11000, silver: 9000, bronze: 5000 }
    : stage === 24 ? { gold: 13500, silver: 11000, bronze: 5000 }
      : { gold: 15500, silver: 12500, bronze: 5000 };
  for (const value of Object.values(mastery)) Object.freeze(value);
  return Object.freeze({ version: 1, date, seed, stage, armyName,
    loadout: Object.freeze(loadout), upgrades: Object.freeze(upgrades), mastery: Object.freeze(mastery),
    boardSeed: `${seed}:board`, combatSeed: `${seed}:combat`, events: true, reward: DAILY_REWARD,
    medalTargets: Object.freeze(medalTargets),
  });
}

function medalFor(score, won, targets) {
  return !won ? 'none' : score >= targets.gold ? 'gold' : score >= targets.silver ? 'silver' : 'bronze';
}

/** Scores terminal results only. Practice and replay use the identical formula. */
export function scoreDaily(result, config) {
  const source = record(result) ? result : {};
  if (!(typeof source.won === 'boolean' && source.status === undefined
    || source.status === 'won' || source.status === 'lost')) return { score: 0, medal: 'none', won: false };
  const targets = config?.medalTargets || { gold: 11000, silver: 9000, bronze: 5000 };
  const won = source.status === undefined ? source.won === true : source.status === 'won';
  const kills = Math.floor(finite(source.kills, 0, 300));
  const matches = Math.floor(finite(source.matches, 0, 200));
  const chain = Math.floor(finite(source.bestChain, 1, 24));
  const events = Math.floor(finite(source.events, 0, 5));
  const time = finite(source.time, 0, 900, 900);
  const hp = finite(source.playerHp, 0, 10000), maxHp = finite(source.playerMax, 0, 10000);
  const health = maxHp > 0 ? Math.min(1, hp / maxHp) : 0;
  const tactics = kills * 75 + matches * 20 + (chain - 1) * 150 + events * 150;
  const score = won ? 5000 + tactics + Math.round(health * 1000)
    + Math.round(Math.max(0, 1 - time / 180) * 2000)
    : Math.min(targets.bronze - 1, Math.round(tactics * .35));
  const boundedScore = Math.floor(finite(score, 0, MAX_SCORE));
  return { score: boundedScore, medal: medalFor(boundedScore, won, targets), won };
}

/** Whitelist at most the last 30 UTC dates. Personal bests are local to this device. */
export function normalizeDaily(input, today = utcToday()) {
  const records = {};
  const source = record(input) && record(input.records) ? input.records : {};
  for (const date of Object.keys(source).filter(date => isDailyDate(date, today)).sort().reverse().slice(0, HISTORY_DAYS)) {
    const value = source[date];
    if (!record(value) || typeof value.score !== 'number' || !Number.isFinite(value.score) || value.score < 0) continue;
    const config = createDaily(date, today), won = value.won === true;
    const score = Math.floor(finite(value.score, 0, won ? MAX_SCORE : config.medalTargets.bronze - 1));
    records[date] = { score, medal: medalFor(score, won, config.medalTargets), won,
      claimed: won && value.claimed === true, attempts: Math.floor(finite(value.attempts, 1, 9999, 1)) };
  }
  return { records };
}

/** Pure update: campaign progress and mastery are never touched or awarded here. */
export function recordDailyResult(input, config, result, today = utcToday()) {
  const daily = normalizeDaily(input, today), canonical = createDaily(config?.date, today);
  const terminal = record(result) && (typeof result.won === 'boolean' && result.status === undefined
    || result.status === 'won' || result.status === 'lost');
  if (!canonical || config?.seed !== canonical.seed || !terminal) return { daily, score: 0, medal: 'none', won: false, reward: 0, improved: false };
  const assessment = scoreDaily(result, canonical), previous = daily.records[canonical.date];
  const score = Math.max(previous?.score || 0, assessment.score), won = previous?.won === true || assessment.won;
  const reward = assessment.won && canonical.date === today && !previous?.claimed ? DAILY_REWARD : 0;
  const improved = !previous || assessment.score > previous.score;
  daily.records[canonical.date] = { score, medal: medalFor(score, won, canonical.medalTargets), won,
    claimed: previous?.claimed === true || reward > 0, attempts: Math.min(9999, (previous?.attempts || 0) + 1) };
  return { daily, ...assessment, reward, improved };
}
