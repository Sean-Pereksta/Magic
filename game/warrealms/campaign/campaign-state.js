export const CAMPAIGN_SCHEMA = 1;
export const CAMPAIGN_STORAGE_KEY = "warRealmsCampaign.v1";

export const DEFAULT_CAMPAIGN_CONFIG = Object.freeze({
  startingAuthority: 60,
  startingCurrency: 0,
  starterDeck: Object.freeze([
    ...Array(6).fill("starter_coin"),
    ...Array(4).fill("starter_blade")
  ]),
  rewardDestination: "deck"
});

function whole(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : fallback;
}

function strings(value) {
  return Array.isArray(value) ? value.map(String).filter(Boolean) : [];
}

function object(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function makeRunId(now = Date.now(), random = Math.random()) {
  return `campaign_${Math.floor(now).toString(36)}_${Math.floor(Math.max(0, Math.min(.999999, random)) * 0xffffff).toString(36)}`;
}

function countCards(cardIds) {
  return cardIds.reduce((counts, cardId) => {
    counts[cardId] = whole(counts[cardId]) + 1;
    return counts;
  }, {});
}

export function createCampaignProfile(options = {}) {
  const config = { ...DEFAULT_CAMPAIGN_CONFIG, ...(options.config || {}) };
  const deck = strings(config.starterDeck);
  const now = whole(options.now, Date.now());
  return {
    schema: CAMPAIGN_SCHEMA,
    runId: String(options.runId || makeRunId(now, options.random ?? Math.random())),
    status: "active",
    region: 1,
    level: 1,
    nodeIndex: 0,
    battlesWon: 0,
    authority: Math.max(1, whole(config.startingAuthority, 60)),
    maxAuthority: Math.max(1, whole(config.startingAuthority, 60)),
    deck,
    collection: countCards(deck),
    unlockedCards: [...new Set(deck)],
    unlocks: [],
    relics: [],
    currency: whole(config.startingCurrency),
    bossesDefeated: [],
    difficultyScaling: 1,
    rewardDestination: config.rewardDestination === "collection" ? "collection" : "deck",
    pendingReward: null,
    stats: {
      heatSpent: 0,
      heatGained: 0,
      basesConstructed: 0,
      cardsTransformed: 0,
      cardsSacrificed: 0,
      tokensCreated: 0
    },
    history: [{ atMs: now, type: "CAMPAIGN_STARTED", region: 1 }],
    createdAtMs: now,
    updatedAtMs: now
  };
}

export function normalizeCampaignProfile(raw, options = {}) {
  if (!raw || typeof raw !== "object") return null;
  const fallback = createCampaignProfile({ ...options, runId: raw.runId || undefined, now: raw.createdAtMs || options.now });
  const deck = strings(raw.deck);
  const collection = Object.fromEntries(Object.entries(object(raw.collection)).map(([cardId, count]) => [String(cardId), whole(count)]).filter(([, count]) => count > 0));
  for (const [cardId, count] of Object.entries(countCards(deck))) collection[cardId] = Math.max(whole(collection[cardId]), count);
  const status = ["active", "defeated", "completed"].includes(raw.status) ? raw.status : "active";
  return {
    ...fallback,
    ...raw,
    schema: CAMPAIGN_SCHEMA,
    runId: String(raw.runId || fallback.runId),
    status,
    region: Math.max(1, whole(raw.region, 1)),
    level: Math.max(1, whole(raw.level, 1)),
    nodeIndex: whole(raw.nodeIndex),
    battlesWon: whole(raw.battlesWon),
    authority: Math.max(0, whole(raw.authority, fallback.authority)),
    maxAuthority: Math.max(1, whole(raw.maxAuthority, fallback.maxAuthority)),
    deck: deck.length ? deck : fallback.deck,
    collection,
    unlockedCards: [...new Set(strings(raw.unlockedCards))],
    unlocks: [...new Set(strings(raw.unlocks))],
    relics: strings(raw.relics),
    currency: whole(raw.currency),
    bossesDefeated: [...new Set(strings(raw.bossesDefeated))],
    difficultyScaling: Math.max(1, Number(raw.difficultyScaling) || 1),
    rewardDestination: raw.rewardDestination === "collection" ? "collection" : "deck",
    pendingReward: raw.pendingReward && typeof raw.pendingReward === "object" ? raw.pendingReward : null,
    stats: Object.fromEntries(Object.entries({ ...fallback.stats, ...object(raw.stats) }).map(([key, value]) => [key, whole(value)])),
    history: (Array.isArray(raw.history) ? raw.history : fallback.history).filter(entry => entry && typeof entry === "object").slice(-200),
    createdAtMs: whole(raw.createdAtMs, fallback.createdAtMs),
    updatedAtMs: whole(raw.updatedAtMs, fallback.updatedAtMs)
  };
}

export function addCampaignCard(profile, cardId, destination = profile?.rewardDestination || "deck") {
  if (!profile || !cardId) return profile;
  const id = String(cardId);
  profile.collection = object(profile.collection);
  profile.collection[id] = whole(profile.collection[id]) + 1;
  if (destination !== "collection") {
    profile.deck = strings(profile.deck);
    profile.deck.push(id);
  }
  profile.unlockedCards = [...new Set([...strings(profile.unlockedCards), id])];
  return profile;
}

export function campaignStorageAvailable(storage = globalThis.localStorage) {
  return !!storage && typeof storage.getItem === "function" && typeof storage.setItem === "function";
}

export function loadCampaignProfile(storage = globalThis.localStorage) {
  if (!campaignStorageAvailable(storage)) return null;
  try {
    const value = storage.getItem(CAMPAIGN_STORAGE_KEY);
    return value ? normalizeCampaignProfile(JSON.parse(value)) : null;
  } catch {
    return null;
  }
}

export function saveCampaignProfile(profile, storage = globalThis.localStorage, now = Date.now()) {
  if (!campaignStorageAvailable(storage) || !profile) return false;
  profile.updatedAtMs = whole(now, Date.now());
  storage.setItem(CAMPAIGN_STORAGE_KEY, JSON.stringify(profile));
  return true;
}

export function deleteCampaignProfile(storage = globalThis.localStorage) {
  if (!storage || typeof storage.removeItem !== "function") return false;
  storage.removeItem(CAMPAIGN_STORAGE_KEY);
  return true;
}
