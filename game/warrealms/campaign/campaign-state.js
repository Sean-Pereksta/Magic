import {
  DEFAULT_COMMANDER_ID,
  applyCommanderStarterDeck,
  defaultCommanderForSpecialization,
  getCampaignCommander,
  normalizeCommanderPerks
} from "./campaign-commanders.js";

export const CAMPAIGN_SCHEMA = 4;
export const CAMPAIGN_STORAGE_KEY = "warRealmsCampaign.v1";

export const CAMPAIGN_SPECIALIZATIONS = Object.freeze([
  Object.freeze({
    id: "warrior",
    label: "Warrior",
    faction: "green",
    description: "Replace one Militia Blade with Knucklebone Rusher for a stronger opening attack.",
    replace: "starter_blade",
    with: "knucklebone_rusher"
  }),
  Object.freeze({
    id: "merchant",
    label: "Merchant",
    faction: "yellow",
    description: "Replace one Realm Coin with Unspent Possibility for a stronger opening economy.",
    replace: "starter_coin",
    with: "unspent_possibility"
  }),
  Object.freeze({
    id: "engineer",
    label: "Engineer",
    faction: "green",
    description: "Replace one Realm Coin with Mudwall Camp, a simple Base that establishes your field.",
    replace: "starter_coin",
    with: "mudwall_camp"
  }),
  Object.freeze({
    id: "mystic",
    label: "Mystic",
    faction: "red",
    description: "Replace one Realm Coin with Ashledger Acolyte so your deck can begin purging weak cards.",
    replace: "starter_coin",
    with: "ashledger_acolyte"
  })
]);

const specializationMap = new Map(CAMPAIGN_SPECIALIZATIONS.map(specialization => [specialization.id, specialization]));

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

export function getCampaignSpecialization(specializationId) {
  return specializationMap.get(String(specializationId || "")) || null;
}

function specializedStarterDeck(starterDeck, specializationId) {
  const deck = strings(starterDeck);
  const specialization = getCampaignSpecialization(specializationId);
  if (!specialization) return deck;
  const replaceIndex = deck.indexOf(specialization.replace);
  if (replaceIndex >= 0) deck.splice(replaceIndex, 1, specialization.with);
  return deck;
}

export function createCampaignProfile(options = {}) {
  const config = { ...DEFAULT_CAMPAIGN_CONFIG, ...(options.config || {}) };
  const specialization = getCampaignSpecialization(options.specialization || config.specialization);
  const requestedCommander = getCampaignCommander(options.commanderId || config.commanderId);
  const commander = requestedCommander
    || (specialization ? defaultCommanderForSpecialization(specialization.id) : null)
    || getCampaignCommander(DEFAULT_COMMANDER_ID);
  const deck = commander
    ? applyCommanderStarterDeck(config.starterDeck, commander)
    : specializedStarterDeck(config.starterDeck, specialization?.id);
  const now = whole(options.now, Date.now());
  const startingAuthority = Math.max(1, whole(config.startingAuthority, 60) + (Number(commander?.startingAuthorityModifier) || 0));
  return {
    schema: CAMPAIGN_SCHEMA,
    runId: String(options.runId || makeRunId(now, options.random ?? Math.random())),
    status: "active",
    region: 1,
    level: 1,
    nodeIndex: 0,
    battlesWon: 0,
    authority: startingAuthority,
    maxAuthority: startingAuthority,
    deck,
    collection: countCards(deck),
    unlockedCards: [...new Set(deck)],
    unlocks: [],
    relics: [],
    currency: whole(config.startingCurrency),
    bossesDefeated: [],
    pathChoices: {},
    specialization: specialization?.id || commander?.legacySpecialization || "",
    commanderId: commander?.id || DEFAULT_COMMANDER_ID,
    commanderLevel: 1,
    commanderPerks: {},
    nextBattleShield: 0,
    starterRemovalsAvailable: 0,
    rewardRerolls: 0,
    rewardSeedOffset: 0,
    warCampPurchases: {},
    difficultyScaling: 1,
    rewardDestination: config.rewardDestination === "collection" ? "collection" : "deck",
    pendingReward: null,
    pendingBattleReport: null,
    stats: {
      heatSpent: 0,
      heatGained: 0,
      basesConstructed: 0,
      cardsTransformed: 0,
      cardsSacrificed: 0,
      tokensCreated: 0,
      starterCardsRemoved: 0
    },
    history: [{
      atMs: now,
      type: "CAMPAIGN_STARTED",
      region: 1,
      specialization: specialization?.id || commander?.legacySpecialization || "",
      commanderId: commander?.id || DEFAULT_COMMANDER_ID
    }],
    createdAtMs: now,
    updatedAtMs: now
  };
}

export function normalizeCampaignProfile(raw, options = {}) {
  if (!raw || typeof raw !== "object") return null;
  const rawSchema = whole(raw.schema, 1);
  const requestedCommander = getCampaignCommander(raw.commanderId);
  const commander = requestedCommander
    || defaultCommanderForSpecialization(raw.specialization)
    || getCampaignCommander(DEFAULT_COMMANDER_ID);
  const fallback = createCampaignProfile({
    ...options,
    commanderId: commander?.id,
    specialization: raw.specialization,
    runId: raw.runId || undefined,
    now: raw.createdAtMs || options.now
  });
  const deck = strings(raw.deck);
  const collection = Object.fromEntries(Object.entries(object(raw.collection)).map(([cardId, count]) => [String(cardId), whole(count)]).filter(([, count]) => count > 0));
  for (const [cardId, count] of Object.entries(countCards(deck))) collection[cardId] = Math.max(whole(collection[cardId]), count);
  const status = ["active", "defeated", "completed"].includes(raw.status) ? raw.status : "active";
  const history = (Array.isArray(raw.history) ? raw.history : fallback.history).filter(entry => entry && typeof entry === "object").slice(-199);
  if (rawSchema < CAMPAIGN_SCHEMA) history.push({
    atMs: whole(raw.updatedAtMs, whole(options.now, Date.now())),
    type: "CAMPAIGN_MIGRATED",
    fromSchema: rawSchema,
    toSchema: CAMPAIGN_SCHEMA,
    commanderId: commander?.id || DEFAULT_COMMANDER_ID
  });
  const legacyNodeIndex = whole(raw.nodeIndex);
  const nodeIndex = rawSchema < 4 && legacyNodeIndex >= 6 ? legacyNodeIndex + 1 : legacyNodeIndex;
  const level = Math.max(1, whole(raw.level, 1));
  return {
    ...fallback,
    ...raw,
    schema: CAMPAIGN_SCHEMA,
    runId: String(raw.runId || fallback.runId),
    status,
    region: Math.max(1, whole(raw.region, 1)),
    level,
    nodeIndex,
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
    pathChoices: Object.fromEntries(Object.entries(object(raw.pathChoices)).map(([nodeId, pathId]) => [String(nodeId), String(pathId)]).filter(([, pathId]) => pathId)),
    specialization: getCampaignSpecialization(raw.specialization)?.id || commander?.legacySpecialization || "",
    commanderId: commander?.id || DEFAULT_COMMANDER_ID,
    commanderLevel: Math.max(level, whole(raw.commanderLevel, level), whole(raw.battlesWon) + 1),
    commanderPerks: normalizeCommanderPerks(commander, raw.commanderPerks),
    nextBattleShield: whole(raw.nextBattleShield),
    starterRemovalsAvailable: whole(raw.starterRemovalsAvailable),
    rewardRerolls: whole(raw.rewardRerolls),
    rewardSeedOffset: whole(raw.rewardSeedOffset),
    warCampPurchases: Object.fromEntries(Object.entries(object(raw.warCampPurchases)).map(([nodeId, purchaseIds]) => [String(nodeId), [...new Set(strings(purchaseIds))]])),
    difficultyScaling: Math.max(1, Number(raw.difficultyScaling) || 1),
    rewardDestination: raw.rewardDestination === "collection" ? "collection" : "deck",
    pendingReward: raw.pendingReward && typeof raw.pendingReward === "object" ? raw.pendingReward : null,
    pendingBattleReport: raw.pendingBattleReport && typeof raw.pendingBattleReport === "object" ? raw.pendingBattleReport : null,
    stats: Object.fromEntries(Object.entries({ ...fallback.stats, ...object(raw.stats) }).map(([key, value]) => [key, whole(value)])),
    history: history.slice(-200),
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

export function removeCampaignDeckCardCopy(profile, cardId, options = {}) {
  if (!profile || !cardId) return { ok: false, reason: "Choose a campaign card to remove." };
  profile.deck = strings(profile.deck);
  if (profile.deck.length <= Math.max(5, whole(options.minimumDeckSize, 5))) {
    return { ok: false, reason: "The campaign deck cannot contain fewer than five cards." };
  }
  const id = String(cardId);
  const index = profile.deck.indexOf(id);
  if (index < 0) return { ok: false, reason: "That card is not in the campaign deck." };
  profile.deck.splice(index, 1);
  profile.stats = { ...object(profile.stats) };
  if (id === "starter_coin" || id === "starter_blade") {
    profile.stats.starterCardsRemoved = whole(profile.stats.starterCardsRemoved) + 1;
  }
  profile.history = Array.isArray(profile.history) ? profile.history : [];
  profile.history.push({
    atMs: Number(options.now) || Date.now(),
    type: "CAMPAIGN_CARD_REMOVED",
    cardId: id,
    method: String(options.method || "campaign")
  });
  profile.history = profile.history.slice(-200);
  return { ok: true, cardId: id };
}

export function useCampaignStarterRemoval(profile, starterId, options = {}) {
  const id = String(starterId || "");
  if (!profile || !["starter_coin", "starter_blade"].includes(id)) return { ok: false, reason: "Choose a Realm Coin or Militia Blade." };
  if (whole(profile.starterRemovalsAvailable) < 1) return { ok: false, reason: "No free starter removal is available." };
  const result = removeCampaignDeckCardCopy(profile, id, { ...options, method: options.method || "victory-cleanup" });
  if (!result.ok) return result;
  profile.starterRemovalsAvailable = whole(profile.starterRemovalsAvailable) - 1;
  return result;
}

export function skipCampaignStarterRemoval(profile, options = {}) {
  if (!profile || whole(profile.starterRemovalsAvailable) < 1) return false;
  profile.starterRemovalsAvailable = whole(profile.starterRemovalsAvailable) - 1;
  profile.history = Array.isArray(profile.history) ? profile.history : [];
  profile.history.push({ atMs: Number(options.now) || Date.now(), type: "CAMPAIGN_STARTER_REMOVAL_SKIPPED" });
  profile.history = profile.history.slice(-200);
  return true;
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
