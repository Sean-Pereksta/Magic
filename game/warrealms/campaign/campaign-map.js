import { CAMPAIGN_BOSSES, campaignBossForRegion } from "./campaign-bosses.js";

export const CAMPAIGN_NODE_TYPES = Object.freeze(["path", "battle", "elite", "boss", "reward", "shop", "event", "rest"]);

export const CAMPAIGN_NODE_ICONS = Object.freeze({
  path: "⌁",
  battle: "⚔",
  elite: "✹",
  boss: "♛",
  reward: "◆",
  shop: "⚒",
  event: "?",
  rest: "✦"
});

export const CAMPAIGN_PATHS = Object.freeze([
  Object.freeze({
    id: "vanguard",
    label: "Vanguard Road",
    risk: "Measured",
    description: "Fight a disciplined warhost with standard regional strength.",
    encounterType: "battle",
    encounter: "realm_vanguard",
    rewardRarity: "common",
    rewardMinCost: 1,
    rewardMaxCost: 3,
    enemyAuthorityMultiplier: .94,
    enemyShield: 0,
    enemyStartingBases: 0,
    enemyBonusTrade: 0,
    enemyBonusCombat: 0,
    enemyHandSize: 0,
    currencyBonus: 0
  }),
  Object.freeze({
    id: "siegebreaker",
    label: "Siegebreaker Pass",
    risk: "Dangerous",
    description: "Challenge a fortified elite for a rarer card and additional spoils.",
    encounterType: "elite",
    encounter: "siegebreaker_host",
    rewardRarity: "uncommon",
    rewardMinCost: 4,
    rewardMaxCost: 5,
    enemyAuthorityMultiplier: 1.1,
    enemyShield: 3,
    enemyStartingBases: 1,
    enemyBonusTrade: 0,
    enemyBonusCombat: 0,
    enemyHandSize: 0,
    currencyBonus: 20
  }),
  Object.freeze({
    id: "rift_gambit",
    label: "Rift Gambit",
    risk: "Severe",
    description: "Enter a volatile elite battle with a larger hand and immediate combat pressure.",
    encounterType: "elite",
    encounter: "rift_gambit_host",
    rewardRarity: "rare",
    rewardMinCost: 5,
    rewardMaxCost: 7,
    enemyAuthorityMultiplier: 1.18,
    enemyShield: 2,
    enemyStartingBases: 1,
    enemyBonusTrade: 0,
    enemyBonusCombat: 1,
    enemyHandSize: 1,
    currencyBonus: 35
  })
]);

const pathMap = new Map(CAMPAIGN_PATHS.map(path => [path.id, path]));

// A region alternates decisions, rewards, recovery, and escalation. War Camp and
// Rest are intentionally separate so currency and recovery both matter.
export const CAMPAIGN_REGION_SEQUENCE = Object.freeze([
  Object.freeze({ type: "path", routeSlot: 1 }),
  Object.freeze({ type: "route", routeSlot: 1, choiceNode: 1 }),
  Object.freeze({ type: "routeReward", routeSlot: 1, choiceNode: 1 }),
  Object.freeze({ type: "path", routeSlot: 2 }),
  Object.freeze({ type: "route", routeSlot: 2, choiceNode: 4 }),
  Object.freeze({ type: "routeReward", routeSlot: 2, choiceNode: 4 }),
  Object.freeze({ type: "event", label: "Campaign Event" }),
  Object.freeze({ type: "shop", label: "War Camp" }),
  Object.freeze({ type: "elite", encounter: "regional_gate", enemyAuthorityMultiplier: 1.1, enemyShield: 4, enemyStartingBases: 1, enemyBonusCombat: 0 }),
  Object.freeze({ type: "reward", rarity: "rare", minCost: 4, maxCost: 6, overrideCostCap: true, milestone: "elite" }),
  Object.freeze({ type: "rest", label: "Rest Before the Boss" }),
  Object.freeze({ type: "boss" }),
  Object.freeze({ type: "reward", bossReward: true })
]);

const MODIFIER_ROTATION = Object.freeze([
  Object.freeze({ id: "armored_front", label: "Armored Front", description: "Enemies begin with additional Shield." }),
  Object.freeze({ id: "accelerated_market", label: "Accelerated Market", description: "Enemy purchases and economy become more aggressive." }),
  Object.freeze({ id: "siege_lines", label: "Siege Lines", description: "Elite encounters begin with a completed Base." }),
  Object.freeze({ id: "relentless", label: "Relentless", description: "Enemy opening hands gain additional pressure." }),
  Object.freeze({ id: "volatile_realm", label: "Volatile Realm", description: "Boss abilities activate on a tighter cadence." })
]);

function hash(text) {
  let value = 2166136261;
  for (const char of String(text || "")) {
    value ^= char.charCodeAt(0);
    value = Math.imul(value, 16777619);
  }
  return value >>> 0;
}

function routeChoiceId(region, nodeNumber) {
  return `region_${region}_node_${nodeNumber}`;
}

function selectedPath(pathChoices, region, choiceNode) {
  return pathMap.get(String(pathChoices?.[routeChoiceId(region, choiceNode)] || "")) || CAMPAIGN_PATHS[0];
}

export function getCampaignPath(pathId) {
  return pathMap.get(String(pathId || "")) || null;
}

export function campaignRegionScaling(region = 1) {
  const safeRegion = Math.max(1, Math.floor(Number(region) || 1));
  const index = safeRegion - 1;
  const tier = safeRegion <= 5 ? 0 : safeRegion <= 10 ? 1 : safeRegion <= 15 ? 2 : safeRegion <= 20 ? 3 : 4 + Math.floor((safeRegion - 21) / 6);
  const modifierCount = Math.min(MODIFIER_ROTATION.length, Math.max(0, Math.floor((safeRegion - 2) / 2)));
  const endlessIndex = Math.max(0, safeRegion - CAMPAIGN_BOSSES.length);
  const modifiers = Array.from({ length: modifierCount }, (_, offset) => {
    const modifier = MODIFIER_ROTATION[(tier + offset) % MODIFIER_ROTATION.length];
    return { ...modifier, rank: 1 + Math.floor((tier + offset) / MODIFIER_ROTATION.length) };
  });
  const baseEnemyAuthority = safeRegion <= 6
    ? 25 + safeRegion * 5
    : safeRegion <= 10
      ? 55 + (safeRegion - 6) * 5
      : safeRegion <= 15
        ? 75 + (safeRegion - 10) * 7
        : 110 + (safeRegion - 15) * 8;
  const enemyBonusCombat = safeRegion <= 3
    ? 0
    : Math.min(8, 1 + Math.floor((safeRegion - 4) / 3));
  const preEndlessAuthorityPressure = Math.min(.25, Math.max(0, safeRegion - 1) * .01);
  return {
    region: safeRegion,
    tier,
    progressBand: safeRegion <= 5 ? "beginner" : safeRegion <= 10 ? "early" : safeRegion <= 15 ? "mid" : safeRegion <= CAMPAIGN_BOSSES.length ? "late" : "endless",
    baseEnemyAuthority,
    enemyAuthorityMultiplier: 1 + preEndlessAuthorityPressure + endlessIndex * .06 + Math.floor(endlessIndex / Math.max(1, CAMPAIGN_BOSSES.length)) * .08,
    enemyBaseHealthMultiplier: 1 + Math.max(0, safeRegion - 4) * .035 + endlessIndex * .015,
    enemyStartingBases: safeRegion <= 4 ? 0 : Math.min(3, 1 + Math.floor((safeRegion - 5) / 6)),
    enemyBonusTrade: safeRegion <= 4 ? 0 : Math.min(4, 1 + Math.floor((safeRegion - 5) / 5)),
    enemyBonusCombat,
    enemyHandSize: Math.min(7, 5 + Math.floor(Math.max(0, safeRegion - 1) / 4)),
    bossAbilityFrequencyReduction: Math.min(1, Math.floor(Math.max(0, safeRegion - 13) / 8) + Math.floor(endlessIndex / 10)),
    rewardQuality: Math.log2(safeRegion + 1),
    modifiers
  };
}

function resolveRegionTemplate(template, safeRegion, pathChoices) {
  if (template.type === "path") {
    return {
      ...template,
      choices: CAMPAIGN_PATHS,
      label: `Choose Route ${template.routeSlot}`
    };
  }
  if (template.type === "route" || template.type === "routeReward") {
    const path = selectedPath(pathChoices, safeRegion, template.choiceNode);
    if (template.type === "routeReward") {
      return {
        type: "reward",
        rarity: path.rewardRarity,
        minCost: path.rewardMinCost,
        maxCost: path.rewardMaxCost,
        overrideCostCap: path.id !== "vanguard",
        starterPurge: safeRegion === 1 && template.routeSlot === 1 ? 1 : 0,
        pathId: path.id,
        routeSlot: template.routeSlot,
        label: `${path.label} Reward`
      };
    }
    return {
      type: path.encounterType,
      encounter: path.encounter,
      pathId: path.id,
      routeSlot: template.routeSlot,
      enemyAuthorityMultiplier: path.enemyAuthorityMultiplier,
      enemyShield: path.enemyShield,
      enemyStartingBases: path.enemyStartingBases,
      enemyBonusTrade: path.enemyBonusTrade,
      enemyBonusCombat: path.enemyBonusCombat,
      enemyHandSize: path.enemyHandSize,
      currencyBonus: path.currencyBonus,
      label: path.label
    };
  }
  if (template.type === "boss") {
    const boss = campaignBossForRegion(safeRegion);
    return { ...template, bossId: boss.id, label: `Region Boss · ${boss.name}` };
  }
  if (template.bossReward) {
    const boss = campaignBossForRegion(safeRegion);
    const beginner = boss.arc === "beginner";
    return {
      ...template,
      rarity: beginner ? (safeRegion <= 2 ? "uncommon" : "rare") : "boss",
      minCost: beginner ? (safeRegion <= 2 ? 3 : 4) : 6,
      maxCost: beginner ? (safeRegion <= 2 ? 5 : 7) : 99,
      overrideCostCap: true,
      grantRelic: safeRegion === 1 ? "Vanguard Standard" : "",
      label: beginner ? "Beginner Boss Reward" : "Boss Reward"
    };
  }
  if (template.type === "reward" && template.milestone === "elite") {
    return { ...template, starterPurge: safeRegion === 1 ? 1 : 0, label: "Elite Reward" };
  }
  return { ...template };
}

export function generateCampaignRegion(region = 1, runId = "campaign", pathChoices = {}) {
  const safeRegion = Math.max(1, Math.floor(Number(region) || 1));
  const seed = hash(`${runId}:${safeRegion}`);
  return CAMPAIGN_REGION_SEQUENCE.map((template, index) => {
    const resolved = resolveRegionTemplate(template, safeRegion, pathChoices);
    return {
      ...resolved,
      id: `region_${safeRegion}_node_${index + 1}`,
      region: safeRegion,
      index,
      seed: (seed + Math.imul(index + 1, 2654435761)) >>> 0,
      label: resolved.label || (resolved.type === "elite"
        ? "Elite Battle"
        : resolved.type.charAt(0).toUpperCase() + resolved.type.slice(1))
    };
  });
}

export function campaignNodeAt(profile) {
  if (!profile) return null;
  return generateCampaignRegion(profile.region, profile.runId, profile.pathChoices)[Math.max(0, Number(profile.nodeIndex) || 0)] || null;
}

export function campaignPathOptions(profile) {
  const node = campaignNodeAt(profile);
  return node?.type === "path" ? [...CAMPAIGN_PATHS] : [];
}

export function campaignNodePresentation(node, profile = {}) {
  if (!node) return { icon: "?", state: "future", type: "unknown", label: "Unknown" };
  const currentIndex = Math.max(0, Number(profile.nodeIndex) || 0);
  const state = node.index < currentIndex ? "complete" : node.index === currentIndex ? "current" : "future";
  return {
    icon: CAMPAIGN_NODE_ICONS[node.type] || "◆",
    state,
    type: node.type,
    label: node.label || node.type,
    routeSlot: Math.max(0, Number(node.routeSlot) || 0),
    pathId: String(node.pathId || ""),
    boss: node.type === "boss"
  };
}

export function chooseCampaignPath(profile, pathId, options = {}) {
  const node = campaignNodeAt(profile);
  const path = getCampaignPath(pathId);
  if (!profile || node?.type !== "path" || !path) return { ok: false, reason: "Choose a valid campaign route." };
  profile.pathChoices = profile.pathChoices && typeof profile.pathChoices === "object" ? profile.pathChoices : {};
  profile.pathChoices[node.id] = path.id;
  profile.history = Array.isArray(profile.history) ? profile.history : [];
  profile.history.push({
    atMs: Number(options.now) || Date.now(),
    type: "CAMPAIGN_PATH_CHOSEN",
    nodeId: node.id,
    pathId: path.id,
    region: profile.region
  });
  profile.history = profile.history.slice(-200);
  advanceCampaignNode(profile);
  return { ok: true, path, nextNode: campaignNodeAt(profile) };
}

export function advanceCampaignNode(profile) {
  if (!profile) return null;
  const nodes = generateCampaignRegion(profile.region, profile.runId, profile.pathChoices);
  profile.nodeIndex = Math.max(0, Number(profile.nodeIndex) || 0) + 1;
  if (profile.nodeIndex >= nodes.length) {
    profile.region = Math.max(1, Number(profile.region) || 1) + 1;
    profile.level = Math.max(Number(profile.level) || 1, profile.region);
    profile.nodeIndex = 0;
  }
  const scaling = campaignRegionScaling(profile.region);
  profile.difficultyScaling = scaling.enemyAuthorityMultiplier;
  return campaignNodeAt(profile);
}
