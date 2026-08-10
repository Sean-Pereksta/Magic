import { campaignBossForRegion } from "./campaign-bosses.js";

export const CAMPAIGN_NODE_TYPES = Object.freeze(["path", "battle", "elite", "boss", "reward", "shop", "event", "rest"]);

export const CAMPAIGN_PATHS = Object.freeze([
  Object.freeze({
    id: "vanguard",
    label: "Vanguard Road",
    risk: "Measured",
    description: "Fight a disciplined warhost with standard regional strength.",
    encounterType: "battle",
    encounter: "realm_vanguard",
    rewardRarity: "common",
    enemyAuthorityMultiplier: 1,
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
    rewardRarity: "rare",
    enemyAuthorityMultiplier: 1.18,
    enemyShield: 5,
    enemyStartingBases: 1,
    enemyBonusTrade: 0,
    enemyBonusCombat: 2,
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
    enemyAuthorityMultiplier: 1.32,
    enemyShield: 2,
    enemyStartingBases: 1,
    enemyBonusTrade: 0,
    enemyBonusCombat: 4,
    enemyHandSize: 1,
    currencyBonus: 35
  })
]);

const pathMap = new Map(CAMPAIGN_PATHS.map(path => [path.id, path]));

// Two route decisions, a mandatory elite gate, and the regional boss make every
// region longer and more demanding than the original linear eight-node route.
export const CAMPAIGN_REGION_SEQUENCE = Object.freeze([
  Object.freeze({ type: "path", routeSlot: 1 }),
  Object.freeze({ type: "route", routeSlot: 1, choiceNode: 1 }),
  Object.freeze({ type: "routeReward", routeSlot: 1, choiceNode: 1 }),
  Object.freeze({ type: "path", routeSlot: 2 }),
  Object.freeze({ type: "route", routeSlot: 2, choiceNode: 4 }),
  Object.freeze({ type: "routeReward", routeSlot: 2, choiceNode: 4 }),
  Object.freeze({ type: "elite", encounter: "regional_gate", enemyAuthorityMultiplier: 1.22, enemyShield: 6, enemyStartingBases: 1, enemyBonusCombat: 2 }),
  Object.freeze({ type: "reward", rarity: "rare" }),
  Object.freeze({ type: "boss" }),
  Object.freeze({ type: "reward", rarity: "boss" })
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
  const tier = Math.floor(index / 3);
  const modifierCount = Math.min(MODIFIER_ROTATION.length, Math.floor(index / 2));
  const modifiers = Array.from({ length: modifierCount }, (_, offset) => {
    const modifier = MODIFIER_ROTATION[(tier + offset) % MODIFIER_ROTATION.length];
    return { ...modifier, rank: 1 + Math.floor((tier + offset) / MODIFIER_ROTATION.length) };
  });
  return {
    region: safeRegion,
    tier,
    enemyAuthorityMultiplier: 1 + index * .075 + tier * .04,
    enemyBaseHealthMultiplier: 1 + index * .055,
    enemyStartingBases: Math.min(3, Math.floor(index / 4)),
    enemyBonusTrade: Math.min(3, Math.floor(index / 7)),
    enemyBonusCombat: Math.min(8, Math.floor(index / 4)),
    enemyHandSize: Math.min(7, 5 + Math.floor(index / 6)),
    bossAbilityFrequencyReduction: Math.min(2, Math.floor(index / 7)),
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
