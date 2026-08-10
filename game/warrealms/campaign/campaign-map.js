export const CAMPAIGN_NODE_TYPES = Object.freeze(["battle", "elite", "boss", "reward", "shop", "event", "rest"]);

export const CAMPAIGN_REGION_SEQUENCE = Object.freeze([
  Object.freeze({ type: "battle", encounter: "border_skirmish" }),
  Object.freeze({ type: "reward", rarity: "common" }),
  Object.freeze({ type: "battle", encounter: "realm_vanguard" }),
  Object.freeze({ type: "reward", rarity: "uncommon" }),
  Object.freeze({ type: "elite", encounter: "fortified_host" }),
  Object.freeze({ type: "reward", rarity: "rare" }),
  Object.freeze({ type: "boss", bossId: "world_eater" }),
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
    enemyAuthorityMultiplier: 1 + index * .06 + tier * .03,
    enemyBaseHealthMultiplier: 1 + index * .045,
    enemyStartingBases: Math.min(3, Math.floor(index / 4)),
    enemyBonusTrade: Math.min(4, Math.floor(index / 6)),
    enemyBonusCombat: Math.min(6, Math.floor(index / 5)),
    enemyHandSize: Math.min(7, 5 + Math.floor(index / 8)),
    bossAbilityFrequencyReduction: Math.min(1, Math.floor(index / 7)),
    rewardQuality: Math.log2(safeRegion + 1),
    modifiers
  };
}

export function generateCampaignRegion(region = 1, runId = "campaign") {
  const safeRegion = Math.max(1, Math.floor(Number(region) || 1));
  const seed = hash(`${runId}:${safeRegion}`);
  return CAMPAIGN_REGION_SEQUENCE.map((template, index) => ({
    ...template,
    id: `region_${safeRegion}_node_${index + 1}`,
    region: safeRegion,
    index,
    seed: (seed + Math.imul(index + 1, 2654435761)) >>> 0,
    label: template.type === "boss"
      ? "Region Boss"
      : template.type === "elite"
        ? "Elite Battle"
        : template.type.charAt(0).toUpperCase() + template.type.slice(1)
  }));
}

export function campaignNodeAt(profile) {
  if (!profile) return null;
  return generateCampaignRegion(profile.region, profile.runId)[Math.max(0, Number(profile.nodeIndex) || 0)] || null;
}

export function advanceCampaignNode(profile) {
  if (!profile) return null;
  const nodes = generateCampaignRegion(profile.region, profile.runId);
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
