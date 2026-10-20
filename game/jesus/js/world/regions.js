export const BIOME_ORDER = ["countryside", "desert", "city", "mountains", "ruins", "storm"];
export const REGION_ALIASES = { galilee: "countryside", rome: "city", jerusalem: "ruins", hell: "desert", heaven: "storm" };
export function normalizeBiome(value = "countryside") { return REGION_ALIASES[value] || (BIOME_ORDER.includes(value) ? value : "countryside"); }
export const REGIONS = {
  countryside: {
    name: "Galilee Countryside", shortName: "Countryside", lesson: "Wait for the raft, ride, then leap ashore", chunks: 17, previousChunks: 34, legacyChunks: 68,
    enemyPool: ["wanderer", "hound", "zealot", "cave-bat"], bossId: "warBeast",
    mechanics: ["river-crossings", "terraces", "collapse", "branches"], hazardTypes: ["brambles", "water"],
    weather: { kind: "pollen", density: 0.25, wind: 0 },
    props: ["olive-tree", "cypress-tree", "olive-shrub", "tall-grass", "wildflowers", "fence", "amphorae", "hay-bale"],
  },
  desert: {
    name: "Wilderness Desert", shortName: "Desert", lesson: "Ride the pillars above the canyon", chunks: 19, previousChunks: 38, legacyChunks: 76,
    enemyPool: ["hound", "vulture", "leaping-scorpion", "javelin"], bossId: "pitLord",
    mechanics: ["dunes", "oases", "chase", "sandstone"], hazardTypes: ["spikes", "fireJet", "wind"],
    weather: { kind: "sand", density: 0.45, wind: 28 },
    props: ["date-palm", "thorn-tree", "thorn-scrub", "agave", "dry-grass", "fossil-rock", "camp-tent", "water-amphorae"],
  },
  city: {
    name: "Roman City", shortName: "City", lesson: "Watch the mechanisms and take the high road", chunks: 24, previousChunks: 48, legacyChunks: 96,
    enemyPool: ["legionary", "shield-bearer", "javelin", "temple-guard"], bossId: "warBeast",
    mechanics: ["rooflines", "aqueducts", "switches", "arenas"], hazardTypes: ["spikes", "fireJet"],
    weather: { kind: "dust", density: 0.2, wind: 0 },
    props: ["potted-olive", "potted-cypress", "hanging-ivy", "flowerpots", "awning-purple", "market-stall", "amphorae", "market-crates"],
  },
  mountains: {
    name: "Mountain Pass", shortName: "Mountains", lesson: "Ride the ropeway across the ravine", chunks: 25, previousChunks: 50, legacyChunks: 100,
    enemyPool: ["cave-bat", "stone-guardian", "vulture", "tempest-sentinel"], bossId: "celestialTrial",
    mechanics: ["vertical-climbs", "wind", "rope-lifts", "rockfall"], hazardTypes: ["fallingRock", "wind", "spikes"],
    weather: { kind: "snow", density: 0.5, wind: 38 },
    props: ["wind-pine", "cedar", "alpine-shrub", "blue-ferns", "white-flowers", "rope-anchor", "mountain-urns", "timber-crates"],
  },
  ruins: {
    name: "Temple Ruins", shortName: "Ruins", lesson: "Read the ancient mechanisms", chunks: 26, previousChunks: 52, legacyChunks: 104,
    enemyPool: ["zealot", "wraith", "temple-guard", "stone-guardian"], bossId: "templeJudge",
    mechanics: ["secret-chambers", "switches", "collapse", "temple-stairs"], hazardTypes: ["spikes", "fallingRock", "water"],
    weather: { kind: "motes", density: 0.25, wind: 0 },
    props: ["fig-tree", "ivy-column", "ferns", "cave-mushrooms", "ivy-bust", "blue-crystals", "altar", "shattered-amphorae"],
  },
  storm: {
    name: "Storm Summit", shortName: "Storm", lesson: "Time the crossing between lightning strikes", chunks: 27, previousChunks: 54, legacyChunks: 108,
    enemyPool: ["imp", "brim-hound", "fire-skull", "wraith", "tempest-sentinel"], bossId: "celestialTrial",
    mechanics: ["lightning", "gusts", "collapse", "summit-traversal"], hazardTypes: ["lightning", "wind", "fireJet"],
    weather: { kind: "rain", density: 0.85, wind: 52 },
    props: ["wind-pine", "split-tree", "thorn-scrub", "wind-grass", "heather", "wet-moss", "torn-shelter", "fallen-timber"],
  },
};
for (const [alias, biome] of Object.entries(REGION_ALIASES)) Object.defineProperty(REGIONS, alias, { value: REGIONS[biome], enumerable: false });
export function regionForDepth(depth) { return BIOME_ORDER[Math.floor(Math.max(0, depth)) % BIOME_ORDER.length]; }
export function endlessChunkCount(completedRegions = 0) { return Math.min(12, 6 + Math.floor(Math.max(0, completedRegions) / 3)); }
