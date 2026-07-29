export const WAR_REALMS_CARD_VERSION = 5;
export const COMMAND_DECK_SIZE = 50;
export const MAX_COPIES_PER_CARD = 4;

export const FACTIONS = Object.freeze({
  yellow: {
    id: "yellow",
    name: "Xythe Concord",
    title: "Alien Mystic Beasts",
    identity: "Control, disabling, forced discards, and market manipulation.",
    icon: "◈"
  },
  blue: {
    id: "blue",
    name: "Azure Ascendancy",
    title: "Warrior-Priest Order",
    identity: "Healing, trade, resilient bases, and sustained value.",
    icon: "✦"
  },
  green: {
    id: "green",
    name: "Gorak Warhost",
    title: "Orcish Brutes",
    identity: "Heavy combat, explosive ally bonuses, and relentless pressure.",
    icon: "⬢"
  },
  red: {
    id: "red",
    name: "Umbral Covenant",
    title: "Dark Sorcerers",
    identity: "Permanent deck scrapping, sacrifice, and ruthless conversion of cards into power.",
    icon: "◒"
  },
  neutral: {
    id: "neutral",
    name: "Unaligned",
    title: "Starter Forces",
    identity: "Basic economy and combat cards used only in starting decks.",
    icon: "◆"
  }
});

export const STARTER_CARDS = Object.freeze({
  starter_coin: {
    id: "starter_coin",
    name: "Realm Coin",
    faction: "neutral",
    cost: 0,
    shop_cost: 3,
    type: "ship",
    sigil: "◆",
    effect: { trade: 1 },
    ally: {},
    text: "Gain 1 Trade.",
    allyText: "",
    flavor: "A stamped promise accepted in every war market."
  },
  starter_blade: {
    id: "starter_blade",
    name: "Militia Blade",
    faction: "neutral",
    cost: 0,
    shop_cost: 3,
    type: "ship",
    sigil: "⚔",
    effect: { combat: 1 },
    ally: {},
    text: "Gain 1 Combat.",
    allyText: "",
    flavor: "Every realm begins with someone willing to draw steel."
  }
});

export const CARDS = Object.freeze([
  // ==========================================================
  // YELLOW — XYTHE CONCORD
  // ==========================================================
  {
    id: "starwhisper_scout",
    name: "Starwhisper Scout",
    faction: "yellow",
    cost: 1,
    shop_cost: 15,
    type: "ship",
    sigil: "⌬",
    effect: { trade: 1 },
    ally: { draw: 1 },
    text: "Gain 1 Trade.",
    allyText: "Draw 1 card.",
    flavor: "It listens to routes that have not happened yet."
  },
  {
    id: "neural_leash",
    name: "Neural Leash",
    faction: "yellow",
    cost: 2,
    shop_cost: 25,
    type: "ship",
    sigil: "⌁",
    effect: { combat: 1, opponentDiscard: 1 },
    ally: { trade: 1 },
    text: "Gain 1 Combat. The next enemy draws 1 fewer card.",
    allyText: "Gain 1 Trade.",
    flavor: "The victim remembers choosing obedience."
  },
  {
    id: "phase_warden",
    name: "Phase Warden",
    faction: "yellow",
    cost: 2,
    shop_cost: 25,
    type: "base",
    defense: 4,
    outpost: true,
    sigil: "⟁",
    effect: { trade: 1 },
    ally: { stun: 1 },
    text: "Outpost. Gain 1 Trade.",
    allyText: "Gain 1 Disable.",
    flavor: "It occupies every doorway at once."
  },
  {
    id: "gravity_hook",
    name: "Gravity Hook",
    faction: "yellow",
    cost: 3,
    shop_cost: 30,
    type: "ship",
    sigil: "◉",
    effect: { combat: 3, stun: 1 },
    ally: {},
    text: "Gain 3 Combat and 1 Disable.",
    allyText: "",
    flavor: "The Concord does not chase prey; it edits distance."
  },
  {
    id: "mirage_broker",
    name: "Mirage Broker",
    faction: "yellow",
    cost: 3,
    shop_cost: 25,
    type: "ship",
    sigil: "◫",
    effect: { trade: 2, heal: 1 },
    ally: { draw: 1 },
    text: "Gain 2 Trade and 1 Authority.",
    allyText: "Draw 1 card.",
    flavor: "Its contracts remain binding even when the signer never existed."
  },
  {
    id: "null_prism",
    name: "Null Prism",
    faction: "yellow",
    cost: 3,
    shop_cost: 35,
    type: "base",
    defense: 5,
    outpost: false,
    sigil: "⧈",
    effect: { trade: 1, stun: 1 },
    ally: { combat: 2 },
    text: "Gain 1 Trade and 1 Disable.",
    allyText: "Gain 2 Combat.",
    flavor: "Within its shadow, mechanisms forget their purpose."
  },
  {
    id: "signal_eater",
    name: "Signal Eater",
    faction: "yellow",
    cost: 4,
    shop_cost: 45,
    type: "ship",
    sigil: "⌾",
    effect: { trade: 2, opponentDiscard: 1 },
    ally: {},
    sacrifice: { draw: 1, trade: 2 },
    text: "Gain 2 Trade. The next enemy draws 1 fewer card.",
    allyText: "",
    sacrificeText: "Sacrifice: Draw 1 card and gain 2 Trade.",
    flavor: "It feeds on warnings before they can become knowledge."
  },
  {
    id: "chrono_beast",
    name: "Chrono Beast",
    faction: "yellow",
    cost: 4,
    shop_cost: 35,
    type: "ship",
    sigil: "◴",
    effect: { combat: 4 },
    ally: { opponentDiscard: 1 },
    text: "Gain 4 Combat.",
    allyText: "The next enemy draws 1 fewer card.",
    flavor: "Its wounds arrive before its claws."
  },
  {
    id: "dominion_relay",
    name: "Dominion Relay",
    faction: "yellow",
    cost: 4,
    shop_cost: 40,
    type: "base",
    defense: 5,
    outpost: false,
    sigil: "⬡",
    effect: { trade: 2 },
    ally: { opponentDiscard: 1 },
    text: "Gain 2 Trade.",
    allyText: "The next enemy draws 1 fewer card.",
    flavor: "A thought repeated across a thousand obedient minds."
  },
   {
    id: "paradox_warden",
    name: "Paradox Warden",
    faction: "yellow",
    cost: 5,
    shop_cost: 60,
    type: "base",
    defense: 6,
    outpost: true,
    sigil: "◈",
    effect: { shield: 4 },
    ally: { stun: 1 },
    sacrifice: { draw: 1 },
    text: "Outpost. Gain 4 Shield.",
    allyText: "Gain 1 Disable.",
    sacrificeText: "Sacrifice: Draw 1 card.",
    flavor: "The attack arrives. The Warden decides it never did."
  },
  {
    id: "causal_severer",
    name: "Causal Severer",
    faction: "yellow",
    cost: 7,
    shop_cost: 140,
    type: "ship",
    sigil: "◈",
    effect: { combat: 5, destroyBase: 1 },
    ally: { opponentDiscard: 1 },
    text: "Gain 5 Combat and Raze 1.",
    allyText: "The next enemy draws 1 fewer card.",
    flavor: "It does not destroy the fortress. It removes the reason it was built."
  },
  {
    id: "aegis_tide_saint",
    name: "Aegis Tide Saint",
    faction: "blue",
    cost: 5,
    shop_cost: 60,
    type: "ship",
    sigil: "✦",
    effect: { combat: 3, heal: 2, lifelink: 0.5 },
    ally: { combat: 2 },
    text: "Gain 3 Combat, 2 Authority, and Lifelink 50% this turn.",
    allyText: "Gain 2 Combat.",
    flavor: "Every wound answered by the tide becomes strength for the faithful."
  },
  {
    id: "basilica_of_many_lights",
    name: "Basilica of Many Lights",
    faction: "blue",
    cost: 6,
    shop_cost: 85,
    type: "base",
    defense: 7,
    outpost: false,
    sigil: "✦",
    effect: { trade: 1, healPerBase: 1 },
    ally: { shield: 3 },
    text: "Gain 1 Trade and 1 Authority for each active Base you control.",
    allyText: "Gain 3 Shield.",
    flavor: "One sanctuary is refuge. Many sanctuaries become a kingdom."
  },
  {
    id: "reliquary_shieldfleet",
    name: "Reliquary Shieldfleet",
    faction: "blue",
    cost: 7,
    shop_cost: 130,
    type: "base",
    defense: 7,
    outpost: true,
    sigil: "✦",
    effect: { combat: 2, shield: 5 },
    ally: { draw: 1 },
    sacrifice: { heal: 6 },
    text: "Outpost. Gain 2 Combat and 5 Shield.",
    allyText: "Draw 1 card.",
    sacrificeText: "Sacrifice: Gain 6 Authority.",
    flavor: "The relic travels only where an entire fleet is willing to stand."
  },
  {
    id: "warcamp_foundry",
    name: "Warcamp Foundry",
    faction: "green",
    cost: 4,
    shop_cost: 45,
    type: "base",
    defense: 5,
    outpost: false,
    sigil: "⬢",
    effect: { tradePerBase: 1 },
    ally: { combat: 2 },
    sacrifice: { trade: 3 },
    text: "Gain 1 Trade for each active Base you control.",
    allyText: "Gain 2 Combat.",
    sacrificeText: "Sacrifice: Gain 3 Trade.",
    flavor: "Every camp feeds the forge. Every forge feeds the war."
  },
  {
    id: "siege_root_colossus",
    name: "Siege-Root Colossus",
    faction: "green",
    cost: 7,
    shop_cost: 140,
    type: "ship",
    sigil: "⬢",
    effect: { combat: 4, combatPerBase: 2 },
    ally: { damageAll: 1 },
    text: "Gain 4 Combat plus 2 Combat for each active Base you control.",
    allyText: "Damage All 1.",
    flavor: "Every wall behind it becomes another fist."
  },
  {
    id: "gatebreaker_alpha",
    name: "Gatebreaker Alpha",
    faction: "green",
    cost: 6,
    shop_cost: 100,
    type: "ship",
    sigil: "⬢",
    effect: { combat: 5, destroyBase: 1 },
    ally: { combat: 3 },
    text: "Gain 5 Combat and Raze 1.",
    allyText: "Gain 3 Combat.",
    flavor: "A locked gate is simply a challenge written in metal."
  },
  {
    id: "eclipse_harrower",
    name: "Eclipse Harrower",
    faction: "red",
    cost: 7,
    shop_cost: 145,
    type: "ship",
    sigil: "◒",
    effect: { combat: 6, scrapOwn: 1, lifelink: 0.5 },
    ally: { draw: 1 },
    sacrifice: { damageAll: 2 },
    text: "Gain 6 Combat, 1 Purge, and Lifelink 50% this turn.",
    allyText: "Draw 1 card.",
    sacrificeText: "Sacrifice: Damage All 2.",
    flavor: "The Covenant wastes neither victory nor what remains after it."
  },
  {
    id: "ashen_tithe_engine",
    name: "Ashen Tithe Engine",
    faction: "red",
    cost: 5,
    shop_cost: 70,
    type: "base",
    defense: 5,
    outpost: false,
    sigil: "◒",
    effect: { damageAll: 1, scrapOwn: 1 },
    ally: { combat: 2 },
    sacrifice: { draw: 1 },
    text: "Damage All 1 and gain 1 Purge.",
    allyText: "Gain 2 Combat.",
    sacrificeText: "Sacrifice: Draw 1 card.",
    flavor: "Every discarded past becomes a debt collected from the living."
  },
  {
    id: "rift_manta",
    name: "Rift Manta",
    faction: "yellow",
    cost: 5,
    shop_cost: 55,
    type: "ship",
    sigil: "⌇",
    effect: { combat: 5, stun: 1 },
    ally: { trade: 1 },
    text: "Gain 5 Combat and 1 Disable.",
    allyText: "Gain 1 Trade.",
    flavor: "It swims through the seams between decisions."
  },
  {
    id: "mind_crown",
    name: "Mind Crown",
    faction: "yellow",
    cost: 5,
    shop_cost: 60,
    type: "base",
    defense: 6,
    outpost: true,
    sigil: "♛",
    effect: { stun: 1 },
    ally: { draw: 1 },
    text: "Outpost. Gain 1 Disable.",
    allyText: "Draw 1 card.",
    flavor: "No army passes until the Crown permits the idea."
  },
  {
    id: "astral_taxer",
    name: "Astral Taxer",
    faction: "yellow",
    cost: 5,
    shop_cost: 55,
    type: "ship",
    sigil: "¤",
    effect: { trade: 3, opponentDiscard: 1 },
    ally: { combat: 2 },
    text: "Gain 3 Trade. The next enemy draws 1 fewer card.",
    allyText: "Gain 2 Combat.",
    flavor: "Every passage has a toll, including passage through time."
  },
  {
    id: "sovereign_dreadnought",
    name: "Sovereign Dreadnought",
    faction: "yellow",
    cost: 6,
    shop_cost: 90,
    type: "ship",
    sigil: "◈",
    effect: { combat: 7, opponentDiscard: 1 },
    ally: { stun: 1 },
    text: "Gain 7 Combat. The next enemy draws 1 fewer card.",
    allyText: "Gain 1 Disable.",
    flavor: "A mobile decree backed by impossible weapons."
  },
  {
    id: "reality_harvester",
    name: "Reality Harvester",
    faction: "yellow",
    cost: 7,
    shop_cost: 125,
    type: "ship",
    sigil: "◍",
    effect: { trade: 3, combat: 3 },
    ally: { draw: 1, stun: 1 },
    sacrifice: { draw: 1, trade: 3 },
    text: "Gain 3 Trade and 3 Combat.",
    allyText: "Draw 1 card and gain 1 Disable.",
    sacrificeText: "Sacrifice: Draw 1 card and gain 3 Trade.",
    flavor: "It collects outcomes and leaves causes behind."
  },
  {
    id: "void_leviathan",
    name: "Void Leviathan",
    faction: "yellow",
    cost: 8,
    shop_cost: 185,
    type: "ship",
    sigil: "∞",
    effect: { combat: 8, stun: 2 },
    ally: { opponentDiscard: 1, draw: 1 },
    text: "Gain 8 Combat and 2 Disable.",
    allyText: "Draw 1 card. The next enemy draws 1 fewer card.",
    flavor: "When it opens its eyes, fleets lose their next move."
  },
  {
    id: "threadseer_familiar",
    name: "Threadseer Familiar",
    faction: "yellow",
    cost: 3,
    shop_cost: 25,
    type: "ship",
    sigil: "⌘",
    effect: { draw: 1 },
    ally: { trade: 1 },
    text: "Draw 1 card.",
    allyText: "Gain 1 Trade.",
    flavor: "It tugs one useful possibility out of a thousand futures."
  },
  {
    id: "dreamfold_navigator",
    name: "Dreamfold Navigator",
    faction: "yellow",
    cost: 4,
    shop_cost: 40,
    type: "ship",
    sigil: "◒",
    effect: { combat: 2, draw: 1 },
    ally: { stun: 1 },
    text: "Gain 2 Combat and draw 1 card.",
    allyText: "Gain 1 Disable.",
    flavor: "It plots routes through thoughts no enemy has finished thinking."
  },
  {
    id: "probability_nursery",
    name: "Probability Nursery",
    faction: "yellow",
    cost: 5,
    shop_cost: 55,
    type: "base",
    defense: 5,
    outpost: false,
    sigil: "❉",
    effect: { trade: 1 },
    ally: { draw: 1 },
    sacrifice: { draw: 2 },
    text: "Gain 1 Trade.",
    allyText: "Draw 1 card.",
    sacrificeText: "Sacrifice: Draw 2 cards.",
    flavor: "Young futures are cultivated until one becomes useful."
  },
  {
    id: "many_mind_oracle",
    name: "Many-Mind Oracle",
    faction: "yellow",
    cost: 7,
    shop_cost: 130,
    type: "ship",
    sigil: "⟡",
    effect: { trade: 2, draw: 2 },
    ally: { opponentDiscard: 1 },
    text: "Gain 2 Trade and draw 2 cards.",
    allyText: "The next enemy draws 1 fewer card.",
    flavor: "Every mind offers an answer; the Oracle keeps only the winning ones."
  },
  {
    id: "echo_shepherd",
    name: "Echo Shepherd",
    faction: "yellow",
    cost: 3,
    shop_cost: 35,
    type: "ship",
    sigil: "⌇",
    effect: { trade: 1, opponentDiscard: 1 },
    ally: { draw: 1 },
    text: "Gain 1 Trade. The next enemy draws 1 fewer card.",
    allyText: "Draw 1 card.",
    flavor: "It gathers abandoned thoughts and sends them back as commands."
  },
  {
    id: "stasis_menagerie",
    name: "Stasis Menagerie",
    faction: "yellow",
    cost: 5,
    shop_cost: 60,
    type: "base",
    defense: 6,
    outpost: false,
    sigil: "◫",
    effect: { stun: 1 },
    ally: { trade: 2 },
    sacrifice: { draw: 2 },
    text: "Gain 1 Disable.",
    allyText: "Gain 2 Trade.",
    sacrificeText: "Sacrifice: Draw 2 cards.",
    flavor: "Its creatures wait between moments, perfectly awake and perfectly still."
  },
  {
    id: "fate_tether_colossus",
    name: "Fate-Tether Colossus",
    faction: "yellow",
    cost: 7,
    shop_cost: 130,
    type: "ship",
    sigil: "⟡",
    effect: { combat: 6, stun: 1 },
    ally: { draw: 1, opponentDiscard: 1 },
    text: "Gain 6 Combat and 1 Disable.",
    allyText: "Draw 1 card. The next enemy draws 1 fewer card.",
    flavor: "Every chain fastened to it ends around an enemy possibility."
  },

  {
    id: "hourglass_devourer",
    name: "Hourglass Devourer",
    faction: "yellow",
    cost: 4,
    shop_cost: 35,
    type: "ship",
    sigil: "◈",
    effect: { combat: 2, shield: 2 },
    ally: { stun: 1 },
    text: "Gain 2 Combat and 2 Shield.",
    allyText: "Gain 1 Disable.",
    flavor: "It does not stop time. It eats the moment in which resistance began."
  },
  {
    id: "archive_swarm",
    name: "Archive Swarm",
    faction: "yellow",
    cost: 6,
    shop_cost: 95,
    type: "ship",
    sigil: "◈",
    effect: { trade: 2, opponentDiscard: 1 },
    ally: { drawPerBase: 1 },
    text: "Gain 2 Trade. The next enemy draws 1 fewer card.",
    allyText: "Draw 1 card for each active Base you control.",
    flavor: "Every structure remembers. The swarm simply gathers the testimony."
  },
  {
    id: "paradox_gate",
    name: "Paradox Gate",
    faction: "yellow",
    cost: 6,
    shop_cost: 90,
    type: "base",
    defense: 6,
    outpost: true,
    sigil: "◈",
    effect: { shield: 3 },
    ally: { trade: 1 },
    sacrifice: { destroyBase: 1 },
    text: "Outpost. Gain 3 Shield.",
    allyText: "Gain 1 Trade.",
    sacrificeText: "Sacrifice: Gain 1 Raze.",
    flavor: "The gate cannot be breached, because one version of it was never built."
  },

  // ==========================================================
  // BLUE — AZURE ASCENDANCY
  // ==========================================================
  {
    id: "tithe_acolyte",
    name: "Tithe Acolyte",
    faction: "blue",
    cost: 1,
    shop_cost: 10,
    type: "ship",
    sigil: "✧",
    effect: { trade: 1, heal: 1 },
    ally: {},
    text: "Gain 1 Trade and 1 Authority.",
    allyText: "",
    flavor: "Every coin is a prayer made tangible."
  },
  {
    id: "pilgrim_of_dawn",
    name: "Pilgrim of Dawn",
    faction: "blue",
    cost: 2,
    shop_cost: 15,
    type: "ship",
    sigil: "☼",
    effect: { trade: 1, heal: 2 },
    ally: { trade: 1 },
    text: "Gain 1 Trade and 2 Authority.",
    allyText: "Gain 1 Trade.",
    flavor: "The road is sacred because someone must walk it first."
  },
  {
    id: "votive_scribe",
    name: "Votive Scribe",
    faction: "blue",
    cost: 2,
    shop_cost: 20,
    type: "ship",
    sigil: "✎",
    effect: { heal: 2 },
    ally: { draw: 1 },
    text: "Gain 2 Authority.",
    allyText: "Draw 1 card.",
    flavor: "Names written in azure ink are difficult for death to claim."
  },
  {
    id: "seraphic_envoy",
    name: "Seraphic Envoy",
    faction: "blue",
    cost: 2,
    shop_cost: 15,
    type: "ship",
    sigil: "𓆩✧𓆪",
    effect: { trade: 2, heal: 1 },
    ally: { heal: 2 },
    text: "Gain 2 Trade and 1 Authority.",
    allyText: "Gain 2 Authority.",
    flavor: "Its arrival converts panic into procession."
  },
  {
    id: "azure_chapel",
    name: "Azure Chapel",
    faction: "blue",
    cost: 3,
    shop_cost: 25,
    type: "base",
    defense: 4,
    outpost: false,
    sigil: "◇",
    effect: { trade: 1, heal: 2 },
    ally: {},
    text: "Gain 1 Trade and 2 Authority.",
    allyText: "",
    flavor: "A quiet sanctuary built in the path of war."
  },
  {
    id: "lumen_knight",
    name: "Lumen Knight",
    faction: "blue",
    cost: 3,
    shop_cost: 25,
    type: "ship",
    sigil: "♰",
    effect: { combat: 3, heal: 2 },
    ally: { combat: 2 },
    text: "Gain 3 Combat and 2 Authority.",
    allyText: "Gain 2 Combat.",
    flavor: "The sword is only holy when it shields another."
  },
  {
    id: "reliquary_guard",
    name: "Reliquary Guard",
    faction: "blue",
    cost: 3,
    shop_cost: 30,
    type: "base",
    defense: 5,
    outpost: true,
    sigil: "✥",
    effect: { heal: 2 },
    ally: { trade: 1 },
    sacrifice: { trade: 2, heal: 3 },
    text: "Outpost. Gain 2 Authority.",
    allyText: "Gain 1 Trade.",
    sacrificeText: "Sacrifice: Gain 2 Trade and 3 Authority.",
    flavor: "The relic is protected by those who have become relics themselves."
  },
  {
    id: "choir_of_wings",
    name: "Choir of Wings",
    faction: "blue",
    cost: 4,
    shop_cost: 40,
    type: "ship",
    sigil: "𓆩♫𓆪",
    effect: { trade: 2, combat: 2, heal: 3 },
    ally: { draw: 1 },
    text: "Gain 2 Trade, 2 Combat, and 3 Authority.",
    allyText: "Draw 1 card.",
    flavor: "Their hymn is heard as courage by allies and thunder by enemies."
  },
  {
    id: "absolution_choir",
    name: "Absolution Choir",
    faction: "blue",
    cost: 4,
    shop_cost: 40,
    type: "ship",
    sigil: "♫",
    effect: { heal: 4, trade: 1 },
    ally: { draw: 1 },
    sacrifice: { draw: 1 },
    text: "Gain 4 Authority and 1 Trade.",
    allyText: "Draw 1 card.",
    sacrificeText: "Sacrifice: Draw 1 card.",
    flavor: "Forgiveness is another kind of reinforcement."
  },
  {
    id: "dawn_bastion",
    name: "Dawn Bastion",
    faction: "blue",
    cost: 5,
    shop_cost: 55,
    type: "base",
    defense: 6,
    outpost: true,
    sigil: "▣",
    effect: { trade: 2, heal: 3 },
    ally: { combat: 1 },
    text: "Outpost. Gain 2 Trade and 3 Authority.",
    allyText: "Gain 1 Combat.",
    flavor: "Its gates open with the sun and close against despair."
  },
  {
    id: "mercy_herald",
    name: "Mercy Herald",
    faction: "blue",
    cost: 5,
    shop_cost: 50,
    type: "ship",
    sigil: "✤",
    effect: { combat: 3, heal: 5 },
    ally: { trade: 2 },
    text: "Gain 3 Combat and 5 Authority.",
    allyText: "Gain 2 Trade.",
    flavor: "Mercy arrives armored because the world is not."
  },
  {
    id: "throne_of_tides",
    name: "Throne of Tides",
    faction: "blue",
    cost: 6,
    shop_cost: 90,
    type: "base",
    defense: 7,
    outpost: true,
    sigil: "♛",
    effect: { trade: 3, heal: 4 },
    ally: { draw: 1 },
    text: "Outpost. Gain 3 Trade and 4 Authority.",
    allyText: "Draw 1 card.",
    flavor: "The faithful call it a throne; enemies call it a wall."
  },
  {
    id: "saint_of_the_deep",
    name: "Saint of the Deep",
    faction: "blue",
    cost: 6,
    shop_cost: 85,
    type: "ship",
    sigil: "𓆩♢𓆪",
    effect: { combat: 4, heal: 6 },
    ally: { combat: 2 },
    sacrifice: { heal: 7 },
    text: "Gain 4 Combat and 6 Authority.",
    allyText: "Gain 2 Combat.",
    sacrificeText: "Sacrifice: Gain 7 Authority.",
    flavor: "It carries a cathedral beneath the sea of stars."
  },
  {
    id: "golden_litany",
    name: "Golden Litany",
    faction: "blue",
    cost: 7,
    shop_cost: 120,
    type: "ship",
    sigil: "☀",
    effect: { trade: 4, heal: 4 },
    ally: { draw: 1, combat: 2 },
    text: "Gain 4 Trade and 4 Authority.",
    allyText: "Draw 1 card and gain 2 Combat.",
    flavor: "The prayer is long because empire is expensive."
  },
  {
    id: "ascendant_host",
    name: "Ascendant Host",
    faction: "blue",
    cost: 8,
    shop_cost: 170,
    type: "ship",
    sigil: "✺",
    effect: { combat: 6, heal: 6 },
    ally: { draw: 2 },
    text: "Gain 6 Combat and 6 Authority.",
    allyText: "Draw 2 cards.",
    flavor: "An army that marches as though the victory were already remembered."
  },
  {
    id: "radiant_lancer",
    name: "Radiant Lancer",
    faction: "blue",
    cost: 3,
    shop_cost: 25,
    type: "ship",
    sigil: "⚜",
    effect: { combat: 4 },
    ally: { draw: 1 },
    text: "Gain 4 Combat.",
    allyText: "Draw 1 card.",
    flavor: "Its charge begins as a prayer and ends as a verdict."
  },
  {
    id: "battle_psalmist",
    name: "Battle Psalmist",
    faction: "blue",
    cost: 4,
    shop_cost: 40,
    type: "ship",
    sigil: "♪",
    effect: { combat: 3, draw: 1 },
    ally: { heal: 2 },
    text: "Gain 3 Combat and draw 1 card.",
    allyText: "Gain 2 Authority.",
    flavor: "Each verse names the next strike before it falls."
  },
  {
    id: "aegis_gunship",
    name: "Aegis Gunship",
    faction: "blue",
    cost: 5,
    shop_cost: 55,
    type: "base",
    defense: 6,
    outpost: true,
    sigil: "✠",
    effect: { combat: 3 },
    ally: { draw: 1 },
    text: "Outpost. Gain 3 Combat.",
    allyText: "Draw 1 card.",
    flavor: "A flying sanctuary whose bells answer with cannon fire."
  },
  {
    id: "judgment_seraph",
    name: "Judgment Seraph",
    faction: "blue",
    cost: 7,
    shop_cost: 125,
    type: "ship",
    sigil: "𓆩⚔𓆪",
    effect: { combat: 7, draw: 1 },
    ally: { combat: 3 },
    text: "Gain 7 Combat and draw 1 card.",
    allyText: "Gain 3 Combat.",
    flavor: "Its wings open only when mercy has finished speaking."
  },
  {
    id: "tidebound_chaplain",
    name: "Tidebound Chaplain",
    faction: "blue",
    cost: 2,
    shop_cost: 15,
    type: "ship",
    sigil: "♢",
    effect: { trade: 1, heal: 2 },
    ally: { combat: 1 },
    text: "Gain 1 Trade and 2 Authority.",
    allyText: "Gain 1 Combat.",
    flavor: "Its blessings rise and fall with the sacred tide."
  },
  {
    id: "beacon_monastery",
    name: "Beacon Monastery",
    faction: "blue",
    cost: 4,
    shop_cost: 40,
    type: "base",
    defense: 5,
    outpost: false,
    sigil: "✧",
    effect: { heal: 3 },
    ally: { draw: 1 },
    sacrifice: { trade: 3, heal: 3 },
    text: "Gain 3 Authority.",
    allyText: "Draw 1 card.",
    sacrificeText: "Sacrifice: Gain 3 Trade and 3 Authority.",
    flavor: "Its final bell is rung only when the faithful need a road home."
  },
  {
    id: "oathwing_marshal",
    name: "Oathwing Marshal",
    faction: "blue",
    cost: 6,
    shop_cost: 85,
    type: "ship",
    sigil: "𓆩✠𓆪",
    effect: { combat: 5, heal: 4 },
    ally: { draw: 1 },
    text: "Gain 5 Combat and 4 Authority.",
    allyText: "Draw 1 card.",
    flavor: "An entire crusade follows the movement of its wings."
  },

  {
    id: "tideglass_confessor",
    name: "Tideglass Confessor",
    faction: "blue",
    cost: 4,
    shop_cost: 45,
    type: "ship",
    sigil: "✦",
    effect: { combat: 3, lifelink: 0.5 },
    ally: { shield: 2 },
    text: "Gain 3 Combat and Lifelink 50% this turn.",
    allyText: "Gain 2 Shield.",
    flavor: "Every blow is a confession. Every answered wound becomes absolution."
  },
  {
    id: "procession_of_seven_banners",
    name: "Procession of Seven Banners",
    faction: "blue",
    cost: 6,
    shop_cost: 90,
    type: "base",
    defense: 6,
    outpost: false,
    sigil: "✦",
    effect: { healPerBase: 1 },
    ally: { shield: 2 },
    sacrifice: { damageAll: 1 },
    text: "Gain 1 Authority for each active Base you control.",
    allyText: "Gain 2 Shield.",
    sacrificeText: "Sacrifice: Damage All 1.",
    flavor: "Where one banner stands, hope survives. Where seven gather, kingdoms endure."
  },

  // ==========================================================
  // GREEN — GORAK WARHOST
  // ==========================================================
  {
    id: "gore_runner",
    name: "Gore Runner",
    faction: "green",
    cost: 1,
    shop_cost: 10,
    type: "ship",
    sigil: "⚔",
    effect: { combat: 2 },
    ally: {},
    text: "Gain 2 Combat.",
    allyText: "",
    flavor: "It reaches the enemy before the war horn finishes."
  },
  {
    id: "scrapfang_runt",
    name: "Scrapfang Runt",
    faction: "green",
    cost: 1,
    shop_cost: 10,
    type: "ship",
    sigil: "♠",
    effect: { combat: 2 },
    ally: {},
    sacrifice: { combat: 3 },
    text: "Gain 2 Combat.",
    allyText: "",
    sacrificeText: "Sacrifice: Gain 3 Combat.",
    flavor: "Small enough to throw. Angry enough to volunteer."
  },
  {
    id: "loot_cart",
    name: "Loot Cart",
    faction: "green",
    cost: 2,
    shop_cost: 15,
    type: "ship",
    sigil: "▣",
    effect: { trade: 2 },
    ally: { combat: 1 },
    sacrifice: { trade: 3 },
    text: "Gain 2 Trade.",
    allyText: "Gain 1 Combat.",
    sacrificeText: "Sacrifice: Gain 3 Trade.",
    flavor: "Its inventory is organized by who used to own it."
  },
  {
    id: "axe_mob",
    name: "Axe Mob",
    faction: "green",
    cost: 2,
    shop_cost: 15,
    type: "ship",
    sigil: "🪓",
    effect: { combat: 3 },
    ally: { combat: 2 },
    text: "Gain 3 Combat.",
    allyText: "Gain 2 Combat.",
    flavor: "A strategy meeting with fewer chairs and more axes."
  },
  {
    id: "siege_boar",
    name: "Siege Boar",
    faction: "green",
    cost: 3,
    shop_cost: 25,
    type: "ship",
    sigil: "◆",
    effect: { combat: 4 },
    ally: { heal: 1 },
    text: "Gain 4 Combat.",
    allyText: "Gain 1 Authority.",
    flavor: "Bred to mistake walls for invitations."
  },
  {
    id: "war_drum_camp",
    name: "War Drum Camp",
    faction: "green",
    cost: 3,
    shop_cost: 30,
    type: "base",
    defense: 4,
    outpost: true,
    sigil: "◉",
    effect: { combat: 2 },
    ally: { combat: 1 },
    text: "Outpost. Gain 2 Combat.",
    allyText: "Gain 1 Combat.",
    flavor: "The drums make retreat sound impossible."
  },
  {
    id: "rage_caller",
    name: "Rage Caller",
    faction: "green",
    cost: 3,
    shop_cost: 25,
    type: "ship",
    sigil: "☄",
    effect: { combat: 4 },
    ally: { combat: 2 },
    sacrifice: { combat: 4 },
    text: "Gain 4 Combat.",
    allyText: "Gain 2 Combat.",
    sacrificeText: "Sacrifice: Gain 4 Combat.",
    flavor: "It turns fear into a louder emotion."
  },
  {
    id: "breakneck_stockade",
    name: "Breakneck Stockade",
    faction: "green",
    cost: 4,
    shop_cost: 40,
    type: "base",
    defense: 5,
    outpost: true,
    sigil: "▰",
    effect: { combat: 2 },
    ally: { combat: 2 },
    sacrifice: { combat: 6 },
    text: "Outpost. Gain 2 Combat.",
    allyText: "Gain 2 Combat.",
    sacrificeText: "Sacrifice: Gain 6 Combat.",
    flavor: "Built quickly, defended loudly, abandoned never."
  },
  {
    id: "skullcrag_warlord",
    name: "Skullcrag Warlord",
    faction: "green",
    cost: 5,
    shop_cost: 50,
    type: "ship",
    sigil: "♜",
    effect: { combat: 6 },
    ally: { combat: 3 },
    text: "Gain 6 Combat.",
    allyText: "Gain 3 Combat.",
    flavor: "Every scar is a campaign map."
  },
  {
    id: "stampede_totem",
    name: "Stampede Totem",
    faction: "green",
    cost: 5,
    shop_cost: 50,
    type: "base",
    defense: 5,
    outpost: false,
    sigil: "♞",
    effect: { combat: 3 },
    ally: { draw: 1 },
    text: "Gain 3 Combat.",
    allyText: "Draw 1 card.",
    flavor: "The carved herd moves when no one is looking."
  },
  {
    id: "last_charge_titan",
    name: "Last-Charge Titan",
    faction: "green",
    cost: 6,
    shop_cost: 85,
    type: "ship",
    sigil: "♞",
    effect: { combat: 7 },
    ally: { draw: 1 },
    sacrifice: { combat: 7 },
    text: "Gain 7 Combat.",
    allyText: "Draw 1 card.",
    sacrificeText: "Sacrifice: Gain 7 Combat.",
    flavor: "Every charge is its last. It has survived hundreds."
  },
  {
    id: "bone_wagon",
    name: "Bone Wagon",
    faction: "green",
    cost: 6,
    shop_cost: 80,
    type: "ship",
    sigil: "☠",
    effect: { trade: 3, combat: 3 },
    ally: { combat: 3 },
    text: "Gain 3 Trade and 3 Combat.",
    allyText: "Gain 3 Combat.",
    flavor: "The wheels complain in several languages."
  },
  {
    id: "worldbreaker_horde",
    name: "Worldbreaker Horde",
    faction: "green",
    cost: 7,
    shop_cost: 120,
    type: "ship",
    sigil: "⬢",
    effect: { combat: 9 },
    ally: { draw: 1 },
    text: "Gain 9 Combat.",
    allyText: "Draw 1 card.",
    flavor: "They do not conquer worlds. They make worlds stop being difficult."
  },
  {
    id: "gorak_colossus",
    name: "Gorak Colossus",
    faction: "green",
    cost: 7,
    shop_cost: 120,
    type: "ship",
    sigil: "▤",
    effect: { combat: 8, heal: 2 },
    ally: { combat: 3 },
    text: "Gain 8 Combat and 2 Authority.",
    allyText: "Gain 3 Combat.",
    flavor: "A fortress taught to sprint."
  },
  {
    id: "endwar_mammoth",
    name: "Endwar Mammoth",
    faction: "green",
    cost: 8,
    shop_cost: 170,
    type: "ship",
    sigil: "♜",
    effect: { combat: 11 },
    ally: { combat: 3, draw: 1 },
    text: "Gain 11 Combat.",
    allyText: "Gain 3 Combat and draw 1 card.",
    flavor: "Its tusks have ended dynasties that history never named."
  },
  {
    id: "plunder_shaman",
    name: "Plunder Shaman",
    faction: "green",
    cost: 3,
    shop_cost: 25,
    type: "ship",
    sigil: "☊",
    effect: { trade: 2 },
    ally: { draw: 1 },
    text: "Gain 2 Trade.",
    allyText: "Draw 1 card.",
    flavor: "It reads tomorrow's raid in the dents of yesterday's loot."
  },
  {
    id: "ironroot_encampment",
    name: "Ironroot Encampment",
    faction: "green",
    cost: 4,
    shop_cost: 35,
    type: "base",
    defense: 5,
    outpost: false,
    sigil: "▥",
    effect: { trade: 2 },
    ally: { combat: 2 },
    text: "Gain 2 Trade.",
    allyText: "Gain 2 Combat.",
    flavor: "A market, barracks, and fortress assembled from the same stolen walls."
  },
  {
    id: "raidmasters_cache",
    name: "Raidmaster's Cache",
    faction: "green",
    cost: 5,
    shop_cost: 55,
    type: "base",
    defense: 6,
    outpost: false,
    sigil: "▦",
    effect: { trade: 2 },
    ally: { draw: 1 },
    sacrifice: { trade: 4 },
    text: "Gain 2 Trade.",
    allyText: "Draw 1 card.",
    sacrificeText: "Sacrifice: Gain 4 Trade.",
    flavor: "The best supplies are saved for the warriors least likely to ask."
  },
  {
    id: "thunderhoof_quartermaster",
    name: "Thunderhoof Quartermaster",
    faction: "green",
    cost: 6,
    shop_cost: 85,
    type: "ship",
    sigil: "♞",
    effect: { trade: 3, combat: 4 },
    ally: { draw: 1 },
    text: "Gain 3 Trade and 4 Combat.",
    allyText: "Draw 1 card.",
    flavor: "It delivers weapons, rations, and the battle itself."
  },
  {
    id: "chainmaw_ravager",
    name: "Chainmaw Ravager",
    faction: "green",
    cost: 4,
    shop_cost: 40,
    type: "ship",
    sigil: "⛓",
    effect: { combat: 5 },
    ally: { combat: 2 },
    sacrifice: { draw: 1 },
    text: "Gain 5 Combat.",
    allyText: "Gain 2 Combat.",
    sacrificeText: "Sacrifice: Draw 1 card.",
    flavor: "The chains are not restraints. They are additional weapons."
  },
  {
    id: "spoils_foundry",
    name: "Spoils Foundry",
    faction: "green",
    cost: 5,
    shop_cost: 55,
    type: "base",
    defense: 5,
    outpost: false,
    sigil: "⚒",
    effect: { trade: 2, combat: 2 },
    ally: { draw: 1 },
    sacrifice: { combat: 5 },
    text: "Gain 2 Trade and 2 Combat.",
    allyText: "Draw 1 card.",
    sacrificeText: "Sacrifice: Gain 5 Combat.",
    flavor: "Yesterday's enemy fortress becomes tomorrow's battering ram."
  },

  {
    id: "spoils_harvester",
    name: "Spoils Harvester",
    faction: "green",
    cost: 3,
    shop_cost: 30,
    type: "ship",
    sigil: "⬢",
    effect: { trade: 2, scrapMarket: 1 },
    ally: { combat: 2 },
    text: "Gain 2 Trade and 1 Market Erase.",
    allyText: "Gain 2 Combat.",
    flavor: "If the Warhost cannot use it, nobody will."
  },
  {
    id: "chaincamp_drumworks",
    name: "Chaincamp Drumworks",
    faction: "green",
    cost: 5,
    shop_cost: 60,
    type: "base",
    defense: 5,
    outpost: false,
    sigil: "⬢",
    effect: { combatPerBase: 1 },
    ally: { shield: 2 },
    sacrifice: { trade: 3 },
    text: "Gain 1 Combat for each active Base you control.",
    allyText: "Gain 2 Shield.",
    sacrificeText: "Sacrifice: Gain 3 Trade.",
    flavor: "One camp hears the drum. Every camp answers."
  },
  {
    id: "bastion_eater_alpha",
    name: "Bastion-Eater Alpha",
    faction: "green",
    cost: 7,
    shop_cost: 140,
    type: "ship",
    sigil: "⬢",
    effect: { combat: 4, destroyBase: 1 },
    ally: { tradePerBase: 1 },
    text: "Gain 4 Combat and 1 Raze.",
    allyText: "Gain 1 Trade for each active Base you control.",
    flavor: "It breaks the enemy’s walls and sells the pieces back as weapons."
  },

  // ==========================================================
  // RED — UMBRAL COVENANT
  // ==========================================================
  {
    id: "ash_initiate",
    name: "Ash Initiate",
    faction: "red",
    cost: 1,
    shop_cost: 15,
    type: "ship",
    sigil: "✦",
    effect: { trade: 1, scrapOwn: 1 },
    ally: {},
    text: "Gain 1 Trade and 1 Purge.",
    allyText: "",
    flavor: "The first lesson is deciding what no longer deserves to exist."
  },
  {
    id: "coin_curse",
    name: "Coin Curse",
    faction: "red",
    cost: 1,
    shop_cost: 10,
    type: "ship",
    sigil: "¤",
    effect: { combat: 1 },
    ally: {},
    sacrifice: { trade: 3 },
    text: "Gain 1 Combat.",
    allyText: "",
    sacrificeText: "Sacrifice: Gain 3 Trade.",
    flavor: "Spend it once and it spends you forever."
  },
  {
    id: "blood_channeler",
    name: "Blood Channeler",
    faction: "red",
    cost: 2,
    shop_cost: 20,
    type: "ship",
    sigil: "♢",
    effect: { combat: 2 },
    ally: { trade: 1 },
    sacrifice: { draw: 1 },
    text: "Gain 2 Combat.",
    allyText: "Gain 1 Trade.",
    sacrificeText: "Sacrifice: Draw 1 card.",
    flavor: "Nothing is wasted when everything is fuel."
  },
  {
    id: "bone_appraiser",
    name: "Bone Appraiser",
    faction: "red",
    cost: 2,
    shop_cost: 20,
    type: "ship",
    sigil: "☠",
    effect: { trade: 2 },
    ally: { combat: 1 },
    sacrifice: { trade: 2, combat: 2 },
    text: "Gain 2 Trade.",
    allyText: "Gain 1 Combat.",
    sacrificeText: "Sacrifice: Gain 2 Trade and 2 Combat.",
    flavor: "It can price a soul by weight."
  },
  {
    id: "grave_bargain",
    name: "Grave Bargain",
    faction: "red",
    cost: 3,
    shop_cost: 30,
    type: "ship",
    sigil: "†",
    effect: { trade: 2, combat: 2, scrapMarket: 1 },
    ally: {},
    text: "Gain 2 Trade, 2 Combat, and 1 Market Erase.",
    allyText: "",
    flavor: "The dead cannot object to revised terms."
  },
  {
    id: "obsidian_altar",
    name: "Obsidian Altar",
    faction: "red",
    cost: 3,
    shop_cost: 25,
    type: "base",
    defense: 4,
    outpost: false,
    sigil: "⛧",
    effect: { trade: 1 },
    ally: { combat: 2 },
    sacrifice: { combat: 5 },
    text: "Gain 1 Trade.",
    allyText: "Gain 2 Combat.",
    sacrificeText: "Sacrifice: Gain 5 Combat.",
    flavor: "Its stones remember every offering."
  },
  {
    id: "hexed_portal",
    name: "Hexed Portal",
    faction: "red",
    cost: 3,
    shop_cost: 30,
    type: "base",
    defense: 4,
    outpost: false,
    sigil: "◌",
    effect: { trade: 1, combat: 1 },
    ally: { scrapMarket: 1 },
    sacrifice: { draw: 1 },
    text: "Gain 1 Trade and 1 Combat.",
    allyText: "Gain 1 Market Erase.",
    sacrificeText: "Sacrifice: Draw 1 card.",
    flavor: "It opens only for those willing to leave something behind."
  },
  {
    id: "flesh_ledger",
    name: "Flesh Ledger",
    faction: "red",
    cost: 4,
    shop_cost: 40,
    type: "ship",
    sigil: "▤",
    effect: { trade: 3, scrapOwn: 1 },
    ally: { combat: 2 },
    text: "Gain 3 Trade and 1 Purge.",
    allyText: "Gain 2 Combat.",
    flavor: "Every debt is entered beneath the skin."
  },
  {
    id: "soul_furnace",
    name: "Soul Furnace",
    faction: "red",
    cost: 4,
    shop_cost: 45,
    type: "base",
    defense: 5,
    outpost: true,
    sigil: "◐",
    effect: { combat: 3 },
    ally: { scrapOwn: 1 },
    sacrifice: { heal: 5, combat: 3 },
    text: "Outpost. Gain 3 Combat.",
    allyText: "Gain 1 Purge.",
    sacrificeText: "Sacrifice: Gain 5 Authority and 3 Combat.",
    flavor: "Its smoke contains recognizable faces."
  },
  {
    id: "death_tithe",
    name: "Death Tithe",
    faction: "red",
    cost: 4,
    shop_cost: 40,
    type: "ship",
    sigil: "†",
    effect: { combat: 4, heal: 2 },
    ally: { trade: 2 },
    sacrifice: { trade: 4 },
    text: "Gain 4 Combat and 2 Authority.",
    allyText: "Gain 2 Trade.",
    sacrificeText: "Sacrifice: Gain 4 Trade.",
    flavor: "The Covenant collects from both sides of the grave."
  },
  {
    id: "cinder_surgeon",
    name: "Cinder Surgeon",
    faction: "red",
    cost: 5,
    shop_cost: 55,
    type: "ship",
    sigil: "✚",
    effect: { heal: 3, scrapOwn: 1 },
    ally: { draw: 1 },
    text: "Gain 3 Authority and 1 Purge.",
    allyText: "Draw 1 card.",
    flavor: "It cures weakness by removing the weak part."
  },
  {
    id: "void_auctioneer",
    name: "Void Auctioneer",
    faction: "red",
    cost: 5,
    shop_cost: 65,
    type: "ship",
    sigil: "◍",
    effect: { trade: 3, combat: 3, scrapMarket: 1 },
    ally: { draw: 1 },
    sacrifice: { trade: 5, combat: 3 },
    text: "Gain 3 Trade, 3 Combat, and 1 Market Erase.",
    allyText: "Draw 1 card.",
    sacrificeText: "Sacrifice: Gain 5 Trade and 3 Combat.",
    flavor: "The highest bidder receives what the universe was using."
  },
  {
    id: "eclipse_archon",
    name: "Eclipse Archon",
    faction: "red",
    cost: 6,
    shop_cost: 95,
    type: "ship",
    sigil: "◒",
    effect: { combat: 7, draw: 1 },
    ally: { combat: 3 },
    sacrifice: { combat: 5, heal: 5 },
    text: "Gain 7 Combat and draw 1 card.",
    allyText: "Gain 3 Combat.",
    sacrificeText: "Sacrifice: Gain 5 Combat and 5 Authority.",
    flavor: "It rules the moment when light realizes it can lose."
  },
  {
    id: "black_sun_cathedral",
    name: "Black Sun Cathedral",
    faction: "red",
    cost: 7,
    shop_cost: 125,
    type: "base",
    defense: 7,
    outpost: true,
    sigil: "☀",
    effect: { combat: 3, scrapOwn: 1 },
    ally: { trade: 2 },
    text: "Outpost. Gain 3 Combat and 1 Purge.",
    allyText: "Gain 2 Trade.",
    flavor: "Its congregation enters by name and leaves as power."
  },
  {
    id: "realm_eater",
    name: "Realm Eater",
    faction: "red",
    cost: 8,
    shop_cost: 180,
    type: "ship",
    sigil: "●",
    effect: { combat: 9, scrapOwn: 2 },
    ally: { draw: 1, combat: 2 },
    text: "Gain 9 Combat and 2 Purge.",
    allyText: "Draw 1 card and gain 2 Combat.",
    flavor: "It becomes stronger by making your past smaller."
  },
  {
    id: "culling_whisper",
    name: "Culling Whisper",
    faction: "red",
    cost: 3,
    shop_cost: 30,
    type: "ship",
    sigil: "⌁",
    effect: { combat: 2, scrapOwn: 1 },
    ally: { trade: 1 },
    text: "Gain 2 Combat and 1 Purge.",
    allyText: "Gain 1 Trade.",
    flavor: "It names the weakest memory and waits for you to agree."
  },
  {
    id: "memory_pyre",
    name: "Memory Pyre",
    faction: "red",
    cost: 4,
    shop_cost: 45,
    type: "base",
    defense: 5,
    outpost: false,
    sigil: "♨",
    effect: { trade: 1, scrapOwn: 1 },
    ally: { combat: 2 },
    sacrifice: { draw: 1 },
    text: "Gain 1 Trade and 1 Purge.",
    allyText: "Gain 2 Combat.",
    sacrificeText: "Sacrifice: Draw 1 card.",
    flavor: "Old failures burn brightest when the Covenant needs direction."
  },
  {
    id: "ashen_reclaimer",
    name: "Ashen Reclaimer",
    faction: "red",
    cost: 5,
    shop_cost: 75,
    type: "ship",
    sigil: "↻",
    effect: { purgeAndDraw: 2 },
    ally: { combat: 2 },
    text: "You may purge up to 2 cards from your hand. Draw 1 card for each card purged this way.",
    allyText: "Gain 2 Combat.",
    flavor: "What it removes from your past returns as a sharper future."
  },
  {
    id: "abyssal_winnower",
    name: "Abyssal Winnower",
    faction: "red",
    cost: 6,
    shop_cost: 95,
    type: "ship",
    sigil: "◓",
    effect: { combat: 5, scrapOwn: 2 },
    ally: { draw: 1 },
    text: "Gain 5 Combat and 2 Purge.",
    allyText: "Draw 1 card.",
    flavor: "It leaves only the cards ruthless enough to survive your deck."
  },
  {
    id: "severance_adept",
    name: "Severance Adept",
    faction: "red",
    cost: 3,
    shop_cost: 30,
    type: "ship",
    sigil: "✂",
    effect: { trade: 1, scrapOwn: 1 },
    ally: { draw: 1 },
    text: "Gain 1 Trade and 1 Purge.",
    allyText: "Draw 1 card.",
    flavor: "It cuts away possessions, loyalties, and inconvenient histories."
  },
  {
    id: "black_sun_auditor",
    name: "Black Sun Auditor",
    faction: "red",
    cost: 5,
    shop_cost: 70,
    type: "ship",
    sigil: "◒",
    effect: {
      combat: 2,
      damageAll: 1,
      opponentDiscard: 1
    },
    ally: { lifelink: 0.5 },
    text: "Gain 2 Combat, Damage All 1, and the next enemy draws 1 fewer card.",
    allyText: "Gain Lifelink 50% this turn.",
    flavor: "The Covenant records every breath as borrowed property."
  },
  {
    id: "duskchain_inquisitor",
    name: "Duskchain Inquisitor",
    faction: "red",
    cost: 4,
    shop_cost: 50,
    type: "ship",
    sigil: "◒",
    effect: { combat: 3, stun: 1 },
    ally: { damageAll: 1 },
    sacrifice: { scrapMarket: 1 },
    text: "Gain 3 Combat and 1 Disable.",
    allyText: "Damage All 1.",
    sacrificeText: "Sacrifice: Gain 1 Market Erase.",
    flavor: "First it silences the fortress. Then it removes every memory of its allies."
  },

  {
    id: "nightglass_crucible",
    name: "Nightglass Crucible",
    faction: "red",
    cost: 6,
    shop_cost: 95,
    type: "base",
    defense: 6,
    outpost: true,
    sigil: "◑",
    effect: { combat: 4 },
    ally: { scrapOwn: 1 },
    sacrifice: { combat: 3, draw: 2 },
    text: "Outpost. Gain 4 Combat.",
    allyText: "Gain 1 Purge.",
    sacrificeText: "Sacrifice: Gain 3 Combat and draw 2 cards.",
    flavor: "Everything placed within returns sharper, darker, or not at all."
  }
]);

export const CARD_MAP = Object.freeze(
  Object.fromEntries(CARDS.map(card => [card.id, card]))
);

export const ALL_CARD_MAP = Object.freeze({
  ...STARTER_CARDS,
  ...CARD_MAP
});

export function getCard(cardId) {
  return ALL_CARD_MAP[cardId] || null;
}

export function createStarterDeck() {
  return [
    ...Array(8).fill("starter_coin"),
    ...Array(2).fill("starter_blade")
  ];
}

export function defaultOwnedCards() {
  return Object.fromEntries(CARDS.map(card => [card.id, 1]));
}

export function countCards(cardIds = []) {
  return cardIds.reduce((counts, cardId) => {
    counts[cardId] = (counts[cardId] || 0) + 1;
    return counts;
  }, {});
}

export function buildBalancedCommandDeck(
  owned = defaultOwnedCards()
) {
  const factionOrder = ["yellow", "blue", "green", "red"];
  const targets = [13, 13, 12, 12];

  const byFaction = factionOrder.map(faction =>
    CARDS.filter(
      card =>
        card.faction === faction &&
        (owned[card.id] || 0) > 0
    )
  );

  const deck = [];

  byFaction.forEach((cards, factionIndex) => {
    if (!cards.length) return;

    const faction = factionOrder[factionIndex];
    const target = targets[factionIndex];
    let cursor = 0;

    while (
      deck.filter(
        cardId => CARD_MAP[cardId]?.faction === faction
      ).length < target
    ) {
      const card = cards[cursor % cards.length];
      const used = deck.filter(
        cardId => cardId === card.id
      ).length;

      const allowed = Math.min(
        MAX_COPIES_PER_CARD,
        owned[card.id] || 0
      );

      if (used < allowed) {
        deck.push(card.id);
      }

      cursor += 1;

      if (
        cursor >
        cards.length * MAX_COPIES_PER_CARD * 2
      ) {
        break;
      }
    }
  });

  for (const card of CARDS) {
    while (
      deck.length < COMMAND_DECK_SIZE &&
      deck.filter(cardId => cardId === card.id).length <
        Math.min(
          MAX_COPIES_PER_CARD,
          owned[card.id] || 0
        )
    ) {
      deck.push(card.id);
    }
  }

  return deck.slice(0, COMMAND_DECK_SIZE);
}

export function validateCommandDeck(
  deck,
  owned = defaultOwnedCards()
) {
  if (!Array.isArray(deck)) {
    return {
      valid: false,
      message: "Deck data is missing."
    };
  }

  if (deck.length !== COMMAND_DECK_SIZE) {
    return {
      valid: false,
      message:
        `Your command deck must contain exactly ` +
        `${COMMAND_DECK_SIZE} cards.`
    };
  }

  const counts = countCards(deck);

  for (const [cardId, count] of Object.entries(counts)) {
    if (!CARD_MAP[cardId]) {
      return {
        valid: false,
        message: `Unknown card in deck: ${cardId}.`
      };
    }

    if (count > MAX_COPIES_PER_CARD) {
      return {
        valid: false,
        message:
          `No more than ${MAX_COPIES_PER_CARD} ` +
          `copies of one card are allowed.`
      };
    }

    if (count > (owned[cardId] || 0)) {
      return {
        valid: false,
        message:
          `You only own ${owned[cardId] || 0} copies of ` +
          `${CARD_MAP[cardId].name}.`
      };
    }
  }

  return {
    valid: true,
    message: "Deck is ready."
  };
}

export function cardStorePrice(card) {
  if (
    Number.isInteger(card?.shop_cost) &&
    card.shop_cost >= 0
  ) {
    return card.shop_cost;
  }

  // Backward-compatible fallback for older card data.
  return Math.max(
    3,
    card.cost * 3 +
      (card.type === "base" ? 2 : 0)
  );
}

export function effectSummary(effect = {}) {
  const parts = [];

  if (effect.trade) {
    parts.push(`◆ ${effect.trade} Trade`);
  }

  if (effect.tradePerBase) {
    parts.push(
      `◆ ${effect.tradePerBase} per Active Base`
    );
  }

  if (effect.combat) {
    parts.push(`⚔ ${effect.combat} Combat`);
  }

  if (effect.combatPerBase) {
    parts.push(
      `⚔ ${effect.combatPerBase} per Active Base`
    );
  }

  if (effect.heal) {
    parts.push(`♥ ${effect.heal} Authority`);
  }

  if (effect.healPerBase) {
    parts.push(
      `♥ ${effect.healPerBase} per Active Base`
    );
  }

  if (effect.shield) {
    parts.push(`🛡 ${effect.shield} Shield`);
  }

  if (effect.draw) {
    parts.push(`Draw ${effect.draw}`);
  }

  if (effect.drawPerBase) {
    parts.push(
      `Draw ${effect.drawPerBase} per Active Base`
    );
  }

  if (effect.opponentDiscard) {
    parts.push(
      `Enemy -${effect.opponentDiscard} card`
    );
  }

  if (effect.stun) {
    parts.push(`⌁ ${effect.stun} Disable`);
  }

  if (effect.destroyBase) {
    parts.push(`Raze ${effect.destroyBase}`);
  }

  if (effect.damageAll) {
    parts.push(`☄ Damage All ${effect.damageAll}`);
  }

  if (effect.lifelink) {
    parts.push(
      `Lifelink ${Math.round(effect.lifelink * 100)}%`
    );
  }

  if (effect.scrapMarket) {
    parts.push(
      `⊘ ${effect.scrapMarket} Market Erase`
    );
  }

  if (effect.scrapOwn) {
    parts.push(`✕ ${effect.scrapOwn} Purge`);
  }

  if (effect.purgeAndDraw) {
    parts.push(
      `✕ Purge up to ${effect.purgeAndDraw}; ` +
      `draw per Purge`
    );
  }

  return parts.join(" · ");
}

export function fullCardRules(card) {
  const lines = [];

  if (card.type === "base") {
    lines.push(
      `${card.outpost ? "Outpost" : "Base"} — ` +
      `${card.defense} Defense.`
    );
  }

  if (card.text) {
    lines.push(card.text);
  }

  if (card.allyText) {
    lines.push(
      `Faction Ally — ${card.allyText}`
    );
  }

  if (card.sacrificeText) {
    lines.push(card.sacrificeText);
  }

  return lines.join(" ");
}

export function assertCardLibrary() {
  const ids = new Set();

  const validFactions = new Set([
    "yellow",
    "blue",
    "green",
    "red"
  ]);

  for (const card of CARDS) {
    if (!card?.id) {
      throw new Error(
        "War Realms card is missing an id."
      );
    }

    if (ids.has(card.id)) {
      throw new Error(
        `Duplicate War Realms card id: ${card.id}`
      );
    }

    ids.add(card.id);

    if (!validFactions.has(card.faction)) {
      throw new Error(
        `Invalid faction on ${card.id}: ` +
        `${card.faction}`
      );
    }

    if (
      !Number.isInteger(card.shop_cost) ||
      card.shop_cost < 1
    ) {
      throw new Error(
        `Invalid shop_cost on ${card.id}: ` +
        `${card.shop_cost}`
      );
    }
  }

  for (const starterCard of Object.values(STARTER_CARDS)) {
    if (
      !Number.isInteger(starterCard.shop_cost) ||
      starterCard.shop_cost < 0
    ) {
      throw new Error(
        `Invalid shop_cost on ${starterCard.id}: ` +
        `${starterCard.shop_cost}`
      );
    }
  }

  return true;
}

assertCardLibrary();
