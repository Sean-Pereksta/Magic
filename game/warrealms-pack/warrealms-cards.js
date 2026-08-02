export const WAR_REALMS_CARD_VERSION = 11;
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
    image: "starter_coin.png",
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
    image: "starter_blade.png",
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
    image: "starwhisper_scout.png",
    faction: "yellow",
    cost: 2,
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
    image: "neural_leash.png",
    faction: "yellow",
    cost: 3,
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
    image: "phase_warden.png",
    faction: "yellow",
    cost: 3,
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
    image: "gravity_hook.png",
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
    image: "mirage_broker.png",
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
    image: "null_prism.png",
    faction: "yellow",
    cost: 4,
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
    image: "signal_eater.png",
    faction: "yellow",
    cost: 6,
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
    image: "chrono_beast.png",
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
  id: "hired_looter",
  name: "Hired Looter",
  image: "hired_looter.png",
  faction: "neutral",
  collectible: false,
  permanentMarket: true,
  cost: 2,
  shop_cost: 20,
  type: "ship",
  sigil: "◆",
  effect: { trade: 2 },
  ally: {},
  sacrifice: { combat: 2 },
  text: "Gain 2 Trade.",
  allyText: "",
  sacrificeText: "Sacrifice: Gain 2 Combat.",
  flavor: "His loyalty lasts exactly as long as the coin does."
},
  {
    id: "dominion_relay",
    name: "Dominion Relay",
    image: "dominion_relay.png",
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
    image: "paradox_warden.png",
    faction: "yellow",
    cost: 7,
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
    image: "causal_severer.png",
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
    image: "aegis_tide_saint.png",
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
  id: "discarded_future",
  name: "Discarded Future",
  image: "discarded_future.png",
  faction: "yellow",
  cost: 3,
  shop_cost: 20,
  type: "ship",
  sigil: "◈",
  effect: { trade: 1 },
  ally: {},
  sacrifice: { draw: 2 },
  text: "Gain 1 Trade.",
  allyText: "",
  sacrificeText: "Sacrifice: Draw 2 cards.",
  flavor: "The Concord abandons one future so two better paths may survive."
},

{
  id: "nullpulse_mite",
  name: "Nullpulse Mite",
  image: "nullpulse_mite.png",
  faction: "yellow",
  cost: 2,
  shop_cost: 20,
  type: "ship",
  sigil: "◈",
  effect: { combat: 1 },
  ally: {},
  sacrifice: { stun: 3 },
  text: "Gain 1 Combat.",
  allyText: "",
  sacrificeText: "Sacrifice: Gain 3 Disable.",
  flavor: "Its final pulse leaves entire fortresses unable to remember their purpose."
},
  {
  id: "probability_beacon",
  name: "Probability Beacon",
  image: "probability_beacon.png",
  faction: "yellow",
  cost: 2,
  shop_cost: 25,
  type: "base",
  defense: 3,
  outpost: false,
  sigil: "◈",
  effect: { trade: 1 },
  ally: { shield: 1 },
  text: "Gain 1 Trade.",
  allyText: "Gain 1 Shield.",
  flavor: "It marks the future most likely to reward those who follow."
},

{
  id: "nullfield_pylon",
  name: "Nullfield Pylon",
  image: "nullfield_pylon.png",
  faction: "yellow",
  cost: 3,
  shop_cost: 35,
  type: "base",
  defense: 4,
  outpost: true,
  sigil: "◈",
  effect: { shield: 1 },
  ally: { stun: 1 },
  text: "Outpost. Gain 1 Shield.",
  allyText: "Gain 1 Disable.",
  flavor: "Everything near it becomes slightly less certain of its purpose."
},

{
  id: "threadway_station",
  name: "Threadway Station",
  image: "threadway_station.png",
  faction: "yellow",
  cost: 3,
  shop_cost: 35,
  type: "base",
  defense: 4,
  outpost: false,
  sigil: "◈",
  effect: { trade: 1 },
  ally: { combat: 1 },
  sacrifice: { draw: 1 },
  text: "Gain 1 Trade.",
  allyText: "Gain 1 Combat.",
  sacrificeText: "Sacrifice: Draw 1 card.",
  flavor: "Every route passes through it, including several that do not yet exist."
},


// ==========================================================
// BLUE — AZURE ASCENDANCY
// ==========================================================

{
  id: "roadside_shrine",
  name: "Roadside Shrine",
  image: "roadside_shrine.png",
  faction: "blue",
  cost: 1,
  shop_cost: 15,
  type: "base",
  defense: 3,
  outpost: false,
  sigil: "✦",
  effect: { heal: 1 },
  ally: {},
  text: "Gain 1 Authority.",
  allyText: "",
  flavor: "A small sanctuary can still shelter a very large hope."
},

{
  id: "silver_gate_watch",
  name: "Silver-Gate Watch",
  image: "silver_gate_watch.png",
  faction: "blue",
  cost: 2,
  shop_cost: 25,
  type: "base",
  defense: 4,
  outpost: true,
  sigil: "✦",
  effect: { shield: 1 },
  ally: { heal: 1 },
  text: "Outpost. Gain 1 Shield.",
  allyText: "Gain 1 Authority.",
  flavor: "The gate remains open only to those arriving beneath the proper banner."
},

{
  id: "pilgrim_supply_chapel",
  name: "Pilgrim Supply Chapel",
  image: "pilgrim_supply_chapel.png",
  faction: "blue",
  cost: 3,
  shop_cost: 35,
  type: "base",
  defense: 4,
  outpost: false,
  sigil: "✦",
  effect: { trade: 1 },
  ally: { heal: 2 },
  sacrifice: { shield: 3 },
  text: "Gain 1 Trade.",
  allyText: "Gain 2 Authority.",
  sacrificeText: "Sacrifice: Gain 3 Shield.",
  flavor: "Bread, steel, and blessings are distributed from the same sacred counter."
},
{
  id: "voidstall_picker",
  name: "Voidstall Picker",
  image: "voidstall_picker.png",
  faction: "yellow",
  cost: 2,
  shop_cost: 25,
  type: "ship",
  sigil: "◈",
  effect: { scrapMarket: 1 },
  ally: { trade: 1 },
  text: "Gain 1 Market Erase.",
  allyText: "Gain 1 Trade.",
  flavor: "It removes the least favorable possibility before anyone else notices it."
},

{
  id: "wreckmarket_runt",
  name: "Wreckmarket Runt",
  image: "wreckmarket_runt.png",
  faction: "green",
  cost: 2,
  shop_cost: 15,
  type: "ship",
  sigil: "⬢",
  effect: { combat: 1 },
  ally: {},
  sacrifice: { scrapMarket: 3 },
  text: "Gain 1 Combat.",
  allyText: "",
  sacrificeText: "Sacrifice: Gain 3 Market Erase.",
  flavor: "When the runt cannot afford the market, it makes sure nobody else can either."
},

{
  id: "ash_contract_broker",
  name: "Ash-Contract Broker",
  image: "ash_contract_broker.png",
  faction: "red",
  cost: 2,
  shop_cost: 25,
  type: "ship",
  sigil: "◒",
  effect: { trade: 1, scrapMarket: 1 },
  ally: {},
  text: "Gain 1 Trade and 1 Market Erase.",
  allyText: "",
  flavor: "Every agreement it signs quietly removes another offer from consideration."
},

// ==========================================================
// GREEN — GORAK WARHOST
// ==========================================================

{
  id: "mudwall_camp",
  name: "Mudwall Camp",
  image: "mudwall_camp.png",
  faction: "green",
  cost: 1,
  shop_cost: 15,
  type: "base",
  defense: 3,
  outpost: false,
  sigil: "⬢",
  effect: { combat: 1 },
  ally: {},
  text: "Gain 1 Combat.",
  allyText: "",
  flavor: "It is mostly mud, sharpened stakes, and very confident shouting."
},

{
  id: "scrapwood_barricade",
  name: "Scrapwood Barricade",
  image: "scrapwood_barricade.png",
  faction: "green",
  cost: 2,
  shop_cost: 25,
  type: "base",
  defense: 4,
  outpost: true,
  sigil: "⬢",
  effect: { combat: 1 },
  ally: { combat: 1 },
  text: "Outpost. Gain 1 Combat.",
  allyText: "Gain 1 Combat.",
  flavor: "Nothing matches, but everything points toward the enemy."
},


// ==========================================================
// RED — UMBRAL COVENANT
// ==========================================================

{
  id: "candlecrypt_altar",
  name: "Candlecrypt Altar",
  image: "candlecrypt_altar.png",
  faction: "red",
  cost: 2,
  shop_cost: 25,
  type: "base",
  defense: 3,
  outpost: false,
  sigil: "◒",
  effect: { combat: 1 },
  ally: { scrapOwn: 1 },
  text: "Gain 1 Combat.",
  allyText: "Gain 1 Purge.",
  flavor: "Each candle marks something the Covenant has chosen to forget."
},

{
  id: "black_tithe_post",
  name: "Black Tithe Post",
  image: "black_tithe_post.png",
  faction: "red",
  cost: 4,
  shop_cost: 35,
  type: "base",
  defense: 4,
  outpost: true,
  sigil: "◒",
  effect: { trade: 1 },
  ally: { combat: 2 },
  sacrifice: { scrapOwn: 1 },
  text: "Outpost. Gain 1 Trade.",
  allyText: "Gain 2 Combat.",
  sacrificeText: "Sacrifice: Gain 1 Purge.",
  flavor: "All who pass must leave behind coin, blood, or an unwanted memory."
},

{
  id: "probability_splinter",
  name: "Probability Splinter",
  image: "probability_splinter.png",
  faction: "yellow",
  cost: 5,
  shop_cost: 40,
  type: "ship",
  sigil: "◈",
  effect: { trade: 2, shield: 1 },
  ally: { combat: 1 },
  sacrifice: { draw: 2, stun: 1 },
  text: "Gain 2 Trade and 1 Shield.",
  allyText: "Gain 1 Combat.",
  sacrificeText: "Sacrifice: Draw 2 cards and gain 1 Disable.",
  flavor: "Break it carefully. Every fragment contains a different victory."
},

// ==========================================================
// BLUE — AZURE ASCENDANCY
// ==========================================================

{
  id: "last_light_martyr",
  name: "Last-Light Martyr",
  image: "last_light_martyr.png",
  faction: "blue",
  cost: 2,
  shop_cost: 15,
  type: "ship",
  sigil: "✦",
  effect: { heal: 2 },
  ally: {},
  sacrifice: { heal: 6 },
  text: "Gain 2 Authority.",
  allyText: "",
  sacrificeText: "Sacrifice: Gain 6 Authority.",
  flavor: "Its light burns brightest only when it knows it will not return."
},

{
  id: "votive_reliquary",
  name: "Votive Reliquary",
  image: "votive_reliquary.png",
  faction: "blue",
  cost: 5,
  shop_cost: 35,
  type: "base",
  defense: 4,
  outpost: false,
  sigil: "✦",
  effect: { trade: 1, heal: 2 },
  ally: { shield: 1 },
  sacrifice: { draw: 1, heal: 5 },
  text: "Gain 1 Trade and 2 Authority.",
  allyText: "Gain 1 Shield.",
  sacrificeText: "Sacrifice: Draw 1 card and gain 5 Authority.",
  flavor: "The vessel may break. The blessing stored within it does not."
},

// ==========================================================
// GREEN — GORAK WARHOST
// ==========================================================

{
  id: "fuseback_runt",
  name: "Fuseback Runt",
  image: "fuseback_runt.png",
  faction: "green",
  cost: 2,
  shop_cost: 15,
  type: "ship",
  sigil: "⬢",
  effect: { combat: 2 },
  ally: {},
  sacrifice: { combat: 6 },
  text: "Gain 2 Combat.",
  allyText: "",
  sacrificeText: "Sacrifice: Gain 6 Combat.",
  flavor: "The Warhost points it toward the enemy and begins counting backward."
},

{
  id: "loot_cart_bomber",
  name: "Loot-Cart Bomber",
  image: "loot_cart_bomber.png",
  faction: "green",
  cost: 5,
  shop_cost: 35,
  type: "ship",
  sigil: "⬢",
  effect: { trade: 2, combat: 2 },
  ally: { combat: 1 },
  sacrifice: { damageAll: 2, combat: 3 },
  text: "Gain 2 Trade and 2 Combat.",
  allyText: "Gain 1 Combat.",
  sacrificeText: "Sacrifice: Damage All 2 and gain 3 Combat.",
  flavor: "Anything can be delivered quickly when the delivery is also the explosion."
},

// ==========================================================
// RED — UMBRAL COVENANT
// ==========================================================

{
  id: "gravewax_token",
  name: "Gravewax Token",
  image: "gravewax_token.png",
  faction: "red",
  cost: 3,
  shop_cost: 20,
  type: "ship",
  sigil: "◒",
  effect: { trade: 1 },
  ally: {},
  sacrifice: { scrapOwn: 3 },
  text: "Gain 1 Trade.",
  allyText: "",
  sacrificeText: "Sacrifice: Gain 3 Purge.",
  flavor: "Melt the coin, and three unwanted memories vanish with the smoke."
},

{
  id: "ashmouth_familiar",
  name: "Ashmouth Familiar",
  image: "ashmouth_familiar.png",
  faction: "red",
  cost: 3,
  shop_cost: 25,
  type: "ship",
  sigil: "◒",
  effect: { combat: 2, trade: 1 },
  ally: {},
  sacrifice: { scrapMarket: 2 },
  text: "Gain 2 Combat and 1 Trade.",
  allyText: "",
  sacrificeText: "Sacrifice: Gain 2 Market Erase.",
  flavor: "Its final meal removes two possibilities from everyone else's future."
},
  {
  id: "preemptive_envoy",
  name: "Preemptive Envoy",
  image: "preemptive_envoy.png",
  faction: "yellow",
  cost: 3,
  shop_cost: 45,
  type: "ship",
  sigil: "◈",
  onPurchase: { stun: 1 },
  effect: { trade: 2 },
  ally: { shield: 1 },
  text: "Gain 2 Trade.",
  onPurchaseText: "Deploy: Gain 1 Disable immediately when purchased.",
  allyText: "Gain 1 Shield.",
  flavor: "It arrives one decision before anyone agrees to summon it."
},
// ==========================================================
// 20 NEW CARDS
//
// 10 LOW-COST SINGLE-ACTION CARDS
// 10 CARDS THAT SCALE WITH ACTIVE BASES
// ==========================================================


// ==========================================================
// LOW-COST SINGLE-ACTION CARDS
//
// Each card costs 1–3 and has only one Primary effect type.
// Draw-only cards receive a small Ally or Double Ally ability.
// ==========================================================


// ==========================================================
// YELLOW — XYTHE CONCORD
// ==========================================================

{
  id: "unspent_possibility",
  name: "Unspent Possibility",
  image: "unspent_possibility.png",
  faction: "yellow",
  cost: 1,
  shop_cost: 12,
  type: "ship",
  sigil: "◈",
  effect: { trade: 2 },
  ally: {},
  text: "Gain 2 Trade.",
  allyText: "",
  flavor: "Even a future that never arrives may still pay its debts."
},

{
  id: "thoughtsnare_mite",
  name: "Thoughtsnare Mite",
  image: "thoughtsnare_mite.png",
  faction: "yellow",
  cost: 2,
  shop_cost: 22,
  type: "ship",
  sigil: "◈",
  effect: { stun: 1 },
  ally: {},
  text: "Gain 1 Disable.",
  allyText: "",
  flavor: "It catches one command and leaves an army waiting for instructions."
},

{
  id: "fractured_foresight",
  name: "Fractured Foresight",
  image: "fractured_foresight.png",
  faction: "yellow",
  cost: 3,
  shop_cost: 25,
  type: "ship",
  sigil: "◈",
  effect: { draw: 1 },
  ally: { shield: 1 },
  doubleAlly: { trade: 1 },
  text: "Draw 1 card.",
  allyText: "Gain 1 Shield.",
  doubleAllyText: "Gain 1 Trade.",
  flavor: "The vision is incomplete, but the missing pieces still shape the path."
},


// ==========================================================
// BLUE — AZURE ASCENDANCY
// ==========================================================

{
  id: "dawnward_attendant",
  name: "Dawnward Attendant",
  image: "dawnward_attendant.png",
  faction: "blue",
  cost: 1,
  shop_cost: 12,
  type: "ship",
  sigil: "✦",
  effect: { heal: 3 },
  ally: {},
  text: "Gain 3 Authority.",
  allyText: "",
  flavor: "The smallest blessing can carry a soldier through the longest night."
},

{
  id: "oathplate_novice",
  name: "Oathplate Novice",
  image: "oathplate_novice.png",
  faction: "blue",
  cost: 2,
  shop_cost: 20,
  type: "ship",
  sigil: "✦",
  effect: { shield: 3 },
  ally: {},
  text: "Gain 3 Shield.",
  allyText: "",
  flavor: "The armor is borrowed. The courage is entirely their own."
},

{
  id: "reliquary_revelation",
  name: "Reliquary Revelation",
  image: "reliquary_revelation.png",
  faction: "blue",
  cost: 3,
  shop_cost: 32,
  type: "ship",
  sigil: "✦",
  effect: { draw: 1 },
  ally: { heal: 1 },
  doubleAlly: { shield: 1 },
  text: "Draw 1 card.",
  allyText: "Gain 1 Authority.",
  doubleAllyText: "Gain 1 Shield.",
  flavor: "The relic offers no answer without also offering hope."
},


// ==========================================================
// GREEN — GORAK WARHOST
// ==========================================================

{
  id: "knucklebone_rusher",
  name: "Knucklebone Rusher",
  image: "knucklebone_rusher.png",
  faction: "green",
  cost: 1,
  shop_cost: 12,
  type: "ship",
  sigil: "⬢",
  effect: { combat: 3 },
  ally: {},
  text: "Gain 3 Combat.",
  allyText: "",
  flavor: "It began charging before anyone finished explaining the target."
},

{
  id: "wreckstall_raider",
  name: "Wreckstall Raider",
  image: "wreckstall_raider.png",
  faction: "green",
  cost: 2,
  shop_cost: 22,
  type: "ship",
  sigil: "⬢",
  effect: { scrapMarket: 1 },
  ally: {},
  text: "Gain 1 Market Erase.",
  allyText: "",
  flavor: "What cannot be stolen can still be made unavailable."
},


// ==========================================================
// RED — UMBRAL COVENANT
// ==========================================================

{
  id: "ashledger_acolyte",
  name: "Ashledger Acolyte",
  image: "ashledger_acolyte.png",
  faction: "red",
  cost: 2,
  shop_cost: 15,
  type: "ship",
  sigil: "◒",
  effect: { scrapOwn: 1 },
  ally: {},
  text: "Gain 1 Purge.",
  allyText: "",
  flavor: "Every erased name leaves room for a more useful obligation."
},

{
  id: "graveglass_vision",
  name: "Graveglass Vision",
  image: "graveglass_vision.png",
  faction: "red",
  cost: 4,
  shop_cost: 35,
  type: "ship",
  sigil: "◒",
  effect: { draw: 1 },
  ally: { combat: 1 },
  doubleAlly: { scrapOwn: 1 },
  text: "Draw 1 card.",
  allyText: "Gain 1 Combat.",
  doubleAllyText: "Gain 1 Purge.",
  flavor: "The glass reveals tomorrow by consuming one unwanted yesterday."
},


// ==========================================================
// CARDS THAT SCALE WITH ACTIVE BASES
//
// These use existing supported scaling keys:
// - combatPerBase
// - tradePerBase
// - healPerBase
//
// The scaling includes the Base card itself if it is active.
// ==========================================================


// ==========================================================
// YELLOW — XYTHE CONCORD
// ==========================================================

{
  id: "convergence_tithekeeper",
  name: "Convergence Tithekeeper",
  image: "convergence_tithekeeper.png",
  faction: "yellow",
  cost: 3,
  shop_cost: 42,
  type: "ship",
  sigil: "◈",
  effect: { tradePerBase: 1 },
  ally: { shield: 1 },
  text: "Gain 1 Trade for each active Base you control.",
  allyText: "Gain 1 Shield.",
  flavor: "Every secured reality owes the Concord a portion of its prosperity."
},

{
  id: "many_gate_predator",
  name: "Many-Gate Predator",
  image: "many_gate_predator.png",
  faction: "yellow",
  cost: 5,
  shop_cost: 80,
  type: "ship",
  sigil: "◈",
  effect: { combat: 2, combatPerBase: 1 },
  ally: { stun: 1 },
  text: "Gain 2 Combat plus 1 Combat for each active Base you control.",
  allyText: "Gain 1 Disable.",
  flavor: "Each open gate gives it another direction from which to strike."
},

{
  id: "network_of_unmade_paths",
  name: "Network of Unmade Paths",
  image: "network_of_unmade_paths.png",
  faction: "yellow",
  cost: 6,
  shop_cost: 75,
  type: "base",
  defense: 5,
  outpost: false,
  sigil: "◈",
  effect: { tradePerBase: 1 },
  ally: { shield: 2 },
  sacrifice: { stun: 2 },
  text: "Gain 1 Trade for each active Base you control.",
  allyText: "Gain 2 Shield.",
  sacrificeText: "Sacrifice: Gain 2 Disable.",
  flavor: "A single route is travel. A thousand routes become dominion."
},


// ==========================================================
// BLUE — AZURE ASCENDANCY
// ==========================================================

{
  id: "procession_quartermaster",
  name: "Procession Quartermaster",
  image: "procession_quartermaster.png",
  faction: "blue",
  cost: 3,
  shop_cost: 40,
  type: "ship",
  sigil: "✦",
  effect: { tradePerBase: 1 },
  ally: { heal: 1 },
  text: "Gain 1 Trade for each active Base you control.",
  allyText: "Gain 1 Authority.",
  flavor: "Every sanctuary adds another wagon to the sacred procession."
},

{
  id: "saint_of_many_shelters",
  name: "Saint of Many Shelters",
  image: "saint_of_many_shelters.png",
  faction: "blue",
  cost: 4,
  shop_cost: 58,
  type: "ship",
  sigil: "✦",
  effect: { heal: 1, healPerBase: 1 },
  ally: { shield: 2 },
  text: "Gain 1 Authority plus 1 Authority for each active Base you control.",
  allyText: "Gain 2 Shield.",
  flavor: "Every roof raised in mercy strengthens the blessing carried between them."
},

{
  id: "cathedral_supply_chain",
  name: "Cathedral Supply Chain",
  image: "cathedral_supply_chain.png",
  faction: "blue",
  cost: 5,
  shop_cost: 72,
  type: "base",
  defense: 5,
  outpost: false,
  sigil: "✦",
  effect: { trade: 1, healPerBase: 1 },
  ally: { trade: 1 },
  sacrifice: { heal: 5 },
  text: "Gain 1 Trade and 1 Authority for each active Base you control.",
  allyText: "Gain 1 Trade.",
  sacrificeText: "Sacrifice: Gain 5 Authority.",
  flavor: "No sanctuary stands alone when every road between them is consecrated."
},


// ==========================================================
// GREEN — GORAK WARHOST
// ==========================================================

{
  id: "campcount_brute",
  name: "Campcount Brute",
  image: "campcount_brute.png",
  faction: "green",
  cost: 3,
  shop_cost: 42,
  type: "ship",
  sigil: "⬢",
  effect: { combatPerBase: 2 },
  ally: {},
  text: "Gain 2 Combat for each active Base you control.",
  allyText: "",
  flavor: "It counts war camps by the number of armies ready to follow it."
},

{
  id: "fortress_back_breaker",
  name: "Fortress-Back Breaker",
  image: "fortress_back_breaker.png",
  faction: "green",
  cost: 5,
  shop_cost: 78,
  type: "ship",
  sigil: "⬢",
  effect: { combat: 3, combatPerBase: 2 },
  ally: { combat: 2 },
  text: "Gain 3 Combat plus 2 Combat for each active Base you control.",
  allyText: "Gain 2 Combat.",
  flavor: "Every fortress behind it is another reason to strike harder ahead."
},


// ==========================================================
// RED — UMBRAL COVENANT
// ==========================================================

{
  id: "tithe_of_black_towers",
  name: "Tithe of Black Towers",
  image: "tithe_of_black_towers.png",
  faction: "red",
  cost: 3,
  shop_cost: 45,
  type: "ship",
  sigil: "◒",
  effect: { combatPerBase: 1 },
  ally: { scrapOwn: 1 },
  text: "Gain 1 Combat for each active Base you control.",
  allyText: "Gain 1 Purge.",
  flavor: "Each tower pays its due in fear, blood, and obedient silence."
},

{
  id: "covenant_tribute_engine",
  name: "Covenant Tribute Engine",
  image: "covenant_tribute_engine.png",
  faction: "red",
  cost: 7,
  shop_cost: 75,
  type: "base",
  defense: 5,
  outpost: false,
  sigil: "◒",
  effect: { combat: 1, combatPerBase: 1 },
  ally: { scrapOwn: 1 },
  sacrifice: { draw: 1 },
  text: "Gain 1 Combat plus 1 Combat for each active Base you control.",
  allyText: "Gain 1 Purge.",
  sacrificeText: "Sacrifice: Draw 1 card.",
  flavor: "Every altar feeds the engine, and the engine remembers every offering."
},

{
  id: "relief_procession",
  name: "Relief Procession",
  image: "relief_procession.png",
  faction: "blue",
  cost: 3,
  shop_cost: 45,
  type: "ship",
  sigil: "✦",
  onPurchase: { heal: 4 },
  effect: { trade: 2, heal: 2 },
  ally: {},
  text: "Gain 2 Trade and 2 Authority.",
  onPurchaseText: "Deploy: Gain 4 Authority immediately when purchased.",
  allyText: "",
  flavor: "The blessing reaches the battlefield before the procession itself."
},

{
  id: "rampaging_mercenary",
  name: "Rampaging Mercenary",
  image: "rampaging_mercenary.png",
  faction: "green",
  cost: 4,
  shop_cost: 60,
  type: "ship",
  sigil: "⬢",
  onPurchase: { combat: 3 },
  effect: { combat: 5 },
  ally: { combat: 1 },
  text: "Gain 5 Combat.",
  onPurchaseText: "Deploy: Gain 3 Combat immediately when purchased.",
  allyText: "Gain 1 Combat.",
  flavor: "Payment is merely permission to begin the charge."
},

{
  id: "blood_price_broker",
  name: "Blood-Price Broker",
  image: "blood_price_broker.png",
  faction: "red",
  cost: 4,
  shop_cost: 70,
  type: "ship",
  sigil: "◒",
  onPurchase: { scrapOwn: 1 },
  effect: { trade: 2, combat: 2 },
  ally: { combat: 2 },
  text: "Gain 2 Trade and 2 Combat.",
  onPurchaseText: "Deploy: Gain 1 Purge immediately when purchased.",
  allyText: "Gain 2 Combat.",
  flavor: "The first payment is always something you no longer wish to remember."
},


// ==========================================================
// RESURRECTION — TEMPORARILY REPLAY CARDS FROM THE DISCARD PILE
// Cards with sacrificeAfterPlay are removed after the revived play resolves.
// ==========================================================

{
  id: "gravebound_reclaimer",
  name: "Gravebound Reclaimer",
  image: "gravebound_reclaimer.png",
  faction: "red",
  cost: 5,
  shop_cost: 95,
  type: "ship",
  sigil: "◒",
  effect: {
    combat: 3,
    resurrect: {
      count: 1,
      source: "discard",
      cardType: "ship",
      maxCost: 3,
      destination: "hand",
      sacrificeAfterPlay: true
    }
  },
  ally: { combat: 2 },
  text: "Gain 3 Combat. Return a ship costing 3 or less from your discard pile to your hand. Sacrifice it after it is played.",
  allyText: "Gain 2 Combat.",
  resurrectText: "Resurrect a ship costing 3 or less. Sacrifice it after its revived play.",
  flavor: "It returns the fallen for exactly as long as their usefulness survives."
},

{
  id: "black_procession_necromancer",
  name: "Black Procession Necromancer",
  image: "black_procession_necromancer.png",
  faction: "red",
  cost: 10,
  shop_cost: 155,
  type: "ship",
  sigil: "◒",
  effect: {
    combat: 5,
    resurrect: {
      count: 1,
      source: "discard",
      cardType: "ship",
      maxCost: 6,
      destination: "hand",
      sacrificeAfterPlay: true
    }
  },
  ally: { draw: 1 },
  doubleAlly: { combat: 4 },
  text: "Gain 5 Combat. Return a ship costing 6 or less from your discard pile to your hand. Sacrifice it after it is played.",
  allyText: "Draw 1 card.",
  doubleAllyText: "Gain 4 Combat.",
  resurrectText: "Resurrect a ship costing 6 or less. Sacrifice it after its revived play.",
  flavor: "Behind every conqueror marches an army that has already lost once."
},

{
  id: "a_grave_remembers",
  name: "A Grave Remembers",
  image: "a_grave_remembers.png",
  faction: "red",
  cost: 16,
  shop_cost: 130,
  type: "base",
  defense: 6,
  outpost: false,
  sigil: "◒",
  effect: {
    combat: 2,
    graveEcho: {
      source: "sacrificePile",
      count: 1,
      copy: "primary",
      maxCost: 7,
      excludeKeys: [
        "draw",
        "resurrect",
        "graveEcho",
        "echo",
        "transform"
      ]
    }
  },
  ally: { scrapOwn: 1 },
  sacrifice: { draw: 1, combat: 3 },
  text: "Gain 2 Combat. Choose a sacrificed card costing 7 or less and repeat its Primary ability. Draw, Resurrection, Echo, and Transform effects cannot be copied.",
  allyText: "Gain 1 Purge.",
  sacrificeText: "Sacrifice: Draw 1 card and gain 3 Combat.",
  graveEchoText: "Repeat the allowed Primary effects of one card in your sacrifice pile.",
  flavor: "The earth forgets the name. It never forgets the final act."
},


// ==========================================================
// AUTHORITY THRESHOLDS — EXTRA EFFECTS WHILE AT LOW AUTHORITY
// Threshold effects are additional to the normal Primary effect.
// ==========================================================

{
  id: "last_wall_confessor",
  name: "Last-Wall Confessor",
  image: "last_wall_confessor.png",
  faction: "blue",
  cost: 3,
  shop_cost: 50,
  type: "ship",
  sigil: "✦",
  effect: { heal: 3 },
  authorityThreshold: {
    atOrBelow: 25,
    effect: { heal: 4, draw: 1 }
  },
  ally: { shield: 1 },
  text: "Gain 3 Authority.",
  thresholdText: "Last Stand — At 25 Authority or less: gain 4 additional Authority and draw 1 card.",
  allyText: "Gain 1 Shield.",
  flavor: "Only when the final wall trembles does the truest prayer begin."
},

{
  id: "blood_scent_challenger",
  name: "Blood-Scent Challenger",
  image: "blood_scent_challenger.png",
  faction: "green",
  cost: 4,
  shop_cost: 65,
  type: "ship",
  sigil: "⬢",
  effect: { combat: 4 },
  authorityThreshold: {
    atOrBelow: 20,
    effect: { combat: 6 }
  },
  ally: { combat: 2 },
  text: "Gain 4 Combat.",
  thresholdText: "Last Stand — At 20 Authority or less: gain 6 additional Combat.",
  allyText: "Gain 2 Combat.",
  flavor: "The closer death stands, the louder its challenge becomes."
},

{
  id: "desperation_oracle",
  name: "Desperation Oracle",
  image: "desperation_oracle.png",
  faction: "yellow",
  cost: 5,
  shop_cost: 100,
  type: "ship",
  sigil: "◈",
  effect: { trade: 2, shield: 2 },
  authorityThreshold: {
    atOrBelow: 15,
    effect: { draw: 2, stun: 1 }
  },
  ally: { combat: 2 },
  text: "Gain 2 Trade and 2 Shield.",
  thresholdText: "Last Stand — At 15 Authority or less: draw 2 cards and gain 1 Disable.",
  allyText: "Gain 2 Combat.",
  flavor: "Only the futures nearest extinction reveal every hidden path."
},


// ==========================================================
// CHARGES — BASES BUILD COUNTERS AND SPEND THEM FOR EFFECTS
// Each physical copy of a Base must track its own charge count.
// ==========================================================

{
  id: "storm_vault_monastery",
  name: "Storm-Vault Monastery",
  image: "storm_vault_monastery.png",
  faction: "blue",
  cost: 5,
  shop_cost: 95,
  type: "base",
  defense: 6,
  outpost: false,
  sigil: "✦",
  effect: { heal: 2 },
  charge: {
    trigger: "startOfTurn",
    gain: 1,
    max: 4,
    actions: [
      {
        label: "Aegis Release",
        cost: 1,
        repeatable: true,
        effect: { shield: 2 }
      },
      {
        label: "Mercy Release",
        cost: 1,
        repeatable: true,
        effect: { heal: 2 }
      }
    ]
  },
  ally: { trade: 1 },
  text: "Gain 2 Authority. At the start of your turn, place 1 Charge here, up to 4.",
  chargeText: "Spend 1 Charge: gain 2 Shield or 2 Authority. You may repeat this.",
  allyText: "Gain 1 Trade.",
  flavor: "Every unanswered prayer is stored until the sky itself replies."
},

{
  id: "doomsday_drum",
  name: "Doomsday Drum",
  image: "doomsday_drum.png",
  faction: "green",
  cost: 5,
  shop_cost: 100,
  type: "base",
  defense: 5,
  outpost: false,
  sigil: "⬢",
  effect: { combat: 1 },
  charge: {
    trigger: "friendlyFactionPlayed",
    faction: "green",
    gain: 1,
    max: 6,
    actions: [
      {
        label: "Sound the Horde",
        cost: "all",
        minimum: 2,
        effectPerCharge: { combat: 2 }
      }
    ]
  },
  ally: { combat: 2 },
  text: "Gain 1 Combat. Whenever you play another green card, place 1 Charge here, up to 6.",
  chargeText: "Spend all Charges, with at least 2: gain 2 Combat for each Charge spent.",
  allyText: "Gain 2 Combat.",
  flavor: "Each beat is a promise. The final beat is an arrival."
},

{
  id: "blackglass_soul_furnace",
  name: "Blackglass Soul Furnace",
  image: "blackglass_soul_furnace.png",
  faction: "red",
  cost: 15,
  shop_cost: 125,
  type: "base",
  defense: 6,
  outpost: true,
  sigil: "◒",
  effect: { combat: 3 },
  charge: {
    trigger: "ownCardPurged",
    gain: 1,
    max: 6,
    actions: [
      {
        label: "Consume the Ash",
        cost: 2,
        repeatable: false,
        oncePerTurn: true,
        effect: { draw: 1, combat: 3 }
      }
    ]
  },
  ally: { scrapOwn: 1 },
  text: "Outpost. Gain 3 Combat. Whenever you Purge one of your cards, place 1 Charge here, up to 6.",
  chargeText: "Once per turn, spend 2 Charges: draw 1 card and gain 3 Combat.",
  allyText: "Gain 1 Purge.",
  flavor: "Nothing cast away is wasted. The furnace teaches absence to burn."
},


// ==========================================================
// ECHO — COPY AN EARLIER CARD PLAYED DURING THE SAME TURN
// Recursive, Draw, Resurrection, and Transform effects are excluded.
// ==========================================================

{
  id: "echo_of_the_unchosen",
  name: "Echo of the Unchosen",
  image: "echo_of_the_unchosen.png",
  faction: "yellow",
  cost: 5,
  shop_cost: 110,
  type: "ship",
  sigil: "◈",
  effect: { trade: 1 },
  echo: {
    target: "previousPlayedShip",
    copy: "primary",
    excludeKeys: [
      "draw",
      "echo",
      "resurrect",
      "graveEcho",
      "transform"
    ]
  },
  ally: { shield: 2 },
  text: "Gain 1 Trade. Repeat the allowed Primary effects of the ship played immediately before this card.",
  echoText: "Echo cannot copy Draw, Resurrection, Grave Echo, another Echo, or Transform.",
  allyText: "Gain 2 Shield.",
  flavor: "The possibility was rejected. Its consequence arrived anyway."
},

{
  id: "warhowl_mimic",
  name: "Warhowl Mimic",
  image: "warhowl_mimic.png",
  faction: "green",
  cost: 4,
  shop_cost: 75,
  type: "ship",
  sigil: "⬢",
  effect: { combat: 2 },
  echo: {
    target: "previousPlayedShip",
    copyKeys: ["combat"],
    maximumCopiedCombat: 6
  },
  ally: { combat: 2 },
  text: "Gain 2 Combat. Repeat up to 6 Combat from the ship played immediately before this card.",
  echoText: "Echo only the previous ship's Primary Combat, to a maximum of 6.",
  allyText: "Gain 2 Combat.",
  flavor: "It does not understand the war cry. It understands what follows."
},


// ==========================================================
// TRANSFORMING CARDS
// The original card tracks progress per physical copy.
// The transformed forms are not collectible and never enter the Trade Deck.
// ==========================================================

{
  id: "young_siege_beast",
  name: "Young Siege-Beast",
  image: "young_siege_beast.png",
  faction: "green",
  cost: 2,
  shop_cost: 35,
  type: "ship",
  sigil: "⬢",
  effect: { combat: 2 },
  transform: {
    trigger: "timesPlayed",
    required: 3,
    into: "mature_gate_devourer",
    destination: "discard"
  },
  ally: {},
  text: "Gain 2 Combat.",
  transformText: "Growth — After this card has been played 3 times, transform it into Mature Gate-Devourer.",
  allyText: "",
  flavor: "Today it chews shields. Tomorrow it will discover walls."
},

{
  id: "mature_gate_devourer",
  name: "Mature Gate-Devourer",
  image: "mature_gate_devourer.png",
  faction: "green",
  collectible: false,
  token: true,
  transformedFrom: "young_siege_beast",
  cost: 5,
  shop_cost: 0,
  type: "ship",
  sigil: "⬢",
  effect: { combat: 7 },
  ally: { combat: 3 },
  text: "Gain 7 Combat.",
  allyText: "Gain 3 Combat.",
  flavor: "Its childhood ended with the first fortress it swallowed whole."
},

{
  id: "candle_vow_pilgrim",
  name: "Candle-Vow Pilgrim",
  image: "candle_vow_pilgrim.png",
  faction: "blue",
  cost: 2,
  shop_cost: 35,
  type: "ship",
  sigil: "✦",
  effect: { trade: 1, heal: 2 },
  transform: {
    trigger: "cumulativePrimaryEffect",
    effectKey: "heal",
    required: 6,
    into: "dawn_crowned_saint",
    destination: "discard"
  },
  ally: {},
  text: "Gain 1 Trade and 2 Authority.",
  transformText: "Ascension — After this card has granted a total of 6 Authority, transform it into Dawn-Crowned Saint.",
  allyText: "",
  flavor: "Every mile shortens the distance between pilgrim and relic."
},

{
  id: "dawn_crowned_saint",
  name: "Dawn-Crowned Saint",
  image: "dawn_crowned_saint.png",
  faction: "blue",
  collectible: false,
  token: true,
  transformedFrom: "candle_vow_pilgrim",
  cost: 5,
  shop_cost: 0,
  type: "ship",
  sigil: "✦",
  effect: { trade: 2, heal: 5 },
  ally: { draw: 1 },
  text: "Gain 2 Trade and 5 Authority.",
  allyText: "Draw 1 card.",
  flavor: "The road did not lead to the dawn. The road taught the pilgrim to become it."
},

{
  id: "graveborn_larva",
  name: "Graveborn Larva",
  image: "graveborn_larva.png",
  faction: "red",
  cost: 5,
  shop_cost: 40,
  type: "ship",
  sigil: "◒",
  effect: { combat: 2 },
  ally: {},
  sacrifice: { combat: 4 },
  transform: {
    trigger: "sacrificed",
    into: "graveborn_abomination",
    destination: "discard",
    replaceSacrificeRemoval: true
  },
  text: "Gain 2 Combat.",
  allyText: "",
  sacrificeText: "Sacrifice: Gain 4 Combat, then transform this into Graveborn Abomination in your discard pile.",
  transformText: "Metamorphosis — When sacrificed, replace this card with Graveborn Abomination in your discard pile.",
  flavor: "The Covenant calls the cocoon a grave because nothing innocent emerges."
},

{
  id: "graveborn_abomination",
  name: "Graveborn Abomination",
  image: "graveborn_abomination.png",
  faction: "red",
  collectible: false,
  token: true,
  transformedFrom: "graveborn_larva",
  cost: 9,
  shop_cost: 0,
  type: "ship",
  sigil: "◒",
  effect: { combat: 6, scrapOwn: 1 },
  ally: { lifelink: 0.5 },
  sacrifice: { damageAll: 2 },
  text: "Gain 6 Combat and 1 Purge.",
  allyText: "Gain Lifelink 50% this turn.",
  sacrificeText: "Sacrifice: Damage All 2.",
  flavor: "It remembers being small only as a reason to hate the world."
},

{
  id: "bloodscript_initiate",
  name: "Bloodscript Initiate",
  image: "bloodscript_initiate.png",
  faction: "red",
  cost: 8,
  shop_cost: 40,
  type: "ship",
  sigil: "◒",
  effect: { combat: 3, scrapOwn: 1 },
  ally: { trade: 1 },
  sacrifice: { draw: 2, combat: 3 },
  text: "Gain 3 Combat and 1 Purge.",
  allyText: "Gain 1 Trade.",
  sacrificeText: "Sacrifice: Draw 2 cards and gain 3 Combat.",
  flavor: "The final line of the ritual is always written with the author."
},
  {
    id: "basilica_of_many_lights",
    name: "Basilica of Many Lights",
    image: "basilica_of_many_lights.png",
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
    image: "reliquary_shieldfleet.png",
    faction: "blue",
    cost: 11,
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
  // ==========================================================
// 10 LOW-COST CARDS — COST 1–2 — NO ALLY ABILITIES
// ==========================================================

// ==========================================================
// YELLOW — XYTHE CONCORD
// ==========================================================
{
  id: "starpath_mite",
  name: "Starpath Mite",
  image: "starpath_mite.png",
  faction: "yellow",
  cost: 1,
  shop_cost: 10,
  type: "ship",
  sigil: "◈",
  effect: { trade: 1, combat: 1 },
  ally: {},
  text: "Gain 1 Trade and 1 Combat.",
  allyText: "",
  flavor: "Even the smallest Concord creature knows where the enemy will step."
},
{
  id: "future_shard",
  name: "Future Shard",
  image: "future_shard.png",
  faction: "yellow",
  cost: 3,
  shop_cost: 20,
  type: "ship",
  sigil: "◈",
  effect: { draw: 1 },
  ally: { trade: 1 },
  text: "Draw 1 card.",
  allyText: "",
  flavor: "A single possibility preserved long enough to become useful."
},
{
  id: "void_current_scout",
  name: "Void-Current Scout",
  image: "void_current_scout.png",
  faction: "yellow",
  cost: 2,
  shop_cost: 20,
  type: "ship",
  sigil: "◈",
  effect: { combat: 2, trade: 1 },
  ally: {},
  text: "Gain 2 Combat and 1 Trade.",
  allyText: "",
  flavor: "It rides the invisible current between profit and violence."
},

// ==========================================================
// BLUE — AZURE ASCENDANCY
// ==========================================================
{
  id: "chapel_attendant",
  name: "Chapel Attendant",
  image: "chapel_attendant.png",
  faction: "blue",
  cost: 1,
  shop_cost: 10,
  type: "ship",
  sigil: "✦",
  effect: { heal: 2 },
  ally: {},
  text: "Gain 2 Authority.",
  allyText: "",
  flavor: "A quiet prayer can hold a wounded soldier together."
},
{
  id: "dawn_supply_runner",
  name: "Dawn Supply Runner",
  image: "dawn_supply_runner.png",
  faction: "blue",
  cost: 2,
  shop_cost: 20,
  type: "ship",
  sigil: "✦",
  effect: { trade: 2, heal: 1 },
  ally: {},
  text: "Gain 2 Trade and 1 Authority.",
  allyText: "",
  flavor: "The Ascendancy considers punctual delivery a sacred duty."
},

// ==========================================================
// GREEN — GORAK WARHOST
// ==========================================================
{
  id: "mudboot_rusher",
  name: "Mudboot Rusher",
  image: "mudboot_rusher.png",
  faction: "green",
  cost: 1,
  shop_cost: 10,
  type: "ship",
  sigil: "⬢",
  effect: { combat: 2 },
  ally: {},
  text: "Gain 2 Combat.",
  allyText: "",
  flavor: "It has never waited for the ground to become suitable."
},
{
  id: "scrap_pocket_raider",
  name: "Scrap-Pocket Raider",
  image: "scrap_pocket_raider.png",
  faction: "green",
  cost: 2,
  shop_cost: 20,
  type: "ship",
  sigil: "⬢",
  effect: { trade: 2, combat: 1 },
  ally: {},
  text: "Gain 2 Trade and 1 Combat.",
  allyText: "",
  flavor: "Every pocket is full before the raid officially begins."
},
{
  id: "two_axe_runt",
  name: "Two-Axe Runt",
  image: "two_axe_runt.png",
  faction: "green",
  cost: 2,
  shop_cost: 20,
  type: "ship",
  sigil: "⬢",
  effect: { combat: 4 },
  ally: {},
  text: "Gain 4 Combat.",
  allyText: "",
  flavor: "It carries two axes because counting beyond two seemed unnecessary."
},

// ==========================================================
// RED — UMBRAL COVENANT
// ==========================================================
{
  id: "candle_blood_apprentice",
  name: "Candleblood Apprentice",
  image: "candle_blood_apprentice.png",
  faction: "red",
  cost: 2,
  shop_cost: 10,
  type: "ship",
  sigil: "◒",
  effect: { combat: 1, scrapOwn: 1 },
  ally: {},
  text: "Gain 1 Combat and 1 Purge.",
  allyText: "",
  flavor: "The first lesson is learning which part of yourself to burn."
},
{
  id: "grave_coin_collector",
  name: "Grave-Coin Collector",
  image: "grave_coin_collector.png",
  faction: "red",
  cost: 3,
  shop_cost: 20,
  type: "ship",
  sigil: "◒",
  effect: { trade: 2, scrapOwn: 1 },
  ally: {},
  text: "Gain 2 Trade and 1 Purge.",
  allyText: "",
  flavor: "The dead have little use for currency or unfinished promises."
},
  {
  id: "sorynth_choice_seer",
  name: "Sorynth Choice-Seer",
  image: "sorynth_choice_seer.png",
  faction: "yellow",
  cost: 3,
  shop_cost: 55,
  type: "ship",
  sigil: "◈",
  effect: {
    or: [
      {
        label: "Harvest",
        effect: { trade: 2 }
      },
      {
        label: "Assault",
        effect: { combat: 3 }
      },
      {
        label: "Interfere",
        effect: { stun: 1 }
      }
    ]
  },
  ally: { shield: 1 },
  text: "Choose one: gain 2 Trade; gain 3 Combat; or gain 1 Disable.",
  allyText: "Gain 1 Shield.",
  flavor: "It studies three futures and permits only the useful one."
},
{
  id: "nhalor_forked_oracle",
  name: "Nhalor Forked Oracle",
  image: "nhalor_forked_oracle.png",
  faction: "yellow",
  cost: 5,
  shop_cost: 95,
  type: "ship",
  sigil: "◈",
  effect: {
    or: [
      {
        label: "Foresee",
        effect: { draw: 1, trade: 1 }
      },
      {
        label: "Deny",
        effect: {
          opponentDiscard: 1,
          shield: 2
        }
      },
      {
        label: "Collapse",
        effect: { combat: 5 }
      }
    ]
  },
  ally: { combat: 1 },
  text: "Choose one: draw 1 card and gain 1 Trade; gain 2 Shield and the next enemy draws 1 fewer card; or gain 5 Combat.",
  allyText: "Gain 1 Combat.",
  flavor: "Every answer is true until Nhalor chooses which truth survives."
},

// ==========================================================
// BLUE — CHOICE / OR TROOPS
// ==========================================================
{
  id: "dawnspear_adjudicator",
  name: "Dawnspear Adjudicator",
  image: "dawnspear_adjudicator.png",
  faction: "blue",
  cost: 3,
  shop_cost: 50,
  type: "ship",
  sigil: "✦",
  effect: {
    or: [
      {
        label: "Judgment",
        effect: { combat: 3 }
      },
      {
        label: "Provision",
        effect: {
          trade: 2,
          heal: 2
        }
      },
      {
        label: "Intercession",
        effect: { shield: 3 }
      }
    ]
  },
  ally: { heal: 1 },
  text: "Choose one: gain 3 Combat; gain 2 Trade and 2 Authority; or gain 3 Shield.",
  allyText: "Gain 1 Authority.",
  flavor: "Its verdict may arrive as blade, blessing, or barrier."
},
{
  id: "seraph_of_three_mercies",
  name: "Seraph of Three Mercies",
  image: "seraph_of_three_mercies.png",
  faction: "blue",
  cost: 5,
  shop_cost: 90,
  type: "ship",
  sigil: "✦",
  effect: {
    or: [
      {
        label: "Renew",
        effect: {
          draw: 1,
          heal: 2
        }
      },
      {
        label: "Defend",
        effect: {
          combat: 4,
          shield: 2
        }
      },
      {
        label: "Sustain",
        effect: {
          trade: 3,
          heal: 3
        }
      }
    ]
  },
  ally: { shield: 2 },
  text: "Choose one: draw 1 card and gain 2 Authority; gain 4 Combat and 2 Shield; or gain 3 Trade and 3 Authority.",
  allyText: "Gain 2 Shield.",
  flavor: "Mercy is not indecision. It is the power to answer every need."
},

// ==========================================================
// GREEN — CHOICE / OR TROOPS
// ==========================================================
{
  id: "split_tusk_reaver",
  name: "Split-Tusk Reaver",
  image: "split_tusk_reaver.png",
  faction: "green",
  cost: 3,
  shop_cost: 45,
  type: "ship",
  sigil: "⬢",
  effect: {
    or: [
      {
        label: "Smash",
        effect: { combat: 5 }
      },
      {
        label: "Loot",
        effect: { trade: 3 }
      }
    ]
  },
  ally: { combat: 2 },
  text: "Choose one: gain 5 Combat or gain 3 Trade.",
  allyText: "Gain 2 Combat.",
  flavor: "One tusk breaks the gate. The other carries away the hinges."
},
{
  id: "rukgar_war_chooser",
  name: "Rukgar War-Chooser",
  image: "rukgar_war_chooser.png",
  faction: "green",
  cost: 6,
  shop_cost: 85,
  type: "ship",
  sigil: "⬢",
  effect: {
    or: [
      {
        label: "Crush",
        effect: { combat: 7 }
      },
      {
        label: "Plunder",
        effect: {
          trade: 3,
          scrapMarket: 1
        }
      },
      {
        label: "Rally",
        effect: {
          draw: 1,
          combat: 3
        }
      }
    ]
  },
  ally: { combat: 2 },
  text: "Choose one: gain 7 Combat; gain 3 Trade and 1 Market Erase; or draw 1 card and gain 3 Combat.",
  allyText: "Gain 2 Combat.",
  flavor: "Rukgar calls it strategy when he chooses what to break first."
},

// ==========================================================
// RED — CHOICE / OR TROOPS
// ==========================================================
{
  id: "ashen_crossroads_adept",
  name: "Ashen Crossroads Adept",
  image: "ashen_crossroads_adept.png",
  faction: "red",
  cost: 3,
  shop_cost: 55,
  type: "ship",
  sigil: "◒",
  effect: {
    or: [
      {
        label: "Extort",
        effect: { trade: 2 }
      },
      {
        label: "Torment",
        effect: { combat: 3 }
      },
      {
        label: "Sever",
        effect: { scrapOwn: 1 }
      }
    ]
  },
  ally: { combat: 1 },
  text: "Choose one: gain 2 Trade; gain 3 Combat; or gain 1 Purge.",
  allyText: "Gain 1 Combat.",
  flavor: "Every road demands a payment. The Adept chooses the currency."
},
{
  id: "null_vow_executioner",
  name: "Null-Vow Executioner",
  image: "null_vow_executioner.png",
  faction: "red",
  cost: 9,
  shop_cost: 120,
  type: "ship",
  sigil: "◒",
  effect: {
    or: [
      {
        label: "Annihilate",
        effect: { combat: 7 }
      },
      {
        label: "Claim",
        effect: { trade: 4 }
      },
      {
        label: "Unmake",
        effect: {
          scrapOwn: 2,
          draw: 1
        }
      }
    ]
  },
  ally: { combat: 2 },
  text: "Choose one: gain 7 Combat; gain 4 Trade; or gain 2 Purge and draw 1 card.",
  allyText: "Gain 2 Combat.",
  flavor: "The vow ends only when the speaker, the witness, or the world is gone."
},
  {
  id: "ilyrion_keeper_of_the_unchosen",
  name: "Ilyrion, Keeper of the Unchosen",
  image: "ilyrion_keeper_of_the_unchosen.png",
  faction: "yellow",
  cost: 16,
  shop_cost: 475,
  type: "ship",
  sigil: "◈",
  effect: {
    or: [
      {
        label: "Foresee",
        effect: {
          draw: 2,
          trade: 2
        }
      },
      {
        label: "Deny",
        effect: {
          shield: 3,
          stun: 2,
          opponentDiscard: 1
        }
      },
      {
        label: "Collapse",
        effect: {
          combat: 11,
          destroyBase: 1
        }
      }
    ]
  },
  ally: {
    draw: 1
  },
  doubleAlly: {
    stun: 1,
    opponentDiscard: 1
  },
  text: "Choose one: draw 2 cards and gain 2 Trade; gain 3 Shield and 2 Disable, and the next enemy draws 1 fewer card; or gain 11 Combat and 1 Raze.",
  allyText: "Draw 1 card.",
  doubleAllyText: "Gain 1 Disable. The next enemy draws 1 fewer card.",
  flavor: "It remembers every future the universe was forced to abandon."
},

{
  id: "the_impossible_citadel",
  name: "The Impossible Citadel",
  image: "the_impossible_citadel.png",
  faction: "yellow",
  cost: 16,
  shop_cost: 650,
  type: "base",
  defense: 11,
  outpost: true,
  sigil: "◈",
  effect: {
    shield: 6,
    stun: 1
  },
  ally: {
    draw: 1,
    trade: 2
  },
  doubleAlly: {
    opponentDiscard: 2,
    shield: 4
  },
  sacrifice: {
    destroyBase: 1,
    draw: 2
  },
  text: "Outpost. Gain 6 Shield and 1 Disable.",
  allyText: "Draw 1 card and gain 2 Trade.",
  doubleAllyText: "Gain 4 Shield. The next enemy draws 2 fewer cards.",
  sacrificeText: "Sacrifice: Gain 1 Raze and draw 2 cards.",
  flavor: "It stands where no fortress could exist and guards a war that never began."
},


// ==========================================================
// BLUE — AZURE ASCENDANCY
// ==========================================================

{
  id: "seraph_of_the_last_dawn",
  name: "Seraph of the Last Dawn",
  image: "seraph_of_the_last_dawn.png",
  faction: "blue",
  cost: 14,
  shop_cost: 450,
  type: "ship",
  sigil: "✦",
  effect: {
    combat: 8,
    heal: 8,
    lifelink: 0.5
  },
  ally: {
    draw: 1,
    shield: 3
  },
  doubleAlly: {
    combat: 4,
    heal: 4
  },
  text: "Gain 8 Combat, 8 Authority, and Lifelink 50% this turn.",
  allyText: "Draw 1 card and gain 3 Shield.",
  doubleAllyText: "Gain 4 Combat and 4 Authority.",
  flavor: "When every other light is extinguished, its wings become the morning."
},

{
  id: "eternal_procession",
  name: "Eternal Procession",
  image: "eternal_procession.png",
  faction: "blue",
  cost: 16,
  shop_cost: 625,
  type: "base",
  defense: 10,
  outpost: false,
  sigil: "✦",
  effect: {
    trade: 4,
    healPerBase: 2
  },
  ally: {
    shield: 5
  },
  doubleAlly: {
    draw: 2,
    heal: 5
  },
  sacrifice: {
    damageAll: 2,
    heal: 8
  },
  text: "Gain 4 Trade and 2 Authority for each active Base you control.",
  allyText: "Gain 5 Shield.",
  doubleAllyText: "Draw 2 cards and gain 5 Authority.",
  sacrificeText: "Sacrifice: Damage All 2 and gain 8 Authority.",
  flavor: "The first banner began marching before the oldest kingdom had a name."
},

{
  id: "tideborne_world_sanctifier",
  name: "Tideborne World-Sanctifier",
  image: "tideborne_world_sanctifier.png",
  faction: "blue",
  cost: 15,
  shop_cost: 485,
  type: "ship",
  sigil: "✦",
  effect: {
    or: [
      {
        label: "Judgment",
        effect: {
          combat: 10,
          heal: 4
        }
      },
      {
        label: "Renewal",
        effect: {
          draw: 2,
          heal: 8
        }
      },
      {
        label: "Provision",
        effect: {
          trade: 6,
          heal: 5
        }
      }
    ]
  },
  ally: {
    shield: 3
  },
  sacrifice: {
    heal: 10
  },
  text: "Choose one: gain 10 Combat and 4 Authority; draw 2 cards and gain 8 Authority; or gain 6 Trade and 5 Authority.",
  allyText: "Gain 3 Shield.",
  sacrificeText: "Sacrifice: Gain 10 Authority.",
  flavor: "Entire worlds are submerged, cleansed, and raised again beneath its blessing."
},


// ==========================================================
// GREEN — GORAK WARHOST
// ==========================================================

{
  id: "morgath_gate_ending",
  name: "Morgath Gate-Ending",
  image: "morgath_gate_ending.png",
  faction: "green",
  cost: 16,
  shop_cost: 440,
  type: "ship",
  sigil: "⬢",
  effect: {
    combat: 13,
    destroyBase: 1
  },
  ally: {
    combat: 5
  },
  sacrifice: {
    combat: 8
  },
  text: "Gain 13 Combat and 1 Raze.",
  allyText: "Gain 5 Combat.",
  sacrificeText: "Sacrifice: Gain 8 Combat.",
  flavor: "Morgath has never encountered a gate twice."
},

{
  id: "the_hundred_drum_horde",
  name: "The Hundred-Drum Horde",
  image: "the_hundred_drum_horde.png",
  faction: "green",
  cost: 12,
  shop_cost: 600,
  type: "ship",
  sigil: "⬢",
  effect: {
    combat: 8,
    combatPerBase: 2
  },
  ally: {
    damageAll: 2
  },
  doubleAlly: {
    combat: 6,
    draw: 1
  },
  text: "Gain 8 Combat plus 2 Combat for each active Base you control.",
  allyText: "Damage All 2.",
  doubleAllyText: "Gain 6 Combat and draw 1 card.",
  flavor: "The first drum calls the army. The hundredth announces that resistance has ended."
},

{
  id: "gorak_thronecrusher",
  name: "Gorak Thronecrusher",
  image: "gorak_thronecrusher.png",
  faction: "green",
  cost: 16,
  shop_cost: 675,
  type: "ship",
  sigil: "⬢",
  effect: {
    or: [
      {
        label: "Trample",
        effect: {
          combat: 16
        }
      },
      {
        label: "Demolish",
        effect: {
          combat: 10,
          destroyBase: 1
        }
      },
      {
        label: "Pillage",
        effect: {
          trade: 7,
          scrapMarket: 1
        }
      }
    ]
  },
  ally: {
    combat: 4
  },
  sacrifice: {
    combat: 10
  },
  text: "Choose one: gain 16 Combat; gain 10 Combat and 1 Raze; or gain 7 Trade and 1 Market Erase.",
  allyText: "Gain 4 Combat.",
  sacrificeText: "Sacrifice: Gain 10 Combat.",
  flavor: "It does not sit upon conquered thrones. It carries them away as trophies."
},


// ==========================================================
// RED — UMBRAL COVENANT
// ==========================================================

{
  id: "vharos_debt_of_worlds",
  name: "Vharos, Debt of Worlds",
  image: "vharos_debt_of_worlds.png",
  faction: "red",
  cost: 16,
  shop_cost: 475,
  type: "ship",
  sigil: "◒",
  effect: {
    combat: 9,
    scrapOwn: 2,
    lifelink: 0.5
  },
  ally: {
    draw: 1,
    combat: 3
  },
  doubleAlly: {
    destroyBase: 1,
    opponentDiscard: 1
  },
  text: "Gain 9 Combat, 2 Purge, and Lifelink 50% this turn.",
  allyText: "Draw 1 card and gain 3 Combat.",
  doubleAllyText: "Gain 1 Raze. The next enemy draws 1 fewer card.",
  flavor: "Every empire eventually discovers that its victories were purchased on Vharos's credit."
},

{
  id: "black_sun_apotheosis",
  name: "Black Sun Apotheosis",
  image: "black_sun_apotheosis.png",
  faction: "red",
  cost: 16,
  shop_cost: 700,
  type: "base",
  defense: 9,
  outpost: true,
  sigil: "◒",
  effect: {
    combat: 5,
    damageAll: 2,
    scrapOwn: 1
  },
  ally: {
    draw: 1
  },
  doubleAlly: {
    combat: 6,
    lifelink: 0.5
  },
  sacrifice: {
    combat: 8,
    draw: 2
  },
  text: "Outpost. Gain 5 Combat, Damage All 2, and gain 1 Purge.",
  allyText: "Draw 1 card.",
  doubleAllyText: "Gain 6 Combat and Lifelink 50% this turn.",
  sacrificeText: "Sacrifice: Gain 8 Combat and draw 2 cards.",
  flavor: "The faithful did not build a temple beneath the eclipse. They taught the eclipse to worship."
},
  {
    id: "warcamp_foundry",
    name: "Warcamp Foundry",
    image: "warcamp_foundry.png",
    faction: "green",
    cost: 6,
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
    image: "siege_root_colossus.png",
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
    image: "gatebreaker_alpha.png",
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
    image: "eclipse_harrower.png",
    faction: "red",
    cost: 12,
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
    image: "ashen_tithe_engine.png",
    faction: "red",
    cost: 9,
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
    image: "rift_manta.png",
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
    image: "mind_crown.png",
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
    image: "astral_taxer.png",
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
    image: "soveriegn_dreadnought.png",
    faction: "yellow",
    cost: 7,
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
    image: "reality_harvester.png",
    faction: "yellow",
    cost: 9,
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
    image: "void_leviathan.png",
    faction: "yellow",
    cost: 11,
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
    image: "threadseer_familiar.png",
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
    image: "dreamfold_navigator.png",
    faction: "yellow",
    cost: 5,
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
    image: "probability_nursery.png",
    faction: "yellow",
    cost: 6,
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
    image: "many_mind_oracle.png",
    faction: "yellow",
    cost: 9,
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
    image: "echo_shepherd.png",
    faction: "yellow",
    cost: 5,
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
    image: "stasis_menagerie.png",
    faction: "yellow",
    cost: 7,
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
    image: "fate_tether_colossus.png",
    faction: "yellow",
    cost: 8,
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
    image: "hourglass_devourer.png",
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
    image: "archive_swarm.png",
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
    image: "paradox_gate.png",
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

  // DOUBLE ALLY CARDS
  {
    id: "triune_oracle",
    name: "Triune Oracle",
    image: "triune_oracle.png",
    faction: "yellow",
    cost: 4,
    shop_cost: 150,
    type: "ship",
    sigil: "◈",
    effect: { trade: 1 },
    ally: { combat: 1 },
    doubleAlly: { draw: 2 },
    text: "Gain 1 Trade.",
    allyText: "Gain 1 Combat.",
    doubleAllyText: "Draw 2 cards.",
    flavor: "One mind predicts. Three minds decide."
  },
  {
    id: "hourglass_convergence",
    name: "Hourglass Convergence",
    image: "hourglass_convergence.png",
    faction: "yellow",
    cost: 5,
    shop_cost: 190,
    type: "base",
    defense: 5,
    outpost: false,
    sigil: "◈",
    effect: { trade: 1, shield: 1 },
    ally: {},
    doubleAlly: { draw: 1, opponentDiscard: 1 },
    text: "Gain 1 Trade and 1 Shield.",
    allyText: "",
    doubleAllyText: "Draw 1 card. The next enemy draws 1 fewer card.",
    flavor: "Every future narrows toward the Concord."
  },
  {
    id: "many_mind_apex",
    name: "Many-Mind Apex",
    image: "many_mind_apex.png",
    faction: "yellow",
    cost: 7,
    shop_cost: 290,
    type: "ship",
    sigil: "◈",
    effect: { combat: 3, shield: 2 },
    ally: { draw: 1 },
    doubleAlly: { combat: 4, stun: 2 },
    text: "Gain 3 Combat and 2 Shield.",
    allyText: "Draw 1 card.",
    doubleAllyText: "Gain 4 Combat and 2 Disable.",
    flavor: "Consensus is merely the first stage of assimilation."
  },

  {
    id: "veyrix_pathsplitter",
    name: "Veyrix Pathsplitter",
    image: "veyrix_pathsplitter.png",
    faction: "yellow",
    cost: 3,
    shop_cost: 110,
    type: "ship",
    sigil: "◈",
    effect: { trade: 1 },
    ally: { combat: 1 },
    doubleAlly: { draw: 1, stun: 1 },
    text: "Gain 1 Trade.",
    allyText: "Gain 1 Combat.",
    doubleAllyText: "Draw 1 card and gain 1 Disable.",
    flavor: "One route reaches the destination. Three routes ensure nothing else does."
  },
  {
    id: "the_third_possibility",
    name: "The Third Possibility",
    image: "the_third_possibility.png",
    faction: "yellow",
    cost: 5,
    shop_cost: 195,
    type: "base",
    defense: 6,
    outpost: false,
    sigil: "◈",
    effect: { shield: 2 },
    ally: { trade: 1 },
    doubleAlly: { draw: 2 },
    text: "Gain 2 Shield.",
    allyText: "Gain 1 Trade.",
    doubleAllyText: "Draw 2 cards.",
    flavor: "Two minds see the choice. The third discovers the outcome hidden between them."
  },
  {
    id: "orrakai_horizon_eater",
    name: "Orrakai, Horizon-Eater",
    image: "orrakai_horizon_eater.png",
    faction: "yellow",
    cost: 7,
    shop_cost: 300,
    type: "ship",
    sigil: "◈",
    effect: { combat: 4, shield: 2 },
    ally: { draw: 1 },
    doubleAlly: { combat: 5, stun: 2, opponentDiscard: 1 },
    text: "Gain 4 Combat and 2 Shield.",
    allyText: "Draw 1 card.",
    doubleAllyText: "Gain 5 Combat and 2 Disable. The next enemy draws 1 fewer card.",
    flavor: "Every future ends at the edge of its gaze."
  },

  // ==========================================================
  // BLUE — AZURE ASCENDANCY
  // ==========================================================
  {
    id: "tithe_acolyte",
    name: "Tithe Acolyte",
    image: "tithe_acolyte.png",
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
    image: "pilgrim_of_dawn.png",
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
    image: "votive_scribe.png",
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
    image: "seraphic_envoy.png",
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
    image: "azure_chapel.png",
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
    image: "lumen_knight.png",
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
    image: "reliquary_guard.png",
    faction: "blue",
    cost: 4,
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
    image: "choir_of_wings.png",
    faction: "blue",
    cost: 5,
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
    image: "absolution_choir.png",
    faction: "blue",
    cost: 5,
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
    image: "dawn_bastion.png",
    faction: "blue",
    cost: 6,
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
    image: "mercy_herald.png",
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
    image: "throne_of_tides.png",
    faction: "blue",
    cost: 9,
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
    image: "saint_of_the_deep.png",
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
    image: "golden_litany.png",
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
    image: "ascendant_host.png",
    faction: "blue",
    cost: 9,
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
    image: "radiant_lancer.png",
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
    image: "battle_psalmist.png",
    faction: "blue",
    cost: 5,
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
    image: "aegis_gunship.png",
    faction: "blue",
    cost: 6,
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
    image: "judgment_seraph.png",
    faction: "blue",
    cost: 8,
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
    image: "tidebound_chaplain.png",
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
    image: "beacon_monastery.png",
    faction: "blue",
    cost: 5,
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
    image: "oathwing_marshal.png",
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
    image: "tideglass_confessor.png",
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
    image: "procession_of_seven_banners.png",
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

  // DOUBLE ALLY CARDS
  {
    id: "threefold_vanguard",
    name: "Threefold Vanguard",
    image: "threefold_vanguard.png",
    faction: "blue",
    cost: 3,
    shop_cost: 105,
    type: "ship",
    sigil: "✦",
    effect: { combat: 2 },
    ally: { shield: 1 },
    doubleAlly: { shield: 4 },
    text: "Gain 2 Combat.",
    allyText: "Gain 1 Shield.",
    doubleAllyText: "Gain 4 Shield.",
    flavor: "One guards the left. One guards the right. One guards the oath."
  },
  {
    id: "beacon_trinity",
    name: "Beacon Trinity",
    image: "beacon_trinity.png",
    faction: "blue",
    cost: 5,
    shop_cost: 185,
    type: "base",
    defense: 6,
    outpost: false,
    sigil: "✦",
    effect: { trade: 1 },
    ally: {},
    doubleAlly: { draw: 2, shield: 2 },
    text: "Gain 1 Trade.",
    allyText: "",
    doubleAllyText: "Draw 2 cards and gain 2 Shield.",
    flavor: "No vessel is lost while all three lights remain."
  },
  {
    id: "oathfleet_paragon",
    name: "Oathfleet Paragon",
    image: "oathfleet_paragon.png",
    faction: "blue",
    cost: 6,
    shop_cost: 240,
    type: "ship",
    sigil: "✦",
    effect: { combat: 4 },
    ally: { trade: 1 },
    doubleAlly: { combat: 4, draw: 1 },
    text: "Gain 4 Combat.",
    allyText: "Gain 1 Trade.",
    doubleAllyText: "Gain 4 Combat and draw 1 card.",
    flavor: "Three captains. One command."
  },

  {
    id: "saelune_intercessor",
    name: "Saelune Intercessor",
    image: "saelune_intercessor.png",
    faction: "blue",
    cost: 3,
    shop_cost: 115,
    type: "ship",
    sigil: "✦",
    effect: { combat: 2, heal: 1 },
    ally: { shield: 2 },
    doubleAlly: { draw: 1, shield: 3 },
    text: "Gain 2 Combat and 1 Authority.",
    allyText: "Gain 2 Shield.",
    doubleAllyText: "Draw 1 card and gain 3 Shield.",
    flavor: "Every shield is a vow made visible."
  },
  {
    id: "monastery_of_the_drowned_star",
    name: "Monastery of the Drowned Star",
    image: "monastery_of_the_drowned_star.png",
    faction: "blue",
    cost: 6,
    shop_cost: 250,
    type: "base",
    defense: 7,
    outpost: true,
    sigil: "✦",
    effect: { heal: 3 },
    ally: { shield: 2 },
    doubleAlly: { draw: 2, heal: 3 },
    text: "Outpost. Gain 3 Authority.",
    allyText: "Gain 2 Shield.",
    doubleAllyText: "Draw 2 cards and gain 3 Authority.",
    flavor: "The sea buried the star. The faithful built downward to meet it."
  },
  {
    id: "halcyon_vowmarshal",
    name: "Halcyon Vowmarshal",
    image: "halcyon_vowmarshal.png",
    faction: "blue",
    cost: 7,
    shop_cost: 300,
    type: "ship",
    sigil: "✦",
    effect: { combat: 4, shield: 2 },
    ally: { heal: 2 },
    doubleAlly: { combat: 5, draw: 1, shield: 3 },
    text: "Gain 4 Combat and 2 Shield.",
    allyText: "Gain 2 Authority.",
    doubleAllyText: "Gain 5 Combat, draw 1 card, and gain 3 Shield.",
    flavor: "A vow spoken in its presence becomes armor, command, and blade."
  },

  // ==========================================================
  // GREEN — GORAK WARHOST
  // ==========================================================
  {
    id: "gore_runner",
    name: "Gore Runner",
    image: "gore_runner.png",
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
    image: "scrapfang_runt.png",
    faction: "green",
    cost: 2,
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
    image: "loot_cart.png",
    faction: "green",
    cost: 3,
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
    image: "axe_mob.png",
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
    image: "siege_boar.png",
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
    image: "war_drum_camp.png",
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
    image: "rage_caller.png",
    faction: "green",
    cost: 4,
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
    image: "breakneck_stockade.png",
    faction: "green",
    cost: 6,
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
    image: "skullcrag_warlord.png",
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
    image: "stampede_totem.png",
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
    image: "last_charge_titan.png",
    faction: "green",
    cost: 8,
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
    image: "bone_wagon.png",
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
    image: "worldbreaker_horde.png",
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
    image: "gorak_colossus.png",
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
    image: "endwar_mammoth.png",
    faction: "green",
    cost: 9,
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
    image: "plunder_shaman.png",
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
    image: "ironroot_encampment.png",
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
    image: "raidmaster's_cache.png",
    faction: "green",
    cost: 6,
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
    image: "thunderhoof_quartermaster.png",
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
    image: "chainmaw_ravager.png",
    faction: "green",
    cost: 5,
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
    image: "spoils_foundry.png",
    faction: "green",
    cost: 8,
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
    image: "spoils_harvester.png",
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
    image: "chaincamp_drumworks.png",
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
    image: "bastion_eater_alpha.png",
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

  // DOUBLE ALLY CARDS
  {
    id: "spoils_council",
    name: "Spoils Council",
    image: "spoils_council.png",
    faction: "green",
    cost: 4,
    shop_cost: 145,
    type: "ship",
    sigil: "⬢",
    effect: { trade: 2 },
    ally: {},
    doubleAlly: { trade: 3, combat: 2 },
    text: "Gain 2 Trade.",
    allyText: "",
    doubleAllyText: "Gain 3 Trade and 2 Combat.",
    flavor: "The first takes the gold. The second takes the steel. The third takes whatever remains."
  },
  {
    id: "rootbound_legion",
    name: "Rootbound Legion",
    image: "rootbound_legion.png",
    faction: "green",
    cost: 5,
    shop_cost: 195,
    type: "base",
    defense: 6,
    outpost: true,
    sigil: "⬢",
    effect: { combat: 2 },
    ally: { shield: 1 },
    doubleAlly: { combat: 3, shield: 3 },
    text: "Outpost. Gain 2 Combat.",
    allyText: "Gain 1 Shield.",
    doubleAllyText: "Gain 3 Combat and 3 Shield.",
    flavor: "A single root bends. A forest does not."
  },

  {
    id: "grinskull_manyhand",
    name: "Grinskull Manyhand",
    image: "grinskull_manyhand.png",
    faction: "green",
    cost: 3,
    shop_cost: 115,
    type: "ship",
    sigil: "⬢",
    effect: { combat: 3 },
    ally: { combat: 1 },
    doubleAlly: { combat: 4, trade: 2 },
    text: "Gain 3 Combat.",
    allyText: "Gain 1 Combat.",
    doubleAllyText: "Gain 4 Combat and 2 Trade.",
    flavor: "Every hand carries a trophy. Every trophy is still sharp."
  },
  {
    id: "murog_skymaul",
    name: "Murog Skymaul",
    image: "murog_skymaul.png",
    faction: "green",
    cost: 6,
    shop_cost: 255,
    type: "ship",
    sigil: "⬢",
    effect: { combat: 6 },
    ally: { combat: 2 },
    doubleAlly: { combat: 4, destroyBase: 1 },
    text: "Gain 6 Combat.",
    allyText: "Gain 2 Combat.",
    doubleAllyText: "Gain 4 Combat and 1 Raze.",
    flavor: "The platform provides altitude. Murog provides the landing."
  },

  // ==========================================================
  // RED — UMBRAL COVENANT
  // ==========================================================
  {
    id: "ash_initiate",
    name: "Ash Initiate",
    image: "ash_initiate.png",
    faction: "red",
    cost: 3,
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
    image: "coin_curse.png",
    faction: "red",
    cost: 2,
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
    image: "blood_channeler.png",
    faction: "red",
    cost: 3,
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
    image: "bone_appraiser.png",
    faction: "red",
    cost: 3,
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
    image: "grave_bargain.png",
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
    image: "obsidian_altar.png",
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
    image: "hexed_portal.png",
    faction: "red",
    cost: 5,
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
    image: "flesh_ledger.png",
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
    image: "soul_furnace.png",
    faction: "red",
    cost: 7,
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
    image: "death_tithe.png",
    faction: "red",
    cost: 5,
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
    image: "cinder_surgeon.png",
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
    image: "void_auctioneer.png",
    faction: "red",
    cost: 9,
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
    image: "eclipse_archon.png",
    faction: "red",
    cost: 11,
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
    image: "black_sun_cathedral.png",
    faction: "red",
    cost: 9,
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
    image: "realm_eater.png",
    faction: "red",
    cost: 13,
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
    image: "culling_whisper.png",
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
    image: "memory_pyre.png",
    faction: "red",
    cost: 8,
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
    image: "ashen_reclaimer.png",
    faction: "red",
    cost: 12,
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
    image: "abyssal_winnower.png",
    faction: "red",
    cost: 9,
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
    image: "severance_adept.png",
    faction: "red",
    cost: 4,
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
    image: "black_sun_auditor.png",
    faction: "red",
    cost: 6,
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
    image: "duskchain_inquisitor.png",
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
    image: "nightglass_crucible.png",
    faction: "red",
    cost: 11,
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
  },

  // DOUBLE ALLY CARDS
  {
    id: "nightglass_coven",
    name: "Nightglass Coven",
    image: "nightglass_conven.png",
    faction: "red",
    cost: 4,
    shop_cost: 165,
    type: "ship",
    sigil: "◒",
    effect: { combat: 2 },
    ally: {},
    doubleAlly: { opponentDiscard: 2 },
    text: "Gain 2 Combat.",
    allyText: "",
    doubleAllyText: "The next enemy draws 2 fewer cards.",
    flavor: "They do not steal your thoughts. They remove the moment in which you formed them."
  },
  {
    id: "umbral_triumvir",
    name: "Umbral Triumvir",
    image: "umbral_triumvir.png",
    faction: "red",
    cost: 7,
    shop_cost: 300,
    type: "ship",
    sigil: "◒",
    effect: { combat: 4 },
    ally: { combat: 2 },
    doubleAlly: { combat: 5, draw: 1, opponentDiscard: 1 },
    text: "Gain 4 Combat.",
    allyText: "Gain 2 Combat.",
    doubleAllyText: "Gain 5 Combat, draw 1 card, and the next enemy draws 1 fewer card.",
    flavor: "Each rules a kingdom. Together, they rule what remains."
  },

  {
    id: "veshras_whispercourt",
    name: "Veshra’s Whispercourt",
    image: "veshras_whispercourt.png",
    faction: "red",
    cost: 4,
    shop_cost: 165,
    type: "ship",
    sigil: "◒",
    effect: { combat: 2 },
    ally: { trade: 1 },
    doubleAlly: { draw: 1, opponentDiscard: 1 },
    text: "Gain 2 Combat.",
    allyText: "Gain 1 Trade.",
    doubleAllyText: "Draw 1 card. The next enemy draws 1 fewer card.",
    flavor: "The ruler never speaks. The court ensures everyone remembers the command."
  },
  {
    id: "the_throne_beneath_night",
    name: "The Throne Beneath Night",
    image: "the_throne_beneath_night.png",
    faction: "red",
    cost: 7,
    shop_cost: 300,
    type: "base",
    defense: 7,
    outpost: false,
    sigil: "◒",
    effect: { combat: 3 },
    ally: { scrapOwn: 1 },
    doubleAlly: { combat: 5, draw: 1, lifelink: 0.5 },
    text: "Gain 3 Combat.",
    allyText: "Gain 1 Purge.",
    doubleAllyText: "Gain 5 Combat, draw 1 card, and gain Lifelink 50% this turn.",
    flavor: "Every shadow is a road leading downward to its crown."
  },

  // ==========================================================
  // NEW LOW-COST CARDS — COST 2–3
  // ==========================================================

  // YELLOW — XYTHE CONCORD
  {
    id: "astral_duelist",
    name: "Astral Duelist",
    image: "astral_duelist.png",
    faction: "yellow",
    cost: 2,
    shop_cost: 20,
    type: "ship",
    sigil: "◈",
    effect: { combat: 2 },
    ally: { trade: 2 },
    text: "Gain 2 Combat.",
    allyText: "Gain 2 Trade.",
    flavor: "It fights in the present and collects payment from the future."
  },
  {
    id: "flicker_collector",
    name: "Flicker Collector",
    image: "flicker_collector.png",
    faction: "yellow",
    cost: 2,
    shop_cost: 20,
    type: "ship",
    sigil: "◈",
    effect: { trade: 1, combat: 1 },
    ally: { draw: 1 },
    text: "Gain 1 Trade and 1 Combat.",
    allyText: "Draw 1 card.",
    flavor: "It gathers brief possibilities before they disappear."
  },
  {
    id: "thoughtpiercer",
    name: "Thoughtpiercer",
    image: "thoughtpiercer.png",
    faction: "yellow",
    cost: 4,
    shop_cost: 30,
    type: "ship",
    sigil: "◈",
    effect: { combat: 2, draw: 1 },
    ally: { trade: 1 },
    text: "Gain 2 Combat and draw 1 card.",
    allyText: "Gain 1 Trade.",
    flavor: "The first strike opens the mind. The second steals its answer."
  },

  // BLUE — AZURE ASCENDANCY
  {
    id: "dawnfield_medic",
    name: "Dawnfield Medic",
    image: "dawnfield_medic.png",
    faction: "blue",
    cost: 2,
    shop_cost: 20,
    type: "ship",
    sigil: "✦",
    effect: { combat: 1, heal: 2 },
    ally: { heal: 1 },
    text: "Gain 1 Combat and 2 Authority.",
    allyText: "Gain 1 Authority.",
    flavor: "Its blade ends one danger while its light mends another."
  },
  {
    id: "lantern_quartermaster",
    name: "Lantern Quartermaster",
    image: "lantern_quartermaster.png",
    faction: "blue",
    cost: 4,
    shop_cost: 30,
    type: "ship",
    sigil: "✦",
    effect: { trade: 1, draw: 1 },
    ally: { heal: 2 },
    text: "Gain 1 Trade and draw 1 card.",
    allyText: "Gain 2 Authority.",
    flavor: "Every lamp, ration, and prayer reaches the correct hands."
  },

  // GREEN — GORAK WARHOST
  {
    id: "ironjaw_scavenger",
    name: "Ironjaw Scavenger",
    image: "ironjaw_scavenger.png",
    faction: "green",
    cost: 2,
    shop_cost: 20,
    type: "ship",
    sigil: "⬢",
    effect: { combat: 2, trade: 1 },
    ally: { combat: 1 },
    text: "Gain 2 Combat and 1 Trade.",
    allyText: "Gain 1 Combat.",
    flavor: "It takes a trophy before the battle has officially started."
  },
  {
    id: "skull_tossers",
    name: "Skull Tossers",
    image: "skull_tossers.png",
    faction: "green",
    cost: 4,
    shop_cost: 30,
    type: "ship",
    sigil: "⬢",
    effect: { combat: 3, draw: 1 },
    ally: {},
    text: "Gain 3 Combat and draw 1 card.",
    allyText: "",
    flavor: "Their ammunition is crude, abundant, and deeply insulting."
  },
  {
    id: "raid_spoils_carrier",
    name: "Raid-Spoils Carrier",
    image: "raid_spoils_carrier.png",
    faction: "green",
    cost: 3,
    shop_cost: 30,
    type: "ship",
    sigil: "⬢",
    effect: { trade: 1, combat: 2 },
    ally: { trade: 2 },
    text: "Gain 1 Trade and 2 Combat.",
    allyText: "Gain 2 Trade.",
    flavor: "The Warhost calls it logistics because theft sounded informal."
  },

  // RED — UMBRAL COVENANT
  {
    id: "ash_sifter",
    name: "Ash Sifter",
    image: "ash_sifter.png",
    faction: "red",
    cost: 3,
    shop_cost: 20,
    type: "ship",
    sigil: "◒",
    effect: { trade: 1, scrapOwn: 1 },
    ally: { combat: 1 },
    text: "Gain 1 Trade and 1 Purge.",
    allyText: "Gain 1 Combat.",
    flavor: "It searches ruined histories for one remaining advantage."
  },
  {
    id: "voidblood_initiate",
    name: "Voidblood Initiate",
    image: "voidblood_initiate.png",
    faction: "red",
    cost: 6,
    shop_cost: 35,
    type: "ship",
    sigil: "◒",
    effect: { draw: 1, scrapOwn: 1 },
    ally: { combat: 2 },
    text: "Draw 1 card and gain 1 Purge.",
    allyText: "Gain 2 Combat.",
    flavor: "Each discarded weakness leaves more room for the void."
  }

]);

export const CARD_MAP = Object.freeze(
  Object.fromEntries(CARDS.map(card => [card.id, card]))
);

// Cards available for permanent ownership, the Armory, and command decks.
// Permanent Trade Row cards such as Hired Looter remain in CARD_MAP for battle lookup.
export function isCollectibleCard(card) {
  return Boolean(card) &&
    card.collectible !== false &&
    card.token !== true &&
    !card.transformedFrom;
}

export const COLLECTIBLE_CARDS = Object.freeze(
  CARDS.filter(isCollectibleCard)
);

export const COLLECTIBLE_CARD_MAP = Object.freeze(
  Object.fromEntries(
    COLLECTIBLE_CARDS.map(card => [card.id, card])
  )
);

export const PERMANENT_MARKET_CARDS = Object.freeze(
  CARDS.filter(card => card.permanentMarket === true)
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
  return Object.fromEntries(
    COLLECTIBLE_CARDS.map(card => [card.id, 1])
  );
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
    COLLECTIBLE_CARDS.filter(
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

  for (const card of COLLECTIBLE_CARDS) {
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
    const card = CARD_MAP[cardId];

    if (!card) {
      return {
        valid: false,
        message: `Unknown card in deck: ${cardId}.`
      };
    }

    if (card.collectible === false) {
      return {
        valid: false,
        message:
          `${card.name} is a permanent Trade Row card ` +
          `and cannot be added to a command deck.`
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
          `${card.name}.`
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

  if (card.doubleAllyText) {
    lines.push(
      `Double Ally — ${card.doubleAllyText}`
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
    "red",
    "neutral"
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

    // Collectible cards must have a real Armory price.
    // Tokens, evolved forms, and other noncollectible cards
    // may use shop_cost: 0 because they cannot be purchased.
    const minimumShopCost =
      isCollectibleCard(card) ? 1 : 0;

    if (
      !Number.isInteger(card.shop_cost) ||
      card.shop_cost < minimumShopCost
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
