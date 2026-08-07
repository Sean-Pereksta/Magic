export const WAR_REALMS_CARD_VERSION = 16;
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
  collectible_edition: true,
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
    id: "brood_path_forerunner",
    name: "Brood-Path Forerunner",
    image: "brood_path_forerunner.png",
    faction: "green",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "⬢",
    effect: {
      combat: 2,
      createToken: {
        id: "spawn",
        count: 1,
        zone: "discard"
      },
      drawFromDrawPile: {
        id: "spawn",
        count: 1,
        look: 4
      }
    },
    ally: {
      combat: 2
    },
    text:
      "Gain 2 Combat, create a Spawn in your discard pile, then draw a Spawn from the top 4 cards of your draw pile if one is there.",
    allyText: "Gain 2 Combat.",
    flavor:
      "It does not track the brood. It opens the road the brood was already coming through."
  },
{
  id: "broodheart_idol",
  name: "Broodheart Idol",
  image: "broodheart_idol.png",
  faction: "green",
  cost: 5,
  shop_cost: 70,
  type: "base",
  defense: 5,
  outpost: false,
  sigil: "⬢",
  effect: {},
  sacrificeTrigger: {
    sacrificedId: "spawn",
    effect: {
      heal: 2
    }
  },
  text: "Whenever you sacrifice a Spawn, gain 2 Authority.",
  flavor:
    "The brood does not mourn its fallen. It feeds upon their strength."
},
  {
    id: "gorge_nest_matriarch",
    name: "Gorge-Nest Matriarch",
    image: "gorge_nest_matriarch.png",
    faction: "green",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: false,
    sigil: "⬢",
    effect: {},
    recurring: {
      everyTurns: 3,
      effect: {
        createToken: {
          id: "spawn",
          count: 2,
          zone: "discard"
        }
      }
    },
    charge: {
      trigger: "cardSacrificed",
      sacrificedId: "spawn",
      gain: 1,
      max: 5,
      actions: [
        {
          label: "Feed the Gorge",
          cost: 2,
          effect: {
            trade: 2
          }
        },
        {
          label: "Open the Nest",
          cost: 5,
          effect: {
            createToken: {
              id: "spawn",
              count: 2,
              zone: "hand"
            }
          }
        }
      ]
    },
    text:
      "Every third turn, create two Spawn in your discard pile. Whenever you sacrifice a Spawn, gain 1 Charge, up to 5.",
    chargeText:
      "Spend 2: gain 2 Trade. Spend 5: create two Spawn in hand.",
    flavor:
      "The gorge echoes because every empty chamber is learning how to hatch."
  },
  {
  id: "voracynth_dream_egg",
  name: "Voracynth Dream-Egg",
  image: "voracynth_dream_egg.png",
  faction: "yellow",
  cost: 4,
  shop_cost: 110,
  type: "ship",
  sigil: "◈",

  effect: {},

  // Advanced once at the beginning of each of this card owner's turns while
  // this physical card remains in an owned battle zone. Progress is tracked
  // by instanceId so multiple Dream-Egg copies hatch independently.
  transform: {
    trigger: "ownerTurnsElapsed",
    required: 2,
    into: "voracynth_star_hatchling",
    destination: "discard",
    resetProgress: true
  },

  text: "This card has no immediate effect.",

  transformText:
    "Ascendant — After 2 of your turns, transform this card into Voracynth Star-Hatchling in your discard pile.",

  flavor:
    "Something inside the shell dreams of stars that have not yet been born."
},

{
  id: "voracynth_star_hatchling",
  name: "Voracynth Star-Hatchling",
  image: "voracynth_star_hatchling.png",
  faction: "yellow",
  collectible: false,
  token: true,
  transformedFrom: "voracynth_dream_egg",
  cost: 7,
  shop_cost: 0,
  type: "ship",
  sigil: "◈",

  effect: {
    combat: 1,
    trade: 1
  },

  transform: {
    trigger: "ownerTurnsElapsed",
    required: 2,
    into: "voracynth_rift_juvenile",
    destination: "discard",
    resetProgress: true
  },

  text: "Gain 1 Combat and 1 Trade.",

  transformText:
    "Ascendant — After 2 of your turns in this form, transform this card into Voracynth Rift-Juvenile in your discard pile.",

  flavor:
    "It enters the world hungry, curious, and only slightly dangerous."
},
{
  id: "worldheart_dream_egg",
  name: "Worldheart Dream-Egg",
  image: "worldheart_dream_egg.png",
  faction: "yellow",
  cost: 8,
  shop_cost: 175,
  type: "ship",
  sigil: "◈",

  effect: {},

  transform: {
    trigger: "ownerTurnsElapsed",
    required: 12,
    into: "voracynth_world_ender",
    destination: "discard",
    resetProgress: true
  },

  text:
    "This card has no immediate effect.",

  transformText:
    "Ascendant — After 12 of your turns, transform this card into Voracynth, World-Ender in your discard pile.",

  flavor:
    "For twelve cycles, the shell absorbs the fear of every world beneath it."
},

{
  id: "voracynth_world_ender",
  name: "Voracynth, World-Ender",
  image: "voracynth_world_ender.png",
  faction: "yellow",
  collectible: false,
  token: true,
  transformedFrom: "worldheart_dream_egg",
  cost: 16,
  shop_cost: 0,
  type: "ship",
  sigil: "◈",

  effect: {
    combat: 25
  },

  optionalSacrificeFriendlyCard: {
    allowedIds: [
      "voracynth_star_hatchling",
      "voracynth_rift_juvenile",
      "voracynth_void_elder",
      "voralyth_future_devourer",
      "voracynth_apex_of_tomorrow"
    ],
    excludeSelf: true,
    zone: "played",
    count: 1,
    effect: {
      combat: 25
    }
  },

  text:
    "Gain 25 Combat.",

  abilityText:
    "Devour Its Own Kind — You may sacrifice another listed Voracynth you played this turn. If you do, gain 25 additional Combat.",

  flavor:
    "It recognizes its own blood only as another world waiting to be consumed."
},
{
  id: "voracynth_rift_juvenile",
  name: "Voracynth Rift-Juvenile",
  image: "voracynth_rift_juvenile.png",
  faction: "yellow",
  collectible: false,
  token: true,
  transformedFrom: "voracynth_star_hatchling",
  cost: 8,
  shop_cost: 0,
  type: "ship",
  sigil: "◈",

  effect: {
    combat: 3,
    trade: 1
  },

  ally: {
    shield: 2
  },

  transform: {
    trigger: "ownerTurnsElapsed",
    required: 2,
    into: "voracynth_void_elder",
    destination: "discard",
    resetProgress: true
  },

  text: "Gain 3 Combat and 1 Trade.",

  allyText: "Gain 2 Shield.",

  transformText:
    "Ascendant — After 2 of your turns in this form, transform this card into Voracynth Void-Elder in your discard pile.",

  flavor:
    "Its growing wings bend distance before they learn how to fly."
},

{
  id: "voracynth_void_elder",
  name: "Voracynth Void-Elder",
  image: "voracynth_void_elder.png",
  faction: "yellow",
  collectible: false,
  token: true,
  transformedFrom: "voracynth_rift_juvenile",
  cost: 10,
  shop_cost: 0,
  type: "ship",
  sigil: "◈",

  effect: {
    combat: 6,
    stun: 1
  },

  ally: {
    draw: 1
  },

  transform: {
    trigger: "ownerTurnsElapsed",
    required: 3,
    into: "voracynth_apex_of_tomorrow",
    destination: "discard",
    resetProgress: true
  },

  text: "Gain 6 Combat and 1 Disable.",

  allyText: "Draw 1 card.",

  transformText:
    "Final Ascension — After 3 of your turns in this form, transform this card into Voracynth, Apex of Tomorrow in your discard pile.",

  flavor:
    "It no longer hunts within the present. It waits where its prey will eventually be."
},

{
  id: "voracynth_apex_of_tomorrow",
  name: "Voracynth, Apex of Tomorrow",
  image: "voracynth_apex_of_tomorrow.png",
  faction: "yellow",
  collectible: false,
  token: true,
  transformedFrom: "voracynth_void_elder",
  cost: 14,
  shop_cost: 0,
  type: "ship",
  sigil: "◈",

  effect: {
    combat: 10,
    stun: 2,
    opponentDiscard: 1
  },

  ally: {
    draw: 1,
    trade: 2
  },

  doubleAlly: {
    combat: 4
  },

  text:
    "Gain 10 Combat and 2 Disable. The next enemy draws 1 fewer card.",

  allyText:
    "Draw 1 card and gain 2 Trade.",

  doubleAllyText:
    "Gain 4 additional Combat.",

  flavor:
    "The creature that entered the egg is gone. What emerged remembers devouring tomorrow."
},

  {
    id: "spawnlash_alpha",
    name: "Spawnlash Alpha",
    image: "spawnlash_alpha.png",
    faction: "green",
    cost: 5,
    shop_cost: 90,
    type: "ship",
    sigil: "⬢",
    effect: {
      combat: 3
    },
    tokenThresholds: [
      {
        metric: "playedBefore",
        tokenId: "spawn",
        at: 1,
        effect: {
          combat: 2
        }
      },
      {
        metric: "playedBefore",
        tokenId: "spawn",
        at: 2,
        effect: {
          trade: 2
        }
      },
      {
        metric: "playedBefore",
        tokenId: "spawn",
        at: 3,
        effect: {
          draw: 1
        }
      }
    ],
    text:
      "Gain 3 Combat. If 1 Spawn was played before this, gain 2 Combat; at 2 Spawn, gain 2 Trade; at 3 Spawn, draw 1 card.",
    flavor:
      "Each smaller mouth announces the arrival of the one that taught them hunger."
  },

  {
    id: "carrion_nursery_raider",
    name: "Carrion Nursery Raider",
    image: "carrion_nursery_raider.png",
    faction: "green",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "⬢",
    effect: {
      trade: 2,
      createToken: {
        id: "spawn",
        count: 2,
        zone: "discard"
      }
    },
    ally: {
      combat: 2
    },
    sacrifice: {
      createToken: {
        id: "spawn",
        count: 2,
        zone: "hand"
      }
    },
    text:
      "Gain 2 Trade and create two Spawn in your discard pile.",
    allyText: "Gain 2 Combat.",
    sacrificeText: "Sacrifice: Create two Spawn in hand.",
    flavor:
      "It raids for meat, metal, and anything warm enough to become a nest."
  },

  {
    id: "maw_of_the_third_brood",
    name: "Maw of the Third Brood",
    image: "maw_of_the_third_brood.png",
    faction: "green",
    cost: 7,
    shop_cost: 125,
    type: "ship",
    sigil: "⬢",
    effect: {
      combat: 5
    },
    tokenThresholds: [
      {
        metric: "playedBefore",
        tokenId: "spawn",
        at: 2,
        effect: {
          createToken: {
            id: "spawn",
            count: 1,
            zone: "hand"
          }
        }
      },
      {
        metric: "playedBefore",
        tokenId: "spawn",
        at: 3,
        effect: {
          draw: 1
        }
      }
    ],
    doubleAlly: {
      combat: 3
    },
    text:
      "Gain 5 Combat. If two Spawn were played before this, create a Spawn in hand. If three were played, also draw 1 card.",
    doubleAllyText: "Gain 3 Combat.",
    flavor:
      "The third brood is never counted by bodies. It is counted by vanished cities."
  },

  // ==========================================================
  // GREEN — WORKER PACKAGE
  // ==========================================================
  {
    id: "ironroot_foreman",
    name: "Ironroot Foreman",
    image: "ironroot_foreman.png",
    faction: "green",
    cost: 3,
    shop_cost: 35,
    type: "ship",
    sigil: "⬢",
    effect: {
      trade: 2,
      createToken: {
        id: "worker",
        count: 1,
        zone: "discard"
      },
      drawFromDrawPile: {
        id: "worker",
        count: 1,
        look: 5
      }
    },
    ally: {
      trade: 1
    },
    text:
      "Gain 2 Trade, create a Worker in your discard pile, then draw a Worker from the top 5 cards of your draw pile if one is there.",
    allyText: "Gain 1 Trade.",
    flavor:
      "He measures progress in walls raised before the enemy notices the quarry is empty."
  },

  {
    id: "mobile_foundry_crew",
    name: "Mobile Foundry Crew",
    image: "mobile_foundry_crew.png",
    faction: "green",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "⬢",
    effect: {
      or: [
        {
          label: "Collect Wages",
          effect: {
            trade: 3
          }
        },
        {
          label: "Call the Crew",
          effect: {
            createToken: {
              id: "worker",
              count: 1,
              zone: "hand"
            }
          }
        },
        {
          label: "Search the Line",
          effect: {
            drawFromDrawPile: {
              id: "worker",
              count: 1,
              look: 6
            }
          }
        }
      ]
    },
    ally: {
      combat: 2
    },
    text:
      "Choose one: gain 3 Trade; create a Worker in hand; or draw a Worker from the top 6 cards of your draw pile if one is there.",
    allyText: "Gain 2 Combat.",
    flavor:
      "Wherever the wheels stop, a factory begins arguing with the horizon."
  },

  {
    id: "labor_tithe_overseer",
    name: "Labor-Tithe Overseer",
    image: "labor_tithe_overseer.png",
    faction: "green",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: false,
    sigil: "⬢",
    effect: {
      trade: 1
    },
    sacrificeThresholds: [
      {
        at: 1,
        requiresSacrificedId: "worker",
        effect: {
          trade: 2
        }
      },
      {
        at: 2,
        requiresSacrificedId: "worker",
        effect: {
          draw: 1
        }
      },
      {
        at: 3,
        requiresSacrificedId: "worker",
        effect: {
          createToken: {
            id: "worker",
            count: 1,
            zone: "hand"
          }
        }
      }
    ],
    text:
      "Gain 1 Trade. Each turn, your first sacrificed Worker gives 2 Trade; your second draws 1 card; your third creates a Worker in hand.",
    flavor:
      "The books balance because every missing laborer is entered as future productivity."
  },

  {
    id: "siegeworks_coordinator",
    name: "Siegeworks Coordinator",
    image: "siegeworks_coordinator.png",
    faction: "green",
    cost: 5,
    shop_cost: 90,
    type: "ship",
    sigil: "⬢",
    effect: {
      combat: 2,
      createToken: {
        id: "worker",
        count: 1,
        zone: "discard"
      }
    },
    tokenThresholds: [
      {
        metric: "playedBefore",
        tokenId: "worker",
        at: 1,
        effect: {
          trade: 2
        }
      },
      {
        metric: "playedBefore",
        tokenId: "worker",
        at: 2,
        effect: {
          combat: 3
        }
      },
      {
        metric: "playedBefore",
        tokenId: "worker",
        at: 3,
        effect: {
          draw: 1
        }
      }
    ],
    text:
      "Gain 2 Combat and create a Worker in discard. If 1 Worker was played before this, gain 2 Trade; at 2 Workers, gain 3 Combat; at 3, draw 1 card.",
    flavor:
      "The ram, the road, and the replacement wall are all on the same schedule."
  },

  {
    id: "worldroot_mobilizer",
    name: "Worldroot Mobilizer",
    image: "worldroot_mobilizer.png",
    faction: "green",
    cost: 7,
    shop_cost: 125,
    type: "ship",
    sigil: "⬢",
    effect: {
      trade: 3,
      createToken: {
        id: "worker",
        count: 2,
        zone: "discard"
      },
      drawFromDrawPile: {
        id: "worker",
        count: 2,
        look: 7
      }
    },
    doubleAlly: {
      createToken: {
        id: "worker",
        count: 1,
        zone: "hand"
      }
    },
    text:
      "Gain 3 Trade, create two Workers in your discard pile, then draw up to two Workers from the top 7 cards of your draw pile.",
    doubleAllyText: "Create a Worker in hand.",
    flavor:
      "When the worldroot moves, every camp becomes a city before nightfall."
  },

  // ==========================================================
  // YELLOW — DRONE PACKAGE
  // ==========================================================
  {
    id: "sparkline_dispatcher",
    name: "Sparkline Dispatcher",
    image: "sparkline_dispatcher.png",
    faction: "yellow",
    cost: 3,
    shop_cost: 35,
    type: "ship",
    sigil: "◈",
    effect: {
      trade: 1,
      createToken: {
        id: "drone",
        count: 1,
        zone: "discard"
      },
      drawFromDrawPile: {
        id: "drone",
        count: 1,
        look: 5
      }
    },
    ally: {
      createToken: {
        id: "drone",
        count: 1,
        zone: "hand"
      }
    },
    text:
      "Gain 1 Trade, create a Drone in your discard pile, then draw a Drone from the top 5 cards of your draw pile if one is there.",
    allyText: "Create a Drone in hand.",
    flavor:
      "Its orders arrive one heartbeat before the machines realize they have been built."
  },

  {
    id: "recursive_swarm_array",
    name: "Recursive Swarm Array",
    image: "recursive_swarm_array.png",
    faction: "yellow",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: false,
    sigil: "◈",
    effect: {
      trade: 1
    },
    charge: {
      trigger: "tokenPlayed",
      tokenId: "drone",
      gain: 1,
      max: 6,
      actions: [
        {
          label: "Convert Signal",
          cost: 2,
          effect: {
            trade: 2
          }
        },
        {
          label: "Print Reinforcement",
          cost: 4,
          effect: {
            createToken: {
              id: "drone",
              count: 1,
              zone: "hand"
            }
          }
        },
        {
          label: "Recursive Launch",
          cost: 6,
          effect: {
            draw: 1,
            createToken: {
              id: "drone",
              count: 1,
              zone: "hand"
            }
          }
        }
      ]
    },
    text:
      "Gain 1 Trade. Whenever you play a Drone, gain 1 Charge, up to 6.",
    chargeText:
      "Spend 2: gain 2 Trade. Spend 4: create a Drone in hand. Spend 6: draw 1 card and create a Drone in hand.",
    flavor:
      "Each signal contains the blueprint for the machine that will repeat it."
  },

  {
    id: "swarm_arithmetic_savant",
    name: "Swarm-Arithmetic Savant",
    image: "swarm_arithmetic_savant.png",
    faction: "yellow",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "◈",
    effect: {
      trade: 2
    },
    tokenThresholds: [
      {
        metric: "playedBefore",
        tokenId: "drone",
        at: 1,
        effect: {
          combat: 2
        }
      },
      {
        metric: "playedBefore",
        tokenId: "drone",
        at: 2,
        effect: {
          trade: 2
        }
      },
      {
        metric: "playedBefore",
        tokenId: "drone",
        at: 3,
        effect: {
          draw: 1
        }
      }
    ],
    text:
      "Gain 2 Trade. If 1 Drone was played before this, gain 2 Combat; at 2 Drones, gain 2 Trade; at 3, draw 1 card.",
    flavor:
      "To the Savant, a swarm is only an equation that has learned to fly."
  },

  {
    id: "scrap_spark_replicator",
    name: "Scrap-Spark Replicator",
    image: "scrap_spark_replicator.png",
    faction: "yellow",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "◈",
    effect: {
      combat: 2,
      drawFromDrawPile: {
        id: "drone",
        count: 1,
        look: 6
      }
    },
    ally: {
      trade: 2
    },
    sacrifice: {
      createToken: {
        id: "drone",
        count: 2,
        zone: "hand"
      }
    },
    text:
      "Gain 2 Combat, then draw a Drone from the top 6 cards of your draw pile if one is there.",
    allyText: "Gain 2 Trade.",
    sacrificeText: "Sacrifice: Create two Drones in hand.",
    flavor:
      "Breaking the original only proves the copies were always more important."
  },

  {
    id: "thousand_eye_carrier",
    name: "Thousand-Eye Carrier",
    image: "thousand_eye_carrier.png",
    faction: "yellow",
    cost: 7,
    shop_cost: 125,
    type: "ship",
    sigil: "◈",
    effect: {
      combat: 4,
      createToken: {
        id: "drone",
        count: 2,
        zone: "discard"
      },
      drawFromDrawPile: {
        id: "drone",
        count: 2,
        look: 8
      }
    },
    tokenThresholds: [
      {
        metric: "playedBefore",
        tokenId: "drone",
        at: 3,
        effect: {
          combat: 4
        }
      }
    ],
    doubleAlly: {
      trade: 3
    },
    text:
      "Gain 4 Combat, create two Drones in discard, then draw up to two Drones from the top 8 cards of your draw pile. If three Drones were played before this, gain 4 Combat.",
    doubleAllyText: "Gain 3 Trade.",
    flavor:
      "Every eye watches a different future. Every future contains the same swarm."
  },

  // ==========================================================
  // BLUE — INTERCEPTOR PACKAGE
  // ==========================================================
  {
    id: "halo_scramble_officer",
    name: "Halo Scramble Officer",
    image: "halo_scramble_officer.png",
    faction: "blue",
    cost: 3,
    shop_cost: 35,
    type: "ship",
    sigil: "✦",
    effect: {
      combat: 2,
      createToken: {
        id: "interceptor",
        count: 1,
        zone: "discard"
      },
      drawFromDrawPile: {
        id: "interceptor",
        count: 1,
        look: 5
      }
    },
    ally: {
      shield: 2
    },
    text:
      "Gain 2 Combat, create an Interceptor in your discard pile, then draw an Interceptor from the top 5 cards of your draw pile if one is there.",
    allyText: "Gain 2 Shield.",
    flavor:
      "The order to launch is considered late if the pilots have not already returned."
  },

  {
    id: "skywall_muster_deck",
    name: "Skywall Muster Deck",
    image: "skywall_muster_deck.png",
    faction: "blue",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: true,
    sigil: "✦",
    effect: {
      shield: 1
    },
    recurring: {
      everyTurns: 2,
      effect: {
        createToken: {
          id: "interceptor",
          count: 1,
          zone: "discard"
        },
        drawFromDrawPile: {
          id: "interceptor",
          count: 1,
          look: 4
        }
      }
    },
    charge: {
      trigger: "tokenPlayed",
      tokenId: "interceptor",
      gain: 1,
      max: 4,
      actions: [
        {
          label: "Raise the Skywall",
          cost: 4,
          effect: {
            armor: {
              amount: 1,
              all: true
            }
          }
        }
      ]
    },
    text:
      "Outpost. Gain 1 Shield. Every second turn, create an Interceptor in discard, then draw one from the top 4 cards of your draw pile if one is there. Played Interceptors add Charge.",
    chargeText:
      "Spend 4: Give all of your bases 1 temporary Armor.",
    flavor:
      "Its deck is a prayer platform with engines beneath every answer."
  },

  {
    id: "formation_vow_captain",
    name: "Formation Vow-Captain",
    image: "formation_vow_captain.png",
    faction: "blue",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "✦",
    effect: {
      shield: 2
    },
    tokenThresholds: [
      {
        metric: "playedBefore",
        tokenId: "interceptor",
        at: 1,
        effect: {
          combat: 2
        }
      },
      {
        metric: "playedBefore",
        tokenId: "interceptor",
        at: 2,
        effect: {
          armor: {
            amount: 2
          }
        }
      },
      {
        metric: "playedBefore",
        tokenId: "interceptor",
        at: 3,
        effect: {
          draw: 1
        }
      }
    ],
    text:
      "Gain 2 Shield. If 1 Interceptor was played before this, gain 2 Combat; at 2 Interceptors, give a base 2 Armor; at 3, draw 1 card.",
    flavor:
      "A formation is a vow spoken by wings instead of voices."
  },

  {
    id: "last_light_recovery_wing",
    name: "Last-Light Recovery Wing",
    image: "last_light_recovery_wing.png",
    faction: "blue",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "✦",
    effect: {
      heal: 3,
      reclaim: {
        ids: ["interceptor"]
      }
    },
    ally: {
      trade: 2
    },
    sacrifice: {
      createToken: {
        id: "interceptor",
        count: 1,
        zone: "hand"
      }
    },
    text:
      "Gain 3 Authority and Reclaim an Interceptor from your discard pile.",
    allyText: "Gain 2 Trade.",
    sacrificeText:
      "Sacrifice: Create an Interceptor in hand.",
    flavor:
      "No light is called last while one pilot can still be brought home."
  },

  {
    id: "seraphic_air_marshal",
    name: "Seraphic Air Marshal",
    image: "seraphic_air_marshal.png",
    faction: "blue",
    cost: 7,
    shop_cost: 125,
    type: "ship",
    sigil: "✦",
    effect: {
      combat: 5,
      shield: 3,
      drawFromDrawPile: {
        id: "interceptor",
        count: 2,
        look: 7
      }
    },
    doubleAlly: {
      createToken: {
        id: "interceptor",
        count: 1,
        zone: "hand"
      }
    },
    text:
      "Gain 5 Combat and 3 Shield, then draw up to two Interceptors from the top 7 cards of your draw pile.",
    doubleAllyText: "Create an Interceptor in hand.",
    flavor:
      "The Marshal does not command the sky. The sky has simply taken the same oath."
  },

  // ==========================================================
  // RED — EMBERLING PACKAGE
  // ==========================================================
  {
    id: "ashspark_midwife",
    name: "Ashspark Midwife",
    image: "ashspark_midwife.png",
    faction: "red",
    cost: 3,
    shop_cost: 35,
    type: "ship",
    sigil: "◒",
    effect: {
      combat: 2,
      createToken: {
        id: "emberling",
        count: 1,
        zone: "discard"
      },
      drawFromDrawPile: {
        id: "emberling",
        count: 1,
        look: 5
      }
    },
    ally: {
      createToken: {
        id: "emberling",
        count: 1,
        zone: "hand"
      }
    },
    text:
      "Gain 2 Combat, create an Emberling in your discard pile, then draw an Emberling from the top 5 cards of your draw pile if one is there.",
    allyText: "Create an Emberling in hand.",
    flavor:
      "She teaches every spark the single word it needs: consume."
  },

  {
    id: "cinder_choir",
    name: "Cinder Choir",
    image: "cinder_choir.png",
    faction: "red",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 7,
    outpost: false,
    sigil: "◒",
    effect: {},
    recurring: {
      everyTurns: 3,
      effect: {
        createToken: {
          id: "emberling",
          count: 2,
          zone: "hand"
        }
      }
    },
    sacrificeThresholds: [
      {
        at: 1,
        requiresSacrificedId: "emberling",
        effect: {
          combat: 2
        }
      },
      {
        at: 2,
        requiresSacrificedId: "emberling",
        effect: {
          trade: 2
        }
      }
    ],
    text:
      "Every third turn, create two Emberlings in hand. Each turn, your first sacrificed Emberling gives 2 Combat and your second gives 2 Trade.",
    flavor:
      "Each voice burns out quickly. Together they sing long enough to ignite the walls."
  },

  {
    id: "furnace_reveler",
    name: "Furnace Reveler",
    image: "furnace_reveler.png",
    faction: "red",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "◒",
    effect: {
      combat: 3
    },
    sacrificeThresholds: [
      {
        at: 1,
        requiresSacrificedId: "emberling",
        effect: {
          trade: 2
        }
      },
      {
        at: 2,
        requiresSacrificedId: "emberling",
        effect: {
          draw: 1
        }
      },
      {
        at: 3,
        requiresSacrificedId: "emberling",
        effect: {
          combat: 4
        }
      }
    ],
    text:
      "Gain 3 Combat. Each turn, your first sacrificed Emberling gives 2 Trade; your second draws 1 card; your third gives 4 Combat.",
    flavor:
      "It celebrates every extinguished flame by throwing another into the furnace."
  },

  {
    id: "debt_kindler_broker",
    name: "Debt-Kindler Broker",
    image: "debt_kindler_broker.png",
    faction: "red",
    cost: 5,
    shop_cost: 90,
    type: "ship",
    sigil: "◒",
    effect: {
      trade: 3
    },
    ally: {
      combat: 2
    },
    sacrifice: {
      createToken: {
        id: "emberling",
        count: 2,
        zone: "hand"
      },
      drawFromDrawPile: {
        id: "emberling",
        count: 1,
        look: 5
      }
    },
    text: "Gain 3 Trade.",
    allyText: "Gain 2 Combat.",
    sacrificeText:
      "Sacrifice: Create two Emberlings in hand, then draw an Emberling from the top 5 cards of your draw pile if one is there.",
    flavor:
      "Every debt is written in ash so the payment can be burned twice."
  },

  {
    id: "pyre_heart_tyrant",
    name: "Pyre-Heart Tyrant",
    image: "pyre_heart_tyrant.png",
    faction: "red",
    cost: 7,
    shop_cost: 125,
    type: "ship",
    sigil: "◒",
    effect: {
      combat: 6,
      createToken: {
        id: "emberling",
        count: 2,
        zone: "discard"
      },
      drawFromDrawPile: {
        id: "emberling",
        count: 2,
        look: 8
      }
    },
    tokenThresholds: [
      {
        metric: "playedBefore",
        tokenId: "emberling",
        at: 2,
        effect: {
          draw: 1
        }
      },
      {
        metric: "playedBefore",
        tokenId: "emberling",
        at: 3,
        effect: {
          combat: 4
        }
      }
    ],
    doubleAlly: {
      combat: 3
    },
    text:
      "Gain 6 Combat, create two Emberlings in discard, then draw up to two Emberlings from the top 8 cards of your draw pile. If two Emberlings were played before this, draw 1 card; at three, gain 4 Combat.",
    doubleAllyText: "Gain 3 Combat.",
    flavor:
      "Its throne is the point where every smaller fire agrees to become one war."
  },
  {
    id: "aegis_tide_saint",
    name: "Aegis Tide Saint",
    image: "aegis_tide_saint.png",
    faction: "blue",
    collectible_edition: true,
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
  // ==========================================================
// NEW ASCENDANT CREATURES
// Four collectible creatures and their noncollectible final forms.
// ==========================================================


// ==========================================================
// YELLOW — THE EGG BEYOND TOMORROW
// Does nothing initially. Ascends after five plays.
// ==========================================================

{
  id: "egg_beyond_tomorrow",
  name: "Egg Beyond Tomorrow",
  image: "egg_beyond_tomorrow.png",
  faction: "yellow",
  cost: 6,
  shop_cost: 90,
  type: "ship",
  sigil: "◈",
  effect: {},
  ally: {},
  transform: {
    trigger: "timesPlayed",
    required: 5,
    into: "voralyth_future_devourer",
    destination: "discard"
  },
  text: "This card has no immediate effect.",
  allyText: "",
  transformText: "Ascendant — After this card has been played 5 times, transform it into Voralyth, Future-Devourer in your discard pile.",
  flavor: "The shell contains no heartbeat—only the sound of futures being swallowed."
},

{
  id: "voralyth_future_devourer",
  name: "Voralyth, Future-Devourer",
  image: "voralyth_future_devourer.png",
  faction: "yellow",
  collectible: false,
  token: true,
  transformedFrom: "egg_beyond_tomorrow",
  cost: 14,
  shop_cost: 0,
  type: "ship",
  sigil: "◈",
  effect: {
    combat: 11,
    stun: 2,
    opponentDiscard: 1
  },
  ally: {
    draw: 1
  },
  text: "Gain 11 Combat and 2 Disable. The next enemy draws 1 fewer card.",
  allyText: "Draw 1 card.",
  flavor: "It hatched five wars too late for the civilizations it was born to consume."
},


// ==========================================================
// GREEN — THE DOOMCHARGE BEAST
// Begins as a weak attacker. Its final form exists only to die.
// ==========================================================

{
  id: "doomhoof_whelp",
  name: "Doomhoof Whelp",
  image: "doomhoof_whelp.png",
  faction: "green",
  cost: 4,
  shop_cost: 55,
  type: "ship",
  sigil: "⬢",
  effect: {
    combat: 2
  },
  ally: {},
  transform: {
    trigger: "timesPlayed",
    required: 3,
    into: "doomhoof_final_charge",
    destination: "discard"
  },
  text: "Gain 2 Combat.",
  allyText: "",
  transformText: "Ascendant — After this card has been played 3 times, transform it into Doomhoof, Final Charge.",
  flavor: "Every charge teaches it less fear and fewer reasons to return."
},

{
  id: "doomhoof_final_charge",
  name: "Doomhoof, Final Charge",
  image: "doomhoof_final_charge.png",
  faction: "green",
  collectible: false,
  token: true,
  transformedFrom: "doomhoof_whelp",
  cost: 13,
  shop_cost: 0,
  type: "ship",
  sigil: "⬢",
  effect: {},
  ally: {},
  sacrifice: {
    or: [
      {
        label: "Shatter the Fortresses",
        effect: {
          destroyBase: 5
        }
      },
      {
        label: "The Final Charge",
        effect: {
          combat: 15
        }
      }
    ]
  },
  text: "This card has no immediate effect.",
  allyText: "",
  sacrificeText: "Sacrifice — Choose one: gain 5 Raze; or gain 15 Combat.",
  flavor: "It was bred for one perfect moment and given no instincts for what comes afterward."
},


// ==========================================================
// BLUE — CHERUB OF THE LAST MERCY
// Begins with minor protection and ascends into the Death Seraph.
// ==========================================================

{
  id: "cherub_of_last_mercy",
  name: "Cherub of Last Mercy",
  image: "cherub_of_last_mercy.png",
  faction: "blue",
  cost: 5,
  shop_cost: 70,
  type: "ship",
  sigil: "✦",
  effect: {
    shield: 2
  },
  ally: {},
  transform: {
    trigger: "timesPlayed",
    required: 3,
    into: "seraph_of_the_final_bell",
    destination: "discard"
  },
  text: "Gain 2 Shield.",
  allyText: "",
  transformText: "Ascendant — After this card has been played 3 times, transform it into Seraph of the Final Bell.",
  flavor: "It shelters the dying because it already knows the road they will travel."
},
{
    id: "sunspark_drone_carrier",
    name: "Sunspark Drone Carrier",
    image: "sunspark_drone_carrier.png",
    faction: "yellow",
    cost: 4,
    shop_cost: 65,
    type: "ship",
    sigil: "◈",
    effect: {
      combat: 2,
      createToken: {
        id: "drone",
        count: 1,
        zone: "discard",
        factionPlayedZoneOverride: {
          faction: "yellow",
          at: 3,
          zone: "topdeck"
        }
      }
    },
    text: "Gain 2 Combat. Create a Drone in your discard pile.",
    factionText: "Third Yellow: Put the Drone on top of your deck instead.",
    flavor: "Every spark becomes another mind in the swarm."
  },
  {
    id: "helix_reactor_scout",
    name: "Helix Reactor Scout",
    image: "helix_reactor_scout.png",
    faction: "yellow",
    cost: 3,
    shop_cost: 50,
    type: "ship",
    sigil: "◈",
    effect: {
      trade: 2
    },
    heat: {
      gain: 1,
      max: 5,
      thresholds: [
        {
          at: 2,
          effect: {
            peekTop: {
              count: 1,
              mayBottom: true
            }
          }
        },
        {
          at: 4,
          effect: {
            trade: 1,
            draw: 1
          },
          resetTo: 1
        }
      ]
    },
    text: "Gain 2 Trade and add 1 Heat to this card.",
    heatText: "At 2+ Heat, look at the top card of your deck; you may place it on the bottom. At 4+ Heat, gain 1 additional Trade and draw a card, then reset this card to 1 Heat. Maximum Heat 5.",
    flavor: "It has witnessed tomorrow often enough to begin changing it."
  },
  {
    id: "concord_swarm_director",
    name: "Concord Swarm Director",
    image: "concord_swarm_director.png",
    faction: "yellow",
    cost: 6,
    shop_cost: 105,
    type: "ship",
    sigil: "◈",
    effect: {
      trade: 3
    },
    tokenScaling: {
      metric: "played",
      tokenId: "drone",
      per: 2,
      maxUnits: 3,
      effectPerUnit: {
        trade: 1
      }
    },
    tokenThresholds: [
      {
        metric: "played",
        tokenId: "drone",
        at: 3,
        effect: {
          draw: 1,
          topdeckFromHand: 1
        }
      }
    ],
    text: "Gain 3 Trade. Gain 1 additional Trade for every two Drones played this turn, up to 3 additional Trade.",
    thresholdText: "If three or more Drones were played this turn, draw a card, then place one card from your hand on top of your deck.",
    flavor: "A billion minor calculations resolve into one perfect command."
  },
  {
    id: "infinite_assembly_node",
    name: "Infinite Assembly Node",
    image: "infinite_assembly_node.png",
    faction: "yellow",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: false,
    sigil: "◈",
    effect: {},
    recurring: {
      everyTurns: 2,
      effect: {
        createToken: {
          id: "drone",
          count: 1,
          zone: "discard"
        },
        drawFromDrawPile: {
          id: "drone",
          count: 1
        }
      },
      ownedFactionModifiers: [
        {
          faction: "yellow",
          at: 10,
          modify: {
            path: "createToken.zone",
            value: "topdeck"
          }
        },
        {
          faction: "yellow",
          at: 15,
          additionalEffect: {
            createToken: {
              id: "drone",
              count: 1,
              zone: "discard"
            }
          }
        }
      ]
    },
    text: "At the start of every second turn this base remains in play, create a Drone in your discard pile, then draw a Drone from your draw pile if one is there.",
    factionText: "10 Yellow Cards Owned: Create the Drone on top of your deck instead. 15 Yellow Cards Owned: Create a second Drone in your discard pile.",
    flavor: "The line has no beginning, and production has no end."
  },
  {
    id: "prismatic_heat_exchange",
    name: "Prismatic Heat Exchange",
    image: "prismatic_heat_exchange.png",
    faction: "yellow",
    cost: 6,
    shop_cost: 110,
    type: "base",
    defense: 9,
    outpost: false,
    sigil: "◈",
    effect: {},
    heatAura: {
      trigger: "shipReachesHeat",
      at: 3,
      firstTimeEachTurn: true,
      effect: {
        trade: 1
      }
    },
    activatedAbility: {
      label: "Transfer Heat",
      oncePerTurn: true,
      effect: {
        moveHeat: {
          amount: 1,
          from: "friendlyShip",
          to: "differentFriendlyShip"
        }
      }
    },
    factionThresholds: [
      {
        metric: "played",
        faction: "yellow",
        at: 4,
        oncePerTurn: true,
        effect: {
          coolHeat: {
            amount: 1
          }
        }
      }
    ],
    text: "The first time each turn that a ship reaches 3 Heat, gain 1 Trade. Once per turn, move 1 Heat from one of your ships to another one of your ships.",
    factionText: "Fourth Yellow: You may remove 1 Heat from any ship.",
    flavor: "Energy is never lost. It merely accepts a more useful future."
  },

  // ==========================================================
  // BLUE — AZURE ASCENDANCY
  // ==========================================================
  {
    id: "acolyte_procession_leader",
    name: "Acolyte Procession Leader",
    image: "acolyte_procession_leader.png",
    faction: "blue",
    cost: 4,
    shop_cost: 65,
    type: "ship",
    sigil: "✦",
    effect: {
      shield: 2,
      createToken: {
        id: "acolyte",
        count: 1,
        zone: "discard"
      },
      drawFromDrawPile: {
        id: "acolyte",
        count: 1
      }
    },
    tokenThresholds: [
      {
        metric: "playedBefore",
        tokenId: "acolyte",
        at: 1,
        effect: {
          trade: 1,
          armor: {
            amount: 1,
            temporary: true
          }
        }
      }
    ],
    text: "Gain 2 Shield. Create an Acolyte in your discard pile, then draw an Acolyte from your draw pile if one is there.",
    thresholdText: "If an Acolyte was already played this turn, gain 1 Trade and give one base 1 temporary Armor.",
    flavor: "Each quiet voice strengthens the hymn of the whole."
  },
  {
    id: "reliquary_searcher",
    name: "Reliquary Searcher",
    image: "reliquary_searcher.png",
    faction: "blue",
    cost: 5,
    shop_cost: 85,
    type: "ship",
    sigil: "✦",
    effect: {
      combat: 3
    },
    factionThresholds: [
      {
        metric: "played",
        faction: "blue",
        at: 3,
        effect: {
          reclaim: {
            cardId: "acolyte",
            from: "discard",
            to: "hand",
            count: 1
          }
        }
      }
    ],
    tokenThresholds: [
      {
        metric: "playedBefore",
        tokenId: "acolyte",
        at: 2,
        optional: true,
        effect: {
          topdeckFromDiscard: {
            faction: "blue",
            minCost: 5,
            count: 1
          }
        }
      }
    ],
    text: "Gain 3 Combat.",
    factionText: "Third Blue: Reclaim an Acolyte from your discard pile into your hand.",
    thresholdText: "If two Acolytes have already been played this turn, you may place one Blue card costing 5 or more from your discard pile on top of your deck.",
    flavor: "The faithful are never lost. They are merely waiting to be called."
  },
  {
    id: "coolant_wing_chaplain",
    name: "Coolant Wing Chaplain",
    image: "coolant_wing_chaplain.png",
    faction: "blue",
    cost: 3,
    shop_cost: 50,
    type: "ship",
    sigil: "✦",
    effect: {
      or: [
        {
          id: "coolant_wing_chaplain_cool",
          label: "Measured Cooling",
          effect: {
            coolHeat: {
              amount: 2
            }
          }
        },
        {
          id: "coolant_wing_chaplain_interceptor",
          label: "Launch Interceptor",
          effect: {
            createToken: {
              id: "interceptor",
              count: 1,
              zone: "discard"
            },
            drawFromDrawPile: {
              id: "interceptor",
              count: 1
            }
          }
        },
        {
          id: "coolant_wing_chaplain_shield",
          label: "Protective Hymn",
          effect: {
            shield: 2
          }
        }
      ]
    },
    factionThresholds: [
      {
        metric: "played",
        faction: "blue",
        at: 4,
        effect: {
          chooseAdditionalDifferentOption: {
            from: "effect.or",
            count: 1
          }
        }
      }
    ],
    text: "Choose one: remove up to 2 Heat from one ship; create an Interceptor in your discard pile, then draw an Interceptor from your draw pile if one is there; or gain 2 Shield.",
    factionText: "Fourth Blue: Choose a second different option.",
    flavor: "Restraint is not weakness. It is power held in perfect discipline."
  },
  {
    id: "hangar_of_the_silver_vow",
    name: "Hangar of the Silver Vow",
    image: "hangar_of_the_silver_vow.png",
    faction: "blue",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 9,
    outpost: false,
    sigil: "✦",
    effect: {},
    recurring: {
      everyTurns: 2,
      effect: {
        createToken: {
          id: "interceptor",
          count: 1,
          zone: "discard",
          ownedFactionModifiers: [
            {
              faction: "blue",
              at: 12,
              tokenModifier: {
                combat: 1,
                sourceCardOnly: true
              }
            }
          ]
        },
        drawFromDrawPile: {
          id: "interceptor",
          count: 1
        }
      }
    },
    tokenSacrificeTrigger: {
      tokenId: "interceptor",
      effect: {
        armor: {
          amount: 1,
          target: "self",
          permanent: true,
          totalCap: 3
        }
      }
    },
    text: "At the start of every second turn this base remains in play, create an Interceptor in your discard pile, then draw an Interceptor from your draw pile if one is there. Whenever you sacrifice an Interceptor, give this base 1 Armor, up to 3 Armor.",
    factionText: "12 Blue Cards Owned: Interceptors created by this base gain 1 additional Combat when played.",
    flavor: "Every launched wing carries the weight of an ancient promise."
  },
  {
    id: "choir_of_unbroken_formation",
    name: "Choir of Unbroken Formation",
    image: "choir_of_unbroken_formation.png",
    faction: "blue",
    cost: 6,
    shop_cost: 105,
    type: "base",
    defense: 10,
    outpost: false,
    sigil: "✦",
    effect: {},
    factionThresholds: [
      {
        metric: "played",
        faction: "blue",
        at: 2,
        oncePerTurn: true,
        effect: {
          armor: {
            amount: 1,
            temporary: true
          }
        }
      },
      {
        metric: "played",
        faction: "blue",
        at: 4,
        oncePerTurn: true,
        effect: {
          shield: 3
        }
      },
      {
        metric: "played",
        faction: "blue",
        at: 6,
        oncePerTurn: true,
        effect: {
          createToken: {
            id: "acolyte",
            count: 1,
            zone: "hand"
          }
        }
      }
    ],
    text: "Second Blue: Give one base 1 temporary Armor. Fourth Blue: Gain 3 Shield. Sixth Blue: Create an Acolyte in your hand. Each threshold triggers only once per turn.",
    flavor: "No single voice commands the heavens. The formation does."
  },

  // ==========================================================
  // GREEN — GORAK WARHOST
  // ==========================================================
  {
    id: "brood_chain_stalker",
    name: "Brood-Chain Stalker",
    image: "brood_chain_stalker.png",
    faction: "green",
    cost: 3,
    shop_cost: 50,
    type: "ship",
    sigil: "⬢",
    effect: {
      combat: 2,
      createToken: {
        id: "spawn",
        count: 1,
        zone: "discard"
      }
    },
    tokenThresholds: [
      {
        metric: "playedBefore",
        tokenId: "spawn",
        at: 1,
        effect: {
          combat: 2
        }
      },
      {
        metric: "played",
        tokenId: "spawn",
        at: 3,
        effect: {
          combatAgainstBases: 2
        }
      }
    ],
    text: "Gain 2 Combat. Create a Spawn in your discard pile.",
    thresholdText: "If another Spawn was played this turn, gain 2 additional Combat. If three Spawn were played this turn, gain 2 additional Combat against bases.",
    flavor: "One calls. Ten answer. Then the ground begins to move."
  },
  {
    id: "warhost_token_devourer",
    name: "Warhost Token Devourer",
    image: "warhost_token_devourer.png",
    faction: "green",
    cost: 5,
    shop_cost: 85,
    type: "ship",
    sigil: "⬢",
    effect: {
      combat: 3
    },
    optionalTokenSacrificeChain: [
      {
        sacrificeNumber: 1,
        effect: {
          combat: 3
        }
      },
      {
        sacrificeNumber: 2,
        effect: {
          createToken: {
            id: "worker",
            count: 1,
            zone: "discard"
          }
        }
      },
      {
        sacrificeNumber: 3,
        effect: {
          draw: 1
        }
      }
    ],
    text: "Gain 3 Combat. You may sacrifice one token to gain 3 additional Combat. You may sacrifice a second token to create a Worker in your discard pile. You may sacrifice a third token to draw a card.",
    flavor: "The Warhost wastes nothing—not even its own."
  },
  {
    id: "furnace_hide_behemoth",
    name: "Furnace-Hide Behemoth",
    image: "furnace_hide_behemoth.png",
    faction: "green",
    cost: 6,
    shop_cost: 110,
    type: "ship",
    sigil: "⬢",
    effect: {
      combat: 4
    },
    heat: {
      gain: 1,
      max: 5,
      scaling: {
        effectPerHeat: {
          combat: 1
        }
      },
      overload: {
        at: 5,
        or: [
          {
            id: "furnace_hide_behemoth_siege",
            label: "Siege Overload",
            effect: {
              combatAgainstBases: 8
            },
            resetTo: 1
          },
          {
            id: "furnace_hide_behemoth_brood",
            label: "Brood Overload",
            effect: {
              createToken: {
                id: "spawn",
                count: 2,
                zone: "discard"
              }
            },
            resetTo: 0
          }
        ]
      }
    },
    text: "Gain 4 Combat and add 1 Heat to this card. Gain 1 additional Combat for each Heat currently on this card.",
    heatText: "Overload at 5 Heat — choose one: gain 8 additional Combat against bases, then reset to 1 Heat; or create two Spawn in your discard pile, then reset to 0 Heat. Maximum Heat 5.",
    flavor: "Its blood boils long before its enemies do."
  },
  {
    id: "spawning_war_nest",
    name: "Spawning War-Nest",
    image: "spawning_war_nest.png",
    faction: "green",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: false,
    sigil: "⬢",
    effect: {},
    factionThresholds: [
      {
        id: "spawning_war_nest_spawn",
        metric: "played",
        faction: "green",
        at: 3,
        oncePerTurn: true,
        effect: {
          createToken: {
            id: "spawn",
            count: 1,
            zone: "discard",
            trackCreatedToken: true
          },
          drawFromDrawPile: {
            id: "spawn",
            count: 1
          }
        }
      },
      {
        metric: "played",
        faction: "green",
        at: 5,
        oncePerTurn: true,
        effect: {
          moveTrackedCreatedToken: {
            sourceThresholdId: "spawning_war_nest_spawn",
            from: "discard",
            to: "topdeck"
          }
        }
      },
      {
        metric: "owned",
        faction: "green",
        at: 14,
        persistentModifier: {
          firstTokenPlayedEachTurn: {
            tokenId: "spawn",
            combat: 1
          }
        }
      }
    ],
    text: "Third Green: Create a Spawn in your discard pile, then draw a Spawn from your draw pile if one is there. Fifth Green: Put the created Spawn on top of your deck instead.",
    factionText: "14 Green Cards Owned: The first Spawn you play each turn gains 1 additional Combat.",
    flavor: "The nest does not sleep. It listens for the rhythm of marching feet."
  },
  {
    id: "labor_horde_encampment",
    name: "Labor-Horde Encampment",
    image: "labor_horde_encampment.png",
    faction: "green",
    cost: 6,
    shop_cost: 100,
    type: "base",
    defense: 9,
    outpost: false,
    sigil: "⬢",
    effect: {},
    recurring: {
      everyTurns: 2,
      effect: {
        createToken: {
          id: "worker",
          count: 1,
          zone: "discard"
        },
        drawFromDrawPile: {
          id: "worker",
          count: 1
        }
      }
    },
    tokenSacrificeTrigger: {
      tokenId: "worker",
      effect: {
        combat: 1
      },
      ownedFactionBonus: {
        faction: "green",
        at: 12,
        firstEachTurn: true,
        effect: {
          trade: 1
        }
      }
    },
    text: "At the start of every second turn this base remains in play, create a Worker in your discard pile, then draw a Worker from your draw pile if one is there. Whenever a Worker is sacrificed, gain 1 Combat.",
    factionText: "12 Green Cards Owned: The first Worker sacrificed each turn also grants 1 Trade.",
    flavor: "They build the road, drag the siege engines, and become the fuel."
  },

  // ==========================================================
  // RED — UMBRAL COVENANT
  // ==========================================================
  {
    id: "cinder_core_raider",
    name: "Cinder-Core Raider",
    image: "cinder_core_raider.png",
    faction: "red",
    cost: 3,
    shop_cost: 50,
    type: "ship",
    sigil: "◒",
    effect: {
      combat: 2
    },
    heat: {
      gain: 1,
      max: 5,
      thresholds: [
        {
          at: 3,
          effect: {
            combat: 2
          }
        }
      ],
      overload: {
        at: 5,
        optionalTokenSacrifice: {
          successEffect: {
            combat: 7
          },
          successResetTo: 1,
          declineEffect: {
            selfDamage: 2
          },
          declineResetTo: 0
        }
      }
    },
    text: "Gain 2 Combat and add 1 Heat to this card. At 3+ Heat, gain 2 additional Combat.",
    heatText: "Overload at 5 Heat: You may sacrifice a token. If you do, gain 7 additional Combat and reset to 1 Heat. Otherwise, take 2 Authority damage and reset to 0 Heat. Maximum Heat 5.",
    flavor: "The core demands payment. It has never cared whose blood pays it."
  },
  {
    id: "emberling_taskmaster",
    name: "Emberling Taskmaster",
    image: "emberling_taskmaster.png",
    faction: "red",
    cost: 4,
    shop_cost: 65,
    type: "ship",
    sigil: "◒",
    effect: {
      combat: 2,
      createToken: {
        id: "emberling",
        count: 1,
        zone: "discard"
      },
      drawFromDrawPile: {
        id: "emberling",
        count: 1
      }
    },
    tokenSacrificeThresholds: [
      {
        tokenId: "emberling",
        at: 1,
        effect: {
          trade: 1
        }
      },
      {
        tokenId: "emberling",
        at: 2,
        effect: {
          combat: 3
        }
      },
      {
        tokenId: "emberling",
        at: 3,
        effect: {
          draw: 1
        }
      }
    ],
    text: "Gain 2 Combat. Create an Emberling in your discard pile, then draw an Emberling from your draw pile if one is there.",
    thresholdText: "First Emberling sacrificed this turn: gain 1 Trade. Second: gain 3 Combat. Third: draw a card.",
    flavor: "Individually, they are sparks. Together, they are a verdict."
  },
  {
    id: "covenant_heat_harvester",
    name: "Covenant Heat Harvester",
    image: "covenant_heat_harvester.png",
    faction: "red",
    cost: 6,
    shop_cost: 105,
    type: "ship",
    sigil: "◒",
    effect: {
      combat: 4,
      harvestHeat: {
        target: "friendlyShip",
        min: 1,
        max: "all",
        effectPerHeat: {
          combat: 2
        },
        effectCap: {
          combat: 8
        },
        thresholds: [
          {
            removedAtLeast: 4,
            effect: {
              selfDamage: 2
            }
          }
        ]
      }
    },
    text: "Gain 4 Combat. Choose one of your ships with Heat and remove any amount of Heat from it. Gain 2 Combat for each Heat removed, up to 8 additional Combat.",
    thresholdText: "If four or more Heat was removed, take 2 Authority damage.",
    flavor: "The machine cools only because its master has learned to burn."
  },
  {
    id: "pyre_swarm_foundry",
    name: "Pyre-Swarm Foundry",
    image: "pyre_swarm_foundry.png",
    faction: "red",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: false,
    sigil: "◒",
    effect: {},
    recurring: {
      everyTurns: 2,
      effect: {
        createToken: {
          id: "emberling",
          count: 1,
          zone: "discard"
        },
        drawFromDrawPile: {
          id: "emberling",
          count: 1
        }
      }
    },
    tokenSacrificeTrigger: {
      tokenId: "emberling",
      perTurnCap: 2,
      effect: {
        or: [
          {
            id: "pyre_swarm_foundry_add_heat",
            label: "Add Heat",
            effect: {
              addHeat: {
                amount: 1,
                target: "friendlyHeatShip"
              }
            }
          },
          {
            id: "pyre_swarm_foundry_remove_heat",
            label: "Remove Heat",
            effect: {
              coolHeat: {
                amount: 1
              }
            }
          }
        ]
      }
    },
    text: "At the start of every second turn this base remains in play, create an Emberling in your discard pile, then draw an Emberling from your draw pile if one is there.",
    thresholdText: "Whenever an Emberling is sacrificed, add 1 Heat to one of your Heat ships or remove 1 Heat from one of your Heat ships. This triggers up to twice per turn.",
    flavor: "Every creature born within it already knows how it will die."
  },
  {
    id: "throne_of_escalating_ruin",
    name: "Throne of Escalating Ruin",
    image: "throne_of_escalating_ruin.png",
    faction: "red",
    cost: 7,
    shop_cost: 125,
    type: "base",
    defense: 10,
    outpost: false,
    sigil: "◒",
    effect: {},
    factionThresholds: [
      {
        metric: "played",
        faction: "red",
        at: 2,
        oncePerTurn: true,
        effect: {
          combat: 2
        }
      },
      {
        metric: "played",
        faction: "red",
        at: 4,
        oncePerTurn: true,
        effect: {
          createToken: {
            id: "emberling",
            count: 1,
            zone: "discard",
            ownedFactionZoneOverride: {
              faction: "red",
              at: 15,
              zone: "hand"
            }
          }
        }
      },
      {
        metric: "played",
        faction: "red",
        at: 6,
        oncePerTurn: true,
        effect: {
          draw: 1,
          selfDamage: 1
        }
      }
    ],
    text: "Second Red: Gain 2 Combat. Fourth Red: Create an Emberling in your discard pile. Sixth Red: Draw a card and take 1 Authority damage. Each threshold triggers only once per turn.",
    factionText: "15 Red Cards Owned: The Emberling created by this base enters your hand instead.",
    flavor: "The Covenant measures devotion by how much remains after the fire."
  },
{
  id: "seraph_of_the_final_bell",
  name: "Seraph of the Final Bell",
  image: "seraph_of_the_final_bell.png",
  faction: "blue",
  collectible: false,
  token: true,
  transformedFrom: "cherub_of_last_mercy",
  cost: 10,
  shop_cost: 0,
  type: "ship",
  sigil: "✦",
  effect: {
    or: [
      {
        label: "Final Judgment",
        effect: {
          destroyBase: 1
        }
      },
      {
        label: "Merciful Passage",
        effect: {
          combat: 4,
          shield: 2
        }
      }
    ]
  },
  ally: {
    draw: 1
  },
  text: "Choose one: gain 1 Raze; or gain 4 Combat and 2 Shield.",
  allyText: "Draw 1 card.",
  flavor: "Its bell does not announce death. It announces that death has already arrived."
},


// ==========================================================
// RED — THE VILE GREAT LOCUST
// Expensive and weak at first, but devastating after ascension.
// ==========================================================

{
  id: "baby_great_locust",
  name: "Baby Great Locust",
  image: "baby_great_locust.png",
  faction: "red",
  cost: 5,
  shop_cost: 75,
  type: "ship",
  sigil: "◒",
  effect: {
    combat: 1
  },
  ally: {},
  transform: {
    trigger: "timesPlayed",
    required: 3,
    into: "vile_great_locust",
    destination: "discard"
  },
  text: "Gain 1 Combat.",
  allyText: "",
  transformText: "Ascendant — After this card has been played 3 times, transform it into the Vile Great Locust.",
  flavor: "Its first hunger is almost harmless. Almost."
},

{
  id: "vile_great_locust",
  name: "Vile Great Locust",
  image: "vile_great_locust.png",
  faction: "red",
  collectible: false,
  token: true,
  transformedFrom: "baby_great_locust",
  cost: 13,
  shop_cost: 0,
  type: "ship",
  sigil: "◒",
  effect: {
    combat: 5
  },
  ally: {},
  doubleAlly: {
    scrapOwn: 2,
    draw: 2
  },
  text: "Gain 5 Combat.",
  allyText: "",
  doubleAllyText: "Gain 2 Purge and draw 2 cards.",
  flavor: "When its wings darken the realm, nothing unwanted survives—not even memory."
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
  collectible_edition: true,
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
  collectible_edition: true,
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
    collectible_edition: true,
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
    collectible_edition: true,
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
    collectible_edition: true,
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
    collectible_edition: true,
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
    collectible_edition: true,
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
    collectible_edition: true,
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
    collectible_edition: true,
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
    collectible_edition: true,
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
    collectible_edition: true,
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
    collectible_edition: true,
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
  },

  // ==========================================================
  // PERSISTENT SYSTEMS — INITIAL PLAYABLE TEST SET
  // Heat, tokens, attachments, expansion bases, construction,
  // discard recovery, sacrifice chains, and faction density.
  // ==========================================================

  // ----- REAL DECK TOKENS -----
  {
    id: "drone",
    name: "Drone",
    image: "drone.png",
    faction: "yellow",
    cost: 0,
    shop_cost: 0,
    type: "ship",
    sigil: "◈",
    token: true,
    collectible: false,
    effect: {
      combat: 1,
      drawFromDrawPile: {
        id: "drone",
        count: 1,
        look: 6
      }
    },
    ally: { trade: 1 },
    text: "Token. Gain 1 Combat. If another Drone is among the top 3 cards of your draw pile, draw it.",
    allyText: "Gain 1 Trade.",
    flavor: "A disposable possibility given engines and a purpose."
  },
  {
    id: "worker",
    name: "Worker",
    image: "worker.png",
    faction: "green",
    cost: 0,
    shop_cost: 0,
    type: "ship",
    sigil: "⬢",
    token: true,
    collectible: false,
    effect: {
      trade: 1,
      drawFromDrawPile: {
        id: "worker",
        count: 1,
        look: 3
      }
    },
    sacrifice: {
      draw: 1,
      or: [
        { label: "Build", effect: { advanceConstruction: { amount: 1 } } },
        { label: "Repair", effect: { repair: { amount: 1 } } }
      ]
    },
    text: "Token. Gain 1 Trade. If a Worker is among the top 3 cards of your draw pile, draw it.",
    sacrificeText: "Sacrifice — draw 1 card, then remove 1 Construction or repair an Expansion Base for 1.",
    flavor: "Worlds are conquered by armies and made permanent by labor."
  },
  {
    id: "interceptor",
    name: "Interceptor",
    image: "interceptor.png",
    faction: "blue",
    cost: 0,
    shop_cost: 0,
    type: "ship",
    sigil: "✦",
    token: true,
    collectible: false,
    effect: {
      combat: 3,
      drawFromDrawPile: {
        id: "interceptor",
        count: 1,
        look: 3
      }
    },
    sacrifice: { armor: { amount: 2 } },
    text: "Token. Gain 1 Combat. If an Interceptor is among the top 3 cards of your draw pile, draw one.",
    sacrificeText: "Sacrifice: Give one base 2 temporary Armor.",
    flavor: "Its final maneuver is a shield drawn across another vessel."
  },
  {
    id: "spawn",
    name: "Spawn",
    image: "spawn.png",
    faction: "green",
    cost: 0,
    shop_cost: 0,
    type: "ship",
    sigil: "⬢",
    token: true,
    collectible: false,
    effect: {
      combat: 2,
      drawFromDrawPile: {
        id: "spawn",
        count: 1,
        look: 2
      }
    },
    tokenCombo: { id: "spawn", count: 3, into: "brood_horror", oncePerTurn: true },
    sacrifice: {
      drawFromDrawPile: {
        id: "spawn",
        count: 2,
        look: 6
      }
    },
    text: "Token. Gain 2 Combat. If a Spawn is among the top 2 cards of your draw pile, draw it. Three played Spawn may merge into a Brood Horror.",
    sacrificeText: "Sacrifice: Draw up to 2 Spawn from the top 6 cards of your draw pile.",
    flavor: "One is vermin. Three are an omen."
  },
  {
    id: "brood_horror",
    name: "Brood Horror",
    image: "brood_horror.png",
    faction: "green",
    cost: 0,
    shop_cost: 0,
    type: "ship",
    sigil: "⬢",
    token: true,
    collectible: false,
    tags: ["spawn"],
    effect: { combat: 8 },
    doubleAlly: {draw: 1},
    sacrifice: { combat: 5 },
    text: "Token. Gain 8 Combat. This counts as a Spawn.",
    sacrificeText: "Sacrifice: Gain 5 additional Combat.",
    flavor: "The brood remembers every body it used to become one."
  },
  {
    id: "acolyte",
    name: "Acolyte",
    image: "acolyte.png",
    faction: "blue",
    cost: 0,
    shop_cost: 0,
    type: "ship",
    sigil: "✦",
    token: true,
    collectible: false,
    effect: { trade: 1 },
    tokenCombo: { id: "acolyte", count: 2, searchMinCost: 5, oncePerTurn: true },
    text: "Token. Gain 1 Trade. Two played Acolytes may be sacrificed to draw a cost 5+ card from your deck.",
    flavor: "Two voices are enough to open the sealed reliquary."
  },
  {
    id: "emberling",
    name: "Emberling",
    image: "emberling.png",
    faction: "red",
    cost: 0,
    shop_cost: 0,
    type: "ship",
    sigil: "◒",
    token: true,
    collectible: false,
    effect: { combat: 3 },
    doubleAlly: {
      drawFromDrawPile: {
        id: "emberling",
        count: 1,
        look: 5
      }
    },
    sacrifice: { combat: 1 },
    text: "Token. Gain 1 Combat.",
    doubleAllyText: "If another Emberling is among the top 5 cards of your draw pile, draw it.",
    sacrificeText: "Sacrifice: Gain 1 additional Combat.",
    flavor: "A small flame is still willing to consume itself."
  },

  // ----- HEAT AND OVERLOAD -----
  {
    id: "reactor_skirmisher",
    name: "Reactor Skirmisher",
    image: "reactor_skirmisher.png",
    faction: "red",
    cost: 3,
    shop_cost: 35,
    type: "ship",
    sigil: "◒",
    effect: { combat: 2 },
    heat: {
      gain: 1,
      max: 5,
      thresholds: [{ at: 3, effect: { combat: 2 } }],
      overload: { at: 5, optional: true, reset: 1, selfDamage: 2, effect: { combat: 5 } }
    },
    text: "Gain 2 Combat and 1 Heat. At Heat 3+, gain 2 more Combat. At Heat 5, you may Overload: gain 5 Combat, take 2 Authority damage, and reset to Heat 1.",
    heatText: "Maximum Heat 5. Optional Overload at Heat 5.",
    flavor: "Its reactor is safest when no one asks it to win."
  },
  {
    id: "furnace_dreadnought",
    name: "Furnace Dreadnought",
    image: "furnace_dreadnought.png",
    faction: "red",
    cost: 7,
    shop_cost: 125,
    type: "ship",
    sigil: "◒",
    effect: { combat: 5 },
    heat: {
      gain: 1,
      max: 5,
      thresholds: [{ at: 3, effect: { combat: 3 } }],
      overload: { at: 5, optional: false, reset: 0, selfDamage: 3, effect: { combat: 4 } }
    },
    text: "Gain 5 Combat and 1 Heat. At Heat 3+, gain 3 more Combat. At Heat 5, automatically Overload to 12 total Combat, take 3 Authority damage, and reset to Heat 0.",
    heatText: "Maximum Heat 5. Automatic Overload at Heat 5.",
    flavor: "The crew measures victory in seconds before containment failure."
  },
  {
    id: "voltaic_corsair",
    name: "Voltaic Corsair",
    image: "voltaic_corsair.png",
    faction: "yellow",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "◈",
    effect: { trade: 2, combat: 1 },
    heat: {
      gain: 1,
      max: 5,
      actions: [{ label: "Convert Heat to Insight", cost: 2, oncePerTurn: true, effect: { draw: 1 } }]
    },
    text: "Gain 2 Trade, 1 Combat, and 1 Heat. You may remove 2 Heat from this card to draw a card.",
    heatText: "Maximum Heat 5. Spend 2 Heat: draw 1 card.",
    flavor: "Every overheating circuit predicts one more profitable escape."
  },
  {
    id: "thermal_aegis",
    name: "Thermal Aegis",
    image: "thermal_aegis.png",
    faction: "blue",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "✦",
    effect: { shield: 2, coolHeat: { amount: 1, ifChangedEffect: { shield: 2 } } },
    ally: { trade: 1 },
    text: "Gain 2 Shield. Remove 1 Heat from one of your ships; if Heat was removed, gain 2 additional Shield.",
    allyText: "Gain 1 Trade.",
    flavor: "The order calls cooling a mercy. Reactor captains call it survival."
  },
  {
    id: "coolant_tender",
    name: "Coolant Tender",
    image: "coolant_tender.png",
    faction: "blue",
    cost: 3,
    shop_cost: 35,
    type: "ship",
    sigil: "✦",
    effect: {
      or: [
        { label: "Deep Cool", effect: { coolHeat: { amount: 2 } } },
        { label: "Split Cool", effect: { coolHeat: { amount: 1, targets: 2 } } },
        { label: "Supply Run", effect: { trade: 2 } }
      ]
    },
    text: "Choose one: remove up to 2 Heat from one ship; remove 1 Heat from up to two ships; or gain 2 Trade.",
    flavor: "It carries the one resource every reactor eventually worships."
  },

  // ----- CHARGE BASES -----
  {
    id: "storm_capacitor",
    name: "Storm Capacitor",
    image: "storm_capacitor.png",
    faction: "yellow",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: false,
    sigil: "◈",
    effect: {},
    charge: {
      trigger: "friendlyFactionPlayed",
      faction: "yellow",
      gain: 1,
      max: 8,
      actions: [
        { label: "Market Pulse", cost: 2, effect: { trade: 2 } },
        { label: "Probability Release", cost: 4, effect: { draw: 1 } },
        { label: "Perfect Discharge", cost: 8, effect: { draw: 2, topdeckFromHand: 1 } }
      ]
    },
    text: "Whenever you play another Yellow card, gain 1 Charge, up to 8.",
    chargeText: "Spend 2: gain 2 Trade. Spend 4: draw 1. Spend 8: draw 2, then put one card from hand on top of your deck.",
    flavor: "It stores the lightning from futures that never arrived."
  },
  {
    id: "aegis_font",
    name: "Aegis Font",
    image: "aegis_font.png",
    faction: "blue",
    cost: 5,
    shop_cost: 95,
    type: "base",
    defense: 9,
    outpost: false,
    sigil: "✦",
    effect: { shield: 1 },
    charge: {
      trigger: "shieldGained",
      gain: 1,
      max: 8,
      perTurnCap: 2,
      actions: [
        { label: "Fortify", cost: 2, effect: { armor: { amount: 3 } } },
        { label: "Sanctuary", cost: 5, effect: { shield: 5 } },
        { label: "Aegis Wave", cost: 8, effect: { armor: { amount: 3, all: true } } }
      ]
    },
    text: "Gain 1 Shield. Whenever you gain Shield, gain 1 Charge, up to twice each turn and 8 total.",
    chargeText: "Spend 2: give one base 3 Armor. Spend 5: gain 5 Shield. Spend 8: all bases gain 3 Armor.",
    flavor: "Every shield raised becomes a prayer stored in blue fire."
  },
  {
    id: "brood_vat",
    name: "Brood Vat",
    image: "brood_vat.png",
    faction: "green",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: false,
    sigil: "⬢",
    effect: {},
    charge: {
      trigger: "tokenPlayed",
      tokenId: "spawn",
      gain: 1,
      max: 8,
      actions: [
        { label: "Seed the Brood", cost: 2, effect: { createToken: { id: "spawn", count: 1, zone: "discard" } } },
        { label: "Release Spawn", cost: 5, effect: { createToken: { id: "spawn", count: 1, zone: "hand" } } },
        { label: "Birth Horror", cost: 8, effect: { createToken: { id: "brood_horror", count: 1, zone: "discard" } } }
      ]
    },
    text: "Whenever a Spawn is played, gain 1 Charge, up to 8.",
    chargeText: "Spend 2: create a Spawn in discard. Spend 5: create one in hand. Spend 8: create a Brood Horror in discard.",
    flavor: "The vat does not manufacture life. It remembers hunger."
  },
  {
    id: "blood_tithe_crucible",
    name: "Blood-Tithe Crucible",
    image: "blood_tithe_crucible.png",
    faction: "red",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: false,
    sigil: "◒",
    effect: {},
    charge: {
      trigger: "cardSacrificed",
      gain: 1,
      max: 9,
      actions: [
        { label: "Tithe of War", cost: 2, effect: { combat: 3 } },
        { label: "Painful Insight", cost: 5, effect: { draw: 1, selfDamage: 1 } },
        { label: "Return the Ash", cost: 9, effect: { combat: 10, reclaim: { maxCost: 3, sacrificedOnly: true } } }
      ]
    },
    text: "Whenever you sacrifice one of your cards, gain 1 Charge, up to 9.",
    chargeText: "Spend 2: gain 3 Combat. Spend 5: draw 1 and take 1 damage. Spend 9: gain 10 Combat and Reclaim a sacrificed cost 3 or less card.",
    flavor: "The Crucible keeps exact accounts, especially of what no longer exists."
  },

  // ----- SACRIFICE CHAINS -----
  {
    id: "cinder_initiate",
    name: "Cinder Initiate",
    image: "cinder_initiate.png",
    faction: "red",
    cost: 2,
    shop_cost: 20,
    type: "ship",
    sigil: "◒",
    effect: { combat: 1 },
    sacrifice: { combat: 3 },
    text: "Gain 1 Combat.",
    sacrificeText: "Sacrifice: Gain 3 additional Combat and advance your Sacrifice Count.",
    flavor: "The first lesson is that ash can still strike."
  },
  {
    id: "chain_reaction_marauder",
    name: "Chain-Reaction Marauder",
    image: "chain_reaction_marauder.png",
    faction: "red",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "◒",
    effect: { combat: 2 },
    sacrificeThresholds: [
      { at: 1, effect: { combat: 2 } },
      { at: 2, effect: { combat: 3 } },
      { at: 3, effect: { draw: 1 } }
    ],
    text: "Gain 2 Combat. At Sacrifice 1 gain 2 Combat; at 2 gain 3 more; at 3 draw a card.",
    flavor: "Every loss is another fuse already burning."
  },
  {
    id: "altar_of_escalation",
    name: "Altar of Escalation",
    image: "altar_of_escalation.png",
    faction: "red",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: false,
    sigil: "◒",
    effect: {},
    sacrificeThresholds: [
      { at: 1, effect: { combat: 1 } },
      { at: 2, effect: { trade: 2 } },
      { at: 3, effect: { draw: 1 } }
    ],
    text: "Each turn: after your first sacrifice gain 1 Combat; after your second gain 2 Trade; after your third draw a card.",
    flavor: "The altar never asks what was offered, only what comes next."
  },
  {
    id: "reclamation_crew",
    name: "Reclamation Crew",
    image: "reclamation_crew.png",
    faction: "green",
    cost: 3,
    shop_cost: 35,
    type: "ship",
    sigil: "⬢",
    effect: { trade: 2 },
    sacrificeThresholds: [
      { at: 1, requiresSacrificedId: "worker", effect: { advanceConstruction: { amount: 1 } } },
      { at: 2, effect: { repair: { amount: 2 } } }
    ],
    text: "Gain 2 Trade. If a Worker was sacrificed this turn, remove 1 Construction. At Sacrifice 2, repair an Expansion Base for 2.",
    flavor: "They rebuild with whatever the war was forced to surrender."
  },

  // ----- ATTACHMENTS -----
  {
    id: "reactive_plating",
    name: "Reactive Plating",
    image: "reactive_plating.png",
    faction: "blue",
    cost: 3,
    shop_cost: 35,
    type: "attachment",
    sigil: "✦",
    attachment: { defense: 2, expansionHealth: 4 },
    effect: {},
    text: "Attach to a base. A normal base gains +2 Defense; an Expansion Base gains +4 maximum and current Health.",
    flavor: "Every impact teaches the plating how to survive the next."
  },
  {
    id: "drone_bay",
    name: "Drone Bay",
    image: "drone_bay.png",
    faction: "yellow",
    cost: 4,
    shop_cost: 60,
    type: "attachment",
    sigil: "◈",
    attachment: { startOfTurn: { createToken: { id: "drone", count: 1, zone: "discard" } } },
    effect: {},
    text: "Attach to a base. At the start of your turn, create a Drone in your discard pile.",
    flavor: "The bay is never empty; it is only between launch cycles."
  },
  {
    id: "construction_crane",
    name: "Construction Crane",
    image: "construction_crane.png",
    faction: "green",
    cost: 4,
    shop_cost: 60,
    type: "attachment",
    sigil: "⬢",
    attachment: {
      expansionOnly: true,
      onAttach: { advanceConstruction: { amount: 1, attachedBase: true } },
      everyTurns: 3,
      recurring: { advanceConstruction: { amount: 1, attachedBase: true }, repairFallback: 2 }
    },
    effect: {},
    text: "Attach only to an Expansion Base. Remove 1 Construction immediately and again every third turn; if complete, repair it for 2 instead.",
    flavor: "The crane's arm is the first horizon of every new fortress."
  },
  {
    id: "void_cannon",
    name: "Void Cannon",
    image: "void_cannon.png",
    faction: "red",
    cost: 5,
    shop_cost: 90,
    type: "attachment",
    sigil: "◒",
    attachment: { action: { label: "Fire Void Cannon", oncePerTurn: true, effect: { selfDamage: 1, combat: 6 } } },
    effect: {},
    text: "Attach to a base. Once per turn, take 1 Authority damage to gain 4 Combat.",
    flavor: "The ammunition is absence. The recoil is paid in years."
  },

  // ----- EXPANSION BASES -----
  {
    id: "horizon_probability_engine",
    name: "Horizon Probability Engine",
    image: "horizon_probability_engine.png",
    faction: "yellow",
    cost: 8,
    shop_cost: 150,
    type: "base",
    expansion: true,
    health: 20,
    construction: 3,
    defense: 20,
    outpost: false,
    sigil: "◈",
    constructionEffect: { peekTop: 1 },
    effect: { trade: 2 },
    charge: {
      trigger: "friendlyFactionPlayed",
      faction: "yellow",
      gain: 1,
      max: 8,
      actions: [
        { label: "Select Future", cost: 4, effect: { draw: 1 } },
        { label: "Open Horizons", cost: 8, effect: { draw: 2 } }
      ]
    },
    text: "Expansion Base — 20 Health, Construction 3. When complete, gain 2 Trade each turn and Charge from Yellow cards.",
    chargeText: "Spend 4: draw 1. Spend 8: draw 2.",
    flavor: "A continent-sized machine voting on which tomorrow may exist."
  },
  {
    id: "cathedral_of_the_last_light",
    name: "Cathedral of the Last Light",
    image: "cathedral_of_the_last_light.png",
    faction: "blue",
    cost: 8,
    shop_cost: 150,
    type: "base",
    expansion: true,
    health: 24,
    construction: 3,
    defense: 24,
    outpost: false,
    sigil: "✦",
    constructionEffect: { shield: 1 },
    effect: { armor: { amount: 1, all: true } },
    charge: {
      trigger: "tokenPlayed",
      tokenId: "acolyte",
      gain: 1,
      max: 6,
      actions: [{ label: "Grand Benediction", cost: 6, effect: { armor: { amount: 3, all: true } } }]
    },
    text: "Expansion Base — 24 Health, Construction 3. While building, gain 1 Shield each turn. When complete, all bases gain 1 Armor each turn.",
    chargeText: "Acolytes add Charge. Spend 6: all bases gain 3 Armor.",
    flavor: "Its final lamp is visible from every wounded world."
  },
  {
    id: "worldroot_foundry",
    name: "Worldroot Foundry",
    image: "worldroot_foundry.png",
    faction: "green",
    cost: 8,
    shop_cost: 150,
    type: "base",
    expansion: true,
    health: 25,
    construction: 3,
    defense: 25,
    outpost: false,
    sigil: "⬢",
    effect: { createToken: { id: "worker", count: 1, zone: "discard" }, drawFromDrawPile: { id: "worker", count: 1 } },
    charge: {
      trigger: "cardSacrificed",
      sacrificedId: "worker",
      gain: 1,
      max: 6,
      actions: [{ label: "Mobilize Labor", cost: 6, effect: { createToken: { id: "worker", count: 2, zone: "hand" } } }]
    },
    text: "Expansion Base — 25 Health, Construction 3. When complete, create a Worker in discard each turn, then draw a Worker from your draw pile if one is there. Sacrificed Workers add Charge.",
    chargeText: "Spend 6: create two Workers in hand.",
    flavor: "The roots mine stone below while the foundry builds empires above."
  },
  {
    id: "furnace_of_a_thousand_oaths",
    name: "Furnace of a Thousand Oaths",
    image: "furnace_of_a_thousand_oaths.png",
    faction: "red",
    cost: 8,
    shop_cost: 150,
    type: "base",
    expansion: true,
    health: 21,
    construction: 3,
    defense: 21,
    outpost: false,
    sigil: "◒",
    effect: {},
    sacrificeThresholds: [
      { at: 1, effect: { combat: 2 } },
      { at: 2, effect: { trade: 2 } },
      { at: 3, effect: { draw: 1, selfDamage: 1 } }
    ],
    text: "Expansion Base — 21 Health, Construction 3. When complete: Sacrifice 1 gives 2 Combat; 2 gives 2 Trade; 3 draws a card and deals 1 damage to you.",
    flavor: "Every oath burns brighter after the speaker is gone."
  },
  {
    id: "forge_crew",
    name: "Forge Crew",
    image: "forge_crew.png",
    faction: "green",
    cost: 3,
    shop_cost: 35,
    type: "ship",
    sigil: "⬢",
    effect: {
      or: [
        { label: "Trade", effect: { trade: 2 } },
        { label: "Construct", effect: { advanceConstruction: { amount: 1 } } },
        { label: "Repair", effect: { repair: { amount: 2 } } }
      ]
    },
    ally: { repeatPrimaryChoice: 1 },
    text: "Choose one: gain 2 Trade; remove 1 Construction; or repair an Expansion Base for 2.",
    allyText: "After the first choice resolves, choose a different Forge Crew option.",
    flavor: "Their hammers keep time for buildings too large to see at once."
  },

  // ----- DISCARD RECOVERY -----
  {
    id: "salvage_navigator",
    name: "Salvage Navigator",
    image: "salvage_navigator.png",
    faction: "yellow",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "◈",
    effect: {
      or: [
        { label: "Reclaim", effect: { reclaim: { tags: ["drone"], types: ["attachment"] } } },
        { label: "Trade", effect: { trade: 3 } }
      ]
    },
    text: "Choose one: Reclaim a Drone or Attachment from discard; or gain 3 Trade.",
    flavor: "It navigates by the wake of things already lost."
  },
  {
    id: "grave_circuit_engineer",
    name: "Grave-Circuit Engineer",
    image: "grave_circuit_engineer.png",
    faction: "red",
    cost: 5,
    shop_cost: 90,
    type: "ship",
    sigil: "◒",
    effect: { redeploy: { maxCost: 2, sacrificedThisTurn: true, oncePerTurn: true }, selfDamage: 1 },
    text: "Take 1 Authority damage. Redeploy a card sacrificed this turn costing 2 or less; after it resolves, put it on the bottom of your deck.",
    redeployText: "Redeploy resolves the recovered Primary ability immediately and then bottom-decks that exact card instance.",
    flavor: "Death is only a circuit with one missing bridge."
  },

  // ----- TOKEN GENERATORS -----
  {
    id: "drone_fabricator",
    name: "Drone Fabricator",
    image: "drone_fabricator.png",
    faction: "yellow",
    cost: 3,
    shop_cost: 35,
    type: "ship",
    sigil: "◈",
    effect: { trade: 1, createToken: { id: "drone", count: 1, zone: "discard" }, drawFromDrawPile: { id: "drone", count: 1 } },
    ally: { createToken: { id: "drone", count: 1, zone: "topdeck" } },
    text: "Gain 1 Trade, create a Drone in your discard pile, then draw a Drone from your draw pile if one is there.",
    allyText: "Create another Drone on top of your deck.",
    flavor: "The first drone is a prototype. The next thousand are policy."
  },
  {
    id: "replication_carrier",
    name: "Replication Carrier",
    image: "replication_carrier.png",
    faction: "yellow",
    cost: 6,
    shop_cost: 110,
    type: "ship",
    sigil: "◈",
    effect: { combat: 3, createToken: { id: "drone", count: 1, zone: "discard" }, drawFromDrawPile: { id: "drone", count: 1 } },
    factionThresholds: [{ metric: "played", faction: "yellow", at: 3, effect: { createToken: { id: "drone", count: 1, zone: "discard" } } }],
    text: "Gain 3 Combat, create a Drone in discard, then draw a Drone from your draw pile if one is there. If this is your third Yellow card this turn, create a second Drone.",
    flavor: "Its hangars contain smaller hangars, each opening at once."
  },
  {
    id: "chapel_recruiter",
    name: "Chapel Recruiter",
    image: "chapel_recruiter.png",
    faction: "blue",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "✦",
    effect: { createToken: { id: "acolyte", count: 1, zone: "discard" }, drawFromDrawPile: { id: "acolyte", count: 1 } },
    ally: { reclaim: { ids: ["acolyte"] } },
    text: "Create an Acolyte in your discard pile, then draw an Acolyte from your draw pile if one is there.",
    allyText: "Reclaim an Acolyte.",
    flavor: "The chapel always has room for one more vow."
  },
  {
    id: "interceptor_hangar",
    name: "Interceptor Hangar",
    image: "interceptor_hangar.png",
    faction: "blue",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: false,
    sigil: "✦",
    effect: {},
    recurring: { everyTurns: 2, effect: { createToken: { id: "interceptor", count: 1, zone: "discard" }, drawFromDrawPile: { id: "interceptor", count: 1 } } },
    charge: {
      trigger: "tokenPlayed",
      tokenId: "interceptor",
      gain: 1,
      max: 3,
      actions: [{ label: "Scramble Interceptor", cost: 3, effect: { createToken: { id: "interceptor", count: 1, zone: "hand" } } }]
    },
    text: "Every second turn, create an Interceptor in discard, then draw an Interceptor from your draw pile if one is there. Played Interceptors add Charge.",
    chargeText: "Spend 3: create an Interceptor in hand.",
    flavor: "The bells ring only after the defenders are already airborne."
  },
  {
    id: "worker_caravan",
    name: "Worker Caravan",
    image: "worker_caravan.png",
    faction: "green",
    cost: 3,
    shop_cost: 35,
    type: "ship",
    sigil: "⬢",
    effect: { trade: 2, createToken: { id: "worker", count: 1, zone: "discard" }, drawFromDrawPile: { id: "worker", count: 1 } },
    ally: { trade: 1 },
    text: "Gain 2 Trade, create a Worker in your discard pile, then draw a Worker from your draw pile if one is there.",
    allyText: "Gain 1 Trade.",
    flavor: "Every road ends at a wall that still needs building."
  },
  {
    id: "brood_shepherd",
    name: "Brood Shepherd",
    image: "brood_shepherd.png",
    faction: "green",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "⬢",
    effect: { createToken: { id: "spawn", count: 1, zone: "discard", topdeckIfPlayed: "spawn" } },
    ally: { combat: 2 },
    text: "Create a Spawn in discard. If a Spawn was already played this turn, put it on top of your deck instead.",
    allyText: "Gain 2 Combat.",
    flavor: "It leads by feeding whatever follows."
  },
  {
  id: "probability_lens_array",
  name: "Probability Lens Array",
  image: "probability_lens_array.png",
  faction: "yellow",
  cost: 1,
  shop_cost: 35,
  type: "attachment",
  sigil: "◈",

  attachment: {
    startOfTurn: {
      peekTop: 1
    }
  },

  effect: {},

  text:
    "Attach to a base. At the start of your turn, inspect the top card of your deck.",

  flavor:
    "The machine never predicts the future. It simply rejects the futures it dislikes."
},

{
  id: "nullfield_projector",
  name: "Nullfield Projector",
  image: "nullfield_projector.png",
  faction: "yellow",
  cost: 2,
  shop_cost: 60,
  type: "attachment",
  sigil: "◈",

  attachment: {
    action: {
      label: "Project Nullfield",
      oncePerTurn: true,
      effect: {
        stun: 1
      }
    }
  },

  effect: {},

  text:
    "Attach to a base. Once per turn, gain 1 Disable.",

  flavor:
    "Inside its radius, engines remember every reason they should stop."
},

{
  id: "recursive_drone_hangar",
  name: "Recursive Drone Hangar",
  image: "recursive_drone_hangar.png",
  faction: "yellow",
  cost: 5,
  shop_cost: 90,
  type: "attachment",
  sigil: "◈",

  attachment: {
    everyTurns: 2,
    recurring: {
      createToken: {
        id: "drone",
        count: 2,
        zone: "hand"
      }
    }
  },

  effect: {},

  text:
    "Attach to a base. Every second turn, create 2 Drones in your hand.",

  flavor:
    "Each finished machine immediately begins assembling its successor."
},

{
  id: "causal_accelerator",
  name: "Causal Accelerator",
  image: "causal_accelerator.png",
  faction: "yellow",
  cost: 3,
  shop_cost: 90,
  type: "attachment",
  sigil: "◈",

  attachment: {
    expansionOnly: true,

    onAttach: {
      advanceConstruction: {
        amount: 1,
        attachedBase: true
      }
    },

    action: {
      label: "Accelerate Construction",
      oncePerTurn: true,
      effect: {
        advanceConstruction: {
          amount: 1,
          attachedBase: true
        }
      }
    }
  },

  effect: {},

  text:
    "Attach only to an Expansion Base. Remove 1 Construction immediately. Once per turn, remove 1 additional Construction from this base.",

  flavor:
    "The final tower is already standing. The workers are merely persuading history to catch up."
},

{
  id: "market_refraction_spire",
  name: "Market Refraction Spire",
  image: "market_refraction_spire.png",
  faction: "yellow",
  cost: 4,
  shop_cost: 60,
  type: "attachment",
  sigil: "◈",

  attachment: {
    action: {
      label: "Refract Market",
      oncePerTurn: true,
      effect: {
        scrapMarket: 2
      }
    }
  },

  effect: {},

  text:
    "Attach to a base. Once per turn, gain 2 Market Erase.",

  flavor:
    "Goods disappear from the market before merchants remember offering them."
},

{
  id: "dimensional_anchor_grid",
  name: "Dimensional Anchor Grid",
  image: "dimensional_anchor_grid.png",
  faction: "yellow",
  cost: 5,
  shop_cost: 60,
  type: "attachment",
  sigil: "◈",

  attachment: {
    defense: 2,
    expansionHealth: 7
  },

  effect: {},

  text:
    "Attach to a base. A normal base gains +2 Defense; an Expansion Base gains +7 maximum and current Health.",

  flavor:
    "Destroying the fortress becomes difficult once reality itself has agreed that it belongs there."
},

{
  id: "paradox_relay",
  name: "Paradox Relay",
  image: "paradox_relay.png",
  faction: "yellow",
  cost: 3,
  shop_cost: 110,
  type: "attachment",
  sigil: "◈",

  attachment: {
    expansionOnly: true,

    action: {
      label: "Borrow Tomorrow",
      oncePerTurn: true,
      effect: {
        draw: 1,
        selfDamage: 1
      }
    }
  },

  effect: {},

  text:
    "Attach only to an Expansion Base. Once per turn, take 1 Authority damage to draw 1 card.",

  flavor:
    "The debt is always collected yesterday."
},

{
  id: "horizon_command_node",
  name: "Horizon Command Node",
  image: "horizon_command_node.png",
  faction: "yellow",
  cost: 7,
  shop_cost: 125,
  type: "attachment",
  sigil: "◈",

  attachment: {
    expansionOnly: true,

    startOfTurn: {
      trade: 2,
      createToken: {
        id: "drone",
        count: 2,
        zone: "discard"
      }
    }
  },

  effect: {},

  text:
    "Attach only to an Expansion Base. At the start of your turn, gain 2 Trade and create 2 Drones in your discard pile.",

  flavor:
    "A single thought here becomes an instruction everywhere."
},


// ============================================================================
// BLUE — AZURE ASCENDANCY ATTACHMENTS
// Defense / recovery / authority / protection
// ============================================================================

{
  id: "sanctified_bulkhead",
  name: "Sanctified Bulkhead",
  image: "sanctified_bulkhead.png",
  faction: "blue",
  cost: 6,
  shop_cost: 35,
  type: "attachment",
  sigil: "✦",

  attachment: {
    defense: 3,
    expansionHealth: 8
  },

  effect: {},

  text:
    "Attach to a base. A normal base gains +3 Defense; an Expansion Base gains +8 maximum and current Health.",

  flavor:
    "Every plate bears the name of someone who survived behind another."
},

{
  id: "restoration_chapel",
  name: "Restoration Chapel",
  image: "restoration_chapel.png",
  faction: "blue",
  cost: 3,
  shop_cost: 60,
  type: "attachment",
  sigil: "✦",

  attachment: {
    expansionOnly: true,

    startOfTurn: {
      repair: {
        amount: 5,
        attachedBase: true
      }
    }
  },

  effect: {},

  text:
    "Attach only to an Expansion Base. At the start of your turn, repair this base for 5.",

  flavor:
    "Stone is taught the same doctrine as soldiers: rise again."
},

{
  id: "interceptor_launch_rail",
  name: "Interceptor Launch Rail",
  image: "interceptor_launch_rail.png",
  faction: "blue",
  cost: 5,
  shop_cost: 90,
  type: "attachment",
  sigil: "✦",

  attachment: {
    everyTurns: 2,
    recurring: {
      createToken: {
        id: "interceptor",
        count: 2,
        zone: "hand"
      }
    }
  },

  effect: {},

  text:
    "Attach to a base. Every second turn, create 2 Interceptors in hand.",

  flavor:
    "Pilots launch from the cathedral before the warning bell finishes ringing."
},

{
  id: "pilgrims_aegis",
  name: "Pilgrim's Aegis",
  image: "pilgrims_aegis.png",
  faction: "blue",
  cost: 4,
  shop_cost: 60,
  type: "attachment",
  sigil: "✦",

  attachment: {
    startOfTurn: {
      shield: 4
    }
  },

  effect: {},

  text:
    "Attach to a base. At the start of your turn, gain 4 Shield.",

  flavor:
    "Those who shelter beneath it claim the light becomes physically heavier."
},

{
  id: "reliquary_repair_arm",
  name: "Reliquary Repair Arm",
  image: "reliquary_repair_arm.png",
  faction: "blue",
  cost: 5,
  shop_cost: 90,
  type: "attachment",
  sigil: "✦",

  attachment: {
    action: {
      label: "Restore Structure",
      oncePerTurn: true,
      effect: {
        repair: {
          amount: 7
        }
      }
    }
  },

  effect: {},

  text:
    "Attach to a base. Once per turn, repair an Expansion Base for 7.",

  flavor:
    "It treats shattered masonry as a wound rather than a ruin."
},

{
  id: "consecrated_shipyard",
  name: "Consecrated Shipyard",
  image: "consecrated_shipyard.png",
  faction: "blue",
  cost: 2,
  shop_cost: 110,
  type: "attachment",
  sigil: "✦",

  attachment: {
    expansionOnly: true,

    everyTurns: 3,

    recurring: {
      createToken: {
        id: "interceptor",
        count: 1,
        zone: "hand"
      },
      heal: 2
    }
  },

  effect: {},

  text:
    "Attach only to an Expansion Base. Every third turn, gain 2 Authority and create an Interceptor in hand.",

  flavor:
    "Every vessel leaves the yard already blessed for a battle nobody has named."
},

{
  id: "oathbound_armor_matrix",
  name: "Oathbound Armor Matrix",
  image: "oathbound_armor_matrix.png",
  faction: "blue",
  cost: 4,
  shop_cost: 110,
  type: "attachment",
  sigil: "✦",

  attachment: {
    action: {
      label: "Invoke the Oath",
      oncePerTurn: true,
      effect: {
        armor: {
          amount: 2,
          all: true
        }
      }
    }
  },

  effect: {},

  text:
    "Attach to a base. Once per turn, give all your bases 2 temporary Armor.",

  flavor:
    "One fortress speaks the vow. Every wall answers."
},

{
  id: "last_light_beacon",
  name: "Last-Light Beacon",
  image: "last_light_beacon.png",
  faction: "blue",
  cost: 3,
  shop_cost: 125,
  type: "attachment",
  sigil: "✦",

  attachment: {
    expansionOnly: true,

    startOfTurn: {
      heal: 3,
      shield: 2
    }
  },

  effect: {},

  text:
    "Attach only to an Expansion Base. At the start of your turn, gain 2 Authority and 2 Shield.",

  flavor:
    "If even one tower remains lit, the Ascendancy considers the realm unconquered."
},


// ============================================================================
// GREEN — GORAK WARHOST ATTACHMENTS
// Construction / Workers / brute-force infrastructure
// ============================================================================

{
  id: "warhost_scaffolding",
  name: "Warhost Scaffolding",
  image: "warhost_scaffolding.png",
  faction: "green",
  cost: 1,
  shop_cost: 25,
  type: "attachment",
  sigil: "⬢",

  attachment: {
    expansionOnly: true,

    onAttach: {
      advanceConstruction: {
        amount: 1,
        attachedBase: true
      }
    }
  },

  effect: {},

  text:
    "Attach only to an Expansion Base. Immediately remove 1 Construction from it.",

  flavor:
    "Anything can become scaffolding if enough orcs are willing to stand on it."
},

{
  id: "worker_barracks",
  name: "Worker Barracks",
  image: "worker_barracks.png",
  faction: "green",
  cost: 4,
  shop_cost: 60,
  type: "attachment",
  sigil: "⬢",

  attachment: {
    everyTurns: 2,

    recurring: {
      createToken: {
        id: "worker",
        count: 2,
        zone: "hand"
      }
    }
  },

  effect: {},

  text:
    "Attach to a base. Every second turn, create 2 Workers in hand.",

  flavor:
    "The beds are never empty because the shift never truly ends."
},

{
  id: "worldroot_crane",
  name: "Worldroot Crane",
  image: "worldroot_crane.png",
  faction: "green",
  cost: 3,
  shop_cost: 90,
  type: "attachment",
  sigil: "⬢",

  attachment: {
    expansionOnly: true,

    onAttach: {
      advanceConstruction: {
        amount: 1,
        attachedBase: true
      }
    },

    everyTurns: 2,

    recurring: {
      advanceConstruction: {
        amount: 1,
        attachedBase: true
      },
      repairFallback: 3
    }
  },

  effect: {},

  text:
    "Attach only to an Expansion Base. Remove 1 Construction immediately and every second turn. Once complete, repair it for 3 instead.",

  flavor:
    "Its roots pull stone upward faster than gravity can object."
},

{
  id: "siege_foundry",
  name: "Siege Foundry",
  image: "siege_foundry.png",
  faction: "green",
  cost: 5,
  shop_cost: 90,
  type: "attachment",
  sigil: "⬢",

  attachment: {
    action: {
      label: "Forge Siegeworks",
      oncePerTurn: true,
      effect: {
        combat: 6
      }
    }
  },

  effect: {},

  text:
    "Attach to a base. Once per turn, gain 6 Combat.",

  flavor:
    "A fortress is only a weapon that has decided not to move."
},

{
  id: "industrial_overgrowth",
  name: "Industrial Overgrowth",
  image: "industrial_overgrowth.png",
  faction: "green",
  cost: 4,
  shop_cost: 60,
  type: "attachment",
  sigil: "⬢",

  attachment: {
    expansionHealth: 8,
    defense: 2
  },

  effect: {},

  text:
    "Attach to a base. A normal base gains +2 Defense; an Expansion Base gains +8 maximum and current Health.",

  flavor:
    "The roots grow around the steel until neither remembers which one was reinforcement."
},

{
  id: "labor_command_deck",
  name: "Labor Command Deck",
  image: "labor_command_deck.png",
  faction: "green",
  cost: 4,
  shop_cost: 110,
  type: "attachment",
  sigil: "⬢",

  attachment: {
    expansionOnly: true,

    startOfTurn: {
      createToken: {
        id: "worker",
        count: 1,
        zone: "discard"
      },
      drawFromDrawPile: {
        id: "worker",
        count: 1,
        look: 5
      }
    }
  },

  effect: {},

  text:
    "Attach only to an Expansion Base. At the start of your turn, create a Worker in discard, then draw a Worker from the top 5 cards of your draw pile if one is there.",

  flavor:
    "Every unfinished wall is entered into the ledger as an emergency."
},

{
  id: "colossal_lifting_rig",
  name: "Colossal Lifting Rig",
  image: "colossal_lifting_rig.png",
  faction: "green",
  cost: 5,
  shop_cost: 125,
  type: "attachment",
  sigil: "⬢",

  attachment: {
    expansionOnly: true,

    onAttach: {
      advanceConstruction: {
        amount: 2,
        attachedBase: true
      }
    }
  },

  effect: {},

  text:
    "Attach only to an Expansion Base. Immediately remove 2 Construction.",

  flavor:
    "Mountains become building materials when the crane is taller than the mountain."
},


// ============================================================================
// RED — UMBRAL COVENANT ATTACHMENTS
// Sacrifice / dangerous engines / purge / offensive infrastructure
// ============================================================================

{
  id: "blood_furnace",
  name: "Blood Furnace",
  image: "blood_furnace.png",
  faction: "red",
  cost: 3,
  shop_cost: 35,
  type: "attachment",
  sigil: "◒",

  attachment: {
    action: {
      label: "Feed the Furnace",
      oncePerTurn: true,
      effect: {
        selfDamage: 1,
        combat: 5
      }
    }
  },

  effect: {},

  text:
    "Attach to a base. Once per turn, take 1 Authority damage to gain 5 Combat.",

  flavor:
    "It accepts every fuel except mercy."
},

{
  id: "emberling_incubator",
  name: "Emberling Incubator",
  image: "emberling_incubator.png",
  faction: "red",
  cost: 4,
  shop_cost: 60,
  type: "attachment",
  sigil: "◒",

  attachment: {
    everyTurns: 2,

    recurring: {
      createToken: {
        id: "emberling",
        count: 2,
        zone: "hand"
      }
    }
  },

  effect: {},

  text:
    "Attach to a base. Every second turn, create 2 Emberlings in hand.",

  flavor:
    "The chambers are designed to open from the inside."
},

{
  id: "grave_reclamation_hook",
  name: "Grave Reclamation Hook",
  image: "grave_reclamation_hook.png",
  faction: "red",
  cost: 5,
  shop_cost: 90,
  type: "attachment",
  sigil: "◒",

  attachment: {
    action: {
      label: "Reclaim the Dead",
      oncePerTurn: true,
      effect: {
        reclaim: {
          types: ["attachment"]
        }
      }
    }
  },

  effect: {},

  text:
    "Attach to a base. Once per turn, Reclaim an Attachment from your discard pile.",

  flavor:
    "Nothing installed in the Covenant is permitted the luxury of remaining destroyed."
},

{
  id: "soul_driven_crane",
  name: "Soul-Driven Crane",
  image: "soul_driven_crane.png",
  faction: "red",
  cost: w,
  shop_cost: 60,
  type: "attachment",
  sigil: "◒",

  attachment: {
    expansionOnly: true,

    action: {
      label: "Overdrive Construction",
      oncePerTurn: true,
      effect: {
        selfDamage: 1,
        advanceConstruction: {
          amount: 1,
          attachedBase: true
        }
      }
    }
  },

  effect: {},

  text:
    "Attach only to an Expansion Base. Once per turn, take 1 Authority damage to remove 1 Construction from this base.",

  flavor:
    "The dead do not tire. The architects call this efficiency."
},

{
  id: "annihilation_turret",
  name: "Annihilation Turret",
  image: "annihilation_turret.png",
  faction: "red",
  cost: 6,
  shop_cost: 110,
  type: "attachment",
  sigil: "◒",

  attachment: {
    action: {
      label: "Fire Annihilation Turret",
      oncePerTurn: true,
      effect: {
        combat: 9,
        selfDamage: 2
      }
    }
  },

  effect: {},

  text:
    "Attach to a base. Once per turn, take 2 Authority damage to gain 9 Combat.",

  flavor:
    "There is no recoil absorber. The architects considered pain cheaper."
},

{
  id: "covenant_scrap_vault",
  name: "Covenant Scrap Vault",
  image: "covenant_scrap_vault.png",
  faction: "red",
  cost: 5,
  shop_cost: 90,
  type: "attachment",
  sigil: "◒",

  attachment: {
    action: {
      label: "Open the Scrap Vault",
      oncePerTurn: true,
      effect: {
        scrapOwn: 1,
        trade: 2
      }
    }
  },

  effect: {},

  text:
    "Attach to a base. Once per turn, gain 1 Purge and 2 Trade.",

  flavor:
    "Every obsolete thing becomes either currency or kindling."
},

{
  id: "black_sun_reactor",
  name: "Black Sun Reactor",
  image: "black_sun_reactor.png",
  faction: "red",
  cost: 5,
  shop_cost: 125,
  type: "attachment",
  sigil: "◒",

  attachment: {
    expansionOnly: true,

    startOfTurn: {
      combat: 6,
      selfDamage: 1
    }
  },

  effect: {},

  text:
    "Attach only to an Expansion Base. At the start of your turn, gain 6 Combat and take 1 Authority damage.",

  flavor:
    "It powers a city with the energy produced by slowly deleting a star."
},


// ============================================================================
// EXPANSION / CONSTRUCTION BASES — 15
// ============================================================================


// ============================================================================
// YELLOW EXPANSION BASES
// ============================================================================

{
  id: "chronosphere_assembly",
  name: "Chronosphere Assembly",
  image: "chronosphere_assembly.png",
  faction: "yellow",
  cost: 7,
  shop_cost: 125,
  type: "base",

  expansion: true,
  health: 18,
  defense: 18,
  construction: 3,
  attachmentSlots: 2,

  outpost: false,
  sigil: "◈",

  constructionEffect: {
    trade: 1
  },

  effect: {
    trade: 2,
    stun: 1
  },

  text:
    "Expansion Base — 18 Health, Construction 3, 2 Attachment slots. While building, gain 1 Trade each turn. When complete, gain 2 Trade and 1 Disable each turn.",

  flavor:
    "The construction schedule is measured in futures rather than days."
},

{
  id: "many_mind_megacomplex",
  name: "Many-Mind Megacomplex",
  image: "many_mind_megacomplex.png",
  faction: "yellow",
  cost: 9,
  shop_cost: 175,
  type: "base",

  expansion: true,
  health: 24,
  defense: 24,
  construction: 4,
  attachmentSlots: 3,

  outpost: false,
  sigil: "◈",

  constructionEffect: {
    peekTop: 1
  },

  effect: {
    trade: 2
  },

  charge: {
    trigger: "friendlyFactionPlayed",
    faction: "yellow",
    gain: 1,
    max: 8,

    actions: [
      {
        label: "Shared Calculation",
        cost: 3,
        effect: {
          trade: 2
        }
      },
      {
        label: "Consensus Future",
        cost: 5,
        effect: {
          draw: 1
        }
      },
      {
        label: "Total Convergence",
        cost: 8,
        effect: {
          draw: 1,
          stun: 2
        }
      }
    ]
  },

  text:
    "Expansion Base — 24 Health, Construction 4, 3 Attachment slots. While building, inspect your top card each turn. When complete, gain 2 Trade and Charge from Yellow cards.",

  chargeText:
    "Spend 3: gain 2 Trade. Spend 5: draw 1. Spend 8: draw 1 and gain 2 Disable.",

  flavor:
    "Millions of minds occupy one structure and disagree only about which enemy should cease existing first."
},

{
  id: "reality_lattice_citadel",
  name: "Reality-Lattice Citadel",
  image: "reality_lattice_citadel.png",
  faction: "yellow",
  cost: 10,
  shop_cost: 200,
  type: "base",

  expansion: true,
  health: 30,
  defense: 30,
  construction: 5,
  attachmentSlots: 4,

  outpost: true,
  sigil: "◈",

  constructionEffect: {
    shield: 1
  },

  effect: {
    trade: 2,
    shield: 2
  },

  recurring: {
    everyTurns: 3,
    effect: {
      opponentDiscard: 1
    }
  },

  text:
    "Expansion Outpost — 30 Health, Construction 5, 4 Attachment slots. While building, gain 1 Shield each turn. When complete, gain 2 Trade and 2 Shield each turn. Every third turn, the next enemy draws 1 fewer card.",

  flavor:
    "The fortress does not occupy territory. Territory is instructed to occupy the fortress."
},

{
  id: "infinite_replication_yard",
  name: "Infinite Replication Yard",
  image: "infinite_replication_yard.png",
  faction: "yellow",
  cost: 8,
  shop_cost: 150,
  type: "base",

  expansion: true,
  health: 20,
  defense: 20,
  construction: 4,
  attachmentSlots: 3,

  outpost: false,
  sigil: "◈",

  constructionEffect: {
    trade: 1
  },

  effect: {
    createToken: {
      id: "drone",
      count: 1,
      zone: "discard"
    },
    drawFromDrawPile: {
      id: "drone",
      count: 1,
      look: 5
    }
  },

  text:
    "Expansion Base — 20 Health, Construction 4, 3 Attachment slots. While building, gain 1 Trade. When complete, create a Drone in discard each turn, then draw a Drone from your top 5 if one is there.",

  flavor:
    "Production ceased being a process when the factory learned to manufacture factories."
},


// ============================================================================
// BLUE EXPANSION BASES
// ============================================================================

{
  id: "bastion_of_seven_vows",
  name: "Bastion of Seven Vows",
  image: "bastion_of_seven_vows.png",
  faction: "blue",
  cost: 7,
  shop_cost: 125,
  type: "base",

  expansion: true,
  health: 24,
  defense: 24,
  construction: 3,
  attachmentSlots: 2,

  outpost: true,
  sigil: "✦",

  constructionEffect: {
    shield: 1
  },

  effect: {
    shield: 3
  },

  text:
    "Expansion Outpost — 24 Health, Construction 3, 2 Attachment slots. While building, gain 1 Shield each turn. When complete, gain 3 Shield each turn.",

  flavor:
    "Seven vows hold the gates. Stone is merely there to make them visible."
},

{
  id: "grand_recovery_basilica",
  name: "Grand Recovery Basilica",
  image: "grand_recovery_basilica.png",
  faction: "blue",
  cost: 8,
  shop_cost: 150,
  type: "base",

  expansion: true,
  health: 26,
  defense: 26,
  construction: 4,
  attachmentSlots: 3,

  outpost: false,
  sigil: "✦",

  constructionEffect: {
    heal: 1
  },

  effect: {
    heal: 2,
    repair: {
      amount: 2
    }
  },

  text:
    "Expansion Base — 26 Health, Construction 4, 3 Attachment slots. While building, gain 1 Authority each turn. When complete, gain 2 Authority and repair an Expansion Base for 2 each turn.",

  flavor:
    "Every ruined vessel returns here as evidence that restoration is stronger than destruction."
},

{
  id: "skywall_fortress_monastery",
  name: "Skywall Fortress-Monastery",
  image: "skywall_fortress_monastery.png",
  faction: "blue",
  cost: 10,
  shop_cost: 200,
  type: "base",

  expansion: true,
  health: 32,
  defense: 32,
  construction: 5,
  attachmentSlots: 4,

  outpost: true,
  sigil: "✦",

  constructionEffect: {
    shield: 2
  },

  effect: {
    armor: {
      amount: 1,
      all: true
    }
  },

  recurring: {
    everyTurns: 2,
    effect: {
      createToken: {
        id: "interceptor",
        count: 1,
        zone: "discard"
      }
    }
  },

  text:
    "Expansion Outpost — 32 Health, Construction 5, 4 Attachment slots. While building, gain 2 Shield. When complete, give all bases 1 Armor each turn and create an Interceptor in discard every second turn.",

  flavor:
    "The monastery is large enough that entire squadrons take vows without ever touching the ground."
},

{
  id: "sanctuary_of_returning_wings",
  name: "Sanctuary of Returning Wings",
  image: "sanctuary_of_returning_wings.png",
  faction: "blue",
  cost: 8,
  shop_cost: 150,
  type: "base",

  expansion: true,
  health: 22,
  defense: 22,
  construction: 4,
  attachmentSlots: 2,

  outpost: false,
  sigil: "✦",

  constructionEffect: {
    heal: 1
  },

  effect: {
    reclaim: {
      ids: ["interceptor", "acolyte"]
    }
  },

  text:
    "Expansion Base — 22 Health, Construction 4, 2 Attachment slots. While building, gain 1 Authority. When complete, Reclaim an Interceptor or Acolyte each turn.",

  flavor:
    "Every road home ends beneath its towers."
},


// ============================================================================
// GREEN EXPANSION BASES
// ============================================================================

{
  id: "ironroot_construction_camp",
  name: "Ironroot Construction Camp",
  image: "ironroot_construction_camp.png",
  faction: "green",
  cost: 6,
  shop_cost: 110,
  type: "base",

  expansion: true,
  health: 20,
  defense: 20,
  construction: 3,
  attachmentSlots: 2,

  outpost: false,
  sigil: "⬢",

  constructionEffect: {
    createToken: {
      id: "worker",
      count: 1,
      zone: "discard"
    }
  },

  effect: {
    trade: 2
  },

  text:
    "Expansion Base — 20 Health, Construction 3, 2 Attachment slots. While building, create a Worker in discard each turn. When complete, gain 2 Trade each turn.",

  flavor:
    "The workers built the camp, then the camp began building the workers."
},

{
  id: "continental_siegeworks",
  name: "Continental Siegeworks",
  image: "continental_siegeworks.png",
  faction: "green",
  cost: 9,
  shop_cost: 175,
  type: "base",

  expansion: true,
  health: 28,
  defense: 28,
  construction: 4,
  attachmentSlots: 3,

  outpost: false,
  sigil: "⬢",

  constructionEffect: {
    combat: 1
  },

  effect: {
    combat: 5
  },

  charge: {
    trigger: "cardSacrificed",
    sacrificedId: "worker",
    gain: 1,
    max: 6,

    actions: [
      {
        label: "Emergency Labor",
        cost: 2,
        effect: {
          advanceConstruction: {
            amount: 1
          }
        }
      },
      {
        label: "Roll Out the Siege Engines",
        cost: 6,
        effect: {
          combat: 8
        }
      }
    ]
  },

  text:
    "Expansion Base — 28 Health, Construction 4, 3 Attachment slots. While building, gain 1 Combat. When complete, gain 5 Combat. Sacrificed Workers add Charge.",

  chargeText:
    "Spend 2: remove 1 Construction. Spend 6: gain 8 Combat.",

  flavor:
    "The Warhost once built siege engines. Eventually it simply built the siege."
},

{
  id: "worldroot_industrial_colossus",
  name: "Worldroot Industrial Colossus",
  image: "worldroot_industrial_colossus.png",
  faction: "green",
  cost: 10,
  shop_cost: 200,
  type: "base",

  expansion: true,
  health: 35,
  defense: 35,
  construction: 6,
  attachmentSlots: 4,

  outpost: true,
  sigil: "⬢",

  constructionEffect: {
    trade: 1
  },

  effect: {
    trade: 3,
    createToken: {
      id: "worker",
      count: 1,
      zone: "hand"
    }
  },

  recurring: {
    everyTurns: 3,
    effect: {
      combat: 6
    }
  },

  text:
    "Expansion Outpost — 35 Health, Construction 6, 4 Attachment slots. While building, gain 1 Trade. When complete, gain 3 Trade and create a Worker in hand each turn. Every third turn, gain 6 Combat.",

  flavor:
    "It is factory, fortress, quarry, barracks, and eventually the horizon."
},

{
  id: "gorge_engineering_hive",
  name: "Gorge Engineering Hive",
  image: "gorge_engineering_hive.png",
  faction: "green",
  cost: 8,
  shop_cost: 150,
  type: "base",

  expansion: true,
  health: 25,
  defense: 25,
  construction: 4,
  attachmentSlots: 3,

  outpost: false,
  sigil: "⬢",

  constructionEffect: {
    createToken: {
      id: "spawn",
      count: 1,
      zone: "discard"
    }
  },

  effect: {
    createToken: {
      id: "worker",
      count: 1,
      zone: "discard"
    }
  },

  sacrificeThresholds: [
    {
      at: 1,
      requiresSacrificedId: "spawn",
      effect: {
        combat: 2
      }
    },
    {
      at: 2,
      requiresSacrificedId: "worker",
      effect: {
        trade: 2
      }
    }
  ],

  text:
    "Expansion Base — 25 Health, Construction 4, 3 Attachment slots. While building, create a Spawn in discard. When complete, create a Worker in discard. Spawn and Worker sacrifices provide additional Combat and Trade.",

  flavor:
    "Half construction site, half breeding pit, entirely unacceptable to neighboring realms."
},


// ============================================================================
// RED EXPANSION BASES
// ============================================================================

{
  id: "ash_foundation_temple",
  name: "Ash-Foundation Temple",
  image: "ash_foundation_temple.png",
  faction: "red",
  cost: 7,
  shop_cost: 125,
  type: "base",

  expansion: true,
  health: 19,
  defense: 19,
  construction: 3,
  attachmentSlots: 2,

  outpost: false,
  sigil: "◒",

  constructionEffect: {
    combat: 1
  },

  effect: {
    combat: 3
  },

  text:
    "Expansion Base — 19 Health, Construction 3, 2 Attachment slots. While building, gain 1 Combat each turn. When complete, gain 3 Combat each turn.",

  flavor:
    "Its cornerstone was once a throne. Nobody remembers whose."
},

{
  id: "necropolis_engineworks",
  name: "Necropolis Engineworks",
  image: "necropolis_engineworks.png",
  faction: "red",
  cost: 9,
  shop_cost: 175,
  type: "base",

  expansion: true,
  health: 25,
  defense: 25,
  construction: 4,
  attachmentSlots: 3,

  outpost: false,
  sigil: "◒",

  constructionEffect: {
    selfDamage: 1,
    advanceConstruction: {
      amount: 1
    }
  },

  effect: {
    combat: 4
  },

  sacrificeThresholds: [
    {
      at: 1,
      effect: {
        trade: 2
      }
    },
    {
      at: 2,
      effect: {
        combat: 3
      }
    },
    {
      at: 3,
      effect: {
        draw: 1
      }
    }
  ],

  text:
    "Expansion Base — 25 Health, Construction 4, 3 Attachment slots. While building, take 1 Authority damage and remove 1 additional Construction each turn. When complete, gain 4 Combat and reward repeated sacrifices.",

  flavor:
    "Its construction crews are listed in the payroll under Previous Casualties."
},

{
  id: "black_star_weapon_foundry",
  name: "Black-Star Weapon Foundry",
  image: "black_star_weapon_foundry.png",
  faction: "red",
  cost: 10,
  shop_cost: 200,
  type: "base",

  expansion: true,
  health: 28,
  defense: 28,
  construction: 5,
  attachmentSlots: 4,

  outpost: true,
  sigil: "◒",

  constructionEffect: {
    combat: 1,
    selfDamage: 1
  },

  effect: {
    combat: 6,
    selfDamage: 1
  },

  charge: {
    trigger: "cardSacrificed",
    gain: 1,
    max: 8,

    actions: [
      {
        label: "Feed the Reactor",
        cost: 3,
        effect: {
          combat: 4
        }
      },
      {
        label: "Black-Star Salvo",
        cost: 8,
        effect: {
          combat: 10,
          destroyBase: 1
        }
      }
    ]
  },

  text:
    "Expansion Outpost — 28 Health, Construction 5, 4 Attachment slots. While building, gain 1 Combat and take 1 damage. When complete, gain 6 Combat and take 1 damage each turn. Sacrifices add Charge.",

  chargeText:
    "Spend 3: gain 4 Combat. Spend 8: gain 10 Combat and Raze 1.",

  flavor:
    "The final weapon is not stored inside the foundry. The foundry is the weapon."
},


// ============================================================================
// 10 CONSTRUCTION-RELATED CARDS
// Various factions
// ============================================================================


// ============================================================================
// YELLOW CONSTRUCTION SUPPORT
// ============================================================================

{
  id: "temporal_site_engineer",
  name: "Temporal Site Engineer",
  image: "temporal_site_engineer.png",
  faction: "yellow",
  cost: 3,
  shop_cost: 35,
  type: "ship",
  sigil: "◈",

  effect: {
    or: [
      {
        label: "Accelerate",
        effect: {
          advanceConstruction: {
            amount: 1
          }
        }
      },
      {
        label: "Fund the Project",
        effect: {
          trade: 2
        }
      }
    ]
  },

  ally: {
    stun: 1
  },

  text:
    "Choose one: remove 1 Construction from an Expansion Base; or gain 2 Trade.",

  allyText:
    "Gain 1 Disable.",

  flavor:
    "A delayed project is simply moved into a timeline where it was already finished."
},

{
  id: "architect_of_impossible_angles",
  name: "Architect of Impossible Angles",
  image: "architect_of_impossible_angles.png",
  faction: "yellow",
  cost: 5,
  shop_cost: 90,
  type: "ship",
  sigil: "◈",

  effect: {
    advanceConstruction: {
      amount: 1
    },
    trade: 2
  },

  ally: {
    draw: 1
  },

  text:
    "Gain 2 Trade and remove 1 Construction from an Expansion Base.",

  allyText:
    "Draw 1 card.",

  flavor:
    "The shortest distance between blueprint and fortress is rarely a straight line."
},


// ============================================================================
// BLUE CONSTRUCTION SUPPORT
// ============================================================================

{
  id: "sanctuary_mason",
  name: "Sanctuary Mason",
  image: "sanctuary_mason.png",
  faction: "blue",
  cost: 3,
  shop_cost: 35,
  type: "ship",
  sigil: "✦",

  effect: {
    or: [
      {
        label: "Build",
        effect: {
          advanceConstruction: {
            amount: 1
          }
        }
      },
      {
        label: "Restore",
        effect: {
          repair: {
            amount: 3
          }
        }
      }
    ]
  },

  ally: {
    shield: 2
  },

  text:
    "Choose one: remove 1 Construction; or repair an Expansion Base for 3.",

  allyText:
    "Gain 2 Shield.",

  flavor:
    "The same hands raise new walls and mend the old ones."
},

{
  id: "basilica_logistics_convoy",
  name: "Basilica Logistics Convoy",
  image: "basilica_logistics_convoy.png",
  faction: "blue",
  cost: 5,
  shop_cost: 90,
  type: "ship",
  sigil: "✦",

  effect: {
    trade: 2,
    heal: 2,
    advanceConstruction: {
      amount: 1
    }
  },

  ally: {
    armor: {
      amount: 2
    }
  },

  text:
    "Gain 2 Trade and 2 Authority, then remove 1 Construction from an Expansion Base.",

  allyText:
    "Give a base 2 temporary Armor.",

  flavor:
    "Stone, medicine, ammunition, prayer—the convoy considers all four building materials."
},

{
  id: "fortress_vow_master",
  name: "Fortress Vow-Master",
  image: "fortress_vow_master.png",
  faction: "blue",
  cost: 6,
  shop_cost: 110,
  type: "ship",
  sigil: "✦",

  effect: {
    shield: 3,
    repair: {
      amount: 3
    }
  },

  doubleAlly: {
    advanceConstruction: {
      amount: 1
    }
  },

  text:
    "Gain 3 Shield and repair an Expansion Base for 3.",

  doubleAllyText:
    "Remove 1 Construction from an Expansion Base.",

  flavor:
    "A fortress survives first in the convictions of those rebuilding it."
},


// ============================================================================
// GREEN CONSTRUCTION SUPPORT
// ============================================================================

{
  id: "mega_project_foreman",
  name: "Mega-Project Foreman",
  image: "mega_project_foreman.png",
  faction: "green",
  cost: 4,
  shop_cost: 60,
  type: "ship",
  sigil: "⬢",

  effect: {
    trade: 2,
    createToken: {
      id: "worker",
      count: 1,
      zone: "discard"
    }
  },

  ally: {
    advanceConstruction: {
      amount: 1
    }
  },

  text:
    "Gain 2 Trade and create a Worker in your discard pile.",

  allyText:
    "Remove 1 Construction from an Expansion Base.",

  flavor:
    "His schedules contain only two categories: finished and about to be finished."
},

{
  id: "worldroot_architect",
  name: "Worldroot Architect",
  image: "worldroot_architect.png",
  faction: "green",
  cost: 6,
  shop_cost: 110,
  type: "ship",
  sigil: "⬢",

  effect: {
    advanceConstruction: {
      amount: 2
    }
  },

  ally: {
    trade: 2
  },

  sacrifice: {
    repair: {
      amount: 5
    }
  },

  text:
    "Remove 2 Construction from an Expansion Base.",

  allyText:
    "Gain 2 Trade.",

  sacrificeText:
    "Sacrifice: Repair an Expansion Base for 5.",

  flavor:
    "He does not design buildings. He teaches landscapes what shape they will become."
},

{
  id: "mobile_megaforge",
  name: "Mobile Megaforge",
  image: "mobile_megaforge.png",
  faction: "green",
  cost: 7,
  shop_cost: 125,
  type: "ship",
  sigil: "⬢",

  effect: {
    combat: 3,

    or: [
      {
        label: "Raise the Walls",
        effect: {
          advanceConstruction: {
            amount: 1
          }
        }
      },
      {
        label: "Repair the Works",
        effect: {
          repair: {
            amount: 4
          }
        }
      },
      {
        label: "Produce Labor",
        effect: {
          createToken: {
            id: "worker",
            count: 1,
            zone: "hand"
          }
        }
      }
    ]
  },

  doubleAlly: {
    combat: 4
  },

  text:
    "Gain 3 Combat. Choose one: remove 1 Construction; repair an Expansion Base for 4; or create a Worker in hand.",

  doubleAllyText:
    "Gain 4 additional Combat.",

  flavor:
    "The Warhost solved the problem of transporting factories by giving the factories engines."
},


// ============================================================================
// RED CONSTRUCTION SUPPORT
// ============================================================================

{
  id: "graveyard_contractors",
  name: "Graveyard Contractors",
  image: "graveyard_contractors.png",
  faction: "red",
  cost: 4,
  shop_cost: 60,
  type: "ship",
  sigil: "◒",

  effect: {
    combat: 2,

    or: [
      {
        label: "Paid in Blood",
        effect: {
          selfDamage: 1,
          advanceConstruction: {
            amount: 1
          }
        }
      },
      {
        label: "Paid in Coin",
        effect: {
          trade: 2
        }
      }
    ]
  },

  text:
    "Gain 2 Combat. Choose one: take 1 Authority damage to remove 1 Construction; or gain 2 Trade.",

  flavor:
    "The Covenant discovered that the dead accept extremely competitive labor contracts."
},

{
  id: "architect_of_ruin",
  name: "Architect of Ruin",
  image: "architect_of_ruin.png",
  faction: "red",
  cost: 6,
  shop_cost: 110,
  type: "ship",
  sigil: "◒",

  effect: {
    combat: 4,
    selfDamage: 1,
    advanceConstruction: {
      amount: 1
    }
  },

  ally: {
    scrapOwn: 1
  },

  sacrifice: {
    advanceConstruction: {
      amount: 2
    }
  },

  text:
    "Gain 4 Combat, take 1 Authority damage, and remove 1 Construction from an Expansion Base.",

  allyText:
    "Gain 1 Purge.",

  sacrificeText:
    "Sacrifice: Remove 2 Construction from an Expansion Base.",

  flavor:
    "The fastest way to build the future is to burn everything that would have occupied its foundation."
},
  {
    id: "ember_nest",
    name: "Ember Nest",
    image: "ember_nest.png",
    faction: "red",
    cost: 4,
    shop_cost: 60,
    type: "base",
    defense: 7,
    outpost: false,
    sigil: "◒",
    effect: {},
    recurring: { everyTurns: 2, effect: { createToken: { id: "emberling", count: 1, zone: "discard" }, drawFromDrawPile: { id: "emberling", count: 1 } } },
    sacrificeThresholds: [{ at: 1, requiresSacrificedId: "emberling", effect: { combat: 1 } }],
    text: "Every second turn, create an Emberling in discard, then draw an Emberling from your draw pile if one is there. The first Emberling sacrificed each turn gives 1 additional Combat.",
    flavor: "The nest cools only when it is empty, and it is never empty long."
  },
  {
    id: "cinder_broodmother",
    name: "Cinder Broodmother",
    image: "cinder_broodmother.png",
    faction: "red",
    cost: 6,
    shop_cost: 110,
    type: "ship",
    sigil: "◒",
    effect: { createToken: { id: "emberling", count: 2, zone: "discard", handAtSacrifice: 2, handCount: 1 }, drawFromDrawPile: { id: "emberling", count: 1 } },
    ally: { combat: 3 },
    text: "Create two Emberlings in discard, then draw an Emberling from your draw pile if one is there. At Sacrifice 2, create one of the new Emberlings in hand instead.",
    allyText: "Gain 3 Combat.",
    flavor: "Its young inherit fire before they inherit shape."
  },

  // ----- FACTION DENSITY -----
  {
    id: "constellation_vanguard",
    name: "Constellation Vanguard",
    image: "constellation_vanguard.png",
    faction: "yellow",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "◈",
    effect: { trade: 1 },
    factionScaling: { metric: "played", faction: "yellow", per: 2, maxUnits: 3, effectPerUnit: { trade: 1 } },
    text: "Gain 1 Trade, plus 1 for every two Yellow cards played this turn, up to 3 additional Trade.",
    flavor: "It navigates by constellations made from allied hulls."
  },
  {
    id: "many_mind_navigator",
    name: "Many-Mind Navigator",
    image: "many_mind_navigator.png",
    faction: "yellow",
    cost: 6,
    shop_cost: 110,
    type: "ship",
    sigil: "◈",
    effect: { draw: 1 },
    factionThresholds: [
      { metric: "owned", faction: "yellow", at: 10, effect: { trade: 1 } },
      { metric: "owned", faction: "yellow", at: 15, effect: { draw: 1 } }
    ],
    text: "Draw 1. If you own 10 Yellow cards, gain 1 Trade; at 15 Yellow cards, draw 1 additional card.",
    flavor: "Each mind charts one route; together they choose the impossible one."
  },
  {
    id: "oathwing_formation",
    name: "Oathwing Formation",
    image: "oathwing_formation.png",
    faction: "blue",
    cost: 5,
    shop_cost: 90,
    type: "ship",
    sigil: "✦",
    effect: { shield: 2 },
    factionScaling: { metric: "playedBefore", faction: "blue", per: 1, maxUnits: 4, effectPerUnit: { shield: 1 } },
    text: "Gain 2 Shield, plus 1 for each Blue card played before this card this turn, up to 4 additional Shield.",
    flavor: "Each wing protects the vow flying beside it."
  },
  {
    id: "grand_reliquary_guard",
    name: "Grand Reliquary Guard",
    image: "grand_reliquary_guard.png",
    faction: "blue",
    cost: 7,
    shop_cost: 125,
    type: "ship",
    sigil: "✦",
    effect: { combat: 4, shield: 3 },
    factionThresholds: [
      { metric: "owned", faction: "blue", at: 12, effect: { armor: { amount: 3 } } },
      { metric: "owned", faction: "blue", at: 16, effect: { draw: 1 } }
    ],
    text: "Gain 4 Combat and 3 Shield. At 12 owned Blue cards give a base 3 Armor; at 16, draw a card.",
    flavor: "The Guard counts faith not in years, but in banners still standing."
  },
  {
    id: "warhost_stampede",
    name: "Warhost Stampede",
    image: "warhost_stampede.png",
    faction: "green",
    cost: 5,
    shop_cost: 90,
    type: "ship",
    sigil: "⬢",
    effect: {},
    factionScaling: { metric: "played", faction: "green", per: 1, maxUnits: 8, effectPerUnit: { combat: 1 } },
    factionThresholds: [{ metric: "played", faction: "green", at: 5, effect: { createToken: { id: "spawn", count: 1, zone: "discard" } } }],
    text: "Gain 1 Combat for each Green card played this turn. At five Green cards, create a Spawn in discard.",
    flavor: "The ground counts the Warhost before the enemy can."
  },
  {
    id: "horde_census",
    name: "Horde Census",
    image: "horde_census.png",
    faction: "green",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: false,
    sigil: "⬢",
    effect: {},
    factionScaling: { metric: "owned", faction: "green", per: 4, maxUnits: 6, effectPerUnit: { combat: 1 } },
    factionThresholds: [{ metric: "owned", faction: "green", at: 16, effect: { createToken: { id: "worker", count: 1, zone: "discard" } }, everyTurns: 2 }],
    text: "Gain 1 Combat for every four Green cards you own. At 16 Green cards, create a Worker every other turn.",
    flavor: "The Warhost calls it a census. Other realms call it an evacuation order."
  },
  {
    id: "covenant_escalator",
    name: "Covenant Escalator",
    image: "covenant_escalator.png",
    faction: "red",
    cost: 4,
    shop_cost: 60,
    type: "ship",
    sigil: "◒",
    effect: { combat: 2 },
    factionScaling: { metric: "playedAfterFirst", faction: "red", per: 1, maxUnits: 5, effectPerUnit: { combat: 1 } },
    factionThresholds: [{ metric: "played", faction: "red", at: 5, effect: { draw: 1, sacrificeRequired: 1 } }],
    text: "Gain 2 Combat, plus 1 for each Red card played this turn after the first. At five Red cards, you may sacrifice a card to draw.",
    flavor: "Every bargain makes the next demand sound reasonable."
  },
  {
    id: "growing_corruption",
    name: "Growing Corruption",
    image: "growing_corruption.png",
    faction: "red",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: false,
    sigil: "◒",
    effect: {},
    charge: {
      trigger: "cardAcquired",
      faction: "red",
      gain: 1,
      max: 8,
      actions: [{ label: "Spread Corruption", cost: 3, effect: { combat: 4 }, ownedFactionBonus: { faction: "red", at: 12, effect: { combat: 2 } } }]
    },
    text: "Whenever you acquire a Red card, gain 1 Charge.",
    chargeText: "Spend 3: gain 4 Combat; gain 6 instead while you own at least 12 Red cards.",
    flavor: "It grows fastest where everyone insists the stain is contained."
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

// Set collectible_edition: true on an Armory card to allow its cosmetic
// _collectible.png edition to be unlocked after all four normal copies are owned.
export function supportsCollectibleEdition(card) {
  return isCollectibleCard(card) && card.collectible_edition === true;
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

  if (effect.drawFromDrawPile) {
    const targetName = getCard(effect.drawFromDrawPile.id)?.name || "matching card";
    const look = Math.max(0, Math.floor(Number(effect.drawFromDrawPile.look) || 0));
    parts.push(`Draw ${targetName} from ${look ? `top ${look} of ` : ""}draw pile`);
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

  if (effect.createToken) {
    parts.push(`Create ${effect.createToken.count || 1} ${getCard(effect.createToken.id)?.name || "Token"}`);
  }

  if (effect.reclaim) {
    parts.push(`Reclaim${effect.reclaim.maxCost ? ` cost ≤${effect.reclaim.maxCost}` : ""}`);
  }

  if (effect.redeploy) {
    parts.push(`Redeploy${effect.redeploy.maxCost ? ` cost ≤${effect.redeploy.maxCost}` : ""}`);
  }

  if (effect.armor) {
    parts.push(`⬡ ${effect.armor.amount || 0} Base Armor`);
  }

  if (effect.repair) {
    parts.push(`Repair ${effect.repair.amount || 0}`);
  }

  if (effect.advanceConstruction) {
    parts.push(`⚒ ${effect.advanceConstruction.amount || 0} Construction`);
  }

  if (effect.coolHeat) {
    parts.push(`Cool ${effect.coolHeat.amount || 1} Heat`);
  }

  return parts.join(" · ");
}

export function fullCardRules(card) {
  const lines = [];

  if (card.expansion) {
    lines.push(
      `Expansion Base — ${card.health} Health; ` +
      `Construction ${card.construction}.`
    );
  } else if (card.type === "base") {
    lines.push(
      `${card.outpost ? "Outpost" : "Base"} — ` +
      `${card.defense} Defense.`
    );
  }

  if (card.type === "attachment") {
    lines.push("Attachment — play onto a legal base.");
  }

  if (card.token) {
    lines.push("Token.");
  }

  if (card.text) {
    lines.push(card.text);
  }

  if (card.abilityText) {
    lines.push(card.abilityText);
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

    if (card.collectible_edition !== undefined && typeof card.collectible_edition !== "boolean") {
      throw new Error(
        `Invalid collectible_edition on ${card.id}: ` +
        "use true or false."
      );
    }

    if (card.collectible_edition === true && !isCollectibleCard(card)) {
      throw new Error(
        `Noncollectible card ${card.id} cannot offer a collectible edition.`
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
