const DIFFICULTY_RANK = Object.freeze({ easy: 1, medium: 2, hard: 3, impossible: 4, mythic: 5 });

function freezeBoss(config) {
  return Object.freeze({
    startingShield: 0,
    startingBases: Object.freeze([]),
    startingDeck: Object.freeze([]),
    economy: Object.freeze({ bonusTrade: 0, tradeLimit: 6 }),
    aggression: 1,
    bossCard: "",
    phases: Object.freeze([]),
    ...config,
    startingBases: Object.freeze([...(config.startingBases || [])]),
    startingDeck: Object.freeze([...(config.startingDeck || [])]),
    economy: Object.freeze({ bonusTrade: 0, tradeLimit: 6, ...(config.economy || {}) }),
    bossAbility: Object.freeze({
      trigger: "turn",
      every: 3,
      label: "Boss Ability",
      ...(config.bossAbility || {}),
      effect: Object.freeze({ ...(config.bossAbility?.effect || {}) })
    }),
    phases: Object.freeze((config.phases || []).map(phase => Object.freeze({ ...phase })))
  });
}

function deck(...cardIds) {
  return cardIds;
}

// These five leaders form a deliberately forgiving introductory arc. Their
// Authority values are not region-multiplied during the first campaign cycle.
export const BEGINNER_CAMPAIGN_BOSSES = Object.freeze([
  freezeBoss({
    id: "coppertrail_reaver", name: "Brakka Coin-Taker", title: "The Coppertrail Reaver", faction: "green", difficulty: "easy", tier: 0, arc: "beginner",
    story: "A loud border raider whose confidence is considerably larger than her first warband.",
    authority: 30,
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "starter_blade", "starter_blade"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: .88,
    bossAbility: { every: 4, label: "Coppertrail Rush", effect: { gainCombat: 1 } }
  }),
  freezeBoss({
    id: "lantern_road_apprentice", name: "Sister Elowen", title: "The Lantern-Road Apprentice", faction: "blue", difficulty: "easy", tier: 0, arc: "beginner",
    story: "A young shrine-keeper testing a borrowed shield and an oath she has only just learned to carry.",
    authority: 35, startingShield: 1,
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "starter_blade", "starter_blade", "tithe_acolyte"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: .9,
    bossAbility: { every: 4, label: "Lantern Ward", effect: { gainShield: 2 } }
  }),
  freezeBoss({
    id: "threadglass_wayfinder", name: "Orrin Threadglass", title: "The Unproven Wayfinder", faction: "yellow", difficulty: "easy", tier: 0, arc: "beginner",
    story: "A future-reader who can see every road ahead except the one on which he loses.",
    authority: 40, startingShield: 2, startingBases: ["probability_beacon"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "starter_blade", "starpath_mite", "starwhisper_scout"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: .92,
    bossAbility: { every: 4, label: "Favorable Thread", effect: { gainTrade: 1, gainCombat: 1 } }
  }),
  freezeBoss({
    id: "emberscar_cadet", name: "Vessa Emberscar", title: "The Ash-Court Cadet", faction: "red", difficulty: "easy", tier: 0, arc: "beginner",
    story: "An ambitious Covenant cadet hunting a victory impressive enough to earn her first blackglass seal.",
    authority: 45, startingShield: 3, startingBases: ["candlecrypt_altar"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "starter_blade", "coin_curse", "cinder_initiate", "ashledger_acolyte"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: .98,
    bossAbility: { every: 3, label: "Cadet's Hex", effect: { gainCombat: 2 } }
  }),
  freezeBoss({
    id: "mudwall_chieftain", name: "Drokka Mudwall", title: "Chieftain of the First Siege", faction: "green", difficulty: "easy", tier: 0, arc: "beginner",
    story: "The first commander in the borderlands with enough discipline to turn a raid into a real siege.",
    authority: 55, startingShield: 3, startingBases: ["scrapwood_barricade"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "wreckmarket_runt", "mudboot_rusher", "fuseback_runt", "ironjaw_scavenger"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: 1.02,
    bossAbility: { every: 3, label: "First Siege", effect: { gainCombat: 3 } },
    phases: [{ id: "mudwall_stands", name: "Phase II · Mudwall Stands", authorityAtOrBelow: 28, createBase: "mudwall_camp", bossCardFrequency: 3 }]
  })
]);

// The original roster remains intact and begins only after the introductory arc.
export const EXISTING_CAMPAIGN_BOSSES = Object.freeze([
  freezeBoss({
    id: "scrapfang_raider", name: "Ruk Scrapfang", title: "The Border Raider", faction: "green", difficulty: "easy", tier: 1,
    authority: 70, startingBases: ["mudwall_camp"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "wreckmarket_runt", "wreckmarket_runt", "ironroot_foreman", "grukkin_embercub"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: 1.08,
    bossAbility: { every: 3, label: "Scrapfang Charge", effect: { gainCombat: 3 } },
    phases: [{ id: "raider_fury", name: "Phase II · Raider Fury", authorityAtOrBelow: 32, createBase: "scrapwood_barricade", bossCardFrequency: 2 }]
  }),
  freezeBoss({
    id: "veil_market_seer", name: "Ilyra Veil-Eyed", title: "The Market Seer", faction: "yellow", difficulty: "easy", tier: 1,
    authority: 72, startingBases: ["probability_beacon"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "starwhisper_scout", "starwhisper_scout", "neural_leash", "gravity_hook"),
    economy: { bonusTrade: 1, tradeLimit: 6 }, aggression: 1.02,
    bossAbility: { every: 3, label: "Foreseen Acquisition", effect: { gainTrade: 2, devourCheapestMarket: true } },
    phases: [{ id: "sealed_future", name: "Phase II · The Sealed Future", authorityAtOrBelow: 34, createBase: "phase_warden", bossCardFrequency: 2 }]
  }),
  freezeBoss({
    id: "silver_gate_prior", name: "Prior Caelis", title: "Keeper of the Silver Gate", faction: "blue", difficulty: "easy", tier: 1,
    authority: 76, startingShield: 4, startingBases: ["silver_gate_watch"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "halo_scramble_officer", "halo_scramble_officer", "keystone_kid", "coolant_wing_chaplain"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: 1,
    bossAbility: { every: 3, label: "Gatekeeper's Vow", effect: { gainShield: 3, gainCombat: 2 } },
    phases: [{ id: "last_watch", name: "Phase II · The Last Watch", authorityAtOrBelow: 36, createBase: "roadside_shrine", bossCardFrequency: 2 }]
  }),
  freezeBoss({
    id: "cinder_debt_collector", name: "Morcant Ember-Tithe", title: "The Cinder Collector", faction: "red", difficulty: "easy", tier: 1,
    authority: 78, startingBases: ["candlecrypt_altar"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "ashspark_midwife", "ashspark_midwife", "cinderwisp_hatchling", "cinder_debt_engine"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: 1.12,
    bossAbility: { every: 3, label: "Collect the Debt", effect: { damageAuthority: 2, gainCombat: 2 } },
    phases: [{ id: "compound_interest", name: "Phase II · Compound Ruin", authorityAtOrBelow: 38, createBase: "obsidian_altar", bossCardFrequency: 2 }]
  }),
  freezeBoss({
    id: "border_war_captain", name: "Captain Varka", title: "The Broken Banner", faction: "green", supportFaction: "red", difficulty: "easy", tier: 1,
    authority: 82, startingBases: ["war_drum_camp"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "wreckmarket_runt", "ironroot_foreman", "grukkin_embercub", "ashspark_midwife", "cinderwisp_hatchling"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: 1.18,
    bossAbility: { every: 3, label: "Broken Banner Rush", effect: { gainCombat: 5 } },
    phases: [{ id: "no_retreat", name: "Phase II · No Retreat", authorityAtOrBelow: 40, createBase: "scrapwood_barricade", bossCardFrequency: 2 }]
  }),

  freezeBoss({
    id: "ironroot_warchief", name: "Gorun Ironroot", title: "Warchief of the Foundries", faction: "green", difficulty: "medium", tier: 2,
    authority: 92, startingBases: ["ironroot_encampment"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "ironroot_foreman", "ironroot_foreman", "brood_path_forerunner", "magma_hide_warbeast", "grukkin_ironjaw_brute"),
    economy: { bonusTrade: 1, tradeLimit: 6 }, aggression: 1.22,
    bossAbility: { every: 2, label: "Foundry Warcry", effect: { gainCombat: 5, gainShield: 2 } },
    phases: [{ id: "iron_march", name: "Phase II · The Iron March", authorityAtOrBelow: 46, createBase: "breakneck_stockade", bossCardFrequency: 1 }]
  }),
  freezeBoss({
    id: "null_prism_archon", name: "Archon Veyl", title: "The Null-Prism Archon", faction: "yellow", difficulty: "medium", tier: 2,
    authority: 96, startingShield: 5, startingBases: ["null_prism"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "starwhisper_scout", "neural_leash", "gravity_hook", "chrono_beast", "signal_eater"),
    economy: { bonusTrade: 1, tradeLimit: 6 }, aggression: 1.08,
    bossAbility: { every: 2, label: "Null the Market", effect: { devourCheapestMarket: true, gainCombatFromCost: true, gainShield: 2 } },
    phases: [{ id: "prism_lock", name: "Phase II · Prism Lock", authorityAtOrBelow: 48, createBase: "phase_warden", bossCardFrequency: 1 }]
  }),
  freezeBoss({
    id: "storm_vault_exarch", name: "Exarch Solenne", title: "The Storm-Vault Exarch", faction: "blue", difficulty: "medium", tier: 2,
    authority: 100, startingShield: 8, startingBases: ["storm_vault_monastery"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "halo_scramble_officer", "keystone_kid", "twin_core_coolant_frigate", "cherub_of_last_mercy", "rampart_ram"),
    economy: { bonusTrade: 0, tradeLimit: 6 }, aggression: 1.12,
    bossAbility: { every: 2, label: "Vault Lightning", effect: { gainCombat: 4, gainShield: 4 } },
    phases: [{ id: "open_the_vault", name: "Phase II · Open the Vault", authorityAtOrBelow: 50, createBase: "skywall_muster_deck", bossCardFrequency: 1 }]
  }),
  freezeBoss({
    id: "ashen_tithe_queen", name: "Queen Seraxa", title: "The Ashen Tithe", faction: "red", difficulty: "medium", tier: 2,
    authority: 102, startingBases: ["black_tithe_post"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "ashspark_midwife", "cinderwisp_hatchling", "hexspark_impkin", "ashen_chain_cannon", "redline_executioner"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: 1.3,
    bossAbility: { every: 2, label: "Ashen Levy", effect: { damageAuthority: 3, gainCombat: 4 } },
    phases: [{ id: "final_tithe", name: "Phase II · The Final Tithe", authorityAtOrBelow: 51, createBase: "cinder_choir", bossCardFrequency: 1 }]
  }),
  freezeBoss({
    id: "broodheart_matriarch", name: "Magra Broodheart", title: "Mother of the War-Nests", faction: "green", difficulty: "medium", tier: 2,
    authority: 106, startingBases: ["broodheart_idol", "gorge_nest_matriarch"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_coin", "starter_blade", "starter_blade", "brood_path_forerunner", "spawnlash_alpha", "carrion_nursery_raider", "magma_hide_warbeast", "grukkin_broodback_ravager"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: 1.32,
    bossAbility: { every: 2, label: "Brood Surge", effect: { gainCombat: 6, createBase: "mudwall_camp" } },
    phases: [{ id: "hatch_the_host", name: "Phase II · Hatch the Host", authorityAtOrBelow: 53, createBase: "spawning_war_nest", bossCardFrequency: 1 }]
  }),

  freezeBoss({
    id: "gatebreaker_supreme", name: "Grukkar Gatebreaker", title: "The Walking Siege", faction: "green", difficulty: "hard", tier: 3,
    authority: 118, startingBases: ["breakneck_stockade", "doomsday_drum"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_blade", "starter_blade", "brood_path_forerunner", "magma_hide_warbeast", "magma_hide_warbeast", "grukkin_ironjaw_brute", "grukkin_ironjaw_brute", "maw_of_the_third_brood"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: 1.45,
    bossAbility: { every: 2, label: "Break the Line", effect: { gainCombat: 8 } },
    phases: [{ id: "through_the_gate", name: "Phase II · Through the Gate", authorityAtOrBelow: 59, createBase: "warcamp_foundry", bossCardFrequency: 1 }]
  }),
  freezeBoss({
    id: "choir_marshal", name: "Marshal Aurelian", title: "The Unbroken Choir", faction: "blue", difficulty: "hard", tier: 3,
    authority: 122, startingShield: 12, startingBases: ["choir_of_unbroken_formation", "azure_chapel"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_blade", "starter_blade", "halo_scramble_officer", "formation_vow_captain", "twin_core_coolant_frigate", "cherub_of_last_mercy", "rampart_ram", "seraphic_air_marshal"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: 1.28,
    bossAbility: { every: 2, label: "Canticle of War", effect: { gainShield: 6, gainCombat: 6 } },
    phases: [{ id: "unbroken_formation", name: "Phase II · Unbroken Formation", authorityAtOrBelow: 61, createBase: "reliquary_guard", bossCardFrequency: 1 }]
  }),
  freezeBoss({
    id: "paradox_oracle", name: "Orryx of Nine Futures", title: "The Paradox Oracle", faction: "yellow", difficulty: "hard", tier: 3,
    authority: 126, startingShield: 8, startingBases: ["paradox_warden", "probability_beacon"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_blade", "starter_blade", "starwhisper_scout", "neural_leash", "chrono_beast", "chrono_beast", "signal_eater", "tomorrow_shell"),
    economy: { bonusTrade: 1, tradeLimit: 6 }, aggression: 1.2,
    bossAbility: { every: 2, label: "Erase the Worst Future", effect: { devourMostExpensiveMarket: true, gainCombatFromCost: true } },
    phases: [{ id: "only_one_future", name: "Phase II · Only One Future", authorityAtOrBelow: 63, createBase: "recursive_swarm_array", bossCardFrequency: 1 }]
  }),
  freezeBoss({
    id: "blackglass_patriarch", name: "Patriarch Noct Var", title: "Lord of Blackglass", faction: "red", difficulty: "hard", tier: 3,
    authority: 130, startingShield: 6, startingBases: ["hexed_portal", "soul_furnace"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_blade", "starter_blade", "cinderwisp_hatchling", "hexspark_impkin", "ashen_chain_cannon", "redline_executioner", "unbound_hellkite", "thrice_fired_abomination"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: 1.48,
    bossAbility: { every: 2, label: "Blackglass Sacrament", effect: { damageAuthority: 4, gainCombat: 6, gainShield: 3 } },
    phases: [{ id: "shatter_the_glass", name: "Phase II · Shatter the Glass", authorityAtOrBelow: 65, createBase: "black_tithe_post", bossCardFrequency: 1 }]
  }),
  freezeBoss({
    id: "doomhoof_warlord", name: "Krag Doomhoof", title: "The Final Charge", faction: "green", difficulty: "hard", tier: 3,
    authority: 134, startingBases: ["stampede_totem", "war_drum_camp"],
    startingDeck: deck("starter_coin", "starter_coin", "starter_blade", "starter_blade", "doomhoof_whelp", "doomhoof_whelp", "rampaging_furnace_tusk", "magma_hide_warbeast", "grukkin_ironjaw_brute", "worldroot_mobilizer"),
    economy: { bonusTrade: 0, tradeLimit: 4 }, aggression: 1.6,
    bossAbility: { every: 2, label: "Doomhoof Stampede", effect: { gainCombat: 10 } },
    phases: [{ id: "final_charge", name: "Phase II · Final Charge", authorityAtOrBelow: 67, createBase: "doomsday_drum", bossCardFrequency: 1 }]
  }),

  freezeBoss({
    id: "endwar_tyrant", name: "Tharok Endwar", title: "Tyrant of the Last Siege", faction: "green", difficulty: "impossible", tier: 4,
    authority: 148, startingShield: 10, startingBases: ["breakneck_stockade", "raidmasters_cache"],
    startingDeck: deck("starter_coin", "starter_blade", "brood_path_forerunner", "magma_hide_warbeast", "grukkin_ironjaw_brute", "grukkin_ironjaw_brute", "grukkin_broodback_ravager", "maw_of_the_third_brood", "worldroot_mobilizer", "doomhoof_final_charge"),
    economy: { bonusTrade: 0, tradeLimit: 4 }, aggression: 1.72,
    bossAbility: { every: 1, label: "Endwar Decree", effect: { gainCombat: 9, gainShield: 2 } },
    phases: [{ id: "last_siege", name: "Phase II · The Last Siege", authorityAtOrBelow: 74, createBase: "warcamp_foundry", bossCardFrequency: 1 }]
  }),
  freezeBoss({
    id: "last_dawn_seraph", name: "Ilyrion Last-Dawn", title: "Seraph of the Last Dawn", faction: "blue", difficulty: "impossible", tier: 4,
    authority: 154, startingShield: 18, startingBases: ["basilica_of_many_lights", "storm_vault_monastery"],
    startingDeck: deck("starter_coin", "starter_blade", "halo_scramble_officer", "formation_vow_captain", "twin_core_coolant_frigate", "cherub_of_last_mercy", "rampart_ram", "reliquary_hart", "seraphic_air_marshal", "aurelium_haloform"),
    economy: { bonusTrade: 0, tradeLimit: 5 }, aggression: 1.5,
    bossAbility: { every: 1, label: "Last Dawn", effect: { gainCombat: 7, gainShield: 7, heal: 2 } },
    phases: [{ id: "sunless_dawn", name: "Phase II · Sunless Dawn", authorityAtOrBelow: 77, createBase: "choir_of_unbroken_formation", bossCardFrequency: 1 }]
  }),
  freezeBoss({
    id: "nine_future_sovereign", name: "Xal the Ninefold", title: "Sovereign of Nine Futures", faction: "yellow", difficulty: "impossible", tier: 4,
    authority: 160, startingShield: 12, startingBases: ["paradox_warden", "network_of_unmade_paths"],
    startingDeck: deck("starter_coin", "starter_blade", "starwhisper_scout", "neural_leash", "chrono_beast", "chrono_beast", "signal_eater", "tomorrow_shell", "afterimage_stalker", "causal_severer"),
    economy: { bonusTrade: 1, tradeLimit: 6 }, aggression: 1.42,
    bossAbility: { every: 1, label: "Collapse the Trade Row", effect: { devourMostExpensiveMarket: true, gainCombatFromCost: true, gainShield: 3 } },
    phases: [{ id: "tenth_future", name: "Phase II · The Tenth Future", authorityAtOrBelow: 80, createBase: "recursive_swarm_array", bossCardFrequency: 1 }]
  }),
  freezeBoss({
    id: "blood_crowned_calamity", name: "Veyra Blood-Crowned", title: "The Covenant Calamity", faction: "red", difficulty: "impossible", tier: 4,
    authority: 168, startingShield: 8, startingBases: ["throne_of_escalating_ruin", "pyre_swarm_foundry"],
    startingDeck: deck("starter_coin", "starter_blade", "cinderwisp_hatchling", "hexspark_impkin", "ashen_chain_cannon", "redline_executioner", "unbound_hellkite", "thrice_fired_abomination", "bloodhorn_pactbeast", "blood_crowned_calamity"),
    economy: { bonusTrade: 0, tradeLimit: 4 }, aggression: 1.78,
    bossAbility: { every: 1, label: "Calamity Unbound", effect: { damageAuthority: 5, gainCombat: 9 } },
    phases: [{ id: "crown_ignites", name: "Phase II · The Crown Ignites", authorityAtOrBelow: 84, createBase: "soul_furnace", bossCardFrequency: 1 }]
  }),
  freezeBoss({
    id: "realm_furnace_leviathan", name: "Azgorath Realm-Furnace", title: "The Furnace Leviathan", faction: "red", supportFaction: "green", difficulty: "impossible", tier: 4,
    authority: 178, startingShield: 14, startingBases: ["soul_furnace", "warcamp_foundry", "cinder_choir"],
    startingDeck: deck("starter_coin", "starter_blade", "magma_hide_warbeast", "furnace_hide_behemoth", "cinderwisp_hatchling", "ashen_chain_cannon", "redline_executioner", "unbound_hellkite", "grukkin_ironjaw_brute", "realm_eater"),
    economy: { bonusTrade: 0, tradeLimit: 4 }, aggression: 1.85,
    bossAbility: { every: 1, label: "Feed the Realm-Furnace", effect: { devourCheapestMarket: true, gainCombatFromCost: true, gainCombat: 7, createBase: "candlecrypt_altar" } },
    phases: [{ id: "furnace_open", name: "Phase II · Furnace Open", authorityAtOrBelow: 89, createBase: "throne_of_escalating_ruin", bossCardFrequency: 1 }]
  }),

  freezeBoss({
    id: "world_eater",
    name: "Voracynth, World Eater",
    title: "The World Eater",
    faction: "yellow",
    supportFaction: "red",
    difficulty: "mythic",
    tier: 5,
    authority: 190,
    startingShield: 20,
    startingBases: ["worldheart_core", "rift_maw"],
    startingDeck: deck("starter_coin", "starter_blade", "voracynth_dream_egg", "voracynth_dream_egg", "voracynth_star_hatchling", "voracynth_star_hatchling", "signal_eater", "causal_severer", "tomorrow_shell", "voracynth_boss_card"),
    economy: { bonusTrade: 0, tradeLimit: 4 },
    aggression: 2,
    bossCard: "voracynth_boss_card",
    bossAbility: {
      trigger: "turn",
      every: 2,
      label: "Devour the Market",
      effect: { devourCheapestMarket: true, gainCombatFromCost: true, damageAuthority: 3 }
    },
    phases: [
      { id: "phase_two", name: "Phase II · Endless Hunger", authorityAtOrBelow: 95, createBase: "rift_maw", bossCardFrequency: 1 }
    ]
  })
]);

export const CAMPAIGN_BOSSES = Object.freeze([
  ...BEGINNER_CAMPAIGN_BOSSES,
  ...EXISTING_CAMPAIGN_BOSSES
]);

const bossMap = new Map(CAMPAIGN_BOSSES.map(boss => [boss.id, boss]));

export function getCampaignBoss(bossId) {
  return bossMap.get(String(bossId || "")) || null;
}

export function campaignBossForRegion(region = 1) {
  const safeRegion = Math.max(1, Math.floor(Number(region) || 1));
  return CAMPAIGN_BOSSES[(safeRegion - 1) % CAMPAIGN_BOSSES.length];
}

export function campaignBossCycle(region = 1) {
  const safeRegion = Math.max(1, Math.floor(Number(region) || 1));
  return 1 + Math.floor((safeRegion - 1) / CAMPAIGN_BOSSES.length);
}

export function bossDifficultyRank(boss) {
  return DIFFICULTY_RANK[boss?.difficulty] || 1;
}

export function effectiveBossAbilityFrequency(boss, bossState = {}) {
  let frequency = Math.max(1, Math.floor(Number(boss?.bossAbility?.every) || 1));
  const triggered = new Set(bossState.triggeredPhases || []);
  for (const phase of boss?.phases || []) {
    if (triggered.has(phase.id) && Number.isFinite(Number(phase.bossCardFrequency))) frequency = Math.max(1, Math.floor(Number(phase.bossCardFrequency)));
  }
  frequency = Math.max(1, frequency - Math.max(0, Math.floor(Number(bossState.frequencyReduction) || 0)));
  return frequency;
}

export function bossAbilityStatus(boss, bossState = {}) {
  const turns = Math.max(0, Math.floor(Number(bossState.turns) || 0));
  const every = effectiveBossAbilityFrequency(boss, bossState);
  const remainder = turns % every;
  return {
    every,
    turns,
    due: turns > 0 && remainder === 0,
    activatesIn: remainder === 0 ? every : every - remainder
  };
}

export function advanceBossTurn(boss, bossState = {}) {
  const next = {
    ...bossState,
    turns: Math.max(0, Math.floor(Number(bossState.turns) || 0)) + 1,
    triggeredPhases: [...new Set(bossState.triggeredPhases || [])]
  };
  return { state: next, ability: bossAbilityStatus(boss, next) };
}

export function pendingBossPhases(boss, authority, bossState = {}) {
  const triggered = new Set(bossState.triggeredPhases || []);
  return (boss?.phases || []).filter(phase => !triggered.has(phase.id) && Number(authority) <= Number(phase.authorityAtOrBelow));
}

export function applyBossPhaseState(bossState = {}, phase) {
  return {
    ...bossState,
    triggeredPhases: [...new Set([...(bossState.triggeredPhases || []), phase.id])]
  };
}
