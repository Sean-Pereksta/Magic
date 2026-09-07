// Dwarf World — Fortress Command & Identity Expansion
// This file is injected verbatim inside DwarfWorld-core.html's existing game IIFE.
// Keep it dependency-free: it intentionally closes over the core game's private state.

if (!window.__dwarfWorldFortressExpansionInstalled) {
  window.__dwarfWorldFortressExpansionInstalled = true;

  const DWFX_SAVE_VERSION = 3;
  const DWFX_BUILD_BASE = 100;
  const DWFX_MAP_SIZE = 240;
  const DWFX_RELIC_COLOR = "#ffd66f";
  const DWFX_RELIC_GLOW = "rgba(255,214,111,.7)";

  const DWFX_BUILDINGS = {
    greatHall: {
      mode: DWFX_BUILD_BASE + 1, name: "Great Hall", icon: "🏛️", minDepth: 0, cost: 260, radius: 7, unique: true,
      effect: "+dwarf capacity • cheaper recruitment • unlocks advanced civic buildings"
    },
    tavern: {
      mode: DWFX_BUILD_BASE + 2, name: "Tavern", icon: "🍺", minDepth: 0, cost: 220, radius: 8, unique: true,
      effect: "recovery aura • visiting recruit candidates"
    },
    treasury: {
      mode: DWFX_BUILD_BASE + 3, name: "Treasury", icon: "💰", minDepth: 120, cost: 340, radius: 5, unique: true,
      effect: "+3/6/10% returned treasure value"
    },
    barracks: {
      mode: DWFX_BUILD_BASE + 4, name: "Barracks", icon: "⚔️", minDepth: 120, cost: 380, radius: 10, unique: true,
      effect: "nearby dwarves gain +8% damage and +5% defense"
    },
    workshop: {
      mode: DWFX_BUILD_BASE + 5, name: "Workshop", icon: "🛠️", minDepth: 120, cost: 400, radius: 6, unique: true,
      effect: "lower forge crafting and structure costs"
    },
    mushroomFarm: {
      mode: DWFX_BUILD_BASE + 6, name: "Mushroom Farm", icon: "🍄", minDepth: 260, cost: 360, radius: 9, unique: true,
      effect: "passive supplies • Deepcap Rations • travel recovery"
    },
    infirmary: {
      mode: DWFX_BUILD_BASE + 7, name: "Infirmary", icon: "✚", minDepth: 260, cost: 430, radius: 10, unique: true,
      effect: "critical retreat point • strong healing • high-level death save"
    },
    surveyHall: {
      mode: DWFX_BUILD_BASE + 8, name: "Survey Hall", icon: "🔭", minDepth: 420, cost: 520, radius: 22, unique: true,
      effect: "periodic hints for valuable deposits and danger beyond the frontier"
    },
    liftShaft: {
      mode: DWFX_BUILD_BASE + 9, name: "Lift Shaft", icon: "↕️", minDepth: 420, cost: 650, radius: 3, unique: false,
      effect: "paired rapid travel between explored distant regions"
    },
    shrine: {
      mode: DWFX_BUILD_BASE + 10, name: "Shrine of the Ancestors", icon: "🕯️", minDepth: 620, cost: 760, radius: 10, unique: true,
      effect: "veterans gain Inspired and slightly more XP"
    },
    greatVault: {
      mode: DWFX_BUILD_BASE + 11, name: "Great Vault", icon: "🔐", minDepth: 860, cost: 1050, radius: 5, unique: true,
      effect: "protects legendary gear • slightly improves unique relic discovery"
    }
  };
  const DWFX_BUILDING_BY_MODE = new Map(Object.values(DWFX_BUILDINGS).map(def => [def.mode, def]));
  const DWFX_BUILDING_BY_TYPE = new Map(Object.entries(DWFX_BUILDINGS).map(([type, def]) => [type, def]));

  const DWFX_RELICS = [
    {
      id: "worldsplitter", category: "pickaxe", name: "WORLDSPLITTER", icon: "⛏", minDepth: 260,
      flavor: "The mountain learned to fear the hand that held it.",
      desc: "Mining occasionally fractures surrounding rock in a 3×3 area.",
      make(depth) {
        const it = mkItemPickaxe("legendary");
        Object.assign(it, { miningPower: Math.max(it.miningPower, 28 + Math.floor(depth / 80)), miningRate: Math.max(it.miningRate, 1.75) });
        return it;
      }
    },
    {
      id: "heart_of_mountain", category: "pickaxe", name: "HEART OF THE MOUNTAIN", icon: "⛏", minDepth: 420,
      flavor: "Every fathom downward wakes another pulse.",
      desc: "Mining power rises with depth.",
      make(depth) {
        const it = mkItemPickaxe("legendary");
        Object.assign(it, { miningPower: Math.max(it.miningPower, 24), miningRate: Math.max(it.miningRate, 1.65) });
        return it;
      }
    },
    {
      id: "fortunes_end", category: "pickaxe", name: "FORTUNE'S END", icon: "⛏", minDepth: 420,
      flavor: "It does not find treasure. It decides where treasure was always meant to be.",
      desc: "Improves valuable returns and can expose gemstones in ordinary rock.",
      make(depth) {
        const it = mkItemPickaxe("legendary");
        Object.assign(it, { miningPower: Math.max(it.miningPower, 25), miningRate: Math.max(it.miningRate, 1.7) });
        return it;
      }
    },
    {
      id: "king_under_stone", category: "weapon", name: "KING UNDER STONE", icon: "⚔", minDepth: 420,
      flavor: "The crown vanished. The axe remembered.",
      desc: "Ancient axe whose damage rises with the wielder's level.",
      make(depth) {
        const it = mkItemWeapon("legendary");
        Object.assign(it, { weaponType: "axe", ranged: false, range: 1.35, rate: 1.08, dmg: Math.max(it.dmg, 42 + Math.floor(depth / 100)) });
        return it;
      }
    },
    {
      id: "spawnbreaker", category: "weapon", name: "SPAWNBREAKER", icon: "⚔", minDepth: 260,
      flavor: "No nest is old enough to outlive its oath.",
      desc: "Deals massively increased damage to spawners and nest structures.",
      make(depth) {
        const it = mkItemWeapon("legendary");
        Object.assign(it, { weaponType: "axe", ranged: false, range: 1.35, rate: 1.0, dmg: Math.max(it.dmg, 39 + Math.floor(depth / 120)) });
        return it;
      }
    },
    {
      id: "deepward_bow", category: "weapon", name: "DEEPWARD BOW", icon: "🏹", minDepth: 420,
      flavor: "Its string grows taut when home is far behind.",
      desc: "Exceptional range and more damage far from the fortress.",
      make(depth) {
        const it = mkItemWeapon("legendary");
        Object.assign(it, { weaponType: "bow", ranged: true, range: 11.5, rate: 1.35, dmg: Math.max(it.dmg, 30 + Math.floor(depth / 130)) });
        return it;
      }
    },
    {
      id: "grudgeplate", category: "armor", name: "GRUDGEPLATE", icon: "🛡", minDepth: 420,
      flavor: "Names are hammered into it where dents should be.",
      desc: "Major kills record Grudges that grant a small permanent defensive bonus.",
      make(depth) {
        const it = mkItemArmor("legendary");
        Object.assign(it, { defense: Math.max(it.defense, .48), bonusHp: Math.max(it.bonusHp, 110), grudges: 0 });
        return it;
      }
    },
    {
      id: "last_kings_bulwark", category: "armor", name: "LAST KING'S BULWARK", icon: "🛡", minDepth: 620,
      flavor: "One retreat was written into the steel. Only one was needed.",
      desc: "A lethal hit can leave the wearer at 1 HP and trigger an emergency retreat. Long cooldown.",
      make(depth) {
        const it = mkItemArmor("legendary");
        Object.assign(it, { defense: Math.max(it.defense, .52), bonusHp: Math.max(it.bonusHp, 130) });
        return it;
      }
    },
    {
      id: "armor_deep_road", category: "armor", name: "ARMOR OF THE DEEP ROAD", icon: "🛡", minDepth: 420,
      flavor: "Distance is only another weight it was forged to carry.",
      desc: "Movement speed increases while far from the fortress.",
      make(depth) {
        const it = mkItemArmor("legendary");
        Object.assign(it, { defense: Math.max(it.defense, .44), bonusHp: Math.max(it.bonusHp, 95) });
        return it;
      }
    },
    {
      id: "lantern_first_delver", category: "relic", name: "LANTERN OF THE FIRST DELVER", icon: "✦", minDepth: 260,
      flavor: "Its flame remembers passages no living cartographer has seen.",
      desc: "Expands fog-of-war vision and pulses toward nearby valuable ore.",
      make() {
        return { id: uuid(), category: "relic", rarity: "legendary", sell: 700, name: "Lantern of the First Delver" };
      }
    },
    {
      id: "voidhook", category: "relic", name: "VOIDHOOK", icon: "✦", minDepth: 620,
      flavor: "The Deep takes many. This one thing learned to pull back.",
      desc: "At critical health, can pull the wearer to a safe explored position. Long cooldown.",
      make() {
        return { id: uuid(), category: "relic", rarity: "legendary", sell: 900, name: "Voidhook" };
      }
    },
    {
      id: "ancestors_token", category: "relic", name: "ANCESTOR'S TOKEN", icon: "✦", minDepth: 420,
      flavor: "Every lesson arrives with older hands behind it.",
      desc: "Increases XP and slightly improves perk milestone choices.",
      make() {
        return { id: uuid(), category: "relic", rarity: "legendary", sell: 820, name: "Ancestor's Token" };
      }
    }
  ];
  const DWFX_RELIC_BY_ID = new Map(DWFX_RELICS.map(r => [r.id, r]));

  function dwfxDefaultState() {
    return {
      version: DWFX_SAVE_VERSION,
      civicStructures: [],
      relicsFound: [],
      relicAnnouncements: [],
      deepestSeen: 0,
      latestDiscovery: null,
      surveyHints: [],
      visitor: null,
      visitorTimer: 72,
      surveyTimer: 24,
      rationTimer: 65,
      rationStock: 0,
      passiveTimer: 20,
      selectedStructureId: null
    };
  }
  let dwfxState = dwfxDefaultState();
  let dwfxInventoryMode = "all";
  let dwfxRosterNeedFilter = null;
  let dwfxPendingPlacement = null;
  let dwfxMapLastDraw = 0;
  let dwfxMapFullscreen = false;
  let dwfxPlacementCard = null;
  let dwfxStructureCard = null;
  let dwfxRelicModal = null;
  let dwfxActiveDwarf = null;
  let dwfxLastWorldClick = null;
  let dwfxLegacyBaseCosts = { ...COST };

  const dwfxCss = document.createElement("style");
  dwfxCss.textContent = `
    #dwarfList{gap:7px;border:0;background:transparent;overflow:visible}
    .dwarfBtn.dwfx-card{position:relative;display:grid;grid-template-columns:42px minmax(0,1fr);gap:8px;align-items:start;
      min-height:104px;border:1px solid rgba(255,255,255,.10)!important;border-radius:12px;background:linear-gradient(180deg,rgba(255,255,255,.055),rgba(255,255,255,.025));
      padding:8px 9px 12px;overflow:hidden}
    .dwarfBtn.dwfx-card:hover{background:rgba(255,255,255,.075)}
    .dwarfBtn.dwfx-card.sel{border-color:rgba(120,220,255,.55)!important;box-shadow:0 0 0 1px rgba(120,220,255,.18) inset}
    .dwfx-avatar{width:42px;height:42px;border-radius:10px;display:grid;place-items:center;font-size:25px;border:1px solid rgba(255,255,255,.15);
      background:radial-gradient(circle at 50% 35%,rgba(226,190,140,.25),rgba(70,45,25,.20)),rgba(255,255,255,.04)}
    .dwfx-card-main{min-width:0;display:grid;gap:4px}
    .dwfx-name{display:flex;justify-content:space-between;gap:8px;align-items:center;font-size:12px;font-weight:1000}
    .dwfx-name span:first-child{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .dwfx-activity{font-size:10px;color:rgba(231,238,247,.76);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .dwfx-equipment{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:4px;margin-top:2px}
    .dwfx-eq{font-size:9px;font-weight:1000;border:1px solid rgba(255,255,255,.12);border-radius:7px;padding:3px 4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;background:rgba(0,0,0,.16)}
    .dwfx-eq.empty{color:#ff9aa8;border-color:rgba(255,100,120,.35);background:rgba(255,90,110,.07)}
    .dwfx-eq[data-rarity="uncommon"]{border-color:rgba(133,255,176,.42);color:#85ffb0}
    .dwfx-eq[data-rarity="rare"]{border-color:rgba(127,215,255,.46);color:#7fd7ff}
    .dwfx-eq[data-rarity="epic"]{border-color:rgba(214,166,255,.52);color:#d6a6ff}
    .dwfx-eq[data-rarity="legendary"]{border-color:rgba(255,214,111,.62);color:#ffd66f}
    .dwfx-perks{display:flex;gap:3px;flex-wrap:wrap;min-height:14px}
    .dwfx-perk{width:17px;height:17px;border-radius:5px;display:grid;place-items:center;font-size:9px;font-weight:1000;background:rgba(255,214,111,.10);border:1px solid rgba(255,214,111,.25);color:#ffe7a4}
    .dwfx-hpbar{position:absolute;left:0;right:0;bottom:0;height:4px;background:rgba(255,255,255,.06)}
    .dwfx-hpbar>i{display:block;height:100%;background:linear-gradient(90deg,#ff667a,#ffd66f 45%,#7be0a4);transition:width .18s ease}
    .dwfx-needbox{display:grid;gap:7px;margin-top:8px;padding:8px;border:1px solid rgba(255,255,255,.09);border-radius:10px;background:rgba(0,0,0,.13)}
    .dwfx-needs{display:flex;gap:6px;flex-wrap:wrap}
    .dwfx-need{height:28px!important;font-size:10px!important;padding:0 8px!important}
    .dwfx-need.on{outline:1px solid rgba(120,220,255,.55);background:rgba(120,220,255,.13)}
    .dwfx-auto{height:32px!important;background:linear-gradient(180deg,rgba(255,214,111,.17),rgba(255,214,111,.08))!important;border-color:rgba(255,214,111,.34)!important}
    .dwfx-filterbar{display:flex;gap:5px;flex-wrap:wrap;margin-top:7px}
    .dwfx-filterbar button{height:28px;font-size:9px;padding:0 7px}
    .dwfx-filterbar button.on{background:rgba(120,220,255,.14);border-color:rgba(120,220,255,.38)}
    .dwfx-build-grid{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-top:8px}
    .dwfx-build-grid button{height:auto;min-height:48px;padding:6px 7px;text-align:left;font-size:10px}
    .dwfx-build-grid button b{display:block;font-size:11px;margin-bottom:2px}
    .dwfx-build-grid button:disabled{opacity:.38}
    #dwfxMapPanel{position:fixed;right:12px;top:74px;z-index:44;width:264px;padding:9px;border:1px solid rgba(255,255,255,.13);border-radius:14px;
      background:linear-gradient(180deg,rgba(9,12,16,.94),rgba(7,9,13,.90));backdrop-filter:blur(10px);box-shadow:0 14px 42px rgba(0,0,0,.42);display:none}
    #dwfxMapPanel.open{display:block}
    #dwfxMapPanel.fullscreen{inset:8px;width:auto;display:grid;grid-template-rows:auto minmax(0,1fr) auto;place-items:center;z-index:85}
    #dwfxMapPanel.fullscreen #dwfxMap{width:min(86vw,86vh);height:min(86vw,86vh);max-width:none}
    #dwfxMap{display:block;width:240px;height:240px;border:1px solid rgba(255,255,255,.12);border-radius:9px;background:#020406;image-rendering:pixelated;cursor:crosshair}
    .dwfx-map-head{display:flex;align-items:center;justify-content:space-between;gap:6px;margin-bottom:7px}
    .dwfx-map-actions{display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-top:7px}
    .dwfx-map-actions button{height:28px;font-size:9px;padding:0 5px}
    #dwfxPlacementCard,#dwfxStructureCard{position:fixed;z-index:61;left:50%;transform:translateX(-50%);bottom:16px;width:min(430px,calc(100vw - 24px));
      padding:10px;border:1px solid rgba(255,255,255,.14);border-radius:12px;background:rgba(8,11,15,.94);box-shadow:0 14px 38px rgba(0,0,0,.42);pointer-events:none}
    #dwfxPlacementCard{display:none}
    #dwfxStructureCard{display:none;pointer-events:auto;bottom:76px}
    .dwfx-quality{font-weight:1000;letter-spacing:.08em}
    .dwfx-quality.excellent{color:#7be0a4}.dwfx-quality.good{color:#7fd7ff}.dwfx-quality.poor{color:#ffd66f}.dwfx-quality.invalid{color:#ff7d8e}
    .dwfx-cardline{display:flex;justify-content:space-between;gap:10px;font-size:11px;margin-top:4px;color:rgba(231,238,247,.77)}
    #dwfxMobileConfirm{position:fixed;left:8px;right:8px;bottom:62px;z-index:82;display:none;gap:7px;padding:8px;border-radius:13px;border:1px solid rgba(120,220,255,.30);background:rgba(7,11,16,.96)}
    #dwfxMobileConfirm.show{display:flex}#dwfxMobileConfirm button{flex:1}
    #dwfxRelicModal{position:fixed;inset:0;z-index:96;display:none;place-items:center;padding:18px;background:rgba(2,4,7,.82);backdrop-filter:blur(8px)}
    #dwfxRelicModal.open{display:grid}
    .dwfx-relic-card{width:min(560px,95vw);border:1px solid rgba(255,214,111,.62);border-radius:18px;padding:22px;background:linear-gradient(180deg,rgba(35,27,13,.97),rgba(8,10,14,.98));
      box-shadow:0 0 44px rgba(255,214,111,.18),0 24px 70px rgba(0,0,0,.7)}
    .dwfx-relic-title{text-align:center;color:#ffd66f;font-weight:1000;letter-spacing:.18em;font-size:12px}
    .dwfx-relic-name{text-align:center;font-size:25px;font-weight:1000;margin:10px 0 5px;text-shadow:0 0 18px rgba(255,214,111,.35)}
    .dwfx-relic-flavor{text-align:center;font-style:italic;color:rgba(255,235,180,.72);margin-bottom:14px}
    .dwfx-relic-actions{display:flex;gap:7px;flex-wrap:wrap;margin-top:14px}.dwfx-relic-actions button{flex:1;min-width:120px}
    .dwfx-relic-border{box-shadow:0 0 0 1px rgba(255,214,111,.45) inset,0 0 14px rgba(255,214,111,.18)}
    @media(max-width:820px),(pointer:coarse){
      .dwarfBtn.dwfx-card{min-height:108px}
      .dwfx-equipment{grid-template-columns:1fr}
      #dwfxMapPanel{top:52px;right:6px;left:6px;width:auto;bottom:60px}
      #dwfxMapPanel:not(.fullscreen) #dwfxMap{width:min(62vw,270px);height:min(62vw,270px);margin:auto}
      .dwfx-build-grid{grid-template-columns:1fr}
      #dwfxPlacementCard{bottom:114px}
      #dwfxStructureCard{bottom:114px}
    }
  `;
  document.head.appendChild(dwfxCss);

  function dwfxRarityLetter(it) {
    if (!it) return "—";
    if (it.relic) return "✦";
    const map = { common: "C", uncommon: "U", rare: "R", epic: "E", legendary: "L" };
    return map[it.rarity] || "?";
  }
  function dwfxEsc(text) {
    return String(text ?? "").replace(/[&<>"']/g, ch => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  }
  function dwfxStructure(type) {
    return dwfxState.civicStructures.find(s => s.type === type) || null;
  }
  function dwfxStructures(type) {
    return dwfxState.civicStructures.filter(s => !type || s.type === type);
  }
  function dwfxMaxLevel(type) {
    return dwfxStructures(type).reduce((m, s) => Math.max(m, s.level || 1), 0);
  }
  function dwfxGreatHallCapacity() {
    const hall = dwfxStructure("greatHall");
    if (!hall) return 8;
    const extra = hall.level >= 3 ? 16 : hall.level === 2 ? 8 : 3;
    return 8 + extra;
  }
  function dwfxRecruitDiscount() {
    const level = dwfxMaxLevel("greatHall");
    return level >= 3 ? .10 : level >= 2 ? .05 : 0;
  }
  function dwfxWorkshopDiscount() {
    const level = dwfxMaxLevel("workshop");
    return level >= 3 ? .05 : level >= 2 ? .03 : 0;
  }
  function dwfxForgeDiscount() {
    const level = dwfxMaxLevel("workshop");
    return level >= 3 ? .10 : level >= 2 ? .06 : level >= 1 ? .03 : 0;
  }
  function dwfxTreasuryBonus() {
    const level = dwfxMaxLevel("treasury");
    return level >= 3 ? .10 : level >= 2 ? .06 : level >= 1 ? .03 : 0;
  }
  function dwfxStructureCost(type) {
    const def = DWFX_BUILDING_BY_TYPE.get(type);
    if (!def) return 0;
    return Math.max(1, Math.round(def.cost * (1 - dwfxWorkshopDiscount())));
  }
  function dwfxCanUnlock(type) {
    const def = DWFX_BUILDING_BY_TYPE.get(type);
    if (!def) return false;
    if (dwfxState.deepestSeen < def.minDepth) return false;
    if (type !== "greatHall" && def.minDepth >= 120 && !dwfxStructure("greatHall")) return false;
    return true;
  }
  function dwfxUpdateLegacyCosts() {
    const disc = dwfxWorkshopDiscount();
    for (const key of Object.keys(dwfxLegacyBaseCosts)) COST[key] = Math.max(1, Math.round(dwfxLegacyBaseCosts[key] * (1 - disc)));
    markUI("hud");
  }

  const dwfxBaseRecruitCost = recruitCost;
  recruitCost = function() {
    return Math.max(1, Math.round(dwfxBaseRecruitCost() * (1 - dwfxRecruitDiscount())));
  };

  UI.btnRecruit.addEventListener("click", e => {
    if (dwarves.length >= dwfxGreatHallCapacity()) {
      e.preventDefault();
      e.stopImmediatePropagation();
      toast(`Dwarf capacity reached (${dwarves.length}/${dwfxGreatHallCapacity()}). Upgrade the Great Hall.`);
    }
  }, true);

  const dwfxBaseForgeCraftCost = forgeCraftCost;
  forgeCraftCost = function(level) {
    return Math.max(1, Math.round(dwfxBaseForgeCraftCost(level) * (1 - dwfxForgeDiscount())));
  };

  function dwfxEnsureDwarfFields(d) {
    if (!d) return d;
    if (!("relic" in d)) d.relic = null;
    if (!Array.isArray(d.perks)) d.perks = [];
    return d;
  }
  function dwfxRelicDef(item) {
    return item?.relicId ? DWFX_RELIC_BY_ID.get(item.relicId) || null : null;
  }
  function dwfxDecorateRelic(item, def, depth) {
    item.category = def.category;
    item.rarity = "legendary";
    item.relic = true;
    item.relicId = def.id;
    item.name = def.name;
    item.effectName = def.name;
    item.effectDesc = def.desc;
    item.relicFlavor = def.flavor;
    item.protected = true;
    item.identityDone = true;
    item.sell = Math.max(500, item.sell || 0);
    item.depthName = depthLayerAt(depth).name;
    return item;
  }
  function dwfxMakeRelic(def, depth) {
    return dwfxDecorateRelic(def.make(depth), def, depth);
  }
  function dwfxRelicEligible(depth) {
    const found = new Set(dwfxState.relicsFound || []);
    return DWFX_RELICS.filter(r => depth >= r.minDepth && !found.has(r.id));
  }
  function dwfxRelicChance(depth) {
    const vault = dwfxMaxLevel("greatVault");
    return clamp(.00055 + depth * .0000014 + vault * .00045, .00055, .0046);
  }

  const dwfxBaseRollLootDrop = rollLootDrop;
  rollLootDrop = function(depth, extraChance = 0) {
    const candidates = dwfxRelicEligible(depth);
    if (candidates.length && Math.random() < dwfxRelicChance(depth)) {
      const def = pickRand(candidates);
      if (!dwfxState.relicsFound.includes(def.id)) dwfxState.relicsFound.push(def.id);
      return dwfxMakeRelic(def, depth);
    }
    return dwfxBaseRollLootDrop(depth, extraChance);
  };

  function dwfxProtectItem(item, protect = true) {
    if (!item) return;
    item.protected = item.relic ? true : !!protect;
  }
  function dwfxProtectAllLegendary() {
    let n = 0;
    const all = [...loot];
    for (const d of dwarves) for (const slot of ["armor", "weapon", "pickaxe", "relic"]) if (d[slot]) all.push(d[slot]);
    for (const it of all) {
      if (it.relic || it.rarity === "legendary") {
        if (!it.protected) n++;
        it.protected = true;
      }
    }
    toast(n ? `Protected ${n} Legendary/Relic items.` : "All Legendary/Relic items are already protected.");
    markUI("inv", "dwarves", "selected");
  }

  const dwfxBaseSellItemById = sellItemById;
  sellItemById = function(itemId) {
    const it = loot.find(x => x.id === itemId);
    if (it?.protected || it?.relic) {
      toast(`${it.name} is protected.`);
      return;
    }
    return dwfxBaseSellItemById(itemId);
  };
  sellAll = function() {
    let gained = 0;
    let kept = 0;
    for (let i = loot.length - 1; i >= 0; i--) {
      const it = loot[i];
      if (it.protected || it.relic) { kept++; continue; }
      gained += Math.max(1, it.sell | 0);
      loot.splice(i, 1);
    }
    gold += gained;
    toast(gained ? `Sold unprotected equipment (+${gained}g)${kept ? ` • ${kept} protected` : ""}` : "No unprotected equipment to sell.");
    markUI("inv", "hud");
  };
  sellJunk = function() {
    let gained = 0;
    for (let i = loot.length - 1; i >= 0; i--) {
      const it = loot[i];
      if (it.protected || it.relic) continue;
      if (it.rarity === "common" || it.rarity === "uncommon") {
        gained += Math.max(1, it.sell | 0);
        loot.splice(i, 1);
      }
    }
    gold += gained;
    toast(gained ? `Sold junk (+${gained}g)` : "No unprotected junk to sell.");
    markUI("inv", "hud");
  };

  const dwfxBaseEquipSelected = equipSelected;
  equipSelected = function(itemId) {
    const d = getSelectedDwarf();
    const it = loot.find(x => x.id === itemId);
    if (!d || !it) return dwfxBaseEquipSelected(itemId);
    if (it.category !== "relic") return dwfxBaseEquipSelected(itemId);
    const i = loot.findIndex(x => x.id === itemId);
    const picked = loot.splice(i, 1)[0];
    if (d.relic) addLootItem(d.relic);
    d.relic = picked;
    picked.protected = true;
    applyDwarfStats(d);
    toast(`Equipped on ${d.name}: ${picked.name}`);
    markUI("selected", "dwarves", "inv", "hud");
  };

  function dwfxUnequipRelic() {
    const d = getSelectedDwarf();
    if (!d?.relic) return;
    addLootItem(d.relic);
    d.relic = null;
    applyDwarfStats(d);
    toast("Unequipped relic.");
    markUI("selected", "dwarves", "inv", "hud");
  }

  const dwfxBaseApplyDwarfStats = applyDwarfStats;
  applyDwarfStats = function(d) {
    dwfxEnsureDwarfFields(d);
    dwfxBaseApplyDwarfStats(d);
    if (d.armor?.relicId === "grudgeplate") {
      d.defense = clamp(d.defense + Math.min(.10, (d.armor.grudges || 0) * .01), 0, .75);
    }
    d._dwfxStatBase = {
      dmg: d.dmg, miningPower: d.miningPower, miningRate: d.miningRate,
      speed: d.speed, defense: d.defense, maxHp: d.maxHp
    };
  };

  function dwfxNear(type, x, y, extra = 0) {
    let best = null;
    let bestD = Infinity;
    for (const s of dwfxStructures(type)) {
      const def = DWFX_BUILDING_BY_TYPE.get(s.type);
      const r = (s.radius || def?.radius || 0) + extra;
      const d2 = dist2(x, y, s.x, s.y);
      if (d2 <= r * r && d2 < bestD) { bestD = d2; best = s; }
    }
    return best;
  }
  function dwfxNearest(type, x, y) {
    let best = null, bestD = Infinity;
    for (const s of dwfxStructures(type)) {
      const dd = manhattan(Math.round(x), Math.round(y), s.x, s.y);
      if (dd < bestD) { bestD = dd; best = s; }
    }
    return best;
  }
  function dwfxNow() { return nowSec(); }

  function dwfxResetDynamicStats(d) {
    if (!d._dwfxStatBase) {
      dwfxBaseApplyDwarfStats(d);
      d._dwfxStatBase = { dmg: d.dmg, miningPower: d.miningPower, miningRate: d.miningRate, speed: d.speed, defense: d.defense, maxHp: d.maxHp };
    }
    const b = d._dwfxStatBase;
    d.dmg = b.dmg; d.miningPower = b.miningPower; d.miningRate = b.miningRate; d.speed = b.speed; d.defense = b.defense;
  }
  function dwfxApplyDynamicStats(d) {
    dwfxResetDynamicStats(d);
    const dep = depthAt(Math.round(d.x), Math.round(d.y));
    if (d.pickaxe?.relicId === "heart_of_mountain") d.miningPower = Math.round(d.miningPower * (1 + Math.min(.75, dep / 900)));
    if (d.weapon?.relicId === "king_under_stone") d.dmg = Math.round(d.dmg * (1 + Math.min(.85, Math.max(0, d.level - 1) * .045)));
    if (d.weapon?.relicId === "deepward_bow") d.dmg = Math.round(d.dmg * (1 + Math.min(.55, dep / 1100)));
    if (d.weapon?.relicId === "spawnbreaker" && d.target?.kind === "spawner") d.dmg = Math.round(d.dmg * 2.6);
    if (d.armor?.relicId === "armor_deep_road" && dep > 80) d.speed *= 1 + Math.min(.28, (dep - 80) / 1500);
    if (dwfxNear("barracks", d.x, d.y)) { d.dmg = Math.round(d.dmg * 1.08); d.defense = clamp(d.defense + .05, 0, .75); }
    if (d.state !== "fight" && dwfxNear("tavern", d.x, d.y)) d.speed *= 1.06;
    if (d.state !== "fight" && dwfxNear("mushroomFarm", d.x, d.y)) d.speed *= 1.07;
    if ((d._dwfxInspiredUntil || 0) > dwfxNow()) { d.dmg = Math.round(d.dmg * 1.10); d.miningPower = Math.round(d.miningPower * 1.12); d.speed *= 1.08; }
    if ((dwfxState.rationBuffUntil || 0) > dwfxNow()) { d.miningRate *= 1.12; d.speed *= 1.10; }
    if ((d._dwfxEscapeUntil || 0) > dwfxNow()) d.speed *= 1.45;
  }

  const dwfxBaseGrantXp = grantXp;
  grantXp = function(d, amount) {
    let mult = 1;
    if (d?.relic?.relicId === "ancestors_token") mult *= 1.15;
    if (d?.level >= 6 && dwfxStructure("shrine")) mult *= 1.05;
    return dwfxBaseGrantXp(d, Math.max(1, Math.round(amount * mult)));
  };

  const dwfxBaseChoosePerkOptions = choosePerkOptions;
  choosePerkOptions = function(d) {
    const base = dwfxBaseChoosePerkOptions(d);
    if (d?.relic?.relicId !== "ancestors_token") return base;
    const preferred = d.weapon ? ["executioner", "sharpshooter", "bulwark", "vampire_oath"] :
      d.pickaxe ? ["ore_sense", "efficient_strikes", "gem_lore", "quake_miner"] : ["steady_hands", "pathfinder", "tough_skin"];
    const owned = new Set(d.perks || []);
    const candidate = preferred.map(id => PERKS[id]).find(p => p && !owned.has(p.id) && !base.some(x => x.id === p.id));
    if (candidate && base.length) base[base.length - 1] = candidate;
    return base;
  };

  const dwfxBaseStampVision = stampVision;
  stampVision = function(cx, cy, r) {
    const before = dwfxState.deepestSeen || 0;
    dwfxBaseStampVision(cx, cy, r);
    const depth = Math.min(W + H, depthAt(cx, cy) + r);
    if (depth > before) {
      dwfxState.deepestSeen = depth;
      if (depth - before >= 8) dwfxState.latestDiscovery = { x: cx, y: cy, depth, at: Date.now() };
      dwfxRefreshBuildButtons();
    }
  };

  const dwfxBaseRefreshVisibility = refreshVisibility;
  refreshVisibility = function(dt = 0) {
    dwfxBaseRefreshVisibility(dt);
    for (const d of dwarves) {
      if (d.relic?.relicId === "lantern_first_delver") {
        stampVision(Math.round(d.x), Math.round(d.y), DW_VIS_R + 5);
      }
    }
  };

  const dwfxBaseMineTile = mineTile;
  mineTile = function(tx, ty, power) {
    const d = dwfxActiveDwarf;
    const ttBefore = inBounds(tx, ty) ? tType[idx(tx, ty)] : -1;
    const out = dwfxBaseMineTile(tx, ty, power);
    if (!d || !out?.ok) return out;
    if (d.pickaxe?.relicId === "worldsplitter" && Math.random() < .11) {
      for (let oy = -1; oy <= 1; oy++) for (let ox = -1; ox <= 1; ox++) {
        if (!ox && !oy) continue;
        const x = tx + ox, y = ty + oy;
        if (!inBounds(x, y)) continue;
        const ii = idx(x, y);
        if (tType[ii] !== TILE_ROCK) continue;
        const hit = Math.max(1, Math.round(power * .42));
        if (tHp[ii] <= hit) { tType[ii] = TILE_EMPTY; tHp[ii] = 0; }
        else tHp[ii] -= hit;
      }
      fxBurst(tx, ty, "rgba(255,214,111,.80)", 1.45);
    }
    if (d.pickaxe?.relicId === "fortunes_end" && out.destroyed && ttBefore === TILE_ROCK && !out.currency && Math.random() < .012) {
      const dep = depthAt(tx, ty);
      const roll = Math.random();
      const currency = roll < .58 ? "sapphire" : roll < .88 ? "emerald" : "diamond";
      const base = currency === "sapphire" ? 95 : currency === "emerald" ? 155 : 250;
      out.currency = { kind: "currency", currency, value: base + Math.round(dep * .28) };
      fxText(tx, ty, "HIDDEN GEM", "rgba(255,214,111,.95)", .9);
    }
    return out;
  };

  function dwfxEmergencyRetreatTarget(d) {
    const inf = dwfxNearest("infirmary", d.x, d.y);
    return inf || fortress;
  }
  function dwfxAttemptVoidHook(d) {
    if (d?.relic?.relicId !== "voidhook" || d.hp > d.maxHp * .24 || (d._dwfxVoidhookCd || 0) > dwfxNow()) return;
    const target = dwfxEmergencyRetreatTarget(d);
    const tx = target.x, ty = target.y;
    if (!inBounds(tx, ty) || !isWalkable(tType[idx(tx, ty)]) || !tSeen[idx(tx, ty)]) return;
    d.x = tx; d.y = ty; d.path = null; d.target = null; d.state = "idle";
    d._dwfxVoidhookCd = dwfxNow() + 100;
    fxBurst(d.x, d.y, "rgba(190,130,255,.95)", 1.6);
    toast(`${d.name}'s Voidhook pulled them to safety.`);
  }

  function dwfxTryLift(d) {
    if (!d?.path?.length || (d._dwfxLiftCd || 0) > dwfxNow()) return;
    const lifts = dwfxStructures("liftShaft").filter(s => s.pairId);
    if (!lifts.length) return;
    const here = lifts.find(s => dist2(d.x, d.y, s.x, s.y) <= 1.4 * 1.4);
    if (!here) return;
    const mate = lifts.find(s => s.id === here.pairId);
    if (!mate || !tSeen[idx(mate.x, mate.y)]) return;
    const goal = d.path[d.path.length - 1];
    if (!goal) return;
    const direct = manhattan(Math.round(d.x), Math.round(d.y), goal.x, goal.y);
    const via = manhattan(mate.x, mate.y, goal.x, goal.y);
    if (direct - via < 45) return;
    d.x = mate.x; d.y = mate.y;
    d.path = bfsPath(mate.x, mate.y, (x, y) => x === goal.x && y === goal.y, 220000) || null;
    d._dwfxLiftCd = dwfxNow() + 5;
    fxBurst(mate.x, mate.y, "rgba(127,215,255,.75)", 1.2);
  }

  function dwfxApplyCriticalRetreat(d) {
    if (!d || d.carrying || d.hp > d.maxHp * .23) return;
    const inf = dwfxNearest("infirmary", d.x, d.y);
    if (!inf || dist2(d.x, d.y, inf.x, inf.y) <= 2) return;
    if ((d._dwfxRetreatPathAt || 0) > dwfxNow()) return;
    const p = bfsPath(Math.round(d.x), Math.round(d.y), (x, y) => x === inf.x && y === inf.y, 220000);
    if (!p) return;
    d.state = "infirmary";
    d.target = { kind: "infirmary", id: inf.id };
    d.path = p;
    d.think = 999;
    d._dwfxRetreatPathAt = dwfxNow() + 3;
  }

  const dwfxBaseUpdateDwarf = updateDwarf;
  updateDwarf = function(d, dt) {
    dwfxEnsureDwarfFields(d);
    dwfxApplyDynamicStats(d);
    dwfxAttemptVoidHook(d);
    dwfxTryLift(d);
    const priorCarry = d.carrying?.kind === "currency" ? { ...d.carrying } : null;
    const priorGold = gold;
    dwfxActiveDwarf = d;
    try {
      dwfxBaseUpdateDwarf(d, dt);
    } finally {
      dwfxActiveDwarf = null;
    }
    if (priorCarry && !d.carrying && gold > priorGold) {
      const bonus = dwfxTreasuryBonus();
      if (bonus > 0) {
        const extra = Math.max(1, Math.round((gold - priorGold) * bonus));
        gold += extra;
        const treasury = dwfxStructure("treasury");
        if (treasury) {
          treasury.goldReturned = (treasury.goldReturned || 0) + (gold - priorGold);
          treasury.deliveries = (treasury.deliveries || 0) + 1;
        }
      }
    }
    dwfxApplyCriticalRetreat(d);
  };

  const dwfxBaseDealDwarfEnemyDamage = dealDwarfEnemyDamage;
  dealDwarfEnemyDamage = function(d, e, baseDmg) {
    let out = baseDmg;
    if (d?.weapon?.relicId === "deepward_bow") {
      const dep = depthAt(Math.round(d.x), Math.round(d.y));
      out *= 1 + Math.min(.45, dep / 1100);
    }
    return dwfxBaseDealDwarfEnemyDamage(d, e, Math.round(out));
  };

  const dwfxBaseHandleEnemyDeath = handleEnemyDeath;
  handleEnemyDeath = function(e) {
    if (e?.lastHit?.kind === "dwarf") {
      const killer = dwarves.find(d => d.id === e.lastHit.id);
      if (killer?.armor?.relicId === "grudgeplate" && (e.type === "troll" || e.type === "necromancer" || (e.sourceDepth || 0) >= 500)) {
        killer.armor.grudges = Math.min(10, (killer.armor.grudges || 0) + 1);
        applyDwarfStats(killer);
        toast(`${killer.name}'s Grudgeplate recorded a Grudge (${killer.armor.grudges}/10).`);
      }
    }
    return dwfxBaseHandleEnemyDeath(e);
  };

  const dwfxBaseRemoveDead = removeDead;
  removeDead = function() {
    const now = dwfxNow();
    const infLevel = dwfxMaxLevel("infirmary");
    for (const d of dwarves) {
      if (d.hp > 0) continue;
      let saved = false;
      if (d.armor?.relicId === "last_kings_bulwark" && (d._dwfxBulwarkCd || 0) <= now) {
        d.hp = 1; d._dwfxBulwarkCd = now + 120; d._dwfxEscapeUntil = now + 12; saved = true;
        toast(`${d.name} survived through the Last King's Bulwark!`);
      } else if (infLevel >= 3 && (d._dwfxInfirmarySaveCd || 0) <= now && Math.random() < .20) {
        d.hp = 1; d._dwfxInfirmarySaveCd = now + 160; d._dwfxEscapeUntil = now + 8; saved = true;
        toast(`The Infirmary saved ${d.name} from a lethal wound.`);
      }
      if (saved) {
        const target = dwfxEmergencyRetreatTarget(d);
        const p = bfsPath(Math.round(d.x), Math.round(d.y), (x, y) => x === target.x && y === target.y, 220000);
        d.path = p || null; d.state = "infirmary"; d.think = 999;
      }
    }
    return dwfxBaseRemoveDead();
  };

  function dwfxTickCivic(dt) {
    const now = dwfxNow();
    for (const s of dwfxState.civicStructures) {
      const def = DWFX_BUILDING_BY_TYPE.get(s.type);
      if (!def) continue;
      if (s.type === "tavern") {
        for (const d of dwarves) if (d.state !== "fight" && dist2(d.x, d.y, s.x, s.y) <= def.radius ** 2 && d.hp < d.maxHp) {
          d.hp = Math.min(d.maxHp, d.hp + dt * (1.8 + s.level * .8));
        }
      }
      if (s.type === "infirmary") {
        for (const d of dwarves) if (dist2(d.x, d.y, s.x, s.y) <= def.radius ** 2 && d.hp < d.maxHp) {
          d.hp = Math.min(d.maxHp, d.hp + dt * (5 + s.level * 2.5));
          if (d.state === "infirmary" && d.hp >= d.maxHp * .72) { d.state = "idle"; d.target = null; d.think = 0; }
        }
      }
      if (s.type === "shrine") {
        for (const d of dwarves) {
          if (d.level < 6 || dist2(d.x, d.y, s.x, s.y) > def.radius ** 2) continue;
          if ((d._dwfxInspiredCd || 0) <= now) {
            d._dwfxInspiredUntil = now + 45;
            d._dwfxInspiredCd = now + 150;
            fxText(d.x, d.y, "INSPIRED", "rgba(255,214,111,.95)", 1.0);
          }
        }
      }
    }

    dwfxState.visitorTimer -= dt;
    if (dwfxStructure("tavern") && !dwfxState.visitor && dwfxState.visitorTimer <= 0) {
      const level = dwfxMaxLevel("tavern");
      const candidate = {
        id: uuid(),
        name: randomDwarfName(),
        level: Math.max(1, level + randInt(0, Math.max(1, level + 1))),
        trait: randomTrait(),
        rarity: level >= 3 && Math.random() < .18 ? "rare" : level >= 2 && Math.random() < .28 ? "uncommon" : "common"
      };
      dwfxState.visitor = candidate;
      dwfxState.visitorTimer = Math.max(55, 95 - level * 10);
      toast(`A visiting dwarf has arrived at the Tavern: ${candidate.name}.`);
      dwfxRenderVisitor();
    }

    if (dwfxStructure("mushroomFarm")) {
      const level = dwfxMaxLevel("mushroomFarm");
      dwfxState.passiveTimer -= dt;
      if (dwfxState.passiveTimer <= 0) {
        gold += 2 + level * 2;
        dwfxState.passiveTimer = Math.max(15, 28 - level * 3);
        markUI("hud");
      }
      dwfxState.rationTimer -= dt;
      if (dwfxState.rationTimer <= 0) {
        dwfxState.rationStock = Math.min(5, (dwfxState.rationStock || 0) + 1);
        dwfxState.rationTimer = Math.max(45, 80 - level * 10);
        toast("The Mushroom Farm produced Deepcap Rations.");
        dwfxRenderCivicStatus();
      }
    }
  }

  function dwfxRunSurvey() {
    const hall = dwfxStructure("surveyHall");
    if (!hall) return;
    const level = hall.level || 1;
    const range = 150 + level * 55;
    let best = null, bestScore = -Infinity;
    const minX = Math.max(0, hall.x - range), maxX = Math.min(W - 1, hall.x + range);
    const minY = Math.max(0, hall.y - range), maxY = Math.min(H - 1, hall.y + range);
    for (let y = minY; y <= maxY; y += 4) for (let x = minX; x <= maxX; x += 4) {
      if (dist2(x, y, hall.x, hall.y) > range * range) continue;
      const ii = idx(x, y);
      if (tSeen[ii]) continue;
      const tt = tType[ii];
      let score = 0, kind = "";
      if (tt === TILE_DIAMOND) { score = 1000; kind = "diamond"; }
      else if (tt === TILE_EMERALD) { score = 720; kind = "emerald"; }
      else if (tt === TILE_SAPPHIRE) { score = 520; kind = "sapphire"; }
      else if (tt === TILE_GOLD) { score = 310; kind = "gold"; }
      else if (level >= 2 && tt === TILE_SPAWNER_SEALED) { score = 450; kind = "danger"; }
      if (!score) continue;
      score += depthAt(x, y) * .15 + Math.random() * 90;
      if (score > bestScore) { bestScore = score; best = { x, y, kind }; }
    }
    if (!best) return;
    const spread = level >= 3 ? 7 : level === 2 ? 12 : 18;
    const hint = {
      x: clamp(best.x + randInt(-spread, spread), 0, W - 1),
      y: clamp(best.y + randInt(-spread, spread), 0, H - 1),
      kind: best.kind, accuracy: spread, at: Date.now()
    };
    dwfxState.surveyHints = [hint];
    dwfxState.latestDiscovery = { x: hint.x, y: hint.y, depth: depthAt(hint.x, hint.y), at: Date.now(), survey: true };
    const dirX = hint.x < fortress.x - 10 ? "west" : hint.x > fortress.x + 10 ? "east" : "";
    const dirY = hint.y < fortress.y - 10 ? "north" : hint.y > fortress.y + 10 ? "south" : "";
    const label = hint.kind === "danger" ? "unusual spawner activity" : hint.kind === "gold" ? "a rich mineral formation" : `a promising ${hint.kind} formation`;
    toast(`Surveyors report ${label} ${[dirY, dirX].filter(Boolean).join("-") || "nearby"}.`);
    dwfxDrawMinimap(true);
  }

  function dwfxTickSurvey(dt) {
    if (!dwfxStructure("surveyHall")) return;
    dwfxState.surveyTimer -= dt;
    if (dwfxState.surveyTimer <= 0) {
      dwfxState.surveyTimer = Math.max(34, 72 - dwfxMaxLevel("surveyHall") * 10);
      dwfxRunSurvey();
    }
  }

  function dwfxTick(dt) {
    dwfxTickCivic(dt);
    dwfxTickSurvey(dt);
    if (performance.now() - dwfxMapLastDraw > 700) dwfxDrawMinimap();
  }

  const dwfxBaseUpdateSpawnersAndCombat = updateSpawnersAndCombat;
  updateSpawnersAndCombat = function(dt) {
    dwfxBaseUpdateSpawnersAndCombat(dt);
    dwfxTick(dt);
  };

  function dwfxPairLift(shaft) {
    const candidates = dwfxStructures("liftShaft").filter(s => s.id !== shaft.id && !s.pairId);
    if (!candidates.length) return;
    candidates.sort((a, b) => manhattan(a.x, a.y, shaft.x, shaft.y) - manhattan(b.x, b.y, shaft.x, shaft.y));
    const mate = candidates[0];
    if (manhattan(mate.x, mate.y, shaft.x, shaft.y) < 40) return;
    mate.pairId = shaft.id;
    shaft.pairId = mate.id;
    toast("Lift Shafts connected.");
  }

  function dwfxAddCivic(type, x, y) {
    const def = DWFX_BUILDING_BY_TYPE.get(type);
    if (!def) return null;
    const structure = {
      id: uuid(), type, x, y, level: 1, hp: 360 + Math.round(def.cost * .35), maxHp: 360 + Math.round(def.cost * .35),
      radius: def.radius, deliveries: 0, goldReturned: 0, dwarvesServiced: 0
    };
    dwfxState.civicStructures.push(structure);
    if (type === "liftShaft") dwfxPairLift(structure);
    if (type === "workshop") dwfxUpdateLegacyCosts();
    if (type === "greatVault") dwfxProtectAllLegendary();
    dwfxState.selectedStructureId = structure.id;
    dwfxRefreshBuildButtons();
    dwfxRenderStructureCard();
    return structure;
  }

  function dwfxUpgradeCost(s) {
    const def = DWFX_BUILDING_BY_TYPE.get(s.type);
    return Math.round(dwfxStructureCost(s.type) * (.68 + s.level * .48));
  }
  function dwfxUpgradeSelectedStructure() {
    const s = dwfxState.civicStructures.find(x => x.id === dwfxState.selectedStructureId);
    if (!s) return;
    if (s.level >= 3) return toast("This civic building is already max level.");
    const cost = dwfxUpgradeCost(s);
    if (gold < cost) return toast(`Need ${cost}g to upgrade ${DWFX_BUILDING_BY_TYPE.get(s.type)?.name || "building"}.`);
    gold -= cost; s.level++; s.maxHp = Math.round(s.maxHp * 1.22); s.hp = s.maxHp;
    if (s.type === "workshop") dwfxUpdateLegacyCosts();
    if (s.type === "greatVault") dwfxProtectAllLegendary();
    toast(`${DWFX_BUILDING_BY_TYPE.get(s.type).name} upgraded to Lv ${s.level}.`);
    markUI("hud", "dwarves", "selected", "forge");
    dwfxRenderStructureCard();
  }

  const dwfxBaseCanBuildAt = canBuildAt;
  canBuildAt = function(x, y) {
    if (!dwfxBaseCanBuildAt(x, y)) return false;
    if (dwfxState.civicStructures.some(s => s.x === x && s.y === y)) return false;
    return true;
  };

  function dwfxModeToType(mode) {
    for (const [type, def] of Object.entries(DWFX_BUILDINGS)) if (def.mode === mode) return type;
    return null;
  }
  function dwfxBuildModeName(mode) {
    const def = DWFX_BUILDING_BY_MODE.get(mode);
    if (def) return def.name;
    return mode === BUILD_DEPOT ? "Depot" : mode === BUILD_ARROW ? "Arrow Tower" : mode === BUILD_CANNON ? "Cannon Tower" :
      mode === BUILD_FROST ? "Frost Tower" : mode === BUILD_HEAL ? "Healing Tower" : mode === BUILD_HASTE ? "Haste Tower" :
      mode === BUILD_GUARD ? "Guard Post" : "Structure";
  }
  function dwfxLegacyModeCost(mode) {
    return mode === BUILD_DEPOT ? COST.depot : mode === BUILD_ARROW ? COST.arrow : mode === BUILD_CANNON ? COST.cannon :
      mode === BUILD_FROST ? COST.frost : mode === BUILD_HEAL ? COST.heal : mode === BUILD_HASTE ? COST.haste :
      mode === BUILD_GUARD ? COST.guard : 0;
  }
  function dwfxBuildAt(mode, x, y) {
    if (!canBuildAt(x, y)) return toast("Can't build there.");
    const type = dwfxModeToType(mode);
    if (type) {
      const def = DWFX_BUILDING_BY_TYPE.get(type);
      if (!dwfxCanUnlock(type)) return toast(`${def.name} is not unlocked at your current depth.`);
      if (def.unique && dwfxStructure(type)) return toast(`${def.name} already exists. Select it to upgrade.`);
      const cost = dwfxStructureCost(type);
      if (gold < cost) return toast(`Need ${cost}g.`);
      gold -= cost; dwfxAddCivic(type, x, y);
      toast(`${def.name} built.`);
    } else {
      const cost = dwfxLegacyModeCost(mode);
      if (gold < cost) return toast("Not enough gold.");
      gold -= cost;
      if (mode === BUILD_DEPOT) addDepot(x, y);
      else if (mode === BUILD_ARROW) addTower("arrow", x, y);
      else if (mode === BUILD_CANNON) addTower("cannon", x, y);
      else if (mode === BUILD_FROST) addTower("frost", x, y);
      else if (mode === BUILD_HEAL) addTower("heal", x, y);
      else if (mode === BUILD_HASTE) addTower("haste", x, y);
      else if (mode === BUILD_GUARD) { addTower("guard", x, y); rebuildGuardLists(); fillGuardPosts(); }
      else return;
      toast(`${dwfxBuildModeName(mode)} built.`);
    }
    dwfxPendingPlacement = null;
    setBuildMode(BUILD_NONE);
    dwfxRenderMobileConfirm();
    markUI("hud", "dwarves", "inv", "selected");
    dwfxDrawMinimap(true);
  }

  function dwfxEstimatePlacement(mode, x, y) {
    const valid = canBuildAt(x, y);
    const type = dwfxModeToType(mode);
    const def = type ? DWFX_BUILDING_BY_TYPE.get(type) : null;
    let radius = def?.radius || (mode === BUILD_DEPOT ? 12 : mode === BUILD_HEAL ? 7.5 : mode === BUILD_GUARD ? 28 : 9);
    let score = valid ? 38 : -999;
    let reason = valid ? "Usable tunnel location." : "Cannot build on this tile.";
    let coverage = 0, threats = 0, savings = 0;
    if (valid) {
      coverage = dwarves.filter(d => dist2(d.x, d.y, x, y) <= radius * radius).length;
      threats = enemies.filter(e => dist2(e.x, e.y, x, y) <= (radius * 1.8) ** 2).length +
        spawners.filter(s => dist2(s.x, s.y, x, y) <= (radius * 2.4) ** 2).length;
      score += coverage * 6 + Math.min(24, threats * 9);
      if (mode === BUILD_DEPOT) {
        const nearest = nearestDropPoint(x, y);
        savings = Math.max(0, manhattan(x, y, fortress.x, fortress.y) - manhattan(x, y, nearest.x, nearest.y));
        const miners = dwarves.filter(d => d.pickaxe && dist2(d.x, d.y, x, y) <= 35 ** 2).length;
        score += miners * 9 + Math.min(28, savings * .18);
        reason = `${miners} miners nearby • approx. ${Math.round(savings)} travel tiles saved`;
      } else if (mode === BUILD_HEAL || type === "infirmary") {
        const injured = dwarves.filter(d => d.hp < d.maxHp * .75 && dist2(d.x, d.y, x, y) <= (radius * 1.7) ** 2).length;
        score += injured * 14;
        reason = `${coverage} dwarves covered • ${injured} currently wounded`;
      } else if (mode === BUILD_GUARD || type === "barracks") {
        score += threats * 13;
        reason = `${coverage} dwarves covered • ${threats} nearby threats`;
      } else if (type === "liftShaft") {
        const mate = dwfxStructures("liftShaft").filter(s => !s.pairId).sort((a, b) => manhattan(a.x, a.y, x, y) - manhattan(b.x, b.y, x, y))[0];
        savings = mate ? manhattan(mate.x, mate.y, x, y) : 0;
        score += Math.min(55, savings * .15);
        reason = mate ? `Nearest unpaired shaft • estimated trip reduction ${savings} tiles` : "Build another distant Lift Shaft to create a pair.";
      } else if (type === "surveyHall") {
        score += Math.min(35, depthAt(x, y) * .06);
        reason = `Survey influence reaches beyond depth ${depthAt(x, y)}.`;
      } else {
        reason = `${coverage} dwarves nearby • ${threats} active threats`;
      }
      const overlap = dwfxState.civicStructures.some(s => dist2(s.x, s.y, x, y) < (Math.max(4, radius * .5)) ** 2);
      if (overlap) score -= 22;
    }
    const quality = !valid ? "INVALID" : score >= 78 ? "EXCELLENT" : score >= 52 ? "GOOD" : "POOR";
    return { valid, quality, score, reason, radius, coverage, threats, savings };
  }

  function dwfxCurrentPreviewTile() {
    if (dwfxPendingPlacement) return dwfxPendingPlacement;
    const tx = Math.round(mouseWorld.x), ty = Math.round(mouseWorld.y);
    return { mode: buildMode, x: tx, y: ty };
  }

  function dwfxRenderPlacementCard() {
    if (!dwfxPlacementCard) return;
    if (buildMode === BUILD_NONE && !dwfxPendingPlacement) { dwfxPlacementCard.style.display = "none"; return; }
    const p = dwfxCurrentPreviewTile();
    if (!inBounds(p.x, p.y)) { dwfxPlacementCard.style.display = "none"; return; }
    const ev = dwfxEstimatePlacement(p.mode, p.x, p.y);
    const type = dwfxModeToType(p.mode);
    const cost = type ? dwfxStructureCost(type) : dwfxLegacyModeCost(p.mode);
    dwfxPlacementCard.style.display = "block";
    dwfxPlacementCard.innerHTML = `
      <div style="display:flex;justify-content:space-between;gap:10px"><b>${dwfxEsc(dwfxBuildModeName(p.mode))}</b>
        <span class="dwfx-quality ${ev.quality.toLowerCase()}">${ev.quality}</span></div>
      <div class="dwfx-cardline"><span>Cost</span><b>${cost}g</b></div>
      <div class="dwfx-cardline"><span>Coverage</span><span>${ev.coverage} dwarves • ${ev.threats} threats</span></div>
      <div class="tiny" style="margin-top:5px">${dwfxEsc(ev.reason)}</div>`;
  }

  function dwfxRenderMobileConfirm() {
    const bar = document.getElementById("dwfxMobileConfirm");
    if (!bar) return;
    bar.classList.toggle("show", !!dwfxPendingPlacement);
    if (dwfxPendingPlacement) {
      const p = dwfxPendingPlacement;
      const ev = dwfxEstimatePlacement(p.mode, p.x, p.y);
      bar.querySelector("[data-dwfx-confirm]").disabled = !ev.valid;
      bar.querySelector("[data-dwfx-confirm]").textContent = `Build • ${ev.quality}`;
    }
  }

  canvas.addEventListener("mousedown", e => {
    if (!isMobileLayout() || e.button !== 0 || buildMode === BUILD_NONE) return;
    const rect = canvas.getBoundingClientRect();
    const w = screenToWorld(e.clientX - rect.left, e.clientY - rect.top);
    const tx = Math.round(w.x), ty = Math.round(w.y);
    if (!inBounds(tx, ty)) return;
    e.preventDefault();
    e.stopImmediatePropagation();
    dwfxPendingPlacement = { mode: buildMode, x: tx, y: ty };
    dwfxLastWorldClick = { x: tx, y: ty };
    dwfxRenderPlacementCard();
    dwfxRenderMobileConfirm();
  }, true);

  canvas.addEventListener("mousedown", e => {
    if (e.button !== 0 || isMobileLayout()) return;
    if (buildMode >= DWFX_BUILD_BASE) {
      const rect = canvas.getBoundingClientRect();
      const w = screenToWorld(e.clientX - rect.left, e.clientY - rect.top);
      const tx = Math.round(w.x), ty = Math.round(w.y);
      if (inBounds(tx, ty)) dwfxBuildAt(buildMode, tx, ty);
      return;
    }
    if (buildMode !== BUILD_NONE) return;
    const rect = canvas.getBoundingClientRect();
    const w = screenToWorld(e.clientX - rect.left, e.clientY - rect.top);
    const near = dwfxState.civicStructures.find(s => Math.hypot(s.x - w.x, s.y - w.y) < .8);
    if (near) {
      dwfxState.selectedStructureId = near.id;
      dwfxRenderStructureCard();
    }
  });

  const dwfxBaseSetBuildMode = setBuildMode;
  setBuildMode = function(m) {
    dwfxBaseSetBuildMode(m);
    document.querySelectorAll("[data-dwfx-build]").forEach(b => b.classList.toggle("primary", Number(b.dataset.dwfxBuild) === m));
    if (m === BUILD_NONE) dwfxPendingPlacement = null;
    dwfxRenderMobileConfirm();
    dwfxRenderPlacementCard();
  };

  function dwfxDrawCivic() {
    for (const s of dwfxState.civicStructures) {
      if (!tSeen[idx(s.x, s.y)]) continue;
      const def = DWFX_BUILDING_BY_TYPE.get(s.type);
      if (!def) continue;
      const p = worldToScreen(s.x, s.y), ts = p.s;
      ctx.save();
      ctx.shadowColor = "rgba(255,184,92,.36)";
      ctx.shadowBlur = ts * .32;
      ctx.fillStyle = "rgba(105,80,50,.95)";
      ctx.strokeStyle = "rgba(255,210,140,.74)";
      ctx.lineWidth = Math.max(1, ts * .055);
      ctx.beginPath(); ctx.roundRect?.(p.x + ts * .12, p.y + ts * .18, ts * .76, ts * .66, ts * .1);
      if (ctx.roundRect) { ctx.fill(); ctx.stroke(); } else { ctx.fillRect(p.x + ts * .12, p.y + ts * .18, ts * .76, ts * .66); }
      ctx.shadowBlur = 0;
      ctx.fillStyle = "rgba(255,241,205,.96)";
      ctx.font = `${Math.max(9, ts * .36)}px system-ui`;
      ctx.textAlign = "center"; ctx.textBaseline = "middle";
      ctx.fillText(def.icon, p.x + ts * .5, p.y + ts * .49);
      ctx.fillStyle = "rgba(255,220,160,.9)";
      ctx.font = `900 ${Math.max(7, ts * .14)}px system-ui`;
      ctx.fillText(`L${s.level}`, p.x + ts * .5, p.y + ts * .80);
      if (dwfxState.selectedStructureId === s.id) {
        ctx.strokeStyle = "rgba(255,214,111,.95)";
        ctx.lineWidth = Math.max(2, ts * .1);
        ctx.strokeRect(p.x + ts * .05, p.y + ts * .05, ts * .9, ts * .9);
        if (def.radius > 0) {
          ctx.globalAlpha = .33;
          ctx.beginPath(); ctx.arc(p.x + ts * .5, p.y + ts * .5, def.radius * ts, 0, Math.PI * 2); ctx.stroke();
        }
      }
      ctx.restore();
    }
  }

  function dwfxDrawPlacementOverlay() {
    const preview = dwfxCurrentPreviewTile();
    if ((buildMode === BUILD_NONE && !dwfxPendingPlacement) || !inBounds(preview.x, preview.y)) return;
    const ev = dwfxEstimatePlacement(preview.mode, preview.x, preview.y);
    const p = worldToScreen(preview.x, preview.y), ts = p.s;
    ctx.save();
    const col = ev.quality === "EXCELLENT" ? "rgba(123,224,164,.82)" : ev.quality === "GOOD" ? "rgba(127,215,255,.80)" :
      ev.quality === "POOR" ? "rgba(255,214,111,.80)" : "rgba(255,90,110,.82)";
    ctx.strokeStyle = col;
    ctx.fillStyle = col.replace(".8", ".12");
    ctx.lineWidth = Math.max(2, ts * .10);
    ctx.beginPath(); ctx.arc(p.x + ts * .5, p.y + ts * .5, ev.radius * ts, 0, Math.PI * 2); ctx.stroke();
    ctx.fillRect(p.x + ts * .12, p.y + ts * .12, ts * .76, ts * .76);
    ctx.font = `900 ${Math.max(10, ts * .19)}px system-ui`; ctx.textAlign = "center";
    ctx.fillStyle = col; ctx.fillText(ev.quality, p.x + ts * .5, p.y - ts * .35);
    ctx.restore();
  }

  const dwfxBaseDraw = draw;
  draw = function() {
    dwfxBaseDraw();
    dwfxDrawCivic();
    dwfxDrawPlacementOverlay();
    dwfxRenderPlacementCard();
  };

  function dwfxStateActivity(d) {
    const dep = depthAt(Math.round(d.x), Math.round(d.y));
    const layer = depthLayerAt(dep)?.name || "Deepwild";
    const map = {
      idle: "Idle", fight: "Fighting", mine: "Mining Frontier", mineOre: "Mining Ore", return: "Returning Loot",
      guard: "Guarding Fortress", heal: "Healing", infirmary: "Retreating to Infirmary", focusPatrol: "Exploring Focus Region"
    };
    let text = map[d.state] || String(d.state || "Idle").replace(/([A-Z])/g, " $1");
    if (d.state === "fight" && d.target?.kind === "spawner") text = "Fighting Spawner";
    if (d.carrying) text = "Returning Loot";
    return `${text} — ${layer} · Depth ${dep}`;
  }
  function dwfxAvatarFor(d) {
    const name = d.name || "Dwarf";
    let h = 0; for (let i = 0; i < name.length; i++) h = ((h << 5) - h + name.charCodeAt(i)) | 0;
    const faces = ["🧔", "🧔‍♂️", "⛏️", "🪓"];
    return faces[Math.abs(h) % faces.length];
  }
  function dwfxEqHtml(slot, it) {
    const icon = slot === "armor" ? "🛡" : slot === "weapon" ? "⚔" : "⛏";
    if (!it) return `<span class="dwfx-eq empty" data-slot="${slot}">${icon} EMPTY</span>`;
    return `<span class="dwfx-eq ${it.relic ? "dwfx-relic-border" : ""}" data-slot="${slot}" data-rarity="${dwfxEsc(it.rarity)}" title="${dwfxEsc(it.name)}">${icon} ${dwfxRarityLetter(it)} · ${dwfxEsc(it.name)}</span>`;
  }
  function dwfxEquipmentNeedCounts() {
    return {
      armor: dwarves.filter(d => !d.armor).length,
      weapon: dwarves.filter(d => !d.weapon).length,
      pickaxe: dwarves.filter(d => !d.pickaxe).length
    };
  }
  function dwfxVisibleRosterOrder() {
    const ids = [...UI.dwarfList.querySelectorAll("[data-dwarf-id]")].map(el => el.dataset.dwarfId);
    const ordered = ids.map(id => dwarves.find(d => d.id === id)).filter(Boolean);
    const seen = new Set(ordered.map(d => d.id));
    for (const d of dwarves) if (!seen.has(d.id)) ordered.push(d);
    return ordered;
  }

  function dwfxAutoEquipEmptySlots() {
    const order = dwfxVisibleRosterOrder();
    const counts = { armor: 0, weapon: 0, pickaxe: 0 };
    for (const category of ["armor", "weapon", "pickaxe"]) {
      const slot = category;
      const eligible = order.filter(d => !d[slot]);
      const items = loot.filter(it => it.category === category).slice().sort((a, b) => itemScore(b) - itemScore(a));
      const n = Math.min(eligible.length, items.length);
      for (let i = 0; i < n; i++) {
        const d = eligible[i];
        if (d[slot]) continue; // critical safety rule: never replace existing gear
        const it = items[i];
        const li = loot.findIndex(x => x.id === it.id);
        if (li < 0) continue;
        d[slot] = loot.splice(li, 1)[0];
        applyDwarfStats(d);
        counts[category]++;
      }
    }
    const needs = dwfxEquipmentNeedCounts();
    const empty = needs.armor + needs.weapon + needs.pickaxe;
    toast(`Auto Equipped • ${counts.weapon} Weapons • ${counts.armor} Armor • ${counts.pickaxe} Pickaxes • ${empty} empty slots remain`);
    markUI("dwarves", "selected", "inv", "hud");
  }

  function dwfxRenderNeedHeader() {
    const host = document.getElementById("dwfxNeedHeader");
    if (!host) return;
    const n = dwfxEquipmentNeedCounts(), total = n.armor + n.weapon + n.pickaxe;
    host.innerHTML = `
      <div style="display:flex;justify-content:space-between;gap:8px;align-items:center">
        <b>${total ? "Equipment Needed" : "✓ All dwarves equipped"}</b><span class="pill">${dwarves.length}/${dwfxGreatHallCapacity()} capacity</span>
      </div>
      <div class="dwfx-needs">
        <button class="dwfx-need ${dwfxRosterNeedFilter === "armor" ? "on" : ""}" data-dwfx-need="armor">🛡 ${n.armor}</button>
        <button class="dwfx-need ${dwfxRosterNeedFilter === "weapon" ? "on" : ""}" data-dwfx-need="weapon">⚔ ${n.weapon}</button>
        <button class="dwfx-need ${dwfxRosterNeedFilter === "pickaxe" ? "on" : ""}" data-dwfx-need="pickaxe">⛏ ${n.pickaxe}</button>
        <button class="dwfx-need" data-dwfx-need="all">All</button>
      </div>
      <button class="dwfx-auto" id="dwfxAutoEquip" type="button">⭐ Auto Equip Empty Slots</button>`;
    host.querySelector("#dwfxAutoEquip")?.addEventListener("click", dwfxAutoEquipEmptySlots);
    host.querySelectorAll("[data-dwfx-need]").forEach(b => b.addEventListener("click", () => {
      const val = b.dataset.dwfxNeed;
      dwfxRosterNeedFilter = val === "all" || dwfxRosterNeedFilter === val ? null : val;
      markUI("dwarves");
    }));
  }

  updateDwarfListUI = function() {
    UI.dwarfList.innerHTML = "";
    const source = dwarves.filter(d => !dwfxRosterNeedFilter || !d[dwfxRosterNeedFilter]);
    for (const d of source) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "dwarfBtn dwfx-card" + ((selected.kind === "dwarf" && selected.id === d.id) ? " sel" : "");
      btn.dataset.dwarfId = d.id;
      const hpPct = clamp((d.hp || 0) / Math.max(1, d.maxHp || 1), 0, 1) * 100;
      const perks = (d.perks || []).map(id => PERKS[id]).filter(Boolean);
      btn.innerHTML = `
        <span class="dwfx-avatar" aria-hidden="true">${dwfxAvatarFor(d)}</span>
        <span class="dwfx-card-main">
          <span class="dwfx-name"><span>${dwfxEsc(d.name)}</span><span>Lv. ${d.level}</span></span>
          <span class="dwfx-activity">${dwfxEsc(dwfxStateActivity(d))}</span>
          <span class="dwfx-activity">❤️ ${Math.round(d.hp)} / ${d.maxHp}${d.relic ? ` • ✦ ${dwfxEsc(d.relic.name)}` : ""}</span>
          <span class="dwfx-equipment">${dwfxEqHtml("armor", d.armor)}${dwfxEqHtml("weapon", d.weapon)}${dwfxEqHtml("pickaxe", d.pickaxe)}</span>
          <span class="dwfx-perks">${perks.length ? perks.map(p => `<span class="dwfx-perk" title="${dwfxEsc(p.desc)}">${dwfxEsc(p.name.slice(0, 1))}</span>`).join("") : `<span class="tiny">No perks yet</span>`}</span>
        </span>
        <span class="dwfx-hpbar"><i style="width:${hpPct}%"></i></span>`;
      UI.dwarfList.appendChild(btn);
    }
    UI.dwCount.textContent = `${dwarves.length}/${dwfxGreatHallCapacity()}`;
    dwfxRenderNeedHeader();
  };

  UI.dwarfList.addEventListener("click", e => {
    const btn = e.target.closest("[data-dwarf-id]");
    if (!btn) return;
    const d = dwarves.find(x => x.id === btn.dataset.dwarfId);
    if (!d) return;
    cam.x = d.x; cam.y = d.y;
    markUI("selected", "dwarves");
  });

  const dwfxBaseUpdateSelectedUI = updateSelectedUI;
  updateSelectedUI = function() {
    dwfxBaseUpdateSelectedUI();
    const d = getSelectedDwarf();
    const slot = document.getElementById("dwfxRelicSlot");
    if (slot) {
      slot.querySelector(".val").textContent = d?.relic ? `${d.relic.name} — ${d.relic.effectDesc || "Unique relic"}` : "—";
      slot.querySelector("button").disabled = !d?.relic;
    }
  };

  function dwfxItemIsUpgrade(it) {
    if (!["armor", "weapon", "pickaxe"].includes(it.category)) return false;
    return dwarves.some(d => itemScore(it) > itemScore(d[it.category]));
  }
  function dwfxItemNeeded(it) {
    return ["armor", "weapon", "pickaxe"].includes(it.category) && dwarves.some(d => !d[it.category]);
  }
  function dwfxEquippedItems() {
    const out = [];
    for (const d of dwarves) for (const slot of ["armor", "weapon", "pickaxe", "relic"]) {
      const it = d[slot];
      if (it) out.push({ ...it, _dwfxEquippedBy: d.name, _dwfxEquippedSlot: slot });
    }
    return out;
  }

  recomputeInvFiltered = function() {
    const q = invFilter.q, cat = invFilter.cat, rar = invFilter.rar;
    const source = dwfxInventoryMode === "equipped" ? dwfxEquippedItems() : loot;
    invFiltered = source.filter(it => {
      if (cat !== "all" && it.category !== cat) return false;
      if (rar !== "all" && it.rarity !== rar) return false;
      if (dwfxInventoryMode === "relics" && !it.relic) return false;
      if (dwfxInventoryMode === "legendary" && !(it.relic || it.rarity === "legendary")) return false;
      if (dwfxInventoryMode === "upgrades" && !dwfxItemIsUpgrade(it)) return false;
      if (dwfxInventoryMode === "needed" && !dwfxItemNeeded(it)) return false;
      if (dwfxInventoryMode === "armor" && it.category !== "armor") return false;
      if (dwfxInventoryMode === "weapon" && it.category !== "weapon") return false;
      if (dwfxInventoryMode === "pickaxe" && it.category !== "pickaxe") return false;
      if (q) {
        const hay = `${it.name || ""} ${it.category || ""} ${it.rarity || ""} ${it.weaponType || ""} ${it.armorTier || ""} ${it.effectName || ""} ${it.depthName || ""}`.toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    });
    UI.invCount.textContent = `${loot.length}/${LOOT_LIMIT}`;
    UI.invSpacer.style.height = (invFiltered.length * INV_ROW_H) + "px";
    document.querySelectorAll("[data-dwfx-invfilter]").forEach(b => b.classList.toggle("on", b.dataset.dwfxInvfilter === dwfxInventoryMode));
  };

  const dwfxBaseEnsureRowPool = ensureRowPool;
  ensureRowPool = function() {
    dwfxBaseEnsureRowPool();
    for (const row of rowPool) {
      if (row._protect) continue;
      const b = document.createElement("button");
      b.className = "btn";
      b.style.height = "30px"; b.style.padding = "0 8px";
      b.dataset.act = "protect"; b.textContent = "Protect";
      row.querySelector(".actions")?.appendChild(b);
      row._protect = b;
    }
  };

  renderInv = function() {
    invNeedRender = false;
    ensureRowPool();
    const scrollTop = UI.invScroll.scrollTop;
    const start = Math.floor(scrollTop / INV_ROW_H);
    const sel = getSelectedDwarf();
    const equippedView = dwfxInventoryMode === "equipped";
    for (let i = 0; i < rowPool.length; i++) {
      const idx2 = start + i, row = rowPool[i];
      if (idx2 < 0 || idx2 >= invFiltered.length) { row.style.display = "none"; continue; }
      const it = invFiltered[idx2];
      row.style.display = "flex";
      row.style.transform = `translateY(${idx2 * INV_ROW_H}px)`;
      row._name.textContent = `${it.relic ? "✦ " : ""}${it.name}`;
      let sub = "";
      if (it.category === "armor") sub = `Armor • ${it.armorTier || "relic"} • DEF ${Math.round((it.defense || 0) * 100)}% • +HP ${it.bonusHp || 0}`;
      else if (it.category === "weapon") sub = `Weapon • ${it.weaponType || "relic"} • DMG ${it.dmg || 0} • ${it.ranged ? `RNG ${it.range}` : "melee"}`;
      else if (it.category === "pickaxe") sub = `Pickaxe • PWR ${it.miningPower || 0} • SPD x${Number(it.miningRate || 1).toFixed(2)}`;
      else sub = `Unique Relic • ${it.effectDesc || "utility relic"}`;
      if (it._dwfxEquippedBy) sub += ` • Equipped: ${it._dwfxEquippedBy}`;
      else sub += ` • Sell ${it.sell || 0}g`;
      if (it.effectName && !it.relic) sub += ` • ✨ ${it.effectName}`;
      if (it.vaulted) sub += " • 🔐 Great Vault";
      else if (it.protected || it.relic) sub += " • 🔒 Protected";
      row._sub.textContent = sub;
      row._rar.className = `rar ${it.rarity || "legendary"}`;
      row._rar.textContent = it.relic ? "✦ RELIC" : String(it.rarity || "").toUpperCase();
      row._equip.dataset.id = it.id;
      row._sell.dataset.id = it.id;
      row._protect.dataset.id = it.id;
      row._equip.disabled = equippedView || !sel;
      row._sell.disabled = equippedView || !!it.protected || !!it.relic;
      row._protect.disabled = equippedView || !!it.relic;
      row._protect.textContent = it.protected || it.relic ? "🔒" : "Protect";
      row.classList.toggle("dwfx-relic-border", !!it.relic);
    }
  };

  UI.invContent.addEventListener("click", e => {
    const btn = e.target.closest('button[data-act="protect"][data-id]');
    if (!btn) return;
    const it = loot.find(x => x.id === btn.dataset.id);
    if (!it) return;
    dwfxProtectItem(it, !it.protected);
    toast(`${it.name} ${it.protected ? "protected" : "unprotected"}.`);
    markUI("inv");
  });

  function dwfxShowRelicModal(item) {
    if (!item?.relic || !dwfxRelicModal) return;
    if (!dwfxState.relicAnnouncements.includes(item.relicId)) dwfxState.relicAnnouncements.push(item.relicId);
    const def = dwfxRelicDef(item);
    dwfxRelicModal.querySelector("[data-dwfx-relic-name]").textContent = item.name;
    dwfxRelicModal.querySelector("[data-dwfx-relic-flavor]").textContent = `"${item.relicFlavor || def?.flavor || ""}"`;
    dwfxRelicModal.querySelector("[data-dwfx-relic-desc]").textContent = item.effectDesc || def?.desc || "Unique relic";
    dwfxRelicModal.dataset.itemId = item.id;
    const vaultBtn = dwfxRelicModal.querySelector("[data-dwfx-relic-vault]");
    if (vaultBtn) {
      vaultBtn.disabled = !dwfxStructure("greatVault");
      vaultBtn.textContent = dwfxStructure("greatVault") ? "Send to Vault" : "Vault Locked";
    }
    dwfxRelicModal.classList.add("open");
    paused = true; UI.btnPause.textContent = "Resume";
  }
  function dwfxCloseRelicModal() {
    if (!dwfxRelicModal) return;
    dwfxRelicModal.classList.remove("open");
    paused = false; UI.btnPause.textContent = "Pause";
  }

  const dwfxBaseAddLootItem = addLootItem;
  addLootItem = function(item) {
    if (item?.relic) item.protected = true;
    const wasAnnounced = item?.relic && dwfxState.relicAnnouncements.includes(item.relicId);
    dwfxBaseAddLootItem(item);
    if (item?.relic && !wasAnnounced) setTimeout(() => dwfxShowRelicModal(item), 0);
  };

  function dwfxRenderVisitor() {
    const box = document.getElementById("dwfxVisitor");
    if (!box) return;
    const v = dwfxState.visitor;
    if (!v) { box.innerHTML = `<div class="tiny">No visiting candidate right now. Higher Tavern levels improve visitors.</div>`; return; }
    box.innerHTML = `<b>${dwfxEsc(v.name)} · Lv ${v.level}</b><div class="tiny">${dwfxEsc(v.trait?.name || "Traveler")} • ${dwfxEsc(v.rarity)} visitor</div>
      <button id="dwfxRecruitVisitor" type="button" style="margin-top:6px">Recruit (${recruitCost()}g)</button>`;
    box.querySelector("#dwfxRecruitVisitor")?.addEventListener("click", () => {
      if (dwarves.length >= dwfxGreatHallCapacity()) return toast("Dwarf capacity is full.");
      const cost = recruitCost(); if (gold < cost) return toast("Not enough gold.");
      gold -= cost; recruitCount++;
      const d = makeDwarf();
      d.name = v.name; d.level = v.level; d.trait = v.trait; d.nextXp = levelXpNeed(d.level);
      if (v.rarity === "rare") d.weapon = mkItemWeapon("rare");
      else if (v.rarity === "uncommon") d.pickaxe = mkItemPickaxe("uncommon");
      applyDwarfStats(d); d.hp = d.maxHp; dwarves.push(d);
      dwfxState.visitor = null;
      toast(`${d.name} joined from the Tavern.`);
      markUI("all"); dwfxRenderVisitor();
    });
  }
  function dwfxUseRation() {
    if ((dwfxState.rationStock || 0) <= 0) return toast("No Deepcap Rations are ready.");
    dwfxState.rationStock--; dwfxState.rationBuffUntil = dwfxNow() + 45;
    toast("Deepcap Rations issued: mining and movement boosted for 45s.");
    dwfxRenderCivicStatus();
  }
  function dwfxRenderCivicStatus() {
    const box = document.getElementById("dwfxCivicStatus");
    if (!box) return;
    box.innerHTML = `<div class="small"><b>Settlement</b> • depth record ${Math.round(dwfxState.deepestSeen || 0)}</div>
      <div class="tiny" style="margin-top:4px">${dwfxState.civicStructures.length} civic buildings • dwarf capacity ${dwarves.length}/${dwfxGreatHallCapacity()}</div>
      <div class="row" style="margin-top:7px"><button id="dwfxUseRation" type="button">🍄 Rations (${dwfxState.rationStock || 0})</button>
      <button id="dwfxProtectLegendary" type="button">🔒 Protect Legendary</button></div>`;
    box.querySelector("#dwfxUseRation")?.addEventListener("click", dwfxUseRation);
    box.querySelector("#dwfxProtectLegendary")?.addEventListener("click", dwfxProtectAllLegendary);
  }

  function dwfxRefreshBuildButtons() {
    document.querySelectorAll("[data-dwfx-build]").forEach(btn => {
      const type = btn.dataset.dwfxType, def = DWFX_BUILDING_BY_TYPE.get(type);
      const uniqueBuilt = def?.unique && !!dwfxStructure(type);
      const unlocked = dwfxCanUnlock(type);
      btn.disabled = !unlocked || uniqueBuilt;
      const cost = dwfxStructureCost(type);
      btn.innerHTML = `<b>${def.icon} ${dwfxEsc(def.name)} · ${cost}g</b><span>${uniqueBuilt ? `Built · select to upgrade` : unlocked ? dwfxEsc(def.effect) : `Unlock at depth ${def.minDepth}${def.minDepth >= 120 && !dwfxStructure("greatHall") ? " + Great Hall" : ""}`}</span>`;
    });
    dwfxRenderCivicStatus();
  }

  function dwfxRenderStructureCard() {
    if (!dwfxStructureCard) return;
    const s = dwfxState.civicStructures.find(x => x.id === dwfxState.selectedStructureId);
    if (!s) { dwfxStructureCard.style.display = "none"; return; }
    const def = DWFX_BUILDING_BY_TYPE.get(s.type);
    if (!def) { dwfxStructureCard.style.display = "none"; return; }
    const up = s.level < 3 ? dwfxUpgradeCost(s) : 0;
    const lift = s.type === "liftShaft" ? (s.pairId ? "Connected lift network" : "Waiting for a distant unpaired shaft") : "";
    dwfxStructureCard.style.display = "block";
    dwfxStructureCard.innerHTML = `<div style="display:flex;justify-content:space-between;gap:8px"><b>${def.icon} ${dwfxEsc(def.name)} · Lv ${s.level}</b>
      <button data-dwfx-close-structure type="button" style="height:26px">×</button></div>
      <div class="tiny" style="margin-top:5px">${dwfxEsc(def.effect)}${lift ? ` • ${dwfxEsc(lift)}` : ""}</div>
      <div class="dwfx-cardline"><span>HP</span><span>${Math.round(s.hp)}/${s.maxHp}</span></div>
      ${s.type === "treasury" ? `<div class="dwfx-cardline"><span>Returned treasure</span><span>${Math.round(s.goldReturned || 0)}g · ${s.deliveries || 0} deliveries</span></div>` : ""}
      <div class="row" style="margin-top:7px">${s.level < 3 ? `<button data-dwfx-upgrade type="button">Upgrade (${up}g)</button>` : `<span class="pill">MAX LEVEL</span>`}
      <button data-dwfx-focus-structure type="button">Focus</button></div>`;
    dwfxStructureCard.querySelector("[data-dwfx-close-structure]")?.addEventListener("click", () => {
      dwfxState.selectedStructureId = null; dwfxRenderStructureCard();
    });
    dwfxStructureCard.querySelector("[data-dwfx-upgrade]")?.addEventListener("click", dwfxUpgradeSelectedStructure);
    dwfxStructureCard.querySelector("[data-dwfx-focus-structure]")?.addEventListener("click", () => { cam.x = s.x; cam.y = s.y; });
  }

  function dwfxMapPoint(x, y, canvasEl) {
    return { x: x / W * canvasEl.width, y: y / H * canvasEl.height };
  }
  function dwfxMapFocus(x, y) {
    cam.x = clamp(x, 0, W - 1); cam.y = clamp(y, 0, H - 1);
    dwfxDrawMinimap(true);
  }
  function dwfxDeepestKnownPoint() {
    let best = { x: fortress.x, y: fortress.y, d: 0 };
    for (const d of dwarves) {
      const dep = depthAt(Math.round(d.x), Math.round(d.y));
      if (dep > best.d) best = { x: d.x, y: d.y, d: dep };
    }
    if (dwfxState.latestDiscovery && dwfxState.latestDiscovery.depth > best.d) best = { x: dwfxState.latestDiscovery.x, y: dwfxState.latestDiscovery.y, d: dwfxState.latestDiscovery.depth };
    return best;
  }

  function dwfxDrawMinimap(force = false) {
    const c = document.getElementById("dwfxMap");
    const panel = document.getElementById("dwfxMapPanel");
    if (!c || !panel?.classList.contains("open")) return;
    const now = performance.now();
    if (!force && now - dwfxMapLastDraw < 650) return;
    dwfxMapLastDraw = now;
    const m = c.getContext("2d");
    const w = c.width, h = c.height;
    const img = m.createImageData(w, h), data = img.data;
    const sx = W / w, sy = H / h;
    for (let py = 0; py < h; py++) for (let px = 0; px < w; px++) {
      const x = Math.min(W - 1, Math.floor(px * sx)), y = Math.min(H - 1, Math.floor(py * sy));
      const ii = idx(x, y), off = (px + py * w) * 4;
      let r = 3, g = 5, b = 7;
      if (tSeen[ii]) {
        const tt = tType[ii], dep = depthAt(x, y), layer = depthLayerAt(dep);
        if (tt === TILE_EMPTY || tt === TILE_FORTRESS) { r = 48; g = 50; b = 55; }
        else { r = Math.round(layer.rock[0] * .38); g = Math.round(layer.rock[1] * .38); b = Math.round(layer.rock[2] * .38); }
        if (tt === TILE_GOLD) { r = 180; g = 138; b = 48; }
        else if (tt === TILE_SAPPHIRE) { r = 55; g = 126; b = 188; }
        else if (tt === TILE_EMERALD) { r = 47; g = 160; b = 100; }
        else if (tt === TILE_DIAMOND) { r = 183; g = 224; b = 244; }
      }
      data[off] = r; data[off + 1] = g; data[off + 2] = b; data[off + 3] = 255;
    }
    m.putImageData(img, 0, 0);
    function pin(x, y, color, size = 2.5, ring = false) {
      const p = dwfxMapPoint(x, y, c);
      m.save(); m.fillStyle = color; m.strokeStyle = color;
      m.beginPath(); m.arc(p.x, p.y, size, 0, Math.PI * 2);
      if (ring) { m.lineWidth = 1.5; m.stroke(); } else m.fill();
      m.restore();
    }
    pin(fortress.x, fortress.y, "#ffd66f", 4);
    if (forge) pin(forge.x, forge.y, "#ff9a66", 3);
    for (const d of depots) if (tSeen[idx(d.x, d.y)]) pin(d.x, d.y, "#85ffb0", 2.4);
    for (const t of towers) if (tSeen[idx(t.x, t.y)]) pin(t.x, t.y, "#78dcff", 2.0);
    for (const s of dwfxState.civicStructures) if (tSeen[idx(s.x, s.y)]) pin(s.x, s.y, "#ffd08a", s.id === dwfxState.selectedStructureId ? 4 : 2.8, s.id === dwfxState.selectedStructureId);
    for (const d of dwarves) pin(d.x, d.y, selected.kind === "dwarf" && selected.id === d.id ? "#ffffff" : "#a7f3d0", selected.kind === "dwarf" && selected.id === d.id ? 3.4 : 1.7);
    for (const s of spawners) if (tSeen[idx(s.x, s.y)]) pin(s.x, s.y, "#c084fc", 2.4);
    for (const e of enemies) {
      const ex = Math.round(e.x), ey = Math.round(e.y);
      if (inBounds(ex, ey) && tSeen[idx(ex, ey)]) pin(e.x, e.y, "#ff667a", 1.25);
    }
    for (const hint of dwfxState.surveyHints || []) {
      const p = dwfxMapPoint(hint.x, hint.y, c);
      m.save(); m.strokeStyle = hint.kind === "danger" ? "rgba(255,102,122,.85)" : "rgba(255,214,111,.85)";
      m.lineWidth = 1.2; m.setLineDash([3, 3]); m.beginPath(); m.arc(p.x, p.y, Math.max(5, hint.accuracy * c.width / W), 0, Math.PI * 2); m.stroke(); m.restore();
    }
    const camP = dwfxMapPoint(cam.x, cam.y, c);
    m.strokeStyle = "rgba(255,255,255,.82)"; m.lineWidth = 1; m.strokeRect(camP.x - 3, camP.y - 3, 6, 6);
  }

  function dwfxToggleMap(force) {
    const panel = document.getElementById("dwfxMapPanel");
    if (!panel) return;
    const open = force == null ? !panel.classList.contains("open") : !!force;
    panel.classList.toggle("open", open);
    if (open) dwfxDrawMinimap(true);
  }
  function dwfxToggleMapFullscreen() {
    const panel = document.getElementById("dwfxMapPanel");
    if (!panel) return;
    dwfxMapFullscreen = !dwfxMapFullscreen;
    panel.classList.toggle("fullscreen", dwfxMapFullscreen);
    setTimeout(() => dwfxDrawMinimap(true), 0);
  }

  function dwfxInjectUI() {
    const dwarfSection = document.getElementById("tab-dwarves");
    const firstCard = dwarfSection?.querySelector(".card");
    if (firstCard && !document.getElementById("dwfxNeedHeader")) {
      const need = document.createElement("div");
      need.id = "dwfxNeedHeader"; need.className = "dwfx-needbox";
      firstCard.appendChild(need);
    }

    const selectedSlots = document.querySelector("#tab-dwarves .slots");
    if (selectedSlots && !document.getElementById("dwfxRelicSlot")) {
      const slot = document.createElement("div");
      slot.className = "slot"; slot.id = "dwfxRelicSlot";
      slot.innerHTML = `<div class="label"><span>✦ Relic</span><button type="button" class="btn">Unequip</button></div><div class="val">—</div>`;
      slot.querySelector("button").addEventListener("click", dwfxUnequipRelic);
      selectedSlots.appendChild(slot);
    }

    const buildSection = document.getElementById("tab-build");
    if (buildSection && !document.getElementById("dwfxCivicBuilds")) {
      const card = document.createElement("div");
      card.className = "card"; card.id = "dwfxCivicBuilds";
      card.innerHTML = `<b>Civic Fortress</b><div class="tiny" style="margin-top:4px">Settlement buildings unlock as the Deepwild is explored.</div><div class="dwfx-build-grid"></div>`;
      const grid = card.querySelector(".dwfx-build-grid");
      for (const [type, def] of Object.entries(DWFX_BUILDINGS)) {
        const b = document.createElement("button");
        b.type = "button"; b.dataset.dwfxBuild = def.mode; b.dataset.dwfxType = type;
        b.addEventListener("click", () => setBuildMode(buildMode === def.mode ? BUILD_NONE : def.mode));
        grid.appendChild(b);
      }
      buildSection.appendChild(card);
      const status = document.createElement("div");
      status.className = "card"; status.id = "dwfxCivicStatus";
      buildSection.appendChild(status);
      const visitor = document.createElement("div");
      visitor.className = "card"; visitor.innerHTML = `<b>Tavern Visitor</b><div id="dwfxVisitor" style="margin-top:6px"></div>`;
      buildSection.appendChild(visitor);
    }

    const filters = UI.invSearch?.closest(".filters");
    if (filters && !document.getElementById("dwfxInvFilters")) {
      const bar = document.createElement("div");
      bar.id = "dwfxInvFilters"; bar.className = "dwfx-filterbar";
      const modes = [
        ["all", "All"], ["equipped", "Equipped"], ["unequipped", "Unequipped"], ["upgrades", "Upgrades"],
        ["relics", "Relics"], ["legendary", "Legendary+"], ["armor", "Armor"], ["weapon", "Weapons"], ["pickaxe", "Pickaxes"], ["needed", "Equipment Needed"]
      ];
      for (const [mode, label] of modes) {
        const b = document.createElement("button"); b.type = "button"; b.dataset.dwfxInvfilter = mode; b.textContent = label;
        b.addEventListener("click", () => { dwfxInventoryMode = mode === "unequipped" ? "all" : mode; markUI("inv"); });
        bar.appendChild(b);
      }
      filters.parentElement.insertBefore(bar, UI.invScroll);
      const protect = document.createElement("button");
      protect.type = "button"; protect.textContent = "🔒 Protect All Legendary Items"; protect.style.marginTop = "6px";
      protect.addEventListener("click", dwfxProtectAllLegendary);
      filters.parentElement.insertBefore(protect, UI.invScroll);
    }

    const mapPanel = document.createElement("div");
    mapPanel.id = "dwfxMapPanel";
    mapPanel.innerHTML = `<div class="dwfx-map-head"><b>Underground Map</b><span><button data-dwfx-mapfull type="button">⛶</button> <button data-dwfx-mapclose type="button">×</button></span></div>
      <canvas id="dwfxMap" width="${DWFX_MAP_SIZE}" height="${DWFX_MAP_SIZE}"></canvas>
      <div class="dwfx-map-actions">
        <button data-mapnav="fortress">🏰 Fortress</button><button data-mapnav="selected">👷 Selected Dwarf</button>
        <button data-mapnav="deepest">⬇ Deepest Explored</button><button data-mapnav="latest">✦ Latest Discovery</button>
      </div>`;
    document.body.appendChild(mapPanel);
    mapPanel.querySelector("[data-dwfx-mapclose]").addEventListener("click", () => dwfxToggleMap(false));
    mapPanel.querySelector("[data-dwfx-mapfull]").addEventListener("click", dwfxToggleMapFullscreen);
    mapPanel.querySelector("#dwfxMap").addEventListener("click", e => {
      const c = e.currentTarget, r = c.getBoundingClientRect();
      const x = clamp((e.clientX - r.left) / r.width * W, 0, W - 1), y = clamp((e.clientY - r.top) / r.height * H, 0, H - 1);
      dwfxMapFocus(x, y);
      const near = dwfxState.civicStructures.find(s => Math.hypot(s.x - x, s.y - y) < 13);
      if (near) { dwfxState.selectedStructureId = near.id; dwfxRenderStructureCard(); }
    });
    mapPanel.querySelectorAll("[data-mapnav]").forEach(b => b.addEventListener("click", () => {
      const key = b.dataset.mapnav;
      if (key === "fortress") dwfxMapFocus(fortress.x, fortress.y);
      else if (key === "selected") { const d = getSelectedDwarf(); if (d) dwfxMapFocus(d.x, d.y); else toast("Select a dwarf first."); }
      else if (key === "deepest") { const p = dwfxDeepestKnownPoint(); dwfxMapFocus(p.x, p.y); }
      else if (key === "latest") { const p = dwfxState.latestDiscovery; if (p) dwfxMapFocus(p.x, p.y); else toast("No recent discovery is recorded yet."); }
    }));

    const mapBtn = document.createElement("button");
    mapBtn.type = "button"; mapBtn.className = "btn"; mapBtn.id = "dwfxMapBtn"; mapBtn.textContent = "🗺 Map";
    mapBtn.addEventListener("click", () => dwfxToggleMap());
    document.querySelector(".hud-right")?.insertBefore(mapBtn, UI.btnFocus);

    if (UI.mobileBar && !document.getElementById("mMap")) {
      const m = document.createElement("button");
      m.id = "mMap"; m.className = "mbtn"; m.type = "button"; m.innerHTML = `<b>🗺️</b><span>Map</span>`;
      m.addEventListener("click", () => { dwfxToggleMap(true); if (!dwfxMapFullscreen) dwfxToggleMapFullscreen(); closeMobileSheets(); });
      UI.mobileBar.insertBefore(m, UI.mFocus);
    }

    dwfxPlacementCard = document.createElement("div"); dwfxPlacementCard.id = "dwfxPlacementCard"; document.body.appendChild(dwfxPlacementCard);
    dwfxStructureCard = document.createElement("div"); dwfxStructureCard.id = "dwfxStructureCard"; document.body.appendChild(dwfxStructureCard);
    const mobileConfirm = document.createElement("div"); mobileConfirm.id = "dwfxMobileConfirm";
    mobileConfirm.innerHTML = `<button data-dwfx-confirm class="btn primary" type="button">Build</button><button data-dwfx-cancel class="btn danger" type="button">Cancel</button>`;
    document.body.appendChild(mobileConfirm);
    mobileConfirm.querySelector("[data-dwfx-confirm]").addEventListener("click", () => {
      const p = dwfxPendingPlacement; if (p) dwfxBuildAt(p.mode, p.x, p.y);
    });
    mobileConfirm.querySelector("[data-dwfx-cancel]").addEventListener("click", () => {
      dwfxPendingPlacement = null; dwfxRenderMobileConfirm(); dwfxRenderPlacementCard();
    });

    dwfxRelicModal = document.createElement("div");
    dwfxRelicModal.id = "dwfxRelicModal";
    dwfxRelicModal.innerHTML = `<section class="dwfx-relic-card">
      <div class="dwfx-relic-title">✦ RELIC DISCOVERED ✦</div>
      <div class="dwfx-relic-name" data-dwfx-relic-name></div>
      <div class="dwfx-relic-flavor" data-dwfx-relic-flavor></div>
      <div class="small" data-dwfx-relic-desc></div>
      <div class="dwfx-relic-actions"><button data-dwfx-relic-equip class="btn primary" type="button">Equip</button>
        <button data-dwfx-relic-vault type="button">Send to Vault</button><button data-dwfx-relic-close type="button">Continue</button></div></section>`;
    document.body.appendChild(dwfxRelicModal);
    dwfxRelicModal.querySelector("[data-dwfx-relic-close]").addEventListener("click", dwfxCloseRelicModal);
    dwfxRelicModal.querySelector("[data-dwfx-relic-vault]").addEventListener("click", () => {
      const it = loot.find(x => x.id === dwfxRelicModal.dataset.itemId);
      if (it && dwfxStructure("greatVault")) { it.protected = true; it.vaulted = true; }
      dwfxCloseRelicModal(); markUI("inv");
    });
    dwfxRelicModal.querySelector("[data-dwfx-relic-equip]").addEventListener("click", () => {
      const id = dwfxRelicModal.dataset.itemId;
      if (!getSelectedDwarf()) return toast("Select a dwarf first, then Equip.");
      equipSelected(id); dwfxCloseRelicModal();
    });

    dwfxRefreshBuildButtons(); dwfxRenderNeedHeader(); dwfxRenderVisitor(); dwfxRenderCivicStatus();
  }

  function dwfxPruneEntity(obj) {
    const out = pruneEntity(obj);
    for (const key of Object.keys(out)) if (key.startsWith("_dwfx")) delete out[key];
    return out;
  }
  function dwfxSerializeState() {
    return {
      version: DWFX_SAVE_VERSION,
      civicStructures: dwfxState.civicStructures.map(s => ({ ...s })),
      relicsFound: [...new Set(dwfxState.relicsFound || [])],
      relicAnnouncements: [...new Set(dwfxState.relicAnnouncements || [])],
      deepestSeen: dwfxState.deepestSeen || 0,
      latestDiscovery: dwfxState.latestDiscovery ? { ...dwfxState.latestDiscovery } : null,
      surveyHints: (dwfxState.surveyHints || []).map(x => ({ ...x })),
      visitor: dwfxState.visitor ? { ...dwfxState.visitor } : null,
      visitorTimer: dwfxState.visitorTimer,
      surveyTimer: dwfxState.surveyTimer,
      rationTimer: dwfxState.rationTimer,
      rationStock: dwfxState.rationStock || 0,
      rationBuffUntil: 0,
      passiveTimer: dwfxState.passiveTimer,
      selectedStructureId: dwfxState.selectedStructureId || null
    };
  }
  function dwfxRestoreState(raw) {
    const base = dwfxDefaultState();
    if (!raw || typeof raw !== "object") { dwfxState = base; return; }
    dwfxState = {
      ...base, ...raw,
      civicStructures: Array.isArray(raw.civicStructures) ? raw.civicStructures.map(s => ({ ...s })) : [],
      relicsFound: Array.isArray(raw.relicsFound) ? [...raw.relicsFound] : [],
      relicAnnouncements: Array.isArray(raw.relicAnnouncements) ? [...raw.relicAnnouncements] : [],
      surveyHints: Array.isArray(raw.surveyHints) ? raw.surveyHints.map(x => ({ ...x })) : []
    };
  }
  function dwfxMigrateItem(it) {
    if (!it) return it;
    if (it.relic || it.relicId) {
      it.relic = true; it.protected = true; it.rarity = "legendary";
      const def = dwfxRelicDef(it);
      if (def) { it.effectName = def.name; it.effectDesc = def.desc; it.relicFlavor = def.flavor; }
    }
    return it;
  }

  saveGameAsync = async function() {
    try {
      const save = {
        v: DWFX_SAVE_VERSION,
        W, H,
        fortress: { x: fortress.x, y: fortress.y, hp: fortress.hp, maxHp: fortress.maxHp },
        cam: { x: cam.x, y: cam.y, zoom: cam.zoom },
        gold, recruitCount, dwarfCounter,
        forge: forge ? { ...forge } : null,
        depots: depots.map(dwfxPruneEntity),
        towers: towers.map(dwfxPruneEntity),
        spawners: spawners.map(dwfxPruneEntity),
        enemies: enemies.map(dwfxPruneEntity),
        groundLoot: groundLoot.map(L => ({ id: L.id, x: L.x, y: L.y, payload: L.payload })),
        loot: loot.map(dwfxPruneEntity),
        dwarves: dwarves.map(dwfxPruneEntity),
        fortressExpansion: dwfxSerializeState(),
        tTypeB64: _u8ToB64(new Uint8Array(tType.buffer)),
        tHpB64: _u8ToB64(new Uint8Array(tHp.buffer)),
        tSeenB64: _u8ToB64(new Uint8Array(tSeen.buffer))
      };
      const payloadB64 = await gzipStrToB64(JSON.stringify(save));
      const packed = SAVE_MAGIC + payloadB64;
      localStorage.removeItem(SAVE_KEY);
      localStorage.setItem(SAVE_KEY, packed);
      const approxBytes = new Blob([packed]).size;
      toast(`Saved locally. (~${Math.round(approxBytes / 1024)} KB)`);
    } catch (err) {
      console.error(err);
      toast("Save failed (storage may be full).");
    }
  };

  loadGameAsync = async function() {
    try {
      const raw = localStorage.getItem(SAVE_KEY);
      if (!raw) { toast("No local save found."); return; }
      let jsonStr = raw;
      if (raw.startsWith(SAVE_MAGIC)) jsonStr = await gunzipB64ToStr(raw.slice(SAVE_MAGIC.length));
      const save = JSON.parse(jsonStr);
      const ver = save?.v ?? 1;
      if (!save || ![1, 2, 3].includes(ver) || save.W !== W || save.H !== H) {
        toast("Save incompatible with this version/world size."); return;
      }
      if (ver >= 2) {
        const tTypeU8 = _b64ToU8(save.tTypeB64), tHpU8 = _b64ToU8(save.tHpB64), tSeenU8 = save.tSeenB64 ? _b64ToU8(save.tSeenB64) : null;
        if (tTypeU8.length !== tType.byteLength || tHpU8.length !== tHp.byteLength) { toast("Save corrupted (tile data size mismatch)."); return; }
        new Uint8Array(tType.buffer).set(tTypeU8); new Uint8Array(tHp.buffer).set(tHpU8);
        tSeen.fill(0);
        if (tSeenU8 && tSeenU8.length === tSeen.byteLength) new Uint8Array(tSeen.buffer).set(tSeenU8);
      } else {
        const tTypeU8 = b64.b64ToU8(save.tType), tHpU8 = b64.b64ToU8(save.tHp);
        if (tTypeU8.length !== tType.byteLength || tHpU8.length !== tHp.byteLength) { toast("Save corrupted (tile data size mismatch)."); return; }
        new Uint8Array(tType.buffer).set(tTypeU8); new Uint8Array(tHp.buffer).set(tHpU8); tSeen.fill(0);
      }
      fortress.hp = save.fortress.hp; fortress.maxHp = save.fortress.maxHp;
      cam.x = save.cam?.x ?? fortress.x; cam.y = save.cam?.y ?? fortress.y; cam.zoom = save.cam?.zoom ?? 1;
      gold = save.gold | 0; recruitCount = save.recruitCount | 0; dwarfCounter = save.dwarfCounter | 0;
      depots.length = 0; towers.length = 0; spawners.length = 0; enemies.length = 0; groundLoot.length = 0; loot.length = 0; dwarves.length = 0;
      forge = save.forge ? { ...save.forge } : null;
      for (const d of (save.depots || [])) depots.push({ ...d });
      for (const t of (save.towers || [])) towers.push({ ...t });
      for (const s of (save.spawners || [])) spawners.push({ ...s });
      for (const e of (save.enemies || [])) enemies.push({ ...e });
      for (const L of (save.groundLoot || [])) groundLoot.push({ ...L });
      for (const it of (save.loot || [])) { const copy = dwfxMigrateItem({ ...it }); if (!copy.identityDone && !copy.relic) decorateItem(copy, 0); loot.push(copy); }
      loot.sort(sortLoot);
      dwfxRestoreState(save.fortressExpansion);
      for (const d of (save.dwarves || [])) {
        const dd = dwfxEnsureDwarfFields({ ...d });
        dd.path = null; dd.think = 0; dd.miningTimer = dd.miningTimer || 0; dd.atkTimer = dd.atkTimer || 0;
        dd.trait = dd.trait || randomTrait(); dd.perks = Array.isArray(dd.perks) ? dd.perks : [];
        for (const slot of ["armor", "weapon", "pickaxe", "relic"]) if (dd[slot]) dd[slot] = dwfxMigrateItem(dd[slot]);
        applyDwarfStats(dd); dd.hp = clamp(d.hp ?? dd.hp, 0, dd.maxHp); dwarves.push(dd);
      }
      if (ver < 3) {
        dwfxState.deepestSeen = Math.max(depthAt(Math.round(cam.x), Math.round(cam.y)), ...dwarves.map(d => depthAt(Math.round(d.x), Math.round(d.y))), 0);
      }
      setSelected(null, null); setBuildMode(BUILD_NONE); tVis.fill(0); visMark = 1; refreshVisibility(0);
      dwfxUpdateLegacyCosts(); dwfxRefreshBuildButtons(); dwfxRenderVisitor(); dwfxRenderCivicStatus();
      toast("Loaded local save.");
      markUI("all"); dwfxDrawMinimap(true);
    } catch (err) {
      console.error(err);
      toast("Load failed (save may be corrupted).");
    }
  };

  function dwfxPostBootMigrate() {
    for (const d of dwarves) { dwfxEnsureDwarfFields(d); applyDwarfStats(d); }
    for (const it of loot) dwfxMigrateItem(it);
    dwfxState.deepestSeen = Math.max(dwfxState.deepestSeen || 0, ...dwarves.map(d => depthAt(Math.round(d.x), Math.round(d.y))), 0);
    dwfxRefreshBuildButtons(); markUI("all");
  }

  dwfxInjectUI();
  dwfxPostBootMigrate();

  window.DwarfWorldFortressExpansion = {
    version: DWFX_SAVE_VERSION,
    autoEquipEmptySlots: dwfxAutoEquipEmptySlots,
    openMap: () => dwfxToggleMap(true),
    closeMap: () => dwfxToggleMap(false),
    protectAllLegendary: dwfxProtectAllLegendary,
    getState: () => dwfxState
  };
}
