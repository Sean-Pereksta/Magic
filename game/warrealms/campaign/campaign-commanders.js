export const COMMANDER_ART_PATH = "../graphics/warrealmscommanders/";
export const DEFAULT_COMMANDER_ID = "varek";
export const COMMANDER_PERK_MILESTONES = Object.freeze([3, 6, 9]);

const FACTION_LABELS = Object.freeze({
  green: "Gorak",
  red: "Umbral",
  yellow: "Xythe",
  blue: "Azure"
});

function freezeRules(rules = {}) {
  return Object.freeze({ ...rules });
}

function freezeAbility(ability = {}) {
  return Object.freeze({
    ...ability,
    rules: freezeRules(ability.rules)
  });
}

function freezeCommander(config) {
  return Object.freeze({
    startingAuthorityModifier: 0,
    flavorText: "",
    ...config,
    factions: Object.freeze([...(config.factions || [])]),
    starterDeckChanges: Object.freeze((config.starterDeckChanges || []).map(change => Object.freeze({ count: 1, ...change }))),
    startingPassive: freezeAbility(config.startingPassive),
    perkTree: Object.freeze((config.perkTree || []).map(tier => Object.freeze({
      ...tier,
      choices: Object.freeze((tier.choices || []).map(freezeAbility))
    })))
  });
}

export const CAMPAIGN_COMMANDERS = Object.freeze([
  freezeCommander({
    id: "varek",
    name: "Varek",
    title: "Siege Marshal",
    factions: ["blue", "green"],
    description: "An Azure-Gorak field commander who turns durable fortifications into an advancing siege line.",
    flavorText: "A wall is only a weapon waiting to move.",
    image: "varek.png",
    legacySpecialization: "engineer",
    starterDeckChanges: [{ remove: "starter_coin", add: "mudwall_camp" }],
    startingPassive: {
      id: "fortified_advance",
      name: "Fortified Advance",
      description: "The first Base you deploy each battle gains 1 Armor.",
      rules: { firstBaseArmor: 1 }
    },
    perkTree: [
      { level: 3, choices: [
        { id: "bastion_line", name: "Bastion Line", description: "Fortified Advance grants 2 Armor instead.", rules: { firstBaseArmor: 1 } },
        { id: "breach_orders", name: "Breach Orders", description: "The first enemy Base you destroy each turn returns 1 Combat.", rules: { baseDestroyedCombat: 1 } }
      ] },
      { level: 6, choices: [
        { id: "field_engineers", name: "Field Engineers", description: "Begin each battle with 3 Shield.", rules: { startingShield: 3 } },
        { id: "rolling_fortress", name: "Rolling Fortress", description: "The first Base you deploy each battle gains another 2 Armor.", rules: { firstBaseArmor: 2 } }
      ] },
      { level: 9, choices: [
        { id: "total_breach", name: "Total Breach", description: "The first enemy Base destroyed each turn returns 3 additional Combat.", rules: { baseDestroyedCombat: 3 } },
        { id: "citadel_command", name: "Citadel Command", description: "Begin battles with 6 Shield and gain an extra reward reroll each region.", rules: { startingShield: 6, rewardRerolls: 1 } }
      ] }
    ]
  }),
  freezeCommander({
    id: "kaelra",
    name: "Kaelra",
    title: "Ashfang Reaver",
    factions: ["green", "red"],
    description: "A Gorak-Umbral commander who feeds sacrifice engines into sudden waves of pressure.",
    flavorText: "Every ending leaves a sharper edge.",
    image: "kaelra.png",
    legacySpecialization: "mystic",
    starterDeckChanges: [{ remove: "starter_coin", add: "ashledger_acolyte" }],
    startingPassive: {
      id: "blood_in_the_wake",
      name: "Blood in the Wake",
      description: "The first card you sacrifice each turn draws a card.",
      rules: { firstSacrificeDraw: 1 }
    },
    perkTree: [
      { level: 3, choices: [
        { id: "ember_tithe", name: "Ember Tithe", description: "The first sacrifice each turn also generates 1 Combat.", rules: { firstSacrificeCombat: 1 } },
        { id: "brood_oath", name: "Brood Oath", description: "The first Token created each turn generates 1 Combat.", rules: { firstTokenCombat: 1 } }
      ] },
      { level: 6, choices: [
        { id: "grave_momentum", name: "Grave Momentum", description: "The first sacrifice each turn draws one additional card.", rules: { firstSacrificeDraw: 1 } },
        { id: "warhost_hunger", name: "Warhost Hunger", description: "The first Token created each turn generates 3 Combat.", rules: { firstTokenCombat: 3 } }
      ] },
      { level: 9, choices: [
        { id: "endless_offering", name: "Endless Offering", description: "The first sacrifice each turn draws 2 cards and generates 2 Combat.", rules: { firstSacrificeDraw: 1, firstSacrificeCombat: 2 } },
        { id: "ashfang_host", name: "Ashfang Host", description: "Begin each battle with 2 Shield; first Tokens create a stronger surge.", rules: { startingShield: 2, firstTokenCombat: 4 } }
      ] }
    ]
  }),
  freezeCommander({
    id: "aurelia",
    name: "Aurelia Vale",
    title: "Gilded Castellan",
    factions: ["yellow", "blue"],
    description: "A Xythe-Azure architect of compound value, cheap fortifications, and patient control.",
    flavorText: "The safest road is the one you own.",
    image: "aurelia.png",
    legacySpecialization: "merchant",
    starterDeckChanges: [{ remove: "starter_coin", add: "unspent_possibility" }],
    startingPassive: {
      id: "fortress_contract",
      name: "Fortress Contract",
      description: "The first Base you purchase each turn costs 1 less.",
      rules: { firstBasePurchaseDiscount: 1 }
    },
    perkTree: [
      { level: 3, choices: [
        { id: "secured_credit", name: "Secured Credit", description: "Your first Base purchase each turn costs 2 less.", rules: { firstBasePurchaseDiscount: 1 } },
        { id: "reserve_ward", name: "Reserve Ward", description: "Begin each battle with 3 Shield.", rules: { startingShield: 3 } }
      ] },
      { level: 6, choices: [
        { id: "blueprint_dividend", name: "Blueprint Dividend", description: "The first Base you deploy each battle gains 2 Armor.", rules: { firstBaseArmor: 2 } },
        { id: "contingency_fund", name: "Contingency Fund", description: "The first card purchased each turn refunds 1 Trade.", rules: { firstPurchaseRefund: 1 } }
      ] },
      { level: 9, choices: [
        { id: "realm_concession", name: "Realm Concession", description: "Base contracts improve again and the first purchase refunds 1 Trade.", rules: { firstBasePurchaseDiscount: 1, firstPurchaseRefund: 1 } },
        { id: "unbroken_ledger", name: "Unbroken Ledger", description: "Begin battles with 7 Shield and gain improved Rest recovery.", rules: { startingShield: 7, restRecoveryBonus: 6 } }
      ] }
    ]
  }),
  freezeCommander({
    id: "morriva",
    name: "Morriva",
    title: "Veil-Market Oracle",
    factions: ["yellow", "red"],
    description: "A Xythe-Umbral manipulator who profits from erased futures and ruthlessly thins weak plans.",
    flavorText: "What never reaches the market can never oppose me.",
    image: "morriva.png",
    legacySpecialization: "mystic",
    starterDeckChanges: [{ remove: "starter_coin", add: "ashledger_acolyte" }],
    startingPassive: {
      id: "profitable_absence",
      name: "Profitable Absence",
      description: "The first Trade Row card you erase each turn generates 1 Trade.",
      rules: { firstMarketEraseTrade: 1 }
    },
    perkTree: [
      { level: 3, choices: [
        { id: "priced_void", name: "Priced Void", description: "The first market erase each turn generates 2 Trade.", rules: { firstMarketEraseTrade: 1 } },
        { id: "cinder_credit", name: "Cinder Credit", description: "The first card you purchase each turn refunds 1 Trade.", rules: { firstPurchaseRefund: 1 } }
      ] },
      { level: 6, choices: [
        { id: "sealed_outcome", name: "Sealed Outcome", description: "Begin each battle with 4 Shield.", rules: { startingShield: 4 } },
        { id: "blackglass_margin", name: "Blackglass Margin", description: "Market erasure generates another 2 Trade on its first use each turn.", rules: { firstMarketEraseTrade: 2 } }
      ] },
      { level: 9, choices: [
        { id: "monopoly_of_futures", name: "Monopoly of Futures", description: "First purchases refund 2 Trade and first erasures generate 2 Trade.", rules: { firstPurchaseRefund: 2, firstMarketEraseTrade: 2 } },
        { id: "perfected_void", name: "Perfected Void", description: "Gain a reward reroll each region and improved campaign currency outcomes.", rules: { rewardRerolls: 1, currencyBonusPercent: 20 } }
      ] }
    ]
  }),
  freezeCommander({
    id: "torak",
    name: "Torak Redcoin",
    title: "Warpath Quartermaster",
    factions: ["green", "yellow"],
    description: "A Gorak-Xythe commander who turns disciplined purchasing into immediate battlefield tempo.",
    flavorText: "A paid army marches faster.",
    image: "torak.png",
    legacySpecialization: "warrior",
    starterDeckChanges: [{ remove: "starter_blade", add: "knucklebone_rusher" }],
    startingPassive: {
      id: "marching_orders",
      name: "Marching Orders",
      description: "The first card you purchase each turn refunds 1 Trade.",
      rules: { firstPurchaseRefund: 1 }
    },
    perkTree: [
      { level: 3, choices: [
        { id: "supply_column", name: "Supply Column", description: "Begin each battle with 1 Shield and improve the first purchase refund.", rules: { startingShield: 1, firstPurchaseRefund: 1 } },
        { id: "forward_pay", name: "Forward Pay", description: "The first enemy Base destroyed each turn returns 2 Combat.", rules: { baseDestroyedCombat: 2 } }
      ] },
      { level: 6, choices: [
        { id: "open_requisition", name: "Open Requisition", description: "Your first Base purchase each turn costs 1 less.", rules: { firstBasePurchaseDiscount: 1 } },
        { id: "victory_dividend", name: "Victory Dividend", description: "Gain 15% more campaign currency from victories and events.", rules: { currencyBonusPercent: 15 } }
      ] },
      { level: 9, choices: [
        { id: "total_mobilization", name: "Total Mobilization", description: "First purchases refund 3 Trade and battles begin with 2 Shield.", rules: { firstPurchaseRefund: 2, startingShield: 2 } },
        { id: "siege_payroll", name: "Siege Payroll", description: "The first enemy Base destroyed each turn returns 4 Combat.", rules: { baseDestroyedCombat: 4 } }
      ] }
    ]
  }),
  freezeCommander({
    id: "selene",
    name: "Selene Noct",
    title: "Duskward Exarch",
    factions: ["blue", "red"],
    description: "An Azure-Umbral guardian who absorbs the opening assault, then converts survival into control.",
    flavorText: "Night does not retreat. It endures.",
    image: "selene.png",
    startingAuthorityModifier: 4,
    legacySpecialization: "engineer",
    starterDeckChanges: [{ remove: "starter_coin", add: "mudwall_camp" }],
    startingPassive: {
      id: "dusk_aegis",
      name: "Dusk Aegis",
      description: "Begin each battle with 3 Shield and 4 additional maximum Authority.",
      rules: { startingShield: 3 }
    },
    perkTree: [
      { level: 3, choices: [
        { id: "blackglass_ward", name: "Blackglass Ward", description: "Begin each battle with 3 more Shield.", rules: { startingShield: 3 } },
        { id: "patient_retribution", name: "Patient Retribution", description: "The first enemy Base destroyed each turn returns 1 Combat.", rules: { baseDestroyedCombat: 1 } }
      ] },
      { level: 6, choices: [
        { id: "last_light", name: "Last Light", description: "Rest recovery restores 8 additional Authority.", rules: { restRecoveryBonus: 8 } },
        { id: "warded_foundation", name: "Warded Foundation", description: "The first Base you deploy each battle gains 3 Armor.", rules: { firstBaseArmor: 3 } }
      ] },
      { level: 9, choices: [
        { id: "eclipse_bastion", name: "Eclipse Bastion", description: "Begin battles with 8 more Shield and improve Rest recovery.", rules: { startingShield: 8, restRecoveryBonus: 4 } },
        { id: "midnight_counterstroke", name: "Midnight Counterstroke", description: "The first Base destroyed each turn returns 4 Combat; first sacrifices draw a card.", rules: { baseDestroyedCombat: 4, firstSacrificeDraw: 1 } }
      ] }
    ]
  })
]);

const commanderMap = new Map(CAMPAIGN_COMMANDERS.map(commander => [commander.id, commander]));

function whole(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : fallback;
}

function object(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

export function getCampaignCommander(commanderId) {
  return commanderMap.get(String(commanderId || "")) || null;
}

export function defaultCommanderForSpecialization(specializationId) {
  return CAMPAIGN_COMMANDERS.find(commander => commander.legacySpecialization === String(specializationId || ""))
    || getCampaignCommander(DEFAULT_COMMANDER_ID);
}

export function commanderFactionLabel(commanderOrId) {
  const commander = typeof commanderOrId === "object" ? commanderOrId : getCampaignCommander(commanderOrId);
  return (commander?.factions || []).map(faction => FACTION_LABELS[faction] || faction).join(" / ") || "Unaligned";
}

export function commanderImagePath(commanderOrId) {
  const commander = typeof commanderOrId === "object" ? commanderOrId : getCampaignCommander(commanderOrId);
  return commander?.image ? `${COMMANDER_ART_PATH}${commander.image}` : "";
}

export function commanderInitials(commanderOrId) {
  const commander = typeof commanderOrId === "object" ? commanderOrId : getCampaignCommander(commanderOrId);
  return String(commander?.name || "Commander").split(/\s+/).map(part => part[0]).join("").slice(0, 2).toUpperCase();
}

export function applyCommanderStarterDeck(starterDeck = [], commanderOrId) {
  const commander = typeof commanderOrId === "object" ? commanderOrId : getCampaignCommander(commanderOrId);
  const result = (Array.isArray(starterDeck) ? starterDeck : []).map(String).filter(Boolean);
  for (const change of commander?.starterDeckChanges || []) {
    let remaining = Math.max(1, whole(change.count, 1));
    while (remaining > 0) {
      const index = result.indexOf(change.remove);
      if (index < 0) break;
      result.splice(index, 1, change.add);
      remaining -= 1;
    }
  }
  return result;
}

export function normalizeCommanderPerks(commanderOrId, rawPerks = {}) {
  const commander = typeof commanderOrId === "object" ? commanderOrId : getCampaignCommander(commanderOrId);
  const source = object(rawPerks);
  const normalized = {};
  for (const tier of commander?.perkTree || []) {
    const selected = String(source[tier.level] || source[String(tier.level)] || "");
    if (tier.choices.some(choice => choice.id === selected)) normalized[tier.level] = selected;
  }
  return normalized;
}

export function commanderPerkChoice(commanderOrId, perkId) {
  const commander = typeof commanderOrId === "object" ? commanderOrId : getCampaignCommander(commanderOrId);
  for (const tier of commander?.perkTree || []) {
    const perk = tier.choices.find(choice => choice.id === String(perkId || ""));
    if (perk) return { tier, perk };
  }
  return null;
}

export function pendingCommanderPerkLevel(profile) {
  const commander = getCampaignCommander(profile?.commanderId);
  const level = Math.max(1, whole(profile?.commanderLevel, whole(profile?.level, 1)));
  const perks = normalizeCommanderPerks(commander, profile?.commanderPerks);
  return commander?.perkTree.find(tier => level >= tier.level && !perks[tier.level])?.level || 0;
}

export function commanderPerkOptions(profile) {
  const commander = getCampaignCommander(profile?.commanderId);
  const pendingLevel = pendingCommanderPerkLevel(profile);
  return [...(commander?.perkTree.find(tier => tier.level === pendingLevel)?.choices || [])];
}

export function chooseCommanderPerk(profile, perkId, options = {}) {
  const commander = getCampaignCommander(profile?.commanderId);
  const level = pendingCommanderPerkLevel(profile);
  const tier = commander?.perkTree.find(candidate => candidate.level === level);
  const perk = tier?.choices.find(candidate => candidate.id === String(perkId || ""));
  if (!profile || !commander || !tier || !perk) return { ok: false, reason: "Choose an available commander perk." };
  profile.commanderPerks = normalizeCommanderPerks(commander, profile.commanderPerks);
  profile.commanderPerks[level] = perk.id;
  profile.history = Array.isArray(profile.history) ? profile.history : [];
  profile.history.push({
    atMs: Number(options.now) || Date.now(),
    type: "COMMANDER_PERK_CHOSEN",
    commanderId: commander.id,
    level,
    perkId: perk.id
  });
  profile.history = profile.history.slice(-200);
  return { ok: true, commander, level, perk };
}

export function commanderBattleRules(profileOrCommanderId, rawPerks = {}) {
  const profile = typeof profileOrCommanderId === "object" && "commanderId" in profileOrCommanderId ? profileOrCommanderId : null;
  const commander = getCampaignCommander(profile?.commanderId || profileOrCommanderId) || getCampaignCommander(DEFAULT_COMMANDER_ID);
  const perks = normalizeCommanderPerks(commander, profile?.commanderPerks || rawPerks);
  const rules = { ...(commander?.startingPassive?.rules || {}) };
  for (const tier of commander?.perkTree || []) {
    const selected = tier.choices.find(choice => choice.id === perks[tier.level]);
    for (const [key, value] of Object.entries(selected?.rules || {})) rules[key] = (Number(rules[key]) || 0) + (Number(value) || 0);
  }
  return rules;
}

export function commanderProgression(profile) {
  const commander = getCampaignCommander(profile?.commanderId) || getCampaignCommander(DEFAULT_COMMANDER_ID);
  const perks = normalizeCommanderPerks(commander, profile?.commanderPerks);
  const level = Math.max(1, whole(profile?.commanderLevel, whole(profile?.level, 1)));
  return {
    commander,
    level,
    passive: commander.startingPassive,
    tiers: commander.perkTree.map(tier => ({
      level: tier.level,
      status: perks[tier.level] ? "chosen" : level >= tier.level ? "available" : "locked",
      selected: tier.choices.find(choice => choice.id === perks[tier.level]) || null,
      choices: [...tier.choices]
    }))
  };
}

export function commanderEventRequirementMet(profile, requirement = {}) {
  const commander = getCampaignCommander(profile?.commanderId);
  if (!commander) return false;
  const factions = new Set(commander.factions);
  const perks = new Set(Object.values(normalizeCommanderPerks(commander, profile?.commanderPerks)));
  if (requirement.commanderId && commander.id !== requirement.commanderId) return false;
  if (requirement.faction && !factions.has(requirement.faction)) return false;
  if (Array.isArray(requirement.anyFaction) && !requirement.anyFaction.some(faction => factions.has(faction))) return false;
  if (requirement.perk && !perks.has(requirement.perk)) return false;
  return true;
}
