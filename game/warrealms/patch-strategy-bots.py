from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]


def read(rel):
    return (ROOT / rel).read_text(encoding="utf-8")


def write(rel, text):
    (ROOT / rel).write_text(text, encoding="utf-8")


def replace_once(text, old, new, label):
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one anchor, found {count}")
    return text.replace(old, new, 1)


# 1) Quick Play strategy cards. Keep this order identical to the native BOT_STRATEGIES object.
launcher_path = "game/warrealms/ui/play-launcher-core.js"
launcher = read(launcher_path)
launcher_old = '  Object.freeze({ id: "marketeer", name: "Marketeer", icon: "◆", color: "#d6bb3d", secondary: "#c2465d", factions: "Yellow + Red", deck: "Trade · market control · flow", description: "Shapes the shared market while keeping cards and Trade flowing." })\n]);'
launcher_new = '''  Object.freeze({ id: "marketeer", name: "Marketeer", icon: "◆", color: "#d6bb3d", secondary: "#c2465d", factions: "Yellow + Red", deck: "Trade · market control · flow", description: "Shapes the shared market while keeping cards and Trade flowing." }),
  Object.freeze({ id: "summoning", name: "Summoning", icon: "✦", color: "#67b64c", secondary: "#d6bb3d", factions: "Green + Yellow", deck: "Tokens · deck pulls · swarm", description: "Builds around token makers and cards that pull extra bodies from the draw pile." }),
  Object.freeze({ id: "ascendents", name: "Ascendents", icon: "◇", color: "#c2465d", secondary: "#67b64c", factions: "Red + Green", deck: "Evolution · transformation · scaling", description: "Prioritizes cards that evolve, ascend, hatch, or transform into stronger forms." }),
  Object.freeze({ id: "bastion", name: "Bastion", icon: "▣", color: "#49a7e8", secondary: "#67b64c", factions: "Blue + Green", deck: "Structures · healing · shields", description: "Commits heavily to Bases, repair, healing, Shield, and durable defensive engines." }),
  Object.freeze({ id: "fleet", name: "Fleet", icon: "✧", color: "#49a7e8", secondary: "#d6bb3d", factions: "Blue + Yellow", deck: "Drones · Interceptors · token synergy", description: "A focused summoning plan that hunts Drone and Interceptor producers and their payoffs." })
]);'''
launcher = replace_once(launcher, launcher_old, launcher_new, "Quick Play strategies")
write(launcher_path, launcher)


# 2) Test Lab strategies and strategy scoring.
sim_path = "game/warrealms/ui/test-lab-simulator.js"
sim = read(sim_path)
sim_old = '  { id: "marketeer", name: "Marketeer", primary: ["yellow", "red"], support: ["blue"], description: "Trade, market control, and card flow." }\n]);'
sim_new = '''  { id: "marketeer", name: "Marketeer", primary: ["yellow", "red"], support: ["blue"], description: "Trade, market control, and card flow." },
  { id: "summoning", name: "Summoning", primary: ["green", "yellow"], support: ["blue"], description: "Token production, draw-pile pulls, and swarm value." },
  { id: "ascendents", name: "Ascendents", primary: ["red", "green"], support: ["blue"], description: "Evolution, ascension, hatching, and other card transformations." },
  { id: "bastion", name: "Bastion", primary: ["blue", "green"], support: ["yellow"], description: "Structures, repair, healing, Shield, and defensive engines." },
  { id: "fleet", name: "Fleet", primary: ["blue", "yellow"], support: ["green"], description: "Drone and Interceptor token producers plus their supporting payoffs." }
]);'''
sim = replace_once(sim, sim_old, sim_new, "Test Lab strategy registry")

strategic_old = '''function strategicTags(card) {
  return {
    economy: sumKey(card, "trade") + sumKey(card, "draw") * 1.2 + (hasKey(card, new Set(["scrapOwn", "purge", "purgeAndDraw"])) ? 1.4 : 0),
    offense: sumKey(card, "combat") + (hasKey(card, new Set(["destroyBase", "combatAgainstBases", "damageAll"])) ? 2 : 0),
    defense: number(card?.health || card?.defense) * (card?.type === "base" ? .25 : 0) + sumKey(card, "shield") + sumKey(card, "heal"),
    control: hasKey(card, new Set(["stun", "disable", "discard", "oppDiscard", "scrapMarket", "marketErase"])) ? 2.5 : 0,
    engine: hasKey(card, new Set(["addHeat", "charge", "createToken", "sacrifice", "transform", "construction", "attachment"])) ? 2.1 : 0
  };
}
'''
strategic_new = '''function createdTokenCount(value, wantedIds = null, seen = new WeakSet()) {
  if (!value || typeof value !== "object") return 0;
  if (seen.has(value)) return 0;
  seen.add(value);
  if (Array.isArray(value)) return value.reduce((total, child) => total + createdTokenCount(child, wantedIds, seen), 0);
  let total = 0;
  for (const [key, child] of Object.entries(value)) {
    if (key === "createToken" && child && typeof child === "object") {
      const tokenId = String(child.id || "").toLowerCase();
      const count = Math.max(1, number(child.count ?? child.amount) || 1);
      if (!wantedIds?.length || wantedIds.includes(tokenId)) total += count;
    }
    total += createdTokenCount(child, wantedIds, seen);
  }
  return total;
}

function cardReferencesTokens(card, tokenIds = []) {
  if (!card || !tokenIds.length) return false;
  const text = JSON.stringify(card).toLowerCase();
  return tokenIds.some(tokenId => text.includes(String(tokenId).toLowerCase()));
}

function hasTransformPath(card) {
  if (!card?.transform || typeof card.transform !== "object") return false;
  return !!(card.transform.into || (Array.isArray(card.transform.choose) && card.transform.choose.length) || (card.transform.choose && typeof card.transform.choose === "object"));
}

function strategicTags(card) {
  const summonedTokens = createdTokenCount(card);
  const fleetTokens = createdTokenCount(card, ["drone", "interceptor"]);
  const drawPilePulls = sumKey(card, "drawFromDrawPile");
  const shield = sumKey(card, "shield");
  const repair = sumKey(card, "repair");
  return {
    economy: sumKey(card, "trade") + sumKey(card, "draw") * 1.2 + drawPilePulls * 1.35 + (hasKey(card, new Set(["scrapOwn", "purge", "purgeAndDraw"])) ? 1.4 : 0),
    offense: sumKey(card, "combat") + (hasKey(card, new Set(["destroyBase", "combatAgainstBases", "damageAll"])) ? 2 : 0),
    defense: number(card?.health || card?.defense) * (card?.type === "base" ? .25 : 0) + shield + sumKey(card, "heal") + repair * .8,
    control: hasKey(card, new Set(["stun", "disable", "discard", "oppDiscard", "scrapMarket", "marketErase"])) ? 2.5 : 0,
    engine: hasKey(card, new Set(["addHeat", "charge", "createToken", "sacrifice", "transform", "construction", "attachment"])) ? 2.1 : 0,
    summoning: summonedTokens * 2.4 + drawPilePulls * 2.1 + (hasKey(card, new Set(["TOKEN_CREATED", "TOKEN_PLAYED", "tokenCombo", "tokenSacrificeTrigger"])) ? 1.4 : 0),
    transform: hasTransformPath(card) ? 5.5 : 0,
    bastion: (card?.type === "base" ? 2.8 : 0) + shield * 1.4 + sumKey(card, "heal") * 1.2 + repair * 1.35,
    fleet: fleetTokens * 4.4 + (cardReferencesTokens(card, ["drone", "interceptor"]) ? 1.5 : 0)
  };
}
'''
sim = replace_once(sim, strategic_old, strategic_new, "Test Lab strategic tags")

switch_old = '''    case "attrition": bonus += tags.control * .72 + tags.defense * .28 + tags.engine * .22; break;
    case "marketeer": bonus += tags.economy * .72 + tags.control * .5; break;
    default: bonus += (tags.economy + tags.offense + tags.defense + tags.control + tags.engine) * .16;
'''
switch_new = '''    case "attrition": bonus += tags.control * .72 + tags.defense * .28 + tags.engine * .22; break;
    case "marketeer": bonus += tags.economy * .72 + tags.control * .5; break;
    case "summoning": bonus += tags.summoning * 1.18 + tags.engine * .34 + tags.economy * .2; break;
    case "ascendents": bonus += tags.transform * 2 + tags.engine * .45 + tags.economy * .15; break;
    case "bastion": bonus += tags.bastion * 1.16 + tags.defense * .66 + (card.type === "base" ? 2.9 : 0); break;
    case "fleet": bonus += tags.fleet * 1.65 + tags.summoning * .48 + tags.offense * .16; break;
    default: bonus += (tags.economy + tags.offense + tags.defense + tags.control + tags.engine) * .16;
'''
sim = replace_once(sim, switch_old, switch_new, "Test Lab strategy scoring")

helper_anchor = '''function intrinsicCardValue(card) {
'''
helper_insert = '''export function testLabStrategyFit(cardOrId, strategyId) {
  const card = typeof cardOrId === "string" ? CARD_BY_ID.get(cardOrId) : cardOrId;
  return card ? strategyBonus(card, strategyId) : 0;
}

function intrinsicCardValue(card) {
'''
sim = replace_once(sim, helper_anchor, helper_insert, "Test Lab strategy fit helper")
write(sim_path, sim)


# 3) Native interactive Warbot strategies in the main game.
html_path = "game/warrealms.html"
html = read(html_path)

native_strategies = '''
    summoning: Object.freeze({
      name: "Summoning",
      preferredFactions: Object.freeze(["green", "yellow"]),
      weights: Object.freeze({ createToken: 3.4, draw: 1.05, trade: .28, combat: .18, scrapOwn: .35 }),
      coreAbilities: Object.freeze(["createToken", "draw"]),
      secondaryAbilities: Object.freeze(["drawFromDrawPile", "combat"]),
      preferredTypes: Object.freeze(["unit", "base"]),
      costProfile: Object.freeze({ idealMin: 1, idealMax: 6, expensiveTolerance: .46, signatureChance: .48 }),
      factionTargets: Object.freeze({ primary: .56, support: .26 }),
      minimumAbilities: Object.freeze([Object.freeze({ abilities: Object.freeze(["createToken"]), min: 4 }), Object.freeze({ abilities: Object.freeze(["draw", "drawFromDrawPile"]), min: 4 })]),
      playPriorities: Object.freeze(["createToken", "drawFromDrawPile", "draw"]),
      baseTarget: 5,
      baseBias: .12,
      defenseWeight: .06,
      drawPriority: true
    }),
    ascendents: Object.freeze({
      name: "Ascendents",
      preferredFactions: Object.freeze(["red", "green"]),
      weights: Object.freeze({ addHeat: .72, draw: .48, combat: .25, trade: .2, shield: .18 }),
      coreAbilities: Object.freeze(["draw", "addHeat"]),
      secondaryAbilities: Object.freeze(["combat", "shield"]),
      preferredTypes: Object.freeze(["unit", "base"]),
      costProfile: Object.freeze({ idealMin: 2, idealMax: 7, expensiveTolerance: .64, signatureChance: .68 }),
      factionTargets: Object.freeze({ primary: .56, support: .27 }),
      minimumAbilities: Object.freeze([Object.freeze({ abilities: Object.freeze(["transform"]), min: 2 })]),
      playPriorities: Object.freeze(["transform", "heat", "scaling"]),
      baseTarget: 5,
      baseBias: .18,
      defenseWeight: .08,
      drawPriority: false
    }),
    bastion: Object.freeze({
      name: "Bastion",
      preferredFactions: Object.freeze(["blue", "green"]),
      weights: Object.freeze({ shield: 1.62, heal: 1.24, healPerBase: 1.42, repair: 1.28, combatPerBase: .48, tradePerBase: .46, drawPerBase: .62 }),
      coreAbilities: Object.freeze(["shield", "heal", "repair"]),
      secondaryAbilities: Object.freeze(["healPerBase", "combatPerBase", "tradePerBase"]),
      preferredTypes: Object.freeze(["base", "attachment"]),
      costProfile: Object.freeze({ idealMin: 3, idealMax: 8, expensiveTolerance: .68, signatureChance: .72 }),
      factionTargets: Object.freeze({ primary: .58, support: .28 }),
      minimumAbilities: Object.freeze([Object.freeze({ types: Object.freeze(["base"]), min: 10 }), Object.freeze({ abilities: Object.freeze(["shield", "heal", "repair", "healPerBase"]), min: 4 })]),
      playPriorities: Object.freeze(["base", "shield", "heal", "repair"]),
      baseTarget: 14,
      baseBias: 1.62,
      defenseWeight: .27,
      drawPriority: false
    }),
    fleet: Object.freeze({
      name: "Fleet",
      preferredFactions: Object.freeze(["blue", "yellow"]),
      weights: Object.freeze({ createToken: 2.35, draw: .68, combat: .36, shield: .22, trade: .18 }),
      coreAbilities: Object.freeze(["createToken", "draw"]),
      secondaryAbilities: Object.freeze(["combat", "shield"]),
      preferredTypes: Object.freeze(["unit", "base"]),
      costProfile: Object.freeze({ idealMin: 2, idealMax: 7, expensiveTolerance: .58, signatureChance: .62 }),
      factionTargets: Object.freeze({ primary: .56, support: .27 }),
      minimumAbilities: Object.freeze([Object.freeze({ abilities: Object.freeze(["createToken"]), min: 4 })]),
      playPriorities: Object.freeze(["drone", "interceptor", "createToken"]),
      baseTarget: 6,
      baseBias: .22,
      defenseWeight: .08,
      drawPriority: true
    })'''

native_pattern = re.compile(r'(    marketeer: Object\.freeze\(\{.*?      drawPriority: true\n    \})\n)(  \}\);)', re.S)
match = native_pattern.search(html)
if not match:
    raise RuntimeError("Native BOT_STRATEGIES marketeer tail anchor not found")
html = html[:match.start()] + match.group(1)[:-1] + ',\n' + native_strategies + '\n' + match.group(2) + html[match.end():]

# Native helper scoring for createToken, specific fleet token IDs, and transforms.
strategy_effect_old = '''    if (normalized.drawFromDrawPile) value += Math.max(1, Number(normalized.drawFromDrawPile.count) || 1) * Math.max(1, Number(weights.draw) || 2);
    const choices = choiceEffectOptions(effect);
'''
strategy_effect_new = '''    if (normalized.drawFromDrawPile) value += Math.max(1, Number(normalized.drawFromDrawPile.count) || 1) * Math.max(1, Number(weights.draw) || 2);
    if (normalized.createToken) value += Math.max(1, Number(normalized.createToken.count) || 1) * Math.max(0, Number(weights.createToken) || 0);
    const choices = choiceEffectOptions(effect);
'''
html = replace_once(html, strategy_effect_old, strategy_effect_new, "Native create-token effect scoring")

ability_old = '''    if (ability === "drawFromDrawPile" && normalized.drawFromDrawPile) return true;
    if (ability === "draw" && (Math.max(0, Number(normalized.draw) || 0) > 0 || normalized.drawFromDrawPile)) return true;
'''
ability_new = '''    if (ability === "drawFromDrawPile" && normalized.drawFromDrawPile) return true;
    if (ability === "createToken" && normalized.createToken) return true;
    if (ability === "draw" && (Math.max(0, Number(normalized.draw) || 0) > 0 || normalized.drawFromDrawPile)) return true;
'''
html = replace_once(html, ability_old, ability_new, "Native create-token ability detection")

requirement_old = '''  function botCardMatchesRequirement(card, requirement = {}) {
    if (!card) return false;
    const normalizedType = card.type === "card" || card.type === "ship" ? "unit" : card.type;
    if (requirement.types?.length && requirement.types.includes(normalizedType)) return true;
    return (requirement.abilities || []).some(ability => botCardEffectSources(card).some(effect => effectHasBotAbility(effect, ability)));
  }
'''
requirement_new = '''  function botCardMatchesRequirement(card, requirement = {}) {
    if (!card) return false;
    const normalizedType = card.type === "card" || card.type === "ship" ? "unit" : card.type;
    if (requirement.types?.length && requirement.types.includes(normalizedType)) return true;
    if ((requirement.abilities || []).includes("transform") && card.transform) return true;
    return (requirement.abilities || []).some(ability => botCardEffectSources(card).some(effect => effectHasBotAbility(effect, ability)));
  }
'''
html = replace_once(html, requirement_old, requirement_new, "Native transform requirement detection")

value_anchor = '''  function botStrategyCardValue(card, strategyKey) {
'''
value_helpers = '''  function botCreatedTokenCount(value, wantedIds = null, seen = new WeakSet()) {
    if (!value || typeof value !== "object") return 0;
    if (seen.has(value)) return 0;
    seen.add(value);
    if (Array.isArray(value)) return value.reduce((total, child) => total + botCreatedTokenCount(child, wantedIds, seen), 0);
    let total = 0;
    for (const [key, child] of Object.entries(value)) {
      if (key === "createToken" && child && typeof child === "object") {
        const tokenId = String(child.id || "").toLowerCase();
        const count = Math.max(1, Number(child.count ?? child.amount) || 1);
        if (!wantedIds?.length || wantedIds.includes(tokenId)) total += count;
      }
      total += botCreatedTokenCount(child, wantedIds, seen);
    }
    return total;
  }

  function botCardReferencesToken(card, tokenIds = []) {
    if (!card || !tokenIds.length) return false;
    const text = JSON.stringify(card).toLowerCase();
    return tokenIds.some(tokenId => text.includes(String(tokenId).toLowerCase()));
  }

  function botStrategyCardValue(card, strategyKey) {
'''
html = replace_once(html, value_anchor, value_helpers, "Native token helper insertion")

value_old = '''    const createsToken = !!(card.effect?.createToken || card.ally?.createToken || card.recurring?.effect?.createToken);
    const recoversCard = !!(card.effect?.reclaim || card.effect?.redeploy || card.ally?.reclaim || card.ally?.redeploy);
    if (strategyKey === "engine") value += (card.heat ? 5 : 0) + (card.charge ? 4 : 0);
'''
value_new = '''    const createsToken = botCreatedTokenCount(card) > 0;
    const fleetTokenCount = botCreatedTokenCount(card, ["drone", "interceptor"]);
    const pullsFromDrawPile = botCardEffectSources(card).some(effect => effectHasBotAbility(effect, "drawFromDrawPile"));
    const recoversCard = !!(card.effect?.reclaim || card.effect?.redeploy || card.ally?.reclaim || card.ally?.redeploy);
    if (strategyKey === "engine") value += (card.heat ? 5 : 0) + (card.charge ? 4 : 0);
'''
html = replace_once(html, value_old, value_new, "Native strategy card helper values")

specific_old = '''    if (strategyKey === "vanguard" || strategyKey === "siege") value += createsToken ? 3.5 : 0;
    if (strategyKey === "marketeer") value += card.factionScaling || card.factionThresholds ? 3 : 0;
    if (card.type === "base") value += strategy.baseBias + Math.max(0, Number(card.defense) || 0) * strategy.defenseWeight;
'''
specific_new = '''    if (strategyKey === "vanguard" || strategyKey === "siege") value += createsToken ? 3.5 : 0;
    if (strategyKey === "marketeer") value += card.factionScaling || card.factionThresholds ? 3 : 0;
    if (strategyKey === "summoning") value += botCreatedTokenCount(card) * 5.4 + (pullsFromDrawPile ? 6.2 : 0) + (botCardReferencesToken(card, ["token"]) ? 1.4 : 0);
    if (strategyKey === "ascendents") value += card.transform ? 12 : 0;
    if (strategyKey === "bastion") value += (card.type === "base" ? 4.5 : 0) + (isExpansionBase(card) ? 4 : 0) + (botCardEffectSources(card).some(effect => effectHasBotAbility(effect, "shield") || effectHasBotAbility(effect, "heal") || effectHasBotAbility(effect, "repair")) ? 4.2 : 0);
    if (strategyKey === "fleet") value += fleetTokenCount * 10 + (fleetTokenCount === 0 && botCardReferencesToken(card, ["drone", "interceptor"]) ? 4.6 : 0) + (createsToken ? 2 : 0);
    if (card.type === "base") value += strategy.baseBias + Math.max(0, Number(card.defense) || 0) * strategy.defenseWeight;
'''
html = replace_once(html, specific_old, specific_new, "Native strategy-specific card scoring")

purchase_old = '''      if (bot.botStrategy === "stronghold" && card.type === "base") {
        const ownedBases = ownedIds.filter(cardId => getCard(cardId)?.type === "base").length;
        bonus += ownedBases < strategy.baseTarget ? 3.1 : .4;
      }
      if ((bot.botStrategy === "engine" || bot.botStrategy === "marketeer") && round <= 5) bonus += profile.income * .85;
'''
purchase_new = '''      if ((bot.botStrategy === "stronghold" || bot.botStrategy === "bastion") && card.type === "base") {
        const ownedBases = ownedIds.filter(cardId => getCard(cardId)?.type === "base").length;
        bonus += ownedBases < strategy.baseTarget ? (bot.botStrategy === "bastion" ? 4.2 : 3.1) : .4;
      }
      if (bot.botStrategy === "summoning" && (botCreatedTokenCount(card) > 0 || pullsFromDrawPile)) bonus += 2.8;
      if (bot.botStrategy === "ascendents" && card.transform) bonus += 4.5;
      if (bot.botStrategy === "fleet" && botCreatedTokenCount(card, ["drone", "interceptor"]) > 0) bonus += 5.5;
      if ((bot.botStrategy === "engine" || bot.botStrategy === "marketeer" || bot.botStrategy === "summoning") && round <= 5) bonus += profile.income * .85;
'''
# pullsFromDrawPile is local to botStrategyCardValue, so use direct detection in this scope.
purchase_new = purchase_new.replace('(botCreatedTokenCount(card) > 0 || pullsFromDrawPile)', '(botCreatedTokenCount(card) > 0 || botCardEffectSources(card).some(effect => effectHasBotAbility(effect, "drawFromDrawPile")))')
html = replace_once(html, purchase_old, purchase_new, "Native purchase adaptation")

play_old = '''      if (hasAbility("draw")) score += strategyKey === "engine" || strategyKey === "cycle" || strategyKey === "marketeer" ? 8 : 5;
      if (hasAbility("trade")) score += strategyKey === "engine" || strategyKey === "marketeer" ? 4.5 : 1;
'''
play_new = '''      if (hasAbility("draw")) score += ["engine", "cycle", "marketeer", "summoning", "fleet"].includes(strategyKey) ? 8 : 5;
      if (hasAbility("trade")) score += strategyKey === "engine" || strategyKey === "marketeer" ? 4.5 : 1;
      if (strategyKey === "summoning" && (hasAbility("createToken") || hasAbility("drawFromDrawPile"))) score += 6;
      if (strategyKey === "ascendents" && card?.transform) score += 7;
      if (strategyKey === "bastion" && (card?.type === "base" || hasAbility("shield") || hasAbility("heal") || hasAbility("repair"))) score += 5;
      if (strategyKey === "fleet" && botCreatedTokenCount(card, ["drone", "interceptor"]) > 0) score += 8;
'''
html = replace_once(html, play_old, play_new, "Native play sequencing")
write(html_path, html)


# 4) Cache-bust Test Lab imports so the new registry is picked up immediately.
for rel in ["game/warrealms/ui/test-lab.js", "game/warrealms/ui/test-lab-runner.js", "game/warrealms/ui/test-lab-strategy-analytics.js"]:
    text = read(rel)
    text, count = re.subn(r'test-lab-simulator\.js\?v=\d+', 'test-lab-simulator.js?v=3', text)
    if count < 1:
        raise RuntimeError(f"{rel}: simulator import not found")
    write(rel, text)


# 5) Regression tests: registry behavior + Quick Play/native order contract.
test_path = ROOT / "game/warrealms/tests/bot-strategies.test.mjs"
test_path.write_text('''import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

import { TEST_LAB_STRATEGIES, testLabStrategyFit } from "../ui/test-lab-simulator.js";

const here = dirname(fileURLToPath(import.meta.url));
const newIds = ["summoning", "ascendents", "bastion", "fleet"];

test("new strategy archetypes are registered and mechanically specialized", () => {
  const ids = TEST_LAB_STRATEGIES.map(strategy => strategy.id);
  newIds.forEach(id => assert.ok(ids.includes(id), `${id} should be available in Test Lab`));

  const tokenCard = { faction: "green", type: "ship", cost: 4, effect: { createToken: { id: "spawn", count: 2 } } };
  const transformCard = { faction: "red", type: "ship", cost: 5, transform: { trigger: "turns", required: 2, into: "greater_form" } };
  const bastionCard = { faction: "blue", type: "base", cost: 5, health: 8, effect: { shield: 4, repair: { amount: 3 } } };
  const fleetCard = { faction: "blue", type: "ship", cost: 5, effect: { createToken: { id: "drone", count: 2 } } };

  assert.ok(testLabStrategyFit(tokenCard, "summoning") > testLabStrategyFit(tokenCard, "marketeer"));
  assert.ok(testLabStrategyFit(transformCard, "ascendents") > testLabStrategyFit(transformCard, "vanguard"));
  assert.ok(testLabStrategyFit(bastionCard, "bastion") > testLabStrategyFit(bastionCard, "cycle"));
  assert.ok(testLabStrategyFit(fleetCard, "fleet") > testLabStrategyFit(fleetCard, "summoning"));
});

test("Quick Play and the native Warbot keep strategy indices in lockstep", () => {
  const launcher = readFileSync(resolve(here, "../ui/play-launcher-core.js"), "utf8");
  const nativeGame = readFileSync(resolve(here, "../../warrealms.html"), "utf8");
  const launcherBlock = launcher.match(/const BOT_STRATEGIES = Object\.freeze\(\[(.*?)\n\]\);/s)?.[1] || "";
  const nativeBlock = nativeGame.match(/const BOT_STRATEGIES = Object\.freeze\(\{(.*?)\n  \}\);/s)?.[1] || "";
  const launcherOrder = [...launcherBlock.matchAll(/id: "([a-z_]+)", name:/g)].map(match => match[1]);
  const nativeOrder = [...nativeBlock.matchAll(/^    ([a-z_]+): Object\.freeze\(\{/gm)].map(match => match[1]);

  assert.deepEqual(nativeOrder, launcherOrder, "Quick Play forcing depends on identical native/menu strategy order");
  newIds.forEach(id => assert.ok(nativeOrder.includes(id), `${id} should exist in native Warbot strategies`));
});
''', encoding="utf-8")

print("War Realms strategy bot patch applied successfully")
