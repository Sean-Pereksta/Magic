import fs from "node:fs";
import path from "node:path";

const root = process.cwd();
const cardsPath = path.join(root, "game/warrealms-pack/warrealms-cards.js");
const htmlPath = path.join(root, "game/warrealms.html");
const testPath = path.join(root, "game/warrealms/tests/heat-payoff-cards.test.mjs");
const workflowPath = path.join(root, ".github/workflows/apply-warrealms-heat-payoffs.yml");
const selfPath = path.join(root, "scripts/patch-warrealms-heat-payoffs.mjs");

function mustReplace(source, from, to, label) {
  if (!source.includes(from)) throw new Error(`Patch anchor missing: ${label}`);
  return source.replace(from, to);
}

let cards = fs.readFileSync(cardsPath, "utf8");
if (!cards.includes('id: "sanctum_core_imbuer"')) {
  cards = cards.replace(
    "export const WAR_REALMS_CARD_VERSION = 19;",
    "export const WAR_REALMS_CARD_VERSION = 20;"
  );

  const marker = "export const CARDS = Object.freeze([";
  const additions = String.raw`

  // ==========================================================
  // HEAT PAYOFF MINI-PACK — active-card Heat acceleration
  // ==========================================================
  {
    id: "sanctum_core_imbuer",
    name: "Sanctum-Core Imbuer",
    image: "sanctum_core_imbuer.png",
    faction: "blue",
    cost: 7,
    shop_cost: 120,
    type: "ship",
    sigil: "✦",
    effect: {
      addHeat: {
        amount: 2,
        target: "friendlyHeatCard",
        excludeSelf: true
      }
    },
    ally: { shield: 2 },
    doubleAlly: {
      or: [
        {
          id: "sanctum_core_imbuer_repair",
          label: "Reinforce the Sanctum",
          effect: { repair: { amount: 5 } }
        },
        {
          id: "sanctum_core_imbuer_shield",
          label: "Raise the Aegis",
          effect: { shield: 5 }
        }
      ]
    },
    text: "Add 2 Heat to another friendly Heat card.",
    allyText: "Gain 2 Shield.",
    doubleAllyText: "Choose one: repair a Base for 5; or gain 5 Shield.",
    flavor: "The Ascendancy does not create instability. It simply decides where instability belongs."
  },
  {
    id: "heavenlance_thermal_turret",
    name: "Heavenlance Thermal Turret",
    image: "heavenlance_thermal_turret.png",
    faction: "blue",
    cost: 5,
    shop_cost: 90,
    type: "base",
    defense: 8,
    outpost: false,
    sigil: "✦",
    effect: {},
    heat: {
      max: 6,
      overload: {
        at: 6,
        optional: false,
        reset: 0,
        effect: { combat: 15 }
      }
    },
    text: "This Base produces no Combat normally.",
    heatText: "At 6 Heat, automatically spend all 6 Heat: gain 15 Combat, then reset to 0 Heat.",
    flavor: "For six cycles, the weapon is silent. On the seventh, there is considerably less horizon."
  },
  {
    id: "eightfold_drone_ark",
    name: "Eightfold Drone Ark",
    image: "eightfold_drone_ark.png",
    faction: "yellow",
    cost: 6,
    shop_cost: 105,
    type: "ship",
    sigil: "◈",
    effect: {},
    heat: {
      max: 8,
      overload: {
        at: 8,
        optional: false,
        reset: 0,
        effect: {
          createToken: {
            id: "drone",
            count: 4,
            zone: "discard"
          }
        }
      }
    },
    text: "This card has no immediate effect.",
    heatText: "At 8 Heat, automatically spend all 8 Heat: create four Drones in your discard pile, then reset to 0 Heat.",
    flavor: "The empty hangars are not empty. Their occupants simply have not happened yet."
  },
`;
  if (!cards.includes(marker)) throw new Error("CARDS array marker not found");
  cards = cards.replace(marker, marker + additions);
  fs.writeFileSync(cardsPath, cards);
}

let html = fs.readFileSync(htmlPath, "utf8");
html = html.replace(
  '"./warrealms-pack/warrealms-cards.js?v=18"',
  '"./warrealms-pack/warrealms-cards.js?v=20"'
);

if (!html.includes("function queueAddHeatTargetChoice")) {
  const queueCoolMarker = "  function queueHeatTargetChoice(game, player, config = {}, context = {}) {";
  const addHeatFunction = String.raw`  function queueAddHeatTargetChoice(game, player, config = {}, context = {}) {
    const amount = Math.max(1, Math.floor(Number(config.amount) || 1));
    const targetRule = String(config.target || "friendlyHeatCard");
    const activeEntries = [
      ...(player.played || []),
      ...(player.bases || []),
      ...(player.attachments || [])
    ];
    const targets = activeEntries.filter(entry => {
      const targetCard = getCard(entry);
      if (!targetCard?.heat) return false;
      if ((config.excludeSelf !== false || targetRule === "friendlyHeatShip") && entry.instanceId === context.instanceId) return false;
      if (targetRule === "friendlyHeatShip" && targetCard.type !== "ship") return false;
      return (entry.heat || 0) < Math.max(0, Number(targetCard.heat.max) || 99);
    });
    const options = targets.map(entry => ({
      label: `${getCard(entry).name} · Heat ${entry.heat || 0}`,
      effect: {
        __heatChange: {
          instanceId: entry.instanceId,
          delta: amount,
          remainingTargets: Math.max(0, Math.floor(Number(config.targets) || 1) - 1),
          targetMode: "add",
          targetRule,
          excludeSelf: config.excludeSelf !== false,
          ifChangedEffect: clone(config.ifChangedEffect || {})
        }
      }
    }));
    if (!queueSpecialChoice(game, player, options, { ...context, source: "addHeat" })) {
      addLog(game, `${player.name} had no eligible Heat card to empower.`);
      return false;
    }
    return true;
  }

`;
  if (!html.includes(queueCoolMarker)) throw new Error("Heat target function anchor missing");
  html = html.replace(queueCoolMarker, addHeatFunction + queueCoolMarker);

  html = mustReplace(
    html,
    "    const coolHeatConfig = normalized.coolHeat;\n    const armorConfig = normalized.armor;",
    "    const coolHeatConfig = normalized.coolHeat;\n    const addHeatConfig = normalized.addHeat;\n    const armorConfig = normalized.armor;",
    "capture addHeat config"
  );
  html = mustReplace(
    html,
    "    delete normalized.coolHeat;\n    delete normalized.armor;",
    "    delete normalized.coolHeat;\n    delete normalized.addHeat;\n    delete normalized.armor;",
    "remove addHeat from normalized effect"
  );
  html = mustReplace(
    html,
    "    if (coolHeatConfig) queueHeatTargetChoice(game, player, coolHeatConfig, context);\n    if (armorConfig)",
    "    if (coolHeatConfig) queueHeatTargetChoice(game, player, coolHeatConfig, context);\n    if (addHeatConfig) queueAddHeatTargetChoice(game, player, addHeatConfig, context);\n    if (armorConfig)",
    "dispatch addHeat"
  );

  html = mustReplace(
    html,
    "      if (entry.heat > before && resolveHeatTransformIfReady(game, player, entry, card)) return true;",
    String.raw`      if (entry.heat > before) {
        for (const threshold of heatThresholdsReached(card.heat, entry.heat)) {
          applyEffect(game, player, threshold.effect, { cardId: card.id, instanceId: entry.instanceId, source: "external-heat" });
        }
        if (resolveHeatTransformIfReady(game, player, entry, card)) return true;
        const overload = card.heat.overload;
        if (overload && heatOverloadReady(card.heat, entry.heat)) {
          const overloadInstruction = {
            instanceId: entry.instanceId,
            reset: Math.max(0, Number(overload.reset) || 0),
            selfDamage: Math.max(0, Number(overload.selfDamage) || 0),
            effect: clone(overload.effect || {})
          };
          if (overload.optional) {
            queueSpecialChoice(game, player, [
              { label: "Overload", effect: { __heatOverload: overloadInstruction } },
              { label: "Hold Heat", effect: { __noop: true } }
            ], { cardId: card.id, instanceId: entry.instanceId, source: "heat-overload" });
          } else {
            resolveHeatOverload(game, player, overloadInstruction, { cardId: card.id, instanceId: entry.instanceId, source: "heat-overload" });
          }
        }
      }`,
    "external Heat thresholds and overload"
  );

  html = mustReplace(
    html,
    "    if (instruction.remainingTargets > 0) queueHeatTargetChoice(game, player, { amount: Math.abs(Number(instruction.delta) || 1), targets: instruction.remainingTargets }, context);",
    String.raw`    if (instruction.remainingTargets > 0) {
      const nextConfig = {
        amount: Math.abs(Number(instruction.delta) || 1),
        targets: instruction.remainingTargets,
        target: instruction.targetRule,
        excludeSelf: instruction.excludeSelf
      };
      if (instruction.targetMode === "add") queueAddHeatTargetChoice(game, player, nextConfig, context);
      else queueHeatTargetChoice(game, player, nextConfig, context);
    }`,
    "multi-target Heat routing"
  );

  html = html.replace(
    "      (normalized.coolHeat ? Math.max(0, Number(normalized.coolHeat.amount) || 0) * 1.1 : 0);",
    "      (normalized.coolHeat ? Math.max(0, Number(normalized.coolHeat.amount) || 0) * 1.1 : 0) +\n      (normalized.addHeat ? Math.max(0, Number(normalized.addHeat.amount) || 0) * 1.5 : 0);"
  );

  fs.writeFileSync(htmlPath, html);
}

const testSource = String.raw`import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { loadCardPack } from "./test-utils.mjs";

const pack = await loadCardPack();
const byId = new Map(pack.CARDS.map(card => [card.id, card]));

test("Heat payoff mini-pack card definitions are present", () => {
  const imbuer = byId.get("sanctum_core_imbuer");
  assert.ok(imbuer);
  assert.equal(imbuer.faction, "blue");
  assert.equal(imbuer.cost, 7);
  assert.deepEqual(imbuer.effect.addHeat, { amount: 2, target: "friendlyHeatCard", excludeSelf: true });
  assert.equal(imbuer.ally.shield, 2);
  assert.equal(imbuer.doubleAlly.or[0].effect.repair.amount, 5);
  assert.equal(imbuer.doubleAlly.or[1].effect.shield, 5);

  const turret = byId.get("heavenlance_thermal_turret");
  assert.ok(turret);
  assert.equal(turret.type, "base");
  assert.equal(turret.defense, 8);
  assert.equal(turret.heat.max, 6);
  assert.equal(turret.heat.overload.at, 6);
  assert.equal(turret.heat.overload.effect.combat, 15);
  assert.equal(turret.heat.overload.reset, 0);

  const ark = byId.get("eightfold_drone_ark");
  assert.ok(ark);
  assert.equal(ark.faction, "yellow");
  assert.equal(ark.heat.max, 8);
  assert.equal(ark.heat.overload.at, 8);
  assert.deepEqual(ark.heat.overload.effect.createToken, { id: "drone", count: 4, zone: "discard" });
});

test("live battle resolver supports adding Heat to active Heat cards", async () => {
  const html = await fs.readFile(new URL("../../warrealms.html", import.meta.url), "utf8");
  assert.match(html, /function queueAddHeatTargetChoice\(/);
  assert.match(html, /const addHeatConfig = normalized\.addHeat;/);
  assert.match(html, /queueAddHeatTargetChoice\(game, player, addHeatConfig, context\)/);
  assert.match(html, /targetRule === "friendlyHeatShip" && targetCard\.type !== "ship"/);
  assert.match(html, /heatOverloadReady\(card\.heat, entry\.heat\)/);
});
`;
fs.writeFileSync(testPath, testSource);

// Remove the temporary patch machinery from the final branch tree.
for (const tempPath of [workflowPath, selfPath]) {
  if (fs.existsSync(tempPath)) fs.rmSync(tempPath);
}

console.log("Warrealms Heat payoff cards and resolver patch applied.");
