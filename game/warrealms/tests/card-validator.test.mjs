import test from "node:test";
import assert from "node:assert/strict";

import { validateCardLibrary } from "../engine/card-validator.js";
import { CAMPAIGN_BOSSES } from "../campaign/campaign-bosses.js";
import { loadCardPack } from "./test-utils.mjs";

test("the complete War Realms card pack validates cleanly", async () => {
  const pack = await loadCardPack();
  const result = validateCardLibrary({
    cards: pack.CARDS,
    starterCards: pack.STARTER_CARDS,
    factions: pack.FACTIONS,
    bosses: CAMPAIGN_BOSSES
  });
  assert.equal(result.errors.length, 0, JSON.stringify(result.errors, null, 2));
  assert.equal(result.warnings.length, 0, JSON.stringify(result.warnings, null, 2));
  assert.equal(pack.COLLECTIBLE_CARDS.length, 414);
  assert.equal(pack.COLLECTIBLE_CARDS.some(card => card.campaignOnly), false);
  assert.match(result.info.at(-1).message, new RegExp(`Validated ${pack.CARDS.length + Object.keys(pack.STARTER_CARDS).length} War Realms card definitions`));
});

test("all Heat branching evolutions use and display Heat 3", async () => {
  const pack = await loadCardPack();
  const heatTransforms = pack.CARDS.filter(card => card.transform?.trigger === "heat");
  assert.equal(heatTransforms.length, 24);
  for (const card of heatTransforms) {
    assert.equal(card.transform.required, 3, `${card.id} must transform at Heat 3`);
    assert.match([card.text, card.transformText, card.heatText].filter(Boolean).join(" "), /Heat 3/i, `${card.id} must display Heat 3`);
  }
});

test("validator reports duplicate IDs, unsupported events, and Heat text mismatches", () => {
  const factions = { red: { id: "red" } };
  const base = { id: "duplicate", name: "One", image: "one.png", faction: "red", type: "ship", shop_cost: 1 };
  const result = validateCardLibrary({
    factions,
    cards: [
      base,
      { ...base, name: "Two" },
      {
        id: "heat_card",
        name: "Heat Card",
        image: "heat.png",
        faction: "red",
        type: "ship",
        shop_cost: 1,
        text: "At Heat 4, transform.",
        trigger: { event: "NOT_A_REAL_EVENT", effect: { combat: 1 } },
        transform: { trigger: "heat", required: 3, into: "missing" }
      }
    ]
  });
  assert.ok(result.errors.some(error => error.code === "DUPLICATE_CARD_ID"));
  assert.ok(result.errors.some(error => error.code === "UNSUPPORTED_TRIGGER_EVENT"));
  assert.ok(result.warnings.some(warning => warning.code === "HEAT_TEXT_MISMATCH"));
});
