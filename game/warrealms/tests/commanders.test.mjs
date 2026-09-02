import test from "node:test";
import assert from "node:assert/strict";

import {
  CAMPAIGN_COMMANDERS,
  applyCommanderStarterDeck,
  chooseCommanderPerk,
  commanderBattleRules,
  commanderFactionLabel,
  commanderImagePath,
  commanderPerkOptions,
  getCampaignCommander,
  pendingCommanderPerkLevel
} from "../campaign/campaign-commanders.js";
import {
  CAMPAIGN_SCHEMA,
  createCampaignProfile,
  normalizeCampaignProfile
} from "../campaign/campaign-state.js";
import {
  campaignEventChoiceOptions,
  getCampaignEvent,
  resolveCampaignEventChoice
} from "../campaign/campaign-events.js";

test("six data-driven multi-faction commanders include portraits, passives, starters, and perk branches", () => {
  assert.equal(CAMPAIGN_COMMANDERS.length, 6);
  assert.equal(new Set(CAMPAIGN_COMMANDERS.map(commander => commander.id)).size, 6);
  for (const commander of CAMPAIGN_COMMANDERS) {
    assert.equal(commander.factions.length, 2);
    assert.match(commander.image, /\.png$/);
    assert.ok(commander.startingPassive?.id);
    assert.ok(commander.starterDeckChanges.length);
    assert.deepEqual(commander.perkTree.map(tier => tier.level), [3, 6, 9]);
    assert.ok(commander.perkTree.every(tier => tier.choices.length === 2));
  }
});

test("commander presentation supports single-faction identities and missing artwork", () => {
  const singleFaction = { name: "Sol", factions: ["blue"], image: "" };
  assert.equal(commanderFactionLabel(singleFaction), "Azure");
  assert.equal(commanderImagePath(singleFaction), "");
  assert.equal(commanderFactionLabel("missing"), "Unaligned");
  assert.equal(commanderImagePath("missing"), "");
});

test("new campaigns persist the commander and apply exactly one starter replacement", () => {
  const commander = getCampaignCommander("torak");
  const starter = [...Array(6).fill("starter_coin"), ...Array(4).fill("starter_blade")];
  const deck = applyCommanderStarterDeck(starter, commander);
  assert.equal(deck.length, 10);
  assert.equal(deck.filter(cardId => cardId === "starter_blade").length, 3);
  assert.equal(deck.filter(cardId => cardId === "knucklebone_rusher").length, 1);

  const profile = createCampaignProfile({ commanderId: commander.id, runId: "commander_run", now: 10 });
  assert.equal(profile.commanderId, commander.id);
  assert.equal(profile.commanderLevel, 1);
  assert.deepEqual(profile.commanderPerks, {});
  assert.deepEqual(profile.deck, deck);
});

test("commander upgrades persist and combine into battle rules", () => {
  const profile = createCampaignProfile({ commanderId: "kaelra", runId: "perk_run", now: 10 });
  profile.commanderLevel = 3;
  assert.equal(pendingCommanderPerkLevel(profile), 3);
  assert.deepEqual(commanderPerkOptions(profile).map(perk => perk.id), ["ember_tithe", "brood_oath"]);
  assert.equal(chooseCommanderPerk(profile, "ember_tithe", { now: 20 }).ok, true);
  assert.equal(pendingCommanderPerkLevel(profile), 0);
  assert.equal(commanderBattleRules(profile).firstSacrificeDraw, 1);
  assert.equal(commanderBattleRules(profile).firstSacrificeCombat, 1);

  const normalized = normalizeCampaignProfile(profile);
  assert.equal(normalized.commanderPerks[3], "ember_tithe");
  assert.equal(normalized.history.at(-1).perkId, "ember_tithe");
});

test("legacy and invalid campaign saves migrate without losing the run", () => {
  const legacy = normalizeCampaignProfile({
    schema: 3,
    runId: "legacy_run",
    status: "active",
    specialization: "merchant",
    region: 4,
    nodeIndex: 6,
    battlesWon: 5,
    authority: 31,
    maxAuthority: 60,
    deck: ["starter_coin", "starter_blade", "unspent_possibility"],
    collection: {},
    history: []
  }, { now: 100 });
  assert.equal(legacy.schema, CAMPAIGN_SCHEMA);
  assert.equal(legacy.commanderId, "aurelia");
  assert.equal(legacy.nodeIndex, 7);
  assert.equal(legacy.region, 4);
  assert.equal(legacy.authority, 31);
  assert.deepEqual(legacy.deck, ["starter_coin", "starter_blade", "unspent_possibility"]);
  assert.ok(legacy.history.some(entry => entry.type === "CAMPAIGN_MIGRATED"));

  const invalid = normalizeCampaignProfile({ ...legacy, schema: CAMPAIGN_SCHEMA, commanderId: "not_real" });
  assert.equal(invalid.commanderId, "aurelia");
});

test("commander factions unlock event-specific options and outcomes", () => {
  const profile = createCampaignProfile({ commanderId: "morriva", runId: "event_run", now: 10 });
  profile.currency = 40;
  const event = getCampaignEvent("broken_caravan");
  const merchantChoice = campaignEventChoiceOptions(profile, event).find(choice => choice.id === "quartermaster_terms");
  assert.equal(merchantChoice.available, true);
  const result = resolveCampaignEventChoice(profile, event.id, merchantChoice.id, {
    now: 20,
    randomFactionCardId: "unspent_possibility"
  });
  assert.equal(result.ok, true);
  assert.equal(profile.currency, 65);
  assert.ok(profile.deck.includes("unspent_possibility"));
});
