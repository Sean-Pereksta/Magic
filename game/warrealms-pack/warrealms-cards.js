import * as cardLibrary from "./warrealms-cards-base.js";

const BALANCE_VERSION = 22;

function getMutableCard(id) {
  const card = cardLibrary.CARD_MAP?.[id];
  if (!card) throw new Error(`WarRealms balance patch could not find card: ${id}`);
  return card;
}

function setCost(id, cost) {
  getMutableCard(id).cost = cost;
}

function mergeImmediateEffect(id, effect) {
  const card = getMutableCard(id);
  card.effect = { ...(card.effect || {}), ...effect };
}

function mergeOnAttach(id, effect) {
  const card = getMutableCard(id);
  card.attachment = {
    ...(card.attachment || {}),
    onAttach: {
      ...(card.attachment?.onAttach || {}),
      ...effect
    }
  };
}

function prependRulesText(id, sentence) {
  const card = getMutableCard(id);
  const current = String(card.text || "").trim();
  if (!current.includes(sentence)) card.text = `${sentence}${current ? ` ${current}` : ""}`;
}

// Test Lab balance pass — roughly 150,000 simulated matches.
// Keep this layer card-data only: no bot strategy or analytics behavior belongs here.
for (const [id, cost] of Object.entries({
  flesh_ledger: 5,
  grave_bargain: 4,
  lumen_knight: 4,
  ironroot_encampment: 5,
  rage_caller: 5,
  death_tithe: 6,
  choir_of_wings: 6,
  mercy_herald: 6,
  skullcrag_warlord: 6,
  saint_of_the_deep: 7,
  vacancy_engine: 3
})) {
  setCost(id, cost);
}

const eightfoldDroneArk = getMutableCard("eightfold_drone_ark");
eightfoldDroneArk.heat = {
  ...(eightfoldDroneArk.heat || {}),
  gain: 1,
  max: 7,
  overload: {
    ...(eightfoldDroneArk.heat?.overload || {}),
    at: 7
  }
};
eightfoldDroneArk.text = "When this card is played, add 1 Heat to itself.";
eightfoldDroneArk.heatText = "At 7 Heat, automatically spend all 7 Heat: create four Drones in your discard pile, then reset to 0 Heat.";

mergeImmediateEffect("drone_fabricator", {
  createToken: { id: "drone", count: 1, zone: "discard" }
});
prependRulesText("drone_fabricator", "Create 1 Drone in your discard pile immediately.");

mergeOnAttach("worker_barracks", {
  createToken: { id: "worker", count: 1, zone: "discard" }
});
prependRulesText("worker_barracks", "When attached, create 1 Worker in your discard pile immediately.");

mergeOnAttach("interceptor_launch_rail", {
  createToken: { id: "interceptor", count: 1, zone: "discard" }
});
prependRulesText("interceptor_launch_rail", "When attached, create 1 Interceptor in your discard pile immediately.");

mergeOnAttach("recursive_drone_hangar", {
  createToken: { id: "drone", count: 1, zone: "discard" }
});
prependRulesText("recursive_drone_hangar", "When attached, create 1 Drone in your discard pile immediately.");

mergeOnAttach("sanctuary_chorus", {
  repair: { amount: 3, attachedBase: true }
});
prependRulesText("sanctuary_chorus", "When attached, immediately repair that Expansion Base for 3.");

mergeOnAttach("reliquary_repair_arm", {
  repair: { amount: 4, attachedBase: true }
});
prependRulesText("reliquary_repair_arm", "When attached, immediately repair that Base for 4.");

export const WAR_REALMS_CARD_VERSION = BALANCE_VERSION;
export * from "./warrealms-cards-base.js";
