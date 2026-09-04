import test from "node:test";
import assert from "node:assert/strict";
import { loadCardPack } from "./test-utils.mjs";

function cardMap(pack) {
  return Object.fromEntries(pack.CARDS.map(card => [card.id, card]));
}

test("Test Lab balance pass applies requested card costs", async () => {
  const pack = await loadCardPack();
  const cards = cardMap(pack);
  assert.equal(pack.WAR_REALMS_CARD_VERSION, 22);

  const expectedCosts = {
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
  };

  for (const [id, expected] of Object.entries(expectedCosts)) {
    assert.equal(cards[id]?.cost, expected, `${id} cost`);
  }
});

test("Eightfold Drone Ark self-heats and overloads at 7 without increasing its payoff", async () => {
  const cards = cardMap(await loadCardPack());
  const ark = cards.eightfold_drone_ark;
  assert.equal(ark.cost, 6);
  assert.equal(ark.heat?.gain, 1);
  assert.equal(ark.heat?.max, 7);
  assert.equal(ark.heat?.overload?.at, 7);
  assert.equal(ark.heat?.overload?.reset, 0);
  assert.deepEqual(ark.heat?.overload?.effect?.createToken, {
    id: "drone",
    count: 4,
    zone: "discard"
  });
});

test("token infrastructure cards provide one immediate token", async () => {
  const cards = cardMap(await loadCardPack());
  assert.deepEqual(cards.drone_fabricator?.effect?.createToken, {
    id: "drone",
    count: 1,
    zone: "discard"
  });
  assert.deepEqual(cards.worker_barracks?.attachment?.onAttach?.createToken, {
    id: "worker",
    count: 1,
    zone: "discard"
  });
  assert.deepEqual(cards.interceptor_launch_rail?.attachment?.onAttach?.createToken, {
    id: "interceptor",
    count: 1,
    zone: "discard"
  });
  assert.deepEqual(cards.recursive_drone_hangar?.attachment?.onAttach?.createToken, {
    id: "drone",
    count: 1,
    zone: "discard"
  });
});

test("weak repair attachments gain host-specific immediate repair", async () => {
  const cards = cardMap(await loadCardPack());
  assert.deepEqual(cards.sanctuary_chorus?.attachment?.onAttach?.repair, {
    amount: 3,
    attachedBase: true
  });
  assert.deepEqual(cards.reliquary_repair_arm?.attachment?.onAttach?.repair, {
    amount: 4,
    attachedBase: true
  });
});
