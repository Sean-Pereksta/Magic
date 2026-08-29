import test from "node:test";
import assert from "node:assert/strict";
import { describePresentationEvent } from "../ui/presentation-polish.js";

test("presentation labels make resource and damage amounts readable", () => {
  assert.equal(describePresentationEvent({ type: "HEAT_GAINED", amount: 3 })?.label, "HEAT +3");
  assert.equal(describePresentationEvent({ type: "BASE_DAMAGED", amount: 4 })?.label, "BASE HIT −4");
  assert.equal(describePresentationEvent({ type: "BASE_REPAIRED", amount: 2 })?.label, "REPAIR +2");
  assert.equal(describePresentationEvent({ type: "SHIELD_GAINED", amount: 5 })?.label, "SHIELD +5");
});

test("presentation labels retain high-impact trigger names", () => {
  assert.equal(describePresentationEvent({ type: "CARD_SACRIFICED" })?.label, "SACRIFICE");
  assert.equal(describePresentationEvent({ type: "BASE_CONSTRUCTION_COMPLETED" })?.label, "CONSTRUCTION COMPLETE");
  assert.equal(describePresentationEvent({ type: "DOUBLE_ALLY_TRIGGERED" })?.label, "DOUBLE ALLY");
  assert.equal(describePresentationEvent({ type: "CARD_TRANSFORMED" })?.label, "TRANSFORM");
});

test("unknown engine events do not invent a presentation", () => {
  assert.equal(describePresentationEvent({ type: "TURN_STARTED" }), null);
  assert.equal(describePresentationEvent({ type: "NOT_A_REAL_EVENT" }), null);
});
