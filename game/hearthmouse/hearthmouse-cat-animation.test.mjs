import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { computeGroundingOffset } from "./hearthmouse-character-models-base.mjs";
import { CAT_MODEL_RIG, catGroomPose, catPouncePose } from "./hearthmouse-character-models-polish.mjs";

test("procedural animation targets the uploaded cat model's exact 19-node skeleton", () => {
  assert.deepEqual(CAT_MODEL_RIG, {
    root: 16,
    spine: 9,
    chest: 5,
    head: 0,
    tail: [8, 7, 6],
    frontLeft: [2, 1],
    frontRight: [4, 3],
    hindLeft: [12, 11, 10],
    hindRight: [15, 14, 13],
  });
});

test("pounce pose has a deep load, airborne reach, landing brace, and smooth recovery", () => {
  const loaded = catPouncePose("windup", 1);
  const airborne = catPouncePose("flight", 0.5);
  const braced = catPouncePose("flight", 0.92);
  const settling = catPouncePose("landing", 0.5);
  const recovered = catPouncePose("landing", 1);
  assert.ok(loaded.hindMiddle > 1 && loaded.chestPitch > 0.4);
  assert.ok(airborne.lift >= 0.119 && airborne.frontLower < -1.3);
  assert.ok(braced.hindMiddle > 0.35 && braced.frontUpper < -0.6);
  assert.ok(settling.active && Math.abs(settling.chestPitch) > 0.02);
  assert.equal(recovered.active, false);
  assert.equal(recovered.lift, 0);
});

test("grooming visibly progresses through paw licking, face wiping, flank licking, and reset", () => {
  const paw = catGroomPose(0.3, -1);
  const face = catGroomPose(0.58, -1);
  const flank = catGroomPose(0.8, -1);
  const mirrored = catGroomPose(0.3, 1);
  assert.equal(paw.stage, "paw-lick");
  assert.ok(paw.raisedFrontUpper < -0.85 && paw.headPitch > 0.6);
  assert.equal(face.stage, "face-wipe");
  assert.ok(Math.abs(face.raisedFrontRoll) > 0.3);
  assert.equal(flank.stage, "flank-lick");
  assert.ok(Math.abs(flank.headYaw) > 0.8 && Math.abs(flank.spineYaw) > 0.35);
  assert.equal(Math.sign(paw.headYaw), -Math.sign(mirrored.headYaw));
  assert.equal(catGroomPose(1, -1).active, false);
});

test("grounding uses a transformed bounding box and one cached official baseline", () => {
  assert.equal(computeGroundingOffset(0.025, -0.075), 0.1);
  assert.equal(computeGroundingOffset(Number.NaN, 1), 0);
  const baseSource = fs.readFileSync(new URL("./hearthmouse-character-models-base.mjs", import.meta.url), "utf8");
  const polishSource = fs.readFileSync(new URL("./hearthmouse-character-models-polish.mjs", import.meta.url), "utf8");
  assert.match(baseSource, /setFromObject\(wrapper, true\)/);
  assert.match(baseSource, /groundingProfiles\.set\(profileKey, grounding\)/);
  assert.match(baseSource, /existingProfile\?\.baselineY/);
  assert.match(baseSource, /cat:shared-official-floor/);
  assert.match(polishSource, /auditAndRepairGrounding/);
  assert.match(polishSource, /grounding\.baselineY/);
  assert.match(polishSource, /lastPouncePhase = "landing"/);
});
