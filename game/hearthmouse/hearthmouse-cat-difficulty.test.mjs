import assert from "node:assert/strict";
import test from "node:test";
import { CAT_DIFFICULTIES, applyCatDifficulty, catDifficultyProfile } from "./hearthmouse-cat-difficulty.mjs";
import { chooseHuntMode, observedIntercept, witnessedSearchGoal } from "./hearthmouse-predator-upgrade.mjs";

test("cat difficulty changes which tactics cats choose in the same encounter", () => {
  const { gentle, standard, hard, nightmare } = CAT_DIFFICULTIES;
  const encounter = { distance: 4, roll: 0.24, moving: 0.8 };
  assert.equal(chooseHuntMode({ ...encounter, profile: gentle }), "chase");
  assert.equal(chooseHuntMode({ ...encounter, profile: standard }), "stalk");
  assert.equal(chooseHuntMode({ ...encounter, profile: nightmare }), "ambush");
  assert.equal(chooseHuntMode({ ...encounter, profile: nightmare, anotherAmbusher: true }), "stalk");
  assert.deepEqual([gentle, standard, hard, nightmare].map(profile => profile.searches), [0, 1, 2, 3]);
  assert.ok(nightmare.pounceRange > hard.pounceRange && nightmare.windupScale < gentle.windupScale);
});

test("night start applies the selection with Standard as the fallback", () => {
  const engine = { catDifficultySelection: "hard" };
  assert.equal(catDifficultyProfile(engine), CAT_DIFFICULTIES.standard);
  applyCatDifficulty(engine);
  assert.equal(catDifficultyProfile(engine), CAT_DIFFICULTIES.hard);
});

test("intercepts and search sweeps use a cat's last witnessed movement", () => {
  const output = {
    copy(point) { return this.set(point.x, point.y, point.z); },
    set(x, y, z) { Object.assign(this, { x, y, z }); return this; },
  };
  const hunt = { seen: { x: 2, y: 0, z: 3 }, seenAt: 10, vx: 1, vz: 0 };
  assert.equal(observedIntercept(hunt, 10.1, output, CAT_DIFFICULTIES.gentle.leadTime).x, 2);
  assert.ok(observedIntercept(hunt, 10.1, output, CAT_DIFFICULTIES.nightmare.leadTime).x > 2.4);
  assert.equal(observedIntercept(hunt, 10.4, output, CAT_DIFFICULTIES.nightmare.leadTime).x, 2);
  assert.ok(witnessedSearchGoal(hunt, 1, 1, output).z > 3);
  assert.ok(witnessedSearchGoal(hunt, 2, 1, output).z < 3);
  assert.equal(witnessedSearchGoal({ ...hunt, vx: 0 }, 0, 1, output), null);
});
