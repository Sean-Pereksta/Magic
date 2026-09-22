import test from "node:test";
import assert from "node:assert/strict";
import { bundledRuntime } from "./hearthmouse-runtime-fixture.mjs";
import { nestGateCoordinates, pointInsideBounds } from "./hearthmouse-mouse-pathing-guard.mjs";
import { foodLoad } from "./hearthmouse-survival-core.mjs";
import {
  buildTraversal, moveTraversingPlayer, pointSupported, advanceTraversal,
  isMousePassThroughCollider,
} from "./hearthmouse-survival-traversal.mjs";

const R = bundledRuntime(), I = R.I;
const v = (x = 0, z = 0) => new I.Vector3(x, 0.025, z);

function player(world = R.world(new I.Group()), indexed = false) {
  const engine = {
    world, time: 0, carriedFood: null, playerPosition: v(),
    playerVelocity: new I.Vector3(), onGround: true, emitNoise() {},
  };
  if (indexed) {
    // Exercise the indexed candidate path as well as the full-world fallback.
    engine.__expansion = { spatial: { colliders: {
      queryAabb(minX, minZ, maxX, maxZ, out) {
        out.length = 0;
        for (const c of world.colliders) {
          if (c.maxX >= minX && c.minX <= maxX && c.maxZ >= minZ && c.minZ <= maxZ) out.push(c);
        }
        return out;
      },
    } } };
  }
  return { engine, state: { traversal: buildTraversal(engine, I) } };
}

function walkTo(engine, state, goal, step = 0.02) {
  const motion = new I.Vector3();
  const initialDistance = Math.hypot(goal.x - engine.playerPosition.x, goal.z - engine.playerPosition.z);
  const limit = Math.ceil(initialDistance / step) + 30;
  for (let i = 0; i < limit; i++) {
    motion.set(goal.x - engine.playerPosition.x, 0, goal.z - engine.playerPosition.z);
    const remaining = motion.length();
    if (remaining < 0.002) return;
    motion.multiplyScalar(Math.min(1, step / remaining));
    engine.time += 1 / 60;
    moveTraversingPlayer(engine, state, I, motion);
    advanceTraversal(engine, state, 1 / 60);
  }
  assert.fail(`Mouse blocked at ${engine.playerPosition.x},${engine.playerPosition.z}; target ${goal.x},${goal.z}`);
}

for (const indexed of [false, true]) {
  for (const value of [0, 1, 2, 5]) {
    test(`real nest round trip, load ${value}, ${indexed ? "indexed" : "fallback"} collisions`, () => {
      const { engine: e, state } = player(undefined, indexed);
      const { nestCenter, nestBounds, nestDeposit } = e.world;
      const gate = nestGateCoordinates(nestBounds, nestDeposit);
      const goal = v(gate.x, gate.z), inside = v(nestCenter.x, gate.z);
      const masks = e.world.colliders.filter(c => c.catOnly).map(c => [c, c.catOnly, c.active]);
      assert.ok(masks.length > 0, "Use the real world's cat-only colliders, not an empty fixture");
      e.playerPosition.copy(nestCenter).setY(0.025);
      assert.ok(pointInsideBounds(nestBounds, e.playerPosition));
      for (let trip = 0; trip < 2; trip++) {
        e.carriedFood = null;
        walkTo(e, state, inside);
        walkTo(e, state, goal);
        assert.equal(pointInsideBounds(nestBounds, e.playerPosition), false, "Mouse must leave the nest");
        e.carriedFood = value ? { kind: "cheese", value } : null;
        walkTo(e, state, inside, 0.02 * foodLoad(e.carriedFood).speed);
        walkTo(e, state, v(nestDeposit.x, nestDeposit.z));
        assert.ok(pointInsideBounds(nestBounds, e.playerPosition), "Mouse must return to the deposit area");
        assert.equal(state.traversal.elevation, 0, "No climbing workaround should be required at the entrance");
      }
      for (const [c, catOnly, active] of masks) {
        assert.equal(c.catOnly, catOnly, "Never mutate shared cat collision masks");
        assert.equal(c.active, active, "Never disable the nest's protection");
      }
    });
  }
}

test("cats remain excluded from the real nest after the mouse crosses its entrance", () => {
  const { engine: e, state } = player();
  const gate = nestGateCoordinates(e.world.nestBounds, e.world.nestDeposit);
  e.playerPosition.copy(e.world.nestCenter).setY(0.025);
  walkTo(e, state, v(e.playerPosition.x, gate.z));
  walkTo(e, state, v(gate.x, gate.z));
  const cat = v(gate.x + 0.3, gate.z), motion = new I.Vector3(-0.02, 0, 0);
  for (let i = 0; i < 160; i++) I.moveActor(cat, motion, 0.205, e.world.colliders, "cat");
  assert.equal(pointInsideBounds(e.world.nestBounds, cat), false);
});

function box(overrides = {}) {
  return { minX: 0.2, maxX: 0.5, minZ: -1, maxZ: 1, minY: 0, maxY: 1, active: true, ...overrides };
}
function fixture(colliders) {
  return player({ root: new I.Group(), colliders });
}
function pressEast(engine, state) {
  for (let i = 0; i < 50; i++) moveTraversingPlayer(engine, state, I, new I.Vector3(0.02, 0, 0));
}

test("ground-level cat-only barriers never become mouse walls or phantom platforms", () => {
  for (const minY of [undefined, -0.02, 0, 0.01]) {
    const c = box({ catOnly: true, minY });
    assert.equal(isMousePassThroughCollider(c), true);
    for (const elevation of [0, 0.08, 0.4]) {
      const { engine, state } = fixture([c]);
      state.traversal.elevation = elevation;
      pressEast(engine, state);
      assert.ok(engine.playerPosition.x > c.maxX + 0.055);
      assert.equal(state.traversal.solidCopies.has(c), false);
    }
    assert.equal(pointSupported(v(0.3), c, c.maxY), false);
    assert.equal(c.catOnly, true);
  }
});

test("solid walls still block mice on the floor and at climbing height", () => {
  const wall = box();
  for (const elevation of [0, 0.4]) {
    const { engine, state } = fixture([wall]);
    state.traversal.elevation = elevation;
    pressEast(engine, state);
    assert.ok(engine.playerPosition.x < wall.minX);
  }
  const { engine, state } = fixture([box({ active: false })]);
  pressEast(engine, state);
  assert.ok(engine.playerPosition.x > 0.5);
});

test("raised furniture retains scurry clearance, elevated collision, and support", () => {
  const furniture = box({ catOnly: true, minY: 0.3, maxY: 0.8 });
  assert.equal(isMousePassThroughCollider(furniture), false);
  const ground = fixture([furniture]);
  pressEast(ground.engine, ground.state);
  assert.ok(ground.engine.playerPosition.x > furniture.maxX);
  const elevated = fixture([furniture]);
  elevated.state.traversal.elevation = 0.4;
  pressEast(elevated.engine, elevated.state);
  assert.ok(elevated.engine.playerPosition.x < furniture.minX);
  assert.equal(pointSupported(v(0.3), furniture, 0.8), true);
  assert.equal(furniture.catOnly, true);
});

test("cat exclusion volumes do not prevent scrambling onto real low obstacles", () => {
  const step = box({ maxY: 0.08 });
  const exclusion = box({ minX: -1, maxX: 1, catOnly: true });
  const { engine, state } = fixture([step, exclusion]);
  pressEast(engine, state);
  assert.ok(engine.playerPosition.x > step.minX);
  assert.equal(state.traversal.elevation, step.maxY);
});
