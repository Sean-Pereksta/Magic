import assert from "node:assert/strict";
import test from "node:test";

import {
  ACTOR_SIMULATION_TIER,
  ACTOR_VISUAL_TIER,
  HearthmousePerformanceManager,
  catVisionIntervalSeconds,
  deferActorRigVisuals,
  registerCharacterVisualStage,
  registerRoomRenderGroup,
  runCharacterVisualScheduler,
  shouldInvalidateLosSample,
  simulationDecisionIntervalSeconds,
  visualUpdateIntervalSeconds,
} from "./hearthmouse-performance-manager.mjs";

function mockRoot(x, z) {
  const mesh = { isMesh: true, castShadow: true, userData: {} };
  return {
    position: { x, y: 0, z },
    rotation: { y: 0 },
    userData: {},
    parent: {},
    visible: true,
    mesh,
    traverse(visitor) {
      visitor(this);
      visitor(mesh);
    },
  };
}

function mockMouse(id, x, z, task = "waiting") {
  const root = mockRoot(x, z);
  let rigUpdates = 0;
  const mouse = {
    member: { id, alive: true },
    task,
    rig: {
      root,
      update() { rigUpdates++; },
    },
  };
  Object.defineProperty(mouse, "rigUpdates", { get: () => rigUpdates });
  return mouse;
}

function mockEngine() {
  return {
    time: 0,
    yaw: -Math.PI / 2,
    playerPosition: { x: -5, y: 0, z: 0 },
    snapshot: { phase: "foraging", night: 12 },
    world: {},
    mice: [],
    cats: [],
    __expansion: { isTouchDevice: false },
  };
}

test("visual, simulation, and cat-vision tiers use the requested frequency bands", () => {
  assert.equal(visualUpdateIntervalSeconds(ACTOR_VISUAL_TIER.IMMEDIATE), 0);
  assert.equal(visualUpdateIntervalSeconds(ACTOR_VISUAL_TIER.NEARBY), 1 / 24);
  assert.equal(visualUpdateIntervalSeconds(ACTOR_VISUAL_TIER.DISTANT), 1 / 8);
  assert.equal(visualUpdateIntervalSeconds(ACTOR_VISUAL_TIER.SLEEPING), Infinity);
  assert.equal(simulationDecisionIntervalSeconds(ACTOR_SIMULATION_TIER.NEARBY), 1 / 22);
  assert.equal(simulationDecisionIntervalSeconds(ACTOR_SIMULATION_TIER.DISTANT, true), 1 / 8);
  assert.equal(simulationDecisionIntervalSeconds(ACTOR_SIMULATION_TIER.IDLE, false), 1 / 3);
  assert.equal(catVisionIntervalSeconds("chase", ACTOR_VISUAL_TIER.DISTANT), 1 / 45);
  assert.equal(catVisionIntervalSeconds("alert", ACTOR_VISUAL_TIER.DISTANT), 1 / 24);
  assert.equal(catVisionIntervalSeconds("relaxed", ACTOR_VISUAL_TIER.DISTANT), 1 / 6);
});

test("LOS samples invalidate on meaningful state, topology, movement, rotation, and proximity changes", () => {
  const sample = {
    time: 2,
    state: "relaxed",
    routeRevision: 4,
    catRoom: "living",
    targetRoom: "kitchen",
    catX: -1,
    catZ: 0,
    targetX: 1,
    targetZ: 0,
    yaw: 0,
  };
  const current = { ...sample, time: 2.02, distanceSquared: 4 };
  assert.equal(shouldInvalidateLosSample(sample, current), false);
  assert.equal(shouldInvalidateLosSample(sample, { ...current, state: "chase" }), true);
  assert.equal(shouldInvalidateLosSample(sample, { ...current, routeRevision: 5 }), true);
  assert.equal(shouldInvalidateLosSample(sample, { ...current, targetRoom: "dining" }), true);
  assert.equal(shouldInvalidateLosSample(sample, { ...current, targetX: 1.3 }), true);
  assert.equal(shouldInvalidateLosSample(sample, { ...current, yaw: Math.PI / 4 }), true);
  assert.equal(shouldInvalidateLosSample(sample, { ...current, distanceSquared: 0.25 }), true);
});

test("portal room culling enables immediately and hides only after hysteresis", () => {
  const engine = mockEngine();
  const living = { userData: {}, visible: true };
  const kitchen = { userData: {}, visible: true };
  const garage = { userData: {}, visible: true };
  registerRoomRenderGroup(engine.world, "living", living);
  registerRoomRenderGroup(engine.world, "kitchen", kitchen);
  registerRoomRenderGroup(engine.world, "garage", garage);
  const manager = new HearthmousePerformanceManager(engine);

  manager.beginFrame(1 / 60);
  assert.equal(living.visible, true);
  assert.equal(kitchen.visible, true);
  assert.equal(garage.visible, false);
  assert.equal(manager.isRoomRenderActive("kitchen"), true);
  manager.endFrame();

  engine.yaw = Math.PI / 2;
  manager.beginFrame(0.2);
  assert.equal(kitchen.visible, true);
  manager.endFrame();
  manager.beginFrame(0.2);
  assert.equal(kitchen.visible, false);
  manager.endFrame();
});

test("distant AI work is staggered by actor identity and immediate promotion bypasses its bucket", () => {
  const engine = mockEngine();
  engine.yaw = Math.PI / 2;
  engine.mice = Array.from({ length: 18 }, (_, index) => mockMouse(`bucket-${index}`, 22, 4, "nesting"));
  const manager = new HearthmousePerformanceManager(engine);
  engine.__expansion.performanceManager = manager;
  let totalUpdates = 0;
  let worstFrame = 0;

  for (let frame = 0; frame < 60; frame++) {
    manager.beginFrame(1 / 60);
    let frameUpdates = 0;
    for (const mouse of engine.mice) {
      if (manager.shouldRunActorWork(mouse, "bucket-test")) frameUpdates++;
    }
    totalUpdates += frameUpdates;
    worstFrame = Math.max(worstFrame, frameUpdates);
    manager.endFrame();
  }

  assert.ok(totalUpdates > 0);
  assert.ok(worstFrame < engine.mice.length / 2);
  const promoted = engine.mice[0];
  manager.promoteActor(promoted, 1);
  manager.beginFrame(1 / 60);
  assert.equal(manager.getActorSimulationTier(promoted, "mouse"), ACTOR_SIMULATION_TIER.IMMEDIATE);
  assert.equal(manager.shouldRunActorWork(promoted, "bucket-test"), true);
  manager.endFrame();
});

test("the unified scheduler updates immediate actors every frame and throttles distant visuals and shadows", () => {
  const engine = mockEngine();
  const immediate = mockMouse("near", -4.8, 0, "escaping");
  const distant = mockMouse("far", 3.6, 9, "to-food");
  engine.mice.push(immediate, distant);
  const manager = new HearthmousePerformanceManager(engine);
  engine.__expansion.performanceManager = manager;
  deferActorRigVisuals(engine, immediate);
  deferActorRigVisuals(engine, distant);

  const stageCounts = new Map();
  registerCharacterVisualStage("performance-manager-test-stage", (actor) => {
    stageCounts.set(actor, (stageCounts.get(actor) ?? 0) + 1);
  }, 999);

  for (let frame = 0; frame < 60; frame++) {
    engine.time += 1 / 60;
    manager.beginFrame(1 / 60);
    immediate.rig.update(engine.time, 1, 1, false);
    distant.rig.update(engine.time, 1, 0, false);
    runCharacterVisualScheduler(engine, 1 / 60);
    manager.endFrame();
  }

  assert.equal(manager.getActorVisualTier(immediate, "mouse"), ACTOR_VISUAL_TIER.IMMEDIATE);
  assert.equal(manager.getActorVisualTier(distant, "mouse"), ACTOR_VISUAL_TIER.DISTANT);
  assert.equal(stageCounts.get(immediate), 60);
  assert.ok(stageCounts.get(distant) >= 6 && stageCounts.get(distant) <= 9);
  assert.equal(immediate.rigUpdates, 60);
  assert.ok(distant.rigUpdates >= 6 && distant.rigUpdates <= 9);
  assert.equal(immediate.rig.root.mesh.castShadow, true);
  assert.equal(distant.rig.root.mesh.castShadow, false);
});
