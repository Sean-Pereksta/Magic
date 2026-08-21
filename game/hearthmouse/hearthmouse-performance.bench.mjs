import { performance } from "node:perf_hooks";

import { FoodCandidateCache, SpatialGrid } from "./hearthmouse-expansion.mjs";
import {
  HearthmousePerformanceManager,
  deferActorRigVisuals,
  registerCharacterVisualStage,
  runCharacterVisualScheduler,
} from "./hearthmouse-performance-manager.mjs";

const FOOD_COUNT = 1200;
const MOUSE_COUNT = 18;
const CAT_COUNT = 3;
const ITERATIONS = 450;
const nest = { x: -6.78, z: 3.74 };

const foods = Array.from({ length: FOOD_COUNT }, (_, index) => ({
  id: index,
  value: 1 + (index % 6),
  depth: index % 8,
  room: "living",
  position: {
    x: -12 + ((index * 37) % 370) / 10,
    z: -18 + ((index * 53) % 330) / 10,
  },
}));
const mice = Array.from({ length: MOUSE_COUNT }, (_, index) => ({
  x: nest.x + (index % 6) * 0.35,
  z: nest.z + Math.floor(index / 6) * 0.42,
  caution: 0.25 + (index % 7) * 0.1,
}));
const cats = Array.from({ length: CAT_COUNT }, (_, index) => ({
  x: -1 + index * 9,
  z: -5 + index * 4,
  yaw: 0.7 + index,
  lookYaw: 0.2 - index * 0.08,
}));

const grid = new SpatialGrid(2.5);
for (const food of foods) grid.insert(food, food.position.x, food.position.z);
const cache = new FoodCandidateCache().rebuild(foods, nest);
const queryBuffer = [];

function dynamicExposure(position) {
  let exposure = 0;
  for (const cat of cats) {
    const distance = Math.hypot(cat.x - position.x, cat.z - position.z);
    if (distance < 5) exposure = Math.max(exposure, 1 - distance / 5);
  }
  return exposure;
}

function naiveLocalFoodPass() {
  let checksum = 0;
  for (const mouse of mice) {
    for (const food of foods) {
      const nestDistance = Math.hypot(food.position.x - nest.x, food.position.z - nest.z);
      if (nestDistance > 6.2) continue;
      checksum += nestDistance + food.depth + food.value + Math.hypot(food.position.x - mouse.x, food.position.z - mouse.z) + dynamicExposure(food.position);
    }
  }
  return checksum;
}

function indexedCachedLocalFoodPass() {
  let checksum = 0;
  grid.queryRadius(nest.x, nest.z, 6.2, queryBuffer);
  for (const mouse of mice) {
    for (const food of queryBuffer) {
      const metadata = cache.get(food);
      if (metadata.nestDistance > 6.2) continue;
      checksum += metadata.nestDistance + metadata.depth + metadata.value + Math.hypot(food.position.x - mouse.x, food.position.z - mouse.z) + dynamicExposure(food.position);
    }
  }
  return checksum;
}

function naiveDangerPass() {
  let checksum = 0;
  for (const mouse of mice) {
    for (const cat of cats) {
      const facing = cat.yaw + cat.lookYaw * 0.5;
      const dx = mouse.x - cat.x;
      const dz = mouse.z - cat.z;
      const length = Math.max(0.001, Math.hypot(dx, dz));
      checksum += dx / length * -Math.sin(facing) + dz / length * -Math.cos(facing);
    }
  }
  return checksum;
}

const dangerSnapshot = cats.map(() => ({ x: 0, z: 0, forwardX: 0, forwardZ: 0 }));

function snapshottedDangerPass() {
  for (let index = 0; index < cats.length; index++) {
    const cat = cats[index];
    const entry = dangerSnapshot[index];
    const facing = cat.yaw + cat.lookYaw * 0.5;
    entry.x = cat.x;
    entry.z = cat.z;
    entry.forwardX = -Math.sin(facing);
    entry.forwardZ = -Math.cos(facing);
  }
  let checksum = 0;
  for (const mouse of mice) {
    for (const cat of dangerSnapshot) {
      const dx = mouse.x - cat.x;
      const dz = mouse.z - cat.z;
      const length = Math.max(0.001, Math.hypot(dx, dz));
      checksum += dx / length * cat.forwardX + dz / length * cat.forwardZ;
    }
  }
  return checksum;
}

function benchmark(label, operation, iterations = ITERATIONS) {
  let checksum = 0;
  for (let index = 0; index < 20; index++) checksum += operation();
  const started = performance.now();
  for (let index = 0; index < iterations; index++) checksum += operation();
  return { label, milliseconds: performance.now() - started, checksum };
}

const STRESS_POSITIONS = Object.freeze([
  [-6, 1], [-3, -2], [4, 1], [5, -9], [3.5, 9], [-3, -14],
  [0.5, -14], [-7.8, -13], [13, 4], [17, 4], [4, 13], [10, -9],
  [10, -14], [22, 4], [-8, -8], [6, -7], [15.8, 3], [20, 6],
]);
const PLAYER_STRESS_PATH = Object.freeze([
  [-5, 0], [-0.5, 0], [3.6, 2], [3.6, 9], [3.6, 13], [3.6, 9], [3.6, 2], [-0.5, 0], [-5, 0],
]);

function stressRoot(x, z) {
  const shadowMesh = { isMesh: true, castShadow: true, userData: {} };
  return {
    position: { x, y: 0, z },
    rotation: { y: 0, z: 0 },
    userData: {},
    parent: {},
    visible: true,
    traverse(visitor) {
      visitor(this);
      visitor(shadowMesh);
    },
  };
}

function stressMouse(index) {
  const [x, z] = STRESS_POSITIONS[index % STRESS_POSITIONS.length];
  return {
    member: { id: `stress-mouse-${index}`, alive: true },
    task: index % 5 === 0 ? "to-food" : index % 5 === 1 ? "returning" : "nesting",
    rig: { root: stressRoot(x, z), update() {} },
  };
}

function stressCat(index, chase) {
  const [x, z] = STRESS_POSITIONS[(index * 5 + 2) % STRESS_POSITIONS.length];
  const root = stressRoot(x, z);
  return {
    id: `stress-cat-${index}`,
    state: chase ? "chase" : index % 2 ? "alert" : "relaxed",
    targetId: chase ? "player" : null,
    pouncePhase: "none",
    lookYaw: 0,
    yaw: 0,
    leisureMode: null,
    rig: {
      root,
      body: { position: { y: 0 }, rotation: { z: 0 } },
      chest: { position: { y: 0 } },
      headPivot: { rotation: { x: 0, y: 0 } },
      legs: [],
      tail: [],
      eyes: [],
      update() {},
    },
  };
}

registerCharacterVisualStage("benchmark-animation-sample", (_actor, context) => {
  context.recordAnimationSample();
}, 1000);

function runManagerStressScenario(label, mouseCount, catCount, {
  chase = false,
  isTouchDevice = false,
  rapidCamera = false,
  traverseRooms = false,
} = {}) {
  const engine = {
    time: 0,
    yaw: -Math.PI / 2,
    playerPosition: { x: -5, y: 0, z: 0 },
    snapshot: { phase: "foraging", night: 12 },
    world: {},
    mice: Array.from({ length: mouseCount }, (_, index) => stressMouse(index)),
    cats: Array.from({ length: catCount }, (_, index) => stressCat(index, chase)),
    __expansion: { isTouchDevice },
  };
  const manager = new HearthmousePerformanceManager(engine, { isTouchDevice });
  engine.__expansion.performanceManager = manager;
  const actors = [...engine.mice, ...engine.cats];
  for (const actor of actors) deferActorRigVisuals(engine, actor);

  const frameDelta = 1 / 60;
  let worstAiUpdates = 0;
  let roomSetChanges = 0;
  let previousRoomSignature = "";
  const started = performance.now();
  for (let frame = 0; frame < 600; frame++) {
    engine.time += frameDelta;
    if (traverseRooms) {
      const segment = Math.floor(frame / 75) % (PLAYER_STRESS_PATH.length - 1);
      const progress = (frame % 75) / 75;
      const from = PLAYER_STRESS_PATH[segment];
      const to = PLAYER_STRESS_PATH[segment + 1];
      engine.playerPosition.x = from[0] + (to[0] - from[0]) * progress;
      engine.playerPosition.z = from[1] + (to[1] - from[1]) * progress;
      engine.yaw = Math.atan2(-(to[0] - from[0]), -(to[1] - from[1]));
    } else {
      engine.yaw = -Math.PI / 2 + Math.sin(frame / 37) * 1.4;
    }
    if (rapidCamera) engine.yaw = frame % 8 * Math.PI / 4 - Math.PI;
    manager.beginFrame(frameDelta);
    const roomSignature = [...manager.activeRenderRooms].sort().join(":");
    if (previousRoomSignature && roomSignature !== previousRoomSignature) roomSetChanges++;
    previousRoomSignature = roomSignature;
    let frameAiUpdates = 0;
    for (const actor of actors) {
      if (manager.shouldRunActorWork(actor, "benchmark-ai")) {
        manager.recordAiDecision(actor);
        frameAiUpdates++;
      } else {
        manager.recordAiSkip(actor);
      }
      actor.rig.update(engine.time, 1, 0, false);
    }
    worstAiUpdates = Math.max(worstAiUpdates, frameAiUpdates);
    for (const cat of engine.cats) {
      if (!manager.shouldSampleCatVision(cat)) continue;
      manager.record("catLosChecks");
      manager.record("losRaycasts", 5);
    }
    runCharacterVisualScheduler(engine, frameDelta);
    manager.endFrame();
  }
  return {
    label,
    actorCount: actors.length,
    milliseconds: performance.now() - started,
    worstAiUpdates,
    roomSetChanges,
    snapshot: manager.getDebugSnapshot(),
  };
}

const results = [
  benchmark("food-before-full-scan", naiveLocalFoodPass),
  benchmark("food-after-grid-cache", indexedCachedLocalFoodPass),
  benchmark("danger-before-per-mouse-state", naiveDangerPass, ITERATIONS * 10),
  benchmark("danger-after-shared-snapshot", snapshottedDangerPass, ITERATIONS * 10),
];

for (const result of results) console.log(`${result.label}: ${result.milliseconds.toFixed(2)}ms (${result.checksum.toFixed(2)})`);
console.log(`food speedup: ${(results[0].milliseconds / results[1].milliseconds).toFixed(2)}x`);
console.log(`danger speedup: ${(results[2].milliseconds / results[3].milliseconds).toFixed(2)}x`);

const stressResults = [
  runManagerStressScenario("small", 4, 1),
  runManagerStressScenario("medium", 12, 2),
  runManagerStressScenario("large", 20, 3),
  runManagerStressScenario("chase", 20, 3, { chase: true, traverseRooms: true }),
  runManagerStressScenario("camera", 12, 2, { rapidCamera: true }),
  runManagerStressScenario("mobile-large", 20, 3, { isTouchDevice: true }),
];
for (const result of stressResults) {
  const stats = result.snapshot;
  const fullVisualRate = result.actorCount * 60;
  const visualSavings = fullVisualRate > 0 ? 1 - stats.characterVisualUpdatesPerSecond / fullVisualRate : 0;
  console.log(
    `${result.label}: ${result.milliseconds.toFixed(2)}ms, rooms ${stats.visibleRooms}/${stats.totalRooms} `
    + `(hidden ${stats.hiddenRooms}), `
    + `visuals ${stats.characterVisualUpdatesPerSecond.toFixed(1)}/s, samples ${stats.animationSamplesPerSecond.toFixed(1)}/s, `
    + `visual savings ${(visualSavings * 100).toFixed(1)}%, animation skips ${stats.distantAnimationSamplesSkippedPerSecond.toFixed(1)}/s, `
    + `AI ${stats.aiDecisionUpdatesPerSecond.toFixed(1)}/s (worst frame ${result.worstAiUpdates}), `
    + `AI skips ${stats.distantAiUpdatesSkippedPerSecond.toFixed(1)}/s, `
    + `LOS ${stats.catLosChecksPerSecond.toFixed(1)}/s, rays ${stats.losRaycastsPerSecond.toFixed(1)}/s, `
    + `shadows ${stats.shadowActors}, sleeping ${stats.sleepingVisualActors}, room-set changes ${result.roomSetChanges}, `
    + `frame ${stats.frameTimeMs.toFixed(2)}ms / worst ${stats.worstFrameTimeMs.toFixed(2)}ms, FPS ${stats.averageFps.toFixed(1)}`,
  );
}
