import { performance } from "node:perf_hooks";

import { FoodCandidateCache, SpatialGrid } from "./hearthmouse-expansion.mjs";

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

function benchmark(label, operation) {
  let checksum = 0;
  for (let index = 0; index < 20; index++) checksum += operation();
  const started = performance.now();
  for (let index = 0; index < ITERATIONS; index++) checksum += operation();
  return { label, milliseconds: performance.now() - started, checksum };
}

const results = [
  benchmark("food-before-full-scan", naiveLocalFoodPass),
  benchmark("food-after-grid-cache", indexedCachedLocalFoodPass),
  benchmark("danger-before-per-mouse-state", naiveDangerPass),
  benchmark("danger-after-shared-snapshot", snapshottedDangerPass),
];

for (const result of results) console.log(`${result.label}: ${result.milliseconds.toFixed(2)}ms (${result.checksum.toFixed(2)})`);
console.log(`food speedup: ${(results[0].milliseconds / results[1].milliseconds).toFixed(2)}x`);
console.log(`danger speedup: ${(results[2].milliseconds / results[3].milliseconds).toFixed(2)}x`);
