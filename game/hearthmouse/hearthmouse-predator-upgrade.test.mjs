import test from "node:test";
import assert from "node:assert/strict";
import { bundledRuntime } from "./hearthmouse-runtime-fixture.mjs";
import { prepareProceduralRig, updateProceduralActor, poseCarriedFood } from "./hearthmouse-procedural-characters.mjs";
import { carveMouseOpening, addNestHabitat, isFoodPositionClear } from "./hearthmouse-habitat.mjs";
import { chooseHuntMode, rememberObservation, observedIntercept, installPredatorUpgrade } from "./hearthmouse-predator-upgrade.mjs";
import { installEnginePatches } from "./hearthmouse-expansion-core.mjs";
import { installExpansionSightGuard } from "./hearthmouse-expansion-startup-guard.mjs";
import { extractPersistentRoomStructures, applyPersistentStructureVisibility, batchPersistentStructures } from "./hearthmouse-render-stability.mjs";

const { I, cat: makeCat, mouse: makeMouse, Raycaster } = bundledRuntime();
const vector = (x = 0, z = 0) => new I.Vector3(x, 0, z);

test("real procedural rigs stay grounded, animate turns, and reduce geometry work", () => {
  const actor = { id: "mabel", state: "relaxed", rig: makeCat(), speed: 0, pouncePhase: "none" };
  const state = prepareProceduralRig(actor);
  assert.ok(state.trianglesAfter < state.trianglesBefore * 0.8, `${state.trianglesBefore} → ${state.trianglesAfter}`);
  const context = { kind: "cat", delta: 1 / 60, engine: { time: 0 }, recordAnimationSample() {} };
  for (let i = 0; i < 180; i++) {
    context.engine.time += context.delta;
    actor.rig.root.position.x += 0.01;
    actor.rig.root.rotation.y += 0.01;
    actor.rig.update(context.engine.time, 0.6, actor.state, 0, 0);
    updateProceduralActor(actor, context);
    for (const leg of actor.rig.legs) assert.ok(leg.position.y > 0.20);
    assert.ok(Number.isFinite(actor.rig.body.rotation.z));
  }
  assert.ok(Math.abs(actor.rig.body.rotation.z) > 0.001);
  const standing = actor.rig.body.position.y;
  actor.hunt = { mode: "stalk" };
  updateProceduralActor(actor, context);
  assert.ok(actor.rig.body.position.y < standing - 0.04);
  actor.pouncePhase = "flight";
  updateProceduralActor(actor, context);
  assert.equal(actor.rig.legs[0].rotation.x, -0.9);
  assert.equal(prepareProceduralRig(actor), state);
});

test("stationary mice do not scurry and carried food stays at the muzzle", () => {
  const actor = { member: { id: "mouse" }, task: "waiting", rig: makeMouse() };
  const context = { kind: "mouse", delta: 1 / 60, engine: { time: 1 } };
  actor.rig.update(1, 0, 0, false);
  updateProceduralActor(actor, context);
  assert.ok(actor.rig.paws.every(p => p.rotation.x === 0));
  const food = { value: 3, mesh: new I.Group(), __pickupTime: 0 };
  poseCarriedFood(food, 1, 0.7, false);
  assert.ok(food.mesh.position.z < -0.1 && food.mesh.position.z > -0.14);
  assert.ok(food.mesh.scale.x < 0.75);
});

test("interception learns velocity only from sight and expires without following hidden movement", () => {
  const h = { seen: vector(), seenAt: -Infinity, targetId: null, vx: 0, vz: 0 };
  rememberObservation(h, { id: "player", visible: 1, position: vector(1, 1) }, 1);
  rememberObservation(h, { id: "player", visible: 1, position: vector(1.2, 1) }, 1.2);
  const output = vector();
  observedIntercept(h, 1.3, output);
  assert.ok(output.x > 1.2 && output.x < 2);
  assert.equal(rememberObservation(h, { id: "player", visible: 0, position: vector(8, 8) }, 1.4), false);
  observedIntercept(h, 2, output);
  assert.equal(output.x, 1.2);
  assert.equal(output.z, 1);
  assert.equal(chooseHuntMode({ distance: 0.8, roll: 0.01 }), "chase");
  assert.notEqual(chooseHuntMode({ distance: 4, roll: 0.01, anotherAmbusher: true }), "ambush");
});

test("cut wall has a real opening, opaque upper wall, and unchanged cat collision", () => {
  const root = new I.Group();
  const wall = new I.Mesh(new I.BoxGeometry(0.2, 3, 4), new I.MeshStandardMaterial());
  wall.name = "bedroom-west-wall"; wall.position.set(0, 1.5, 0); root.add(wall);
  const collider = { minX: -0.1, maxX: 0.1, minZ: -2, maxZ: 2, minY: 0, maxY: 3 };
  const engine = { world: { root, colliders: [collider] } };
  assert.equal(carveMouseOpening(engine, I, { id: "hole", x: -0.11, z: 0, axis: "x", style: "gnawed-wall" }), 1);
  root.updateWorldMatrix(true, true);
  const ray = new Raycaster(new I.Vector3(-1, 0.09, 0), new I.Vector3(1, 0, 0));
  assert.equal(ray.intersectObject(wall).length, 0);
  ray.set(new I.Vector3(-1, 0.5, 0), new I.Vector3(1, 0, 0));
  assert.ok(ray.intersectObject(wall).length > 0);
  assert.equal(engine.world.colliders[0], collider);
  assert.equal(I.lineClear(vector(-1), vector(1), 0.205, engine.world.colliders, "cat"), false);
});

test("structural shells escape hidden ancestors, including locked rooms and new additions", () => {
  const root = new I.Group(), hidden = new I.Group(), room = new I.Group();
  root.add(hidden); hidden.add(room); hidden.visible = room.visible = false;
  const wall = new I.Mesh(new I.BoxGeometry(2, 3, 0.2), new I.MeshStandardMaterial());
  wall.name = "partition-north"; room.position.x = 2; room.add(wall);
  const manager = { engine: { world: { root, __hearthmouseRoomGroups: new Map([["room", new Set([room])]]) } }, roomUnlocked: () => false };
  extractPersistentRoomStructures(manager); applyPersistentStructureVisibility(manager);
  assert.equal(wall.parent, root); assert.equal(wall.visible, true); assert.equal(wall.position.x, 2);
  const lateWall = wall.clone(); lateWall.userData = {}; room.add(lateWall);
  assert.equal(extractPersistentRoomStructures(manager), 1);
  assert.equal(lateWall.parent, root);
  wall.userData.__hearthmouseStructureRemoved = true;
  applyPersistentStructureVisibility(manager); assert.equal(wall.visible, false);
});

test("nest detail batches dozens of pieces into five static draws and food avoids furniture", () => {
  const root = new I.Group(); addNestHabitat({ world: { nestCenter: vector() } }, I, root);
  assert.equal(root.children.length, 5);
  assert.ok(root.children.every(m => !m.matrixAutoUpdate && !m.castShadow));
  assert.ok(root.children.reduce((sum, m) => sum + m.geometry.attributes.position.count, 0) > 1000);
  assert.equal(isFoodPositionClear(vector(), [{ minX: -1, maxX: 1, minZ: -1, maxZ: 1, minY: 0, maxY: 0.4 }]), false);
  assert.equal(isFoodPositionClear(vector(3, 3), []), true);
});

function huntingEngine() {
  class Engine {}
  const p = Engine.prototype;
  p.__catPressureHotfixInstalled = p.__livingHouseInstalled = p.__houseEvolutionInstalled = true;
  p.processCatVision = (cat, target) => { if (target) { cat.state = "chase"; cat.targetId = target.id; cat.pathTimer = 0; } };
  p.updateCatSearchMotion = () => 0; p.updateCatChase = () => ({ speed: 0, pounce: 0 });
  p.planCatPath = (cat, target) => { cat.path = [target.clone()]; cat.pathReachable = true; };
  p.updateCatPatrol = () => 0; p.updateCats = () => {};
  p.mouseTakeFood = p.playerTakeFood = () => {}; p.catHeadLook = () => 0;
  p.setCatState = (cat, state, timer) => { cat.state = state; cat.stateTimer = timer; };
  p.followCatPath = () => 0.5;
  installPredatorUpgrade({ ...I, Engine });
  const e = new Engine();
  e.time = 1; e.snapshot = { night: 1 }; e.world = { colliders: [], nestCenter: vector() }; e.foods = [];
  const cat = { id: "test", rig: makeCat(), state: "chase", targetId: "player", lastSeen: vector(), investigation: vector(), awareness: 0.5,
    path: [vector(1)], pathIndex: 0, pathTimer: 0.3, pouncePhase: "none" };
  e.cats = [cat]; return { e, cat };
}

test("repeated target observations retain chase replan budget; stalking times out", () => {
  const { e, cat } = huntingEngine();
  e.processCatVision(cat, { id: "player", visible: 1, distance: 3, position: vector(3), moving: 1 }, 0.1);
  assert.equal(cat.pathTimer, 0.3);
  cat.hunt.mode = "stalk"; cat.hunt.until = 8; cat.state = "suspicious";
  e.time = 4;
  e.processCatVision(cat, null, 0.1);
  assert.equal(cat.state, "search"); assert.equal(cat.targetId, null); assert.equal(cat.lastSeen.x, 3);
});

test("food watching has a finite deadline and a long revisit cooldown", () => {
  const { e, cat } = huntingEngine(); cat.state = "relaxed";
  cat.rig.root.position.set(4, 0, 0);
  e.foods = [{ mesh: { position: vector(4.5), visible: true }, exposure: "open" }];
  e.time = 50; e.updateCatPatrol(cat, 0.1);
  assert.ok(cat.hunt.foodUntil > e.time && cat.hunt.foodUntil <= e.time + 5);
  e.time += 6; e.updateCatPatrol(cat, 0.1);
  assert.equal(cat.hunt.mode, "patrol"); assert.ok(cat.hunt.nextGuard > e.time + 15);
});


test("actual expanded house spawns exposed and sheltered food across progression", () => {
  const R = bundledRuntime(), internals = R.I;
  const oldWindow = globalThis.window;
  globalThis.window = { HearthmouseInternals: internals, matchMedia: () => ({ matches: false }) };
  try {
    const e = Object.create(internals.Engine.prototype);
    Object.assign(e, { scene: new internals.Group(), snapshot: { night: 1, population: 4 }, cats: [], mice: [], foods: [],
      claimedPrizeIds: new Set(), startOfNightPopulation: 4, isTouchOnlyDevice: () => false });
    e.world = R.world(e.scene); e.playerPosition = e.world.mouseSpawn.clone(); e.playerView = new internals.Group();
    installExpansionSightGuard(e, internals); installEnginePatches(internals);
    e.chooseNightEvent(1); e.__expansion.campaignSeed = 1337;
    for (const night of [1, 4, 8, 11]) {
      e.snapshot.night = night; e.foods = []; e.world.setNight(night); e.chooseNightEvent(night); e.spawnFood();
      assert.ok(e.foods.length >= 12);
      const open = e.foods.filter(f => f.exposure === "open");
      assert.ok(open.length >= 4, `night ${night}: ${open.length} exposed food`);
      assert.ok(e.foods.some(f => f.exposure === "sheltered"));
      assert.ok(open.every(f => isFoodPositionClear(f.mesh.position, e.world.colliders, 0.055)));
      assert.ok(e.foods.every(f => f.mesh.matrixAutoUpdate === false));
    }
  } finally { if (oldWindow === undefined) delete globalThis.window; else globalThis.window = oldWindow; }
});


test("structural batching preserves invisible-source raycasts and rebuilds after a wall cut or removal", () => {
  const root = new I.Group(), room = new I.Group(); root.add(room);
  const material = new I.MeshStandardMaterial();
  const walls = [0, 3].map(x => {
    const mesh = new I.Mesh(new I.BoxGeometry(0.2, 3, 4), material);
    mesh.name = "room-wall"; mesh.position.set(x, 1.5, 0); room.add(mesh); return mesh;
  });
  const manager = { stats: {}, engine: { world: { root, __hearthmouseRoomGroups: new Map([["room", new Set([room])]]) } } };
  extractPersistentRoomStructures(manager);
  assert.equal(batchPersistentStructures(manager, I), 1);
  assert.equal(manager.stats.structuralDraws, 1);
  assert.ok(walls.every(wall => !wall.visible));
  root.updateWorldMatrix(true, true);
  const ray = new Raycaster(new I.Vector3(-1, 0.09, 0), new I.Vector3(1, 0, 0));
  assert.ok(ray.intersectObject(walls[0]).length > 0, "hidden source still blocks cat sight");
  assert.equal(carveMouseOpening(manager.engine, I, { id: "cut", x: -0.11, z: 0, axis: "x", style: "gnawed-wall" }), 1);
  const oldBatch = root.getObjectByName("persistent-structure-batch");
  batchPersistentStructures(manager, I);
  const batch = root.getObjectByName("persistent-structure-batch");
  assert.notEqual(batch, oldBatch);
  root.updateWorldMatrix(true, true); ray.far = 2;
  assert.equal(ray.intersectObject(batch).length, 0, "batch contains the cut opening");
  ray.set(new I.Vector3(-1, 0.5, 0), new I.Vector3(1, 0, 0));
  assert.ok(ray.intersectObject(batch).length > 0, "wall above the hole remains opaque");
  walls[0].userData.__hearthmouseStructureRemoved = true;
  assert.equal(batchPersistentStructures(manager, I), 0);
  assert.equal(walls[0].visible, false); assert.equal(walls[1].visible, true);
});
