import assert from "node:assert/strict";
import test from "node:test";
import { bundledRuntime } from "./hearthmouse-runtime-fixture.mjs";
import {
  framePlayerBite,
  installCatBiteAnimations,
  sampleCatBite,
  updateCatBiteVisual,
} from "./hearthmouse-cat-bite.mjs";

const runtime = bundledRuntime();
const visualAt = (cat, age) => updateCatBiteVisual(cat, { time: age }, runtime.I);

function fixture() {
  class Engine {
    constructor() {
      this.time = 0;
      this.snapshot = { phase: "foraging" };
      this.world = { colliders: [] };
      this.playerPosition = new runtime.I.Vector3(0, 0, -0.25);
      this.camera = { position: new runtime.I.Vector3(0, 0.06, -0.25), rotation: { x: 0, y: 0, z: 0 } };
    }
    mouseCaught(mouse) {
      mouse.member.alive = false;
      mouse.rig.root.visible = false;
      return "ally caught";
    }
    playerCaught() {
      this.snapshot.phase = "dying";
      return "player caught";
    }
  }
  const I = { ...runtime.I, Engine };
  assert.ok(installCatBiteAnimations(I));
  const engine = new Engine();
  const cat = { id: "mabel", rig: runtime.cat(), pouncePhase: "none" };
  return { engine, cat, I };
}

test("cat bites have a held gape, a snap, two chews and a swallow", () => {
  assert.equal(sampleCatBite(0.23, {}).gape, 1);
  assert.equal(sampleCatBite(0.41, {}).gape, 0);
  assert.ok(sampleCatBite(0.57, {}).gape > 0.5);
  assert.equal(sampleCatBite(0.7, {}).gape, 0);
  assert.ok(sampleCatBite(0.83, {}).gape > 0.35);
  assert.ok(sampleCatBite(1.07, {}).dip < 0);
  assert.equal(sampleCatBite(1.07, {}).swallow, 1);
});

test("pounces expose both articulated tooth rows without moving the sight pivot", () => {
  const { cat } = fixture();
  const eyeBefore = cat.rig.eyeWorldPosition(new runtime.I.Vector3());
  cat.pouncePhase = "flight";
  visualAt(cat, 0.2);
  const mouth = cat.rig.__hearthmouseMouth;
  assert.equal(mouth.root.visible, true);
  assert.ok(mouth.jaw.rotation.x < -0.65);
  assert.ok(mouth.root.getObjectByName("cat-upper-fangs").geometry.attributes.position.count > 100);
  assert.ok(mouth.jaw.getObjectByName("cat-lower-fangs").geometry.attributes.position.count > 100);
  assert.ok(cat.rig.eyeWorldPosition(new runtime.I.Vector3()).equals(eyeBefore));
  cat.pouncePhase = "recover";
  visualAt(cat, 0.3);
  assert.equal(mouth.root.visible, false);
  assert.equal(mouth.jaw.rotation.x, -0);
  assert.equal(mouth.muzzle.scale.y, mouth.muzzleHeight);
});

test("a cat briefly holds a caught colony mouse while the actual actor remains hidden", () => {
  const { engine, cat } = fixture();
  const mouse = { member: { alive: true }, rig: runtime.mouse() };
  assert.equal(engine.mouseCaught(mouse, cat), "ally caught");
  const eyeBefore = cat.rig.eyeWorldPosition(new runtime.I.Vector3());
  visualAt(cat, 0.38);
  const mouth = cat.rig.__hearthmouseMouth;
  assert.ok(mouth.prey.group.visible);
  assert.ok(!mouse.member.alive && !mouse.rig.root.visible);
  assert.ok(cat.rig.eyeWorldPosition(new runtime.I.Vector3()).equals(eyeBefore));
  const size = mouth.prey.group.scale.x;
  visualAt(cat, 1.02);
  assert.ok(mouth.prey.group.scale.x < size);
  visualAt(cat, 1.13);
  assert.equal(mouth.prey.group.visible, false);
  visualAt(cat, 1.5);
  assert.equal(mouth.root.visible, false);
  assert.equal(mouth.poseHead.rotation.x, -0);
});

test("the caught player's camera frames the bite without moving the player", () => {
  const { engine, cat, I } = fixture();
  const original = engine.playerPosition.clone();
  assert.equal(engine.playerCaught(cat), "player caught");
  engine.time = 0.23;
  visualAt(cat, engine.time);
  framePlayerBite(engine, I);
  assert.equal(engine.snapshot.phase, "dying");
  assert.ok(engine.camera.position.z < -0.6);
  assert.ok(engine.camera.rotation.x > 0 && Math.abs(engine.camera.rotation.y) > 2);
  assert.ok(engine.playerPosition.equals(original));
  const meshes = [];
  cat.rig.root.updateWorldMatrix(true, true);
  cat.rig.root.traverseVisible((node) => { if (node.isMesh) meshes.push(node); });
  const ray = new runtime.Raycaster();
  const direction = new I.Vector3();
  const corner = new I.Vector3();
  for (const name of ["cat-upper-fangs", "cat-lower-fangs"]) {
    const row = cat.rig.root.getObjectByName(name);
    const vertices = row.geometry.attributes.position;
    let exposed = false;
    for (let index = 0; index < vertices.count && !exposed; index += 3) {
      direction.set(0, 0, 0);
      for (let vertex = 0; vertex < 3; vertex++) direction.add(corner.fromBufferAttribute(vertices, index + vertex));
      row.localToWorld(direction.multiplyScalar(1 / 3));
      ray.set(engine.camera.position, direction.sub(engine.camera.position).normalize());
      exposed = ray.intersectObjects(meshes, false)[0]?.object === row;
    }
    assert.ok(exposed, name);
  }
  engine.snapshot.phase = "foraging";
  engine.camera.rotation.y = 0;
  framePlayerBite(engine, I);
  assert.equal(engine.camera.rotation.y, 0);
});
