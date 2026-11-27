import { mergeStaticMeshes } from "./hearthmouse-habitat.mjs";
import { registerCharacterVisualStage, registerFrameVisualStage } from "./hearthmouse-performance-manager.mjs";

const BITE_SECONDS = 1.45;
const bites = new WeakMap();
const playerShots = new WeakMap();
const installedPrototypes = new WeakSet();

function ease(age, start, end) {
  const progress = Math.max(0, Math.min(1, (age - start) / (end - start)));
  return progress * progress * (3 - 2 * progress);
}

export function sampleCatBite(age, pose) {
  const grab = ease(age, 0, 0.19);
  const snap = ease(age, 0.29, 0.4);
  const firstChew = ease(age, 0.47, 0.56) - ease(age, 0.59, 0.68);
  const secondChew = ease(age, 0.71, 0.81) - ease(age, 0.86, 0.95);
  const gulp = ease(age, 0.94, 1.07) - ease(age, 1.16, 1.39);
  const release = ease(age, 1.19, BITE_SECONDS);
  pose.gape = (0.36 + grab * 0.64) * (1 - snap) + firstChew * 0.55 + secondChew * 0.36 + gulp * 0.2;
  pose.dip = (grab * 0.36 - gulp * 0.54) * (1 - release);
  pose.roll = (firstChew - secondChew) * 0.11;
  pose.swallow = ease(age, 0.85, 1.07);
  return pose;
}

function shape(I, geometry, material, parent, name, location, size) {
  const mesh = new I.Mesh(geometry, material);
  mesh.name = name;
  mesh.position.set(...location);
  mesh.scale.set(...size);
  parent.add(mesh);
  return mesh;
}

function toothRow(I, Cone, enamel, parent, upper) {
  const group = new I.Group();
  parent.add(group);
  const teeth = [];
  for (const side of [-1, 1]) {
    for (const type of ["canine", "incisor", "rear"]) {
      const canine = type === "canine";
      const incisor = type === "incisor";
      const length = canine ? (upper ? 0.057 : 0.036) : 0.016;
      const width = canine ? 0.013 : 0.008;
      const tooth = new I.Mesh(new Cone(width, length, 7), enamel);
      const x = side * (incisor ? 0.018 : canine ? 0.049 : 0.062);
      const z = incisor ? -0.118 : canine ? -0.101 : -0.078;
      tooth.position.set(x, (upper ? -1 : 1) * (length * 0.32 + 0.004), z);
      tooth.rotation.x = upper ? Math.PI + 0.18 : -0.16;
      tooth.rotation.z = side * (upper ? -0.14 : 0.12);
      group.add(tooth);
      teeth.push(tooth);
    }
  }
  const row = new I.Mesh(mergeStaticMeshes(I, teeth, group), enamel);
  row.name = upper ? "cat-upper-fangs" : "cat-lower-fangs";
  for (const tooth of teeth) tooth.geometry.dispose();
  group.remove(...teeth);
  group.add(row);
  return group;
}

function makePreyVisual(I, sphere, parent) {
  const coat = new I.MeshStandardMaterial({ color: 0x82705f, roughness: 0.9 });
  const group = new I.Group();
  group.name = "cat-held-mouse";
  parent.add(group);
  const pieces = [
    [[0, 0, 0.012], [0.032, 0.025, 0.062]],
    [[0, 0.004, -0.034], [0.027, 0.023, 0.035]],
    [[-0.019, 0.026, -0.025], [0.013, 0.014, 0.009]],
    [[0.019, 0.026, -0.025], [0.013, 0.014, 0.009]],
    [[0.004, -0.011, 0.083], [0.009, 0.009, 0.033]],
    [[0.014, -0.014, 0.125], [0.006, 0.006, 0.025]],
  ].map(([position, scale]) => shape(I, sphere, coat, group, "", position, scale));
  pieces.at(-1).rotation.y = 0.24;
  const mouse = new I.Mesh(mergeStaticMeshes(I, pieces, group), coat);
  group.remove(...pieces);
  group.add(mouse);
  group.visible = false;
  return { group, coat };
}

function prepareMouth(cat, I) {
  const rig = cat.rig;
  const head = rig?.headPivot;
  const muzzle = head?.getObjectByName("cat-muzzle");
  const sphere = muzzle?.geometry;
  const Cone = rig?.ears?.[0]?.children[0]?.geometry?.constructor;
  if (!sphere || !Cone || !I?.Mesh) return null;

  // The original pivot also anchors cat vision. Pose only its visible parts.
  const poseHead = new I.Group();
  poseHead.add(...head.children.slice());
  head.add(poseHead);
  const root = new I.Group();
  root.name = "cat-articulated-mouth";
  poseHead.add(root);
  const jaw = new I.Group();
  jaw.name = "cat-jaw-hinge";
  jaw.position.set(0, -0.099, -0.102);
  root.add(jaw);
  const ivory = new I.MeshStandardMaterial({ color: 0xffedcc, roughness: 0.42, emissive: 0xd3a46b, emissiveIntensity: 0.15 });
  const cavityMaterial = new I.MeshStandardMaterial({ color: 0x24151b, roughness: 0.83 });
  const tongueMaterial = new I.MeshStandardMaterial({ color: 0x864749, roughness: 0.78 });
  const cavity = shape(I, sphere, cavityMaterial, root, "cat-mouth-interior", [0, -0.104, -0.174], [0.068, 0.021, 0.038]);
  shape(I, sphere, muzzle.material, jaw, "cat-lower-muzzle", [0, -0.012, -0.065], [0.071, 0.025, 0.057]);
  shape(I, sphere, tongueMaterial, jaw, "cat-tongue", [0, 0.009, -0.077], [0.045, 0.011, 0.045]);
  const upper = toothRow(I, Cone, ivory, root, true);
  upper.position.set(0, -0.088, -0.103);
  toothRow(I, Cone, ivory, jaw, false);
  const prey = makePreyVisual(I, sphere, root);
  root.visible = false;
  return { poseHead, root, muzzle, jaw, cavity, prey, muzzleY: muzzle.position.y, muzzleHeight: muzzle.scale.y, pose: {} };
}

export function updateCatBiteVisual(cat, context, I = globalThis.window?.HearthmouseInternals) {
  const rig = cat.rig;
  const bite = bites.get(cat);
  const age = bite ? context.time - bite.started : Infinity;
  const biting = age >= 0 && age < BITE_SECONDS;
  if (bite && age >= BITE_SECONDS) bites.delete(cat);
  const launchGape = cat.pouncePhase === "flight" ? 0.78
    : cat.pouncePhase === "windup" && cat.pounceTimer < 0.16 ? 0.29 : 0;
  if (!biting && !launchGape && !rig.__hearthmouseMouth) return;
  const mouth = rig.__hearthmouseMouth ??= prepareMouth(cat, I);
  if (!mouth) return;
  if (!biting && !launchGape && !mouth.root.visible && !mouth.poseHead.rotation.x && !mouth.poseHead.rotation.z) return;

  const pose = biting ? sampleCatBite(age, mouth.pose) : null;
  const openness = pose?.gape ?? launchGape;
  const holding = !!(biting && !bite.player && age >= 0.25 && age < 1.07);
  mouth.root.visible = openness > 0.02 || holding;
  mouth.jaw.rotation.x = -Math.max(openness, holding ? 0.13 : 0) * 0.92;
  mouth.cavity.scale.y = 0.017 + openness * 0.042;
  mouth.cavity.position.y = -0.108 - openness * 0.021;
  mouth.muzzle.position.y = mouth.muzzleY + openness * 0.024;
  mouth.muzzle.scale.y = mouth.muzzleHeight * (1 - openness * 0.21);
  mouth.prey.group.visible = holding;
  const dip = pose?.dip ?? 0;
  mouth.poseHead.rotation.set(-dip, 0, pose?.roll ?? 0);
  mouth.poseHead.position.y = -Math.max(0, dip) * 0.23;
  if (holding) {
    if (mouth.prey.coatHex !== bite.coat) {
      mouth.prey.coatHex = bite.coat;
      mouth.prey.coat.color.setHex(bite.coat);
    }
    mouth.prey.group.position.set(0, -0.126 - openness * 0.026, -0.221 + pose.swallow * 0.093);
    mouth.prey.group.rotation.set(age * 0.65, Math.PI * 0.5, Math.sin(age * 16) * (1 - pose.swallow) * 0.14);
    mouth.prey.group.scale.setScalar(0.94 - pose.swallow * 0.65);
  }
}

// Use a render-only shot during the existing death phase, with a clear line to
// the jaw. Player position, collision, catch outcome and death timing stay in the engine.
export function framePlayerBite(engine, I = globalThis.window?.HearthmouseInternals) {
  const shot = playerShots.get(engine);
  if (!shot) return;
  if (engine.snapshot.phase !== "dying") {
    playerShots.delete(engine);
    return;
  }
  const camera = engine.camera;
  const head = shot.cat.rig.__hearthmouseMouth?.poseHead ?? shot.cat.rig.headPivot;
  if (!camera || !head || !I?.Vector3) return;
  shot.focus ??= new I.Vector3();
  head.localToWorld(shot.focus.set(0, -0.125, -0.225));
  const age = engine.time - shot.started;
  const blend = ease(age, 0, 0.16);
  if (!shot.eye) {
    shot.eye = camera.position.clone();
    const catYaw = shot.cat.rig.root.rotation.y;
    const eye = camera.position.clone();
    eye.x = shot.focus.x - Math.sin(catYaw) * 0.49 + Math.cos(catYaw) * 0.11;
    eye.z = shot.focus.z - Math.cos(catYaw) * 0.49 - Math.sin(catYaw) * 0.11;
    // A furniture edge can leave the mouse nearly under the muzzle. Only
    // adjust the eye position if the short camera movement is unobstructed.
    if (I.lineClear?.(camera.position, eye, 0.025, engine.world.colliders, "mouse")) shot.eye.copy(eye);
  }
  camera.position.x += (shot.eye.x - camera.position.x) * blend;
  camera.position.z += (shot.eye.z - camera.position.z) * blend;
  const dx = shot.focus.x - camera.position.x;
  const dz = shot.focus.z - camera.position.z;
  const yaw = Math.atan2(-dx, -dz) - camera.rotation.y;
  const pitch = Math.atan2(shot.focus.y - camera.position.y, Math.hypot(dx, dz));
  camera.rotation.y += Math.atan2(Math.sin(yaw), Math.cos(yaw)) * blend;
  camera.rotation.x += (pitch - camera.rotation.x) * blend;
}

export function installCatBiteAnimations(I = globalThis.window?.HearthmouseInternals) {
  const proto = I?.Engine?.prototype;
  if (typeof proto?.mouseCaught !== "function" || typeof proto.playerCaught !== "function") return false;
  if (installedPrototypes.has(proto)) return true;
  installedPrototypes.add(proto);

  const mouseCaught = proto.mouseCaught;
  proto.mouseCaught = function animateCaughtMouse(mouse, cat) {
    const wasAlive = mouse?.member?.alive;
    const result = mouseCaught.call(this, mouse, cat);
    if (cat && wasAlive && !mouse.member.alive) {
      const coat = mouse.rig?.body?.material?.color?.getHex() ?? 0x82705f;
      bites.set(cat, { started: this.time, player: false, coat });
    }
    return result;
  };
  const playerCaught = proto.playerCaught;
  proto.playerCaught = function animateCaughtPlayer(cat) {
    const wasForaging = this.snapshot.phase === "foraging";
    const result = playerCaught.call(this, cat);
    if (cat && wasForaging && this.snapshot.phase === "dying") {
      bites.set(cat, { started: this.time, player: true });
      playerShots.set(this, { cat, started: this.time });
    }
    return result;
  };
  return true;
}

let installed = installCatBiteAnimations();
registerCharacterVisualStage("cat-articulated-bite", (actor, context) => {
  if (context.kind === "cat") updateCatBiteVisual(actor, context);
}, 30);
registerFrameVisualStage("player-bite-camera", (engine) => {
  if (!installed) installed = installCatBiteAnimations();
  framePlayerBite(engine);
});
