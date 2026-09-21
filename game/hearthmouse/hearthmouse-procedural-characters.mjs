import { registerCharacterVisualStage, registerFrameVisualStage } from "./hearthmouse-performance-manager.mjs";

const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
const angle = (v) => Math.atan2(Math.sin(v), Math.cos(v));

// Share geometry within each disposable rig, never across independently disposed actors.
export function prepareProceduralRig(actor) {
  const rig = actor?.rig;
  if (!rig?.root?.getObjectByName || !rig.body?.scale || !rig.headPivot || rig.__livingProcedural) return rig?.__livingProcedural;
  if (actor.id === "pepper") {
    rig.body.material.color.setHex(0xd7d2c8);
    const stripe = rig.root.getObjectByName("cat-back-stripe");
    stripe?.material?.color?.setHex(0x716f70);
  }
  const geometries = new Map();
  const retired = new Set();
  let trianglesBefore = 0, trianglesAfter = 0;
  rig.root.traverse((node) => {
    if (!node.isMesh) return;
    const geometry = node.geometry;
    if (!geometry) return;
    trianglesBefore += (geometry.index?.count ?? geometry.attributes?.position?.count ?? 0) / 3;
    if (geometry.type === "SphereGeometry" && geometry.parameters?.radius === 1) {
      // Smooth silhouettes at mouse scale, with one unit sphere for each detail tier.
      const segments = /body|head|chest|rump/.test(node.name) ? 18 : 12;
      if (!geometries.has(segments)) geometries.set(segments, new geometry.constructor(1, segments, Math.round(segments * 0.7)));
      node.geometry = geometries.get(segments);
      retired.add(geometry);
    }
    trianglesAfter += (node.geometry.index?.count ?? node.geometry.attributes?.position?.count ?? 0) / 3;
    node.frustumCulled = true;
    if (/whisker|glint|pupil|stripe|inner-ear/.test(node.name)) node.castShadow = false;
  });
  for (const geometry of retired) geometry.dispose();
  const knees = [];
  for (const leg of rig.legs ?? []) {
    const knee = new rig.root.constructor();
    knee.name = "cat-lower-leg-joint";
    knee.position.y = -0.1;
    for (const part of [...leg.children].slice(1)) {
      part.position.y += 0.1;
      knee.add(part);
    }
    leg.add(knee); knees.push(knee);
  }
  const state = { knees,
    x: rig.root.position.x, z: rig.root.position.z, yaw: rig.root.rotation.y, speed: 0, stride: 0, turn: 0,
    baseScaleY: rig.body?.scale.y ?? 1,
    rump: rig.root.getObjectByName("cat-rump"),
    bib: rig.root.getObjectByName("cat-bib"),
    blink: 0, trianglesBefore, trianglesAfter,
  };
  rig.root.userData.characterSource = "procedural";
  rig.__livingProcedural = state;
  return state;
}

export function updateProceduralActor(actor, context) {
  const rig = actor.rig;
  const s = prepareProceduralRig(actor);
  if (!s) return;
  const dt = Math.min(0.2, Math.max(0.001, context.delta));
  const time = context.engine.time;
  // Measure travel across scheduled samples; actor.speed is a maximum for mice.
  const speed = clamp(Math.hypot(rig.root.position.x - s.x, rig.root.position.z - s.z) / dt, 0, 5.5);
  s.x = rig.root.position.x; s.z = rig.root.position.z;
  const turn = clamp(angle(rig.root.rotation.y - s.yaw) / dt, -5, 5);
  const acceleration = clamp((speed - s.speed) / dt, -6, 6);
  s.turn += (turn - s.turn) * (1 - Math.exp(-9 * dt));
  s.yaw = rig.root.rotation.y;
  s.speed = speed;
  s.stride += dt * speed * (context.kind === "cat" ? 7.5 : 32);
  const moving = clamp(speed / (context.kind === "cat" ? 0.9 : 0.65), 0, 1);
  const breath = Math.sin(time * 2.8) * 0.007;
  rig.body.scale.y = s.baseScaleY * (1 + breath);
  rig.body.rotation.z = -s.turn * (context.kind === "cat" ? 0.026 : 0.035) * moving;
  rig.body.rotation.x = acceleration * 0.008;

  if (context.kind === "cat") {
    const mode = actor.hunt?.mode;
    const crouch = mode === "stalk" || mode === "ambush" || actor.leisureMode === "tunnel-stalk";
    const windup = actor.pouncePhase === "windup";
    const lower = windup ? 0.075 : crouch ? 0.065 : actor.state === "chase" ? 0.03 : 0;
    const bob = Math.abs(Math.sin(s.stride)) * 0.01 * moving;
    rig.body.position.y = 0.28 - lower + bob;
    rig.chest.position.y = 0.29 - lower + bob;
    if (s.rump) s.rump.position.y = 0.29 - lower * 0.65 + bob;
    if (s.bib) s.bib.position.y = 0.27 - lower + bob;
    rig.headPivot.parent.position.y = 0.39 - lower * 0.72;
    // The old rig update assigned y = crouch * .35, erasing the .25 hip height.
    for (let i = 0; i < rig.legs.length; i++) {
      const leg = rig.legs[i];
      const phase = s.stride + (i === 0 || i === 3 ? 0 : Math.PI);
      leg.position.y = 0.25 - lower * 0.36;
      leg.rotation.x = Math.sin(phase) * (speed > 2 ? 0.72 : 0.35) * moving;
      if (windup) leg.rotation.x = i < 2 ? -0.22 : 0.62;
      if (actor.pouncePhase === "flight") leg.rotation.x = i < 2 ? -0.9 : 0.62;
      s.knees[i].rotation.x = windup ? -0.4 : crouch ? -0.25 : Math.max(0, Math.cos(phase)) * -0.45 * moving;
      if (actor.state === "chase" && actor.hunt && actor.pouncePhase === "none" && i === 0 &&
        Math.hypot(actor.hunt.seen.x - rig.root.position.x, actor.hunt.seen.z - rig.root.position.z) < 0.6) {
        leg.rotation.x -= Math.max(0, Math.sin(time * 10)) * 0.8;
      }
      if (actor.leisureMode === "grooming" && i === 0) leg.rotation.x = -0.9 + Math.sin(time * 5) * 0.2;
    }
    rig.headPivot.rotation.x += crouch ? 0.16 : Math.sin(time * 1.2) * 0.018;
    rig.ears.forEach((ear, i) => {
      ear.rotation.z = (i ? 1 : -1) * (crouch || windup ? 0.22 : 0.04);
      ear.rotation.y += Math.sin(time * 1.8 + i * 2) * 0.09;
    });
    const blink = Math.sin(time * 0.72 + (actor.id?.length ?? 0)) > 0.997;
    rig.eyes.forEach((eye) => { eye.scale.y = 0.047 * (blink ? 0.17 : 1); });
    rig.tail.forEach((part, i) => { part.rotation.y += -s.turn * 0.014 + (crouch ? Math.sin(time * 4 - i * 0.5) * 0.025 : 0); });
  } else {
    const carrying = !!actor.carriedFood;
    rig.headPivot.rotation.x = carrying ? -0.13 : Math.sin(time * 4.1) * 0.03;
    rig.headPivot.rotation.y *= actor.task === "escaping" ? 1.4 : 0.6;
    for (let i = 0; i < (rig.paws?.length ?? 0); i++) {
      const phase = s.stride + (i === 0 || i === 3 ? 0 : Math.PI);
      rig.paws[i].rotation.x = Math.sin(phase) * 0.5 * moving;
      rig.paws[i].position.y = 0.027 + Math.max(0, Math.cos(phase)) * 0.006 * moving;
    }
    const squeeze = actor.__tunnelSqueeze ?? 0;
    rig.body.scale.y *= 1 - squeeze * 0.28;
    rig.headPivot.position.y = 0.067 - squeeze * 0.015;
    if (carrying) poseCarriedFood(actor.carriedFood, time, speed, false);
  }
  context.recordAnimationSample?.();
}

export function poseCarriedFood(food, time, speed, firstPerson) {
  const mesh = food?.mesh;
  if (!mesh) return;
  const age = Math.max(0, time - (food.__pickupTime ?? time));
  const pickup = 1 - Math.exp(-age * 14);
  const weight = clamp((food.value ?? 1) / 5, 0.2, 1);
  const bob = Math.sin(time * (firstPerson ? 12 : 19)) * Math.min(1, speed) * 0.004;
  mesh.matrixAutoUpdate = true;
  mesh.position.set(0, (firstPerson ? -0.12 - weight * 0.025 : -0.023 - weight * 0.009) - (1 - pickup) * 0.025 + bob,
    firstPerson ? -0.27 - weight * 0.065 : -0.103 - weight * 0.024);
  mesh.rotation.set(0.14 + weight * 0.08 + bob * 4, Math.sin(time * 5) * 0.018, bob * 5);
  mesh.scale.setScalar(firstPerson ? 0.78 : 0.62);
}

registerCharacterVisualStage("living-procedural-characters", updateProceduralActor, 15);
registerFrameVisualStage("living-player-carry", (engine) => {
  const view = engine.playerView;
  if (view && !view.userData.carryPaws) view.userData.carryPaws = view.children.filter(p => p.name === "player-paw")
    .map(paw => ({ paw, rest: paw.position.clone(), roll: paw.rotation.z }));
  if (engine.carriedFood) {
    poseCarriedFood(engine.carriedFood, engine.time, engine.playerVelocity?.length?.() ?? 0, true);
    const food = engine.carriedFood.mesh;
    for (const { paw, rest } of view?.userData.carryPaws ?? []) {
      paw.position.set(Math.sign(rest.x) * 0.04, food.position.y - view.position.y - 0.008, food.position.z + 0.035);
      paw.rotation.x = -0.28;
      paw.rotation.z = -Math.sign(rest.x) * 0.45;
    }
  } else {
    for (const { paw, rest, roll } of view?.userData.carryPaws ?? []) {
      paw.position.copy(rest); paw.rotation.z = roll;
    }
  }
  const dropped = engine.__settlingFood;
  if (!dropped?.length) return;
  for (let i = dropped.length - 1; i >= 0; i--) {
    const entry = dropped[i], food = entry.food, progress = clamp((engine.time - entry.startedAt) / 0.24, 0, 1);
    if (!food.carriedBy && !food.deposited) {
      food.mesh.position.y = 0.018 + Math.sin((1 - progress) * Math.PI * 0.5) * 0.026;
      food.mesh.rotation.z = entry.roll * (1 - progress);
    }
    if (progress >= 1 || food.carriedBy || food.deposited) {
      if (!food.carriedBy) { food.mesh.updateMatrix(); food.mesh.matrixAutoUpdate = false; }
      dropped.splice(i, 1);
    }
  }
}, 1 / 45);

export function hearthmouseModelStatus() {
  return { ready: true, source: "procedural", importedModels: false };
}
