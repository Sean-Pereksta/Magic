import "./hearthmouse-character-models-base.mjs";
export * from "./hearthmouse-character-models-base.mjs";

const FLOOR_OFFSETS = Object.freeze({ cat: 0.02, rat: -0.012 });
const CAT_TAIL_NODE_INDICES = Object.freeze([8, 7, 6]);
const CAT_FRONT_LEFT = Object.freeze([2, 1]);
const CAT_FRONT_RIGHT = Object.freeze([4, 3]);
const CAT_HIND_LEFT = Object.freeze([12, 11, 10]);
const CAT_HIND_RIGHT = Object.freeze([15, 14, 13]);
const CAT_HEAD_NODE_INDEX = 0;
const CAT_CHEST_NODE_INDEX = 5;
const CAT_SPINE_NODE_INDEX = 9;
const MOUSE_COAT_COLORS = Object.freeze([
  0x76584a,
  0x9b7358,
  0x5c5f63,
  0xb89b78,
  0x4c4541,
  0x9a8878,
  0x747a80,
  0xc3aa8c,
  0x855f52,
  0xa9a197,
]);
const CAT_COAT_COLORS = Object.freeze({
  biscuit: 0xc7935f,
  pepper: 0xf0ede5,
});
const MOUSE_FEATURE_MATERIAL_PATTERN = /eye|iris|pupil|nose|snout|mouth|teeth|tooth|whisker|claw|nail|ear\s*inner/i;
const CAT_FEATURE_MATERIAL_PATTERN = /eye|iris|pupil|nose|mouth|teeth|tooth|whisker|claw|nail|ear\s*inner/i;

let raf = 0;
let lastTime = 0;

const clamp01 = (value) => Math.max(0, Math.min(1, value));
const smoothstep = (value) => {
  const t = clamp01(value);
  return t * t * (3 - 2 * t);
};

function hashActor(actor) {
  const text = String(actor?.id ?? actor?.name ?? actor?.mouseId ?? "mouse");
  let hash = 2166136261;
  for (let index = 0; index < text.length; index++) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function ensurePolishState(controller) {
  if (controller.__hearthmouseProceduralPolish) return controller.__hearthmouseProceduralPolish;
  const sampleNode = controller.nodes?.find(Boolean);
  const samplePosition = controller.root?.position;
  if (!sampleNode?.quaternion?.clone || !samplePosition?.clone) return null;
  const state = {
    q: sampleNode.quaternion.clone(),
    axisX: samplePosition.clone().set(1, 0, 0),
    axisY: samplePosition.clone().set(0, 1, 0),
    axisZ: samplePosition.clone().set(0, 0, 1),
    groundY: Number.isFinite(controller.wrapper?.position?.y) ? controller.wrapper.position.y : null,
    groomClock: Math.random() * 7,
    mouseTintApplied: false,
    catTintApplied: false,
  };
  controller.__hearthmouseProceduralPolish = state;
  return state;
}

function rotateLocal(controller, index, axis, radians, weight = 1) {
  const node = controller.nodes?.[index];
  const state = controller.__hearthmouseProceduralPolish;
  if (!node || !state || Math.abs(radians * weight) < 1e-5) return;
  state.q.setFromAxisAngle(axis, radians * weight);
  node.quaternion.multiply(state.q).normalize();
}

function groundedWrapperY(controller) {
  const state = controller.__hearthmouseProceduralPolish;
  if (!state || !controller.wrapper?.position) return null;
  if (!Number.isFinite(state.groundY)) state.groundY = controller.wrapper.position.y;
  return state.groundY + (FLOOR_OFFSETS[controller.kind] ?? 0);
}

function anchorWrapperToGround(controller) {
  const y = groundedWrapperY(controller);
  if (y === null) return;
  controller.wrapper.position.y = y;
}

function tintCoat(controller, tint, featurePattern, solidBase = false) {
  controller.wrapper?.traverse?.((object) => {
    if (!object?.isMesh || !object.material) return;
    const originalMaterials = Array.isArray(object.material) ? object.material : [object.material];
    const tintedMaterials = originalMaterials.map((material) => {
      if (!material?.clone || !material.color) return material;
      const materialName = String(material.name ?? object.name ?? "");
      if (featurePattern.test(materialName)) return material;
      const copy = material.clone();
      copy.color?.setHex?.(tint);
      // Alternate cat coats need to remain visibly distinct even if cat.glb ships with a dark base-color texture.
      // Removing the diffuse map is intentionally limited to recolored variants; Mabel keeps the original GLB material intact.
      if (solidBase && copy.map) copy.map = null;
      copy.needsUpdate = true;
      return copy;
    });
    object.material = Array.isArray(object.material) ? tintedMaterials : tintedMaterials[0];
  });
}

function applyMouseCoatTint(controller) {
  const state = controller.__hearthmouseProceduralPolish;
  if (!state || state.mouseTintApplied || controller.kind !== "rat") return;
  state.mouseTintApplied = true;

  const tint = MOUSE_COAT_COLORS[hashActor(controller.actor) % MOUSE_COAT_COLORS.length];
  tintCoat(controller, tint, MOUSE_FEATURE_MATERIAL_PATTERN);
  controller.wrapper.userData.__hearthmouseMouseCoatTint = tint;
}

function applyCatCoatTint(controller) {
  const state = controller.__hearthmouseProceduralPolish;
  if (!state || state.catTintApplied || controller.kind !== "cat") return;
  state.catTintApplied = true;

  const catId = String(controller.actor?.id ?? "mabel").toLowerCase();
  const tint = CAT_COAT_COLORS[catId];

  // Mabel (and any unknown/future cat without an explicit variant) must use cat.glb exactly as authored.
  // This preserves its embedded diffuse texture/material instead of replacing the skin with a flat tint.
  if (!Number.isFinite(tint)) {
    controller.wrapper.userData.__hearthmouseCatUsesOriginalSkin = true;
    return;
  }

  tintCoat(controller, tint, CAT_FEATURE_MATERIAL_PATTERN, true);
  controller.wrapper.userData.__hearthmouseCatCoatTint = tint;
}

function applyCatTail(controller, time, speed, intensity = 1) {
  const state = controller.__hearthmouseProceduralPolish;
  if (!state) return;
  const cadence = Math.min(14.5, 4.2 + speed * 4.5);
  const travel = Math.min(1.5, 0.42 + speed * 0.31) * intensity;
  CAT_TAIL_NODE_INDICES.forEach((nodeIndex, segment) => {
    const phase = time * cadence - segment * 0.52;
    const taper = 0.72 + segment * 0.24;
    const whip = Math.sin(phase * 1.72 + segment * 0.37) * 0.028 * travel;
    rotateLocal(controller, nodeIndex, state.axisY, Math.sin(phase) * 0.2 * taper * travel + whip);
    rotateLocal(controller, nodeIndex, state.axisX, Math.sin(phase * 0.57 + segment) * 0.05 * travel);
  });
}

function applyCatPounce(controller) {
  const cat = controller.actor;
  const state = controller.__hearthmouseProceduralPolish;
  if (!cat || !state) return false;
  const phase = cat.pouncePhase;
  if (phase !== "windup" && phase !== "flight") return false;

  const windupDuration = cat.personality === "hunter" ? 0.48 : 0.34;
  const windup = phase === "windup" ? smoothstep(1 - clamp01((cat.pounceTimer ?? 0) / windupDuration)) : 1;
  const flight = phase === "flight" ? smoothstep(clamp01(cat.pounceVisual ?? (1 - (cat.pounceTimer ?? 0) / 0.38))) : 0;
  const reach = phase === "flight" ? Math.sin(clamp01(flight) * Math.PI) : 0;
  const tuck = phase === "windup" ? windup : Math.max(0, 1 - flight * 2.4);
  const pawReady = phase === "windup"
    ? smoothstep(clamp01((windup - 0.18) / 0.82))
    : Math.max(0, 1 - flight * 1.7);
  const impactBrace = phase === "flight" ? smoothstep(clamp01((flight - 0.68) / 0.32)) : 0;

  // Make the attack read clearly even with cat.glb's simple leg rig: a deep stalking crouch,
  // both forepaws visibly lifting off the floor, then a long two-paw reach through the leap.
  // The wrapper remains anchored to the model's fitted floor height so the stronger crouch cannot bury the cat.
  rotateLocal(controller, CAT_SPINE_NODE_INDEX, state.axisX, 0.3 * tuck - 0.2 * reach + 0.04 * impactBrace);
  rotateLocal(controller, CAT_CHEST_NODE_INDEX, state.axisX, 0.42 * tuck - 0.34 * reach + 0.08 * impactBrace);
  rotateLocal(controller, CAT_HEAD_NODE_INDEX, state.axisX, 0.1 * tuck - 0.14 * reach + 0.04 * impactBrace);

  const frontUpper = -0.72 * pawReady - 1.08 * reach - 0.35 * impactBrace;
  const frontLower = -0.88 * pawReady - 1.28 * reach - 0.48 * impactBrace;
  const pawSpread = 0.1 * pawReady + 0.14 * reach + 0.05 * impactBrace;

  rotateLocal(controller, CAT_FRONT_LEFT[0], state.axisX, frontUpper);
  rotateLocal(controller, CAT_FRONT_RIGHT[0], state.axisX, frontUpper);
  rotateLocal(controller, CAT_FRONT_LEFT[1], state.axisX, frontLower);
  rotateLocal(controller, CAT_FRONT_RIGHT[1], state.axisX, frontLower);
  rotateLocal(controller, CAT_FRONT_LEFT[0], state.axisZ, -pawSpread);
  rotateLocal(controller, CAT_FRONT_RIGHT[0], state.axisZ, pawSpread);
  rotateLocal(controller, CAT_FRONT_LEFT[1], state.axisZ, -pawSpread * 0.45);
  rotateLocal(controller, CAT_FRONT_RIGHT[1], state.axisZ, pawSpread * 0.45);

  CAT_HIND_LEFT.forEach((index, segment) => {
    const bend = segment === 1 ? 1.08 : (segment === 0 ? 0.72 : 0.84);
    rotateLocal(controller, index, state.axisX, bend * tuck - 0.22 * reach);
  });
  CAT_HIND_RIGHT.forEach((index, segment) => {
    const bend = segment === 1 ? 1.08 : (segment === 0 ? 0.72 : 0.84);
    rotateLocal(controller, index, state.axisX, bend * tuck - 0.22 * reach);
  });

  const groundedY = groundedWrapperY(controller);
  if (groundedY !== null) {
    const lift = phase === "flight" ? Math.sin(clamp01(flight) * Math.PI) * 0.055 : 0;
    controller.wrapper.position.y = groundedY + lift;
  }
  return true;
}

function groomingWeight(controller, delta) {
  const cat = controller.actor;
  const state = controller.__hearthmouseProceduralPolish;
  if (!cat || !state) return 0;
  const explicit = cat.leisureMode === "grooming" || cat.leisureMode === "groom";
  const relaxedAndStill = (cat.state === "relaxed" || cat.state === "cooldown") && (cat.speed ?? 0) < 0.055 && cat.pouncePhase === "none";
  if (!explicit && !relaxedAndStill) {
    state.groomClock = Math.min(state.groomClock, 8.5);
    return 0;
  }
  state.groomClock += delta;
  if (!explicit) {
    const cycle = state.groomClock % 12.5;
    if (cycle < 8.7) return 0;
    const local = (cycle - 8.7) / 3.8;
    return smoothstep(Math.min(local / 0.18, (1 - local) / 0.2, 1));
  }
  return 1;
}

function applyCatGrooming(controller, delta, time) {
  const state = controller.__hearthmouseProceduralPolish;
  if (!state) return false;
  const weight = groomingWeight(controller, delta);
  if (weight <= 0) return false;

  const lickPulse = 0.5 + 0.5 * Math.sin(time * 7.4);
  const pawLift = 0.8 + lickPulse * 0.12;
  rotateLocal(controller, CAT_CHEST_NODE_INDEX, state.axisX, 0.2, weight);
  rotateLocal(controller, CAT_HEAD_NODE_INDEX, state.axisX, 0.62 + lickPulse * 0.13, weight);
  rotateLocal(controller, CAT_HEAD_NODE_INDEX, state.axisZ, -0.15, weight);

  rotateLocal(controller, CAT_FRONT_LEFT[0], state.axisX, -1.02 * pawLift, weight);
  rotateLocal(controller, CAT_FRONT_LEFT[0], state.axisZ, -0.34, weight);
  rotateLocal(controller, CAT_FRONT_LEFT[1], state.axisX, -0.72 - lickPulse * 0.16, weight);
  rotateLocal(controller, CAT_FRONT_LEFT[1], state.axisZ, 0.16, weight);

  applyCatTail(controller, time * 0.72, 0.14, 0.38 * weight);
  return true;
}

function polishController(controller, delta, time) {
  const state = ensurePolishState(controller);
  if (!state) return;
  // The initial fit from hearthmouse-character-models-base is the source of truth for ground height.
  // Reapply it every frame so newly spawned/reset actors cannot inherit a transient pounce or stale offset.
  anchorWrapperToGround(controller);

  if (controller.kind === "rat") {
    applyMouseCoatTint(controller);
    return;
  }
  if (controller.kind !== "cat") return;
  applyCatCoatTint(controller);
  const cat = controller.actor;
  const pouncing = applyCatPounce(controller);
  if (pouncing) {
    applyCatTail(controller, time, Math.max(1.2, cat?.speed ?? 0), 1.5);
    return;
  }
  const grooming = applyCatGrooming(controller, delta, time);
  if (!grooming && (cat?.speed ?? 0) > 0.04) applyCatTail(controller, time, cat.speed, cat.state === "chase" ? 1.75 : 1);
}

function frame(timestamp) {
  const delta = lastTime ? Math.min(0.05, Math.max(0, (timestamp - lastTime) / 1000)) : 1 / 60;
  lastTime = timestamp;
  const engine = window.hearthmouseEngine;
  if (engine && !engine.disposed) {
    for (const mouse of engine.mice ?? []) {
      const controller = mouse?.rig?.root?.userData?.__hearthmouseGlbController;
      if (controller) polishController(controller, delta, timestamp / 1000);
    }
    for (const cat of engine.cats ?? []) {
      const controller = cat?.rig?.root?.userData?.__hearthmouseGlbController;
      if (controller) polishController(controller, delta, timestamp / 1000);
    }
  }
  raf = window.requestAnimationFrame(frame);
}

if (typeof window !== "undefined" && typeof window.requestAnimationFrame === "function") {
  raf = window.requestAnimationFrame(frame);
  window.addEventListener("beforeunload", () => {
    if (raf) window.cancelAnimationFrame(raf);
    raf = 0;
    lastTime = 0;
  }, { once: true });
}
