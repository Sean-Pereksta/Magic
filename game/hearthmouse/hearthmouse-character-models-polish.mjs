import "./hearthmouse-character-models-base.mjs";
import { registerCharacterVisualStage } from "./hearthmouse-performance-manager.mjs";
export * from "./hearthmouse-character-models-base.mjs";

const CAT_TAIL_NODE_INDICES = Object.freeze([8, 7, 6]);
const CAT_FRONT_LEFT = Object.freeze([2, 1]);
const CAT_FRONT_RIGHT = Object.freeze([4, 3]);
const CAT_HIND_LEFT = Object.freeze([12, 11, 10]);
const CAT_HIND_RIGHT = Object.freeze([15, 14, 13]);
const CAT_HEAD_NODE_INDEX = 0;
const CAT_CHEST_NODE_INDEX = 5;
const CAT_SPINE_NODE_INDEX = 9;
const CAT_ROOT_NODE_INDEX = 16;
const CAT_POUNCE_FLIGHT_DURATION = 0.38;
const CAT_POUNCE_LANDING_DURATION = 0.18;
const GROUND_AUDIT_INTERVAL = 0.4;
const GROUND_POSITION_EPSILON = 0.001;
export const CAT_MODEL_RIG = Object.freeze({
  root: CAT_ROOT_NODE_INDEX,
  spine: CAT_SPINE_NODE_INDEX,
  chest: CAT_CHEST_NODE_INDEX,
  head: CAT_HEAD_NODE_INDEX,
  tail: CAT_TAIL_NODE_INDICES,
  frontLeft: CAT_FRONT_LEFT,
  frontRight: CAT_FRONT_RIGHT,
  hindLeft: CAT_HIND_LEFT,
  hindRight: CAT_HIND_RIGHT,
});
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
    groundY: Number.isFinite(controller.grounding?.baselineY)
      ? controller.grounding.baselineY
      : Number.isFinite(controller.wrapper?.position?.y) ? controller.wrapper.position.y : null,
    groundAuditClock: 0,
    visualFloorError: 0,
    groomClock: Math.random() * 7,
    groomCycle: 0,
    groomBlend: 0,
    wasGrooming: false,
    groomSide: hashActor(controller.actor) % 2 ? 1 : -1,
    lastPouncePhase: "none",
    landingClock: CAT_POUNCE_LANDING_DURATION,
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
  const official = controller.grounding?.baselineY;
  if (Number.isFinite(official)) state.groundY = official;
  return state.groundY;
}

function anchorWrapperToGround(controller) {
  const y = groundedWrapperY(controller);
  if (y === null) return;
  controller.wrapper.position.y = y;
}

function auditAndRepairGrounding(controller, delta, airborne = false) {
  const state = controller.__hearthmouseProceduralPolish;
  const wrapper = controller.wrapper;
  const root = controller.root;
  const officialY = groundedWrapperY(controller);
  if (!state || !wrapper?.position || officialY === null) return;

  if (!airborne && Math.abs(wrapper.position.y - officialY) > GROUND_POSITION_EPSILON) wrapper.position.y = officialY;
  state.groundAuditClock += delta;
  if (airborne || state.groundAuditClock < GROUND_AUDIT_INTERVAL) return;
  state.groundAuditClock = 0;

  const grounding = controller.grounding;
  const box = controller.groundingBox;
  if (!grounding || !box?.setFromObject || !root?.getWorldPosition || !root?.getWorldScale) return;
  root.updateWorldMatrix?.(true, true);
  wrapper.updateWorldMatrix?.(true, true);
  box.setFromObject(wrapper, true);
  if (box.isEmpty?.()) return;
  root.getWorldPosition(controller.groundingRootWorld);
  root.getWorldScale(controller.groundingRootScale);
  const expectedFloor = controller.groundingRootWorld.y + grounding.floorY * Math.abs(controller.groundingRootScale.y || 1);
  state.visualFloorError = box.min.y - expectedFloor;

  // The official offset is the source of truth. If any reset, teleport, model
  // reload, or stale pounce transform moved the visual, restore it immediately.
  if (Math.abs(wrapper.position.y - grounding.baselineY) > GROUND_POSITION_EPSILON) wrapper.position.y = grounding.baselineY;
  wrapper.userData.__hearthmouseVisualFloorError = state.visualFloorError;
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

export function catPouncePose(phase, progress) {
  const p = clamp01(progress);
  if (phase === "windup") {
    const crouch = smoothstep(p);
    const pawsReady = smoothstep(clamp01((p - 0.16) / 0.84));
    return {
      active: true,
      rootPitch: 0.12 * crouch,
      spinePitch: 0.34 * crouch,
      chestPitch: 0.48 * crouch,
      headPitch: 0.08 * crouch,
      frontUpper: -0.38 * pawsReady,
      frontLower: -0.58 * pawsReady,
      pawSpread: 0.1 * pawsReady,
      hindUpper: 0.78 * crouch,
      hindMiddle: 1.16 * crouch,
      hindLower: 0.7 * crouch,
      tailPitch: -0.08 * crouch,
      tailYaw: 0.12 * crouch,
      lift: 0,
    };
  }
  if (phase === "flight") {
    const launch = 1 - smoothstep(clamp01(p / 0.34));
    const reach = smoothstep(clamp01(p / 0.28)) * (1 - 0.42 * smoothstep(clamp01((p - 0.76) / 0.24)));
    const stretch = Math.sin(Math.PI * clamp01(p * 0.94));
    const landing = smoothstep(clamp01((p - 0.7) / 0.3));
    return {
      active: true,
      rootPitch: 0.1 * launch - 0.13 * stretch + 0.12 * landing,
      spinePitch: 0.28 * launch - 0.3 * stretch + 0.1 * landing,
      chestPitch: 0.36 * launch - 0.44 * stretch + 0.2 * landing,
      headPitch: 0.05 * launch - 0.2 * stretch + 0.12 * landing,
      frontUpper: -1.22 * reach - 0.42 * landing,
      frontLower: -1.42 * reach - 0.56 * landing,
      pawSpread: 0.12 * reach + 0.07 * landing,
      hindUpper: 0.72 * launch - 0.34 * stretch + 0.42 * landing,
      hindMiddle: 1.05 * launch - 0.48 * stretch + 0.68 * landing,
      hindLower: 0.66 * launch - 0.25 * stretch + 0.5 * landing,
      tailPitch: -0.18 * stretch + 0.08 * landing,
      tailYaw: Math.sin(p * Math.PI * 2) * 0.1,
      lift: Math.sin(Math.PI * p) * 0.12,
    };
  }
  if (phase === "landing") {
    const settle = 1 - smoothstep(p);
    return {
      active: settle > 0.001,
      rootPitch: 0.12 * settle,
      spinePitch: 0.1 * settle,
      chestPitch: 0.2 * settle,
      headPitch: 0.1 * settle,
      frontUpper: -0.42 * settle,
      frontLower: -0.56 * settle,
      pawSpread: 0.07 * settle,
      hindUpper: 0.42 * settle,
      hindMiddle: 0.68 * settle,
      hindLower: 0.5 * settle,
      tailPitch: 0.08 * settle,
      tailYaw: 0,
      lift: 0,
    };
  }
  return { active: false, lift: 0 };
}

function applyCatPounce(controller, delta) {
  const cat = controller.actor;
  const state = controller.__hearthmouseProceduralPolish;
  if (!cat || !state) return false;
  const rawPhase = cat.pouncePhase;
  if (state.lastPouncePhase === "flight" && rawPhase !== "flight") {
    state.landingClock = 0;
    state.lastPouncePhase = "landing";
  }
  if (rawPhase === "windup" || rawPhase === "flight") state.lastPouncePhase = rawPhase;

  let phase = rawPhase;
  let progress = 0;
  if (phase === "windup") {
    const duration = Math.max(0.001, cat.pounceWindupDuration ?? (cat.personality === "hunter" ? 0.48 : 0.34));
    progress = 1 - clamp01((cat.pounceTimer ?? 0) / duration);
  } else if (phase === "flight") {
    progress = clamp01(cat.pounceVisual ?? (1 - (cat.pounceTimer ?? 0) / CAT_POUNCE_FLIGHT_DURATION));
  } else if (state.landingClock < CAT_POUNCE_LANDING_DURATION) {
    phase = "landing";
    state.landingClock += delta;
    progress = clamp01(state.landingClock / CAT_POUNCE_LANDING_DURATION);
  } else {
    state.lastPouncePhase = "none";
    return false;
  }

  const pose = catPouncePose(phase, progress);
  if (!pose.active) return false;
  rotateLocal(controller, CAT_ROOT_NODE_INDEX, state.axisX, pose.rootPitch);
  rotateLocal(controller, CAT_SPINE_NODE_INDEX, state.axisX, pose.spinePitch);
  rotateLocal(controller, CAT_CHEST_NODE_INDEX, state.axisX, pose.chestPitch);
  rotateLocal(controller, CAT_HEAD_NODE_INDEX, state.axisX, pose.headPitch);

  for (const [limb, mirror] of [[CAT_FRONT_LEFT, -1], [CAT_FRONT_RIGHT, 1]]) {
    rotateLocal(controller, limb[0], state.axisX, pose.frontUpper);
    rotateLocal(controller, limb[1], state.axisX, pose.frontLower);
    rotateLocal(controller, limb[0], state.axisZ, mirror * pose.pawSpread);
    rotateLocal(controller, limb[1], state.axisZ, mirror * pose.pawSpread * 0.46);
  }
  for (const limb of [CAT_HIND_LEFT, CAT_HIND_RIGHT]) {
    rotateLocal(controller, limb[0], state.axisX, pose.hindUpper);
    rotateLocal(controller, limb[1], state.axisX, pose.hindMiddle);
    rotateLocal(controller, limb[2], state.axisX, pose.hindLower);
  }
  CAT_TAIL_NODE_INDICES.forEach((index, segment) => {
    rotateLocal(controller, index, state.axisX, pose.tailPitch * (1 + segment * 0.22));
    rotateLocal(controller, index, state.axisY, pose.tailYaw * (0.8 + segment * 0.28));
  });

  const groundedY = groundedWrapperY(controller);
  if (groundedY !== null) controller.wrapper.position.y = groundedY + pose.lift;
  return true;
}

function groomingSample(controller, delta) {
  const cat = controller.actor;
  const state = controller.__hearthmouseProceduralPolish;
  if (!cat || !state) return null;
  const explicit = cat.leisureMode === "grooming" || cat.leisureMode === "groom";
  const relaxedAndStill = (cat.state === "relaxed" || cat.state === "cooldown") && (cat.speed ?? 0) < 0.055 && cat.pouncePhase === "none";
  if (!explicit && !relaxedAndStill) {
    state.groomClock = Math.min(state.groomClock, 8.5);
    state.groomCycle = 0;
    state.wasGrooming = false;
    return null;
  }
  state.groomClock += delta;
  let active = explicit;
  if (!explicit) {
    const cycle = state.groomClock % 12.5;
    active = cycle >= 7.1;
  }
  if (!active) {
    state.groomCycle = 0;
    state.wasGrooming = false;
    return null;
  }

  if (!state.wasGrooming) {
    state.wasGrooming = true;
    state.groomCycle = 0;
    state.groomSide *= -1;
  } else {
    state.groomCycle += delta;
  }
  const duration = 5.4;
  const progress = (state.groomCycle % duration) / duration;
  const cycleNumber = Math.floor(state.groomCycle / duration);
  if (cycleNumber !== state.lastGroomCycleNumber) {
    if (Number.isFinite(state.lastGroomCycleNumber)) state.groomSide *= -1;
    state.lastGroomCycleNumber = cycleNumber;
  }
  const fadeIn = smoothstep(progress / 0.055);
  const fadeOut = smoothstep((1 - progress) / 0.065);
  return { progress, weight: Math.min(fadeIn, fadeOut), side: state.groomSide };
}

function stageEnvelope(progress, start, peakIn, peakOut, end) {
  if (progress <= start || progress >= end) return 0;
  if (progress < peakIn) return smoothstep((progress - start) / Math.max(0.001, peakIn - start));
  if (progress <= peakOut) return 1;
  return 1 - smoothstep((progress - peakOut) / Math.max(0.001, end - peakOut));
}

// Pure pose sampler for the exact 19-node cat.glb skeleton. Tests can sample
// contact, wiping, sitting, and recovery without needing a renderer.
export function catGroomPose(progress, side = -1) {
  const p = clamp01(progress);
  const mirror = side < 0 ? -1 : 1;
  const settle = stageEnvelope(p, 0, 0.08, 0.91, 1);
  const pawLick = stageEnvelope(p, 0.13, 0.18, 0.43, 0.49);
  const wipe = stageEnvelope(p, 0.43, 0.49, 0.68, 0.74);
  const flank = stageEnvelope(p, 0.69, 0.75, 0.86, 0.92);
  const lickPulse = pawLick * (0.5 + 0.5 * Math.sin(((p - 0.15) / 0.34) * Math.PI * 6));
  const wipePulse = wipe * Math.sin(smoothstep(clamp01((p - 0.45) / 0.27)) * Math.PI * 2.2);
  const raisedPaw = Math.max(pawLick, wipe * (0.86 + 0.12 * wipePulse));
  const plantedPaw = settle * (1 - 0.16 * flank);

  return {
    active: settle > 0.001,
    stage: pawLick > wipe && pawLick > flank ? "paw-lick" : wipe > flank ? "face-wipe" : flank > 0.01 ? "flank-lick" : "settle",
    rootPitch: 0.12 * settle,
    spinePitch: -0.17 * settle + 0.3 * flank,
    spineYaw: mirror * (0.08 * wipe + 0.43 * flank),
    chestPitch: -0.09 * settle + 0.2 * pawLick + 0.32 * flank,
    chestYaw: mirror * (0.12 * wipe + 0.34 * flank),
    headPitch: 0.22 * settle + 0.58 * pawLick + 0.22 * lickPulse + 0.36 * wipe + 0.62 * flank,
    headYaw: mirror * (0.32 * pawLick - 0.22 * wipePulse + 0.94 * flank),
    headRoll: mirror * (-0.1 * pawLick + 0.36 * wipe + 0.2 * flank),
    raisedFrontUpper: -0.96 * raisedPaw - 0.24 * wipePulse,
    raisedFrontLower: -0.72 * raisedPaw - 0.22 * lickPulse - 0.34 * wipePulse,
    raisedFrontRoll: mirror * (-0.3 * pawLick + 0.44 * wipe),
    plantedFrontUpper: -0.18 * plantedPaw,
    plantedFrontLower: -0.3 * plantedPaw,
    plantedFrontRoll: mirror * 0.06 * plantedPaw,
    hindUpper: 0.72 * settle,
    hindMiddle: 1.04 * settle,
    hindLower: 0.62 * settle,
    flankHindLift: 0.26 * flank,
    tailPitch: 0.16 * settle - 0.08 * flank,
    tailYaw: mirror * (0.34 * settle + 0.12 * Math.sin(p * Math.PI * 4)),
  };
}

function applyCatGrooming(controller, delta) {
  const state = controller.__hearthmouseProceduralPolish;
  if (!state) return false;
  const sample = groomingSample(controller, delta);
  if (!sample) return false;
  const { weight } = sample;
  const pose = catGroomPose(sample.progress, sample.side);
  const raised = sample.side < 0 ? CAT_FRONT_LEFT : CAT_FRONT_RIGHT;
  const planted = sample.side < 0 ? CAT_FRONT_RIGHT : CAT_FRONT_LEFT;
  const raisedMirror = sample.side < 0 ? -1 : 1;

  rotateLocal(controller, CAT_ROOT_NODE_INDEX, state.axisX, pose.rootPitch, weight);
  rotateLocal(controller, CAT_SPINE_NODE_INDEX, state.axisX, pose.spinePitch, weight);
  rotateLocal(controller, CAT_SPINE_NODE_INDEX, state.axisY, pose.spineYaw, weight);
  rotateLocal(controller, CAT_CHEST_NODE_INDEX, state.axisX, pose.chestPitch, weight);
  rotateLocal(controller, CAT_CHEST_NODE_INDEX, state.axisY, pose.chestYaw, weight);
  rotateLocal(controller, CAT_HEAD_NODE_INDEX, state.axisX, pose.headPitch, weight);
  rotateLocal(controller, CAT_HEAD_NODE_INDEX, state.axisY, pose.headYaw, weight);
  rotateLocal(controller, CAT_HEAD_NODE_INDEX, state.axisZ, pose.headRoll, weight);

  rotateLocal(controller, raised[0], state.axisX, pose.raisedFrontUpper, weight);
  rotateLocal(controller, raised[1], state.axisX, pose.raisedFrontLower, weight);
  rotateLocal(controller, raised[0], state.axisZ, pose.raisedFrontRoll, weight);
  rotateLocal(controller, planted[0], state.axisX, pose.plantedFrontUpper, weight);
  rotateLocal(controller, planted[1], state.axisX, pose.plantedFrontLower, weight);
  rotateLocal(controller, planted[0], state.axisZ, pose.plantedFrontRoll, weight);

  for (const limb of [CAT_HIND_LEFT, CAT_HIND_RIGHT]) {
    rotateLocal(controller, limb[0], state.axisX, pose.hindUpper, weight);
    rotateLocal(controller, limb[1], state.axisX, pose.hindMiddle, weight);
    rotateLocal(controller, limb[2], state.axisX, pose.hindLower, weight);
  }
  const flankHind = sample.side < 0 ? CAT_HIND_LEFT : CAT_HIND_RIGHT;
  rotateLocal(controller, flankHind[0], state.axisZ, raisedMirror * pose.flankHindLift, weight);
  CAT_TAIL_NODE_INDICES.forEach((index, segment) => {
    rotateLocal(controller, index, state.axisX, pose.tailPitch * (1 + segment * 0.18), weight);
    rotateLocal(controller, index, state.axisY, pose.tailYaw * (0.72 + segment * 0.3), weight);
  });
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
  const pouncing = applyCatPounce(controller, delta);
  auditAndRepairGrounding(controller, delta, cat?.pouncePhase === "flight");
  if (pouncing) {
    applyCatTail(controller, time, Math.max(1.2, cat?.speed ?? 0), 1.5);
    return;
  }
  const grooming = applyCatGrooming(controller, delta);
  if (!grooming && (cat?.speed ?? 0) > 0.04) applyCatTail(controller, time, cat.speed, cat.state === "chase" ? 1.75 : 1);
}

registerCharacterVisualStage("procedural-character-polish", (actor, context) => {
  const controller = actor?.rig?.root?.userData?.__hearthmouseGlbController;
  if (controller) polishController(controller, context.delta, context.time);
}, 20);
