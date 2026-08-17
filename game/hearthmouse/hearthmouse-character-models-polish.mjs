import "./hearthmouse-character-models-base.mjs";
export * from "./hearthmouse-character-models-base.mjs";

const FLOOR_OFFSETS = Object.freeze({ cat: 0.02, rat: -0.007 });
const CAT_TAIL_NODE_INDICES = Object.freeze([8, 7, 6]);
const CAT_FRONT_LEFT = Object.freeze([2, 1]);
const CAT_FRONT_RIGHT = Object.freeze([4, 3]);
const CAT_HIND_LEFT = Object.freeze([12, 11, 10]);
const CAT_HIND_RIGHT = Object.freeze([15, 14, 13]);
const CAT_HEAD_NODE_INDEX = 0;
const CAT_CHEST_NODE_INDEX = 5;
const CAT_SPINE_NODE_INDEX = 9;
const TWO_PI = Math.PI * 2;

let raf = 0;
let lastTime = 0;

const clamp01 = (value) => Math.max(0, Math.min(1, value));
const smoothstep = (value) => {
  const t = clamp01(value);
  return t * t * (3 - 2 * t);
};

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
    floorOffsetApplied: false,
    groomClock: Math.random() * 7,
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

function applyFloorOffset(controller) {
  const state = controller.__hearthmouseProceduralPolish;
  if (!state || state.floorOffsetApplied || !controller.wrapper?.position) return;
  const offset = FLOOR_OFFSETS[controller.kind] ?? 0;
  controller.wrapper.position.y += offset;
  state.floorOffsetApplied = true;
}

function applyCatTail(controller, time, speed, intensity = 1) {
  const state = controller.__hearthmouseProceduralPolish;
  if (!state) return;
  const cadence = Math.min(13, 3.8 + speed * 4.1);
  const travel = Math.min(1.35, 0.38 + speed * 0.27) * intensity;
  CAT_TAIL_NODE_INDICES.forEach((nodeIndex, segment) => {
    const phase = time * cadence - segment * 0.52;
    const taper = 0.72 + segment * 0.24;
    rotateLocal(controller, nodeIndex, state.axisY, Math.sin(phase) * 0.16 * taper * travel);
    rotateLocal(controller, nodeIndex, state.axisX, Math.sin(phase * 0.57 + segment) * 0.035 * travel);
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
  const tuck = phase === "windup" ? windup : Math.max(0, 1 - flight * 1.8);

  rotateLocal(controller, CAT_SPINE_NODE_INDEX, state.axisX, 0.22 * tuck - 0.13 * reach);
  rotateLocal(controller, CAT_CHEST_NODE_INDEX, state.axisX, 0.16 * tuck - 0.18 * reach);
  rotateLocal(controller, CAT_HEAD_NODE_INDEX, state.axisX, 0.12 * tuck - 0.26 * reach);

  for (const index of CAT_FRONT_LEFT) rotateLocal(controller, index, state.axisX, -0.82 * reach + 0.2 * tuck);
  for (const index of CAT_FRONT_RIGHT) rotateLocal(controller, index, state.axisX, -0.82 * reach + 0.2 * tuck);
  for (const index of CAT_HIND_LEFT) rotateLocal(controller, index, state.axisX, 0.48 * tuck - 0.12 * reach);
  for (const index of CAT_HIND_RIGHT) rotateLocal(controller, index, state.axisX, 0.48 * tuck - 0.12 * reach);

  if (controller.wrapper?.position) {
    const baseOffset = FLOOR_OFFSETS.cat;
    const lift = phase === "flight" ? Math.sin(clamp01(flight) * Math.PI) * 0.035 : 0;
    controller.wrapper.position.y = (controller.wrapper.__hearthmousePolishBaseY ?? (controller.wrapper.position.y - baseOffset)) + baseOffset + lift;
  }
  return true;
}

function groomingWeight(controller, delta, time) {
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
  const weight = groomingWeight(controller, delta, time);
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
  if (controller.wrapper && controller.wrapper.__hearthmousePolishBaseY === undefined) {
    controller.wrapper.__hearthmousePolishBaseY = controller.wrapper.position.y;
  }
  applyFloorOffset(controller);

  if (controller.kind !== "cat") return;
  const cat = controller.actor;
  const pouncing = applyCatPounce(controller);
  if (pouncing) {
    applyCatTail(controller, time, Math.max(1.2, cat?.speed ?? 0), 1.22);
    return;
  }
  const grooming = applyCatGrooming(controller, delta, time);
  if (!grooming && (cat?.speed ?? 0) > 0.04) applyCatTail(controller, time, cat.speed, cat.state === "chase" ? 1.25 : 1);
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
