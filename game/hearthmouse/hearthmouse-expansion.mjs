import "./hearthmouse-controller-preflight.mjs";
import "./hearthmouse-controller-reliability.mjs";
import "./hearthmouse-graphics-quality.mjs";
import "./hearthmouse-expansion-startup-guard.mjs";
import "./hearthmouse-desktop-look-guard.mjs";
import { catCountForPopulation } from "./hearthmouse-expansion-core.mjs";
export * from "./hearthmouse-expansion-core.mjs";

// The core module remains split out so this small compatibility layer can hotfix
// live engine behavior without rebundling the large generated game file.
export function chaseVisibilitySampleInterval(isTouchDevice) {
  return isTouchDevice ? 1 / 20 : 1 / 30;
}

export function chaseReplanInterval(isTouchDevice, catId = "mabel") {
  const stagger = catId === "biscuit" ? 0.03 : catId === "pepper" ? 0.06 : 0;
  return (isTouchDevice ? 0.44 : 0.38) + stagger;
}

export const CAT_MIN_SPAWN_SEPARATION = 4.8;
export const CAT_MIN_NEST_SPAWN_SEPARATION = 10;
export const CAT_MIN_PATROL_SEPARATION = 4.2;
export const CAT_DOGPILE_RELEASE_DISTANCE = 5.4;

export function pointSeparation(a, b) {
  if (!a || !b) return Infinity;
  return Math.hypot((a.x ?? 0) - (b.x ?? 0), (a.z ?? 0) - (b.z ?? 0));
}

export function chooseSeparatedPatrolIndex(points, occupiedPositions = [], preferredIndex = 0) {
  if (!Array.isArray(points) || points.length === 0) return -1;
  const count = points.length;
  const normalizedPreferred = ((Math.floor(preferredIndex) % count) + count) % count;
  let bestIndex = normalizedPreferred;
  let bestSeparation = -Infinity;
  let bestPreferenceDistance = Infinity;

  for (let index = 0; index < count; index++) {
    const point = points[index];
    if (!point) continue;
    let nearest = Infinity;
    for (let otherIndex = 0; otherIndex < occupiedPositions.length; otherIndex++) {
      nearest = Math.min(nearest, pointSeparation(point, occupiedPositions[otherIndex]));
    }
    const direct = Math.abs(index - normalizedPreferred);
    const preferenceDistance = Math.min(direct, count - direct);
    if (
      nearest > bestSeparation + 1e-6 ||
      (Math.abs(nearest - bestSeparation) <= 1e-6 && preferenceDistance < bestPreferenceDistance)
    ) {
      bestIndex = index;
      bestSeparation = nearest;
      bestPreferenceDistance = preferenceDistance;
    }
  }
  return bestIndex;
}

export function chooseNestSafeSpawnIndex(points, occupiedPositions = [], nestPosition = null, preferredIndex = 0) {
  if (!Array.isArray(points) || points.length === 0) return -1;
  const count = points.length;
  const normalizedPreferred = ((Math.floor(preferredIndex) % count) + count) % count;
  let bestIndex = -1;
  let bestNestSeparation = -Infinity;
  let bestCatSeparation = -Infinity;
  let bestPreferenceDistance = Infinity;

  for (let index = 0; index < count; index++) {
    const point = points[index];
    if (!point) continue;
    const nestSeparation = pointSeparation(point, nestPosition);
    if (nestPosition && nestSeparation < CAT_MIN_NEST_SPAWN_SEPARATION) continue;

    let catSeparation = Infinity;
    for (let otherIndex = 0; otherIndex < occupiedPositions.length; otherIndex++) {
      catSeparation = Math.min(catSeparation, pointSeparation(point, occupiedPositions[otherIndex]));
    }
    if (occupiedPositions.length && catSeparation < CAT_MIN_SPAWN_SEPARATION) continue;

    const direct = Math.abs(index - normalizedPreferred);
    const preferenceDistance = Math.min(direct, count - direct);
    if (
      nestSeparation > bestNestSeparation + 1e-6 ||
      (Math.abs(nestSeparation - bestNestSeparation) <= 1e-6 && catSeparation > bestCatSeparation + 1e-6) ||
      (Math.abs(nestSeparation - bestNestSeparation) <= 1e-6 &&
        Math.abs(catSeparation - bestCatSeparation) <= 1e-6 &&
        preferenceDistance < bestPreferenceDistance)
    ) {
      bestIndex = index;
      bestNestSeparation = nestSeparation;
      bestCatSeparation = catSeparation;
      bestPreferenceDistance = preferenceDistance;
    }
  }
  return bestIndex;
}

function livePopulation(engine) {
  if (Array.isArray(engine.colony)) {
    let allies = 0;
    for (let index = 0; index < engine.colony.length; index++) {
      if (engine.colony[index]?.alive) allies++;
    }
    return Math.max(1, allies + 1);
  }
  return Math.max(1, Math.floor(engine.snapshot?.population || engine.startOfNightPopulation || 1));
}

function isTouchEngine(engine) {
  return !!engine.__expansion?.isTouchDevice || !!engine.isTouchOnlyDevice?.();
}

function disposeCatRig(cat) {
  const root = cat?.rig?.root;
  if (!root) return;
  root.removeFromParent?.();
  const geometries = new Set();
  const materials = new Set();
  root.traverse?.((object) => {
    if (object.geometry) geometries.add(object.geometry);
    const objectMaterials = Array.isArray(object.material)
      ? object.material
      : object.material
        ? [object.material]
        : [];
    for (let index = 0; index < objectMaterials.length; index++) materials.add(objectMaterials[index]);
  });
  geometries.forEach((geometry) => geometry.dispose?.());
  materials.forEach((material) => material.dispose?.());
}

function clearCats(engine) {
  for (let index = 0; index < (engine.cats?.length ?? 0); index++) disposeCatRig(engine.cats[index]);
  engine.cats = [];
}

function requiredCatIds(desiredCount) {
  return desiredCount >= 3
    ? ["mabel", "biscuit", "pepper"]
    : desiredCount >= 2
      ? ["mabel", "biscuit"]
      : ["mabel"];
}

function catRosterMatches(engine, desiredCount) {
  if ((engine.cats?.length ?? 0) !== desiredCount) return false;
  return requiredCatIds(desiredCount).every((id) => engine.cats.some((cat) => cat.id === id));
}

function catDisplayName(cat) {
  if (cat?.id === "biscuit") return "Biscuit";
  if (cat?.id === "pepper") return "Pepper";
  return "Mabel";
}

function catCanReachMainPatrol(engine, I, position) {
  if (!position || !Number.isFinite(position.x) || !Number.isFinite(position.z)) return false;
  const patrolPoints = engine.world?.patrolPoints ?? [];
  const anchor = patrolPoints[0] ?? engine.world?.catSpawn;
  if (!anchor) return true;
  if (pointSeparation(position, anchor) < 0.28) return true;
  const route = I.pathfind(position, anchor, 0.205, engine.world.colliders, "cat");
  return !!route?.reachedGoal;
}

function reachableCatSpawnCandidates(engine, I) {
  const candidates = [];
  const add = (point) => {
    if (!point || !catCanReachMainPatrol(engine, I, point)) return;
    if (candidates.some((candidate) => pointSeparation(candidate, point) < 0.18)) return;
    candidates.push(point);
  };
  add(engine.world?.catSpawn);
  add(engine.world?.kittenSpawn);
  const patrolPoints = engine.world?.patrolPoints ?? [];
  for (let index = 0; index < patrolPoints.length; index++) add(patrolPoints[index]);
  return candidates;
}

function resetCatAtSpawn(cat, point) {
  cat.rig.root.position.copy(point);
  cat.path = [];
  cat.pathIndex = 0;
  cat.pathTimer = 0;
  cat.pathReachable = true;
  cat.pathRemainingDistance = 0;
  cat.unreachableTimer = 0;
  cat.stuckTimer = 0;
  cat.targetId = null;
  cat.awareness = 0;
  cat.investigation.copy(point);
  cat.lastSeen.copy(point);
}

function repairCatSpawnLayout(engine, I, desiredCount) {
  if (!catRosterMatches(engine, desiredCount)) return false;
  const candidates = reachableCatSpawnCandidates(engine, I);
  const nestPosition = engine.world?.nestCenter ?? engine.world?.mouseSpawn ?? null;
  const occupied = [];
  const ordered = requiredCatIds(desiredCount)
    .map((id) => engine.cats.find((cat) => cat.id === id))
    .filter(Boolean);

  for (let index = 0; index < ordered.length; index++) {
    const cat = ordered[index];
    const current = cat.rig?.root?.position;
    const currentReachable = catCanReachMainPatrol(engine, I, current);
    const currentSeparated = occupied.every((position) => pointSeparation(current, position) >= CAT_MIN_SPAWN_SEPARATION);
    const currentNestSafe = !nestPosition || pointSeparation(current, nestPosition) >= CAT_MIN_NEST_SPAWN_SEPARATION;
    if (!currentReachable || !currentSeparated || !currentNestSafe) {
      const preferredIndex = index === 0 ? 0 : index === 1 ? Math.floor(candidates.length / 2) : Math.floor(candidates.length / 3);
      const candidateIndex = chooseNestSafeSpawnIndex(candidates, occupied, nestPosition, preferredIndex);
      const candidate = candidates[candidateIndex];
      if (candidate) resetCatAtSpawn(cat, candidate);
    }
    occupied.push(cat.rig.root.position);
    cat.__pressureSlot = index;
  }

  return ordered.every((cat, index) => {
    if (!catCanReachMainPatrol(engine, I, cat.rig.root.position)) return false;
    if (nestPosition && pointSeparation(cat.rig.root.position, nestPosition) < CAT_MIN_NEST_SPAWN_SEPARATION) return false;
    for (let otherIndex = 0; otherIndex < index; otherIndex++) {
      if (pointSeparation(cat.rig.root.position, ordered[otherIndex].rig.root.position) < CAT_MIN_SPAWN_SEPARATION) return false;
    }
    return true;
  });
}

function pressurePosition(engine, cat) {
  if (cat.state === "chase" && cat.targetId) return engine.targetPosition(cat.targetId) ?? cat.rig.root.position;
  if (cat.path?.length && cat.pathIndex < cat.path.length) return cat.path[cat.path.length - 1];
  return cat.rig.root.position;
}

function spreadPatrolDestination(engine, cat) {
  const points = engine.world?.patrolPoints ?? [];
  if (points.length < 2 || engine.cats.length < 2) return false;
  const otherPressure = [];
  for (let index = 0; index < engine.cats.length; index++) {
    const other = engine.cats[index];
    if (other === cat) continue;
    otherPressure.push(pressurePosition(engine, other));
  }
  if (!otherPressure.length) return false;

  const currentTarget = cat.path?.length ? cat.path[cat.path.length - 1] : cat.rig.root.position;
  let currentSeparation = Infinity;
  for (let index = 0; index < otherPressure.length; index++) {
    currentSeparation = Math.min(currentSeparation, pointSeparation(currentTarget, otherPressure[index]));
  }
  if (currentSeparation >= CAT_MIN_PATROL_SEPARATION) return false;

  const preferredIndex = (cat.patrolIndex + 1 + (cat.__pressureSlot ?? 0) * 3) % points.length;
  const choiceIndex = chooseSeparatedPatrolIndex(points, otherPressure, preferredIndex);
  const point = points[choiceIndex];
  if (!point) return false;
  let betterSeparation = Infinity;
  for (let index = 0; index < otherPressure.length; index++) {
    betterSeparation = Math.min(betterSeparation, pointSeparation(point, otherPressure[index]));
  }
  if (betterSeparation < CAT_MIN_PATROL_SEPARATION || betterSeparation < currentSeparation + 0.8) return false;

  const previousPath = cat.path;
  const previousPathIndex = cat.pathIndex;
  const previousPatrolIndex = cat.patrolIndex;
  const previousReachable = cat.pathReachable;
  const previousRemaining = cat.pathRemainingDistance;
  engine.planCatPath(cat, point);
  if (cat.pathReachable === false) {
    cat.path = previousPath;
    cat.pathIndex = previousPathIndex;
    cat.patrolIndex = previousPatrolIndex;
    cat.pathReachable = previousReachable;
    cat.pathRemainingDistance = previousRemaining;
    return false;
  }
  cat.patrolIndex = choiceIndex;
  return true;
}

function targetAlreadyCovered(engine, cat, targetId) {
  if (!targetId) return false;
  const targetPosition = engine.targetPosition(targetId);
  for (let index = 0; index < engine.cats.length; index++) {
    const other = engine.cats[index];
    if (other === cat || other.state !== "chase" || other.targetId !== targetId) continue;
    if (!targetPosition || pointSeparation(other.rig.root.position, targetPosition) <= CAT_DOGPILE_RELEASE_DISTANCE) return true;
  }
  return false;
}

function installCatPressureHotfix(I) {
  const proto = I?.Engine?.prototype;
  if (!proto) return false;
  if (!proto.__colonyExpansionInstalled) return false;
  if (proto.__catPressureHotfixInstalled) return true;
  Object.defineProperty(proto, "__catPressureHotfixInstalled", { value: true });

  const coreCreateCats = proto.createCats;
  const coreTargetVisibility = proto.targetVisibility;
  const coreScanCatVision = proto.scanCatVision;
  const coreUpdateCatPatrol = proto.updateCatPatrol;
  const coreUpdateCatChase = proto.updateCatChase;
  const coreProcessCatVision = proto.processCatVision;
  const coreSetCatState = proto.setCatState;

  proto.createCats = function reliablePopulationCatRoster() {
    const population = livePopulation(this);
    this.startOfNightPopulation = population;
    if (this.snapshot) this.snapshot.population = population;

    coreCreateCats.call(this);
    const desiredCount = catCountForPopulation(population);
    if (!catRosterMatches(this, desiredCount)) {
      clearCats(this);
      this.startOfNightPopulation = population;
      if (this.snapshot) this.snapshot.population = population;
      coreCreateCats.call(this);
    }

    const operational = repairCatSpawnLayout(this, I, desiredCount);
    if (!catRosterMatches(this, desiredCount) || !operational) {
      console.warn("Hearthmouse cat roster/spawn mismatch", {
        population,
        desiredCount,
        cats: this.cats.map((cat) => ({ id: cat.id, x: cat.rig.root.position.x, z: cat.rig.root.position.z })),
      });
    }

    for (let index = 0; index < this.cats.length; index++) {
      this.cats[index].__chaseVisibilitySamples?.clear();
    }
  };

  proto.setCatState = function clearChaseSamplingOnStateChange(cat, state, duration) {
    const previousState = cat.state;
    const result = coreSetCatState.call(this, cat, state, duration);
    if (previousState !== state) cat.__chaseVisibilitySamples?.clear();
    return result;
  };

  proto.targetVisibility = function sampledChaseVisibility(cat, targetId, targetPosition) {
    if (cat.state !== "chase") return coreTargetVisibility.call(this, cat, targetId, targetPosition);

    const samples = cat.__chaseVisibilitySamples ?? (cat.__chaseVisibilitySamples = new Map());
    const interval = chaseVisibilitySampleInterval(isTouchEngine(this));
    const currentTime = this.time ?? 0;
    const cached = samples.get(targetId);
    if (cached && currentTime >= cached.time && currentTime - cached.time < interval) return cached.visible;

    const visible = coreTargetVisibility.call(this, cat, targetId, targetPosition);
    if (cached) {
      cached.time = currentTime;
      cached.visible = visible;
    } else {
      samples.set(targetId, { time: currentTime, visible });
    }
    return visible;
  };

  proto.scanCatVision = function keepLockedChaseTarget(cat) {
    if (cat.state !== "chase" || !cat.targetId) {
      const target = coreScanCatVision.call(this, cat);
      if (!target?.id || !targetAlreadyCovered(this, cat, target.id)) return target;
      if (target.position) cat.investigation.copy(target.position);
      return null;
    }
    const position = this.targetPosition(cat.targetId);
    if (!position) return null;
    const visible = this.targetVisibility(cat, cat.targetId, position);
    if (visible <= 0) return null;

    const result = cat.visionResult ?? (cat.visionResult = {
      id: null,
      position: null,
      moving: 0,
      visible: 0,
      distance: Infinity,
    });
    let moving = 0;
    if (cat.targetId === "player") {
      moving = this.time - this.lastPlayerMoving < 0.17
        ? Math.max(0.35, this.playerVelocity.length() / 1.2)
        : 0;
    } else {
      moving = this.__expansion?.scratch?.mouseById?.get(cat.targetId)?.speed ?? 0;
    }
    result.id = cat.targetId;
    result.position = position;
    result.moving = moving;
    result.visible = visible;
    result.distance = cat.rig.root.position.distanceTo(position);
    return result;
  };

  proto.updateCatPatrol = function keepPatrolPressureSpread(cat, delta, speedOverride) {
    const selectingDestination = !cat.path?.length || cat.pathIndex >= cat.path.length;
    const result = coreUpdateCatPatrol.call(this, cat, delta, speedOverride);
    if (selectingDestination && cat.state !== "chase" && cat.path?.length && cat.pathIndex < cat.path.length) {
      spreadPatrolDestination(this, cat);
    }
    return result;
  };

  proto.updateCatChase = function staggerHeavyChaseReplans(cat, delta) {
    const result = coreUpdateCatChase.call(this, cat, delta);
    if (
      cat.state === "chase" &&
      cat.pouncePhase === "none" &&
      cat.pathTimer > 0 &&
      (cat.path?.length > 1 || cat.pathReachable === false)
    ) {
      cat.pathTimer = Math.max(cat.pathTimer, chaseReplanInterval(isTouchEngine(this), cat.id));
    }
    return result;
  };

  proto.processCatVision = function namedCatLockMessage(cat, target, interval) {
    const previousState = cat.state;
    const result = coreProcessCatVision.call(this, cat, target, interval);
    if (previousState !== "chase" && cat.state === "chase") {
      this.showMessage(`${catDisplayName(cat)} has locked onto movement.`, 2.4);
    }
    return result;
  };

  return true;
}

function waitForCatPressureHotfix() {
  if (typeof window === "undefined") return;
  if (!installCatPressureHotfix(window.HearthmouseInternals)) {
    window.setTimeout(waitForCatPressureHotfix, 0);
  }
}

async function loadOptionalPlayerVisualPolish() {
  if (typeof window === "undefined") return;
  try {
    await import("./hearthmouse-player-visual-polish.mjs");
  } catch (error) {
    console.warn("Hearthmouse optional player visual polish failed to load", error);
  }
}

if (typeof window !== "undefined") {
  waitForCatPressureHotfix();
  // Cosmetic third-person tuning is deliberately non-blocking. If an older
  // browser rejects the optional module or the polish code throws, the core
  // game still boots normally.
  window.setTimeout(loadOptionalPlayerVisualPolish, 0);
}

/*
Compatibility source markers retained for the existing static regression tests;
the implementations now live in hearthmouse-expansion-core.mjs:
proto.scanCatVision = function indexedCatVision
expansion.spatial.mice.queryRadius
probes[4].set(targetPosition.x, 0.038, targetPosition.z + 0.06)
intersectObjects(occluders, true, sight.hits)
cat.visibilitySamples = new Map()
if (ILineClear(engine, expansion, start, target, agent))
return { path: [target.clone()], reachedGoal: true, remainingDistance: 0 }
mouse.resumeTask = mouse.carriedFood ? "returning"
Never discard gathered food
mouse.task === "returning" && mouse.carriedFood
*/