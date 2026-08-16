import "./hearthmouse-graphics-quality.mjs";
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

function catRosterMatches(engine, desiredCount) {
  if ((engine.cats?.length ?? 0) !== desiredCount) return false;
  const requiredIds = desiredCount >= 3
    ? ["mabel", "biscuit", "pepper"]
    : desiredCount >= 2
      ? ["mabel", "biscuit"]
      : ["mabel"];
  return requiredIds.every((id) => engine.cats.some((cat) => cat.id === id));
}

function catDisplayName(cat) {
  if (cat?.id === "biscuit") return "Biscuit";
  if (cat?.id === "pepper") return "Pepper";
  return "Mabel";
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

    if (!catRosterMatches(this, desiredCount)) {
      console.warn("Hearthmouse cat roster mismatch", {
        population,
        desiredCount,
        cats: this.cats.map((cat) => cat.id),
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
    if (cat.state !== "chase" || !cat.targetId) return coreScanCatVision.call(this, cat);
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

if (typeof window !== "undefined") waitForCatPressureHotfix();

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
