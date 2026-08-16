import "./hearthmouse-room-safety.mjs";

function createSightState(I) {
  const V = I?.Vector3;
  if (!V) return null;
  return {
    cat: null,
    time: -Infinity,
    eye: new V(),
    center: new V(),
    flatDirection: new V(),
    forward: new V(),
    rayDirection: new V(),
    up: new V(0, 1, 0),
    probes: Array.from({ length: 5 }, () => new V()),
    hits: [],
    occluderCenter: new V(),
    occluderScale: new V(1, 1, 1),
  };
}

export function catCountForNight(night) {
  const currentNight = Math.max(1, Math.floor(Number(night) || 1));
  if (currentNight >= 8) return 3;
  if (currentNight >= 4) return 2;
  return 1;
}

export function catSpeedMultiplierForNight(night) {
  const currentNight = Math.max(1, Math.floor(Number(night) || 1));
  return 1 + Math.min(0.2, (currentNight - 1) * 0.02);
}

function syntheticPopulationForCatCount(catCount) {
  if (catCount >= 3) return 15;
  if (catCount >= 2) return 8;
  return 1;
}

export function ensureExpansionSight(expansion, I = globalThis.window?.HearthmouseInternals) {
  if (!expansion) return false;
  if (expansion.sight?.occluderCenter && expansion.sight?.occluderScale) return true;
  const sight = createSightState(I);
  if (!sight) return false;
  expansion.sight = sight;
  return true;
}

export function installExpansionSightGuard(engine, I = globalThis.window?.HearthmouseInternals) {
  if (!engine || engine.disposed || !I?.Vector3) return false;

  // Repair a partially-created expansion left behind by the old startup crash.
  if (engine.__expansion) return ensureExpansionSight(engine.__expansion, I);

  const current = Object.getOwnPropertyDescriptor(engine, "__expansion");
  if (current && !current.configurable) return false;
  let pending = current?.value;

  Object.defineProperty(engine, "__expansion", {
    configurable: true,
    enumerable: current?.enumerable ?? true,
    get() {
      return pending;
    },
    set(expansion) {
      pending = expansion;
      ensureExpansionSight(expansion, I);
      Object.defineProperty(engine, "__expansion", {
        value: expansion,
        writable: true,
        configurable: true,
        enumerable: true,
      });
    },
  });
  return true;
}

export function installNightCatPressureGuard(I = globalThis.window?.HearthmouseInternals) {
  const proto = I?.Engine?.prototype;
  if (!proto?.__catPressureHotfixInstalled) return false;
  if (proto.__nightCatPressureGuardInstalled) return true;
  if (typeof proto.createCats !== "function" || typeof proto.followCatPath !== "function") return false;

  const populationCreateCats = proto.createCats;
  const baseFollowCatPath = proto.followCatPath;
  Object.defineProperty(proto, "__nightCatPressureGuardInstalled", { value: true });

  proto.createCats = function nightDrivenCatRoster() {
    const desiredCount = catCountForNight(this.snapshot?.night ?? 1);
    const syntheticPopulation = syntheticPopulationForCatCount(desiredCount);
    const actualColony = this.colony;
    const actualStartOfNightPopulation = this.startOfNightPopulation;
    const actualSnapshotPopulation = this.snapshot?.population;

    // The existing reliable multi-cat factory is population-shaped internally.
    // Feed it the legacy threshold only while it creates the roster, then restore
    // the real colony immediately. Night number is now the gameplay trigger.
    const syntheticAllies = Math.max(0, syntheticPopulation - 1);
    this.colony = Array.from({ length: syntheticAllies }, (_, index) => ({
      ...(actualColony?.[index] ?? {}),
      alive: true,
    }));
    this.startOfNightPopulation = syntheticPopulation;
    if (this.snapshot) this.snapshot.population = syntheticPopulation;

    try {
      populationCreateCats.call(this);
    } finally {
      this.colony = actualColony;
      this.startOfNightPopulation = actualStartOfNightPopulation;
      if (this.snapshot) this.snapshot.population = actualSnapshotPopulation;
    }
  };

  proto.followCatPath = function progressivelyFasterCats(cat, delta, speed) {
    const multiplier = catSpeedMultiplierForNight(this.snapshot?.night ?? 1);
    const scaledSpeed = Number.isFinite(speed) ? speed * multiplier : speed;
    return baseFollowCatPath.call(this, cat, delta, scaledSpeed);
  };

  return true;
}

function guardWhenReady() {
  if (typeof window === "undefined") return;
  const engine = window.hearthmouseEngine;
  if (!engine || engine.disposed || !window.HearthmouseInternals?.Vector3) {
    window.setTimeout(guardWhenReady, 0);
    return;
  }
  installExpansionSightGuard(engine, window.HearthmouseInternals);
}

function installNightCatPressureWhenReady() {
  if (typeof window === "undefined") return;
  if (!installNightCatPressureGuard(window.HearthmouseInternals)) {
    window.setTimeout(installNightCatPressureWhenReady, 16);
  }
}

if (typeof window !== "undefined") {
  guardWhenReady();
  installNightCatPressureWhenReady();
}
