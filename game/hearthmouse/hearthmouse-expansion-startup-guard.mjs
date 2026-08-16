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

function guardWhenReady() {
  if (typeof window === "undefined") return;
  const engine = window.hearthmouseEngine;
  if (!engine || engine.disposed || !window.HearthmouseInternals?.Vector3) {
    window.setTimeout(guardWhenReady, 0);
    return;
  }
  installExpansionSightGuard(engine, window.HearthmouseInternals);
}

if (typeof window !== "undefined") guardWhenReady();
