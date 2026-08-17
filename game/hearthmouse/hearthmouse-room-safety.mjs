export const CAT_AGENT_RADIUS = 0.205;
export const DOORWAY_CLEARANCE_RADIUS = 1.05;
export const STATIC_DOORWAY_CLEARANCE_RADIUS = CAT_AGENT_RADIUS + 0.1;
export const CAT_RECOVERY_CHECK_INTERVAL = 2.25;
export const CAT_MIN_CONNECTED_PATROLS = 2;
export const CAT_CONVERGENCE_MIN_DISTANCE = 1.6;
export const CAT_CONVERGENCE_MAX_DISTANCE = 5.6;

export function planarDistance(a, b) {
  if (!a || !b) return Infinity;
  return Math.hypot((a.x ?? 0) - (b.x ?? 0), (a.z ?? 0) - (b.z ?? 0));
}

export function pointInsideDoorwayClearance(point, doorway, radius = DOORWAY_CLEARANCE_RADIUS) {
  return planarDistance(point, doorway) <= radius;
}

export function colliderInsideDoorwayClearance(collider, doorway, radius = STATIC_DOORWAY_CLEARANCE_RADIUS) {
  if (!collider || !doorway) return false;
  const minX = Number(collider.minX);
  const maxX = Number(collider.maxX);
  const minZ = Number(collider.minZ);
  const maxZ = Number(collider.maxZ);
  if (![minX, maxX, minZ, maxZ].every(Number.isFinite)) return false;
  const nearestX = Math.max(minX, Math.min(maxX, doorway.x ?? 0));
  const nearestZ = Math.max(minZ, Math.min(maxZ, doorway.z ?? 0));
  return Math.hypot(nearestX - (doorway.x ?? 0), nearestZ - (doorway.z ?? 0)) <= radius;
}

export function chooseConvergencePatrolIndex(points, otherPosition, preferredIndex = 0) {
  if (!Array.isArray(points) || points.length === 0 || !otherPosition) return -1;
  const count = points.length;
  const preferred = ((Math.floor(preferredIndex) % count) + count) % count;
  let best = -1;
  let bestPreference = Infinity;
  let bestDistance = Infinity;

  for (let index = 0; index < count; index++) {
    const distance = planarDistance(points[index], otherPosition);
    if (distance < CAT_CONVERGENCE_MIN_DISTANCE || distance > CAT_CONVERGENCE_MAX_DISTANCE) continue;
    const direct = Math.abs(index - preferred);
    const preference = Math.min(direct, count - direct);
    if (preference < bestPreference || (preference === bestPreference && distance < bestDistance)) {
      best = index;
      bestPreference = preference;
      bestDistance = distance;
    }
  }
  return best;
}

function roomUnlocked(expansion, roomId, night) {
  if (!roomId) return true;
  if (expansion.temporaryRoomId === roomId) return true;
  const definition = expansion.roomDefinitions?.find?.((room) => room.id === roomId);
  if (definition) return night >= (definition.unlockNight ?? 1);
  const knownDoor = expansion.roomDoors?.find?.((door) => door.roomId === roomId);
  return !knownDoor || night >= (knownDoor.unlockNight ?? 1);
}

function edgeOpenForCat(engine, expansion, edge) {
  if (!edge || edge.mouseOnly) return false;
  const night = engine.snapshot?.night ?? expansion.planNight ?? 1;
  if (edge.unlockNight && night < edge.unlockNight && expansion.temporaryRoomId !== edge.a && expansion.temporaryRoomId !== edge.b) return false;
  if (!roomUnlocked(expansion, edge.a, night) || !roomUnlocked(expansion, edge.b, night)) return false;
  if (edge.doorId) {
    const door = expansion.roomDoors?.find?.((candidate) => candidate.id === edge.doorId);
    if (door?.collider?.active) return false;
  }
  if (edge.dynamicProp) {
    const prop = expansion.dynamicProps?.get?.(edge.dynamicProp);
    if (prop?.collider?.active) return false;
  }
  if (edge.passage) {
    const blocker = expansion.dynamicProps?.get?.("passage-block");
    if (blocker?.collider?.active) return false;
  }
  return true;
}

function removeOccluder(expansion, mesh) {
  const occluders = expansion.world?.occluders;
  if (!mesh || !Array.isArray(occluders)) return;
  const index = occluders.indexOf(mesh);
  if (index >= 0) occluders.splice(index, 1);
}

function deactivateProp(expansion, prop) {
  if (!prop) return false;
  if (prop.mesh) prop.mesh.visible = false;
  if (prop.collider) prop.collider.active = false;
  removeOccluder(expansion, prop.mesh);
  return true;
}

function deactivateStaticDoorwayCollider(engine, expansion, collider) {
  if (!collider?.active) return false;
  collider.active = false;
  const mesh = collider.name ? engine.world?.root?.getObjectByName?.(collider.name) : null;
  if (mesh) mesh.visible = false;
  removeOccluder(expansion, mesh);
  return true;
}

function clearDynamicPropsFromDoorways(engine, expansion) {
  const edges = expansion.navEdges ?? [];
  const dynamicProps = expansion.dynamicProps;
  if (!dynamicProps?.values) return 0;
  let cleared = 0;

  for (const edge of edges) {
    if (!edgeOpenForCat(engine, expansion, edge) || !edge.point) continue;
    for (const prop of dynamicProps.values()) {
      if (!prop?.collider?.active || !prop.mesh?.position) continue;
      if (edge.dynamicProp && dynamicProps.get(edge.dynamicProp) === prop) continue;
      if (pointInsideDoorwayClearance(prop.mesh.position, edge.point)) {
        deactivateProp(expansion, prop);
        cleared++;
      }
    }
  }
  return cleared;
}

function clearStaticCollidersFromDoorways(engine, expansion, radius = STATIC_DOORWAY_CLEARANCE_RADIUS) {
  const edges = expansion.navEdges ?? [];
  const colliders = engine.world?.colliders ?? [];
  if (!colliders.length) return 0;
  const dynamicColliders = new Set();
  for (const prop of expansion.dynamicProps?.values?.() ?? []) {
    if (prop?.collider) dynamicColliders.add(prop.collider);
  }
  let cleared = 0;

  for (const edge of edges) {
    if (!edgeOpenForCat(engine, expansion, edge) || !edge.point) continue;
    for (const collider of colliders) {
      if (!collider?.active || dynamicColliders.has(collider)) continue;
      if (!colliderInsideDoorwayClearance(collider, edge.point, radius)) continue;
      if (deactivateStaticDoorwayCollider(engine, expansion, collider)) cleared++;
    }
  }
  return cleared;
}

function pathReached(I, engine, from, to) {
  if (!I?.pathfind || !from || !to) return false;
  if (planarDistance(from, to) < 0.25) return true;
  return !!I.pathfind(from, to, CAT_AGENT_RADIUS, engine.world?.colliders ?? [], "cat")?.reachedGoal;
}

function mainPatrolAnchor(engine) {
  return engine.world?.patrolPoints?.[0] ?? engine.world?.catSpawn ?? null;
}

function doorwayConnectivityFailures(engine, expansion, I) {
  const anchor = mainPatrolAnchor(engine);
  if (!anchor) return [];
  const failures = [];
  for (const edge of expansion.navEdges ?? []) {
    if (!edgeOpenForCat(engine, expansion, edge) || !edge.point) continue;
    if (!pathReached(I, engine, edge.point, anchor)) failures.push(edge);
  }
  return failures;
}

function validateDoorwayConnectivity(engine, I) {
  const expansion = engine.__expansion;
  if (!expansion?.navEdges || !engine.world) return true;
  clearDynamicPropsFromDoorways(engine, expansion);
  clearStaticCollidersFromDoorways(engine, expansion);
  let failures = doorwayConnectivityFailures(engine, expansion, I);

  if (failures.length) {
    // A moved prop may be just beyond the nominal clearance radius while still
    // pinching the cat-sized route. Remove nearby dynamic blockers once more
    // before accepting a disconnected entrance. Static trim/furniture also gets
    // a slightly wider pass, while remaining inside the narrow doorway opening.
    for (const edge of failures) {
      for (const prop of expansion.dynamicProps?.values?.() ?? []) {
        if (!prop?.collider?.active || !prop.mesh?.position) continue;
        if (pointInsideDoorwayClearance(prop.mesh.position, edge.point, DOORWAY_CLEARANCE_RADIUS + 0.75)) {
          deactivateProp(expansion, prop);
        }
      }
    }
    clearStaticCollidersFromDoorways(engine, expansion, STATIC_DOORWAY_CLEARANCE_RADIUS + 0.08);
    failures = doorwayConnectivityFailures(engine, expansion, I);
  }

  if (failures.length) {
    console.warn("Hearthmouse entrance connectivity check failed", failures.map((edge) => ({ a: edge.a, b: edge.b, doorId: edge.doorId })));
    return false;
  }
  return true;
}

function reachablePatrolCount(engine, I, position, stopAfter = CAT_MIN_CONNECTED_PATROLS) {
  let count = 0;
  for (const point of engine.world?.patrolPoints ?? []) {
    if (pathReached(I, engine, position, point)) {
      count++;
      if (count >= stopAfter) return count;
    }
  }
  return count;
}

function safestRecoveryPoint(engine, I, cat) {
  const points = engine.world?.patrolPoints ?? [];
  const others = (engine.cats ?? []).filter((candidate) => candidate !== cat && candidate.rig?.root?.position);
  let best = null;
  let bestSeparation = -Infinity;
  for (const point of points) {
    if (reachablePatrolCount(engine, I, point, CAT_MIN_CONNECTED_PATROLS) < CAT_MIN_CONNECTED_PATROLS) continue;
    let separation = Infinity;
    for (const other of others) separation = Math.min(separation, planarDistance(point, other.rig.root.position));
    if (separation > bestSeparation) {
      best = point;
      bestSeparation = separation;
    }
  }
  return best ?? engine.world?.catSpawn ?? points[0] ?? null;
}

function resetCat(cat, point) {
  if (!cat?.rig?.root?.position || !point) return false;
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
  cat.investigation?.copy?.(point);
  cat.lastSeen?.copy?.(point);
  cat.__roomSafetyLastPosition?.copy?.(point);
  return true;
}

function recoverDisconnectedCat(engine, I, cat) {
  const position = cat?.rig?.root?.position;
  if (!position) return false;
  if (reachablePatrolCount(engine, I, position, CAT_MIN_CONNECTED_PATROLS) >= CAT_MIN_CONNECTED_PATROLS) return false;
  const recovery = safestRecoveryPoint(engine, I, cat);
  if (!recovery) return false;
  return resetCat(cat, recovery);
}

function maybeConvergePatrol(engine, cat) {
  if ((engine.cats?.length ?? 0) < 2 || cat.state === "chase" || typeof engine.planCatPath !== "function") return false;
  const now = engine.time ?? 0;
  if (cat.__nextRoomSafetyConvergence == null) {
    cat.__nextRoomSafetyConvergence = now + 18 + (cat.__pressureSlot ?? 0) * 5;
    return false;
  }
  if (now < cat.__nextRoomSafetyConvergence) return false;
  cat.__nextRoomSafetyConvergence = now + 28 + Math.random() * 18;
  if (Math.random() > 0.48) return false;

  const others = engine.cats.filter((candidate) => candidate !== cat && candidate.state !== "chase" && candidate.rig?.root?.position);
  if (!others.length) return false;
  const other = others[Math.floor(Math.random() * others.length)];
  const points = engine.world?.patrolPoints ?? [];
  const preferred = (cat.patrolIndex ?? 0) + 1;
  const index = chooseConvergencePatrolIndex(points, other.rig.root.position, preferred);
  if (index < 0) return false;

  const previousPath = cat.path;
  const previousPathIndex = cat.pathIndex;
  const previousPatrolIndex = cat.patrolIndex;
  const previousReachable = cat.pathReachable;
  const previousRemaining = cat.pathRemainingDistance;
  engine.planCatPath(cat, points[index]);
  if (cat.pathReachable === false) {
    cat.path = previousPath;
    cat.pathIndex = previousPathIndex;
    cat.patrolIndex = previousPatrolIndex;
    cat.pathReachable = previousReachable;
    cat.pathRemainingDistance = previousRemaining;
    return false;
  }
  cat.patrolIndex = index;
  return true;
}

export function installRoomSafetyGuard(I = globalThis.window?.HearthmouseInternals) {
  const proto = I?.Engine?.prototype;
  if (!proto?.__nightCatPressureGuardInstalled) return false;
  if (proto.__roomSafetyGuardInstalled) return true;
  if (typeof proto.createCats !== "function" || typeof proto.updateCatPatrol !== "function") return false;

  Object.defineProperty(proto, "__roomSafetyGuardInstalled", { value: true });
  const baseCreateCats = proto.createCats;
  const baseUpdateCatPatrol = proto.updateCatPatrol;
  const baseUpdateCatChase = proto.updateCatChase;

  proto.createCats = function connectedCatRoster() {
    const result = baseCreateCats.call(this);
    validateDoorwayConnectivity(this, I);
    for (const cat of this.cats ?? []) recoverDisconnectedCat(this, I, cat);
    return result;
  };

  proto.updateCatPatrol = function connectedCatPatrol(cat, delta, speedOverride) {
    const result = baseUpdateCatPatrol.call(this, cat, delta, speedOverride);
    const now = this.time ?? 0;
    if (now >= (cat.__nextRoomSafetyCheck ?? 0)) {
      cat.__nextRoomSafetyCheck = now + CAT_RECOVERY_CHECK_INTERVAL;
      recoverDisconnectedCat(this, I, cat);
    }
    maybeConvergePatrol(this, cat);
    return result;
  };

  if (typeof baseUpdateCatChase === "function") {
    proto.updateCatChase = function connectedCatChase(cat, delta) {
      const result = baseUpdateCatChase.call(this, cat, delta);
      const now = this.time ?? 0;
      if (cat.pathReachable === false && now >= (cat.__nextRoomSafetyCheck ?? 0)) {
        cat.__nextRoomSafetyCheck = now + CAT_RECOVERY_CHECK_INTERVAL;
        recoverDisconnectedCat(this, I, cat);
      }
      return result;
    };
  }

  const engine = globalThis.window?.hearthmouseEngine;
  if (engine?.world?.setNight && !engine.world.__roomSafetySetNightWrapped) {
    const baseSetNight = engine.world.setNight.bind(engine.world);
    engine.world.__roomSafetySetNightWrapped = true;
    engine.world.setNight = (night) => {
      const result = baseSetNight(night);
      validateDoorwayConnectivity(engine, I);
      return result;
    };
    validateDoorwayConnectivity(engine, I);
  }

  return true;
}

function installWhenReady() {
  if (typeof window === "undefined") return;
  if (!installRoomSafetyGuard(window.HearthmouseInternals)) window.setTimeout(installWhenReady, 16);
}

if (typeof window !== "undefined") installWhenReady();
