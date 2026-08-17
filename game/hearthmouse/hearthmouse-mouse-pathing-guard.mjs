const POLL_INTERVAL_MS = 40;
const MAX_INSTALL_ATTEMPTS = 300;
const MOUSE_RADIUS = 0.055;
const NEST_GATE_CLEARANCE = 0.28;
const DETOUR_TRIGGER_SECONDS = 0.44;
const DETOUR_RETRY_COOLDOWN = 0.34;

const MOVING_TASKS = new Set(["escaping", "to-food", "returning", "home", "nesting-move"]);

export function pointInsideBounds(bounds, point, padding = 0) {
  if (!bounds || !point) return false;
  return (
    point.x >= bounds.minX - padding &&
    point.x <= bounds.maxX + padding &&
    point.z >= bounds.minZ - padding &&
    point.z <= bounds.maxZ + padding
  );
}

export function nestGateCoordinates(bounds, deposit, clearance = NEST_GATE_CLEARANCE) {
  if (!bounds) return null;
  const minZ = bounds.minZ + 0.18;
  const maxZ = bounds.maxZ - 0.18;
  const preferredZ = Number.isFinite(deposit?.z) ? deposit.z : (bounds.minZ + bounds.maxZ) * 0.5;
  return {
    x: bounds.maxX + clearance,
    z: Math.max(minZ, Math.min(maxZ, preferredZ)),
  };
}

export function crossesNestBoundary(bounds, start, target) {
  return pointInsideBounds(bounds, start) !== pointInsideBounds(bounds, target);
}

export function shouldTriggerMouseDetour({ stalledFor, moved, distanceToGoal, cooldownElapsed }) {
  return (
    stalledFor >= DETOUR_TRIGGER_SECONDS &&
    moved < 0.006 &&
    distanceToGoal > 0.18 &&
    cooldownElapsed >= DETOUR_RETRY_COOLDOWN
  );
}

function targetForMouse(engine, mouse) {
  if (mouse.task === "escaping" && mouse.escapeGoal) return mouse.escapeGoal;
  if (mouse.task === "to-food") return mouse.targetFood?.mesh?.position ?? null;
  if (mouse.task === "returning") return engine.world?.nestDeposit ?? null;
  if (mouse.task === "home" || mouse.task === "nesting-move") {
    return mouse.nestActivityGoal ?? engine.world?.nestDeposit ?? null;
  }
  return null;
}

function pathEndsNear(path, target, tolerance = 0.34) {
  if (!Array.isArray(path) || path.length === 0 || !target) return false;
  const last = path[path.length - 1];
  if (!last) return false;
  return Math.hypot(last.x - target.x, last.z - target.z) <= tolerance;
}

function clonePoint(point, I) {
  if (!point) return null;
  if (typeof point.clone === "function") return point.clone();
  return new I.Vector3(point.x ?? 0, point.y ?? 0.025, point.z ?? 0);
}

function dedupePath(points, I) {
  const result = [];
  for (let index = 0; index < points.length; index++) {
    const point = points[index];
    if (!point) continue;
    const previous = result[result.length - 1];
    if (previous && Math.hypot(previous.x - point.x, previous.z - point.z) < 0.045) continue;
    result.push(clonePoint(point, I));
  }
  return result;
}

function captureCorePlan(engine, corePlanMousePath, mouse, options = {}) {
  const rootPosition = mouse.rig?.root?.position;
  if (!rootPosition) return [];

  const savedPosition = clonePoint(rootPosition, options.I);
  const savedPath = mouse.path;
  const savedPathIndex = mouse.pathIndex;
  const savedNestActivityGoal = mouse.nestActivityGoal;
  let planned = [];

  try {
    if (options.virtualStart) rootPosition.copy(options.virtualStart);
    if (options.nestGoalOverride) mouse.nestActivityGoal = options.nestGoalOverride;
    corePlanMousePath.call(engine, mouse);
    planned = Array.isArray(mouse.path) ? mouse.path.map((point) => clonePoint(point, options.I)) : [];
  } finally {
    rootPosition.copy(savedPosition);
    mouse.path = savedPath;
    mouse.pathIndex = savedPathIndex;
    mouse.nestActivityGoal = savedNestActivityGoal;
  }

  return planned;
}

function pathfindLeg(I, engine, start, target) {
  if (!start || !target) return null;
  if (I.lineClear?.(start, target, MOUSE_RADIUS, engine.world.colliders, "mouse")) {
    return { path: [clonePoint(target, I)], reachedGoal: true };
  }

  const radii = [MOUSE_RADIUS, 0.05, 0.045];
  for (let index = 0; index < radii.length; index++) {
    const route = I.pathfind(start, target, radii[index], engine.world.colliders, "mouse");
    if (route?.reachedGoal && route.path?.length) return route;
  }
  return null;
}

function buildNestBoundaryPath(engine, I, corePlanMousePath, mouse, start, target) {
  const bounds = engine.world?.nestBounds;
  if (!bounds || !crossesNestBoundary(bounds, start, target)) return null;

  const gateCoords = nestGateCoordinates(bounds, engine.world?.nestDeposit);
  if (!gateCoords) return null;
  const gate = new I.Vector3(gateCoords.x, 0.025, gateCoords.z);
  const startInside = pointInsideBounds(bounds, start);

  if (startInside) {
    // The nest has three solid mouse walls and one deliberately narrow east opening.
    // Always leave through that opening first; otherwise an A* path aimed at food
    // behind the nest can repeatedly press a colony mouse into the north/south wall.
    const exitLeg = pathfindLeg(I, engine, start, gate);
    if (!exitLeg?.reachedGoal) return null;

    const remainder = captureCorePlan(engine, corePlanMousePath, mouse, { I, virtualStart: gate });
    if (!pathEndsNear(remainder, target)) return null;
    return dedupePath([...exitLeg.path, ...remainder], I);
  }

  if (["returning", "home", "nesting-move"].includes(mouse.task)) {
    // Returning mice should approach the same opening from outside instead of
    // choosing the closest geometric side of the nest wall.
    const approach = captureCorePlan(engine, corePlanMousePath, mouse, {
      I,
      nestGoalOverride: gate,
    });
    if (!pathEndsNear(approach, gate)) return null;

    const entryLeg = pathfindLeg(I, engine, gate, target);
    if (!entryLeg?.reachedGoal) return null;
    return dedupePath([...approach, ...entryLeg.path], I);
  }

  return null;
}

function detourCandidateAngles(start, target) {
  const towardGoal = Math.atan2(target.z - start.z, target.x - start.x);
  return [
    towardGoal + Math.PI / 2,
    towardGoal - Math.PI / 2,
    towardGoal + Math.PI / 3,
    towardGoal - Math.PI / 3,
    towardGoal + (Math.PI * 2) / 3,
    towardGoal - (Math.PI * 2) / 3,
    towardGoal + Math.PI,
    towardGoal,
  ];
}

function installLocalDetour(engine, I, corePlanMousePath, mouse, target) {
  const start = mouse.rig?.root?.position;
  if (!start || !target) return false;

  // The nest gateway solution is deterministic and preferable whenever the
  // mouse is crossing the nest boundary.
  const nestPath = buildNestBoundaryPath(engine, I, corePlanMousePath, mouse, start, target);
  if (nestPath?.length) {
    mouse.path = nestPath;
    mouse.pathIndex = 0;
    return true;
  }

  // For furniture/wall corners elsewhere in the house, probe a few clear side
  // steps and virtually re-run the existing smart planner from that point.
  // This preserves the expansion room graph while escaping a bad local start.
  const angles = detourCandidateAngles(start, target);
  const rings = [0.22, 0.38, 0.56];
  for (let ringIndex = 0; ringIndex < rings.length; ringIndex++) {
    const radius = rings[ringIndex];
    for (let angleIndex = 0; angleIndex < angles.length; angleIndex++) {
      const angle = angles[angleIndex];
      const candidate = new I.Vector3(
        start.x + Math.cos(angle) * radius,
        0.025,
        start.z + Math.sin(angle) * radius,
      );
      if (!I.lineClear?.(start, candidate, MOUSE_RADIUS, engine.world.colliders, "mouse")) continue;

      const tail = captureCorePlan(engine, corePlanMousePath, mouse, { I, virtualStart: candidate });
      if (!pathEndsNear(tail, target)) continue;

      mouse.path = dedupePath([candidate, ...tail], I);
      mouse.pathIndex = 0;
      return true;
    }
  }

  return false;
}

function guardState(mouse, engine) {
  if (!mouse.__wallPathGuard) {
    mouse.__wallPathGuard = {
      stalledFor: 0,
      lastDetourAt: -Infinity,
      lastTask: mouse.task,
      lastX: mouse.rig?.root?.position?.x ?? 0,
      lastZ: mouse.rig?.root?.position?.z ?? 0,
      detourAttempts: 0,
      lastPlanAt: engine.time ?? 0,
    };
  }
  return mouse.__wallPathGuard;
}

function installMousePathingGuard(I) {
  const proto = I?.Engine?.prototype;
  if (!proto?.__colonyExpansionInstalled) return false;
  if (proto.__mousePathingGuardInstalled) return true;

  const corePlanMousePath = proto.planMousePath;
  const coreFollowMousePath = proto.followMousePath;
  if (typeof corePlanMousePath !== "function" || typeof coreFollowMousePath !== "function") return false;

  Object.defineProperty(proto, "__mousePathingGuardInstalled", { value: true });

  proto.planMousePath = function nestAndWallAwareMousePath(mouse) {
    const start = mouse.rig?.root?.position;
    const target = targetForMouse(this, mouse);
    if (!start || !target) return corePlanMousePath.call(this, mouse);

    const boundaryPath = buildNestBoundaryPath(this, I, corePlanMousePath, mouse, start, target);
    if (boundaryPath?.length) {
      mouse.path = boundaryPath;
      mouse.pathIndex = 0;
      const state = guardState(mouse, this);
      state.lastPlanAt = this.time ?? 0;
      return;
    }

    corePlanMousePath.call(this, mouse);
    if (!pathEndsNear(mouse.path, target)) {
      installLocalDetour(this, I, corePlanMousePath, mouse, target);
    }
    const state = guardState(mouse, this);
    state.lastPlanAt = this.time ?? 0;
  };

  proto.followMousePath = function wallRecoveringMouseFollow(mouse, delta, speed) {
    const state = guardState(mouse, this);
    const beforeX = mouse.rig?.root?.position?.x ?? state.lastX;
    const beforeZ = mouse.rig?.root?.position?.z ?? state.lastZ;
    const result = coreFollowMousePath.call(this, mouse, delta, speed);
    const position = mouse.rig?.root?.position;
    if (!position) return result;

    const moved = Math.hypot(position.x - beforeX, position.z - beforeZ);
    const target = targetForMouse(this, mouse);
    const taskChanged = state.lastTask !== mouse.task;
    state.lastTask = mouse.task;
    state.lastX = position.x;
    state.lastZ = position.z;

    if (taskChanged || !MOVING_TASKS.has(mouse.task) || !target) {
      state.stalledFor = 0;
      state.detourAttempts = 0;
      return result;
    }

    const distanceToGoal = Math.hypot(target.x - position.x, target.z - position.z);
    const expectedMovement = Math.max(0.0025, Math.abs(speed || 0) * Math.max(0, delta || 0) * 0.12);
    if (moved >= expectedMovement || distanceToGoal <= 0.18) {
      state.stalledFor = 0;
      state.detourAttempts = 0;
      return result;
    }

    state.stalledFor += Math.max(0, delta || 0);
    const now = this.time ?? 0;
    if (!shouldTriggerMouseDetour({
      stalledFor: state.stalledFor,
      moved,
      distanceToGoal,
      cooldownElapsed: now - state.lastDetourAt,
    })) return result;

    state.lastDetourAt = now;
    state.detourAttempts++;
    const repaired = installLocalDetour(this, I, corePlanMousePath, mouse, target);
    if (!repaired) {
      // Clear a stale/partial path and let the guarded planner rebuild it.
      mouse.path = [];
      mouse.pathIndex = 0;
      this.planMousePath(mouse);
    }

    // Do not wait for the older ~1 second anti-stall watchdog before trying a
    // different route. A mouse visibly leaning into a wall should recover fast.
    state.stalledFor = repaired ? 0 : Math.min(state.stalledFor, 0.2);
    return result;
  };

  return true;
}

function installWhenReady(attempt = 0) {
  if (typeof window === "undefined") return;
  if (installMousePathingGuard(window.HearthmouseInternals)) return;
  if (attempt < MAX_INSTALL_ATTEMPTS) {
    window.setTimeout(() => installWhenReady(attempt + 1), POLL_INTERVAL_MS);
  }
}

if (typeof window !== "undefined") installWhenReady();
