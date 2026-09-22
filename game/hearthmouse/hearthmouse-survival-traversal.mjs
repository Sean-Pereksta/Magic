import { foodLoad, clamp, distance } from "./hearthmouse-survival-core.mjs";
import { isFoodPositionClear } from "./hearthmouse-habitat.mjs";

// Ground-level cat-only volumes are exclusion zones, not mouse geometry (the
// nest entrance is one). Raised cat-only furniture still becomes solid when
// the mouse climbs to its height. Use the same floor tolerance as movement.
export function isMousePassThroughCollider(collider) {
  return Boolean(collider.catOnly) && (collider.minY ?? 0) <= 0.018;
}

export function pointSupported(point, collider, elevation) {
  return collider.active !== false && !isMousePassThroughCollider(collider) && Math.abs(collider.maxY - elevation) < 0.035 &&
    point.x >= collider.minX && point.x <= collider.maxX && point.z >= collider.minZ && point.z <= collider.maxZ;
}

// Build climbable fabric/cords on actual furniture, never on arbitrary walls.
// Endpoints are tested against the live house and routes do not pierce geometry.
export function buildTraversal(engine, I) {
  const routes = [], root = engine.world.root;
  const group = new I.Group(); group.name = "mouse-traversal-fabric"; root.add(group);
  const cloth = new I.MeshStandardMaterial({ color: 0x8f6955, roughness: 0.98 });
  const cord = new I.MeshStandardMaterial({ color: 0x4b4338, roughness: 0.85 });
  for (const [name, kind] of [["coffee-table-top", "fabric"], ["window-bench-base", "fabric"], ["tv-console", "cord"], ["study-desk", "cord"]]) {
    const box = engine.world.colliders.find(c => c.name === name);
    if (!box || box.maxY < 0.15 || box.maxY > 1.1) continue;
    for (const side of [1, -1]) {
      const x = side > 0 ? box.maxX : box.minX, z = (box.minZ + box.maxZ) / 2;
      const start = new I.Vector3(x + side * 0.16, 0.025, z);
      if (!isFoodPositionClear(start, engine.world.colliders.filter(c => !c.catOnly), 0.06)) continue;
      const top = new I.Vector3(x - side * 0.12, box.maxY, z);
      const mesh = new I.Mesh(new I.BoxGeometry(kind === "cord" ? 0.024 : 0.038, box.maxY, kind === "cord" ? 0.024 : 0.26), kind === "cord" ? cord : cloth);
      mesh.name = `climb-${kind}-${name}`; mesh.position.set(x + side * 0.045, box.maxY / 2, z);
      mesh.castShadow = false; mesh.updateMatrix(); mesh.matrixAutoUpdate = false; group.add(mesh);
      routes.push({ id: name, kind, start, top, collider: box, direction: -side });
      break;
    }
  }
  return { routes, group, climb: null, nextEntry: 0, landingUntil: 0, elevation: 0, groundEye: 0.066,
    colliders: [], candidates: [], solidCopies: new WeakMap(), scratch: new I.Vector3(), previous: new I.Vector3(), motion: "scurry" };
}

export function moveTraversingPlayer(engine, state, I, motion) {
  const t = state.traversal, p = engine.playerPosition;
  if (!t || t.climb) return;
  const floor = t.elevation;
  // Horizontal collision at the mouse's current height. Overhead furniture
  // leaves a real scurry space; walls still block at every elevation.
  t.colliders.length = 0;
  const candidates = engine.__expansion?.spatial?.colliders?.queryAabb(
    p.x - 0.35, p.z - 0.35, p.x + 0.35, p.z + 0.35, t.candidates) ?? engine.world.colliders;
  for (const c of candidates) {
    if (c.active === false || isMousePassThroughCollider(c) || c.catOnly && floor < (c.minY ?? 0) - 0.07) continue;
    if (c.maxY <= floor + 0.018 || (c.minY ?? 0) > floor + 0.11) continue;
    if (c.catOnly) {
      let solid = t.solidCopies.get(c); if (!solid) { solid = { ...c, catOnly: false }; t.solidCopies.set(c, solid); }
      solid.active = c.active; t.colliders.push(solid);
    } else t.colliders.push(c);
  }
  t.previous.copy(p);
  I.moveActor(p, motion, 0.055, t.colliders, "mouse");
  const travelled = distance(p, t.previous), attempted = motion.length();
  // Scramble onto low solid objects by pressing toward them.
  if (attempted > 0.001 && travelled < attempted * 0.35 && foodLoad(engine.carriedFood).bulk < 0.2) {
    t.scratch.copy(t.previous).addScaledVector(motion, Math.max(1, 0.15 / attempted));
    const step = t.colliders.find(c => c.maxY > floor && c.maxY <= floor + 0.13 &&
      t.scratch.x > c.minX && t.scratch.x < c.maxX && t.scratch.z > c.minZ && t.scratch.z < c.maxZ);
    if (step && isFoodPositionClear(t.scratch, engine.world.colliders.filter(c => c !== step && !isMousePassThroughCollider(c) && c.maxY > step.maxY && (c.minY ?? 0) < step.maxY + 0.12), 0.055)) {
      p.copy(t.scratch); t.elevation = step.maxY; t.motion = "scramble"; t.landingUntil = engine.time + 0.25;
    }
  }
  p.y = 0.025;
}

export function startContextClimb(engine, state) {
  const t = state.traversal;
  if (t.climb || t.elevation > 0.01 || engine.time < t.nextEntry || engine.playerVelocity.length() < 0.1) return;
  for (const route of t.routes) {
    if (distance(engine.playerPosition, route.start) > 0.23 || engine.playerVelocity.x * route.direction < 0.09) continue;
    if (foodLoad(engine.carriedFood).bulk > (route.kind === "cord" ? 0.09 : 0.15)) {
      if (engine.time >= (state.nextLoadHint ?? 0)) { engine.showMessage?.("Too bulky to climb. Drop food with F / the drop button.", 1.8); state.nextLoadHint = engine.time + 4; }
      return;
    }
    t.climb = { route, from: engine.playerPosition.clone(), started: engine.time, duration: 0.8 + route.top.y * 0.8 };
    t.motion = route.kind === "cord" ? "climb" : "scramble";
    engine.emitNoise(engine.playerPosition, 0.22);
    engine.observePlayerHabit?.(`furniture:${route.id}`, route.start);
    return;
  }
}

export function advanceTraversal(engine, state, delta) {
  const t = state.traversal;
  if (t.climb) {
    const climb = t.climb, progress = clamp((engine.time - climb.started) / climb.duration, 0, 1);
    // Climbing is interruptible; stepping back drops the mouse at the approach.
    if (engine.playerVelocity.x * climb.route.direction < -0.2) {
      engine.playerPosition.copy(climb.from); t.climb = null; t.nextEntry = engine.time + 0.6; t.motion = "drop";
    } else {
      const horizontal = Math.max(0, (progress - 0.7) / 0.3);
      engine.playerPosition.lerpVectors(climb.from, climb.route.top, horizontal).setY(0.025);
      t.elevation = climb.route.top.y * Math.min(1, progress / 0.8);
      engine.playerVelocity.set(0, 0, 0);
      if (progress >= 1) { t.climb = null; t.nextEntry = engine.time + 0.7; t.motion = "land"; t.landingUntil = engine.time + 0.25; }
    }
  } else if (t.elevation > 0 && engine.onGround !== false) {
    if (!engine.world.colliders.some(c => pointSupported(engine.playerPosition, c, t.elevation))) {
      t.elevation = Math.max(0, t.elevation - delta * 1.9); t.motion = "drop";
      if (!t.elevation) { t.landingUntil = engine.time + 0.3; engine.emitNoise(engine.playerPosition, 0.38); t.motion = "land"; }
    }
  } else if (engine.time >= t.landingUntil) t.motion = "scurry";
  engine.playerElevation = t.elevation;
}
