import "./hearthmouse-backroom-decor.mjs";

export const LEGACY_DOORWAY_BASEBOARDS = Object.freeze([
  "south-baseboard",
  "north-baseboard-east",
]);

export const DOORWAY_FLOORBOARD_CLEARANCE = 0.72;
export const MAX_FLOORBOARD_TOP = 0.18;
export const MAX_FLOORBOARD_HEIGHT = 0.2;

function removeFromArray(items, item) {
  if (!Array.isArray(items) || !item) return;
  let index = items.indexOf(item);
  while (index >= 0) {
    items.splice(index, 1);
    index = items.indexOf(item);
  }
}

function meshesNamed(world, name) {
  const matches = [];
  const root = world?.root;
  if (!root) return matches;
  if (typeof root.traverse === "function") {
    root.traverse((object) => {
      if (object?.name === name) matches.push(object);
    });
    return matches;
  }
  const single = root.getObjectByName?.(name);
  if (single) matches.push(single);
  return matches;
}

function hideNamedWorldObject(world, name) {
  let hidden = 0;
  for (const mesh of meshesNamed(world, name)) {
    if (mesh.visible !== false) hidden++;
    mesh.visible = false;
    removeFromArray(world.occluders, mesh);
  }
  return hidden;
}

function disableNamedCollider(world, name) {
  let disabled = 0;
  for (const collider of world?.colliders ?? []) {
    if (collider?.name !== name) continue;
    if (collider.active !== false) disabled++;
    collider.active = false;
  }
  hideNamedWorldObject(world, name);
  return disabled;
}

export function colliderNearDoorway(collider, doorway, radius = DOORWAY_FLOORBOARD_CLEARANCE) {
  if (!collider || !doorway) return false;
  const minX = Number(collider.minX);
  const maxX = Number(collider.maxX);
  const minZ = Number(collider.minZ);
  const maxZ = Number(collider.maxZ);
  const x = Number(doorway.x);
  const z = Number(doorway.z);
  if (![minX, maxX, minZ, maxZ, x, z].every(Number.isFinite)) return false;
  const nearestX = Math.max(minX, Math.min(maxX, x));
  const nearestZ = Math.max(minZ, Math.min(maxZ, z));
  return Math.hypot(nearestX - x, nearestZ - z) <= radius;
}

export function isLowFloorboardCollider(collider) {
  if (!collider || collider.catOnly) return false;
  const minY = Number(collider.minY);
  const maxY = Number(collider.maxY);
  if (![minY, maxY].every(Number.isFinite)) return false;
  return maxY <= MAX_FLOORBOARD_TOP && maxY - minY <= MAX_FLOORBOARD_HEIGHT;
}

function clearLowDoorwayCollider(world, collider) {
  if (!collider) return false;
  const wasActive = collider.active !== false;
  collider.active = false;
  if (collider.name) hideNamedWorldObject(world, collider.name);
  return wasActive;
}

export function clearExpandedDoorwayFloorboards(engine) {
  const world = engine?.world;
  const expansion = engine?.__expansion;
  const edges = expansion?.navEdges;

  // Wait until the expansion has finished building all room geometry. The old
  // version could run against the base house too early, then miss a later board.
  if (!world?.root || !Array.isArray(edges) || edges.length === 0) return false;

  for (const name of LEGACY_DOORWAY_BASEBOARDS) disableNamedCollider(world, name);

  const doorwayPoints = edges.map((edge) => edge?.point).filter(Boolean);
  for (const collider of world.colliders ?? []) {
    if (collider?.active === false || !isLowFloorboardCollider(collider)) continue;
    if (!doorwayPoints.some((point) => colliderNearDoorway(collider, point))) continue;
    clearLowDoorwayCollider(world, collider);
  }

  if (!world.__doorwayFloorboardSetNightWrapped && typeof world.setNight === "function") {
    const baseSetNight = world.setNight.bind(world);
    Object.defineProperty(world, "__doorwayFloorboardSetNightWrapped", {
      value: true,
      configurable: true,
    });
    world.setNight = (night) => {
      const result = baseSetNight(night);
      clearExpandedDoorwayFloorboards(engine);
      return result;
    };
  }

  Object.defineProperty(world, "__doorwayBaseboardsRepaired", {
    value: true,
    configurable: true,
  });
  return true;
}

// Backward-compatible export name used by the first version of the hotfix.
export const repairLegacyDoorwayBaseboards = clearExpandedDoorwayFloorboards;

function installWhenReady() {
  if (typeof window === "undefined") return;
  if (!clearExpandedDoorwayFloorboards(window.hearthmouseEngine)) {
    window.setTimeout(installWhenReady, 16);
  }
}

if (typeof window !== "undefined") installWhenReady();
