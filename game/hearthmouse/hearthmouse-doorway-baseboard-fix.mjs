export const LEGACY_DOORWAY_BASEBOARDS = Object.freeze([
  Object.freeze({
    name: "south-baseboard",
    minX: -10.35,
    maxX: 10.35,
    openings: Object.freeze([
      Object.freeze({ center: 3.6, width: 1.56 }),
    ]),
  }),
  Object.freeze({
    name: "north-baseboard-east",
    minX: -6.8,
    maxX: 10.33,
    openings: Object.freeze([
      Object.freeze({ center: -1.3, width: 1.56 }),
      Object.freeze({ center: 4.5, width: 1.41 }),
    ]),
  }),
]);

export function horizontalSegmentsAroundOpenings(minX, maxX, openings = []) {
  const start = Math.min(minX, maxX);
  const end = Math.max(minX, maxX);
  const cuts = openings
    .map(({ center, width }) => {
      const half = Math.max(0, Number(width) || 0) / 2;
      return [
        Math.max(start, (Number(center) || 0) - half),
        Math.min(end, (Number(center) || 0) + half),
      ];
    })
    .filter(([cutStart, cutEnd]) => cutEnd > cutStart)
    .sort((a, b) => a[0] - b[0]);

  const merged = [];
  for (const cut of cuts) {
    const previous = merged[merged.length - 1];
    if (!previous || cut[0] > previous[1]) merged.push([...cut]);
    else previous[1] = Math.max(previous[1], cut[1]);
  }

  const segments = [];
  let cursor = start;
  for (const [cutStart, cutEnd] of merged) {
    if (cutStart > cursor) segments.push({ minX: cursor, maxX: cutStart });
    cursor = Math.max(cursor, cutEnd);
  }
  if (cursor < end) segments.push({ minX: cursor, maxX: end });
  return segments;
}

function removeFromArray(items, item) {
  if (!Array.isArray(items) || !item) return;
  const index = items.indexOf(item);
  if (index >= 0) items.splice(index, 1);
}

function disableLegacyBaseboard(world, source, name) {
  source.visible = false;
  removeFromArray(world.occluders, source);
  for (const collider of world.colliders ?? []) {
    if (collider?.name === name) collider.active = false;
  }
}

function addVisualTrimSegments(world, I, source, spec) {
  const height = source.geometry?.parameters?.height ?? 0.14;
  const depth = source.geometry?.parameters?.depth ?? 0.1;
  const y = Number.isFinite(source.position?.y) ? source.position.y : 0.07;
  const z = Number.isFinite(source.position?.z) ? source.position.z : 0;
  const segments = horizontalSegmentsAroundOpenings(spec.minX, spec.maxX, spec.openings);

  segments.forEach((segment, index) => {
    const width = segment.maxX - segment.minX;
    if (!(width > 0.001)) return;
    const mesh = new I.Mesh(new I.BoxGeometry(width, height, depth), source.material);
    mesh.name = `doorway-safe-${spec.name}-${index}`;
    mesh.position.set((segment.minX + segment.maxX) / 2, y, z);
    if (mesh.rotation?.copy && source.rotation) mesh.rotation.copy(source.rotation);
    mesh.castShadow = source.castShadow ?? false;
    mesh.receiveShadow = source.receiveShadow ?? true;
    mesh.userData.__doorwaySafeTrim = true;
    world.root.add(mesh);
  });
}

export function repairLegacyDoorwayBaseboards(
  engine,
  I = globalThis.window?.HearthmouseInternals,
) {
  const world = engine?.world;
  if (!world?.root?.getObjectByName || !I?.Mesh || !I?.BoxGeometry) return false;
  if (world.__doorwayBaseboardsRepaired) return true;

  let repaired = 0;
  for (const spec of LEGACY_DOORWAY_BASEBOARDS) {
    const source = world.root.getObjectByName(spec.name);
    if (!source) continue;
    disableLegacyBaseboard(world, source, spec.name);
    addVisualTrimSegments(world, I, source, spec);
    repaired++;
  }

  if (!repaired) return false;
  Object.defineProperty(world, "__doorwayBaseboardsRepaired", {
    value: true,
    configurable: true,
  });
  return true;
}

function installWhenReady() {
  if (typeof window === "undefined") return;
  const engine = window.hearthmouseEngine;
  const I = window.HearthmouseInternals;
  if (!repairLegacyDoorwayBaseboards(engine, I)) {
    window.setTimeout(installWhenReady, 16);
  }
}

if (typeof window !== "undefined") installWhenReady();
