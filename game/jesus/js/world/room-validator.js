import { PHYSICS } from "../config.js";

function horizontalGap(a, b) {
  if (a.x + a.w < b.x) return b.x - (a.x + a.w);
  if (b.x + b.w < a.x) return a.x - (b.x + b.w);
  return 0;
}

export function canReachSurface(from, to, physics = PHYSICS) {
  if (from === to) return true;
  const upwardRise = from.y - to.y;
  const downwardDrop = Math.max(0, to.y - from.y);
  const gap = horizontalGap(from, to);
  const maxRise = (physics.jumpVelocity ** 2) / (2 * physics.gravity) - 18;
  if (upwardRise > maxRise) return false;
  if (downwardDrop > 330) return false;

  const risePenalty = upwardRise > 0 ? upwardRise * 0.7 : 0;
  const dropBonus = Math.min(115, downwardDrop * 0.4);
  const maxGap = 225 - risePenalty + dropBonus;
  const landingWidth = Math.min(to.w, to.kind === "moving" ? to.w + to.range * 0.4 : to.w);
  return gap <= maxGap && landingWidth >= physics.requiredLandingWidth;
}

function asSurface(platform) {
  if (platform.kind !== "moving") return platform;
  return {
    ...platform,
    x: platform.axis === "x" ? platform.x - platform.range : platform.x,
    y: platform.axis === "y" ? platform.y - platform.range : platform.y,
    w: platform.axis === "x" ? platform.w + platform.range * 2 : platform.w,
  };
}

export function validateRoom(room, physics = PHYSICS) {
  const surfaces = [...room.platforms, ...(room.movers || [])].map(asSurface);
  const startIndices = surfaces.map((surface, index) => surface.entry ? index : -1).filter((index) => index >= 0);
  const exitIndices = surfaces.map((surface, index) => surface.exit ? index : -1).filter((index) => index >= 0);
  const starts = startIndices.length ? startIndices : [0];
  const exits = new Set(exitIndices.length ? exitIndices : [room.platforms.length - 1]);
  const queue = [...starts];
  const visited = new Set(queue);
  const edges = [];

  while (queue.length) {
    const fromIndex = queue.shift();
    for (let toIndex = 0; toIndex < surfaces.length; toIndex++) {
      if (fromIndex === toIndex || !canReachSurface(surfaces[fromIndex], surfaces[toIndex], physics)) continue;
      edges.push([fromIndex, toIndex]);
      if (!visited.has(toIndex)) {
        visited.add(toIndex);
        queue.push(toIndex);
      }
    }
  }

  const reachableExit = [...exits].some((index) => visited.has(index));
  return {
    valid: reachableExit,
    surfaceCount: surfaces.length,
    visitedCount: visited.size,
    edges,
    reason: reachableExit ? "reachable" : "no physical entrance-to-exit path",
  };
}

