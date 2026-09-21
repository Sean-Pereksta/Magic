// Pure, bounded helpers shared by the runtime and its headless regressions.
export const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
export const distance = (a, b) => Math.hypot(a.x - b.x, a.z - b.z);

const load = (label, speed, acceleration, turn, jump, noise, bulk) => Object.freeze({ label, speed, acceleration, turn, jump, noise, bulk });
export const FOOD_LOADS = Object.freeze({
  empty: load("Unburdened", 1, 1, 1, 1, 1, 0),
  small: load("Light", 0.97, 0.94, 0.97, 0.96, 1.05, 0.06),
  medium: load("Bulky", 0.82, 0.73, 0.76, 0.78, 1.28, 0.13),
  large: load("Heavy", 0.63, 0.51, 0.55, 0.56, 1.65, 0.22),
});
export function foodLoad(food) {
  if (!food) return FOOD_LOADS.empty;
  if (food.loadClass && FOOD_LOADS[food.loadClass]) return FOOD_LOADS[food.loadClass];
  const value = Number(food.value) || 1;
  return value >= 4 || food.prizeId ? FOOD_LOADS.large : value >= 2 ? FOOD_LOADS.medium : FOOD_LOADS.small;
}

export function canReuseActorPath(actor, target, context, clear) {
  const memo = actor.__routeMemo;
  if (!memo || memo.path !== actor.path || memo.revision !== context.revision || memo.intent !== context.intent || memo.playerRoom !== context.playerRoom ||
      memo.targetRoom !== context.targetRoom || memo.tier !== context.tier || context.time < memo.time) return false;
  const tolerance = context.chasing ? 0.4 : 0.7;
  if (distance(memo, target) > tolerance || (actor.stuckTimer ?? 0) > 0.32) return false;
  const position = actor.rig.root.position;
  if (distance(position, memo.origin) > 3 && !actor.path?.length) return false;
  const waypoint = actor.path?.[actor.pathIndex];
  if (!waypoint) return context.time - memo.time < (memo.reachable === false ? 0.9 : 0.18);
  if (context.time >= memo.nextCheck) {
    memo.nextCheck = context.time + (context.chasing ? 0.18 : 0.45);
    if (!clear(position, waypoint)) return false;
  }
  return true;
}
export function rememberActorPath(actor, target, context) {
  actor.__routeMemo = { ...context, path: actor.path, x: target.x, z: target.z,
    origin: { x: actor.rig.root.position.x, z: actor.rig.root.position.z },
    reachable: actor.pathReachable, nextCheck: context.time + 0.2 };
}

// Tokens count actual rays. Reservations protect urgent cats even when colony
// threat checks run before the cat loop. Deferred cats rotate within a priority.
export class SightBudget {
  constructor() { this.grants = new Map(); this.order = []; this.lastGrant = new WeakMap(); this.used = 0; this.limit = 0; }
  begin(cats, frame, limit, priority, cost) {
    this.used = 0; this.limit = limit; this.grants.clear(); this.order.length = 0;
    for (const cat of cats) if (cost(cat) > 0) this.order.push(cat);
    this.order.sort((a, b) => priority(a) - priority(b) || (this.lastGrant.get(a) ?? -1) - (this.lastGrant.get(b) ?? -1));
    let remaining = limit;
    for (const cat of this.order) {
      const rays = cost(cat);
      if (remaining < rays) continue;
      this.grants.set(cat, rays); this.lastGrant.set(cat, frame); remaining -= rays;
    }
    this.spare = remaining;
  }
  take(cat, rays) {
    const grant = this.grants.get(cat) ?? 0;
    if (this.used + rays > this.limit || grant + this.spare < rays) return false;
    this.grants.set(cat, Math.max(0, grant - rays));
    this.spare -= Math.max(0, rays - grant); this.used += rays;
    return true;
  }
}

// Only distinct witnessed visits count. Dwelling in view for a minute is one
// visit, not hundreds of lessons. Entries decay, and each cat learns privately.
export class HabitMemory {
  constructor(rate = 1) { this.entries = new Map(); this.rate = rate; }
  observe(key, position, now, witnessed = false) {
    if (!witnessed || !position || !key) return 0;
    const old = this.entries.get(key);
    if (old && now - old.last < 9) return this.score(key, now);
    const score = Math.min(6, this.score(key, now) + this.rate);
    if (this.entries.size >= 40 && !old) this.entries.delete(this.entries.keys().next().value);
    this.entries.set(key, { x: position.x, z: position.z, score, last: now });
    return score;
  }
  score(key, now) { const e = this.entries.get(key); return e ? e.score * Math.exp(-Math.max(0, now - e.last) / 150) : 0; }
  best(now, filter = () => true) {
    let best = null, bestScore = 2.1;
    for (const [key, entry] of this.entries) {
      const score = this.score(key, now);
      if (score > bestScore && filter(key, entry)) { best = { ...entry, key, score }; bestScore = score; }
    }
    return best;
  }
}

export function darknessVisibility(light, range) {
  if (range <= 0.85) return 1;
  const illumination = clamp(light, 0.08, 1);
  const identificationRange = 2.2 + illumination * 7.8;
  if (range > identificationRange) return 0;
  return clamp(0.2 + illumination * 0.8, 0.26, 1);
}
export function sweptPawHit(from, to, target, radius = 0.12) {
  const dx = to.x - from.x, dz = to.z - from.z;
  const t = clamp(((target.x - from.x) * dx + (target.z - from.z) * dz) / Math.max(1e-8, dx * dx + dz * dz), 0, 1);
  const y = (from.y ?? 0.08) + ((to.y ?? 0.08) - (from.y ?? 0.08)) * t;
  return Math.hypot(target.x - from.x - dx * t, target.z - from.z - dz * t) <= radius && Math.abs((target.y ?? 0.06) - y) <= 0.17;
}
