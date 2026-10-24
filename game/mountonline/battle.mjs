import {
  UNITS,
  UNIT_KEYS,
  ORDERS,
  FORMATIONS,
  FACTIONS,
  STEP,
  hash,
  clamp,
  armySize,
} from "./data.mjs";
import { Navigator, SpatialHash } from "./navigation.mjs";
import { requireThat } from "./progression.mjs";
const internals = new WeakMap();
const distance = (a, b) => Math.hypot(a.x - b.x, a.y - b.y);
const mounted = (t) => t.role === "cavalry" || t.role === "horseArcher";
const missile = (t) => t.range > 3 && t.role !== "healer";
const guarded = (u) => ["hold", "defend", "brace", "regroup"].includes(u.order.kind);
const active = (u) => u && u.hp > 0 && !u.routed;
const shieldMax = (t) => t.shield === false ? 0 : typeof t.shield === "number" ? t.shield * (t.shield <= 3 ? 100 : 1) : t.role === "shield" ? 100 : 0;
const direction = (team) => team === 0 ? 1 : -1;
const angleDifference = (a, b) => Math.atan2(Math.sin(a - b), Math.cos(a - b));
function face(u, point, dt = 1) {
  const wanted = Math.atan2(point.y - u.y, point.x - u.x);
  u.heading += clamp(angleDifference(wanted, u.heading), -4 * dt, 4 * dt);
  u.facing = Math.cos(u.heading) < 0 ? -1 : 1;
}
function frontal(u, source) {
  if (!Number.isFinite(source?.x + source?.y)) return false;
  return Math.cos(Math.atan2(source.y - u.y, source.x - u.x) - u.heading) > 0.42;
}
function initializeUnit(u, b) {
  const t = UNITS[u.key];
  u.heading ??= u.facing < 0 ? Math.PI : 0;
  u.cohesion ??= 0.7;
  u.stamina ??= 100;
  u.morale ??= b.morale[u.team];
  u.shield ??= shieldMax(t);
  u.formation ??= b.formation[u.team];
  u.ammo ??= missile(t) ? (t.ammo ?? (t.role === "mage" ? 20 : 32)) : -1;
  u.braceTime ??= 0;
  u.chargeRecovery ??= 0;
  u.regroupUntil ??= 0;
  u.suppressed ??= 0;
}
function runtime(b) {
  let rt = internals.get(b);
  if (!rt) {
    const map = makeMap(b.seed, b.siege, b.terrain);
    if (b.gate?.hp <= 0) {
      map.costs[20 * 64 + 42] = 1;
      map.costs[19 * 64 + 42] = 1;
    }
    rt = {
      map,
      nav: new Navigator(map),
      spatial: new SpatialHash(3),
      byId: new Map(b.units.map((u) => [u.id, u])),
      pathBudget: 12,
      claims: new Map(),
      regionVersion: -1,
      regions: null,
      initialCounts: [b.units.filter((u) => u.team === 0).length, b.units.filter((u) => u.team === 1).length],
    };
    for (const u of b.units) initializeUnit(u, b);
    internals.set(b, rt);
  }
  return rt;
}
function updateRegions(rt) {
  if (rt.regionVersion === rt.map.version) return;
  const { width: w, height: h, costs } = rt.map;
  const regions = new Int16Array(w * h), queue = new Int32Array(w * h);
  let region = 0;
  for (let i = 0; i < regions.length; i++) {
    if (!costs[i] || regions[i]) continue;
    let head = 0, tail = 1;
    queue[0] = i;
    regions[i] = ++region;
    while (head < tail) {
      const p = queue[head++], x = p % w, y = Math.floor(p / w);
      for (const q of [x > 0 ? p - 1 : -1, x + 1 < w ? p + 1 : -1, y > 0 ? p - w : -1, y + 1 < h ? p + w : -1]) {
        if (q < 0 || !costs[q] || regions[q]) continue;
        regions[q] = region;
        queue[tail++] = q;
      }
    }
  }
  rt.regions = regions;
  rt.regionVersion = rt.map.version;
}
function reachable(rt, a, b) {
  updateRegions(rt);
  const ai = clamp(Math.floor(a.y), 0, 39) * 64 + clamp(Math.floor(a.x), 0, 63),
    bi = clamp(Math.floor(b.y), 0, 39) * 64 + clamp(Math.floor(b.x), 0, 63);
  return rt.regions[ai] > 0 && rt.regions[ai] === rt.regions[bi];
}
export function makeMap(seed = 1, siege = false, terrain = "plains") {
  const width = 64,
    height = 40,
    tiles = new Uint8Array(width * height),
    costs = new Float32Array(width * height),
    elevation = new Uint8Array(width * height);
  costs.fill(1);
  for (let y = 2; y < height - 2; y++)
    for (let x = 14; x < width - 12; x++) {
      const n = hash(x, y, seed) % 100;
      elevation[y * width + x] = hash(Math.floor(x / 7), Math.floor(y / 6), seed) % 3;
      if (n < 5 && Math.abs(y - 20) > 4) {
        tiles[y * width + x] = 1;
        costs[y * width + x] = 1.35;
      }
      if (n < 2 && Math.abs(y - 20) > 6) {
        tiles[y * width + x] = 2;
        costs[y * width + x] = 0;
      }
    }
  if (siege)
    for (let y = 0; y < height; y++) {
      tiles[y * width + 42] = 2;
      costs[y * width + 42] = 0;
    }
  return { width, height, tiles, costs, elevation, terrain, version: 1 };
}
export function formationSlots(
  count,
  shape = "line",
  center = { x: 10, y: 20 },
  direction = 1,
) {
  const columns =
    shape === "square"
      ? Math.ceil(Math.sqrt(count))
      : shape === "shieldwall"
        ? Math.min(50, Math.max(4, Math.ceil(count / 3)))
        : shape === "loose"
          ? Math.ceil(Math.sqrt(count * 2))
          : Math.max(5, Math.ceil(Math.sqrt(count) * 1.8));
  const spacing =
      shape === "loose" ? 1.45 : shape === "shieldwall" ? 0.68 : 0.88,
    slots = [];
  for (let i = 0; i < count; i++) {
    let row = Math.floor(i / columns),
      col = i % columns,
      x,
      y;
    if (shape === "wedge") {
      row = Math.floor(Math.sqrt(i));
      col = i - row * row;
      x = center.x - row * 0.85 * direction;
      y = center.y + (col - row) * 0.85;
    } else {
      x = center.x - row * spacing * direction;
      y =
        center.y +
        (col - (Math.min(columns, count - row * columns) - 1) / 2) * spacing;
    }
    slots.push({ x, y });
  }
  // Fit the formation as a whole. Per-soldier clamping piles large armies on
  // top of one another at map edges and destroys shield-wall spacing.
  if (slots.length) {
    const minX = Math.min(...slots.map((s) => s.x)),
      maxX = Math.max(...slots.map((s) => s.x)),
      minY = Math.min(...slots.map((s) => s.y)),
      maxY = Math.max(...slots.map((s) => s.y)),
      sx = Math.min(1, 61.4 / Math.max(1, maxX - minX)),
      sy = Math.min(1, 37.4 / Math.max(1, maxY - minY)),
      startX = clamp(minX, 1.3, 62.7 - (maxX - minX) * sx),
      startY = clamp(minY, 1.3, 38.7 - (maxY - minY) * sy);
    for (const s of slots) {
      s.x = startX + (s.x - minX) * sx;
      s.y = startY + (s.y - minY) * sy;
    }
  }
  return slots;
}
export function createBattle({
  armyA,
  armyB,
  seed = 1,
  siege = false,
  morale = [100, 100],
  banners = [[], []],
  names = ["Your company", "Enemy company"],
  id = "local",
  tactics = [0, 0],
  factions = ["crown", "ember"],
  commanders = [false, false],
  terrain = "plains",
  encounterType = "field",
  objective = undefined,
}) {
  const b = {
    id,
    seed,
    siege,
    names,
    factions,
    terrain,
    encounterType: encounterType === "caravan" ? "caravanDefense" : encounterType,
    commanders: [0, 1].map((team) => ({
      enabled: !!commanders[team],
      doctrine: FACTIONS.find((f) => f.id === factions[team])?.doctrine || (team ? "aggressive" : "defensive"),
      nextPlan: 0.25 + team * 0.35,
      plans: 0,
      phase: "deploy",
      initial: armySize(team === 0 ? armyA : armyB),
      ...(typeof commanders[team] === "object" ? commanders[team] : {}),
    })),
    units: [],
    projectiles: [],
    effects: [],
    time: 0,
    tick: 0,
    winner: null,
    finished: false,
    morale: [...morale],
    banners,
    tactics,
    formation: ["line", "line"],
    abilities: [{}, {}],
    nextProjectile: 1,
    gate: siege
      ? {
          x: 42.5,
          y: 20,
          hp: 240 + armySize(armyB) * 3,
          max: 240 + armySize(armyB) * 3,
        }
      : null,
  };
  let idCounter = 0;
  for (let team = 0; team < 2; team++) {
    const army = team === 0 ? armyA : armyB,
      count = armySize(army);
    requireThat(count <= 600, "A battlefield supports 600 troops per army.");
    const slots = formationSlots(
      count,
      "line",
      { x: team === 0 ? 11 : 54, y: 20 },
      team === 0 ? 1 : -1,
    );
    let index = 0;
    // Frontline first; healers and missile troops deploy behind the shields.
    const sorted = [...UNIT_KEYS].sort(
      (a, c) =>
        (["ranged", "crossbow", "mage", "horseArcher", "healer"].includes(UNITS[a].role)
          ? 1
          : 0) -
        (["ranged", "crossbow", "mage", "horseArcher", "healer"].includes(UNITS[c].role)
          ? 1
          : 0),
    );
    for (const key of sorted)
      for (let i = 0; i < (army[key] || 0); i++) {
        const t = UNITS[key],
          pos = slots[index++];
        b.units.push({
          id: idCounter++,
          key,
          team,
          ...pos,
          px: pos.x,
          py: pos.y,
          hp: t.hp,
          maxHp: t.hp,
          state: "idle",
          facing: team === 0 ? 1 : -1,
          order: { kind: "auto", x: pos.x, y: pos.y },
          target: null,
          cooldown: (hash(i, team, seed) % 50) / 100,
          think: 0,
          anim: 0,
          charged: 0,
          stuck: 0,
          routed: false,
          deathAt: 0,
          formation: "line",
        });
      }
  }
  if (b.encounterType === "ambush") {
    const ambushers = b.units.filter((u) => u.team === 1), half = Math.ceil(ambushers.length / 2);
    const slots = [...formationSlots(half, "loose", { x: 29, y: 9 }, -1), ...formationSlots(ambushers.length - half, "loose", { x: 29, y: 31 }, -1)];
    ambushers.forEach((u, i) => { u.x = u.px = slots[i].x; u.y = u.py = slots[i].y; u.order.x = u.x; u.order.y = u.y; });
  }
  const rt = runtime(b);
  for (const u of b.units) if (!rt.nav.walkable(Math.floor(u.x), Math.floor(u.y))) {
    const ground = rt.nav.nearest(u.x, u.y);
    if (ground) { u.x = u.px = ground.x + 0.5; u.y = u.py = ground.y + 0.5; u.order.x = u.x; u.order.y = u.y; }
  }
  b.objective = makeObjective(b, objective);
  return b;
}
export function battleMap(b) {
  return runtime(b).map;
}
export function issueOrder(
  b,
  team,
  ids,
  kind,
  point = {},
  formation = b.formation[team],
) {
  requireThat(!b.finished, "This battle is over.");
  requireThat(ORDERS.includes(kind), "Unknown order.");
  requireThat(FORMATIONS.includes(formation), "Unknown formation.");
  requireThat(
    Array.isArray(ids) && ids.length > 0 && ids.length <= 600,
    "Select troops first.",
  );
  const rt = runtime(b), selectedIds = new Set(ids),
    selected = [...selectedIds].map((id) => rt.byId.get(id));
  requireThat(
    selected.every((u) => u && u.team === team && u.hp > 0 && !u.routed),
    "You can only command your own active troops.",
  );
  const center = {
    x: selected.reduce((n, u) => n + u.x, 0) / selected.length,
    y: selected.reduce((n, u) => n + u.y, 0) / selected.length,
  };
  // This order is deliberately immediate: no map point or second click is required.
  if (kind === "attackNearest") {
    rt.spatial.rebuild(b.units);
    rt.claims.clear();
    for (const u of b.units) if (active(u) && u.team === team && !selectedIds.has(u.id) && u.target != null)
      rt.claims.set(u.target, (rt.claims.get(u.target) || 0) + 1);
    let assigned = 0;
    for (const u of selected) {
      u.order = { kind, x: u.x, y: u.y, anchorX: u.x, anchorY: u.y };
      const target = findTarget(b, u, rt, true);
      u.target = target?.id ?? null;
      u.order.target = u.target;
      u.path = null;
      u.think = target ? 0.25 + (u.id % 7) * 0.04 : 0;
      if (target) {
        assigned++;
        rt.claims.set(target.id, (rt.claims.get(target.id) || 0) + 1);
      }
    }
    if (!assigned) {
      b.notice = "No reachable enemy is available.";
      b.effects.push({ kind: "notice", x: center.x, y: center.y, time: b.time, team, text: b.notice });
    } else b.notice = null;
    b.effects.push({ kind: "order", x: center.x, y: center.y, time: b.time, order: kind, team });
    return assigned ? selected.length : 0;
  }
  let x = point.x ?? (kind === "fallback" ? clamp(center.x - direction(team) * 7, 1, 63) : center.x),
    y = point.y ?? center.y;
  requireThat(
    Number.isFinite(x) &&
      Number.isFinite(y) &&
      x >= 0 &&
      x <= 64 &&
      y >= 0 &&
      y <= 40,
    "Choose a destination on the battlefield.",
  );
  const target = point.target == null ? null : rt.byId.get(point.target);
  b.notice = null;
  if (point.target != null)
    requireThat(
      target && target.hp > 0 && !target.routed,
      "This target is no longer available.",
    );
  if (target && ["follow", "defend"].includes(kind))
    requireThat(
      target.team === team,
      "Choose an allied unit to protect or follow.",
    );
  if (target && ["attack", "focus"].includes(kind))
    requireThat(target.team !== team, "Choose an enemy to attack.");
  if (target) {
    x = target.x;
    y = target.y;
  }
  const anchor = rt.nav.nearest(x, y);
  requireThat(anchor, "No accessible ground near that destination.");
  const slots = formationSlots(
    selected.length,
    formation,
    { x: anchor.x + 0.5, y: anchor.y + 0.5 },
    team === 0 ? 1 : -1,
  );
  b.formation[team] = formation;
  for (let i = 0; i < selected.length; i++) {
    const u = selected[i],
      desired = slots[i],
      nearest = rt.nav.nearest(desired.x, desired.y) || anchor,
      slot = rt.nav.walkable(Math.floor(desired.x), Math.floor(desired.y))
        ? desired
        : { x: nearest.x + 0.5, y: nearest.y + 0.5 };
    u.order =
      kind === "hold" || (kind === "follow" && target?.id === u.id)
        ? { kind: "hold", x: u.x, y: u.y, anchorX: u.x, anchorY: u.y }
        : {
            kind,
            x: slot.x,
            y: slot.y,
            target: target?.id ?? null,
            anchorX: x,
            anchorY: y,
            fromX: u.x,
            fromY: u.y,
            leg: 0,
            side: point.side === "left" || point.side === -1 ? -1 : point.side === "right" || point.side === 1 ? 1 : (y < center.y ? -1 : 1),
            focusKey: kind === "focus" ? target?.key : null,
          };
    u.target = null;
    u.path = null;
    u.think = 0;
    u.charged = 0;
    u.formation = formation;
    u.braceTime = 0;
    u.state = kind === "hold" ? "defend" : "idle";
  }
  b.effects.push({ kind: "order", x, y, time: b.time, order: kind, team });
  return selected.length;
}
function findTarget(b, u, rt, entireField = false) {
  const t = UNITS[u.key];
  updateRegions(rt);
  const region = rt.regions[clamp(Math.floor(u.y), 0, 39) * 64 + clamp(Math.floor(u.x), 0, 63)];
  let best = null,
    score = Infinity;
  const consider = (candidate, d) => {
      if (!active(candidate) || candidate.id === u.id) return;
      if (t.role === "healer") {
        if (candidate.team !== u.team || candidate.hp >= candidate.maxHp)
          return;
        const s = (candidate.hp / candidate.maxHp) * 60 + d * 0.04;
        if (s < score) {
          score = s;
          best = candidate;
        }
        return;
      }
      if (candidate.team === u.team) return;
      if (!region || region !== rt.regions[clamp(Math.floor(candidate.y), 0, 39) * 64 + clamp(Math.floor(candidate.x), 0, 63)]) return;
      let s = d;
      if (u.order.kind === "attackNearest") s += (rt.claims.get(candidate.id) || 0) * 9;
      if (u.order.kind === "focus" && candidate.key === u.order.focusKey) s *= 0.35;
      if (u.order.kind === "patrol") {
        const o = u.order,
          dx = o.anchorX - o.fromX,
          dy = o.anchorY - o.fromY,
          f = clamp(
            ((candidate.x - o.fromX) * dx + (candidate.y - o.fromY) * dy) /
              (dx * dx + dy * dy || 1),
            0,
            1,
          );
        if (
          (candidate.x - o.fromX - dx * f) ** 2 +
            (candidate.y - o.fromY - dy * f) ** 2 >
          36
        )
          return;
      }
      if (
        t.role === "cavalry" &&
        ["ranged", "crossbow", "mage", "healer", "horseArcher"].includes(UNITS[candidate.key].role)
      )
        s *= 0.55;
      if (
        ["hold", "defend", "brace", "regroup"].includes(u.order.kind) &&
        distance(candidate, {
          x: u.order.anchorX ?? u.order.x,
          y: u.order.anchorY ?? u.order.y,
          }) > Math.max(t.range + 1, 4)
      )
        return;
      if (s < score) {
        score = s;
        best = candidate;
      }
    };
  if (entireField || u.order.kind === "attackNearest") {
    for (const v of b.units) consider(v, (v.x - u.x) ** 2 + (v.y - u.y) ** 2);
  } else rt.spatial.each(u.x, u.y, t.role === "healer" ? t.range : 18, consider);
  return best;
}
function damage(b, target, amount, source, kind = "melee") {
  if (!target || target.hp <= 0 || target.routed) return;
  const t = UNITS[target.key], armor = t.armor,
    formation = target.formation || b.formation[target.team],
    guard = guarded(target), front = frontal(target, source);
  let reduction = 0;
  if (front && target.shield > 0 && target.stamina > 5) {
    const condition = Math.min(1, target.shield / (shieldMax(t) || 100));
    reduction = (guard ? 0.42 : 0.2) * condition * (0.4 + target.stamina / 170);
    if (guard && ["shieldwall", "square"].includes(formation))
      reduction += 0.24 * target.cohesion;
    target.shield = Math.max(0, target.shield - amount * (kind === "magic" ? 0.35 : 0.2));
    target.stamina = Math.max(0, target.stamina - amount * 0.12);
    if (b.effects.length < 200) b.effects.push({ kind: "block", x: target.x, y: target.y, time: b.time, team: target.team });
  } else if (!front && source?.team !== target.team) {
    amount *= 1.17;
    target.morale = Math.max(0, target.morale - 3);
    target.braceTime = 0;
  }
  if (guard && b.banners[target.team]?.includes("bulwark")) reduction += 0.08;
  const armorScale = (kind === "crossbow" ? 0.35 : kind === "magic" ? 0 : 1) * (1 - (UNITS[source?.key]?.armorPierce || 0));
  const dealt = Math.max(1, (amount - armor * armorScale) * (1 - Math.min(0.78, reduction)));
  target.hp -= dealt;
  target.morale = Math.max(0, target.morale - dealt / target.maxHp * 11);
  target.suppressed = Math.max(target.suppressed, kind === "melee" ? 0.1 : 0.45);
  target.anim = 0.2;
  if (target.hp <= 0) {
    target.hp = 0;
    target.state = "dying";
    target.deathAt = b.time;
    b.morale[target.team] = Math.max(0, b.morale[target.team] -
      65 / Math.max(10, runtime(b).initialCounts[target.team]) - (t.officer ? 7 : 0));
    runtime(b).spatial.each(target.x, target.y, 4, (u) => {
      if (u.team === target.team) u.morale = Math.max(0, u.morale - (t.officer ? 12 : 3));
      else u.morale = Math.min(100, u.morale + 1.5);
    });
  }
  if (b.effects.length < 200)
    b.effects.push({
      kind: kind === "magic" ? "fire" : "impact",
      x: target.x,
      y: target.y,
      time: b.time,
      team: source?.team ?? 0,
    });
}
function shoot(b, u, target, amount, kind) {
  const t = UNITS[u.key];
  b.projectiles.push({
    id: b.nextProjectile++,
    x: u.x,
    y: u.y,
    fromX: u.x,
    fromY: u.y,
    target: target.id,
    tx: target.x,
    ty: target.y,
    team: u.team,
    source: u.id ?? null,
    damage: amount,
    kind,
    life: 0,
    splashRadius: t?.splashRadius ?? (kind === "magic" ? 1.6 : 0),
    splashFalloff: t?.splashFalloff ?? 0.65,
    friendlyFire: t?.friendlyFire === true ? 1 : clamp(Number(t?.friendlyFire) || 0, 0, 1),
  });
}
function groundSpeed(b, u, rt) {
  const t = UNITS[u.key], i = Math.floor(u.y) * 64 + Math.floor(u.x);
  let multiplier = 1 / Math.max(1, rt.map.costs[i]);
  if (["forest", "woods"].includes(b.terrain)) multiplier *= mounted(t) ? 0.62 : 0.9;
  if (["marsh", "swamp"].includes(b.terrain)) multiplier *= mounted(t) ? 0.5 : t.armor >= 7 ? 0.65 : 0.85;
  if (["mountain", "mountains", "snow"].includes(b.terrain)) multiplier *= mounted(t) ? 0.7 : 0.9;
  if (t.terrainAffinity === b.terrain) multiplier = Math.min(1.08, multiplier * 1.18);
  return multiplier;
}
function walk(b, u, target, dt, rt, speed) {
  let dest = target;
  const length = distance(u, target);
  if (length < 0.14) return false;
  // Replan only when blocked, with a strict shared budget; every soldier still moves each tick.
  if (u.path?.[u.pathIndex] && distance(u, u.path[u.pathIndex]) < 0.3)
    u.pathIndex++;
  if (u.path?.[u.pathIndex]) dest = u.path[u.pathIndex];
  const d = distance(u, dest);
  if (d < 0.01) return false;
  let dx = (dest.x - u.x) / d,
    dy = (dest.y - u.y) / d;
  let sx = 0,
    sy = 0;
  rt.spatial.each(u.x, u.y, 0.68, (v, dist) => {
    if (v.id === u.id || dist < 0.0001) return;
    const f = (0.68 - Math.sqrt(dist)) * (v.team === u.team ? 1.5 : 0.75);
    sx += ((u.x - v.x) / Math.sqrt(dist)) * f;
    sy += ((u.y - v.y) / Math.sqrt(dist)) * f;
  });
  dx += sx;
  dy += sy;
  const norm = Math.hypot(dx, dy) || 1,
    step = Math.min(d, speed * groundSpeed(b, u, rt) * (0.62 + u.stamina * 0.0038) * dt),
    x = u.x + (dx / norm) * step,
    y = u.y + (dy / norm) * step;
  if (
    rt.nav.walkable(Math.floor(x), Math.floor(y)) &&
    rt.nav.clearLine(u, { x, y })
  ) {
    u.x = clamp(x, 0.6, 63.4);
    u.y = clamp(y, 0.6, 39.4);
    const turning = Math.abs(angleDifference(Math.atan2(dest.y - u.y, dest.x - u.x), u.heading));
    face(u, dest, dt);
    u.state = "move";
    u.stuck = 0;
    u.charged = turning > 0.8 ? u.charged * 0.5 : Math.min(10, u.charged + step);
    u.stamina = Math.max(0, u.stamina - dt * (u.order.kind === "charge" ? 5 : mounted(UNITS[u.key]) ? 1.6 : 0.75) * (b.terrain === "desert" && UNITS[u.key].terrainAffinity !== "desert" ? 1.35 : 1) * (100 / (UNITS[u.key].stamina || 100)));
    u.braceTime = 0;
    return true;
  }
  u.stuck += dt;
  if (u.stuck > 0.15 && rt.pathBudget > 0) {
    rt.pathBudget--;
    u.path = rt.nav.path(u, target);
    u.pathIndex = 0;
    u.stuck = 0;
  }
  return false;
}
export function useAbility(b, team, name, point) {
  requireThat(!b.finished, "This battle has ended.");
  requireThat(
    ["rally", "volley", "firestorm"].includes(name),
    "Unknown ability.",
  );
  requireThat(
    Number.isFinite(point?.x) &&
      Number.isFinite(point?.y) &&
      point.x >= 0 &&
      point.x <= 64 &&
      point.y >= 0 &&
      point.y <= 40,
    "Choose a battlefield location.",
  );
  requireThat(
    (b.abilities[team][name] || 0) <= b.time,
    "This command is recharging.",
  );
  b.abilities[team][name] = b.time + (name === "rally" ? 30 : 40);
  const rt = runtime(b),
    radius = name === "rally" ? 7 : 4;
  let hits = 0;
  rt.spatial.rebuild(b.units);
  rt.spatial.each(point.x, point.y, radius, (u) => {
    if (name === "rally") {
      if (u.team === team) {
        u.hp = Math.min(u.maxHp, u.hp + u.maxHp * 0.18);
        u.morale = Math.min(100, u.morale + 25);
        u.stamina = Math.min(100, u.stamina + 20);
      }
    } else if (u.team !== team) {
      if (name === "volley")
        shoot(b, { x: point.x - 8, y: point.y - 12, team }, u, 18, "arrow");
      else damage(b, u, 24 + 3 * b.tactics[team], { team }, "magic");
      hits++;
    }
  });
  if (name === "rally") b.morale[team] = Math.min(100, b.morale[team] + 20);
  b.effects.push({ kind: name, x: point.x, y: point.y, time: b.time, team });
  return hits;
}
function makeObjective(b, input) {
  if (input === false || input === null) return null;
  const spec = input || {}, type = spec.type || spec.kind || b.encounterType;
  if (!["defensive", "caravanDefense", "caravan", "raid", "rearguard", "reinforcement"].includes(type)) return null;
  const team = spec.team === 1 ? 1 : 0;
  const count = b.units.filter((u) => u.team === team).length;
  const x = clamp(spec.zone?.x ?? spec.wagon?.x ?? spec.x ?? (type === "raid" ? 51 : team ? 52 : 12), 2, 62);
  const y = clamp(spec.zone?.y ?? spec.wagon?.y ?? spec.y ?? 20, 2, 38);
  const hp = clamp(spec.wagon?.hp ?? spec.hp ?? 160, 1, 10000);
  return {
    ...spec,
    type: type === "caravan" ? "caravanDefense" : type,
    team, x, y,
    radius: clamp(spec.zone?.radius ?? spec.radius ?? 5, 1, 15),
    duration: clamp(spec.duration ?? (type === "raid" || type === "caravanDefense" || type === "caravan" ? 100 : type === "rearguard" ? 60 : 45), 1, 290),
    delay: clamp(spec.delay ?? 12, 0, 120),
    required: clamp(Math.ceil(spec.required ?? spec.requiredEscapes ?? count * 0.5), 1, Math.max(1, count)),
    hp, maxHp: hp,
    targetX: clamp(spec.wagon?.goalX ?? spec.targetX ?? (team ? 12 : 52), 2, 62),
    targetY: clamp(spec.wagon?.goalY ?? spec.targetY ?? y, 2, 38),
    speed: clamp(spec.wagon?.speed ?? spec.speed ?? 0.85, 0.1, 4),
    elapsed: 0, heldFor: 0, progress: 0, contestedFor: 0, escaped: 0, completed: false,
    waves: (Array.isArray(spec.waves) ? spec.waves : []).slice(0, 6).map((w) => ({ ...w, at: clamp(Number(w.at) || 0, 0, 250), team: w.team === 1 ? 1 : 0, delivered: false })),
  };
}
function deliverWaves(b, rt) {
  const o = b.objective;
  for (const wave of o?.waves || []) {
    if (wave.delivered || b.time < wave.at) continue;
    wave.delivered = true;
    let room = 600 - b.units.filter((u) => u.team === wave.team).length;
    let id = b.units.reduce((n, u) => Math.max(n, u.id + 1), 0);
    const additions = [];
    for (const key of UNIT_KEYS) for (let i = 0, count = Math.min(room, Math.max(0, Math.floor(Number(wave.army?.[key]) || 0))); i < count; i++) {
      const t = UNITS[key];
      additions.push({ id: id++, key, team: wave.team, hp: t.hp, maxHp: t.hp, state: "idle", facing: direction(wave.team), order: { kind: "auto" }, target: null, cooldown: i % 4 * 0.1, think: 0, anim: 0, charged: 0, stuck: 0, routed: false, deathAt: 0, temporary: true });
      room--;
    }
    const slots = formationSlots(additions.length, "line", { x: clamp(wave.spawn?.x ?? (wave.team ? 59 : 5), 2, 62), y: clamp(wave.spawn?.y ?? 20, 2, 38) }, direction(wave.team));
    additions.forEach((u, i) => {
      const ground = rt.nav.nearest(slots[i].x, slots[i].y);
      u.x = u.px = ground ? ground.x + 0.5 : slots[i].x;
      u.y = u.py = ground ? ground.y + 0.5 : slots[i].y;
      u.order.x = u.x; u.order.y = u.y;
      initializeUnit(u, b);
      rt.byId.set(u.id, u);
      b.units.push(u);
    });
    rt.initialCounts[wave.team] += additions.length;
    b.effects.push({ kind: "reinforcement", x: wave.spawn?.x ?? (wave.team ? 59 : 5), y: wave.spawn?.y ?? 20, time: b.time, team: wave.team, text: "Reinforcements have arrived." });
  }
}
function finishObjective(b, winner, reason) {
  b.finished = true;
  b.winner = winner;
  b.objective.completed = winner === b.objective.team;
  b.objective.reason = reason;
}
function advanceObjective(b, rt, dt) {
  const o = b.objective;
  if (!o || b.finished) return;
  o.elapsed += dt;
  const near = [0, 0];
  rt.spatial.each(o.x, o.y, o.radius, (u) => { if (active(u)) near[u.team]++; });
  if (["defensive", "reinforcement"].includes(o.type)) {
    o.heldFor ??= 0;
    if (near[o.team] > 0 && near[o.team] >= near[1 - o.team]) o.heldFor += dt;
    o.contestedFor = near[1 - o.team] > near[o.team] ? o.contestedFor + dt : Math.max(0, o.contestedFor - dt * 0.5);
    o.progress = clamp(o.heldFor / o.duration, 0, 1);
    if (o.contestedFor >= 8) finishObjective(b, 1 - o.team, "The defended position was overrun.");
    else if (o.heldFor >= o.duration) finishObjective(b, o.team, o.type === "reinforcement" ? "The army held until relief arrived." : "The position held through the assault.");
  } else if (o.type === "raid") {
    o.hp = Math.max(0, o.hp - Math.min(6, Math.max(0, near[o.team] - near[1 - o.team] * 0.5)) * dt * 3);
    o.progress = 1 - o.hp / o.maxHp;
    if (!o.hp) finishObjective(b, o.team, "The enemy supplies were destroyed.");
    else if (o.elapsed >= o.duration) finishObjective(b, 1 - o.team, "The supply raid ran out of time.");
  } else if (o.type === "rearguard") {
    o.escaped = b.units.filter((u) => u.team === o.team && u.routed && u.hp > 0 && u.order.kind === "retreat" && !u.temporary && u.escapedAt >= o.delay).length;
    o.progress = clamp(o.escaped / o.required, 0, 1);
    if (o.escaped >= o.required) finishObjective(b, o.team, "The required survivors escaped under the rearguard.");
    else if (o.elapsed >= o.duration) finishObjective(b, 1 - o.team, "Too few troops escaped before the pursuit closed in.");
  } else if (o.type === "caravanDefense") {
    const threats = [0, 0];
    rt.spatial.each(o.x, o.y, 2.8, (u) => { if (active(u)) threats[u.team]++; });
    o.hp = Math.max(0, o.hp - Math.min(5, threats[1 - o.team]) * dt * 5);
    const goal = { x: o.targetX, y: o.targetY }, d = distance(o, goal);
    if (near[o.team] && !threats[1 - o.team] && d > 0.1) {
      if (o.path?.[o.pathIndex] && distance(o, o.path[o.pathIndex]) < 0.2) o.pathIndex++;
      let next = o.path?.[o.pathIndex] || goal;
      if (!rt.nav.clearLine(o, next)) { o.path = rt.nav.path(o, goal); o.pathIndex = 0; next = o.path[0] || o; }
      const length = distance(o, next), step = Math.min(length, o.speed * dt);
      o.x += (next.x - o.x) / (length || 1) * step;
      o.y += (next.y - o.y) / (length || 1) * step;
    }
    o.startDistance ??= Math.max(1, d);
    o.progress = clamp(1 - distance(o, goal) / o.startDistance, 0, 1);
    if (!o.hp) finishObjective(b, 1 - o.team, "The caravan was destroyed.");
    else if (distance(o, goal) < 0.5) finishObjective(b, o.team, "The caravan reached safety.");
    else if (o.elapsed >= o.duration) finishObjective(b, 1 - o.team, "The caravan did not reach safety in time.");
  }
}
function planCommanders(b, rt, centers) {
  for (let team = 0; team < 2; team++) {
    const c = b.commanders?.[team];
    if (!c?.enabled || c.nextPlan > b.time) continue;
    c.nextPlan = b.time + 2.4 + team * 0.35;
    c.plans++;
    const own = b.units.filter((u) => u.team === team && active(u));
    if (!own.length) continue;
    const visible = new Map(), visibility = ["forest", "marsh"].includes(b.terrain) ? 12 : 22;
    for (const u of own.filter((_, i) => i % Math.max(1, Math.floor(own.length / 12)) === 0))
      rt.spatial.each(u.x, u.y, visibility, (v) => { if (v.team !== team && rt.nav.clearLine(u, v)) visible.set(v.id, v); });
    const enemies = [...visible.values()];
    const enemy = enemies.length ? { x: enemies.reduce((n, u) => n + u.x, 0) / enemies.length, y: enemies.reduce((n, u) => n + u.y, 0) / enemies.length } : c.lastEnemy || { x: team === 0 ? 53 : 11, y: 20 };
    if (enemies.length) c.lastEnemy = enemy;
    const sign = direction(team), doctrine = c.doctrine,
      defensive = ["defensive", "mountain", "mercenary", "forest", "ambush"].includes(doctrine),
      mobile = ["steppe", "desert"].includes(doctrine),
      morale = own.reduce((n, u) => n + u.morale, 0) / own.length,
      cohesion = own.reduce((n, u) => n + u.cohesion, 0) / own.length,
      loss = 1 - own.length / Math.max(1, c.initial || rt.initialCounts[team]);
    const front = own.filter((u) => !missile(UNITS[u.key]) && !mounted(UNITS[u.key]) && UNITS[u.key].role !== "healer"),
      ranged = own.filter((u) => missile(UNITS[u.key]) && UNITS[u.key].role !== "horseArcher"),
      horse = own.filter((u) => UNITS[u.key].role === "horseArcher"),
      cavalry = own.filter((u) => UNITS[u.key].role === "cavalry"),
      support = own.filter((u) => UNITS[u.key].role === "healer");
    const order = (units, kind, point, shape = "line") => {
      if (units.length) issueOrder(b, team, units.map((u) => u.id), kind, { ...point, x: clamp(point.x, 2, 62), y: clamp(point.y, 2, 38) }, shape);
    };
    if (morale < 22 || loss > 0.68) {
      c.phase = "withdraw";
      order(own, "retreat", centers[team]);
      continue;
    }
    const close = enemies.length && distance(centers[team], enemy) < 13;
    if (cohesion < 0.28 && close && !mobile) {
      c.phase = "reorganize";
      order(front, "regroup", { x: centers[team].x - sign * 3, y: centers[team].y }, "shieldwall");
      order(ranged, "fallback", { x: centers[team].x - sign * 6, y: centers[team].y }, "loose");
      continue;
    }
    c.phase = mobile ? "harass" : defensive && (!close || b.time < 16) ? "screen" : "press";
    // Defensive commanders establish their line on nearby high ground and advance it in bounds.
    let lineX = centers[team].x + sign * (defensive ? 2.5 : 5);
    if (defensive && close) lineX = centers[team].x;
    if (b.siege && team === 1) lineX = Math.max(45, lineX);
    const line = { x: clamp(lineX, 5, 59), y: clamp(enemy.y, 7, 33) };
    if (defensive && !close) {
      let best = -Infinity, ground = line;
      for (let dy = -3; dy <= 3; dy++) for (let dx = -2; dx <= 2; dx++) {
        const x = Math.floor(line.x + dx), y = Math.floor(line.y + dy);
        if (!rt.nav.walkable(x, y) || (b.siege && team === 1 && x < 44)) continue;
        const score = (rt.map.elevation[y * 64 + x] || 0) * 1.3 - Math.hypot(dx, dy) * 0.55;
        if (score > best) { best = score; ground = { x: x + 0.5, y: y + 0.5 }; }
      }
      line.x = ground.x; line.y = ground.y;
    }
    if (front.length) {
      const reserve = front.length > 8 ? front.slice(-Math.ceil(front.length * 0.18)) : [];
      const lineTroops = reserve.length ? front.slice(0, -reserve.length) : front;
      const threat = enemies.some((u) => UNITS[u.key].role === "cavalry" && distance(u, line) < 10);
      order(lineTroops, b.siege && team === 0 ? "attackNearest" : threat ? "brace" : c.phase === "press" ? "charge" : "defend", line, defensive ? "shieldwall" : "line");
      const commit = loss > 0.18 || (close && enemies.length < own.length * 0.7);
      order(reserve, commit ? "charge" : "regroup", { x: line.x - sign * 4, y: line.y }, "square");
    }
    order(ranged, mobile || doctrine === "ambush" ? "skirmish" : "defend", { x: line.x - sign * 3.8, y: line.y }, "loose");
    order(horse, "skirmish", { x: clamp(line.x - sign * 6, 3, 61), y: line.y + (team ? -4 : 4) }, "loose");
    order(support, "defend", { x: line.x - sign * 5, y: line.y }, "loose");
    if (cavalry.length) {
      const exposed = enemies.filter((u) => ["healer", "ranged", "crossbow", "mage"].includes(UNITS[u.key].role)).sort((a, d) => distance(a, centers[team]) - distance(d, centers[team]))[0];
      if (close && exposed) order(cavalry, "attack", { x: exposed.x, y: exposed.y, target: exposed.id }, "wedge");
      else if (close || !defensive) order(cavalry, "flank", { x: enemy.x, y: clamp(enemy.y + (team ? -8 : 8), 4, 36), side: team ? -1 : 1 }, "wedge");
      else order(cavalry, "regroup", { x: line.x - sign * 5, y: line.y + 6 }, "wedge");
    }
    // Concentrated splash remains subject to the same sight, ammunition and cooldown rules.
    const splash = ranged.filter((u) => (UNITS[u.key].splashRadius || (UNITS[u.key].role === "mage" ? 1.6 : 0)) > 0);
    if (splash.length && enemies.length) {
      let cluster = null, density = 0;
      for (const v of enemies.slice(0, 40)) {
        let n = 0;
        rt.spatial.each(v.x, v.y, 2, (other) => { if (other.team !== team) n++; });
        if (n > density) { density = n; cluster = v; }
      }
      if (cluster) order(splash, "focus", { x: cluster.x, y: cluster.y, target: cluster.id }, "loose");
    }
  }
}
function updateOrganization(b, u, rt, dt) {
  let friends = 0, foes = 0, officer = 0, withdrawing = 0;
  rt.spatial.each(u.x, u.y, 2.6, (v) => {
    if (v.id === u.id) return;
    if (v.team === u.team) {
      friends++;
      if (UNITS[v.key].officer) officer++;
      if (["retreat", "fallback"].includes(v.order.kind)) withdrawing++;
    } else foes++;
  });
  const settled = distance(u, { x: u.order.x ?? u.x, y: u.order.y ?? u.y }) < 1.4;
  const desired = clamp(0.36 + Math.min(5, friends) * 0.1 + (guarded(u) && settled ? 0.18 : 0) - Math.max(0, foes - friends) * 0.07, 0.1, 1);
  u.cohesion += (desired - u.cohesion) * 0.25;
  const standard = b.banners[u.team]?.length ? 0.12 : 0;
  const moraleGoal = clamp(b.morale[u.team] + officer * 3 + standard * 10 - Math.max(0, foes - friends - 1) * 11 - withdrawing * 1.2, 0, 100);
  u.morale += (moraleGoal - u.morale) * Math.min(0.2, dt * 0.5);
}
function explosion(b, p, point, rt, primary = null) {
  const radius = p.splashRadius || 0;
  if (!radius) { if (active(primary)) damage(b, primary, p.damage, p, p.kind); return; }
  rt.spatial.each(point.x, point.y, radius, (v, d) => {
    if (v.team === p.team && !p.friendlyFire) return;
    if (!rt.nav.clearLine(point, v)) return;
    const falloff = 1 - Math.sqrt(d) / radius * clamp(p.splashFalloff ?? 0.65, 0, 0.9);
    damage(b, v, p.damage * falloff * (v.team === p.team ? Number(p.friendlyFire) : 1), p, p.kind);
  });
  if (b.effects.length < 200) b.effects.push({ kind: "splash", x: point.x, y: point.y, radius, time: b.time, team: p.team });
}
function advanceProjectiles(b, rt, dt) {
  for (let i = b.projectiles.length - 1; i >= 0; i--) {
    const p = b.projectiles[i];
    p.life += dt;
    const target = rt.byId.get(p.target);
    // Mild tracking preserves the original aiming model; every traveled segment still collides.
    if (active(target) && p.life < 0.4) { p.tx = target.x; p.ty = target.y; }
    const d = Math.hypot(p.tx - p.x, p.ty - p.y), step = Math.min(d, (p.kind === "magic" ? 13 : 22) * dt);
    const end = { x: p.x + (p.tx - p.x) / (d || 1) * step, y: p.y + (p.ty - p.y) / (d || 1) * step };
    let hit = null, first = Infinity;
    rt.spatial.each((p.x + end.x) / 2, (p.y + end.y) / 2, step / 2 + 1, (v) => {
      if ((v.team === p.team && !p.friendlyFire) || v.id === p.source) return;
      const f = clamp(((v.x - p.x) * (end.x - p.x) + (v.y - p.y) * (end.y - p.y)) / (step * step || 1), 0, 1);
      const q = { x: p.x + (end.x - p.x) * f, y: p.y + (end.y - p.y) * f };
      const shieldRadius = v.shield > 0 && guarded(v) && frontal(v, p) ? 0.46 + v.cohesion * 0.3 : 0.34;
      if (distance(v, q) <= shieldRadius && f < first && rt.nav.clearLine(p, q)) { hit = v; first = f; }
    });
    const clear = rt.nav.clearLine(p, end);
    if (hit || !clear || d <= step + 0.001 || p.life > 4) {
      const point = hit ? { x: p.x + (end.x - p.x) * first, y: p.y + (end.y - p.y) * first } : clear ? end : { x: p.x, y: p.y };
      // A body intercepts a projectile before its intended target, including a shield protecting an ally.
      explosion(b, p, point, rt, hit);
      b.projectiles.splice(i, 1);
    } else { p.x = end.x; p.y = end.y; }
  }
}
export function tickBattle(b, dt = STEP) {
  if (b.finished) return;
  dt = clamp(Number(dt) || STEP, 0.001, 0.25);
  b.time += dt;
  b.tick++;
  const rt = runtime(b);
  rt.pathBudget = b.units.length > 400 ? 4 : 8;
  deliverWaves(b, rt);
  rt.spatial.rebuild(b.units);
  rt.claims.clear();
  const centers = [{ x: 0, y: 0, n: 0 }, { x: 0, y: 0, n: 0 }], alive = [0, 0];
  for (const u of b.units) if (active(u)) {
    if (UNITS[u.key].role !== "healer") {
      centers[u.team].x += u.x;
      centers[u.team].y += u.y;
      centers[u.team].n++;
    }
    alive[u.team]++;
    if (u.target != null) rt.claims.set(u.target, (rt.claims.get(u.target) || 0) + 1);
  }
  for (let team = 0; team < 2; team++) {
    if (!centers[team].n && alive[team]) {
      for (const u of b.units) if (u.team === team && u.hp > 0) u.routed = true;
      alive[team] = 0;
    }
    centers[team].x /= centers[team].n || 1;
    centers[team].y /= centers[team].n || 1;
  }
  planCommanders(b, rt, centers);
  for (const u of b.units) {
    u.px = u.x;
    u.py = u.y;
    u.anim = Math.max(0, u.anim - dt);
    if (!active(u)) continue;
    const t = UNITS[u.key], o = u.order, healer = t.role === "healer", ranged = missile(t) && u.ammo > 0;
    const range = missile(t) && !ranged ? 1.15 : t.range;
    u.cooldown = Math.max(0, u.cooldown - dt);
    u.chargeRecovery = Math.max(0, u.chargeRecovery - dt);
    u.suppressed = Math.max(0, u.suppressed - dt);
    u.think -= dt;
    // Organization is sampled in staggered batches; movement and projectiles remain at 20 Hz.
    if (b.tick % 5 === u.id % 5) updateOrganization(b, u, rt, dt * 5);
    u.stamina = Math.min(100, u.stamina + dt * (u.state === "move" ? 0.12 : u.anim ? 0.4 : 3.2));
    if (o.kind === "retreat" || (u.morale < 15 && (u.hp < u.maxHp * 0.5 || b.morale[u.team] < 25))) {
      walk(b, u, { x: u.team === 0 ? 0.7 : 63.3, y: u.y }, dt, rt, t.speed * 1.15);
      if (u.x < 1.1 || u.x > 62.9) { u.routed = true; u.state = "idle"; u.escapedAt = b.time; }
      continue;
    }
    let explicit = o.target == null ? null : rt.byId.get(o.target);
    if (!active(explicit)) explicit = null;
    if (u.think <= 0 || (u.target != null && !active(rt.byId.get(u.target)))) {
      u.think = 0.2 + (u.id % 5) * 0.035;
      if (u.target != null) rt.claims.set(u.target, Math.max(0, (rt.claims.get(u.target) || 0) - 1));
      const pinned = ["attack", "focus"].includes(o.kind) && explicit?.team !== u.team ? explicit : null;
      const chosen = pinned && reachable(rt, u, pinned) ? pinned : findTarget(b, u, rt);
      u.target = chosen?.id ?? null;
      if (chosen) rt.claims.set(chosen.id, (rt.claims.get(chosen.id) || 0) + 1);
      if (o.kind === "attackNearest") o.target = u.target;
    }
    let target = rt.byId.get(u.target);
    if (!active(target)) target = null;
    const anchor = { x: o.x ?? u.x, y: o.y ?? u.y };
    const anchored = ["hold", "defend", "brace", "regroup"].includes(o.kind);
    const atStation = distance(u, anchor) < 0.65;
    if (anchored && target) face(u, target, dt);
    if (o.kind === "brace" && atStation && u.stamina > 20 && u.cohesion > 0.3) u.braceTime += dt;
    else if (o.kind !== "brace") u.braceTime = 0;
    u.braced = (t.role === "spear" || t.brace) && o.kind === "brace" && u.braceTime >= 0.7 && atStation;
    // Cavalry must disengage after an impact before rebuilding charge momentum.
    if (t.role === "cavalry" && b.time < u.regroupUntil && !anchored && !["move", "retreat", "fallback"].includes(o.kind)) {
      walk(b, u, { x: clamp(u.regroupX, 1, 63), y: clamp(u.regroupY, 1, 39) }, dt, rt, t.speed);
      continue;
    }
    let attacked = false;
    const targetDistance = target ? distance(u, target) : Infinity;
    const canFire = !ranged || (target && rt.nav.clearLine(u, target));
    const atSlot = !anchored || atStation || (o.kind === "defend" && targetDistance < 1.8);
    const mayAttack = !["move", "follow"].includes(o.kind) && (o.kind !== "flank" || o.leg >= 1 || targetDistance < 2);
    if (target && targetDistance <= range && mayAttack && canFire && atSlot && (o.kind !== "regroup" || atStation || targetDistance < 1.5)) {
      if (!u.cooldown) {
        u.cooldown = t.cooldown * (u.stamina < 20 ? 1.3 : 1);
        u.state = healer ? "defend" : "attack";
        u.anim = 0.45;
        if (t.role !== "horseArcher" || !["skirmish", "auto", "fallback"].includes(o.kind)) face(u, target);
        u.stamina = Math.max(0, u.stamina - (healer ? 1 : ranged ? 2 : 4));
        attacked = true;
        if (healer) {
          const mult = b.banners[u.team]?.includes("sun") ? 1.2 : 1;
          target.hp = Math.min(target.maxHp, target.hp + t.damage * mult);
          target.morale = Math.min(100, target.morale + 1);
          if (b.effects.length < 200) b.effects.push({ kind: "heal", x: target.x, y: target.y, time: b.time, team: u.team });
        } else {
          let amount = t.damage * (1 + b.tactics[u.team] * 0.025) * (u.stamina < 15 ? 0.78 : 1);
          if (missile(t) && !ranged) amount *= 0.35;
          const impact = t.role === "cavalry" && u.charged >= 3.5 && u.chargeRecovery <= 0 && u.stamina > 20;
          if (impact) {
            const momentum = Math.min(9, u.charged) * groundSpeed(b, u, rt) * (0.6 + u.cohesion * 0.4);
            if (target.braced && frontal(target, u)) {
              damage(b, u, (UNITS[target.key].damage * 1.6 + momentum * 2) * (Number(UNITS[target.key].brace) || 1), target, "melee");
              amount *= 0.4;
              u.suppressed = 0.9;
              b.effects.push({ kind: "brace", x: target.x, y: target.y, time: b.time, team: target.team });
            } else amount *= 1 + momentum * 0.1 * (Number(t.charge) || 1);
            u.charged = 0;
            u.chargeRecovery = 4;
            u.regroupUntil = b.time + 2.1;
            u.regroupX = u.x - direction(u.team) * 5;
            u.regroupY = u.y + (u.id % 2 ? 2 : -2);
          }
          if (t.role === "spear" && mounted(UNITS[target.key])) amount *= u.braced ? 1.8 : 1.3;
          if (active(u)) {
            if (ranged) {
              if (t.role === "horseArcher" && !frontal(u, target)) amount *= 0.74;
              const index = (v) => Math.floor(v.y) * 64 + Math.floor(v.x);
              amount *= 1 + Math.max(0, (rt.map.elevation[index(u)] || 0) - (rt.map.elevation[index(target)] || 0)) * 0.035;
              shoot(b, u, target, amount, t.role === "mage" ? "magic" : t.role === "crossbow" ? "crossbow" : "arrow");
              u.ammo--;
            } else if (t.splashRadius && !missile(t)) {
              explosion(b, { x: u.x, y: u.y, team: u.team, damage: amount, kind: "melee", splashRadius: t.splashRadius, splashFalloff: t.splashFalloff, friendlyFire: Number(t.friendlyFire) || 0 }, target, rt, target);
            } else damage(b, target, amount, u);
          }
        }
      } else if (!u.anim) u.state = anchored ? "defend" : "idle";
      if (anchored && atStation) continue;
      if (!ranged && o.kind !== "fallback") continue;
      if (healer) continue;
    }
    let destination = null, speed = t.speed, backpedal = false;
    const skirmishing = ranged && (o.kind === "skirmish" || (t.role === "horseArcher" && o.kind === "auto"));
    if (skirmishing && target) {
      const preferred = range * (t.role === "horseArcher" ? 0.78 : 0.72);
      if (targetDistance < preferred * 0.88 || (u.cooldown > t.cooldown * 0.42 && targetDistance < range * 1.08)) {
        const awayX = (u.x - target.x) / (targetDistance || 1), awayY = (u.y - target.y) / (targetDistance || 1);
        const fallback = { x: o.anchorX, y: o.anchorY };
        const validFallback = Number.isFinite(fallback.x + fallback.y) && distance(fallback, target) > targetDistance + 1;
        destination = validFallback && distance(u, fallback) > 1.2 ? fallback : { x: clamp(u.x + awayX * 4 - awayY * (u.id % 2 ? 2 : -2), 1, 63), y: clamp(u.y + awayY * 4 + awayX * (u.id % 2 ? 2 : -2), 1, 39) };
      } else if (targetDistance > preferred || !canFire) destination = target;
      else if (!attacked && !u.anim) u.state = "idle";
    } else if (["hold", "move", "brace", "regroup"].includes(o.kind)) destination = anchor;
    else if (o.kind === "fallback") { destination = anchor; speed *= 0.8; backpedal = true; }
    else if (o.kind === "follow") destination = explicit ? { x: explicit.x - direction(u.team) * 2, y: explicit.y + ((u.id % 5) - 2) } : anchor;
    else if (o.kind === "defend") {
      const station = explicit ? { x: explicit.x - direction(u.team) * (healer || ranged ? 2 : 0.8), y: explicit.y + ((u.id % 3) - 1) } : anchor;
      // A defending line keeps its slots rather than pursuing enemies outside melee reach.
      destination = !ranged && !healer && targetDistance < 2 && distance(target, station) < 1.8 ? target : station;
    } else if (o.kind === "flank" && target) {
      const side = o.side || 1;
      destination = o.leg === 0 ? { x: target.x - direction(u.team) * 4, y: clamp(target.y + side * 8, 2, 38) } : o.leg === 1 ? { x: target.x + direction(u.team) * 4, y: clamp(target.y + side * 6, 2, 38) } : target;
      if (distance(u, destination) < 1.4) { o.leg = Math.min(2, o.leg + 1); u.path = null; }
    } else if (o.kind === "patrol") {
      destination = target && !healer ? target : o.leg ? { x: o.fromX, y: o.fromY } : anchor;
      if (!target && distance(u, destination) < 0.6) o.leg = 1 - o.leg;
    } else if (healer) destination = target || { x: centers[u.team].x - direction(u.team) * 3, y: centers[u.team].y };
    else if (!skirmishing) destination = target || (o.kind === "attack" ? anchor : centers[1 - u.team]);
    // Missile troops with a clear shot stay in place, while blocked firing lines reposition laterally.
    if (ranged && target && !skirmishing && !["fallback", "hold", "brace", "regroup"].includes(o.kind) && targetDistance <= range && mayAttack) {
      if (canFire) destination = null;
      else destination = { x: clamp(u.x + direction(u.team) * 0.5, 1, 63), y: clamp(u.y + (u.id % 2 ? 2.5 : -2.5), 1, 39) };
    }
    if (b.siege && b.gate.hp > 0 && u.team === 0 && destination?.x > 41) {
      destination = { x: 40.9, y: 20 + ((u.id % 3) - 1) * 0.4 };
      if (distance(u, { x: 41, y: 20 }) < Math.max(1.8, range) && !u.cooldown && !["hold", "move", "brace", "fallback"].includes(o.kind)) {
        b.gate.hp = Math.max(0, b.gate.hp - t.damage * (t.role === "cavalry" ? 0.65 : 1) * (t.siegeBonus || 1));
        u.cooldown = t.cooldown;
        u.state = "attack";
        u.anim = 0.5;
        b.effects.push({ kind: "siege", x: 42, y: 20, time: b.time, team: 0 });
        if (!b.gate.hp) {
          for (const y of [19, 20]) { rt.map.costs[y * 64 + 42] = 1; rt.map.tiles[y * 64 + 42] = 4; }
          rt.map.version++;
          rt.nav.cache.clear();
          for (const v of b.units) v.path = null;
        }
        continue;
      }
    }
    if (b.siege && u.team === 1 && destination?.x < 43) destination = { x: 44, y: clamp(destination.y, 16, 24) };
    if (destination && distance(u, destination) > 0.4 && u.suppressed < 0.8) {
      const heading = u.heading;
      walk(b, u, destination, dt, rt, speed * (o.kind === "charge" ? 1.18 : 1) * (u.morale < 35 ? 0.85 : 1));
      if (backpedal) { u.heading = heading; if (target) face(u, target, dt); }
    } else if (!u.anim) { u.state = anchored || o.kind === "fallback" ? "defend" : "idle"; u.charged = Math.max(0, u.charged - dt * 1.5); }
  }
  rt.spatial.rebuild(b.units);
  advanceProjectiles(b, rt, dt);
  advanceObjective(b, rt, dt);
  if (b.siege && b.gate.hp > 0 && Math.floor(b.time / 3) > Math.floor((b.time - dt) / 3)) {
    const victim = b.units.find((u) => u.team === 0 && active(u) && u.x > 31);
    if (victim) shoot(b, { x: 43, y: 15, team: 1 }, victim, 15, "arrow");
  }
  b.effects = b.effects.filter((e) => b.time - e.time < (e.kind === "order" || e.kind === "notice" ? 2 : 1.2)).slice(-200);
  const remaining = [0, 0];
  for (const u of b.units) if (active(u)) remaining[u.team]++;
  if (!b.finished && (!remaining[0] || !remaining[1] || b.time >= 300)) {
    b.finished = true;
    b.winner = remaining[0] === remaining[1] ? null : remaining[0] > remaining[1] ? 0 : 1;
  }
}
export function battleView(b) {
  const { path, pathIndex, ...objective } = b.objective || {};
  return {
    id: b.id,
    seed: b.seed,
    siege: b.siege,
    names: b.names,
    seatRoles: b.seatRoles || ["army", "army"],
    factions: b.factions || ["crown", "ember"],
    terrain: b.terrain || "plains",
    encounterType: b.encounterType || "field",
    commanders: b.commanders?.map((c) => ({ doctrine: c.doctrine, phase: c.phase, enabled: c.enabled, plans: c.plans })) || [],
    notice: b.notice || null,
    objective: b.objective ? objective : null,
    time: b.time,
    tick: b.tick,
    finished: b.finished,
    winner: b.winner,
    formation: b.formation,
    morale: b.morale,
    abilities: b.abilities,
    gate: b.gate,
    // Keep the original compact troop tuple intact for old clients and saved snapshots.
    tactical: b.units.map((u) => [u.id, Math.round((u.heading ?? (u.facing < 0 ? Math.PI : 0)) * 100) / 100, Math.round((u.cohesion ?? 0.7) * 100), Math.round(u.stamina ?? 100), Math.round(u.morale ?? b.morale[u.team]), u.ammo ?? -1, Math.round(u.shield ?? 0), u.braced ? 1 : 0, Math.round((u.charged || 0) * 10) / 10, u.formation || b.formation[u.team]]),
    units: b.units.map((u) => [
      u.id,
      UNIT_KEYS.indexOf(u.key),
      u.team,
      Math.round(u.x * 100) / 100,
      Math.round(u.y * 100) / 100,
      Math.round(u.hp),
      u.state,
      u.facing,
      u.order.kind,
      u.routed ? 1 : 0,
    ]),
    projectiles: b.projectiles.map((p) => ({
      x: p.x,
      y: p.y,
      tx: p.tx,
      ty: p.ty,
      kind: p.kind,
      team: p.team,
    })),
    effects: b.effects,
  };
}
export function hydrateView(view, previous = null) {
  const old = new Map(previous?.units?.map((u) => [u.id, u]) || []);
  const tactical = new Map(view.tactical?.map((a) => [a[0], a]) || []);
  return {
    ...view,
    units: view.units.map((a) => {
      const prev = old.get(a[0]);
      const status = tactical.get(a[0]);
      return {
        id: a[0],
        key: UNIT_KEYS[a[1]],
        team: a[2],
        x: a[3],
        y: a[4],
        px: prev?.x ?? a[3],
        py: prev?.y ?? a[4],
        hp: a[5],
        maxHp: UNITS[UNIT_KEYS[a[1]]].hp,
        state: a[6],
        facing: a[7],
        heading: status?.[1] ?? (a[7] < 0 ? Math.PI : 0),
        cohesion: status ? status[2] / 100 : 0.7,
        stamina: status?.[3] ?? 100,
        morale: status?.[4] ?? view.morale?.[a[2]] ?? 100,
        ammo: status?.[5] ?? -1,
        shield: status?.[6] ?? 0,
        braced: !!status?.[7],
        charged: status?.[8] ?? 0,
        formation: status?.[9] ?? view.formation?.[a[2]] ?? "line",
        order: { kind: a[8] },
        routed: !!a[9],
        deathAt: prev?.deathAt || (a[5] <= 0 ? view.time : 0),
        anim: 0,
      };
    }),
  };
}
