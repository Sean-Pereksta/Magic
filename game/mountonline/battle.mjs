import {
  UNITS,
  UNIT_KEYS,
  ORDERS,
  FORMATIONS,
  STEP,
  hash,
  rng,
  clamp,
  armySize,
} from "./data.mjs";
import { Navigator, SpatialHash } from "./navigation.mjs";
import { requireThat } from "./progression.mjs";
const internals = new WeakMap();
const distance = (a, b) => Math.hypot(a.x - b.x, a.y - b.y);
function runtime(b) {
  let rt = internals.get(b);
  if (!rt) {
    const map = makeMap(b.seed, b.siege);
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
    };
    internals.set(b, rt);
  }
  return rt;
}
export function makeMap(seed = 1, siege = false) {
  const width = 64,
    height = 40,
    tiles = new Uint8Array(width * height),
    costs = new Float32Array(width * height);
  costs.fill(1);
  for (let y = 2; y < height - 2; y++)
    for (let x = 14; x < width - 12; x++) {
      const n = hash(x, y, seed) % 100;
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
  return { width, height, tiles, costs, version: 1 };
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
}) {
  const b = {
    id,
    seed,
    siege,
    names,
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
        (["ranged", "crossbow", "mage", "healer"].includes(UNITS[a].role)
          ? 1
          : 0) -
        (["ranged", "crossbow", "mage", "healer"].includes(UNITS[c].role)
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
        });
      }
  }
  runtime(b);
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
  const rt = runtime(b),
    selected = [...new Set(ids)].map((id) => rt.byId.get(id));
  requireThat(
    selected.every((u) => u && u.team === team && u.hp > 0 && !u.routed),
    "You can only command your own active troops.",
  );
  const center = {
    x: selected.reduce((n, u) => n + u.x, 0) / selected.length,
    y: selected.reduce((n, u) => n + u.y, 0) / selected.length,
  };
  let x = point.x ?? center.x,
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
  if (target && kind === "attack")
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
          };
    u.target = null;
    u.path = null;
    u.think = 0;
    u.charged = 0;
    u.state = kind === "hold" ? "defend" : "idle";
  }
  b.effects.push({ kind: "order", x, y, time: b.time, order: kind, team });
  return selected.length;
}
function findTarget(b, u, rt) {
  const t = UNITS[u.key];
  let best = null,
    score = Infinity;
  rt.spatial.each(
    u.x,
    u.y,
    t.role === "healer" ? t.range : 16,
    (candidate, d) => {
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
      let s = d;
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
        ["ranged", "mage", "healer"].includes(UNITS[candidate.key].role)
      )
        s *= 0.55;
      if (
        ["hold", "defend"].includes(u.order.kind) &&
        distance(candidate, {
          x: u.order.anchorX ?? u.order.x,
          y: u.order.anchorY ?? u.order.y,
        }) > 7
      )
        return;
      if (s < score) {
        score = s;
        best = candidate;
      }
    },
  );
  return best;
}
function damage(b, target, amount, source, kind = "melee") {
  if (!target || target.hp <= 0 || target.routed) return;
  const armor = UNITS[target.key].armor,
    formation = b.formation[target.team],
    guard =
      target.state === "defend" ||
      target.order.kind === "hold" ||
      target.order.kind === "defend";
  let reduction =
    guard && (formation === "shieldwall" || formation === "square") ? 0.3 : 0;
  if (guard && b.banners[target.team]?.includes("bulwark")) reduction += 0.08;
  const armorScale = kind === "crossbow" ? 0.35 : kind === "magic" ? 0 : 1;
  target.hp -= Math.max(1, (amount - armor * armorScale) * (1 - reduction));
  target.anim = 0.2;
  if (target.hp <= 0) {
    target.hp = 0;
    target.state = "dying";
    target.deathAt = b.time;
    b.morale[target.team] = Math.max(
      0,
      b.morale[target.team] -
        65 / Math.max(10, b.units.filter((u) => u.team === target.team).length),
    );
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
    damage: amount,
    kind,
    life: 0,
  });
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
  rt.spatial.each(u.x, u.y, 0.62, (v, dist) => {
    if (v.id === u.id || dist < 0.0001) return;
    const f = (0.62 - Math.sqrt(dist)) * 0.65;
    sx += ((u.x - v.x) / Math.sqrt(dist)) * f;
    sy += ((u.y - v.y) / Math.sqrt(dist)) * f;
  });
  dx += sx;
  dy += sy;
  const norm = Math.hypot(dx, dy) || 1,
    step = Math.min(d, speed * dt),
    x = u.x + (dx / norm) * step,
    y = u.y + (dy / norm) * step;
  if (
    rt.nav.walkable(Math.floor(x), Math.floor(y)) &&
    rt.nav.clearLine(u, { x, y })
  ) {
    u.x = clamp(x, 0.6, 63.4);
    u.y = clamp(y, 0.6, 39.4);
    u.facing = dx < 0 ? -1 : 1;
    u.state = "move";
    u.stuck = 0;
    u.charged += step;
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
      if (u.team === team) u.hp = Math.min(u.maxHp, u.hp + u.maxHp * 0.18);
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
export function tickBattle(b, dt = STEP) {
  if (b.finished) return;
  b.time += dt;
  b.tick++;
  const rt = runtime(b);
  rt.pathBudget = 12;
  rt.spatial.rebuild(b.units);
  const centers = [
      { x: 0, y: 0, n: 0 },
      { x: 0, y: 0, n: 0 },
    ],
    alive = [0, 0];
  for (const u of b.units)
    if (u.hp > 0 && !u.routed) {
      if (UNITS[u.key].role !== "healer") {
        centers[u.team].x += u.x;
        centers[u.team].y += u.y;
        centers[u.team].n++;
      }
      alive[u.team]++;
    }
  // Support-only remnants withdraw with survivors rather than fleeing forever
  // while a castle's last archers wait on their walls.
  for (let team = 0; team < 2; team++)
    if (!centers[team].n && alive[team]) {
      for (const u of b.units) if (u.team === team && u.hp > 0) u.routed = true;
      alive[team] = 0;
    }
  for (const c of centers) {
    c.x /= c.n || 1;
    c.y /= c.n || 1;
  }
  for (const u of b.units) {
    u.px = u.x;
    u.py = u.y;
    u.anim = Math.max(0, u.anim - dt);
    if (u.hp <= 0 || u.routed) continue;
    const t = UNITS[u.key],
      o = u.order;
    u.cooldown = Math.max(0, u.cooldown - dt);
    u.think -= dt;
    if (
      o.kind === "retreat" ||
      (b.morale[u.team] < 20 && u.hp < u.maxHp * 0.3 && o.kind !== "hold")
    ) {
      walk(
        b,
        u,
        { x: u.team === 0 ? 0.7 : 63.3, y: u.y },
        dt,
        rt,
        t.speed * 1.2,
      );
      if (u.x < 1.1 || u.x > 62.9) {
        u.routed = true;
        u.state = "idle";
      }
      continue;
    }
    let explicit = o.target == null ? null : rt.byId.get(o.target);
    if (explicit?.hp <= 0 || explicit?.routed) explicit = null;
    if (u.think <= 0) {
      u.think = 0.18 + (u.id % 4) * 0.025;
      const chosen =
        (o.kind === "attack" && explicit?.team !== u.team ? explicit : null) ||
        findTarget(b, u, rt);
      u.target = chosen?.id ?? null;
    }
    let target = rt.byId.get(u.target);
    if (target?.hp <= 0 || target?.routed) target = null;
    const healer = t.role === "healer",
      range = t.range;
    if (
      target &&
      distance(u, target) <= range &&
      o.kind !== "move" &&
      o.kind !== "follow"
    ) {
      if (!u.cooldown) {
        u.cooldown = t.cooldown;
        u.state = healer ? "defend" : "attack";
        u.anim = 0.45;
        u.facing = target.x < u.x ? -1 : 1;
        if (healer) {
          const mult = b.banners[u.team]?.includes("sun") ? 1.2 : 1;
          target.hp = Math.min(target.maxHp, target.hp + t.damage * mult);
          if (b.effects.length < 200)
            b.effects.push({
              kind: "heal",
              x: target.x,
              y: target.y,
              time: b.time,
              team: u.team,
            });
        } else {
          let amount = t.damage * (1 + b.tactics[u.team] * 0.025);
          if (t.role === "cavalry" && u.charged > 3) {
            amount *= 1.65;
            u.charged = 0;
          }
          if (t.role === "spear" && UNITS[target.key].role === "cavalry")
            amount *= 1.75;
          if (range > 3) {
            if (rt.nav.clearLine(u, target))
              shoot(
                b,
                u,
                target,
                amount,
                t.role === "mage"
                  ? "magic"
                  : t.role === "crossbow"
                    ? "crossbow"
                    : "arrow",
              );
          } else damage(b, target, amount, u);
        }
      } else if (!u.anim)
        u.state = o.kind === "hold" || o.kind === "defend" ? "defend" : "idle";
      // Archers give ground to a charge, but never walk off a hold order.
      if (
        !healer &&
        range > 3 &&
        distance(u, target) < 2.4 &&
        !["hold", "defend"].includes(o.kind)
      )
        walk(
          b,
          u,
          { x: u.x + (u.x - target.x) * 2, y: u.y + (u.y - target.y) * 2 },
          dt,
          rt,
          t.speed * 0.75,
        );
      continue;
    }
    let destination = null;
    if (["hold", "move"].includes(o.kind)) destination = { x: o.x, y: o.y };
    else if (o.kind === "follow")
      destination = explicit
        ? {
            x: explicit.x - (u.team === 0 ? 2 : -2),
            y: explicit.y + ((u.id % 5) - 2),
          }
        : { x: o.x, y: o.y };
    else if (o.kind === "defend")
      destination =
        target &&
        distance(target, { x: o.anchorX, y: o.anchorY }) < 7 &&
        !healer
          ? target
          : explicit || { x: o.x, y: o.y };
    else if (o.kind === "patrol") {
      if (target && !healer) destination = target;
      else {
        destination = o.leg ? { x: o.fromX, y: o.fromY } : { x: o.x, y: o.y };
        if (distance(u, destination) < 0.6) o.leg = 1 - o.leg;
      }
    } else if (healer)
      destination = target || {
        x: centers[u.team].x - (u.team === 0 ? 3 : -3),
        y: centers[u.team].y,
      };
    else
      destination =
        target ||
        (o.kind === "attack" ? { x: o.x, y: o.y } : centers[1 - u.team]);
    if (
      t.role === "cavalry" &&
      ["auto", "charge"].includes(o.kind) &&
      Math.abs(u.x - centers[1 - u.team].x) > 8 &&
      distance(u, centers[1 - u.team]) > 10
    )
      destination = {
        x: centers[1 - u.team].x + (u.team === 0 ? -5 : 5),
        y: clamp(centers[1 - u.team].y + (u.id % 2 ? 7 : -7), 2, 38),
      };
    // The gate is the only route through a siege wall. Defenders never chase outside it.
    if (b.siege && b.gate.hp > 0 && u.team === 0 && destination.x > 41) {
      destination = { x: 40.9, y: 20 + ((u.id % 3) - 1) * 0.4 };
      if (
        distance(u, { x: 41, y: 20 }) < Math.max(1.8, range) &&
        !u.cooldown &&
        o.kind !== "hold" &&
        o.kind !== "move"
      ) {
        b.gate.hp = Math.max(
          0,
          b.gate.hp - t.damage * (t.role === "cavalry" ? 0.65 : 1),
        );
        u.cooldown = t.cooldown;
        u.state = "attack";
        u.anim = 0.5;
        b.effects.push({ kind: "siege", x: 42, y: 20, time: b.time, team: 0 });
        if (b.gate.hp === 0) {
          for (const y of [19, 20]) {
            rt.map.costs[y * 64 + 42] = 1;
            rt.map.tiles[y * 64 + 42] = 4;
          }
          rt.map.version++;
          rt.nav.cache.clear();
          for (const v of b.units) v.path = null;
        }
        continue;
      }
    }
    if (b.siege && u.team === 1 && destination.x < 43)
      destination = { x: 44, y: clamp(destination.y, 16, 24) };
    if (destination && distance(u, destination) > 0.4) {
      walk(
        b,
        u,
        destination,
        dt,
        rt,
        t.speed *
          (o.kind === "charge" ? 1.18 : 1) *
          (b.morale[u.team] < 35 ? 0.85 : 1),
      );
    } else if (!u.anim)
      u.state = ["hold", "defend"].includes(o.kind) ? "defend" : "idle";
  }
  // Projectiles carry damage until impact; visual arrows and spells are the actual attacks.
  for (let i = b.projectiles.length - 1; i >= 0; i--) {
    const p = b.projectiles[i],
      target = rt.byId.get(p.target);
    p.life += dt;
    if (target && target.hp > 0) {
      p.tx = target.x;
      p.ty = target.y;
    }
    const d = Math.hypot(p.tx - p.x, p.ty - p.y),
      step = (p.kind === "magic" ? 13 : 22) * dt;
    if (d <= step || p.life > 3) {
      if (target && target.hp > 0) {
        damage(b, target, p.damage, p, p.kind);
        if (p.kind === "magic")
          rt.spatial.each(target.x, target.y, 1.6, (u) => {
            if (u.id !== target.id && u.team !== p.team)
              damage(b, u, p.damage * 0.4, p, "magic");
          });
      }
      b.projectiles.splice(i, 1);
    } else {
      p.x += ((p.tx - p.x) / d) * step;
      p.y += ((p.ty - p.y) / d) * step;
    }
  }
  if (
    b.siege &&
    b.gate.hp > 0 &&
    Math.floor(b.time / 3) > Math.floor((b.time - dt) / 3)
  ) {
    let victim = null;
    for (const u of b.units)
      if (u.team === 0 && u.hp > 0 && !u.routed && u.x > 31) {
        victim = u;
        break;
      }
    if (victim) shoot(b, { x: 43, y: 15, team: 1 }, victim, 15, "arrow");
  }
  b.effects = b.effects
    .filter((e) => b.time - e.time < (e.kind === "order" ? 2 : 1.2))
    .slice(-200);
  if (!alive[0] || !alive[1] || b.time >= 300) {
    b.finished = true;
    b.winner =
      alive[0] === alive[1]
        ? null
        : alive[0] > 0 && !alive[1]
          ? 0
          : alive[1] > 0 && !alive[0]
            ? 1
            : alive[0] > alive[1]
              ? 0
              : 1;
  }
}
export function battleView(b) {
  return {
    id: b.id,
    seed: b.seed,
    siege: b.siege,
    names: b.names,
    seatRoles: b.seatRoles || ["army", "army"],
    time: b.time,
    tick: b.tick,
    finished: b.finished,
    winner: b.winner,
    formation: b.formation,
    morale: b.morale,
    abilities: b.abilities,
    gate: b.gate,
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
  return {
    ...view,
    units: view.units.map((a) => {
      const prev = old.get(a[0]);
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
        order: { kind: a[8] },
        routed: !!a[9],
        deathAt: prev?.deathAt || (a[5] <= 0 ? view.time : 0),
        anim: 0,
      };
    }),
  };
}
