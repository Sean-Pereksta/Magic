import { WORLD, hash, rng, clamp, UNITS, emptyArmy } from "./data.mjs";
import { Navigator } from "./navigation.mjs";
export const SETTLEMENTS = [
  {
    id: "hearthford",
    name: "Hearthford",
    kind: "village",
    x: 13.5,
    y: 44.5,
    faction: "crown",
  },
  {
    id: "oakwatch",
    name: "Oakwatch",
    kind: "village",
    x: 27.5,
    y: 27.5,
    faction: "vale",
  },
  {
    id: "dawnkeep",
    name: "Dawnkeep",
    kind: "castle",
    x: 38.5,
    y: 44.5,
    faction: "crown",
    garrison: 24,
  },
  {
    id: "bracken",
    name: "Bracken",
    kind: "village",
    x: 48.5,
    y: 66.5,
    faction: "vale",
  },
  {
    id: "greenhold",
    name: "Greenhold",
    kind: "castle",
    x: 56.5,
    y: 22.5,
    faction: "vale",
    garrison: 48,
  },
  {
    id: "rivercross",
    name: "Rivercross",
    kind: "village",
    x: 75.5,
    y: 44.5,
    faction: "crown",
  },
  {
    id: "redmill",
    name: "Redmill",
    kind: "village",
    x: 92.5,
    y: 66.5,
    faction: "ember",
  },
  {
    id: "embergate",
    name: "Embergate",
    kind: "castle",
    x: 104.5,
    y: 44.5,
    faction: "ember",
    garrison: 74,
  },
  {
    id: "highcrown",
    name: "Highcrown",
    kind: "castle",
    x: 112.5,
    y: 19.5,
    faction: "ember",
    garrison: 100,
  },
  {
    id: "frostmere",
    name: "Frostmere",
    kind: "village",
    x: 91.5,
    y: 18.5,
    faction: "vale",
  },
];
export function createWorld(seed = WORLD.seed) {
  const { width: w, height: h } = WORLD,
    tiles = new Uint8Array(w * h),
    costs = new Float32Array(w * h),
    props = [];
  for (let y = 0; y < h; y++)
    for (let x = 0; x < w; x++) {
      const n = hash(x, y, seed) % 100,
        forest = Math.sin(x * 0.15) + Math.cos(y * 0.21) > 0.65,
        rock = Math.sin(x * 0.13 - y * 0.19) > 1 - 0.15;
      let t = forest ? 1 : 0;
      if (rock && n < 50) t = 2;
      if (y < 9) t = 6;
      if (x > 85 && y > 55) t = 5;
      const river = 71 + Math.round(Math.sin(y * 0.1) * 2);
      if (Math.abs(x - river) <= 1) t = 3;
      tiles[y * w + x] = t;
      costs[y * w + x] =
        t === 3 || t === 2 ? 0 : t === 1 ? 1.6 : t === 6 ? 1.4 : 1;
      if (t === 1 && n < 22) {
        props.push({ x: x + 0.5, y: y + 0.5, type: 8 });
        costs[y * w + x] = 0;
      }
      if (t === 2 && n < 25) props.push({ x: x + 0.5, y: y + 0.5, type: 9 });
    }
  const road = (a, b) => {
    let x = Math.floor(a.x),
      y = Math.floor(a.y);
    for (let i = 0; i < w + h; i++) {
      for (let dy = -1; dy <= 1; dy++)
        for (let dx = -1; dx <= 1; dx++) {
          const nx = x + dx,
            ny = y + dy;
          if (nx < 0 || ny < 0 || nx >= w || ny >= h) continue;
          const at = ny * w + nx;
          tiles[at] = tiles[at] === 3 || tiles[at] === 7 ? 7 : 4;
          costs[at] = 0.75;
        }
      if (x === Math.floor(b.x) && y === Math.floor(b.y)) break;
      if (x !== Math.floor(b.x)) x += Math.sign(b.x - x);
      else y += Math.sign(b.y - y);
    }
  };
  road({ x: 2, y: 44 }, { x: 125, y: 44 });
  for (const s of SETTLEMENTS) road(s, { x: s.x, y: 44 });
  const world = {
    width: w,
    height: h,
    seed,
    tiles,
    costs,
    version: 1,
    props: props.filter(
      (p) => tiles[Math.floor(p.y) * w + Math.floor(p.x)] === p.type - 7,
    ),
    settlements: SETTLEMENTS.map((s) => ({
      ...s,
      owner: null,
      level: 1,
      garrison: s.garrison || 0,
      prosperity: 1,
    })),
  };
  world.nav = new Navigator(world);
  return world;
}
export function createNPCs(world, seed = world.seed) {
  const random = rng(seed),
    npcs = [];
  for (let i = 0; i < 28; i++) {
    const kind = i < 13 ? "bandit" : i < 20 ? "patrol" : "merchant",
      home = world.settlements[i % world.settlements.length],
      target = world.settlements[(i * 3 + 1) % world.settlements.length];
    const pos = world.nav.nearest(
      home.x + (random() - 0.5) * 8,
      home.y + (random() - 0.5) * 8,
    ) || { x: 15, y: 44 };
    const size =
      kind === "bandit"
        ? 5 + Math.floor(random() * 15)
        : kind === "patrol"
          ? 25 + Math.floor(random() * 28)
          : 4;
    const army = {
      ...emptyArmy(),
      recruits: Math.ceil(size * 0.45),
      archers: Math.floor(size * 0.25),
      footmen: Math.floor(size * 0.3),
    };
    npcs.push({
      id: `${kind}-${i}`,
      kind,
      name:
        kind === "bandit"
          ? "Outlaw company"
          : kind === "patrol"
            ? `${home.faction} patrol`
            : "Trade caravan",
      faction: kind === "bandit" ? "outlaw" : home.faction,
      x: pos.x + 0.5,
      y: pos.y + 0.5,
      home: home.id,
      target: target.id,
      army,
      route: [],
      routeIndex: 0,
      respawn: 0,
      decision: random() * 2,
    });
  }
  return npcs;
}
export function travel(entity, nav, dt, speed) {
  let remaining = dt * speed,
    moved = false;
  while (entity.routeIndex < (entity.route?.length || 0) && remaining > 0) {
    const t = entity.route[entity.routeIndex],
      dx = t.x - entity.x,
      dy = t.y - entity.y,
      d = Math.hypot(dx, dy);
    const cost =
        nav.map.costs[
          Math.floor(entity.y) * nav.map.width + Math.floor(entity.x)
        ] || 1,
      step = remaining / cost;
    if (d <= step) {
      entity.x = t.x;
      entity.y = t.y;
      remaining -= d * cost;
      entity.routeIndex++;
    } else {
      entity.x += (dx / d) * step;
      entity.y += (dy / d) * step;
      remaining = 0;
    }
    moved = true;
  }
  return moved;
}
export function tickNPCs(world, npcs, players, dt, time) {
  for (const n of npcs) {
    if (n.battleId) continue;
    if (n.respawn > time) continue;
    n.decision -= dt;
    if (n.decision <= 0) {
      n.decision = 1.5;
      let target =
        world.settlements.find((s) => s.id === n.target) ||
        world.settlements[0];
      if (n.kind === "bandit") {
        const near = players.find(
          (p) =>
            !p.activeBattle &&
            Math.hypot(p.x - n.x, p.y - n.y) < 9 &&
            Object.values(p.army).reduce((a, b) => a + b, 0) <
              Object.values(n.army).reduce((a, b) => a + b, 0) * 1.4,
        );
        if (near) target = near;
      }
      if (n.kind === "patrol") {
        const invaded = world.settlements.find(
          (s) => s.faction === n.faction && s.owner,
        );
        if (invaded) target = invaded;
      }
      if (Math.hypot(n.x - target.x, n.y - target.y) < 1) {
        const index =
          (Math.floor(time / 13) + Number(n.id.split("-")[1])) %
          world.settlements.length;
        n.target = world.settlements[index].id;
        target = world.settlements[index];
      }
      n.route = world.nav.path(n, target);
      n.routeIndex = 0;
    }
    travel(
      n,
      world.nav,
      dt,
      n.kind === "merchant" ? 1.15 : n.kind === "patrol" ? 1.6 : 1.9,
    );
  }
}
export function battleArmy(size, faction = "outlaw") {
  const army = emptyArmy(),
    keys =
      faction === "ember"
        ? ["footmen", "cavalry", "mages"]
        : faction === "vale"
          ? ["recruits", "veteranArchers", "priests"]
          : ["recruits", "footmen", "archers"];
  for (let i = 0; i < size; i++) army[keys[i % keys.length]]++;
  return army;
}
