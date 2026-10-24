import { WORLD, FACTIONS, hash, rng, clamp, armySize, emptyArmy } from "./data.mjs";
import { Navigator } from "./navigation.mjs";
const ORIGINAL_SETTLEMENTS = [
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
// The original ten settlements remain at their saved coordinates. Each kingdom has
// one new capital, two castles, two towns and three villages in the expanded realm.
const REGIONS = [
  ["crown", 35, 92, ["Kingsrest", "Lionwatch", "Westwall", "Goldbrook", "Crownmarket", "Wheatmere", "Lowfield", "Bellstead"]],
  ["vale", 65, 119, ["Elderhaven", "Briar Keep", "Deepwood", "Leafmarket", "Willowford", "Fernstead", "Mossbrook", "Alderfen"]],
  ["ember", 119, 92, ["Cinderhall", "Ashwatch", "Firebreak", "Coalhaven", "Redwater", "Ashfield", "Scorchwell", "Cinderstead"]],
  ["steppe", 244, 87, ["Karakhor", "Falcon Roost", "Windfort", "Horsefair", "Skycross", "Reedplain", "Longgrass", "Kestrel Camp"]],
  ["stone", 159, 32, ["Stoneheart", "Granite Gate", "Peakwatch", "Anvilmarket", "Orebridge", "Deepdelve", "Flintstead", "Silvercrag"]],
  ["suncoast", 51, 183, ["Port Aurelia", "Saltwatch", "Gull Fortress", "Pearlport", "Saffron Quay", "Olivegrove", "Netters Rest", "Tidefield"]],
  ["frost", 257, 29, ["Winterhall", "Icefang", "Ravenwall", "Frostharbor", "Snowmarket", "Pinehearth", "Whitebrook", "Birchstead"]],
  ["desert", 255, 175, ["Qasr Nadiya", "Sandgate", "Sunspire", "Saffron Oasis", "Silkcross", "Datewell", "Duneshade", "Amber Spring"]],
  ["iron", 176, 131, ["Ironspire", "Chainhold", "Black Bastion", "Forgecross", "Steelmarket", "Cokefield", "Slagbrook", "Hammerstead"]],
  ["marsh", 142, 185, ["Fenmeet", "Reedwatch", "Mirewall", "Eelmarket", "Willow Quay", "Sedgehome", "Frogwell", "Rushstead"]],
];
const offsets = [[0, 0], [-16, -11], [16, -9], [-13, 12], [13, 14], [-22, 4], [0, 23], [24, 6]];
const kinds = ["capital", "castle", "castle", "town", "town", "village", "village", "village"];
const additions = [];
for (const [faction, x, y, names] of REGIONS) {
  const existing = ORIGINAL_SETTLEMENTS.filter((s) => s.faction === faction);
  const used = { capital: 0, castle: 0, town: 0, village: 0 };
  for (let i = 0; i < kinds.length; i++) {
    const kind = kinds[i];
    if (used[kind]++ < existing.filter((s) => s.kind === kind).length) continue;
    additions.push({ id: names[i].toLowerCase().replace(/[^a-z0-9]+/g, "-"), name: names[i], kind,
      x: x + offsets[i][0] + .5, y: y + offsets[i][1] + .5, faction,
      ...(kind === "castle" || kind === "capital" ? { garrison: kind === "capital" ? 90 : 45 } : {}) });
  }
}
export const SETTLEMENTS = [...ORIGINAL_SETTLEMENTS, ...additions];
export const BIOMES = Object.freeze({ plains: 0, forest: 1, mountain: 2, water: 3, steppe: 4, desert: 5, snow: 6, marsh: 7, coast: 8 });
export const BIOME_NAMES = Object.freeze(Object.fromEntries(Object.entries(BIOMES).map(([name, code]) => [code, name])));
export function createWorld(seed = WORLD.seed) {
  const { width: w, height: h } = WORLD;
  const tiles = new Uint8Array(w * h), costs = new Float32Array(w * h),
    biomes = new Uint8Array(w * h), elevation = new Uint8Array(w * h), props = [], roads = [];
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
    const n = hash(x, y, seed) % 100;
    let t = 0, biome = 0;
    if (x < 128 && y < 88) {
      // Preserve the original region, including its seeded obstacles.
      const forest = Math.sin(x * .15) + Math.cos(y * .21) > .65,
        rock = Math.sin(x * .13 - y * .19) > .85;
      t = forest ? 1 : 0;
      if (rock && n < 50) t = 2;
      if (y < 9) t = 6;
      if (x > 85 && y > 55) t = 5;
      biome = t;
    } else {
      const grove = Math.sin(x * .035 + seed * .0001) + Math.cos(y * .045) + Math.sin((x + y) * .019);
      if (grove > .7) { t = 1; biome = BIOMES.forest; }
      if (x > 205 && y > 55 && y < 135) { t = 0; biome = BIOMES.steppe; }
      if (x > 208 && y > 135) { t = 5; biome = BIOMES.desert; }
      if (y < 43) { t = 6; biome = BIOMES.snow; }
      if (x > 108 && x < 190 && y > 162) { t = n < 17 ? 3 : 0; biome = BIOMES.marsh; }
      const ridge = 151 + Math.sin(y * .045) * 13;
      if ((Math.abs(x - ridge) < 6 || (y < 64 && Math.abs(y - (46 + Math.sin(x * .06) * 8)) < 3)) && n < 72) {
        t = 2; biome = BIOMES.mountain;
      }
      if (x > 308 + Math.sin(y * .06) * 3 || y > 215 + Math.sin(x * .05) * 3) { t = 3; biome = BIOMES.coast; }
    }
    const river = 71 + Math.round(Math.sin(y * .1) * 2), easternRiver = 205 + Math.round(Math.sin(y * .045) * 7);
    if (Math.abs(x - river) <= 1 || (x >= 128 && Math.abs(x - easternRiver) <= 1)) { t = 3; biome = BIOMES.water; }
    const at = y * w + x;
    tiles[at] = t; biomes[at] = biome;
    elevation[at] = t === 2 ? 4 : t === 3 ? 0 : 1 + (hash(Math.floor(x / 9), Math.floor(y / 9), seed) % 3);
    costs[at] = t === 3 || t === 2 ? 0 : biome === BIOMES.marsh ? 1.9 : t === 1 ? 1.6 : t === 6 || t === 5 ? 1.4 : 1;
    if (t === 1 && n < 22) { props.push({ x: x + .5, y: y + .5, type: 8 }); costs[at] = 0; }
    if (t === 2 && n < 25) props.push({ x: x + .5, y: y + .5, type: 9 });
  }
  const road = (a, b, legacy = false) => {
    let x = Math.floor(a.x), y = Math.floor(a.y), points = [{ x: x + .5, y: y + .5 }];
    const bx = Math.floor(b.x), by = Math.floor(b.y);
    for (let i = 0; i < w + h; i++) {
      for (let dy = -1; dy <= 1; dy++) for (let dx = -1; dx <= 1; dx++) {
        const nx = x + dx, ny = y + dy;
        if (nx < 0 || ny < 0 || nx >= w || ny >= h) continue;
        const at = ny * w + nx;
        tiles[at] = tiles[at] === 3 || tiles[at] === 7 ? 7 : 4;
        costs[at] = .75;
      }
      if (x === bx && y === by) break;
      if (legacy) { if (x !== bx) x += Math.sign(bx - x); else y += Math.sign(by - y); }
      else { x += Math.sign(bx - x); y += Math.sign(by - y); }
      points.push({ x: x + .5, y: y + .5 });
    }
    return points;
  };
  road({ x: 2, y: 44 }, { x: 125, y: 44 }, true);
  for (const s of ORIGINAL_SETTLEMENTS) road(s, { x: s.x, y: 44 }, true);
  // A connected backbone bounds long-distance navigation to short local searches.
  // Each new site connects to its nearest existing site; a second link prevents
  // the expanded continent from becoming a single corridor.
  for (let i = 1; i < SETTLEMENTS.length; i++) {
    const a = SETTLEMENTS[i];
    const near = SETTLEMENTS.slice(0, i).map((b, j) => ({ b, j, d: Math.hypot(a.x - b.x, a.y - b.y) }))
      .sort((a, b) => a.d - b.d || a.j - b.j).slice(0, i < 10 ? 1 : 2);
    for (const { b, j } of near) roads.push({ a: i, b: j, points: road(a, b) });
  }
  const world = { width: w, height: h, seed, tiles, costs, biomes, elevation,
    version: 2, worldVersion: 2, roads,
    props: props.filter((p) => costs[Math.floor(p.y) * w + Math.floor(p.x)] === 0 && tiles[Math.floor(p.y) * w + Math.floor(p.x)] === p.type - 7),
    settlements: SETTLEMENTS.map((s) => ({ ...s, baseFaction: s.faction, currentFaction: s.faction,
      owner: null, level: 1, garrison: s.garrison || 0, prosperity: 1, loyalty: 100,
      resource: FACTIONS.find((f) => f.id === s.faction).economy[hash(Math.floor(s.x), Math.floor(s.y), seed) % 2],
      food: s.kind === "village" ? 90 : 55, supplies: 100, recruitment: s.kind === "capital" ? 3 : s.kind === "castle" ? 2 : 1,
      production: 1, tradeIncome: 0 })),
  };
  world.propChunks = new Map();
  for (const prop of world.props) {
    const key = `${Math.floor(prop.x / 12)},${Math.floor(prop.y / 12)}`;
    if (!world.propChunks.has(key)) world.propChunks.set(key, []);
    world.propChunks.get(key).push(prop);
  }
  world.nav = new Navigator(world);
  return world;
}
const LORD_NAMES = {
  crown: ["Sir Rowan", "Lady Beatrice", "Lord Osric"], ember: ["Lord Varric", "Lady Ashara", "Sir Calder"],
  vale: ["Lady Thalia", "Lord Alder", "Warden Sylven"], steppe: ["Noyan Arslan", "Lady Saran", "Noyan Batu"],
  stone: ["Thane Durn", "Lady Brynja", "Thane Orin"], suncoast: ["Captain Lucio", "Lady Mira", "Captain Dante"],
  frost: ["Jarl Eirik", "Jarl Astrid", "Jarl Ulf"], desert: ["Emir Farid", "Lady Samira", "Emir Khalil"],
  iron: ["Legate Draven", "Lady Iskra", "Legate Mordren"], marsh: ["Warden Rook", "Lady Sedge", "Warden Moss"],
};
export function createNPCs(world, seed = world.seed) {
  const random = rng(seed), npcs = [], lordCounts = Object.fromEntries(FACTIONS.map((f) => [f.id, 0]));
  function add(kind, home, id, target) {
    const pos = world.nav.nearest(home.x + (random() - .5) * 8, home.y + (random() - .5) * 8) || { x: 15, y: 44 };
    const size = kind === "bandit" ? 5 + Math.floor(random() * 15) : kind === "patrol" ? 25 + Math.floor(random() * 28) : 4;
    const lordIndex = kind === "patrol" ? lordCounts[home.faction]++ : 0;
    const faction = FACTIONS.find((f) => f.id === home.faction);
    const n = { id, kind, name: kind === "bandit" ? "Outlaw company" : kind === "patrol" ? LORD_NAMES[home.faction][lordIndex % 3] : `${home.name} caravan`,
      faction: kind === "bandit" ? "outlaw" : home.faction, x: pos.x + .5, y: pos.y + .5,
      home: home.id, target: target.id, army: kind === "patrol" ? battleArmy(size, home.faction) : battleArmy(size),
      route: [], routeIndex: 0, respawn: 0, decision: random() * 5, supplies: 100,
      ...(kind === "patrol" ? { lord: true, leaderId: `${home.faction}-${lordIndex}`, doctrine: faction.doctrine, personality: faction.personality, loyalty: 80 + Math.floor(random() * 20) } : {}),
      ...(kind === "merchant" ? { cargo: 40, goods: home.resource || "food", deliveryCount: 0, deliveredAt: 0, tradeIncome: 0 } : {}) };
    npcs.push(n);
  }
  // Retain the original IDs so active encounters and saved NPCs still resolve.
  for (let i = 0; i < 28; i++) {
    const kind = i < 13 ? "bandit" : i < 20 ? "patrol" : "merchant";
    const towns = world.settlements.filter((s) => s.kind === "town" || s.kind === "capital");
    const home = kind === "merchant" ? towns[(i - 20) % towns.length] : world.settlements[i % 10];
    add(kind, home, `${kind}-${i}`, kind === "merchant" ? towns[i % towns.length] : world.settlements[(i * 3 + 1) % 10]);
  }
  for (const faction of FACTIONS) while (lordCounts[faction.id] < 3) {
    const homes = world.settlements.filter((s) => s.faction === faction.id);
    add("patrol", homes.find((s) => s.kind === "capital"), `patrol-${npcs.length}`, homes[lordCounts[faction.id] % homes.length]);
  }
  for (let i = 13; i < 24; i++) {
    const home = world.settlements[(i * 7) % world.settlements.length];
    add("bandit", home, `bandit-${i}`, home);
  }
  const towns = world.settlements.filter((s) => s.kind === "town" || s.kind === "capital");
  for (let i = 8; i < 20; i++) add("merchant", towns[(i * 3) % towns.length], `merchant-${npcs.length}`, towns[(i * 3 + 7) % towns.length]);
  return npcs;
}
export function travel(entity, nav, dt, speed) {
  let remaining = dt * speed, moved = false;
  while (entity.routeIndex < (entity.route?.length || 0) && remaining > 0) {
    const t = entity.route[entity.routeIndex], dx = t.x - entity.x, dy = t.y - entity.y, d = Math.hypot(dx, dy);
    const cost = nav.map.costs[Math.floor(entity.y) * nav.map.width + Math.floor(entity.x)] || 1, step = remaining / cost;
    if (d <= step) { entity.x = t.x; entity.y = t.y; remaining -= d * cost; entity.routeIndex++; }
    else { entity.x += dx / d * step; entity.y += dy / d * step; remaining = 0; }
    moved = true;
  }
  return moved;
}
const effectiveFaction = (s) => s.currentFaction || s.faction;
const hostile = (world, a, b) => a !== b && !!world.kingdoms?.relations?.[[a, b].sort().join(":")]?.war;
export function tickNPCs(world, npcs, players, dt, time) {
  const byId = new Map(world.settlements.map((s) => [s.id, s]));
  const active = npcs.filter((n) => !n.battleId && !n.capturedBy && !(n.respawn > time));
  let planned = 0;
  const cursor = world.npcPlanCursor || 0;
  for (let i = 0; i < active.length; i++) {
    const n = active[(i + cursor) % active.length];
    const close = players.some((p) => Math.hypot(p.x - n.x, p.y - n.y) < 30);
    n.simulation = close ? "near" : "strategic";
    n.decision -= dt;
    if (n.decision <= 0 && planned < 4) {
      planned++; n.decision = close ? 2 : 8;
      let target = byId.get(n.target) || byId.get(n.home) || world.settlements[0];
      if (n.kind === "merchant" && !["capital", "town"].includes(target.kind)) {
        target = world.settlements.filter((s) => ["capital", "town"].includes(s.kind))
          .sort((a, b) => Math.hypot(a.x - n.x, a.y - n.y) - Math.hypot(b.x - n.x, b.y - n.y))[0] || target;
      }
      if (n.kind === "bandit") {
        const danger = active.find((o) => o.kind === "patrol" && Math.hypot(o.x - n.x, o.y - n.y) < 7 && armySize(o.army) > armySize(n.army));
        const prey = players.find((p) => !p.activeBattle && Math.hypot(p.x - n.x, p.y - n.y) < 9 && armySize(p.army) < armySize(n.army) * 1.4)
          || active.find((o) => o.kind === "merchant" && Math.hypot(o.x - n.x, o.y - n.y) < 12);
        if (danger) {
          const flee = world.nav.nearest(n.x + (n.x - danger.x) * 2, n.y + (n.y - danger.y) * 2);
          if (flee) target = { x: flee.x + .5, y: flee.y + .5, id: "escape" };
        } else if (prey) {
          target = prey;
          if (prey.kind === "merchant" && Math.hypot(n.x - prey.x, n.y - prey.y) < 1.2 && (n.raidAt || 0) <= time) {
            n.loot = (n.loot || 0) + (prey.cargo || 0); prey.cargo = 0; prey.respawn = time + 60;
            n.raidAt = time + 60;
            const home = byId.get(prey.home); if (home) { prey.x = home.x; prey.y = home.y; prey.route = []; prey.routeIndex = 0; }
          }
        } else {
          const village = world.settlements.filter((s) => s.kind === "village" && !s.battleId)
            .sort((a, b) => Math.hypot(a.x - n.x, a.y - n.y) - Math.hypot(b.x - n.x, b.y - n.y))[0];
          if (village) target = village;
          if (village && Math.hypot(village.x - n.x, village.y - n.y) < 1 && time - (village.lastRaided || -120) >= 90 && (n.raidAt || 0) <= time) {
            village.lastRaided = time; village.production = Math.max(.3, (village.production || 1) - .15);
            n.raidAt = time + 60; village.damage = Math.min(90, (village.damage || 0) + 18);
            village.recruitStock = Math.max(0, (village.recruitStock ?? 30) - 5);
            village.food = Math.max(0, (village.food || 0) - 15); village.prosperity = Math.max(.3, village.prosperity - .1);
          }
        }
      } else if (n.kind === "patrol") {
        const invaded = world.settlements.find((s) => s.faction === n.faction && s.owner && (s.ownerReputation ?? -30) <= -25);
        const rival = world.settlements.filter((s) => hostile(world, n.faction, effectiveFaction(s)))
          .sort((a, b) => Math.hypot(a.x - n.x, a.y - n.y) - Math.hypot(b.x - n.x, b.y - n.y))[0];
        const raider = active.find((o) => o.kind === "bandit" && Math.hypot(o.x - n.x, o.y - n.y) < 11);
        if (invaded) target = invaded; else if (raider) target = raider; else if (rival) target = rival;
        else if (Math.hypot(n.x - target.x, n.y - target.y) < 1) {
          const homes = world.settlements.filter((s) => effectiveFaction(s) === n.faction);
          target = homes[(Math.floor(time / 19) + hash(n.id.length, n.leaderId?.length || 0)) % homes.length] || target;
        }
        if (raider && target === raider && Math.hypot(n.x - raider.x, n.y - raider.y) < 1.1 && armySize(n.army) > armySize(raider.army)) raider.respawn = time + 90;
      } else if (n.kind === "merchant" && Math.hypot(n.x - target.x, n.y - target.y) < 1) {
        const delivered = Math.max(0, n.cargo || 0);
        n.deliveryCount = (n.deliveryCount || 0) + 1; n.deliveredAt = time;
        n.tradeIncome = (n.tradeIncome || 0) + delivered; target.tradeIncome = (target.tradeIncome || 0) + delivered;
        target.prosperity = Math.min(2, target.prosperity + delivered / 2000);
        n.cargo = 40; n.goods = target.resource; n.lastStop = target.id;
        const towns = world.settlements.filter((s) => (s.kind === "town" || s.kind === "capital") && s.id !== target.id && !hostile(world, n.faction, effectiveFaction(s)));
        target = towns[(n.deliveryCount + hash(n.id.length, Math.floor(time / 13))) % towns.length] || byId.get(n.home) || target;
      }
      if (target.id && byId.has(target.id)) n.target = target.id;
      const destinationChanged = !n.destination || Math.hypot(n.destination.x - target.x, n.destination.y - target.y) > 2;
      if (destinationChanged || !n.route?.length || n.routeIndex >= n.route.length) {
        n.route = world.nav.path(n, target); n.routeIndex = 0; n.destination = { x: target.x, y: target.y };
      }
    }
    travel(n, world.nav, dt, n.kind === "merchant" ? 1.15 : n.kind === "patrol" ? 1.6 : 1.9);
  }
  world.npcPlanCursor = active.length ? (cursor + Math.max(1, planned)) % active.length : 0;
}
export function battleArmy(size, faction = "outlaw") {
  const army = emptyArmy(), f = FACTIONS.find((f) => f.id === faction);
  // Shared troops anchor the line; doctrine determines the majority of specialists.
  const roster = f ? [...f.units, ...f.signatureUnits, ...f.signatureUnits] : ["recruits", "footmen", "archers"];
  for (let i = 0; i < clamp(Math.floor(size) || 0, 0, 600); i++) army[roster[i % roster.length]]++;
  return army;
}
