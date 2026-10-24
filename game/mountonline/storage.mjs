import {
  VERSION,
  WORLD,
  FACTIONS,
  newProfile,
  safeArmy,
  armySize,
  armyCap,
  clamp,
  clone,
  UNITS,
  ORDERS,
} from "./data.mjs";
import { Realm } from "./realm.mjs";
const finite = (v, a, b) => Number.isFinite(v) && v >= a && v <= b;
export function serializeCampaign(realm, uid) {
  return JSON.stringify({
    version: VERSION,
    profile: realm.player(uid),
    state: realm.checkpoint(),
    battles: [...realm.battles.values()].filter(
      (b) => !b.settled || b.id === realm.player(uid).lastBattle?.id,
    ),
  });
}
export function parseCampaign(text) {
  if (typeof text !== "string" || text.length > 2500000)
    throw new Error("This campaign file is too large.");
  const raw = JSON.parse(text),
    p = raw.profile;
  if (
    raw.version !== VERSION ||
    !p ||
    !finite(p.x, 0, WORLD.width - 0.001) ||
    !finite(p.y, 0, WORLD.height - 0.001) ||
    !finite(p.gold, 0, 1e9) ||
    !finite(p.level, 1, 100)
  )
    throw new Error("This is not a compatible Mountain Blade campaign.");
  const clean = {
    ...newProfile(String(p.username || "Wanderer").slice(0, 40)),
    ...p,
    uid: "local",
    army: safeArmy(p.army),
    reserve: safeArmy(p.reserve),
    supplies: clamp(Number(p.supplies) || 0, 0, 200),
    morale: clamp(Number(p.morale) || 50, 20, 100),
  };
  if (armySize(clean.army) > 600)
    throw new Error("This campaign exceeds the battlefield army limit.");
  clean.inventory = {
    banners: Array.isArray(p.inventory?.banners)
      ? p.inventory.banners.slice(0, 100)
      : [],
    equipped: Array.isArray(p.inventory?.equipped)
      ? p.inventory.equipped.filter((k) => typeof k === "string").slice(0, 3)
      : [],
  };
  clean.route = [];
  clean.routeIndex = 0;
  clean.explored = Array.isArray(p.explored)
    ? p.explored.filter((n) => Number.isInteger(n) && n >= 0 && n < Math.ceil(WORLD.width / 8) * Math.ceil(WORLD.height / 8)).slice(-1200)
    : [];
  const state = raw.state || {};
  if (
    !finite(state.time, 0, 1e10) ||
    !Array.isArray(state.settlements) ||
    state.settlements.length > 160
  )
    throw new Error("The saved realm is invalid.");
  for (const s of state.settlements) {
    if (
      typeof s.id !== "string" || s.id.length > 80 ||
      !finite(s.x, 0, WORLD.width - 0.001) ||
      !finite(s.y, 0, WORLD.height - 0.001) ||
      !finite(s.garrison, 0, 400) ||
      !["castle", "village", "town", "capital"].includes(s.kind)
    )
      throw new Error("A saved settlement is invalid.");
    if (s.owner === p.uid) s.owner = "local";
  }
  // Overlay saved strategic changes on the generated continent. Old campaigns
  // keep their holdings while newly introduced settlements and lords still exist.
  const realm = new Realm({ time: state.time, sequence: finite(state.sequence, 0, 1e10) ? state.sequence : 0,
    seed: finite(state.seed, 0, 4294967295) ? state.seed : WORLD.seed,
    settlements: state.settlements.map((s) => ({ ...s, name: String(s.name).slice(0, 80), garrisonArmy: safeArmy(s.garrisonArmy),
      level: clamp(Number(s.level) || 1, 1, 5), damage: clamp(Number(s.damage) || 0, 0, 100),
      loyalty: clamp(Number(s.loyalty ?? 100), 0, 100), supplies: clamp(Number(s.supplies ?? 100), 0, 100),
    })), kingdoms: state.kingdoms,
    alliances: Array.isArray(state.alliances) ? state.alliances.slice(0, 100) : [],
  });
  realm.enter("local", clean);
  if (Array.isArray(state.npcs) && state.npcs.length <= 160) {
    for (const saved of state.npcs) {
      const n = realm.npcs.find((n) => n.id === saved.id);
      if (n && finite(saved.x, 0, WORLD.width - 0.001) && finite(saved.y, 0, WORLD.height - 0.001)) {
        n.x = saved.x;
        n.y = saved.y;
        n.army = safeArmy(saved.army);
        n.respawn = finite(saved.respawn, 0, 1e10) ? saved.respawn : 0;
        n.battleId = saved.battleId || null;
        n.target = typeof saved.target === "string" ? saved.target : n.target;
        n.capturedBy = saved.capturedBy === p.uid ? "local" : typeof saved.capturedBy === "string" ? saved.capturedBy.slice(0, 128) : null;
        n.capturedUntil = finite(saved.capturedUntil, 0, 1e10) ? saved.capturedUntil : 0;
        n.deliveryCount = finite(saved.deliveryCount, 0, 1e9) ? Math.floor(saved.deliveryCount) : 0;
        n.raidAt = finite(saved.raidAt, 0, 1e10) ? saved.raidAt : 0;
        n.route = [];
        n.routeIndex = 0;
      }
    }
  }
  for (const b of raw.battles || []) {
    if (
      !Array.isArray(b.units) ||
      b.units.length > 1200 ||
      !finite(b.time, 0, 310) ||
      !finite(b.seed, 0, 4294967295)
    )
      throw new Error("The saved battle is invalid.");
    const seen = new Set();
    for (const u of b.units) {
      if (
        !UNITS[u.key] ||
        !Number.isInteger(u.id) ||
        seen.has(u.id) ||
        ![0, 1].includes(u.team) ||
        !finite(u.x, 0, 64) ||
        !finite(u.y, 0, 40) ||
        !finite(u.hp, 0, UNITS[u.key].hp) ||
        !ORDERS.includes(u.order?.kind)
      )
        throw new Error("A saved troop is invalid.");
      seen.add(u.id);
      u.path = null;
      u.px = u.x;
      u.py = u.y;
    }
    b.participants = b.participants.map((uid) =>
      uid === p.uid ? "local" : uid,
    );
    realm.battles.set(b.id, b);
  }
  if (clean.activeBattle && !realm.battles.has(clean.activeBattle))
    throw new Error("The saved battle checkpoint is missing.");
  return realm;
}
export function readCampaign(key) {
  for (const path of [key, key + ":backup"]) {
    try {
      const text = localStorage.getItem(path);
      if (text) return parseCampaign(text);
    } catch {}
  }
  return null;
}
export function saveCampaign(key, realm, uid) {
  const text = serializeCampaign(realm, uid),
    previous = localStorage.getItem(key);
  if (previous) localStorage.setItem(key + ":backup", previous);
  localStorage.setItem(key, text);
  return text;
}
