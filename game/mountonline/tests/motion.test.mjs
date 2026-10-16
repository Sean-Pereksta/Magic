import test from "node:test";
import assert from "node:assert/strict";
import { RemoteMotion } from "../motion.mjs";

function snapshot(x, overrides = {}) {
  return {
    time: 0,
    sequence: 0,
    syncInterval: 1200,
    profile: {
      uid: "captain",
      username: "Captain",
      x,
      y: 20,
      gold: 260,
      route: [],
    },
    npcs: [{ id: "patrol", x: x + 2, y: 22 }],
    others: [{ id: "ally", x: x + 4, y: 24 }],
    ...overrides,
  };
}
function battle(x, id = "field") {
  return {
    id,
    seed: 1,
    time: 0,
    units: [
      {
        id: 0,
        key: "recruits",
        team: 0,
        x,
        y: 20,
        px: x,
        py: 20,
        hp: 50,
        state: "move",
        facing: 1,
      },
    ],
    projectiles: [],
    effects: [],
  };
}
function advance(motion, from, to, hz = 60) {
  const step = 1000 / hz;
  for (let t = from + step; t < to; t += step) motion.sample(t);
  return motion.sample(to);
}
function freeze(value) {
  if (value && typeof value === "object") {
    Object.freeze(value);
    for (const child of Object.values(value)) freeze(child);
  }
  return value;
}

test("early and burst Firestore updates continue from the displayed position, without rewinding to saved coordinates", () => {
  const m = new RemoteMotion();
  m.accept(snapshot(10), null, 0);
  m.accept(snapshot(13), null, 1200);
  advance(m, 1200, 1500);
  const before = m.state.profile.x;
  assert.ok(before > 10 && before < 11);
  m.accept(snapshot(16), null, 1500);
  assert.equal(m.state.profile.x, before);
  assert.ok(
    m.state.profile.x < 13,
    "the old saved position must not become the new visual start",
  );
  advance(m, 1500, 1600);
  const mid = m.state.profile.x;
  m.accept(snapshot(17), null, 1600);
  assert.equal(m.state.profile.x, mid);
  const poses = [];
  for (let t = 1616; t < 2900; t += 16) {
    m.sample(t);
    poses.push(m.state.profile.x);
  }
  assert.ok(poses.every((x, i) => i === 0 || x >= poses[i - 1]));
  assert.ok(poses.every((x, i) => i === 0 || x - poses[i - 1] < 0.2));
  advance(m, 2900, 3500);
  assert.equal(m.state.profile.x, 17);
});

test("identical position updates do not restart movement; companies stop marching when the glide finishes", () => {
  const m = new RemoteMotion();
  m.accept(snapshot(10), null, 0);
  m.accept(snapshot(13), null, 0);
  for (let t = 100; t <= 1400; t += 100) {
    advance(m, t - 100, t);
    m.accept(snapshot(13, { sequence: t }), null, t);
  }
  assert.equal(m.state.profile.x, 13);
  assert.equal(m.state.profile.renderMoving, false);
  assert.equal(m.state.npcs[0].x, 15);
  assert.equal(m.state.others[0].x, 17);
});

test("battle troops keep their current pose on repeated updates; rendering never changes gameplay data", () => {
  const m = new RemoteMotion();
  const first = freeze(snapshot(10, { syncInterval: 500 })),
    b1 = freeze(battle(10));
  m.accept(first, b1, 0);
  const next = freeze(snapshot(12, { syncInterval: 500 })),
    b2 = freeze(battle(14));
  m.accept(next, b2, 500);
  advance(m, 500, 650);
  const x = m.battle.units[0].x;
  assert.ok(x > 10 && x < 14);
  assert.equal(b2.units[0].x, 14);
  const b3 = freeze(battle(17));
  m.accept(next, b3, 650);
  assert.equal(m.battle.units[0].x, x);
  advance(m, 650, 1500);
  assert.equal(m.battle.units[0].x, 17);
  assert.equal(m.battle.units[0].px, 17);
  assert.equal(b3.units[0].px, 17);
  assert.equal(next.profile.x, 12);
  assert.equal(next.profile.renderMoving, undefined);
});

test("normal 30/60/120 Hz rendering converges consistently and does not overshoot a correction", () => {
  const ends = [];
  for (const hz of [30, 60, 120]) {
    const m = new RemoteMotion();
    m.accept(snapshot(10), null, 0);
    m.accept(snapshot(13), null, 0);
    advance(m, 0, 600, hz);
    m.accept(snapshot(11), null, 600);
    const high = m.state.profile.x;
    for (let t = 600 + 1000 / hz; t < 2200; t += 1000 / hz) {
      m.sample(t);
      assert.ok(
        m.state.profile.x >= Math.min(high, 11) - 0.001 &&
          m.state.profile.x <= Math.max(high, 11) + 0.001,
      );
    }
    ends.push(m.state.profile.x);
  }
  assert.ok(Math.max(...ends) - Math.min(...ends) < 0.01);
});

test("respawns, long reconnects, new identities, and new battle IDs reset stale display poses", () => {
  const m = new RemoteMotion();
  m.accept(snapshot(10), battle(10), 0);
  m.accept(snapshot(80), battle(45), 100);
  assert.equal(m.state.profile.x, 80);
  assert.equal(m.battle.units[0].x, 45);
  m.accept(snapshot(84), battle(46), 6200);
  assert.equal(m.state.profile.x, 84);
  assert.equal(m.battle.units[0].x, 46);
  m.accept(snapshot(85), battle(20, "siege"), 6300);
  assert.equal(m.battle.units[0].x, 20);
  m.accept(
    snapshot(15, {
      profile: { uid: "other", username: "Other", x: 15, y: 20 },
    }),
    null,
    6400,
  );
  assert.equal(m.state.profile.x, 15);
  assert.equal(m.battle, null);
  assert.equal(m.troops.records.size, 0);
});

test("removed actors are released and 1,200 troop display objects are reused across animation frames", () => {
  const m = new RemoteMotion(),
    units = Array.from({ length: 1200 }, (_, id) => ({
      ...battle(10).units[0],
      id,
      x: 10 + (id % 20) / 10,
      y: 5 + Math.floor(id / 20) * 0.4,
    }));
  m.accept(snapshot(10), { ...battle(10), units }, 0);
  const poses = m.battle.units,
    object = poses[100];
  advance(m, 0, 500);
  assert.equal(m.battle.units, poses);
  assert.equal(m.battle.units[100], object);
  m.accept(
    snapshot(10, { npcs: [], others: [] }),
    { ...battle(10), units: units.slice(0, 2) },
    600,
  );
  assert.equal(m.troops.records.size, 2);
  assert.equal(m.world.records.size, 1);
});
