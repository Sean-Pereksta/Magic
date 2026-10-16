// Presentation only. Saved positions, pathfinding, combat and Firestore writes
// remain authoritative. Like Chess Warlords, a new target keeps the position
// currently on screen instead of restarting from the previous saved square.
const EPSILON = 0.0025;
const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));

class MotionGroup {
  constructor(teleportDistance) {
    this.teleportDistance = teleportDistance;
    this.records = new Map();
  }
  accept(actors, interval, now, stale) {
    const live = new Set();
    const rendered = actors.map(([id, actor]) => {
      live.add(id);
      let record = this.records.get(id);
      if (!record) {
        record = {
          display: { ...actor },
          x: actor.x,
          y: actor.y,
          end: now,
          moving: false,
          facing: 1,
        };
        this.records.set(id, record);
      }
      const display = record.display;
      const x = display.x,
        y = display.y;
      const changed =
        Math.hypot(actor.x - record.x, actor.y - record.y) > EPSILON;
      Object.assign(display, actor);
      display.x = x;
      display.y = y;
      if (
        stale ||
        Math.hypot(actor.x - x, actor.y - y) > this.teleportDistance
      ) {
        display.x = actor.x;
        display.y = actor.y;
        record.end = now;
      } else if (changed) record.end = now + interval;
      // Unchanged positions (gold updates, order receipts, etc.) must not keep
      // extending an unfinished glide or rewind it to the server position.
      record.x = actor.x;
      record.y = actor.y;
      record.sourceState = actor.state;
      return display;
    });
    for (const id of this.records.keys())
      if (!live.has(id)) this.records.delete(id);
    return rendered;
  }
  sample(now, dt) {
    for (const record of this.records.values()) {
      const p = record.display;
      const dx = record.x - p.x,
        dy = record.y - p.y;
      const remaining = Math.max(0, record.end - now);
      // Deadline-based motion between normal snapshots, with a frame-rate
      // independent easing tail for late packets and corrections. Never take
      // most of a correction in one render frame.
      const amount =
        remaining > 0
          ? Math.min(0.46, dt / Math.max(dt, remaining))
          : 1 - Math.exp(-dt / 90);
      p.x += dx * amount;
      p.y += dy * amount;
      if (Math.hypot(record.x - p.x, record.y - p.y) < EPSILON) {
        p.x = record.x;
        p.y = record.y;
      }
      p.renderMoving = Math.hypot(record.x - p.x, record.y - p.y) > EPSILON;
      if (Math.abs(dx) > EPSILON) record.facing = dx < 0 ? -1 : 1;
      p.renderFacing = record.facing;
      // The online renderer consumes final display coordinates, so it must not
      // interpolate these again using the local simulation's previous tick.
      p.px = p.x;
      p.py = p.y;
      if (record.sourceState !== undefined) {
        p.state =
          p.hp > 0 &&
          p.renderMoving &&
          (record.sourceState === "idle" ||
            record.sourceState === "move" ||
            record.sourceState === "defend")
            ? "move"
            : record.sourceState;
      }
    }
  }
}

export class RemoteMotion {
  constructor() {
    this.reset();
  }
  reset() {
    this.world = new MotionGroup(20);
    this.troops = new MotionGroup(16);
    this.state = null;
    this.battle = null;
    this.uid = null;
    this.battleId = null;
    this.lastFrame = null;
    this.lastPacket = null;
  }
  accept(state, battle, now = performance.now()) {
    const uid = state.profile.uid || state.profile.username;
    if (this.uid !== uid) this.reset();
    const interval = clamp(Number(state.syncInterval) || 1200, 100, 1800);
    const stale =
      this.lastPacket !== null &&
      now - this.lastPacket > Math.max(5000, interval * 4);
    this.uid = uid;
    this.lastPacket = now;
    this.lastFrame ??= now;
    const sources = [
      ["commander", state.profile],
      ...state.npcs.map((n) => [`npc:${n.id}`, n]),
      ...state.others.map((p) => [`player:${p.id}`, p]),
    ];
    const poses = this.world.accept(sources, interval, now, stale);
    this.state = {
      ...state,
      profile: poses[0],
      npcs: poses.slice(1, 1 + state.npcs.length),
      others: poses.slice(1 + state.npcs.length),
    };
    if ((battle?.id || null) !== this.battleId)
      this.troops = new MotionGroup(16);
    this.battleId = battle?.id || null;
    this.battle = battle
      ? {
          ...battle,
          units: this.troops.accept(
            battle.units.map((u) => [u.id, u]),
            interval,
            now,
            stale,
          ),
        }
      : null;
    this.sample(now);
  }
  sample(now = performance.now()) {
    if (!this.state) return null;
    // A resumed tab should show the current known positions, rather than replay
    // minutes of old movement. Normal live frames keep their continuous poses.
    const elapsed = Math.max(0, now - this.lastFrame);
    const dt = Math.min(elapsed, 50);
    this.lastFrame = now;
    if (elapsed > 2000) {
      for (const group of [this.world, this.troops])
        for (const record of group.records.values()) {
          record.display.x = record.x;
          record.display.y = record.y;
        }
    }
    this.world.sample(now, dt);
    this.troops.sample(now, dt);
    return this;
  }
}
