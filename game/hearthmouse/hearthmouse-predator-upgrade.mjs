import "./hearthmouse-house-evolution.mjs";
import { stableHash } from "./hearthmouse-performance-manager.mjs";

const distance = (a, b) => Math.hypot(a.x - b.x, a.z - b.z);
const silent = () => {};
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));

export function rememberObservation(hunt, target, now) {
  if (!target?.position || !(target.visible > 0)) return false;
  const dt = now - hunt.seenAt;
  const sameTarget = hunt.targetId === target.id;
  if (sameTarget && dt > 0.015 && dt < 0.6) {
    hunt.vx = clamp((target.position.x - hunt.seen.x) / dt, -1.8, 1.8);
    hunt.vz = clamp((target.position.z - hunt.seen.z) / dt, -1.8, 1.8);
  } else if (!sameTarget || dt >= 0.6) hunt.vx = hunt.vz = 0;
  hunt.seen.copy(target.position);
  hunt.seenAt = now;
  hunt.targetId = target.id;
  return true;
}

export function observedIntercept(hunt, now, output) {
  output.copy(hunt.seen);
  if (now - hunt.seenAt > 0.28 || now < hunt.seenAt) return output;
  const speed = Math.hypot(hunt.vx, hunt.vz);
  const lead = Math.min(0.48, 0.8 / Math.max(0.01, speed));
  output.x += hunt.vx * lead;
  output.z += hunt.vz * lead;
  return output;
}

export function chooseHuntMode({ distance: range, roll, anotherAmbusher = false, moving = 0 }) {
  if (range < 1.8 || range > 6.5) return "chase";
  if (!anotherAmbusher && roll < 0.18) return "ambush";
  if (roll < 0.48) return "stalk";
  return moving > 0.25 && roll < 0.8 ? "intercept" : "chase";
}

function huntFor(cat) {
  return cat.hunt ??= {
    mode: "patrol", targetId: null, seen: cat.lastSeen.clone(), seenAt: -Infinity,
    vx: 0, vz: 0, goal: cat.lastSeen.clone(), scratch: cat.lastSeen.clone(),
    until: 0, nextDecision: 0, nextPlan: 0, foodUntil: 0, nextGuard: 12 + stableHash(cat.id) % 12,
    encounter: 0, stepAt: 0, cooldown: 0,
  };
}

function stopStalking(engine, cat, chase) {
  const h = huntFor(cat);
  h.mode = chase ? "chase" : "search";
  h.cooldown = engine.time + 12;
  h.until = 0;
  engine.setCatState(cat, chase ? "chase" : "search", chase ? 0 : 6.5);
  if (!chase) {
    cat.targetId = null;
    cat.lastSeen.copy(h.seen);
    cat.investigation.copy(h.seen);
    engine.planCatPath(cat, h.seen);
  }
}

function coverGoal(engine, cat, I) {
  const h = huntFor(cat), origin = cat.rig.root.position;
  let checked = 0, best = Infinity, found = false;
  for (const box of engine.world.colliders) {
    if (box.active === false || box.catOnly || box.maxY < 0.25) continue;
    const cx = (box.minX + box.maxX) / 2, cz = (box.minZ + box.maxZ) / 2;
    if (Math.hypot(cx - h.seen.x, cz - h.seen.z) > 4.5) continue;
    if (++checked > 18) break;
    for (const x of [box.minX - 0.36, box.maxX + 0.36]) {
      for (const z of [box.minZ - 0.36, box.maxZ + 0.36]) {
        h.scratch.set(x, 0, z);
        const score = distance(origin, h.scratch) + distance(h.seen, h.scratch) * 0.45;
        if (score >= best || distance(h.seen, h.scratch) < 1.1) continue;
        if (!I.lineClear(origin, h.scratch, 0.23, engine.world.colliders, "cat")) continue;
        if (I.lineClear(h.scratch, h.seen, 0.055, engine.world.colliders, "mouse")) continue;
        h.goal.copy(h.scratch); best = score; found = true;
      }
    }
  }
  return found;
}

export function installPredatorUpgrade(I = globalThis.window?.HearthmouseInternals) {
  const p = I?.Engine?.prototype;
  if (!p?.__catPressureHotfixInstalled || !p.__livingHouseInstalled || !p.__houseEvolutionInstalled) return false;
  if (p.__livingPredatorsInstalled) return true;
  Object.defineProperty(p, "__livingPredatorsInstalled", { value: true });
  const base = Object.fromEntries(["processCatVision", "updateCatSearchMotion", "updateCatChase", "planCatPath", "updateCatPatrol", "updateCats", "mouseTakeFood", "playerTakeFood", "catHeadLook"].map(k => [k, p[k]]));

  p.processCatVision = function rememberedPredatorVision(cat, target, interval) {
    const h = huntFor(cat), now = this.time;
    rememberObservation(h, target, now);
    if (h.mode === "stalk" || h.mode === "ambush") {
      if (target?.id === h.targetId && target.visible > 0) {
        cat.lastSeen.copy(h.seen); cat.investigation.copy(h.seen);
        if (target.distance < 1.65 || now >= h.until) stopStalking(this, cat, true);
      } else if (now - h.seenAt > 2.3 || now >= h.until) stopStalking(this, cat, false);
      return;
    }
    if (target && cat.state !== "chase" && !cat.__tunnelStalk && cat.awareness >= 0.22 && now >= h.cooldown) {
      const roll = (stableHash(`${cat.id}:${this.snapshot.night}:${h.encounter++}`) % 1000) / 1000;
      const mode = chooseHuntMode({ distance: target.distance, roll, moving: target.moving,
        anotherAmbusher: this.cats.some(other => other !== cat && other.hunt?.mode === "ambush") });
      h.mode = mode;
      h.cooldown = now + 12;
      if (mode === "stalk" || mode === "ambush") {
        h.until = now + (mode === "ambush" ? 5 : 6.5);
        h.nextPlan = 0;
        cat.targetId = target.id;
        cat.lastSeen.copy(h.seen); cat.investigation.copy(h.seen);
        this.setCatState(cat, "suspicious", 8);
        cat.path = []; cat.pathIndex = 0;
        if (mode === "ambush" && !coverGoal(this, cat, I)) h.mode = "stalk";
        return;
      }
    }
    // Seeing the same target must not discard a valid path every vision sample.
    const wasChasing = cat.state === "chase" && cat.targetId === target?.id;
    const pathTimer = cat.pathTimer;
    const result = base.processCatVision.call(this, cat, target, interval);
    if (wasChasing && cat.state === "chase" && cat.path?.length && cat.pathIndex < cat.path.length) cat.pathTimer = pathTimer;
    return result;
  };

  p.updateCatSearchMotion = function stalkingSearch(cat, delta, center, speed) {
    const h = huntFor(cat), now = this.time;
    if (cat.__tunnelStalk) return base.updateCatSearchMotion.call(this, cat, delta, center, speed);
    if (h.mode !== "stalk" && h.mode !== "ambush") return base.updateCatSearchMotion.call(this, cat, delta, center, speed);
    if (now >= h.until || now - h.seenAt > 2.3) { stopStalking(this, cat, false); return 0; }
    if (h.mode === "stalk") {
      const range = distance(cat.rig.root.position, h.seen);
      if (range < 2.1 && now - h.seenAt < 0.35) return 0;
      h.goal.copy(h.seen);
    } else if (distance(cat.rig.root.position, h.goal) < 0.3) {
      // Hold cover briefly; vision and the finite deadline decide when to leave.
      cat.path = []; cat.pathIndex = 0;
      return 0;
    }
    if (now >= h.nextPlan) {
      h.nextPlan = now + 0.72 + (stableHash(cat.id) % 5) * 0.035;
      this.planCatPath(cat, h.goal);
      if (!cat.pathReachable) { stopStalking(this, cat, false); return 0; }
    }
    return this.followCatPath(cat, delta, h.mode === "stalk" ? 0.5 : 0.72);
  };

  p.planCatPath = function perceptualInterception(cat, target) {
    const h = huntFor(cat);
    if (cat.state === "chase" && h.mode === "intercept" && this.time - h.seenAt < 0.28 && distance(target, h.seen) < 0.18) {
      observedIntercept(h, this.time, h.scratch);
      if (I.lineClear(h.seen, h.scratch, 0.23, this.world.colliders, "cat")) target = h.scratch;
    }
    return base.planCatPath.call(this, cat, target);
  };

  p.updateCatChase = function fairChaseMemory(cat, delta) {
    const result = base.updateCatChase.call(this, cat, delta);
    if (cat.state !== "chase") { const h = huntFor(cat); h.mode = "search"; h.cooldown = this.time + 9; }
    return result;
  };

  p.catHeadLook = function lookAtKnownPosition(cat) {
    if (cat.state !== "chase" || cat.lostSightTimer <= 0) return base.catHeadLook.call(this, cat);
    const desired = this.yawToward(cat.rig.root.position, cat.lastSeen) - cat.yaw;
    return clamp(Math.atan2(Math.sin(desired), Math.cos(desired)), -0.88, 0.88);
  };

  p.updateCatPatrol = function briefFoodWatch(cat, delta, speedOverride) {
    const h = huntFor(cat), now = this.time;
    if (cat.state !== "relaxed") { h.foodUntil = 0; return base.updateCatPatrol.call(this, cat, delta, speedOverride); }
    if (h.foodUntil > now) {
      if (distance(cat.rig.root.position, h.goal) < 0.7) return 0;
      return this.followCatPath(cat, delta, 0.56);
    }
    if (h.mode === "food-watch") { h.mode = "patrol"; cat.path = []; cat.pathIndex = 0; }
    if (now >= h.nextGuard) {
      h.nextGuard = now + 28 + stableHash(`${cat.id}:${Math.floor(now)}`) % 18;
      // One food watcher at a time, and never the food immediately outside home.
      if (!this.cats.some(other => other !== cat && other.hunt?.foodUntil > now)) {
        const food = this.foods.find(f => !f.deposited && !f.carriedBy && f.mesh.visible && f.exposure === "open" &&
          distance(f.mesh.position, cat.rig.root.position) < 5 && distance(f.mesh.position, this.world.nestCenter) > 3);
        if (food) {
          h.goal.copy(food.mesh.position);
          this.planCatPath(cat, h.goal);
          if (cat.pathReachable) { h.mode = "food-watch"; h.foodUntil = now + 4.5; cat.leisureMode = null; cat.leisureTimer = 0; }
        }
      }
    }
    return base.updateCatPatrol.call(this, cat, delta, speedOverride);
  };

  p.updateCats = function quietPredators(delta) {
    // The old loop emitted paw audio for stationary cats. Keep the cadence local
    // to each moving cat, and suppress bells during quiet hunts.
    const audio = this.audio;
    const paw = audio.catPaw, bell = audio.bell;
    audio.catPaw = audio.bell = silent;
    try { base.updateCats.call(this, delta); }
    finally { audio.catPaw = paw; audio.bell = bell; }
    for (const cat of this.cats) {
      const h = huntFor(cat), quiet = h.mode === "stalk" || h.mode === "ambush";
      if (cat.speed < 0.06 || this.time < h.stepAt) continue;
      h.stepAt = this.time + (quiet ? 0.8 : cat.state === "chase" ? 0.22 : 0.55);
      const range = distance(cat.rig.root.position, this.playerPosition);
      if (range < (quiet ? 2.6 : 6)) paw?.call(audio, quiet ? range + 3 : range, cat.state === "chase");
    }
  };

  const mouseCaught = p.mouseCaught;
  if (typeof mouseCaught === "function") p.mouseCaught = function settleDroppedFood(mouse, cat) {
    const food = mouse.carriedFood;
    const result = mouseCaught.call(this, mouse, cat);
    if (food && !food.carriedBy && !food.deposited) {
      food.mesh.matrixAutoUpdate = true;
      (this.__settlingFood ??= []).push({ food, startedAt: this.time, roll: food.mesh.rotation.z });
    }
    return result;
  };

  for (const method of ["mouseTakeFood", "playerTakeFood"]) {
    p[method] = function foodPickupTransition(...args) {
      const food = method === "mouseTakeFood" ? args[1] : args[0];
      if (food) food.__pickupTime = this.time;
      return base[method].apply(this, args);
    };
  }
  return true;
}

function installWhenReady(attempt = 0) {
  if (typeof window === "undefined") return;
  if (!installPredatorUpgrade() && attempt < 300) window.setTimeout(() => installWhenReady(attempt + 1), 40);
}
installWhenReady();
