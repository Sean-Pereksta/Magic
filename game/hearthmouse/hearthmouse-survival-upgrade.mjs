import "./hearthmouse-static-batching.mjs";
import "./hearthmouse-predator-upgrade.mjs";
import { foodLoad, HabitMemory, clamp, distance, darknessVisibility, sweptPawHit } from "./hearthmouse-survival-core.mjs";
import { roomForPoint, stableHash, registerFrameVisualStage, registerCharacterVisualStage } from "./hearthmouse-performance-manager.mjs";
import { rebuildFoodIndexes } from "./hearthmouse-expansion-core.mjs";
import { buildTraversal, moveTraversingPlayer, startContextClimb, advanceTraversal } from "./hearthmouse-survival-traversal.mjs";
import { updateHouseholdEvents, resetHouseholdEvents, roomIllumination } from "./hearthmouse-survival-events.mjs";

function stateFor(engine, I) {
  return engine.__survival ??= { I, memories: new Map(), event: null, eventIndex: 0, nextEvent: engine.time + 24,
    traversal: null, nextLoadHint: 0, ui: null, lastRoom: null, lastCell: null, nextObservation: 0 };
}
function memoryFor(engine, cat, I) {
  const state = stateFor(engine, I);
  let memory = state.memories.get(cat.id);
  if (!memory) state.memories.set(cat.id, memory = new HabitMemory(0.85 + (stableHash(cat.id) % 30) / 100));
  return memory;
}
function observedRecently(engine, cat, position) {
  return cat.hunt?.targetId === "player" && engine.time - cat.hunt.seenAt < 0.26 && distance(cat.hunt.seen, position) < 0.5;
}

export function dropPlayerFood(engine, I) {
  const food = engine.carriedFood;
  if (!food || engine.snapshot.phase !== "foraging") return false;
  // Dropping is immediate, even in a trap or while climbing; no input cooldown.
  const position = engine.playerPosition.clone(); position.y = 0.025 + (engine.playerElevation ?? 0);
  const transit = engine.__livingHouse?.playerTunnelTransit;
  if (transit) position.set(transit.source.x + (transit.source.normalX ?? 0) * 0.24, 0.025, transit.source.z + (transit.source.normalZ ?? 0) * 0.24);
  // A mid-climb drop falls to the nearest supporting surface, never hangs in
  // space or gets stranded inside the tunnel wall.
  let support = 0;
  for (const c of engine.world.colliders) if (c.active !== false && c.maxY <= position.y &&
    position.x >= c.minX && position.x <= c.maxX && position.z >= c.minZ && position.z <= c.maxZ) support = Math.max(support, c.maxY);
  position.y = 0.025 + support;
  food.mesh.removeFromParent(); engine.world.root.add(food.mesh);
  food.mesh.position.copy(position); food.mesh.rotation.set(0, engine.yaw, 0); food.mesh.scale.setScalar(foodLoad(food).bulk > 0.2 ? 1.75 : foodLoad(food).bulk > 0.1 ? 1.3 : 1);
  food.mesh.visible = true; food.mesh.matrixAutoUpdate = true;
  food.carriedBy = null; food.reservedBy = null; food.deposited = false; food.__pickupBlockedUntil = engine.time + 1.1;
  food.mesh.updateMatrix(); food.mesh.matrixAutoUpdate = false;
  engine.carriedFood = null; engine.snapshot.carrying = null; engine.snapshot.carryingValue = 0;
  engine.survivalMovement = foodLoad(null);
  if (engine.__expansion) rebuildFoodIndexes(engine, engine.__expansion);
  engine.__distractionKind = food.kind;
  try { engine.emitNoise(position, 0.38 + foodLoad(food).bulk * 1.6); }
  finally { engine.__distractionKind = null; }
  engine.showMessage?.("Food dropped. Run—or come back for it.", 1.5);
  for (const cat of engine.cats) {
    const memory = memoryFor(engine, cat, I), key = `distraction:${food.kind}`;
    const inEarshot = distance(cat.rig.root.position, position) < 2.4;
    const witnessed = observedRecently(engine, cat, position);
    const repeats = memory.score(key, engine.time);
    if (!inEarshot || (cat.state === "chase" && !witnessed)) continue;
    memory.observe(key, position, engine.time, true);
    const edible = /cheese|cereal|cracker|nut/i.test(food.kind ?? "");
    const chance = (cat.personality === "kitten" ? 0.65 : 0.32) / (1 + repeats * 0.7);
    const roll = (stableHash(`${cat.id}:${engine.time.toFixed(1)}:${food.kind}`) % 1000) / 1000;
    if (edible && roll < chance && cat.pouncePhase === "none") {
      cat.__foodSniff = { until: engine.time + 0.65, position: position.clone() };
      cat.investigation.copy(position); engine.setCatState(cat, "investigating", 1.1); engine.planCatPath(cat, position);
    }
  }
  engine.publish?.(true);
  return true;
}

function pounceFor(cat, I) {
  return cat.__physicalPounce ??= { direction: new I.Vector3(), goal: new I.Vector3(), previous: new I.Vector3(),
    from: new I.Vector3(), to: new I.Vector3(), target: new I.Vector3(), motion: new I.Vector3(), cooldown: 0, elapsed: 0, locked: false };
}

export function updatePhysicalPounce(engine, cat, delta, I, followChase) {
  const p = pounceFor(cat, I), now = engine.time;
  if (cat.pouncePhase === "recover") {
    cat.pounceTimer -= delta;
    if (cat.pounceTimer <= 0) { cat.pouncePhase = "none"; p.cooldown = now + 0.5; cat.pathTimer = 0; }
    return { speed: 0, pounce: 0 };
  }
  const target = engine.targetPosition(cat.targetId);
  if (cat.pouncePhase === "none") return followChase();
  if (cat.pouncePhase === "windup") {
    if (!p.locked && target && engine.targetVisibility(cat, cat.targetId, target) > 0) {
      p.goal.copy(target);
      // Prediction comes only from observed velocity; freeze aim before launch.
      const h = cat.hunt, lead = cat.personality === "hunter" ? 0.1 : 0.045;
      if (h && now - h.seenAt < 0.25) { p.goal.x += h.vx * lead; p.goal.z += h.vz * lead; }
    }
    cat.pounceTimer -= delta;
    if (cat.pounceTimer < 0.14) p.locked = true;
    cat.yaw = engine.yawToward(cat.rig.root.position, p.goal); cat.rig.root.rotation.y = cat.yaw;
    if (cat.pounceTimer <= 0) {
      p.direction.copy(p.goal).sub(cat.rig.root.position).setY(0).normalize();
      p.travel = clamp(distance(cat.rig.root.position, p.goal) + 0.22, 0.38, 1.75);
      p.elapsed = 0; cat.pouncePhase = "flight"; cat.pounceTimer = 0.34;
    }
    return { speed: 0, pounce: 0.02 };
  }
  if (cat.pouncePhase !== "flight") return followChase();
  p.previous.copy(cat.rig.root.position);
  const dt = Math.min(delta, cat.pounceTimer);
  p.motion.copy(p.direction).multiplyScalar(p.travel / 0.34 * dt);
  I.moveActor(cat.rig.root.position, p.motion, 0.205, engine.world.colliders, "cat");
  p.elapsed += dt; cat.pounceTimer -= delta; cat.pounceVisual = clamp(p.elapsed / 0.34, 0, 1);
  // Swept front-paw volumes catch fast crossings without a proximity kill.
  if (target && engine.targetPosition(cat.targetId)) {
    p.target.copy(target); p.target.y = cat.targetId === "player" ? 0.065 + (engine.playerElevation ?? 0) + Math.max(0, engine.playerEyeY - 0.066 - (engine.playerElevation ?? 0)) : target.y + 0.05;
    for (const side of [-1, 1]) {
      const sx = p.direction.z * side * 0.09, sz = -p.direction.x * side * 0.09;
      p.from.set(p.previous.x + p.direction.x * 0.23 + sx, 0.09, p.previous.z + p.direction.z * 0.23 + sz);
      p.to.set(cat.rig.root.position.x + p.direction.x * 0.23 + sx, 0.09, cat.rig.root.position.z + p.direction.z * 0.23 + sz);
      if (sweptPawHit(p.from, p.to, p.target) && I.lineClear(cat.rig.root.position, target, 0.205, engine.world.colliders, "cat")) {
        if (cat.targetId === "player") engine.playerCaught(cat);
        else { const mouse = engine.mice.find(m => m.member.id === cat.targetId); if (mouse?.member.alive) engine.mouseCaught(mouse, cat); }
        break;
      }
    }
  }
  const blocked = distance(p.previous, cat.rig.root.position) < p.motion.length() * 0.45;
  if (cat.pounceTimer <= 0 || blocked) {
    cat.pouncePhase = "recover"; cat.pounceTimer = blocked ? 0.82 : 0.62;
    cat.pounceVisual = 0; cat.path = []; cat.pathIndex = 0; p.locked = false;
    engine.emitNoise(cat.rig.root.position, blocked ? 0.65 : 0.32);
  }
  return { speed: p.travel / 0.34, pounce: cat.pounceVisual };
}

export function installSurvivalUpgrade(I = globalThis.window?.HearthmouseInternals) {
  const p = I?.Engine?.prototype;
  if (!p?.__livingPredatorsInstalled) return false;
  if (p.__survivalUpgradeInstalled) return true;
  Object.defineProperty(p, "__survivalUpgradeInstalled", { value: true });
  const methods = ["beginNight", "restartCampaign", "updatePlayer", "updateCamera", "updateGame", "updateCatChase", "checkCatCatch", "processCatVision", "targetVisibility", "updateCatPatrol", "emitNoise", "playerTakeFood", "followMousePath", "dispose", "interact", "endNight"];
  const base = Object.fromEntries(methods.map(k => [k, p[k]]));

  p.beginNight = function survivalNight(...args) {
    const state = stateFor(this, I); resetHouseholdEvents(this, state);
    state.memories.clear(); state.lastRoom = state.lastCell = null;
    state.nextEvent = this.time + 24; state.eventIndex = 0;
    this.playerElevation = 0;
    if (state.traversal) { state.traversal.climb = null; state.traversal.elevation = 0; state.traversal.groundEye = 0.066; state.traversal.nextEntry = 0; }
    const result = base.beginNight.apply(this, args);
    state.traversal ??= buildTraversal(this, I);
    return result;
  };
  p.endNight = function survivalDawn(...args) {
    if (this.__survival) resetHouseholdEvents(this, this.__survival);
    return base.endNight.apply(this, args);
  };
  p.restartCampaign = function survivalRestart(...args) {
    const state = stateFor(this, I); resetHouseholdEvents(this, state); state.memories.clear();
    return base.restartCampaign.apply(this, args);
  };
  p.interact = function carryAction() {
    if (this.carriedFood && !this.insideNest()) return this.dropCarriedFood();
    return base.interact.call(this);
  };
  p.dropCarriedFood = function dropFood() { return dropPlayerFood(this, I); };
  p.moveSurvivalPlayer = function survivalMove(motion) {
    const state = stateFor(this, I);
    if (!state.traversal) return I.moveActor(this.playerPosition, motion, 0.055, this.world.colliders, "mouse");
    return moveTraversingPlayer(this, state, I, motion);
  };
  p.updatePlayer = function burdenedMouse(delta) {
    const state = stateFor(this, I); state.traversal ??= buildTraversal(this, I);
    if (this.keys.delete("KeyF") || this.dropQueued) { this.dropQueued = false; this.dropCarriedFood(); }
    this.survivalMovement = foodLoad(this.carriedFood);
    const t = state.traversal;
    // Let the existing controller/trap/tunnel code own input and jumping.
    const oldElevation = t.elevation;
    this.playerEyeY = t.groundEye;
    state.updatingPlayer = true;
    try { base.updatePlayer.call(this, delta); }
    finally { state.updatingPlayer = false; }
    t.groundEye = this.playerEyeY;
    const living = this.__livingHouse;
    if (!living?.playerTunnelTransit && !living?.playerTrap) {
      startContextClimb(this, state); advanceTraversal(this, state, delta);
    }
    this.playerEyeY = t.groundEye + t.elevation;
    // Preserve the existing camera's run FOV, bob and one projection update.
    if (this.camera?.position) {
      this.camera.position.x = this.playerPosition.x; this.camera.position.z = this.playerPosition.z;
      this.camera.position.y += t.elevation - oldElevation;
    }
  };
  p.updateCamera = function elevatedCamera(...args) {
    const state = this.__survival, t = state?.traversal;
    const eye = this.playerEyeY;
    if (state?.updatingPlayer) this.playerEyeY += t?.elevation ?? 0;
    let result;
    try { result = base.updateCamera.apply(this, args); }
    finally { this.playerEyeY = eye; }
    if (t && this.camera?.position) {
      if (t.climb) { this.camera.rotation.z += Math.sin(this.time * 18) * 0.025; this.camera.position.y += Math.sin(this.time * 24) * 0.004; }
      else if (this.time < t.landingUntil) this.camera.position.y -= Math.sin((t.landingUntil - this.time) * 12) * 0.007;
    }
    return result;
  };
  p.followMousePath = function weightedColony(mouse, delta, speed) { return base.followMousePath.call(this, mouse, delta, speed * foodLoad(mouse.carriedFood).speed); };
  p.playerTakeFood = function cooldownPickup(food) {
    if ((food?.__pickupBlockedUntil ?? 0) > this.time || (this.playerElevation ?? 0) > 0.12 && Math.abs(food.mesh.position.y - this.playerElevation) > 0.16) return;
    const result = base.playerTakeFood.call(this, food);
    if (this.carriedFood === food) this.observePlayerHabit(`food:${roomForPoint(this.playerPosition.x, this.playerPosition.z)}`, this.playerPosition);
    return result;
  };
  p.distractionResponse = function learnedDistraction(cat) {
    if (!this.__distractionKind) return 1;
    const repeats = memoryFor(this, cat, I).score(`distraction:${this.__distractionKind}`, this.time);
    return 1 / (1 + repeats * 0.5);
  };
  p.emitNoise = function weightedNoise(position, strength) {
    let adjusted = strength;
    if (position === this.playerPosition) adjusted *= foodLoad(this.carriedFood).noise;
    if (this.__survival?.event?.mask) adjusted *= 0.4;
    return base.emitNoise.call(this, position, adjusted);
  };
  p.observePlayerHabit = function witnessedHabit(key, position) {
    for (const cat of this.cats) memoryFor(this, cat, I).observe(key, position, this.time, observedRecently(this, cat, position));
  };
  p.observeWitnessedTunnel = function witnessedTunnel(cat, route, source, target, knowsExit) {
    const memory = memoryFor(this, cat, I);
    memory.observe(`hole:${source.id}`, source, this.time, true);
    if (knowsExit) memory.observe(`exit:${route.id}`, target, this.time, true);
    return memory.score(`hole:${source.id}`, this.time);
  };
  p.processCatVision = function learningPredator(cat, target, interval) {
    if (cat.__foodSniff) {
      if (cat.__foodSniff.until > this.time && (!target || target.distance > 0.7)) return;
      cat.__foodSniff = null;
    }
    const result = base.processCatVision.call(this, cat, target, interval);
    if (target?.id === "player" && target.visible > 0) {
      const memory = memoryFor(this, cat, I), position = target.position;
      const room = roomForPoint(position.x, position.z), cell = `path:${Math.floor(position.x * 0.7)}:${Math.floor(position.z * 0.7)}`;
      if (cat.__observedRoom !== room) { memory.observe(`room:${room}`, position, this.time, true); cat.__observedRoom = room; }
      if (cat.__observedCell !== cell) { memory.observe(cell, position, this.time, true); cat.__observedCell = cell; }
      if (target.moving < 0.12) {
        const shelter = this.world.shelterPoints.find(s => distance(s.position, position) < 0.55);
        if (shelter && cat.__observedHide !== shelter.id) { memory.observe(`hide:${shelter.id}`, position, this.time, true); cat.__observedHide = shelter.id; }
      } else cat.__observedHide = null;
      if (cat.state === "chase") {
        const h = cat.hunt;
        if (h && Math.hypot(h.vx, h.vz) > 0.4 && this.time > (cat.__escapeLessonAt ?? 0)) {
          const direction = Math.round(Math.atan2(h.vz, h.vx) / (Math.PI / 2));
          memory.observe(`escape:${room}:${direction}`, position, this.time, true); cat.__escapeLessonAt = this.time + 10;
        }
      }
    }
    return result;
  };
  p.targetVisibility = function darknessAwareSight(cat, id, position) {
    const factor = darknessVisibility(roomIllumination(this, position), distance(cat.rig.root.position, position));
    if (factor === 0) return 0;
    return base.targetVisibility.call(this, cat, id, position) * factor;
  };
  p.updateCatPatrol = function learnedApproaches(cat, delta, speed) {
    const now = this.time;
    if (cat.__habitWatch?.until > now && cat.state === "relaxed") {
      if (distance(cat.rig.root.position, cat.__habitWatch.goal) < 0.45) return 0;
      return this.followCatPath(cat, delta, 0.62);
    }
    if (cat.__habitWatch) { cat.__habitWatch = null; cat.path = []; cat.pathIndex = 0; }
    if (cat.state === "relaxed" && now > (cat.__habitDecision ?? 12)) {
      cat.__habitDecision = now + 13 + stableHash(cat.id) % 7;
      const known = memoryFor(this, cat, I).best(now, (key, entry) => !key.startsWith("distraction:") && distance(entry, this.world.nestCenter) > 3);
      const roll = stableHash(`${cat.id}:${Math.floor(now)}`) % 100;
      if (known && roll < Math.min(65, known.score * 13) && !this.cats.some(c => c !== cat && c.__habitWatch?.until > now)) {
        const goal = new I.Vector3(known.x, 0, known.z);
        this.planCatPath(cat, goal);
        if (cat.pathReachable) { cat.__habitWatch = { goal, until: now + 4 }; cat.leisureMode = null; cat.leisureTimer = 0; }
      }
    }
    return base.updateCatPatrol.call(this, cat, delta, speed);
  };
  p.checkCatCatch = function pawCatchOnly(cat) {
    // All cat catches are resolved by the physical pounce's swept paw volumes.
    // Keep dog and trap catches on their independent existing paths.
    return false;
  };
  p.updateCatChase = function committedPounce(cat, delta) {
    const physical = pounceFor(cat, I);
    if (cat.pouncePhase !== "none") return updatePhysicalPounce(this, cat, delta, I, () => ({ speed: 0, pounce: 0 }));
    if (this.time < physical.cooldown) {
      cat.pathTimer = Math.max(cat.pathTimer, 0.1);
      return { speed: this.followCatPath(cat, delta, 0.7), pounce: 0 };
    }
    const result = base.updateCatChase.call(this, cat, delta);
    const target = this.targetPosition(cat.targetId);
    // The old touch-catch is gone, so point-blank cats also need a short swat windup.
    if (cat.state === "chase" && cat.pouncePhase === "none" && target && distance(cat.rig.root.position, target) < 0.49 &&
        this.targetVisibility(cat, cat.targetId, target) > 0 && I.lineClear(cat.rig.root.position, target, 0.205, this.world.colliders, "cat")) {
      cat.pouncePhase = "windup"; cat.pounceTimer = cat.pounceWindupDuration = 0.22;
    }
    if (cat.pouncePhase === "windup") {
      physical.goal.copy(cat.lastSeen); physical.locked = false;
      this.__expansion?.performanceManager?.promoteActor(cat, 1);
    }
    return result;
  };
  p.updateGame = function livingSurvival(delta) {
    const state = stateFor(this, I);
    updateHouseholdEvents(this, state, I, delta);
    return base.updateGame.call(this, delta);
  };
  p.dispose = function disposeSurvival(...args) {
    const state = this.__survival;
    if (state) { resetHouseholdEvents(this, state); state.ui?.remove(); }
    return base.dispose.apply(this, args);
  };
  return true;
}

function mountDropControl(engine, state) {
  if (typeof document === "undefined" || !document.createElement || state.ui) return;
  const host = document.querySelector?.(".game-shell"); if (!host) return;
  const control = document.createElement("button"); control.type = "button"; control.className = "survival-drop-food";
  control.setAttribute("aria-label", "Drop carried food immediately");
  control.addEventListener("pointerdown", e => { e.preventDefault(); e.stopPropagation(); engine.dropCarriedFood(); });
  control.addEventListener("click", e => { if (e.detail === 0) engine.dropCarriedFood(); });
  host.append(control); state.ui = control;
}
registerFrameVisualStage("survival-carry-control", engine => {
  const state = engine.__survival; if (!state) return;
  mountDropControl(engine, state);
  if (state.ui) {
    state.ui.hidden = !engine.carriedFood || engine.snapshot.phase !== "foraging";
    const label = `${foodLoad(engine.carriedFood).label} · DROP FOOD [F]`;
    if (state.ui.textContent !== label) state.ui.textContent = label;
  }
  const t = state.traversal, paws = engine.playerView?.userData?.carryPaws;
  if (t?.climb && !engine.carriedFood) for (const [i, entry] of (paws ?? []).entries()) {
    entry.paw.rotation.x = -0.9 + Math.sin(engine.time * 18 + i * Math.PI) * 0.6;
  }
}, 1 / 15);
registerCharacterVisualStage("pounce-flight-and-recovery", (cat, context) => {
  if (context.kind !== "cat" || !cat.__physicalPounce) return;
  const rig = cat.rig;
  if (cat.pouncePhase === "flight") {
    const lift = Math.sin(cat.pounceVisual * Math.PI) * 0.16;
    rig.body.position.y += lift; rig.chest.position.y += lift; rig.body.rotation.x = -0.16 + cat.pounceVisual * 0.3;
  } else if (cat.pouncePhase === "recover") {
    rig.body.position.y -= 0.07 * clamp(cat.pounceTimer / 0.65, 0, 1);
    rig.body.rotation.x = 0.18; rig.headPivot.rotation.x += 0.12;
  }
}, 22);
function installWhenReady(attempt = 0) {
  if (typeof window === "undefined") return;
  if (!installSurvivalUpgrade() && attempt < 300) window.setTimeout(() => installWhenReady(attempt + 1), 40);
}
installWhenReady();
