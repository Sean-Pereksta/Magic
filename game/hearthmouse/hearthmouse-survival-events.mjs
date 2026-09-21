import { ROOM_LAYOUT_BY_ID, roomForPoint } from "./hearthmouse-circulation-layout.mjs";
import { stableHash } from "./hearthmouse-performance-manager.mjs";
import { rebuildFoodIndexes, rebuildStaticSpatialIndexes, setWorldObjectActive } from "./hearthmouse-expansion-core.mjs";
import { clamp, distance } from "./hearthmouse-survival-core.mjs";
import { isFoodPositionClear } from "./hearthmouse-habitat.mjs";

export const HOUSEHOLD_EVENTS = Object.freeze(["footsteps", "light", "fridge", "object", "television", "door", "vacuum", "thunder", "food"]);
export function roomIllumination(engine, position) {
  const roomId = roomForPoint(position.x, position.z), room = ROOM_LAYOUT_BY_ID.get(roomId);
  const night = engine.__expansion?.currentPlan?.event;
  let light = room?.gameplay?.light ?? 0.62;
  if (night?.lighting === "outage") light *= 0.35;
  if (night?.lighting === "storm") light *= 0.72;
  if (night?.brightRooms?.includes(roomId)) light = Math.max(light, 0.94);
  const event = engine.__survival?.event;
  if (event?.roomId === roomId && event.light != null) light = event.light;
  const dawn = clamp(1 - (engine.snapshot.timeRemaining ?? 240) / 50, 0, 1);
  return clamp(light + dawn * 0.25, 0.08, 1);
}

function invalidateNavigation(engine) {
  const x = engine.__expansion; if (!x) return;
  x.routeRevision++; x.routeCache.clear(); x.roomRouteCache.clear();
  for (const cat of engine.cats) { cat.visibilitySamples?.clear(); cat.__chaseVisibilitySamples?.clear(); cat.pathTimer = 0; }
  rebuildStaticSpatialIndexes(engine, x);
}
export function canCloseEventDoor(engine, door) {
  const x = engine.__expansion;
  if (!x || door.collider.active || !x.performanceManager.roomUnlocked(door.roomId)) return false;
  const edge = x.navEdges.find(e => e.doorId === door.id); if (!edge) return false;
  const visited = new Set([edge.a]), queue = [edge.a];
  for (let i = 0; i < queue.length; i++) {
    const current = queue[i];
    for (const link of x.navEdges) {
      if (link === edge || link.mouseOnly || link.unlockNight > engine.snapshot.night) continue;
      if (link.doorId && x.roomDoors.find(d => d.id === link.doorId)?.collider.active) continue;
      if (link.dynamicProp && x.dynamicProps.get(link.dynamicProp)?.collider.active) continue;
      if (link.passage && x.dynamicProps.get("passage-block")?.collider.active) continue;
      const next = link.a === current ? link.b : link.b === current ? link.a : null;
      if (!next || visited.has(next) || !x.performanceManager.roomUnlocked(next)) continue;
      if (next === edge.b) return true;
      visited.add(next); queue.push(next);
    }
  }
  return false;
}
function doorwayOccupied(engine, door) {
  const c = door.collider, inside = p => p.x > c.minX - 0.4 && p.x < c.maxX + 0.4 && p.z > c.minZ - 0.4 && p.z < c.maxZ + 0.4;
  return inside(engine.playerPosition) || engine.cats.some(a => inside(a.rig.root.position)) ||
    engine.mice.some(a => a.member.alive && inside(a.rig.root.position)) ||
    engine.foods.some(f => !f.deposited && !f.carriedBy && inside(f.mesh.position));
}
function releaseEvent(engine, state) {
  const e = state.event;
  if (e?.door && e.closed) { setWorldObjectActive(engine.__expansion, e.door, false); invalidateNavigation(engine); }
  state.lightExposure = 1;
  if (state.eventLight) state.eventLight.visible = false;
  if (state.eventProp) state.eventProp.visible = false;
  state.event = null;
}
export function resetHouseholdEvents(engine, state) {
  releaseEvent(engine, state);
  state.nextPulse = 0;
}
function eventPosition(engine, I, roomId) {
  const room = ROOM_LAYOUT_BY_ID.get(roomId);
  if (!room) return engine.playerPosition.clone();
  const candidates = engine.world.foodSpawns?.filter(s => s.room === roomId) ?? [];
  for (const spawn of candidates) if (distance(spawn.position, engine.world.nestCenter) > 2 && isFoodPositionClear(spawn.position, engine.world.colliders, 0.2)) return spawn.position.clone();
  return new I.Vector3((room.minX + room.maxX) / 2, 0.025, (room.minZ + room.maxZ) / 2);
}
function spawnDroppedMeal(engine, I, position) {
  if (!isFoodPositionClear(position, engine.world.colliders, 0.18)) return false;
  // Reuse a collected non-prize meal, or add a bounded event meal. No asset loads.
  let food = engine.foods.find(f => f.deposited && !f.prizeId && !f.trapId);
  if (!food) {
    if (engine.foods.filter(f => f.__householdMeal).length >= 3) return false;
    food = { id: `household-${Math.floor(engine.time * 10)}`, kind: "cheese", value: 3, room: roomForPoint(position.x, position.z), mesh: I.makeFood("cheese"), __householdMeal: true };
    engine.foods.push(food);
  }
  food.carriedBy = null; food.reservedBy = null; food.deposited = false; food.exposure = "open";
  engine.world.root.add(food.mesh); food.mesh.position.copy(position); food.mesh.scale.setScalar(1); food.mesh.rotation.set(0, 0, 0);
  food.mesh.visible = true; food.mesh.updateMatrix(); food.mesh.matrixAutoUpdate = false;
  rebuildFoodIndexes(engine, engine.__expansion);
  return true;
}
function showEventProp(engine, state, I, position, type) {
  if (!state.eventProp) {
    state.eventProp = new I.Mesh(new I.BoxGeometry(1, 1, 1), new I.MeshStandardMaterial({ color: 0x3c3230, roughness: 0.95 }));
    state.eventProp.name = "household-moving-prop"; state.eventProp.castShadow = false; engine.world.root.add(state.eventProp);
  }
  state.eventProp.visible = true; state.eventProp.position.copy(position); state.eventProp.rotation.set(0, 0, 0);
  const shoe = type === "footsteps";
  state.eventProp.scale.set(shoe ? 0.24 : 0.45, shoe ? 0.14 : 0.22, shoe ? 0.6 : 0.42);
  state.eventProp.position.y = shoe ? 0.075 : 0.12;
}
function showRoomLight(engine, state, position) {
  if (!state.eventLight) {
    let template;
    engine.scene?.traverse?.(node => { if (!template && node.isPointLight) template = node; });
    if (template) {
      state.eventLight = template.clone(); state.eventLight.name = "household-temporary-light";
      state.eventLight.castShadow = false; state.eventLight.distance = 7; state.eventLight.decay = 2;
      engine.scene.add(state.eventLight);
    }
  }
  if (state.eventLight) { state.eventLight.visible = true; state.eventLight.position.set(position.x, 1.6, position.z); state.eventLight.intensity = 3; }
}
function humanReaction(engine, position) {
  for (const cat of engine.cats) {
    if (cat.pouncePhase === "flight" || distance(cat.rig.root.position, position) > 4.5) continue;
    if (cat.state === "chase" && distance(cat.rig.root.position, engine.playerPosition) < 1.5) continue;
    cat.targetId = null; cat.pouncePhase = "none"; cat.hunt && (cat.hunt.mode = "patrol");
    engine.setCatState(cat, "cooldown", 2.4);
    const shelter = engine.world.patrolPoints.find(point => distance(point, position) > 3 && distance(point, cat.rig.root.position) < 5);
    if (shelter) engine.planCatPath(cat, shelter);
  }
  for (const mouse of engine.mice) {
    if (!mouse.member.alive || mouse.__tunnelTransit || distance(mouse.rig.root.position, position) > 4) continue;
    const shelter = engine.world.shelterPoints.find(s => s.catProof && distance(s.position, mouse.rig.root.position) < 3);
    if (shelter) { mouse.resumeTask = mouse.carriedFood ? "returning" : "waiting"; mouse.escapeGoal = shelter.position.clone(); mouse.task = "escaping"; mouse.escapeCooldown = 3; engine.planMousePath(mouse); }
  }
}

export function triggerHouseholdEvent(engine, state, I, kind) {
  const roomId = roomForPoint(engine.playerPosition.x, engine.playerPosition.z) ?? "living";
  const position = eventPosition(engine, I, roomId), now = engine.time;
  const event = { kind, roomId, position, start: now, until: now + 5, mask: false, closed: false };
  state.event = event; state.nextPulse = now;
  const messages = {
    footsteps: "Footsteps nearby. The house goes still—find cover.", light: "A switch clicks. Your safest route just changed.",
    fridge: "The refrigerator opens. Fresh food, bright light.", object: "Something crashes to the floor. Cats turn toward the sound.",
    television: "The television comes on. Its voices mask your steps.", door: "A door begins to swing shut. Another route remains open.",
    vacuum: "The vacuum crosses the room. Use the noise to move.", thunder: "Thunder rolls overhead. Run while it lasts.", food: "A piece of food falls onto the open floor.",
  };
  if (kind === "door") {
    const doors = engine.__expansion.roomDoors;
    event.door = doors.find(d => canCloseEventDoor(engine, d) && !doorwayOccupied(engine, d) && distance(engine.playerPosition, d.mesh.position) < 10);
    if (!event.door) { state.event = null; return false; }
    event.position = event.door.mesh.position.clone(); event.until = now + 7;
  } else if (kind === "light") {
    event.light = roomIllumination(engine, position) > 0.55 ? 0.18 : 1;
    event.until = now + 10; if (event.light > 0.5) showRoomLight(engine, state, position);
  } else if (kind === "fridge") {
    event.roomId = "kitchen";
    event.position = eventPosition(engine, I, "kitchen"); event.light = 1; event.until = now + 8;
    showRoomLight(engine, state, event.position); spawnDroppedMeal(engine, I, event.position);
  } else if (kind === "food") spawnDroppedMeal(engine, I, position);
  else if (kind === "object") { showEventProp(engine, state, I, position, kind); state.eventProp.scale.set(0.12, 0.14, 0.1); state.eventProp.position.y = 0.9; }
  else if (["television", "vacuum", "thunder"].includes(kind)) {
    event.mask = true; event.until = now + (kind === "television" ? 8 : 4);
    if (kind === "vacuum") showEventProp(engine, state, I, position, kind);
    if (kind === "thunder") { event.light = 0.95; showRoomLight(engine, state, position); }
  } else if (kind === "footsteps") { showEventProp(engine, state, I, position, kind); humanReaction(engine, position); }
  engine.emitNoise(event.position, kind === "object" ? 2.5 : 1.3);
  engine.audio?.noise?.(0.15, 0.18, kind === "thunder" ? 180 : 1300);
  engine.showMessage?.(messages[kind], 3.5);
  return true;
}

export function updateHouseholdEvents(engine, state, I, delta) {
  if (engine.snapshot.phase !== "foraging" || engine.paused) return;
  const now = engine.time, e = state.event;
  state.lightExposure = 0.7 + roomIllumination(engine, engine.playerPosition) * 0.5;
  if (e) {
    if (now >= e.until) { releaseEvent(engine, state); return; }
    if (e.kind === "door" && !e.closed && now - e.start > 0.9) {
      if (!doorwayOccupied(engine, e.door) && canCloseEventDoor(engine, e.door)) {
        setWorldObjectActive(engine.__expansion, e.door, true); e.closed = true; invalidateNavigation(engine);
      } else { releaseEvent(engine, state); return; }
    }
    if (e.kind === "object" && state.eventProp) {
      state.eventProp.position.y = Math.max(0.07, 0.9 - (now - e.start) ** 2 * 2);
      state.eventProp.rotation.z = Math.min(1.2, (now - e.start) * 2);
    }
    if (e.kind === "footsteps" || e.kind === "vacuum") {
      const prop = state.eventProp;
      if (prop) { prop.position.x = e.position.x + Math.sin((now - e.start) * 1.4) * 0.4; prop.position.y = e.kind === "footsteps" ? 0.08 + Math.abs(Math.sin((now - e.start) * 3)) * 0.16 : 0.12; }
      if (now >= state.nextPulse) { state.nextPulse = now + 0.7; engine.emitNoise(e.position, 1.5); engine.audio?.noise?.(0.06, 0.12, 220); }
    }
    return;
  }
  if (now < state.nextEvent || engine.snapshot.timeRemaining < 18) return;
  const roll = stableHash(`${engine.__expansion.campaignSeed}:${engine.snapshot.night}:${state.eventIndex++}`);
  const kind = HOUSEHOLD_EVENTS[roll % HOUSEHOLD_EVENTS.length];
  if (!triggerHouseholdEvent(engine, state, I, kind)) triggerHouseholdEvent(engine, state, I, "object");
  state.nextEvent = now + 24 + roll % 19;
}
