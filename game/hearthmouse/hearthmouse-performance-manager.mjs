import {
  DOORWAY_CORRIDORS,
  ROOM_LAYOUTS,
  ROOM_LAYOUT_BY_ID,
} from "./hearthmouse-circulation-layout.mjs";

export const ACTOR_VISUAL_TIER = Object.freeze({
  IMMEDIATE: "A",
  NEARBY: "B",
  DISTANT: "C",
  SLEEPING: "D",
});

export const ACTOR_SIMULATION_TIER = Object.freeze({
  IMMEDIATE: "full",
  NEARBY: "nearby",
  DISTANT: "distant",
  IDLE: "idle",
});

const VISUAL_TIER_RANK = Object.freeze({ A: 0, B: 1, C: 2, D: 3 });
const SIMULATION_TIER_RANK = Object.freeze({ full: 0, nearby: 1, distant: 2, idle: 3 });
const ALERT_CAT_STATES = new Set(["alert", "investigating", "suspicious", "search"]);
const ACTIVE_MOUSE_TASKS = new Set(["to-food", "returning", "home", "nesting-move", "escaping", "tunneling"]);
const ROOM_HIDE_DELAY_SECONDS = 0.38;
const ROOM_LINE_OF_SIGHT_DEPTH = 2;
const VISUAL_STAGES = [];
const FRAME_STAGES = [];

const clamp = (value, minimum, maximum) => Math.max(minimum, Math.min(maximum, value));

function reportStageError(stage, error) {
  if (stage.errorReported) return;
  stage.errorReported = true;
  console.warn(`Hearthmouse performance stage "${stage.name}" failed; continuing the main game loop.`, error);
}

function normalizeAngle(angle) {
  let value = angle;
  while (value > Math.PI) value -= Math.PI * 2;
  while (value < -Math.PI) value += Math.PI * 2;
  return value;
}

function planarDistanceSquared(a, b) {
  if (!a || !b) return Infinity;
  const dx = (a.x ?? 0) - (b.x ?? 0);
  const dz = (a.z ?? 0) - (b.z ?? 0);
  return dx * dx + dz * dz;
}

function stableActorId(actor, fallbackIndex = 0) {
  return String(actor?.member?.id ?? actor?.id ?? actor?.name ?? `actor-${fallbackIndex}`);
}

export function stableHash(value) {
  const text = String(value ?? "");
  let hash = 2166136261;
  for (let index = 0; index < text.length; index++) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

export function roomForPoint(x, z, rooms = ROOM_LAYOUTS) {
  for (let index = 0; index < rooms.length; index++) {
    const room = rooms[index];
    if (x >= room.minX - 0.12 && x <= room.maxX + 0.12 && z >= room.minZ - 0.12 && z <= room.maxZ + 0.12) {
      return room.id;
    }
  }
  return null;
}

export function visualUpdateIntervalSeconds(tier, isTouchDevice = false) {
  if (tier === ACTOR_VISUAL_TIER.IMMEDIATE) return 0;
  if (tier === ACTOR_VISUAL_TIER.NEARBY) return 1 / (isTouchDevice ? 20 : 24);
  if (tier === ACTOR_VISUAL_TIER.DISTANT) return 1 / (isTouchDevice ? 6 : 8);
  return Infinity;
}

export function simulationDecisionIntervalSeconds(tier, active = false) {
  if (tier === ACTOR_SIMULATION_TIER.IMMEDIATE) return 1 / 30;
  if (tier === ACTOR_SIMULATION_TIER.NEARBY) return 1 / 22;
  if (tier === ACTOR_SIMULATION_TIER.DISTANT) return 1 / (active ? 8 : 6);
  return 1 / (active ? 5 : 3);
}

export function catVisionIntervalSeconds(catState, visualTier, isTouchDevice = false) {
  if (catState === "chase") return 1 / (isTouchDevice ? 30 : 45);
  if (ALERT_CAT_STATES.has(catState)) return 1 / (isTouchDevice ? 20 : 24);
  if (visualTier === ACTOR_VISUAL_TIER.IMMEDIATE) return 1 / 12;
  if (visualTier === ACTOR_VISUAL_TIER.NEARBY) return 1 / 10;
  return 1 / 6;
}

export function shouldInvalidateLosSample(sample, current) {
  if (!sample) return true;
  if (sample.state !== current.state || sample.routeRevision !== current.routeRevision) return true;
  if (sample.catRoom !== current.catRoom || sample.targetRoom !== current.targetRoom) return true;
  if (!Number.isFinite(sample.time) || current.time < sample.time) return true;
  if (current.distanceSquared < 0.64) return true;
  const targetThreshold = current.state === "chase" ? 0.0025 : 0.0256;
  const catThreshold = current.state === "chase" ? 0.0036 : 0.04;
  if ((sample.targetX - current.targetX) ** 2 + (sample.targetZ - current.targetZ) ** 2 > targetThreshold) return true;
  if ((sample.catX - current.catX) ** 2 + (sample.catZ - current.catZ) ** 2 > catThreshold) return true;
  const yawThreshold = current.state === "chase" ? Math.PI / 30 : Math.PI / 18;
  return Math.abs(normalizeAngle((sample.yaw ?? 0) - current.yaw)) > yawThreshold;
}

export function registerRoomRenderGroup(world, roomId, group) {
  if (!world || !roomId || !group) return false;
  const registry = world.__hearthmouseRoomGroups ?? (world.__hearthmouseRoomGroups = new Map());
  let groups = registry.get(roomId);
  if (!groups) {
    groups = new Set();
    registry.set(roomId, groups);
  }
  groups.add(group);
  group.userData ??= {};
  group.userData.roomId = roomId;
  return true;
}

export function registerCharacterVisualStage(name, update, order = 0) {
  if (!name || typeof update !== "function") return false;
  const existing = VISUAL_STAGES.find((stage) => stage.name === name);
  if (existing) {
    existing.update = update;
    existing.order = order;
    existing.errorReported = false;
  } else {
    VISUAL_STAGES.push({ name, update, order, errorReported: false });
  }
  VISUAL_STAGES.sort((a, b) => a.order - b.order || a.name.localeCompare(b.name));
  return true;
}

export function registerFrameVisualStage(name, update, interval = 0) {
  if (!name || typeof update !== "function") return false;
  const existing = FRAME_STAGES.find((stage) => stage.name === name);
  if (existing) {
    existing.update = update;
    existing.interval = Math.max(0, Number(interval) || 0);
    existing.errorReported = false;
  } else {
    FRAME_STAGES.push({ name, update, interval: Math.max(0, Number(interval) || 0), errorReported: false });
  }
  return true;
}

export function deferActorRigVisuals(engine, actor) {
  const rig = actor?.rig;
  if (!engine || !rig || typeof rig.update !== "function" || rig.__hearthmouseCentralVisualsInstalled) return false;
  const updateRig = rig.update.bind(rig);
  const pendingArgs = [];
  rig.update = function deferredRigVisualUpdate() {
    const manager = engine.__expansion?.performanceManager;
    if (!manager?.frameOpen) {
      actor.__hearthmousePendingRigVisual = null;
      return Reflect.apply(updateRig, null, arguments);
    }
    pendingArgs.length = arguments.length;
    for (let index = 0; index < arguments.length; index++) pendingArgs[index] = arguments[index];
    actor.__hearthmousePendingRigVisual = pendingArgs;
    return undefined;
  };
  rig.__hearthmouseFlushVisual = () => {
    const args = actor.__hearthmousePendingRigVisual;
    if (!args) return false;
    actor.__hearthmousePendingRigVisual = null;
    Reflect.apply(updateRig, null, args);
    return true;
  };
  Object.defineProperty(rig, "__hearthmouseCentralVisualsInstalled", { value: true });
  return true;
}

function actorKind(actor) {
  return actor?.member ? "mouse" : "cat";
}

function actorActive(actor, kind) {
  if (kind === "cat") return (actor?.state !== "relaxed" && actor?.state !== "cooldown") || actor?.leisureMode != null;
  return ACTIVE_MOUSE_TASKS.has(actor?.task);
}

function actorDeadOrLogicallyHidden(actor, kind) {
  if (kind === "mouse") return actor?.member?.alive === false || actor?.task === "dead" || !!actor?.__tunnelTransit;
  return !actor?.rig?.root?.parent;
}

export class HearthmousePerformanceManager {
  constructor(engine, options = {}) {
    this.engine = engine;
    this.isTouchDevice = options.isTouchDevice ?? !!engine?.isTouchOnlyDevice?.();
    this.rooms = options.rooms ?? ROOM_LAYOUTS;
    this.doorways = options.doorways ?? DOORWAY_CORRIDORS;
    this.roomById = new Map(this.rooms.map((room) => [room.id, room]));
    this.adjacency = new Map(this.rooms.map((room) => [room.id, []]));
    for (const doorway of this.doorways) {
      const [first, second] = doorway.rooms ?? [];
      if (!first || !second) continue;
      this.adjacency.get(first)?.push({ room: second, doorway });
      this.adjacency.get(second)?.push({ room: first, doorway });
    }
    this.clock = 0;
    this.frameId = 0;
    this.frameOpen = false;
    this.playerRoom = null;
    this.activeRenderRooms = new Set();
    this.adjacentRooms = new Set();
    this.visibleDoorways = new Set();
    this.desiredRenderRooms = new Set();
    this.firstVisiblePortals = [];
    this.doorwayViews = new Map();
    this.roomLastRelevant = new Map();
    this.roomDistances = new Map();
    this.roomDistanceQueue = [];
    this.actorMetadata = new WeakMap();
    this.frameStageTimes = new Map();
    this.windowElapsed = 0;
    this.windowFrames = 0;
    this.windowWorstFrameTimeMs = 0;
    this.windowCounters = this.createCounterSet();
    this.rates = this.createCounterSet();
    this.stats = {
      averageFps: 0,
      frameTimeMs: 0,
      worstFrameTimeMs: 0,
      activeRenderRooms: 0,
      hiddenRenderRooms: this.rooms.length,
      shadowCastingActors: 0,
      fullAnimationActors: 0,
      reducedAnimationActors: 0,
      distantAnimationActors: 0,
      sleepingVisualActors: 0,
    };
    this.visualContext = {
      engine,
      manager: this,
      actor: null,
      kind: "",
      tier: ACTOR_VISUAL_TIER.IMMEDIATE,
      delta: 0,
      time: 0,
      recordAnimationSample: () => this.record("animationSamples"),
    };
  }

  createCounterSet() {
    return {
      characterVisualUpdates: 0,
      animationSamples: 0,
      distantAnimationSamplesSkipped: 0,
      catLosChecks: 0,
      losRaycasts: 0,
      losCacheHits: 0,
      aiDecisionUpdates: 0,
      distantAiUpdatesSkipped: 0,
      catVisionScansSkipped: 0,
    };
  }

  record(key, amount = 1) {
    if (key in this.windowCounters) this.windowCounters[key] += amount;
  }

  beginFrame(delta) {
    const frameDelta = clamp(Number(delta) || 0, 0, 0.25);
    this.clock += frameDelta;
    this.frameId++;
    this.frameOpen = true;
    this.windowElapsed += frameDelta;
    this.windowFrames++;
    this.stats.frameTimeMs = frameDelta * 1000;
    this.windowWorstFrameTimeMs = Math.max(this.windowWorstFrameTimeMs, this.stats.frameTimeMs);
    this.stats.worstFrameTimeMs = Math.max(this.stats.worstFrameTimeMs, this.stats.frameTimeMs);
    this.stats.shadowCastingActors = 0;
    this.stats.fullAnimationActors = 0;
    this.stats.reducedAnimationActors = 0;
    this.stats.distantAnimationActors = 0;
    this.stats.sleepingVisualActors = 0;
    this.updateRoomVisibility();
  }

  endFrame() {
    this.frameOpen = false;
    if (this.windowElapsed < 1) return;
    const elapsed = this.windowElapsed;
    this.stats.averageFps = this.windowFrames / elapsed;
    for (const key of Object.keys(this.windowCounters)) {
      this.rates[key] = this.windowCounters[key] / elapsed;
      this.windowCounters[key] = 0;
    }
    this.windowElapsed = 0;
    this.windowFrames = 0;
    this.stats.worstFrameTimeMs = this.windowWorstFrameTimeMs;
    this.windowWorstFrameTimeMs = 0;
  }

  roomUnlocked(roomId) {
    const room = this.roomById.get(roomId) ?? ROOM_LAYOUT_BY_ID.get(roomId);
    if (!room) return true;
    const expansion = this.engine?.__expansion;
    return (this.engine?.snapshot?.night ?? 1) >= (room.unlockNight ?? 1) || expansion?.temporaryRoomId === roomId;
  }

  doorwayOpen(doorway) {
    const [first, second] = doorway.rooms ?? [];
    if (!this.roomUnlocked(first) || !this.roomUnlocked(second)) return false;
    const expansion = this.engine?.__expansion;
    if (!expansion) return true;
    const edge = expansion.navEdges?.find?.((candidate) =>
      (candidate.a === first && candidate.b === second) || (candidate.a === second && candidate.b === first));
    if (edge?.doorId) {
      const door = expansion.roomDoors?.find?.((candidate) => candidate.id === edge.doorId);
      if (door?.collider?.active) return false;
    }
    if (edge?.dynamicProp) {
      const prop = expansion.dynamicProps?.get?.(edge.dynamicProp);
      if (prop?.collider?.active) return false;
    }
    const directDoor = expansion.roomDoors?.find?.((candidate) => candidate.id === doorway.id);
    return !directDoor?.collider?.active;
  }

  doorwayView(doorway) {
    const player = this.engine?.playerPosition;
    let view = this.doorwayViews.get(doorway);
    if (!view) {
      view = { distance: Infinity, dot: -1 };
      this.doorwayViews.set(doorway, view);
    }
    if (!player) {
      view.distance = Infinity;
      view.dot = -1;
      return view;
    }
    const dx = doorway.x - player.x;
    const dz = doorway.z - player.z;
    const distance = Math.max(0.001, Math.hypot(dx, dz));
    const yaw = this.engine?.yaw ?? this.engine?.camera?.rotation?.y ?? 0;
    const facingX = -Math.sin(yaw);
    const facingZ = -Math.cos(yaw);
    view.distance = distance;
    view.dot = (dx * facingX + dz * facingZ) / distance;
    return view;
  }

  updateRoomVisibility() {
    const player = this.engine?.playerPosition;
    this.playerRoom = player ? roomForPoint(player.x, player.z, this.rooms) : this.playerRoom;
    const desired = this.desiredRenderRooms;
    const firstPortals = this.firstVisiblePortals;
    desired.clear();
    firstPortals.length = 0;
    this.adjacentRooms.clear();
    this.visibleDoorways.clear();
    if (this.playerRoom) desired.add(this.playerRoom);

    for (const connection of this.adjacency.get(this.playerRoom) ?? []) {
      if (!this.doorwayOpen(connection.doorway)) continue;
      this.adjacentRooms.add(connection.room);
      const view = this.doorwayView(connection.doorway);
      const visible = view.distance <= 2.85 || view.dot > 0.04 && view.distance <= 18;
      if (!visible) continue;
      desired.add(connection.room);
      this.visibleDoorways.add(connection.doorway.id);
      firstPortals.push(connection);
    }

    for (const first of firstPortals) {
      const firstView = this.doorwayView(first.doorway);
      if (firstView.dot < 0.5) continue;
      for (const second of this.adjacency.get(first.room) ?? []) {
        if (second.room === this.playerRoom || !this.doorwayOpen(second.doorway)) continue;
        const view = this.doorwayView(second.doorway);
        if (view.dot < 0.68 || view.distance <= firstView.distance + 0.25 || view.distance > 22) continue;
        const firstLength = Math.max(0.001, Math.hypot(first.doorway.x - player.x, first.doorway.z - player.z));
        const secondLength = Math.max(0.001, Math.hypot(second.doorway.x - first.doorway.x, second.doorway.z - first.doorway.z));
        const alignment = ((first.doorway.x - player.x) / firstLength) * ((second.doorway.x - first.doorway.x) / secondLength)
          + ((first.doorway.z - player.z) / firstLength) * ((second.doorway.z - first.doorway.z) / secondLength);
        if (alignment < 0.58) continue;
        desired.add(second.room);
        this.visibleDoorways.add(second.doorway.id);
      }
    }

    for (const roomId of desired) this.roomLastRelevant.set(roomId, this.clock);
    this.activeRenderRooms.clear();
    for (const room of this.rooms) {
      if (!this.roomUnlocked(room.id)) continue;
      const lastRelevant = this.roomLastRelevant.get(room.id) ?? -Infinity;
      if (desired.has(room.id) || this.clock - lastRelevant <= ROOM_HIDE_DELAY_SECONDS) this.activeRenderRooms.add(room.id);
    }
    this.computeRoomDistances();
    this.applyRoomGroupVisibility();
    this.stats.activeRenderRooms = this.activeRenderRooms.size;
    this.stats.hiddenRenderRooms = Math.max(0, this.rooms.length - this.activeRenderRooms.size);
  }

  computeRoomDistances() {
    this.roomDistances.clear();
    if (!this.playerRoom) return;
    const queue = this.roomDistanceQueue;
    queue.length = 1;
    queue[0] = this.playerRoom;
    this.roomDistances.set(this.playerRoom, 0);
    for (let index = 0; index < queue.length; index++) {
      const roomId = queue[index];
      const depth = this.roomDistances.get(roomId);
      for (const connection of this.adjacency.get(roomId) ?? []) {
        if (this.roomDistances.has(connection.room) || !this.doorwayOpen(connection.doorway)) continue;
        this.roomDistances.set(connection.room, depth + 1);
        queue.push(connection.room);
      }
    }
  }

  applyRoomGroupVisibility() {
    const registry = this.engine?.world?.__hearthmouseRoomGroups;
    if (!(registry instanceof Map)) return;
    for (const [roomId, groups] of registry) {
      const renderRelevant = this.activeRenderRooms.has(roomId);
      const unlocked = this.roomUnlocked(roomId);
      for (const group of groups ?? []) {
        if (!group) continue;
        group.userData ??= {};
        group.userData.__hearthmouseRenderRelevant = renderRelevant;
        group.visible = unlocked && renderRelevant;
      }
    }
  }

  isRoomRenderActive(roomId) {
    return !roomId || this.activeRenderRooms.has(roomId);
  }

  roomDistance(roomId) {
    return roomId ? this.roomDistances.get(roomId) ?? Infinity : Infinity;
  }

  roomsCouldShareLineOfSight(firstRoom, secondRoom) {
    if (!firstRoom || !secondRoom || firstRoom === secondRoom) return true;
    const queue = [{ room: firstRoom, depth: 0 }];
    const visited = new Set([firstRoom]);
    for (let index = 0; index < queue.length; index++) {
      const current = queue[index];
      if (current.depth >= ROOM_LINE_OF_SIGHT_DEPTH) continue;
      for (const connection of this.adjacency.get(current.room) ?? []) {
        if (!this.doorwayOpen(connection.doorway) || visited.has(connection.room)) continue;
        if (connection.room === secondRoom) return true;
        visited.add(connection.room);
        queue.push({ room: connection.room, depth: current.depth + 1 });
      }
    }
    return false;
  }

  metadataFor(actor, fallbackIndex = 0) {
    let metadata = this.actorMetadata.get(actor);
    if (!metadata) {
      const id = stableActorId(actor, fallbackIndex);
      metadata = {
        id,
        hash: stableHash(id),
        frameId: -1,
        room: null,
        visualTier: ACTOR_VISUAL_TIER.SLEEPING,
        simulationTier: ACTOR_SIMULATION_TIER.IDLE,
        previousVisualTier: ACTOR_VISUAL_TIER.SLEEPING,
        previousSimulationTier: ACTOR_SIMULATION_TIER.IDLE,
        immediatePromotion: false,
        forcedUntil: 0,
        visualBucket: null,
        lastVisualTime: null,
        shadowTier: null,
        shadowSignature: null,
        shadowDirty: true,
        shadowCasts: false,
        workBuckets: new Map(),
        lastVisionScanAt: -Infinity,
      };
      this.actorMetadata.set(actor, metadata);
    }
    return metadata;
  }

  classifyActor(actor, kind = actorKind(actor), fallbackIndex = 0) {
    const metadata = this.metadataFor(actor, fallbackIndex);
    if (metadata.frameId === this.frameId) return metadata;
    metadata.frameId = this.frameId;
    metadata.previousVisualTier = metadata.visualTier;
    metadata.previousSimulationTier = metadata.simulationTier;
    const root = actor?.rig?.root;
    const position = root?.position;
    metadata.room = position ? roomForPoint(position.x, position.z, this.rooms) : null;
    const distanceSquared = planarDistanceSquared(position, this.engine?.playerPosition);
    const roomDistance = this.roomDistance(metadata.room);
    const targetedByCat = kind === "mouse" && (this.engine?.cats ?? []).some((cat) => cat.targetId === actor?.member?.id && cat.state === "chase");
    const catImmediate = kind === "cat" && (
      (actor.state === "chase" && actor.targetId === "player")
      || actor.pouncePhase === "windup"
      || actor.pouncePhase === "flight"
    );
    const mouseImmediate = kind === "mouse" && (targetedByCat || (actor.task === "escaping" && distanceSquared <= 64) || !!actor.__tunnelTransit);
    const forced = metadata.forcedUntil > this.clock;
    const immediate = forced || catImmediate || mouseImmediate || distanceSquared <= 7.84 || (metadata.room != null && metadata.room === this.playerRoom);
    const active = actorActive(actor, kind);

    if (actorDeadOrLogicallyHidden(actor, kind)) metadata.visualTier = ACTOR_VISUAL_TIER.SLEEPING;
    else if (immediate) metadata.visualTier = ACTOR_VISUAL_TIER.IMMEDIATE;
    else if ((this.activeRenderRooms.has(metadata.room) && distanceSquared <= 225) || roomDistance === 1 || distanceSquared <= 49) metadata.visualTier = ACTOR_VISUAL_TIER.NEARBY;
    else if (roomDistance === 2 || distanceSquared <= 256 || active && roomDistance <= 3) metadata.visualTier = ACTOR_VISUAL_TIER.DISTANT;
    else metadata.visualTier = ACTOR_VISUAL_TIER.SLEEPING;

    if (immediate || kind === "cat" && (actor.state === "chase" || actor.pouncePhase !== "none") || targetedByCat) {
      metadata.simulationTier = ACTOR_SIMULATION_TIER.IMMEDIATE;
    } else if (roomDistance <= 1 || kind === "cat" && ALERT_CAT_STATES.has(actor.state)) {
      metadata.simulationTier = ACTOR_SIMULATION_TIER.NEARBY;
    } else if (active || roomDistance <= 2) {
      metadata.simulationTier = ACTOR_SIMULATION_TIER.DISTANT;
    } else {
      metadata.simulationTier = ACTOR_SIMULATION_TIER.IDLE;
    }
    metadata.immediatePromotion = SIMULATION_TIER_RANK[metadata.simulationTier] < SIMULATION_TIER_RANK[metadata.previousSimulationTier]
      && metadata.simulationTier === ACTOR_SIMULATION_TIER.IMMEDIATE;
    return metadata;
  }

  getActorVisualTier(actor, kind, fallbackIndex = 0) {
    return this.classifyActor(actor, kind, fallbackIndex).visualTier;
  }

  getActorSimulationTier(actor, kind, fallbackIndex = 0) {
    return this.classifyActor(actor, kind, fallbackIndex).simulationTier;
  }

  promoteActor(actor, duration = 0.6) {
    const metadata = this.metadataFor(actor);
    metadata.forcedUntil = Math.max(metadata.forcedUntil, this.clock + duration);
    metadata.frameId = -1;
    metadata.immediatePromotion = true;
    metadata.lastVisionScanAt = -Infinity;
  }

  consumeImmediatePromotion(actor) {
    const metadata = this.classifyActor(actor);
    const promoted = metadata.immediatePromotion;
    metadata.immediatePromotion = false;
    return promoted;
  }

  decisionIntervalFor(actor, kind = actorKind(actor)) {
    const metadata = this.classifyActor(actor, kind);
    return simulationDecisionIntervalSeconds(metadata.simulationTier, actorActive(actor, kind));
  }

  shouldRunActorWork(actor, purpose, minimumInterval = null) {
    const metadata = this.classifyActor(actor);
    if (metadata.simulationTier === ACTOR_SIMULATION_TIER.IMMEDIATE) return true;
    const interval = Math.max(1 / 60, minimumInterval ?? this.decisionIntervalFor(actor));
    const phase = (metadata.hash % 997) / 997 * interval;
    const bucket = Math.floor((this.clock + phase) / interval);
    const previous = metadata.workBuckets.get(purpose);
    metadata.workBuckets.set(purpose, bucket);
    // New distant actors wait for their hashed bucket boundary instead of all
    // doing their first expensive decision on the installation/spawn frame.
    return previous != null && previous !== bucket;
  }

  recordAiDecision(actor) {
    const tier = this.getActorSimulationTier(actor);
    this.record("aiDecisionUpdates");
    if (tier === ACTOR_SIMULATION_TIER.DISTANT || tier === ACTOR_SIMULATION_TIER.IDLE) return tier;
    return tier;
  }

  recordAiSkip(actor) {
    const tier = this.getActorSimulationTier(actor);
    if (tier === ACTOR_SIMULATION_TIER.DISTANT || tier === ACTOR_SIMULATION_TIER.IDLE) this.record("distantAiUpdatesSkipped");
  }

  shouldUpdateVisual(actor, metadata) {
    const interval = visualUpdateIntervalSeconds(metadata.visualTier, this.isTouchDevice);
    if (!Number.isFinite(interval)) return false;
    if (interval === 0) return true;
    if (VISUAL_TIER_RANK[metadata.visualTier] < VISUAL_TIER_RANK[metadata.previousVisualTier]) return true;
    const phase = (metadata.hash % 991) / 991 * interval;
    const bucket = Math.floor((this.clock + phase) / interval);
    if (metadata.visualBucket == null) {
      metadata.visualBucket = bucket;
      return false;
    }
    if (bucket === metadata.visualBucket) return false;
    metadata.visualBucket = bucket;
    return true;
  }

  consumeVisualDelta(metadata, fallbackDelta) {
    const elapsed = metadata.lastVisualTime == null ? fallbackDelta : this.clock - metadata.lastVisualTime;
    metadata.lastVisualTime = this.clock;
    return clamp(elapsed || fallbackDelta || 1 / 60, 0, 2);
  }

  prepareActor(actor, kind, fallbackIndex = 0) {
    const metadata = this.classifyActor(actor, kind, fallbackIndex);
    this.applyActorVisibility(actor, metadata);
    this.applyActorShadows(actor, metadata);
    if (metadata.visualTier === ACTOR_VISUAL_TIER.IMMEDIATE) this.stats.fullAnimationActors++;
    else if (metadata.visualTier === ACTOR_VISUAL_TIER.NEARBY) this.stats.reducedAnimationActors++;
    else if (metadata.visualTier === ACTOR_VISUAL_TIER.DISTANT) this.stats.distantAnimationActors++;
    else this.stats.sleepingVisualActors++;
    return metadata;
  }

  applyActorVisibility(actor, metadata) {
    const root = actor?.rig?.root;
    if (!root) return;
    root.userData ??= {};
    const logicallyHidden = actorDeadOrLogicallyHidden(actor, actorKind(actor));
    const renderRelevant = !logicallyHidden
      && metadata.visualTier !== ACTOR_VISUAL_TIER.SLEEPING
      && (!metadata.room || this.activeRenderRooms.has(metadata.room) || metadata.visualTier === ACTOR_VISUAL_TIER.IMMEDIATE);
    if (!renderRelevant && root.visible !== false) {
      root.visible = false;
      root.userData.__hearthmousePerformanceHidden = true;
    } else if (renderRelevant && root.userData.__hearthmousePerformanceHidden && !actor.__tunnelTransit) {
      root.visible = true;
      root.userData.__hearthmousePerformanceHidden = false;
    }
  }

  applyActorShadows(actor, metadata) {
    const root = actor?.rig?.root;
    if (!root?.traverse) return;
    const controller = root.userData?.__hearthmouseGlbController;
    const signature = controller?.wrapper ?? root;
    const shadowTier = this.getActorShadowTier(actor, actorKind(actor), metadata);
    if (!metadata.shadowDirty && metadata.shadowTier === shadowTier && metadata.shadowSignature === signature) {
      if (metadata.shadowCasts) this.stats.shadowCastingActors++;
      return;
    }
    metadata.shadowTier = shadowTier;
    metadata.shadowSignature = signature;
    metadata.shadowDirty = false;
    let casts = false;
    root.traverse((object) => {
      if (!object?.isMesh && object?.castShadow == null) return;
      object.userData ??= {};
      if (object.userData.__hearthmouseOriginalCastShadow == null) {
        object.userData.__hearthmouseOriginalCastShadow = !!object.castShadow;
      }
      object.castShadow = shadowTier !== "none" && !!object.userData.__hearthmouseOriginalCastShadow;
      casts ||= !!object.castShadow;
    });
    metadata.shadowCasts = casts;
    if (casts) this.stats.shadowCastingActors++;
  }

  markActorShadowDirty(actor) {
    this.metadataFor(actor).shadowDirty = true;
  }

  shouldScanCatVision(cat) {
    const metadata = this.classifyActor(cat, "cat");
    const interval = catVisionIntervalSeconds(cat?.state, metadata.visualTier, this.isTouchDevice);
    if (cat?.state === "chase") {
      metadata.lastVisionScanAt = this.clock;
      return true;
    }
    if (
      !Number.isFinite(metadata.lastVisionScanAt)
      && (metadata.visualTier === ACTOR_VISUAL_TIER.DISTANT || metadata.visualTier === ACTOR_VISUAL_TIER.SLEEPING)
    ) {
      metadata.lastVisionScanAt = this.clock - (metadata.hash % 997) / 997 * interval;
      this.record("catVisionScansSkipped");
      return false;
    }
    if (this.clock - metadata.lastVisionScanAt >= interval) {
      metadata.lastVisionScanAt = this.clock;
      return true;
    }
    this.record("catVisionScansSkipped");
    return false;
  }

  shouldSampleCatVision(cat) {
    return this.shouldScanCatVision(cat);
  }

  shouldActorCastShadow(actor, kind = actorKind(actor)) {
    return this.getActorShadowTier(actor, kind) !== "none";
  }

  getActorShadowTier(actor, kind = actorKind(actor), metadata = this.classifyActor(actor, kind)) {
    if (this.engine?.qualityReduced || this.engine?.renderer?.shadowMap?.enabled === false) return "none";
    const distanceSquared = planarDistanceSquared(actor?.rig?.root?.position, this.engine?.playerPosition);
    if (metadata.visualTier === ACTOR_VISUAL_TIER.IMMEDIATE) {
      if (kind === "cat") {
        const important = actor?.state === "chase" || actor?.pouncePhase === "windup" || actor?.pouncePhase === "flight";
        if (important || metadata.room === this.playerRoom || distanceSquared <= 36) return "full";
      } else if (distanceSquared <= (this.isTouchDevice ? 9 : 25)) {
        return "full";
      }
    }
    if (
      metadata.visualTier === ACTOR_VISUAL_TIER.NEARBY
      && !this.isTouchDevice
      && this.activeRenderRooms.has(metadata.room)
    ) return "reduced";
    return "none";
  }

  catVisionInterval(cat) {
    const tier = this.getActorVisualTier(cat, "cat");
    return catVisionIntervalSeconds(cat?.state, tier, this.isTouchDevice);
  }

  getDebugSnapshot() {
    const totalActors = this.stats.fullAnimationActors
      + this.stats.reducedAnimationActors
      + this.stats.distantAnimationActors
      + this.stats.sleepingVisualActors;
    return Object.freeze({
      visibleRooms: this.stats.activeRenderRooms,
      hiddenRooms: this.stats.hiddenRenderRooms,
      totalRooms: this.rooms.length,
      fullAnimationActors: this.stats.fullAnimationActors,
      reducedAnimationActors: this.stats.reducedAnimationActors,
      distantAnimationActors: this.stats.distantAnimationActors,
      sleepingVisualActors: this.stats.sleepingVisualActors,
      totalActors,
      shadowActors: this.stats.shadowCastingActors,
      averageFps: this.stats.averageFps,
      frameTimeMs: this.stats.frameTimeMs,
      worstFrameTimeMs: this.stats.worstFrameTimeMs,
      characterVisualUpdatesPerSecond: this.rates.characterVisualUpdates,
      animationSamplesPerSecond: this.rates.animationSamples,
      distantAnimationSamplesSkippedPerSecond: this.rates.distantAnimationSamplesSkipped,
      catLosChecksPerSecond: this.rates.catLosChecks,
      losRaycastsPerSecond: this.rates.losRaycasts,
      losCacheHitsPerSecond: this.rates.losCacheHits,
      aiDecisionUpdatesPerSecond: this.rates.aiDecisionUpdates,
      distantAiUpdatesSkippedPerSecond: this.rates.distantAiUpdatesSkipped,
      catVisionScansSkippedPerSecond: this.rates.catVisionScansSkipped,
      playerRoom: this.playerRoom,
      activeRoomIds: [...this.activeRenderRooms],
      visibleDoorwayIds: [...this.visibleDoorways],
    });
  }
}

export function ensurePerformanceManager(engine) {
  if (!engine) return null;
  const expansion = engine.__expansion;
  const existing = expansion?.performanceManager ?? engine.hearthmousePerformance;
  if (existing instanceof HearthmousePerformanceManager) {
    if (expansion && !expansion.performanceManager) expansion.performanceManager = existing;
    return existing;
  }
  const manager = new HearthmousePerformanceManager(engine, {
    isTouchDevice: expansion?.isTouchDevice ?? !!engine.isTouchOnlyDevice?.(),
  });
  if (expansion) expansion.performanceManager = manager;
  engine.hearthmousePerformance = manager;
  engine.getPerformanceSnapshot = () => manager.getDebugSnapshot();
  if (typeof window !== "undefined") window.hearthmousePerformance = () => manager.getDebugSnapshot();
  return manager;
}

export function runCharacterVisualScheduler(engine, delta) {
  const manager = ensurePerformanceManager(engine);
  if (!manager) return 0;
  const context = manager.visualContext;
  let updated = 0;
  let actorIndex = 0;

  const updateActor = (actor, kind) => {
    if (!actor?.rig?.root) return;
    const metadata = manager.prepareActor(actor, kind, actorIndex++);
    if (!manager.shouldUpdateVisual(actor, metadata)) {
      if (metadata.visualTier === ACTOR_VISUAL_TIER.DISTANT || metadata.visualTier === ACTOR_VISUAL_TIER.SLEEPING) {
        manager.record("distantAnimationSamplesSkipped");
      }
      return;
    }
    const visualDelta = manager.consumeVisualDelta(metadata, delta);
    actor.rig.__hearthmouseFlushVisual?.();
    context.actor = actor;
    context.kind = kind;
    context.tier = metadata.visualTier;
    context.delta = visualDelta;
    context.time = engine.time ?? manager.clock;
    for (let index = 0; index < VISUAL_STAGES.length; index++) {
      const stage = VISUAL_STAGES[index];
      try {
        stage.update(actor, context);
      } catch (error) {
        reportStageError(stage, error);
      }
    }
    manager.record("characterVisualUpdates");
    updated++;
  };

  for (let index = 0; index < (engine.mice?.length ?? 0); index++) updateActor(engine.mice[index], "mouse");
  for (let index = 0; index < (engine.cats?.length ?? 0); index++) updateActor(engine.cats[index], "cat");

  for (let index = 0; index < FRAME_STAGES.length; index++) {
    const stage = FRAME_STAGES[index];
    const last = manager.frameStageTimes.get(stage.name) ?? -Infinity;
    if (stage.interval > 0 && manager.clock - last < stage.interval) continue;
    manager.frameStageTimes.set(stage.name, manager.clock);
    try {
      stage.update(engine, { manager, delta, time: engine.time ?? manager.clock });
    } catch (error) {
      reportStageError(stage, error);
    }
  }
  return updated;
}
