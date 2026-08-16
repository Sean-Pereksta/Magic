const clamp = (value, min, max) => Math.max(min, Math.min(max, value));

export const POLICY_PROFILES = Object.freeze({
  cautious: Object.freeze({
    id: "cautious",
    label: "CAUTIOUS",
    description: "Stay close, flee early, and stop the instant tonight is fed.",
    localRadius: 3.1,
    expandedRadius: 6.2,
    fleeThreshold: 0.25,
    panicMultiplier: 2.45,
    surplusRatio: 0,
    finishSafeTrip: false,
    deepRoomPenalty: 9.5,
    valueTemptation: 0.38,
  }),
  balanced: Object.freeze({
    id: "balanced",
    label: "BALANCED",
    description: "Clear safe areas first, then widen the search only as needed.",
    localRadius: 5.4,
    expandedRadius: 11.5,
    fleeThreshold: 0.39,
    panicMultiplier: 2.28,
    surplusRatio: 0,
    finishSafeTrip: true,
    deepRoomPenalty: 4.8,
    valueTemptation: 0.82,
  }),
  desperate: Object.freeze({
    id: "desperate",
    label: "DESPERATE",
    description: "Range deep, chase rich prizes, and risk lives for a winter surplus.",
    localRadius: 9,
    expandedRadius: Infinity,
    fleeThreshold: 0.55,
    panicMultiplier: 2.12,
    surplusRatio: 0.32,
    finishSafeTrip: true,
    deepRoomPenalty: 1.35,
    valueTemptation: 1.65,
  }),
});

export function getPolicyProfile(policy) {
  return POLICY_PROFILES[policy] ?? POLICY_PROFILES.balanced;
}

const POLICY_ORDER = Object.freeze(["cautious", "balanced", "desperate"]);

export function nextColonyPolicy(policy) {
  const currentIndex = POLICY_ORDER.indexOf(getPolicyProfile(policy).id);
  return POLICY_ORDER[(currentIndex + 1) % POLICY_ORDER.length];
}

export function computePounceWindup(night, personality = "hunter") {
  const currentMaximumDifficulty = personality === "kitten" ? 0.34 : 0.48;
  const earlyWarning = personality === "kitten" ? 0.32 : 0.5;
  const campaignProgress = clamp((night - 1) / 11, 0, 1);
  return currentMaximumDifficulty + earlyWarning * (1 - campaignProgress);
}

export function forageTarget(policy, nightlyRequirement) {
  const profile = getPolicyProfile(policy);
  return (
    nightlyRequirement +
    (profile.surplusRatio > 0
      ? Math.max(3, Math.ceil(nightlyRequirement * profile.surplusRatio))
      : 0)
  );
}

export function shouldForagersStop({ policy, delivered, nightlyRequirement }) {
  return delivered >= forageTarget(policy, nightlyRequirement);
}

export function initialForagerDelay(policy, colonyIndex, randomUnit = 0.5) {
  const mode = getPolicyProfile(policy).id;
  const base = mode === "cautious" ? 0.72 : mode === "desperate" ? 0.22 : 0.42;
  const wave = mode === "cautious" ? 0.17 : mode === "desperate" ? 0.075 : 0.115;
  const jitter = mode === "cautious" ? 0.26 : mode === "desperate" ? 0.14 : 0.2;
  return base + (Math.max(0, colonyIndex) % 6) * wave + clamp(randomUnit, 0, 1) * jitter;
}

export function activeForagerLimit(policy, mouseCount, remainingFood) {
  if (mouseCount <= 0 || remainingFood <= 0) return 0;
  const mode = getPolicyProfile(policy).id;
  const foodPerMouse = mode === "cautious" ? 2.1 : mode === "desperate" ? 1.12 : 1.55;
  const extraScouts = mode === "cautious" ? 1 : mode === "desperate" ? 4 : 2;
  const minimum = mode === "cautious" ? 1 : 2;
  return Math.min(mouseCount, Math.max(minimum, Math.ceil(remainingFood / foodPerMouse) + extraScouts));
}

const MOUSE_STALL_THRESHOLDS = Object.freeze({
  escaping: 0.62,
  "to-food": 1.05,
  returning: 1.2,
  home: 1.35,
  "nesting-move": 1.5,
});

export function shouldRecoverMouse({ task, stalledFor, distanceMoved, distanceToGoal }) {
  const threshold = MOUSE_STALL_THRESHOLDS[task];
  return !!threshold && stalledFor >= threshold && distanceMoved < 0.018 && distanceToGoal > 0.16;
}

export function scoreFoodCandidate(candidate, policy, mouseCaution = 0.5) {
  const profile = getPolicyProfile(policy);
  const ringSize = policy === "cautious" ? 2.4 : policy === "balanced" ? 3.5 : 5.5;
  const ring = Math.floor(candidate.nestDistance / ringSize);
  const depthPenalty = candidate.depth * profile.deepRoomPenalty * (0.75 + mouseCaution * 0.5);
  const valueReward = candidate.value * profile.valueTemptation * (1.25 - mouseCaution * 0.45);
  const exposurePenalty = (candidate.exposure ?? 0) * (7 + mouseCaution * 7);
  return ring * 18 + candidate.mouseDistance * 1.15 + depthPenalty + exposurePenalty - valueReward;
}

export const ROOM_DEFINITIONS = Object.freeze([
  { id: "hallway", name: "Hallway", unlockNight: 2, minX: -4.7, maxX: 2.1, minZ: -11.8, maxZ: -6.38, surface: "carpet", depth: 2, color: 0x6b5a4a },
  { id: "pantry", name: "Pantry", unlockNight: 4, minX: 2.1, maxX: 7.2, minZ: -11.8, maxZ: -6.38, surface: "paper", depth: 3, color: 0x7d6848 },
  { id: "dining", name: "Dining Room", unlockNight: 5, minX: 0.4, maxX: 6.8, minZ: 6.38, maxZ: 11.6, surface: "wood", depth: 3, color: 0x6e4937 },
  { id: "bathroom", name: "Bathroom", unlockNight: 6, minX: -4.7, maxX: -1.3, minZ: -16, maxZ: -11.8, surface: "tile", depth: 4, color: 0x8ba0a0 },
  { id: "bedroom", name: "Bedroom", unlockNight: 7, minX: -1.3, maxX: 2.1, minZ: -16, maxZ: -11.8, surface: "carpet", depth: 4, color: 0x735a67 },
  { id: "children", name: "Children's Room", unlockNight: 8, minX: -10.38, maxX: -5.02, minZ: -15.6, maxZ: -10.68, surface: "carpet", depth: 5, color: 0x76654b },
  { id: "utility", name: "Utility Closet", unlockNight: 8, minX: 15, maxX: 18.8, minZ: 1.84, maxZ: 6.26, surface: "metal", depth: 5, color: 0x646d70 },
  { id: "mudroom", name: "Mudroom", unlockNight: 9, minX: 0.4, maxX: 6.8, minZ: 11.6, maxZ: 14.2, surface: "tile", depth: 5, color: 0x665f52 },
  { id: "basement-access", name: "Basement Access", unlockNight: 10, minX: 7.2, maxX: 12.4, minZ: -11.8, maxZ: -6.38, surface: "wood", depth: 6, color: 0x52473d },
  { id: "basement", name: "Basement", unlockNight: 11, minX: 7.2, maxX: 12.4, minZ: -16.8, maxZ: -11.8, surface: "tile", depth: 7, color: 0x3f4648 },
  { id: "garage", name: "Garage", unlockNight: 11, minX: 18.8, maxX: 24.5, minZ: -0.5, maxZ: 8.5, surface: "metal", depth: 7, color: 0x4a4f52 },
]);

const NIGHT_EVENTS = Object.freeze([
  { id: "movie-night", name: "Movie Night", description: "Fresh crumbs cover the living-room rug.", minNight: 1, roomWeights: { living: 3.2 }, catFocus: "living" },
  { id: "late-snack", name: "Late Snack", description: "A short trail of crumbs crosses the kitchen doorway.", minNight: 1, roomWeights: { living: 1.3, kitchen: 2.4 }, catFocus: "kitchen" },
  { id: "clean-kitchen", name: "Clean Kitchen", description: "The starting rooms were swept bare. Safer food is scarce.", minNight: 2, removeRooms: ["kitchen"], nearHomeMultiplier: 0.2 },
  { id: "grocery-night", name: "Grocery Night", description: "Paper bags and pantry boxes hide a noisy feast.", minNight: 3, roomWeights: { kitchen: 1.7, pantry: 3.4 }, noisyPaper: true },
  { id: "laundry-night", name: "Laundry Night", description: "The machines periodically mask every hurried pawstep.", minNight: 3, roomWeights: { laundry: 2.4, utility: 2.2 }, maskCycle: "laundry" },
  { id: "storm-night", name: "Storm Night", description: "Thunder opens brief windows for a reckless dash.", minNight: 1, maskCycle: "storm" },
  { id: "quiet-house", name: "Quiet House", description: "Mabel is listening to every scrape and footfall.", minNight: 2, soundSensitivity: 1.45 },
  { id: "dinner-leftovers", name: "Dinner Leftovers", description: "Rich scraps surround the dining table, and Mabel knows it.", minNight: 5, roomWeights: { dining: 4.2 }, catFocus: "dining" },
  { id: "closed-door", name: "Closed Door", description: "A familiar shortcut is shut; use the longer room-to-room route.", minNight: 4, forceCondition: "closed-shortcut" },
  { id: "open-door", name: "Open Door", description: "A door was left ajar, exposing one room ahead of schedule.", minNight: 2, temporaryOpen: true },
  { id: "human-activity", name: "Human Activity", description: "Furniture shifted after dark, changing the safest lines.", minNight: 2, forceCondition: "moved-furniture" },
  { id: "pantry-spill", name: "Pantry Spill", description: "A distant container burst. The reward is enormous and exposed.", minNight: 4, roomWeights: { pantry: 5 }, extraSet: "pantry-spill", catFocus: "pantry" },
  { id: "nothing-near-home", name: "Nothing Near Home", description: "Almost no food remains near the nest. Tonight demands a decision.", minNight: 3, nearHomeMultiplier: 0.04 },
  { id: "midnight-baking", name: "Midnight Baking", description: "Kitchen scraps are plentiful, but metal trays make every crossing loud.", minNight: 3, roomWeights: { kitchen: 3.1 }, soundSensitivity: 1.1 },
  { id: "toy-trail", name: "Toy Trail", description: "A trail of crackers leads toward the children's room.", minNight: 8, roomWeights: { children: 4 }, extraSet: "toy-trail" },
  { id: "garage-windfall", name: "Garage Windfall", description: "A torn sack waits beyond the mudroom, deep in Mabel's range.", minNight: 11, roomWeights: { garage: 4.5 }, extraSet: "garage-windfall", catFocus: "garage" },
]);

const ROOM_CONDITIONS = Object.freeze([
  { id: "chair-shifted", label: "A dining chair blocks the direct line.", minNight: 2, prop: "moved-chair" },
  { id: "box-added", label: "A cardboard box creates fresh mouse cover.", minNight: 2, prop: "cardboard-cover" },
  { id: "grocery-bag", label: "A crackling grocery bag sits near the pantry route.", minNight: 3, prop: "grocery-bag" },
  { id: "basket-shifted", label: "The laundry basket moved into the corridor.", minNight: 3, prop: "laundry-basket" },
  { id: "blanket-dropped", label: "A dropped blanket protects a once-open crossing.", minNight: 2, prop: "blanket-cover" },
  { id: "passage-exposed", label: "A mouse-sized passage has opened behind the study.", minNight: 6, prop: "passage-open" },
  { id: "closed-shortcut", label: "The hall shortcut is closed tonight.", minNight: 4, prop: "closed-shortcut" },
  { id: "moved-furniture", label: "Several familiar pieces are out of place.", minNight: 2, prop: "moved-furniture" },
]);

function seededUnit(seed) {
  const value = Math.sin(seed * 12.9898 + 78.233) * 43758.5453;
  return value - Math.floor(value);
}

export function selectNightPlan(seed, night, previousEventId = "") {
  const available = NIGHT_EVENTS.filter((event) => event.minNight <= night);
  let event = available[Math.floor(seededUnit(seed + night * 193) * available.length)];
  if (event.id === previousEventId && available.length > 1) {
    event = available[(available.indexOf(event) + 1 + (night % (available.length - 1))) % available.length];
  }
  const availableConditions = ROOM_CONDITIONS.filter((condition) => condition.minNight <= night);
  const forced = event.forceCondition
    ? availableConditions.find((condition) => condition.id === event.forceCondition)
    : null;
  const first = forced ?? availableConditions[Math.floor(seededUnit(seed * 3 + night * 71) * availableConditions.length)];
  const second = night >= 7
    ? availableConditions[Math.floor(seededUnit(seed * 7 + night * 131) * availableConditions.length)]
    : null;
  return {
    event,
    conditions: [first, second].filter((condition, index, list) => condition && list.indexOf(condition) === index),
  };
}

export function roomUnlocked(roomId, night, temporaryRoomId = null) {
  if (["living", "kitchen"].includes(roomId)) return true;
  if (roomId === "laundry") return night >= 3 || temporaryRoomId === roomId;
  if (roomId === "study") return night >= 6 || temporaryRoomId === roomId;
  const room = ROOM_DEFINITIONS.find((candidate) => candidate.id === roomId);
  return !!room && (night >= room.unlockNight || temporaryRoomId === roomId);
}

const waitForGame = () => {
  if (!window.HearthmouseInternals?.Engine) {
    window.setTimeout(waitForGame, 0);
    return;
  }
  installEnginePatches(window.HearthmouseInternals);
  waitForEngineInstance();
};

const waitForEngineInstance = () => {
  const engine = window.hearthmouseEngine;
  if (!engine || engine.disposed) {
    window.setTimeout(waitForEngineInstance, 40);
    return;
  }
  ensureExpansion(engine);
  mountColonyControls(engine);
};

function installEnginePatches(I) {
  const proto = I.Engine.prototype;
  if (proto.__colonyExpansionInstalled) return;
  Object.defineProperty(proto, "__colonyExpansionInstalled", { value: true });

  const base = {
    beginNight: proto.beginNight,
    endNight: proto.endNight,
    restartCampaign: proto.restartCampaign,
    createCats: proto.createCats,
    createMice: proto.createMice,
    spawnFood: proto.spawnFood,
    updateGame: proto.updateGame,
    updateCats: proto.updateCats,
    updateCatPatrol: proto.updateCatPatrol,
    updateCatChase: proto.updateCatChase,
    processCatVision: proto.processCatVision,
    setCatState: proto.setCatState,
    mouseDeposit: proto.mouseDeposit,
    registerPrizeDelivery: proto.registerPrizeDelivery,
  };

  proto.beginNight = function expandedBeginNight(night) {
    ensureExpansion(this);
    return base.beginNight.call(this, night);
  };

  proto.restartCampaign = function expandedRestartCampaign() {
    ensureExpansion(this);
    this.__expansion.campaignSeed = Math.floor(Math.random() * 1_000_000_000);
    this.__expansion.previousEventId = "";
    this.__expansion.currentPlan = null;
    this.__expansion.temporaryRoomId = null;
    this.__expansion.planNight = 1;
    this.__expansion.wellFedNights = 0;
    this.__expansion.upgrades = { tunnel: false, scouts: 0, insulation: 0 };
    this.__expansion.routeRevision++;
    this.__expansion.routeCache.clear();
    return base.restartCampaign.call(this);
  };

  proto.endNight = function expandedEndNight() {
    const expansion = ensureExpansion(this);
    const wasFed = this.snapshot.deliveredTonight >= this.snapshot.tonightRequirement;
    base.endNight.call(this);
    if (!wasFed) expansion.wellFedNights = 0;
    else if (this.snapshot.phase === "summary") {
      expansion.wellFedNights++;
      const alreadyGrew = (this.snapshot.summary?.newMice.length ?? 0) > 0;
      const interval = Math.max(1, (this.colonyPolicy === "cautious" ? 3 : 2) - Math.min(1, expansion.upgrades.insulation));
      const currentPopulation = 1 + this.colony.filter((member) => member.alive).length;
      if (!alreadyGrew && expansion.wellFedNights >= interval && currentPopulation < 24) {
        const newcomer = this.createNewMouse();
        this.colony.push(newcomer);
        this.newMiceTonight.push(newcomer.name);
        this.snapshot.population = currentPopulation + 1;
        this.snapshot.winterGoal = this.snapshot.population * 8;
        this.snapshot.summary.newMice.push(newcomer.name);
        expansion.wellFedNights = 0;
        this.publish(true);
      } else if (alreadyGrew) expansion.wellFedNights = 0;
    }
  };

  proto.createCats = function expandedCreateCats() {
    ensureExpansion(this);
    base.createCats.call(this);
    this.cats.forEach((cat, index) => initializeCat(cat, index));
  };

  proto.createMice = function expandedCreateMice() {
    ensureExpansion(this);
    base.createMice.call(this);
    this.mice.forEach((mouse, index) => initializeMouse(mouse, index, this.colonyPolicy));
  };

  proto.chooseNightEvent = function expandedNightEvent(night) {
    const expansion = ensureExpansion(this);
    const plan = selectNightPlan(expansion.campaignSeed, night, expansion.previousEventId);
    expansion.previousEventId = plan.event.id;
    expansion.currentPlan = plan;
    expansion.planNight = night;
    expansion.temporaryRoomId = plan.event.temporaryOpen ? nextLockedRoom(night) : null;
    const unlocked = ROOM_DEFINITIONS.filter((room) => room.unlockNight === night).map((room) => room.name);
    if (night === 3) unlocked.unshift("Laundry Room");
    if (night === 6) unlocked.unshift("Deep Study");
    const unlockText = unlocked.length ? ` New tonight: ${unlocked.join(" and ")}.` : "";
    this.eventName = plan.event.name;
    this.eventDescription = `${plan.event.description}${unlockText}`;
    applyNightPlan(this, plan);
  };

  proto.spawnFood = function expandedSpawnFood() {
    const expansion = ensureExpansion(this);
    spawnExpandedFood(this, expansion, I);
  };

  proto.updateGame = function expandedUpdateGame(delta) {
    const expansion = ensureExpansion(this);
    updateEnvironmentalWindow(this, expansion, delta);
    base.updateGame.call(this, delta);
  };

  proto.updateMice = function expandedMouseAI(delta) {
    updateColonyMice(this, ensureExpansion(this), I, delta);
  };

  proto.mouseIsThreatened = function expandedThreatCheck(mouse, cat, distance) {
    return assessMouseThreat(this, ensureExpansion(this), mouse, cat, distance).score >= getPolicyProfile(this.colonyPolicy).fleeThreshold;
  };

  proto.sendMouseToShelter = function expandedShelterChoice(mouse, cat) {
    return sendMouseToBestShelter(this, ensureExpansion(this), I, mouse, cat);
  };

  proto.assignFood = function expandedFoodChoice(mouse) {
    assignLocalFood(this, ensureExpansion(this), I, mouse);
  };

  proto.planMousePath = function expandedMousePath(mouse) {
    const target = mouse.task === "escaping" && mouse.escapeGoal
      ? mouse.escapeGoal
      : ["returning", "home", "nesting-move"].includes(mouse.task)
        ? (mouse.nestActivityGoal ?? this.world.nestDeposit)
        : (mouse.targetFood?.mesh.position ?? this.world.nestCenter);
    const result = buildSmartPath(this, ensureExpansion(this), I, mouse.rig.root.position, target, "mouse");
    mouse.path = result.path;
    mouse.pathIndex = 0;
  };

  proto.planCatPath = function expandedCatPath(cat, target) {
    const result = buildSmartPath(this, ensureExpansion(this), I, cat.rig.root.position, target, "cat");
    cat.path = result.path;
    cat.pathIndex = 0;
    cat.pathReachable = result.reachedGoal;
    cat.pathRemainingDistance = result.remainingDistance;
  };

  proto.updateCats = function expandedCatAI(delta) {
    const expansion = ensureExpansion(this);
    prepareCatLeisure(this, expansion, delta);
    base.updateCats.call(this, delta);
    applyCatBehaviorAnimation(this, expansion, delta);
  };

  proto.updateCatPatrol = function expandedPatrol(cat, delta, speedOverride) {
    const expansion = ensureExpansion(this);
    if (["grooming", "watching", "cover-watch"].includes(cat.leisureMode) && cat.leisureTimer > 0) return 0;
    if ((!cat.path.length || cat.pathIndex >= cat.path.length) && this.world.patrolPoints.length) {
      const focus = expansion.currentPlan?.event.catFocus;
      const focused = focus
        ? this.world.patrolPoints
            .map((point, index) => ({ point, index }))
            .filter(({ point }) => roomForPosition(point.x, point.z) === focus)
        : [];
      const pool = focused.length && Math.random() < 0.62
        ? focused
        : this.world.patrolPoints.map((point, index) => ({ point, index }));
      const choice = pool[Math.floor(Math.random() * pool.length)];
      if (choice) cat.patrolIndex = (choice.index - 1 + this.world.patrolPoints.length) % this.world.patrolPoints.length;
    }
    return base.updateCatPatrol.call(this, cat, delta, speedOverride);
  };

  proto.updateCatChase = function expandedPounce(cat, delta) {
    const previousPhase = cat.pouncePhase;
    const result = base.updateCatChase.call(this, cat, delta);
    if (previousPhase === "none" && cat.pouncePhase === "windup") {
      cat.pounceWindupDuration = computePounceWindup(this.snapshot.night, cat.personality);
      cat.pounceTimer = cat.pounceWindupDuration;
      cat.pounceCuePlayed = false;
    }
    return result;
  };

  proto.processCatVision = function expandedCatVision(cat, target, interval) {
    const expansion = ensureExpansion(this);
    if (cat.state !== "chase" && cat.leisureMode === "grooming" && target) {
      const closeEnoughToInterrupt = target.distance < 0.82;
      return base.processCatVision.call(this, cat, target, interval * (closeEnoughToInterrupt ? 1 : 0.24));
    }
    const sensitivity = expansion.currentPlan?.event.id === "quiet-house" ? 1.16 : 1;
    return base.processCatVision.call(this, cat, target, interval * sensitivity);
  };

  proto.setCatState = function expandedCatState(cat, state, duration) {
    if (state === "chase" || state === "alert" || state === "investigating") {
      cat.leisureMode = null;
      cat.leisureTimer = 0;
    }
    return base.setCatState.call(this, cat, state, duration);
  };

  proto.emitNoise = function expandedNoise(position, strength) {
    emitExpandedNoise(this, ensureExpansion(this), position, strength);
  };

  proto.mouseDeposit = function expandedMouseDeposit(mouse) {
    base.mouseDeposit.call(this, mouse);
    if (colonyShouldStop(this)) sendMouseHome(this, mouse);
  };

  proto.registerPrizeDelivery = function expandedPrizeDelivery(food) {
    const expansion = ensureExpansion(this);
    if (!food.prizeId || this.deliveredPrizeIds.has(food.prizeId)) return false;
    this.deliveredPrizeIds.add(food.prizeId);
    if (food.prizeEffect === "nursery") {
      this.deliveredNurseryBoost++;
      expansion.upgrades.insulation++;
      this.showMessage("Warm nesting material will help more young mice forage at dawn.", 4.4);
    } else if (food.prizeEffect === "tunnel") {
      expansion.upgrades.tunnel = true;
      setDynamicPropActive(expansion, "passage-block", false);
      expansion.routeRevision++;
      expansion.routeCache.clear();
      this.showMessage("A permanent mouse tunnel now links the study and hallway.", 4.4);
    } else if (food.prizeEffect === "scouts") {
      expansion.upgrades.scouts++;
      this.showMessage("The recovered house map lets colony scouts warn one another sooner.", 4.4);
    } else {
      this.showMessage("A deep-room winter cache made it safely home.", 3.8);
    }
    return true;
  };

  proto.setColonyPolicy = function setColonyPolicy(policy) {
    if (!POLICY_PROFILES[policy] || !this.insideNest()) return false;
    this.colonyPolicy = policy;
    this.snapshot.colonyPolicy = policy;
    try { window.localStorage.setItem("hearthmouse-colony-policy", policy); } catch {}
    const profile = getPolicyProfile(policy);
    this.showMessage(`${profile.label}: ${profile.description}`, 3.2);
    this.publish(true);
    return true;
  };
}

function ensureExpansion(engine) {
  if (engine.__expansion) return engine.__expansion;
  let storedPolicy = "balanced";
  try { storedPolicy = window.localStorage.getItem("hearthmouse-colony-policy") || "balanced"; } catch {}
  if (!POLICY_PROFILES[storedPolicy]) storedPolicy = "balanced";
  engine.colonyPolicy = storedPolicy;
  engine.snapshot.colonyPolicy = storedPolicy;
  const expansion = {
    campaignSeed: Math.floor(Math.random() * 1_000_000_000),
    previousEventId: "",
    currentPlan: null,
    planNight: engine.snapshot.night || 1,
    temporaryRoomId: null,
    routeRevision: 1,
    routeCache: new Map(),
    dangerSignals: [],
    dynamicProps: new Map(),
    roomDoors: [],
    extraPatrolPoints: [],
    navEdges: [],
    ambientMask: 1,
    maskActive: false,
    lastMaskActive: false,
    frame: 0,
    reservationSweepTimer: 0,
    wellFedNights: 0,
    upgrades: { tunnel: false, scouts: 0, insulation: 0 },
  };
  engine.__expansion = expansion;
  buildExpandedHouse(engine, expansion, window.HearthmouseInternals);
  return expansion;
}

function initializeMouse(mouse, index, policy = "balanced") {
  mouse.delay = Math.min(mouse.delay, initialForagerDelay(policy, index, Math.random()));
  mouse.aiDecisionTimer = 0.04 + (index % 8) * 0.027;
  mouse.safeTimer = 0;
  mouse.noiseTimer = 0.25 + (index % 5) * 0.11;
  mouse.lastCatDistances = new Map();
  mouse.colonyIndex = index;
  mouse.foragingSector = index % 6;
  mouse.nestActivityTimer = 1 + (index % 7) * 0.36;
  mouse.nestActivityGoal = null;
  mouse.lastThreatScore = 0;
  mouse.progressPosition = mouse.rig.root.position.clone();
  mouse.progressSampleTimer = 0;
  mouse.stalledFor = 0;
  mouse.recoveryAttempts = 0;
  mouse.blockedFoodUntil = new Map();
}

function initializeCat(cat, index) {
  cat.leisureMode = null;
  cat.leisureTimer = 0;
  cat.leisureCooldown = 2.5 + index;
  cat.pounceWindupDuration = computePounceWindup(1, cat.personality);
  cat.pounceCuePlayed = false;
}

function buildExpandedHouse(engine, expansion, I) {
  const world = engine.world;
  if (world.__expandedHouseBuilt) return;
  world.__expandedHouseBuilt = true;
  expansion.world = world;
  const roomRoot = new I.Group();
  roomRoot.name = "expanded-house-rooms";
  world.root.add(roomRoot);

  const materials = new Map();
  const material = (color, roughness = 0.9) => {
    const key = `${color}:${roughness}`;
    if (!materials.has(key)) materials.set(key, new I.MeshStandardMaterial({ color, roughness, metalness: 0 }));
    return materials.get(key);
  };

  const addBox = ({ name, x, y, z, w, h, d, color = 0x66574b, collide = false, catOnly = false, occlude = true, dynamicId = null, active = true }) => {
    const mesh = new I.Mesh(new I.BoxGeometry(w, h, d), material(color));
    mesh.name = name;
    mesh.position.set(x, y, z);
    mesh.visible = active;
    mesh.castShadow = false;
    mesh.receiveShadow = true;
    roomRoot.add(mesh);
    if (occlude) {
      mesh.userData.occluder = true;
      if (active) world.occluders.push(mesh);
    }
    let collider = null;
    if (collide) {
      collider = { minX: x - w / 2, maxX: x + w / 2, minZ: z - d / 2, maxZ: z + d / 2, minY: y - h / 2, maxY: y + h / 2, name, catOnly, active };
      world.colliders.push(collider);
    }
    if (dynamicId) expansion.dynamicProps.set(dynamicId, { mesh, collider, occlude });
    return { mesh, collider };
  };

  const disableBase = (name) => {
    const mesh = world.root.getObjectByName(name);
    if (mesh) {
      mesh.visible = false;
      const index = world.occluders.indexOf(mesh);
      if (index >= 0) world.occluders.splice(index, 1);
    }
    world.colliders.filter((collider) => collider.name === name).forEach((collider) => { collider.active = false; });
  };

  const addHorizontalWall = (name, z, minX, maxX, openings = [], color = 0xb9ad99) => {
    const sorted = openings.map(({ center, width }) => [center - width / 2, center + width / 2]).sort((a, b) => a[0] - b[0]);
    let cursor = minX;
    sorted.forEach(([start, end], index) => {
      if (start > cursor) addBox({ name: `${name}-${index}`, x: (cursor + start) / 2, y: 1.475, z, w: start - cursor, h: 2.95, d: 0.2, color, collide: true });
      cursor = Math.max(cursor, end);
    });
    if (cursor < maxX) addBox({ name: `${name}-end`, x: (cursor + maxX) / 2, y: 1.475, z, w: maxX - cursor, h: 2.95, d: 0.2, color, collide: true });
  };

  const addVerticalWall = (name, x, minZ, maxZ, openings = [], color = 0xb9ad99) => {
    const sorted = openings.map(({ center, width }) => [center - width / 2, center + width / 2]).sort((a, b) => a[0] - b[0]);
    let cursor = minZ;
    sorted.forEach(([start, end], index) => {
      if (start > cursor) addBox({ name: `${name}-${index}`, x, y: 1.475, z: (cursor + start) / 2, w: 0.2, h: 2.95, d: start - cursor, color, collide: true });
      cursor = Math.max(cursor, end);
    });
    if (cursor < maxZ) addBox({ name: `${name}-end`, x, y: 1.475, z: (cursor + maxZ) / 2, w: 0.2, h: 2.95, d: maxZ - cursor, color, collide: true });
  };

  const roomOpenings = {
    hallway: { south: [{ center: -1.3, width: 1.4 }], east: [{ center: -8.9, width: 1.3 }], north: [{ center: -3, width: 1.05 }, { center: 0.45, width: 1.05 }], west: [{ center: -9, width: 0.72 }] },
    pantry: { south: [{ center: 4.5, width: 1.25 }], west: [{ center: -8.9, width: 1.3 }], east: [{ center: -8.9, width: 1.1 }] },
    dining: { north: [{ center: 3.6, width: 1.4 }], south: [{ center: 3.6, width: 1.1 }] },
    bathroom: { south: [{ center: -3, width: 1.05 }] },
    bedroom: { south: [{ center: 0.45, width: 1.05 }] },
    children: { south: [{ center: -7.7, width: 1.15 }] },
    utility: { west: [{ center: 4.05, width: 1.2 }], east: [{ center: 4, width: 1.2 }] },
    mudroom: { north: [{ center: 3.6, width: 1.1 }] },
    "basement-access": { west: [{ center: -8.9, width: 1.1 }], north: [{ center: 9.8, width: 1.05 }] },
    basement: { south: [{ center: 9.8, width: 1.05 }] },
    garage: { west: [{ center: 4, width: 1.2 }] },
  };

  ROOM_DEFINITIONS.forEach((room) => {
    const centerX = (room.minX + room.maxX) / 2;
    const centerZ = (room.minZ + room.maxZ) / 2;
    const width = room.maxX - room.minX;
    const depth = room.maxZ - room.minZ;
    addBox({ name: `${room.id}-floor`, x: centerX, y: -0.055, z: centerZ, w: width, h: 0.11, d: depth, color: room.color, occlude: false });
    addBox({ name: `${room.id}-ceiling`, x: centerX, y: 3.02, z: centerZ, w: width, h: 0.12, d: depth, color: 0xb9ad99, occlude: false });
    const openings = roomOpenings[room.id] ?? {};
    addHorizontalWall(`${room.id}-north-wall`, room.minZ, room.minX, room.maxX, openings.north ?? []);
    addHorizontalWall(`${room.id}-south-wall`, room.maxZ, room.minX, room.maxX, openings.south ?? []);
    addVerticalWall(`${room.id}-west-wall`, room.minX, room.minZ, room.maxZ, openings.west ?? []);
    addVerticalWall(`${room.id}-east-wall`, room.maxX, room.minZ, room.maxZ, openings.east ?? []);
  });

  disableBase("north-wall-east");
  addHorizontalWall("rebuilt-main-north", -6.38, -6.95, 10.48, [{ center: -1.3, width: 1.4 }, { center: 4.5, width: 1.25 }]);
  disableBase("south-wall");
  disableBase("south-baseboard");
  addHorizontalWall("rebuilt-main-south", 6.38, -10.48, 10.48, [{ center: 3.6, width: 1.4 }], 0xa99f8c);
  disableBase("laundry-east-wall");
  addVerticalWall("rebuilt-laundry-east", 15, 1.84, 6.26, [{ center: 4.05, width: 1.2 }]);
  disableBase("study-north-wall");
  addHorizontalWall("rebuilt-study-north", -10.68, -10.38, -5.02, [{ center: -7.7, width: 1.15 }], 0xa99f8c);
  disableBase("study-east-wall");
  addVerticalWall("rebuilt-study-east", -5.02, -10.68, -6.38, [{ center: -9, width: 0.72 }]);

  const addDoor = (id, roomId, unlockNight, x, z, w, d) => {
    const item = addBox({ name: `locked-${id}`, x, y: 1.08, z, w, h: 2.16, d, color: 0x35271f, collide: true });
    const door = { id, roomId, unlockNight, ...item };
    expansion.roomDoors.push(door);
    return door;
  };
  addDoor("hallway-door", "hallway", 2, -1.3, -6.33, 1.28, 0.16);
  addDoor("pantry-hall-door", "pantry", 4, 2.05, -8.9, 0.16, 1.18);
  addDoor("pantry-kitchen-door", "pantry", 4, 4.5, -6.33, 1.14, 0.16);
  addDoor("dining-door", "dining", 5, 3.6, 6.33, 1.28, 0.16);
  addDoor("bathroom-door", "bathroom", 6, -3, -11.75, 0.95, 0.16);
  addDoor("bedroom-door", "bedroom", 7, 0.45, -11.75, 0.95, 0.16);
  addDoor("children-door", "children", 8, -7.7, -10.63, 1.05, 0.16);
  addDoor("utility-door", "utility", 8, 15.05, 4.05, 0.16, 1.1);
  addDoor("mudroom-door", "mudroom", 9, 3.6, 11.55, 1, 0.16);
  addDoor("basement-access-door", "basement-access", 10, 7.15, -8.9, 0.16, 1);
  addDoor("basement-door", "basement", 11, 9.8, -11.75, 0.95, 0.16);
  addDoor("garage-door", "garage", 11, 18.75, 4, 0.16, 1.1);

  const addCover = (id, roomId, x, z, color = 0x4b392d) => {
    addBox({ name: `${id}-roof`, x, y: 0.17, z, w: 1.15, h: 0.12, d: 0.78, color, collide: true, catOnly: true });
    addBox({ name: `${id}-back`, x, y: 0.1, z: z - 0.35, w: 1.15, h: 0.2, d: 0.08, color, collide: true });
    world.shelterPoints.push({ id, roomId, position: new I.Vector3(x, 0.025, z + 0.08), unlockNight: ROOM_DEFINITIONS.find((room) => room.id === roomId)?.unlockNight ?? 1 });
  };

  const addFurniture = (name, roomId, x, z, w, d, color) => {
    addBox({ name: `${name}-top`, x, y: 0.68, z, w, h: 0.12, d, color, collide: true, catOnly: true });
    for (const sideX of [-1, 1]) for (const sideZ of [-1, 1]) {
      addBox({
        name: `${name}-leg-${sideX}-${sideZ}`,
        x: x + sideX * Math.max(0.12, w * 0.38),
        y: 0.31,
        z: z + sideZ * Math.max(0.12, d * 0.34),
        w: 0.1,
        h: 0.62,
        d: 0.1,
        color,
        collide: true,
      });
    }
    addCover(`${name}-gap`, roomId, x, z, color);
  };

  addFurniture("hall-console", "hallway", -3.85, -8.1, 1.15, 0.5, 0x4b3528);
  addFurniture("pantry-shelves", "pantry", 6.45, -9.1, 0.65, 3.2, 0x5d4028);
  addFurniture("dining-sideboard", "dining", 5.9, 9.6, 1.25, 0.65, 0x4e3024);
  addFurniture("bath-vanity", "bathroom", -4.05, -14.7, 0.75, 1.45, 0x73888a);
  addFurniture("bedroom-bed", "bedroom", 1.2, -14.2, 1.25, 2.5, 0x5d4657);
  addFurniture("toy-chest", "children", -9.35, -13.9, 1.35, 0.75, 0x755534);
  addFurniture("utility-shelf", "utility", 17.75, 4.9, 0.65, 1.8, 0x586063);
  addFurniture("mudroom-bench", "mudroom", 5.75, 12.8, 1.45, 0.62, 0x544538);
  addFurniture("stairs-cabinet", "basement-access", 11.45, -9.4, 0.65, 2.4, 0x40362e);
  addFurniture("basement-crates", "basement", 8.3, -15.2, 1.35, 1.2, 0x51402e);
  addFurniture("garage-workbench", "garage", 23.4, 6.7, 1.45, 2.1, 0x45494a);

  addBox({ name: "garage-car", x: 21.5, y: 0.55, z: 2.15, w: 2.1, h: 1.1, d: 4.1, color: 0x303a41, collide: true });
  addCover("garage-under-car", "garage", 21.5, 4.45, 0x303a41);
  addBox({ name: "bath-tub", x: -2.15, y: 0.35, z: -14.4, w: 1.05, h: 0.7, d: 2.25, color: 0xb7c0bd, collide: true });
  addCover("bath-pipe-gap", "bathroom", -2.4, -12.55, 0x697777);

  const dynamic = (id, options) => addBox({ ...options, dynamicId: id, active: options.active ?? false });
  dynamic("moved-chair", { name: "night-moved-chair", x: 1.2, y: 0.38, z: 1.15, w: 0.72, h: 0.76, d: 0.72, color: 0x493329, collide: true });
  dynamic("cardboard-cover", { name: "night-cardboard-box", x: -1.05, y: 0.17, z: 1.35, w: 1.2, h: 0.34, d: 0.9, color: 0x8f7148, collide: true, catOnly: true });
  dynamic("grocery-bag", { name: "night-grocery-bag", x: 4.7, y: 0.26, z: -5.35, w: 0.78, h: 0.52, d: 0.68, color: 0x9a8057, collide: true });
  dynamic("laundry-basket", { name: "night-laundry-basket", x: 11.45, y: 0.24, z: 4.55, w: 0.95, h: 0.48, d: 0.8, color: 0x7a6747, collide: true, catOnly: true });
  dynamic("blanket-cover", { name: "night-blanket", x: -0.7, y: 0.11, z: -2.15, w: 1.8, h: 0.22, d: 1.3, color: 0x6b3e4c, collide: true, catOnly: true });
  dynamic("closed-shortcut", { name: "night-closed-shortcut", x: 2.05, y: 1.05, z: -8.9, w: 0.16, h: 2.1, d: 1.18, color: 0x392a22, collide: true });
  dynamic("passage-block", { name: "study-passage-block", x: -5.02, y: 0.18, z: -9, w: 0.18, h: 0.36, d: 0.62, color: 0x795d3d, collide: true, active: true });
  addBox({ name: "study-passage-low-roof", x: -4.86, y: 0.17, z: -9, w: 0.45, h: 0.12, d: 0.74, color: 0x403027, collide: true, catOnly: true });
  world.shelterPoints.push({ id: "study-hall-passage", roomId: "hallway", position: new I.Vector3(-4.83, 0.025, -9), unlockNight: 6 });
  world.shelterPoints.push(
    { id: "night-cardboard-cover", roomId: "living", dynamicProp: "cardboard-cover", position: new I.Vector3(-1.05, 0.025, 1.35), unlockNight: 1 },
    { id: "night-blanket-cover", roomId: "living", dynamicProp: "blanket-cover", position: new I.Vector3(-0.7, 0.025, -2.15), unlockNight: 1 },
    { id: "night-laundry-basket", roomId: "laundry", dynamicProp: "laundry-basket", position: new I.Vector3(11.45, 0.025, 4.55), unlockNight: 3 },
  );

  expansion.extraPatrolPoints = ROOM_DEFINITIONS.flatMap((room) => [
    { roomId: room.id, unlockNight: room.unlockNight, position: new I.Vector3((room.minX + room.maxX) / 2, 0, (room.minZ + room.maxZ) / 2) },
    { roomId: room.id, unlockNight: room.unlockNight, position: new I.Vector3(room.minX + 0.7, 0, room.maxZ - 0.7) },
  ]);

  addExpandedFoodSpawns(world, I);
  expansion.navEdges = createNavigationEdges(I);

  const baseSetNight = world.setNight.bind(world);
  world.setNight = (night) => {
    baseSetNight(night);
    for (const [roomId, objectName] of [["laundry", "locked-laundry-door"], ["study", "locked-study-door"]]) {
      if (expansion.temporaryRoomId !== roomId) continue;
      const mesh = world.root.getObjectByName(objectName);
      const collider = world.colliders.find((candidate) => candidate.name === objectName);
      if (mesh && collider) setWorldObjectActive(expansion, { mesh, collider }, false);
    }
    expansion.roomDoors.forEach((door) => {
      const open = roomUnlocked(door.roomId, night, expansion.temporaryRoomId);
      setWorldObjectActive(expansion, door, !open);
    });
    const existing = new Set(world.patrolPoints.map((point) => `${point.x.toFixed(2)}:${point.z.toFixed(2)}`));
    expansion.extraPatrolPoints
      .filter((entry) => roomUnlocked(entry.roomId, night, expansion.temporaryRoomId))
      .forEach((entry) => {
        const key = `${entry.position.x.toFixed(2)}:${entry.position.z.toFixed(2)}`;
        if (!existing.has(key)) world.patrolPoints.push(entry.position);
      });
  };

  const baseSurfaceAt = world.surfaceAt.bind(world);
  world.surfaceAt = (x, z) => {
    const room = ROOM_DEFINITIONS.find((candidate) => x >= candidate.minX && x <= candidate.maxX && z >= candidate.minZ && z <= candidate.maxZ);
    if (room) return room.surface;
    const plan = expansion.currentPlan;
    if (plan?.event.noisyPaper && x > 3.8 && x < 6.2 && z < -4.7 && z > -6.1) return "paper";
    return baseSurfaceAt(x, z);
  };

  world.setNight(engine.snapshot.night || 1);
}

function addExpandedFoodSpawns(world, I) {
  const V = I.Vector3;
  const add = (x, z, kind, value, room, unlockNight, prizeId = null, prizeEffect = null) => {
    world.foodSpawns.push({ position: new V(x, 0.018, z), kind, value, room, unlockNight, prizeId, prizeEffect });
  };
  [
    [-3.8, -7.5, "cereal", 1, "hallway", 2], [-0.8, -9.3, "cracker", 2, "hallway", 2],
    [3.1, -7.4, "nut", 2, "pantry", 4], [5.9, -10.2, "cheese", 3, "pantry", 4], [4.8, -9.1, "cracker", 5, "pantry", 4, "pantry-seed-tin", "winter-cache"],
    [1.4, 8.1, "cereal", 1, "dining", 5], [4.5, 9.3, "cheese", 3, "dining", 5], [3.3, 10.7, "nut", 4, "dining", 5],
    [-3.8, -13, "cracker", 2, "bathroom", 6], [-2.2, -15.1, "nut", 4, "bathroom", 6, "bathroom-thread", "tunnel"],
    [-0.5, -13.2, "cereal", 2, "bedroom", 7], [1.2, -15.2, "cheese", 4, "bedroom", 7, "bedroom-wool", "nursery"],
    [-9.2, -12.2, "cracker", 2, "children", 8], [-6.2, -14.4, "cheese", 4, "children", 8], [-8.1, -14.6, "nut", 5, "children", 8, "child-house-map", "scouts"],
    [16.1, 2.8, "cereal", 2, "utility", 8], [17.8, 5.4, "cracker", 4, "utility", 8, "dryer-lint", "nursery"],
    [1.2, 12.7, "nut", 2, "mudroom", 9], [5.5, 13.5, "cheese", 4, "mudroom", 9],
    [8.1, -7.6, "cereal", 2, "basement-access", 10], [11.2, -10.4, "cracker", 4, "basement-access", 10],
    [8.1, -15.6, "nut", 4, "basement", 11], [10.8, -14.4, "cheese", 6, "basement", 11, "cellar-winter-cache", "winter-cache"],
    [20.1, 0.5, "cereal", 2, "garage", 11], [23.3, 7.2, "cracker", 4, "garage", 11], [21.9, 6.6, "cheese", 7, "garage", 11, "garage-grain-sack", "winter-cache"],
  ].forEach((args) => add(...args));
}

function createNavigationEdges(I) {
  const V = I.Vector3;
  const edge = (a, b, x, z, options = {}) => ({ a, b, point: new V(x, 0.025, z), ...options });
  return [
    edge("living", "kitchen", 0, 0, { dynamicProp: "main-shortcut" }),
    edge("living", "study", -7.65, -6.18, { unlockNight: 6 }),
    edge("kitchen", "laundry", 10.25, 4.05, { unlockNight: 3 }),
    edge("living", "hallway", -1.3, -6.18, { doorId: "hallway-door" }),
    edge("kitchen", "pantry", 4.5, -6.18, { doorId: "pantry-kitchen-door" }),
    edge("hallway", "pantry", 2, -8.9, { doorId: "pantry-hall-door", dynamicProp: "closed-shortcut" }),
    edge("hallway", "bathroom", -3, -11.62, { doorId: "bathroom-door" }),
    edge("hallway", "bedroom", 0.45, -11.62, { doorId: "bedroom-door" }),
    edge("study", "children", -7.7, -10.52, { doorId: "children-door" }),
    edge("kitchen", "dining", 3.6, 6.18, { doorId: "dining-door" }),
    edge("dining", "mudroom", 3.6, 11.42, { doorId: "mudroom-door" }),
    edge("laundry", "utility", 14.82, 4.05, { doorId: "utility-door" }),
    edge("utility", "garage", 18.62, 4, { doorId: "garage-door" }),
    edge("pantry", "basement-access", 7.02, -8.9, { doorId: "basement-access-door" }),
    edge("basement-access", "basement", 9.8, -11.62, { doorId: "basement-door" }),
    edge("study", "hallway", -4.86, -9, { mouseOnly: true, passage: true }),
  ];
}

function nextLockedRoom(night) {
  if (night < 3) return "laundry";
  if (night < 6) return "study";
  return ROOM_DEFINITIONS.find((room) => room.unlockNight > night)?.id ?? null;
}

function applyNightPlan(engine, plan) {
  const expansion = engine.__expansion;
  expansion.routeRevision++;
  expansion.routeCache.clear();
  expansion.dangerSignals.length = 0;
  expansion.maskActive = false;
  expansion.lastMaskActive = false;
  expansion.ambientMask = 1;
  ["moved-chair", "cardboard-cover", "grocery-bag", "laundry-basket", "blanket-cover", "closed-shortcut"].forEach((id) => setDynamicPropActive(expansion, id, false));
  const passageOpen = expansion.upgrades.tunnel || plan.conditions.some((condition) => condition.prop === "passage-open");
  setDynamicPropActive(expansion, "passage-block", !passageOpen);
  plan.conditions.forEach((condition) => {
    if (condition.prop === "moved-furniture") {
      setDynamicPropActive(expansion, "moved-chair", true);
      setDynamicPropActive(expansion, "laundry-basket", true);
    } else if (condition.prop && condition.prop !== "passage-open") {
      setDynamicPropActive(expansion, condition.prop, true);
    }
  });
  if (plan.event.id === "pantry-spill" || plan.event.id === "grocery-night") setDynamicPropActive(expansion, "grocery-bag", true);
  engine.world.setNight(expansion.planNight || engine.snapshot.night || 1);
}

function setDynamicPropActive(expansion, id, active) {
  const prop = expansion.dynamicProps.get(id);
  if (!prop) return;
  setWorldObjectActive(expansion, prop, active);
}

function setWorldObjectActive(expansion, object, active) {
  object.mesh.visible = active;
  if (object.collider) object.collider.active = active;
  if (!object.occlude && object.occlude !== undefined) return;
  const occluders = expansion.world?.occluders;
  if (!occluders) return;
  const index = occluders.indexOf(object.mesh);
  if (active && index < 0) occluders.push(object.mesh);
  if (!active && index >= 0) occluders.splice(index, 1);
}

function spawnExpandedFood(engine, expansion, I) {
  const night = engine.snapshot.night || 1;
  const population = engine.startOfNightPopulation || engine.snapshot.population || 4;
  const plan = expansion.currentPlan ?? selectNightPlan(expansion.campaignSeed, night);
  const event = plan.event;
  let spawns = engine.world.foodSpawns.filter((spawn) => {
    if (!roomUnlocked(spawn.room, night, expansion.temporaryRoomId)) return false;
    if (spawn.prizeId && engine.claimedPrizeIds.has(spawn.prizeId)) return false;
    if (event.removeRooms?.includes(spawn.room)) return false;
    const nestDistance = spawn.position.distanceTo(engine.world.nestCenter);
    if (event.nearHomeMultiplier != null && nestDistance < 4.2) {
      return seededUnit(expansion.campaignSeed + night * 101 + spawn.position.x * 17 + spawn.position.z * 29) < event.nearHomeMultiplier;
    }
    return true;
  });

  const extras = eventExtraSpawns(event.extraSet, I, night);
  spawns = [...spawns, ...extras];
  const prizes = spawns.filter((spawn) => spawn.prizeId);
  const ordinary = spawns.filter((spawn) => !spawn.prizeId);
  const desired = Math.min(ordinary.length, Math.max(12, Math.ceil(population * 0.78) + 7));
  const weighted = ordinary
    .map((spawn, index) => {
      const roomWeight = event.roomWeights?.[spawn.room] ?? 1;
      const rank = seededUnit(expansion.campaignSeed + night * 977 + index * 47) / roomWeight;
      return { spawn, rank };
    })
    .sort((a, b) => a.rank - b.rank)
    .slice(0, desired)
    .map(({ spawn }) => spawn);
  const selected = [...weighted, ...prizes];
  const batches = population > 14 ? 2 : 1;
  let id = 0;
  for (let batch = 0; batch < batches; batch++) {
    for (const [index, spawn] of selected.entries()) {
      if (batch && (spawn.prizeId || roomDepth(spawn.room) >= 5)) continue;
      const mesh = I.makeFood(spawn.kind, night * 1000 + index + batch * 79);
      const angle = seededUnit(night * 311 + index * 37 + batch * 991) * Math.PI * 2;
      const radius = 0.045 + seededUnit(night * 701 + index * 19 + batch * 313) * (batch ? 0.18 : 0.12);
      const candidate = spawn.position.clone().add(new I.Vector3(Math.cos(angle) * radius, 0, Math.sin(angle) * radius));
      mesh.position.copy(candidate);
      mesh.rotation.y = seededUnit(index * 83 + night * 29) * Math.PI * 2;
      if (spawn.prizeId) engine.decoratePrize(mesh, spawn.prizeEffect);
      engine.world.root.add(mesh);
      engine.foods.push({
        id: id++, kind: spawn.kind, value: spawn.value, mesh,
        reservedBy: null, carriedBy: null, deposited: false,
        prizeId: spawn.prizeId, prizeEffect: spawn.prizeEffect,
        room: spawn.room, depth: roomDepth(spawn.room),
        nestDistance: mesh.position.distanceTo(engine.world.nestCenter),
      });
    }
  }
}

function eventExtraSpawns(set, I, night) {
  if (!set) return [];
  const V = I.Vector3;
  const make = (x, z, kind, value, room) => ({ position: new V(x, 0.018, z), kind, value, room, unlockNight: night });
  if (set === "pantry-spill") return [make(4.6, -10.2, "cereal", 3, "pantry"), make(4.9, -10, "cracker", 4, "pantry"), make(5.15, -10.25, "cheese", 5, "pantry")];
  if (set === "toy-trail") return [make(-5.8, -11.4, "cracker", 2, "children"), make(-7, -12.4, "cracker", 2, "children"), make(-8.4, -13.2, "cheese", 4, "children")];
  if (set === "garage-windfall") return [make(22.8, 6.2, "cereal", 4, "garage"), make(23.1, 6.5, "nut", 5, "garage"), make(22.5, 6.8, "cheese", 6, "garage")];
  return [];
}

function updateEnvironmentalWindow(engine, expansion, delta) {
  expansion.frame++;
  expansion.dangerSignals.forEach((signal) => { signal.ttl -= delta; });
  expansion.dangerSignals = expansion.dangerSignals.filter((signal) => signal.ttl > 0);
  const cycle = expansion.currentPlan?.event.maskCycle;
  let active = false;
  if (cycle === "storm") active = (engine.time % 9.2) < 1.45;
  if (cycle === "laundry") active = (engine.time % 11.5) < 2.25;
  expansion.maskActive = active;
  expansion.ambientMask = active ? (cycle === "storm" ? 0.2 : 0.34) : 1;
  if (active && !expansion.lastMaskActive) {
    engine.showMessage(cycle === "storm" ? "Thunder masks the floor — move now." : "The machines roar — pawsteps are hidden.", 1.6);
  }
  expansion.lastMaskActive = active;
}

function colonyShouldStop(engine) {
  return shouldForagersStop({
    policy: engine.colonyPolicy,
    delivered: engine.snapshot.deliveredTonight,
    nightlyRequirement: engine.snapshot.tonightRequirement,
  });
}

function updateColonyMice(engine, expansion, I, delta) {
  const profile = getPolicyProfile(engine.colonyPolicy);
  const stop = colonyShouldStop(engine);
  expansion.reservationSweepTimer -= delta;
  if (expansion.reservationSweepTimer <= 0) {
    releaseStaleFoodReservations(engine);
    expansion.reservationSweepTimer = 0.42;
  }
  const committed = committedFoodValue(engine);
  const remaining = Math.max(0, forageTarget(engine.colonyPolicy, engine.snapshot.tonightRequirement) - engine.snapshot.deliveredTonight - committed);
  const maxForagers = activeForagerLimit(engine.colonyPolicy, engine.mice.length, remaining);
  let assignedForagers = engine.mice.filter((mouse) => ["to-food", "returning", "escaping", "hiding"].includes(mouse.task)).length;

  for (const mouse of engine.mice) {
    if (!mouse.member.alive || mouse.task === "dead") continue;
    mouse.delay -= delta;
    mouse.escapeCooldown = Math.max(0, mouse.escapeCooldown - delta);
    mouse.aiDecisionTimer -= delta;
    mouse.noiseTimer -= delta;
    mouse.nestActivityTimer -= delta;
    const nearestCat = engine.nearestCat(mouse.rig.root.position);
    const catDistance = nearestCat ? nearestCat.rig.root.position.distanceTo(mouse.rig.root.position) : Infinity;

    if (mouse.aiDecisionTimer <= 0) {
      mouse.aiDecisionTimer = 0.11 + (mouse.colonyIndex % 7) * 0.019;
      const threat = assessMouseThreat(engine, expansion, mouse, nearestCat, catDistance);
      mouse.lastThreatScore = threat.score;
      const fleeThreshold = Math.max(0.16, profile.fleeThreshold - expansion.upgrades.scouts * 0.025 - Math.log2(Math.max(1, engine.snapshot.population)) * 0.012);
      if (!["escaping", "hiding"].includes(mouse.task) && mouse.escapeCooldown <= 0 && threat.score >= fleeThreshold) {
        sendMouseToBestShelter(engine, expansion, I, mouse, threat.cat ?? nearestCat, threat);
      }
    }

    if (mouse.task === "escaping") {
      if (mouse.carriedFood && mouse.lastThreatScore > 0.78) dropMouseFood(engine, mouse);
      const speed = mouse.speed * profile.panicMultiplier * (mouse.carriedFood ? 0.93 : 1);
      const moved = engine.followMousePath(mouse, delta, speed);
      if (mouse.noiseTimer <= 0 && moved > 0.4) {
        mouse.noiseTimer = 0.34 + (mouse.colonyIndex % 4) * 0.07;
        engine.emitNoise(mouse.rig.root.position, 0.62);
      }
      if (mouse.escapeGoal && mouse.rig.root.position.distanceTo(mouse.escapeGoal) < 0.14) {
        mouse.task = "hiding";
        mouse.hideTimer = 2.1 + mouse.member.caution * 2.4;
        mouse.safeTimer = 0;
        mouse.path = [];
        mouse.pathIndex = 0;
      }
      recoverMouseIfStalled(engine, expansion, I, mouse, delta);
      animateMouse(engine, expansion, mouse, moved, 1, true);
      continue;
    }

    if (mouse.task === "hiding") {
      resetMouseProgress(mouse);
      mouse.hideTimer -= delta;
      const threat = assessMouseThreat(engine, expansion, mouse, nearestCat, catDistance, true);
      const clear = threat.score < profile.fleeThreshold * 0.55 && catDistance > 2.35 + mouse.member.caution;
      mouse.safeTimer = clear ? mouse.safeTimer + delta : 0;
      if (mouse.hideTimer <= 0 && mouse.safeTimer >= 1.25) {
        mouse.escapeGoal = null;
        mouse.escapeCooldown = 1.8 + mouse.member.caution;
        if (mouse.carriedFood) {
          mouse.task = "returning";
          engine.planMousePath(mouse);
        } else if (stop) {
          sendMouseHome(engine, mouse);
        } else if (mouse.resumeTask === "to-food" && mouse.targetFood && !mouse.targetFood.deposited && !mouse.targetFood.carriedBy) {
          mouse.task = "to-food";
          engine.planMousePath(mouse);
        } else {
          mouse.task = "waiting";
          mouse.delay = 0.2 + Math.random() * 0.35;
        }
      }
      animateMouse(engine, expansion, mouse, 0, 1, false);
      continue;
    }

    if (stop && !mouse.carriedFood) {
      if (mouse.targetFood) {
        mouse.targetFood.reservedBy = null;
        mouse.targetFood = null;
      }
      if (!isInsideNestBounds(engine, mouse.rig.root.position) && !["home", "nesting-move"].includes(mouse.task)) sendMouseHome(engine, mouse);
      if (isInsideNestBounds(engine, mouse.rig.root.position) && !["nesting", "nesting-move"].includes(mouse.task)) mouse.task = "nesting";
    }

    if (mouse.task === "waiting" && mouse.delay <= 0) {
      if (!stop && assignedForagers < maxForagers) {
        engine.assignFood(mouse);
        if (mouse.task === "to-food") assignedForagers++;
      } else sendMouseHome(engine, mouse);
    }

    let moved = 0;
    if (mouse.task === "to-food") {
      if (!mouse.targetFood || mouse.targetFood.deposited || mouse.targetFood.carriedBy) {
        mouse.targetFood = null;
        mouse.task = "waiting";
        mouse.delay = 0.22;
      } else if (stop && !(profile.finishSafeTrip && mouse.rig.root.position.distanceTo(mouse.targetFood.mesh.position) < 0.55 && mouse.lastThreatScore < 0.12)) {
        mouse.targetFood.reservedBy = null;
        mouse.targetFood = null;
        sendMouseHome(engine, mouse);
      } else {
        moved = engine.followMousePath(mouse, delta, mouse.speed);
        if (mouse.rig.root.position.distanceTo(mouse.targetFood.mesh.position) < 0.11) engine.mouseTakeFood(mouse, mouse.targetFood);
      }
    } else if (mouse.task === "returning") {
      moved = engine.followMousePath(mouse, delta, mouse.speed * (mouse.member.carrying > 1 ? 0.88 : 0.94));
      if (mouse.rig.root.position.distanceTo(engine.world.nestDeposit) < 0.16) engine.mouseDeposit(mouse);
    } else if (mouse.task === "home" || mouse.task === "nesting-move") {
      moved = engine.followMousePath(mouse, delta, mouse.task === "home" ? mouse.speed * 1.08 : mouse.speed * 0.35);
      if (mouse.rig.root.position.distanceTo(mouse.nestActivityGoal ?? engine.world.nestDeposit) < 0.15) {
        mouse.task = "nesting";
        mouse.path = [];
        mouse.nestActivityGoal = null;
        mouse.nestActivityTimer = 1.3 + Math.random() * 2.5;
      }
    } else if (mouse.task === "nesting") {
      if (!stop && assignedForagers < maxForagers && mouse.delay <= 0) {
        engine.assignFood(mouse);
        if (mouse.task === "to-food") assignedForagers++;
      } else if (mouse.nestActivityTimer <= 0) {
        const slot = mouse.colonyIndex % 12;
        mouse.nestActivityGoal = new I.Vector3(-6.95 + (slot % 4) * 0.22, 0.025, 3.25 + Math.floor(slot / 4) * 0.36);
        mouse.task = "nesting-move";
        engine.planMousePath(mouse);
      }
    }

    recoverMouseIfStalled(engine, expansion, I, mouse, delta);

    if (moved > 0.35 && mouse.noiseTimer <= 0) {
      const surface = engine.world.surfaceAt(mouse.rig.root.position.x, mouse.rig.root.position.z);
      const noise = { carpet: 0.12, wood: 0.24, tile: 0.3, paper: 0.48, metal: 0.62 }[surface] ?? 0.24;
      mouse.noiseTimer = 0.72 + (mouse.colonyIndex % 5) * 0.09;
      engine.emitNoise(mouse.rig.root.position, noise);
    }
    const alert = nearestCat?.targetId === mouse.member.id ? 1 : catDistance < 2.4 ? 0.45 : mouse.lastThreatScore * 0.35;
    animateMouse(engine, expansion, mouse, moved, alert, !!mouse.carriedFood);
  }
}

function releaseStaleFoodReservations(engine) {
  const activeMice = new Map(engine.mice.filter((mouse) => mouse.member.alive && mouse.task !== "dead").map((mouse) => [mouse.member.id, mouse]));
  for (const food of engine.foods) {
    if (!food.reservedBy) continue;
    const holder = activeMice.get(food.reservedBy);
    if (!holder || holder.task !== "to-food" || holder.targetFood !== food) food.reservedBy = null;
  }
}

function resetMouseProgress(mouse) {
  if (!mouse.progressPosition) mouse.progressPosition = mouse.rig.root.position.clone();
  else mouse.progressPosition.copy(mouse.rig.root.position);
  mouse.progressSampleTimer = 0;
  mouse.stalledFor = 0;
  mouse.recoveryAttempts = 0;
}

function movementGoalForMouse(engine, mouse) {
  if (mouse.task === "escaping") return mouse.escapeGoal;
  if (mouse.task === "to-food") return mouse.targetFood?.mesh.position;
  if (mouse.task === "returning") return engine.world.nestDeposit;
  if (mouse.task === "home" || mouse.task === "nesting-move") return mouse.nestActivityGoal ?? engine.world.nestDeposit;
  return null;
}

function abandonFoodAssignment(engine, mouse) {
  const food = mouse.targetFood;
  if (food?.reservedBy === mouse.member.id) food.reservedBy = null;
  if (food) mouse.blockedFoodUntil.set(food.id, engine.time + 4.5);
  mouse.targetFood = null;
  mouse.path = [];
  mouse.pathIndex = 0;
  mouse.task = "waiting";
  mouse.delay = 0.06 + (mouse.colonyIndex % 5) * 0.025;
  mouse.recoveryAttempts = 0;
}

function recoverMouseIfStalled(engine, expansion, I, mouse, delta) {
  const goal = movementGoalForMouse(engine, mouse);
  if (!goal) {
    resetMouseProgress(mouse);
    return false;
  }
  if (!mouse.progressPosition) mouse.progressPosition = mouse.rig.root.position.clone();
  mouse.progressSampleTimer += delta;
  if (mouse.progressSampleTimer < 0.32) return false;
  const sampleWindow = mouse.progressSampleTimer;
  mouse.progressSampleTimer = 0;
  const distanceMoved = mouse.progressPosition.distanceTo(mouse.rig.root.position);
  mouse.progressPosition.copy(mouse.rig.root.position);
  const distanceToGoal = mouse.rig.root.position.distanceTo(goal);
  if (distanceMoved >= 0.018 || distanceToGoal <= 0.16) {
    mouse.stalledFor = 0;
    mouse.recoveryAttempts = 0;
    return false;
  }
  mouse.stalledFor += sampleWindow;
  if (!shouldRecoverMouse({ task: mouse.task, stalledFor: mouse.stalledFor, distanceMoved, distanceToGoal })) return false;

  mouse.stalledFor = 0;
  mouse.recoveryAttempts++;
  expansion.routeRevision++;
  expansion.routeCache.clear();

  if (mouse.task === "to-food" && mouse.recoveryAttempts >= 2) {
    abandonFoodAssignment(engine, mouse);
    return true;
  }
  if (mouse.task === "escaping") {
    const cat = engine.nearestCat(mouse.rig.root.position);
    mouse.path = [];
    mouse.pathIndex = 0;
    if (cat && sendMouseToBestShelter(engine, expansion, I, mouse, cat)) {
      mouse.recoveryAttempts = 0;
      return true;
    }
    mouse.task = mouse.carriedFood ? "returning" : "waiting";
    mouse.delay = 0.08;
  } else if ((mouse.task === "home" || mouse.task === "nesting-move") && isInsideNestBounds(engine, mouse.rig.root.position)) {
    mouse.task = "nesting";
    mouse.path = [];
    mouse.pathIndex = 0;
    mouse.nestActivityGoal = null;
    mouse.nestActivityTimer = 0.8 + Math.random() * 1.2;
    mouse.recoveryAttempts = 0;
    return true;
  }

  if (mouse.task === "returning" && mouse.recoveryAttempts >= 2) {
    const direct = I.pathfind(mouse.rig.root.position, engine.world.nestDeposit, 0.055, engine.world.colliders, "mouse");
    if (direct.reachedGoal && direct.path.length) {
      mouse.path = direct.path.map((point) => point.clone());
      mouse.pathIndex = 0;
      mouse.recoveryAttempts = 0;
      return true;
    }
  }
  if (["to-food", "returning", "home", "nesting-move"].includes(mouse.task)) {
    mouse.path = [];
    mouse.pathIndex = 0;
    engine.planMousePath(mouse);
    if (mouse.path.length) return true;
  }
  if (mouse.task === "to-food") {
    abandonFoodAssignment(engine, mouse);
  } else if (mouse.task === "returning" && mouse.recoveryAttempts >= 3) {
    const dropped = mouse.carriedFood;
    if (dropped) {
      dropMouseFood(engine, mouse);
      mouse.blockedFoodUntil.set(dropped.id, engine.time + 4.5);
    }
    mouse.task = "waiting";
    mouse.delay = 0.08;
    mouse.path = [];
    mouse.pathIndex = 0;
    mouse.recoveryAttempts = 0;
  } else if (!mouse.path.length) {
    mouse.task = "waiting";
    mouse.delay = 0.1 + Math.random() * 0.12;
  }
  return true;
}

function animateMouse(engine, expansion, mouse, speed, alert, carrying) {
  const far = mouse.rig.root.position.distanceToSquared(engine.playerPosition) > 42;
  const home = ["nesting", "hiding"].includes(mouse.task);
  if ((far || home) && (expansion.frame + mouse.colonyIndex) % (far ? 3 : 2) !== 0) return;
  mouse.rig.update(engine.time, speed, alert, carrying);
}

function assessMouseThreat(engine, expansion, mouse, nearestCat, nearestDistance, cheap = false) {
  let best = { score: 0, cat: nearestCat, urgent: false };
  for (const cat of engine.cats) {
    const position = mouse.rig.root.position;
    const distance = cat.rig.root.position.distanceTo(position);
    const dx = position.x - cat.rig.root.position.x;
    const dz = position.z - cat.rig.root.position.z;
    const length = Math.max(0.001, Math.hypot(dx, dz));
    const facing = cat.yaw + cat.lookYaw * 0.5;
    const lookDot = (dx / length) * -Math.sin(facing) + (dz / length) * -Math.cos(facing);
    const previous = mouse.lastCatDistances.get(cat.id) ?? distance;
    mouse.lastCatDistances.set(cat.id, distance);
    const approaching = previous - distance > 0.055;
    let score = 0;
    let urgent = false;
    if (cat.state === "chase" && cat.targetId === mouse.member.id) {
      score = 1.2;
      urgent = true;
    } else if (cat.state === "chase") {
      const chased = engine.targetPosition(cat.targetId);
      const sharedDanger = chased ? chased.distanceTo(position) < 2.8 : false;
      score = sharedDanger ? 0.94 : distance < 3.4 ? 0.77 : 0.25;
      urgent = sharedDanger && distance < 2.4;
    } else if (["alert", "investigating", "suspicious", "search"].includes(cat.state)) {
      score = distance < 2.2 ? 0.9 : distance < 3.7 ? 0.62 : distance < 5.2 ? 0.34 : 0;
      if (lookDot > 0.2) score += 0.15;
    } else if (distance < 4.7 && lookDot > 0.28) {
      const clearLine = cheap ? true : ILineClear(engine, cat.rig.root.position, position, "cat");
      if (clearLine) score = (1 - distance / 5.2) * (cat.leisureMode === "grooming" ? 0.52 : 0.82) + lookDot * 0.16;
    }
    if (approaching && distance < 5.3) score += 0.18;
    if (distance < 1.15) { score = Math.max(score, 0.94); urgent = true; }
    if (score > best.score) best = { score, cat, urgent };
  }
  for (const signal of expansion.dangerSignals) {
    const distance = signal.position.distanceTo(mouse.rig.root.position);
    if (distance < signal.radius) best.score = Math.max(best.score, signal.score * (1 - distance / signal.radius));
  }
  best.score = clamp(best.score + expansion.upgrades.scouts * 0.035, 0, 1.25);
  return best;
}

function ILineClear(engine, start, end, agent) {
  return window.HearthmouseInternals.lineClear(start, end, agent === "cat" ? 0.205 : 0.055, engine.world.colliders, agent);
}

function sendMouseToBestShelter(engine, expansion, I, mouse, cat, knownThreat = null) {
  if (!cat) return false;
  const threat = knownThreat ?? assessMouseThreat(engine, expansion, mouse, cat, cat.rig.root.position.distanceTo(mouse.rig.root.position));
  const candidates = engine.world.shelterPoints
    .filter((shelter) =>
      isRoomAccessible(engine, expansion, shelter.roomId ?? roomForPosition(shelter.position.x, shelter.position.z)) &&
      (!shelter.dynamicProp || expansion.dynamicProps.get(shelter.dynamicProp)?.mesh.visible)
    )
    .map((shelter) => ({ shelter, direct: mouse.rig.root.position.distanceTo(shelter.position), catDistance: cat.rig.root.position.distanceTo(shelter.position) }))
    .sort((a, b) => (a.direct - a.catDistance * 0.34) - (b.direct - b.catDistance * 0.34))
    .slice(0, 6);
  let best = null;
  for (const candidate of candidates) {
    const route = cachedSmartPath(engine, expansion, I, mouse.rig.root.position, candidate.shelter.position, "mouse", `shelter:${candidate.shelter.id}`);
    if (!route.reachedGoal) continue;
    const pathDistance = pathLength(mouse.rig.root.position, route.path);
    const mouseEta = pathDistance / Math.max(0.4, mouse.speed * getPolicyProfile(engine.colonyPolicy).panicMultiplier);
    const catEta = candidate.catDistance / 3.62;
    const interceptPenalty = catEta < mouseEta ? (mouseEta - catEta) * 8 : 0;
    const score = pathDistance - candidate.catDistance * (0.42 + mouse.member.caution * 0.32) + interceptPenalty;
    if (!best || score < best.score) best = { ...candidate, route, pathDistance, score };
  }
  if (!best) {
    mouse.escapeCooldown = 0.45;
    return false;
  }
  mouse.resumeTask = mouse.carriedFood ? "returning" : mouse.targetFood ? "to-food" : "waiting";
  if (mouse.targetFood && !mouse.carriedFood) {
    mouse.targetFood.reservedBy = null;
    mouse.targetFood = null;
    mouse.resumeTask = "waiting";
  }
  if (mouse.carriedFood && (threat.urgent || best.pathDistance > 1.65 || mouse.carriedFood.value >= 4 && threat.score > 0.68)) dropMouseFood(engine, mouse);
  mouse.task = "escaping";
  mouse.escapeGoal = best.shelter.position;
  mouse.path = best.route.path.map((point) => point.clone());
  mouse.pathIndex = 0;
  mouse.escapeCooldown = 2.2;
  expansion.dangerSignals.push({ position: mouse.rig.root.position.clone(), ttl: 2.6, radius: 2.8 + expansion.upgrades.scouts * 0.55, score: 0.78 });
  if (expansion.dangerSignals.length > 12) expansion.dangerSignals.shift();
  return true;
}

function dropMouseFood(engine, mouse) {
  const food = mouse.carriedFood;
  if (!food) return;
  food.carriedBy = null;
  food.reservedBy = null;
  food.mesh.removeFromParent();
  engine.world.root.add(food.mesh);
  food.mesh.position.copy(mouse.rig.root.position).setY(0.018);
  food.mesh.scale.setScalar(1);
  food.mesh.visible = true;
  food.room = roomForPosition(food.mesh.position.x, food.mesh.position.z);
  food.nestDistance = food.mesh.position.distanceTo(engine.world.nestCenter);
  mouse.carriedFood = null;
}

function assignLocalFood(engine, expansion, I, mouse) {
  if (colonyShouldStop(engine) || engine.snapshot.deliveredTonight + committedFoodValue(engine) >= forageTarget(engine.colonyPolicy, engine.snapshot.tonightRequirement)) {
    sendMouseHome(engine, mouse);
    return;
  }
  const profile = getPolicyProfile(engine.colonyPolicy);
  let radius = profile.localRadius;
  let candidates = availableFood(engine, expansion, mouse, radius);
  if (!candidates.length) {
    radius = profile.expandedRadius;
    candidates = availableFood(engine, expansion, mouse, radius);
  }
  // Orders decide how far and how boldly mice prefer to range, but an empty
  // preferred ring must never turn into an idle colony while food remains.
  if (!candidates.length) candidates = availableFood(engine, expansion, mouse, Infinity);
  candidates.sort((a, b) => scoreFoodCandidate(a, engine.colonyPolicy, mouse.member.caution) - scoreFoodCandidate(b, engine.colonyPolicy, mouse.member.caution));
  for (const candidate of candidates.slice(0, 12)) {
    const route = cachedSmartPath(engine, expansion, I, mouse.rig.root.position, candidate.food.mesh.position, "mouse", `food:${candidate.food.id}`);
    if (!route.reachedGoal) continue;
    candidate.food.reservedBy = mouse.member.id;
    mouse.targetFood = candidate.food;
    mouse.task = "to-food";
    mouse.path = route.path.map((point) => point.clone());
    mouse.pathIndex = 0;
    return;
  }
  mouse.task = "waiting";
  mouse.delay = (engine.colonyPolicy === "cautious" ? 0.42 : engine.colonyPolicy === "desperate" ? 0.14 : 0.24) + Math.random() * 0.2;
  if (!isInsideNestBounds(engine, mouse.rig.root.position)) sendMouseHome(engine, mouse);
}

function committedFoodValue(engine) {
  return engine.foods.reduce((sum, food) => {
    if (food.deposited) return sum;
    return food.carriedBy || food.reservedBy ? sum + food.value : sum;
  }, 0);
}

function availableFood(engine, expansion, mouse, radius) {
  for (const [foodId, blockedUntil] of mouse.blockedFoodUntil ?? []) {
    if (blockedUntil <= engine.time) mouse.blockedFoodUntil.delete(foodId);
  }
  return engine.foods
    .filter((food) =>
      !food.deposited &&
      !food.carriedBy &&
      !food.reservedBy &&
      food.mesh.visible &&
      food.nestDistance <= radius &&
      (mouse.blockedFoodUntil?.get(food.id) ?? 0) <= engine.time &&
      isRoomAccessible(engine, expansion, food.room ?? roomForPosition(food.mesh.position.x, food.mesh.position.z))
    )
    .map((food) => ({
      food,
      value: food.value,
      depth: food.depth ?? roomDepth(food.room),
      nestDistance: food.nestDistance ?? food.mesh.position.distanceTo(engine.world.nestCenter),
      mouseDistance: food.mesh.position.distanceTo(mouse.rig.root.position),
      exposure: exposureAt(engine, food.mesh.position),
    }));
}

function exposureAt(engine, position) {
  let exposure = 0;
  engine.cats.forEach((cat) => {
    const distance = cat.rig.root.position.distanceTo(position);
    if (distance < 5) exposure = Math.max(exposure, 1 - distance / 5);
  });
  return exposure;
}

function sendMouseHome(engine, mouse) {
  if (mouse.targetFood) mouse.targetFood.reservedBy = null;
  mouse.targetFood = null;
  mouse.task = "home";
  mouse.nestActivityGoal = engine.world.nestDeposit;
  engine.planMousePath(mouse);
}

function isInsideNestBounds(engine, position) {
  const bounds = engine.world.nestBounds;
  return position.x >= bounds.minX && position.x <= bounds.maxX && position.z >= bounds.minZ && position.z <= bounds.maxZ;
}

function prepareCatLeisure(engine, expansion, delta) {
  for (const cat of engine.cats) {
    cat.leisureCooldown = Math.max(0, (cat.leisureCooldown ?? 0) - delta);
    if (cat.state === "chase") {
      cat.leisureMode = null;
      cat.leisureTimer = 0;
      continue;
    }
    if (!["relaxed", "cooldown"].includes(cat.state)) continue;
    if (cat.leisureMode === "cover-approach" && (!cat.path.length || cat.pathIndex >= cat.path.length || (cat.coverTarget && cat.rig.root.position.distanceTo(cat.coverTarget) < 0.48))) {
      cat.leisureMode = "cover-watch";
      cat.leisureTimer = 2.8 + Math.random() * 3.4;
      cat.stateTimer = cat.leisureTimer;
      cat.path = [];
      continue;
    }
    if (cat.leisureTimer > 0) {
      cat.leisureTimer -= delta;
      if (cat.leisureTimer <= 0) {
        cat.leisureMode = null;
        cat.leisureCooldown = 3.5 + Math.random() * 4.5;
      }
      continue;
    }
    if (cat.leisureCooldown > 0 || cat.path.length && cat.pathIndex < cat.path.length) continue;
    const roll = Math.random();
    if (roll < 0.24) {
      cat.leisureMode = "grooming";
      cat.leisureTimer = 4.4 + Math.random() * 4.2;
      cat.stateTimer = cat.leisureTimer;
      cat.path = [];
    } else if (roll < 0.48) {
      cat.leisureMode = "watching";
      cat.leisureTimer = 3.2 + Math.random() * 4.5;
      cat.stateTimer = cat.leisureTimer;
      cat.path = [];
    } else if (roll < 0.65) {
      const shelters = engine.world.shelterPoints.filter((shelter) => isRoomAccessible(engine, expansion, shelter.roomId ?? roomForPosition(shelter.position.x, shelter.position.z)));
      const shelter = shelters[Math.floor(Math.random() * shelters.length)];
      if (shelter) {
        const angle = Math.random() * Math.PI * 2;
        cat.coverTarget = shelter.position.clone().add(engine.tempA.set(Math.cos(angle) * 0.72, 0, Math.sin(angle) * 0.72));
        cat.investigation.copy(shelter.position);
        cat.leisureMode = "cover-approach";
        cat.leisureTimer = 6.5;
        engine.planCatPath(cat, cat.coverTarget);
      } else {
        cat.leisureMode = "watching";
        cat.leisureTimer = 3.2;
        cat.stateTimer = cat.leisureTimer;
        cat.path = [];
      }
    } else {
      cat.leisureMode = roll < 0.82 ? "inspecting" : "wandering";
      cat.leisureTimer = 5 + Math.random() * 5;
    }
  }
}

function applyCatBehaviorAnimation(engine, expansion) {
  let pounceWarning = 0;
  for (const cat of engine.cats) {
    cat.rig.body.rotation.z = 0;
    cat.rig.root.rotation.z = 0;
    if (cat.pouncePhase === "windup") {
      const duration = cat.pounceWindupDuration || computePounceWindup(engine.snapshot.night, cat.personality);
      const progress = clamp(1 - cat.pounceTimer / duration, 0, 1);
      const tension = progress * progress * (3 - 2 * progress);
      cat.rig.body.position.y -= 0.055 * tension;
      cat.rig.chest.position.y -= 0.044 * tension;
      cat.rig.body.rotation.z = Math.sin(engine.time * 35) * 0.055 * tension;
      cat.rig.headPivot.rotation.x += 0.16 * tension;
      cat.rig.legs.slice(2).forEach((leg, index) => { leg.rotation.x += Math.sin(engine.time * 31 + index * Math.PI) * 0.13 * tension; });
      cat.rig.tail[0] && (cat.rig.tail[0].rotation.y += Math.sin(engine.time * 32) * 0.08 * tension);
      cat.rig.eyes.forEach((eye) => { if (eye.material) eye.material.emissiveIntensity = Math.max(eye.material.emissiveIntensity, 0.8 + tension * 0.75); });
      pounceWarning = Math.max(pounceWarning, tension);
    } else if (cat.leisureMode === "grooming" && cat.state !== "chase") {
      cat.rig.body.position.y -= 0.035;
      cat.rig.chest.position.y -= 0.02;
      cat.rig.headPivot.rotation.x += 0.42 + Math.sin(engine.time * 3.4) * 0.12;
      cat.rig.headPivot.rotation.y += 0.46;
      if (cat.rig.legs[0]) cat.rig.legs[0].rotation.x = -1.05 + Math.sin(engine.time * 4.8) * 0.28;
    } else if (["watching", "cover-watch"].includes(cat.leisureMode)) {
      cat.rig.body.position.y -= 0.025;
      cat.rig.headPivot.rotation.y += Math.sin(engine.time * 0.62) * 0.34;
      cat.rig.headPivot.rotation.x += 0.08;
    } else if (cat.leisureMode === "inspecting") {
      cat.rig.headPivot.rotation.x += 0.22;
    }
  }
  engine.snapshot.pounceWarning = pounceWarning;
}

function emitExpandedNoise(engine, expansion, position, rawStrength) {
  const event = expansion.currentPlan?.event;
  const sensitivity = event?.soundSensitivity ?? 1;
  const strength = rawStrength * expansion.ambientMask;
  for (const cat of engine.cats) {
    if (cat.state === "chase") continue;
    const distance = cat.rig.root.position.distanceTo(position);
    const personality = cat.personality === "hunter" ? 1 : 1.18;
    let hearingRange = (0.65 + strength * 4.6) * personality * sensitivity;
    if (cat.leisureMode === "grooming") {
      const loudEnough = rawStrength >= 0.56 || distance < 0.72;
      if (!loudEnough) continue;
      hearingRange *= 0.48;
    }
    if (distance > hearingRange) continue;
    const error = Math.max(0.06, distance * 0.055) / Math.max(0.45, strength);
    const angle = Math.random() * Math.PI * 2;
    cat.investigation.copy(position).add(engine.tempA.set(Math.cos(angle) * error, 0, Math.sin(angle) * error));
    cat.awareness = Math.max(cat.awareness, Math.min(0.34, strength * 0.17));
    cat.leisureMode = null;
    cat.leisureTimer = 0;
    if (cat.state === "relaxed" || cat.state === "cooldown") engine.setCatState(cat, "alert", cat.personality === "kitten" ? 0.28 : 0.52);
    else if (cat.state === "suspicious") cat.stateTimer = Math.max(cat.stateTimer, 2.6);
  }
}

function cachedSmartPath(engine, expansion, I, start, target, agent, purpose) {
  const key = `${expansion.routeRevision}:${agent}:${purpose}:${Math.round(start.x * 2)}:${Math.round(start.z * 2)}`;
  const cached = expansion.routeCache.get(key);
  if (cached) return { ...cached, path: cached.path.map((point) => point.clone()) };
  const result = buildSmartPath(engine, expansion, I, start, target, agent);
  if (expansion.routeCache.size > 320) expansion.routeCache.clear();
  expansion.routeCache.set(key, { ...result, path: result.path.map((point) => point.clone()) });
  return result;
}

function buildSmartPath(engine, expansion, I, start, target, agent) {
  const startRoom = roomForPosition(start.x, start.z);
  const targetRoom = roomForPosition(target.x, target.z);
  if (!startRoom || !targetRoom || startRoom === targetRoom) return I.pathfind(start, target, agent === "cat" ? 0.205 : 0.055, engine.world.colliders, agent);
  const route = roomRoute(engine, expansion, startRoom, targetRoom, agent);
  if (!route.length) return I.pathfind(start, target, agent === "cat" ? 0.205 : 0.055, engine.world.colliders, agent);
  const radius = agent === "cat" ? 0.205 : 0.055;
  let cursor = start;
  const path = [];
  let reachedGoal = true;
  let remainingDistance = 0;
  for (const waypoint of [...route.map((edge) => edge.point), target]) {
    const leg = I.pathfind(cursor, waypoint, radius, engine.world.colliders, agent);
    path.push(...leg.path);
    if (!leg.reachedGoal) {
      reachedGoal = false;
      remainingDistance = leg.remainingDistance + waypoint.distanceTo(target);
      break;
    }
    cursor = waypoint;
  }
  return { path, reachedGoal, remainingDistance };
}

function roomRoute(engine, expansion, startRoom, targetRoom, agent) {
  const openEdges = expansion.navEdges.filter((edge) => {
    if (edge.mouseOnly && agent === "cat") return false;
    if (edge.unlockNight && engine.snapshot.night < edge.unlockNight && expansion.temporaryRoomId !== edge.a && expansion.temporaryRoomId !== edge.b) return false;
    if (edge.doorId) {
      const door = expansion.roomDoors.find((candidate) => candidate.id === edge.doorId);
      if (door?.collider.active) return false;
    }
    if (edge.dynamicProp === "closed-shortcut" && expansion.dynamicProps.get("closed-shortcut")?.collider.active) return false;
    if (edge.passage && expansion.dynamicProps.get("passage-block")?.collider.active) return false;
    return isRoomAccessible(engine, expansion, edge.a) && isRoomAccessible(engine, expansion, edge.b);
  });
  const queue = [{ room: startRoom, path: [] }];
  const visited = new Set([startRoom]);
  while (queue.length) {
    const current = queue.shift();
    if (current.room === targetRoom) return current.path;
    for (const edge of openEdges) {
      const next = edge.a === current.room ? edge.b : edge.b === current.room ? edge.a : null;
      if (!next || visited.has(next)) continue;
      visited.add(next);
      queue.push({ room: next, path: [...current.path, edge] });
    }
  }
  return [];
}

function isRoomAccessible(engine, expansion, roomId) {
  if (!roomId) return true;
  return roomUnlocked(roomId, engine.snapshot.night, expansion.temporaryRoomId);
}

function roomForPosition(x, z) {
  const expanded = ROOM_DEFINITIONS.find((room) => x >= room.minX - 0.12 && x <= room.maxX + 0.12 && z >= room.minZ - 0.12 && z <= room.maxZ + 0.12);
  if (expanded) return expanded.id;
  if (x >= 10.35 && x <= 15.1 && z >= 1.7 && z <= 6.35) return "laundry";
  if (x >= -10.5 && x <= -4.9 && z >= -10.8 && z <= -6.25) return "study";
  if (x < 0.1 && z >= -6.5 && z <= 6.5) return "living";
  if (x >= -0.1 && x <= 10.6 && z >= -6.5 && z <= 6.5) return "kitchen";
  return null;
}

function roomDepth(roomId) {
  if (["living", "kitchen"].includes(roomId)) return 0;
  if (roomId === "laundry") return 2;
  if (roomId === "study") return 3;
  return ROOM_DEFINITIONS.find((room) => room.id === roomId)?.depth ?? 1;
}

function pathLength(start, path) {
  let total = 0;
  let cursor = start;
  path.forEach((point) => { total += cursor.distanceTo(point); cursor = point; });
  return total;
}

function mountColonyControls(engine) {
  let root = document.querySelector(".hearthmouse-colony-panel");
  if (!root) {
    root = document.createElement("aside");
    root.className = "hearthmouse-colony-panel";
    root.innerHTML = `
      <button type="button" class="colony-policy-badge"><span>COLONY</span><strong></strong></button>
      <section class="nest-policy-card" aria-label="Colony risk policy">
        <p class="nest-policy-eyebrow">NEST ORDERS</p>
        <h2>Colony Risk</h2>
        <div class="policy-options"></div>
        <p class="policy-description"></p>
        <div class="night-variation"><strong></strong><span></span></div>
        <p class="room-condition"></p>
        <p class="policy-key-hint">At the nest: press 1, 2, or 3</p>
      </section>`;
    document.querySelector(".game-shell")?.appendChild(root);
    const compactToggle = root.querySelector(".colony-policy-badge");
    const mobilePolicyQuery = window.matchMedia?.("(pointer: coarse) and (any-hover: none)");
    compactToggle.addEventListener("pointerdown", (event) => {
      if (!mobilePolicyQuery?.matches) return;
      event.preventDefault();
      event.stopPropagation();
      const current = window.hearthmouseEngine;
      if (!current?.insideNest()) return;
      current.setColonyPolicy(nextColonyPolicy(current.colonyPolicy));
    });
    const options = root.querySelector(".policy-options");
    Object.values(POLICY_PROFILES).forEach((profile, index) => {
      const button = document.createElement("button");
      button.type = "button";
      button.dataset.policy = profile.id;
      button.innerHTML = `<span>${index + 1}</span>${profile.label}`;
      button.addEventListener("pointerdown", (event) => {
        event.preventDefault();
        event.stopPropagation();
        window.hearthmouseEngine?.setColonyPolicy(profile.id);
      });
      options.appendChild(button);
    });
    document.addEventListener("keydown", (event) => {
      const current = window.hearthmouseEngine;
      if (!current?.insideNest()) return;
      const policy = { Digit1: "cautious", Digit2: "balanced", Digit3: "desperate" }[event.code];
      if (policy) current.setColonyPolicy(policy);
    });
  }

  const refresh = () => {
    const current = window.hearthmouseEngine;
    if (!current || current.disposed) {
      window.setTimeout(() => waitForEngineInstance(), 80);
      return;
    }
    const expansion = ensureExpansion(current);
    const profile = getPolicyProfile(current.colonyPolicy);
    const atNest = current.snapshot.phase === "foraging" && current.insideNest();
    root.classList.toggle("at-nest", atNest);
    root.classList.toggle("mask-window", expansion.maskActive);
    const compactToggle = root.querySelector(".colony-policy-badge");
    compactToggle.querySelector("strong").textContent = profile.label;
    compactToggle.disabled = !atNest;
    compactToggle.setAttribute("aria-label", `Colony risk: ${profile.label}. ${atNest ? "Tap to switch policy." : "Return to the nest to switch policy."}`);
    compactToggle.title = atNest ? `${profile.label} — tap to switch policy` : `${profile.label} — change at the nest`;
    root.querySelector(".policy-description").textContent = profile.description;
    root.querySelectorAll("[data-policy]").forEach((button) => {
      button.classList.toggle("active", button.dataset.policy === profile.id);
      button.disabled = !atNest;
    });
    const plan = expansion.currentPlan;
    root.querySelector(".night-variation strong").textContent = expansion.maskActive ? "NOISE MASKED" : (plan?.event.name ?? "THE HOUSE IS CHANGING");
    root.querySelector(".night-variation span").textContent = expansion.maskActive ? "Run while the sound lasts" : (plan?.event.description ?? "Listen before leaving cover.");
    root.querySelector(".room-condition").textContent = plan?.conditions.map((condition) => condition.label).join(" ") ?? "";
    window.setTimeout(refresh, 180);
  };
  refresh();
}

if (typeof window !== "undefined") waitForGame();
