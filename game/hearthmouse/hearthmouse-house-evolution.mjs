import { registerFrameVisualStage } from "./hearthmouse-performance-manager.mjs";

const POLL_INTERVAL_MS = 40;
const MAX_INSTALL_ATTEMPTS = 300;
const DAWN_WINDOW_SECONDS = 50;
const LOUD_NOISE_THRESHOLD = 1.65;

export const NOISE_ZONES = Object.freeze([
  Object.freeze({ id: "living-paper", label: "crackling paper", room: "living", minX: -1.55, maxX: -0.35, minZ: 2.35, maxZ: 3.55, strength: 1.8, radius: 18, cadence: 0.62, kind: "paper" }),
  Object.freeze({ id: "kitchen-tray", label: "metal feeding tray", room: "kitchen", minX: 4.75, maxX: 6.25, minZ: -1.55, maxZ: -0.25, strength: 2.45, radius: 24, cadence: 0.72, kind: "metal" }),
  Object.freeze({ id: "hall-creak", label: "loose hallway boards", room: "hallway", minX: -2.8, maxX: -0.25, minZ: -9.75, maxZ: -8.15, strength: 1.95, radius: 20, cadence: 0.78, kind: "wood" }),
  Object.freeze({ id: "pantry-paper", label: "crushed pantry paper", room: "pantry", minX: 3.05, maxX: 5.75, minZ: -10.25, maxZ: -8.0, strength: 2.05, radius: 22, cadence: 0.7, kind: "paper" }),
  Object.freeze({ id: "dining-cutlery", label: "fallen cutlery", room: "dining", minX: 1.6, maxX: 5.3, minZ: 7.0, maxZ: 8.25, strength: 2.75, radius: 29, cadence: 0.88, kind: "metal" }),
  Object.freeze({ id: "bathroom-tile", label: "loose bathroom tile", room: "bathroom", minX: -4.25, maxX: -2.05, minZ: -14.85, maxZ: -12.95, strength: 2.25, radius: 23, cadence: 0.82, kind: "tile" }),
  Object.freeze({ id: "bedroom-clutter", label: "bedside clutter", room: "bedroom", minX: -0.75, maxX: 1.55, minZ: -14.75, maxZ: -13.0, strength: 1.85, radius: 19, cadence: 0.72, kind: "clutter" }),
  Object.freeze({ id: "children-toys", label: "hard plastic toys", room: "children", minX: -9.75, maxX: -5.85, minZ: -14.7, maxZ: -12.05, strength: 2.95, radius: 32, cadence: 0.8, kind: "toys" }),
  Object.freeze({ id: "laundry-pan", label: "laundry machine pan", room: "laundry", minX: 11.85, maxX: 14.75, minZ: 2.05, maxZ: 3.35, strength: 2.6, radius: 27, cadence: 0.9, kind: "metal" }),
  Object.freeze({ id: "utility-grate", label: "utility grate", room: "utility", minX: 15.45, maxX: 18.15, minZ: 2.6, maxZ: 5.55, strength: 3.05, radius: 34, cadence: 0.92, kind: "metal" }),
  Object.freeze({ id: "mudroom-tray", label: "boot tray", room: "mudroom", minX: 1.25, maxX: 5.55, minZ: 12.0, maxZ: 13.55, strength: 2.35, radius: 25, cadence: 0.86, kind: "clutter" }),
  Object.freeze({ id: "basement-stairs", label: "creaking basement steps", room: "basement-access", minX: 7.8, maxX: 11.65, minZ: -10.75, maxZ: -7.95, strength: 2.35, radius: 26, cadence: 0.76, kind: "wood" }),
  Object.freeze({ id: "basement-pipe", label: "loose basement pipe", room: "basement", minX: 8.0, maxX: 11.65, minZ: -15.85, maxZ: -13.0, strength: 3.2, radius: 36, cadence: 0.96, kind: "metal" }),
  Object.freeze({ id: "garage-clatter", label: "garage metal clutter", room: "garage", minX: 19.45, maxX: 23.55, minZ: 1.7, maxZ: 7.05, strength: 3.4, radius: 40, cadence: 1.0, kind: "metal" }),
]);

export function clamp01(value) {
  return Math.max(0, Math.min(1, Number(value) || 0));
}

export function dawnProgress(timeRemaining, windowSeconds = DAWN_WINDOW_SECONDS) {
  const remaining = Number(timeRemaining);
  if (!Number.isFinite(remaining) || remaining >= windowSeconds) return 0;
  return clamp01((windowSeconds - Math.max(0, remaining)) / windowSeconds);
}

export function findNoiseZone(x, z, zones = NOISE_ZONES) {
  for (let index = 0; index < zones.length; index++) {
    const zone = zones[index];
    if (x >= zone.minX && x <= zone.maxX && z >= zone.minZ && z <= zone.maxZ) return zone;
  }
  return null;
}

export function personalityTuning(catId) {
  if (catId === "biscuit") return Object.freeze({ patrolSpeed: 0.88, searchScale: 0.78, suspiciousScale: 0.82, investigateScale: 0.9, rushScale: 1.9 });
  if (catId === "pepper") return Object.freeze({ patrolSpeed: 0.64, searchScale: 1.12, suspiciousScale: 1.22, investigateScale: 1.18, rushScale: 1.68 });
  return Object.freeze({ patrolSpeed: 0.56, searchScale: 1.38, suspiciousScale: 1.18, investigateScale: 1.05, rushScale: 1.55 });
}

function hexRgb(hex) {
  return {
    r: ((hex >> 16) & 255) / 255,
    g: ((hex >> 8) & 255) / 255,
    b: (hex & 255) / 255,
  };
}

function mixRgb(a, b, t) {
  return {
    r: a.r + (b.r - a.r) * t,
    g: a.g + (b.g - a.g) * t,
    b: a.b + (b.b - a.b) * t,
  };
}

function colorRgb(color) {
  return color ? { r: color.r ?? 0, g: color.g ?? 0, b: color.b ?? 0 } : { r: 0, g: 0, b: 0 };
}

function setColorRgb(color, rgb) {
  if (!color) return;
  if (typeof color.setRGB === "function") color.setRGB(rgb.r, rgb.g, rgb.b);
  else {
    color.r = rgb.r;
    color.g = rgb.g;
    color.b = rgb.b;
  }
}

function stateFor(engine) {
  if (engine.__hearthmouseHouseEvolution) return engine.__hearthmouseHouseEvolution;
  const state = {
    lastTime: engine.time ?? 0,
    playerZoneId: null,
    playerNoiseAt: -Infinity,
    allyNoiseAt: new Map(),
    loudMessageAt: -Infinity,
    dawnAnnounced: false,
    dawnPeakAnnounced: false,
    dawnOverlay: null,
    baseBackground: colorRgb(engine.scene?.background),
    baseFog: colorRgb(engine.scene?.fog?.color),
    baseExposure: engine.renderer?.toneMappingExposure ?? 1,
    lightBaselines: [],
    nestGroup: null,
    nestSignature: "",
    noiseDecorGroup: null,
  };
  engine.scene?.traverse?.((object) => {
    if (!object?.isLight) return;
    if (!(object.isAmbientLight || object.isHemisphereLight || object.isDirectionalLight)) return;
    state.lightBaselines.push({ object, intensity: object.intensity ?? 1, color: colorRgb(object.color) });
  });
  engine.__hearthmouseHouseEvolution = state;
  return state;
}

function installDawnOverlay(state) {
  if (typeof document === "undefined") return null;
  if (state.dawnOverlay?.isConnected) return state.dawnOverlay;
  const existing = document.querySelector(".hearthmouse-dawn-arrival");
  if (existing) {
    state.dawnOverlay = existing;
    return existing;
  }
  const overlay = document.createElement("div");
  overlay.className = "hearthmouse-dawn-arrival";
  overlay.setAttribute("aria-hidden", "true");
  Object.assign(overlay.style, {
    position: "fixed",
    inset: "0",
    pointerEvents: "none",
    zIndex: "2",
    opacity: "0",
    background: "linear-gradient(155deg, rgba(255,220,172,.48) 0%, rgba(245,176,118,.17) 24%, rgba(102,137,173,.07) 48%, rgba(0,0,0,0) 72%)",
    mixBlendMode: "screen",
    transition: "opacity 180ms linear",
  });
  document.body?.appendChild(overlay);
  state.dawnOverlay = overlay;
  return overlay;
}

function updateDawn(engine, state) {
  const active = engine.snapshot?.phase === "foraging";
  const progress = active ? dawnProgress(engine.snapshot?.timeRemaining) : 0;
  const eased = progress * progress * (3 - 2 * progress);
  const skyTarget = hexRgb(0x8b7b78);
  const fogTarget = hexRgb(0x8f7d72);
  setColorRgb(engine.scene?.background, mixRgb(state.baseBackground, skyTarget, eased * 0.74));
  setColorRgb(engine.scene?.fog?.color, mixRgb(state.baseFog, fogTarget, eased * 0.58));
  if (engine.renderer) engine.renderer.toneMappingExposure = state.baseExposure * (1 + eased * 0.38);

  const warm = hexRgb(0xffd6a0);
  for (let index = 0; index < state.lightBaselines.length; index++) {
    const baseline = state.lightBaselines[index];
    baseline.object.intensity = baseline.intensity * (1 + eased * (baseline.object.isDirectionalLight ? 0.68 : 0.34));
    setColorRgb(baseline.object.color, mixRgb(baseline.color, warm, eased * 0.34));
  }

  const overlay = installDawnOverlay(state);
  if (overlay) overlay.style.opacity = String(eased * 0.26);

  if (!active || progress <= 0) {
    state.dawnAnnounced = false;
    state.dawnPeakAnnounced = false;
    return;
  }
  if (!state.dawnAnnounced && progress >= 0.08) {
    state.dawnAnnounced = true;
    engine.showMessage?.("The windows are beginning to pale. Dawn is coming.", 2.8);
  }
  if (!state.dawnPeakAnnounced && (engine.snapshot?.timeRemaining ?? Infinity) <= 12) {
    state.dawnPeakAnnounced = true;
    engine.showMessage?.("Morning light is spilling into the house. Get home.", 2.6);
  }
}

function makeMaterial(I, color, roughness = 0.92) {
  return new I.MeshStandardMaterial({ color, roughness, metalness: 0 });
}

function addBox(I, group, spec) {
  const mesh = new I.Mesh(new I.BoxGeometry(spec.w, spec.h, spec.d), makeMaterial(I, spec.color, spec.roughness));
  mesh.name = spec.name;
  mesh.position.set(spec.x, spec.y, spec.z);
  if (Number.isFinite(spec.rotationY)) mesh.rotation.y = spec.rotationY;
  mesh.castShadow = false;
  mesh.receiveShadow = true;
  mesh.userData.__nestStage = spec.stage ?? 0;
  mesh.userData.__upgrade = spec.upgrade ?? "";
  group.add(mesh);
  return mesh;
}

function ensureNestEvolution(engine, state, I) {
  if (state.nestGroup?.parent) return state.nestGroup;
  if (!engine.world?.root || !engine.world?.nestCenter) return null;
  const n = engine.world.nestCenter;
  const group = new I.Group();
  group.name = "hearthmouse-evolving-nest";
  engine.world.root.add(group);

  addBox(I, group, { name: "nest-warm-cloth-a", x: n.x - 0.12, y: 0.045, z: n.z - 0.12, w: 0.48, h: 0.035, d: 0.31, color: 0x84545d, stage: 1, rotationY: -0.14 });
  addBox(I, group, { name: "nest-warm-cloth-b", x: n.x + 0.18, y: 0.055, z: n.z + 0.05, w: 0.42, h: 0.028, d: 0.26, color: 0xb08d78, stage: 2, rotationY: 0.19 });
  addBox(I, group, { name: "nest-storage-crate", x: n.x - 0.42, y: 0.12, z: n.z + 0.34, w: 0.24, h: 0.24, d: 0.28, color: 0x6e4c32, stage: 2 });
  addBox(I, group, { name: "nest-nursery-divider", x: n.x + 0.43, y: 0.115, z: n.z + 0.16, w: 0.07, h: 0.23, d: 0.55, color: 0x6a4a35, stage: 3 });
  addBox(I, group, { name: "nest-nursery-bedding", x: n.x + 0.26, y: 0.04, z: n.z + 0.34, w: 0.3, h: 0.035, d: 0.28, color: 0xc0a987, stage: 3, rotationY: -0.12 });
  addBox(I, group, { name: "nest-reinforced-beam-a", x: n.x - 0.48, y: 0.21, z: n.z - 0.28, w: 0.08, h: 0.42, d: 0.08, color: 0x493223, stage: 4 });
  addBox(I, group, { name: "nest-reinforced-beam-b", x: n.x + 0.48, y: 0.21, z: n.z - 0.28, w: 0.08, h: 0.42, d: 0.08, color: 0x493223, stage: 4 });
  addBox(I, group, { name: "nest-map-scrap", x: n.x - 0.03, y: 0.205, z: n.z + 0.47, w: 0.46, h: 0.012, d: 0.26, color: 0xd1bd8d, stage: 5, rotationY: 0.08 });
  addBox(I, group, { name: "nest-insulation-layer", x: n.x + 0.02, y: 0.075, z: n.z - 0.32, w: 0.92, h: 0.06, d: 0.18, color: 0x90766c, upgrade: "insulation" });
  addBox(I, group, { name: "nest-scout-board", x: n.x - 0.56, y: 0.21, z: n.z + 0.02, w: 0.04, h: 0.34, d: 0.42, color: 0xbca77a, upgrade: "scouts" });
  addBox(I, group, { name: "nest-tunnel-marker", x: n.x - 0.58, y: 0.11, z: n.z - 0.43, w: 0.22, h: 0.22, d: 0.1, color: 0x4e3828, upgrade: "tunnel" });
  state.nestGroup = group;
  return group;
}

function nestStage(engine) {
  const night = Math.max(1, Math.floor(engine.snapshot?.night ?? 1));
  const stockpile = Math.max(0, Number(engine.snapshot?.stockpile) || 0);
  const population = Math.max(1, Math.floor(engine.snapshot?.population ?? 1));
  const campaignStage = Math.floor((night - 1) / 2);
  const prosperity = stockpile >= 18 ? 1 : 0;
  const colonyBonus = population >= 12 ? 1 : 0;
  return Math.min(5, campaignStage + prosperity + colonyBonus);
}

function updateNestEvolution(engine, state, I) {
  const group = ensureNestEvolution(engine, state, I);
  if (!group) return;
  const stage = nestStage(engine);
  const upgrades = engine.__expansion?.upgrades ?? {};
  const signature = `${stage}:${upgrades.tunnel ? 1 : 0}:${upgrades.scouts ?? 0}:${upgrades.insulation ?? 0}`;
  if (signature === state.nestSignature) return;
  state.nestSignature = signature;
  group.traverse?.((object) => {
    if (!object?.isMesh) return;
    const neededStage = object.userData.__nestStage ?? 0;
    const upgrade = object.userData.__upgrade;
    let visible = neededStage <= stage;
    if (upgrade === "tunnel") visible = !!upgrades.tunnel;
    else if (upgrade === "scouts") visible = (upgrades.scouts ?? 0) > 0;
    else if (upgrade === "insulation") visible = (upgrades.insulation ?? 0) > 0;
    object.visible = visible;
  });
}

function decorateNoiseZones(engine, state, I) {
  if (state.noiseDecorGroup?.parent || !engine.world?.root) return;
  const group = new I.Group();
  group.name = "hearthmouse-noisy-floor-details";
  engine.world.root.add(group);
  for (let index = 0; index < NOISE_ZONES.length; index++) {
    const zone = NOISE_ZONES[index];
    const cx = (zone.minX + zone.maxX) * 0.5;
    const cz = (zone.minZ + zone.maxZ) * 0.5;
    const width = Math.min(0.9, Math.max(0.42, (zone.maxX - zone.minX) * 0.34));
    const depth = Math.min(0.7, Math.max(0.3, (zone.maxZ - zone.minZ) * 0.28));
    const color = zone.kind === "metal" ? 0x777b7c : zone.kind === "paper" ? 0xb49a6a : zone.kind === "toys" ? 0x8a4a43 : zone.kind === "tile" ? 0x8b9695 : 0x66513d;
    for (let strip = 0; strip < 3; strip++) {
      const mesh = addBox(I, group, {
        name: `noise-detail-${zone.id}-${strip}`,
        x: cx + (strip - 1) * width * 0.38,
        y: 0.008 + strip * 0.0005,
        z: cz + ((strip % 2) - 0.5) * depth * 0.22,
        w: width * (0.72 + strip * 0.08),
        h: 0.012,
        d: depth * 0.28,
        color,
        roughness: zone.kind === "metal" ? 0.38 : 0.9,
        rotationY: (strip - 1) * 0.13,
      });
      mesh.userData.__hearthmouseNoiseZone = zone.id;
    }
  }
  state.noiseDecorGroup = group;
}

function catHearsLoudNoise(engine, cat, position, zone, strength) {
  if (!cat || cat.state === "chase") return false;
  const distance = cat.rig?.root?.position?.distanceTo?.(position) ?? Infinity;
  const radius = Math.max(zone?.radius ?? 0, 9 + strength * 6.5);
  if (distance > radius) return false;
  cat.investigation?.copy?.(position);
  cat.lastSeen?.copy?.(position);
  cat.awareness = Math.max(cat.awareness ?? 0, Math.min(0.48, 0.15 + strength * 0.09));
  cat.leisureMode = null;
  cat.leisureTimer = 0;
  cat.__loudNoiseRushTimer = Math.max(cat.__loudNoiseRushTimer ?? 0, 3.2 + strength * 0.42);
  cat.__loudNoiseSource = zone?.id ?? "impact";
  if (typeof engine.setCatState === "function") engine.setCatState(cat, "alert", cat.personality === "kitten" ? 0.2 : 0.32);
  return true;
}

function playZoneClatter(engine, zone, intensity) {
  const audio = engine.audio;
  if (!audio?.context) return;
  if (zone.kind === "metal") {
    audio.tone?.(980, 0.16, 0.1 * intensity, "triangle", 420);
    audio.noise?.(0.18, 0.11 * intensity, 2300);
  } else if (zone.kind === "paper") {
    audio.noise?.(0.2, 0.08 * intensity, 900);
  } else if (zone.kind === "toys" || zone.kind === "clutter") {
    audio.tone?.(520, 0.11, 0.07 * intensity, "triangle", 290);
    audio.noise?.(0.13, 0.06 * intensity, 1500);
  } else if (zone.kind === "tile") {
    audio.tone?.(740, 0.12, 0.075 * intensity, "triangle", 500);
  } else {
    audio.tone?.(165, 0.1, 0.055 * intensity, "triangle", 115);
  }
}

function triggerLoudZone(engine, state, zone, position, strength, sourceId, playerTriggered = false) {
  const currentTime = engine.time ?? 0;
  const scaled = Math.max(zone.strength * 0.68, strength);
  engine.emitNoise?.(position, scaled);
  playZoneClatter(engine, zone, Math.min(1.4, scaled / 2.2));
  let heard = 0;
  if (scaled >= LOUD_NOISE_THRESHOLD) {
    for (let index = 0; index < (engine.cats?.length ?? 0); index++) {
      if (catHearsLoudNoise(engine, engine.cats[index], position, zone, scaled)) heard++;
    }
  }
  if (playerTriggered && heard > 0 && currentTime - state.loudMessageAt > 4.5) {
    state.loudMessageAt = currentTime;
    engine.showMessage?.(`The ${zone.label} erupts with noise — ${heard === 1 ? "a cat is" : "the cats are"} coming to inspect.`, 3.1);
  }
  return sourceId;
}

function updateNoiseZones(engine, state) {
  if (engine.snapshot?.phase !== "foraging") return;
  const currentTime = engine.time ?? 0;
  const speed = engine.playerVelocity?.length?.() ?? 0;
  const playerZone = findNoiseZone(engine.playerPosition?.x ?? Infinity, engine.playerPosition?.z ?? Infinity);
  state.playerZoneId = playerZone?.id ?? null;
  if (playerZone && speed > 0.24) {
    const cadence = playerZone.cadence * (speed > 1.25 ? 0.52 : speed > 0.75 ? 0.72 : 1);
    if (currentTime - state.playerNoiseAt >= cadence) {
      state.playerNoiseAt = currentTime;
      const movementScale = Math.min(1.35, 0.72 + speed * 0.38);
      triggerLoudZone(engine, state, playerZone, engine.playerPosition, playerZone.strength * movementScale, "player", true);
    }
  }

  for (let index = 0; index < (engine.mice?.length ?? 0); index++) {
    const mouse = engine.mice[index];
    if (!mouse?.member?.alive || ["dead", "hiding", "waiting"].includes(mouse.task)) continue;
    const position = mouse.rig?.root?.position;
    if (!position) continue;
    const zone = findNoiseZone(position.x, position.z);
    if (!zone || (mouse.speed ?? 0) < 0.28) continue;
    const key = mouse.member.id;
    const last = state.allyNoiseAt.get(key) ?? -Infinity;
    const cadence = zone.cadence * 1.35;
    if (currentTime - last < cadence) continue;
    state.allyNoiseAt.set(key, currentTime);
    triggerLoudZone(engine, state, zone, position, zone.strength * 0.78, key, false);
  }
}

function installCatPersonalityAndNoiseHotfix(I) {
  const proto = I?.Engine?.prototype;
  if (!proto) return false;
  if (proto.__houseEvolutionInstalled) return true;
  Object.defineProperty(proto, "__houseEvolutionInstalled", { value: true });

  const baseSetCatState = proto.setCatState;
  const baseUpdateCatPatrol = proto.updateCatPatrol;
  const baseFollowCatPath = proto.followCatPath;
  const baseUpdateCats = proto.updateCats;
  const baseEmitNoise = proto.emitNoise;

  proto.setCatState = function personalityStateDuration(cat, state, duration) {
    const tuning = personalityTuning(cat?.id);
    let adjusted = duration;
    if (state === "search") adjusted *= tuning.searchScale;
    else if (state === "suspicious") adjusted *= tuning.suspiciousScale;
    else if (state === "investigating") adjusted *= tuning.investigateScale;
    return baseSetCatState.call(this, cat, state, adjusted);
  };

  proto.updateCatPatrol = function personalityPatrol(cat, delta, speedOverride) {
    const tuning = personalityTuning(cat?.id);
    const selecting = !cat.path?.length || cat.pathIndex >= cat.path.length;
    const result = baseUpdateCatPatrol.call(this, cat, delta, speedOverride ?? tuning.patrolSpeed);

    if (cat?.id === "pepper" && selecting && cat.state === "relaxed" && this.world?.patrolPoints?.length > 4) {
      const nest = this.world.nestCenter;
      const currentTarget = cat.path?.length ? cat.path[cat.path.length - 1] : null;
      if (nest && currentTarget && currentTarget.distanceTo(nest) < 9.5) {
        let bestIndex = -1;
        let bestScore = -Infinity;
        for (let index = 0; index < this.world.patrolPoints.length; index++) {
          const point = this.world.patrolPoints[index];
          const score = point.distanceTo(nest) + ((index + Math.floor(this.time ?? 0)) % 5) * 0.05;
          if (score > bestScore) {
            bestScore = score;
            bestIndex = index;
          }
        }
        if (bestIndex >= 0) {
          cat.patrolIndex = bestIndex;
          this.planCatPath?.(cat, this.world.patrolPoints[bestIndex]);
        }
      }
    }
    return result;
  };

  proto.followCatPath = function rushToLoudNoise(cat, delta, speed) {
    const tuning = personalityTuning(cat?.id);
    const rushing = (cat?.__loudNoiseRushTimer ?? 0) > 0 && cat.state !== "chase";
    return baseFollowCatPath.call(this, cat, delta, rushing ? speed * tuning.rushScale : speed);
  };

  proto.updateCats = function maintainLoudNoiseRush(delta) {
    for (let index = 0; index < (this.cats?.length ?? 0); index++) {
      const cat = this.cats[index];
      cat.__loudNoiseRushTimer = Math.max(0, (cat.__loudNoiseRushTimer ?? 0) - delta);
      if (cat.state === "chase") cat.__loudNoiseRushTimer = 0;
    }
    return baseUpdateCats.call(this, delta);
  };

  proto.emitNoise = function preserveUniversalHearing(position, strength) {
    const result = baseEmitNoise.call(this, position, strength);
    if (strength >= LOUD_NOISE_THRESHOLD) {
      const syntheticZone = { id: "loud-impact", radius: 10 + strength * 6.5 };
      for (let index = 0; index < (this.cats?.length ?? 0); index++) {
        catHearsLoudNoise(this, this.cats[index], position, syntheticZone, strength);
      }
    }
    return result;
  };

  return true;
}

function updateHouseEvolution(engine, I) {
  if (!engine || engine.disposed) return;
  const state = stateFor(engine);
  updateDawn(engine, state);
  updateNestEvolution(engine, state, I);
  decorateNoiseZones(engine, state, I);
  updateNoiseZones(engine, state);
}

registerFrameVisualStage("house-evolution", (engine) => {
  const I = typeof window !== "undefined" ? window.HearthmouseInternals : null;
  if (I?.Engine) updateHouseEvolution(engine, I);
}, 1 / 30);

function installWhenReady(attempt = 0) {
  if (typeof window === "undefined") return;
  const I = window.HearthmouseInternals;
  if (installCatPersonalityAndNoiseHotfix(I)) return;
  if (attempt < MAX_INSTALL_ATTEMPTS) window.setTimeout(() => installWhenReady(attempt + 1), POLL_INTERVAL_MS);
}

if (typeof window !== "undefined") {
  installWhenReady();
}
