import {
  CATALOG,
  REGIONS,
  MISSIONS,
  ARENAS,
  CHARACTERS,
  campaignLayout
} from './campaign.mjs';
import { WaveDirector } from './waves.mjs';
export { CATALOG, campaignLayout };
export const CHECKPOINT_KEY = 'turncraft.shattered-front.checkpoint.v1';
const copy = (value) => JSON.parse(JSON.stringify(value));
const distance = (a, b) => Math.hypot(a.x - b.x, a.y - b.y);

// Adapter keeps the original entity, combat, production and input systems authoritative.
export function createExpansion(
  adapter,
  {
    storage = globalThis.localStorage,
    emit = (type, detail) =>
      globalThis.dispatchEvent(new CustomEvent(type, { detail }))
  } = {}
) {
  let run = null,
    profile = {},
    lastState = 0,
    lastSave = 0,
    objectiveClock = 0;
  const readSaved = () => {
    try {
      return JSON.parse(storage?.getItem(CHECKPOINT_KEY) || 'null');
    } catch {
      return null;
    }
  };
  function announce(speaker, text) {
    adapter.dialogue?.({
      speaker: CHARACTERS.find((c) => c.id === speaker) || CHARACTERS[0],
      text
    });
  }
  function getState() {
    if (!run) return null;
    const s = run.director.state;
    return {
      ...copy(s),
      mode: run.mode,
      missionId: run.mission?.id || null,
      arenaId: run.arena?.id || null,
      regionId: run.region.id,
      title: run.mission?.title || run.arena.name,
      totalWaves: run.mission?.waves || null,
      objectives: copy(run.objectives),
      factionId: run.factionId,
      difficulty: run.difficulty,
      seed: run.seed,
      protectedHealth: adapter.protectedHealth?.(run.objectives) ?? 1
    };
  }
  function notify() {
    const state = getState();
    adapter.hud?.(state);
    emit('sf:state', state);
    emit('turncraft:expansion-state', state);
  }
  function start(mode, id, options = {}) {
    if (adapter.isMultiplayer?.())
      throw new Error(
        'Leave the multiplayer battlefield before starting a solo mission.'
      );
    const mission =
        mode === 'campaign' ? MISSIONS.find((m) => m.id === id) : null,
      arena =
        mode === 'survival'
          ? ARENAS.find((a) => a.id === id || a.regionId === id)
          : null;
    if (!mission && !arena)
      throw new Error('Unknown mission or survival arena.');
    const region = REGIONS.find((r) => r.id === (mission || arena).regionId),
      factionId = options.factionId || adapter.getFaction?.() || 'frontier';
    if (!['frontier', 'ascendant', 'brood'].includes(factionId))
      throw new Error('Unknown faction.');
    const difficulty = ['easy', 'medium', 'hard', 'pro'].includes(
        options.difficulty
      )
        ? options.difficulty
        : 'medium',
      seed = Number(options.seed ?? Date.now()) >>> 0,
      enemyFaction = ['brood', 'frontier', 'ascendant'][region.index % 3];
    run = {
      mode,
      mission,
      arena,
      region,
      factionId,
      difficulty,
      seed,
      enemyFaction,
      objectives: (
        mission?.objectives || [
          {
            id: 'bastion',
            type: 'protect',
            label: 'Keep your command base alive',
            x: 6,
            y: 15
          }
        ]
      ).map((o) => ({
        ...copy(o),
        progress: 0,
        completed: false,
        failed: false,
        entityId: null
      })),
      director: new WaveDirector({
        waves: mission?.waves || null,
        preparationSeconds: mission?.preparationSeconds || 60,
        betweenWavesSeconds: mission?.betweenWavesSeconds || 35,
        seed,
        regionIndex: region.index,
        difficulty,
        strategy: mission?.strategy,
        enemyFaction
      }),
      result: null,
      hazards: [],
      hazardClock: 0
    };
    adapter.begin({
      mode,
      mission,
      arena,
      region,
      factionId,
      difficulty,
      seed,
      enemyFaction,
      layout: campaignLayout(region.id, seed)
    });
    for (const objective of run.objectives)
      objective.entityId = adapter.createObjective?.(objective) || null;
    lastState = 0;
    lastSave = 0;
    objectiveClock = 0;
    announce(
      mission?.briefing.speaker || 'elara_venn',
      mission?.briefing.text ||
        'The gates will keep opening. Prepare your defenses, and make every wave count.'
    );
    notify();
    checkpoint();
    return getState();
  }
  function completeObjective(o) {
    if (o.completed) return;
    o.completed = true;
    o.progress = 1;
    adapter.reward?.(o.optional ? 180 : 100, o.optional ? 60 : 30);
    announce(
      o.type === 'rescue' ? 'nyx' : 'kade_rusk',
      `${o.label}: complete. Coalition supplies delivered.`
    );
    if (o.type === 'rescue') adapter.reinforce?.(o);
    notify();
  }
  function updateObjectives(dt) {
    for (const o of run.objectives) {
      const entity = o.entityId ? adapter.entity(o.entityId) : null;
      if (o.failed || o.completed) continue;
      if (o.type === 'protect') {
        if (!entity) {
          o.failed = true;
          if (!o.optional) {
            finish(false, `${o.label}: objective destroyed.`);
            return;
          }
        }
        continue;
      }
      if (o.type === 'escort') {
        if (!entity) {
          o.failed = true;
          finish(false, 'The convoy was destroyed.');
          return;
        }
        if (distance(entity, o.to) < 1.5) {
          completeObjective(o);
          continue;
        }
        adapter.escort?.(entity, o.to);
        continue;
      }
      if (o.type === 'destroy') {
        if (!entity) completeObjective(o);
        continue;
      }
      const friendly = adapter.nearby?.(o.x, o.y, 2.6, 0) || [],
        enemy = adapter.nearby?.(o.x, o.y, 3.5, 1) || [];
      if (friendly.length && !enemy.length) {
        o.progress = Math.min(1, o.progress + dt / (o.seconds || 6));
        if (o.progress >= 1) completeObjective(o);
      }
    }
  }
  function nextWave() {
    if (!run || adapter.paused?.()) return null;
    const plan = run.director.nextWave(adapter.army?.());
    if (plan) onWave(plan);
    notify();
    return plan;
  }
  function onWave(plan) {
    announce(
      'nyx',
      `Wave ${plan.number}: ${plan.label}. ${plan.routes.length > 1 ? 'Multiple approaches detected.' : 'Incoming force on the marked approach.'}`
    );
    const event = run.mission?.events.find((e) => e.wave === plan.number);
    if (event) announce(event.speaker, event.text);
    if (
      plan.modifiers.some((m) => m.id === 'disruption') &&
      plan.number % 5 === 0
    )
      adapter.disrupt?.();
    // Final battle unlocks one core phase at a time; it cannot be sniped during preparation.
    if (run.mission?.strategy === 'finale')
      adapter.phase?.(
        Math.min(3, 1 + Math.floor((plan.number - 1) / 5)),
        plan.number
      );
  }
  function tick(dt) {
    if (!run || run.result || adapter.paused?.()) return;
    const events = run.director.tick(dt, {
      alive: (id) => !!adapter.entity(id),
      spawn: (type, plan, index) => adapter.spawn(type, plan, index, run),
      army: adapter.army?.()
    });
    for (const e of events) {
      if (e.type === 'wave') onWave(e.plan);
      if (e.type === 'cleared') {
        adapter.cleanup?.();
        adapter.reward?.(70 + run.director.state.wave * 9, 20);
        checkpoint();
      }
      if (e.type === 'milestone')
        announce(
          'elara_venn',
          `Wave ${e.wave} secured. Your coalition service emblem has been recorded.`
        );
    }
    objectiveClock += dt;
    if (objectiveClock >= 0.3) {
      updateObjectives(objectiveClock);
      objectiveClock = 0;
      if (run.result) return;
    }
    if (!adapter.hasBase?.()) {
      finish(false, 'Your final command base has fallen.');
      return;
    }
    if (
      run.director.state.phase === 'objectives' &&
      run.objectives
        .filter((o) => !o.optional && o.type !== 'protect')
        .every((o) => o.completed)
    ) {
      finish(true, 'All Fracture waves and mission objectives completed.');
      return;
    }
    const s = run.director.state;
    if (s.elapsed - lastState >= 0.5) {
      adapter.commandWave?.(s.activeIds, s.wavePlan, run);
      lastState = s.elapsed;
      notify();
    }
    if (s.elapsed - lastSave >= 20) {
      lastSave = s.elapsed;
      checkpoint();
    }
    if (
      run.region.weather === 'eruption' ||
      s.modifiers.some((m) => m.id === 'hazard')
    ) {
      run.hazardClock += dt;
      if (run.hazardClock > 22) {
        run.hazardClock = 0;
        adapter.hazard?.(run.seed + s.wave + Math.floor(s.elapsed));
      }
    }
  }
  function finish(won, reason = '') {
    if (!run || run.result) return null;
    run.director.finish(won);
    const s = run.director.state,
      stats = adapter.stats?.() || {},
      optionalObjectives = run.objectives
        .filter((o) => o.optional && o.completed)
        .map((o) => o.id),
      required = run.objectives.filter(
        (o) => !o.optional && o.type !== 'protect'
      );
    const stars = won
      ? 1 +
        (optionalObjectives.length ===
        run.objectives.filter((o) => o.optional).length
          ? 1
          : 0) +
        ((stats.unitsLost || 0) < Math.max(8, s.wave * 2) ? 1 : 0)
      : 0;
    const result = {
      mode: run.mode,
      missionId: run.mission?.id || null,
      arenaId: run.arena?.id || null,
      regionId: run.region.id,
      title: run.mission?.title || run.arena.name,
      won,
      wave: s.cleared,
      highestWave: s.wave,
      wavesSurvived: s.cleared,
      enemiesDefeated: s.enemiesDefeated,
      unitsLost: stats.unitsLost || 0,
      durationSeconds: Math.round(s.elapsed),
      stars,
      optionalObjectives,
      milestones: [...s.milestones],
      factionId: run.factionId,
      difficulty: run.difficulty,
      seed: run.seed,
      mostEffectiveUnit: stats.mostEffectiveUnit || '',
      reason,
      completedObjectives: required.filter((o) => o.completed).map((o) => o.id)
    };
    run.result = result;
    try {
      storage?.removeItem(CHECKPOINT_KEY);
    } catch {}
    adapter.finish?.(result);
    notify();
    if (won)
      announce(
        run.mission?.victory.speaker || 'elara_venn',
        run.mission?.victory.text || 'The coalition holds.'
      );
    emit('sf:result', result);
    emit('turncraft:expansion-result', result);
    return result;
  }
  function checkpoint() {
    if (!run || run.result) return null;
    const checkpoint = {
      version: 1,
      savedAt: Date.now(),
      mode: run.mode,
      missionId: run.mission?.id || null,
      arenaId: run.arena?.id || null,
      regionId: run.region.id,
      factionId: run.factionId,
      difficulty: run.difficulty,
      seed: run.seed,
      objectives: copy(run.objectives),
      director: run.director.serialize(),
      simulation: adapter.snapshot(),
      hazardClock: run.hazardClock
    };
    try {
      storage?.setItem(CHECKPOINT_KEY, JSON.stringify(checkpoint));
    } catch {}
    emit('turncraft:checkpoint', { checkpoint });
    return checkpoint;
  }
  function resume(saved = readSaved()) {
    if (run && !run.result && adapter.isStarted?.()) {
      adapter.unpause?.();
      notify();
      return getState();
    }
    if (
      saved?.version !== 1 ||
      !saved.simulation ||
      !Array.isArray(saved.objectives)
    )
      throw new Error('No compatible campaign checkpoint is available.');
    // Validate all director data before touching the existing simulation.
    const director = WaveDirector.restore(saved.director);
    start(
      saved.mode,
      saved.mode === 'campaign' ? saved.missionId : saved.arenaId,
      saved
    );
    run.director = director;
    run.objectives = copy(saved.objectives);
    run.hazardClock = saved.hazardClock || 0;
    adapter.restore(saved.simulation, saved.savedAt);
    adapter.unpause?.();
    lastState = director.state.elapsed;
    lastSave = lastState;
    notify();
    checkpoint();
    return getState();
  }
  const api = {
    catalog: CATALOG,
    startCampaign: (id, options) => start('campaign', id, options),
    startSurvival: (id, options) => start('survival', id, options),
    nextWave,
    tick,
    getState,
    checkpoint,
    resume,
    finish,
    hasCheckpoint: () => !!readSaved(),
    abandon() {
      run = null;
      adapter.hud?.(null);
      adapter.abandon?.();
    },
    isActive: () => !!run && !run.result,
    getProgress: () => copy(profile.campaign || profile.progress || {}),
    setProfile(value) {
      profile = value || {};
      adapter.setProfile?.(value);
    },
    getFactions: () => adapter.getFactions(),
    getUnits: () => adapter.getUnits(),
    loadUnitArt: (type) => adapter.loadUnitArt?.(type),
    getMaps: () => adapter.getMaps(),
    getIdentity: () => adapter.getIdentity(),
    selectFaction: (id) => adapter.selectFaction(id),
    configureSolo: (options) => adapter.configureSolo(options),
    showSkirmish: () => {
      run = null;
      adapter.hud?.(null);
      adapter.showSkirmish();
    },
    resumeBattle() {
      adapter.unpause?.();
      return getState();
    },
    exitToLobby: () => adapter.exitToLobby?.(),
    returnToMenu() {
      checkpoint();
      adapter.pause?.();
      emit('sf:menu', { checkpoint: readSaved(), state: getState() });
    },
    getProfile: () => copy(profile)
  };
  return api;
}

// Simulation timers use absolute dates; a restored solo battle must not finish every queue at once.
const TIMER_KEYS =
  /^(?:.*(?:At|Until|End|Start)|nextLarvaAt|morphEnd|buildEnd|buildStart)$/;
export function rebaseTimers(value, delta) {
  if (!value || typeof value !== 'object') return value;
  for (const [key, item] of Object.entries(value)) {
    if (typeof item === 'number' && item > 1e11 && TIMER_KEYS.test(key))
      value[key] = item + delta;
    else if (item && typeof item === 'object') rebaseTimers(item, delta);
  }
  return value;
}
