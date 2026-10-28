import {
  CATALOG,
  REGIONS,
  MISSIONS,
  ARENAS,
  CHARACTERS,
  campaignLayout
} from './campaign.mjs';
import { WaveDirector } from './waves.mjs';
import { checkpointKey } from './progress.mjs';
import { campaignSupport } from './operations.mjs';
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
      return JSON.parse(storage?.getItem(checkpointKey(adapter.getIdentity?.()?.username)) || 'null');
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
      resultId: run.resultId,
      missionId: run.mission?.id || null,
      arenaId: run.arena?.id || null,
      regionId: run.region.id,
      title: run.mission?.title || run.arena.name,
      totalWaves: run.mission?.waves || null,
      objectives: copy(run.objectives),
      factionId: run.factionId,
      difficulty: run.difficulty,
      seed: run.seed,
      operation: run.operation ? copy(run.operation) : null,
      stageTitle: run.mission?.operation?.stages[run.operation?.stage]?.title || null,
      missionStyle: run.mission?.missionStyle || null,
      enemyEconomy: !!run.mission?.operation.economy,
      limitedArmy: !!run.mission?.operation.limitedArmy,
      totalStages: run.mission?.operation.stages.length || null,
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
      username: options.username || adapter.getIdentity?.()?.username || null,
      resultId: options.resultId || globalThis.crypto?.randomUUID?.() || `run-${seed}-${Date.now()}`,
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
      hazardClock: 0,
      operation: mission?.operation ? { version: 1, stage: 0, stageElapsed: 0, assaultClock: 15, completedStages: [] } : null
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
      support: mode === 'campaign' ? campaignSupport(profile, mission?.index) : null,
      layout: campaignLayout(region.id, seed)
    });
    for (const objective of run.objectives) {
      // Future convoys are materialized at departure, so they cannot die offscreen before their stage.
      if (!(objective.type === 'escort' && (objective.stage > 0 || objective.activateStage > 0)))
        objective.entityId = adapter.createObjective?.(objective) || null;
    }
    if (run.operation) {
      run.director.config.waves = null;
      run.director.state.remaining = 1e9;
      adapter.stage?.(run.operation.stage, mission.operation.stages[0], run);
    }
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
    adapter.objectiveComplete?.(o, run);
    if (run.mission?.operation.choice?.includes(o.id)) {
      for (const other of run.objectives)
        if (other.id !== o.id && run.mission.operation.choice.includes(other.id)) other.unavailable = true;
    }
    notify();
  }
  function updateObjectives(dt) {
    for (const o of run.objectives) {
      const entity = o.entityId ? adapter.entity(o.entityId) : null;
      if (o.failed || o.completed || o.unavailable) continue;
      if (run.operation && o.activateStage > run.operation.stage) continue;
      if (run.operation && o.stage >= 0 && o.stage !== run.operation.stage) continue;
      if (o.type === 'survive') {
        o.progress = Math.min(1, run.operation.stageElapsed / o.seconds);
        if (o.progress >= 1) completeObjective(o);
        continue;
      }
      if (o.type === 'protect') {
        if (!entity && run.mission?.operation.limitedArmy && run.operation.stage === 0) continue;
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
          if (o.optional) {announce('nyx', `${o.label}: lost. The remaining evacuation can continue.`);notify();continue;}
          finish(false, 'The convoy was destroyed.');
          return;
        }
        if (distance(entity, o.to) < 1.5) {
          completeObjective(o);
          continue;
        }
        const threats = adapter.nearby?.(entity.x, entity.y, 5, 1) || [];
        o.blocked = threats.length > 0 || (run.mission?.operation.blockedUntil && !run.objectives.find(item => item.id === run.mission.operation.blockedUntil)?.completed);
        if (o.blocked) adapter.stopEscort?.(entity);
        else adapter.escort?.(entity, o.to, o, run);
        continue;
      }
      if (o.type === 'destroy') {
        if (!entity && o.entityId) completeObjective(o);
        else if (!o.entityId) o.entityId = adapter.createObjective?.(o) || null;
        continue;
      }
      const friendly = (o.workers ? adapter.workersNearby?.(o.x, o.y, 2.6) : adapter.nearby?.(o.x, o.y, 2.6, 0)) || [],
        enemy = adapter.nearby?.(o.x, o.y, 3.5, 1) || [];
      if (friendly.length && !enemy.length) {
        o.progress = Math.min(1, o.progress + dt / (o.seconds || 6));
        if (o.progress >= 1) completeObjective(o);
      }
    }
  }
  function nextWave() {
    if (!run || adapter.paused?.()) return null;
    if (run.operation) return null;
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
    dt = Math.max(0, Math.min(1, Number(dt) || 0));
    if (run.operation) {
      const operation = run.operation, stage = run.mission.operation.stages[operation.stage];
      operation.stageElapsed += dt;
      operation.assaultClock -= dt;
      if (stage.assault && operation.assaultClock <= 0 && run.director.state.phase === 'preparation' && (!run.mission.operation.portal || !run.objectives.find(o => o.id === run.mission.operation.portal)?.completed)) {
        const plan = run.director.nextWave(adapter.army?.());
        // Scripted pressure supports the current tactical objective, with bounded reinforcement size.
        if (plan) {
          plan.composition = plan.composition.slice(0, Math.min(20, 4 + run.region.index * 2 + operation.stage * 2));
          adapter.configureWave?.(plan,run);
          run.director.state.pending = [...plan.composition];
          onWave(plan);
        }
        operation.assaultClock = Math.max(24, 52 - run.region.index * 3);
      }
      if (run.director.state.phase === 'preparation') run.director.state.remaining = 1e9;
      adapter.operationTick?.(dt, run);
    }
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
    if (run.operation) {
      const operation = run.operation, stage = run.mission.operation.stages[operation.stage];
      if (stage.ids[stage.any?'some':'every'](id => run.objectives.find(o => o.id === id)?.completed)) {
        operation.completedStages.push(operation.stage);
        operation.stage++;
        operation.stageElapsed = 0;
        operation.assaultClock = 6;
        if (operation.stage >= run.mission.operation.stages.length) {
          finish(true, 'All tactical stages and primary objectives completed.');
          return;
        }
        const next = run.mission.operation.stages[operation.stage];
        for (const objective of run.objectives) if ((objective.stage === operation.stage || objective.activateStage === operation.stage) && !objective.entityId && ['escort','destroy'].includes(objective.type)) objective.entityId = adapter.createObjective?.(objective) || null;
        adapter.stage?.(operation.stage, next, run);
        announce('elara_venn', next.title);
        notify(); checkpoint();
      }
    }
    if (!(run.mission?.operation.limitedArmy ? (adapter.hasForces?.() ?? adapter.hasBase?.()) : adapter.hasBase?.())) {
      finish(false, 'Your final command base has fallen.');
      return;
    }
    if (
      !run.operation && run.director.state.phase === 'objectives' &&
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
        (o) => !o.optional && !o.unavailable && o.type !== 'protect'
      );
    const stars = won
      ? 1 +
        (optionalObjectives.length ===
        run.objectives.filter((o) => o.optional && !o.unavailable).length
          ? 1
          : 0) +
        ((stats.unitsLost || 0) < Math.max(8, s.wave * 2) ? 1 : 0)
      : 0;
    const result = {
      username: run.username, resultId: run.resultId, completedAt: Date.now(),
      mode: run.mode,
      missionId: run.mission?.id || null,
      arenaId: run.arena?.id || null,
      regionId: run.region.id,
      title: run.mission?.title || run.arena.name,
      won,
      wave: s.cleared,
      highestWave: s.wave,
      wavesSurvived: s.cleared,
      enemiesDefeated: stats.enemiesDefeated ?? s.enemiesDefeated,
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
    if (run.operation) {
      result.completedStages = copy(run.operation.completedStages);
      result.campaignRewards = won ? [run.mission.campaignReward] : [];
      if(run.mission.index===11){result.evacuatedGroups=run.objectives.filter(o=>o.type==='escort'&&o.completed).length;if(won)result.campaignRewards.push({id:'evacuation-survivors',label:`${result.evacuatedGroups} convoy groups evacuated`});}
      result.starReasons = [{earned:won,label:'Complete every primary tactical objective'}, {earned:won && optionalObjectives.length === run.objectives.filter(o => o.optional && !o.unavailable).length,label:'Complete every available optional objective'}, {earned:won && (stats.unitsLost || 0) < Math.max(8,s.wave*2),label:'Preserve your army'}];
    }
    run.result = result;
    try {
      storage?.removeItem(checkpointKey(run.username));
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
      username: run.username, resultId: run.resultId,
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
    if (run.operation) checkpoint.operation = copy(run.operation);
    try {
      storage?.setItem(checkpointKey(run.username), JSON.stringify(checkpoint));
    } catch {}
    emit('turncraft:checkpoint', { checkpoint });
    return checkpoint;
  }
  function resume(saved = readSaved()) {
    const owner = adapter.getIdentity?.()?.username || null;
    if (saved?.username && saved.username !== owner)
      throw new Error('This checkpoint belongs to another Game Hub profile.');
    if (run && !run.result && adapter.isStarted?.() && run.username === owner && saved?.resultId === run.resultId) {
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
    if (run.operation) {
      run.operation = saved.operation?.version === 1 ? copy(saved.operation) : run.operation;
      // Older checkpoints keep their objective progress and begin at the first unfinished tactical stage.
      if (!saved.operation) while (run.operation.stage < run.mission.operation.stages.length - 1 && run.mission.operation.stages[run.operation.stage].ids.every(id => run.objectives.find(o => o.id === id)?.completed)) run.operation.stage++;
      for (const objective of run.mission.objectives) if (!run.objectives.some(o => o.id === objective.id)) run.objectives.push({...copy(objective), progress:0, completed:false, failed:false, entityId:null});
      run.objectives = run.objectives.map(o => {const definition=run.mission.objectives.find(definition => definition.id === o.id);return {...copy(definition || {}), ...o, stage: definition?.stage ?? o.stage, optional: definition?.optional ?? o.optional};});
      run.director.config.waves = null;
      if(run.director.state.phase==='objectives'){run.director.state.phase='preparation';run.director.state.remaining=1e9;}
    }
    adapter.restore(saved.simulation, saved.savedAt);
    if (run.operation) for (const objective of run.objectives) if (!objective.entityId && !objective.completed && objective.stage <= run.operation.stage && !(objective.activateStage > run.operation.stage)) objective.entityId = adapter.createObjective?.(objective) || null;
    if (run.operation) adapter.stage?.(run.operation.stage, run.mission.operation.stages[run.operation.stage], run, {restoring:true});
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
    getSoloSettings: () => adapter.getSoloSettings?.(),
    drawMapPreview: (canvas, id) => adapter.drawMapPreview?.(canvas, id),
    startSkirmish(options) { run = null; adapter.hud?.(null); return adapter.startSkirmish?.(options); },
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
