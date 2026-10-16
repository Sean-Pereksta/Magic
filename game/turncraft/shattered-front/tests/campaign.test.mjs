import test from 'node:test';
import assert from 'node:assert/strict';
import {
  CATALOG,
  REGIONS,
  MISSIONS,
  campaignLayout,
  missionUnlocked
} from '../campaign.mjs';
import { buildWave, WaveDirector, ROSTERS, modifiersFor } from '../waves.mjs';
import { createExpansion, rebaseTimers, CHECKPOINT_KEY } from '../engine.mjs';
import { readProgress, recordResult } from '../progress.mjs';

test('offline campaign and survival results remain queued after reloading local progress', (t) => {
  const previousStorage = globalThis.localStorage;
  const key = 'turncraft.shattered-front.progress.Commander.v1';
  const campaign = {
    mode: 'campaign', missionId: MISSIONS[0].id, regionId: REGIONS[0].id,
    won: true, stars: 3, difficulty: 'hard', optionalObjectives: []
  };
  const survival = {
    mode: 'survival', arenaId: CATALOG.arenas[0].id,
    factionId: 'frontier', difficulty: 'medium', wave: 12,
    durationSeconds: 420, enemiesDefeated: 80
  };
  const saved = recordResult(recordResult(null, campaign), survival);
  saved.pendingResults = [
    campaign, null, 'invalid', [], { mode: 'ranked' },
    { ...campaign, won: false }, survival
  ];
  const storage = new Map([[key, JSON.stringify(saved)]]);
  globalThis.localStorage = { getItem: (name) => storage.get(name) || null };
  t.after(() => {
    if (previousStorage === undefined) delete globalThis.localStorage;
    else globalThis.localStorage = previousStorage;
  });

  const restored = readProgress(key);
  assert.deepEqual(restored.pendingResults, [campaign, {...campaign, won: false}, survival]);
  assert.equal(restored.campaign.completedMissions[campaign.missionId].stars, 3);
  assert.equal(restored.survival.recordsByMap[`${survival.arenaId}:medium`].wave, 12);

  storage.set(key, JSON.stringify({ ...saved, pendingResults: {} }));
  const malformedQueue = readProgress(key);
  assert.deepEqual(malformedQueue.pendingResults, []);
  assert.equal(malformedQueue.campaign.completedMissions[campaign.missionId].stars, 3);
});

test('campaign has 24 authored missions, four per distinct connected region', () => {
  assert.equal(MISSIONS.length, 24);
  const signatures = new Set();
  for (const r of REGIONS) {
    const ms = MISSIONS.filter((m) => m.regionId === r.id);
    assert.equal(ms.length, 4);
    const map = campaignLayout(r.id, 47);
    assert.equal(map.terrain.length, 1260);
    signatures.add(map.terrain.map((t) => (t.blocked ? '#' : '.')).join(''));
    const seen = new Set(),
      queue = [map.starts[0]];
    const key = (p) => p.y * 42 + p.x;
    seen.add(key(queue[0]));
    for (let i = 0; i < queue.length; i++) {
      const p = queue[i];
      for (const [dx, dy] of [
        [1, 0],
        [-1, 0],
        [0, 1],
        [0, -1]
      ]) {
        const x = p.x + dx,
          y = p.y + dy,
          t =
            x >= 0 && x < 42 && y >= 0 && y < 30
              ? map.terrain[y * 42 + x]
              : null;
        if (t && !t.blocked && !seen.has(key(t))) {
          seen.add(key(t));
          queue.push(t);
        }
      }
    }
    for (const p of [
      ...map.spawnPoints,
      ...map.expansions,
      ...ms.flatMap((m) => m.objectives)
    ])
      assert.ok(seen.has(key(p)), `${r.id}: inaccessible ${JSON.stringify(p)}`);
  }
  assert.equal(signatures.size, 6);
  assert.equal(new Set(MISSIONS.map((m) => m.id)).size, 24);
  assert.ok(MISSIONS.every((m) => m.objectives.length >= 2));
  assert.ok(missionUnlocked(MISSIONS[0].id, []));
  assert.ok(!missionUnlocked(MISSIONS[1].id, []));
  assert.ok(missionUnlocked(MISSIONS[1].id, [MISSIONS[0].id]));
  assert.equal(CATALOG.characters.length, 5);
});

test('deterministic waves use valid faction rosters, counters and bounded late armies', () => {
  for (const faction of Object.keys(ROSTERS))
    for (const wave of [1, 2, 5, 10, 50, 101, 500]) {
      const config = {
          wave,
          seed: 917,
          enemyFaction: faction,
          army: { flying: 12, total: 18, structures: 10 }
        },
        plan = buildWave(config);
      assert.deepEqual(plan, buildWave(config));
      assert.ok(plan.composition.length <= 72);
      const roster = new Set(Object.values(ROSTERS[faction]).flat());
      assert.ok(plan.composition.every((t) => roster.has(t)));
      assert.ok(plan.routes.length >= 1 && plan.routes.length <= 5);
      if (wave % 10 === 0) assert.ok(plan.elite);
    }
  assert.ok(modifiersFor(5).some((m) => m.id === 'haste'));
  assert.equal(modifiersFor(100).length, 8);
  assert.notDeepEqual(
    buildWave({ seed: 3, wave: 6 }),
    buildWave({ seed: 8, wave: 6 })
  );
});

test('director completes a mission and an extended 125-wave survival without an artificial ending', () => {
  function run(waves, target) {
    const d = new WaveDirector({
        waves,
        seed: 13,
        preparationSeconds: 0,
        betweenWavesSeconds: 0
      }),
      alive = new Set();
    let id = 0,
      ticks = 0,
      peak = 0;
    while (d.state.cleared < target && ticks++ < 100000) {
      d.tick(1, {
        alive: (n) => alive.has(n),
        spawn: () => {
          alive.add(++id);
          return id;
        }
      });
      peak = Math.max(peak, d.state.activeIds.length);
      alive.clear();
    }
    assert.ok(ticks < 100000);
    assert.ok(peak <= 54);
    return d;
  }
  const mission = run(4, 4);
  assert.equal(mission.state.phase, 'objectives');
  const survival = run(null, 125);
  assert.equal(survival.state.cleared, 125);
  assert.notEqual(survival.state.phase, 'objectives');
  assert.deepEqual(survival.state.milestones, [5, 10, 15, 20, 30, 50, 75, 100]);
});

function harness() {
  const storageMap = new Map(),
    events = [],
    units = new Map();
  let id = 0,
    started = false,
    paused = false,
    base = true,
    snapshot = { gameKey: 'chess_starfront', entities: [], players: [{}] },
    restored = null;
  const adapter = {
    begin() {
      started = true;
      base = true;
      units.clear();
    },
    isStarted: () => started,
    isMultiplayer: () => false,
    paused: () => paused,
    unpause() {
      paused = false;
    },
    pause() {
      paused = true;
    },
    createObjective(o) {
      if (o.type === 'capture' || o.type === 'rescue') return null;
      const e = { id: ++id, x: o.x, y: o.y, hp: 100, maxHp: 100 };
      units.set(e.id, e);
      return e.id;
    },
    entity: (n) => units.get(n),
    hasBase: () => base,
    nearby: () => [],
    spawn() {
      const n = ++id;
      units.set(n, { id: n });
      return n;
    },
    army: () => ({}),
    stats: () => ({ unitsLost: 2 }),
    snapshot: () => structuredClone(snapshot),
    restore(value) {
      restored = value;
    },
    getFactions: () => [],
    getUnits: () => [],
    getMaps: () => [],
    getIdentity: () => ({}),
    selectFaction: () => true,
    configureSolo: () => true,
    showSkirmish() {},
    finish() {},
    abandon() {
      started = false;
    }
  };
  const storage = {
    getItem: (key) => storageMap.get(key) || null,
    setItem: (key, value) => storageMap.set(key, value),
    removeItem: (key) => storageMap.delete(key)
  };
  const engine = createExpansion(adapter, {
    storage,
    emit: (type, detail) => events.push({ type, detail })
  });
  return {
    engine,
    units,
    adapter,
    storage,
    events,
    get restored() {
      return restored;
    },
    loseBase() {
      base = false;
    },
    clearWave() {
      for (const n of engine.getState().activeIds) units.delete(n);
    }
  };
}

test('switching profiles keeps an active run with its owner and starts/resumes the next run under the new profile', () => {
  const h = harness(); let selected = 'ProfileA';
  h.adapter.getIdentity = () => ({username: selected});
  h.engine.startCampaign(MISSIONS[0].id, {factionId: 'frontier'});
  const old = h.engine.checkpoint();
  assert.equal(old.username, 'ProfileA');
  selected = 'ProfileB';
  const oldResult = h.engine.finish(false, 'Defeat after account switch');
  assert.equal(oldResult.username, 'ProfileA');
  h.engine.startCampaign(MISSIONS[0].id, {factionId: 'brood', username: selected});
  const current = h.engine.checkpoint();
  assert.equal(current.username, 'ProfileB');
  assert.notEqual(current.resultId, old.resultId);
  assert.equal(JSON.parse(h.storage.getItem(`${CHECKPOINT_KEY}.ProfileB`)).username, 'ProfileB');
  h.engine.abandon(); selected = 'ProfileA';
  assert.throws(() => h.engine.resume(current), /another Game Hub profile/);
  selected = 'ProfileB';
  h.engine.resume(current);
  assert.equal(h.engine.checkpoint().username, 'ProfileB');
});

test('Continue restores the selected profile checkpoint while another profile has an active simulation', () => {
  const h = harness(); let selected = 'ProfileB';
  h.adapter.getIdentity = () => ({username: selected});
  h.engine.startCampaign(MISSIONS[0].id, {factionId: 'brood'});
  const savedB = h.engine.checkpoint();
  h.engine.abandon(); selected = 'ProfileA';
  h.engine.startCampaign(MISSIONS[0].id, {factionId: 'frontier'});
  const savedA = h.engine.checkpoint();
  selected = 'ProfileB';
  assert.throws(() => h.engine.resume(savedA), /another Game Hub profile/);
  h.engine.resume(savedB);
  assert.equal(h.engine.getState().factionId, 'brood');
  assert.equal(h.engine.checkpoint().resultId, savedB.resultId);
  assert.equal(h.engine.checkpoint().username, 'ProfileB');
});

test('actual mission objective, rewards, results and defeat lifecycle', () => {
  const h = harness();
  h.engine.startCampaign('amber-wastes-1', { seed: 19 });
  h.engine.nextWave();
  let ticks = 0;
  while (!h.events.some((e) => e.type === 'sf:result') && ticks++ < 3000) {
    h.engine.tick(1);
    h.clearWave();
    if (h.engine.getState().phase === 'preparation') h.engine.nextWave();
  }
  assert.ok(ticks < 3000);
  const result = h.events.find((e) => e.type === 'sf:result').detail;
  assert.ok(result.won);
  assert.equal(result.wave, 4);
  assert.equal(result.missionId, 'amber-wastes-1');
  assert.equal(result.stars, 2);
  assert.equal(h.storage.getItem(CHECKPOINT_KEY), null);
  h.engine.startSurvival('frozen-rift', { seed: 9, factionId: 'ascendant' });
  h.loseBase();
  h.engine.tick(0.5);
  const results = h.events.filter((e) => e.type === 'sf:result');
  assert.equal(results.length, 2);
  assert.equal(results[1].detail.won, false);
  assert.equal(results[1].detail.mode, 'survival');
  h.engine.finish(false);
  assert.equal(h.events.filter((e) => e.type === 'sf:result').length, 2);
});

test('checkpoint restores pending wave troops, objective progress and simulation instead of restarting', () => {
  const first = harness();
  first.engine.startSurvival('ironwood-basin', {
    seed: 500,
    factionId: 'brood'
  });
  first.engine.nextWave();
  first.engine.tick(1);
  const save = first.engine.checkpoint(),
    expected = first.engine.getState();
  assert.ok(save.director.state.pending.length > 0);
  assert.ok(save.director.state.activeIds.length > 0);
  const second = harness();
  second.engine.resume(save);
  const resumed = second.engine.getState();
  assert.equal(resumed.seed, 500);
  assert.equal(resumed.wave, expected.wave);
  assert.equal(resumed.elapsed, expected.elapsed);
  assert.deepEqual(resumed.pending, expected.pending);
  assert.deepEqual(second.restored, save.simulation);
  assert.equal(second.engine.checkpoint().director.state.phase, 'assault');
  assert.throws(() => WaveDirector.restore({ version: 9 }), /checkpoint/);
});

test('absolute entity and production timers move forward with checkpoint restore', () => {
  const now = Date.now(),
    state = {
      entities: [
        {
          attackReadyAt: now + 200,
          queue: [{ finishAt: now + 1200, startAt: now - 300 }],
          hp: 100,
          nextLarvaAt: now + 700
        }
      ],
      matchStartedAt: now - 9000
    };
  rebaseTimers(state, 80000);
  assert.equal(state.entities[0].attackReadyAt, now + 80200);
  assert.equal(state.entities[0].queue[0].finishAt, now + 81200);
  assert.equal(state.entities[0].hp, 100);
  assert.equal(state.matchStartedAt, now + 71000);
});
