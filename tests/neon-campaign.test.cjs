'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const Campaign = require('../game/neon-arena/neon-campaign.js');
const Progress = require('../game/neon-arena/neon-survival-progress.js');
const player = () => ({ x: 0, y: 1.86, z: 0, hp: 100, armor: 50, yaw: 0, weapon: 'burrower' });
const position = { x: 0, y: 1.86, z: 0 };
function finishStage(run, extras = {}) {
  const index = run.index, actor = player(), escort = { ...actor };
  let result;
  for (let i = 0; i < 1000 && run.index === index && run.status === 'active'; i++) result = Campaign.step(run, .1, { player: actor, objectivePosition: position, interact: true, escort, enemyCount: 0, ...extras });
  return result;
}
function finishMission(id, extras) { const run = Campaign.createRun(id); while (run.status === 'active') finishStage(run, extras); return run; }

test('both perspectives have five sequential missions and all six physical battlefields', () => {
  assert.equal(Campaign.catalog().length, 10);
  for (const faction of ['cats', 'mice']) {
    const list = Campaign.catalog(faction); assert.deepEqual(list.map(m => m.chapter), [1, 2, 3, 4, 5]);
    assert.ok(list.every(m => m.briefing.length > 100 && m.debrief.length > 70));
    assert.equal(new Set(list.map(m => m.reward)).size, 5);
  }
  assert.equal(new Set(Campaign.catalog().map(m => m.mapId)).size, 6);
  assert.ok(['reach', 'intel', 'sabotage', 'rescue', 'escort', 'defend', 'extract'].every(type => Campaign.catalog().some(m => m.objectives.some(o => o.type === type))));
});

test('every authored mission can complete its staged gameplay without a server', () => {
  for (const m of Campaign.catalog()) {
    const run = finishMission(m.id);
    assert.equal(run.status, 'complete', m.id); assert.equal(run.completed.length, m.objectives.length);
    assert.equal(Campaign.currentObjective(run), null);
    assert.ok(run.elapsed > 5); assert.ok(run.checkpointRevision > 0);
  }
});

test('objective interaction requires proximity, matching floor and an unobstructed use ray', () => {
  const run = Campaign.createRun('mice_behind_enemy_lines'); finishStage(run);
  assert.equal(Campaign.currentObjective(run).type, 'sabotage');
  for (let i = 0; i < 50; i++) Campaign.step(run, .1, { player: { ...player(), x: 30 }, objectivePosition: position, interact: true });
  assert.equal(run.progress, 0);
  for (let i = 0; i < 50; i++) Campaign.step(run, .1, { player: { ...player(), y: 9 }, objectivePosition: position, interact: true });
  assert.equal(run.progress, 0);
  for (let i = 0; i < 50; i++) Campaign.step(run, .1, { player: player(), objectivePosition: position, interact: true, canInteract: false });
  assert.equal(run.index, 1); assert.equal(run.progress, 0);
  finishStage(run); assert.equal(run.index, 2); assert.equal(run.alarm.enabled, false);
});

test('hold actions lose partial progress if interrupted while defense remains earned', () => {
  const run = Campaign.createRun('mice_operation_floodgate');
  Campaign.step(run, .25, { player: player(), objectivePosition: position, interact: true }); assert.equal(run.progress, .25);
  Campaign.step(run, .25, { player: player(), objectivePosition: position, interact: false }); assert.equal(run.progress, 0);
  finishStage(run); finishStage(run);
  Campaign.step(run, .25, { player: player(), objectivePosition: position }); assert.equal(run.progress, .25);
  Campaign.step(run, .25, { player: player(), objectivePosition: position, enemyCount: 1 }); assert.equal(run.progress, .25);
});

test('rescue creates an escort requirement and living prisoners must reach both destinations', () => {
  const run = Campaign.createRun('mice_whiskerhold_rescue'); finishStage(run); const result = finishStage(run);
  assert.equal(run.escort.rescued, true); assert.ok(result.events.some(e => e.type === 'rescue'));
  const index = run.index;
  for (let i = 0; i < 50; i++) Campaign.step(run, .1, { player: player(), objectivePosition: position });
  assert.equal(run.index, index, 'the player cannot substitute for a prisoner');
  Campaign.step(run, .1, { player: player(), objectivePosition: position, escort: { ...player(), x: 40 } }); assert.equal(run.index, index);
  finishStage(run); assert.equal(run.escort.delivered, true);
  Campaign.step(run, .1, { player: player(), objectivePosition: position, escort: { ...player(), dead: true } }); assert.equal(run.status, 'failed');
});

test('defend objectives dispatch counterattacks and cannot advance while contested', () => {
  const run = Campaign.createRun('cats_foundry_offensive'); finishStage(run); const result = finishStage(run);
  assert.equal(Campaign.currentObjective(run).type, 'defend'); assert.ok(result.events.some(e => e.type === 'counterattack'));
  assert.ok(Campaign.consumeReinforcements(run) >= 6); assert.equal(Campaign.consumeReinforcements(run), 0);
  for (let i = 0; i < 500; i++) Campaign.step(run, .1, { player: player(), objectivePosition: position, enemyCount: 2 });
  assert.equal(run.index, 2); assert.equal(run.progress, 0);
  finishStage(run); assert.equal(run.index, 3);
});

test('gunfire alarm stores only uncertain evidence and escalates through bounded reinforcements', () => {
  const run = Campaign.createRun('cats_night_market_hunt'), sound = { x: 12, y: 1.86, z: 20, uncertainty: 3.5, kind: 'gunfire' };
  assert.equal(Campaign.raiseAlarm(run, sound), true); sound.x = 999;
  assert.equal(run.alarm.lastKnown.x, 12); assert.equal(run.alarm.lastKnown.uncertainty, 3.5);
  for (let i = 0; i < 400; i++) Campaign.step(run, .1, { detected: true });
  assert.equal(run.alarm.level, 3); assert.ok(run.reinforcementsPending > 0 && run.reinforcementsPending <= 12);
  const at = run.alarm.lastKnown; assert.equal(at.x, 12, 'subsequent alerts without position cannot track the intruder');
  assert.ok(Campaign.consumeReinforcements(run) > 0); assert.equal(Campaign.consumeReinforcements(run), 0);
});

test('breaking contact reduces alarm and disabling the relay stops additional alarm waves', () => {
  const run = Campaign.createRun('mice_broken_convoy'); Campaign.raiseAlarm(run, position);
  for (let i = 0; i < 260; i++) Campaign.step(run, .1, {});
  assert.equal(run.alarm.level, 0);
  Campaign.raiseAlarm(run, position); Campaign.disableAlarm(run);
  for (let i = 0; i < 500; i++) Campaign.step(run, .1, { detected: true });
  assert.equal(run.alarm.level, 0); assert.equal(run.reinforcementsPending, 0);
});

test('optional relay sabotage is physical, contextual and records a cosmetic medal condition', () => {
  const run = Campaign.createRun('mice_broken_convoy');
  for (let i = 0; i < 40; i++) Campaign.step(run, .1, { player: player(), anchors: { security: { ...position, x: 15 } }, optionalInteract: true });
  assert.equal(run.alarm.enabled, true);
  for (let i = 0; i < 40; i++) Campaign.step(run, .1, { player: player(), anchors: { security: position }, optionalInteract: true });
  assert.equal(run.alarm.enabled, false); assert.deepEqual(run.optionalDone, ['silent_relay']);
});

test('stage checkpoints restore objectives, escort and player loadout without reviving stale perception', () => {
  const run = Campaign.createRun('mice_whiskerhold_rescue'); finishStage(run); finishStage(run);
  const saved = Campaign.checkpoint(run, { ...player(), hp: 20 }); Campaign.raiseAlarm(run, position);
  run.status = 'failed'; const resumed = Campaign.restoreCheckpoint(run, saved);
  assert.equal(resumed.index, 2); assert.equal(resumed.status, 'active'); assert.equal(resumed.escort.rescued, true);
  assert.equal(resumed.player.weapon, 'burrower'); assert.equal(resumed.player.hp, 35); assert.equal(resumed.alarm.level, 0);
  assert.equal(resumed.checkpointData.checkpointData, null, 'checkpoint serialization stays bounded');
  assert.equal(Campaign.restoreCheckpoint(run, { ...saved, index: 999 }), null);
  assert.equal(Campaign.restoreCheckpoint(run, { ...saved, missionId: 'other' }), null);
});

test('campaign progression unlocks the next mission for its faction and rewards no combat stats', () => {
  let progress = Campaign.freshProgress();
  assert.equal(Campaign.isUnlocked(progress, 'mice_broken_convoy'), true);
  assert.equal(Campaign.isUnlocked(progress, 'cats_silent_outpost'), true);
  assert.equal(Campaign.isUnlocked(progress, 'mice_behind_enemy_lines'), false);
  const run = finishMission('mice_broken_convoy'); progress = Campaign.completeProgress(progress, run);
  assert.equal(Campaign.isUnlocked(progress, 'mice_behind_enemy_lines'), true);
  assert.equal(Campaign.isUnlocked(progress, 'cats_night_market_hunt'), false);
  assert.deepEqual(progress.rewards, ['resistance_convoy_patch']); assert.equal(progress.completed[run.missionId].stars, 2);
  assert.ok(!('damage' in progress) && !('hp' in progress));
  assert.equal(Campaign.completeProgress(progress, Campaign.createRun('cats_silent_outpost')).rewards.length, 1);
});

test('saved campaign checkpoints survive existing local backup and profile merging', async () => {
  const data = new Map(), storage = { getItem: key => data.get(key) || null, setItem: (key, value) => data.set(key, value) };
  const store = Progress.createStore({ storage, now: () => 1000 }); store.beginRun('breach_ring'); store.clearWave(10);
  const run = Campaign.createRun('mice_broken_convoy'); finishStage(run);
  store.saveCampaign(Campaign.saveCheckpointProgress(store.getCampaign(), run, player()));
  const restored = Progress.createStore({ storage }); assert.equal(restored.getState().highestWave, 10);
  assert.equal(restored.getCampaign().checkpoints[run.missionId].index, 1);
  data.set(Progress.KEY, 'bad JSON'); const backup = Progress.createStore({ storage }); assert.equal(backup.getCampaign().checkpoints[run.missionId].index, 1);
  const cats = Campaign.completeProgress(Campaign.freshProgress(), finishMission('cats_silent_outpost')); cats.updatedAt = 900;
  store.mergeRemote({ campaign: cats }); assert.ok(store.getCampaign().completed.cats_silent_outpost);
  assert.equal(store.getState().highestWave, 10); assert.ok(store.getCampaign().checkpoints.mice_broken_convoy);
  let synced; await store.attachProfileAdapter({ read: async () => null, write: async state => { synced = state; } });
  assert.ok(synced.campaign.completed.cats_silent_outpost);
});

test('malformed saves cannot inject missions, reward IDs or unbounded checkpoints', () => {
  const value = Campaign.sanitizeProgress({ completed: { fake: { stars: 100 }, mice_broken_convoy: { stars: 100, bestTime: -5, completions: 1e12 } }, rewards: ['damage_cheat'], checkpoints: { mice_broken_convoy: { missionId: 'mice_broken_convoy', status: 'active', index: -1 } } });
  assert.equal(value.completed.fake, undefined); assert.equal(value.completed.mice_broken_convoy.stars, 3);
  assert.equal(value.completed.mice_broken_convoy.bestTime, 0); assert.equal(value.completed.mice_broken_convoy.completions, 1e6);
  assert.deepEqual(value.rewards, ['resistance_convoy_patch']); assert.deepEqual(value.checkpoints, {});
});

test('campaign timing uses simulation frames and reports each transition once', () => {
  const run = Campaign.createRun('mice_broken_convoy');
  const start = Campaign.step(run, 1000, { player: player(), objectivePosition: position });
  assert.equal(run.elapsed, .25); assert.equal(start.advanced, true);
  assert.ok(start.events.some(e => e.type === 'objective-complete'));
  assert.deepEqual(Campaign.step(run, .1, {}).events, []);
  for (let i = 0; i < 100; i++) { run.elapsed += 1; Campaign.raiseAlarm(run, position); }
  assert.ok(run.events.length <= 24); assert.deepEqual(Campaign.step(run, .1, {}).events, []);
});
