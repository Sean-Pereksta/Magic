import test from 'node:test';
import assert from 'node:assert/strict';
import {CAMPAIGN, readProgress, completeLevel, createCampaignBattle} from './campaign.mjs';
import {createGame, legalActions} from './engine.mjs';

test('campaign is a five-mission faction tour with progressively broader battles', () => {
  assert.equal(CAMPAIGN.length, 5);
  assert.equal(new Set(CAMPAIGN.map(level => level.id)).size, 5);
  assert.deepEqual(CAMPAIGN.slice(0, 2).map(level => level.difficulty), ['story', 'easy']);
  assert.equal(CAMPAIGN.at(-1).difficulty, 'hard');
  assert.deepEqual(new Set(CAMPAIGN.map(level => level.playerFaction)), new Set(['bloom', 'chaos', 'choir']));
  assert.deepEqual(new Set(CAMPAIGN.map(level => level.mode)), new Set(['beacons', 'control', 'elimination']));
  for (const level of CAMPAIGN) {
    assert.ok([7, 9, 11].includes(level.width));
    assert.ok([5, 7, 9].includes(level.height));
    assert.ok(['sparse', 'balanced', 'wild'].includes(level.terrain));
    assert.ok(level.briefing.length > 50 && level.rewardText.length > 20);
    assert.ok(Object.isFrozen(level));
  }
  assert.ok(Object.isFrozen(CAMPAIGN));
});

test('absent, malformed, or hostile saved progress falls back to bounded data', () => {
  for (const value of [undefined, null, '', '{broken', 'null', '[]', '5', [], {unlocked: '5'}, {unlocked: -12}]) {
    assert.deepEqual(readProgress(value), {version: 1, unlocked: 1, completed: []});
  }
  const saved = {version: 99, unlocked: 999, completed: ['last-rift', 'missing', 'last-rift', null], html: '<script>'};
  assert.deepEqual(readProgress(JSON.stringify(saved)), {version: 1, unlocked: 5, completed: ['last-rift']});
  assert.deepEqual(readProgress({unlocked: 1, completed: ['last-rift']}), {version: 1, unlocked: 1, completed: []});
});

test('wins unlock one following mission; locked completion and replay cannot skip or regress', () => {
  let progress = readProgress(null);
  assert.deepEqual(completeLevel(progress, CAMPAIGN[4].id), progress);
  assert.deepEqual(completeLevel(progress, 'unknown'), progress);
  for (let index = 0; index < CAMPAIGN.length; index++) {
    const before = structuredClone(progress);
    progress = completeLevel(progress, CAMPAIGN[index].id);
    assert.deepEqual(before.completed, CAMPAIGN.slice(0, index).map(level => level.id));
    assert.equal(progress.unlocked, Math.min(5, index + 2));
    assert.deepEqual(progress.completed, CAMPAIGN.slice(0, index + 1).map(level => level.id));
    assert.deepEqual(completeLevel(progress, CAMPAIGN[0].id), progress);
  }
  assert.deepEqual(readProgress(JSON.stringify(progress)), progress);
});

test('saved completed missions restore missing counters and return independent arrays', () => {
  const saved = {completed: [CAMPAIGN[1].id, CAMPAIGN[0].id, CAMPAIGN[0].id]};
  const recovered = readProgress(saved);
  assert.equal(recovered.unlocked, 3);
  assert.deepEqual(recovered.completed, CAMPAIGN.slice(0, 2).map(level => level.id));
  recovered.completed.push('mutated');
  assert.equal(saved.completed.length, 3);
  assert.deepEqual(readProgress(null).completed, []);
});

test('battle options are deterministic, isolated, and reject unknown mission ids', () => {
  for (const level of CAMPAIGN) {
    const options = createCampaignBattle(level.id);
    assert.deepEqual(options, createCampaignBattle(level.id));
    assert.equal(options.mode, level.mode);
    assert.equal(options.goal, level.goal);
    assert.equal(options.duration, level.duration);
    assert.equal(options.title, undefined);
    assert.equal(options.briefing, undefined);
    options.width = 100;
    assert.equal(createCampaignBattle(level.id).width, level.width);
  }
  assert.throws(() => createCampaignBattle('__proto__'), RangeError);
  assert.throws(() => createCampaignBattle(), RangeError);
});

test('every mission starts a legal battle using its advertised board, mode, and rules', () => {
  for (const level of CAMPAIGN) {
    const state = createGame(createCampaignBattle(level.id));
    assert.equal(state.width, level.width, level.id);
    assert.equal(state.height, level.height, level.id);
    assert.equal(state.mode, level.mode, level.id);
    assert.equal(state.difficulty, level.difficulty, level.id);
    assert.equal(state.duration, level.duration, level.id);
    assert.equal(state.goal, level.mode === 'elimination' ? 0 : level.goal, level.id);
    assert.equal(state.units.filter(unit => unit.side === 'player').length, 5, level.id);
    assert.equal(state.winner, null, level.id);
    assert.ok(state.units.filter(unit => unit.side === 'player').some(unit => legalActions(state, unit.id).some(action => action.type === 'move')), level.id);
  }
});
