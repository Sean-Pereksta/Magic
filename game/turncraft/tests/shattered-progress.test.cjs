const { test } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { pathToFileURL } = require('node:url');
const moduleReady = import(
  pathToFileURL(path.resolve(__dirname, '../shattered-front/progress.mjs'))
);
test('campaign stars never regress and one star opens the next mission', async () => {
  const { recordResult, missionUnlocked } = await moduleReady;
  let progress = {
    campaign: { completedMissions: {} },
    survival: { recordsByMap: {} },
    achievements: {}
  };
  const catalog = { missions: [{ id: 'one' }, { id: 'two' }] };
  assert.equal(missionUnlocked(catalog.missions[1], catalog, progress), false);
  progress = recordResult(progress, {
    mode: 'campaign',
    missionId: 'one',
    won: true,
    stars: 3
  });
  progress = recordResult(progress, {
    mode: 'campaign',
    missionId: 'one',
    won: true,
    stars: 1
  });
  assert.equal(progress.campaign.completedMissions.one.stars, 3);
  assert.equal(missionUnlocked(catalog.missions[1], catalog, progress), true);
});
test('survival records stay separate by arena and difficulty; no permanent power reward', async () => {
  const { recordResult } = await moduleReady;
  const result = {
    mode: 'survival',
    arenaId: 'amber',
    difficulty: 'easy',
    wave: 20
  };
  let progress = recordResult(null, result);
  progress = recordResult(progress, { ...result, wave: 5 });
  progress = recordResult(progress, {
    ...result,
    difficulty: 'hard',
    wave: 10
  });
  assert.equal(progress.survival.recordsByMap['amber:easy'].wave, 20);
  assert.equal(progress.survival.recordsByMap['amber:hard'].wave, 10);
  assert.equal(progress.achievements['survival-20'], true);
  assert.equal(progress.mmr, undefined);
  assert.equal(progress.statBonus, undefined);
});
