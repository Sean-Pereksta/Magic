import test from 'node:test';
import assert from 'node:assert/strict';
import { createDaily, seededRng, utcToday, isDailyDate, scoreDaily, normalizeDaily, recordDailyResult } from '../game/matchwars/daily.mjs?v=4';
import { CLASSES } from '../game/matchwars/classes.mjs?v=4';
import { masteryRank, specializationFor } from '../game/matchwars/mastery.mjs?v=4';
import { Board } from '../game/matchwars/board.mjs?v=4';
import { Battle } from '../game/matchwars/combat.mjs?v=4';

const TODAY = '2026-11-04';
const dayBefore = (date, days) => new Date(new Date(`${date}T00:00:00Z`).getTime() - days * 86400000).toISOString().slice(0, 10);
const win = { won: true, time: 60, kills: 80, matches: 80, bestChain: 7, playerHp: 100, playerMax: 100, events: 2 };

test('share dates are strict UTC calendar dates within thirty days including today', () => {
  assert.equal(utcToday(new Date('2026-11-05T00:10:00+05:00')), TODAY);
  assert.ok(createDaily(TODAY, TODAY));
  assert.ok(createDaily(dayBefore(TODAY, 29), TODAY));
  for (const invalid of [dayBefore(TODAY, 30), '2026-11-05', '2026-02-30', '2026-11-4',
    '2026-11-04T00:00:00Z', '', null, 42, '__proto__']) assert.equal(createDaily(invalid, TODAY), null);
  assert.ok(isDailyDate('2028-02-29', '2028-03-01'));
  assert.equal(isDailyDate('2027-02-29', '2027-03-01'), false);
  assert.equal(createDaily(TODAY, 'broken'), null);
});

test('daily configuration is identical for every player and has valid fixed class training', () => {
  assert.deepEqual(createDaily(TODAY, TODAY), createDaily(TODAY, TODAY));
  const armies = new Set(), stages = new Set(), classes = new Set();
  for (let offset = 0; offset < 30; offset++) {
    const config = createDaily(dayBefore(TODAY, offset), TODAY);
    armies.add(config.armyName); stages.add(config.stage);
    assert.ok([12, 24, 36].includes(config.stage));
    assert.ok(config.events);
    assert.ok(config.boardSeed !== config.combatSeed);
    assert.ok(config.medalTargets.gold > config.medalTargets.silver);
    assert.ok(config.medalTargets.silver > config.medalTargets.bronze);
    for (const [element, id] of Object.entries(config.loadout)) {
      assert.equal(CLASSES[id].element, element);
      classes.add(id);
    }
    for (const [id, training] of Object.entries(config.mastery)) {
      assert.equal(masteryRank(training.xp), 3);
      assert.ok(specializationFor(id, training.specialization));
    }
    assert.ok(Object.values(config.upgrades).every(rank => rank === 2));
    assert.throws(() => { config.loadout.ember = 'changed'; }, TypeError);
  }
  assert.equal(stages.size, 3);
  assert.equal(armies.size, 6);
  assert.equal(classes.size, 18);
});

test('seeded streams are reproducible, bounded and isolated from one another', () => {
  const rng = seededRng('daily-test');
  assert.deepEqual(Array.from({ length: 5 }, () => rng()), [
    0.3840493082534522, 0.959202874219045, 0.1548052600119263, 0.4523279401473701, 0.6120717010926455,
  ], 'published daily seed behavior stays stable');
  const config = createDaily(TODAY, TODAY), board = seededRng(config.boardSeed);
  const combat = seededRng(config.combatSeed), replayCombat = seededRng(config.combatSeed);
  for (let index = 0; index < 1000; index++) {
    const value = board(); assert.ok(value >= 0 && value < 1);
  }
  assert.deepEqual(Array.from({ length: 20 }, () => combat()), Array.from({ length: 20 }, () => replayCombat()));
});

test('replays build the same event-enabled board and battle without using campaign training', () => {
  const config = createDaily(TODAY, TODAY);
  const buildBoard = () => new Board({ rng: seededRng(config.boardSeed), events: config.events, stage: config.stage });
  const first = buildBoard(), replay = buildBoard();
  assert.deepEqual(first.snapshot(), replay.snapshot());
  const hint = first.hint();
  assert.deepEqual(first.trySwap(hint.from, hint.to), replay.trySwap(hint.from, hint.to));
  const buildBattle = () => new Battle({ stage: config.stage, loadout: config.loadout,
    upgrades: config.upgrades, mastery: config.mastery, rng: seededRng(config.combatSeed) });
  const battle = buildBattle(), replayBattle = buildBattle();
  assert.deepEqual(battle.forecast(), replayBattle.forecast());
  assert.deepEqual(battle.loadout, config.loadout);
  assert.deepEqual(battle.upgrades, config.upgrades);
  battle.summon('sun'); replayBattle.summon('sun');
  for (let index = 0; index < 200; index++) { battle.update(.05); replayBattle.update(.05); }
  assert.deepEqual(battle.units, replayBattle.units);
});

test('score rewards survival, speed, cascades and rare events; losses never earn medals', () => {
  const config = createDaily(TODAY, TODAY);
  const basic = scoreDaily({ won: true, time: 180, playerHp: 0, playerMax: 100 }, config);
  assert.equal(basic.score, config.medalTargets.bronze);
  assert.equal(basic.medal, 'bronze');
  assert.ok(scoreDaily({ ...win, time: 30 }, config).score > scoreDaily({ ...win, time: 90 }, config).score);
  assert.ok(scoreDaily({ ...win, playerHp: 100 }, config).score > scoreDaily({ ...win, playerHp: 40 }, config).score);
  assert.ok(scoreDaily({ ...win, bestChain: 8 }, config).score > scoreDaily({ ...win, bestChain: 2 }, config).score);
  assert.equal(scoreDaily({ ...win, events: 3 }, config).score - scoreDaily({ ...win, events: 2 }, config).score, 150);
  assert.equal(scoreDaily(win, config).medal, 'gold');
  const loss = scoreDaily({ ...win, status: 'lost', kills: 999999 }, config);
  assert.equal(loss.won, false);
  assert.equal(loss.medal, 'none');
  assert.ok(loss.score < config.medalTargets.bronze);
  assert.deepEqual(scoreDaily({ ...win, status: 'playing' }, config), { score: 0, medal: 'none', won: false });
  const malformed = scoreDaily({ won: true, kills: Infinity, matches: -5, bestChain: NaN,
    playerHp: NaN, playerMax: 0, time: Infinity }, config);
  assert.ok(Number.isFinite(malformed.score) && malformed.score >= 0);
});

test('first win today awards 200 once; replay preserves the personal best without farming', () => {
  const config = createDaily(TODAY, TODAY), original = { records: {} };
  const first = recordDailyResult(original, config, win, TODAY);
  assert.equal(first.reward, 200);
  assert.equal(first.improved, true);
  assert.deepEqual(original, { records: {} }, 'record updates are pure');
  assert.equal(first.daily.records[TODAY].claimed, true);
  assert.equal(first.daily.records[TODAY].attempts, 1);
  const again = recordDailyResult(first.daily, config, { won: true, time: 180 }, TODAY);
  assert.equal(again.reward, 0);
  assert.equal(again.improved, false);
  assert.equal(again.daily.records[TODAY].score, first.score);
  assert.equal(again.daily.records[TODAY].medal, 'gold');
  assert.equal(again.daily.records[TODAY].attempts, 2);
  const afterLoss = recordDailyResult(again.daily, config, { won: false, kills: 8 }, TODAY);
  assert.equal(afterLoss.reward, 0);
  assert.equal(afterLoss.daily.records[TODAY].won, true);
  assert.equal(afterLoss.daily.records[TODAY].claimed, true);
});

test('past daily practice and a run ending after UTC midnight give no daily gold', () => {
  const yesterday = dayBefore(TODAY, 1), config = createDaily(yesterday, TODAY);
  const practice = recordDailyResult({ records: {} }, config, win, TODAY);
  assert.equal(practice.reward, 0);
  assert.equal(practice.daily.records[yesterday].claimed, false);
  const todayRun = recordDailyResult(practice.daily, createDaily(TODAY, TODAY), win, TODAY);
  assert.equal(todayRun.reward, 200);
  const overnight = recordDailyResult({ records: {} }, config, win, TODAY);
  assert.equal(overnight.reward, 0);
  assert.equal(overnight.daily.records[yesterday].won, true);
});

test('record normalization keeps thirty UTC records, recomputes medals and rejects bad data', () => {
  const records = Object.fromEntries(Array.from({ length: 60 }, (_, offset) => [dayBefore(TODAY, offset),
    { score: 300, medal: 'gold', won: false, claimed: true, attempts: 3 }]));
  records['2026-11-05'] = { score: 999999, won: true, claimed: true };
  records.broken = { score: 999999, won: true };
  const normalized = normalizeDaily({ records }, TODAY);
  assert.equal(Object.keys(normalized.records).length, 30);
  assert.equal(normalized.records[TODAY].medal, 'none');
  assert.equal(normalized.records[TODAY].claimed, false);
  assert.equal(normalized.records['2026-11-05'], undefined);
  assert.equal(normalized.records[dayBefore(TODAY, 30)], undefined);
  assert.deepEqual(normalizeDaily({ records: [] }, TODAY), { records: {} });
  assert.deepEqual(normalizeDaily({ records: { [TODAY]: { score: NaN, won: true } } }, TODAY), { records: {} });
});

test('invalid or unfinished results cannot create a record or claim a reward', () => {
  const config = createDaily(TODAY, TODAY), empty = { records: {} };
  for (const invalidConfig of [null, { ...config, date: '2026-11-05' }, { ...config, seed: 'forged' }]) {
    const result = recordDailyResult(empty, invalidConfig, win, TODAY);
    assert.equal(result.reward, 0); assert.deepEqual(result.daily, empty);
  }
  for (const unfinished of [null, {}, { status: 'playing', ...win }]) {
    const result = recordDailyResult(empty, config, unfinished, TODAY);
    assert.equal(result.reward, 0); assert.deepEqual(result.daily, empty);
  }
});
