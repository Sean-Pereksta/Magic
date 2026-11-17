import test from 'node:test';
import assert from 'node:assert/strict';
import {createRankings} from '../rankings.mjs';
import {trashTacticsRank} from '../rank-tiers.mjs';
import {getRankedToken} from '../ranked-auth.mjs';

function fixture() {
  const rows = new Map([
    ['users/Alice', {displayName: 'Alice', password: 'legacy fixture', gameStats: {turncraft: {achievements: []}}}],
    ['users/Bob', {displayName: 'Bob'}],
    ['factionForgeAccounts/a', {verified: true, username: 'Alice'}],
    ['factionForgeAccounts/b', {verified: true, username: 'Bob'}],
    ['factionForgeNames/Alice', {verified: true, uid: 'a'}],
    ['factionForgeNames/Bob', {verified: true, uid: 'b'}]
  ]);
  const snap = path => ({id: path.split('/').at(-1), exists: rows.has(path), data: () => structuredClone(rows.get(path))});
  const db = {
    collection: collection => ({doc: name => ({path: `${collection}/${name}`, get: async () => snap(`${collection}/${name}`)})}),
    async runTransaction(run) {
      const pending = [];
      const result = await run({
        get: async ({path}) => snap(path),
        update({path}, field, data) {
          assert.equal(field, 'gameStats.trashTactics');
          pending.push(() => rows.set(path, {...rows.get(path), gameStats: {...rows.get(path).gameStats, trashTactics: data}}));
        },
        create({path}, data) { pending.push(() => rows.set(path, data)); }
      });
      pending.forEach(write => write());
      return result;
    }
  };
  const auth = {async verifyIdToken(token, checkRevoked) {
    assert.equal(checkRevoked, true);
    if (token === 'expired') throw Object.assign(new Error('expired'), {code: 'auth/id-token-expired'});
    return {uid: token};
  }};
  return {rows, rankings: createRankings({db, auth})};
}

test('ranked results atomically update only the existing profile ratings and settle once', async () => {
  const {rows, rankings} = fixture();
  const players = await Promise.all(['a', 'b'].map(rankings.identity));
  assert.deepEqual(players[0], {uid: 'a', username: 'Alice', displayName: 'Alice', rating: 1000, wins: 0, losses: 0, draws: 0, games: 0});
  const match = {id: 'match-a', players, winnerTeam: 0};
  const result = await rankings.recordResult(match);
  assert.deepEqual(result.players.map(player => player.rating), [1016, 984]);
  assert.deepEqual(await rankings.recordResult(match), result);
  await assert.rejects(rankings.recordResult({...match, winnerTeam: 1}), {code: 'RESULT_CONFLICT'});
  assert.deepEqual(rows.get('users/Alice').gameStats.turncraft, {achievements: []});
  assert.equal(rows.get('users/Alice').password, 'legacy fixture');
  assert.equal((await rankings.profile('Alice')).wins, 1);
  assert.equal((await rankings.profile('Bob')).losses, 1);
  assert.equal(trashTacticsRank(await rankings.profile('Alice')).title, 'Silver');
  assert.equal(trashTacticsRank(await rankings.profile('Bob')).title, 'Bronze');

  await rankings.recordResult({id: 'match-b', players, winnerTeam: null});
  const [alice, bob] = await Promise.all(['Alice', 'Bob'].map(rankings.profile));
  assert.equal(alice.rating + bob.rating, 2000);
  assert.deepEqual([alice.draws, bob.draws, alice.games, bob.games], [1, 1, 2, 2]);
});

test('ranked identity and result settlement require the verified account in both binding directions', async () => {
  const {rows, rankings} = fixture();
  await assert.rejects(rankings.identity('expired'), {code: 'AUTH_REQUIRED'});
  await assert.rejects(rankings.identity('guest'), {code: 'ACCOUNT_NOT_LINKED'});
  const players = await Promise.all(['a', 'b'].map(rankings.identity));
  rows.set('factionForgeNames/Alice', {uid: 'b', verified: true});
  await assert.rejects(rankings.identity('a'), {code: 'ACCOUNT_NOT_LINKED'});
  await assert.rejects(rankings.recordResult({id: 'm', players, winnerTeam: 1}), {code: 'ACCOUNT_NOT_LINKED'});
  assert.equal(rows.has('trashTacticsRankedResults/m'), false);
  assert.equal((await rankings.profile('Bob')).games, 0);
});

test('ranked tokens cannot be supplied to a community tunnel or a spoofed service origin', async () => {
  for (const url of ['https://a.trycloudflare.com', 'https://trash-tactics.catnmice.com.example.org', 'http://127.0.0.1:4173']) {
    await assert.rejects(getRankedToken(url), /official Catnmice server/);
  }
});
