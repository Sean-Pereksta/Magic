import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from '../network.mjs';
import { answersFor, phaseKey, votersFor } from '../state.mjs';

const root = fileURLToPath(new URL('../../..', import.meta.url));
const require = createRequire(process.env.QUIP_DEPENDENCIES ? path.resolve(process.env.QUIP_DEPENDENCIES, 'package.json') : path.join(root, 'package.json'));
const sdk = require('firebase/firestore');
const { initializeTestEnvironment, assertFails } = require('@firebase/rules-unit-testing');
const [host = '127.0.0.1', rawPort = '8189'] = (process.env.FIRESTORE_EMULATOR_HOST || '').split(':');
let env;
before(async () => {
  env = await initializeTestEnvironment({ projectId: 'demo-quip-clash', firestore: { host, port: Number(rawPort), rules: fs.readFileSync(path.join(root, 'firestore.rules'), 'utf8') } });
});
after(async () => { await env?.cleanup(); });
const identity = index => ({ uid: `quip-player-${index}`, username: `QuipTest${index}`, displayName: `Player ${index}` });
const make = index => { const player = identity(index), db = env.authenticatedContext(player.uid).firestore(); return { ...player, db, client: createClient({ db, sdk, identity: player }) }; };
const read = (player, code) => sdk.getDocFromServer(sdk.doc(player.db, 'lobbies', code)).then(snap => snap.data());
async function expire(code) {
  await env.withSecurityRulesDisabled(async context => {
    await sdk.updateDoc(sdk.doc(context.firestore(), 'lobbies', code), { phaseStartedAt: sdk.Timestamp.fromMillis(Date.now() - 120000) });
  });
}

test('existing rules allow a complete five-player game, concurrent votes, reconnect, and rematch', { timeout: 120000 }, async () => {
  const players = Array.from({ length: 5 }, (_, i) => make(i));
  const first = players[0], code = await first.client.create({ name: 'Emulator party' });
  for (const p of players.slice(1)) await p.client.join(code);
  await assert.rejects(() => players[1].client.start(code), /Only the host/);
  await first.client.start(code);
  let room = await read(first, code), totalPools = 0;
  assert.equal(room.phase, 'writing'); assert.equal(room.roster.length, 5);
  for (let round = 1; round <= 3; round++) {
    room = await read(first, code);
    assert.equal(room.round, round);
    const writingKey = phaseKey(room);
    await Promise.all(players.map(p => p.client.save(code, Object.fromEntries(room.questions.filter(q => q.authors.includes(p.uid)).map(q => [q.id, `${p.displayName}: ${'A delightfully ridiculous answer. '.repeat(20)}`])), true, writingKey)));
    await players[4].client.advance(code, writingKey); // Host need not remain connected.
    room = await read(first, code); assert.equal(room.phase, 'voting');
    for (let index = 0; index < room.questions.length; index++) {
      room = await read(first, code);
      const key = phaseKey(room), options = answersFor(room), eligible = votersFor(room);
      await assert.rejects(() => first.client.vote(code, 'not-an-answer', key));
      await Promise.all(eligible.map(uid => {
        const p = players.find(p => p.uid === uid), answer = options.find(a => a.uid !== uid);
        return p.client.vote(code, answer.id, key);
      }));
      // Many clients see all votes at once. Only one ledger entry may be awarded.
      await Promise.all(players.map(p => p.client.advance(code, key)));
      room = await read(first, code); assert.equal(room.phase, 'reveal');
      totalPools += round * 1000;
      assert.equal(room.roster.reduce((n, p) => n + p.score, 0), totalPools);
      const ledger = JSON.stringify(room.receipts), scores = room.roster.map(p => p.score);
      await first.client.advance(code, key);
      const unchanged = await read(first, code); assert.equal(JSON.stringify(unchanged.receipts), ledger); assert.deepEqual(unchanged.roster.map(p => p.score), scores);
      await first.client.advance(code, phaseKey(room), true);
      room = await read(first, code);
    }
    assert.equal(room.phase, round === 3 ? 'finished' : 'scores');
    if (round < 3) { await expire(code); await players[3].client.advance(code, phaseKey(room)); }
  }
  const reconnected = make(2); await reconnected.client.join(code);
  const final = await read(reconnected, code); assert.equal(final.roster.length, 5); assert.equal(final.roster.reduce((n, p) => n + p.score, 0), totalPools);
  await first.client.replay(code);
  room = await read(first, code); assert.equal(room.phase, 'waiting'); assert.deepEqual(room.receipts, {}); assert.ok(room.roster.every(p => p.score === 0));
  assert.notEqual(room.matchId, final.matchId);
});

test('timed-out writing preserves partial answers; timed-out zero votes award zero', { timeout: 60000 }, async () => {
  const players = Array.from({ length: 4 }, (_, i) => make(20 + i));
  const p = players[0], code = await p.client.create();
  for (const other of players.slice(1)) await other.client.join(code);
  await p.client.start(code);
  let room = await read(p, code), key = phaseKey(room);
  const ownQuestion = room.questions.find(q => q.authors.includes(p.uid));
  await p.client.save(code, { [ownQuestion.id]: 'Saved even if my phone disconnects.' }, false, key);
  await p.client.advance(code, key); assert.equal((await read(p, code)).phase, 'writing');
  await expire(code); await players[1].client.advance(code, key);
  room = await read(p, code); assert.equal(room.responses[p.uid][ownQuestion.id], 'Saved even if my phone disconnects.');
  await assert.rejects(() => p.client.save(code, { [ownQuestion.id]: 'Too late' }, true), /moved on/);
  await expire(code); await players[1].client.advance(code, phaseKey(room));
  room = await read(p, code); assert.equal(room.phase, 'reveal'); assert.ok(room.roster.every(p => p.score === 0));
});

test('existing rules reject unauthenticated room access', async () => {
  const db = env.unauthenticatedContext().firestore();
  await assertFails(sdk.setDoc(sdk.doc(db, 'lobbies', 'UNAUTH'), { gameType: 'quipclash' }));
  await assertFails(sdk.getDoc(sdk.doc(db, 'lobbies', 'UNAUTH')));
});

test('a failed clock probe preserves the successfully committed room code for recovery', async () => {
  const p = make(40);
  const client = createClient({ db: p.db, identity: identity(40), sdk: { ...sdk, updateDoc: async () => { throw new Error('Probe interrupted'); } } });
  let committedCode;
  await assert.rejects(() => client.create(), error => { committedCode = error.roomCode; return error.message === 'Probe interrupted' && /^[A-Z2-9]{6}$/.test(committedCode); });
  assert.equal((await read(p, committedCode)).hostUid, p.uid);
  await p.client.join(committedCode); assert.equal(p.client.clock.synced, true);
});
