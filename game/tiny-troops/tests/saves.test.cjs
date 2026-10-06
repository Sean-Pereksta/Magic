const test = require('node:test');
const assert = require('node:assert/strict');
const P = require('../saves.js');
function memory() {
  const data = new Map();
  return { data, get length() { return data.size; }, key: i => [...data.keys()][i], getItem: k => data.get(k) || null, setItem: (k, v) => data.set(k, v) };
}
const payload = (clean = 'army', time = 10, extra = {}) => ({ username: clean, cleanUsername: clean, password: 'code', updatedAtMs: time, sequence: time, state: { round: time, coins: 20, squad: [{ n: 'Archer', e: '🏹' }], tt: { id: clean + '-run' } }, ...extra });

test('existing save keys are discovered without a separate index or exposing codes', () => {
  const storage = memory(), store = P.createStore(storage);
  storage.setItem(P.PREFIX + 'old', JSON.stringify(payload('old', 2)));
  store.write('new', payload('new', 10)); storage.setItem('unrelated', 'not a game save');
  const entries = store.list(); assert.deepEqual(entries.map(e => e.clean), ['new', 'old']);
  assert.equal(entries[0].army, '🏹'); assert.equal(entries[0].wave, 10); assert.equal(entries[0].size, 1);
  assert.equal(entries.some(e => 'password' in e || 'state' in e), false);
});
test('a broken primary recovers the last readable checkpoint and keeps the original file', () => {
  const storage = memory(), store = P.createStore(storage);
  store.write('army', payload('army', 5)); store.write('army', payload('army', 8));
  storage.setItem(P.PREFIX + 'army', '{broken');
  assert.equal(store.read('army').data.state.round, 5); assert.equal(store.read('army').recovered, true);
  assert.equal(store.list()[0].recovered, true); assert.equal(storage.getItem(P.PREFIX + 'army'), '{broken');
  store.write('army', payload('army', 9)); assert.equal(JSON.parse(storage.getItem(P.BACKUP_PREFIX + 'army')).state.round, 5);
});
test('a backup is discoverable even if the primary file is missing', () => {
  const storage = memory(), store = P.createStore(storage); storage.setItem(P.BACKUP_PREFIX + 'army', JSON.stringify(payload()));
  assert.equal(store.list()[0].readable, true); assert.equal(store.list()[0].recovered, true);
});
test('unreadable files remain visible and storage failures never report success', () => {
  const storage = memory(), store = P.createStore(storage); storage.setItem(P.PREFIX + 'broken', 'null');
  assert.equal(store.list()[0].readable, false);
  store.write('army', payload()); const before = storage.getItem(P.PREFIX + 'army');
  storage.setItem = () => { throw new Error('Quota exceeded'); };
  assert.equal(store.write('army', payload('army', 20)).ok, false); assert.equal(storage.getItem(P.PREFIX + 'army'), before);
});
test('blocked browser storage is safe to read and reports a failed write', () => {
  const store = P.createStore({ get length() { throw new Error('blocked'); }, getItem() { throw new Error('blocked'); }, setItem() { throw new Error('blocked'); } });
  assert.deepEqual(store.list(), []); assert.equal(store.available(), false); assert.equal(store.read('army').data, null); assert.equal(store.write('army', payload()).ok, false);
});
test('malformed checkpoints cannot replace readable device or cloud saves', async () => {
  const store = P.createStore(memory()); store.write('army', payload()); let cloudWrites = 0;
  const writer = P.createWriter({ store, writeCloud: () => { cloudWrites++; } });
  const result = await writer.save(payload('army', 20, { state: null }), true);
  assert.equal(result.local, false); assert.equal(result.cloud, false); assert.equal(cloudWrites, 0); assert.equal(store.read('army').data.state.round, 10);
});
test('load chooses the newest valid matching-code checkpoint and can use a local fallback', () => {
  const device = { data: payload('army', 20), source: 'device' }, cloud = { data: payload('army', 30), source: 'cloud' };
  assert.equal(P.choose([device, cloud], 'code').source, 'cloud'); assert.equal(P.choose([device], 'code').source, 'device');
  cloud.data.password = 'other'; assert.equal(P.choose([device, cloud], 'code').source, 'device');
  assert.throws(() => P.choose([device], 'wrong'), /did not match/);
  cloud.data.password = 'code'; cloud.data.state.corrupt = true;
  assert.equal(P.choose([device, cloud], 'code', state => { if (state.corrupt) throw new Error('corrupt'); return state; }).source, 'device');
});
test('queued cloud writes keep their own profile and detached checkpoint', async () => {
  const store = P.createStore(memory()), writes = []; let release;
  const gate = new Promise(resolve => { release = resolve; });
  const writer = P.createWriter({ store, writeCloud: async data => { await gate; writes.push(data); } });
  const original = payload('first', 10), first = writer.save(original, true);
  original.cleanUsername = 'second'; original.state.round = 999;
  const second = writer.save(payload('second', 20), true); release(); await Promise.all([first, second]);
  assert.deepEqual(writes.map(w => [w.cleanUsername, w.state.round]), [['first', 10], ['second', 20]]);
});
test('rapid saves coalesce pending cloud writes while saving every checkpoint locally', async () => {
  const store = P.createStore(memory()), writes = [], messages = []; let release;
  const gate = new Promise(resolve => { release = resolve; });
  const writer = P.createWriter({ store, notify: s => messages.push(s), writeCloud: async data => { await gate; writes.push(data); } });
  const first = writer.save(payload('army', 1), true), second = writer.save(payload('army', 2), true), third = writer.save(payload('army', 3), true);
  assert.equal(store.read('army').data.state.round, 3); assert.ok(writer.pendingCount() <= 2);
  assert.equal((await second).superseded, true); release(); await Promise.all([first, third]);
  assert.deepEqual(writes.map(w => w.state.round), [1, 3]); assert.equal(messages.filter(m => m.phase === 'saved').length, 1);
});
test('a cloud failure preserves local progress and accurately reports its source', async () => {
  const store = P.createStore(memory()), messages = [];
  const writer = P.createWriter({ store, notify: s => messages.push(s), writeCloud: async () => { throw new Error('offline'); } });
  const result = await writer.save(payload(), true); assert.equal(result.local, true); assert.equal(result.cloud, false);
  assert.equal(messages.at(-1).local, true); assert.equal(messages.at(-1).phase, 'failed'); assert.equal(store.read('army').data.state.round, 10);
});
test('cloud conflicts retain their actionable error type', async () => {
  const messages = [], store = P.createStore(memory());
  const writer = P.createWriter({ store, notify: status => messages.push(status), writeCloud: () => { throw Object.assign(new Error('Other cloud run'), { code: 'save/name-conflict' }); } });
  await writer.save(payload(), true);
  assert.equal(messages.at(-1).code, 'save/name-conflict'); assert.equal(store.read('army').data.state.round, 10);
  const numeric = P.createWriter({ store, notify: status => messages.push(status), writeCloud: () => { throw Object.assign(new Error('Cloud failure'), { code: 22 }); } });
  await numeric.save(payload(), true); assert.equal(messages.at(-1).code, '22');
});
test('loading invalidates queued writes and gives running transactions a cancellation guard', async () => {
  const store = P.createStore(memory()), writes = []; let release, began;
  const gate = new Promise(resolve => { release = resolve; }), started = new Promise(resolve => { began = resolve; });
  const writer = P.createWriter({ store, writeCloud: async (data, isCurrent) => { began(); await gate; if (!isCurrent()) throw new Error('superseded'); writes.push(data); } });
  const first = writer.save(payload('army', 10), true), queued = writer.save(payload('army', 20), true);
  await started; writer.invalidate('army'); release();
  assert.equal((await queued).superseded, true); assert.equal((await first).cloud, false); assert.deepEqual(writes, []);
  assert.equal(store.read('army').data.state.round, 20); assert.equal((await writer.save(payload('army', 30), true)).cloud, true);
});
test('cloud-only saves can succeed when browser storage is full', async () => {
  const store = P.createStore({ getItem: () => null, setItem: () => { throw new Error('full'); } });
  const writer = P.createWriter({ store, writeCloud: async () => {} });
  assert.deepEqual(await writer.save(payload(), true), { local: false, cloud: true });
});
test('a stalled cloud request times out and does not block the next profile', async () => {
  const writer = P.createWriter({ store: P.createStore(memory()), timeoutMs: 15, writeCloud: data => data.cleanUsername === 'stalled' ? new Promise(() => {}) : Promise.resolve() });
  const stalled = writer.save(payload('stalled'), true), next = writer.save(payload('next'), true);
  assert.match((await stalled).error, /timed out/); assert.equal((await next).cloud, true);
});
test('new runs retain existing files and generate a distinct name within the input limit', () => {
  const clean = name => name.toLowerCase().replace(/\s/g, '_');
  assert.equal(P.unusedName('Army', [{ clean: 'army' }, { clean: 'army_2' }], clean), 'Army 3');
  const long = 'abcdefghijklmnopqrstuvwx1234', result = P.unusedName(long, [{ clean: long }], clean);
  assert.ok(result.length <= 28); assert.notEqual(clean(result), long);
});
