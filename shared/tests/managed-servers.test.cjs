'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const script = fs.readFileSync(path.join(__dirname, '../managed-servers.js'), 'utf8');
const base = 'projects/magic-servers-1ca0c/databases/(default)/documents';

class Element {
  constructor() {
    this.style = {}; this.children = []; this.selectors = new Map();
    this.value = ''; this.isConnected = true;
  }
  setAttribute() {}
  addEventListener() {}
  append(...items) { this.children.push(...items); }
  replaceChildren() { this.children = []; }
  querySelector(key) {
    if (!this.selectors.has(key)) this.selectors.set(key, new Element());
    return this.selectors.get(key);
  }
  remove() { this.isConnected = false; }
}

function listing(game, code, label, players = 0, visibility = 'public', phase = 'waiting') {
  const origin = 'https://' + label + '.trycloudflare.com';
  const acorn = game === 'acornrally';
  const info = { origin, name: label, [acorn ? 'ownerUid' : 'hostId']: 'id-' + label, visibility, phase,
    joinUrl: origin + (acorn ? '/?room=join' : '/__catnmice/join') };
  const fields = Object.fromEntries(Object.entries(info).map(([key, val]) => [key, { stringValue: val }]));
  fields.players = { integerValue: String(players) };
  fields.queued = { integerValue: '0' };
  const capacity = game === 'mobamania' ? 6 : 4;
  fields.joinable = { booleanValue: players < capacity && phase === 'waiting' };
  fields.capacity = { integerValue: String(capacity) };
  fields.updatedAt = { timestampValue: new Date().toISOString() };
  fields.expiresAt = { timestampValue: new Date(Date.now() + 90000).toISOString() };
  const collection = acorn ? '/acornRallyLobbies/' : '/managedGameDirectories/' + game + '/lobbies/';
  return { name: base + collection + code, fields };
}

function setup(game, docs, statuses = {}) {
  const body = new Element(), navigations = [], requests = [];
  const window = { location: { assign: target => navigations.push(target) } };
  const document = { body, createElement: () => new Element() };
  const reply = (data, status = 200) => ({ ok: status === 200, status, json: async () => data });
  async function fetch(url, options) {
    requests.push(url);
    if (url.endsWith(':runQuery')) {
      const acorn = game === 'acornrally';
      const parent = acorn ? '' : '/managedGameDirectories/' + game;
      assert.equal(new URL(url).pathname, '/v1/' + base + parent + ':runQuery');
      const query = JSON.parse(options.body).structuredQuery;
      assert.equal(query.from[0].collectionId, acorn ? 'acornRallyLobbies' : 'lobbies');
      assert.equal(query.where.fieldFilter.value.stringValue, 'public');
      assert.equal(query.limit, 25);
      const offset = query.startAt ? docs.findIndex(doc => doc.name === query.startAt.values[0].referenceValue) + 1 : 0;
      return reply(docs.slice(offset, offset + 25).map(document => ({ document })));
    }
    if (url.includes('firestore.googleapis.com')) {
      const doc = docs.find(doc => url.endsWith(doc.name));
      return reply(doc, doc ? 200 : 404);
    }
    const acorn = game === 'acornrally';
    const statusPath = acorn ? '/api/rooms/status' : '/__catnmice/status';
    const doc = docs.find(doc => url === doc.fields.origin.stringValue + statusPath);
    if (!doc) return reply({}, 404);
    const f = doc.fields, overrides = statuses[f.name.stringValue];
    if (overrides === false) return reply({}, 502);
    const route = acorn ? {game: 'acorn-rally', waiting: 0, queueEnabled: true} :
      {game, hostId: f.hostId.stringValue, queued: 0, joinable: true};
    return reply({ ...route, phase: f.phase.stringValue,
      visibility: f.visibility.stringValue, players: Number(f.players.integerValue), capacity: Number(f.capacity.integerValue), ...overrides });
  }
  vm.runInNewContext(script, { window, document, URL, URLSearchParams, AbortController, fetch, setTimeout, clearTimeout });
  return { requests, navigations, open: options => {
    const controls = window.CatnmiceServers.open(game, options);
    return { controls, el: selector => body.children.at(-1).children[0].querySelector(selector) };
  } };
}
const settle = () => new Promise(resolve => setImmediate(resolve));

for (const game of ['bannerlands', 'trashtactics', 'cat', 'lastsignal', 'mobamania', 'orbitbloom']) {
  test(game + ' Live Join verifies the host and prefers an available lobby', async () => {
    const docs = [listing(game, '0001', 'expired'), listing(game, '0002', 'wrong-host'),
      listing(game, '0003', 'playing', 0, 'public', 'playing'), listing(game, '0004', 'locked'),
      listing(game, '0005', 'offline'), listing(game, '0006', 'full', game === 'mobamania' ? 6 : 4),
      listing(game, '0007', 'waiting', 2), listing(game, '0008', 'private', 0, 'code')];
    docs[0].fields.expiresAt.timestampValue = new Date(0).toISOString();
    const app = setup(game, docs, { 'wrong-host': { hostId: 'other' }, locked: { joinable: false }, offline: false });
    app.open({ live: true }); await settle();
    assert.deepEqual(app.navigations, ['https://waiting.trycloudflare.com/__catnmice/join']);
    assert.ok(!app.requests.some(url => /\/\/(private|expired)\./.test(url)));
  });
}

test('selection and private code preserve the chosen host and player name', async () => {
  const game = 'lastsignal', app = setup(game, [
    listing(game, '0002', 'alpha'), listing(game, '0003', 'beta'), listing(game, '0021', 'friends', 0, 'code'),
    listing('cat', '1200', 'different-game')
  ]);
  const { el } = app.open();
  await settle();
  const rows = el('ul').children;
  assert.equal(rows.length, 2);
  el('input[name=player]').value = 'Red Fox';
  rows[1].children[1].onclick();
  await settle();
  assert.equal(app.navigations[0], 'https://beta.trycloudflare.com/__catnmice/join?name=Red+Fox');
  el('input[name=code]').value = '0021';
  await el('form').onsubmit({ preventDefault() {} });
  assert.equal(app.navigations[1], 'https://friends.trycloudflare.com/__catnmice/join?name=Red+Fox#code=0021');
});

test('closing while querying suppresses navigation', async () => {
  const game = 'cat', app = setup(game, [listing(game, '1331', 'ready')]);
  app.open({ live: true }).controls.close();
  await settle();
  assert.deepEqual(app.navigations, []);
});

test('Acorn uses the native join handoff and can queue for a race in progress', async () => {
  const game = 'acornrally', app = setup(game, [
    listing(game, '0001', 'paused', 4, 'public', 'racing'),
    listing(game, '0002', 'running', 4, 'public', 'racing'),
    listing(game, '0012', 'friends', 3, 'code', 'waiting')
  ], { paused: {waiting: 128} });
  const { el } = app.open({live: true});
  await settle();
  assert.deepEqual(app.navigations, ['https://running.trycloudflare.com/?room=join#join=1']);
  el('input[name=player]').value = 'Red Fox';
  el('input[name=code]').value = '0012';
  await el('form').onsubmit({preventDefault() {}});
  assert.equal(app.navigations[1], 'https://friends.trycloudflare.com/?room=join#join=1&name=Red+Fox&code=0012');
});
