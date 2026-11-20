'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const script = fs.readFileSync(path.join(__dirname, '../managed-servers.js'), 'utf8');
const base = 'projects/bible-game-246c0/databases/(default)/documents/managedGameDirectories/';

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
  const info = { origin, name: label, hostId: 'id-' + label, visibility, phase, joinUrl: origin + '/__catnmice/join' };
  const fields = Object.fromEntries(Object.entries(info).map(([key, val]) => [key, { stringValue: val }]));
  fields.players = { integerValue: String(players) };
  fields.queued = { integerValue: '0' };
  fields.joinable = { booleanValue: players < 4 && phase === 'waiting' };
  fields.capacity = { integerValue: '4' };
  fields.updatedAt = { timestampValue: new Date().toISOString() };
  fields.expiresAt = { timestampValue: new Date(Date.now() + 90000).toISOString() };
  return { name: base + game + '/lobbies/' + code, fields };
}

function setup(game, docs, statuses = {}) {
  const body = new Element(), navigations = [], requests = [];
  const window = { location: { assign: target => navigations.push(target) } };
  const document = { body, createElement: () => new Element() };
  const reply = (data, status = 200) => ({ ok: status === 200, status, json: async () => data });
  async function fetch(url, options) {
    requests.push(url);
    if (url.endsWith(':runQuery')) {
      assert.ok(url.endsWith('/managedGameDirectories/' + game + ':runQuery'));
      const query = JSON.parse(options.body).structuredQuery;
      assert.equal(query.where.fieldFilter.value.stringValue, 'public');
      const offset = query.startAt ? docs.findIndex(doc => doc.name === query.startAt.values[0].referenceValue) + 1 : 0;
      return reply(docs.slice(offset, offset + 25).map(document => ({ document })));
    }
    if (url.includes('firestore.googleapis.com')) {
      const doc = docs.find(doc => url.endsWith(doc.name));
      return reply(doc, doc ? 200 : 404);
    }
    const doc = docs.find(doc => url === doc.fields.origin.stringValue + '/__catnmice/status');
    if (!doc) return reply({}, 404);
    const f = doc.fields, overrides = statuses[f.name.stringValue];
    if (overrides === false) return reply({}, 502);
    return reply({ game, phase: f.phase.stringValue, hostId: f.hostId.stringValue,
      visibility: f.visibility.stringValue, players: Number(f.players.integerValue), capacity: 4,
      queued: 0, joinable: true, ...overrides });
  }
  vm.runInNewContext(script, { window, document, URL, URLSearchParams, AbortController, fetch, setTimeout, clearTimeout });
  return { requests, navigations, open: options => {
    const controls = window.CatnmiceServers.open(game, options);
    return { controls, el: selector => body.children.at(-1).children[0].querySelector(selector) };
  } };
}
const settle = () => new Promise(resolve => setImmediate(resolve));

for (const game of ['bannerlands', 'trashtactics', 'cat', 'lastsignal']) {
  test(game + ' Live Join verifies the host and prefers an available lobby', async () => {
    const docs = [listing(game, '0001', 'expired'), listing(game, '0002', 'wrong-host'),
      listing(game, '0003', 'playing', 0, 'public', 'playing'), listing(game, '0004', 'locked'),
      listing(game, '0005', 'offline'), listing(game, '0006', 'full', 4),
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
