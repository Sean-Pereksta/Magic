const test = require('node:test');
const assert = require('node:assert/strict');
const {firebaseConfig} = require('../desktop/firebase-config.mjs');
const {configureSaveSession, serveSaveConfig, supportsSaveSync} = require('../desktop/save-sync.cjs');

const origin = 'http://127.0.0.1:31415';
const key = '?key=' + firebaseConfig.apiKey;
const database = 'projects/' + firebaseConfig.projectId + '/databases/(default)';
const auth = 'https://identitytoolkit.googleapis.com/v1/accounts:';
const firestore = 'https://firestore.googleapis.com';
const listen = firestore + '/google.firestore.v1.Firestore/Listen/channel?database=' + encodeURIComponent(database);
const sdk = 'https://www.gstatic.com/firebasejs/10.7.1/';
function makeSession(gameId, sync) {
  let handler;
  const session = {webRequest: {onBeforeRequest(fn) { handler = fn; }}};
  const close = configureSaveSession(session, {gameId, origin, sync});
  function allows(url, resourceType = 'xhr', method = 'POST') {
    let answer;
    handler({url, resourceType, method}, ({cancel}) => { answer = !cancel; });
    return answer;
  }
  return {allows, close};
}

test('each eligible game uses its own pinned SDK, anonymous Auth and Firestore transports', () => {
  for (const id of ['bramblebound', 'dwarfworld', 'spacetyrants']) {
    assert.ok(supportsSaveSync(id));
    const {allows, close} = makeSession(id, true);
    for (const module of ['firebase-app.js', 'firebase-auth.js', 'firebase-firestore.js']) {
      assert.ok(allows(sdk + module, 'script', 'GET'));
    }
    for (const endpoint of ['signUp', 'lookup']) assert.ok(allows(auth + endpoint + key));
    assert.ok(allows('https://securetoken.googleapis.com/v1/token' + key));
    for (const method of ['POST', 'OPTIONS', 'GET']) {
      assert.ok(allows(listen, 'xhr', method));
      assert.ok(allows(listen.replace('Listen', 'Write'), 'xhr', method));
    }
    for (const action of ['batchGet', 'commit']) assert.ok(allows(firestore + '/v1/' + database + '/documents:' + action));
    close();
    assert.equal(allows(auth + 'signUp' + key), false);
    assert.ok(allows(origin + '/game/index.html', 'mainFrame', 'GET'));
  }
});

test('ordinary offline launch and unsupported games keep only their own local transport', () => {
  for (const [id, sync] of [['dwarfworld', false], ['trashtactics', true], ['unlisted', true]]) {
    const {allows} = makeSession(id, sync);
    assert.ok(allows(origin + '/save', 'xhr', 'GET'));
    assert.ok(allows('ws://127.0.0.1:31415/game', 'webSocket', 'GET'));
    assert.equal(allows('http://127.0.0.1:31416/elsewhere'), false);
    assert.equal(allows(auth + 'signUp' + key), false);
    assert.equal(allows(sdk + 'firebase-app.js', 'script', 'GET'), false);
    assert.equal(allows(listen), false);
  }
  assert.equal(supportsSaveSync('trashtactics'), false);
});

test('sync mode restricts the project, requests, module versions and navigation', () => {
  const {allows} = makeSession('bramblebound', true);
  for (const url of [
    'https://www.catnmice.com/', 'http://identitytoolkit.googleapis.com/v1/accounts:signUp' + key,
    auth + 'signUp?key=other', auth + 'signUp' + key + '&key=other', auth + 'delete' + key,
    listen.replace('firestore.googleapis.com', 'firestore.googleapis.com.example'),
    listen.replace(encodeURIComponent(database), 'another'), listen.replace('Listen', 'Other'),
    firestore + '/v1/' + database + '/documents:runQuery',
    'https://securetoken.googleapis.com:444/v1/token' + key
  ]) assert.equal(allows(url), false, url);
  assert.equal(allows(sdk.replace('/10.7.1/', '/latest/') + 'firebase-app.js', 'script', 'GET'), false);
  assert.equal(allows(sdk + 'firebase-storage.js', 'script', 'GET'), false);
  assert.equal(allows(listen, 'mainFrame', 'GET'), false);
  assert.equal(allows(listen, 'subFrame', 'GET'), false);
  assert.equal(allows(auth + 'signUp' + key, 'xhr', 'DELETE'), false);
});

test('only Bramblebound sync mode serves the original local public configuration module', () => {
  const response = {
    writeHead(status, headers) { this.status = status; this.headers = headers; },
    end(body) { this.body = body; }
  };
  const route = '/lobby/firebase-config.mjs';
  for (const [id, sync, url] of [
    ['bramblebound', false, route], ['dwarfworld', true, route], ['bramblebound', true, '/other.mjs']
  ]) assert.equal(serveSaveConfig(id, sync, url, {method:'GET'}, response), false);
  assert.ok(serveSaveConfig('bramblebound', true, route, {method:'GET'}, response));
  assert.equal(response.status, 200);
  assert.match(response.body.toString(), /export const firebaseConfig/);
  assert.ok(response.body.toString().includes(firebaseConfig.projectId));
  const expectedLength = response.body.length;
  assert.ok(serveSaveConfig('bramblebound', true, route, {method:'HEAD'}, response));
  assert.equal(response.body, undefined);
  assert.equal(response.headers['Content-Length'], expectedLength);
});
