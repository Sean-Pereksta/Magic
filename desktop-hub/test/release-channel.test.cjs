const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { plan } = require('../build/plan-release.cjs');
const { buildRelease } = require('../build/release.cjs');

const magic = 'a'.repeat(40), truth = 'b'.repeat(40), older = 'c'.repeat(40);
const repo = 'Sean-Pereksta/Magic';
const environment = { GITHUB_EVENT_NAME: 'schedule', GITHUB_REF: 'refs/heads/main', GITHUB_SHA: magic, GITHUB_REPOSITORY: repo };
const assets = ['Catnmice-Game-Hub-Setup.exe', 'github-stable.json', 'github-stable.sigstore.json'].map(name => ({ name, state: 'uploaded' }));
const previous = { assets, body: 'Magic revision: ' + magic + '\r\nTruth revision: ' + truth + '\r\nSequence: 20\r\n' };
function github(channel, head = magic) {
  return async query => {
    if (query === 'Sean-Pereksta/Truth/commits/main') return { sha: truth };
    if (query === repo + '/commits/main') return { sha: head };
    if (query === repo + '/releases/tags/catnmice-hub-channel') return channel;
    throw Error('Unexpected query ' + query);
  };
}

test('unchanged sources skip the build; either repository changing triggers publication', async () => {
  assert.deepEqual(await plan(environment, github(previous)), { build: false, publish: false, truth });
  for (const channel of [null, { ...previous, body: previous.body.replace(magic, older) },
    { ...previous, body: previous.body.replace(truth, older) }, { ...previous, assets: [] }]) {
    assert.deepEqual(await plan(environment, github(channel)), { build: true, publish: true, truth });
  }
});

test('pull requests, stale Magic heads, and explicitly disabled automatic runs cannot publish', async () => {
  assert.equal((await plan({ ...environment, GITHUB_EVENT_NAME: 'pull_request' }, github(null))).publish, false);
  assert.equal((await plan({ ...environment, GITHUB_REPOSITORY: 'example/fork' }, github(null))).publish, false);
  assert.equal((await plan(environment, github(null, older))).build, false);
  assert.equal((await plan({ ...environment, AUTO_PUBLISH: 'false' }, github(null))).build, false);
  const requested = { ...environment, GITHUB_EVENT_NAME: 'workflow_dispatch', MANUAL_PUBLISH: 'true' };
  await assert.rejects(plan({ ...requested, TRUTH_REVISION: older }, github(null)), /current Truth main/);
  assert.deepEqual(await plan({ ...environment, GITHUB_EVENT_NAME: 'repository_dispatch', TRUTH_REVISION: older }, github(null)),
    { build: true, publish: true, truth });
});

test('GitHub builds need no secret and optionally keep the old signed feed working', t => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'catnmice-release-'));
  t.after(() => fs.rmSync(dir, { force: true, recursive: true }));
  const config = path.join(dir, 'release.json');
  const digest = '1'.repeat(64);
  fs.writeFileSync(path.join(dir, 'components.json'), JSON.stringify([{ id: 'hub', sha256: digest, size: 100 }]));
  const env = { GITHUB_SHA: magic, TRUTH_SHA: truth, GITHUB_RUN_NUMBER: '22' };
  const source = { magic, truth };
  buildRelease(dir, config, source, '45.3.4', env);
  const raw = fs.readFileSync(path.join(dir, 'github-stable.json'));
  const manifest = JSON.parse(raw);
  assert.deepEqual(JSON.parse(fs.readFileSync(config)), { provider: 'github', sequence: 22, components: { hub: digest } });
  assert.deepEqual(manifest.source, source);
  assert.equal(manifest.items[0].url, 'https://github.com/Sean-Pereksta/Magic/releases/download/catnmice-hub-22-aaaaaaaa/hub.zip');
  assert.equal(manifest.items[0].runtime, '45.3.4');
  assert.equal(fs.existsSync(path.join(dir, 'stable.json')), false);
  const pair = crypto.generateKeyPairSync('ed25519');
  const pem = pair.privateKey.export({ type: 'pkcs8', format: 'pem' });
  buildRelease(dir, config, source, '45.3.4', { ...env, HUB_UPDATE_PRIVATE_KEY: pem });
  const signed = JSON.parse(fs.readFileSync(path.join(dir, 'stable.json')));
  assert.deepEqual(Buffer.from(signed.payload, 'base64'), raw);
  assert.ok(crypto.verify(null, raw, pair.publicKey, Buffer.from(signed.signature, 'base64')));
  assert.throws(() => buildRelease(dir, config, { magic, truth: older }, '45.3.4', env), /source revisions/);
});
