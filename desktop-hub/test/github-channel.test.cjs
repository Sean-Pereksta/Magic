const test = require('node:test');
const assert = require('node:assert/strict');
const {getFeed, validateGitHubManifest} = require('../desktop/updates.cjs');

function release() {
  const source = {magic: '1'.repeat(40), truth: '2'.repeat(40)};
  const version = 'catnmice-hub-10-11111111';
  const items = ['hub', 'catalog', 'bannerlands'].map(id => ({
    id, version, size: 10, sha256: 'f'.repeat(64),
    url: 'https://github.com/Sean-Pereksta/Magic/releases/download/'+version+'/'+id+'.zip'
  }));
  return {schema: 1, sequence: 10, source, items};
}

test('GitHub channel pins both sources and every component to one numbered release', () => {
  const current = release();
  assert.equal(validateGitHubManifest(current), current);
  for (const change of [
    m => { delete m.source.truth; },
    m => { m.source.magic = '3'.repeat(40); },
    m => { m.items.pop(); m.items.pop(); },
    m => { m.items[0].url = m.items[0].url.replace('/Magic/', '/Truth/'); },
    m => { m.items[1].version = 'latest'; }
  ]) {
    const invalid = release();
    change(invalid);
    assert.throws(() => validateGitHubManifest(invalid));
  }
});

test('GitHub update mode rejects unavailable, oversized and unattested release manifests', async () => {
  const config = {provider: 'github', sequence: 9};
  await assert.rejects(getFeed(config, 0, async () => new Response(null, {status: 404})), /GitHub/);
  await assert.rejects(getFeed(config, 0, async () => new Response(Buffer.alloc(1024*1024+1))), /limit/);
  const requests = new Set();
  await assert.rejects(getFeed(config, 0, async url => {
    requests.add(new URL(url).pathname.split('/').pop());
    return Response.json(url.endsWith('.sigstore.json') ? {} : release());
  }), error => error.code !== 'MODULE_NOT_FOUND');
  assert.deepEqual([...requests].sort(), ['github-stable.json', 'github-stable.sigstore.json']);
});
