const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

function buildRelease(directory, configPath, source, runtime, env) {
  const parts = JSON.parse(fs.readFileSync(path.join(directory, 'components.json'), 'utf8'));
  const sequence = Number(env.GITHUB_RUN_NUMBER);
  const revision = /^[a-f0-9]{40}$/;
  if (!Number.isSafeInteger(sequence) || sequence < 1 || !revision.test(source.magic) ||
      !revision.test(source.truth) || source.magic !== env.GITHUB_SHA || source.truth !== env.TRUTH_SHA) {
    throw Error('The release must match both selected source revisions.');
  }
  const tag = 'catnmice-hub-' + sequence + '-' + source.magic.slice(0, 8);
  const base = 'https://github.com/Sean-Pereksta/Magic/releases/download/' + tag + '/';
  const items = parts.map(part => ({
    ...part,
    url: base + part.id + '.zip',
    version: tag,
    ...(part.id === 'hub' ? { runtime } : {})
  }));
  const manifest = Buffer.from(JSON.stringify({ schema: 1, sequence, source, items }));
  fs.writeFileSync(path.join(directory, 'github-stable.json'), manifest);
  const config = {
    provider: 'github',
    sequence,
    components: Object.fromEntries(parts.map(part => [part.id, part.sha256]))
  };
  if (env.HUB_UPDATE_PRIVATE_KEY) {
    const key = crypto.createPrivateKey(env.HUB_UPDATE_PRIVATE_KEY);
    if (key.asymmetricKeyType !== 'ed25519') throw Error('Invalid legacy Catnmice update key.');
    config.key = crypto.createPublicKey(key).export({ type: 'spki', format: 'pem' });
    fs.writeFileSync(path.join(directory, 'stable.json'), JSON.stringify({
      payload: manifest.toString('base64'),
      signature: crypto.sign(null, manifest, key).toString('base64')
    }));
  }
  fs.writeFileSync(configPath, JSON.stringify(config));
}

if (require.main === module) {
  buildRelease(process.argv[2], process.argv[3], require('../content/catalog.json').source,
    require('../package.json').devDependencies.electron, process.env);
}

module.exports = { buildRelease };
