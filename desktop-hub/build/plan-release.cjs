const fs = require('node:fs');

const repo = 'Sean-Pereksta/Magic';
const channel = 'catnmice-hub-channel';
const revision = /^[a-f0-9]{40}$/;

async function api(path, optional = false) {
  const response = await fetch('https://api.github.com/repos/' + path, {
    headers: { Authorization: 'Bearer ' + process.env.GH_TOKEN, Accept: 'application/vnd.github+json' }
  });
  if (optional && response.status === 404) return null;
  if (!response.ok) throw Error('GitHub returned ' + response.status + ' for ' + path);
  return response.json();
}

async function plan(env, request = api) {
  const event = env.GITHUB_EVENT_NAME;
  const automatic = ['push', 'schedule', 'repository_dispatch'].includes(event);
  const publish = env.GITHUB_REPOSITORY === repo && env.GITHUB_REF === 'refs/heads/main' &&
    (event === 'workflow_dispatch' ? env.MANUAL_PUBLISH === 'true' : automatic && env.AUTO_PUBLISH !== 'false');
  const magic = env.GITHUB_SHA;
  const selected = env.TRUTH_REVISION || 'main';
  if (!revision.test(magic) || (selected !== 'main' && !revision.test(selected))) {
    throw Error('Use a full Truth commit SHA or leave the revision empty for main.');
  }
  // Publication always takes the latest Truth main. Delayed dispatches cannot roll it back.
  const truth = (await request('Sean-Pereksta/Truth/commits/' + (publish ? 'main' : selected))).sha;
  if (!revision.test(truth)) throw Error('Invalid Truth revision returned by GitHub.');
  if (publish && event === 'workflow_dispatch' && selected !== 'main' && selected !== truth) {
    throw Error('Publish the current Truth main. Uncheck Publish to test another revision.');
  }
  if (!publish) return { build: event !== 'schedule', publish, truth };
  if ((await request(repo + '/commits/main')).sha !== magic) return { build: false, publish: false, truth };
  const previous = await request(repo + '/releases/tags/' + channel, true);
  const published = previous && !previous.draft && [
    'Catnmice-Game-Hub-Setup.exe', 'github-stable.json', 'github-stable.sigstore.json'
  ].every(name => previous.assets.some(asset => asset.name === name && asset.state === 'uploaded'));
  const notes = previous?.body?.split(/\r?\n/) || [];
  const alreadyCurrent = published && notes.includes('Magic revision: ' + magic) && notes.includes('Truth revision: ' + truth);
  return { build: !alreadyCurrent, publish: !alreadyCurrent, truth };
}

if (require.main === module) {
  plan(process.env).then(result => {
    fs.appendFileSync(process.env.GITHUB_OUTPUT, Object.entries(result).map(([key, value]) => key + '=' + value + '\n').join(''));
    console.log(result.build ? 'Building with Truth ' + result.truth : 'No new release needed.');
  }).catch(error => { console.error(error.message); process.exitCode = 1; });
}

module.exports = { plan };
