const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

// Accept either the installation just exercised by the smoke tests, or the exact
// candidate installer about to be promoted to the public download channel.
function checkInstalledRelease(candidate, manifest, bundle) {
  if (process.platform !== 'win32') throw Error('Use a Windows release worker.');
  let installed = path.resolve(candidate);
  let temporary;
  try {
    if (fs.statSync(installed).isFile()) {
      temporary = fs.mkdtempSync(path.join(process.env.RUNNER_TEMP || os.tmpdir(), 'catnmice-release-'));
      const setup = installed;
      installed = path.join(temporary, 'installed');
      execFileSync(setup, ['/S', '/TESTROOT="' + installed + '"'], {
        windowsHide: true, windowsVerbatimArguments: true, timeout: 600000
      });
    }
    const executable = path.join(installed, 'Catnmice Game Hub.exe');
    const verifier = path.join(installed, 'resources', 'app', 'desktop', 'provenance', 'verify.cjs');
    const output = execFileSync(executable, [verifier, path.resolve(manifest), path.resolve(bundle)], {
      env: { ...process.env, ELECTRON_RUN_AS_NODE: '1' },
      encoding: 'utf8', windowsHide: true, timeout: 60000, stdio: ['ignore', 'pipe', 'inherit']
    });
    if (!output.trim().split(/\r?\n/).includes('Verified Catnmice release provenance.')) {
      throw Error('The packaged Windows verifier did not confirm the release.');
    }
    console.log('The installed Catnmice launcher verified the signed release.');
  } finally {
    if (temporary) fs.rmSync(temporary, { recursive: true, force: true, maxRetries: 3 });
  }
}

checkInstalledRelease(...process.argv.slice(2));
