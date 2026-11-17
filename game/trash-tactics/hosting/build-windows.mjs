import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { cp, mkdir, mkdtemp, readFile, readdir, realpath, rename, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const gameRoot = fileURLToPath(new URL('../', import.meta.url));
const archiveName = 'Trash-Tactics-Host-Windows-x64.zip';
const runtimeHashes = {
  node: '3602f2bb1a10f2cbab4c36886218a33c1ab3db87290e73b033c46c77147d0237',
  cloudflared: 'd5c1d82031da6253dcdcff64c944cd0fe063016edeb2e4fd088075fc9bd52d0a',
};
const { values: options } = parseArgs({ options: {
  output: { type: 'string' },
  node: { type: 'string', default: process.execPath },
  cloudflared: { type: 'string' },
  notices: { type: 'string' },
  source: { type: 'string' },
} });

if (process.platform !== 'win32') throw new Error('Build the Windows host ZIP on Windows.');
if (!options.cloudflared || !options.notices || !options.output) {
  throw new Error('Provide --output, --cloudflared and --notices. See hosting/README.md.');
}
const hash = async file => createHash('sha256').update(await readFile(file)).digest('hex');
for (const [runtime, expected] of Object.entries(runtimeHashes)) {
  if (await hash(options[runtime]) !== expected) {
    throw new Error(runtime + ' does not match the pinned Windows x64 runtime. See hosting/README.md.');
  }
}
for (const name of ['Node-LICENSE.txt', 'cloudflared-LICENSE.txt', 'cloudflared-dependencies/Go-runtime/LICENSE']) {
  await readFile(path.join(options.notices, name));
}
const output = path.resolve(options.output);
await mkdir(output, { recursive: true });
const actualOutput = await realpath(output);
const staging = await mkdtemp(path.join(actualOutput, '.trash-tactics-build-'));
const packageRoot = path.join(staging, 'Trash-Tactics-Host');
const hostingRoot = path.join(packageRoot, 'hosting');
const runtimeRoot = path.join(hostingRoot, 'runtime');
const copy = async (source, destination) => cp(source, destination, { recursive: true, errorOnExist: true, force: false });
function run(executable, args, cwd) {
  const result = spawnSync(executable, args, { cwd, stdio: 'inherit', windowsHide: true });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(executable + ' failed with exit code ' + result.status);
}
try {
  await mkdir(runtimeRoot, { recursive: true });
  await copy(options.node, path.join(runtimeRoot, 'node.exe'));
  await copy(options.cloudflared, path.join(runtimeRoot, 'cloudflared.exe'));
  await copy(options.notices, path.join(packageRoot, 'notices'));
  await copy(path.join(gameRoot, 'assets'), path.join(packageRoot, 'assets'));
  for (const entry of await readdir(gameRoot, { withFileTypes: true })) {
    if (entry.isFile() && /\.(?:m?js|css|html|json)$/.test(entry.name)) {
      await copy(path.join(gameRoot, entry.name), path.join(packageRoot, entry.name));
    }
  }
  for (const entry of ['launch.mjs', 'tunnel.mjs', 'quick-tunnel.yml']) {
    await copy(path.join(gameRoot, 'hosting', entry), path.join(hostingRoot, entry));
  }
  const launcher = await readFile(path.join(gameRoot, 'hosting', 'Start Trash Tactics Host.cmd'), 'utf8');
  await writeFile(path.join(packageRoot, 'Start Trash Tactics Host.cmd'), launcher.replace(/\r?\n/g, '\r\n'));
  run('cmd.exe', ['/d', '/s', '/c', 'npm.cmd ci --omit=dev --ignore-scripts --no-audit --no-fund'], packageRoot);
  const { version } = JSON.parse(await readFile(path.join(packageRoot, 'package.json'), 'utf8'));
  await writeFile(path.join(packageRoot, 'build.json'), JSON.stringify({
    version,
    platform: 'windows-x64',
    source: options.source ?? null,
    runtimes: { node: '24.19.0', cloudflared: '2026.11.0' },
  }, null, 2) + '\n');
  await writeFile(path.join(packageRoot, 'Start Here.txt'), [
    'TRASH TACTICS ONLINE HOST',
    '',
    'Extract the whole ZIP, then open Start Trash Tactics Host.cmd.',
    'The game opens in your browser when the internet connection is ready.',
    'Create a lobby so browser players can find it, or share the guest invite.',
    'Keep this window open and your computer awake during the match.',
    'Press Ctrl+C in the host window when you are done.',
    '',
    'This is a portable Windows x64 package. Node.js and cloudflared are bundled.',
    'Community matches are casual. Ranked games use the Catnmice match server.',
    'Community lobbies use temporary Cloudflare addresses.',
    'https://developers.cloudflare.com/tunnel/get-started/quick-tunnels/',
    '',
    'Runtime licenses are in notices/.',
  ].join('\r\n') + '\r\n');
  const archive = path.join(staging, archiveName);
  run('tar.exe', ['-a', '-cf', archive, '-C', staging, 'Trash-Tactics-Host'], staging);
  const archiveHash = await hash(archive);
  await rename(archive, path.join(actualOutput, archiveName));
  await writeFile(path.join(actualOutput, archiveName + '.sha256'), archiveHash + '  ' + archiveName + '\n');
  console.log('Built ' + path.join(actualOutput, archiveName) + ' (SHA-256 ' + archiveHash + ').');
} finally {
  const actualStaging = await realpath(staging);
  if (path.dirname(actualStaging) === actualOutput && path.basename(actualStaging).startsWith('.trash-tactics-build-')) {
    await rm(actualStaging, { recursive: true, force: true });
  }
}
