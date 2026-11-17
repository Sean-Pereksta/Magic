import assert from 'node:assert/strict';
import { mkdtemp, readdir, readFile, rm, rmdir } from 'node:fs/promises';
import { extname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawn } from 'node:child_process';
import { tmpdir } from 'node:os';

const root = resolve(process.argv[2]);
const startup = await readFile(join(root, 'index.html'), 'utf8');
const scripts = [...startup.matchAll(/<script\b[^>]*src=["']([^"']+)["']/g)].map(match => match[1]);
const css = [...startup.matchAll(/<link\b[^>]*href=["']([^"']+\.css)["']/g)].map(match => match[1]);
const art = await readdir(join(root, 'art'), { recursive: true, withFileTypes: true });
const painted = art.filter(item => item.isFile() && ['.png', '.jpg', '.webp'].includes(extname(item.name)));
assert.ok(scripts.length && css.length && painted.length, 'The offline package needs its client, styles and painted artwork.');
for (const file of ['title', 'terrain', 'units', 'towers', 'effects', 'facing', 'mill-preview', 'walls', 'ground']) {
  assert.ok(painted.some(item => item.name === file + '-v1.webp'), 'Missing the painted ' + file + ' atlas.');
}
const sim = await import(pathToFileURL(join(root, 'simulation.mjs')));
const solo = sim.createSimulation({
  matchId: 'offline-package-check', players: [{ id: 'offline', name: 'Pip', loadout: sim.STARTER_LOADOUT }]
});
solo.step();
assert.equal(solo.snapshot().tick, 1);

const data = await mkdtemp(join(tmpdir(), 'cat-mouse-package-'));
const launcher = spawn(process.execPath, [
  join(root, 'server', 'launcher.mjs'), '--no-open', '--local-only', '--app-port', '0', '--data-dir', data
], { stdio: ['ignore', 'pipe', 'inherit'], windowsHide: true });
try {
  const bootstrap = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('The packaged launcher did not open.')), 8_000);
    let output = '';
    launcher.stdout.on('data', part => {
      output += part;
      if (!output.includes('\n')) return;
      clearTimeout(timer);
      resolve(JSON.parse(output.split('\n')[0]).bootstrapUrl);
    });
    launcher.once('exit', code => {
      clearTimeout(timer);
      reject(new Error('The packaged launcher exited with ' + code + '.'));
    });
  });
  const home = new URL(bootstrap);
  const hash = new URLSearchParams(home.hash.slice(1));
  const control = 'http://127.0.0.1:' + hash.get('control');
  const auth = { Authorization: 'Bearer ' + hash.get('desktop') };
  const sources = ['/', ...scripts, ...css, ...painted.map(item => {
    const absolute = join(item.parentPath, item.name);
    return absolute.slice(root.length).replaceAll('\\', '/');
  })];
  for (const source of sources) {
    const target = new URL(source, home);
    assert.equal(target.origin, home.origin, 'The offline package must ship its startup resources.');
    const response = await fetch(target);
    assert.ok(response.ok, target.pathname + ' should load from the offline package.');
    assert.ok((await response.arrayBuffer()).byteLength, target.pathname + ' should contain data.');
  }
  const shutDown = await fetch(control + '/app', { method: 'DELETE', headers: auth });
  assert.equal(shutDown.status, 202);
  await new Promise(done => {
    if (launcher.exitCode !== null) done();
    else launcher.once('exit', done);
  });
} finally {
  if (launcher.exitCode === null) {
    launcher.kill();
    await new Promise(done => launcher.once('exit', done));
  }
  await rm(join(data, 'desktop-0.json'), { force: true });
  await rmdir(data);
}
