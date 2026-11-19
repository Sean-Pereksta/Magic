import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createInterface } from 'node:readline';
import { setTimeout as delay } from 'node:timers/promises';
import { chromium } from 'playwright-core';

if (!process.argv[2]) throw new Error('Usage: node build/cat-host-smoke.mjs <installed Game Hub folder>');
const installed = path.resolve(process.argv[2]);
const pack = path.join(installed, 'resources', 'app', 'content', 'packs', 'cat');
const edition = path.join(pack, 'game', 'catandmouse', 'edition');
const runtime = path.join(pack, 'hub-runtime', 'node.exe');
const data = await mkdtemp(path.join(os.tmpdir(), 'catnmice-cat-smoke-'));

// Use the shipping launcher and a loopback-only match. Never start a tunnel
// or list the test lobby in the public directory.
const launch = spawn(runtime, [
  path.join(edition, 'server', 'launcher.mjs'),
  '--app-port', '0', '--data-dir', data, '--no-open', '--no-reuse', '--local-only'
], { windowsHide: true, stdio: ['ignore', 'pipe', 'inherit', 'ipc'] });
const stopped = new Promise(resolve => launch.once('close', resolve));
const output = createInterface({ input: launch.stdout });
let browser;
try {
  const address = await Promise.race([
    once(output, 'line').then(([line]) => JSON.parse(line).bootstrapUrl),
    once(launch, 'error').then(([error]) => { throw error; }),
    stopped.then(() => { throw new Error('The installed Cat & Mouse host exited during startup.'); }),
    delay(12_000, null, { ref: false }).then(() => { throw new Error('The installed Cat & Mouse host did not publish a local address.'); })
  ]);
  const bootstrap = new URL(address);
  assert.equal(bootstrap.protocol, 'http:');
  assert.equal(bootstrap.hostname, '127.0.0.1');
  bootstrap.searchParams.set('host', '1');
  const capability = new URLSearchParams(bootstrap.hash.slice(1));
  const controlPort = Number(capability.get('control'));
  assert.ok(Number.isInteger(controlPort) && controlPort > 0 && controlPort <= 65535);
  assert.match(capability.get('desktop') || '', /^[\w-]{43}$/);
  const headers = { Authorization: 'Bearer ' + capability.get('desktop') };

  browser = await chromium.launch({ channel: 'msedge', headless: true });
  const host = await browser.newPage();
  const guest = await browser.newPage();
  host.setDefaultTimeout(15_000);
  guest.setDefaultTimeout(15_000);
  await host.goto(bootstrap.href);
  await host.locator('#room-name').fill('Launcher test');
  await host.locator('#host-name').fill('Test host');
  await host.locator('#capacity').selectOption('2');
  await host.locator('#visibility').selectOption('public');
  await host.locator('[data-action="host-start"]').click();
  await host.locator('#roster').getByText('Test host', { exact: false }).waitFor();

  const control = 'http://127.0.0.1:' + controlPort + '/host';
  const status = await fetch(control, { headers, signal: AbortSignal.timeout(4_000) });
  assert.equal(status.status, 200);
  const { lobbyId, serverUrl, stage, publicUrl } = await status.json();
  assert.equal(stage, 'ready');
  assert.equal(publicUrl, null);
  const room = new URL(serverUrl);
  assert.equal(room.protocol, 'http:');
  assert.equal(room.hostname, '127.0.0.1');
  assert.match(lobbyId, /^[a-zA-Z0-9_-]+$/);
  room.hash = new URLSearchParams({ room: lobbyId });
  await guest.goto(room.href);
  await guest.locator('#guest-name').fill('Test guest');
  await guest.locator('[data-action="join-room"]').click();
  await host.locator('#roster').getByText('Test guest', { exact: true }).waitFor();
  await guest.locator('#roster').getByText('Test host', { exact: false }).waitFor();

  await Promise.all([
    host.locator('[data-action="ready"]').click(),
    guest.locator('[data-action="ready"]').click()
  ]);
  await host.locator('[data-action="normal-start"]').click();
  await Promise.all([host, guest].map(page =>
    page.waitForFunction(() => document.documentElement.dataset.catnmicePlaying === 'true')
  ));
  console.log('Installed Cat & Mouse host and separate browser guest loaded and began the same game.');
} finally {
  await browser?.close();
  output.close();
  if (launch.connected) launch.send({ type: 'stop' }, () => {});
  const exited = await Promise.race([stopped.then(() => true), delay(6_000, false, { ref: false })]);
  if (!exited) launch.kill();
  await stopped;
  await rm(data, { recursive: true, force: true });
}
