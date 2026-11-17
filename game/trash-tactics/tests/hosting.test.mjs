import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { startTunnel } from '../hosting/tunnel.mjs';

const localUrl = 'http://127.0.0.1:57291';
const instance = 'this-host-instance';

function fakeProcess() {
  const child = new EventEmitter();
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  let kills = 0;
  child.kill = () => {
    kills++;
    child.emit('exit', 0);
  };
  return { child, countKills: () => kills };
}

test('Quick Tunnel waits for the matching instance and shuts down its own child', async () => {
  const { child, countKills } = fakeProcess();
  const lifecycle = new AbortController();
  let probes = 0;
  const tunnel = await startTunnel({
    executable: 'cloudflared',
    localUrl,
    instance,
    signal: lifecycle.signal,
    spawnProcess(executable, args) {
      assert.equal(executable, 'cloudflared');
      assert.equal(args[args.indexOf('--url') + 1], localUrl);
      assert.match(args[args.indexOf('--config') + 1], /quick-tunnel\.yml$/);
      queueMicrotask(() => {
        child.stderr.write('| https://test-ad');
        child.stderr.write('dress.trycloudflare.com |\n');
      });
      return child;
    },
    async fetchHealth(url) {
      assert.equal(url, 'https://test-address.trycloudflare.com/api/health');
      return Response.json({ ok: true, instance: ++probes === 1 ? 'wrong-host' : instance });
    },
    onExit: () => assert.fail('Intentional stop must not report an unexpected exit'),
  });
  assert.equal(probes, 2);
  assert.equal(tunnel.url, 'https://test-address.trycloudflare.com');
  lifecycle.abort();
  tunnel.stop();
  assert.equal(countKills(), 1);
});

test('An unexpected Cloudflare closure is reported to the launcher once', async () => {
  const { child, countKills } = fakeProcess();
  const errors = [];
  const tunnel = await startTunnel({
    executable: 'cloudflared',
    localUrl,
    instance,
    spawnProcess(_executable, _args, options) {
      assert.equal(options.env.TUNNEL_TOKEN, undefined);
      queueMicrotask(() => child.stdout.write('| https://example.trycloudflare.com |\n'));
      return child;
    },
    fetchHealth: async () => Response.json({ ok: true, instance }),
    onExit: error => errors.push(error),
  });
  assert.equal(tunnel.url, 'https://example.trycloudflare.com');
  child.emit('exit', 1);
  tunnel.stop();
  assert.match(errors[0]?.message, /connection closed/);
  assert.equal(errors.length, 1);
  assert.equal(countKills(), 0);
});

test('Tunnel rejects nonloopback origins before spawning', async () => {
  await assert.rejects(startTunnel({
    executable: 'cloudflared',
    localUrl: 'http://0.0.0.0:57291',
    instance,
    spawnProcess: () => assert.fail('An invalid origin must not start a process'),
  }), /loopback game server/);
});
