import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

const quickAddress = /https:\/\/[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.trycloudflare\.com(?=[\s|"'\x1B]|$)/i;
const configPath = fileURLToPath(new URL('./quick-tunnel.yml', import.meta.url));

export async function startTunnel({
  executable, localUrl, instance, signal, onExit,
  spawnProcess = spawn, fetchHealth = fetch, timeoutMs = 90_000,
}) {
  const origin = new URL(localUrl);
  if (origin.origin !== localUrl || origin.protocol !== 'http:' ||
      origin.hostname !== '127.0.0.1' || !origin.port) {
    throw new Error('The tunnel origin must be a loopback game server.');
  }
  const deadline = AbortSignal.timeout(timeoutMs);
  const lifecycle = new AbortController();
  const startupSignal = AbortSignal.any([deadline, lifecycle.signal, ...(signal ? [signal] : [])]);
  startupSignal.throwIfAborted();
  // Ignore an existing named tunnel's credentials, flags, and ingress routes.
  const env = Object.fromEntries(Object.entries(process.env)
    .filter(([name]) => !/^(?:TUNNEL_|TRASH_TACTICS_HOST_TOKEN$)/i.test(name)));
  const args = ['tunnel', '--config', configPath, '--no-autoupdate', '--protocol', 'http2',
    '--metrics', '127.0.0.1:0', '--url', origin.origin];
  const child = spawnProcess(executable, args, { env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  let closed = false;
  let stopped = false;
  let verified = false;
  const fail = error => {
    closed = true;
    lifecycle.abort(error);
    if (verified && !stopped) onExit?.(error);
  };
  child.once('error', error => fail(new Error(
    'Cloudflare could not start. Check that the bundled cloudflared executable is available.', { cause: error })));
  child.once('exit', () => fail(new Error('The Cloudflare connection closed.')));
  const stop = () => {
    if (stopped) return;
    stopped = true;
    lifecycle.abort(new Error('Hosting stopped.'));
    if (!closed) child.kill();
  };

  try {
    const address = await new Promise((resolve, reject) => {
      let output = '';
      const consume = chunk => {
        // The address can be split across chunks. Keep the last partial line.
        output += chunk.toString();
        const lineBreak = output.lastIndexOf('\n');
        if (lineBreak === -1) {
          output = output.slice(-8192);
          return;
        }
        const match = output.slice(0, lineBreak).match(quickAddress);
        output = output.slice(lineBreak + 1);
        if (match) resolve(match[0].toLowerCase());
      };
      child.stdout?.on('data', consume);
      child.stderr?.on('data', consume);
      startupSignal.addEventListener('abort', () => reject(startupSignal.reason), { once: true });
    });

    while (true) {
      startupSignal.throwIfAborted();
      try {
        const response = await fetchHealth(address + '/api/health', {
          redirect: 'error',
          signal: AbortSignal.any([startupSignal, AbortSignal.timeout(3500)]),
          headers: { Accept: 'application/json' },
        });
        const health = response.ok ? await response.json() : null;
        if (health?.ok === true && health.instance === instance) {
          startupSignal.throwIfAborted();
          verified = true;
          signal?.addEventListener('abort', stop, { once: true });
          return { url: address, stop };
        }
      } catch {
        startupSignal.throwIfAborted();
      }
      await delay(750, undefined, { signal: startupSignal });
    }
  } catch (error) {
    stop();
    if (deadline.aborted) {
      throw new Error('The Cloudflare address did not reach this game server within ' +
        Math.ceil(timeoutMs / 1000) + ' seconds. Check your connection or tunnel route and retry.', { cause: error });
    }
    throw startupSignal.reason ?? error;
  }
}
