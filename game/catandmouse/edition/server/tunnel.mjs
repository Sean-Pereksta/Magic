import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { PROTOCOL_VERSION } from '../directory-config.mjs';

function probeSocket(origin, roomId, signal) {
  return new Promise((resolve, reject) => {
    const endpoint = new URL('/socket', origin);
    endpoint.protocol = 'wss:';
    const socket = new WebSocket(endpoint);
    const cleanup = () => {
      signal.removeEventListener('abort', abort);
      socket.close();
    };
    const abort = () => {
      cleanup();
      reject(signal.reason);
    };
    socket.addEventListener('open', () => socket.send(JSON.stringify({
      type: 'probe', roomId, version: PROTOCOL_VERSION
    })), { once: true });
    socket.addEventListener('message', event => {
      cleanup();
      try {
        const answer = JSON.parse(event.data);
        if (answer.type === 'probe' && answer.roomId === roomId && answer.version === PROTOCOL_VERSION) {
          resolve();
        } else {
          reject(new Error('The public connection reached a different room.'));
        }
      } catch {
        reject(new Error('The public connection did not answer the protocol check.'));
      }
    }, { once: true });
    socket.addEventListener('error', () => {
      cleanup();
      reject(new Error('The public address did not accept the game connection.'));
    }, { once: true });
    socket.addEventListener('close', () => {
      signal.removeEventListener('abort', abort);
      reject(new Error('The public game connection closed during verification.'));
    }, { once: true });
    signal.addEventListener('abort', abort, { once: true });
    if (signal.aborted) abort();
  });
}

export function openTunnel({ executable, serverUrl, lobbyId, onStage, onDisconnect }) {
  const cancellation = new AbortController();
  const signal = AbortSignal.any([cancellation.signal, AbortSignal.timeout(65_000)]);
  const process = spawn(executable, ['tunnel', '--no-autoupdate', '--protocol', 'http2', '--url', serverUrl], {
    stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true
  });
  let connected = false;
  let stopped = false;
  const finished = new Promise(done => {
    process.once('close', done);
  });
  const candidate = new Promise((resolve, reject) => {
    let output = '';
    const capture = chunk => {
      output = (output + chunk.toString()).slice(-8_000);
      const address = /https:\/\/[a-z0-9-]+\.trycloudflare\.com\b/.exec(output)?.[0];
      if (address) resolve(address);
    };
    process.stdout.on('data', capture);
    process.stderr.on('data', capture);
    process.once('error', error => reject(new Error('Could not start cloudflared. ' + error.message)));
    process.once('close', () => {
      const error = new Error('The Cloudflare process stopped before providing a public address.');
      reject(error);
      if (!connected && !stopped) cancellation.abort(error);
      if (connected && !stopped) onDisconnect();
    });
    signal.addEventListener('abort', () => reject(signal.reason), { once: true });
  });

  async function stop() {
    if (stopped) return finished;
    stopped = true;
    cancellation.abort(new Error('Hosting cancelled.'));
    if (process.exitCode === null) process.kill();
    await finished;
  }

  const verified = (async () => {
    try {
      const address = await candidate;
      onStage('verifying', 'Checking the public room address and game connection…');
      let failure;
      while (!signal.aborted) {
        const attempt = AbortSignal.any([signal, AbortSignal.timeout(5_000)]);
        try {
          const health = await fetch(address + '/health', { signal: attempt, redirect: 'error' });
          if (!health.ok) throw new Error('The public address is still starting.');
          const identity = await health.json();
          if (identity.game !== 'catandmouse' || identity.roomId !== lobbyId || identity.version !== PROTOCOL_VERSION) {
            throw new Error('The public address reached a different game or room.');
          }
          await probeSocket(address, lobbyId, attempt);
          if (process.exitCode !== null) throw new Error('The public connection closed.');
          connected = true;
          return address;
        } catch (error) {
          failure = error;
        }
        await delay(900, undefined, { signal }).catch(() => {});
      }
      throw failure ?? signal.reason;
    } catch (error) {
      await stop();
      throw error;
    }
  })();
  return { verified, stop };
}
