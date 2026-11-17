import { fork, spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { access } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { startTunnel } from './tunnel.mjs';

const gameRoot = fileURLToPath(new URL('../', import.meta.url));
const isWindows = process.platform === 'win32';
const bundledCloudflared = fileURLToPath(new URL('./runtime/cloudflared.exe', import.meta.url));
const controller = new AbortController();
let gameProcess;
let shuttingDown = false;

function stopHosting(error) {
  if (shuttingDown) return;
  shuttingDown = true;
  if (error) {
    process.exitCode = 1;
    console.error(error.message);
  }
  controller.abort(error);
  if (gameProcess?.connected) {
    gameProcess.send({ type: 'trash-tactics:stop' }, error => {
      if (error && gameProcess.exitCode === null) gameProcess.kill();
    });
    setTimeout(() => {
      if (gameProcess.exitCode === null) gameProcess.kill();
    }, 3500).unref();
  }
}

for (const signal of ['SIGINT', 'SIGTERM', ...(isWindows ? ['SIGBREAK'] : [])]) {
  process.once(signal, () => stopHosting());
}

try {
  const port = process.env.TRASH_TACTICS_HOST_PORT ?? '0';
  if (!/^\d{1,5}$/.test(port) || Number(port) > 65535) {
    throw new Error('TRASH_TACTICS_HOST_PORT must be a valid port.');
  }
  const instance = randomBytes(16).toString('hex');
  const hostToken = randomBytes(32).toString('base64url');
  const gameEnv = {
    ...process.env,
    TRASH_TACTICS_COMMUNITY_HOST: '1',
    TRASH_TACTICS_HOST_TOKEN: hostToken,
    TRASH_TACTICS_INSTANCE: instance,
  };
  delete gameEnv.TUNNEL_TOKEN;
  delete gameEnv.GOOGLE_APPLICATION_CREDENTIALS;
  let executable = process.env.TRASH_TACTICS_CLOUDFLARED;
  if (!executable && isWindows) {
    executable = await access(bundledCloudflared).then(() => bundledCloudflared, () => null);
  }
  executable ??= 'cloudflared';
  console.log('Starting the Trash Tactics game server…');
  controller.signal.throwIfAborted();
  gameProcess = fork(fileURLToPath(new URL('../server.mjs', import.meta.url)), [
    '--host', '127.0.0.1', '--port', port,
  ], {
    cwd: gameRoot,
    env: gameEnv,
    execArgv: [],
    stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
    windowsHide: true,
  });
  gameProcess.once('error', stopHosting);
  gameProcess.once('exit', code => {
    if (!shuttingDown) stopHosting(code === 0 ? undefined : new Error('The game server stopped unexpectedly.'));
  });
  const actualPort = await new Promise((resolve, reject) => {
    const readiness = AbortSignal.any([controller.signal, AbortSignal.timeout(15_000)]);
    const listening = message => {
      if (message?.type !== 'trash-tactics:listening' ||
          !Number.isInteger(message.port) || message.port < 1 || message.port > 65535) return;
      gameProcess.off('message', listening);
      resolve(message.port);
    };
    readiness.addEventListener('abort', () => {
      gameProcess.off('message', listening);
      reject(controller.signal.reason ?? new Error('The game server did not start within 15 seconds.'));
    }, { once: true });
    gameProcess.on('message', listening);
  });
  const localUrl = 'http://127.0.0.1:' + actualPort;
  console.log('Opening a Cloudflare connection. The browser opens when guests can reach the game.');
  const tunnel = await startTunnel({
    executable, localUrl, instance, signal: controller.signal, onExit: stopHosting,
  });
  await new Promise((resolve, reject) => {
    gameProcess.send({ type: 'trash-tactics:public-url', url: tunnel.url }, error => error ? reject(error) : resolve());
  });
  console.log('Connection open. Keep this window running while players are in your lobby.');

  const hostUrl = localUrl + '/#host=' + hostToken;
  const openCommand = { win32: 'rundll32.exe', darwin: 'open' }[process.platform] ?? 'xdg-open';
  const openArgs = isWindows ? ['url.dll,FileProtocolHandler', hostUrl] : [hostUrl];
  const browser = spawn(openCommand, openArgs, { stdio: 'ignore', windowsHide: true });
  const showLink = () => console.log('Open this private host link on this computer only:\n' + hostUrl);
  browser.once('error', showLink);
  browser.once('exit', code => { if (code) showLink(); });
} catch (error) {
  if (!shuttingDown) stopHosting(error);
}
