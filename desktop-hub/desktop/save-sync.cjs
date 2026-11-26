const fs = require('node:fs');
const path = require('node:path');
const {firebaseConfig} = require('./firebase-config.mjs');
const {acornHost} = require('./managed-invite.cjs');

const games = new Set(['bramblebound', 'dwarfworld', 'spacetyrants']);
const database = 'projects/' + firebaseConfig.projectId + '/databases/(default)';
const documents = '/v1/' + database + '/documents:';
const sdkModules = new Set(['firebase-app.js', 'firebase-auth.js', 'firebase-firestore.js']
  .map(name => '/firebasejs/10.7.1/' + name));
const channels = new Set([
  '/google.firestore.v1.Firestore/Listen/channel',
  '/google.firestore.v1.Firestore/Write/channel'
]);
const networkProtocols = new Set(['http:', 'https:', 'ws:', 'wss:']);
const configSource = fs.readFileSync(path.join(__dirname, 'firebase-config.mjs'));

function supportsSaveSync(gameId) {
  return games.has(gameId);
}

function matchesParameter(url, name, value) {
  const values = url.searchParams.getAll(name);
  return values.length === 1 && values[0] === value;
}

function allowsCloudRequest(url, {resourceType, method}) {
  if (url.protocol !== 'https:' || url.port || url.username || url.password) return false;
  if (url.hostname === 'www.gstatic.com') {
    return resourceType === 'script' && method === 'GET' && sdkModules.has(url.pathname);
  }
  if (resourceType === 'mainFrame' || resourceType === 'subFrame') return false;
  const rpc = resourceType === 'xhr' || resourceType === 'other';
  const post = method === 'POST' || method === 'OPTIONS';
  if (url.hostname === 'identitytoolkit.googleapis.com' || url.hostname === 'securetoken.googleapis.com') {
    if (!rpc || !post || !matchesParameter(url, 'key', firebaseConfig.apiKey)) return false;
    if (url.hostname === 'securetoken.googleapis.com') return url.pathname === '/v1/token';
    return url.pathname === '/v1/accounts:signUp' || url.pathname === '/v1/accounts:lookup';
  }
  if (url.hostname !== 'firestore.googleapis.com') return false;
  if (channels.has(url.pathname) && matchesParameter(url, 'database', database)) {
    const terminating = url.searchParams.get('TYPE') === 'terminate';
    return (rpc || (terminating && (resourceType === 'ping' || resourceType === 'image'))) &&
      (method === 'GET' || post);
  }
  return rpc && post && (url.pathname === documents + 'batchGet' || url.pathname === documents + 'commit');
}

function configureSaveSession(session, {gameId, origin, sync = false}) {
  const local = new URL(origin);
  if (local.protocol !== 'http:' || local.hostname !== '127.0.0.1' || !local.port) {
    throw new Error('A local game server is required to open the save session.');
  }
  let cloud = sync === true && supportsSaveSync(gameId);
  let acornDirectory = gameId === 'acornrally';
  session.webRequest.onBeforeRequest((details, reply) => {
    let allow = false;
    try {
      const url = new URL(details.url);
      allow = !networkProtocols.has(url.protocol) || url.origin === local.origin ||
        (url.protocol === 'ws:' && url.host === local.host) ||
        (cloud && allowsCloudRequest(url, details)) ||
        (acornDirectory && allowsAcornDirectory(url, details));
    } catch { /* Keep malformed requests blocked. */ }
    reply({cancel: !allow});
  });
  return () => { cloud = false; acornDirectory = false; };
}

function allowsAcornDirectory(url, { resourceType, method }) {
  if (!['xhr','other'].includes(resourceType) || url.search || url.protocol !== 'https:' ||
      url.username || url.password || url.port) return false;
  if (acornHost(url)) return method === 'GET' && url.pathname === '/api/rooms/status';
  if (url.hostname !== 'firestore.googleapis.com') return false;
  const base = '/v1/projects/magic-servers-1ca0c/databases/(default)/documents';
  if (url.pathname === base + ':runQuery') return ['POST','OPTIONS'].includes(method);
  return method === 'GET' && (url.pathname === '/v1/' + database + '/documents/acornRallyService/public' ||
    (url.pathname.startsWith(base + '/') && /^acornRallyLobbies\/\d{4}$/.test(url.pathname.slice(base.length + 1))));
}

// Bramblebound imports the lobby's shared public project configuration by absolute path.
// The bytes are copied from the same Magic revision used to build its game pack.
function serveSaveConfig(gameId, sync, pathname, req, res) {
  if (!sync || gameId !== 'bramblebound' || pathname !== '/lobby/firebase-config.mjs') return false;
  res.writeHead(200, {
    'Content-Type': 'text/javascript; charset=utf-8',
    'Content-Length': configSource.length,
    'X-Content-Type-Options': 'nosniff',
    'Cache-Control': 'no-store'
  });
  res.end(req.method === 'HEAD' ? undefined : configSource);
  return true;
}

module.exports = {supportsSaveSync, configureSaveSession, serveSaveConfig};
