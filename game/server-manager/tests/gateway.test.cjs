'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { createGateway, snapshot, responseHeaders } = require('../host/gateway.cjs');
const { advertise, runHost } = require('../host/supervisor.cjs');

function socketPath() {
  const id = 'catnmice-' + randomUUID();
  return process.platform === 'win32' ? '\\\\.\\pipe\\' + id : path.join(os.tmpdir(), id + '.sock');
}
function listen(server, pipe) {
  return new Promise((resolve, reject) => server.once('error', reject).listen(pipe, resolve));
}

test('public and private joins, native HTTP and WebSocket admission', async t => {
  const nativePipe = socketPath(), gatewayPipe = socketPath(), publicPipe = socketPath(), signalPipe = socketPath();
  const native = http.createServer((req, res) => res.end(JSON.stringify({ host: req.headers.host,
    cookie: req.headers.cookie || '', origin: req.headers.origin })));
  native.on('upgrade', (req, peer) => {
    peer.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n');
    peer.once('data', data => peer.end(data));
    peer.on('error', () => {});
  });
  await listen(native, nativePipe);
  t.after(() => new Promise(resolve => native.close(resolve)));
  let players = 1;
  const options = { adapter: { origin: 'http://127.0.0.1:4567', getStatus: () => ({ phase: 'waiting', capacity: 2, players }),
    joinPath: () => '/play?round=7#native' }, gameId: 'cat', hostId: randomUUID(), name: 'Mouse house', getCode: () => '0123' };
  const gateway = createGateway({ ...options, visibility:'code' });
  const publicGateway = createGateway({ ...options, visibility:'public' });
  const signalGateway = createGateway({ ...options, gameId:'lastsignal', visibility:'code' });
  await listen(gateway.server, gatewayPipe);
  await listen(publicGateway.server, publicPipe);
  await listen(signalGateway.server, signalPipe);
  t.after(() => Promise.all([gateway.close(), publicGateway.close(), signalGateway.close()]));
  // Windows pipes and Unix sockets exercise the same HTTP/Upgrade streams without requiring an open TCP port.
  const request = http.request.bind(http);
  t.mock.method(http, 'request', (opts, cb) => {
    assert.equal(opts.hostname, '127.0.0.1');
    assert.equal(Number(opts.port), 4567);
    return request({ ...opts, socketPath: nativePipe, agent:false }, cb);
  });
  function call(url, { method='GET', body, headers={}, pipe=gatewayPipe }={}) {
    return new Promise((resolve, reject) => {
      const req = request({ socketPath: pipe, path: url, method, agent:false, headers:{
        Host:'laptop.test', ...(body && { 'Content-Type':'application/json' }), ...headers } }, res => {
        let data='';
        res.setEncoding('utf8').on('data', chunk => { data += chunk; });
        res.on('end', () => resolve({ status:res.statusCode, headers:res.headers, data }));
      });
      req.on('error', reject).end(body && JSON.stringify(body));
    });
  }
  const fields = JSON.parse((await call('/__catnmice/status')).data);
  assert.equal(fields.hostId, options.hostId);
  assert.equal(fields.joinable, true);
  assert.equal(Object.hasOwn(fields, 'code'), false);
  const publicJoin = await call('/__catnmice/join?name=Jay', { pipe:publicPipe });
  assert.equal(publicJoin.headers.location, '/play?round=7&name=Jay#native');
  assert.equal((await call('/__catnmice/join')).status, 200);
  assert.equal((await call('/play')).status, 403);
  assert.equal((await call('/__catnmice/access',{ method:'POST',body:{ code:'123' } })).status, 403);
  assert.equal((await call('/__catnmice/access',{ method:'POST',body:{ code:'0123' },headers:{Origin:'https://other.test'} })).status, 403);
  const granted = await call('/__catnmice/access',{ method:'POST',body:{ code:'0123' } });
  assert.equal(granted.status, 204);
  assert.match(granted.headers['set-cookie'][0], /HttpOnly/);
  const cookie = granted.headers['set-cookie'][0].split(';')[0];
  const headers = { Cookie:cookie + '; local=native', Origin:'http://laptop.test' };
  assert.equal((await call('/__catnmice/join?name=Jules',{headers})).headers.location, '/play?round=7&name=Jules#native');
  assert.deepEqual(JSON.parse((await call('/play',{headers})).data),
    {host:'127.0.0.1:4567',cookie:'local=native',origin:'http://127.0.0.1:4567'});
  assert.equal((await call('/api/desktop/shutdown',{headers,method:'POST'})).status, 404);
  players = 2;
  assert.equal((await call('/__catnmice/join',{headers})).status, 409);
  function upgrade(headers, url='/socket', pipe=gatewayPipe) {
    return new Promise((resolve, reject) => {
      const req = request({ socketPath:pipe,path:url,agent:false,headers:{
        Host:'laptop.test', Connection:'Upgrade', Upgrade:'websocket', ...headers } });
      req.on('error',reject);
      req.on('response', res => { res.resume(); resolve(res.statusCode); });
      req.on('upgrade', (_,socket) => {
        socket.on('error',reject).once('data', data => { socket.destroy(); resolve(data.toString()); });
        socket.write('frame bytes');
      });
      req.end();
    });
  }
  assert.equal(await upgrade({}), 403);
  assert.equal(await upgrade({...headers,Origin:'https://other.test'}), 403);
  assert.equal(await upgrade(headers,'/control'), 403);
  assert.equal(await upgrade(headers), 'frame bytes');
  const signalAccess = await call('/__catnmice/access', {pipe:signalPipe,method:'POST',body:{code:'0123'}});
  const signalClient = {Cookie:signalAccess.headers['set-cookie'][0].split(';')[0]};
  for (const path of ['/ws', '/mobile/ws']) {
    assert.equal(await upgrade({},path,signalPipe),403);
    assert.equal(await upgrade(signalClient,path,signalPipe),'frame bytes');
    assert.equal(await upgrade({...signalClient,Origin:'https://other.test'},path,signalPipe),403);
  }
  assert.equal(await upgrade(signalClient,'/control',signalPipe),403);
});

test('a stale listing cannot promise a locked or full seat', () => {
  const state = { players:1, capacity:2, phase:'playing' };
  assert.equal(snapshot(state).joinable, false);
  assert.equal(snapshot({...state,joinable:true}).joinable, true);
  assert.equal(snapshot({...state,players:2,joinable:true}).joinable, false);
  assert.equal(snapshot({...state,phase:'finished',joinable:true}).joinable, false);
  assert.throws(() => snapshot({...state,players:-1}));
});

test('Last Signal mobile socket policy uses the public tunnel or LAN address', () => {
  const headers = { 'content-security-policy': "connect-src 'self' ws://127.0.0.1:1234; frame-ancestors 'none'" };
  function policy(host) {
    return responseHeaders('lastsignal', { headers:{host}, socket:{} }, headers, '127.0.0.1:1234')['content-security-policy'];
  }
  assert.match(policy('signal.trycloudflare.com'), / wss:\/\/signal\.trycloudflare\.com;/);
  assert.match(policy('192.168.0.2:8795'), / ws:\/\/192\.168\.0\.2:8795;/);
  assert.equal(policy("evil;'"), headers['content-security-policy']);
});

test('a successful listing adopts its reserved code and releases its lease on stop', async () => {
  const stop = new AbortController();
  const lease = {code:'0010',updateTime:'revision2'};
  let activeCode = '0001', released;
  const directory = {
    async reserve(record, preferred) {
      assert.equal(preferred, '0001'); assert.equal(record.hostId, 'host'); return lease;
    },
    async release(value) { released = value; }
  };
  await assert.rejects(advertise({ config:{name:'Server',visibility:'code'}, directory, hostId:'host',
    getStatus:async()=>({phase:'waiting',players:0,capacity:4}),getCode:()=>activeCode,
    setCode:next=>{activeCode=next;}, origin:'https://a.trycloudflare.com',signal:stop.signal,
    report:status=>{ assert.equal(status,'online'); stop.abort(); } }));
  assert.equal(activeCode, '0010');
  assert.equal(released,lease);
});

test('the supervisor monitors the right game instance, publishes a private code and stops its native game', async t => {
  const pipe = socketPath(), original = net.Server.prototype.listen, stop = new AbortController();
  t.mock.method(net.Server.prototype, 'listen', function (port, bind) {
    assert.equal(port,8793); assert.equal(bind,'0.0.0.0'); return original.call(this,pipe);
  });
  let closed = 0, removed = 0;
  const statuses = [], leased = {code:'0050',updateTime:'r1'}, tunnelAbort = new AbortController();
  const config = {gameId:'trashtactics',name:'Paper Cup',visibility:'code',publicPort:8793,
    packRoot:__dirname,dataDir:__dirname,cloudflared:process.execPath};
  const dependencies = {
    adapterModule:{ async start(opts) {
      assert.equal(opts.name,config.name);
      return {origin:'http://127.0.0.1:4567',getStatus:()=>({phase:'waiting',players:1,capacity:4}),
        joinPath:()=>'/play',close:async()=>{closed++;}};
    }},
    tunnelFactory(opts) {
      assert.equal(opts.probePath,'/__catnmice/status');
      assert.equal(opts.matchesProbe({game:'lastsignal',hostId:opts.probeId}),false);
      assert.equal(opts.matchesProbe({game:config.gameId,hostId:'other host'}),false);
      assert.equal(opts.matchesProbe({game:config.gameId,hostId:opts.probeId}),true);
      return {ready:Promise.resolve('https://example.trycloudflare.com'),signal:tunnelAbort.signal,
        close:()=>tunnelAbort.abort()};
    },
    directoryFactory(opts) {
      assert.equal(opts.listingCollection,'managedGameDirectories/trashtactics/lobbies');
      assert.equal(opts.joinPath,'/__catnmice/join');
      return {async reserve(record) {
        assert.equal(record.players,1); assert.equal(record.joinable,true); assert.equal(record.game,config.gameId);
        assert.equal(opts.listingExtras(record).hostId.stringValue,record.hostId); return leased;
      }, async release(value) { assert.equal(value, leased); removed++; }};
    }
  };
  await assert.rejects(runHost(config,stop.signal,update=>{
    statuses.push(update.status);
    if(update.status==='online'){assert.equal(update.code,leased.code);stop.abort();}
  },dependencies));
  assert.deepEqual(statuses,['starting','online']);
  assert.equal(closed,1); assert.equal(removed,1);
});
