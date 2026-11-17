import http from 'node:http';
import net from 'node:net';
import {readFile} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createRequire} from 'node:module';
import {createLedger,guestKey,accountKey} from './ledger.mjs';
import {createEngineFactory} from './engines.mjs';
import {manageMatches} from './matches.mjs';
import {createDesktopAuthorization,createDesktopTokenIssuer} from './desktop-authorization.mjs';

const officialSite='https://www.catnmice.com', publicService='https://hub.catnmice.com';
const error=(status,message)=>Object.assign(new Error(message),{status});
const json=(res,status,data,headers={})=>{res.writeHead(status,{'Content-Type':'application/json','Cache-Control':'no-store','X-Content-Type-Options':'nosniff',...headers});res.end(JSON.stringify(data));};
const cookie=req=>(req.headers.cookie||'').match(/(?:^|;\s*)__Host-catnmiceSeat=([a-zA-Z0-9_-]{43})(?:;|$)/)?.[1];
const safeHeaders={'Cache-Control':'no-store','Referrer-Policy':'no-referrer','X-Content-Type-Options':'nosniff'};
async function body(req) {
  if(!/^application\/json(?:;|$)/i.test(req.headers['content-type']||''))throw error(415,'Send JSON.');
  let size=0,parts=[];for await(const part of req){size+=part.length;if(size>16384)throw error(413,'Request is too large.');parts.push(part);}
  try{const object=JSON.parse(Buffer.concat(parts).toString());if(object&&!Array.isArray(object)&&typeof object==='object')return object;}catch{}
  throw error(400,'Invalid JSON.');
}
const queueRoutes=new Set(['/api/state','/api/ready','/api/synchronized','/api/start','/api/command','/api/heartbeat']);
function proxy(req,res,entry,path) {
  const base=new URL(entry.seat.url);
  // The two games share this host. Each game's original service worker
  // assumes exclusive ownership of the origin and its navigation cache.
  if(path==='/sw.js')return json(res,404,{error:'Ranked matches use live assets.'});
  if(path.startsWith('/api/')){
    if(entry.game!=='bannerlands'||!queueRoutes.has(path)||req.headers.authorization!=='Bearer '+entry.seat.session.token)return json(res,403,{error:'Unavailable in this ranked queue.'});
  }else if(req.method!=='GET'&&req.method!=='HEAD')return json(res,405,{error:'Method not allowed.'});
  const headers={host:base.host};for(const k of ['authorization','content-type','content-length','accept-encoding','if-none-match'])if(req.headers[k])headers[k]=req.headers[k];
  const inject=req.method==='GET'&&(path==='/'||path==='/index.html');
  if(inject){delete headers['accept-encoding'];delete headers['if-none-match'];}
  const upstream=http.request({hostname:'127.0.0.1',port:base.port,path:req.url,method:req.method,headers},reply=>{
    const outgoing={...reply.headers,...safeHeaders};delete outgoing['access-control-allow-origin'];
    if(inject&&reply.statusCode===200){
      delete outgoing.etag;delete outgoing['content-length'];delete outgoing['transfer-encoding'];
      const data=[];reply.on('data',d=>data.push(d));reply.on('end',()=>{res.writeHead(200,outgoing);res.end(Buffer.concat(data).toString().replace('</body>','<script type="module" src="/hub-status.mjs"></script></body>'));});
    }else{res.writeHead(reply.statusCode,outgoing);reply.pipe(res);}
    reply.on('error',()=>res.destroy());
  });
  upstream.on('error',()=>{if(!res.headersSent)json(res,503,{error:'The match ended or is unreachable.'});else res.destroy();});
  res.on('close',()=>{if(!res.writableEnded)upstream.destroy();});req.pipe(upstream);
}
export function createHubService({ledger,identity,engineFactory,mintDesktopToken,serviceOrigin=publicService,siteOrigin=officialSite}) {
  const matches=manageMatches(ledger,engineFactory);
  const desktop=mintDesktopToken&&createDesktopAuthorization({mint:mintDesktopToken,site:siteOrigin});
  const rate=new Map();
  const upkeep=setInterval(()=>{matches.tick().catch(()=>console.error('Ranked processing will retry.'));for(const [key,bucket] of rate)if(bucket.start<Date.now()-60000)rate.delete(key);},500);upkeep.unref();
  const server=http.createServer({requestTimeout:15000,headersTimeout:10000},(req,res)=>{handle(req,res).catch(e=>{if(!res.headersSent)json(res,e.status||503,{error:e.status?e.message:'The service is temporarily unavailable.'});else res.destroy();});});
  async function verified(req,username){
    const token=/^Bearer ([^\s]{1,8192})$/.exec(req.headers.authorization||'')?.[1];
    if(!token)throw error(401,'Sign in to your verified Catnmice account.');
    const who=await identity(token);
    if(username!==undefined&&username!==who.username)throw error(403,'Choose the same verified Catnmice profile.');
    return who;
  }
  async function handle(req,res){
    const url=new URL(req.url,'http://localhost'),path=url.pathname,origin=req.headers.origin;
    const guestTelemetry=path==='/v1/activity'&&(origin==='https://sean-pereksta.github.io'||/^https:\/\/[a-z0-9-]+\.trycloudflare\.com$/.test(origin||''));
    const fromSite=origin===siteOrigin||origin==='https://catnmice.com';
    if(fromSite||origin===serviceOrigin||guestTelemetry){res.setHeader('Access-Control-Allow-Origin',origin);res.setHeader('Vary','Origin');}
    if(req.method==='OPTIONS'){
      if(!fromSite&&origin!==serviceOrigin&&!guestTelemetry)return json(res,403,{error:'Unsupported origin.'});
      res.writeHead(204,{'Access-Control-Allow-Methods':'GET,POST,OPTIONS','Access-Control-Allow-Headers':'Authorization,Content-Type'});return res.end();
    }
    if(req.method==='GET'&&['/hand-off','/hand-off.mjs','/hub-status.mjs'].includes(path)){
      const file=path==='/hand-off'?'hand-off.html':path.slice(1);
      res.writeHead(200,{...safeHeaders,'Content-Type':path==='/hand-off'?'text/html; charset=utf-8':'text/javascript; charset=utf-8','Content-Security-Policy':"default-src 'self'; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'"});
      return res.end(await readFile(new URL('./web/'+file,import.meta.url)));
    }
    if(!path.startsWith('/v1/')){
      const entry=matches.session(cookie(req));if(!entry)return json(res,404,{error:'Enter a ranked match through the Catnmice hub.'});
      if(origin&&origin!==serviceOrigin)return json(res,403,{error:'Unsupported match origin.'});
      return proxy(req,res,entry,path);
    }
    const key=req.headers['cf-connecting-ip']||req.socket.remoteAddress;
    let bucket=rate.get(key);if(!bucket||bucket.start<Date.now()-60000){bucket={start:Date.now(),n:0};rate.set(key,bucket);}
    if(++bucket.n>240)return json(res,429,{error:'Try again shortly.'});
    if(req.method==='GET'){
      if(path==='/v1/health')return json(res,200,{available:true,queues:['bannerlands','catandmouse']});
      if(path==='/v1/desktop/profile'){
        if(origin!==siteOrigin)throw error(403,'Open the official Catnmice website.');
        const player=await verified(req);return json(res,200,{username:player.username});
      }
      if(path==='/v1/counts')return json(res,200,ledger.counts());
      if(path==='/v1/leaderboard')return json(res,200,ledger.leaderboard(url.searchParams.get('game'),url.searchParams.get('season')||undefined));
      if(path==='/v1/history'){const player=await verified(req);return json(res,200,{matches:ledger.history(player.username)});}
      if(path==='/v1/result'){
        const e=matches.session(cookie(req));if(!e)throw error(401,'Ranked session expired.');
        return json(res,200,{receipt:e.receipt||null,username:e.player.username});
      }
    }
    if(req.method!=='POST')throw error(404,'Unknown route.');
    if(origin&&!fromSite&&origin!==serviceOrigin&&!guestTelemetry)throw error(403,'Unsupported origin.');
    const message=await body(req);
    if(path.startsWith('/v1/desktop/')){
      if(!desktop)throw error(503,'Desktop authorization is unavailable.');
      if(path==='/v1/desktop/approve'||path==='/v1/desktop/deny'){
        if(origin!==siteOrigin)throw error(403,'Authorize the launcher on the official Catnmice website.');
        if(typeof message.username!=='string'||!message.username)throw error(400,'Choose a signed-in Catnmice profile.');
        const player=await verified(req,message.username);
        if(path.endsWith('/deny'))return json(res,200,desktop.deny(message.id,message.code));
        if(message.consent!==true)throw error(400,'Confirm authorization for your launcher.');
        return json(res,200,desktop.approve(message.id,message.code,player));
      }
      // The verifier belongs to the native main process, not a web origin.
      if(origin)throw error(403,'Start this action in the Catnmice desktop launcher.');
      if(path==='/v1/desktop/start')return json(res,201,desktop.start(message.challenge,key));
      if(path==='/v1/desktop/cancel')return json(res,200,desktop.cancel(message.id,message.verifier));
      if(path==='/v1/desktop/redeem'){
        const result=await desktop.redeem(message.id,message.verifier);
        return json(res,result.state==='pending'?202:200,result);
      }
      throw error(404,'Unknown desktop action.');
    }
    if(path==='/v1/activity'){
      const guest=/^Guest (.+)$/.exec(req.headers.authorization||'');
      if(guestTelemetry&&!guest)throw error(403,'Account activity uses the official website.');
      const actor=guest?guestKey(guest[1]):accountKey(await verified(req,message.username));ledger.activity(actor,message.games);return json(res,200,{accepted:true});
    }
    if(path==='/v1/link'){const who=await verified(req,message.username);ledger.linkGuest(guestKey(message.guest),accountKey(who));return json(res,200,{linked:true});}
    if(path==='/v1/queue'){const who=await verified(req,message.username);return json(res,200,await matches.enqueue(who,message.game));}
    if(path==='/v1/cancel'){matches.cancel(await verified(req,message.username));return json(res,200,{cancelled:true});}
    if(path==='/v1/claim'){
      if(origin!==serviceOrigin)throw error(403,'Open the official handoff page.');
      const {cookie:token,...client}=matches.claim(message.key);
      return json(res,200,client,{'Set-Cookie':'__Host-catnmiceSeat='+token+'; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=21600'});
    }
    throw error(404,'Unknown route.');
  }
  server.on('upgrade',(req,socket,head)=>{
    const entry=matches.session(cookie(req));
    if(req.url!=='/socket'||req.headers.origin!==serviceOrigin||entry?.game!=='catandmouse')return socket.destroy();
    const target=new URL(entry.seat.url);
    const upstream=net.connect(Number(target.port),'127.0.0.1',()=>{
      const headers=['GET /socket HTTP/1.1','Host: '+target.host,'Origin: http://'+target.host,'Connection: Upgrade','Upgrade: websocket'];
      for(const key of ['sec-websocket-key','sec-websocket-version','sec-websocket-protocol'])if(req.headers[key])headers.push(key+': '+req.headers[key]);
      upstream.write(headers.join('\r\n')+'\r\n\r\n');if(head.length)upstream.write(head);socket.pipe(upstream).pipe(socket);
    });socket.on('error',()=>upstream.destroy());upstream.on('error',()=>socket.destroy());socket.on('close',()=>upstream.destroy());upstream.on('close',()=>socket.destroy());
  });
  return {server,close:async()=>{clearInterval(upkeep);await matches.close();server.closeAllConnections();await new Promise(done=>server.close(done));}};
}
if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url) {
  const {MAGIC_ROOT,TRUTH_ROOT,CATALOG_PATH,HUB_DATABASE}=process.env;
  if(!MAGIC_ROOT||!TRUTH_ROOT||!CATALOG_PATH||!HUB_DATABASE)throw new Error('Set MAGIC_ROOT, TRUTH_ROOT, CATALOG_PATH and HUB_DATABASE.');
  const config=JSON.parse(await readFile(CATALOG_PATH,'utf8'));
  const catalog=(Array.isArray(config)?config:config.games).map(g=>g.id||g.key);
  const lib=await import(pathToFileURL(join(MAGIC_ROOT,'game/trash-tactics/rankings.mjs')).href);
  const requireFromMagic=createRequire(join(resolve(MAGIC_ROOT),'package.json'));
  const {applicationDefault,initializeApp}=requireFromMagic('firebase-admin/app');
  const {getAuth}=requireFromMagic('firebase-admin/auth');
  const {getFirestore}=requireFromMagic('firebase-admin/firestore');
  const app=initializeApp({projectId:'bible-game-246c0',credential:applicationDefault()},'catnmice-official-hub');
  const permissions={auth:getAuth(app),db:getFirestore(app)};
  const rankings=lib.createRankings(permissions);
  const ledger=createLedger(HUB_DATABASE,catalog);
  const gateway=createHubService({ledger,identity:rankings.identity,mintDesktopToken:createDesktopTokenIssuer(permissions),engineFactory:createEngineFactory({bannerlands:join(TRUTH_ROOT,'games/bannerlands'),catEdition:join(MAGIC_ROOT,'game/catandmouse/edition')})});
  gateway.server.listen(Number(process.env.PORT||57292),'127.0.0.1');
  process.once('SIGTERM',async()=>{await gateway.close();ledger.close();});
}
