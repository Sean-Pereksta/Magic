import http from 'node:http';
import {createReadStream} from 'node:fs';
import {stat} from 'node:fs/promises';
import {randomBytes} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {attachMultiplayer} from './multiplayer.mjs';
import {attachDirectory,quickTunnelOrigin} from './lobby-directory.mjs';
import {OFFICIAL_ORIGIN,SITE_ORIGINS} from './online-rules.mjs';

const root=fileURLToPath(new URL('.',import.meta.url));
const mime={
  '.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.mjs':'text/javascript; charset=utf-8',
  '.css':'text/css; charset=utf-8','.png':'image/png','.jpg':'image/jpeg','.webp':'image/webp',
  '.gif':'image/gif','.svg':'image/svg+xml','.json':'application/json','.woff2':'font/woff2'
};
const privateModules=/^(?:server|multiplayer|lobby-directory|rankings|online-snapshot)\.mjs$/i;
const json=(res,status,body)=>{
  res.writeHead(status,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});
  res.end(JSON.stringify(body));
};

export function createTrashTacticsServer({
  rankings=null,trusted=false,community=false,managed=false,hostToken='',instance='',
  directoryUrl=new URL('/api/lobbies',OFFICIAL_ORIGIN).href
}={}) {
  const hostInstance=instance || randomBytes(18).toString('base64url');
  const server=http.createServer((req,res)=>{void handle(req,res).catch(()=>{
    if (!res.headersSent) json(res,500,{error:'This request could not be completed.'});
    else res.destroy();
  });});
  server.requestTimeout=12_000;
  server.headersTimeout=10_000;
  const multiplayer=attachMultiplayer(server,{
    rankings:trusted&&!community&&!managed ? rankings : null,community,managed,hostToken,
    publicOrigin:trusted ? OFFICIAL_ORIGIN : '',onRooms:()=>directory.schedule()
  });
  const directory=attachDirectory({community,trusted,directoryUrl,openRooms:multiplayer.openRooms});
  async function handle(req,res) {
    const url=new URL(req.url,'http://localhost');
    const origin=req.headers.origin;
    res.setHeader('X-Content-Type-Options','nosniff');
    res.setHeader('Referrer-Policy','no-referrer');
    if (SITE_ORIGINS.includes(origin)) {
      res.setHeader('Access-Control-Allow-Origin',origin);
      res.setHeader('Vary','Origin');
    }
    if (req.method==='GET' && url.pathname==='/api/health') {
      return json(res,200,{ok:true,instance:hostInstance,community,ranked:trusted&&!!rankings,directory:directory.status()});
    }
    if (url.pathname==='/api/lobbies' || url.pathname==='/api/directory-proof') return directory.handle(req,res,url,json);
    if (req.method==='GET' && ['/api/leaderboard','/api/profile'].includes(url.pathname)) {
      if (!trusted || !rankings) return json(res,503,{error:'Ranked profiles are currently unavailable.'});
      if (url.pathname==='/api/leaderboard') return json(res,200,{players:await rankings.leaderboard(30)});
      const username=url.searchParams.get('username');
      if (!username || username.length>90) return json(res,400,{error:'Enter a Catnmice username.'});
      const player=await rankings.profile(username);
      return json(res,player ? 200 : 404,{player});
    }
    if (req.method!=='GET' && req.method!=='HEAD') return json(res,405,{error:'Method unavailable.'});
    let requested;
    try { requested=decodeURIComponent(url.pathname); } catch { return json(res,400,{error:'Invalid path.'}); }
    const relative=requested==='/' ? 'index.html' : requested.slice(1);
    const pieces=relative.split('/');
    const safe=pieces.every(part=>part && !part.startsWith('.') && /^[a-z0-9_ .-]+$/i.test(part));
    const onlyBrowserFiles=pieces.length===1 ? !privateModules.test(relative) && !/^(?:package|serviceAccount)/i.test(relative) : pieces[0]==='assets';
    const type=mime[path.extname(relative).toLowerCase()];
    if (!safe || !onlyBrowserFiles || !type) return json(res,404,{error:'Not found.'});
    const file=path.join(root,relative);
    const details=await stat(file).catch(()=>null);
    if (!details?.isFile()) return json(res,404,{error:'Not found.'});
    res.writeHead(200,{'Content-Type':type,'Content-Length':details.size,'Cache-Control':pieces[0]==='assets' ? 'public, max-age=3600' : 'no-cache'});
    if (req.method==='HEAD') return res.end();
    const stream=createReadStream(file);
    stream.on('error',()=>res.destroy());
    stream.pipe(res);
  }
  return {
    server,
    getStatus:multiplayer.managedStatus,
    setPublicUrl(value) {
      const origin=community ? quickTunnelOrigin(value) : null;
      if (!origin) return false;
      multiplayer.setPublicOrigin(origin);
      directory.setPublicOrigin(origin);
      return true;
    },
    async close() {
      multiplayer.stop();
      await directory.stop();
      await new Promise(resolve=>server.close(resolve));
    }
  };
}

async function main() {
  const args=process.argv.slice(2);
  const option=name=>{const index=args.indexOf(name);return index<0 ? undefined : args[index+1];};
  const community=process.env.TRASH_TACTICS_COMMUNITY_HOST==='1';
  const trusted=!community && args.includes('--trusted');
  const host=option('--host') || process.env.HOST || '127.0.0.1';
  const port=Number(option('--port') ?? process.env.PORT ?? 57291);
  const rankings=trusted && !args.includes('--casual-only') ? await (await import('./rankings.mjs')).createCatnmiceRankings() : null;
  const app=createTrashTacticsServer({
    rankings,trusted,community,hostToken:process.env.TRASH_TACTICS_HOST_TOKEN,
    instance:process.env.TRASH_TACTICS_INSTANCE,directoryUrl:process.env.TRASH_TACTICS_DIRECTORY_URL
  });
  let closing=false;
  async function close() {
    if (closing) return;
    closing=true;
    await app.close();
    if (process.connected) process.disconnect();
  }
  process.once('SIGINT',close);
  process.once('SIGTERM',close);
  process.once('disconnect',close);
  process.on('message',message=>{
    if (message?.type==='trash-tactics:public-url') app.setPublicUrl(message.url);
    if (message?.type==='trash-tactics:stop') void close();
  });
  app.server.listen(port,host,()=>{
    const actual=app.server.address().port;
    console.log('Trash Tactics listening on '+host+':'+actual);
    process.send?.({type:'trash-tactics:listening',port:actual});
  });
}

if (process.argv[1] && path.resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
  main().catch(error=>{console.error('Trash Tactics could not start.',error.code || error.name);process.exitCode=1;});
}
