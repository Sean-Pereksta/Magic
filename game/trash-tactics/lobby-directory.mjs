import {randomBytes,timingSafeEqual} from 'node:crypto';
import {MAP_BRIEFS} from './skirmish-menu.js';
import {cleanName} from './online-rules.mjs';

const HEARTBEAT=15_000, LEASE=50_000, MAX_PUBLIC_HOSTS=75;
const codePattern=/^[A-F0-9]{10}$/;
const validKey=key=>typeof key==='string' && /^[a-zA-Z0-9_-]{32}$/.test(key);
const matches=(a,b)=>validKey(a) && validKey(b) && timingSafeEqual(Buffer.from(a),Buffer.from(b));

export function quickTunnelOrigin(value) {
  try {
    const url=new URL(value);
    if (url.protocol==='https:' && !url.port && !url.username && !url.password &&
        url.pathname==='/' && !url.search && !url.hash &&
        /^[a-z0-9]+(?:-[a-z0-9]+)*\.trycloudflare\.com$/.test(url.hostname)) return url.origin;
  } catch {}
  return null;
}

async function readJson(stream,maxBytes) {
  const chunks=[];
  let length=0;
  for await (const chunk of stream) {
    length+=chunk.length;
    if (length>maxBytes) throw new Error('Payload limit');
    chunks.push(Buffer.from(chunk));
  }
  return JSON.parse(Buffer.concat(chunks).toString());
}

export function attachDirectory({community,trusted,directoryUrl,openRooms}) {
  const entries=new Map(), checking=new Set(), recentlyChecked=new Map();
  const publishKey=randomBytes(24).toString('base64url');
  let publicOrigin='', publication='offline', pending, busy=false, refresh=false, advertised=false, stopped=false;

  async function write(method,timeout) {
    const response=await fetch(directoryUrl,{
      method,headers:{'Content-Type':'application/json'},body:JSON.stringify({origin:publicOrigin,key:publishKey}),
      signal:AbortSignal.timeout(timeout),redirect:'error'
    });
    await response.body?.cancel();
    if (!response.ok) throw new Error('Directory rejected listing');
  }
  async function publish() {
    if (!community || !publicOrigin || stopped) return;
    if (busy) { refresh=true; return; }
    if (!openRooms().length && !advertised) { publication='ready'; return; }
    busy=true;
    try {
      const method=openRooms().length ? 'POST' : 'DELETE';
      await write(method,6000);
      advertised=method==='POST';
      publication=advertised ? 'listed' : 'ready';
    } catch { publication='invite-only'; }
    finally {
      busy=false;
      if (refresh) { refresh=false; schedule(); }
    }
  }
  function schedule() {
    if (!community || !publicOrigin || stopped) return;
    clearTimeout(pending);
    pending=setTimeout(()=>void publish(),400);
    pending.unref();
  }
  const heartbeat=setInterval(()=>void publish(),HEARTBEAT);
  heartbeat.unref();

  async function handle(req,res,url,json) {
    const method=req.method, now=Date.now();
    if (url.pathname==='/api/directory-proof') {
      if (!community || method!=='GET' || !matches(req.headers['x-trash-listing-key'],publishKey)) return json(res,404,{error:'Not found.'});
      return json(res,200,{rooms:openRooms()});
    }
    for (const [origin,entry] of entries) if (entry.expires<=now) entries.delete(origin);
    for (const [origin,at] of recentlyChecked) if (now-at>LEASE) recentlyChecked.delete(origin);
    if (method==='GET') return json(res,200,{rooms:[...openRooms(),...Array.from(entries.values(),entry=>entry.room)]});
    if (!trusted || !['POST','DELETE'].includes(method)) return json(res,405,{error:'Method unavailable.'});
    if (!req.headers['content-type']?.startsWith('application/json') || Number(req.headers['content-length'])>512) return json(res,400,{error:'Invalid listing.'});
    let data;
    try {data=await readJson(req,512);} catch {return json(res,400,{error:'Invalid listing.'});}
    const origin=quickTunnelOrigin(data?.origin);
    if (!origin || !validKey(data?.key)) return json(res,400,{error:'Invalid listing.'});
    if (method==='DELETE') {
      if (matches(entries.get(origin)?.key,data.key)) entries.delete(origin);
      return json(res,200,{ok:true});
    }
    if ((!entries.has(origin) && entries.size>=MAX_PUBLIC_HOSTS) || checking.size>=6 ||
        (!recentlyChecked.has(origin) && recentlyChecked.size>=MAX_PUBLIC_HOSTS*2) ||
        checking.has(origin) || now-(recentlyChecked.get(origin)||0)<2500) return json(res,429,{error:'Try again at the next heartbeat.'});
    checking.add(origin);
    recentlyChecked.set(origin,now);
    try {
      const response=await fetch(origin+'/api/directory-proof',{
        headers:{'X-Trash-Listing-Key':data.key},redirect:'error',signal:AbortSignal.timeout(4000)
      });
      if (!response.ok) { await response.body?.cancel(); throw new Error('Proof failed'); }
      const proof=await readJson(response.body,3072);
      const room=proof?.rooms?.[0];
      if (!room || typeof room.id!=='string' || !codePattern.test(room.id) || !Object.hasOwn(MAP_BRIEFS,room.map) ||
          room.count!==1 || room.capacity!==2 || room.ranked) throw new Error('Lobby unavailable');
      entries.set(origin,{key:data.key,expires:Date.now()+LEASE,room:{
        id:room.id,name:cleanName(room.name) || 'Community lobby',map:room.map,count:1,capacity:2,ranked:false,
        community:true,url:origin+'/?room='+room.id
      }});
      return json(res,200,{ok:true});
    } catch {
      return json(res,400,{error:'The host could not prove an open lobby is available.'});
    } finally {checking.delete(origin);}
  }
  return {
    handle,schedule,status:()=>publication,
    setPublicOrigin(origin) {publicOrigin=origin;publication='ready';schedule();},
    async stop() {
      stopped=true;
      clearTimeout(pending);
      clearInterval(heartbeat);
      if (advertised) {try {await write('DELETE',1500);} catch {}}
    }
  };
}
