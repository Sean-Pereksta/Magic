import {fresh,finite,living,distance,hash,runKey,trapWindow,usableTrap} from './core.mjs';
export const $=id=>document.getElementById(id);
export function text(node,value){if(node&&node.textContent!==String(value))node.textContent=String(value);}
export function element(tag,className='',content=''){const node=document.createElement(tag);node.className=className;node.textContent=content;return node;}
export const color=id=>['#6ab8ff','#bb9bff','#6ee7b7','#fb9bd3','#ffa96b','#72e5e5','#d5eb89','#b9c9f5'][hash(id)%8];
export function roster(session){return [...session.players].filter(([,p])=>!['dm','viewer'].includes(p.role)&&p.ready!==false&&(!p.runId||p.runId===runKey(session.s))).sort(([a,pa],[b,pb])=>Number(b===session.s?.carrierId)-Number(a===session.s?.carrierId)||Number(fresh(pb,session.now()))-Number(fresh(pa,session.now()))||a.localeCompare(b)).slice(0,8);}
export function playerStatus(p,session){const now=session.now();return !fresh(p,now)?'Offline':p.hidden?'Away':p.alive===false?`Respawn ${Math.max(0,Math.ceil((finite(p.deadUntil)-now)/1000))}s`:finite(p.protectedUntil)>now?'Protected':'Alive';}
export function crownLabel(session){const s=session.s;if(!s)return 'Waiting for dungeon';if(s.phase==='ended')return s.winner==='players'?'Players escaped with the crown':'Dungeon Master wins';return s.carrierId?`👑 ${s.carrierName||session.players.get(s.carrierId)?.name||'Player'} HAS THE CROWN`:s.crown?'👑 Crown on the ground':'👑 Crown not placed';}
export function timeLabel(s,now){if(!s)return '—';if(s.phase==='ended')return 'Round over';const left=Math.max(0,finite(s.phase==='setup'?s.setupDeadline:s.endAt)-now);return s.phase==='setup'?`Setup ${Math.ceil(left/1000)}s`:`${Math.floor(left/60000)}:${String(Math.floor(left/1000)%60).padStart(2,'0')}`;}
export function updateHUD(session,origin=null){
  text($('connection'),session.status);$('connection')?.classList.toggle('warning',!session.synchronized);
  text($('timer'),timeLabel(session.s,session.now()));text($('crown-banner'),crownLabel(session));
  $('crown-banner')?.classList.toggle('carried',!!session.s?.carrierId);
  const panel=$('party');if(!panel)return;
  const seen=new Set();
  for(const [id,p] of roster(session)){
    seen.add(id);let row=[...panel.children].find(n=>n.dataset.id===id);
    if(!row){row=element('div','party-row');row.dataset.id=id;row.append(element('i','party-dot'),element('span','party-name'),element('span','party-status'));panel.append(row);}
    const carrier=session.s?.carrierId===id,online=fresh(p,session.now());
    row.classList.toggle('carrier',carrier);row.classList.toggle('stale',!online||p.hidden);
    row.children[0].style.background=carrier?'#ffd568':color(id);
    text(row.children[1],`${carrier?'👑 ':''}${p.name||'Player'}${id===session.uid?' (you)':''}`);
    const suffix=origin&&id!==session.uid&&online?` · ${Math.round(distance(origin,p))}m`:'';
    text(row.children[2],playerStatus(p,session)+suffix);
  }
  for(const row of [...panel.children])if(!seen.has(row.dataset.id))row.remove();
}
export function notifications(session){
  let lastEvent='',lastNotice='';let timer;
  const show=value=>{if(!value)return;const node=$('notice');text(node,value);node.hidden=false;clearTimeout(timer);timer=setTimeout(()=>node.hidden=true,4200);};
  const off=session.subscribe(()=>{const event=`${runKey(session.s)}:${session.s?.lastEventAt}`;if(event!==lastEvent){lastEvent=event;show(session.s?.lastEvent);}if(session.notice!==lastNotice){lastNotice=session.notice;show(session.notice);}});
  return()=>{off();clearTimeout(timer);};
}
export function mapLayout(canvas,maze){const pad=18,scale=Math.min((canvas.width-pad*2)/(maze.w*maze.size),(canvas.height-pad*2)/(maze.h*maze.size+9));return {scale,x0:(canvas.width-maze.w*maze.size*scale)/2,z0:pad,to:p=>({x:(canvas.width-maze.w*maze.size*scale)/2+(p.x-maze.ox)*scale,y:pad+(p.z-maze.oz)*scale})};}
let cachedMaze=null,cachedCanvas=null;
export function drawMap(canvas,session,{dungeon=false,origin=null}={}){
  const maze=session.maze;if(!maze)return;const ctx=canvas.getContext('2d'),layout=mapLayout(canvas,maze),{to,scale}=layout,now=session.now();
  ctx.clearRect(0,0,canvas.width,canvas.height);ctx.fillStyle='#101a2d';ctx.fillRect(0,0,canvas.width,canvas.height);
  if(dungeon){
    if(cachedMaze!==maze||cachedCanvas?.width!==canvas.width){cachedMaze=maze;cachedCanvas=document.createElement('canvas');cachedCanvas.width=canvas.width;cachedCanvas.height=canvas.height;const c=cachedCanvas.getContext('2d');c.fillStyle='#8196b5';for(const b of maze.boxes){const p=to({x:b.minX,z:b.minZ});c.fillRect(p.x,p.y,Math.max(1,b.sx*scale),Math.max(1,b.sz*scale));}}
    ctx.drawImage(cachedCanvas,0,0);
    for(const [id,t] of session.traps){if(!usableTrap(t,session.s,now))continue;const e=session.enemies[id],pos=to(e||t),armed=now>=trapWindow(t,session.s).armedAt;ctx.globalAlpha=armed?1:.45;ctx.fillStyle={fire:'#ff835e',fog:'#a0b2ca',ghost:'#8de9f7',demon:'#fb799b'}[t.type];ctx.beginPath();ctx.arc(pos.x,pos.y,Math.max(3,scale*.65),0,Math.PI*2);ctx.fill();ctx.globalAlpha=1;}
  }
  const entry=to(maze.spawn());ctx.fillStyle='#67e8a5';ctx.fillRect(entry.x-4,entry.y-3,8,6);
  for(const [id,p] of roster(session)){
    if(!fresh(p,now))continue;const pos=to(origin&&id===session.uid?origin:p);ctx.globalAlpha=p.alive===false||p.hidden?.4:1;
    if(dungeon&&living(p,now,session.s)){ctx.strokeStyle='#df71805c';ctx.beginPath();ctx.arc(pos.x,pos.y,maze.size*2*scale,0,Math.PI*2);ctx.stroke();}
    ctx.fillStyle=session.s?.carrierId===id?'#ffd568':color(id);ctx.beginPath();ctx.arc(pos.x,pos.y,id===session.uid?4.5:3.5,0,Math.PI*2);ctx.fill();
    if(dungeon){ctx.font='11px system-ui';ctx.textAlign='center';ctx.fillStyle='#f3f7ff';ctx.fillText(p.name||'Player',pos.x,pos.y+13);}
    ctx.globalAlpha=1;
  }
  const holder=session.players.get(session.s?.carrierId),crown=session.s?.carrierId?(holder&&fresh(holder,now)?holder:null):session.s?.crown;
  if(crown){const pos=to(origin&&session.s.carrierId===session.uid?origin:crown);ctx.strokeStyle='#ffd568';ctx.lineWidth=2;ctx.beginPath();ctx.arc(pos.x,pos.y,8,0,Math.PI*2);ctx.stroke();ctx.fillStyle='#ffd568';ctx.font=dungeon?'18px system-ui':'13px system-ui';ctx.textAlign='center';ctx.fillText('♛',pos.x,pos.y-9);}
}
export function fatal(error){console.error(error);const panel=$('screen');if(panel){panel.hidden=false;panel.replaceChildren(element('h1','','Unable to join the dungeon'),element('p','',error.message||String(error)));const retry=element('button','','Reconnect');retry.onclick=()=>location.reload();panel.append(retry);}text($('connection'),'Connection issue');}
export function lifecycle(session,dispose){const close=()=>{session.stop();dispose?.();};window.addEventListener('pagehide',close,{once:true});window.addEventListener('pageshow',event=>{if(event.persisted)location.reload();});}
