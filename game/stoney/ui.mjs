import {StoneyRoom,firebaseDriver} from './network.mjs';
import {finite,distance,isOnline,RULES} from './core.mjs';
export const $=id=>document.getElementById(id);
let toastTimer;
export function notify(text){if(!text)return;const el=$('toast');el.textContent=text;el.classList.add('visible');clearTimeout(toastTimer);toastTimer=setTimeout(()=>el.classList.remove('visible'),4500);}
export async function connect(role,onChange=()=>{}){
  const q=new URLSearchParams(location.search);let room;const timeout=setTimeout(()=>{$('netText').textContent='Still connecting — check your connection or reload.';},15000);
  try{const driver=await firebaseDriver();room=new StoneyRoom(driver,{id:q.get('gameId')?.trim(),name:q.get('username')?.trim()||'Player',role,onChange,onEvent:notify,onStatus:text=>{$('netText').textContent=text;$('netText').classList.toggle('warning',!text.startsWith('Online'));}});await room.start();return room;}
  catch(e){room?.close();$('netText').textContent='Unable to connect';notify(e.message);if($('centerMsg')){$('centerMsg').hidden=false;$('centerTitle').textContent='Connection issue';$('centerSub').textContent=e.message;}throw e;}
  finally{clearTimeout(timeout);}
}
export function wireLifecycle(room,cleanup,reset=()=>{}){
  const online=()=>room.setOffline(false),offline=()=>room.setOffline(true),visibility=()=>{reset();room.lastOffer=0;},hide=()=>{cleanup();room.close();clearTimeout(toastTimer);window.removeEventListener('online',online);window.removeEventListener('offline',offline);document.removeEventListener('visibilitychange',visibility);};
  window.addEventListener('online',online);window.addEventListener('offline',offline);document.addEventListener('visibilitychange',visibility);window.addEventListener('pagehide',hide,{once:true});
  window.addEventListener('pageshow',e=>{if(e.persisted)location.reload();});
}
export function updateHUD(room,me){
  const s=room.state,now=room.now();$('meName').textContent=room.name;
  if(!s){$('timerText').textContent='Waiting for DM';$('crownText').textContent='The Dungeon Master is preparing the maze.';return;}
  const remaining=Math.max(0,(s.phase==='setup'?finite(s.setupDeadline):finite(s.endAt))-now);
  $('timerText').textContent=s.phase==='ended'?'Round ended':`${s.phase==='setup'?'Setup · ':''}${Math.floor(Math.ceil(remaining/1000)/60)}:${String(Math.ceil(remaining/1000)%60).padStart(2,'0')}`;
  const crown=s.carrierId?(s.carrierId===room.uid?'👑 YOU HAVE THE CROWN — return through the green entrance':`👑 ${s.carrierName||'A teammate'} HAS THE CROWN — escort them!`):s.phase==='ended'?(s.winner==='players'?`🏆 ${s.winnerName||'Your team'} escaped!`:'The Dungeon Master wins.'):'👑 Find the crown, then escape together through the green entrance.';
  if($('crownText').textContent!==crown)$('crownText').textContent=crown;$('crownText').classList.toggle('held',!!s.carrierId);
  const players=room.players().filter(p=>p.stoneyRole!=='dm').sort((a,b)=>(a.uid===s.carrierId?-1:0)-(b.uid===s.carrierId?-1:0)||String(a.name).localeCompare(String(b.name)));
  const signature=players.map(p=>{const online=isOnline(p,now),dead=p.alive===false,dist=me&&p.uid!==room.uid?`${Math.round(distance(me,p))}m`:'You';return[p.uid,p.name,online,dead,dist,Math.ceil(Math.max(0,finite(p.deadUntil)-now)/1000),p.uid===s.carrierId].join('|');}).join(';');
  if(signature!==updateHUD.signature){updateHUD.signature=signature;const fragment=document.createDocumentFragment();for(const p of players){const row=document.createElement('div');row.className='team-row';if(p.uid===s.carrierId)row.classList.add('crowned');const name=document.createElement('strong');name.textContent=`${p.uid===s.carrierId?'👑 ':''}${p.name}${p.uid===room.uid?' (you)':''}`;const detail=document.createElement('span');detail.textContent=!isOnline(p,now)?'Offline · last known':p.alive===false?`Respawn ${Math.ceil(Math.max(0,finite(p.deadUntil)-now)/1000)}s`:me&&p.uid!==room.uid?`${Math.round(distance(me,p))}m away`:'Alive';row.append(name,detail);fragment.append(row);}if(!players.length){const empty=document.createElement('span');empty.textContent='Waiting for players…';fragment.append(empty);}$('teamList').replaceChildren(fragment);$('teamSummary').textContent=`Team · ${players.filter(p=>isOnline(p,now)).length} online`;}
  if($('energyT')){$('energyT').textContent=`${Math.floor(Math.min(100,Math.max(0,finite(s.dmEnergyBase)+(now-finite(s.dmEnergyStamp,now))/1000*6)))}/100`;}
}
