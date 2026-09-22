import {COST,energy,roundKey,suggestSpawn,defaultCrown} from './core.mjs';
import {makeMap} from './view.mjs';
import {$,connect,notify,wireLifecycle,updateHUD} from './ui.mjs';
let room,map,mapKey='',mode='crown',placing=false,timer;
const buttons=[...document.querySelectorAll('[data-type]')];
function select(type){mode=type;for(const b of buttons){b.classList.toggle('selected',b.dataset.type===type);b.setAttribute('aria-pressed',String(b.dataset.type===type));}$('warnLine').textContent='';}
for(const b of buttons)b.addEventListener('click',()=>select(b.dataset.type));select('crown');
async function place(point){if(placing||!room?.playable()||!point)return;const type=mode;placing=true;$('smartBtn').disabled=true;$('warnLine').textContent='Saving placement…';try{const result=await room.place(type,point);$('warnLine').textContent=result?'Placement saved.':(room.lastError?.message||'Placement was not saved. Wait for sync, then try again.');}finally{placing=false;$('smartBtn').disabled=false;}}
// One pointer-up handler for touch and mouse: no duplicate click/pointer placements.
let press=null;$('map').addEventListener('pointerdown',e=>{if(e.isPrimary===false)return;press={id:e.pointerId,x:e.clientX,y:e.clientY};});$('map').addEventListener('pointerup',e=>{if(!press||press.id!==e.pointerId)return;const start=press;press=null;if(Math.hypot(e.clientX-start.x,e.clientY-start.y)>15)return;const point=map?.cell(e.clientX,e.clientY);if(point)place(point);});$('map').addEventListener('pointercancel',()=>{press=null;});
$('smartBtn').addEventListener('click',()=>{if(!room?.state||!room.maze)return;const point=mode==='crown'?defaultCrown(room.maze):suggestSpawn(room.maze,room.state,room.players(),room.activeTraps(),room.now(),mode);if(!point){$('warnLine').textContent='No fair spawn position is available. Let players move or existing traps expire.';return;}place(point);});
$('readyBtn').addEventListener('click',()=>room?.ready());
async function boot(){try{room=await connect('dm');let lastDraw=0;timer=setInterval(()=>{room.tick(.05);if(performance.now()-lastDraw<100)return;lastDraw=performance.now();updateHUD(room);if(room.maze){if(mapKey!==roundKey(room.state)){map=makeMap($('map'),room.maze);mapKey=roundKey(room.state);}map.draw(room);}const s=room.state;$('readyBtn').disabled=!room.playable()||s?.phase!=='setup'||room.busy.has('ready');for(const b of buttons)b.disabled=!room.playable()||placing||!s||s.phase==='ended'||(b.dataset.type==='crown'?s.phase!=='setup':energy(s,room.now())<COST[b.dataset.type]);},50);wireLifecycle(room,()=>clearInterval(timer));}catch(e){console.error(e);notify(e.message);$('warnLine').textContent=e.message;}}
boot();
