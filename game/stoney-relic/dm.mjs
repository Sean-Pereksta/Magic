import {RelicSession} from './network.mjs';
import {RULES,chooseSpawn,placementError,energy,usableTrap} from './core.mjs';
import {$,text,element,updateHUD,notifications,drawMap,mapLayout,fatal,lifecycle} from './ui.mjs';
async function main(){
  const qs=new URLSearchParams(location.search),session=new RelicSession({gameId:(qs.get('gameId')||'').trim(),name:qs.get('username')||'Dungeon Master',dm:true});
  let mode='crown',timer=null;const buttons=[];
  for(const [key,label] of [['crown','👑 Crown'],['fire','🔥 Fire'],['fog','🌫 Fog'],['ghost','👻 Ghost'],['demon','😈 Demon']]){
    const b=element('button',key===mode?'selected':'',`${label} · ${RULES.cost[key]||'Free'}`);b.dataset.mode=key;b.setAttribute('aria-pressed',String(mode===key));b.onclick=()=>{mode=key;for(const n of buttons){n.classList.toggle('selected',n===b);n.setAttribute('aria-pressed',String(n===b));}text($('placement-hint'),`${label} selected. Tap a clear map cell or choose Smart placement.`);};$('powers').append(b);buttons.push(b);
  }
  $('ready').onclick=()=>session.startRound();
  $('smart').onclick=async()=>{const type=mode;if(!session.maze)return;const point=chooseSpawn(type,session.s,session.maze,session.players,session.traps,session.now());if(!point){session.notice='No safe spawn is available. Wait for players to move or traps to expire.';session.emit();return;}await session.place(type,point);};
  // One click handler works for mouse, touch and keyboard activation; no duplicate pointerdown placement.
  $('map').addEventListener('click',async event=>{
    const maze=session.maze;if(!maze||!session.amDM)return;const canvas=$('map'),r=canvas.getBoundingClientRect(),x=(event.clientX-r.left)*canvas.width/r.width,y=(event.clientY-r.top)*canvas.height/r.height;
    const layout=mapLayout(canvas,maze),cell=maze.cell({x:maze.ox+(x-layout.x0)/layout.scale,z:maze.oz+(y-layout.z0)/layout.scale});if(cell<0)return;
    const point=maze.center(cell),type=mode,error=placementError(type,point,session.s,maze,session.players,session.traps,session.now());
    if(error){session.notice=error;session.emit();return;}await session.place(type,point);
  });
  function update(){
    updateHUD(session);drawMap($('map'),session,{dungeon:true});
    const s=session.s,available=s?energy(s,session.now()):0,busy=session.actions.get('Placement')?.busy;
    text($('energy-value'),`${Math.floor(available)} / 100`);$('energy-fill').style.width=`${available}%`;
    text($('phase'),s?`${s.phase.toUpperCase()} · ${session.amDM?'Dungeon Master':'Read-only viewer'}`:'Connecting');
    text($('enemy-count'),`${[...session.traps.values()].filter(t=>usableTrap(t,s,session.now())).length} / ${RULES.maxTraps} active traps`);
    $('ready').disabled=!session.amDM||!session.synchronized||s?.phase!=='setup'||session.actions.get('Start round')?.busy;
    $('smart').disabled=!session.amDM||!session.synchronized||busy||!['setup','play'].includes(s?.phase)||(mode==='crown'&&s?.phase!=='setup');
    for(const b of buttons)b.disabled=!session.amDM||busy||(b.dataset.mode==='crown'?s?.phase!=='setup':available<RULES.cost[b.dataset.mode]);
    if(session.synchronized)$('screen').hidden=true;
    if(s?.phase==='ended')text($('placement-hint'),s.winner==='players'?`${s.winnerName||'A player'} escaped with the crown. Players win!`:'Time is up. Dungeon Master wins!');
  }
  const off=notifications(session);lifecycle(session,()=>{clearInterval(timer);off();});
  await session.start();update();timer=setInterval(update,120);
}
main().catch(fatal);
