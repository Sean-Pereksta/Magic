import {Game,TYPES} from './engine.js';
import {Renderer,drawIcon} from './render.js';

const $=id=>document.getElementById(id), canvas=$('world');
const COMBAT=['rock','paper','scissors'], BUILDINGS=['desk','cabinet','printer'];
const names={desktop:'Desktop Meadow',breakroom:'Breakroom Blitz'};
const descriptions={twig:'A scrappy little worker. Carries 5 trash per trip. Can build your entire base.',rock:'A solid choice. Crushes scissors. Watch out for paper.',paper:'A clean sheet with a mean streak. Wraps rocks. Scissors are trouble.',scissors:'Sharp, speedy, and slightly unhinged. Cuts paper. Avoid rocks.',desk:'Home, sweet headquarters. Twigs return their trash here. Train more workers or expand.',printer:'Print a little trouble. Train your army and research a special ability for each troop.',cabinet:'Room for more mischief. Adds 8 supply to your little empire.'};
const skills={rock:{name:'Launch',icon:'↗',desc:'Leap to a spot in formation',cooldown:11},paper:{name:'Crumple',icon:'◉',desc:'3s invincible · faster · no attacks',cooldown:12},scissors:{name:'Sweep',icon:'✂',desc:'Cut every paper in a short arc',cooldown:10}};
let game,renderer, selected=new Set(),mode=null,abilityType=null,pointer={x:600,y:800},drag=null;
let paused=false,started=false,inLobby=true,chosenMap='desktop',chosenDifficulty='normal',chosenPlayers=2;
let groups={},lastGroup={key:'',at:0},lastFrame=performance.now(),uiTick=0,signature='',endShown=false,toastTimeout;
let keys=new Set(), pointerState=null,touches=new Map(),pinch=null,guideWasPaused=false;
let muted=false;try{muted=localStorage.getItem('trash-tactics-muted')==='true';}catch{}
let audioContext,lastHitSound=0;
function sound(kind='click'){
 if(muted)return;
 try{
  audioContext??=new (window.AudioContext||window.webkitAudioContext)();
  if(audioContext.state==='suspended')audioContext.resume();
  const now=audioContext.currentTime;
  if(kind==='hit'&&now-lastHitSound<.11)return;if(kind==='hit')lastHitSound=now;
  const tones=kind==='victory'?[523,659,784,1046]:kind==='defeat'?[330,294,220]:[kind==='hit'?145:kind==='deposit'?820:kind==='select'?410:kind==='ability'?250:570];
  tones.forEach((freq,i)=>{const o=audioContext.createOscillator(),v=audioContext.createGain(),t=now+i*.13;o.type=kind==='hit'?'triangle':'sine';o.frequency.setValueAtTime(freq,t);o.frequency.exponentialRampToValueAtTime(freq*.7,t+.13);v.gain.setValueAtTime(.0001,t);v.gain.exponentialRampToValueAtTime(kind==='hit'?.025:.055,t+.008);v.gain.exponentialRampToValueAtTime(.0001,t+.15);o.connect(v).connect(audioContext.destination);o.start(t);o.stop(t+.17);});
 }catch{}
}
function updateSound(){ $('soundButton').textContent=muted?'♪̸':'♫';$('soundButton').setAttribute('aria-pressed',String(!muted));$('soundButton').title=muted?'Enable sound':'Mute sound';}
function toast(message){if(!message)return;$('toast').textContent=message;$('toast').hidden=false;clearTimeout(toastTimeout);toastTimeout=setTimeout(()=>$('toast').hidden=true,3200);}
function onEvent(e){
 if(e.type==='toast'&&(!('team'in e)||e.team===0))toast(e.message);
 if(e.type==='hit'&&!paused&&!inLobby)sound('hit');
 if(e.type==='deposit'&&e.team===0&&Math.random()<.13&&!paused&&!inLobby)sound('deposit');
}
function resetGame(){game=new Game({map:chosenMap,difficulty:chosenDifficulty,players:chosenPlayers,onEvent});selected.clear();groups={};mode=null;abilityType=null;drag=null;endShown=false;signature='';if(renderer){renderer.game=game;renderer.resize();}else renderer=new Renderer(canvas,game);renderer.camera.zoom=window.innerWidth<761?.72:.94;const d=game.entities.find(e=>e.team===0&&e.type==='desk');renderer.centerOn(d.x,d.y+40);$('matchLabel').textContent=names[chosenMap].toUpperCase()+' · '+chosenPlayers+' PLAYERS · '+chosenDifficulty.toUpperCase();$('operationName').textContent=chosenMap==='desktop'?'The great desk takeover':'A little breakroom business';updateUI(true);}
function start(){sound();resetGame();started=true;inLobby=false;paused=false;lastFrame=performance.now();$('lobby').hidden=true;$('pauseOverlay').hidden=true;$('endOverlay').hidden=true;const desk=game.entities.find(e=>e.team===0&&e.type==='desk');selected=new Set([desk.id]);updateUI(true);toast('Your twigs are gathering. Train a squad, then give the rival a little trouble.');canvas.focus({preventScroll:true});}
function setMode(next,type=null){mode=next;abilityType=type;const label=BUILDINGS.includes(mode)?`Place ${TYPES[mode].label.toLowerCase()} · choose clear ground`:mode==='ability'?`${type?skills[type].name:'Special abilities'} · choose a target or direction`:mode==='attack'?'Attack move · choose a spot to fight toward':'';$('modeBanner').textContent=label+(label?' · X cancel · Esc menu':'');$('modeBanner').hidden=!label;canvas.style.cursor=next?'crosshair':'default';updateUI(true);}
function unselect(){selected.clear();setMode(null);sound('select');}
function ownSelected(){return [...selected].map(id=>game.getEntity(id)).filter(e=>e?.team===0);}
function selectEntities(entities,add=false){if(!add)selected.clear();for(const e of entities)if(e.team===0)selected.add(e.id);setMode(null);sound('select');updateUI(true);}
function centerHome(){const d=game.entities.find(e=>e.team===0&&e.type==='desk'&&e.hp>0);if(d)renderer.centerOn(d.x+90,d.y);}
function selectArmy(){const army=game.entities.filter(e=>e.team===0&&COMBAT.includes(e.type)&&e.hp>0);selectEntities(army);if(!army.length)toast('No troops yet. Select a printer to train your squad.');}
function selectWorkers(){const ws=game.entities.filter(e=>e.team===0&&e.type==='twig'&&e.hp>0&&['idle','stop'].includes(e.order?.type));selectEntities(ws);if(!ws.length)toast('All your twigs are busy. Keep the trash coming!');}
function result(r,success){if(r?.ok){sound();if(success)toast(success);}else{sound('hit');toast(r?.reason||'That command is unavailable.');}updateUI(true);return !!r?.ok;}
function issue(world,hit=null,forceAttack=false){
 const own=ownSelected();if(!own.length)return;
 if(BUILDINGS.includes(mode)){if(result(game.build(own.filter(e=>e.type==='twig').map(e=>e.id),mode,world.x,world.y),'Building started. Your twig will get right to it.'))setMode(null);return;}
 if(mode==='ability'){const ids=own.filter(e=>!abilityType||e.type===abilityType).map(e=>e.id);if(result(game.ability(ids,world.x,world.y))){sound('ability');setMode(null);}return;}
 const troops=own.filter(e=>!TYPES[e.type].building),buildings=own.filter(e=>TYPES[e.type].building);
 if(!mode&&hit?.team===0&&hit.constructing){const worker=troops.filter(e=>e.type==='twig').sort((a,b)=>Math.hypot(a.x-hit.x,a.y-hit.y)-Math.hypot(b.x-hit.x,b.y-hit.y))[0];if(worker){hit.builderId=worker.id;game.command([worker.id],{type:'build',targetId:hit.id});sound();toast('Your twig is finishing construction.');return;}}
 if(!troops.length){for(const b of buildings)if(['desk','printer'].includes(b.type))b.rally={x:world.x,y:world.y,targetId:hit?.type==='trash'?hit.id:null};toast('Rally point set. New units will head here.');renderer.marker={...world,type:'move',life:.8};return;}
 let order;if(mode==='attack'||forceAttack)order={type:'attackMove',...world};else if(hit?.team>0)order={type:'attack',targetId:hit.id,...world};else if(hit?.type==='trash')order={type:'gather',targetId:hit.id,...world};else order={type:'move',...world};
 game.command(troops.map(e=>e.id),order);renderer.marker={...world,type:order.type.startsWith('attack')?'attack':'move',life:.8};sound();setMode(null);
}
function nearestAttack(){const own=ownSelected().filter(e=>COMBAT.includes(e.type));if(!own.length){toast('Select combat troops first.');return;}const center={x:own.reduce((n,e)=>n+e.x,0)/own.length,y:own.reduce((n,e)=>n+e.y,0)/own.length};const enemy=game.entities.filter(e=>e.team>0&&e.hp>0).sort((a,b)=>Math.hypot(a.x-center.x,a.y-center.y)-Math.hypot(b.x-center.x,b.y-center.y))[0];if(enemy){game.command(own.map(e=>e.id),{type:'attackMove',x:enemy.x,y:enemy.y});renderer.marker={x:enemy.x,y:enemy.y,type:'attack',life:.8};sound();setMode(null);}}
function activateAbility(type=null){const units=ownSelected().filter(e=>COMBAT.includes(e.type)&&(!type||e.type===type)&&game.teams[0].upgrades[e.type]);if(!units.length){toast('Research special abilities at a printer first.');return;}if(units.every(e=>e.type==='paper')){result(game.ability(units.map(e=>e.id),units[0].x,units[0].y));sound('ability');return;}setMode('ability',type);}
function icons(root=document){root.querySelectorAll('canvas[data-icon]').forEach(c=>{const size=c.width;drawIcon(c.getContext('2d'),c.dataset.icon,size/2,size/2,size*.8,0,{sprite:true});});}
function icon(type,size=60){return `<canvas data-icon="${type}" width="${size}" height="${size}" aria-hidden="true"></canvas>`;}
function actionCard(type,hotkey,action='train'){const cost=TYPES[type].cost;return `<button class="action-card" data-action="${action}" data-type="${type}" title="${descriptions[type]} Costs ${cost} trash."><span class="hotkey">${hotkey||''}</span>${icon(type,60)}<b>${type==='desk'?'Home desk':TYPES[type].label}</b><span class="cost">♻ ${cost}</span></button>`;}
function queueHtml(e){if(!e.queue?.length)return '';return `<div class="production"><div class="section-label">IN THE QUEUE <small>${e.queue.length} / 6</small></div>${e.queue.map((q,i)=>`<div class="queue-item">${icon(q.type,32)}<div><b>${q.kind==='research'?skills[q.type].name+' research':TYPES[q.type].label}</b><small>${i===0?Math.ceil(q.remaining)+'s':'QUEUED'}</small><div class="queue-bar"><span style="width:${i===0?Math.max(0,100*(1-q.remaining/q.total)):0}%"></span></div></div></div>`).join('')}</div>`;}
function updateUI(force=false){
 const supply=game.getSupply(0),team=game.teams[0];$('trashCount').textContent=Math.floor(team.trash).toLocaleString();$('supplyCount').textContent=`${supply.used} / ${supply.cap}`;$('supplyCount').style.color=supply.used>=supply.cap?'#ca7555':'';
 const seconds=Math.floor(game.time);$('timer').textContent=String(Math.floor(seconds/60)).padStart(2,'0')+':'+String(seconds%60).padStart(2,'0');
 for(const id of selected)if(!game.getEntity(id))selected.delete(id);
 const own=ownSelected(),e=own[0],units=own.filter(e=>!TYPES[e.type].building),workers=units.filter(e=>e.type==='twig'),combat=units.filter(e=>COMBAT.includes(e.type));
 document.querySelector('.sidebar').classList.toggle('empty',!own.length);document.querySelector('.sidebar').classList.toggle('narrow',own.length===1&&e.type==='cabinet');document.querySelector('.sidebar').dataset.selection=own.length>1?(workers.length===own.length?'twig':'squad'):(e?.type||'none');
 const sig=own.map(e=>[e.id,Math.ceil(e.hp),Math.ceil(e.constructing||0),Math.ceil(e.abilityCooldown||0),e.queue?.map(q=>q.type+Math.ceil(q.remaining)).join(',')].join(':')).join('|')+JSON.stringify(team.upgrades)+mode+abilityType;
 if(!force&&sig===signature)return;signature=sig;
 $('clearButton').disabled=!own.length;
 let info='',actions='',roster='';
 if(!e){info=`<div class="selected-heading"><div class="portrait">${icon('twig')}</div><div><h2>Hey, commander.</h2><p>YOUR DESK AWAITS</p></div></div><p class="selection-description">Select a desk to train twigs, a printer for troops, or a twig to start building.</p>`;actions=`<div class="section-label">QUICK START</div><button class="research-card" data-action="selectDesk">${icon('desk',40)}<span><strong>Visit your home desk</strong><small>Train workers · grow your economy</small></span><span class="research-cost">↗</span></button><button class="research-card" style="margin-top:7px" data-action="selectPrinter">${icon('printer',40)}<span><strong>Fire up the printer</strong><small>Train troops · research abilities</small></span><span class="research-cost">↗</span></button><p class="action-note">Little tip: a twig is already collecting from each of five nearby cans. More cans mean more income.</p>`;}
 else{
  const title=own.length>1?`${own.length} ${combat.length===own.length?'troops':workers.length===own.length?'twigs':'selected'}`:TYPES[e.type].label;
  const subtitle=own.length>1?`${combat.length} TROOPS · ${workers.length} TWIGS`:TYPES[e.type].building?'BASE BUILDING':e.type==='twig'?'WORKER · 1 SUPPLY':'COMBAT UNIT · 1 SUPPLY';
  info=`<div class="selected-heading"><div class="portrait">${icon(e.type)}</div><div style="flex:1"><h2>${title}</h2><p>${subtitle}</p>${own.length===1?`<div class="health-meter"><span style="width:${100*e.hp/e.maxHp}%"></span></div>`:''}</div></div><p class="selection-description">${own.length===1?descriptions[e.type]:workers.length===own.length?'Many twigs make light work. Spread them across trash cans or build something great.':'Give the squad a destination. They’ll handle the little details.'}</p>`;
  if(own.length>1)roster=own.slice(0,30).map(e=>`<button class="roster-item" data-unit="${e.id}" title="Select this ${TYPES[e.type].label}">${icon(e.type,40)}</button>`).join('');
  if(e.constructing&&own.length===1)actions=`<div class="section-label">UNDER CONSTRUCTION <small>${Math.ceil(e.constructing)}s</small></div><div class="queue-bar"><span style="width:${100*(1-e.constructing/e.buildTotal)}%"></span></div><p class="action-note">Your twig is putting the pieces together.</p>`;
  else if(e.type==='desk'&&own.length===1)actions=`<div class="section-label">GROW YOUR ECONOMY <small>4s / TWIG</small></div><div class="action-grid">${actionCard('twig','Q')}</div><p class="action-note">New twigs automatically find a trash can. Right click to set a rally point.</p>${queueHtml(e)}`;
  else if(e.type==='printer'&&own.length===1){actions=`<div class="section-label">PRINT SOME TROUBLE <small>5s / TROOP</small></div><div class="action-grid">${actionCard('rock','Q')}${actionCard('paper','W')}${actionCard('scissors','R')}</div><div class="section-label" style="margin-top:23px">A LITTLE EXTRA POWER <small>150 TRASH EACH</small></div><div class="research-grid">${COMBAT.map(type=>{const researched=team.upgrades[type],pending=game.entities.some(b=>b.team===0&&b.queue?.some(q=>q.kind==='research'&&q.type===type));return `<button class="research-card ${researched?'unlocked':''}" data-action="research" data-type="${type}" ${researched||pending?'disabled':''} title="${skills[type].desc}">${icon(type,44)}<span><strong>${skills[type].name}</strong><small>${skills[type].desc}</small></span><span class="research-cost">${researched?'✓':pending?'…':'♻ 150'}</span></button>`;}).join('')}</div>${queueHtml(e)}`;}
  else if(e.type==='cabinet'&&own.length===1)actions='<div class="section-label">A PLACE FOR EVERYONE</div><p class="action-note">Your cabinet gives you 8 more supply. Select a twig to build more and grow your army.</p>';
  if(workers.length){actions+=`<div class="section-label">BUILD YOUR BASE <small>CHOOSE + PLACE</small></div><div class="action-grid">${actionCard('desk','D','build')}${actionCard('cabinet','C','build')}${actionCard('printer','B','build')}</div><p class="action-note">Choose a building, then a clear spot. Put new desks near fresh trash cans.</p>`;}
  if(units.length){actions+=`<div class="section-label">GIVE AN ORDER</div><div class="command-grid"><button class="command-button ${mode==='attack'?'active':''}" data-action="attack"><span class="command-symbol">⌖</span> Attack nearest <small>A</small></button><button class="command-button" data-action="stop"><span class="command-symbol">□</span> Stop <small>S</small></button></div>`;if(combat.length)actions+=COMBAT.filter(type=>combat.some(e=>e.type===type)).map(type=>{const unlocked=team.upgrades[type],ready=combat.filter(e=>e.type===type).some(e=>e.abilityCooldown<=0),wait=Math.ceil(Math.min(...combat.filter(e=>e.type===type).map(e=>e.abilityCooldown)));return `<button class="ability-button" data-action="ability" data-type="${type}" ${!unlocked||!ready?'disabled':''}><span>${skills[type].icon}</span><span><b>${skills[type].name}</b><small>${!unlocked?'Unlock at a printer':!ready?'Recharging · '+wait+'s':skills[type].desc}</small></span><kbd>E</kbd></button>`;}).join('');}
 }
 $('selectionInfo').innerHTML=info;$('actionContent').innerHTML=actions;$('unitRoster').innerHTML=roster;icons($('selectionInfo'));icons($('actionContent'));icons($('unitRoster'));
}
$('actionContent').addEventListener('click',evt=>{const b=evt.target.closest('[data-action]');if(!b||b.disabled||inLobby||paused)return;const own=ownSelected(),e=own[0],type=b.dataset.type;switch(b.dataset.action){case 'train':result(game.train(e.id,type),TYPES[type].label+' added to the queue.');break;case 'research':result(game.research(e.id,type),skills[type].name+' research queued.');break;case 'build':setMode(type);sound();break;case 'attack':nearestAttack();break;case 'stop':game.command([...selected],{type:'stop'});setMode(null);sound();break;case 'ability':activateAbility(type);break;case 'selectDesk':case 'selectPrinter':{const target=game.entities.find(t=>t.team===0&&t.hp>0&&t.type===(b.dataset.action==='selectDesk'?'desk':'printer'));if(target){selectEntities([target]);renderer.centerOn(target.x,target.y);}break;}}});
$('unitRoster').addEventListener('click',e=>{const b=e.target.closest('[data-unit]');if(b){const unit=game.getEntity(Number(b.dataset.unit));if(unit)selectEntities([unit],e.shiftKey);}});
function hitTest(world){return game.entities.filter(e=>e.hp>0&&Math.hypot(e.x-world.x,e.y-world.y)<Math.max(e.r+10,22/renderer.camera.zoom)).sort((a,b)=>(TYPES[a.type].building?1:0)-(TYPES[b.type].building?1:0)||Math.hypot(a.x-world.x,a.y-world.y)-Math.hypot(b.x-world.x,b.y-world.y))[0];}
function localPoint(e){const r=canvas.getBoundingClientRect();return {x:e.clientX-r.left,y:e.clientY-r.top};}
function usable(){return started&&!inLobby&&!paused&&game.winner===null&&$('guide').hidden;}
function finishDrag(add){if(!drag)return;const minX=Math.min(drag.x1,drag.x2),maxX=Math.max(drag.x1,drag.x2),minY=Math.min(drag.y1,drag.y2),maxY=Math.max(drag.y1,drag.y2);let found=game.entities.filter(e=>{const s=renderer.worldToScreen(e.x,e.y);return e.team===0&&e.hp>0&&s.x>=minX&&s.x<=maxX&&s.y>=minY&&s.y<=maxY;});if(found.some(e=>!TYPES[e.type].building))found=found.filter(e=>!TYPES[e.type].building);selectEntities(found,add);drag=null;}
canvas.addEventListener('contextmenu',e=>e.preventDefault());
canvas.addEventListener('pointerdown',e=>{
 e.preventDefault();canvas.focus({preventScroll:true});if(!usable())return;
 canvas.setPointerCapture(e.pointerId);const point=localPoint(e);pointer=renderer.screenToWorld(e.clientX,e.clientY);
 if(e.pointerType==='touch'){touches.set(e.pointerId,{x:e.clientX,y:e.clientY});if(touches.size>=2){clearTimeout(pointerState?.hold);drag=null;pointerState=null;const p=[...touches.values()];pinch={distance:Math.hypot(p[0].x-p[1].x,p[0].y-p[1].y),x:(p[0].x+p[1].x)/2,y:(p[0].y+p[1].y)/2};return;}}
 if(e.button===2){issue(pointer,hitTest(pointer));return;}
 pointerState={id:e.pointerId,button:e.button,start:point,last:point,startedAt:performance.now(),touch:e.pointerType==='touch',selecting:false,panning:e.button===1||keys.has(' '),moved:false,hold:null};
 if(pointerState.touch&&!mode)pointerState.hold=setTimeout(()=>{if(pointerState&&!pointerState.moved&&!pinch){pointerState.selecting=true;drag={x1:point.x,y1:point.y,x2:point.x,y2:point.y};sound('select');}},320);
});
canvas.addEventListener('pointermove',e=>{
 const point=localPoint(e);pointer=renderer.screenToWorld(e.clientX,e.clientY);
 if(e.pointerType==='touch'&&touches.has(e.pointerId))touches.set(e.pointerId,{x:e.clientX,y:e.clientY});
 if(pinch&&touches.size>=2){const p=[...touches.values()],distance=Math.hypot(p[0].x-p[1].x,p[0].y-p[1].y),x=(p[0].x+p[1].x)/2,y=(p[0].y+p[1].y)/2;renderer.zoomAt(distance/Math.max(1,pinch.distance),x,y);renderer.pan(x-pinch.x,y-pinch.y);pinch={distance,x,y};return;}
 const s=pointerState;if(!s||s.id!==e.pointerId||!usable())return;
 const distance=Math.hypot(point.x-s.start.x,point.y-s.start.y);if(distance>6){s.moved=true;clearTimeout(s.hold);}
 if(s.panning||(s.touch&&s.moved&&!s.selecting&&!mode))renderer.pan(point.x-s.last.x,point.y-s.last.y);
 else if((s.selecting||(!s.touch&&s.moved))&&!mode){s.selecting=true;drag={x1:s.start.x,y1:s.start.y,x2:point.x,y2:point.y};}
 s.last=point;
});
function pointerUp(e){
 if(e.pointerType==='touch'){touches.delete(e.pointerId);if(pinch){if(touches.size<2)pinch=null;pointerState=null;drag=null;return;}}
 const s=pointerState;if(!s||s.id!==e.pointerId)return;clearTimeout(s.hold);pointerState=null;if(!usable()){drag=null;return;}
 if(s.selecting&&s.moved){finishDrag(e.shiftKey);return;}if(s.panning||s.moved){drag=null;return;}
 drag=null;const w=renderer.screenToWorld(e.clientX,e.clientY),hit=hitTest(w);
 if(mode){issue(w,hit);return;}
 if(s.touch&&hit?.team===0&&hit.constructing&&ownSelected().some(e=>e.type==='twig')){issue(w,hit);return;}
 if(hit?.team===0){selectEntities([hit],e.shiftKey);return;}
 if(s.touch&&selected.size){issue(w,hit);return;}
 if(!e.shiftKey)unselect();
}
canvas.addEventListener('pointerup',pointerUp);
canvas.addEventListener('pointercancel',e=>{touches.delete(e.pointerId);clearTimeout(pointerState?.hold);pointerState=null;pinch=null;drag=null;});
canvas.addEventListener('dblclick',e=>{if(!usable())return;const hit=hitTest(renderer.screenToWorld(e.clientX,e.clientY));if(hit?.team===0&&!TYPES[hit.type].building){selectEntities(game.entities.filter(t=>t.team===0&&t.type===hit.type&&t.hp>0&&Math.hypot(t.x-hit.x,t.y-hit.y)<700));}});
canvas.addEventListener('wheel',e=>{e.preventDefault();if(!inLobby)renderer.zoomAt(Math.exp(-e.deltaY*.001),e.clientX,e.clientY);},{passive:false});
$('minimap').addEventListener('pointerdown',e=>{e.preventDefault();if(!usable())return;const r=e.currentTarget.getBoundingClientRect();const w={x:(e.clientX-r.left)/r.width*game.width,y:(e.clientY-r.top)/r.height*game.height};if(e.button===2||mode==='attack')issue(w,null,mode==='attack');else renderer.centerOn(w.x,w.y);});
$('minimap').addEventListener('contextmenu',e=>e.preventDefault());
window.addEventListener('keydown',e=>{
 if(['INPUT','TEXTAREA','SELECT'].includes(document.activeElement?.tagName))return;
 const key=e.key.toLowerCase();if([' ','arrowup','arrowdown','arrowleft','arrowright','f2','f3'].includes(key)||key==='a'&&usable())e.preventDefault();
 keys.add(key);if(e.repeat)return;
 if(key==='escape'){e.preventDefault();if(!$('guide').hidden){closeGuide();return;}if(!inLobby&&started){setMode(null);togglePause();}return;}
 if(key==='x'&&usable()){unselect();return;}
 if(key==='?'||key==='f1'){e.preventDefault();openGuide();return;}
 if(key==='p'&&!inLobby){togglePause();return;}if(!usable())return;
 if(key==='f2'){selectArmy();return;}if(key==='f3'){selectWorkers();return;}if(key===' '){centerHome();return;}
 if(/^[1-9]$/.test(key)){if(e.ctrlKey||e.metaKey){groups[key]=[...selected];toast('Squad '+key+' saved.');}else if(groups[key]){selectEntities(groups[key].map(id=>game.getEntity(id)).filter(Boolean),e.shiftKey);if(lastGroup.key===key&&performance.now()-lastGroup.at<350){const es=ownSelected();if(es.length)renderer.centerOn(es.reduce((n,u)=>n+u.x,0)/es.length,es.reduce((n,u)=>n+u.y,0)/es.length);}lastGroup={key,at:performance.now()};}e.preventDefault();return;}
 const own=ownSelected(),b=own.length===1?own[0]:null;
 if(b?.type==='printer'&&['q','w','r'].includes(key)){result(game.train(b.id,{q:'rock',w:'paper',r:'scissors'}[key]));return;}
 if(b?.type==='desk'&&key==='q'){result(game.train(b.id,'twig'));return;}
 if(key==='a'){setMode(mode==='attack'?null:'attack');return;}if(key==='e'){activateAbility();return;}
 if(own.some(e=>e.type==='twig')&&['d','c','b'].includes(key)){setMode({d:'desk',c:'cabinet',b:'printer'}[key]);return;}
 if(key==='s'&&!e.ctrlKey&&!e.metaKey){game.command([...selected],{type:'stop'});setMode(null);sound();return;}
});
window.addEventListener('keyup',e=>keys.delete(e.key.toLowerCase()));window.addEventListener('blur',()=>{keys.clear();clearTimeout(pointerState?.hold);pointerState=null;drag=null;});
function togglePause(){if(inLobby||!started||game.winner!==null||endShown)return;paused=!paused;$('pauseOverlay').hidden=!paused;$('pauseButton').textContent=paused?'▶':'Ⅱ';keys.clear();sound();}
function openGuide(){if(!$('guide').hidden)return;guideWasPaused=paused;paused=true;$('guide').hidden=false;icons($('guide'));sound();}
function closeGuide(){$('guide').hidden=true;paused=guideWasPaused;sound();}
function openLobby(){if(started&&game.winner===null){$('resumeFromLobby').hidden=false;}else $('resumeFromLobby').hidden=true;inLobby=true;paused=true;$('pauseOverlay').hidden=true;$('lobby').hidden=false;setMode(null);sound();}
$('startButton').addEventListener('click',start);$('playAgain').addEventListener('click',()=>{$('endOverlay').hidden=true;openLobby();});
document.querySelectorAll('[data-map]').forEach(b=>b.addEventListener('click',()=>{chosenMap=b.dataset.map;document.querySelectorAll('[data-map]').forEach(x=>x.classList.toggle('active',x===b));sound();}));
document.querySelectorAll('[data-difficulty]').forEach(b=>b.addEventListener('click',()=>{chosenDifficulty=b.dataset.difficulty;document.querySelectorAll('[data-difficulty]').forEach(x=>x.classList.toggle('active',x===b));sound();}));
document.querySelectorAll('[data-players]').forEach(b=>b.addEventListener('click',()=>{chosenPlayers=Number(b.dataset.players);document.querySelectorAll('[data-players]').forEach(x=>x.classList.toggle('active',x===b));sound();}));
$('resumeFromLobby').addEventListener('click',()=>{inLobby=false;paused=false;$('lobby').hidden=true;sound();});
for(const id of ['guideButton','lobbyGuide','pauseGuide'])$(id).addEventListener('click',openGuide);for(const id of ['closeGuide','guideDone'])$(id).addEventListener('click',closeGuide);
for(const id of ['pauseButton','resumeButton','compactMenu'])$(id).addEventListener('click',togglePause);for(const id of ['menuButton','brandButton','newMatchButton'])$(id).addEventListener('click',openLobby);
$('soundButton').addEventListener('click',()=>{muted=!muted;try{localStorage.setItem('trash-tactics-muted',String(muted));}catch{}updateSound();if(!muted)sound();});
$('pauseSound').addEventListener('click',()=>{$('soundButton').click();$('pauseSound').textContent=muted?'Sound: off':'Sound: on';});
$('homeButton').addEventListener('click',centerHome);$('clearButton').addEventListener('click',unselect);$('armyButton').addEventListener('click',selectArmy);$('workersButton').addEventListener('click',selectWorkers);$('zoomIn').addEventListener('click',()=>renderer.zoomAt(1.16));$('zoomOut').addEventListener('click',()=>renderer.zoomAt(1/1.16));
window.addEventListener('resize',()=>renderer.resize());
function endMatch(){endShown=true;paused=true;const won=game.winner===0;$('endSymbol').textContent=won?'✦':'☂';$('endEyebrow').textContent=won?'THE DESK IS YOURS':'A MINOR PAPERWORK INCIDENT';$('endTitle').textContent=won?'Beautifully trashed.':'Back to the drawing desk.';$('endMessage').textContent=won?'Your scrap pack took down every rival desk. Tiny troops, tremendous teamwork.':'The rival destroyed your desks. Try an earlier expansion, more workers, or a different troop mix.';$('endStats').innerHTML=`<div><b>${Math.floor(game.time/60)}:${String(Math.floor(game.time)%60).padStart(2,'0')}</b>TIME</div><div><b>${game.stats?.collected?.[0]??0}</b>TRASH GATHERED</div><div><b>${game.stats?.trained?.[0]??0}</b>UNITS TRAINED</div>`;$('endOverlay').hidden=false;sound(won?'victory':'defeat');}
function frame(now){const dt=Math.min(.05,(now-lastFrame)/1000);lastFrame=now;
 if(usable()){
  let dx=0,dy=0;if(keys.has('arrowleft'))dx+=480*dt;if(keys.has('arrowright'))dx-=480*dt;if(keys.has('arrowup'))dy+=480*dt;if(keys.has('arrowdown'))dy-=480*dt;if(dx||dy)renderer.pan(dx,dy);
  game.update(dt);if((game.winner!==null||game.teams[0].eliminated)&&!endShown)endMatch();
 }
 renderer.draw({selected,pointer,drag,mode,paused:paused||inLobby,time:game.time});
 uiTick+=dt;if(uiTick>.2){uiTick=0;updateUI();renderer.drawMinimap($('minimap'));}
 requestAnimationFrame(frame);
}
resetGame();updateSound();icons();requestAnimationFrame(frame);
// Small, read-only friendly integration handle for an embedding game lobby.
window.trashTactics={get game(){return game;},get renderer(){return renderer;},get selected(){return [...selected];}};
