import {Game,TYPES,isCombat,canTransport,snapWallPoint} from './engine.js';
import {Renderer,drawIcon} from './render.js';

const $=id=>document.getElementById(id), canvas=$('world');
const COMBAT=['rock','paper','scissors'], BUILDINGS=['desk','cabinet','printer','cdreader','mouse','workshop','laboratory','wall'];
const names={desktop:'Desktop Meadow',breakroom:'Breakroom Blitz',canyon:'Stapler Canyon',conveyor:'Conveyor Chaos'};
const descriptions={twig:'A scrappy little worker. Carries 5 trash per trip. Can build your entire base.',rock:'A solid choice. Crushes scissors. Watch out for paper.',paper:'A clean sheet with a mean streak. Wraps rocks. Scissors are trouble.',scissors:'Sharp, speedy, and slightly unhinged. Cuts paper. Avoid rocks.',desk:'Home, sweet headquarters. Twigs return their trash here. Train more workers or expand.',printer:'Print a little trouble. Train your army and research a special ability for each troop.',cabinet:'Room for more mischief. Adds 8 supply to your little empire.'};
descriptions.cdreader='Summon flying CDs to carry your army over the desk. Each CD holds six troops. No twigs aboard.';
descriptions.cd='A flying troop carrier. Select troops and tap a CD to board. Load finds the nearest troops; Drop chooses where to land. Holds six troops, no twigs.';
descriptions.mouse='Reinforced pointer defense. 850 health, 360 range, 28 impact damage every 1.25s, plus 6 damage per second for 3s. Targets enemy aircraft and combat troops.';
descriptions.wall='Reinforced office-supply wall. 20 trash, 1,200 health, 3s to build. Snaps to neighboring wall blocks to make solid ground barriers. Aircraft fly over.';
const skills={rock:{name:'Launch',key:'Q',icon:'↗',desc:'Leap to a spot in formation',cooldown:11},paper:{name:'Crumple',key:'W',icon:'◉',desc:'3s invincible · faster · no attacks',cooldown:12},scissors:{name:'Sweep',key:'E',icon:'✂',desc:'Cut every paper in a short arc',cooldown:10}};
const abilityKeys={q:'rock',w:'paper',e:'scissors'};
const specialistSkills={
 snipe:{type:'sniper',name:'Precision Snipe',key:'F',icon:'⌖',desc:'700 range · 1.5s aim · eliminates one visible Paper',cooldown:18,single:true},
 intercept:{type:'airplane',name:'Intercept',key:'I',icon:'↗',desc:'Pursue a visible enemy aircraft',cooldown:0},
 patrol:{type:'airplane',name:'Patrol',key:'J',icon:'⟳',desc:'Tap two or more points, then Finish route',cooldown:0,route:true},
 crash:{type:'airplane',name:'Crash Landing',key:'K',icon:'✦',desc:'Sacrifice aircraft · distributed 145 damage impacts',cooldown:0},
 forcefield:{type:'mage',name:'Force Field Dome',key:'G',icon:'◠',desc:'420 range · 10s barrier · two charges per 5s',cooldown:5,single:true},
 division:{type:'divider',name:'Triple Division',key:'T',icon:'◴',desc:'600 range · sorts and contains all three families for 9s',cooldown:38,single:true},
 roll:{type:'rollingrock',name:'Crushing Roll',key:'Y',icon:'●',desc:'750 range · accelerate and crush along parallel lanes',cooldown:17},
 shadowpath:{type:'shears',name:'Shadow Path',key:'H',icon:'✂',desc:'Draw a route or tap waypoints · 900 maximum length',cooldown:18,route:true}
};
Object.assign(descriptions,{
 workshop:'Tier 2 production. Requires an operational Printer. Train snipers, airplanes, and mages.',
 laboratory:'Tier 3 production. Requires a Printer and completed Prototype Workshop. Build an army of specialists.',
 sniper:'Scissors specialist. Keeps its distance and aims devastating precision shots at visible Paper troops.',
 airplane:'Paper fighter and high-vision scout. Reveals 900 pixels around itself. Intercepts aircraft, patrols routes, or sacrifices itself in a distributed crash bombardment. Scissors are immune.',
 mage:'Rock wizard. Creates overlapping physical domes that trap or reroute ground troops. Two casts recharge every five seconds.',
 divider:'Paper wizard. Sorts both armies into Rock, Paper, and Scissors sectors. Magical walls contain troops and block attacks.',
 rollingrock:'Heavy Rock juggernaut. Charges in parallel lanes and knocks troops aside. Paper stops its charge.',
 shears:'Scissors assassin. Draw a flanking route for a rapid cutting dash. Rocks are immune; terrain and barriers stop the dash.'
});
let routePoints=[],routeDrawing=false;
let game,renderer, selected=new Set(),mode=null,abilityType=null,pointer={x:0,y:0},drag=null;
let paused=false,started=false,inLobby=true,chosenMap='desktop',chosenDifficulty='normal',chosenPlayers=2;
let groups={},lastGroup={key:'',at:0},lastFrame=performance.now(),uiTick=0,signature='',endShown=false,toastTimeout;
const GROUP_HOLD_MS=550, GROUP_DOUBLE_MS=350;
let groupKeyPress=null;
const groupPointerPresses=new Map();
let keys=new Set(), pointerState=null,touches=new Map(),pinch=null,guideWasPaused=false,mouseHover=null,minimapDrag=null;
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
 if(e.type==='hit'&&!paused&&!inLobby&&(!e.target||e.target.team===0||visibleEntity(e.target)))sound('hit');
 if(e.type==='deposit'&&e.team===0&&Math.random()<.13&&!paused&&!inLobby)sound('deposit');
}
function resetGame(){cancelGroupPresses();lastGroup={key:'',at:0};game=new Game({map:chosenMap,difficulty:chosenDifficulty,players:chosenPlayers,onEvent});selected.clear();groups={};mode=null;abilityType=null;drag=null;mouseHover=null;endShown=false;signature='';if(renderer){renderer.game=game;renderer.resize();}else renderer=new Renderer(canvas,game);renderer.camera.zoom=window.innerWidth<761?.72:.94;const d=game.entities.find(e=>e.team===0&&e.type==='desk');pointer={x:d.x,y:d.y};renderer.centerOn(d.x,d.y+40);$('matchLabel').textContent=names[chosenMap].toUpperCase()+' · '+chosenPlayers+' PLAYERS · '+chosenDifficulty.toUpperCase();$('operationName').textContent={desktop:'The great desk takeover',breakroom:'A little breakroom business',canyon:'Control the crossings',conveyor:'Keep the factory moving'}[chosenMap];updateUI(true);}
function start(){sound();resetGame();started=true;inLobby=false;paused=false;lastFrame=performance.now();$('lobby').hidden=true;$('pauseOverlay').hidden=true;$('endOverlay').hidden=true;const desk=game.entities.find(e=>e.team===0&&e.type==='desk');selected=new Set([desk.id]);updateUI(true);toast('Scout through the fog. Use ramps to reach high ground; Q / W / E activate troop abilities.');canvas.focus({preventScroll:true});}
function setMode(next,type=null){routePoints=[];routeDrawing=false;mode=next;abilityType=type;const label=mode==='wall'?'Place connected walls · 20 trash · 1,200 HP · tap repeatedly to extend':BUILDINGS.includes(mode)?`Place ${TYPES[mode].label.toLowerCase()} · choose clear ground`:mode==='drop'?'Drop troops · tap a destination for your selected CDs':mode==='ability'?`${type?skills[type].name:'Special abilities'} · choose a target or direction`:mode==='specialist'?`${specialistSkills[type].name} · ${specialistSkills[type].route?'draw a route or tap waypoints, then Finish route':specialistSkills[type].desc}`:mode==='attack'?'Attack move · choose a spot to fight toward':'';$('modeBanner').textContent=label+(label?' · X cancel · Esc menu':'');$('modeBanner').hidden=!label;canvas.style.cursor=next?'crosshair':'default';updateUI(true);}
function unselect(){selected.clear();setMode(null);sound('select');}
function ownSelected(){return [...selected].map(id=>game.getEntity(id)).filter(e=>e?.team===0&&!e.transportedBy);}
function visibleEntity(entity){return !!entity&&(game.isVisible?game.isVisible(entity,0):true);}
function selectEntities(entities,add=false){if(!add)selected.clear();for(const e of entities)if(e.team===0&&!e.transportedBy)selected.add(e.id);setMode(null);sound('select');}
function centerHome(){const d=game.entities.find(e=>e.team===0&&e.type==='desk'&&e.hp>0);if(d)renderer.centerOn(d.x+90,d.y);}
function selectArmy(){const army=game.entities.filter(e=>e.team===0&&isCombat(e.type)&&e.hp>0&&!e.transportedBy);selectEntities(army);if(!army.length)toast('No ground troops ready. Select a CD to drop its passengers, or a printer to train troops.');}
function selectWorkers(){const ws=game.entities.filter(e=>e.team===0&&e.type==='twig'&&e.hp>0&&['idle','stop'].includes(e.order?.type));selectEntities(ws);if(!ws.length)toast('All your twigs are busy. Keep the trash coming!');}
function result(r,success){if(r?.ok){sound();if(success)toast(success);}else{sound('hit');toast(r?.reason||'That command is unavailable.');}updateUI(true);return !!r?.ok;}
function issue(world,hit=null,forceAttack=false){
 const own=ownSelected();if(!own.length)return;
 if(hit&&!visibleEntity(hit))hit=null;
 if(mode==='specialist'){
  const skill=specialistSkills[abilityType];
  if(skill.route){if(routePoints.length===0&&abilityType==='shadowpath'){const unit=own.find(e=>e.type===skill.type);if(unit)routePoints.push({x:unit.x,y:unit.y});}routePoints.push({...world});updateUI(true);return;}
  const target={...world,targetId:hit?.id};
  if(result(game.useAbility(own.map(e=>e.id),abilityType,target))){sound('ability');renderer.marker={...world,type:'attack',life:.8};if(!skill.single)setMode(null);}
  return;
 }
 if(mode==='drop'){if(result(game.dropTransports(own.filter(e=>e.type==='cd').map(e=>e.id),world.x,world.y),'CDs heading to the drop zone.')){renderer.marker={...world,type:'drop',life:.8};setMode(null);}return;}
 if(BUILDINGS.includes(mode)){const site=mode==='wall'?snapWallPoint(world.x,world.y):world;if(game.canSee&&!game.canSee(0,site.x,site.y)){toast('Scout this ground before building here.');return;}const placed=game.build(own.filter(e=>e.type==='twig').map(e=>e.id),mode,site.x,site.y);if(result(placed,mode==='wall'?(placed.queued?'Wall queued. Your twig will finish its current block first.':'Wall started. Tap the next grid space to extend it.'):'Building started. Your twig will get right to it.')){renderer.marker={...site,type:'move',life:.8};if(mode!=='wall')setMode(null);}return;}
 if(mode==='ability'){const ids=own.filter(e=>!abilityType||e.type===abilityType).map(e=>e.id);if(result(game.ability(ids,world.x,world.y))){sound('ability');setMode(null);}return;}
 const troops=own.filter(e=>!TYPES[e.type].building),buildings=own.filter(e=>TYPES[e.type].building);
 if(!mode&&hit?.team===0&&hit.type==='cd'&&troops.some(e=>canTransport(e.type))){if(result(game.loadTransport(troops.map(e=>e.id),[hit.id]),'Troops boarding the CD. Six seats; no twigs.'))selectEntities([hit]);return;}
 if(!mode&&hit?.team===0&&hit.constructing){const worker=troops.filter(e=>e.type==='twig').sort((a,b)=>Math.hypot(a.x-hit.x,a.y-hit.y)-Math.hypot(b.x-hit.x,b.y-hit.y))[0];if(worker){hit.builderId=worker.id;game.command([worker.id],{type:'build',targetId:hit.id});sound();toast('Your twig is finishing construction.');return;}}
 if(!troops.length){for(const b of buildings)if(['desk','printer','cdreader','workshop','laboratory'].includes(b.type))b.rally={x:world.x,y:world.y,targetId:hit?.type==='trash'?hit.id:null};toast('Rally point set. New units will head here.');renderer.marker={...world,type:'move',life:.8};return;}
 let order;if(mode==='attack'||forceAttack)order={type:'attackMove',...world};else if(hit&&(hit.team>0||hit.type==='staplergate'))order={type:'attack',targetId:hit.id,...world};else if(hit?.type==='trash')order={type:'gather',targetId:hit.id,...world};else order={type:'move',...world};
 if(result(game.command(troops.map(e=>e.id),order))){renderer.marker={...world,type:order.type.startsWith('attack')?'attack':'move',life:.8};setMode(null);}
}
function nearestAttack(){const own=ownSelected().filter(e=>isCombat(e.type));if(!own.length){toast('Select combat troops first.');return;}const center={x:own.reduce((n,e)=>n+e.x,0)/own.length,y:own.reduce((n,e)=>n+e.y,0)/own.length};const enemy=game.entities.filter(e=>visibleEntity(e)&&own.some(u=>game.canTarget(u,e))).sort((a,b)=>Math.hypot(a.x-center.x,a.y-center.y)-Math.hypot(b.x-center.x,b.y-center.y))[0];if(enemy){game.command(own.map(e=>e.id),{type:'attackMove',x:enemy.x,y:enemy.y});renderer.marker={x:enemy.x,y:enemy.y,type:'attack',life:.8};sound();setMode(null);}else toast('No visible enemy you can target. Scout ahead or attack-move into the fog.');}
function loadSelected(){result(game.loadNearest(ownSelected().filter(e=>e.type==='cd').map(e=>e.id)),'Nearest troops boarding. Six seats per CD; no twigs.');}
function chooseDrop(){const cds=ownSelected().filter(e=>e.type==='cd');if(!cds.some(e=>e.cargo?.length)){toast('Load troops into a CD first.');return;}setMode(mode==='drop'?null:'drop');}
function activateAbility(type){
 const units=ownSelected().filter(e=>e.type===type),skill=skills[type];
 if(!skill||!units.length)return;
 if(!game.teams[0].upgrades[type]){toast(`Research ${skill.name} at a printer first.`);return;}
 const ready=units.filter(e=>e.abilityCooldown<=0&&!e.leap);
 if(!ready.length){toast(`${skill.name} is recharging · ${Math.max(1,Math.ceil(Math.min(...units.map(e=>e.abilityCooldown))))}s.`);return;}
 if(type==='paper'){if(result(game.ability(ready.map(e=>e.id),ready[0].x,ready[0].y))){sound('ability');setMode(null);}return;}
 setMode(mode==='ability'&&abilityType===type?null:'ability',type);
}
function activateSpecialist(name){
 const skill=specialistSkills[name],units=ownSelected().filter(e=>e.type===skill?.type);if(!skill||!units.length)return;
 const ready=units.some(e=>name==='forcefield'?(e.mageCharges??2)>0:!e.abilityCooldown||['intercept','patrol','crash'].includes(name));
 if(!ready){toast(`${skill.name} is recharging.`);return;}
 setMode(mode==='specialist'&&abilityType===name?null:'specialist',name);
}
function finishRoute(){
 if(mode!=='specialist'||!specialistSkills[abilityType]?.route)return;
 if(routePoints.length<2){toast('Choose at least two route points.');return;}
 if(result(game.useAbility(ownSelected().map(e=>e.id),abilityType,{points:routePoints.map(p=>({...p})),...routePoints.at(-1)}))){sound('ability');setMode(null);}
}
function specialistButtons(combat){return Object.entries(specialistSkills).filter(([,skill])=>combat.some(e=>e.type===skill.type)).map(([name,skill])=>{
 const units=combat.filter(e=>e.type===skill.type),charges=name==='forcefield'?units.reduce((n,e)=>n+(e.mageCharges??2),0):0;
 const wait=Math.max(0,Math.ceil(Math.min(...units.map(e=>name==='forcefield'?(e.mageRecharge||0):(e.abilityCooldown||0)))));
 const ready=name==='forcefield'?charges>0:wait===0||['intercept','patrol','crash'].includes(name);
 const status=ready?skill.desc:'Recharging · '+wait+'s';
 return `<button class="ability-button ${mode==='specialist'&&abilityType===name?'active':''}" data-action="specialist" data-type="${name}" aria-keyshortcuts="${skill.key}" title="${skill.name} (${skill.key}) · ${status}" ${ready?'':'disabled'}><span>${skill.icon}</span><span><b>${skill.name}</b><small>${status}</small></span><kbd>${skill.key}</kbd><span class="ability-status">${name==='forcefield'?charges+' charges':ready?'Ready':wait+'s'}</span></button>`;
 }).join('');}
function icons(root=document){root.querySelectorAll('canvas[data-icon]').forEach(c=>{const size=c.width;drawIcon(c.getContext('2d'),c.dataset.icon,size/2,size/2,size*.8,0,{sprite:true});});}
function icon(type,size=60){return `<canvas data-icon="${type}" width="${size}" height="${size}" aria-hidden="true"></canvas>`;}
function actionCard(type,hotkey,action='train'){const cost=TYPES[type].cost;return `<button class="action-card ${action==='build'&&mode===type?'active':''}" data-action="${action}" data-type="${type}" title="${descriptions[type]} Costs ${cost} trash."><span class="hotkey">${hotkey||''}</span>${icon(type,60)}<b>${type==='desk'?'Home desk':TYPES[type].label}</b><span class="cost">♻ ${cost}${TYPES[type].trainTime?' · '+TYPES[type].trainTime+'s · '+TYPES[type].supply+' supply':TYPES[type].buildTime?' · '+TYPES[type].buildTime+'s':''}${type==='wall'?' · 1,200 HP':''}</span></button>`;}
function queueHtml(e){if(!e.queue?.length)return '';return `<div class="production"><div class="section-label">IN THE QUEUE <small>${e.queue.length} / 6</small></div>${e.queue.map((q,i)=>`<div class="queue-item">${icon(q.type,32)}<div><b>${q.kind==='research'?skills[q.type].name+' research':TYPES[q.type].label}</b><small>${i===0?Math.ceil(q.remaining)+'s':'QUEUED'}</small><div class="queue-bar"><span style="width:${i===0?Math.max(0,100*(1-q.remaining/q.total)):0}%"></span></div></div></div>`).join('')}</div>`;}
function setText(element,value){if(element.textContent!==value)element.textContent=value;}
function updateUI(force=false){
 const supply=game.getSupply(0),team=game.teams[0];setText($('trashCount'),Math.floor(team.trash).toLocaleString());setText($('supplyCount'),`${supply.used} / ${supply.cap}`);$('supplyCount').style.color=supply.used>=supply.cap?'#ca7555':'';
 const seconds=Math.floor(game.time);setText($('timer'),String(Math.floor(seconds/60)).padStart(2,'0')+':'+String(seconds%60).padStart(2,'0'));
 updateGroups();
 for(const id of selected)if(!game.getEntity(id)||game.getEntity(id).transportedBy)selected.delete(id);
 const own=ownSelected(),e=own[0],units=own.filter(e=>!TYPES[e.type].building),workers=units.filter(e=>e.type==='twig'),combat=units.filter(e=>isCombat(e.type)),cds=units.filter(e=>e.type==='cd');
 document.querySelector('.sidebar').classList.toggle('empty',!own.length);document.querySelector('.sidebar').classList.toggle('narrow',own.length===1&&e.type==='cabinet');document.querySelector('.sidebar').dataset.selection=own.length>1?(workers.length===own.length?'twig':'squad'):(e?.type||'none');
 const sig=own.map(e=>[e.id,Math.ceil(e.hp),Math.ceil(e.constructing||0),Math.ceil(e.abilityCooldown||0),e.cargo?.join(','),e.mageCharges,Math.ceil(e.mageRecharge||0),e.aim?.remaining,e.crash?.diving,e.buildQueue?.join(','),e.order?.targetId,e.queue?.map(q=>q.type+Math.ceil(q.remaining)).join(',')].join(':')).join('|')+JSON.stringify(team.upgrades)+mode+abilityType+routePoints.length;
 if(!force&&sig===signature)return;signature=sig;
 $('clearButton').disabled=!own.length;
 let info='',actions='',roster='';
 if(!e){info=`<div class="selected-heading"><div class="portrait">${icon('twig')}</div><div><h2>Hey, commander.</h2><p>YOUR DESK AWAITS</p></div></div><p class="selection-description">Select a desk to train twigs, a printer for troops, or a twig to start building.</p>`;actions=`<div class="section-label">QUICK START</div><button class="research-card" data-action="selectDesk">${icon('desk',40)}<span><strong>Visit your home desk</strong><small>Train workers · grow your economy</small></span><span class="research-cost">↗</span></button><button class="research-card" style="margin-top:7px" data-action="selectPrinter">${icon('printer',40)}<span><strong>Fire up the printer</strong><small>Train troops · research abilities</small></span><span class="research-cost">↗</span></button><p class="action-note">Little tip: a twig is already collecting from each of five nearby cans. More cans mean more income.</p>`;}
 else{
  const title=own.length>1?`${own.length} ${cds.length===own.length?'CDs':combat.length===own.length?'troops':workers.length===own.length?'twigs':'selected'}`:TYPES[e.type].label;
  const subtitle=own.length>1?`${combat.length} TROOPS · ${workers.length} TWIGS${cds.length?' · '+cds.length+' CDs':''}`:TYPES[e.type].building?'BASE BUILDING':e.type==='cd'?'FLYING TRANSPORT · 1 SUPPLY':e.type==='twig'?'WORKER · 1 SUPPLY':`TIER ${TYPES[e.type].tier||1} · ${TYPES[e.type].family||e.type} · ${TYPES[e.type].supply} SUPPLY`;
  info=`<div class="selected-heading"><div class="portrait">${icon(e.type)}</div><div style="flex:1"><h2>${title}</h2><p>${subtitle}</p>${own.length===1?`<div class="health-meter"><span style="width:${100*e.hp/e.maxHp}%"></span></div>`:''}</div></div><p class="selection-description">${own.length===1?descriptions[e.type]:workers.length===own.length?'Many twigs make light work. Spread them across trash cans or build something great.':'Give the squad a destination. They’ll handle the little details.'}</p>`;
  if(own.length>1)roster=own.slice(0,30).map(e=>`<button class="roster-item" data-unit="${e.id}" title="Select this ${TYPES[e.type].label}">${icon(e.type,40)}</button>`).join('');
  if(e.constructing&&own.length===1)actions=`<div class="section-label">UNDER CONSTRUCTION <small>${Math.ceil(e.constructing)}s</small></div><div class="queue-bar"><span style="width:${100*(1-e.constructing/e.buildTotal)}%"></span></div><p class="action-note">Your twig is putting the pieces together.</p>`;
  else if(e.type==='desk'&&own.length===1)actions=`<div class="section-label">GROW YOUR ECONOMY <small>4s / TWIG</small></div><div class="action-grid">${actionCard('twig','Q')}</div><p class="action-note">New twigs automatically find a trash can. Right click to set a rally point.</p>${queueHtml(e)}`;
  else if(e.type==='cdreader'&&own.length===1)actions=`<div class="section-label">SUMMON A TRANSPORT <small>8s / CD</small></div><div class="action-grid">${actionCard('cd','Q')}</div><p class="action-note">Flying CDs carry six combat troops each. Right click to set a flight rally point.</p>${queueHtml(e)}`;
  else if(e.type==='printer'&&own.length===1){actions=`<div class="section-label">PRINT SOME TROUBLE <small>5s / TROOP</small></div><div class="action-grid">${actionCard('rock','Q')}${actionCard('paper','W')}${actionCard('scissors','E')}</div><div class="section-label" style="margin-top:23px">A LITTLE EXTRA POWER <small>150 TRASH EACH</small></div><div class="research-grid">${COMBAT.map(type=>{const researched=team.upgrades[type],pending=game.entities.some(b=>b.team===0&&b.queue?.some(q=>q.kind==='research'&&q.type===type));return `<button class="research-card ${researched?'unlocked':''}" data-action="research" data-type="${type}" ${researched||pending?'disabled':''} title="${skills[type].desc}">${icon(type,44)}<span><strong>${skills[type].name}</strong><small>${skills[type].desc}</small></span><span class="research-cost">${researched?'✓':pending?'…':'♻ 150'}</span></button>`;}).join('')}</div>${queueHtml(e)}`;}
  else if(['workshop','laboratory'].includes(e.type)&&own.length===1){const types=e.type==='workshop'?['sniper','airplane','mage']:['divider','rollingrock','shears'];actions=`<div class="section-label">${e.type==='workshop'?'TIER 2 · SPECIALISTS':'TIER 3 · BATTLE CHANGERS'}</div><div class="action-grid">${types.map((type,i)=>actionCard(type,['Q','W','E'][i])).join('')}</div><p class="action-note">Abilities come ready. Right click to set a rally point. Supply is reserved for queued troops.</p>${queueHtml(e)}`;}
  else if(e.type==='cabinet'&&own.length===1)actions='<div class="section-label">A PLACE FOR EVERYONE</div><p class="action-note">Your cabinet gives you 8 more supply. Select a twig to build more and grow your army.</p>';
  if(e.type==='mouse'&&own.length===1&&!e.constructing)actions=`<div class="defense-status"><strong>Pointer defense · reinforced</strong><span>${TYPES.mouse.hp} HP · ${TYPES.mouse.range} range</span><small>${TYPES.mouse.impactDamage} impact every ${TYPES.mouse.shotInterval}s + ${TYPES.mouse.clickDps} damage/s for ${TYPES.mouse.clickDuration}s</small></div>`;
  if(e.type==='wall'&&own.length===1)actions+='<div class="fortification-status"><strong>Connected ground barrier</strong><span>1,200 HP · 20 trash per block</span><small>Build adjacent blocks with a Twig. Aircraft fly over.</small><button class="command-button" data-action="dismantleWall" title="Remove this owned block to open a passage. No trash refund.">Dismantle block</button></div>';
  if(workers.length){const pending=workers.reduce((n,w)=>n+(w.buildQueue?.length||0)+(game.getEntity(w.order?.targetId)?.type==='wall'?1:0),0);if(mode==='wall')actions+=`<div class="wall-controls"><span>Tap to extend · ${pending} ${pending===1?'block':'blocks'} planned</span><button class="command-button" data-action="cancelTarget">✓ Done placing</button></div>`;actions+=`<div class="section-label">BUILD YOUR BASE <small>CHOOSE + PLACE</small></div><div class="action-grid">${actionCard('desk','D','build')}${actionCard('cabinet','C','build')}${actionCard('wall','Z','build')}${actionCard('printer','B','build')}${actionCard('cdreader','V','build')}${actionCard('mouse','M','build')}${actionCard('workshop','N','build')}${actionCard('laboratory','O','build')}</div><p class="action-note">Choose a building, then a clear spot. Walls snap together; keep tapping to extend them.</p>`;}
  if(cds.length){const passengers=cds.reduce((n,e)=>n+(e.cargo?.length||0),0);actions+=`<div class="transport-manifest" role="status"><strong>${passengers} / ${cds.length*6} aboard</strong><span>${cds.length} ${cds.length===1?'CD':'CDs'} · No twigs</span><div class="transport-seats">${cds.map(cd=>`<span title="CD ${cd.id}: ${cd.cargo?.length||0} / 6 troops">${Array.from({length:6},(_,i)=>`<i class="${i<(cd.cargo?.length||0)?'occupied':''}"></i>`).join('')}</span>`).join('')}</div></div><div class="command-grid transport-commands"><button class="command-button" data-action="load" title="Board the nearest combat troops into each selected CD"><span class="command-symbol">↥</span> Load <small>L</small></button><button class="command-button ${mode==='drop'?'active':''}" data-action="drop" ${!passengers?'disabled':''} title="Fly to a chosen location and drop all passengers"><span class="command-symbol">↧</span> Drop <small>U</small></button></div>`;}
  if(e.type==='station'&&own.length===1)actions+='<button class="command-button" data-action="reverse">⇄ Reverse conveyor</button><p class="action-note">Keep ground troops nearby to hold this control station.</p>';
  if(units.length){actions+=`<div class="section-label">GIVE AN ORDER</div><div class="command-grid">${combat.length?`<button class="command-button ${mode==='attack'?'active':''}" data-action="attack"><span class="command-symbol">⌖</span> Attack nearest <small>A</small></button>`:''}<button class="command-button" data-action="stop"><span class="command-symbol">□</span> Stop <small>S</small></button></div>`;if(combat.length)actions+=COMBAT.filter(type=>combat.some(e=>e.type===type)).map(type=>{const skill=skills[type],unlocked=team.upgrades[type],ready=combat.filter(e=>e.type===type).some(e=>e.abilityCooldown<=0&&!e.leap),wait=Math.max(1,Math.ceil(Math.min(...combat.filter(e=>e.type===type).map(e=>e.abilityCooldown)))),status=!unlocked?'Unlock at a printer':!ready?'Recharging · '+wait+'s':skill.desc;return `<button class="ability-button ${mode==='ability'&&abilityType===type?'active':''}" data-action="ability" data-type="${type}" aria-keyshortcuts="${skill.key}" title="${skill.name} (${skill.key}) · ${status}" ${!unlocked||!ready?'disabled':''}><span>${skill.icon}</span><span><b>${skill.name}</b><small>${status}</small></span><kbd>${skill.key}</kbd><span class="ability-status">${!unlocked?'🔒':!ready?wait+'s':'Ready'}</span></button>`;}).join('');actions+=specialistButtons(combat);}
  if(mode==='specialist'&&specialistSkills[abilityType]?.route)actions+=`<div class="route-controls"><b>${routePoints.length} route points</b><button class="command-button" data-action="finishRoute">✓ Finish route <small>Enter</small></button><button class="command-button" data-action="cancelTarget">× Cancel</button></div>`;
 }
 $('selectionInfo').innerHTML=info;$('actionContent').innerHTML=actions;$('unitRoster').innerHTML=roster;icons($('selectionInfo'));icons($('actionContent'));icons($('unitRoster'));
}
$('actionContent').addEventListener('click',evt=>{const b=evt.target.closest('[data-action]');if(!b||b.disabled||inLobby||paused)return;const own=ownSelected(),e=own[0],type=b.dataset.type;switch(b.dataset.action){case 'train':if(e)result(game.train(e.id,type),TYPES[type].label+' added to the queue.');break;case 'research':if(e)result(game.research(e.id,type),skills[type].name+' research queued.');break;case 'build':setMode(type);sound();break;case 'load':loadSelected();break;case 'drop':chooseDrop();break;case 'attack':nearestAttack();break;case 'stop':game.command([...selected],{type:'stop'});setMode(null);sound();break;case 'ability':activateAbility(type);break;case 'specialist':activateSpecialist(type);break;case 'finishRoute':finishRoute();break;case 'cancelTarget':setMode(null);break;case 'dismantleWall':if(e)result(game.removeWall(e.id,0),'Wall dismantled. The passage is open.');break;case 'reverse':if(e)result(game.reverseConveyor(e.id,0),'Conveyor reversed.');break;case 'selectDesk':case 'selectPrinter':{const target=game.entities.find(t=>t.team===0&&t.hp>0&&t.type===(b.dataset.action==='selectDesk'?'desk':'printer'));if(target){selectEntities([target]);renderer.centerOn(target.x,target.y);}break;}}});
$('unitRoster').addEventListener('click',e=>{const b=e.target.closest('[data-unit]');if(b){const unit=game.getEntity(Number(b.dataset.unit));if(unit)selectEntities([unit],e.shiftKey);}});
function hitTest(world){const distance=e=>Math.hypot(e.x-world.x,e.y-(TYPES[e.type].flying?27:0)-world.y);return game.entities.filter(e=>e.hp>0&&!e.transportedBy&&visibleEntity(e)&&distance(e)<Math.max(e.r+10,22/renderer.camera.zoom)).sort((a,b)=>(a.type==='cd'?-1:TYPES[a.type].building?1:0)-(b.type==='cd'?-1:TYPES[b.type].building?1:0)||distance(a)-distance(b))[0];}
function localPoint(e){const r=canvas.getBoundingClientRect();return {x:e.clientX-r.left,y:e.clientY-r.top,width:r.width,height:r.height};}
function usable(){return started&&!inLobby&&!paused&&game.winner===null&&$('guide').hidden;}
// Groups store unit IDs so their membership follows the same troops as they move.
function groupUnits(key){
 const members=(groups[key]||[]).map(id=>game.getEntity(id)).filter(e=>e&&e.team===0&&e.hp>0&&TYPES[e.type]&&!TYPES[e.type].building&&e.type!=='trash');
 if(groups[key])groups[key]=members.map(e=>e.id);
 // Recall an embarked group through its carriers, retaining the original troop IDs for landing.
 return [...new Map(members.map(e=>e.transportedBy?game.getEntity(e.transportedBy):e).filter(Boolean).map(e=>[e.id,e])).values()];
}
function updateGroups(){
 for(let i=1;i<=4;i++){
  const key=String(i),button=$('group-'+key);if(!button)continue;
  const units=groupUnits(key),count=units.length;
  const active=count>0&&selected.size===count&&units.every(e=>selected.has(e.id));
  const status=String(count)+':'+active;if(button.dataset.status===status)continue;button.dataset.status=status;
  button.classList.toggle('assigned',count>0);button.classList.toggle('selected',active);
  button.dataset.count=String(count);
  const countLabel=button.querySelector('[data-group-count]');if(countLabel)setText(countLabel,count?String(count):'—');
  button.setAttribute('aria-label',`Command group ${key}, ${count} ${count===1?'troop':'troops'}. Tap to select, double tap to center, hold to assign.`);
  button.setAttribute('aria-pressed',String(active));
 }
}
function assignGroup(key){
 if(!usable())return;
 const units=ownSelected().filter(e=>e.hp>0&&TYPES[e.type]&&!TYPES[e.type].building&&e.type!=='trash');
 if(!units.length){toast('Select troops before assigning a command group.');return;}
 groups[key]=units.map(e=>e.id);lastGroup={key:'',at:0};updateGroups();sound('select');
 toast(`Group ${key} assigned · ${units.length} ${units.length===1?'troop':'troops'}.`);
}
function recallGroup(key,add=false){
 if(!usable())return;
 const units=groupUnits(key);updateGroups();
 if(!units.length){lastGroup={key:'',at:0};toast(`Group ${key} is empty. Select troops and hold to assign.`);return;}
 const now=performance.now(),center=lastGroup.key===key&&now-lastGroup.at<GROUP_DOUBLE_MS;
 selectEntities(units,add);
 if(center)renderer.centerOn(units.reduce((n,u)=>n+u.x,0)/units.length,units.reduce((n,u)=>n+u.y,0)/units.length);
 lastGroup=center?{key:'',at:0}:{key,at:now};updateGroups();
}
function cancelGroupPresses(){
 if(groupKeyPress){clearTimeout(groupKeyPress.timer);groupKeyPress=null;}
 for(const press of groupPointerPresses.values())clearTimeout(press.timer);
 groupPointerPresses.clear();
}
for(let i=1;i<=4;i++){
 const key=String(i),button=$('group-'+key);if(!button)continue;
 let suppressClickUntil=0;
 button.addEventListener('pointerdown',e=>{
  if(!usable()||e.button!==0||groupPointerPresses.has(key))return;
  e.preventDefault();suppressClickUntil=performance.now()+1000;button.setPointerCapture(e.pointerId);
  const press={id:e.pointerId,x:e.clientX,y:e.clientY,held:false,timer:null};
  press.timer=setTimeout(()=>{if(groupPointerPresses.get(key)===press){press.held=true;assignGroup(key);}},GROUP_HOLD_MS);
  groupPointerPresses.set(key,press);
 });
 button.addEventListener('pointermove',e=>{
  const press=groupPointerPresses.get(key);if(press?.id===e.pointerId&&Math.hypot(e.clientX-press.x,e.clientY-press.y)>12){clearTimeout(press.timer);groupPointerPresses.delete(key);}
 });
 button.addEventListener('pointerup',e=>{
  const press=groupPointerPresses.get(key);if(!press||press.id!==e.pointerId)return;
  e.preventDefault();suppressClickUntil=performance.now()+1000;clearTimeout(press.timer);groupPointerPresses.delete(key);
  if(!press.held)recallGroup(key,e.shiftKey);
 });
 const cancel=e=>{const press=groupPointerPresses.get(key);if(press?.id===e.pointerId){clearTimeout(press.timer);groupPointerPresses.delete(key);}};
 button.addEventListener('pointercancel',cancel);button.addEventListener('lostpointercapture',cancel);
 button.addEventListener('contextmenu',e=>e.preventDefault());
 // Keyboard activation of a focused button remains available for assistive technology.
 button.addEventListener('keydown',e=>{if(e.key===' '||e.key==='Enter'){suppressClickUntil=0;e.stopPropagation();}});
 button.addEventListener('click',e=>{
  if(e.detail!==0||performance.now()<suppressClickUntil){e.preventDefault();return;}
  recallGroup(key,e.shiftKey);
 });
}
function finishDrag(add){if(!drag)return;const minX=Math.min(drag.x1,drag.x2),maxX=Math.max(drag.x1,drag.x2),minY=Math.min(drag.y1,drag.y2),maxY=Math.max(drag.y1,drag.y2);let found=game.entities.filter(e=>{const s=renderer.worldToScreen(e.x,e.y-(TYPES[e.type].flying?27:0));return e.team===0&&e.hp>0&&!e.transportedBy&&s.x>=minX&&s.x<=maxX&&s.y>=minY&&s.y<=maxY;});if(found.some(e=>!TYPES[e.type].building))found=found.filter(e=>!TYPES[e.type].building);selectEntities(found,add);drag=null;}
canvas.addEventListener('contextmenu',e=>e.preventDefault());
canvas.addEventListener('pointerdown',e=>{
 e.preventDefault();canvas.focus({preventScroll:true});if(!usable())return;
 canvas.setPointerCapture(e.pointerId);const point=localPoint(e);pointer=renderer.screenToWorld(e.clientX,e.clientY);
 if(e.pointerType==='touch'){touches.set(e.pointerId,{x:e.clientX,y:e.clientY});if(touches.size>=2){clearTimeout(pointerState?.hold);drag=null;pointerState=null;const p=[...touches.values()];pinch={distance:Math.hypot(p[0].x-p[1].x,p[0].y-p[1].y),x:(p[0].x+p[1].x)/2,y:(p[0].y+p[1].y)/2};return;}}
 if(e.button===2){issue(pointer,hitTest(pointer));return;}
 if(mode==='specialist'&&abilityType==='shadowpath'&&e.button===0){routeDrawing=true;if(!routePoints.length){const unit=ownSelected().find(e=>e.type==='shears');if(unit)routePoints.push({x:unit.x,y:unit.y});}routePoints.push({...pointer});}
 pointerState={id:e.pointerId,button:e.button,start:point,last:point,startedAt:performance.now(),touch:e.pointerType==='touch',selecting:false,panning:e.button===1||keys.has(' '),moved:false,hold:null};
 if(pointerState.touch&&!mode)pointerState.hold=setTimeout(()=>{if(pointerState&&!pointerState.moved&&!pinch){pointerState.selecting=true;drag={x1:point.x,y1:point.y,x2:point.x,y2:point.y};sound('select');}},320);
});
canvas.addEventListener('pointermove',e=>{
 const point=localPoint(e);pointer=renderer.screenToWorld(e.clientX,e.clientY);
 mouseHover=e.pointerType==='mouse'?{...point,clientX:e.clientX,clientY:e.clientY}:null;
 if(e.pointerType==='touch'&&touches.has(e.pointerId))touches.set(e.pointerId,{x:e.clientX,y:e.clientY});
 if(pinch&&touches.size>=2){const p=[...touches.values()],distance=Math.hypot(p[0].x-p[1].x,p[0].y-p[1].y),x=(p[0].x+p[1].x)/2,y=(p[0].y+p[1].y)/2;renderer.zoomAt(distance/Math.max(1,pinch.distance),x,y);renderer.pan(x-pinch.x,y-pinch.y);pinch={distance,x,y};return;}
 const s=pointerState;if(!s||s.id!==e.pointerId||!usable())return;
 if(routeDrawing&&mode==='specialist'&&abilityType==='shadowpath'){const last=routePoints.at(-1);if(!last||Math.hypot(pointer.x-last.x,pointer.y-last.y)>14){routePoints.push({...pointer});s.moved=true;}s.last=point;return;}
 const distance=Math.hypot(point.x-s.start.x,point.y-s.start.y);if(distance>6){s.moved=true;clearTimeout(s.hold);}
 if(s.panning||(s.touch&&s.moved&&!s.selecting&&!mode))renderer.pan(point.x-s.last.x,point.y-s.last.y);
 else if((s.selecting||(!s.touch&&s.moved))&&!mode){s.selecting=true;drag={x1:s.start.x,y1:s.start.y,x2:point.x,y2:point.y};}
 s.last=point;
});
function pointerUp(e){
 if(e.pointerType==='touch'){touches.delete(e.pointerId);if(pinch){if(touches.size<2)pinch=null;pointerState=null;drag=null;return;}}
 const s=pointerState;if(!s||s.id!==e.pointerId)return;clearTimeout(s.hold);pointerState=null;if(!usable()){drag=null;return;}
 if(routeDrawing&&mode==='specialist'&&abilityType==='shadowpath'){routeDrawing=false;const end=renderer.screenToWorld(e.clientX,e.clientY);if(Math.hypot(end.x-routePoints.at(-1).x,end.y-routePoints.at(-1).y)>4)routePoints.push(end);if(s.moved)finishRoute();else updateUI(true);return;}
 if(s.selecting&&s.moved){finishDrag(e.shiftKey);return;}if(s.panning||s.moved){drag=null;return;}
 drag=null;const w=renderer.screenToWorld(e.clientX,e.clientY),hit=hitTest(w);
 if(mode){issue(w,hit);return;}
 if(!e.shiftKey&&hit?.team===0&&hit.type==='cd'&&ownSelected().some(e=>isCombat(e.type))){issue(w,hit);return;}
 if(s.touch&&hit?.team===0&&hit.constructing&&ownSelected().some(e=>e.type==='twig')){issue(w,hit);return;}
 if(hit?.team===0){selectEntities([hit],e.shiftKey);return;}
 if(s.touch&&selected.size){issue(w,hit);return;}
 if(!e.shiftKey)unselect();
}
canvas.addEventListener('pointerup',pointerUp);
canvas.addEventListener('pointerleave',()=>{mouseHover=null;});
canvas.addEventListener('pointercancel',e=>{touches.delete(e.pointerId);clearTimeout(pointerState?.hold);pointerState=null;pinch=null;drag=null;mouseHover=null;routeDrawing=false;});
canvas.addEventListener('dblclick',e=>{if(!usable())return;const hit=hitTest(renderer.screenToWorld(e.clientX,e.clientY));if(hit?.team===0&&!TYPES[hit.type].building){selectEntities(game.entities.filter(t=>t.team===0&&t.type===hit.type&&t.hp>0&&!t.transportedBy&&Math.hypot(t.x-hit.x,t.y-hit.y)<700));}});
canvas.addEventListener('wheel',e=>{e.preventDefault();if(!inLobby)renderer.zoomAt(Math.exp(-e.deltaY*.001),e.clientX,e.clientY);},{passive:false});
function minimapPoint(e){const r=$('minimap').getBoundingClientRect();return{x:Math.max(0,Math.min(game.width,(e.clientX-r.left)/r.width*game.width)),y:Math.max(0,Math.min(game.height,(e.clientY-r.top)/r.height*game.height))};}
$('minimap').addEventListener('pointerdown',e=>{e.preventDefault();mouseHover=null;if(!usable())return;const w=minimapPoint(e);if(e.button===2||mode==='attack'||mode==='drop'||mode==='ability'||mode==='specialist')issue(w,null,mode==='attack');else{minimapDrag=e.pointerId;e.currentTarget.setPointerCapture(e.pointerId);renderer.centerOn(w.x,w.y);}});
$('minimap').addEventListener('pointermove',e=>{if(minimapDrag!==e.pointerId||!usable())return;e.preventDefault();const w=minimapPoint(e);renderer.centerOn(w.x,w.y);});
for(const type of ['pointerup','pointercancel','lostpointercapture'])$('minimap').addEventListener(type,()=>{minimapDrag=null;});
$('minimap').addEventListener('contextmenu',e=>e.preventDefault());
window.addEventListener('keydown',e=>{
 const focused=document.activeElement;
 if(e.defaultPrevented||e.isComposing||['INPUT','TEXTAREA','SELECT'].includes(focused?.tagName)||focused?.isContentEditable)return;
 const key=e.key.toLowerCase();
 if(focused?.tagName==='BUTTON'&&(key===' '||key==='enter'))return;
 if(e.altKey||(e.ctrlKey||e.metaKey)&&!(/^[1-4]$/.test(key)&&usable()))return;
 if([' ','arrowup','arrowdown','arrowleft','arrowright','f2','f3'].includes(key)||key==='a'&&usable())e.preventDefault();
 keys.add(key);if(e.repeat){if(/^[1-4]$/.test(key)&&usable())e.preventDefault();return;}
 if(key==='escape'){e.preventDefault();if(!$('guide').hidden){closeGuide();return;}if(!inLobby&&started){if(mode){setMode(null);return;}setMode(null);togglePause();}return;}
 if(key==='x'&&usable()){unselect();return;}
 if(key==='?'||key==='f1'){e.preventDefault();openGuide();return;}
 if(key==='p'&&!inLobby){togglePause();return;}if(!usable())return;
 if(key==='f2'){selectArmy();return;}if(key==='f3'){selectWorkers();return;}if(key===' '){centerHome();return;}
 if(/^[1-4]$/.test(key)){
  e.preventDefault();
  if(e.ctrlKey||e.metaKey){assignGroup(key);return;}
  if(groupKeyPress){clearTimeout(groupKeyPress.timer);groupKeyPress=null;}
  const press={key,held:false,add:e.shiftKey,timer:null};
  press.timer=setTimeout(()=>{if(groupKeyPress===press){press.held=true;assignGroup(key);}},GROUP_HOLD_MS);
  groupKeyPress=press;return;
 }
 if(key==='enter'&&mode==='specialist'){e.preventDefault();finishRoute();return;}
 const own=ownSelected(),b=own.length===1?own[0]:null;
 if(['workshop','laboratory'].includes(b?.type)&&['q','w','e'].includes(key)){result(game.train(b.id,(b.type==='workshop'?['sniper','airplane','mage']:['divider','rollingrock','shears'])[['q','w','e'].indexOf(key)]));return;}
 if(b?.type==='printer'&&['q','w','e','r'].includes(key)){result(game.train(b.id,{q:'rock',w:'paper',e:'scissors',r:'scissors'}[key]));return;}
 if(b?.type==='desk'&&key==='q'){result(game.train(b.id,'twig'));return;}
 if(b?.type==='cdreader'&&key==='q'){result(game.train(b.id,'cd'));return;}
 if(own.some(e=>e.type==='cd')&&key==='l'){loadSelected();return;}
 if(own.some(e=>e.type==='cd')&&key==='u'){chooseDrop();return;}
 const special=Object.entries(specialistSkills).find(([,skill])=>skill.key.toLowerCase()===key&&own.some(e=>e.type===skill.type));if(special){activateSpecialist(special[0]);return;}
 if(key==='a'){setMode(mode==='attack'?null:'attack');return;}if(abilityKeys[key]){activateAbility(abilityKeys[key]);return;}
 if(own.some(e=>e.type==='twig')&&['d','c','b','v','m','n','o','z'].includes(key)){setMode(key==='z'&&mode==='wall'?null:{d:'desk',c:'cabinet',b:'printer',v:'cdreader',m:'mouse',n:'workshop',o:'laboratory',z:'wall'}[key]);return;}
 if(key==='s'&&!e.ctrlKey&&!e.metaKey){game.command([...selected],{type:'stop'});setMode(null);sound();return;}
});
window.addEventListener('keyup',e=>{
 const key=e.key.toLowerCase();keys.delete(key);
 if(groupKeyPress?.key===key){const press=groupKeyPress;clearTimeout(press.timer);groupKeyPress=null;if(!press.held)recallGroup(key,press.add);}
});
window.addEventListener('blur',()=>{keys.clear();cancelGroupPresses();clearTimeout(pointerState?.hold);pointerState=null;drag=null;mouseHover=null;minimapDrag=null;});
function togglePause(){if(inLobby||!started||game.winner!==null||endShown)return;cancelGroupPresses();mouseHover=null;paused=!paused;$('pauseOverlay').hidden=!paused;$('pauseButton').textContent=paused?'▶':'Ⅱ';keys.clear();sound();}
function openGuide(){if(!$('guide').hidden)return;cancelGroupPresses();mouseHover=null;minimapDrag=null;guideWasPaused=paused;paused=true;$('guide').hidden=false;icons($('guide'));sound();}
function closeGuide(){$('guide').hidden=true;paused=guideWasPaused;sound();}
function openLobby(){cancelGroupPresses();mouseHover=null;minimapDrag=null;if(started&&game.winner===null){$('resumeFromLobby').hidden=false;}else $('resumeFromLobby').hidden=true;inLobby=true;paused=true;$('pauseOverlay').hidden=true;$('lobby').hidden=false;setMode(null);sound();}
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
window.addEventListener('resize',()=>{mouseHover=null;renderer.resize();});
function endMatch(){cancelGroupPresses();endShown=true;paused=true;const won=game.winner===0;$('endSymbol').textContent=won?'✦':'☂';$('endEyebrow').textContent=won?'THE DESK IS YOURS':'A MINOR PAPERWORK INCIDENT';$('endTitle').textContent=won?'Beautifully trashed.':'Back to the drawing desk.';$('endMessage').textContent=won?'Your scrap pack took down every rival desk. Tiny troops, tremendous teamwork.':'The rival destroyed your desks. Try an earlier expansion, more workers, or a different troop mix.';$('endStats').innerHTML=`<div><b>${Math.floor(game.time/60)}:${String(Math.floor(game.time)%60).padStart(2,'0')}</b>TIME</div><div><b>${game.stats?.collected?.[0]??0}</b>TRASH GATHERED</div><div><b>${game.stats?.trained?.[0]??0}</b>UNITS TRAINED</div>`;$('endOverlay').hidden=false;sound(won?'victory':'defeat');}
function frame(now){const dt=Math.min(.05,(now-lastFrame)/1000);lastFrame=now;
 if(usable()){
  let dx=0,dy=0;if(keys.has('arrowleft'))dx+=480*dt;if(keys.has('arrowright'))dx-=480*dt;if(keys.has('arrowup'))dy+=480*dt;if(keys.has('arrowdown'))dy-=480*dt;
  // Desktop edge scrolling follows the cursor only over the battlefield itself.
  // A captured drag, touch, or the HUD must never start moving the camera.
  if(mouseHover&&!pointerState&&!drag&&!pinch&&!touches.size){
   const edge=26;
   const edgeSpeed=(at,size)=>at<edge?Math.min(1,(edge-at)/edge):at>size-edge?-Math.min(1,(at-size+edge)/edge):0;
   let ex=edgeSpeed(mouseHover.x,mouseHover.width),ey=edgeSpeed(mouseHover.y,mouseHover.height);const length=Math.hypot(ex,ey);if(length>1){ex/=length;ey/=length;}
   dx+=ex*680*dt;dy+=ey*680*dt;
  }
  if(dx||dy){renderer.pan(dx,dy);if(mouseHover)pointer=renderer.screenToWorld(mouseHover.clientX,mouseHover.clientY);}
  game.update(dt);if((game.winner!==null||game.teams[0].eliminated)&&!endShown)endMatch();
 }
 renderer.draw({selected,pointer,drag,mode,abilityType,routePoints,paused:paused||inLobby,time:game.time});
 uiTick+=dt;if(uiTick>=.1){uiTick=0;updateUI();renderer.drawMinimap($('minimap'));}
 requestAnimationFrame(frame);
}
resetGame();updateSound();icons();requestAnimationFrame(frame);
// Small, read-only friendly integration handle for an embedding game lobby.
window.trashTactics={get game(){return game;},get renderer(){return renderer;},get selected(){return [...selected];}};

