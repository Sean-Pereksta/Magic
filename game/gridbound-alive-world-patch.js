// Gridbound Alive World + realtime smoothing patch.
// Injected into the existing Gridbound module scope before boot().
const ALIVE_WORLD_PATCH_VERSION='2026.08.27.1';
const ALIVE_PRESENCE_PACE_MS=520;
const ALIVE_TOWNFOLK_COUNT=12;
const ALIVE_TOWN_WILDLIFE_COUNT=7;
const ALIVE_DUNGEON_AMBIENT_LIMIT=10;
const ALIVE_DUNGEON_WILDLIFE_LIMIT=5;

let alivePresenceTimer=0;
let alivePresenceForce=false;
let alivePresenceResolvers=[];
let aliveWorldRoomKey='';
let aliveWorldLastTick=0;
let aliveTalkScanAt=0;
let aliveFootsteps=[];
let aliveEnemyHp=new Map();
let aliveImpacts=new Map();
let aliveGroundMarks=[];

function aliveResolvePresence(value){const pending=alivePresenceResolvers.splice(0);for(const resolve of pending)try{resolve(value)}catch{}}
function aliveCancelPresenceTimer(resolveValue=false){if(alivePresenceTimer){clearTimeout(alivePresenceTimer);alivePresenceTimer=0}if(alivePresenceResolvers.length)aliveResolvePresence(resolveValue);alivePresenceForce=false}
const aliveOriginalSetPresence=setPresence;
setPresence=function(force=false){
  if(force){aliveCancelPresenceTimer(false);return aliveOriginalSetPresence(true)}
  const elapsed=now()-Number(lastPresenceWriteAt||0);
  if(elapsed>=ALIVE_PRESENCE_PACE_MS&&!alivePresenceTimer)return aliveOriginalSetPresence(false);
  return new Promise(resolve=>{
    alivePresenceResolvers.push(resolve);
    if(alivePresenceTimer)return;
    const wait=Math.max(40,ALIVE_PRESENCE_PACE_MS-elapsed);
    alivePresenceTimer=setTimeout(async()=>{
      alivePresenceTimer=0;
      let ok=false;try{ok=await aliveOriginalSetPresence(alivePresenceForce)}catch{}finally{alivePresenceForce=false;aliveResolvePresence(ok)}
    },wait)
  })
};

// Firebase snapshots are intentionally rendered behind authority. The old 140 ms
// interpolation often finished long before the next room snapshot, causing stop/jump motion.
interpolateSnapshot=function(next,previous,t=now()){
  const rendered=previous?renderEntity(previous,t):null;
  const oldX=Number(rendered?.x),oldY=Number(rendered?.y),targetX=Number(next.x),targetY=Number(next.y);
  const moved=!!previous&&(Number(previous.x)!==targetX||Number(previous.y)!==targetY);
  const gap=previous?clamp(t-Number(previous.snapshotReceivedAt||t),80,1400):INTERPOLATION_MS;
  const span=Number.isFinite(oldX)&&Number.isFinite(oldY)?Math.max(Math.abs(targetX-oldX),Math.abs(targetY-oldY)):0;
  const arena=!!map?.isArena;
  let duration=0;
  if(moved){
    const buffered=gap*(arena?1.02:1.16);
    duration=clamp(buffered,arena?150:300,arena?420:950);
    if(span>3)duration=Math.min(duration,arena?210:330);
  }
  return{...next,previousX:Number.isFinite(oldX)?oldX:targetX,previousY:Number.isFinite(oldY)?oldY:targetY,targetX,targetY,snapshotReceivedAt:t,interpolationDuration:duration,snapshotGapMs:gap}
};
renderEntity=function(entity,t=now()){
  if(!entity||!Number.isFinite(Number(entity.targetX)))return entity;
  const duration=Math.max(1,Number(entity.interpolationDuration)||1),f=clamp((t-Number(entity.snapshotReceivedAt||t))/duration,0,1);
  return{...entity,x:Number(entity.previousX)+(Number(entity.targetX)-Number(entity.previousX))*f,y:Number(entity.previousY)+(Number(entity.targetY)-Number(entity.previousY))*f,_authoritativeX:entity.x,_authoritativeY:entity.y,_renderProgress:f}
};

const aliveTownNames=['Mira','Tobin','Elowen','Bram','Sela','Orin','Pip','Nara','Garrick','Ivy','Joren','Maeve','Tess','Rook','Ansel','Lena'];
const aliveTownRoles=['Baker','Courier','Porter','Gardener','Stablehand','Apprentice','Fisher','Carpenter','Traveler','Herbalist','Mason','Cook'];
const aliveTownWaypoints=[
  [8,8],[14,10],[21,8],[29,10],[37,9],[46,9],[52,13],[49,20],[42,26],[34,29],[25,28],[17,26],[10,22],[18,18],[28,19],[38,18]
];
const aliveTownWildlife=[['Sparrow','🐦'],['Sparrow','🐦'],['Street Cat','🐈'],['Mouse','🐁'],['Crow','🐦‍⬛'],['Butterflies','🦋'],['Hound','🐕']];
const aliveDungeonAmbient=[['Goblin Porter','🧌'],['Torch Bearer','🧑‍🦲'],['Relic Scavenger','🧙'],['Lost Delver','🧝'],['Bone Collector','💀'],['Pack Mule','🐐']];
const aliveDungeonWildlife=[['Cave Bat','🦇'],['Cave Rat','🐀'],['Moth Swarm','🦋'],['Cave Toad','🐸'],['Tunnel Beetle','🪲']];

function aliveOpenPoint(x,y){x=Math.round(x);y=Math.round(y);if(!inBounds(x,y)||tileAt(x,y)!==0||map?.doors?.some(d=>distance(d,{x,y})<=1))return false;if((map?.huts||[]).some(h=>x>=h.x-1&&x<=h.x+h.w&&y>=h.y-1&&y<=h.y+h.h))return false;if((map?.npcs||[]).some(n=>!n._aliveAmbient&&distance(n,{x,y})<1.5))return false;return true}
function aliveNearestOpenPoint(x,y,radius=4){
  const base={x:Math.round(x),y:Math.round(y)};if(aliveOpenPoint(base.x,base.y))return base;
  for(let r=1;r<=radius;r++)for(let oy=-r;oy<=r;oy++)for(let ox=-r;ox<=r;ox++)if(Math.max(Math.abs(ox),Math.abs(oy))===r&&aliveOpenPoint(base.x+ox,base.y+oy))return{x:base.x+ox,y:base.y+oy};
  return null
}
function aliveAmbientNpc(id,name,x,y,kind,extra={}){
  return{id,role:'ambient_world',name,graphic:'',x,y,interactRange:-1,desc:'Ambient world life',_aliveAmbient:true,_aliveKind:kind,_aliveGX:x,_aliveGY:y,_aliveFromX:x,_aliveFromY:y,_aliveToX:x,_aliveToY:y,_aliveMoveStartedAt:0,_aliveMoveEndsAt:0,_aliveTargetX:x,_aliveTargetY:y,_aliveNextMoveAt:now()+1200+Math.floor(Math.random()*4500),_aliveTalkUntil:0,...extra}
}
function aliveTownBoundsFor(n){return{x0:4,y0:4,x1:Math.max(5,map.w-5),y1:Math.max(5,map.h-5)}}
function aliveDungeonBoundsForRoom(room){return{x0:Math.round(room.x+1),y0:Math.round(room.y+1),x1:Math.round(room.x+Math.max(1,room.w-2)),y1:Math.round(room.y+Math.max(1,room.h-2))}}
function aliveAddTownPopulation(){
  const occupied=[];
  for(let i=0;i<ALIVE_TOWNFOLK_COUNT;i++){
    const base=aliveTownWaypoints[i%aliveTownWaypoints.length],p=aliveNearestOpenPoint(base[0],base[1],5);if(!p)continue;
    const name=aliveTownNames[(hashString(currentRoomId)+i*5)%aliveTownNames.length],job=aliveTownRoles[(i*7+hashString(currentRoomId))%aliveTownRoles.length];
    const npc=aliveAmbientNpc(`alive-townfolk-${i}`,`${name} · ${job}`,p.x,p.y,'townfolk',{_aliveEmoji:['🧑','👩','🧔','👩‍🌾','🧑‍🍳','🧑‍🔧'][i%6],_aliveBounds:aliveTownBoundsFor()});map.npcs.push(npc);occupied.push(npc)
  }
  for(let i=0;i<ALIVE_TOWN_WILDLIFE_COUNT;i++){
    const def=aliveTownWildlife[i%aliveTownWildlife.length],base=aliveTownWaypoints[(i*3+4)%aliveTownWaypoints.length],p=aliveNearestOpenPoint(base[0]+(i%2?2:-2),base[1]+((i%3)-1)*2,5);if(!p)continue;
    map.npcs.push(aliveAmbientNpc(`alive-town-wild-${i}`,def[0],p.x,p.y,'wildlife',{_aliveEmoji:def[1],_aliveBounds:aliveTownBoundsFor(),_aliveStepMs:360+(i%3)*80,_aliveRestMin:900,_aliveRestMax:3300}))
  }
}
function aliveAddDungeonPopulation(){
  const rooms=(map.rooms||[]).filter((room,index)=>index>0&&room&&room.w>=4&&room.h>=4).slice(0,ALIVE_DUNGEON_AMBIENT_LIMIT);
  let ambientCount=0,wildCount=0;
  for(let i=0;i<rooms.length;i++){
    const room=rooms[i],bounds=aliveDungeonBoundsForRoom(room),p=aliveNearestOpenPoint(room.cx||room.x+2,room.cy||room.y+2,3);if(!p)continue;
    if(ambientCount<ALIVE_DUNGEON_AMBIENT_LIMIT&&i%2===0){const def=aliveDungeonAmbient[(hashString(currentRoomId)+i)%aliveDungeonAmbient.length];map.npcs.push(aliveAmbientNpc(`alive-dungeon-worker-${i}`,def[0],p.x,p.y,'dungeonfolk',{_aliveEmoji:def[1],_aliveBounds:bounds,_aliveStepMs:650+(i%3)*100,_aliveRestMin:1800,_aliveRestMax:6000}));ambientCount++}
    if(wildCount<ALIVE_DUNGEON_WILDLIFE_LIMIT){const def=aliveDungeonWildlife[(i+hashString(currentRoomId))%aliveDungeonWildlife.length],wp=aliveNearestOpenPoint(p.x+((i%3)-1)*2,p.y+(i%2?2:-2),3)||p;map.npcs.push(aliveAmbientNpc(`alive-dungeon-wild-${i}`,def[0],wp.x,wp.y,'wildlife',{_aliveEmoji:def[1],_aliveBounds:bounds,_aliveStepMs:300+(i%3)*90,_aliveRestMin:700,_aliveRestMax:2500}));wildCount++}
  }
}
function aliveResetWorld(){
  aliveWorldRoomKey=`${currentRoomId}:${currentDepth}`;aliveEnemyHp.clear();aliveImpacts.clear();aliveGroundMarks=[];aliveFootsteps=[];aliveTalkScanAt=0;
  if(!map)return;map.npcs=(map.npcs||[]).filter(n=>!n._aliveAmbient);
  if(map.isTown&&!map.isArena)aliveAddTownPopulation();else if(!map.isArena)aliveAddDungeonPopulation();
  actorSortCache.key='';markEntityIndexesDirty()
}
function alivePickAmbientTarget(n,t){
  const b=n._aliveBounds||aliveTownBoundsFor(n),seed=hashString(`${aliveWorldRoomKey}:${n.id}:${Math.floor(t/1900)}:${Math.round(n._aliveGX)},${Math.round(n._aliveGY)}`),random=rngFrom(seed);
  for(let tries=0;tries<10;tries++){
    const radius=n._aliveKind==='wildlife'?5:n._aliveKind==='dungeonfolk'?4:8;
    const x=clamp(Math.round(n._aliveGX+(random()-.5)*radius*2),b.x0,b.x1),y=clamp(Math.round(n._aliveGY+(random()-.5)*radius*2),b.y0,b.y1);
    if(aliveOpenPoint(x,y)){n._aliveTargetX=x;n._aliveTargetY=y;return}
  }
  n._aliveTargetX=n._aliveGX;n._aliveTargetY=n._aliveGY
}
function aliveBeginAmbientStep(n,t){
  const gx=Math.round(n._aliveGX),gy=Math.round(n._aliveGY),tx=Math.round(n._aliveTargetX),ty=Math.round(n._aliveTargetY);if(gx===tx&&gy===ty)return false;
  const sx=Math.sign(tx-gx),sy=Math.sign(ty-gy),choices=[];if(sx||sy)choices.push({x:gx+sx,y:gy+sy});if(sx)choices.push({x:gx+sx,y:gy});if(sy)choices.push({x:gx,y:gy+sy});
  const next=choices.find(p=>aliveOpenPoint(p.x,p.y));if(!next){n._aliveTargetX=gx;n._aliveTargetY=gy;return false}
  n._aliveFromX=Number(n.x);n._aliveFromY=Number(n.y);n._aliveToX=next.x;n._aliveToY=next.y;n._aliveMoveStartedAt=t;n._aliveMoveEndsAt=t+Number(n._aliveStepMs||760);return true
}
function aliveAdvanceAmbient(n,t){
  if(Number(n._aliveTalkUntil||0)>t)return;
  if(localPlayer){const proximity=distance(localPlayer,n);if(n._aliveKind==='wildlife'&&proximity<=3){const b=n._aliveBounds||aliveTownBoundsFor(n),dx=Math.sign(Number(n.x)-Number(localPlayer.x))||1,dy=Math.sign(Number(n.y)-Number(localPlayer.y))||((hashString(n.id)%2)*2-1);const flee=aliveNearestOpenPoint(clamp(Math.round(n._aliveGX+dx*4),b.x0,b.x1),clamp(Math.round(n._aliveGY+dy*4),b.y0,b.y1),3);if(flee){n._aliveTargetX=flee.x;n._aliveTargetY=flee.y;n._aliveNextMoveAt=t}}else if(n._aliveKind==='townfolk'&&proximity<1.15){const b=n._aliveBounds||aliveTownBoundsFor(n),dx=Math.sign(Number(n.x)-Number(localPlayer.x))||1,dy=Math.sign(Number(n.y)-Number(localPlayer.y));const step=aliveNearestOpenPoint(clamp(Math.round(n._aliveGX+dx*2),b.x0,b.x1),clamp(Math.round(n._aliveGY+dy*2),b.y0,b.y1),2);if(step){n._aliveTargetX=step.x;n._aliveTargetY=step.y;n._aliveNextMoveAt=t}}}
  if(n._aliveMoveEndsAt>n._aliveMoveStartedAt&&t<n._aliveMoveEndsAt){const f=clamp((t-n._aliveMoveStartedAt)/(n._aliveMoveEndsAt-n._aliveMoveStartedAt),0,1);n.x=n._aliveFromX+(n._aliveToX-n._aliveFromX)*f;n.y=n._aliveFromY+(n._aliveToY-n._aliveFromY)*f;return}
  if(n._aliveMoveEndsAt>0){n._aliveGX=n._aliveToX;n._aliveGY=n._aliveToY;n.x=n._aliveGX;n.y=n._aliveGY;n._aliveMoveStartedAt=n._aliveMoveEndsAt=0}
  if(Math.round(n._aliveGX)===Math.round(n._aliveTargetX)&&Math.round(n._aliveGY)===Math.round(n._aliveTargetY)){
    if(t<Number(n._aliveNextMoveAt||0))return;alivePickAmbientTarget(n,t);const min=Number(n._aliveRestMin||2600),max=Number(n._aliveRestMax||7800);n._aliveNextMoveAt=t+min+Math.random()*Math.max(1,max-min)
  }
  aliveBeginAmbientStep(n,t)
}
function aliveScanConversations(t){
  if(!map?.isTown||t<aliveTalkScanAt)return;aliveTalkScanAt=t+1100;
  const people=(map.npcs||[]).filter(n=>n._aliveKind==='townfolk'&&Number(n._aliveTalkUntil||0)<=t);
  for(let i=0;i<people.length;i++)for(let j=i+1;j<people.length;j++){
    const a=people[i],b=people[j];if(distance(a,b)>1.25)continue;const roll=hashString(`${a.id}:${b.id}:${Math.floor(t/1100)}`)%100;if(roll<14){a._aliveTalkUntil=b._aliveTalkUntil=t+1800+roll*55;a._aliveTargetX=a._aliveGX;a._aliveTargetY=a._aliveGY;b._aliveTargetX=b._aliveGX;b._aliveTargetY=b._aliveGY;return}
  }
}
function aliveWorldTick(t=now()){
  if(document.hidden||!map)return;if(aliveWorldRoomKey!==`${currentRoomId}:${currentDepth}`)aliveResetWorld();if(t-aliveWorldLastTick<70)return;aliveWorldLastTick=t;
  let moved=false;for(const n of map.npcs||[])if(n._aliveAmbient){const ox=n.x,oy=n.y;aliveAdvanceAmbient(n,t);if(ox!==n.x||oy!==n.y)moved=true}
  aliveScanConversations(t);if(moved)actorSortCache.key='';
  aliveFootsteps=aliveFootsteps.filter(p=>t-p.at<p.life);aliveGroundMarks=aliveGroundMarks.filter(p=>t<p.until);for(const [id,impact] of aliveImpacts)if(t-impact.at>900)aliveImpacts.delete(id)
}
setInterval(()=>aliveWorldTick(now()),80);

function aliveDrawAmbient(n){
  if(!isVisibleTile(Math.round(n.x),Math.round(n.y)))return;const t=now(),wild=n._aliveKind==='wildlife',dungeon=n._aliveKind==='dungeonfolk',bob=Math.sin((t+hashString(n.id)%1000)/(wild?115:210))*(wild?4:2),p=worldToScreen(n.x,n.y,wild?36:52);
  ctx.save();if(dungeon)ctx.globalAlpha=.82;ctx.textAlign='center';ctx.textBaseline='middle';ctx.font=`${Math.max(15,(wild?23:28)*cam.zoom)}px serif`;ctx.fillText(n._aliveEmoji||'🧑',p.x,p.y+bob*cam.zoom);
  if(!wild&&distance(localPlayer,n)<=6){ctx.font=`800 ${Math.max(7,8*cam.zoom)}px sans-serif`;ctx.fillStyle=dungeon?'#c8bca8':'#d7eadc';ctx.fillText(n.name,p.x,p.y+18*cam.zoom)}
  if(Number(n._aliveTalkUntil||0)>t){const bubble=worldToScreen(n.x,n.y,78),pulse=.85+.15*Math.sin(t/130);ctx.globalAlpha=pulse;ctx.font=`${Math.max(14,18*cam.zoom)}px serif`;ctx.fillText('💬',bubble.x,bubble.y)}
  if(dungeon&&Math.floor((t+hashString(n.id))/850)%5===0){const hand=worldToScreen(n.x,n.y,61);ctx.globalAlpha=.75;ctx.font=`${Math.max(11,14*cam.zoom)}px serif`;ctx.fillText('📦',hand.x+12*cam.zoom,hand.y)}
  ctx.restore()
}

const aliveOriginalDrawHut=drawHut;
drawHut=function(h){
  aliveOriginalDrawHut(h);if(!map?.isTown||!h)return;const t=now(),cx=h.x+(h.w-1)/2,cy=h.y+(h.h-1)/2,base=worldToScreen(cx,cy,8),top=worldToScreen(cx-.55,cy-.55,105);ctx.save();
  for(let i=0;i<3;i++){const phase=((t/1700+i*.27)%1),r=(5+i*2)*cam.zoom;ctx.globalAlpha=.22*(1-phase);ctx.fillStyle='#d8dfd9';ctx.beginPath();ctx.arc(top.x+(Math.sin(t/560+i)*6+phase*7)*cam.zoom,top.y-phase*38*cam.zoom,r*(.7+phase),0,Math.PI*2);ctx.fill()}
  const forge=/forge|smith|anvil|blacksmith/i.test(String(h.name||''));if(forge){ctx.globalAlpha=.18+.08*Math.sin(t/160);ctx.fillStyle='#ffb45f';ctx.beginPath();ctx.ellipse(base.x,base.y-9*cam.zoom,25*cam.zoom,10*cam.zoom,0,0,Math.PI*2);ctx.fill();for(let i=0;i<3;i++){ctx.globalAlpha=.55;ctx.fillStyle='#ffd782';ctx.fillRect(base.x+(Math.sin(t/130+i*2)*16)*cam.zoom,base.y-(18+(t/35+i*11)%22)*cam.zoom,2*cam.zoom,2*cam.zoom)}}
  const sway=Math.sin((t+hashString(h.name||''))/700)*.16,sign=worldToScreen(cx+(h.w>3?1.4:1),cy,66);ctx.translate(sign.x,sign.y);ctx.rotate(sway);ctx.globalAlpha=.72;ctx.strokeStyle='#b88b56';ctx.lineWidth=Math.max(1,2*cam.zoom);ctx.beginPath();ctx.moveTo(0,-10*cam.zoom);ctx.lineTo(0,4*cam.zoom);ctx.stroke();ctx.fillStyle='#6b472c';ctx.fillRect(-7*cam.zoom,3*cam.zoom,14*cam.zoom,9*cam.zoom);ctx.restore()
};

function aliveSpawnFootsteps(from,to,t){
  const dx=Number(to.x)-Number(from.x),dy=Number(to.y)-Number(from.y),steps=Math.max(1,Math.max(Math.abs(dx),Math.abs(dy)));for(let i=0;i<Math.min(steps,3);i++){const f=(i+.35)/Math.max(1,steps),x=from.x+dx*f,y=from.y+dy*f;aliveFootsteps.push({x,y,at:t+i*55,life:900,side:i%2?1:-1})}if(aliveFootsteps.length>32)aliveFootsteps.splice(0,aliveFootsteps.length-32)
}
function aliveDrawFootsteps(t=now()){
  for(const p of aliveFootsteps){const age=t-p.at;if(age<0||age>=p.life)continue;const sp=worldToScreen(p.x,p.y,3),a=1-age/p.life;ctx.save();ctx.globalAlpha=.3*a;ctx.fillStyle=map?.isTown?'#d9c99c':map?.isArena?'#a8c8e8':'#aab1aa';ctx.beginPath();ctx.ellipse(sp.x+p.side*4*cam.zoom,sp.y,4*cam.zoom,2*cam.zoom,.25*p.side,0,Math.PI*2);ctx.fill();ctx.restore()}
}
const aliveOriginalUpdateMovement=updateMovement;
updateMovement=function(t){const from=localPlayer?{x:localPlayer.x,y:localPlayer.y}:null,result=aliveOriginalUpdateMovement(t);if(from&&localPlayer&&(from.x!==localPlayer.x||from.y!==localPlayer.y))aliveSpawnFootsteps(from,localPlayer,t||now());return result};

const aliveOriginalDrawPlayerSprite=drawPlayerSprite;
drawPlayerSprite=function(p,isLocal){
  if(isLocal)aliveDrawFootsteps();aliveOriginalDrawPlayerSprite(p,isLocal);if(!isLocal&&p&&roomState?.enemies?.some(e=>e.hp>0&&distance(e,p)<=4)){const t=now(),sp=worldToScreen(p.x,p.y,83);ctx.save();ctx.globalAlpha=.62+.18*Math.sin(t/145);ctx.font=`${Math.max(10,13*cam.zoom)}px serif`;ctx.textAlign='center';ctx.fillText('⚔️',sp.x,sp.y);ctx.restore()}
};

function aliveTrackEnemyImpact(e){
  if(!e?.id||!Number.isFinite(Number(e.hp)))return;const t=now(),previous=aliveEnemyHp.get(e.id);if(Number.isFinite(previous)&&Number(e.hp)<previous){const amount=previous-Number(e.hp);aliveImpacts.set(e.id,{at:t,x:Number(e.x),y:Number(e.y),amount});aliveGroundMarks.push({id:e.id,x:Number(e.x),y:Number(e.y),at:t,until:t+2600});if(aliveGroundMarks.length>36)aliveGroundMarks.splice(0,aliveGroundMarks.length-36);targetFlashes.set(e.id,Math.max(Number(targetFlashes.get(e.id)||0),t+170))}aliveEnemyHp.set(e.id,Number(e.hp))
}
function aliveDrawEnemyImpact(e){
  if(!e?.id)return;const t=now();for(const mark of aliveGroundMarks)if(mark.id===e.id){const life=clamp((mark.until-t)/Math.max(1,mark.until-mark.at),0,1),g=worldToScreen(mark.x,mark.y,4);ctx.save();ctx.globalAlpha=.13*life;ctx.fillStyle='#412521';ctx.beginPath();ctx.ellipse(g.x,g.y,24*cam.zoom,10*cam.zoom,0,0,Math.PI*2);ctx.fill();ctx.restore()}
  const impact=aliveImpacts.get(e.id);if(!impact)return;const age=t-impact.at;if(age>850)return;const f=age/850,p=worldToScreen(e.x,e.y,28),radius=(10+34*f)*cam.zoom;ctx.save();ctx.globalAlpha=.7*(1-f);ctx.strokeStyle=impact.amount>Math.max(1,Number(e.maxHp||1)*.12)?'#ffe39a':'#fff';ctx.lineWidth=Math.max(1,3*cam.zoom*(1-f));ctx.beginPath();ctx.arc(p.x,p.y,radius,0,Math.PI*2);ctx.stroke();for(let i=0;i<5;i++){const angle=i*1.256+hashString(e.id)%17,inner=12*cam.zoom,outer=(20+22*f)*cam.zoom;ctx.beginPath();ctx.moveTo(p.x+Math.cos(angle)*inner,p.y+Math.sin(angle)*inner);ctx.lineTo(p.x+Math.cos(angle)*outer,p.y+Math.sin(angle)*outer);ctx.stroke()}ctx.restore()
}
const aliveOriginalDrawActor=drawActor;
drawActor=function(actor){
  if(actor?.kind==='npc'&&actor.data?._aliveAmbient){aliveDrawAmbient(actor.data);return}
  if(actor?.kind==='enemy'&&actor.data){aliveTrackEnemyImpact(actor.data);aliveOriginalDrawActor(actor);aliveDrawEnemyImpact(actor.data);return}
  return aliveOriginalDrawActor(actor)
};

const aliveOriginalConnectRoom=connectRoom;
connectRoom=async function(...args){aliveCancelPresenceTimer(false);const result=await aliveOriginalConnectRoom(...args);aliveResetWorld();return result};

// Keep ambience fresh after generated maps are swapped before a room connection completes.
const aliveOriginalInvalidateStaticRendering=invalidateStaticRendering;
invalidateStaticRendering=function(...args){const result=aliveOriginalInvalidateStaticRendering(...args);actorSortCache.key='';return result};

console.info(`[Gridbound] Alive World ${ALIVE_WORLD_PATCH_VERSION} enabled · buffered Firebase rendering ${INTERPOLATION_MS}ms base`);
