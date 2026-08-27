'use strict';

let enemySerial=1;
function spawnRoomEnemies(room){
 const diff=room.difficulty, pool=Object.entries(ENEMY_TYPES).filter(([,e])=>e.min<=game.level+Math.floor(diff*.7)&&e.biomes.includes(room.biome));
 if(room.boss){const b=BOSSES[Math.floor(hash2(room.x,room.y,room.seed+44)*BOSSES.length)];spawnBoss(b,room);return}
 const budget=5+diff*2.1+game.level*.45+(room.elite?5:0), max=clamp(4+Math.floor(diff*.55),4,15);let spent=0,count=0;
 while(spent<budget&&count<max&&pool.length){const [id,t]=pool[Math.floor(Math.random()*pool.length)],cost=1+Math.max(0,t.min-1)*.18;spawnEnemy(id,randomEnemySpawn(),room.elite&&count===0);spent+=cost;count++}
}
function randomEnemySpawn(){
 let p;do{p={x:rnd(ROOM_W-1.2,1.2),y:rnd(ROOM_H-1.2,1.2)}}while(Math.hypot(p.x-ROOM_W/2,p.y-ROOM_H/2)<3);return p
}
function spawnEnemy(id,pos,elite=false,scaleOverride=1){
 const t=ENEMY_TYPES[id]||ENEMY_TYPES.slime, diff=game.roomData?.difficulty||1, scale=(1+(game.level-1)*.075+(diff-1)*.06)*scaleOverride*(elite?1.75:1);
 const e={id:enemySerial++,type:id,name:elite?`Elite ${t.name}`:t.name,x:pos.x,y:pos.y,z:0,r:t.r*(elite?1.18:1),hp:t.hp*scale,maxHp:t.hp*scale,speed:t.speed*(1+Math.min(.28,diff*.012)),damage:t.damage*(1+(game.level-1)*.045)*(elite?1.32:1),color:t.color,proj:t.proj||'#f0b38d',ai:t.ai,xp:Math.round(t.xp*(elite?2.4:1)),elite,boss:false,attack:Math.random()*1.2,state:'idle',stateTime:0,stun:0,slow:0,timeSlow:1,bleed:null,curse:null,flash:0,phase:Math.random()*TAU,facing:{x:-1,y:0},shield:t.ai==='shield'?25*scale:0,shieldMax:t.ai==='shield'?25*scale:0,telegraph:null,dead:false};game.enemies.push(e);return e
}
function spawnBoss(def,room){
 const base=ENEMY_TYPES[def.base],scale=1+(game.level-1)*.09+room.difficulty*.08;const e=spawnEnemy(def.base,{x:ROOM_W/2,y:ROOM_H/2-2},true,def.hp);e.name=def.name;e.boss=true;e.elite=true;e.color=def.color;e.ai=def.ai;e.hp=base.hp*scale*def.hp;e.maxHp=e.hp;e.damage=base.damage*scale*def.damage;e.r*=1.45;e.xp=Math.round(base.xp*8);e.attack=1.2;return e
}
function enemyDir(e){return norm(game.player.x-e.x,game.player.y-e.y)}
function keepInRoom(e){e.x=clamp(e.x,.35,ROOM_W-.35);e.y=clamp(e.y,.35,ROOM_H-.35)}
function moveEnemy(e,dir,speed,dt){const m=(e.slow>0?.48:1)*(e.timeSlow??1);e.x+=dir.x*speed*dt*m;e.y+=dir.y*speed*dt*m;keepInRoom(e);if(Math.hypot(dir.x,dir.y)>.1)e.facing=dir}
function enemyProjectile(e,dir,speed=4.2,kind='enemyOrb',extra={}){game.projectiles.push({owner:'enemy',x:e.x,y:e.y,z:12,vx:dir.x*speed,vy:dir.y*speed,vz:0,r:extra.r||.13,damage:extra.damage||e.damage,life:extra.life||3,maxLife:extra.life||3,color:extra.color||e.proj,color2:extra.color2||'#fff',kind,pierce:extra.pierce||0,seek:0,splash:extra.splash||0,knock:0,trail:extra.trail||'enemy',hit:new Set(),tag:extra.tag||''})}
function enemyFan(e,count,spread,speed=4.2,kind='enemyOrb',extra={}){const d=enemyDir(e),a=Math.atan2(d.y,d.x);for(let i=0;i<count;i++){const o=count===1?0:(i/(count-1)-.5)*spread;enemyProjectile(e,{x:Math.cos(a+o),y:Math.sin(a+o)},speed,kind,extra)}}
function startEnemyTelegraph(e,kind,time,data={}){e.state=kind;e.stateTime=time;e.telegraph={kind,time,maxTime:time,...data};}
function explodeEnemyCast(e,r,damage,color=e.proj){radialPlayerThreat(e.x,e.y,r,damage);fx('explosion',e.x,e.y,.38,color,{r});burst(e.x,e.y,color,16,1.2);shake=Math.max(shake,4)}
function radialPlayerThreat(x,y,r,damage){if(Math.hypot(game.player.x-x,game.player.y-y)<r+game.player.r)damagePlayer(damage)}

function updateEnemies(dt){
 for(const e of [...game.enemies]){
  if(e.dead)continue;e.flash=Math.max(0,e.flash-dt*6);e.attack-=dt;e.stateTime-=dt;e.slow=Math.max(0,e.slow-dt);e.stun=Math.max(0,e.stun-dt);e.timeSlow=1;
  if(e.bleed){e.bleed.life-=dt;e.bleed.tick=(e.bleed.tick||0)-dt;if(e.bleed.tick<=0){e.bleed.tick=.45;damageEnemy(e,e.bleed.damage,'bleed',true)}if(e.bleed.life<=0)e.bleed=null}
  if(e.curse){e.curse.life-=dt;e.curse.tick=(e.curse.tick||0)-dt;if(e.curse.tick<=0){e.curse.tick=.5;damageEnemy(e,e.curse.damage*(e.curse.boss&&e.boss?1.6:1),'soul',true)}if(e.curse.aura)for(const o of game.enemies)if(o!==e&&dist(o,e)<1.1)damageEnemy(o,e.curse.damage*.18*dt*3,'soul',true);if(e.curse.life<=0)e.curse=null}
  if(e.stun>0)continue;
  const slowTime=e.slow>0?.62:1, td=e.timeSlow||1, speed=e.speed*slowTime*td, d=enemyDir(e), range=dist(e,game.player);
  updateEnemyAI(e,d,range,speed,dt*td);
 }
 separateEnemies();game.enemies=game.enemies.filter(e=>!e.dead);
 if(game.enemies.length===0&&game.roomData&&!game.roomData.town)markRoomCleared();
}

function updateEnemyAI(e,d,range,speed,dt){
 const ai=e.ai;
 if(ai==='melee'){moveEnemy(e,d,speed,dt);if(range<e.r+game.player.r+.22&&e.attack<=0){damagePlayer(e.damage);e.attack=.85;fx('slash',e.x,e.y,.22,'#ffc0a0',{dir:d})}}
 else if(ai==='ranged'){kite(e,d,range,speed,dt,4.8,6.6);if(e.attack<=0&&range<8){enemyProjectile(e,d,4.6,'arrow',{r:.08});e.attack=1.55}}
 else if(ai==='shield'){moveEnemy(e,d,speed,dt);if(range<1.15&&e.attack<=0){damagePlayer(e.damage);e.attack=1.3;fx('shieldBash',e.x,e.y,.26,'#d9b68e',{dir:d})}}
 else if(ai==='orbiter'){const tang={x:-d.y,y:d.x};moveEnemy(e,norm(d.x*.22+tang.x,d.y*.22+tang.y),speed,dt);if(e.attack<=0){enemyProjectile(e,d,3.9,'wispShot',{r:.1});e.attack=1.2}}
 else if(ai==='sniper'){kite(e,d,range,speed,dt,7.4,9.2);if(e.state==='aim'){if(e.stateTime<=0){enemyProjectile(e,e.telegraph.dir,8.4,'sniper',{r:.075,damage:e.damage*1.5,trail:'line'});e.state='idle';e.telegraph=null;e.attack=2.2}}else if(e.attack<=0&&range<11){startEnemyTelegraph(e,'aim',.9,{dir:d});e.attack=3}}
 else if(ai==='charger')chargerAI(e,d,range,speed,dt)
 else if(ai==='bomber'){kite(e,d,range,speed,dt,5.5,7.5);if(e.state==='bomb'){if(e.stateTime<=0){const t=e.telegraph;game.effects.push({kind:'enemyBomb',x:t.x,y:t.y,life:.45,maxLife:.45,color:e.proj,damage:e.damage*1.35,r:1.2,landed:false});e.state='idle';e.telegraph=null}}else if(e.attack<=0){const p={x:game.player.x+d.x*rnd(.8,-.8),y:game.player.y+d.y*rnd(.8,-.8)};startEnemyTelegraph(e,'bomb',.8,p);e.attack=2.4}}
 else if(ai==='summoner')summonerAI(e,d,range,speed,dt)
 else if(ai==='spread'){kite(e,d,range,speed,dt,5.2,7);if(e.attack<=0){enemyFan(e,5,1.05,4.2,'arcaneEnemy');e.attack=2}}
 else if(ai==='skirmish'){const tang={x:-d.y,y:d.x};moveEnemy(e,range<4?{x:-d.x,y:-d.y}:norm(d.x*.28+tang.x*.9,d.y*.28+tang.y*.9),speed,dt);if(e.attack<=0){enemyFan(e,2,.28,5.3,'glassDart',{r:.08});e.attack=1.25}}
 else if(ai==='blinker')blinkerAI(e,d,range,speed,dt)
 else if(ai==='turret'){if(range>6.5)moveEnemy(e,d,speed,dt);if(e.attack<=0){for(let i=0;i<8;i++){const a=TAU*i/8+elapsed*.2;enemyProjectile(e,{x:Math.cos(a),y:Math.sin(a)},3.6,'runeBolt',{r:.09})}e.attack=2.5;fx('castRing',e.x,e.y,.4,e.proj,{r:1})}}
 else if(ai==='diver')diverAI(e,d,range,speed,dt)
 else if(ai==='beam')beamAI(e,d,range,speed,dt)
 else if(ai==='shockwave')shockwaveAI(e,d,range,speed,dt)
 else if(ai==='frostmage')frostMageAI(e,d,range,speed,dt)
 else if(ai==='vampire')vampireAI(e,d,range,speed,dt)
 else if(ai==='phoenix')phoenixlingAI(e,d,range,speed,dt)
 else if(ai==='stormknight')stormKnightAI(e,d,range,speed,dt)
 else if(ai==='voideye')voidEyeAI(e,d,range,speed,dt)
 else if(ai==='drake')drakeAI(e,d,range,speed,dt)
 else if(ai==='bossGolem')bossGolemAI(e,d,range,speed,dt)
 else if(ai==='bossNecro')bossNecroAI(e,d,range,speed,dt)
 else if(ai==='bossDrake')bossDrakeAI(e,d,range,speed,dt)
 else if(ai==='bossVoid')bossVoidAI(e,d,range,speed,dt)
}
function kite(e,d,range,speed,dt,min,max){if(range<min)moveEnemy(e,{x:-d.x,y:-d.y},speed,dt);else if(range>max)moveEnemy(e,d,speed,dt);else{const t={x:-d.y,y:d.x};moveEnemy(e,t,speed*.55,dt)}}
function chargerAI(e,d,range,speed,dt){if(e.state==='chargeWind'){if(e.stateTime<=0){e.state='charging';e.stateTime=.55;e.chargeDir=e.telegraph.dir;e.telegraph=null}}else if(e.state==='charging'){moveEnemy(e,e.chargeDir,speed*5,dt);if(dist(e,game.player)<e.r+game.player.r+.18&&!e.didChargeHit){damagePlayer(e.damage*1.65);e.didChargeHit=true;shake=Math.max(shake,7)}if(e.stateTime<=0){e.state='idle';e.didChargeHit=false;e.attack=2.2}}else{moveEnemy(e,d,speed,dt);if(e.attack<=0&&range<7){startEnemyTelegraph(e,'chargeWind',.72,{dir:d});e.attack=2.8}}}
function summonerAI(e,d,range,speed,dt){kite(e,d,range,speed,dt,5,7.5);if(e.state==='summon'){if(e.stateTime<=0){for(let i=0;i<2;i++){const a=Math.random()*TAU;spawnEnemy('skeleton',{x:clamp(e.x+Math.cos(a),.6,ROOM_W-.6),y:clamp(e.y+Math.sin(a),.6,ROOM_H-.6)},false,.68)}e.state='idle';e.telegraph=null;e.attack=4}}else if(e.attack<=0){startEnemyTelegraph(e,'summon',1.15,{r:1.35});e.attack=4.5}}
function blinkerAI(e,d,range,speed,dt){if(e.state==='blink'){if(e.stateTime<=0){const a=Math.atan2(d.y,d.x);e.x=clamp(game.player.x-Math.cos(a)*1.2,.6,ROOM_W-.6);e.y=clamp(game.player.y-Math.sin(a)*1.2,.6,ROOM_H-.6);burst(e.x,e.y,e.color,12,.7);damagePlayer(e.damage*1.25);fx('slash',e.x,e.y,.25,'#e1a5ff',{dir:d});e.state='idle';e.telegraph=null;e.attack=2.5}}else{moveEnemy(e,d,speed*.8,dt);if(e.attack<=0&&range<6){startEnemyTelegraph(e,'blink',.55,{fromX:e.x,fromY:e.y});e.attack=3}}}
function diverAI(e,d,range,speed,dt){const tang={x:-d.y,y:d.x};moveEnemy(e,range>4?d:norm(tang.x*.8-d.x*.2,tang.y*.8-d.y*.2),speed,dt);if(e.attack<=0){startEnemyTelegraph(e,'dive',.55,{dir:d});e.attack=2.1}if(e.state==='dive'&&e.stateTime<=0){enemyFan(e,3,.45,5.5,'windBlade',{r:.08});e.state='idle';e.telegraph=null}}
function beamAI(e,d,range,speed,dt){kite(e,d,range,speed,dt,5.5,8.5);if(e.state==='beamAim'){if(e.stateTime<=0){const td=e.telegraph.dir;game.effects.push({kind:'enemyBeam',x:e.x,y:e.y,dx:td.x,dy:td.y,life:.65,maxLife:.65,color:e.proj,damage:e.damage*1.5,width:.22,hit:false});e.state='idle';e.telegraph=null;e.attack=3}}else if(e.attack<=0){startEnemyTelegraph(e,'beamAim',1.05,{dir:d,range:12});e.attack=3.5}}
function shockwaveAI(e,d,range,speed,dt){moveEnemy(e,d,speed,dt);if(e.state==='slam'){if(e.stateTime<=0){fx('shockRing',e.x,e.y,.8,'#e0b692',{r:4});radialPlayerThreat(e.x,e.y,3.8,e.damage*1.4);shake=Math.max(shake,8);e.state='idle';e.telegraph=null;e.attack=3.2}}else if(e.attack<=0&&range<4.8){startEnemyTelegraph(e,'slam',.85,{r:3.8});e.attack=3.8}}
function frostMageAI(e,d,range,speed,dt){kite(e,d,range,speed,dt,5,7);if(e.attack<=0){const p={x:game.player.x,y:game.player.y};startEnemyTelegraph(e,'frostCast',.75,{x:p.x,y:p.y,r:1.2});e.attack=2.3}if(e.state==='frostCast'&&e.stateTime<=0){const t=e.telegraph;game.effects.push({kind:'enemyIcePatch',x:t.x,y:t.y,life:3,maxLife:3,color:'#a5eaff',r:1.2,damage:e.damage*.55,tick:0});e.state='idle';e.telegraph=null}}
function vampireAI(e,d,range,speed,dt){moveEnemy(e,d,speed,dt);if(e.attack<=0&&range<1.1){damagePlayer(e.damage);e.hp=Math.min(e.maxHp,e.hp+e.damage*.75);fx('soulLink',game.player.x,game.player.y,.35,'#d35178',{toX:e.x,toY:e.y});e.attack=1.25}else if(e.attack<=0&&range>4){burst(e.x,e.y,e.color,8,.4);e.x=clamp(game.player.x-d.x*1.4,.7,ROOM_W-.7);e.y=clamp(game.player.y-d.y*1.4,.7,ROOM_H-.7);e.attack=2.4}}
function phoenixlingAI(e,d,range,speed,dt){const tang={x:-d.y,y:d.x};moveEnemy(e,norm(d.x*.25+tang.x,d.y*.25+tang.y),speed,dt);if(e.attack<=0){enemyFan(e,5,.9,4.8,'ember',{r:.08,splash:.25});e.attack=1.8}}
function stormKnightAI(e,d,range,speed,dt){moveEnemy(e,d,speed,dt);if(e.attack<=0){if(range<1.8){fx('lightning',e.x,e.y,.2,'#9aeaff',{toX:game.player.x,toY:game.player.y,width:4});damagePlayer(e.damage*1.3)}else enemyFan(e,3,.5,5.3,'stormBolt',{r:.09});e.attack=1.55}}
function voidEyeAI(e,d,range,speed,dt){const tang={x:-d.y,y:d.x};moveEnemy(e,tang,speed*.6,dt);if(e.attack<=0){for(let i=0;i<6;i++){const a=TAU*i/6+e.phase;enemyProjectile(e,{x:Math.cos(a),y:Math.sin(a)},3.3,'voidOrb',{r:.15,life:4})}e.phase+=.45;e.attack=1.9}}
function drakeAI(e,d,range,speed,dt){const tang={x:-d.y,y:d.x};moveEnemy(e,range<3?{x:-d.x,y:-d.y}:norm(d.x*.3+tang.x*.7,d.y*.3+tang.y*.7),speed,dt);if(e.state==='breath'){if(e.stateTime<=0){for(let i=-2;i<=2;i++){const a=Math.atan2(e.telegraph.dir.y,e.telegraph.dir.x)+i*.18;enemyProjectile(e,{x:Math.cos(a),y:Math.sin(a)},5,'fireBreath',{r:.14,splash:.22,life:1.5})}e.state='idle';e.telegraph=null;e.attack=2.5}}else if(e.attack<=0){startEnemyTelegraph(e,'breath',.7,{dir:d,cone:.5});e.attack=3}}

function bossGolemAI(e,d,range,speed,dt){moveEnemy(e,d,speed*.7,dt);if(e.attack<=0){const phase=e.hp/e.maxHp;if(Math.random()<.55){startEnemyTelegraph(e,'slam',phase<.45?.55:.85,{r:phase<.45?5:4});e.aiResume='bossGolem'}else{startEnemyTelegraph(e,'bossRocks',.8,{count:phase<.5?8:5});e.aiResume='bossGolem'}e.attack=phase<.4?2:2.8}if(e.state==='slam'&&e.stateTime<=0){fx('shockRing',e.x,e.y,.9,'#e4c29c',{r:e.telegraph.r});radialPlayerThreat(e.x,e.y,e.telegraph.r,e.damage*1.4);for(let i=0;i<10;i++){const a=i/10*TAU;enemyProjectile(e,{x:Math.cos(a),y:Math.sin(a)},3.7,'stoneShard',{r:.1})}shake=10;e.state='idle';e.telegraph=null}else if(e.state==='bossRocks'&&e.stateTime<=0){for(let i=0;i<e.telegraph.count;i++){const x=rnd(ROOM_W-2,2),y=rnd(ROOM_H-2,2);game.effects.push({kind:'enemyBomb',x,y,life:.75+Math.random()*.3,maxLife:1,color:'#c5a17d',damage:e.damage,r:1,landed:false})}e.state='idle';e.telegraph=null}}
function bossNecroAI(e,d,range,speed,dt){kite(e,d,range,speed,dt,4.5,7);if(e.attack<=0){const phase=e.hp/e.maxHp;if(Math.random()<.5){startEnemyTelegraph(e,'summon',.9,{r:2});e.bossSummon=phase<.5?4:3}else{startEnemyTelegraph(e,'beamAim',.95,{dir:d,range:12});e.bossBeam=true}e.attack=phase<.4?1.8:2.6}if(e.state==='summon'&&e.stateTime<=0){for(let i=0;i<e.bossSummon;i++){const a=TAU*i/e.bossSummon;spawnEnemy(i%2?'skeleton':'wisp',{x:clamp(e.x+Math.cos(a)*1.8,.6,ROOM_W-.6),y:clamp(e.y+Math.sin(a)*1.8,.6,ROOM_H-.6)},false,.82)}e.state='idle';e.telegraph=null}else if(e.state==='beamAim'&&e.stateTime<=0){const td=e.telegraph.dir;game.effects.push({kind:'enemyBeam',x:e.x,y:e.y,dx:td.x,dy:td.y,life:.8,maxLife:.8,color:'#d285ff',damage:e.damage*1.65,width:.3,hit:false});e.state='idle';e.telegraph=null}}
function bossDrakeAI(e,d,range,speed,dt){const tang={x:-d.y,y:d.x};moveEnemy(e,norm(tang.x*.8+d.x*.15,tang.y*.8+d.y*.15),speed*1.2,dt);if(e.attack<=0){const phase=e.hp/e.maxHp;if(Math.random()<.55){startEnemyTelegraph(e,'breath',.65,{dir:d,cone:.7});e.bossBreath=true}else{startEnemyTelegraph(e,'bossDive',.8,{dir:d});}e.attack=phase<.4?1.6:2.3}if(e.state==='breath'&&e.stateTime<=0){for(let i=-4;i<=4;i++){const a=Math.atan2(e.telegraph.dir.y,e.telegraph.dir.x)+i*.14;enemyProjectile(e,{x:Math.cos(a),y:Math.sin(a)},5.4,'fireBreath',{r:.13,splash:.32,life:1.8})}e.state='idle';e.telegraph=null}else if(e.state==='bossDive'&&e.stateTime<=0){const td=e.telegraph.dir;e.x=clamp(e.x+td.x*4,.7,ROOM_W-.7);e.y=clamp(e.y+td.y*4,.7,ROOM_H-.7);radialPlayerThreat(e.x,e.y,1.6,e.damage*1.5);fx('explosion',e.x,e.y,.4,'#ff7659',{r:1.7});groundEffect('fire',e.x,e.y,1.3,3.5,'#ff6540',e.damage*.22,.38);shake=8;e.state='idle';e.telegraph=null}}
function bossVoidAI(e,d,range,speed,dt){const tang={x:-d.y,y:d.x};moveEnemy(e,tang,speed*.8,dt);if(e.attack<=0){const phase=e.hp/e.maxHp,r=Math.random();if(r<.4){for(let i=0;i<(phase<.4?12:8);i++){const a=TAU*i/(phase<.4?12:8)+e.phase;enemyProjectile(e,{x:Math.cos(a),y:Math.sin(a)},3.2,'voidOrb',{r:.15,life:4})}e.phase+=.35}else if(r<.72){const p={x:game.player.x,y:game.player.y};game.effects.push({kind:'enemyVoidWell',x:p.x,y:p.y,life:4,maxLife:4,color:'#a454e0',r:2,damage:e.damage*.35,tick:0})}else startEnemyTelegraph(e,'beamAim',.9,{dir:d,range:14});e.attack=phase<.4?1.5:2.2}if(e.state==='beamAim'&&e.stateTime<=0){const td=e.telegraph.dir;game.effects.push({kind:'enemyBeam',x:e.x,y:e.y,dx:td.x,dy:td.y,life:.7,maxLife:.7,color:'#c45eff',damage:e.damage*1.7,width:.34,hit:false});e.state='idle';e.telegraph=null}}

function separateEnemies(){
 for(let i=0;i<game.enemies.length;i++)for(let j=i+1;j<game.enemies.length;j++){const a=game.enemies[i],b=game.enemies[j],dx=b.x-a.x,dy=b.y-a.y,d=Math.hypot(dx,dy),min=(a.r+b.r)*.82;if(d&&d<min){const p=(min-d)*.35,nx=dx/d,ny=dy/d;a.x-=nx*p;a.y-=ny*p;b.x+=nx*p;b.y+=ny*p}}
}
