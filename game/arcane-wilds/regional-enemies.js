'use strict';
(() => {
  const rows=[
    ['thornrunner','Thornrunner','verdant','beast',52,2.2,10,.32,'#88ae69'],['groveShaman','Grove Shaman','verdant','shaman',70,.95,10,.36,'#a0bd77'],['sporeling','Sporeling','verdant','spore',30,1.2,7,.27,'#b7c969'],['mossbackGuardian','Mossback Guardian','verdant','moss',160,.6,18,.65,'#70836a'],['briarWitch','Briar Witch','verdant','briar',85,1.1,12,.34,'#c788ae'],['packAlpha','Pack Alpha','verdant','alpha',110,1.6,14,.45,'#c4b998'],
    ['prismMimic','Prism Mimic','meridian','mimic',100,1.2,15,.36,'#bbdeea'],['thunderRam','Thunder Ram','meridian','ram',155,1.2,20,.52,'#96d6e8'],['shardcaster','Shardcaster','meridian','shard',95,1,16,.34,'#c2eeff'],['frostRevenant','Frost Revenant','meridian','frost',130,.8,16,.43,'#aeeaff'],['cinderWasp','Cinder Wasp','meridian','wasp',40,2.4,13,.25,'#ffae72'],['stormCaptain','Storm Captain','meridian','captain',150,1.1,20,.47,'#a7cae8'],['emberburrower','Emberburrower','meridian','burrow',110,1.4,20,.42,'#e6ad82'],
    ['voidShepherd','Void Shepherd','gloam','shepherd',140,.95,20,.4,'#c392e6'],['astralArcher','Astral Archer','gloam','archer',100,1.1,27,.33,'#ffe8bc'],['soulLeech','Soul Leech','gloam','leech',65,1.8,13,.25,'#d3a4e6'],['eclipseKnight','Eclipse Knight','gloam','knight',230,.9,25,.53,'#dcd8ed'],['riftStalker','Rift Stalker','gloam','stalker',120,1.5,23,.34,'#bb96ec'],['starboundOracle','Starbound Oracle','gloam','oracle',150,.9,25,.38,'#e8d29d'],['gloamDevourer','Gloam Devourer','gloam','devourer',290,.75,30,.67,'#9a7ca8']
  ];
  for(const [id,name,region,behavior,hp,speed,damage,r,color] of rows)ENEMY_TYPES[id]={name,biomes:AWCampaignData.continent(region).biomes,min:region==='verdant'?1:region==='meridian'?9:18,hp,speed,damage,r,color,proj:color,xp:Math.round(hp*.2),ai:`regional_${behavior}`,regionalBehavior:behavior,region};
  ENEMY_TYPES.regionalHealPlant={name:'Healing Bloom',biomes:[],min:1,hp:24,speed:0,damage:0,r:.22,color:'#c6e69c',xp:0,ai:'regional_plant'};
  ENEMY_TYPES.regionalPrismEcho={name:'False Prism',biomes:[],min:1,hp:1,speed:1.1,damage:7,r:.33,color:'#b7cfe0',xp:0,ai:'regional_echo'};
  ENEMY_TYPES.regionalCrystal={name:'Hostile Crystal',biomes:[],min:1,hp:35,speed:0,damage:0,r:.48,color:'#afdcee',xp:0,ai:'regional_wall'};
  let zones=[];
  const addZone=(at,r,life,damage,color,delay=.6,kind='pool')=>{zones.push({...at,r,life,damage,color,delay,kind,tick:0});if(zones.length>24)zones.shift();};
  const oldSpawn=spawnRoomEnemies;spawnRoomEnemies=function(room){const n=AWCampaignData.nodes[room.campaignNode];if(!n||n.shadow||room.boss)return oldSpawn(room);
    const all=rows.filter(r=>r[2]===n.continent).map(r=>r[0]);
    const groups={verdant:[['packAlpha','thornrunner','thornrunner','groveShaman'],['mossbackGuardian','briarWitch','sporeling','sporeling']],meridian:[['stormCaptain','thunderRam','shardcaster','cinderWasp'],['prismMimic','frostRevenant','emberburrower','shardcaster']],gloam:[['voidShepherd','starboundOracle','soulLeech','eclipseKnight'],['gloamDevourer','riftStalker','astralArcher','soulLeech']]};
    if(n.index%3===0){oldSpawn(room);return;}
    const st=intensityState();st.roomKey=room.key;st.encounter={roomKey:room.key,wave:1,totalWaves:1,pending:null,grace:0,hazardCd:999};
    const ids=groups[n.continent][n.index%2];ids.forEach((id,i)=>spawnEnemy(id,randomEnemySpawn(),room.elite&&i===0));
    if(n.type==='danger')spawnEnemy(all[n.index%all.length],randomEnemySpawn(),true);
  };
  function strike(e,range){if(range<e.r+game.player.r+.65&&e.attack<=0){damagePlayer(e.damage);e.attack=1.2;fx('slash',e.x,e.y,.25,e.color,{dir:enemyDir(e)});}}
  function windup(e,time,at=game.player){e.regionalAim={x:at.x,y:at.y};e.state='regionalWind';e.stateTime=time;telegraph('circle',at.x,at.y,1.1,time,e.color);}
  function rush(e,d,speed,dt,range){
    if(e.state==='regionalWind'){if(e.stateTime<=0){e.state='regionalRush';e.stateTime=.75;e.regionalDir=norm(e.regionalAim.x-e.x,e.regionalAim.y-e.y);e.regionalHit=false;}return;}
    if(e.state==='regionalRush'){const dir=e.regionalDir;e.x+=dir.x*speed*4*dt;e.y+=dir.y*speed*4*dt;if(range<e.r+game.player.r+.3&&!e.regionalHit){damagePlayer(e.damage*1.3);e.regionalHit=true;}
      const wall=e.x<.5||e.x>ROOM_W-.5||e.y<.5||e.y>ROOM_H-.5;keepInRoom(e);if(wall||e.stateTime<=0){e.state='idle';e.attack=2.5;if(wall){e.stun=1.2;fx('shockRing',e.x,e.y,.4,e.color,{r:1});}}return;}
    moveEnemy(e,norm(d.x*.35-d.y*.9,d.y*.35+d.x*.9),speed,dt);if(e.attack<=0&&range<9){windup(e,.7);e.attack=3;}
  }
  const baseAI=updateEnemyAI;updateEnemyAI=function(e,d,range,speed,dt){
    const behavior=e.ai.startsWith('regional_')?e.ai.slice(9):null;
    const alpha=game.enemies.find(a=>a!==e&&!a.dead&&a.type==='packAlpha'&&dist(a,e)<4);
    if(alpha&&/wolf|thornrunner|wasp|ram|charger/.test(e.type))speed*=1.2;
    if(!behavior)return baseAI(e,d,range,speed,dt);
    AWRegionalContent.incoming=e;
    try{
      if(['beast','ram','wasp'].includes(behavior))return rush(e,d,speed,dt,range);
      if(behavior==='plant'){e.regionalLife=(e.regionalLife??9)-dt;if(e.regionalLife<=0){e.dead=true;return;}for(const a of game.enemies)if(a!==e&&!a.dead&&dist(a,e)<3)a.hp=Math.min(a.maxHp,a.hp+dt*6);return;}
      if(behavior==='wall'){e.regionalLife=(e.regionalLife??6)-dt;if(e.regionalLife<=0)e.dead=true;return;}
      if(behavior==='echo'){moveEnemy(e,d,speed,dt);strike(e,range);e.regionalLife=(e.regionalLife??5)-dt;if(e.regionalLife<=0)e.dead=true;return;}
      if(behavior==='shaman'){kite(e,d,range,speed,dt,4,6);if(e.attack<=0){if(game.enemies.filter(a=>a.type==='regionalHealPlant').length<3)spawnEnemy('regionalHealPlant',{x:clamp(e.x+1,.6,ROOM_W-.6),y:e.y});e.attack=6;}return;}
      if(behavior==='moss'){const old=e.facing||d,face=norm(lerp(old.x,d.x,dt*1.2),lerp(old.y,d.y,dt*1.2));moveEnemy(e,face,speed,dt);strike(e,range);return;}
      if(behavior==='briar'){kite(e,d,range,speed,dt,3,5);if(e.attack<=0){for(let i=-2;i<=2;i++)addZone({x:game.player.x-d.y*i*.8,y:game.player.y+d.x*i*.8},.55,4,e.damage*.45,e.color,.8,'thorns');e.attack=4;}return;}
      if(behavior==='alpha'||behavior==='spore'){moveEnemy(e,d,speed,dt);strike(e,range);return;}
      if(behavior==='mimic'){kite(e,d,range,speed,dt,3,5);if(e.attack<=0){if(game.enemies.filter(a=>a.type==='regionalPrismEcho').length<4)for(const side of [-1,1])spawnEnemy('regionalPrismEcho',{x:clamp(e.x+side*1.2,.6,ROOM_W-.6),y:e.y});enemyFan(e,3,.55,4.5);e.attack=4;}return;}
      if(behavior==='shard'){kite(e,d,range,speed,dt,4,6);if(e.attack<=0){if(game.enemies.filter(a=>a.type==='regionalCrystal').length<5)for(const side of [-1,1])spawnEnemy('regionalCrystal',{x:clamp(game.player.x+d.y*side,.6,ROOM_W-.6),y:clamp(game.player.y-d.x*side+2,.6,ROOM_H-.6)});enemyFan(e,2,.3,5);e.attack=5;}return;}
      if(behavior==='frost'){moveEnemy(e,d,speed,dt);if(range<3)game.player.regionalChill=Math.max(game.player.regionalChill||0,1);strike(e,range);return;}
      if(behavior==='captain'){moveEnemy(e,d,speed,dt);strike(e,range);e.regionalSignal=(e.regionalSignal??3)-dt;if(e.regionalSignal<=0){for(const a of game.enemies)if(a!==e&&!a.dead&&dist(a,e)<6)a.attack=Math.min(a.attack,.6);fx('shockRing',e.x,e.y,.7,e.color,{r:5});e.regionalSignal=6;}return;}
      if(behavior==='burrow'){if(e.state==='burrow'){const aim=e.regionalAim,dir=norm(aim.x-e.x,aim.y-e.y);moveEnemy(e,dir,speed*2,dt);if(e.stateTime<=0){e.state='idle';e.attack=3;addZone(e,1.5,.4,e.damage,e.color,0,'burst');}return;}moveEnemy(e,d,speed,dt);if(e.attack<=0){e.state='burrow';e.stateTime=1.3;e.regionalAim={x:game.player.x,y:game.player.y};telegraph('circle',e.regionalAim.x,e.regionalAim.y,1.5,1.3,e.color);}return;}
      if(behavior==='shepherd'){kite(e,d,range,speed,dt,3,5);if(range<6&&!(AWRegionalContent.timers.anchored>0)){game.player.x=clamp(game.player.x-d.x*dt*.45,.3,ROOM_W-.3);game.player.y=clamp(game.player.y-d.y*dt*.45,.3,ROOM_H-.3);}if(e.attack<=0){enemyFan(e,3,.8,3.5);e.attack=2.6;}return;}
      if(behavior==='archer'||behavior==='oracle'){
        if(e.state==='regionalWind'){if(e.stateTime<=0){if(behavior==='archer'){const aim=norm(e.regionalAim.x-e.x,e.regionalAim.y-e.y);enemyProjectile(e,aim,10,'sunLance',{life:2,damage:e.damage*1.3});}else addZone(e.regionalAim,1.4,.3,e.damage*1.2,e.color,0,'burst');e.state='idle';e.attack=3;}return;}
        kite(e,d,range,speed,dt,5,8);if(e.attack<=0){const move=AWInput.move,at=behavior==='oracle'?{x:clamp(game.player.x+move.x*1.4,.7,ROOM_W-.7),y:clamp(game.player.y+move.y*1.4,.7,ROOM_H-.7)}:game.player;windup(e,.9,at);}return;
      }
      if(behavior==='leech'){if(e.regionalAttached){e.x=game.player.x+.3;e.y=game.player.y+.2;game.player.regionalChill=.3;if(game.player.dodgeTime>0){e.regionalAttached=false;e.stun=1;e.x+=1.3;return;}if(e.attack<=0){damagePlayer(e.damage*.35);e.attack=.8;}return;}moveEnemy(e,d,speed,dt);if(range<e.r+game.player.r+.1)e.regionalAttached=true;return;}
      if(behavior==='knight'){e.regionalShadow=Math.floor((elapsed+e.phase)/4)%2===1;moveEnemy(e,d,speed*(e.regionalShadow?1.6:.65),dt);if(e.regionalShadow&&e.attack<=0&&range<1.4){damagePlayer(e.damage*1.3);e.attack=1.2;}else strike(e,range);return;}
      if(behavior==='stalker'){if(e.state==='regionalWind'){if(e.stateTime<=0){e.x=e.regionalAim.x;e.y=e.regionalAim.y;e.state='idle';e.attack=1;fx('riftOpen',e.x,e.y,.4,e.color,{r:1});}return;}moveEnemy(e,d,speed,dt);strike(e,range);e.regionalBlink=(e.regionalBlink??3)-dt;if(e.regionalBlink<=0){windup(e,.65,{x:clamp(game.player.x-game.player.facing.x*2,.7,ROOM_W-.7),y:clamp(game.player.y-game.player.facing.y*2,.7,ROOM_H-.7)});e.regionalBlink=5;}return;}
      if(behavior==='devourer'){moveEnemy(e,d,speed,dt);strike(e,range);e.regionalHunger=(e.regionalHunger??4)-dt;if(e.regionalHunger<=0&&(e.regionalMeals||0)<4){const food=game.enemies.find(a=>a!==e&&!a.boss&&!a.dead&&a.maxHp<e.maxHp&&dist(a,e)<3);if(food){food.dead=true;e.regionalMeals=(e.regionalMeals||0)+1;e.hp=Math.min(e.maxHp,e.hp+e.maxHp*.15);e.damage*=1.1;e.r*=1.05;fx('soulLink',food.x,food.y,.5,e.color,{toX:e.x,toY:e.y});}e.regionalHunger=5;}return;}
    }finally{AWRegionalContent.incoming=null;}
  };
  const baseHit=damageEnemy;damageEnemy=function(e,amount,tag,dot){if(e?.ai==='regional_moss'&&!dot){const d=norm(game.player.x-e.x,game.player.y-e.y),f=e.facing||d;amount*=d.x*f.x+d.y*f.y>.3?.38:1.3;}if(e?.ai==='regional_knight'&&!e.regionalShadow)amount*=.55;if(e?.ai==='regional_burrow'&&e.state==='burrow')amount*=.2;return baseHit(e,amount,tag,dot);};
  const baseKill=killEnemy;killEnemy=function(e,tag){if(!e||e.dead)return;if(e.type==='sporeling')addZone(e,1.3,3,e.damage*.45,'#b5cd7e',.35);if(['regionalPrismEcho','regionalHealPlant','regionalCrystal'].includes(e.type)){e.dead=true;return;}return baseKill(e,tag);};
  const baseUpdate=updateEnemyEffects;updateEnemyEffects=function(dt){baseUpdate(dt);if(!running||paused||modalPause||roomTransition)return;
    for(const z of zones){z.delay-=dt;if(z.delay>0)continue;z.life-=dt;z.tick-=dt;if(z.tick<=0){z.tick=.6;if(dist(game.player,z)<z.r+game.player.r){damagePlayer(z.damage);if(z.kind==='thorns')game.player.regionalChill=.8;}}}zones=zones.filter(z=>z.life>0);
    for(const wall of game.enemies.filter(e=>e.type==='regionalCrystal'&&!e.dead)){const p=game.player,dd=dist(p,wall),r=p.r+wall.r;if(dd<r&&p.dodgeTime<=0){const d=norm(p.x-wall.x,p.y-wall.y);p.x=clamp(wall.x+d.x*r,.3,ROOM_W-.3);p.y=clamp(wall.y+d.y*r,.3,ROOM_H-.3);}}
  };
  const baseLoad=loadRoom;loadRoom=function(){zones=[];return baseLoad();};
  const baseDraw=drawEnemy;drawEnemy=function(e){baseDraw(e);if(!e.ai.startsWith('regional_'))return;const s=worldToScreen(e.x,e.y,35);ctx.save();ctx.strokeStyle=e.color;ctx.fillStyle=e.color;ctx.lineWidth=2;
    if(['regional_shaman','regional_plant','regional_spore'].includes(e.ai)){ctx.beginPath();ctx.ellipse(s.x,s.y,16,7,0,Math.PI,TAU);ctx.fill();}
    else if(['regional_wasp','regional_oracle'].includes(e.ai)){for(const side of [-1,1]){ctx.beginPath();ctx.ellipse(s.x+side*13,s.y,12,4,side*Math.sin(elapsed*10),0,TAU);ctx.stroke();}}
    else if(e.type==='mossbackGuardian'||e.type==='eclipseKnight'){ctx.strokeRect(s.x-12,s.y,24,24);if(e.regionalShadow){ctx.fillStyle='#bc86ef';ctx.fillRect(s.x-10,s.y+2,20,20);}}
    else {ctx.beginPath();ctx.moveTo(s.x-10,s.y);ctx.lineTo(s.x,s.y-10);ctx.lineTo(s.x+10,s.y);ctx.stroke();}
    if(e.state==='burrow'){ctx.globalAlpha=.6;ctx.beginPath();ctx.ellipse(s.x,s.y+35,18,5,0,0,TAU);ctx.stroke();}
    if(e.state==='regionalWind'&&e.regionalAim){const at=worldToScreen(e.regionalAim.x,e.regionalAim.y);ctx.beginPath();ctx.moveTo(s.x,s.y);ctx.lineTo(at.x,at.y);ctx.stroke();}
    ctx.restore();};
  const baseFront=drawEffectsFront;drawEffectsFront=function(){baseFront();ctx.save();for(const z of zones){const s=worldToScreen(z.x,z.y);ctx.strokeStyle=z.color;ctx.globalAlpha=z.delay>0?.4:.8;ctx.lineWidth=2;ctx.beginPath();ctx.ellipse(s.x,s.y,z.r*TILE_W*.55,z.r*TILE_H*.55,0,0,TAU);ctx.stroke();}ctx.restore();};
  window.AWRegionalEnemies={ids:rows.map(r=>r[0]),zones:()=>zones,addZone};
})();
