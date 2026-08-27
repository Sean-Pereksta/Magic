'use strict';

/*
 * Arcane Wilds Combat Intensity Layer
 * Adds staggered reinforcement waves, coordinated squads, momentum, perfect
 * dodges, challenge-room catastrophes, spell reactions, advanced elite traits,
 * and multi-phase bosses without rewriting the base combat runtime.
 */
const ARCANE_INTENSITY_VERSION = 1;
const ARCANE_REACTION_WINDOW = 3.6;

function intensityState(){
  if(!game.intensity || game.intensity.version !== ARCANE_INTENSITY_VERSION){
    game.intensity = {
      version:ARCANE_INTENSITY_VERSION,
      momentum:0,
      combo:0,
      comboGrace:0,
      encounter:null,
      hazards:[],
      lastSpell:null,
      reactionLock:0,
      roomKey:null
    };
  }
  return game.intensity;
}

function intensityClampPoint(p){
  return {x:clamp(p.x,.75,ROOM_W-.75),y:clamp(p.y,.75,ROOM_H-.75)};
}

function intensityAvailablePool(room){
  const levelGate=game.level+Math.floor(room.difficulty*.8);
  return Object.entries(ENEMY_TYPES)
    .filter(([,t])=>t.min<=levelGate && t.biomes.includes(room.biome))
    .map(([id])=>id);
}

const INTENSITY_SQUADS=[
  ['chainwarden','mortarwitch','bladedancer'],
  ['executioner','mirrormage','warpriest'],
  ['stormcaller','bladedancer','mage'],
  ['sentinel','archer','assassin'],
  ['necro','skeleton','vampire'],
  ['bomber','charger','serpent'],
  ['warpriest','stormknight','mage'],
  ['frostwitch','sentinel','harpy'],
  ['voideye','assassin','cultist'],
  ['golem','bomber','charger']
];

function intensitySquadIds(pool,count,room,wave){
  if(!pool.length)return [];
  const valid=INTENSITY_SQUADS
    .map(s=>s.filter(id=>pool.includes(id)))
    .filter(s=>s.length>=2);
  let ids=[];
  if(valid.length){
    const preferred=valid[(Math.floor(Math.random()*valid.length)+wave)%valid.length];
    ids.push(...preferred.slice(0,Math.min(preferred.length,count)));
  }
  const challengePrefs=
    room.challenge==='Arcane Crossfire'?['mirrormage','mortarwitch','stormcaller','mage','archer']:
    room.challenge==='Execution Ground'?['executioner','chainwarden','bladedancer','charger']:
    room.challenge==='Relentless Hunt'?['assassin','bladedancer','serpent','vampire','harpy']:
    room.challenge==='Champion Patrol'?['warpriest','stormknight','sentinel','golem']:[];
  while(ids.length<count){
    const candidates=challengePrefs.filter(id=>pool.includes(id));
    const source=candidates.length && Math.random()<.58 ? candidates : pool;
    ids.push(source[irnd(source.length)]);
  }
  return ids.slice(0,count);
}

function intensityWavePoints(count){
  const pts=[];
  const edge=irnd(4);
  for(let i=0;i<count;i++){
    const lane=(i+1)/(count+1);
    let p;
    if(edge===0)p={x:.9+rnd(.55,-.25),y:1+lane*(ROOM_H-2)};
    else if(edge===1)p={x:ROOM_W-.9+rnd(.25,-.55),y:1+lane*(ROOM_H-2)};
    else if(edge===2)p={x:1+lane*(ROOM_W-2),y:.9+rnd(.55,-.25)};
    else p={x:1+lane*(ROOM_W-2),y:ROOM_H-.9+rnd(.25,-.55)};
    if(dist(p,game.player)<2.7)p=randomEnemySpawn();
    pts.push(intensityClampPoint(p));
  }
  return pts;
}

function intensitySpawnWave(enc,wave,ids,points,announce=true){
  if(!game.roomData || game.roomData.key!==enc.roomKey)return;
  let eliteUsed=false;
  for(let i=0;i<ids.length;i++){
    const point=points[i]||randomEnemySpawn();
    const later=wave>1;
    const eliteChance=(game.roomData.challenge?.22:.08)+(game.roomData.difficulty*.008)+(wave-1)*.035;
    const elite=later && !eliteUsed && Math.random()<eliteChance;
    const e=spawnEnemy(ids[i],point,elite);
    eliteUsed ||= elite;
    if(game.roomData.challenge==='Relentless Hunt'){e.speed*=1.06;e.attack-=.12}
    burst(point.x,point.y,e.proj||e.color,10,.8);
    fx('castRing',point.x,point.y,.32,e.proj||e.color,{r:.72});
  }
  enc.wave=wave;
  enc.grace=2.0;
  enc.spawnThreshold=Math.max(1,Math.floor(ids.length*.34));
  if(announce)toastMsg(`Rift surge — wave ${wave} of ${enc.totalWaves}!`);
  intensityRefreshHud();
}

function intensityScheduleWave(enc){
  const room=game.roomData;
  if(!room || enc.pending || enc.wave>=enc.totalWaves)return;
  const next=enc.wave+1;
  const count=clamp(enc.waveSize+(next===enc.totalWaves?1:0),4,8);
  const pool=intensityAvailablePool(room);
  const ids=intensitySquadIds(pool,count,room,next);
  const points=intensityWavePoints(ids.length);
  enc.pending={time:1.05,wave:next,ids,points};
  for(const p of points){
    game.telegraphs.push({kind:'circle',x:p.x,y:p.y,r:.72,time:1.05,maxTime:1.05,color:'#ca75ff',pulse:true});
    burst(p.x,p.y,'#b76cff',5,.35);
  }
  toastMsg(`Arcane rifts opening — wave ${next}!`);
}

const _intensitySpawnRoomEnemies=spawnRoomEnemies;
spawnRoomEnemies=function(room){
  if(typeof augmentRoom==='function')augmentRoom(room);
  const st=intensityState();
  st.roomKey=room.key;
  st.hazards.length=0;
  st.lastSpell=null;
  st.reactionLock=0;
  if(room.boss){
    st.encounter={roomKey:room.key,wave:1,totalWaves:1,boss:true,pending:null,grace:0,hazardCd:2.4};
    const b=BOSSES[Math.floor(hash2(room.x,room.y,room.seed+44)*BOSSES.length)];
    spawnBoss(b,room);
    return;
  }
  const pool=intensityAvailablePool(room);
  if(!pool.length)return _intensitySpawnRoomEnemies(room);
  const totalWaves=clamp(2+(room.difficulty>=6?1:0)+(room.difficulty>=10||room.challenge?1:0),2,4);
  const initialCount=clamp(5+Math.floor(room.difficulty*.28)+(room.challenge?1:0),5,9);
  const waveSize=clamp(4+Math.floor(room.difficulty*.18)+(game.level>=12?1:0),4,7);
  const enc=st.encounter={
    roomKey:room.key,wave:0,totalWaves,boss:false,pending:null,grace:2.2,
    hazardCd:room.challenge?2.4:999,spawnThreshold:2,waveSize
  };
  const ids=intensitySquadIds(pool,initialCount,room,1);
  intensitySpawnWave(enc,1,ids,intensityWavePoints(ids.length),false);
};

const _intensityMarkRoomCleared=markRoomCleared;
markRoomCleared=function(){
  const enc=intensityState().encounter;
  if(enc && game.roomData && enc.roomKey===game.roomData.key && !enc.boss){
    if(enc.pending || enc.wave<enc.totalWaves)return;
  }
  _intensityMarkRoomCleared();
};

function intensityQueueHazard(kind,points,time,damage,color,radius=.85,extra={}){
  const st=intensityState();
  const event={kind,points:points.map(intensityClampPoint),time,damage,color,radius,...extra};
  st.hazards.push(event);
  for(const p of event.points){
    game.telegraphs.push({kind:'circle',x:p.x,y:p.y,r:radius,time,maxTime:time,color,pulse:true});
  }
  return event;
}

function intensityChallengeHazard(room,enc){
  const p=game.player;
  if(room.challenge==='Arcane Crossfire'){
    const horizontal=Math.random()<.5;
    const base=horizontal?clamp(p.y+rnd(1.4,-1.4),1.2,ROOM_H-1.2):clamp(p.x+rnd(1.4,-1.4),1.2,ROOM_W-1.2);
    const pts=[];
    for(let i=0;i<6;i++){
      pts.push(horizontal?{x:1.5+i*(ROOM_W-3)/5,y:base}:{x:base,y:1.4+i*(ROOM_H-2.8)/5});
    }
    intensityQueueHazard('crossfire',pts,.78,12+room.difficulty*1.25,'#9feaff',.72);
    enc.hazardCd=3.3;
  }else if(room.challenge==='Execution Ground'){
    const pts=[{x:p.x,y:p.y},{x:p.x+rnd(2.5,-2.5),y:p.y+rnd(2,-2)},{x:p.x+rnd(3,-3),y:p.y+rnd(2.4,-2.4)}];
    intensityQueueHazard('execution',pts,.92,18+room.difficulty*1.5,'#ff785f',1.05);
    enc.hazardCd=3.8;
  }else if(room.challenge==='Relentless Hunt'){
    const point=intensityClampPoint({x:p.x-p.facing.x*2.6+rnd(.7,-.7),y:p.y-p.facing.y*2.6+rnd(.7,-.7)});
    intensityQueueHazard('hunt',[point],.82,0,'#c77cff',.78);
    enc.hazardCd=4.2;
  }else if(room.challenge==='Champion Patrol'){
    const pts=[];
    for(let i=0;i<3;i++)pts.push({x:p.x+rnd(3,-3),y:p.y+rnd(2.4,-2.4)});
    intensityQueueHazard('minefield',pts,1.05,14+room.difficulty*1.15,'#ffc36a',.9);
    enc.hazardCd=4.5;
  }
}

function intensityResolveHazard(h){
  if(!game.roomData)return;
  if(h.kind==='hunt'){
    const pool=intensityAvailablePool(game.roomData);
    const hunters=['bladedancer','assassin','serpent','vampire','harpy','charger'].filter(id=>pool.includes(id));
    if(hunters.length && game.enemies.length<22){
      const e=spawnEnemy(hunters[irnd(hunters.length)],h.points[0],false);
      e.speed*=1.1;e.attack-=.18;
      burst(e.x,e.y,'#ca79ff',14,1.1);
    }
    return;
  }
  for(const p of h.points){
    radialPlayerThreat(p.x,p.y,h.radius,h.damage);
    if(h.kind==='crossfire'){
      fx('lightning',p.x,p.y,.18,h.color,{toX:p.x+rnd(.25,-.25),toY:p.y-.35,width:3});
      burst(p.x,p.y,h.color,9,.8);
    }else{
      fx(h.kind==='execution'?'shockRing':'explosion',p.x,p.y,.35,h.color,{r:h.radius*1.25});
      burst(p.x,p.y,h.color,12,1.05);
    }
  }
  shake=Math.max(shake,h.kind==='execution'?7:4);
}

function intensityTickEncounter(dt){
  const st=intensityState(), enc=st.encounter, room=game.roomData;
  if(!enc || !room || enc.roomKey!==room.key || room.town || room.cleared)return;
  enc.grace=Math.max(0,(enc.grace||0)-dt);
  if(enc.pending){
    enc.pending.time-=dt;
    if(enc.pending.time<=0){
      const p=enc.pending;enc.pending=null;
      intensitySpawnWave(enc,p.wave,p.ids,p.points,true);
    }
  }else if(!enc.boss && enc.wave<enc.totalWaves && enc.grace<=0 && game.enemies.length<=enc.spawnThreshold){
    intensityScheduleWave(enc);
  }
  if(room.challenge && !enc.boss){
    enc.hazardCd=(enc.hazardCd??2.8)-dt;
    if(enc.hazardCd<=0)intensityChallengeHazard(room,enc);
  }
}

function intensityTickHazards(dt){
  const st=intensityState();
  for(const h of st.hazards)h.time-=dt;
  const ready=st.hazards.filter(h=>h.time<=0);
  st.hazards=st.hazards.filter(h=>h.time>0);
  for(const h of ready)intensityResolveHazard(h);
}

function intensityAddMomentum(amount,label=''){
  const st=intensityState();
  st.momentum=clamp(st.momentum+amount,0,100);
  st.comboGrace=2.4;
  if(label)floatText(game.player.x,game.player.y-.45,label,'#ffd76d');
  intensityRefreshHud();
}

const _intensityKillEnemy=killEnemy;
killEnemy=function(e,tag=''){
  if(!e||e.dead)return;
  const elite=!!e.elite,boss=!!e.boss;
  _intensityKillEnemy(e,tag);
  if(!e.dead)return;
  const st=intensityState();
  st.combo=st.comboGrace>0?Math.min(12,st.combo+1):1;
  const gain=(boss?24:elite?13:5)+Math.min(10,st.combo*1.15);
  intensityAddMomentum(gain,st.combo>=4?`ARCANE CHAIN ×${st.combo}`:'');
  for(const o of game.enemies){
    if(o.dead || o===e || o.intensityTrait!=='Vengeful')continue;
    o.vengeance=Math.min(5,(o.vengeance||0)+1);
    o.damage*=1.045;
    o.speed*=1.025;
    burst(o.x,o.y,'#ff5c7b',7,.55);
  }
};

const _intensityWeaponDamage=weaponDamage;
weaponDamage=function(){
  const m=intensityState().momentum;
  return _intensityWeaponDamage()*(1+m*.0025);
};

const _intensitySpellMods=spellMods;
spellMods=function(spell){
  const m=_intensitySpellMods(spell),momentum=intensityState().momentum;
  return {...m,power:m.power*(1+momentum*.0018),cdr:Math.max(.62,m.cdr*(1-momentum*.00105))};
};

function intensityImminentThreat(){
  const p=game.player;if(!p)return false;
  for(const q of game.projectiles){
    if(q.owner!=='enemy')continue;
    const dx=p.x-q.x,dy=p.y-q.y,d=Math.hypot(dx,dy),spd=Math.hypot(q.vx,q.vy)||1;
    const toward=(dx*q.vx+dy*q.vy)/(Math.max(.01,d)*spd);
    if(d<Math.max(1.05,spd*.24)+p.r+q.r && toward>.35)return true;
  }
  for(const t of game.telegraphs){
    if(t.time>.3)continue;
    if(Number.isFinite(t.x)&&Number.isFinite(t.y)&&Number.isFinite(t.r) && Math.hypot(p.x-t.x,p.y-t.y)<t.r+p.r+.15)return true;
  }
  for(const e of game.enemies){
    if(e.dead || !e.telegraph || e.stateTime>.3)continue;
    const t=e.telegraph;
    if(Number.isFinite(t.x)&&Number.isFinite(t.y)&&Number.isFinite(t.r) && Math.hypot(p.x-t.x,p.y-t.y)<t.r+p.r+.15)return true;
    if(Number.isFinite(t.r) && !Number.isFinite(t.x) && dist(e,p)<t.r+p.r+.18)return true;
    if(t.dir && t.cone && typeof playerInCone==='function' && playerInCone(e,t.dir,t.range||6,t.cone))return true;
    if(t.dir && !t.cone && typeof playerInLine==='function' && playerInLine(e,t.dir,t.range||9,.62))return true;
  }
  return false;
}

function intensityPerfectDodge(){
  const st=intensityState(),p=game.player;
  intensityAddMomentum(14,'PERFECT DODGE');
  p.dodgeCd=Math.max(.35,p.dodgeCd-.32);
  for(const s of Object.values(p.spellState||{}))s.cd=Math.max(0,(s.cd||0)-.7);
  radialDamage(p.x,p.y,1.7,10+game.level*1.35,'arcane',.32);
  fx('shockRing',p.x,p.y,.38,'#f4e7ff',{r:1.8});
  burst(p.x,p.y,'#e7c7ff',24,1.2);
  shake=Math.max(shake,4);
  st.comboGrace=Math.max(st.comboGrace,2.8);
}

const _intensityDodge=dodge;
dodge=function(){
  const p=game.player;
  const ready=!!p && p.dodgeCd<=0 && !paused && !modalPause;
  const perfect=ready && intensityImminentThreat();
  _intensityDodge();
  if(perfect && p.dodgeTime>0)intensityPerfectDodge();
};

const INTENSITY_REACTIONS={
  'frostnova+quake':'shatter',
  'chain+frostnova':'conductive',
  'chain+icelance':'conductive',
  'firebolt+gust':'firestorm',
  'gust+meteor':'firestorm',
  'meteor+voidrift':'cataclysm',
  'firebolt+poison':'toxic',
  'meteor+poison':'toxic',
  'chakram+tempest':'stormmoon'
};

function intensityReactionKey(a,b){return [a,b].sort().join('+')}

function intensityTriggerReaction(kind){
  const p=game.player;
  if(kind==='shatter'){
    for(const e of game.enemies){
      if(e.dead||dist(e,p)>4.3)continue;
      const chilled=e.slow>0||e.stun>0;
      damageEnemy(e,(chilled?30:18)+game.level*1.5,'frost');
      if(chilled&&!e.boss)e.stun=Math.max(e.stun,.55);
    }
    fx('frostNova',p.x,p.y,.72,'#d8fbff',{r:4.4,ring:true});burst(p.x,p.y,'#d5f6ff',28,1.5);
    toastMsg('SPELL REACTION — SHATTERWAKE!');
  }else if(kind==='conductive'){
    const targets=game.enemies.filter(e=>!e.dead&&(e.slow>0||e.stun>0)).sort((a,b)=>dist(a,p)-dist(b,p)).slice(0,6);
    let from={x:p.x,y:p.y};
    for(const e of targets){
      fx('lightning',from.x,from.y,.2,'#a9f4ff',{toX:e.x,toY:e.y,width:3});
      damageEnemy(e,22+game.level*1.25,'lightning');
      from=e;
    }
    toastMsg('SPELL REACTION — CONDUCTIVE SHATTER!');
  }else if(kind==='firestorm'){
    const d=p.facing||{x:1,y:0},pt=intensityClampPoint({x:p.x+d.x*2.4,y:p.y+d.y*2.4});
    groundEffect('fire',pt.x,pt.y,2.15,4.2,'#ff7545',14+game.level*1.1,.28);
    groundEffect('tornado',pt.x,pt.y,1.45,3.8,'#ffc06a',8+game.level*.7,.25,{moving:true});
    fx('explosion',pt.x,pt.y,.45,'#ff9b55',{r:2.2});burst(pt.x,pt.y,'#ffad63',30,1.7);
    toastMsg('SPELL REACTION — FIRESTORM!');
  }else if(kind==='cataclysm'){
    const pt=aimPoint(4.5);
    for(const e of game.enemies){
      if(e.dead)continue;
      const d=Math.hypot(e.x-pt.x,e.y-pt.y);
      if(d<3.3 && d>.05){e.x+=(pt.x-e.x)/d*.8;e.y+=(pt.y-e.y)/d*.8}
    }
    radialDamage(pt.x,pt.y,2.55,42+game.level*2.0,'arcane',.4);
    fx('riftOpen',pt.x,pt.y,.55,'#c46fff',{r:2.8});fx('explosion',pt.x,pt.y,.38,'#ff8c62',{r:2.1});
    burst(pt.x,pt.y,'#d687ff',36,2);
    shake=Math.max(shake,7);
    toastMsg('SPELL REACTION — RIFT CATACLYSM!');
  }else if(kind==='toxic'){
    const blooms=game.hazards.filter(h=>h.kind==='poison').slice(0,3);
    if(blooms.length){
      for(const h of blooms){
        radialDamage(h.x,h.y,h.r*1.45,28+game.level*1.45,'fire');
        fx('explosion',h.x,h.y,.4,'#9ff067',{r:h.r*1.5});burst(h.x,h.y,'#b7f46f',22,1.4);
        h.life=Math.min(h.life,1.2);
      }
    }else{
      const pt=aimPoint(3.5);radialDamage(pt.x,pt.y,1.8,24+game.level*1.2,'fire');fx('explosion',pt.x,pt.y,.4,'#a9ee67',{r:1.8});
    }
    toastMsg('SPELL REACTION — TOXIC COMBUSTION!');
  }else if(kind==='stormmoon'){
    const targets=game.enemies.filter(e=>!e.dead).sort((a,b)=>dist(a,p)-dist(b,p)).slice(0,7);
    for(const e of targets){
      fx('lightning',p.x,p.y,.18,'#b7edff',{toX:e.x,toY:e.y,width:2});
      damageEnemy(e,17+game.level,'lightning');
    }
    fx('castRing',p.x,p.y,.5,'#bda8ff',{r:2.2});burst(p.x,p.y,'#a8eaff',26,1.2);
    toastMsg('SPELL REACTION — STORM MOONS!');
  }
  intensityAddMomentum(10);
}

function intensityRecordSpell(id){
  const st=intensityState();
  if(st.reactionLock>0){st.lastSpell={id,time:ARCANE_REACTION_WINDOW};return}
  if(st.lastSpell && st.lastSpell.id!==id && st.lastSpell.time>0){
    const kind=INTENSITY_REACTIONS[intensityReactionKey(st.lastSpell.id,id)];
    if(kind){
      intensityTriggerReaction(kind);
      st.reactionLock=1.1;
      st.lastSpell=null;
      return;
    }
  }
  st.lastSpell={id,time:ARCANE_REACTION_WINDOW};
}

const _intensityCastSpell=castSpell;
castSpell=function(slot){
  const id=game.player?.activeSpells?.[slot];
  if(!id)return _intensityCastSpell(slot);
  const before=game.player.spellState[id]?.cd||0;
  _intensityCastSpell(slot);
  const after=game.player.spellState[id]?.cd||0;
  if(after>before+.02)intensityRecordSpell(id);
};

const INTENSITY_ELITE_TRAITS=[
  {name:'Stormbound',color:'#84e9ff'},
  {name:'Commander',color:'#ffd36c'},
  {name:'Riftborn',color:'#cc79ff'},
  {name:'Gravecaller',color:'#c5b2d8'},
  {name:'Vengeful',color:'#ff657c'}
];

const _intensitySpawnEnemy=spawnEnemy;
spawnEnemy=function(id,pos,elite=false,scaleOverride=1){
  const e=_intensitySpawnEnemy(id,pos,elite,scaleOverride);
  if(elite && scaleOverride===1 && !e.boss){
    const room=game.roomData;
    const chance=room?.challenge?.62:room?.difficulty>=6?.43:.28;
    if(Math.random()<chance){
      const tr=INTENSITY_ELITE_TRAITS[irnd(INTENSITY_ELITE_TRAITS.length)];
      e.intensityTrait=tr.name;
      e.intensityColor=tr.color;
      e.intensityCd=1.7+rnd(1.2);
      e.name=`${tr.name} ${e.name}`;
      if(tr.name==='Commander')e.shield=Math.max(e.shield||0,18+game.level*2);
      if(tr.name==='Vengeful')e.vengeance=0;
    }
  }
  return e;
};

const _intensityDamageEnemy=damageEnemy;
damageEnemy=function(e,amount,tag='',dot=false){
  if(!e||e.dead)return;
  const before=e.hp;
  _intensityDamageEnemy(e,amount,tag,dot);
  if(!dot && !e.dead && e.intensityTrait==='Riftborn' && e.hp<before && (e.intensityCd||0)<=0 && Math.random()<.42){
    const p=game.player,ang=Math.atan2(e.y-p.y,e.x-p.x)+rnd(1.1,-1.1);
    e.x=clamp(p.x+Math.cos(ang)*rnd(5,3.2),.7,ROOM_W-.7);
    e.y=clamp(p.y+Math.sin(ang)*rnd(4.5,3),.7,ROOM_H-.7);
    e.intensityCd=2.4;
    burst(e.x,e.y,'#cb77ff',15,1.0);fx('castRing',e.x,e.y,.3,'#cb77ff',{r:.9});
  }
};

function intensityTickEliteTraits(dt){
  for(const e of game.enemies){
    if(e.dead||!e.intensityTrait)continue;
    e.intensityCd=(e.intensityCd||0)-dt;
    if(Math.random()<dt*.55)game.particles.push({x:e.x+rnd(.35,-.35),y:e.y+rnd(.35,-.35),z:rnd(16,5),vx:rnd(.12,-.12),vy:rnd(.12,-.12),vz:.3,life:.5,maxLife:.5,size:2.4,color:e.intensityColor||'#fff',soft:true});
    if(e.intensityTrait==='Stormbound' && e.intensityCd<=0){
      const pt={x:game.player.x,y:game.player.y};
      intensityQueueHazard('crossfire',[pt],.62,e.damage*.65,e.intensityColor,.72);
      e.intensityCd=2.4;
    }else if(e.intensityTrait==='Commander'){
      for(const o of game.enemies)if(o!==e&&!o.dead&&dist(o,e)<4.4)o.attack-=dt*.12;
      if(e.intensityCd<=0){fx('castRing',e.x,e.y,.45,e.intensityColor,{r:2.0});e.intensityCd=2.2}
    }else if(e.intensityTrait==='Gravecaller' && e.intensityCd<=0){
      if(game.enemies.length<20){
        for(let i=0;i<2;i++){
          const a=Math.random()*TAU;
          spawnEnemy('skeleton',intensityClampPoint({x:e.x+Math.cos(a)*1.2,y:e.y+Math.sin(a)*1.2}),false,.68);
        }
        fx('summon',e.x,e.y,.55,e.intensityColor,{r:1.25});
      }
      e.intensityCd=4.7;
    }else if(e.intensityTrait==='Vengeful' && e.intensityCd<=0){
      if((e.vengeance||0)>0)fx('castRing',e.x,e.y,.32,e.intensityColor,{r:1+(e.vengeance||0)*.12});
      e.intensityCd=1.9;
    }
  }
}

function intensityBossAdds(e,phase){
  const map={
    bossGolem:['sentinel','charger'],
    bossNecro:['skeleton','vampire','warpriest'],
    bossDrake:['phoenixling','bomber','harpy'],
    bossVoid:['voideye','assassin','cultist'],
    bossExecutioner:['chainwarden','bladedancer'],
    bossOracle:['stormcaller','mirrormage','harpy']
  };
  const pool=(map[e.ai]||[]).filter(id=>ENEMY_TYPES[id] && ENEMY_TYPES[id].min<=game.level+game.roomData.difficulty);
  if(!pool.length)return;
  const count=phase===3?3:2;
  for(let i=0;i<count && game.enemies.length<22;i++){
    const a=TAU*i/count+Math.random()*.4;
    const pos=intensityClampPoint({x:e.x+Math.cos(a)*2.1,y:e.y+Math.sin(a)*1.7});
    spawnEnemy(pool[i%pool.length],pos,false,.82);
    burst(pos.x,pos.y,e.proj||e.color,10,.8);
  }
}

function intensityBossHazard(e){
  const p=game.player,phase=e.intensityPhase||1;
  if(e.ai==='bossOracle'){
    const pts=[{x:p.x,y:p.y}];
    for(let i=1;i<phase+2;i++){const a=TAU*i/(phase+2);pts.push({x:p.x+Math.cos(a)*1.7,y:p.y+Math.sin(a)*1.35})}
    intensityQueueHazard('crossfire',pts,phase>=3?.58:.72,e.damage*.72,e.proj||'#8de8ff',.82);
  }else if(e.ai==='bossExecutioner'){
    const pts=[{x:p.x,y:p.y},{x:p.x+rnd(2.6,-2.6),y:p.y+rnd(2,-2)}];
    if(phase>=3)pts.push({x:p.x+rnd(3,-3),y:p.y+rnd(2.4,-2.4)});
    intensityQueueHazard('execution',pts,phase>=3?.68:.85,e.damage*.78,e.proj||'#ff9d6a',1.08);
  }else if(e.ai==='bossGolem'){
    const pts=[];for(let i=0;i<6;i++){const a=TAU*i/6;pts.push({x:e.x+Math.cos(a)*2.8,y:e.y+Math.sin(a)*2.2})}
    intensityQueueHazard('minefield',pts,.86,e.damage*.6,'#e0b692',.78);
  }else if(e.ai==='bossNecro'){
    if(game.enemies.length<18)intensityBossAdds(e,2);
  }else if(e.ai==='bossDrake'){
    const d=norm(p.x-e.x,p.y-e.y),pts=[];for(let i=1;i<=5;i++)pts.push({x:e.x+d.x*i*1.15,y:e.y+d.y*i*1.15});
    intensityQueueHazard('execution',pts,.72,e.damage*.58,'#ff7b55',.68);
  }else if(e.ai==='bossVoid'){
    const pts=[{x:p.x,y:p.y},{x:p.x+rnd(2.2,-2.2),y:p.y+rnd(1.8,-1.8)}];
    intensityQueueHazard('crossfire',pts,.78,e.damage*.66,'#c168ff',1.0);
  }
}

function intensityBossPhase(e,phase){
  e.intensityPhase=phase;
  e.speed*=phase===2?1.1:1.14;
  e.damage*=phase===2?1.08:1.1;
  e.attack=Math.min(e.attack,phase===2?.65:.38);
  e.intensityBossCd=phase===2?2.8:1.9;
  intensityBossAdds(e,phase);
  burst(e.x,e.y,e.proj||e.color,phase===3?42:30,phase===3?2.4:1.8);
  fx('shockRing',e.x,e.y,.65,e.proj||e.color,{r:phase===3?3.2:2.5});
  shake=Math.max(shake,phase===3?10:7);
  toastMsg(`${e.name} — PHASE ${phase}!`);
}

function intensityTickBosses(dt){
  for(const e of game.enemies){
    if(e.dead||!e.boss)continue;
    if(!e.intensityPhase){e.intensityPhase=1;e.intensityBossCd=3.4}
    const ratio=e.hp/Math.max(1,e.maxHp);
    if(ratio<=.35 && e.intensityPhase<3)intensityBossPhase(e,3);
    else if(ratio<=.70 && e.intensityPhase<2)intensityBossPhase(e,2);
    if(e.intensityPhase>=2){
      e.intensityBossCd-=dt;
      if(e.intensityBossCd<=0){
        intensityBossHazard(e);
        e.intensityBossCd=e.intensityPhase>=3?2.0:3.1;
      }
    }
  }
}

function intensityTickMomentum(dt){
  const st=intensityState();
  st.comboGrace=Math.max(0,st.comboGrace-dt);
  st.reactionLock=Math.max(0,st.reactionLock-dt);
  if(st.lastSpell)st.lastSpell.time-=dt;
  if(st.lastSpell && st.lastSpell.time<=0)st.lastSpell=null;
  if(st.comboGrace<=0){
    st.momentum=Math.max(0,st.momentum-dt*(st.momentum>70?7:5));
    if(st.momentum===0)st.combo=0;
  }
}

function intensityEnsureHud(){
  if(document.getElementById('arcaneIntensityHud'))return;
  const style=document.createElement('style');
  style.textContent=`
    #arcaneIntensityHud{position:fixed;left:50%;bottom:78px;transform:translateX(-50%);z-index:25;min-width:250px;pointer-events:none;
      background:rgba(7,10,20,.76);border:1px solid rgba(196,117,255,.52);border-radius:14px;padding:8px 11px;
      box-shadow:0 8px 28px rgba(0,0,0,.4),0 0 24px rgba(164,89,255,.12);backdrop-filter:blur(7px);color:#eef3ff;font:800 11px/1.1 system-ui,sans-serif}
    #arcaneIntensityHud.hidden{display:none}
    #arcaneIntensityHud .aw-row{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:6px}
    #arcaneIntensityHud .aw-label{letter-spacing:.16em;color:#e5c8ff}
    #arcaneIntensityHud .aw-wave{color:#aeeeff}
    #arcaneIntensityHud .aw-track{height:6px;border-radius:999px;overflow:hidden;background:rgba(255,255,255,.09)}
    #arcaneIntensityHud .aw-fill{height:100%;width:0;background:linear-gradient(90deg,#7555ff,#d260ff,#ffd05b);box-shadow:0 0 14px rgba(207,96,255,.6);transition:width .12s linear}
    @media (max-width:760px){#arcaneIntensityHud{bottom:142px;min-width:210px;padding:7px 9px}}
  `;
  document.head.appendChild(style);
  const hud=document.createElement('div');
  hud.id='arcaneIntensityHud';hud.className='hidden';
  hud.innerHTML='<div class="aw-row"><span class="aw-label">ARCANE MOMENTUM</span><span class="aw-wave"></span></div><div class="aw-track"><div class="aw-fill"></div></div>';
  document.body.appendChild(hud);
}

function intensityRefreshHud(){
  intensityEnsureHud();
  const el=document.getElementById('arcaneIntensityHud'),st=intensityState(),room=game.roomData,enc=st.encounter;
  if(!running||!room||room.town){el.classList.add('hidden');return}
  el.classList.remove('hidden');
  el.querySelector('.aw-fill').style.width=`${st.momentum.toFixed(1)}%`;
  const wave=el.querySelector('.aw-wave');
  wave.textContent=enc?.boss?`BOSS P${game.enemies.find(e=>e.boss)?.intensityPhase||1}`:enc?`WAVE ${enc.wave}/${enc.totalWaves}`:'';
  el.style.opacity=st.momentum>0||enc?'1':'.72';
}

const _intensityLoadRoom=loadRoom;
loadRoom=function(){
  _intensityLoadRoom();
  const st=intensityState(),room=game.roomData;
  if(!room)return;
  st.roomKey=room.key;
  st.hazards.length=0;
  st.lastSpell=null;
  if(room.town||room.cleared)st.encounter=null;
  intensityRefreshHud();
};

const _intensityUpdate=update;
update=function(dt){
  _intensityUpdate(dt);
  if(!running||paused||modalPause||roomTransition||!game.player)return;
  intensityTickMomentum(dt);
  intensityTickEncounter(dt);
  intensityTickHazards(dt);
  intensityTickEliteTraits(dt);
  intensityTickBosses(dt);
  intensityRefreshHud();
};

intensityEnsureHud();
