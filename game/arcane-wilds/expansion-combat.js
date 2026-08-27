/* New high-threat enemy archetypes. */
Object.assign(ENEMY_TYPES,{
 bladedancer:{name:'Glassblade Dancer',icon:'✣',biomes:['ruins','desert','frost'],min:8,hp:88,speed:2.0,damage:18,r:.34,ai:'bladeDancer',color:'#7da6b5',proj:'#d5f5ff',xp:40},
 mortarwitch:{name:'Ash Mortar Witch',icon:'✹',biomes:['volcanic','swamp','desert'],min:9,hp:96,speed:1.0,damage:21,r:.37,ai:'mortarWitch',color:'#9a4f63',proj:'#ff895d',xp:44},
 chainwarden:{name:'Chain Warden',icon:'⛓',biomes:['ruins','crypt'],min:10,hp:155,speed:.95,damage:22,r:.45,ai:'chainWarden',color:'#707783',proj:'#d6c5a3',xp:52},
 mirrormage:{name:'Mirror Mage',icon:'◈',biomes:['frost','ruins','crypt'],min:11,hp:92,speed:1.2,damage:17,r:.35,ai:'mirrorMage',color:'#6f74aa',proj:'#b9c6ff',xp:49},
 burrower:{name:'Grave Burrower',icon:'⬣',biomes:['crypt','swamp','desert'],min:12,hp:175,speed:1.05,damage:25,r:.5,ai:'burrower',color:'#766856',proj:'#d0b78e',xp:58},
 warpriest:{name:'Rift Warpriest',icon:'✠',biomes:['crypt','ruins','volcanic'],min:13,hp:125,speed:1.0,damage:19,r:.39,ai:'warPriest',color:'#744d91',proj:'#d78cff',xp:60},
 stormcaller:{name:'Tempest Caller',icon:'ϟ',biomes:['frost','meadow','ruins'],min:14,hp:118,speed:1.15,damage:22,r:.38,ai:'stormCaller',color:'#4e7f96',proj:'#8de8ff',xp:62},
 executioner:{name:'Iron Executioner',icon:'⚒',biomes:['ruins','volcanic','crypt'],min:15,hp:235,speed:.78,damage:31,r:.56,ai:'executioner',color:'#695c59',proj:'#ffb174',xp:78}
});
BOSSES.push(
 {base:'executioner',name:'The Gilded Executioner',color:'#a87848',hp:4.6,damage:1.28,ai:'bossExecutioner'},
 {base:'stormcaller',name:'Myr, Stormbound Oracle',color:'#55a8bd',hp:4.9,damage:1.3,ai:'bossOracle'}
);

const _baseSpawnEnemy=spawnEnemy;
spawnEnemy=function(id,pos,elite=false,scaleOverride=1){
 const e=_baseSpawnEnemy(id,pos,elite,scaleOverride);if(elite&&scaleOverride===1&&!e.boss){const traits=['Frenzied','Bulwark','Volatile','Hunting'];e.trait=traits[irnd(traits.length)];e.name=`${e.trait} ${e.name}`;if(e.trait==='Frenzied'){e.speed*=1.2;e.damage*=1.12}else if(e.trait==='Bulwark'){e.hp*=1.28;e.maxHp=e.hp;e.shield=Math.max(e.shield,30+game.level*4);e.shieldMax=e.shield}else if(e.trait==='Hunting'){e.damage*=1.2;e.attack-=.35}}
 return e;
};
const _baseSpawnRoomEnemies=spawnRoomEnemies;
spawnRoomEnemies=function(room){
 augmentRoom(room);_baseSpawnRoomEnemies(room);if(room.town||room.boss||!room.challenge)return;
 const roomLeft=Math.max(0,20-game.enemies.length),extra=Math.min(roomLeft,1+Math.floor(room.difficulty/5)+(room.challenge==='Execution Ground'?2:0));
 const preferred=room.challenge==='Arcane Crossfire'?['mirrormage','mortarwitch','stormcaller','mage','archer']:room.challenge==='Execution Ground'?['executioner','chainwarden','bladedancer']:Object.keys(ENEMY_TYPES);
 const pool=preferred.filter(id=>ENEMY_TYPES[id]&&ENEMY_TYPES[id].min<=game.level+Math.floor(room.difficulty*.8)&&ENEMY_TYPES[id].biomes.includes(room.biome));
 for(let i=0;i<extra&&pool.length;i++)spawnEnemy(pool[irnd(pool.length)],randomEnemySpawn(),room.challenge==='Champion Patrol'&&i===0);
 if(room.challenge==='Relentless Hunt')for(const e of game.enemies){e.speed*=1.09;e.attack-=.2}
};

function playerInLine(e,dir,range,width=.6){const p=game.player,rx=p.x-e.x,ry=p.y-e.y,along=rx*dir.x+ry*dir.y,side=Math.abs(rx*(-dir.y)+ry*dir.x);return along>0&&along<range&&side<width+p.r}
function playerInCone(e,dir,range,halfAngle=.55){const v=norm(game.player.x-e.x,game.player.y-e.y),d=dist(e,game.player);return d<range&&v.x*dir.x+v.y*dir.y>Math.cos(halfAngle)}
function bladeDancerAI(e,d,range,speed,dt){
 if(e.state==='bladeWind'){if(e.stateTime<=0){e.state='bladeDash';e.stateTime=.42;e.chargeDir=e.telegraph.dir;e.telegraph=null;e.didChargeHit=false}}else if(e.state==='bladeDash'){moveEnemy(e,e.chargeDir,speed*4.8,dt);if(!e.didChargeHit&&dist(e,game.player)<e.r+game.player.r+.28){damagePlayer(e.damage*1.45);e.didChargeHit=true;fx('slash',e.x,e.y,.24,'#dcf7ff',{dir:e.chargeDir})}if(e.stateTime<=0){e.state='idle';e.attack=1.55}}
 else{const t={x:-d.y,y:d.x};moveEnemy(e,range<2.2?{x:-d.x,y:-d.y}:norm(d.x*.45+t.x*.7,d.y*.45+t.y*.7),speed,dt);if(e.attack<=0&&range<7)startEnemyTelegraph(e,'bladeWind',.55,{dir:d,range:6,cone:.16})}
}
function mortarWitchAI(e,d,range,speed,dt){
 kite(e,d,range,speed,dt,5.5,8);if(e.state==='mortarMark'&&e.stateTime<=0){const pts=e.mortarPoints||[];for(const p of pts){radialPlayerThreat(p.x,p.y,1.05,e.damage*1.25);fx('explosion',p.x,p.y,.35,e.proj,{r:1.2});burst(p.x,p.y,e.proj,12,1.1)}e.state='idle';e.telegraph=null;e.attack=2.8}
 else if(e.attack<=0&&e.state==='idle'){const p={x:game.player.x,y:game.player.y};e.mortarPoints=[p,{x:clamp(p.x+1.45,.7,ROOM_W-.7),y:clamp(p.y-.8,.7,ROOM_H-.7)},{x:clamp(p.x-1.25,.7,ROOM_W-.7),y:clamp(p.y+1,.7,ROOM_H-.7)}];startEnemyTelegraph(e,'mortarMark',.9,{x:p.x,y:p.y,r:1.05});for(const q of e.mortarPoints.slice(1))game.telegraphs.push({kind:'circle',x:q.x,y:q.y,r:1.05,time:.9,maxTime:.9,color:e.proj})}
}
function chainWardenAI(e,d,range,speed,dt){
 if(e.state==='hookAim'&&e.stateTime<=0){const dir=e.telegraph.dir;if(playerInLine(e,dir,8,.48)){damagePlayer(e.damage*1.1);const pull=norm(e.x-game.player.x,e.y-game.player.y);game.player.x+=pull.x*1.8;game.player.y+=pull.y*1.8;fx('lightning',e.x,e.y,.28,'#d7c29c',{toX:game.player.x,toY:game.player.y,width:3});shake=Math.max(shake,5)}e.state='idle';e.telegraph=null;e.attack=2.4}
 else{if(range>4.2)moveEnemy(e,d,speed,dt);else if(range<2.2)moveEnemy(e,{x:-d.x,y:-d.y},speed*.7,dt);if(e.attack<=0&&e.state==='idle'&&range<9)startEnemyTelegraph(e,'hookAim',.85,{dir:d,range:8})}
}
function mirrorMageAI(e,d,range,speed,dt){
 kite(e,d,range,speed,dt,5.2,7.4);if(e.state==='mirrorCast'&&e.stateTime<=0){for(let i=0;i<10;i++){const a=TAU*i/10+elapsed*.55;enemyProjectile(e,{x:Math.cos(a),y:Math.sin(a)},4.4,'mirrorShard',{r:.08,damage:e.damage})}const side=Math.random()<.5?-1:1;e.x=clamp(game.player.x-d.x*4+(-d.y)*side*2,.7,ROOM_W-.7);e.y=clamp(game.player.y-d.y*4+(d.x)*side*2,.7,ROOM_H-.7);burst(e.x,e.y,e.proj,14,.8);e.state='idle';e.telegraph=null;e.attack=2.5}
 else if(e.attack<=0&&e.state==='idle')startEnemyTelegraph(e,'mirrorCast',.75,{r:1.25})
}
function burrowerAI(e,d,range,speed,dt){
 if(e.state==='burrowMark'&&e.stateTime<=0){const t=e.telegraph;e.x=t.x;e.y=t.y;radialPlayerThreat(e.x,e.y,1.5,e.damage*1.55);fx('shockRing',e.x,e.y,.55,e.proj,{r:1.8});burst(e.x,e.y,e.proj,20,1.5);e.hidden=false;e.state='idle';e.telegraph=null;e.attack=3.1}
 else{moveEnemy(e,d,speed*.72,dt);if(e.attack<=0&&e.state==='idle'&&range<8){e.hidden=true;startEnemyTelegraph(e,'burrowMark',1.05,{x:game.player.x,y:game.player.y,r:1.5})}}
}
function warPriestAI(e,d,range,speed,dt){
 kite(e,d,range,speed,dt,4.8,7);if(e.state==='warPrayer'&&e.stateTime<=0){for(const o of game.enemies)if(!o.dead&&dist(o,e)<4.8){const heal=o.maxHp*.09;o.hp=Math.min(o.maxHp,o.hp+heal);burst(o.x,o.y,'#d595ff',5,.35)}enemyFan(e,3,.65,4.5,'priestBolt',{r:.1,damage:e.damage});e.state='idle';e.telegraph=null;e.attack=3.8}
 else if(e.attack<=0&&e.state==='idle')startEnemyTelegraph(e,'warPrayer',1,{r:2.2})
}
function stormCallerAI(e,d,range,speed,dt,boss=false){
 kite(e,d,range,speed,dt,boss?4.5:5.2,boss?7:8);if(e.state==='stormMark'&&e.stateTime<=0){for(const p of e.strikePoints||[]){radialPlayerThreat(p.x,p.y,boss?1.25:.9,e.damage*(boss?1.3:1.05));fx('lightning',p.x,p.y,.18,e.proj,{toX:p.x+rnd(.2,-.2),toY:p.y-.2,width:boss?5:3});burst(p.x,p.y,e.proj,12,1.15)}if(boss)enemyFan(e,7,1.35,5.1,'stormShard',{r:.08,damage:e.damage*.7});e.state='idle';e.telegraph=null;e.attack=boss?1.8:2.9}
 else if(e.attack<=0&&e.state==='idle'){const p={x:game.player.x,y:game.player.y},n=boss?5:3;e.strikePoints=[];for(let i=0;i<n;i++){const a=i/n*TAU;e.strikePoints.push(i===0?p:{x:clamp(p.x+Math.cos(a)*1.6,.7,ROOM_W-.7),y:clamp(p.y+Math.sin(a)*1.3,.7,ROOM_H-.7)})}startEnemyTelegraph(e,'stormMark',boss?.7:.9,{x:p.x,y:p.y,r:boss?1.25:.9});for(const q of e.strikePoints.slice(1))game.telegraphs.push({kind:'circle',x:q.x,y:q.y,r:boss?1.25:.9,time:boss?.7:.9,maxTime:boss?.7:.9,color:e.proj})}
}
function executionerAI(e,d,range,speed,dt,boss=false){
 if(e.state==='executeWind'&&e.stateTime<=0){const dir=e.telegraph.dir;if(playerInCone(e,dir,boss?5.3:4.4,boss?.72:.56))damagePlayer(e.damage*(boss?1.85:1.65));fx('shockRing',e.x,e.y,.55,e.proj,{r:boss?3.2:2.5});if(boss)enemyFan(e,5,1.0,4.6,'axeShard',{r:.1,damage:e.damage*.55});shake=Math.max(shake,boss?10:7);e.state='idle';e.telegraph=null;e.attack=boss?1.7:2.6}
 else{moveEnemy(e,d,speed,dt);if(e.attack<=0&&e.state==='idle'&&range<(boss?6:5))startEnemyTelegraph(e,'executeWind',boss?.75:1.0,{dir:d,range:boss?5.3:4.4,cone:boss?.72:.56})}
}
const _baseUpdateEnemyAI=updateEnemyAI;
updateEnemyAI=function(e,d,range,speed,dt){
 if(e.ai==='bladeDancer')return bladeDancerAI(e,d,range,speed,dt);
 if(e.ai==='mortarWitch')return mortarWitchAI(e,d,range,speed,dt);
 if(e.ai==='chainWarden')return chainWardenAI(e,d,range,speed,dt);
 if(e.ai==='mirrorMage')return mirrorMageAI(e,d,range,speed,dt);
 if(e.ai==='burrower')return burrowerAI(e,d,range,speed,dt);
 if(e.ai==='warPriest')return warPriestAI(e,d,range,speed,dt);
 if(e.ai==='stormCaller')return stormCallerAI(e,d,range,speed,dt,false);
 if(e.ai==='executioner')return executionerAI(e,d,range,speed,dt,false);
 if(e.ai==='bossExecutioner')return executionerAI(e,d,range,speed,dt,true);
 if(e.ai==='bossOracle')return stormCallerAI(e,d,range,speed,dt,true);
 return _baseUpdateEnemyAI(e,d,range,speed,dt);
};

/* Loot/material economy. Gear is intentionally scarce outside bosses and crafting. */
rollLoot=function(chance=1){
 let actual=chance>=.99?.82:chance>=.59?.16:chance>=.49?.11:Math.min(.08,chance*.18);if(Math.random()>actual)return false;return dropGearNow(chance>=.99?'boss':chance>=.59?'elite':'cache');
};
const _baseKillEnemy=killEnemy;
killEnemy=function(e,tag=''){
 if(!e||e.dead)return;const beforeGold=game.gold,_boss=e.boss,_elite=e.elite,volatile=e.trait==='Volatile',volatileDamage=e.damage;_baseKillEnemy(e,tag);
 if(volatile){radialPlayerThreat(e.x,e.y,1.65,volatileDamage*.55);fx('explosion',e.x,e.y,.4,'#ff8a4b',{r:1.65});burst(e.x,e.y,'#ff9b55',18,1.6)}
 const mods=aggregateMods(),baseGain=Math.max(0,game.gold-beforeGold);if(mods.gold>0)game.gold+=Math.floor(baseGain*mods.gold+Math.random()*mods.gold*2);
 const matChance=_boss ? 1 : _elite ? .96 : .52;if(Math.random()<matChance){const key=materialForEnemy(e),count=_boss?irnd(7,4):_elite?irnd(4,2):1;addMaterial(key,count,true);if(_boss)addMaterial(Math.random()<.5?'dust':game.roomData?.biome==='volcanic'?'ember':game.roomData?.biome==='frost'?'frost':'iron',irnd(5,2),true)}
 if(!_boss&&!game.loot&&Math.random()<(_elite ? .075 : .006))dropGearNow(_elite?'elite':'enemy');
 const mend=suffixCount('mending');if(mend)healPlayer(1.1+mend*.8);recordQuestEvent('kill',{enemy:e,elite:_elite,boss:_boss});updateExpansionHUD();
};
const _baseMarkRoomCleared=markRoomCleared;
markRoomCleared=function(){const r=game.roomData,was=!!r?.cleared;_baseMarkRoomCleared();if(r&&!was&&r.cleared){recordQuestEvent('clear',{room:r});const renew=suffixCount('renewal');if(renew)healPlayer(game.player.maxHp*(.035+renew*.018));if(r.boss){addMaterial(materialForEnemy({type:'boss'}),2,true);game.gold+=12+r.difficulty*2}saveExpansion();updateExpansionHUD()}};

/* Equipment behavior hooks. */
const _baseWeaponDamage=weaponDamage;
weaponDamage=function(){return _baseWeaponDamage()*(1+aggregateMods().damage)};
const _baseSpellMods=spellMods;
spellMods=function(id){const m=_baseSpellMods(id),a=aggregateMods();return {...m,power:m.power*(1+a.spell),cdr:Math.max(.55,m.cdr*(1-a.cdr))}};
const _baseRecomputePlayerStats=recomputePlayerStats;
recomputePlayerStats=function(preserveRatio=false){
 const p=game.player;if(!p)return;ensureExpansionState();const ratio=p.maxHp?p.hp/p.maxHp:1;_baseRecomputePlayerStats(preserveRatio);const a=aggregateMods();p.maxHp*=1+a.hp;p.speed*=1+a.move;p.armor=clamp(p.armor+a.armor,0,.68);if(preserveRatio)p.hp=Math.max(1,p.maxHp*ratio);else p.hp=Math.min(p.hp,p.maxHp);updateExpansionHUD();
};
const _baseMagicProjectile=magicProjectile;
magicProjectile=function(opts){
 if(opts?.trail==='weapon'){
   const original=opts.onHit;opts={...opts,tag:opts.tag?`${opts.tag} weapon`:'weapon',onHit:(e,p)=>{
     if(original)original(e,p);const emb=suffixCount('embers'),frost=suffixCount('frost'),storm=suffixCount('storm'),hunt=suffixCount('execution')+aggregateMods().elite;
     if(emb&&Math.random()<Math.min(.5,.16+emb*.08))e.bleed={life:2.8,damage:Math.max(1,weaponDamage()*.075*emb),tick:.28};
     if(frost&&Math.random()<Math.min(.55,.18+frost*.08))e.slow=Math.max(e.slow||0,1.6+frost*.35);
     if(storm&&Math.random()<Math.min(.45,.12+storm*.07)){const t=nearestEnemy(e,3.5,o=>o!==e&&!o.dead);if(t){fx('lightning',e.x,e.y,.18,'#9ceeff',{toX:t.x,toY:t.y,width:2});damageEnemy(t,weaponDamage()*.25*storm,'lightning')}}
     if((e.elite||e.boss)&&hunt>0)damageEnemy(e,Math.max(1,weaponDamage()*(.08+Math.min(.22,hunt*.05))),'arcane',true);
   }};
 }
 return _baseMagicProjectile(opts);
};
const _baseDamagePlayer=damagePlayer;
damagePlayer=function(amount){
 const p=game.player;if(!p)return;const before=p.hp;_baseDamagePlayer(amount);const dealt=Math.max(0,before-p.hp);if(dealt<=0)return;const thorn=suffixCount('thorns');if(thorn){const t=nearestEnemy(p,4.5);if(t)damageEnemy(t,dealt*Math.min(.45,.13*thorn),'arcane',true)}
 if(game.expansion.wardCooldown<=0&&suffixCount('warding')&&Math.random()<.2+aggregateMods().ward){p.shield=Math.max(p.shield,8+game.level*1.4);p.shieldTime=Math.max(p.shieldTime,3.5);game.expansion.wardCooldown=7;burst(p.x,p.y,'#9edcff',10,.5)}
};
const _baseDodge=dodge;
dodge=function(){const before=game.player?.dodgeCd||0;_baseDodge();if(game.player&&game.player.dodgeCd>before)game.player.dodgeCd*=Math.max(.72,1-aggregateMods().cdr*.65)};
