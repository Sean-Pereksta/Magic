'use strict';

/*
 * Arcane Wilds Regional Arsenal Expansion
 * Adds new lands, strongly regional enemy rosters, new combat archetypes,
 * additional weapon classes, and tiered merchant weapon purchasing.
 */
const ARCANE_REGIONAL_EXPANSION_VERSION = 1;

/* ---------- New lands ---------- */
Object.assign(biomePalette,{
  thornwild:{name:'Thornwild Expanse',floor:'#26372c',floor2:'#324636',edge:'#14231a',accent:'#d16b87',sky:'#080f0a'},
  crystal:{name:'Prismglass Hollows',floor:'#2e3f4d',floor2:'#3b5160',edge:'#16242d',accent:'#8ef4ff',sky:'#07121a'},
  stormlands:{name:'Stormsteppe',floor:'#33434a',floor2:'#40525a',edge:'#18262c',accent:'#79d9ff',sky:'#071319'},
  gloam:{name:'Gloam Depths',floor:'#302c3d',floor2:'#3d3650',edge:'#181522',accent:'#d07cff',sky:'#090711'},
  celestial:{name:'Astral Plateau',floor:'#3d4050',floor2:'#4c5062',edge:'#202330',accent:'#ffd98c',sky:'#0d1020'},
  bloodroot:{name:'Bloodroot Wilds',floor:'#423331',floor2:'#503d38',edge:'#241818',accent:'#ff7f72',sky:'#120808'}
});

Object.assign(BIOME_PROP_SETS,{
  thornwild:['oak','fern','flowers','deadTree','mushroom','boulder','stump','grass'],
  crystal:['iceCrystal','purpleCrystal','runeStone','snowRock','boulder','obelisk','iceCrystal'],
  stormlands:['sunRock','boulder','deadTree','obelisk','dryShrub','runeStone','snowRock'],
  gloam:['purpleCrystal','grave','runeStone','deadTree','bogLight','brazier','mushroom'],
  celestial:['column','runeStone','obelisk','frostShrub','iceCrystal','brazier','flowers'],
  bloodroot:['deadTree','oak','bones','skullPile','boulder','dryShrub','emberCrystal','stump']
});

Object.assign(BIOME_NAMES,{
  thornwild:['Thornwild Expanse','Rosefang Thicket','Briar Crown','The Hooked Green','Vermilion Hedge'],
  crystal:['Prismglass Hollows','Crystal Choir','Shiverglass Basin','The Faceted Deep','Luminous Quarry'],
  stormlands:['Stormsteppe','Thundergrass Reach','Skybreak Plain','Galehorn Mesa','The Charged March'],
  gloam:['Gloam Depths','Umbral Sink','Nightglass Hollow','The Violet Below','Starless Chasm'],
  celestial:['Astral Plateau','Sunvault Heights','Constellation Fields','The High Orrery','Golden Zenith'],
  bloodroot:['Bloodroot Wilds','Crimson Timber','Heartwood Scar','Redthorn Basin','The Sanguine Grove']
});

const _regionalBaseBiomeFor = biomeFor;
biomeFor = function(x,y){
  if(x===0&&y===0)return 'town';
  const d=Math.hypot(x,y);
  if(d<3.4)return _regionalBaseBiomeFor(x,y);
  const regionRoll=hash2(Math.floor(x/2),Math.floor(y/2),game.seed+1337);
  const newLandChance=clamp(.20+Math.max(0,d-4)*.035,.20,.68);
  if(regionRoll>newLandChance)return _regionalBaseBiomeFor(x,y);
  const pick=hash2(Math.floor(x/3),Math.floor(y/3),game.seed+7331);
  const lands=d<6
    ? ['thornwild','crystal','stormlands']
    : d<10
      ? ['thornwild','crystal','stormlands','gloam','bloodroot']
      : ['thornwild','crystal','stormlands','gloam','celestial','bloodroot'];
  return lands[Math.min(lands.length-1,Math.floor(pick*lands.length))];
};

/* Let familiar enemies occasionally cross into adjacent lands so regions feel related,
   while the new enemies below remain much more geographically concentrated. */
const REGIONAL_ENEMY_CROSSOVERS={
  wolf:['thornwild','bloodroot'],wisp:['crystal','celestial'],charger:['stormlands','bloodroot'],
  mage:['crystal','gloam','celestial'],serpent:['crystal','stormlands'],assassin:['gloam'],
  sentinel:['crystal','celestial'],harpy:['stormlands','celestial'],cultist:['gloam','bloodroot'],
  golem:['bloodroot'],frostwitch:['crystal'],vampire:['gloam','bloodroot'],voideye:['gloam'],drake:['stormlands','bloodroot'],
  bladedancer:['crystal','gloam'],mortarwitch:['bloodroot'],chainwarden:['gloam'],mirrormage:['crystal','celestial'],
  burrower:['bloodroot'],warpriest:['gloam','celestial'],stormcaller:['stormlands','celestial'],executioner:['bloodroot','gloam']
};
for(const [id,biomes] of Object.entries(REGIONAL_ENEMY_CROSSOVERS)){
  const enemy=ENEMY_TYPES[id];
  if(!enemy)continue;
  for(const biome of biomes)if(!enemy.biomes.includes(biome))enemy.biomes.push(biome);
}

/* ---------- Regional enemies ---------- */
Object.assign(ENEMY_TYPES,{
  briarprowler:{name:'Briar Prowler',icon:'❧',biomes:['thornwild'],min:3,hp:46,speed:2.25,damage:12,r:.31,ai:'regionalPounce',color:'#6f9c68',proj:'#ff829d',xp:17},
  bloomhexer:{name:'Bloom Hexer',icon:'✿',biomes:['thornwild'],min:4,hp:58,speed:1.05,damage:12,r:.34,ai:'regionalBurst',color:'#8d5e7b',proj:'#f28eb3',xp:21},
  rootjuggernaut:{name:'Root Juggernaut',icon:'♣',biomes:['thornwild'],min:7,hp:138,speed:.72,damage:23,r:.52,ai:'regionalSlam',color:'#6f6046',proj:'#d88972',xp:42},

  prismscarab:{name:'Prism Scarab',icon:'⬡',biomes:['crystal'],min:4,hp:62,speed:1.35,damage:12,r:.36,ai:'regionalPrism',color:'#6fa7b7',proj:'#a9fbff',xp:22},
  glassoracle:{name:'Glass Oracle',icon:'◇',biomes:['crystal'],min:6,hp:72,speed:1.0,damage:16,r:.34,ai:'regionalBeam',color:'#7d83ae',proj:'#c7eaff',xp:30},
  shardram:{name:'Shard Ram',icon:'◈',biomes:['crystal'],min:8,hp:154,speed:1.0,damage:25,r:.5,ai:'regionalCharge',color:'#628d9b',proj:'#8deeff',xp:46},

  stormhound:{name:'Storm Hound',icon:'ϟ',biomes:['stormlands'],min:5,hp:66,speed:2.35,damage:14,r:.31,ai:'regionalPounce',color:'#527c8d',proj:'#75e1ff',xp:26},
  skyraider:{name:'Sky Raider',icon:'⌁',biomes:['stormlands'],min:7,hp:82,speed:1.8,damage:17,r:.34,ai:'regionalArtillery',color:'#61798d',proj:'#9cecff',xp:34},
  galeweaver:{name:'Gale Weaver',icon:'⊹',biomes:['stormlands'],min:9,hp:96,speed:1.2,damage:17,r:.36,ai:'regionalSpiral',color:'#4b8090',proj:'#94f4ff',xp:41},

  duskblade:{name:'Duskblade',icon:'✕',biomes:['gloam'],min:8,hp:90,speed:2.05,damage:21,r:.32,ai:'regionalBlink',color:'#705783',proj:'#d485ff',xp:41},
  voidmoth:{name:'Void Moth',icon:'✦',biomes:['gloam'],min:9,hp:74,speed:1.7,damage:17,r:.3,ai:'regionalBurst',color:'#70477f',proj:'#dd7cff',xp:39},
  abyssanchor:{name:'Abyss Anchor',icon:'◉',biomes:['gloam'],min:12,hp:176,speed:.72,damage:23,r:.5,ai:'regionalGravity',color:'#57405f',proj:'#bd66ff',xp:60},

  sunwarden:{name:'Sun Warden',icon:'☼',biomes:['celestial'],min:11,hp:148,speed:.95,damage:22,r:.44,ai:'regionalBeam',color:'#9b7e44',proj:'#ffe09a',xp:55},
  starcaller:{name:'Star Caller',icon:'✶',biomes:['celestial'],min:12,hp:114,speed:1.05,damage:20,r:.36,ai:'regionalArtillery',color:'#776d9c',proj:'#ffe4a8',xp:57},
  astralknight:{name:'Astral Knight',icon:'♞',biomes:['celestial'],min:14,hp:205,speed:1.18,damage:29,r:.5,ai:'regionalCharge',color:'#786f84',proj:'#f8d48f',xp:73},

  marrowboar:{name:'Marrow Boar',icon:'⬟',biomes:['bloodroot'],min:7,hp:146,speed:1.2,damage:24,r:.5,ai:'regionalSlam',color:'#8b514b',proj:'#ff9178',xp:43},
  crimsonwitch:{name:'Crimson Witch',icon:'♠',biomes:['bloodroot'],min:9,hp:92,speed:1.05,damage:19,r:.36,ai:'regionalSpiral',color:'#834c5f',proj:'#ff8296',xp:44},
  thorncolossus:{name:'Thorn Colossus',icon:'♜',biomes:['bloodroot'],min:13,hp:248,speed:.62,damage:32,r:.6,ai:'regionalGravity',color:'#75453e',proj:'#f07d6d',xp:82}
});

function regionalFinishState(e,cooldown){e.state='idle';e.telegraph=null;e.attack=cooldown;}
function regionalPlayerInLine(e,dir,range,width=.62){
  const p=game.player,rx=p.x-e.x,ry=p.y-e.y,along=rx*dir.x+ry*dir.y,side=Math.abs(rx*(-dir.y)+ry*dir.x);
  return along>0&&along<range&&side<width+p.r;
}
function regionalPlayerInCone(e,dir,range,halfAngle=.62){
  const v=norm(game.player.x-e.x,game.player.y-e.y),r=dist(e,game.player);
  return r<range&&v.x*dir.x+v.y*dir.y>Math.cos(halfAngle);
}
function regionalFanFrom(e,count,arc,speed,damageMult=1){
  enemyFan(e,count,arc,speed,'regionalShard',{r:.085,damage:e.damage*damageMult});
}
function regionalPounceAI(e,d,range,speed,dt){
  if(e.state==='regionalLeapWind'&&e.stateTime<=0){
    e.state='regionalLeap';e.stateTime=.34;e.chargeDir=e.telegraph.dir;e.telegraph=null;e.didChargeHit=false;
  }else if(e.state==='regionalLeap'){
    moveEnemy(e,e.chargeDir,speed*5.1,dt);
    if(!e.didChargeHit&&dist(e,game.player)<e.r+game.player.r+.22){damagePlayer(e.damage*1.35);e.didChargeHit=true;}
    if(e.stateTime<=0){fx('shockRing',e.x,e.y,.25,e.proj,{r:.85});regionalFinishState(e,1.65);}
  }else{
    moveEnemy(e,range<1.7?{x:-d.x,y:-d.y}:d,speed,dt);
    if(e.attack<=0&&range<6.4)startEnemyTelegraph(e,'regionalLeapWind',.52,{dir:d,range:5.5,cone:.18});
  }
}
function regionalBurstAI(e,d,range,speed,dt){
  kite(e,d,range,speed,dt,4.6,7.0);
  if(e.state==='regionalBurstWind'&&e.stateTime<=0){
    const count=e.type==='voidmoth'?12:8;
    for(let i=0;i<count;i++){
      const a=TAU*i/count+(e.type==='voidmoth'?elapsed*.65:0),dir={x:Math.cos(a),y:Math.sin(a)};
      enemyProjectile(e,dir,e.type==='voidmoth'?4.8:4.1,'regionalOrb',{r:.09,damage:e.damage*(e.type==='voidmoth'?.72:.82)});
    }
    fx('shockRing',e.x,e.y,.32,e.proj,{r:1.35});regionalFinishState(e,e.type==='voidmoth'?2.0:2.5);
  }else if(e.attack<=0&&e.state==='idle')startEnemyTelegraph(e,'regionalBurstWind',.75,{r:1.2});
}
function regionalSlamAI(e,d,range,speed,dt){
  moveEnemy(e,d,speed,dt);
  if(e.state==='regionalSlamWind'&&e.stateTime<=0){
    const r=e.type==='rootjuggernaut'?1.8:2.15;
    radialPlayerThreat(e.x,e.y,r,e.damage*1.45);
    fx('shockRing',e.x,e.y,.48,e.proj,{r:r*1.3});
    regionalFanFrom(e,6,TAU,4.0,.48);shake=Math.max(shake,6);regionalFinishState(e,2.7);
  }else if(e.attack<=0&&e.state==='idle'&&range<3.4)startEnemyTelegraph(e,'regionalSlamWind',.9,{x:e.x,y:e.y,r:e.type==='rootjuggernaut'?1.8:2.15});
}
function regionalPrismAI(e,d,range,speed,dt){
  kite(e,d,range,speed,dt,3.8,6.3);
  if(e.state==='regionalPrismWind'&&e.stateTime<=0){
    regionalFanFrom(e,5,1.3,5.5,.8);
    regionalFanFrom(e,4,.72,4.8,.52);
    regionalFinishState(e,2.2);
  }else if(e.attack<=0&&e.state==='idle')startEnemyTelegraph(e,'regionalPrismWind',.58,{dir:d,range:7,cone:.62});
}
function regionalBeamAI(e,d,range,speed,dt){
  kite(e,d,range,speed,dt,5.0,8.1);
  if(e.state==='regionalBeamWind'&&e.stateTime<=0){
    const dir=e.telegraph.dir;
    if(regionalPlayerInLine(e,dir,11,e.type==='sunwarden'?.82:.58))damagePlayer(e.damage*1.3);
    fx('lightning',e.x,e.y,.24,e.proj,{toX:e.x+dir.x*11,toY:e.y+dir.y*11,width:e.type==='sunwarden'?5:3});
    if(e.type==='sunwarden')regionalFanFrom(e,5,.95,4.4,.48);
    regionalFinishState(e,e.type==='sunwarden'?2.15:2.6);
  }else if(e.attack<=0&&e.state==='idle'&&range<11)startEnemyTelegraph(e,'regionalBeamWind',e.type==='sunwarden'?.78:.9,{dir:d,range:11});
}
function regionalChargeAI(e,d,range,speed,dt){
  if(e.state==='regionalChargeWind'&&e.stateTime<=0){e.state='regionalCharge';e.stateTime=.48;e.chargeDir=e.telegraph.dir;e.telegraph=null;e.didChargeHit=false;}
  else if(e.state==='regionalCharge'){
    moveEnemy(e,e.chargeDir,speed*4.4,dt);
    if(!e.didChargeHit&&dist(e,game.player)<e.r+game.player.r+.25){damagePlayer(e.damage*1.5);e.didChargeHit=true;}
    if(e.stateTime<=0){regionalFanFrom(e,e.type==='astralknight'?7:5,1.6,4.6,.58);regionalFinishState(e,2.45);}
  }else{
    moveEnemy(e,range>3?d:{x:-d.x,y:-d.y},speed,dt);
    if(e.attack<=0&&e.state==='idle'&&range<8)startEnemyTelegraph(e,'regionalChargeWind',.78,{dir:d,range:7,cone:.18});
  }
}
function regionalArtilleryAI(e,d,range,speed,dt){
  kite(e,d,range,speed,dt,5.2,8.2);
  if(e.state==='regionalArtilleryWind'&&e.stateTime<=0){
    for(const p of e.regionalMarks||[]){radialPlayerThreat(p.x,p.y,.95,e.damage*1.1);fx('explosion',p.x,p.y,.3,e.proj,{r:1.08});burst(p.x,p.y,e.proj,9,.8);}
    regionalFinishState(e,2.65);
  }else if(e.attack<=0&&e.state==='idle'){
    const p=game.player,n=e.type==='starcaller'?5:3;e.regionalMarks=[];
    for(let i=0;i<n;i++){
      const a=i/n*TAU,rad=i===0?0:1.15+(i%2)*.8;
      e.regionalMarks.push({x:clamp(p.x+Math.cos(a)*rad,.7,ROOM_W-.7),y:clamp(p.y+Math.sin(a)*rad,.7,ROOM_H-.7)});
    }
    const t=e.type==='starcaller'?.72:.9;
    startEnemyTelegraph(e,'regionalArtilleryWind',t,{x:e.regionalMarks[0].x,y:e.regionalMarks[0].y,r:.95});
    for(const q of e.regionalMarks.slice(1))game.telegraphs.push({kind:'circle',x:q.x,y:q.y,r:.95,time:t,maxTime:t,color:e.proj,pulse:true});
  }
}
function regionalSpiralAI(e,d,range,speed,dt){
  kite(e,d,range,speed,dt,4.7,7.4);
  if(e.state==='regionalSpiralWind'&&e.stateTime<=0){
    const count=10,offset=elapsed*1.7;
    for(let i=0;i<count;i++){
      const a=offset+i*TAU/count,dir={x:Math.cos(a),y:Math.sin(a)};
      enemyProjectile(e,dir,4.7,'regionalSpiral',{r:.08,damage:e.damage*.7});
    }
    regionalFinishState(e,2.25);
  }else if(e.attack<=0&&e.state==='idle')startEnemyTelegraph(e,'regionalSpiralWind',.7,{r:1.1});
}
function regionalBlinkAI(e,d,range,speed,dt){
  const tangent={x:-d.y,y:d.x};moveEnemy(e,range<2.4?{x:-d.x,y:-d.y}:norm(d.x*.35+tangent.x*.8,d.y*.35+tangent.y*.8),speed,dt);
  if(e.state==='regionalBlinkWind'&&e.stateTime<=0){
    const side=Math.random()<.5?-1:1;
    e.x=clamp(game.player.x-d.x*2.2+tangent.x*side*1.4,.7,ROOM_W-.7);
    e.y=clamp(game.player.y-d.y*2.2+tangent.y*side*1.4,.7,ROOM_H-.7);
    const dir=norm(game.player.x-e.x,game.player.y-e.y);
    if(regionalPlayerInCone(e,dir,3.2,.72))damagePlayer(e.damage*1.4);
    fx('slash',e.x,e.y,.3,e.proj,{dir});burst(e.x,e.y,e.proj,10,.7);regionalFinishState(e,2.0);
  }else if(e.attack<=0&&e.state==='idle'&&range<7)startEnemyTelegraph(e,'regionalBlinkWind',.55,{dir:d,range:3.2,cone:.72});
}
function regionalGravityAI(e,d,range,speed,dt){
  if(range>5.4)moveEnemy(e,d,speed,dt);else if(range<3.0)moveEnemy(e,{x:-d.x,y:-d.y},speed*.65,dt);
  if(e.state==='regionalGravityWind'&&e.stateTime<=0){
    const p=e.telegraph;
    radialPlayerThreat(p.x,p.y,e.type==='thorncolossus'?1.7:1.35,e.damage*1.25);
    fx('shockRing',p.x,p.y,.42,e.proj,{r:e.type==='thorncolossus'?2.0:1.65});
    const pull=norm(p.x-game.player.x,p.y-game.player.y),distance=Math.hypot(p.x-game.player.x,p.y-game.player.y);
    if(distance<4.2){game.player.x+=pull.x*1.05;game.player.y+=pull.y*1.05;}
    regionalFanFrom(e,e.type==='thorncolossus'?8:6,TAU,4.0,.5);regionalFinishState(e,3.05);
  }else if(e.attack<=0&&e.state==='idle')startEnemyTelegraph(e,'regionalGravityWind',1.0,{x:game.player.x,y:game.player.y,r:e.type==='thorncolossus'?1.7:1.35});
}

const _regionalBaseUpdateEnemyAI=updateEnemyAI;
updateEnemyAI=function(e,d,range,speed,dt){
  if(e.ai==='regionalPounce')return regionalPounceAI(e,d,range,speed,dt);
  if(e.ai==='regionalBurst')return regionalBurstAI(e,d,range,speed,dt);
  if(e.ai==='regionalSlam')return regionalSlamAI(e,d,range,speed,dt);
  if(e.ai==='regionalPrism')return regionalPrismAI(e,d,range,speed,dt);
  if(e.ai==='regionalBeam')return regionalBeamAI(e,d,range,speed,dt);
  if(e.ai==='regionalCharge')return regionalChargeAI(e,d,range,speed,dt);
  if(e.ai==='regionalArtillery')return regionalArtilleryAI(e,d,range,speed,dt);
  if(e.ai==='regionalSpiral')return regionalSpiralAI(e,d,range,speed,dt);
  if(e.ai==='regionalBlink')return regionalBlinkAI(e,d,range,speed,dt);
  if(e.ai==='regionalGravity')return regionalGravityAI(e,d,range,speed,dt);
  return _regionalBaseUpdateEnemyAI(e,d,range,speed,dt);
};

/* Bias the new lands heavily toward their own inhabitants instead of producing the
   same global soup of enemies in every biome. */
const REGIONAL_NATIVE_ENEMIES={
  thornwild:['briarprowler','bloomhexer','rootjuggernaut'],
  crystal:['prismscarab','glassoracle','shardram'],
  stormlands:['stormhound','skyraider','galeweaver'],
  gloam:['duskblade','voidmoth','abyssanchor'],
  celestial:['sunwarden','starcaller','astralknight'],
  bloodroot:['marrowboar','crimsonwitch','thorncolossus']
};
const _regionalBaseSpawnRoomEnemies=spawnRoomEnemies;
spawnRoomEnemies=function(room){
  _regionalBaseSpawnRoomEnemies(room);
  const natives=REGIONAL_NATIVE_ENEMIES[room?.biome];
  if(!natives||room.town||room.boss||room.cleared)return;
  const gate=game.level+Math.floor(room.difficulty*.8);
  const eligible=natives.filter(id=>ENEMY_TYPES[id].min<=gate);
  if(!eligible.length)return;
  const nativeCount=game.enemies.filter(e=>eligible.includes(e.type)).length;
  const desired=Math.min(4,1+Math.floor(room.difficulty/4));
  for(let i=nativeCount;i<desired&&game.enemies.length<20;i++)spawnEnemy(eligible[irnd(eligible.length)],randomEnemySpawn(),room.elite&&i===nativeCount&&Math.random()<.3);
};

if(typeof intensityAvailablePool==='function'){
  const _regionalBaseIntensityAvailablePool=intensityAvailablePool;
  intensityAvailablePool=function(room){
    const pool=_regionalBaseIntensityAvailablePool(room),natives=REGIONAL_NATIVE_ENEMIES[room?.biome]||[];
    const eligible=natives.filter(id=>pool.includes(id));
    return eligible.length?[...pool,...eligible,...eligible]:pool;
  };
}

const _regionalBaseMaterialForEnemy=materialForEnemy;
materialForEnemy=function(e){
  if(['briarprowler','bloomhexer','rootjuggernaut','marrowboar','thorncolossus'].includes(e.type))return 'hide';
  if(['prismscarab','glassoracle','shardram'].includes(e.type))return Math.random()<.55?'frost':'dust';
  if(['stormhound','skyraider','galeweaver'].includes(e.type))return Math.random()<.6?'dust':'iron';
  if(['duskblade','voidmoth','abyssanchor','crimsonwitch'].includes(e.type))return Math.random()<.55?'bone':'dust';
  if(['sunwarden','starcaller','astralknight'].includes(e.type))return Math.random()<.6?'dust':'iron';
  return _regionalBaseMaterialForEnemy(e);
};

/* ---------- New weapon classes ---------- */
WEAPON_BASES.push(
  {name:'Ironwood Crossbow',icon:'🏹',type:'crossbow',damage:1.42,attack:.68,range:11.5,speed:15,color:'#e0bb7a'},
  {name:'Star Wand',icon:'🪄',type:'wand',damage:.68,attack:1.72,range:8.8,speed:12,color:'#a9e8ff'},
  {name:'Colossus Greatblade',icon:'⚔️',type:'greatblade',damage:1.62,attack:.62,range:6.2,speed:7.8,color:'#d9c7ff'},
  {name:'Hex Grimoire',icon:'📖',type:'grimoire',damage:.86,attack:1.0,range:9.4,speed:9.6,color:'#c17cff'},
  {name:'Moon Scythe',icon:'🌙',type:'scythe',damage:1.18,attack:.92,range:8.2,speed:10.5,color:'#cda7ff'},
  {name:'Ember Handcannon',icon:'💥',type:'handcannon',damage:1.72,attack:.56,range:9.0,speed:10.2,color:'#ff9a68'},
  {name:'Tempest Glaive',icon:'🔱',type:'glaive',damage:1.05,attack:1.08,range:10.2,speed:12.5,color:'#83edff'},
  {name:'Arcane Orbit',icon:'🔮',type:'arcaneorb',damage:.94,attack:1.18,range:8.8,speed:9.5,color:'#9d8cff'},
  {name:'Twin Fang Daggers',icon:'🗡️',type:'daggers',damage:.75,attack:1.52,range:7.0,speed:13.5,color:'#f2e6cf'},
  {name:'Crystal Repeater',icon:'✦',type:'repeater',damage:.72,attack:1.38,range:9.8,speed:14.2,color:'#9cecff'},
  {name:'Sun Disc',icon:'☀️',type:'sundisc',damage:1.04,attack:.98,range:10.4,speed:10.8,color:'#ffd67e'},
  {name:'Void Pike',icon:'🔱',type:'voidpike',damage:1.3,attack:.82,range:11.2,speed:13.2,color:'#b37cff'}
);

const REGIONAL_WEAPON_TYPES=new Set(['crossbow','wand','greatblade','grimoire','scythe','handcannon','glaive','arcaneorb','daggers','repeater','sundisc','voidpike']);
function regionalAcquireWeaponTarget(p,w){
  let target=null;
  if(aimStick.active||mouse.active){
    const dir=spellAim();let best=1.2;
    for(const e of game.enemies){
      if(e.dead)continue;
      const v=norm(e.x-p.x,e.y-p.y),dot=v.x*dir.x+v.y*dir.y,d=dist(e,p);
      if(d<=w.range&&dot>.76){const score=(1-dot)*3+d*.03;if(score<best){best=score;target=e;}}
    }
  }
  return target||nearestEnemy(p,w.range);
}
function regionalRotate(dir,angle){const c=Math.cos(angle),s=Math.sin(angle);return {x:dir.x*c-dir.y*s,y:dir.x*s+dir.y*c};}
function regionalWeaponProjectile(p,w,dir,opts={}){
  const damage=weaponDamage()*(opts.damageMult??1),speed=w.speed*(opts.speedMult??1);
  magicProjectile({x:p.x,y:p.y,z:17,vx:dir.x*speed,vy:dir.y*speed,r:opts.r??.1,damage,color:w.color,color2:'#fff',kind:opts.kind||'staffBolt',life:w.range/speed+.35,pierce:opts.pierce||0,seek:opts.seek||0,trail:'weapon',splash:opts.splash||0,tag:opts.tag||''});
}
const _regionalBaseAutoAttack=autoAttack;
autoAttack=function(dt){
  const p=game.player,w=p?.weapon;
  if(!w||!REGIONAL_WEAPON_TYPES.has(w.type))return _regionalBaseAutoAttack(dt);
  p.attackTimer-=dt;
  const rate=(1/w.attack)*(p.haste>0?1.28:1);
  if(p.attackTimer>0)return;
  const target=regionalAcquireWeaponTarget(p,w);if(!target)return;
  const d=norm(target.x-p.x,target.y-p.y);p.facing=d;p.attackTimer=Math.max(.14,rate);
  if(w.type==='crossbow')regionalWeaponProjectile(p,w,d,{kind:'arrow',pierce:3,r:.12,damageMult:1.08});
  else if(w.type==='wand')regionalWeaponProjectile(p,w,d,{kind:'sparkBolt',seek:1.65,r:.085});
  else if(w.type==='greatblade')regionalWeaponProjectile(p,w,d,{kind:'bladeWave',pierce:2,r:.22,splash:.38});
  else if(w.type==='grimoire')for(const a of [-.16,0,.16])regionalWeaponProjectile(p,w,regionalRotate(d,a),{kind:'staffBolt',seek:.45,damageMult:.43,r:.085});
  else if(w.type==='scythe')regionalWeaponProjectile(p,w,d,{kind:'miniChakram',pierce:3,r:.18});
  else if(w.type==='handcannon')regionalWeaponProjectile(p,w,d,{kind:'staffBolt',splash:1.18,r:.15,damageMult:1.08,speedMult:.88});
  else if(w.type==='glaive')regionalWeaponProjectile(p,w,d,{kind:'sunLance',pierce:4,r:.12});
  else if(w.type==='arcaneorb')regionalWeaponProjectile(p,w,d,{kind:'sparkBolt',seek:.7,tag:'chain-weapon',r:.11});
  else if(w.type==='daggers')for(const a of [-.09,.09])regionalWeaponProjectile(p,w,regionalRotate(d,a),{kind:'bladeWave',pierce:1,damageMult:.58,r:.095});
  else if(w.type==='repeater')for(const a of [-.11,0,.11])regionalWeaponProjectile(p,w,regionalRotate(d,a),{kind:'arrow',damageMult:.38,r:.075});
  else if(w.type==='sundisc')regionalWeaponProjectile(p,w,d,{kind:'miniChakram',pierce:5,seek:.25,r:.16});
  else if(w.type==='voidpike')regionalWeaponProjectile(p,w,d,{kind:'sunLance',pierce:4,r:.13,damageMult:1.04});
  fx('muzzle',p.x,p.y,.18,w.color,{dir:d});
};

/* ---------- Merchant weapon tiers ---------- */
const REGIONAL_WEAPON_SHOP_TIERS={
  normal:{label:'Normal Weapon',floor:'Common',cost:()=>70+game.level*5,weights:{Common:46,Uncommon:31,Rare:16,Epic:6,Legendary:1},note:'Common floor • solid everyday roll'},
  advanced:{label:'Advanced Weapon',floor:'Uncommon',cost:()=>145+game.level*9,weights:{Uncommon:48,Rare:31,Epic:16,Legendary:5},note:'Uncommon floor • 52% Rare or better'},
  ultra:{label:'Ultra Weapon',floor:'Rare',cost:()=>285+game.level*15,weights:{Rare:52,Epic:34,Legendary:14},note:'Rare floor • 48% Epic or Legendary'}
};
function regionalRollWeightedRarity(weights){
  const entries=Object.entries(weights),total=entries.reduce((s,[,v])=>s+v,0);let roll=Math.random()*total;
  for(const [rarity,weight] of entries){roll-=weight;if(roll<=0)return rarity;}
  return entries[entries.length-1][0];
}
function regionalBuyWeaponTier(tierKey){
  const tier=REGIONAL_WEAPON_SHOP_TIERS[tierKey];if(!tier)return;
  const cost=tier.cost();
  spendGold(cost,()=>{
    const rarity=regionalRollWeightedRarity(tier.weights);
    const item=makeRandomGear({source:'merchant',forceSlot:'weapon',rarity,crafted:tierKey!=='normal'});
    if(tierKey==='ultra'&&!item.prefixKey&&!item.suffixKey)rollAffixes(item,true);
    game.loot=item;toastMsg(`${tier.label}: ${rarity} ${item.baseName||item.name}`);setTimeout(openLootOverlay,120);
  });
}

const _regionalBaseMerchantBuy=merchantBuy;
merchantBuy=function(action){
  if(action&&action.startsWith('weapon-'))return regionalBuyWeaponTier(action.slice(7));
  return _regionalBaseMerchantBuy(action);
};

const _regionalBaseOpenNPCPanel=openNPCPanel;
openNPCPanel=function(npc){
  if(npc?.role!=='Merchant')return _regionalBaseOpenNPCPanel(npc);
  $('npcName').textContent=`${npc.name} • ${npc.role}`;
  const body=$('npcBody');
  const tierButtons=Object.entries(REGIONAL_WEAPON_SHOP_TIERS).map(([key,t])=>`<button class="exp-buy" data-weapon-tier="${key}"><b>${key==='normal'?'⚔️':key==='advanced'?'✨':'🌟'} ${t.label}</b><span>${t.note}</span><strong>🪙 ${t.cost()}</strong></button>`).join('');
  body.innerHTML=`<p class="exp-dialogue">“A cheap blade can save you. An expensive one can change the whole journey.”</p><div class="exp-card"><b>Weapon Counter</b><small>Choose how high the quality floor should be. Every purchase is always a weapon; higher counters remove the worst rarity outcomes entirely.</small></div><div class="exp-shop">${tierButtons}</div><h4>Travel Goods</h4><div class="exp-shop"><button class="exp-buy" data-buy="materials"><b>Prospector Bundle</b><span>3 mixed materials</span><strong>🪙 ${24+game.level}</strong></button><button class="exp-buy" data-buy="gear"><b>Sealed Gear Cache</b><span>Random gear slot with affix chances</span><strong>🪙 ${62+game.level*4}</strong></button><button class="exp-buy" data-buy="trinket"><b>Trinket Case</b><span>Always contains a trinket</span><strong>🪙 ${75+game.level*5}</strong></button></div>`;
  body.querySelectorAll('[data-weapon-tier]').forEach(b=>b.onclick=()=>merchantBuy(`weapon-${b.dataset.weaponTier}`));
  body.querySelectorAll('[data-buy]').forEach(b=>b.onclick=()=>merchantBuy(b.dataset.buy));
  showOverlay('npcPanel');
};
