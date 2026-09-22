'use strict';
/* Deterministic, bounded-window endgame. A compact rolling discovery ledger and
   milestone checkpoints survive saves; room geometry is regenerated from seed. */
(() => {
  const D=AWCampaignData,A=AWCampaign,U=AWCampaignUI;
  const themes=[['Shadow Forest','forest'],['Void Marsh','swamp'],['Ash Wastes','volcanic'],['Crystal Abyss','crystal'],['Forgotten Kingdom','ruins'],['Blood Moon Fields','bloodroot'],['Starless Caverns','gloam'],['Shattered City','ruins'],['Eternal Graveyard','crypt'],['Arcane Stormlands','stormlands'],['Phantom Sea','celestial'],['Obsidian Mountains','volcanic']];
  const shadowContinent={id:'shadow',name:'The Shadow Realms',description:'Unending roads beyond the last horizon.',color:'#bd93ef',range:[28,'∞'],material:'voidshard',biomes:themes.map(t=>t[1]),ruler:'The Unending',portalCity:'gloam-city',requiredBosses:[],boss:'',start:'shadow:1:0',nodes:[],endless:true,mapWidth:800,mapHeight:1700};
  const traits=['Vampiric','Swift','Armored','Reflective','Explosive','Teleporting','Regenerating','Arcane','Frozen','Burning','Summoner','Berserker','Giant','Tiny','Shielded','Shadowed'];
  const exclusive=[['voidStalker','Void Stalker','riftStalker'],['shadowColossus','Shadow Colossus','mossbackGuardian'],['riftMage','Rift Mage','briarWitch'],['hollowKnight','Hollow Knight','eclipseKnight'],['soulDevourer','Soul Devourer','gloamDevourer'],['abyssSpider','Abyss Spider','soulLeech'],['phantomDrake','Phantom Drake','drake'],['eclipseWitch','Eclipse Witch','starboundOracle'],['shadowMimic','Shadow Mimic','prismMimic'],['voidSerpent','Void Serpent','serpent'],['fallenArchmage','Fallen Archmage','shardcaster'],['realityEater','Reality Eater','voidShepherd']];
  for(const [id,name,base] of exclusive)ENEMY_TYPES[id]={...ENEMY_TYPES[base],name,biomes:[],color:'#a590cf',proj:'#ddb7ff',shadowOnly:true};
  const shadowGear=[
    ['shadowsteel','Shadowsteel Reaver','weapon','isolation',{type:'blade',damage:1.45,attack:1.1,range:8,speed:11}],
    ['voidRobes','Vestments of the Unseen','armor','largeHit',{hp:1.3,move:1.12,armor:.14}],
    ['riftArmor','Riftwarden Armor','armor','ashen',{hp:1.4,move:1.02,armor:.18}],
    ['eclipseJewel','Eclipse Heart','trinket','shadowAccuracy',{mods:{spell:.18,cdr:.08}}],
    ['dimensionAtlas','Atlas of Lost Dimensions','trinket','horizon',{mods:{spell:.2},spellSlotBonus:1}]
  ];
  for(const [id,name,slot,special,extra] of shadowGear)D.items[id]={name,slot,special,rarity:'Legendary',color:'#c2a2ff',trim:'#e6ceff',desc:'Shadow-exclusive equipment. Its power grows with discovery depth.',...extra};
  D.mounts.shadowGryphon={...D.mounts.astralGryphon,name:'Eclipse Gryphon',color:'#bfa0ef',desc:'A Glory companion bearing the wings of the Shadow Realms.',cost:0};
  const titles=[[100,'Shadow Walker'],[600,'Rift Hunter'],[1800,'Voidbreaker'],[5000,'Realm Conqueror'],[15000,'The Unending'],[40000,'Lord of Shadows']];
  const hash=(seed,...parts)=>{let h=(Number(seed)||1)>>>0;for(const ch of parts.join(':'))h=Math.imul(h^ch.charCodeAt(0),16777619)>>>0;return h>>>0;};
  const idFor=(depth,lane=0)=>`shadow:${depth}:${lane}`;
  const parse=id=>{const m=/^shadow:(\d+):(-?1|0)$/.exec(id||'');if(!m)return null;const depth=Number(m[1]),lane=Number(m[2]);return Number.isSafeInteger(depth)&&depth>=1&&depth<Number.MAX_SAFE_INTEGER/4?{depth,lane}:null;};
  const isNode=id=>!!parse(id);
  const fresh=()=>({version:1,seed:game.seed||1,unlocked:false,highest:0,glory:0,expedition:0,deaths:0,waystones:[1],ledger:{},archivedThrough:0,unlocks:[],milestones:[],title:'',lastDepth:1});
  function state(){if(!A.state())return null;return A.state().shadow||=(fresh());}
  function normalize(raw){const s=fresh(),number=(v,f=0)=>Number.isFinite(v)?Math.max(0,Math.floor(v)):f;if(!raw||typeof raw!=='object')return s;
    for(const k of ['seed','highest','glory','expedition','deaths','archivedThrough','lastDepth'])s[k]=number(raw[k],s[k]);s.unlocked=!!raw.unlocked;
    s.waystones=[1,...[...new Set((Array.isArray(raw.waystones)?raw.waystones:[]).filter(d=>Number.isSafeInteger(d)&&d>1&&d<=s.highest))].slice(-79)];
    s.unlocks=[...new Set((Array.isArray(raw.unlocks)?raw.unlocks:[]).filter(x=>typeof x==='string'))].slice(0,30);s.milestones=(Array.isArray(raw.milestones)?raw.milestones:[]).filter(Number.isSafeInteger).slice(-80);s.title=String(raw.title||'').slice(0,50);s.pendingLoot=(Array.isArray(raw.pendingLoot)?raw.pendingLoot:[]).filter(i=>i&&D.items[i.regionalId]&&['weapon','armor','trinket'].includes(i.slot)).slice(-12);
    for(const [key,v] of Object.entries(raw.ledger||{}).slice(-32))if(/^\d+$/.test(key)&&v&&typeof v==='object'){s.ledger[key]={};for(const bit of ['seen','cleared','claimed'])s.ledger[key][bit]=/^[0-9a-f]{1,12}$/.test(v[bit]||'')?v[bit]:'0';}return s;
  }
  function bit(id,kind,set=false){const n=parse(id),s=state();if(!n||!s)return false;if(n.depth<=s.archivedThrough)return true;
    const chunk=Math.floor((n.depth-1)/16),offset=((n.depth-1)%16)*3+n.lane+1,entry=s.ledger[chunk]||{seen:'0',cleared:'0',claimed:'0'},mask=1n<<BigInt(offset),value=BigInt('0x'+entry[kind]);
    if(set){entry[kind]=(value|mask).toString(16);s.ledger[chunk]=entry;const keys=Object.keys(s.ledger).map(Number).sort((a,b)=>a-b);while(keys.length>32){const oldest=keys.shift();s.archivedThrough=Math.max(s.archivedThrough,(oldest+1)*16);delete s.ledger[oldest];}return true;}return !!(value&mask);
  }
  const branch=(s,d,l)=>hash(s.seed,d,l,'branch')%100<55;
  function definition(depth,lane=0){const s=state()||fresh(),h=hash(s.seed,depth,lane),theme=themes[Math.floor((depth-1)/8)%themes.length],boss=lane===0&&depth%10===0||lane===0&&depth%25===0;
    let type=lane===0?(depth===1||depth%25===1?'sanctuary':boss?'shadowBoss':depth%7===0?'challenge':depth%6===0?'event':'wildland'):['treasure','shrine','merchant','mount','event','challenge','wildland'][h%7];
    const rank=depth%100===0?'Legendary Realm Boss':depth%50===0?'Greater Shadow Lord':depth%25===0?'Shadow Boss':'Elite Guardian';
    const corruption=depth%13===0?['Briarwatch That Never Was','Sunmere Without Dawn','Prismhold in Ruins'][h%3]:null;
    const n={id:idFor(depth,lane),shadow:true,depth,lane,continent:'shadow',type,biome:theme[1],name:boss?`${rank} • ${theme[0]}`:corruption||`${theme[0]} • ${lane===0?'Road':lane<0?'Echo':'Rift'} ${depth}`,region:'shadow',material:'voidshard',threat:28+Math.floor(Math.log2(depth+1)*2),index:depth*3+lane+1,roomCount:1,x:lane*180+400,y:-depth*115,connections:[],exits:{},waystone:type==='sanctuary',modifier:lane>0&&h%3===0?'Greed':lane<0&&h%3===0?'Safety':null,event:['rescue','defend','hunt','crystals','waves','collapse','escort'][h%7],bossRank:boss?rank:null};
    if(lane===0){if(depth>1)n.exits.S=idFor(depth-1);n.exits.N=idFor(depth+1);if(branch(s,depth,-1))n.exits.W=idFor(depth,-1);if(branch(s,depth,1))n.exits.E=idFor(depth,1);}
    else{n.exits[lane<0?'E':'W']=idFor(depth);if(branch(s,depth+1,lane))n.exits.N=idFor(depth+1,lane);if(depth>1&&branch(s,depth-1,lane))n.exits.S=idFor(depth-1,lane);}
    n.connections=Object.values(n.exits);return n;
  }
  function windowAt(depth){for(const id of Object.keys(D.nodes))if(isNode(id))delete D.nodes[id];shadowContinent.nodes=[];
    for(let d=Math.max(1,depth-5);d<=depth+7;d++)for(const lane of [0,-1,1])if(lane===0||branch(state()||fresh(),d,lane)){const n=definition(d,lane);n.y=(depth+7-d)*115+90;D.nodes[n.id]=n;shadowContinent.nodes.push(n.id);}
  }
  const oldContinent=D.continent;D.continent=id=>id==='shadow'?shadowContinent:oldContinent(id);
  D.coordinates=id=>{const n=parse(id);return {x:2000000+n.depth*3+n.lane+1,y:2000000};};
  D.locate=(x,y)=>{if(y!==2000000)return null;const depth=Math.floor((x-2000000)/3),lane=(x-2000000)%3-1,id=idFor(depth,lane);if(!parse(id))return null;D.nodes[id]||=(definition(depth,lane));return {node:D.nodes[id],room:0,x,y};};
  const oldNormalize=D.normalize;D.normalize=raw=>{
    const shadow=normalize(raw?.shadow),location=parse(raw?.current),copy=location?{...raw,current:'gloam-city',room:0}:raw;
    const s=oldNormalize(copy);s.shadow=shadow;shadow.unlocked=s.defeated.includes('gloam-ruler');
    // Temporarily install normalized state so window generation uses the restored seed.
    const prior=game.campaign;game.campaign=s;if(location&&shadow.unlocked){windowAt(location.depth);s.current=raw.current;s.room=0;}game.campaign=prior;return s;
  };
  const oldReason=D.travelReason;D.travelReason=(s,id,mode)=>{
    if(isNode(id)||isNode(s.current)){const n=D.nodes[s.current];if(!s.shadow?.unlocked)return 'Defeat the third continent ruler first.';if(mode==='waystone')return '';if(!n?.connections.includes(id))return 'Follow a connected Shadow road.';if(!game.roomData?.cleared)return 'Clear this realm before continuing.';return '';}
    return oldReason(s,id,mode);
  };
  function unlock(){const s=state();if(!s||s.unlocked||!A.state().defeated.includes('gloam-ruler'))return false;s.unlocked=true;windowAt(1);AWModernUI.announce('THE SHADOW REALMS HAVE OPENED','A tear beyond the third ruler leads into an unending world.');fx('riftOpen',9,4,4,'#ae75ea',{r:4});saveGame();return true;}
  function enter(){if(!state()?.unlocked||A.current()?.id!=='gloam-city'||!game.roomData?.cleared)return false;state().expedition=0;return resume(1);}
  function resume(depth,recovery=false){if(!state()?.unlocked||!state().waystones.includes(depth)||!running||roomTransition)return false;if(!recovery&&(!game.roomData?.cleared||!A.state().waystones.includes(A.current()?.id)&&A.current()?.id!=='gloam-city'))return false;windowAt(depth);const ok=A.enter(idFor(depth),0,'S');if(ok)saveGame();return ok;}
  function travel(id,entry='S',mode='road'){const s=state(),p=parse(id),n=A.current();if(!s?.unlocked||!p||!running||paused||roomTransition)return false;
    if(mode==='waystone'){if(!s.waystones.includes(p.depth)||p.lane!==0||!game.roomData?.cleared)return false;}else if(!n?.shadow||!n.connections.includes(id)||!game.roomData.cleared)return false;
    windowAt(p.depth);return A.enter(id,0,entry);
  }
  function leave(id='gloam-city'){if(!state()?.unlocked||!game.roomData?.cleared||!A.current()?.shadow||!D.nodes[id]?.town||!A.state().waystones.includes(id)&&id!=='gloam-city')return false;state().expedition=0;return A.enter(id,0,'S');}
  function activate(){const n=A.current();if(!n?.shadow||n.lane!==0||!game.roomData.cleared||!n.waystone&&!game.roomData.shadowCheckpoint&&!state().milestones.includes(n.depth))return false;if(!state().waystones.includes(n.depth))state().waystones.push(n.depth);state().waystones=[1,...state().waystones.filter(d=>d!==1).slice(-79)];saveGame();return true;}
  const rewards=[['title',100],['trail',600],['shadowMerchant',1800],['shadowGryphon',5000],['shadowFireball',1000],['eclipseLightning',2400]];
  function award(amount){const s=state();s.glory+=Math.round(amount);s.expedition+=Math.round(amount);for(const [key,cost] of rewards)if(s.glory>=cost&&!s.unlocks.includes(key)){s.unlocks.push(key);if(D.mounts[key]){if(!A.state().mounts.includes(key))A.state().mounts.push(key);}if(SPELLS[key])A.unlockSpell(key);}s.title=[...titles].reverse().find(([g])=>s.glory>=g)?.[1]||'';}
  const baseSpawn=spawnRoomEnemies;
  spawnRoomEnemies=function(room){const n=D.nodes[room.campaignNode];if(!n?.shadow)return baseSpawn(room);if(bit(n.id,'cleared')){room.cleared=true;return;}
    const st=intensityState();st.roomKey=room.key;st.hazards.length=0;st.encounter={roomKey:room.key,wave:1,totalWaves:1,pending:null,grace:0,hazardCd:999};
    if(['sanctuary','merchant','shrine','treasure','mount'].includes(n.type)){room.cleared=true;return;}
    const boss=n.type==='shadowBoss',count=boss?1:Math.min(10,4+Math.floor(Math.log2(n.depth+1)/2));
    for(let i=0;i<count;i++)spawnShadow(n,i,boss);
    room.shadowEncounter={stage:1,pending:0,elapsed:0,deadline:n.type==='event'&&n.event==='collapse'?40:0,failed:false,threatTimer:3};
  };
  function spawnShadow(n,i,boss=false){const s=state(),h=hash(s.seed,n.depth,n.lane,i),id=exclusive[h%exclusive.length][0],scale=1+n.depth*.025;
    const e=spawnEnemy(id,randomEnemySpawn(),boss||i===0);e.shadow=true;e.shadowDepth=n.depth;e.shadowMinion=false;e.color=['#bb90e6','#829dc8','#bb8198','#b3c597'][h%4];
    e.maxHp*=scale*(boss?6:1)*(n.modifier==='Greed'?1.4:1);e.hp=e.maxHp;e.damage*=1+Math.sqrt(n.depth)*.06;e.speed*=1+Math.min(.45,Math.log2(n.depth+1)*.035);
    e.shadowTraits=[];const count=Math.min(4,1+Math.floor(n.depth/35));for(let j=0;j<count;j++){const trait=traits[(h+j*5)%traits.length];e.shadowTraits.push(trait);}
    if(boss){e.boss=true;e.elite=true;e.ai=['bossGolem','bossNecro','bossDrake','bossVoid'][h%4];e.name=`${e.shadowTraits.join(' ')} ${n.bossRank}`;e.r*=1.5;e.x=9;e.y=5;e.xp*=6;}else e.name=`${e.shadowTraits.join(' ')} ${ENEMY_TYPES[id].name}`;
    if(e.shadowTraits.includes('Giant'))e.r*=1.3;if(e.shadowTraits.includes('Tiny'))e.r*=.7;
    if(e.shadowTraits.includes('Shielded')){e.shield=e.maxHp*.2;e.shieldMax=e.shield;}
    e.shadowPulse=2+(h%20)/10;return e;
  }
  let attacker=null,pulses=[];
  const oldAI=updateEnemyAI;updateEnemyAI=function(e,d,range,speed,dt){if(!e.shadow)return oldAI(e,d,range,speed,dt);const t=e.shadowTraits||[];
    if(t.includes('Swift')||t.includes('Tiny'))speed*=1.18;if(t.includes('Berserker')&&e.hp<e.maxHp*.4)speed*=1.35;
    if(t.includes('Regenerating'))e.hp=Math.min(e.maxHp,e.hp+e.maxHp*.007*dt);
    e.shadowPulse-=dt;
    if(e.shadowPulse<=0){e.shadowPulse=4.5;
      if(t.includes('Teleporting'))pulses.push({life:.7,kind:'teleport',e,x:clamp(game.player.x-d.x*2,.7,ROOM_W-.7),y:clamp(game.player.y-d.y*2,.7,ROOM_H-.7)});
      if(t.includes('Arcane')||e.boss)enemyFan(e,3+(e.boss?2:0),1.1,4.6,'arcaneEnemy',{damage:e.damage*.55});
      if(t.includes('Frozen'))AWRegionalEnemies.addZone(e,1.7,2,e.damage*.25,'#a4e4ff',.5,'thorns');
      if(t.includes('Burning'))AWRegionalEnemies.addZone(e,1.4,3,e.damage*.22,'#edab83',.5);
      if(t.includes('Summoner')&&game.enemies.length<14){const m=spawnEnemy('voidStalker',{x:clamp(e.x+1,.5,ROOM_W-.5),y:e.y});m.shadow=true;m.shadowMinion=true;m.shadowTraits=['Tiny'];m.shadowPulse=4;m.hp=m.maxHp*=.5;}
    }
    attacker=e;try{return oldAI(e,d,range,speed,dt);}finally{attacker=null;}
  };
  const oldHurt=damagePlayer;damagePlayer=function(amount){const before=game.player?.hp,source=attacker||game.projectiles.find(q=>q.owner==='enemy'&&q.life>0&&dist(q,game.player)<q.r+game.player.r+.12)?.regionalSource;oldHurt(amount);const dealt=Math.max(0,before-game.player.hp);if(source?.shadowTraits?.includes('Vampiric')&&dealt)source.hp=Math.min(source.maxHp,source.hp+dealt*.6);};
  const oldHit=damageEnemy;damageEnemy=function(e,amount,tag='',dot=false){if(e?.shadow){const t=e.shadowTraits||[];if(t.includes('Armored'))amount*=.8;if(t.includes('Shadowed')&&Math.sin(elapsed*2+e.phase)>.7)amount*=.5;
    if(t.includes('Reflective')&&!dot&&(e.reflectAt||0)<=elapsed){e.reflectAt=elapsed+1.2;telegraph('circle',e.x,e.y,1,.5,'#eacfff');pulses.push({kind:'reflect',e,life:.5});}}
    return oldHit(e,amount,tag,dot);
  };
  const oldKill=killEnemy;killEnemy=function(e,tag){if(!e||e.dead)return;if(e.shadowMinion){e.dead=true;return;}const n=A.current(),first=n?.shadow&&!bit(n.id,'cleared');oldKill(e,tag);if(first&&e.elite)game.roomData.shadowBonus=(game.roomData.shadowBonus||0)+8;
    if(e.shadowTraits?.includes('Explosive'))AWRegionalEnemies.addZone(e,1.5,.3,e.damage*.5,'#e4a5ed',.8,'burst');
  };
  function loot(n,guaranteed=false){const s=state();if(bit(n.id,'claimed'))return;const h=hash(s.seed,n.depth,n.lane,'loot'),chance=Math.min(.85,.15+n.depth*.002)*(n.modifier==='Greed'?1.7:1);
    if(!guaranteed&&(h%1000)/1000>chance)return;bit(n.id,'claimed',true);
    const id=shadowGear[Math.floor(h/1000)%shadowGear.length][0],rarity=n.depth>=100?'Legendary':n.depth>=20?'Epic':'Rare',base=D.items[id];
    const level=Math.max(18,game.level+Math.floor(Math.log2(n.depth+1)*2)),item=base.slot==='weapon'?makeWeapon(base,level,rarity):base.slot==='armor'?makeArmor(base,level,rarity):makeTrinket(base,level,rarity);
    Object.assign(item,{name:base.name,baseName:base.name,special:base.special,regionalId:id,rarity,origin:`Shadow Depth ${n.depth}`,spellSlotBonus:base.spellSlotBonus||0,shadowDepth:n.depth});
    if(item.slot==='weapon')item.power*=1+n.depth*.004;else if(item.slot==='armor')item.hpBonus*=1+n.depth*.004;else for(const key of Object.keys(item.mods))item.mods[key]*=1+Math.min(1.5,Math.log2(n.depth+1)*.08);
    if(!game.loot){game.loot=item;AWModernUI.offerLoot?.(item);}else if((s.pendingLoot||=[]).length<12)s.pendingLoot.push(item);else{const gold=80+n.depth*4;game.gold+=gold;addMaterial('voidshard',3,true);toastMsg(`Shadow storage full — ${item.name} salvaged for ${gold} gold and 3 Void Shards.`);}
    if(n.depth>=50&&h%37===0){const variant=h%2?'shadowFireball':'eclipseLightning';A.unlockSpell(variant);if(!s.unlocks.includes(variant))s.unlocks.push(variant);}
    if(rarity==='Legendary'){AWModernUI.announce(item.name,`Legendary Shadow discovery • Depth ${n.depth}`);AWPresentation.audio.play('loot');fx('constellation',game.player.x,game.player.y,1.5,'#ddc4ff',{r:3});}
  }
  const oldClear=markRoomCleared;markRoomCleared=function(){const n=A.current();if(!n?.shadow){const result=oldClear();unlock();return result;}
    const room=game.roomData;if(room.cleared||game.enemies.some(e=>!e.dead))return;const encounter=room.shadowEncounter;
    if(n.type==='challenge'||n.type==='event'&&['waves','defend','escort'].includes(n.event))if(encounter&&encounter.stage<3){encounter.pending||=1;return;}
    if(n.type==='event'&&n.event==='rescue'&&encounter?.elapsed<12)return;
    if(n.type==='event'&&n.event==='escort'&&!encounter?.failed&&encounter?.ally?.progress<1)return;
    room.cleared=true;if(!bit(n.id,'cleared')){bit(n.id,'cleared',true);const bonus=n.type==='shadowBoss'?100+Math.floor(n.depth*1.5):n.type==='challenge'?35:12;
      award((bonus+n.depth*.6+(room.shadowBonus||0))*(n.modifier==='Greed'?1.7:1)*(encounter?.failed?.5:1));loot(n,n.type==='shadowBoss');
      if(n.type==='shadowBoss'){state().milestones.push(n.depth);state().milestones=state().milestones.slice(-80);room.shadowCheckpoint=true;game.interactables.push({type:'waystone',x:6,y:7,label:'Activate Shadow Waystone'});}
      if(n.type==='event')toastMsg(encounter?.failed?'The collapsing realm cost half its Glory.':'Shadow event completed — Glory banked.');
    }saveGame();updateHUD();
  };
  function claim(){const n=A.current();if(!n?.shadow||!['shrine','mount','treasure'].includes(n.type)||!game.roomData.cleared||bit(n.id,'claimed'))return false;
    if(n.type==='shrine'){A.unlockSpell(n.depth%2?'shadowFireball':'eclipseLightning');bit(n.id,'claimed',true);award(20+n.depth);}
    else if(n.type==='mount'){if(n.depth>=25&&state().glory>=1800&&!A.state().mounts.includes('shadowGryphon')){A.state().mounts.push('shadowGryphon');state().unlocks.push('shadowGryphon');bit(n.id,'claimed',true);award(40);}else{toastMsg('An Eclipse Gryphon requires Depth 25 and 1,800 Glory.');return false;}}
    else{loot(n,true);award(25+n.depth);}
    bit(n.id,'cleared',true);saveGame();return true;
  }
  const baseLoad=loadRoom;loadRoom=function(){pulses=[];const n=D.nodes[A.state()?.current];if(n?.shadow){windowAt(n.depth);for(const [key,r] of Object.entries(game.rooms))if(r.campaignNode?.startsWith('shadow:')||r.y===2000000)delete game.rooms[key];}
    const result=baseLoad(),at=A.current(),s=state();if(!s)return result;unlock();
    if(at?.shadow){bit(at.id,'seen',true);s.lastDepth=at.depth;if(at.depth>s.highest){award(5);s.highest=at.depth;}
      A.state().visited=A.state().visited.filter(id=>!isNode(id));A.state().cleared=A.state().cleared.filter(id=>!isNode(id));
      A.state().waystones=A.state().waystones.filter(id=>!isNode(id));if(s.waystones.includes(at.depth))A.state().waystones.push(at.id);
      if(bit(at.id,'cleared'))game.roomData.cleared=true;
      if(['sanctuary','merchant','shrine','treasure','mount'].includes(at.type)){game.interactables.push({type:'shadowSite',x:9,y:6,label:at.type==='sanctuary'?'Shadow Sanctuary':at.name});if(at.type==='sanctuary'&&!game.interactables.some(o=>o.type==='waystone'))game.interactables.push({type:'waystone',x:6,y:7,label:'Activate Shadow Waystone'});}
      if(s.waystones.includes(at.depth)&&at.lane===0&&!game.interactables.some(o=>o.type==='waystone'))game.interactables.push({type:'waystone',x:6,y:7,label:'Shadow Waystone'});
      if(at.type==='event'&&!game.roomData.cleared&&game.roomData.shadowEncounter)AWTravel.prepareEvent(at,game.roomData.shadowEncounter);
      game.roomData.shadowMood=true;
    }else if(s.unlocked&&at?.id==='gloam-city')game.interactables.push({type:'shadowPortal',x:12,y:4,label:'Enter the Shadow Realms'});
    saveGame();return result;
  };
  const baseDeath=playerDeath;playerDeath=function(){if(!A.current()?.shadow)return baseDeath();const s=state();s.deaths++;s.expedition=0;game.gold=Math.floor(game.gold*.94);game.player.hp=game.player.maxHp;A.state().riding=false;paused=false;modalPause=false;resume([...s.waystones].reverse().find(d=>d<=s.lastDepth)||1,true);toastMsg('Restored at a Shadow waystone. Permanent Glory is safe.');};
  const baseUpdate=update;update=function(dt){baseUpdate(dt);if(!running||paused||modalPause||roomTransition)return;for(const p of pulses){p.life-=dt;if(p.life<=0&&!p.e.dead){if(p.kind==='teleport'){p.e.x=p.x;p.e.y=p.y;}else enemyProjectile(p.e,enemyDir(p.e),4,'arcaneEnemy',{damage:p.e.damage*.4});}}pulses=pulses.filter(p=>p.life>0);
    const n=A.current(),r=game.roomData,e=r?.shadowEncounter;if(n?.shadow&&e&&!r.cleared){e.elapsed+=dt;e.threatTimer-=dt;AWTravel.updateObjective(e,dt);if(e.deadline&&e.elapsed>e.deadline){e.failed=true;e.deadline=0;AWRegionalEnemies.addZone(game.player,2,3,game.player.maxHp*.08,'#bc8af0',1);}
      if(e.pending){e.pending-=dt;if(e.pending<=0){e.pending=0;e.stage++;for(let i=0;i<3;i++)spawnShadow(n,i+e.stage*4);}}
      if(e.threatTimer<=0&&n.depth>=20){e.threatTimer=Math.max(3,7-Math.log2(n.depth+1)*.3);const at={x:clamp(game.player.x+Math.sin(elapsed)*1.5,.7,ROOM_W-.7),y:game.player.y};AWRegionalEnemies.addZone(at,1.1,1,10+n.depth*.15,'#c6a1ee',1);}
    }
  };
  // Variant casts use the existing learned spell/mutation infrastructure.
  SPELLS.shadowFireball={name:'Shadow Fireball',icon:'◉',rarity:'Legendary',damage:45,cooldown:7,cast:'shadowFireball',category:'Shadow/Void',desc:'A fire orb bursts into a short-lived void pool. Discovered in the Shadow Realms or at 1,000 Glory.'};
  SPELLS.eclipseLightning={name:'Eclipse Lightning',icon:'ϟ',rarity:'Legendary',damage:30,cooldown:8,cast:'eclipseLightning',category:'Storm',tags:['Shadow/Void'],desc:'Lightning chains into a shadow echo. Discovered in the Shadow Realms or at 2,400 Glory.'};
  for(const id of ['shadowFireball','eclipseLightning'])UPGRADE_POOLS[id]=[['power','Dark Channel','✦','Increase damage 30%.','power'],['wide','Dimensional Reach','◎','Increase pool size or arc reach.','wide'],['echo','Night Echo','◈','Add a second projectile or shadow arc.','echo']].map(u=>[id+'_'+u[0],...u.slice(1)]);
  SPELL_CASTS.shadowFireball=(id,s,m)=>{const d=spellAim();for(let i=0;i<(hasUpgrade(id,'echo')?2:1);i++)magicProjectile({vx:d.x*6.5+d.y*i,vy:d.y*6.5-d.x*i,damage:s.damage*m.power*(hasUpgrade(id,'power')?1.3:1),splash:1,color:'#c79cec',kind:'fireball',life:2,tag:'fire',onHit:(e,q)=>groundEffect('void',q.x,q.y,hasUpgrade(id,'wide')?2:1.4,2.5,'#b693e8',s.damage*m.power*.12,.4,{pull:.25})});};
  SPELL_CASTS.eclipseLightning=(id,s,m)=>{SPELL_CASTS.chain(id,s,m);const count=hasUpgrade(id,'echo')?2:1;const targets=game.enemies.filter(e=>!e.dead&&dist(e,game.player)<(hasUpgrade(id,'wide')?12:8)).slice(-count);for(const e of targets){damageEnemy(e,s.damage*m.power*(hasUpgrade(id,'power')?1.3:1)*.5,'soul');fx('lightning',game.player.x,game.player.y,.3,'#c89de9',{toX:e.x,toY:e.y,width:3});}};
  const oldPool=weightedSpellPool;weightedSpellPool=function(){return oldPool().filter(id=>!['shadowFireball','eclipseLightning'].includes(id)||game.player.unlocked.includes(id));};
  window.AWShadow={state,parse,isNode,definition,windowAt,hash,bit,known:id=>bit(id,'seen'),enter,resume,travel,leave,activate,claim,award,loot,unlock,exclusiveIds:exclusive.map(e=>e[0]),traits,gearIds:shadowGear.map(e=>e[0]),spawnShadow,continent:shadowContinent};
})();
