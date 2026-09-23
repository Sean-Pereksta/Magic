'use strict';
/* Single adapter between campaign progression and the existing room/combat engine. */
(() => {
  const D=window.AWCampaignData;
  const originals={getRoomData,loadRoom,beginWorld,spawnRoomEnemies,transitionRoom,markRoomCleared,buildVillagePeople,openNPCPanel,interact,materialForEnemy,playerDeath,updateEnemyAI,drawInteractable,drawPlayer,playerMovement,damagePlayer,autoAttack,dodge,castSpell,render,updateHUD,renderMinimap,gearCard};
  const coordIndex=new Map();
  for(const n of Object.values(D.nodes))for(let i=0;i<n.roomCount;i++){
    const p=n.id==='verdant-city'?{x:0,y:0}:{x:10000+n.index*10+i,y:10000+D.continents.indexOf(D.continent(n.continent))};
    coordIndex.set(roomKey(p.x,p.y),{node:n,room:i,...p});
  }
  function state(){return game.campaign;}
  function current(){return D.nodes[state()?.current];}
  function inCampaign(){return !!state()&&game.roomData?.campaignNode===state().current;}
  function coordinates(id,room=0){if(D.coordinates&&id.startsWith('shadow:'))return D.coordinates(id,room);const n=D.nodes[id];return id==='verdant-city'?{x:0,y:0}:{x:10000+n.index*10+room,y:10000+D.continents.indexOf(D.continent(n.continent))};}
  function addUnique(list,id){if(!list.includes(id))list.push(id);}
  function setup(){
    const legacy=!game.campaign;game.campaign=D.normalize(game.campaign);
    if(legacy){
      // Keep the complete old character and room archive. Only its campaign location changes.
      const old=game.rooms?.['0,0'];if(old?.seen)addUnique(state().visited,'verdant-city');
    }
    game.room=coordinates(state().current,state().room);
    if(legacy&&game.level>1)toastMsg('Your character is safe. A new journey begins at Sunmere Haven.');
  }
  beginWorld=function(){setup();return originals.beginWorld();};
  getRoomData=function(x,y){
    const at=state()&&(D.locate?.(x,y)||coordIndex.get(roomKey(x,y)));if(!at)return originals.getRoomData(x,y);
    const n=at.node,s=state(),c=D.continent(n.continent),key=roomKey(x,y);
    let r=game.rooms[key];
    if(!r||r.campaignNode!==n.id){
      const old=r,town=D.isTown(n),safe=town||['shrine','landmark','mount','merchant','passage','resource','treasure','puzzle','sanctuary'].includes(n.type);
      r={x,y,key,biome:n.biome,town,boss:['boss','ruler','shadowBoss'].includes(n.type)||(n.type==='dungeon'&&at.room===4),elite:n.type==='danger'||n.type==='event'||at.room===3,difficulty:n.threat+at.room,name:n.name,seen:!!old?.seen,cleared:false,scenery:[],deco:[],chests:[],seed:Math.floor(hash2(x,y,game.seed)*1e9),campaignNode:n.id,campaignRoom:at.room,regionTier:D.continents.indexOf(c)+1,villageLevel:D.continents.indexOf(c)+1,challenge:n.type==='event'&&n.continent==='meridian'?'Arcane Crossfire':null,_expanded:true,_campaign:true};
      if(town){
        const theme=D.towns[n.town].theme,palette=`campaign_${n.town}`;
        biomePalette[palette]={...biomePalette[theme],name:n.name};
        BIOME_PROP_SETS[palette]=['house','market','lamp','banner','garden',...(BIOME_PROP_SETS[theme]||[]).slice(0,3)];r.biome=palette;
      }
      generateScenery(r);
      // Arenas use open centers and themed perimeter pillars. Town plans differ by culture.
      if(r.boss){
        r.scenery=r.scenery.filter(p=>Math.hypot(p.x-ROOM_W/2,p.y-ROOM_H/2)>5);
        for(let j=0;j<8;j++){const a=j*TAU/8;r.scenery.push({type:n.biome==='thornwild'?'oak':n.biome==='volcanic'?'basalt':n.biome==='crystal'?'iceCrystal':'column',x:9+Math.cos(a)*6.7,y:7+Math.sin(a)*5,scale:1.35,phase:a});}
      }
      if(town){r.scenery=r.scenery.filter(p=>Math.hypot(p.x-9,p.y-7)>3.8);r.chests=[];}
      if(n.type==='dungeon')r.name=`${n.name} • ${['Entrance','Fork','Relic chamber','Elite chamber','Heart chamber'][at.room]}`;
      if(safe)r.cleared=true;
      r.chests=old?.chests||r.chests;game.rooms[key]=r;
    }
    if(s.cleared.includes(n.id)||(s.dungeonClears[n.id]||[]).includes(at.room))r.cleared=true;
    return r;
  };
  function pool(n){
    const i=D.continents.indexOf(D.continent(n.continent));
    const regional={
      meadow:['wolf','wisp','charger'],forest:['wolf','wisp','archer'],thornwild:['briarprowler','bloomhexer','rootjuggernaut'],swamp:['serpent','necro','skeleton'],ruins:['sentinel','archer','golem'],
      desert:['charger','mage','sentinel'],crystal:['prismscarab','glassoracle','shardram'],stormlands:['stormhound','skyraider','galeweaver','stormknight'],frost:['frostwitch','sentinel','wolf'],volcanic:['drake','bomber','golem'],
      gloam:['duskblade','voidmoth','abyssanchor'],celestial:['sunwarden','starcaller','astralknight'],crypt:['necro','vampire','warpriest','skeleton'],bloodroot:['marrowboar','crimsonwitch','thorncolossus']
    };
    return (regional[n.biome]||regional.forest).filter(id=>ENEMY_TYPES[id]&&(i>0||ENEMY_TYPES[id].min<=n.threat+3));
  }
  function setupEncounter(room,n=D.nodes[room.campaignNode]){
    const st=intensityState();st.roomKey=room.key;st.hazards.length=0;st.lastSpell=null;st.reactionLock=0;
    // Dedicated objective events keep their own scheduler; never stack two controllers.
    const scriptedWaves=n.type==='event'&&['waves','defend'].includes(n.event);
    const tough=room.elite||room.challenge||n.type==='dungeon'||room.difficulty>=6||n.multiWave;
    const totalWaves=room.boss||scriptedWaves||!tough?1:clamp(2+(room.difficulty>=6?1:0)+(room.difficulty>=10||room.challenge?1:0),2,4);
    const completed=room.boss||scriptedWaves?0:Math.min(totalWaves-1,Math.max(0,Number(state().waveProgress?.[room.key])||0));
    return st.encounter={roomKey:room.key,wave:completed+1,totalWaves,boss:room.boss,pending:null,grace:2.2,
      hazardCd:room.challenge?2.4:999,spawnThreshold:2,
      waveSize:clamp(4+Math.floor(room.difficulty*.18)+(game.level>=12?1:0),4,7)};
  }
  spawnRoomEnemies=function(room){
    const n=D.nodes[room.campaignNode];if(!n)return originals.spawnRoomEnemies(room);
    setupEncounter(room,n);
    if(room.boss){
      const c=D.continent(n.continent),b=n.boss||[pool(n).at(-1),'bossGolem'];
      const e=spawnBoss({name:n.type==='ruler'?c.ruler:n.type==='dungeon'?`${n.name} Guardian`:n.name,base:b[0],ai:b[1],hp:n.type==='ruler'?7:4.5,damage:n.type==='ruler'?1.35:1.15,color:c.color},room);
      e.campaignRuler=n.type==='ruler';e.campaignPhase=1;e.proj=c.color;
      return;
    }
    const ids=pool(n),count=Math.min(9,4+D.continents.indexOf(D.continent(n.continent))+(room.elite?2:0));
    // Deliberate role groups: alternating frontline, ranged/support, then fast flanks.
    const frontline=ids.filter(id=>/shield|melee|Slam|Charge|shockwave|charger/.test(ENEMY_TYPES[id].ai));
    const ranged=ids.filter(id=>!frontline.includes(id));
    for(let j=0;j<count;j++){
      const list=j<2&&frontline.length?frontline:ranged.length?ranged:ids;
      const e=spawnEnemy(list[j%list.length],randomEnemySpawn(),room.elite&&j===0);
      e.campaignRole=j<2?'guard':j%3===0?'flank':'ranged';
    }
    intensityRefreshHud();
  };
  buildVillagePeople=function(room){
    const n=D.nodes[room.campaignNode];if(!n?.town)return originals.buildVillagePeople(room);
    const town=D.towns[n.town],c=D.continent(n.continent),roles=[...town.roles,'Villager','Villager','Villager'];
    roles.forEach((role,i)=>{
      const a=(i/roles.length)*TAU,x=9+Math.cos(a)*4,y=7+Math.sin(a)*3.4;
      const names=['Mara','Ilyra','Tovin','Nessa','Orin','Sera','Kest','Fen','Alia','Rook','Vela','Dorian','Ash','Cael','Luma','Ysolde','Thane','Brin','Nox','Elara','Isen','Syl','Morwen','Tala','Sol'];const name=names[(n.index+i*3)%names.length];
      game.interactables.push({type:'npc',npc:true,role,name,label:`${name} • ${role}`,x,y,homeX:x,homeY:y,targetX:x,targetY:y,walkTimer:1+i*.2,phase:a,color:role==='Villager'?biomePalette[town.theme].accent:c.color,campaignTown:n.town,culture:town.culture});
    });
  };
  loadRoom=function(){
    originals.loadRoom();if(!inCampaign())return;
    const s=state(),n=current(),r=game.roomData;
    addUnique(s.visited,n.id);document.body.dataset.awContinent=n.continent;
    if(r.town){s.checkpoint=n.id;addUnique(s.cleared,n.id);game.interactables=game.interactables.filter(o=>!['forge','seer'].includes(o.type));}
    if(n.type==='portalCity'){
      game.interactables.push({type:'continentPortal',x:9,y:3.1,label:'Continent Portal'});
      const c=D.continent(n.continent);
      if(D.portalReady(s,c)&&!s.celebrated.includes(n.id)){addUnique(s.celebrated,n.id);r.portalIgnition=elapsed;burst(9,3.1,c.color,28,2.2);window.AWModernUI?.announce('THE PORTAL ACTIVATES',`${c.ruler} waits beyond the light.`);}
    }
    if(!n.shadow&&['shrine','landmark','mount','merchant','passage','resource','treasure','puzzle'].includes(n.type)){
      addUnique(s.cleared,n.id);game.interactables.push({type:'campaignSite',x:9,y:6,label:n.name});
    }
    if(s.cleared.includes(n.id))r.cleared=true;
    if(!n.shadow&&n.type==='event'&&r.cleared)game.interactables.push({type:'campaignSite',x:9,y:6,label:'Explore the aftermath'});
    if(n.type==='ruler'&&!s.defeated.includes(n.id))window.AWModernUI?.announce(D.continent(n.continent).ruler,'Continent Ruler • break the three phases');
    if(s.rewards.length)toastMsg('A regional reward awaits in Character → Claim Reward.');
    recordLocationQuests();saveGame();updateHUD();
  };
  function enter(id,room=0,entry='S'){
    const n=D.nodes[id];if(!n||!game.player||roomTransition)return false;
    roomTransition=true;
    try{
      window.AWCampaignUI?.close();document.querySelectorAll('#npcPanel,#townPanel').forEach(el=>el.classList.add('hidden'));
      state().current=id;state().room=room;game.room=coordinates(id,room);
      game.player.x=entry==='W'?.8:entry==='E'?ROOM_W-.8:ROOM_W/2;game.player.y=entry==='N'?.8:entry==='S'?ROOM_H-.8:ROOM_H/2;game.player.dodgeTime=0;game.player.invuln=Math.max(game.player.invuln||0,1);
      game.arcaneEntrySide=entry;game.effects.length=0;game.particles.length=0;
      window.AWInput?.clear();modalPause=false;loadRoom();saveGame();return true;
    }finally{roomTransition=false;}
  }
  function travel(id,mode='road'){
    if(!running||paused||roomTransition||!inCampaign())return false;
    const reason=D.travelReason(state(),id,mode);if(reason){toastMsg(reason);return false;}
    const dir=Object.keys(current().exits||{}).find(d=>current().exits[d]===id);
    return enter(id,0,D.opposite?.[dir]||'S');
  }
  function enterPortal(){
    const n=current(),c=D.continent(n?.continent),s=state();
    if(!inCampaign()||n.id!==c.portalCity||!D.portalReady(s,c)||s.defeated.includes(c.boss))return false;
    return enter(c.boss);
  }
  transitionRoom=function(dx,dy,from){
    if(!inCampaign())return originals.transitionRoom(dx,dy,from);
    if(!game.roomData.cleared||roomTransition)return;
    const n=current(),next=n.type==='dungeon'?D.dungeonLinks[state().room][from]:undefined;
    if(next!==undefined)return enter(n.id,next,D.opposite[from]);
    const destination=n.type==='dungeon'?(state().room===0&&from==='S'?n.connections.find(id=>D.nodes[id]?.type!=='ruler'):null):n.exits?.[from];
    if(destination){const reason=D.travelReason(state(),destination);if(!reason)return enter(destination,0,D.opposite[from]);toastMsg(reason);}
    game.player.x=clamp(game.player.x,.5,ROOM_W-.5);game.player.y=clamp(game.player.y,.5,ROOM_H-.5);
  };
  function unlockSpell(id){if(!SPELLS[id])return;addUnique(game.player.unlocked,id);game.player.spellState[id]||={cd:0};}
  function finish(n){
    const s=state(),c=D.continent(n.continent),first=!s.cleared.includes(n.id);
    addUnique(s.cleared,n.id);
    if(['boss','ruler'].includes(n.type)){
      addUnique(s.defeated,n.id);
      if(n.type==='ruler'&&first){
        addUnique(s.mounts,c.mount);s.activeMount||=c.mount;unlockSpell(c.spell);s.rewards.push(c.reward);
        const next=D.continents[D.continents.indexOf(c)+1];if(next)addUnique(s.unlocked,next.id);
        window.AWModernUI?.announce(`${c.ruler} defeated`,next?`${next.name} unlocked • ${D.mounts[c.mount].name} earned`:'The three continents are free.');
      }else if(first&&D.portalReady(s,c)){
        window.AWModernUI?.announce('THE PORTAL AWAKENS',`Return to ${D.nodes[c.portalCity].name}. All four seals are lit.`);
        burst(9,7,c.color,30,2);game.gold+=100;
      }
    }
    if(first){game.gold+=25+n.threat*5;addMaterial(c.material,2+Math.floor(n.threat/4),true);if(n.type==='event')game.interactables.push({type:'campaignSite',x:9,y:6,label:'Explore the aftermath'});}
    recordLocationQuests();saveGame();
  }
  markRoomCleared=function(){
    if(!inCampaign())return originals.markRoomCleared();
    const r=game.roomData,was=r.cleared;originals.markRoomCleared();if(was||!r.cleared)return;
    const n=current();
    if(n.type==='dungeon'){
      const rooms=state().dungeonClears[n.id]||=([]);addUnique(rooms,state().room);
      if(state().room===2&&!state().claimed.includes(n.id)){addUnique(state().claimed,n.id);game.gold+=120+n.threat*5;unlockSpell(['spirits','icelance','seraphicArray'][D.continents.indexOf(D.continent(n.continent))]);}
      if(state().room!==4){saveGame();return;}
    }
    finish(n);
  };
  function recordLocationQuests(){
    if(!game.quests||!state())return;
    for(const q of game.quests.active)if(q.type==='campaign'&&!q.claimed){q.progress=state().cleared.includes(q.destination)?1:0;q.ready=q.progress===1;}
  }
  const baseQuest=questForVillage;
  questForVillage=function(room){
    const n=D.nodes[room.campaignNode];if(!n?.town)return baseQuest(room);
    const town=D.towns[n.town],target=D.nodes[town.quest];
    return {id:`${room.key}:quest`,type:'campaign',destination:target.id,title:`Open the way: ${target.name}`,desc:`Explore and clear ${target.name}. Return here to collect a regional commission.`,target:1,progress:state().cleared.includes(target.id)?1:0,ready:state().cleared.includes(target.id),rewardGold:100+n.threat*12,rewardMat:D.continent(n.continent).material,rewardCount:6};
  };
  const baseAccept=acceptQuest;
  acceptQuest=function(q){baseAccept(q);recordLocationQuests();saveGame();renderNPCPanelQuestList();};
  materialForEnemy=function(e){
    if(!inCampaign())return originals.materialForEnemy(e);
    const n=current();if(n.material&&MATERIALS[n.material])return n.material;return ({forest:'hide',meadow:'hide',thornwild:'hide',frost:'frost',crystal:'frost',volcanic:'ember',crypt:'bone',gloam:'bone',bloodroot:'bone',celestial:'dust',stormlands:'dust',desert:'iron',ruins:'iron',swamp:'hide'})[n.biome]||D.continent(n.continent).material;
  };
  function craftItem(id){
    const t=D.items[id];if(!t?.slot)return null;
    const template={...t,icon:t.icon||(t.slot==='weapon'?'🪄':t.slot==='armor'?'🛡':'💠')};
    const item=t.slot==='weapon'?makeWeapon(template,Math.max(1,game.level),t.rarity):t.slot==='armor'?makeArmor(template,Math.max(1,game.level),t.rarity):makeTrinket(template,Math.max(1,game.level),t.rarity);
    Object.assign(item,{name:t.name,baseName:t.name,desc:t.desc,regionalId:id,origin:D.continents.find(c=>c.reward===id)?.ruler||current().name,spellSlotBonus:t.spellSlotBonus||0,special:t.special});
    if(t.suffix){const suffix=SUFFIXES.find(s=>s.key===t.suffix);item.suffixKey=t.suffix;item.suffixText=suffix?.text;}
    return item;
  }
  function buy(id){
    if(!inCampaign()||!current().town)return false;
    const t=D.items[id],town=D.towns[current().town];
    if(!t||!town.stock.includes(id)||t.requires&&!state().cleared.includes(t.requires))return false;
    if(t.slot&&game.loot){toastMsg('Inspect or leave your pending loot first.');return false;}
    if(game.gold<t.price||Object.entries(t.cost||{}).some(([k,v])=>(game.materials[k]||0)<v)){toastMsg('You need more gold or local materials.');return false;}
    game.gold-=t.price;for(const [k,v] of Object.entries(t.cost||{}))game.materials[k]-=v;
    if(t.kind==='heal')healPlayer(game.player.maxHp*.5);
    else if(t.kind==='material')addMaterial(t.material,t.count,true);
    else {game.loot=craftItem(id);closeOverlay('npcPanel');openLootOverlay();}
    saveGame();updateHUD();return true;
  }
  function learn(id){
    const town=D.towns[current()?.town];if(!inCampaign()||!town?.spells.includes(id)||game.player.unlocked.includes(id))return false;
    const price=70+(rarityRank[SPELLS[id].rarity]||0)*110;if(game.gold<price){toastMsg('Not enough gold.');return false;}
    game.gold-=price;unlockSpell(id);saveGame();toastMsg(`${SPELLS[id].name} added to your Spellbook.`);return true;
  }
  function buyMount(id){
    const town=D.towns[current()?.town],s=state(),m=D.mounts[id];if(!inCampaign()||!town?.mounts.includes(id)||!m||s.mounts.includes(id)||game.gold<m.cost)return false;
    game.gold-=m.cost;addUnique(s.mounts,id);s.activeMount=id;saveGame();return true;
  }
  function mount(id=state()?.activeMount){
    const s=state();if(!inCampaign()||!s.mounts.includes(id))return false;
    if(s.activeMount===id)s.riding=!s.riding;else{s.activeMount=id;s.riding=true;}
    saveGame();toastMsg(s.riding?`${D.mounts[id].name} summoned. Dismount or dodge to fight.`:'Dismounted.');return true;
  }
  function claimSite(){
    const n=current(),s=state();if(!inCampaign()||n.shadow||s.claimed.includes(n.id)||!['mount','landmark','shrine','merchant','event'].includes(n.type)||n.type==='event'&&!game.roomData.cleared)return false;
    if(n.type==='merchant'){window.AWCampaignUI?.openCamp();return true;}
    addUnique(s.claimed,n.id);
    if(n.type==='event'){game.gold+=90+n.threat*7;healPlayer(game.player.maxHp*.3);if(n.continent==='gloam')unlockSpell('starCauseway');toastMsg('The road is safer. Supplies and a traveler’s blessing are yours.');}
    else if(n.mount){addUnique(s.mounts,n.mount);s.activeMount||=n.mount;toastMsg(`${D.mounts[n.mount].name} joins your journey.`);}
    else if(n.spell){unlockSpell(n.spell);healPlayer(game.player.maxHp);toastMsg(`${SPELLS[n.spell].name} discovered. Your wounds are healed.`);}
    else{game.gold+=80+n.threat*6;addMaterial(D.continent(n.continent).material,4,true);toastMsg('Landmark discovered. Ancient treasure recovered.');}
    saveGame();return true;
  }
  function claimReward(){
    if(!state()?.rewards.length||game.loot)return false;
    const id=state().rewards.shift();game.loot=craftItem(id);window.AWCampaignUI?.close();openLootOverlay();saveGame();return true;
  }
  openNPCPanel=function(npc){if(inCampaign())return window.AWCampaignUI?.openTown(npc);return originals.openNPCPanel(npc);};
  interact=function(){
    if(!inCampaign())return originals.interact();
    const o=currentInteraction();
    if(o?.type==='continentPortal')return window.AWCampaignUI?.openPortal();
    if(o?.type==='villagePortal')return window.AWCampaignUI?.open('Map','portal');
    if(o?.type==='campaignSite')return window.AWCampaignUI?.openSite();
    if(o?.type==='well'){healPlayer(game.player.maxHp);saveGame();toastMsg('Sanctuary restores your health.');return;}
    return originals.interact();
  };
  // Village Portal entry points and UI use the same campaign validation as ordinary travel.
  const oldPortals={...window.AWVillagePortals};
  Object.assign(window.AWVillagePortals,{
    destinations:()=>inCampaign()?state().visited.map(id=>D.nodes[id]).filter(n=>D.isTown(n)&&n.id!==state().current).map(n=>({key:n.id,...n})):oldPortals.destinations(),
    travel:key=>inCampaign()?travel(key,'portal'):oldPortals.travel(key),
    open:()=>inCampaign()?window.AWCampaignUI?.open('Map','portal'):oldPortals.open()
  });
  playerDeath=function(){
    if(!inCampaign())return originals.playerDeath();
    game.gold=Math.floor(game.gold*.94);state().riding=false;game.player.hp=game.player.maxHp;
    document.querySelectorAll('.overlay').forEach(el=>el.classList.add('hidden'));paused=false;modalPause=false;
    enter(state().checkpoint);toastMsg(`Restored at ${D.nodes[state().checkpoint].name}. World progress is safe.`);
  };
  playerMovement=function(dt){
    const p=game.player,m=inCampaign()&&state().riding&&D.mounts[state().activeMount],speed=p.speed;
    if(m)p.speed*=m.speed*(m.terrain?.includes(current().biome)?1.08:1);
    try{return originals.playerMovement(dt);}finally{p.speed=speed;}
  };
  autoAttack=function(dt){if(inCampaign()&&state().riding)return;return originals.autoAttack(dt);};
  dodge=function(){if(inCampaign())state().riding=false;return originals.dodge();};
  castSpell=function(slot){if(inCampaign()&&!paused&&!modalPause)state().riding=false;return originals.castSpell(slot);};
  damagePlayer=function(amount,...args){
    if(inCampaign()){
      state().riding=false;
      if(equippedItems().some(i=>i.special==='cinderShelter')&&current().biome==='volcanic')amount*=.8;
      if(equippedItems().some(i=>i.special==='stormShelter')&&current().biome==='stormlands')amount*=.85;
    }
    return originals.damagePlayer(amount,...args);
  };
  updateEnemyAI=function(e,d,range,speed,dt){
    if(inCampaign()&&!e.boss&&e.state==='idle'){
      if(e.campaignRole==='guard'){
        const ally=game.enemies.find(a=>a!==e&&!a.dead&&a.campaignRole==='ranged');
        if(ally&&dist(e,ally)>2.5){const guard={x:ally.x+(game.player.x-ally.x)*.32,y:ally.y+(game.player.y-ally.y)*.32};d=norm(guard.x-e.x,guard.y-e.y);}
      }else if(e.campaignRole==='flank'&&range>2){const side=e.id%2?1:-1;d=norm(d.x-d.y*.6*side,d.y+d.x*.6*side);}
      else if(e.campaignRole==='ranged'){
        const ally=game.enemies.find(a=>a!==e&&!a.dead&&dist(e,a)<1.2);
        if(ally)moveEnemy(e,norm(e.x-ally.x,e.y-ally.y),speed*.4,dt);
      }
    }
    originals.updateEnemyAI(e,d,range,speed,dt);
  };
  const baseBossAdds=intensityBossAdds;
  intensityBossAdds=function(e,phase){
    if(!inCampaign())return baseBossAdds(e,phase);
    const ids=pool(current());
    for(let i=0;i<(phase===3?3:2)&&game.enemies.length<18;i++)spawnEnemy(ids[i%ids.length],randomEnemySpawn(),false,.72);
  };
  const baseBossPhase=intensityBossPhase;
  intensityBossPhase=function(e,phase){
    baseBossPhase(e,phase);
    if(e.campaignRuler){e.campaignPhase=phase;window.AWModernUI?.announce(`${e.name} • Phase ${phase}`,phase===3?'The last seal breaks':'The arena awakens');}
  };
  const baseBossHazard=intensityBossHazard;
  intensityBossHazard=function(e){
    if(!inCampaign()||!e.campaignRuler)return baseBossHazard(e);
    const c=D.continent(current().continent),p=game.player,phase=e.intensityPhase||1;
    const points=[{x:p.x,y:p.y}];
    if(c.id==='verdant'){for(let i=0;i<phase;i++){const a=i*TAU/phase;points.push({x:9+Math.cos(a)*3,y:7+Math.sin(a)*3});}}
    else if(c.id==='meridian'){const d=enemyDir(e);for(let i=1;i<4;i++)points.push({x:e.x+d.x*i*1.8,y:e.y+d.y*i*1.8});}
    else {points.push({x:18-p.x,y:14-p.y});if(phase===3)points.push({x:9,y:7});}
    intensityQueueHazard(c.id==='meridian'?'crossfire':'execution',points,1.1,e.damage*.7,c.color,c.id==='verdant'?1.3:.9);
  };
  gearCard=function(item,label){const html=originals.gearCard(item,label);return item?.regionalId?html.replace('</div>',`<p class="aw-regional-origin">${D.items[item.regionalId]?.desc||''}<br>Origin: ${item.origin||'Continent ruler'}</p></div>`):html;};
  updateHUD=function(...args){originals.updateHUD(...args);if(!inCampaign())return;const n=current(),c=D.continent(n.continent);$('roomSub').textContent=`${c.name} • Threat ${game.roomData.difficulty}${n.type==='dungeon'?` • Room ${state().room+1}/5`:''}`;};
  let mapStamp='';
  renderMinimap=function(){
    if(!inCampaign())return originals.renderMinimap();
    const n=current(),c=D.continent(n.continent),stamp=[n.id,state().cleared.length,state().defeated.length].join('|');if(stamp===mapStamp)return;mapStamp=stamp;
    const root=$('minimapGrid');root.style.display='block';root.textContent=`🗺 ${n.name}${n.shadow?'':` · ${D.sealCount(state(),c)}/4 seals`}`;
    $('minimap').setAttribute('role','button');$('minimap').tabIndex=0;$('minimap').onclick=()=>window.AWCampaignUI?.open('Map');
    $('minimap').onkeydown=e=>{if(e.key==='Enter')window.AWCampaignUI?.open('Map');};
  };
  // A hidden menu never owns a render loop; the world canvas remains frozen while it is open.
  render=function(){if(running&&(paused||modalPause))return;return originals.render();};
  drawInteractable=function(o){
    if(!['continentPortal','campaignSite'].includes(o.type))return originals.drawInteractable(o);
    const n=current();if(!n)return;const c=D.continent(n.continent),p=worldToScreen(o.x,o.y),isPortal=o.type==='continentPortal';
    ctx.save();ctx.translate(p.x,p.y);ctx.textAlign='center';
    if(isPortal){
      const count=D.sealCount(state(),c),ready=D.portalReady(state(),c),defeated=state().defeated.includes(c.boss);
      ctx.lineWidth=12;ctx.strokeStyle='#525b67';ctx.beginPath();ctx.ellipse(0,-53,40,63,0,0,TAU);ctx.stroke();
      ctx.fillStyle=ready?colorAlpha(c.color,.36):'#111622';ctx.beginPath();ctx.ellipse(0,-53,33,57,0,0,TAU);ctx.fill();
      if(ready){
        const age=elapsed-(game.roomData.portalIgnition??-99);
        if(age<4){ctx.fillStyle=colorAlpha(c.color,.22*(1-age/4));ctx.beginPath();ctx.moveTo(-29,-20);ctx.lineTo(-90,-500);ctx.lineTo(90,-500);ctx.lineTo(29,-20);ctx.fill();}
        ctx.lineWidth=3;ctx.strokeStyle=c.color;for(let i=0;i<3;i++){ctx.beginPath();ctx.ellipse(0,-53,12+i*8,25+i*10,elapsed*.2+i,0,TAU);ctx.stroke();}}
      for(let i=0;i<4;i++){const a=-Math.PI*.85+i*Math.PI*.57;ctx.fillStyle=i<count?c.color:'#6a6b72';ctx.fillRect(Math.cos(a)*43-4,-53+Math.sin(a)*65-4,8,8);}
      ctx.fillStyle=c.color;ctx.font='bold 12px system-ui';ctx.fillText(defeated?'CONTINENT LIBERATED':ready?'ENTER THE GREAT PORTAL':`DORMANT • ${count}/4 SEALS`,0,-130);
    }else{
      ctx.strokeStyle=c.color;ctx.lineWidth=3;ctx.beginPath();ctx.ellipse(0,-8,26,12,0,0,TAU);ctx.stroke();ctx.fillStyle=c.color;ctx.font='28px system-ui';ctx.fillText(({shrine:'✦',landmark:'◆',mount:'♞',merchant:'⚖',passage:'⚓'})[n.type]||'✦',0,-21);ctx.font='11px system-ui';ctx.fillText(n.name,0,-58);
    }
    ctx.restore();
  };
  drawPlayer=function(p){
    const m=inCampaign()&&state().riding&&D.mounts[state().activeMount];
    if(m){
      const s=worldToScreen(p.x,p.y),moving=Math.hypot(window.AWInput?.move.x||0,window.AWInput?.move.y||0)>.1,bob=moving?Math.sin(elapsed*16)*3:0;
      ctx.save();ctx.translate(s.x,s.y);ctx.scale(p.facing.x<0?-1:1,1);ctx.strokeStyle=m.color;ctx.lineWidth=5;ctx.lineCap='round';
      for(let j=0;j<4;j++){const x=-16+j*10,walk=moving?Math.sin(elapsed*16+j)*6:0;ctx.beginPath();ctx.moveTo(x,-10+bob);ctx.lineTo(x+walk,6);ctx.stroke();}
      ctx.fillStyle=m.color;ctx.beginPath();ctx.ellipse(0,-20+bob,27,14,0,0,TAU);ctx.fill();ctx.beginPath();ctx.ellipse(26,-35+bob,9,15,-.4,0,TAU);ctx.fill();
      ctx.fillStyle='#342b42';ctx.fillRect(-9,-29+bob,20,8);ctx.strokeStyle=m.color;ctx.lineWidth=3;ctx.beginPath();ctx.moveTo(-23,-25);ctx.lineTo(-34,-16);ctx.stroke();
      if(['elk','stormstag'].includes(state().activeMount)){for(const side of [-1,1]){ctx.beginPath();ctx.moveTo(24,-47);ctx.lineTo(24+side*10,-60);ctx.lineTo(22+side*15,-57);ctx.stroke();}}
      ctx.restore();ctx.save();ctx.translate(0,-17+bob);originals.drawPlayer(p);ctx.restore();return;
    }
    return originals.drawPlayer(p);
  };
  const baseDoors=drawDoors;
  drawDoors=function(room,pal){
    baseDoors(room,pal);if(!inCampaign())return;
    for(const dir of ['N','E','S','W']){
      const next=current().type==='dungeon'?D.dungeonLinks[state().room][dir]:undefined;
      const d=doorRect(dir),p=worldToScreen(d.x,d.y,85);
      ctx.save();ctx.font='bold 10px system-ui';ctx.textAlign='center';ctx.fillStyle='#e4e7c4';ctx.fillText(next===undefined?'WORLD MAP':['ENTRANCE','FORK','RELICS','ELITE','GUARDIAN'][next],p.x,p.y);ctx.restore();
    }
  };
  const baseFloorDetails=drawFloorDetails;
  drawFloorDetails=function(room,pal){baseFloorDetails(room,pal);if(room.campaignNode&&room.town)drawTownRoads();};
  const baseAtmosphere=drawAtmosphere;
  drawAtmosphere=function(room,pal){const town=D.towns[D.nodes[room.campaignNode]?.town];return baseAtmosphere(town?{...room,biome:town.theme}:room,pal);};
  const baseHouse=house;
  house=function(sc,phase){
    const town=inCampaign()&&D.towns[current().town];if(!town||['meadow','forest','thornwild'].includes(town.theme)){baseHouse(sc,phase);if(town?.theme==='thornwild'){ctx.strokeStyle='#85b674';ctx.lineWidth=2;ctx.beginPath();ctx.moveTo(-17*sc,-6*sc);ctx.quadraticCurveTo(-32*sc,-35*sc,-12*sc,-45*sc);ctx.stroke();}return;}
    const theme=town.theme,color=biomePalette[theme].accent;
    isoBox(52*sc,28*sc,42*sc,theme==='volcanic'?'#454044':'#566271',theme==='volcanic'?'#2b292e':'#30394b',theme==='celestial'?'#82749e':'#7c91a2');
    ctx.fillStyle=color;ctx.fillRect(-5*sc,-32*sc,10*sc,15*sc);
    if(theme==='crystal'||theme==='celestial'){
      ctx.fillStyle=colorAlpha(color,.8);ctx.beginPath();ctx.moveTo(0,-83*sc);ctx.lineTo(25*sc,-44*sc);ctx.lineTo(0,-27*sc);ctx.lineTo(-25*sc,-44*sc);ctx.closePath();ctx.fill();
      ctx.strokeStyle='#edf7ff';ctx.lineWidth=1.5;ctx.beginPath();ctx.moveTo(0,-78*sc);ctx.lineTo(0,-31*sc);ctx.stroke();
    }else if(theme==='stormlands'||theme==='crypt'){
      ctx.fillStyle='#738092';for(let i=-2;i<=2;i++)ctx.fillRect(i*11*sc-4*sc,-54*sc,8*sc,12*sc);
      ctx.strokeStyle=color;ctx.lineWidth=2;ctx.beginPath();ctx.moveTo(18*sc,-45*sc);ctx.lineTo(18*sc,-79*sc);ctx.stroke();
    }else{
      ctx.fillStyle='#302932';ctx.fillRect(12*sc,-70*sc,12*sc,30*sc);ctx.strokeStyle=color;ctx.lineWidth=3;ctx.beginPath();ctx.moveTo(-25*sc,-41*sc);ctx.lineTo(25*sc,-41*sc);ctx.stroke();
    }
  };
  const baseNPC=drawVillageNPC;
  drawVillageNPC=function(n){
    baseNPC(n);if(!n.campaignTown)return;
    const town=D.towns[n.campaignTown],p=worldToScreen(n.x,n.y),bob=Math.sin(elapsed*6+n.phase)*1.2;
    ctx.save();ctx.translate(p.x,p.y-13+bob);ctx.fillStyle=biomePalette[town.theme].accent;ctx.strokeStyle=ctx.fillStyle;ctx.lineWidth=2;
    if(['Arcanist','Spell Scribe','Enchanter'].includes(n.role)){
      ctx.beginPath();ctx.moveTo(-9,-22);ctx.lineTo(0,-42);ctx.lineTo(10,-22);ctx.closePath();ctx.fill();
      ctx.beginPath();ctx.moveTo(13,5);ctx.lineTo(13,-24);ctx.stroke();ctx.beginPath();ctx.arc(13,-27,4,0,TAU);ctx.fill();
    }else if(['Blacksmith','Weaponsmith','Armorer'].includes(n.role)){
      ctx.fillStyle='#64717b';ctx.fillRect(-6,-10,12,14);ctx.fillRect(11,-10,4,17);ctx.fillRect(7,-14,12,6);
    }else if(n.role==='Stablemaster'){ctx.beginPath();ctx.ellipse(0,-24,12,3,0,0,TAU);ctx.fill();ctx.strokeRect(12,-8,7,13);}
    else if(n.role==='Alchemist'){ctx.fillStyle='#ade5bd';ctx.beginPath();ctx.arc(13,0,5,0,TAU);ctx.fill();ctx.fillRect(11,-8,4,6);}
    else if(town.theme==='celestial'||town.theme==='crystal'){ctx.beginPath();ctx.ellipse(0,-28,9,3,0,0,TAU);ctx.stroke();}
    else if(town.theme==='thornwild'){ctx.beginPath();ctx.ellipse(0,-24,10,4,-.3,0,TAU);ctx.fill();}
    ctx.restore();
  };
  window.AWCampaign={setupEncounter,enter,data:D,state,current,inCampaign,coordinates,pool,bossSummonType:(e,i)=>inCampaign()&&e.boss?pool(current())[i%pool(current()).length]:null,travel,enterPortal,mount,buy,buyMount,learn,claimSite,claimReward,craftItem,unlockSpell};
})();
