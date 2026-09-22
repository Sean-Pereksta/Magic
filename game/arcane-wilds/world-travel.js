'use strict';
(() => {
  const D=AWCampaignData,A=AWCampaign,U=AWCampaignUI,dirs=['N','E','S','W'],arrows={N:'↑',E:'→',S:'↓',W:'←'};
  const known=id=>A.state()?.visited.includes(id)||A.state()?.scouted.includes(id)||window.AWShadow?.known(id);
  const routeAllowed=(n,dir)=>!n.routeRequirements?.[dir]||A.state().mounts.some(id=>D.mounts[id]?.route===n.routeRequirements[dir]);
  function exits(){
    const n=A.current(),s=A.state();if(!n)return {};
    if(n.type!=='dungeon')return Object.fromEntries(Object.entries(n.exits||{}).filter(([dir])=>routeAllowed(n,dir)).map(([dir,id])=>[dir,{id,room:0,entry:D.opposite[dir]}]));
    const entrance=s.dungeonEntrances?.[n.id]||{node:n.connections.find(id=>D.nodes[id].type!=='ruler'),side:'S'};
    const rotate=dir=>dirs[(dirs.indexOf(dir)+dirs.indexOf(entrance.side)-2+4)%4];
    const result={};for(const [dir,room] of Object.entries(D.dungeonLinks[s.room]||{}))result[rotate(dir)]={id:n.id,room,entry:D.opposite[rotate(dir)]};
    // Each outer chamber connects to the world road on that side. This lets a
    // dungeon form a real junction without replacing its internal passages.
    for(const [dir,id] of Object.entries(n.exits||{})){
      const local=dirs[(dirs.indexOf(dir)-dirs.indexOf(entrance.side)+2+4)%4];
      if(({S:0,W:2,E:3,N:4})[local]===s.room&&routeAllowed(n,dir))result[dir]={id,room:0,entry:D.opposite[dir],returning:id===entrance.node};
    }
    return result;
  }
  function label(dir,exit){const n=D.nodes[exit.id];return n?.id===A.current()?.id&&n.type==='dungeon'?['Entrance','Fork','Relic chamber','Elite chamber','Heart chamber'][exit.room]:known(exit.id)?n?.name||'Unknown Path':'Unknown Path';}
  const baseOpen=doorOpen;doorOpen=function(dir){if(!A.inCampaign())return baseOpen(dir);const all=exits();if(dir&&!all[dir])return false;return !!Object.keys(all).length&&(game.roomData.cleared||game.roomData.town);};
  const baseTransition=transitionRoom;transitionRoom=function(dx,dy,dir){if(!A.inCampaign())return baseTransition(dx,dy,dir);const e=exits()[dir];if(!e||!doorOpen(dir)){game.player.x=clamp(game.player.x,.3,ROOM_W-.3);game.player.y=clamp(game.player.y,.3,ROOM_H-.3);return false;}
    if(window.AWShadow?.isNode(e.id))return AWShadow.travel(e.id,e.entry);
    const n=A.current(),destination=D.nodes[e.id];
    if(destination.type==='dungeon'&&n.id!==destination.id){A.state().dungeonEntrances||={};A.state().dungeonEntrances[destination.id]={node:n.id,side:e.entry};}
    if(n.shadow)return AWShadow.leave();
    return A.enter(e.id,e.room,e.entry);
  };
  const baseTravel=A.travel;A.travel=function(id,mode='road'){
    const n=A.current(),to=D.nodes[id];if(window.AWShadow?.isNode(id))return AWShadow.travel(id,'S',mode);
    if(n?.shadow)return mode==='waystone'?AWShadow.leave(id):false;
    if(n?.type==='ruler'&&id===D.continent(n.continent).start&&game.roomData.cleared)return A.enter(id,0,'S');
    const dir=Object.keys(n?.exits||{}).find(d=>n.exits[d]===id);
    if(to?.type==='dungeon'&&dir){A.state().dungeonEntrances||={};A.state().dungeonEntrances[id]={node:n.id,side:D.opposite[dir]};}
    return baseTravel(id,mode);
  };
  const baseLoad=loadRoom;loadRoom=function(){const wasRiding=A.state()?.riding,active=A.state()?.activeMount;const result=baseLoad();const n=A.current();if(!A.inCampaign())return result;
    if(n.waystone){game.interactables=game.interactables.filter(o=>o.type!=='villagePortal');game.interactables.push({type:'waystone',x:6,y:7,label:A.state().waystones?.includes(n.id)?'Arcane Waystone':'Activate Waystone'});}
    if(['resource','treasure','puzzle'].includes(n.type)){const site=game.interactables.find(o=>o.type==='campaignSite');if(site)site.label=n.type==='resource'?`Gather ${MATERIALS[n.resource]?.name||'materials'}`:n.name;}
    if(n.hidden&&!A.state().secrets.includes(n.id))A.state().secrets.push(n.id);
    if(wasRiding&&['astralGryphon','shadowGryphon'].includes(active)){game.player.gryphonArrival=5;game.player.tailwind=Math.max(game.player.tailwind,5);}
    if(n.type==='event'&&n.event&&!n.shadow&&!game.roomData.cleared){game.roomData.worldEvent={kind:n.event,time:0,wave:1,failed:false};prepareEvent(n,game.roomData.worldEvent);}
    pruneRooms();saveGame();return result;
  };
  function pruneRooms(){const state=A.state();state.cacheClaims||={};const normal=Object.entries(game.rooms).filter(([key,r])=>r.y!==2000000&&(r._campaign||key==='0,0'||r.x>=10000&&r.y>=10000&&r.y<10003));for(const [key,r] of normal)state.cacheClaims[key]=(r.chests||[]).map(c=>!!c.opened);if(normal.length>48){for(const [key,r] of normal.slice(0,normal.length-48))if(r!==game.roomData)delete game.rooms[key];}}
  const baseGet=getRoomData;getRoomData=function(x,y){const r=baseGet(x,y),claims=A.state()?.cacheClaims?.[r.key];if(claims)r.chests.forEach((c,i)=>{if(claims[i])c.opened=true;});return r;};
  const baseInteract=interact;interact=function(){const o=currentInteraction();if(o?.type==='waystone')return openWaystone();return baseInteract();};
  function button(parent,label,fn){const b=document.createElement('button');b.className='btn secondary';b.textContent=label;b.onclick=fn;parent.appendChild(b);return b;}
  function openWaystone(){const n=A.current(),s=A.state(),body=$('npcBody');$('npcName').textContent=n.name+' • Arcane Waystone';body.replaceChildren();
    if(!s.waystones.includes(n.id)){button(body,'Activate waystone',()=>{s.waystones.push(n.id);if(n.shadow)AWShadow.activate();saveGame();openWaystone();});}
    else{const p=document.createElement('p');p.textContent='Travel between activated waystones. Local roads remain available through their directional exits.';body.appendChild(p);
      for(const id of s.waystones)if(id!==n.id&&D.nodes[id]&&!D.nodes[id].shadow)button(body,D.nodes[id].name,()=>{if(n.shadow)AWShadow.leave(id);else A.travel(id,'waystone');});
      if(window.AWShadow?.state()?.unlocked)for(const depth of AWShadow.state().waystones)button(body,`Shadow Sanctuary • Depth ${depth}`,()=>AWShadow.resume(depth));
    }showOverlay('npcPanel');
  }
  const baseSite=U.openSite;U.openSite=function(){const n=A.current(),s=A.state();if(!['resource','treasure','puzzle'].includes(n.type))return baseSite();
    const body=$('npcBody');$('npcName').textContent=n.name;body.replaceChildren();
    const p=document.createElement('p');p.textContent=s.claimed.includes(n.id)?'This discovery is already recorded.':n.type==='puzzle'?'Read the sun cycle: first light, highest sun, fading light. Touch the runes in order.':n.type==='resource'?`A source of ${MATERIALS[n.resource].name}. Supplies replenish only on a new journey.`:'A hidden vault holds regional treasure.';body.appendChild(p);
    if(s.claimed.includes(n.id)){showOverlay('npcPanel');return;}
    function collect(){s.claimed.push(n.id);if(n.type==='resource')addMaterial(n.resource,6+Math.floor(n.threat/5));else{game.gold+=n.rewardGold||80+n.threat*5;addMaterial(n.material,5,true);const gear=AWRegionalContent.gearIds.filter(id=>D.items[id].region===n.continent&&!D.towns[D.items[id].source]);if(gear.length&&!s.itemClaims.includes(gear[n.index%gear.length])){const id=gear[n.index%gear.length];s.itemClaims.push(id);s.rewards.push(id);}}saveGame();U.openSite();}
    if(n.type==='puzzle'){let step=0;for(const word of ['Dusk','Dawn','Zenith'])button(body,word,()=>{if(word===n.puzzle[step]){step++;p.textContent=`${step}/3 runes alight.`;if(step===3)collect();}else{step=0;p.textContent='The runes dim. Follow the sun’s journey.';}});}else button(body,n.type==='resource'?'Gather supplies':'Open vault',collect);showOverlay('npcPanel');
  };
  const baseClaim=A.claimSite;A.claimSite=function(){const ok=baseClaim();if(ok){const n=A.current();if(n.type==='landmark'){for(const id of n.connections){const target=D.nodes[id];if(target.hidden&&!A.state().scouted.includes(id))A.state().scouted.push(id);}saveGame();}}return ok;};
  function prepareEvent(n,event){event.kind=n.event;
    if(['rescue','defend','escort'].includes(n.event)){event.ally={x:n.event==='escort'?4:9,y:7,hp:100,progress:0};game.interactables.push({type:'eventAlly',x:event.ally.x,y:7,event,label:n.event==='rescue'?'Rescue the Mage':n.event==='escort'?'Escort the Merchant':'Defend the Shrine'});}
    if(n.event==='crystals')for(let i=0;i<3;i++){const crystal=spawnEnemy('regionalCrystal',{x:5+i*4,y:5});crystal.name='Rift Crystal';crystal.regionalLife=Infinity;crystal.hp=crystal.maxHp*=2;}
    if(n.event==='hunt'){const target=game.enemies.find(e=>e.elite)||game.enemies[0];if(target){target.elite=true;target.name='Hunted '+target.name;target.hp=target.maxHp*=1.3;}}
    toastMsg(({rescue:'Clear the ambush and protect the mage.',defend:'Protect the shrine through three waves.',escort:'Stay near the merchant to lead them across the realm.',crystals:'Destroy every Rift Crystal and its guardians.',hunt:'Defeat the marked elite.',collapse:'Clear the realm before it collapses.',waves:'Survive three enemy waves.'})[n.event]);
  }
  function updateObjective(event,dt){const ally=event.ally;if(!ally||event.failed)return;
    const threats=game.enemies.filter(e=>!e.dead&&dist(e,ally)<2.3).length;ally.hp=Math.max(0,ally.hp-threats*dt*6);
    if(ally.hp===0){event.failed=true;toastMsg('The objective fell. Finish the encounter for reduced rewards.');}
    if(event.kind==='escort'&&dist(game.player,ally)<3){ally.progress=Math.min(1,ally.progress+dt/14);ally.x=4+ally.progress*10;}
    const marker=game.interactables.find(o=>o.type==='eventAlly'&&o.event===event);if(marker){marker.x=ally.x;marker.y=ally.y;}
  }
  const baseClear=markRoomCleared;markRoomCleared=function(){const event=game.roomData?.worldEvent;if(event&&!game.roomData.cleared){if(['waves','defend'].includes(event.kind)&&event.wave<3){if(!event.pending)event.pending=1;return;}if(event.kind==='rescue'&&event.time<10)return;}
    const was=game.roomData?.cleared;const result=baseClear();if(!was&&game.roomData?.cleared&&event){game.gold+=Math.round((50+game.roomData.difficulty*4)*(event.failed?.5:1));toastMsg(event.failed?'Encounter survived — reduced supplies earned.':`${event.kind==='rescue'?'Mage rescued':event.kind==='defend'?'Shrine defended':event.kind==='hunt'?'Elite hunted':event.kind==='crystals'?'Rift closed':'Waves survived'} — supplies earned.`);addMaterial(A.current().material||'dust',event.failed?2:4,true);A.state().worldQuests[A.current().id]=event.failed?'survived':'completed';saveGame();}return result;};
  const baseUpdate=update;update=function(dt){baseUpdate(dt);if(!running||paused||modalPause||roomTransition)return;const event=game.roomData?.worldEvent;
    if(event&&!game.roomData.cleared){event.time+=dt;updateObjective(event,dt);if(event.pending){event.pending-=dt;if(event.pending<=0){event.pending=0;event.wave++;const ids=AWRegionalEnemies.ids.filter(id=>ENEMY_TYPES[id].region===A.current().continent);for(let i=0;i<3;i++)spawnEnemy(ids[(i+event.wave)%ids.length],randomEnemySpawn(),i===0&&event.wave===3);}}}
  };
  const baseDraw=drawDoors;drawDoors=function(room,pal){if(!A.inCampaign())return baseDraw(room,pal);const all=exits();
    for(const dir of dirs){const exit=all[dir],d=doorRect(dir),s=worldToScreen(d.x,d.y);ctx.save();ctx.translate(s.x,s.y);ctx.lineWidth=3;
      if(!exit){ctx.fillStyle=colorAlpha(pal.edge||'#182027',.9);ctx.fillRect(-29,-28,58,30);ctx.strokeStyle=colorAlpha(pal.accent,.25);ctx.strokeRect(-29,-28,58,30);ctx.restore();continue;}
      const n=D.nodes[exit.id],color=n?.shadow?'#c29aff':biomePalette[D.towns[n?.town]?.theme||n?.biome]?.accent||pal.accent,open=doorOpen(dir);
      ctx.strokeStyle=open?color:'#d8797a';ctx.fillStyle=open?'rgba(5,12,20,.88)':'rgba(79,20,24,.8)';ctx.beginPath();ctx.roundRect(-24,-58,48,62,10);ctx.fill();ctx.stroke();
      ctx.strokeStyle=color;ctx.globalAlpha=.55;for(let i=0;i<3;i++){const x=-15+i*15,y=-15-Math.sin(elapsed*1.5+i)*4;ctx.beginPath();ctx.moveTo(x,y);ctx.lineTo(x,y-15);ctx.stroke();}ctx.globalAlpha=1;
      const near=dist(game.player,d)<3.5;ctx.fillStyle='#eef1de';ctx.font=`${near?'bold 12':'10'}px system-ui`;ctx.textAlign='center';ctx.fillText(`${arrows[dir]} ${label(dir,exit)}`,0,-69);if(!open){ctx.fillStyle='#ffa0a3';ctx.fillText('CLEAR THE AREA',0,-84);}ctx.restore();
    }
  };
  const baseObject=drawInteractable;drawInteractable=function(o){if(o.type!=='waystone'&&o.type!=='eventAlly')return baseObject(o);const p=worldToScreen(o.x,o.y);ctx.save();ctx.translate(p.x,p.y);ctx.fillStyle=o.type==='waystone'?'#bbcaff':'#ffe3a1';ctx.strokeStyle=ctx.fillStyle;ctx.lineWidth=3;ctx.beginPath();ctx.moveTo(0,-47);ctx.lineTo(13,-19);ctx.lineTo(0,1);ctx.lineTo(-13,-19);ctx.closePath();ctx.stroke();if(dist(game.player,o)<2){ctx.font='12px system-ui';ctx.textAlign='center';ctx.fillText(o.label,0,-61);}ctx.restore();};
  const drawObjective=drawInteractable;drawInteractable=function(o){drawObjective(o);if(o.type!=='eventAlly'||!o.event?.ally)return;const at=worldToScreen(o.x,o.y),ally=o.event.ally;ctx.save();ctx.fillStyle='#3c2535';ctx.fillRect(at.x-22,at.y-63,44,5);ctx.fillStyle=ally.hp>30?'#a2db9d':'#ef887d';ctx.fillRect(at.x-22,at.y-63,44*ally.hp/100,5);if(o.event.kind==='escort'){ctx.fillStyle='#e3dcbb';ctx.font='10px system-ui';ctx.textAlign='center';ctx.fillText(Math.round(ally.progress*100)+'%',at.x,at.y-69);}ctx.restore();};
  window.AWTravel={exits,label,openWaystone,pruneRooms,prepareEvent,updateObjective};
})();
