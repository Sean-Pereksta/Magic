'use strict';
/* One integration adapter: ordinary game currency, campaign rooms, saves and input. */
(() => {
  const C=AWHomeCore,A=AWCampaign,HOME='verdant-hearthglade';
  let pending=false,recall=null,portalLock=0,buildPreview=null,visualCache=null,visualAt=0;
  const unique=()=>globalThis.crypto?.randomUUID?.()||`${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  function ensure(){if(!game.player)return null;if(!game.homestead)game.homestead=C.fresh(unique(),Date.now());return game.homestead;}
  const state=()=>ensure(),atHome=()=>A.current()?.id===HOME,inside=()=>atHome()&&A.state().room===1;
  const wallet=()=>({gold:game.gold,materials:{...game.materials}});
  const context=()=>({atHome:atHome(),atTown:!!A.current()?.town,continent:A.current()?.continent,unlocked:A.state()?.unlocked||['verdant'],visited:A.state()?.visited||[],shadowUnlocked:!!window.AWShadow?.state()?.unlocked,shadowWaystones:window.AWShadow?.state()?.waystones||[]});
  function now(){return window.AWHomeCloud?.now?.()||Date.now();}
  function snapshot(){return C.settle(C.clone(state()),now());}
  function visual(){const h=state(),t=now();if(!visualCache||visualCache.journeyId!==h.journeyId||visualCache.revision!==h.revision||t-visualAt>=1000){visualCache=snapshot();visualAt=t;}return visualCache;}
  function accept(result){
    game.homestead=result.home;game.gold=result.wallet.gold;game.materials=result.wallet.materials;
    for(const effect of result.effects||[])if(effect.type==='heal')healPlayer(game.player.maxHp*effect.fraction);else if(effect.type==='message')toastMsg(effect.text);
    if(atHome())rebuild();saveGame();updateHUD();window.AWHomeUI?.refresh();
  }
  async function action(type,args={}){
    if(pending)return false;ensure();pending=true;window.AWHomeUI?.refreshStatus();
    try{
      const clock=window.AWHomeCloud?await AWHomeCloud.actionTime():Date.now();
      const command={...args,type,requestId:unique()},result=C.apply(state(),wallet(),command,clock,context());
      if(window.AWHomeCloud)await AWHomeCloud.commit(result);
      accept(result);return true;
    }catch(error){toastMsg(error.message||'Home action could not be saved.');window.AWHomeUI?.message(error.message);return false;}
    finally{pending=false;window.AWHomeUI?.refreshStatus();}
  }
  const baseBegin=beginWorld;
  beginWorld=function(){ensure();game.homestead=C.normalize(game.homestead,unique(),Date.now());const result=baseBegin();window.AWHomeUI?.refresh();return result;};
  const baseGet=getRoomData;
  getRoomData=function(x,y){const r=baseGet(x,y);if(r.campaignNode===HOME){r.cleared=true;r.town=false;r.boss=false;r.elite=false;r.challenge=null;r.scenery=[];r.deco=[];r.chests=[];r.homestead=true;r.name=r.campaignRoom===1?'Hearthglade · Home Interior':'Hearthglade · Garden and Grounds';}return r;};
  const baseLoad=loadRoom;
  loadRoom=function(){recall=null;buildPreview=null;portalLock=1.2;const result=baseLoad();if(atHome()){ensure().visited=true;game.player.riding=false;rebuild();}window.AWHomeUI?.refresh();return result;};
  function rebuild(){
    if(!atHome()||!game.roomData)return;const h=state(),objects=[];
    game.enemies=[];game.projectiles=[];game.telegraphs=[];const st=intensityState();st.encounter=null;st.hazards=[];
    const object=(kind,x,y,label,extra={})=>objects.push({type:'homeObject',homeKind:kind,x,y,label,...extra});
    if(inside()){
      const floor=C.interior(h);if(!h.tier){A.enter(HOME,0);return;}
      object('exit',9,floor.y+floor.h-.35,'Leave house');
      for(const i of h.items.filter(i=>!i.packed&&!C.items[i.kind].outdoor)){const r=C.footprint(i);object('item',r.x+r.w/2,r.y+r.h/2,C.items[i.kind].name,{homeId:i.id});}
      game.player.x=clamp(game.player.x,floor.x+.35,floor.x+floor.w-.35);game.player.y=clamp(game.player.y,floor.y+.35,floor.y+floor.h-.35);
    }else{
      const r=C.houseRect(h);object('house',r.x+2,r.y+2,h.tier?'Enter '+C.tiers[h.tier].name:'Build your cottage');
      object('well',10,7,'Well · Refill watering can');object('board',8.5,11.5,'Homestead plans');
      object('return',9,12.5,'Return to your adventure');
      for(const p of h.plots)object('plot',p.x+(p.tree?1:.5),p.y+(p.tree?1:.5),p.tree?'Orchard tree':'Garden bed',{homeId:p.id});
      for(const i of h.items.filter(i=>!i.packed&&C.items[i.kind].outdoor)){const r=C.footprint(i);object('item',r.x+r.w/2,r.y+r.h/2,C.items[i.kind].name,{homeId:i.id});}
    }
    game.interactables=objects;game.roomData.scenery=[];game.roomData.cleared=true;window.AWWorld?.invalidate();
  }
  const baseSpawn=spawnRoomEnemies;spawnRoomEnemies=function(room){if(room.campaignNode===HOME){room.cleared=true;return;}return baseSpawn(room);};
  const baseDoor=doorOpen;doorOpen=function(dir){return inside()?false:baseDoor(dir);};
  const baseTransition=transitionRoom;transitionRoom=function(dx,dy,from){if(inside())return;return baseTransition(dx,dy,from);};
  const baseDrawDoors=drawDoors;drawDoors=function(room,pal){if(!inside())return baseDrawDoors(room,pal);};
  function safe(){
    if(!running||roomTransition||pending||!game.roomData?.cleared||game.enemies.some(e=>!e.dead&&e.hp>0))return false;
    const enc=intensityState().encounter,event=game.roomData.worldEvent,shadow=game.roomData.shadowEncounter;
    return !(enc?.roomKey===game.roomData.key&&(enc.pending||enc.wave<enc.totalWaves))&&!event?.pending&&!shadow?.pending;
  }
  function enterHouse(){if(!state().tier)return window.AWHomeUI?.open('build');window.AWHomeUI?.close();A.enter(HOME,1,'S');const floor=C.interior(state());game.player.x=9;game.player.y=floor.y+floor.h-1.2;rebuild();}
  function leaveHouse(){window.AWHomeUI?.close();A.enter(HOME,0,'S');const r=C.houseRect(state());game.player.x=r.x+2;game.player.y=r.y+r.h+.5;}
  function startRecall(){
    if(atHome())return window.AWHomeUI?.open();
    if(!state().tier)return toastMsg('Build the cottage in Hearthglade to unlock home recall.');
    if(!safe())return toastMsg('Finish all enemies, waves and objectives before recalling home.');
    window.AWCampaignUI?.close();window.AWHomeUI?.close();AWInput.clear();recall={remaining:2.5,node:A.current().id,room:A.state().room,x:game.player.x,y:game.player.y,hp:game.player.hp};toastMsg('Recalling to Hearthglade… stand still.');
  }
  function finishRecall(){
    state().returnPoint={node:recall.node,room:recall.room};recall=null;A.enter(HOME,0,'S');saveGame();
  }
  function returnAdventure(){
    if(!atHome()||!safe())return false;const point=state().returnPoint,n=AWCampaignData.nodes[point?.node];let id='verdant-city',room=0;
    if(n&&!n.shadow&&A.state().unlocked.includes(n.continent)&&A.state().visited.includes(n.id)){id=n.id;room=Math.min(point.room||0,n.roomCount-1);}
    window.AWHomeUI?.close();return A.enter(id,room,'S');
  }
  function travelPortal(id){
    if(!atHome()||!safe()||portalLock>0)return false;const i=state().items.find(i=>i.id===id&&!i.packed),destination=i?.destination;
    const reason=i?C.portalReason(i,destination,context()):'Portal unavailable.';if(reason){toastMsg(reason);return false;}
    portalLock=2;window.AWHomeUI?.close();
    if(destination.shadow)return AWShadow.resume(destination.depth,true);
    return A.enter(destination.id,0,'S');
  }
  const baseInteract=interact;interact=function(){const o=currentInteraction();if(!atHome()||o?.type!=='homeObject')return baseInteract();
    if(o.homeKind==='house')return enterHouse();if(o.homeKind==='exit')return leaveHouse();if(o.homeKind==='return')return returnAdventure();
    if(o.homeKind==='well')return window.AWHomeUI?.open('water');if(o.homeKind==='board')return window.AWHomeUI?.open();
    return window.AWHomeUI?.open(o.homeKind,o.homeId);
  };
  const baseClear=markRoomCleared;
  markRoomCleared=function(){
    const room=game.roomData,enc=intensityState().encounter;
    if(A.inCampaign()&&!room.cleared&&!game.enemies.some(e=>!e.dead&&e.hp>0)&&enc?.roomKey===room.key&&enc.wave<enc.totalWaves){
      A.state().waveProgress||={};const done=Math.max(A.state().waveProgress[room.key]||0,enc.wave);if(done!==A.state().waveProgress[room.key]){A.state().waveProgress[room.key]=done;saveGame();}
    }
    const was=room?.cleared,result=baseClear();
    if(!was&&room?.cleared&&A.inCampaign()&&!atHome()){
      ensure();if(state().rested>0)state().rested--;const key=A.current().id+':'+A.state().room;
      const award=C.apply(state(),wallet(),{type:'claimExpedition',requestId:'encounter:'+key,node:key},now(),{earned:true,multiWave:(enc?.totalWaves||1)>1||['waves','defend'].includes(room.worldEvent?.kind)});
      game.homestead=award.home;game.gold=award.wallet.gold;game.materials=award.wallet.materials;
      for(const e of award.effects)toastMsg(e.text);delete A.state().waveProgress?.[room.key];saveGame();
    }return result;
  };
  const baseXP=gainXP;gainXP=function(amount){return baseXP(amount*(game.homestead?.rested>0?1.05:1));};
  function walls(){
    const h=state();if(inside())return C.blocked(h,true);
    const result=C.blocked(h,false);if(h.tier){const r=C.houseRect(h);result.push({...r,h:r.h-1});}return result;
  }
  function allowed(x,y){
    const h=state(),bounds=inside()?C.interior(h):{x:.1,y:.1,w:17.8,h:13.8},r=.19;
    if(inside()&&(x<bounds.x+r||x>bounds.x+bounds.w-r||y<bounds.y+r||y>bounds.y+bounds.h-r))return false;
    return !walls().some(b=>x>b.x-r&&x<b.x+b.w+r&&y>b.y-r&&y<b.y+b.h+r);
  }
  const baseMove=playerMovement;playerMovement=function(dt){
    if(!atHome())return baseMove(dt);const p=game.player,old={x:p.x,y:p.y};baseMove(dt);if(!atHome())return;
    const end={x:p.x,y:p.y},steps=Math.max(1,Math.ceil(Math.hypot(end.x-old.x,end.y-old.y)/.12));p.x=old.x;p.y=old.y;
    for(let k=0;k<steps;k++){const dx=(end.x-old.x)/steps,dy=(end.y-old.y)/steps;if(allowed(p.x+dx,p.y))p.x+=dx;if(allowed(p.x,p.y+dy))p.y+=dy;}
    portalLock=Math.max(0,portalLock-dt);
    if(inside()&&portalLock<=0){const item=state().items.find(i=>!i.packed&&C.items[i.kind].portal&&i.destination&&Math.hypot(p.x-i.x-.5,p.y-i.y-.5)<.85);if(item)travelPortal(item.id);}
  };
  const baseUpdate=update;update=function(dt){baseUpdate(dt);if(!running||paused||modalPause)return;
    if(recall){if(!safe()||A.current().id!==recall.node||Math.hypot(game.player.x-recall.x,game.player.y-recall.y)>.2||game.player.hp<recall.hp){recall=null;toastMsg('Recall interrupted.');}else if((recall.remaining-=dt)<=0)finishRecall();}
  };
  function tile(x,y,color,alpha=1){const pts=[[x,y],[x+1,y],[x+1,y+1],[x,y+1]].map(([x,y])=>worldToScreen(x,y));ctx.save();ctx.globalAlpha=alpha;ctx.fillStyle=color;ctx.beginPath();pts.forEach((p,i)=>i?ctx.lineTo(p.x,p.y):ctx.moveTo(p.x,p.y));ctx.closePath();ctx.fill();ctx.restore();}
  const baseFloor=drawFloorDetails;drawFloorDetails=function(room,pal){if(!room.homestead)return baseFloor(room,pal);
    if(inside()){const r=C.interior(state());for(let x=r.x;x<r.x+r.w;x++)for(let y=r.y;y<r.y+r.h;y++)tile(x,y,(x+y)%2?'#75563f':'#826147');}
    else{for(const zone of [C.garden,C.orchard,C.workshop])for(let x=zone.x;x<zone.x+zone.w;x++)for(let y=zone.y;y<zone.y+zone.h;y++)tile(x,y,zone===C.garden?'#665038':zone===C.orchard?'#466a49':'#6d6954',.8);for(let x=1;x<18;x++)tile(x,7,'#aea282',.7);for(let y=7;y<14;y++)tile(9,y,'#aea282',.7);}
  };
  function labelAt(x,y,label,color='#f1ebd7',z=0){const s=worldToScreen(x,y,z);ctx.fillStyle=color;ctx.font='bold 12px system-ui';ctx.textAlign='center';ctx.fillText(label,s.x,s.y);}
  const baseDraw=drawInteractable;
  drawInteractable=function(o){if(o.type!=='homeObject')return baseDraw(o);const h=visual(),p=worldToScreen(o.x,o.y),scale=window.AWPresentation?.camera?.zoom||1;ctx.save();
    if(o.homeKind==='plot'){
      const plot=h.plots.find(p=>p.id===o.homeId);if(!plot){ctx.restore();return;}
      const copy=C.clone(plot);C.grow(copy,now());const crop=C.crops[copy.plant?.crop],wet=copy.plant?.wateredUntil>now(),progress=copy.plant?copy.plant.growthMs/copy.plant.requiredGrowthMs:0;
      tile(plot.x,plot.y,wet?'#403a29':'#876746');if(plot.tree)for(const [dx,dy]of[[1,0],[0,1],[1,1]])tile(plot.x+dx,plot.y+dy,'#556945');
      if(crop){ctx.strokeStyle='#87b979';ctx.lineWidth=plot.tree?6:3;ctx.beginPath();ctx.moveTo(p.x,p.y);ctx.lineTo(p.x,p.y-(plot.tree?30+progress*30:6+progress*22)*scale);ctx.stroke();ctx.fillStyle=crop.color;ctx.beginPath();ctx.ellipse(p.x,p.y-(plot.tree?45:18)*scale,(plot.tree?23:6+progress*5)*scale,(plot.tree?21:5+progress*3)*scale,0,0,TAU);ctx.fill();if(C.ready(copy))labelAt(o.x,o.y,'HARVEST','#ffe2a1',plot.tree?79:40);else if(!wet)labelAt(o.x,o.y,'WATER','#b7e5ff',plot.tree?79:40);}
      if(plot.soil===2){ctx.strokeStyle='#b2dbef';ctx.lineWidth=1;ctx.strokeRect(p.x-20*scale,p.y-30*scale,40*scale,32*scale);}
    }else if(o.homeKind==='house'){
      const r=C.houseRect(h);for(let x=r.x;x<r.x+r.w;x++)for(let y=r.y;y<r.y+r.h;y++)tile(x,y,h.tier?'#906e4e':'#b7a779');
      ctx.translate(p.x,p.y);ctx.scale(scale,scale);ctx.fillStyle=h.tier?'#ae845b':'#726951';ctx.fillRect(-67,-72,134,75);ctx.fillStyle='#654743';ctx.beginPath();ctx.moveTo(-80,-70);ctx.lineTo(0,-124);ctx.lineTo(80,-70);ctx.closePath();ctx.fill();ctx.fillStyle='#ffd990';ctx.fillRect(-48,-54,24,26);ctx.fillRect(25,-54,24,26);ctx.fillStyle='#302b29';ctx.fillRect(-14,-38,28,41);ctx.fillStyle='#f2e4c7';ctx.font='bold 12px system-ui';ctx.textAlign='center';ctx.fillText(h.tier?C.tiers[h.tier].name:'Cottage foundation',0,-135);
    }else if(o.homeKind==='item'){
      const i=h.items.find(i=>i.id===o.homeId);if(!i){ctx.restore();return;}const d=C.items[i.kind];ctx.translate(p.x,p.y);ctx.scale(scale,scale);
      if(d.portal){ctx.strokeStyle=i.destination?'#bdb3ff':'#797b91';ctx.lineWidth=5;ctx.beginPath();ctx.ellipse(0,-29,18,34,0,0,TAU);ctx.stroke();ctx.globalAlpha=.35+.1*Math.sin(elapsed*2);ctx.fillStyle='#9e88ff';ctx.fill();ctx.globalAlpha=1;ctx.fillStyle='#eee5ff';ctx.font='10px system-ui';ctx.textAlign='center';ctx.fillText(i.destination?.name||'Unattuned',0,-73);}
      else {const r=C.footprint(i);ctx.fillStyle=d.producer?'#9e8855':d.solid===false?'#747d91':'#8d7258';ctx.fillRect(-15*r.w,-22,30*r.w,25);ctx.fillStyle='#f9e5b7';ctx.font='20px system-ui';ctx.textAlign='center';ctx.fillText(d.icon,0,-9);if(d.producer&&i.stored>0){ctx.font='10px system-ui';ctx.fillText('READY',0,-34);}}
    }else{ctx.translate(p.x,p.y);ctx.scale(scale,scale);ctx.fillStyle=o.homeKind==='well'?'#85bdcf':o.homeKind==='return'?'#b19ddb':'#bbab7e';ctx.beginPath();ctx.ellipse(0,-10,17,12,0,0,TAU);ctx.fill();ctx.fillStyle='#f7efd4';ctx.font='bold 13px system-ui';ctx.textAlign='center';ctx.fillText(({well:'WELL',exit:'EXIT',return:'RETURN',board:'HOME'})[o.homeKind],0,-29);}
    ctx.restore();if(dist(game.player,o)<2&&o.homeKind!=='house')labelAt(o.x,o.y,o.label,'#f1ebd7',92);
  };
  const baseRender=render;render=function(){const result=baseRender();if(atHome()&&buildPreview){const {x,y,w,h,valid}=buildPreview;for(let xx=x;xx<x+w;xx++)for(let yy=y;yy<y+h;yy++)tile(xx,yy,valid?'#8ee4b4':'#ee8f9e',.55);}return result;};
  window.AWHome={HOME,state,snapshot,atHome,inside,wallet,context,action,accept,now,rebuild,safe,enterHouse,leaveHouse,startRecall,returnAdventure,travelPortal,pending:()=>pending,setPreview:p=>{buildPreview=p;},unique};
})();
