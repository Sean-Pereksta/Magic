'use strict';
/* Hosted sheets replace only visible bodies; gameplay and presentation overlays remain live. */
(() => {
  if(window.AWArtRenderer)return;
  const P=window.AWPresentation,clocks=new WeakMap();let requestedArmor=null;
  const armorNames={
    'Wayfarer Cloth':'wayfarer-cloth','Emberguard':'emberguard','Frostbound':'frostbound',
    'Verdant Oath':'verdant-oath','Nightglass':'nightglass','Sunforged':'sunforged',
    'Stormcaller':'stormcaller','Bone Regent':'bone-regent','Riftwarden':'riftwarden',
    'Crystal Vindicator':'crystal-vindicator','Wildheart Hunter':'wildheart-hunter',
    'Cinder Sovereign':'cinder-sovereign','Astral Arbiter':'astral-arbiter','Eclipse Sovereign':'eclipse-sovereign',
    'Beast Hide Armor':'wildheart-hunter','Beastmaster Cloak':'wildheart-hunter',
    'Thornkeeper Robes':'verdant-oath','Ancient Bark Plate':'verdant-oath',
    'Mirrorsteel Breastplate':'crystal-vindicator','Prismguard Armor':'crystal-vindicator',
    'Stormglass Crown':'stormcaller','Stormguard Mantle':'stormcaller','Thunderlord Mantle':'stormcaller',
    'Riftwalker Boots':'riftwarden','Meteor-Iron Mantle':'cinder-sovereign',
    'Cinderplate':'cinder-sovereign','Ashen Bulwark':'cinder-sovereign','Snowblind Hood':'frostbound',
    'Starbound Vestments':'astral-arbiter','Bloodroot Vestments':'nightglass','Voidheart Armor':'eclipse-sovereign'
  };
  const normalize=name=>String(name||'').toLowerCase().replace(/[^a-z0-9]/g,'');
  const armorAliases=new Map(Object.entries(armorNames).map(([name,id])=>[normalize(name),'armor-'+id]));
  const creatures=new Set([
    'lw_thornrunner_stalker','lw_spore_hound','lw_briar_warden','lw_lantern_wasp',
    'lw_prism_duelist','lw_thundercoil_skimmer','lw_ash_reclaimer','lw_obsidian_bulwark',
    'lw_rift_marauder','lw_veil_stalker','lw_soul_collector','lw_astral_executioner'
  ]);
  const rulerAssets={'verdant-ruler':'boss_verdant-ruler','meridian-ruler':'boss_meridian-ruler','gloam-ruler':'boss_legacy-3'};
  const bossNames=new Map([
    ['The Thornheart Sovereign','boss_verdant-ruler'],['Vaelith, Tempest Dragon','boss_meridian-ruler'],
    ['The Watching Dark','boss_legacy-3']
  ].map(([name,id])=>[normalize(name),id]));
  function armorId(gear){
    for(const name of [gear?.baseName,gear?.name]){const found=armorAliases.get(normalize(name));if(found)return found;}
    return 'armor-wayfarer-cloth';
  }
  function playerAssetId(player=game.player){return armorId(player?.armorGear);}
  function enemyAssetId(enemy,room=game.roomData){
    if(!enemy)return null;
    if(enemy.boss){
      const ruler=rulerAssets[room?.campaignNode];if(ruler)return ruler;
      const named=bossNames.get(normalize(enemy.name));if(named)return named;
      // Other bosses keep their own appearance, even when they share a creature base.
      return null;
    }
    return creatures.has(enemy.type)?enemy.type:null;
  }
  function facing(entityOrDirection){
    const direction=entityOrDirection?.facing||entityOrDirection||{},x=direction.x,y=direction.y;
    if(!Number.isFinite(x)||!Number.isFinite(y)||Math.hypot(x,y)<.0001)return 'SE';
    if(Math.abs(x)>=Math.abs(y))return x>=0?'SE':'NW';
    return y>=0?'SW':'NE';
  }
  function state(entity,pose=entity?P?.pose(entity):null){
    if(entity?.dead||entity?.hp<=0)return 'death';
    if(entity?.telegraph||/attack|cast|windup/i.test(pose?.state||''))return 'attack';
    if(/run|move|dodge|perfect/i.test(pose?.state||'')||(pose?.speed||0)>.06)return 'move';
    return 'idle';
  }
  function clock(entity,kind,pose){
    const now=typeof elapsed==='number'&&Number.isFinite(elapsed)?elapsed:0;
    // Presentation durations distinguish consecutive attacks without depending on wall time.
    const token=kind==='attack'?(entity.telegraph||pose?.until):null;
    let current=clocks.get(entity);
    if(!current||current.kind!==kind||current.token!==token||now<current.since){current={kind,token,since:now};clocks.set(entity,current);}
    let time=Math.max(0,now-current.since);
    if(kind==='attack'){
      if(entity.telegraph){
        const duration=entity.telegraph.maxTime||entity.telegraph.time||1;
        time=Math.max(time,Math.max(0,duration-(entity.stateTime||0))*.4/duration);
      }else time*=2.5; // Fit the compact windup/impact pair to the existing short attack pose.
    }
    return time;
  }
  function options(entity,scale){
    const pose=P?.pose(entity),kind=state(entity,pose);
    return {state:kind,facing:facing(entity),time:clock(entity,kind,pose),scale};
  }
  function player(p){
    const art=window.AWArt,id=playerAssetId(p);
    if(art&&(requestedArmor!==id||art.shouldRetry?.(id))){requestedArmor=id;art.ensure(id)?.catch(()=>{});}
    if(!art?.get(id))return originalPlayer(p);
    const pose=P.pose(p),screen=worldToScreen(p.x,p.y),zoom=P.camera?.zoom||1;
    const dodge=p.dodgeTime>0||pose.state==='dodge',opts=options(p,.44*zoom);
    shadowAt(p.x,p.y,dodge?23:18,dodge?.18:.38);
    const alpha=p.invuln>0?.72+.28*Math.sin(elapsed*36)**2:1;
    const drawn=art.draw(id,ctx,screen.x+(pose.recoilX||0),screen.y+(pose.recoilY||0),{...opts,alpha});
    if(!drawn)return originalPlayer(p);
    ctx.save();ctx.translate(screen.x+(pose.recoilX||0),screen.y-19*zoom+(pose.recoilY||0));ctx.scale(zoom,zoom);ctx.globalAlpha*=alpha;
    if(typeof drawAura==='function'&&p.armorGear)drawAura(p,p.armorGear);
    const legendary=[p.weapon,p.armorGear,...(p.trinkets||[])].some(item=>item?.rarity==='Legendary');
    if(legendary){
      ctx.strokeStyle='#ffda81';ctx.lineWidth=1.2;
      for(let i=0;i<3;i++){const a=elapsed*.9+i*Math.PI*2/3,x=Math.cos(a)*21,y=-7+Math.sin(a)*10;ctx.beginPath();ctx.moveTo(x,y-4);ctx.lineTo(x+3,y);ctx.lineTo(x,y+4);ctx.lineTo(x-3,y);ctx.closePath();ctx.stroke();}
    }
    if(p.shield>0){ctx.strokeStyle='rgba(164,225,255,.7)';ctx.lineWidth=1.8;ctx.beginPath();ctx.ellipse(0,-6,24,32,0,0,Math.PI*2);ctx.stroke();}
    if(pose.state==='heal'||pose.state==='levelUp'){ctx.strokeStyle=pose.state==='heal'?'#9af4b7':'#ffe39c';ctx.beginPath();ctx.arc(0,-14,30,elapsed*2,elapsed*2+4);ctx.stroke();}
    ctx.restore();
  }
  const originalPlayer=P?.entities?.player;
  if(originalPlayer)P.entities.player=player;
  if(typeof drawEnemyBody==='function'){
    const originalEnemyBody=drawEnemyBody;
    drawEnemyBody=function(enemy){
      const art=window.AWArt,id=enemyAssetId(enemy);
      if(!id||enemy.hidden||!art?.get(id))return originalEnemyBody(enemy);
      // drawEnemy already supplies depth, body size, recoil, shadows, health and telegraphs.
      ctx.save();if(enemy.flash>0)ctx.filter='brightness(1.8)';
      const drawn=art.draw(id,ctx,0,12,options(enemy,.34));ctx.restore();
      if(!drawn)return originalEnemyBody(enemy);
    };
  }
  if(P?.fx?.deaths&&typeof P.event==='function'&&typeof P.fx.drawFront==='function'){
    const pool=P.fx.deaths,baseAdd=pool.add,baseEvent=P.event,baseFront=P.fx.drawFront;
    let dying=null;
    P.event=function(kind,data={}){
      const previous=dying,enemy=kind==='death'&&data.entity,id=enemy&&enemyAssetId(enemy);
      dying=id&&window.AWArt?.get(id)?{id,facing:facing(enemy),scale:Number.isFinite(enemy.r)&&enemy.r>0?enemy.r:.34}:null;
      try{return baseEvent.call(this,kind,data);}finally{dying=previous;}
    };
    pool.add=function(data){
      const item=baseAdd.call(this,data);
      if(dying)item.hostedDeath=dying;
      return item;
    };
    P.fx.drawFront=function(...args){
      baseFront.apply(this,args);
      // Reuse the bounded presentation pool, including its pause, expiry and room-reset rules.
      for(const death of pool.items){
        const sprite=death.hostedDeath;if(death.life<=0||!sprite)continue;
        const screen=worldToScreen(death.x,death.y);
        window.AWArt?.draw(sprite.id,ctx,screen.x,screen.y,{state:'death',facing:sprite.facing,time:Math.max(0,death.maxLife-death.life),scale:sprite.scale,alpha:Math.min(1,death.life/.2)});
      }
    };
  }
  window.AWArtRenderer={armorId,playerAssetId,enemyAssetId,facing,state};
})();
