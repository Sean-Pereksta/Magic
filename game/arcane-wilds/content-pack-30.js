'use strict';

/*
 * Arcane Wilds — 30-piece content pack
 * 10 enemies, 10 new abilities, 10 named items with bespoke mechanics/visuals.
 * Loaded after ability-visuals.js so it can extend the current combat and render layers.
 */
(function(){
  if(window.__arcaneWildsContentPack30Loaded)return;
  window.__arcaneWildsContentPack30Loaded=true;
  if(typeof SPELLS==='undefined'||typeof SPELL_CASTS==='undefined'||typeof ENEMY_TYPES==='undefined')return;

  const NEW_SPELLS={
    voidGuillotine:{name:'Void Guillotine',icon:'🗡️',rarity:'Rare',desc:'Open a towering tear in space that snaps shut across a wide line.',cooldown:6.8,damage:58,cast:'voidGuillotine',upgradeBase:'quake'},
    thunderCathedral:{name:'Thunder Cathedral',icon:'⛪',rarity:'Epic',desc:'Raise four lightning pillars that arc repeatedly through everything trapped between them.',cooldown:10.5,damage:28,cast:'thunderCathedral',upgradeBase:'tempest'},
    dragonfireTorrent:{name:'Dragonfire Torrent',icon:'🐉',rarity:'Rare',desc:'Channel a thick rolling cone of dragonfire that can sweep as you aim.',cooldown:7.2,damage:15,cast:'dragonfireTorrent',upgradeBase:'gust'},
    crystalRailway:{name:'Crystal Railway',icon:'💎',rarity:'Rare',desc:'Rip a sequential railway of giant ice crystals across the battlefield.',cooldown:7.5,damage:34,cast:'crystalRailway',upgradeBase:'icelance'},
    moonfall:{name:'Moonfall',icon:'🌕',rarity:'Epic',desc:'Drag a luminous moon down onto a marked area in a crushing silver impact.',cooldown:12.5,damage:86,cast:'moonfall',upgradeBase:'meteor'},
    spiritStampede:{name:'Spirit Stampede',icon:'🐺',rarity:'Epic',desc:'Send waves of spectral beasts charging from one edge of the room to the other.',cooldown:10.2,damage:30,cast:'spiritStampede',upgradeBase:'phoenix'},
    worldroot:{name:'Worldroot',icon:'🌳',rarity:'Rare',desc:'Branch living roots toward nearby enemies, damaging and pinning them in place.',cooldown:8.2,damage:36,cast:'worldroot',upgradeBase:'thorns'},
    starbreaker:{name:'Starbreaker',icon:'🌟',rarity:'Epic',desc:'Launch a miniature star that detonates into a violent geometric starburst.',cooldown:9.8,damage:62,cast:'starbreaker',upgradeBase:'firebolt'},
    arcaneRailgun:{name:'Arcane Railgun',icon:'💥',rarity:'Legendary',desc:'Flash an aiming line, then fire an impossibly fast room-spanning arcane shot.',cooldown:14,damage:105,cast:'arcaneRailgun',upgradeBase:'icelance'},
    runicBarrage:{name:'Runic Barrage',icon:'🔯',rarity:'Rare',desc:'Conjure six floating runes that fire synchronized piercing rays outward.',cooldown:7.6,damage:24,cast:'runicBarrage',upgradeBase:'arcaneMissiles'}
  };

  function cloneUpgrades(id,source){
    const pool=UPGRADE_POOLS[source]||[];
    UPGRADE_POOLS[id]=pool.map((u,index)=>[`${id}_${index+1}`,u[1],u[2],u[3],u[4]]);
  }
  for(const [id,spell] of Object.entries(NEW_SPELLS)){
    const {upgradeBase,...data}=spell;
    SPELLS[id]=data;
    cloneUpgrades(id,upgradeBase);
  }

  function pushEffect(kind,x,y,life,color,extra={}){
    game.effects.push({kind,x,y,life,maxLife:life,color,...extra});
  }
  function safeLater(ms,fn){
    const key=game.roomData?.key;
    setTimeout(()=>{if(running&&game.roomData?.key===key&&game.player)fn()},ms);
  }
  function rayToEdge(x,y,d,pad=.42){
    const c=[];
    if(d.x>0)c.push((ROOM_W-pad-x)/d.x);if(d.x<0)c.push((pad-x)/d.x);
    if(d.y>0)c.push((ROOM_H-pad-y)/d.y);if(d.y<0)c.push((pad-y)/d.y);
    const travel=Math.max(.1,Math.min(...c.filter(v=>Number.isFinite(v)&&v>0)));
    return {x:x+d.x*travel,y:y+d.y*travel,travel};
  }
  function rayAcrossRoom(cx,cy,d){
    const a=rayToEdge(cx,cy,{x:-d.x,y:-d.y}),b=rayToEdge(cx,cy,d);
    return {ax:a.x,ay:a.y,bx:b.x,by:b.y};
  }
  function segmentInfo(px,py,ax,ay,bx,by){
    const vx=bx-ax,vy=by-ay,len2=vx*vx+vy*vy||1;
    const q=clamp(((px-ax)*vx+(py-ay)*vy)/len2,0,1);
    const x=ax+vx*q,y=ay+vy*q;
    return {q,x,y,d:Math.hypot(px-x,py-y)};
  }
  function lineDamage(ax,ay,bx,by,width,damage,tag='arcane',knockDir=null){
    for(const e of [...game.enemies]){
      if(e.dead)continue;
      const h=segmentInfo(e.x,e.y,ax,ay,bx,by);
      if(h.d>width+e.r)continue;
      damageEnemy(e,damage,tag);
      if(knockDir&&!e.dead){e.x=clamp(e.x+knockDir.x*1.25,.4,ROOM_W-.4);e.y=clamp(e.y+knockDir.y*1.25,.4,ROOM_H-.4)}
    }
  }
  function coneDamage(origin,d,range,halfAngle,damage,tag='fire'){
    const cos=Math.cos(halfAngle);
    for(const e of [...game.enemies]){
      if(e.dead)continue;
      const dx=e.x-origin.x,dy=e.y-origin.y,dd=Math.hypot(dx,dy);
      if(dd>range+e.r||dd<.01)continue;
      const dot=(dx/dd)*d.x+(dy/dd)*d.y;
      if(dot>=cos)damageEnemy(e,damage,tag);
    }
  }

  function castVoidGuillotine(id,s,m){
    const d=spellAim(),pt=aimPoint(4.4),perp={x:-d.y,y:d.x},span=3.25;
    pushEffect('voidGuillotine',pt.x,pt.y,.82,'#bd70ff',{dir:perp,span});
    fx('castRing',pt.x,pt.y,.35,'#9a56e8',{r:1.25});
    safeLater(420,()=>{
      const ax=pt.x-perp.x*span,ay=pt.y-perp.y*span,bx=pt.x+perp.x*span,by=pt.y+perp.y*span;
      lineDamage(ax,ay,bx,by,.42,s.damage*m.power,'soul');
      pushEffect('voidSnap',pt.x,pt.y,.32,'#e7b3ff',{dir:perp,span});
      shake=Math.max(shake,7);
    });
  }

  function castThunderCathedral(id,s,m){
    const pt=aimPoint(4),r=2.05;
    pushEffect('thunderCathedral',pt.x,pt.y,1.42,'#8ff3ff',{r});
    for(const delay of [250,500,750,1000])safeLater(delay,()=>{
      radialDamage(pt.x,pt.y,r,s.damage*.55*m.power,'lightning');
      for(let i=0;i<4;i++){
        const a=i*TAU/4+Math.PI/4,b=(i+1)*TAU/4+Math.PI/4;
        fx('lightning',pt.x+Math.cos(a)*r,pt.y+Math.sin(a)*r,.17,'#a8f7ff',{toX:pt.x+Math.cos(b)*r,toY:pt.y+Math.sin(b)*r,width:2.5});
      }
      burst(pt.x,pt.y,'#9ef5ff',7,.55,12);
    });
  }

  function castDragonfireTorrent(id,s,m){
    for(let i=0;i<6;i++)safeLater(i*135,()=>{
      const p=game.player,d=spellAim();p.facing=d;
      coneDamage(p,d,6.2,.52,s.damage*.42*m.power,'fire');
      pushEffect('dragonfireTorrent',p.x,p.y,.24,'#ff7b3e',{dir:d,range:6.3});
      if(i===0||i===5)burst(p.x+d.x*1.1,p.y+d.y*1.1,'#ffb357',9,1.0,12);
    });
  }

  function castCrystalRailway(id,s,m){
    const p=game.player,d=spellAim();
    for(let i=1;i<=8;i++)safeLater(i*82,()=>{
      const travel=.75+i*.82,x=clamp(p.x+d.x*travel,.5,ROOM_W-.5),y=clamp(p.y+d.y*travel,.5,ROOM_H-.5);
      radialDamage(x,y,.62,s.damage*.52*m.power,'frost');
      for(const e of game.enemies)if(!e.dead&&Math.hypot(e.x-x,e.y-y)<.82+e.r)e.slow=Math.max(e.slow||0,1.7);
      pushEffect('crystalSpike',x,y,.72,'#c4f4ff',{index:i});
    });
    fx('castLine',p.x,p.y,.28,'#b9edff',{dir:d,range:6.6});
  }

  function castMoonfall(id,s,m){
    const pt=aimPoint(5);
    telegraph('circle',pt.x,pt.y,2.05,.72,'#dce8ff',{pulse:true});
    pushEffect('moonfall',pt.x,pt.y,1.02,'#eef3ff',{r:2.05});
    safeLater(720,()=>{
      radialDamage(pt.x,pt.y,2.18,s.damage*m.power,'arcane',.5);
      pushEffect('moonImpact',pt.x,pt.y,.65,'#dbe6ff',{r:2.35});
      burst(pt.x,pt.y,'#f2f6ff',28,2,16);shake=Math.max(shake,10);
    });
  }

  function castSpiritStampede(id,s,m){
    const p=game.player,d=spellAim(),perp={x:-d.y,y:d.x};
    [-1.1,0,1.1].forEach((lane,index)=>safeLater(index*225,()=>{
      const cx=clamp(p.x+perp.x*lane,.6,ROOM_W-.6),cy=clamp(p.y+perp.y*lane,.6,ROOM_H-.6),line=rayAcrossRoom(cx,cy,d);
      lineDamage(line.ax,line.ay,line.bx,line.by,.52,s.damage*.72*m.power,'soul');
      pushEffect('spiritStampede',cx,cy,.92,'#c9f4ff',{...line,dir:d,lane:index});
    }));
  }

  function castWorldroot(id,s,m){
    const p=game.player,targets=[...game.enemies].filter(e=>!e.dead).sort((a,b)=>dist(a,p)-dist(b,p)).slice(0,5);
    if(targets.length){
      targets.forEach((e,index)=>safeLater(index*70,()=>{
        if(e.dead)return;damageEnemy(e,s.damage*m.power,'nature');e.stun=Math.max(e.stun||0,.7);e.slow=Math.max(e.slow||0,2.4);
        pushEffect('worldrootBranch',p.x,p.y,.78,'#82d477',{toX:e.x,toY:e.y,branch:index});
        burst(e.x,e.y,'#9be487',7,.5,5);
      }));
    }else{
      for(let i=0;i<4;i++){const a=i*TAU/4;pushEffect('worldrootBranch',p.x,p.y,.7,'#82d477',{toX:p.x+Math.cos(a)*3.6,toY:p.y+Math.sin(a)*3.6,branch:i})}
    }
  }

  function detonateStarbreaker(q,s,m){
    if(!q||q._starDone)return;q._starDone=true;q.life=0;
    const x=clamp(q.x,.45,ROOM_W-.45),y=clamp(q.y,.45,ROOM_H-.45);
    radialDamage(x,y,1.72,s.damage*m.power,'arcane');
    pushEffect('starbreakerBurst',x,y,.62,'#fff0a5',{r:1.8});
    for(let i=0;i<12;i++){const a=i*TAU/12;magicProjectile({x,y,z:12,vx:Math.cos(a)*7.8,vy:Math.sin(a)*7.8,r:.08,damage:s.damage*.18*m.power,color:'#ffe88e',color2:'#fff',kind:'arcane',life:.62,pierce:1,trail:''})}
    shake=Math.max(shake,7);
  }
  function castStarbreaker(id,s,m){
    const d=spellAim(),q=magicProjectile({vx:d.x*3.8,vy:d.y*3.8,r:.3,damage:s.damage*.25*m.power,color:'#fff1a0',color2:'#ffffff',kind:'starbreaker',life:1.35,pierce:20,trail:'spark',onHit:(e,p)=>detonateStarbreaker(p,s,m)});
    safeLater(1180,()=>detonateStarbreaker(q,s,m));
    pushEffect('starbreakerCast',game.player.x,game.player.y,.42,'#ffe590',{dir:d});
  }

  function castArcaneRailgun(id,s,m){
    const p=game.player,d=spellAim(),end=rayToEdge(p.x,p.y,d,.32);
    pushEffect('railAim',p.x,p.y,.22,'#d69cff',{toX:end.x,toY:end.y});
    safeLater(165,()=>{
      lineDamage(p.x,p.y,end.x,end.y,.28,s.damage*m.power,'arcane',d);
      pushEffect('railBeam',p.x,p.y,.3,'#e8c5ff',{toX:end.x,toY:end.y});
      burst(p.x,p.y,'#e1b0ff',14,1.2,18);burst(end.x,end.y,'#c780ff',12,.9,10);shake=Math.max(shake,10);
    });
  }

  function castRunicBarrage(id,s,m){
    const p=game.player,d=spellAim(),base=Math.atan2(d.y,d.x);
    pushEffect('runicBarrage',p.x,p.y,.9,'#c7a6ff',{base});
    safeLater(310,()=>{
      for(let i=0;i<6;i++){
        const orbitA=i*TAU/6,shotA=base+(i-2.5)*.17,x=p.x+Math.cos(orbitA)*.72,y=p.y+Math.sin(orbitA)*.72;
        magicProjectile({x,y,z:20,vx:Math.cos(shotA)*10.5,vy:Math.sin(shotA)*10.5,r:.09,damage:s.damage*.7*m.power,color:'#c8a9ff',color2:'#fff',kind:'arcane',life:1.05,pierce:3,trail:''});
        pushEffect('runeRay',x,y,.25,'#d8c2ff',{dir:{x:Math.cos(shotA),y:Math.sin(shotA)},range:4});
      }
    });
  }

  Object.assign(SPELL_CASTS,{voidGuillotine:castVoidGuillotine,thunderCathedral:castThunderCathedral,dragonfireTorrent:castDragonfireTorrent,crystalRailway:castCrystalRailway,moonfall:castMoonfall,spiritStampede:castSpiritStampede,worldroot:castWorldroot,starbreaker:castStarbreaker,arcaneRailgun:castArcaneRailgun,runicBarrage:castRunicBarrage});

  /* ---------- 10 new enemies ---------- */
  Object.assign(ENEMY_TYPES,{
    aw30_sunmoth:{name:'Sun-Eater Moth',icon:'✺',biomes:['desert','meadow','volcanic'],min:6,hp:72,speed:1.55,damage:15,r:.37,ai:'aw30_sunmoth',color:'#38312d',proj:'#ffd66f',xp:33},
    aw30_mimic:{name:'Gravestone Mimic',icon:'▥',biomes:['crypt','ruins'],min:5,hp:104,speed:1.1,damage:18,r:.42,ai:'aw30_mimic',color:'#65636d',proj:'#b990ff',xp:36},
    aw30_cinderwheel:{name:'Cinderwheel Imp',icon:'◉',biomes:['volcanic','desert'],min:5,hp:54,speed:1.8,damage:14,r:.31,ai:'aw30_cinderwheel',color:'#9a4936',proj:'#ff7a45',xp:28},
    aw30_mirrorknight:{name:'Mirror Knight',icon:'◇',biomes:['ruins','frost'],min:8,hp:138,speed:1.05,damage:20,r:.43,ai:'aw30_mirrorknight',color:'#74808e',proj:'#d8eeff',xp:48},
    aw30_boglantern:{name:'Bog Lantern',icon:'◌',biomes:['swamp'],min:4,hp:62,speed:1.25,damage:13,r:.34,ai:'aw30_boglantern',color:'#77944e',proj:'#c5f27d',xp:29},
    aw30_stormram:{name:'Stormback Ram',icon:'♈',biomes:['frost','meadow'],min:8,hp:158,speed:1.18,damage:22,r:.5,ai:'aw30_stormram',color:'#516a78',proj:'#94ebff',xp:50},
    aw30_puppeteer:{name:'Rift Puppeteer',icon:'⌘',biomes:['crypt','ruins'],min:10,hp:98,speed:.95,damage:14,r:.36,ai:'aw30_puppeteer',color:'#65468b',proj:'#c779ff',xp:49},
    aw30_burrower:{name:'Shardburrower',icon:'◆',biomes:['frost','desert','ruins'],min:7,hp:88,speed:1.5,damage:19,r:.37,ai:'aw30_burrower',color:'#6697a7',proj:'#b9f3ff',xp:42},
    aw30_ashchoir:{name:'Ash Choir',icon:'☷',biomes:['crypt','volcanic'],min:9,hp:118,speed:1.12,damage:13,r:.41,ai:'aw30_ashchoir',color:'#b7aea4',proj:'#ff8b72',xp:47},
    aw30_behemoth:{name:'Verdant Behemoth',icon:'♣',biomes:['forest','swamp'],min:11,hp:240,speed:.62,damage:25,r:.62,ai:'aw30_behemoth',color:'#4f7653',proj:'#9cdb75',xp:68}
  });

  const baseSpawnEnemy=spawnEnemy;
  spawnEnemy=function(id,pos,elite=false,scaleOverride=1){
    const e=baseSpawnEnemy(id,pos,elite,scaleOverride);
    if(id==='aw30_mimic'){e.awDormant=true;e.state='dormant'}
    if(id==='aw30_cinderwheel')e.awRollDir=norm(rnd(1,-1),rnd(1,-1));
    if(id==='aw30_ashchoir')e.awMasks=5;
    if(id==='aw30_behemoth')e.awBlooms=0;
    return e;
  };

  function awSunMoth(e,d,range,speed,dt){
    const tang={x:-d.y,y:d.x};moveEnemy(e,norm(tang.x*.85+d.x*.18,tang.y*.85+d.y*.18),speed,dt);
    if(e.state==='sunCharge'){
      if(e.stateTime<=0){enemyFan(e,7,1.2,5.2,'arcaneEnemy',{damage:e.damage*1.1,color:'#ffd66f'});pushEffect('sunMothBurst',e.x,e.y,.55,'#ffd56b',{r:1.4});e.state='idle';e.telegraph=null;e.attack=2.8}
    }else if(e.attack<=0){
      const nearby=game.effects.filter(q=>Math.hypot((q.x||0)-e.x,(q.y||0)-e.y)<2.6).length;
      startEnemyTelegraph(e,'sunCharge',Math.max(.55,.95-nearby*.04),{r:1.25});e.attack=3.4;
    }
  }
  function awMimic(e,d,range,speed,dt){
    if(e.awDormant){if(range<2.55){e.awDormant=false;e.state='idle';burst(e.x,e.y,'#b898ff',16,.9);pushEffect('mimicWake',e.x,e.y,.7,'#a978e8',{r:1.2})}return}
    moveEnemy(e,d,speed,dt);
    if(e.attack<=0){
      const perp={x:-d.y,y:d.x};for(const off of [-.9,0,.9]){const x=e.x+d.x*1.1+perp.x*off,y=e.y+d.y*1.1+perp.y*off;game.effects.push({kind:'enemyVoidWell',x,y,life:1.4,maxLife:1.4,color:'#9d65db',damage:e.damage*.28,r:.58,tick:.35})}
      if(range<1.2)damagePlayer(e.damage);e.attack=2.4;
    }
  }
  function awCinderwheel(e,d,range,speed,dt){
    if(e.state==='rolling'){
      const q=e.awRollDir||d;e.x+=q.x*speed*4.1*dt;e.y+=q.y*speed*4.1*dt;
      let bounced=false;if(e.x<.45||e.x>ROOM_W-.45){q.x*=-1;bounced=true}if(e.y<.45||e.y>ROOM_H-.45){q.y*=-1;bounced=true}keepInRoom(e);
      if(Math.random()<dt*8)groundEffect('fire',e.x,e.y,.38,1.45,'#ff6f3f',e.damage*.16,.5);
      if(range<e.r+game.player.r+.18&&!e.awHit){damagePlayer(e.damage*1.35);e.awHit=true}
      if(bounced)pushEffect('cinderRicochet',e.x,e.y,.25,'#ff9a57',{r:.7});
      if(e.stateTime<=0){e.state='idle';e.awHit=false;e.attack=2.15}
    }else{moveEnemy(e,d,speed*.7,dt);if(e.attack<=0){e.state='rolling';e.stateTime=1.05;e.awRollDir=norm(d.x+rnd(.3,-.3),d.y+rnd(.3,-.3));e.attack=3}}
  }
  function awMirrorKnight(e,d,range,speed,dt){
    moveEnemy(e,d,speed,dt);
    if(e.attack<=0){
      if(range<1.35){damagePlayer(e.damage);fx('slash',e.x,e.y,.25,'#e6f4ff',{dir:d})}
      for(const off of [-1,1])pushEffect('mirrorClone',e.x+(-d.y)*off*.72,e.y+d.x*off*.72,.72,'#d5efff',{dir:d});
      e.attack=2.15;
    }
  }
  function awBogLantern(e,d,range,speed,dt){
    const tang={x:-d.y,y:d.x};moveEnemy(e,norm(tang.x*.8-d.x*.18,tang.y*.8-d.y*.18),speed,dt);
    if(e.attack<=0){
      const pt={x:clamp(game.player.x+rnd(1,-1),.6,ROOM_W-.6),y:clamp(game.player.y+rnd(1,-1),.6,ROOM_H-.6)};
      pushEffect('bogBubble',pt.x,pt.y,.95,'#b9e875',{r:.8});telegraph('circle',pt.x,pt.y,.82,.88,'#b9e875',{pulse:true});
      safeLater(880,()=>{radialPlayerThreat(pt.x,pt.y,1.05,e.damage*1.15);game.hazards.push({kind:'poison',x:pt.x,y:pt.y,r:.85,life:2.3,maxLife:2.3,color:'#8bd05f',damage:0,interval:.45,tick:0});burst(pt.x,pt.y,'#b9e875',12,.8)});e.attack=2.45;
    }
  }
  function awStormRam(e,d,range,speed,dt){
    if(e.state==='ramCharge'){
      const q=e.awChargeDir||d;e.x+=q.x*speed*4.7*dt;e.y+=q.y*speed*4.7*dt;
      let wall=false;if(e.x<.46||e.x>ROOM_W-.46||e.y<.46||e.y>ROOM_H-.46)wall=true;keepInRoom(e);
      if(range<e.r+game.player.r+.18&&!e.awHit){damagePlayer(e.damage*1.55);e.awHit=true}
      if(wall||e.stateTime<=0){for(let i=0;i<6;i++){const a=i*TAU/6;enemyProjectile(e,{x:Math.cos(a),y:Math.sin(a)},5.2,'runeBolt',{r:.08,damage:e.damage*.55,color:'#9ceeff'})}pushEffect('stormRamCrash',e.x,e.y,.55,'#9eefff',{r:1.5});e.state='idle';e.awHit=false;e.attack=2.6;shake=Math.max(shake,6)}
    }else{moveEnemy(e,d,speed,dt);if(e.attack<=0&&range<8){startEnemyTelegraph(e,'ramWind',.68,{dir:d,range:8});e.awChargeDir={...d};e.attack=3.1}if(e.state==='ramWind'&&e.stateTime<=0){e.state='ramCharge';e.stateTime=.85;e.telegraph=null}}
  }
  function awPuppeteer(e,d,range,speed,dt){
    kite(e,d,range,speed,dt,5.2,7.5);
    if(e.attack<=0){
      const allies=game.enemies.filter(o=>o!==e&&!o.dead).sort((a,b)=>dist(a,e)-dist(b,e)).slice(0,3);
      for(const o of allies){o.awPuppetUntil=elapsed+2.4;pushEffect('puppetString',e.x,e.y,.8,'#cd83ff',{toX:o.x,toY:o.y})}
      if(!allies.length)enemyFan(e,3,.5,4.6,'arcaneEnemy');e.attack=3.2;
    }
  }
  function awBurrower(e,d,range,speed,dt){
    if(e.state==='burrow'){
      e.x=clamp(e.x+d.x*speed*2.5*dt,.4,ROOM_W-.4);e.y=clamp(e.y+d.y*speed*2.5*dt,.4,ROOM_H-.4);pushEffect('burrowRidge',e.x,e.y,.18,'#aeeeff',{dir:d});
      if(e.stateTime<=0){e.state='erupt';e.stateTime=.18;radialPlayerThreat(e.x,e.y,1.15,e.damage*1.35);pushEffect('shardErupt',e.x,e.y,.65,'#baf4ff',{r:1.2});for(let i=0;i<8;i++){const a=i*TAU/8;enemyProjectile(e,{x:Math.cos(a),y:Math.sin(a)},4.8,'glassDart',{r:.07,damage:e.damage*.45,color:'#baf4ff'})}}
    }else if(e.state==='erupt'){if(e.stateTime<=0){e.state='idle';e.attack=2.3}}
    else{moveEnemy(e,d,speed*.65,dt);if(e.attack<=0){e.state='burrow';e.stateTime=.9;e.attack=2.9}}
  }
  function awAshChoir(e,d,range,speed,dt){
    const pct=e.hp/e.maxHp;e.awMasks=Math.max(1,Math.ceil(pct*5));const frenzy=1+(5-e.awMasks)*.12;
    kite(e,d,range,speed*frenzy,dt,4.7,7.2);
    if(e.attack<=0){enemyFan(e,e.awMasks===1?7:Math.min(5,e.awMasks+1),e.awMasks===1?1.25:.7,4.4+(5-e.awMasks)*.22,'arcaneEnemy',{damage:e.damage*frenzy,color:'#ff9b82'});pushEffect('ashChoirPulse',e.x,e.y,.45,'#d9cbc1',{masks:e.awMasks});e.attack=Math.max(.78,2.1-(5-e.awMasks)*.25)}
  }
  function awBehemoth(e,d,range,speed,dt){
    moveEnemy(e,d,speed,dt);
    if(e.awBloomResolve&&elapsed>=e.awBloomResolve){if(e.awBlooms>0){const heal=e.maxHp*.035*e.awBlooms;e.hp=Math.min(e.maxHp,e.hp+heal);floatText(e.x,e.y,`+${Math.round(heal)}`,'#9ce58d')}e.awBlooms=0;e.awBloomResolve=0}
    if(e.attack<=0){
      if(Math.random()<.52){e.awBlooms=3;e.awBloomResolve=elapsed+2.5;pushEffect('verdantBloom',e.x,e.y,2.5,'#a7e681',{r:1.4})}
      else{const perp={x:-d.y,y:d.x};for(const off of [-1,0,1]){const a=norm(d.x+perp.x*off*.28,d.y+perp.y*off*.28);for(let q=1;q<=4;q++){const x=e.x+a.x*q*1.05,y=e.y+a.y*q*1.05;safeLater(q*90,()=>{radialPlayerThreat(x,y,.58,e.damage*.46);pushEffect('rootSpike',x,y,.45,'#91ce73',{r:.55})})}}}
      e.attack=3.7;
    }
  }

  const baseUpdateEnemyAI=updateEnemyAI;
  updateEnemyAI=function(e,d,range,speed,dt){
    const boosted=e.awPuppetUntil>elapsed;
    const baseDamage=e.damage;if(boosted){speed*=1.24;e.damage=baseDamage*1.2}
    try{
      if(e.ai==='aw30_sunmoth')return awSunMoth(e,d,range,speed,dt);
      if(e.ai==='aw30_mimic')return awMimic(e,d,range,speed,dt);
      if(e.ai==='aw30_cinderwheel')return awCinderwheel(e,d,range,speed,dt);
      if(e.ai==='aw30_mirrorknight')return awMirrorKnight(e,d,range,speed,dt);
      if(e.ai==='aw30_boglantern')return awBogLantern(e,d,range,speed,dt);
      if(e.ai==='aw30_stormram')return awStormRam(e,d,range,speed,dt);
      if(e.ai==='aw30_puppeteer')return awPuppeteer(e,d,range,speed,dt);
      if(e.ai==='aw30_burrower')return awBurrower(e,d,range,speed,dt);
      if(e.ai==='aw30_ashchoir')return awAshChoir(e,d,range,speed,dt);
      if(e.ai==='aw30_behemoth')return awBehemoth(e,d,range,speed,dt);
      return baseUpdateEnemyAI(e,d,range,speed,dt);
    }finally{e.damage=baseDamage}
  };

  /* ---------- 10 named items ---------- */
  const NEW_WEAPONS=[
    {name:'Worldroot Staff',icon:'🌿',type:'staff',damage:1.12,attack:1.02,range:9.4,speed:9.7,color:'#7bd88a',special:'worldrootStaff',desc:'Nature and poison ground effects last longer.'},
    {name:'Frostbite Fang',icon:'🗡️',type:'blade',damage:1.24,attack:1.42,range:5.8,speed:9.8,color:'#a8edff',special:'frostbiteFang',desc:'Every fifth weapon attack launches an ice spike.'},
    {name:'Graviton Scepter',icon:'🪄',type:'scepter',damage:1.06,attack:1.16,range:8.7,speed:9.4,color:'#b37cff',special:'gravitonScepter',desc:'Weapon impacts tug nearby enemies toward the hit point.'}
  ];
  const NEW_ARMOR=[
    {name:'Mirrorsteel Breastplate',icon:'🪞',hp:1.18,move:1,armor:.10,color:'#657786',trim:'#e5f7ff',helm:'crest',aura:'frost',special:'mirrorsteel',desc:'Every 12 seconds, the next enemy projectile is reflected.'},
    {name:'Stormglass Crown',icon:'👑',hp:1.1,move:1.08,armor:.06,color:'#386a7d',trim:'#9df1ff',helm:'crown',aura:'storm',special:'stormglassCrown',desc:'Lightning damage can fork one extra time.'},
    {name:'Riftwalker Boots',icon:'🥾',hp:1.05,move:1.14,armor:.04,color:'#51456f',trim:'#c69bff',helm:'veil',aura:'void',special:'riftwalker',desc:'Dodging tears damaging portals at the start and end of the dash.'},
    {name:'Meteor-Iron Mantle',icon:'🛡️',hp:1.32,move:.93,armor:.15,color:'#62514d',trim:'#ff9a67',helm:'horns',aura:'ember',special:'meteorMantle',desc:'Heavy hits store force that erupts on your next damaging spell.'}
  ];
  const NEW_TRINKETS=[
    {name:'Sunforged Gauntlet',icon:'🧤',mods:{spell:.05,cdr:.02},special:'sunforgedGauntlet',desc:'Solar Beam becomes wider and erupts around enemies it destroys.'},
    {name:'Cinderheart',icon:'❤️‍🔥',mods:{spell:.06},special:'cinderheart',desc:'Fire damage builds Heat; at maximum Heat your next damaging spell detonates a fireburst.'},
    {name:'Choir Bell',icon:'🔔',mods:{spell:.035,cdr:.035},special:'choirBell',desc:'Spirit summons gain an extra wisp and arrive with a spectral pulse.'}
  ];
  for(const item of NEW_WEAPONS)if(!WEAPON_BASES.some(x=>x.name===item.name))WEAPON_BASES.push(item);
  for(const item of NEW_ARMOR)if(!ARMOR_SETS.some(x=>x.name===item.name))ARMOR_SETS.push(item);
  if(typeof TRINKET_BASES!=='undefined')for(const item of NEW_TRINKETS)if(!TRINKET_BASES.some(x=>x.name===item.name))TRINKET_BASES.push(item);

  function equippedSpecial(key){
    const p=game.player;if(!p)return false;
    return [p.weapon,p.armorGear,...(p.trinkets||[])].some(i=>i?.special===key);
  }

  const baseGroundEffect=groundEffect;
  groundEffect=function(kind,x,y,r,life,color,damage=0,interval=.35,extra={}){
    if(equippedSpecial('worldrootStaff')&&['poison','thorns','iceShard'].includes(kind))life*=1.32;
    return baseGroundEffect(kind,x,y,r,life,color,damage,interval,extra);
  };

  const baseMagicProjectile=magicProjectile;
  magicProjectile=function(opts={}){
    if(opts.trail==='weapon'&&equippedSpecial('gravitonScepter')){
      const old=opts.onHit;
      opts={...opts,onHit:(e,q)=>{
        if(old)old(e,q);
        for(const o of game.enemies){if(o.dead||o===e)continue;const dd=Math.hypot(o.x-q.x,o.y-q.y);if(dd>2.5||dd<.05)continue;o.x+=((q.x-o.x)/dd)*.38;o.y+=((q.y-o.y)/dd)*.38}
        pushEffect('gravityRipple',q.x,q.y,.34,'#b878ff',{r:1.1});
      }};
    }
    const q=baseMagicProjectile(opts);
    if(opts.trail==='weapon'&&equippedSpecial('frostbiteFang')){
      const p=game.player;p._frostbiteCount=(p._frostbiteCount||0)+1;
      if(p._frostbiteCount%5===0){const d=norm(opts.vx||p.facing.x,opts.vy||p.facing.y);baseMagicProjectile({x:p.x,y:p.y,z:17,vx:d.x*11,vy:d.y*11,r:.1,damage:weaponDamage()*.58,color:'#b9f1ff',color2:'#fff',kind:'iceLance',life:.95,pierce:3,trail:''});pushEffect('frostbiteProc',p.x,p.y,.35,'#c9f6ff',{dir:d})}
    }
    return q;
  };

  const baseUpdateProjectiles=updateProjectiles;
  updateProjectiles=function(dt){
    const p=game.player;
    if(p&&equippedSpecial('mirrorsteel')&&elapsed>=(p._mirrorsteelReadyAt||0)){
      const q=game.projectiles.find(o=>o.owner==='enemy'&&Math.hypot(o.x-p.x,o.y-p.y)<o.r+p.r+.12);
      if(q){q.owner='player';q.vx*=-1.18;q.vy*=-1.18;q.damage*=1.15;q.hit=new Set();q.tag='arcane';q.color='#dff8ff';q.trail='spark';p._mirrorsteelReadyAt=elapsed+12;pushEffect('mirrorFlash',p.x,p.y,.45,'#e2f7ff',{r:1.1})}
    }
    baseUpdateProjectiles(dt);
  };

  const baseDamageEnemy=damageEnemy;
  damageEnemy=function(e,amount,tag='',dot=false){
    if(e?.type==='aw30_burrower'&&e.state==='burrow'){if(!dot)floatText(e.x,e.y,'PHASED','#bfefff');return}
    if(e?.type==='aw30_mirrorknight'&&!e.dead){
      const toPlayer=norm(game.player.x-e.x,game.player.y-e.y),face=e.facing||toPlayer;
      if(toPlayer.x*face.x+toPlayer.y*face.y>.3){amount*=.42;if(!dot)pushEffect('mirrorBlock',e.x,e.y,.25,'#e5f7ff',{r:.8})}
    }
    if(e?.type==='aw30_behemoth'&&e.awBlooms>0&&!dot){e.awBlooms=Math.max(0,e.awBlooms-1);pushEffect('bloomBreak',e.x,e.y,.28,'#c9f4a3',{r:.7})}
    baseDamageEnemy(e,amount,tag,dot);
    if(tag==='lightning'&&equippedSpecial('stormglassCrown')&&Math.random()<.22){const t=nearestEnemy(e,4,o=>o!==e&&!o.dead);if(t){fx('lightning',e.x,e.y,.17,'#b9f7ff',{toX:t.x,toY:t.y,width:2});baseDamageEnemy(t,amount*.38,'stormglassFork',dot)}}
    if(tag==='fire'&&equippedSpecial('cinderheart')&&game.player){const p=game.player;p._cinderHeat=clamp((p._cinderHeat||0)+.5,0,8);if(p._cinderHeat>=8&&!p._cinderReady){p._cinderReady=true;floatText(p.x,p.y,'CINDERHEART READY','#ffb05d')}}
  };

  const baseDamagePlayer=damagePlayer;
  damagePlayer=function(amount){
    const p=game.player;if(!p)return baseDamagePlayer(amount);
    const before=p.hp+(p.shield||0);baseDamagePlayer(amount);const lost=Math.max(0,before-(p.hp+(p.shield||0)));
    if(equippedSpecial('meteorMantle')&&lost>=14){p._meteorStored=Math.min(70,(p._meteorStored||0)+lost*.35);floatText(p.x,p.y,`Stored ${Math.round(p._meteorStored)}`,'#ffad7a')}
  };

  const baseDodge=dodge;
  dodge=function(){
    const p=game.player,start=p?{x:p.x,y:p.y}:null,ready=p&&p.dodgeCd<=0&&!paused&&!modalPause;
    baseDodge();
    if(ready&&start&&equippedSpecial('riftwalker')){
      radialDamage(start.x,start.y,.85,12+game.level*.8,'arcane');pushEffect('riftStep',start.x,start.y,.7,'#b978ff',{r:.85});
      safeLater(210,()=>{radialDamage(game.player.x,game.player.y,.85,12+game.level*.8,'arcane');pushEffect('riftStep',game.player.x,game.player.y,.7,'#b978ff',{r:.85})});
    }
  };

  const baseCastSpell=castSpell;
  castSpell=function(slot){
    const p=game.player,id=p?.activeSpells?.[slot],before=id?(p.spellState[id]?.cd||0):0;
    baseCastSpell(slot);
    if(!p||!id)return;const after=p.spellState[id]?.cd||0;if(after<=before+.02)return;
    const spell=SPELLS[id];
    if(p._cinderReady&&equippedSpecial('cinderheart')&&spell?.damage>0){const pt=aimPoint(3.2);p._cinderReady=false;p._cinderHeat=0;radialDamage(pt.x,pt.y,1.55,32+game.level*2,'fire');pushEffect('cinderheartBurst',pt.x,pt.y,.55,'#ff8546',{r:1.55})}
    if((p._meteorStored||0)>0&&equippedSpecial('meteorMantle')&&spell?.damage>0){const stored=p._meteorStored;p._meteorStored=0;radialDamage(p.x,p.y,2.25,stored,'arcane');pushEffect('meteorMantleShock',p.x,p.y,.6,'#ff9866',{r:2.25});shake=Math.max(shake,6)}
  };

  if(SPELL_CASTS.solarBeam){
    const baseSolar=SPELL_CASTS.solarBeam;
    SPELL_CASTS.solarBeam=function(id,s,m){
      baseSolar(id,s,m);
      if(!equippedSpecial('sunforgedGauntlet'))return;
      const p=game.player,d=p.facing||spellAim(),end=rayToEdge(p.x,p.y,d,.38),before=[...game.enemies];
      for(const e of before){if(e.dead)continue;const h=segmentInfo(e.x,e.y,p.x,p.y,end.x,end.y);if(h.d<.78+e.r)baseDamageEnemy(e,s.damage*.18*m.power,'fire')}
      for(const e of before)if(e.dead){radialDamage(e.x,e.y,.65,s.damage*.12*m.power,'fire');pushEffect('sunforgedBurst',e.x,e.y,.3,'#ffe182',{r:.7})}
    };
  }
  for(const castName of ['spirits','ancestorChoir'])if(SPELL_CASTS[castName]){
    const base=SPELL_CASTS[castName];SPELL_CASTS[castName]=function(id,s,m){base(id,s,m);if(equippedSpecial('choirBell')){game.summons.push({kind:'wisp',x:game.player.x,y:game.player.y,z:20,angle:Math.random()*TAU,life:9,maxLife:9,damage:s.damage*.85*m.power,heal:false,bomb:false,chain:false,guard:false,shot:0,color:'#e4f7ff'});pushEffect('choirBellPulse',game.player.x,game.player.y,.55,'#e1f5ff',{r:1.35})}};
  }

  /* ---------- Render identities ---------- */
  const baseDrawEffect=drawEffect;
  function prog(e){return clamp(1-e.life/e.maxLife,0,1)}function fade(e){return clamp(e.life/Math.min(.35,e.maxLife),0,1)}
  function drawLineWorld(ax,ay,bx,by,z=8){const a=worldToScreen(ax,ay,z),b=worldToScreen(bx,by,z);return {a,b}}
  drawEffect=function(e,front){
    const t=prog(e),f=fade(e),s=worldToScreen(e.x,e.y,10);
    if(e.kind==='voidGuillotine'||e.kind==='voidSnap'){
      const d=e.dir||{x:1,y:0},g=drawLineWorld(e.x-d.x*e.span,e.y-d.y*e.span,e.x+d.x*e.span,e.y+d.y*e.span,8);ctx.save();ctx.globalCompositeOperation='lighter';ctx.shadowBlur=22;ctx.shadowColor=e.color;ctx.strokeStyle=colorAlpha(e.color,(e.kind==='voidSnap'?.95:.55)*f);ctx.lineWidth=e.kind==='voidSnap'?8:3+t*7;ctx.beginPath();ctx.moveTo(g.a.x,g.a.y-28);for(let i=1;i<7;i++){const q=i/7;ctx.lineTo(lerp(g.a.x,g.b.x,q)+Math.sin(i*5.1)*6,lerp(g.a.y,g.b.y,q)-28+Math.cos(i*3.2)*8)}ctx.lineTo(g.b.x,g.b.y-28);ctx.stroke();ctx.restore();return;
    }
    if(e.kind==='thunderCathedral'){
      const r=e.r||2;ctx.save();ctx.globalCompositeOperation='lighter';for(let i=0;i<4;i++){const a=i*TAU/4+Math.PI/4,p=worldToScreen(e.x+Math.cos(a)*r,e.y+Math.sin(a)*r,0);ctx.strokeStyle=colorAlpha('#b8f8ff',.72*f);ctx.shadowBlur=18;ctx.shadowColor=e.color;ctx.lineWidth=4;ctx.beginPath();ctx.moveTo(p.x,p.y);ctx.lineTo(p.x,p.y-95);ctx.stroke();ctx.fillStyle=colorAlpha('#eaffff',.7*f);ctx.beginPath();ctx.arc(p.x,p.y-95,5,0,TAU);ctx.fill()}ctx.restore();return;
    }
    if(e.kind==='dragonfireTorrent'){
      const a=worldToScreen(e.x,e.y,18),b=worldToScreen(e.x+e.dir.x*e.range,e.y+e.dir.y*e.range,5);ctx.save();ctx.globalCompositeOperation='lighter';ctx.lineCap='round';ctx.shadowBlur=20;ctx.shadowColor='#ff5a2d';ctx.strokeStyle=`rgba(255,84,35,${.5*f})`;ctx.lineWidth=44*(1-t*.25);ctx.beginPath();ctx.moveTo(a.x,a.y-12);ctx.lineTo(b.x,b.y);ctx.stroke();ctx.strokeStyle=`rgba(255,221,110,${.85*f})`;ctx.lineWidth=15;ctx.stroke();ctx.strokeStyle=`rgba(255,250,205,${.9*f})`;ctx.lineWidth=4;ctx.stroke();ctx.restore();return;
    }
    if(e.kind==='crystalSpike'||e.kind==='shardErupt'||e.kind==='rootSpike'){
      ctx.save();ctx.translate(s.x,s.y);ctx.globalAlpha=f;ctx.shadowBlur=14;ctx.shadowColor=e.color;ctx.fillStyle=e.color;const count=e.kind==='shardErupt'?7:e.kind==='rootSpike'?4:5;for(let i=0;i<count;i++){const a=(i/(count-1)-.5)*1.1,h=(e.kind==='rootSpike'?22:34)+i%3*9;ctx.save();ctx.rotate(a*.35);ctx.beginPath();ctx.moveTo(-5,0);ctx.lineTo(0,-h*(.55+.45*Math.sin(Math.PI*Math.min(1,t*1.4))));ctx.lineTo(5,0);ctx.closePath();ctx.fill();ctx.restore()}ctx.restore();return;
    }
    if(e.kind==='moonfall'){
      const height=(1-t)*180;ctx.save();ctx.globalCompositeOperation='lighter';ctx.shadowBlur=28;ctx.shadowColor='#dfe9ff';ctx.fillStyle=`rgba(235,241,255,${.9*f})`;ctx.beginPath();ctx.arc(s.x,s.y-height,28,0,TAU);ctx.fill();ctx.fillStyle='rgba(160,180,220,.25)';ctx.beginPath();ctx.arc(s.x-8,s.y-height-5,6,0,TAU);ctx.fill();ctx.restore();return;
    }
    if(e.kind==='moonImpact'||e.kind==='meteorMantleShock'){
      const r=(e.r||2)*TILE_W*.5*t;ctx.save();ctx.strokeStyle=colorAlpha(e.color,.9*f);ctx.shadowBlur=18;ctx.shadowColor=e.color;ctx.lineWidth=6*(1-t)+2;ctx.beginPath();ctx.ellipse(s.x,s.y,r,r*.5,0,0,TAU);ctx.stroke();ctx.restore();return;
    }
    if(e.kind==='spiritStampede'){
      const a=worldToScreen(e.ax,e.ay,10),b=worldToScreen(e.bx,e.by,10);ctx.save();ctx.globalCompositeOperation='lighter';for(let i=0;i<6;i++){const q=(t+i*.13)%1,x=lerp(a.x,b.x,q),y=lerp(a.y,b.y,q)-10-Math.sin(q*Math.PI)*10;ctx.globalAlpha=(1-q)*.75*f;ctx.fillStyle=i%2?'#e5fbff':'#bfe9ff';ctx.beginPath();ctx.ellipse(x,y,12,7,0,0,TAU);ctx.fill();ctx.beginPath();ctx.moveTo(x-9,y-4);ctx.lineTo(x-18,y-11);ctx.lineTo(x-13,y+1);ctx.fill()}ctx.restore();return;
    }
    if(e.kind==='worldrootBranch'||e.kind==='puppetString'){
      const a=worldToScreen(e.x,e.y,8),b=worldToScreen(e.toX,e.toY,6);ctx.save();ctx.strokeStyle=colorAlpha(e.color,.8*f);ctx.shadowBlur=10;ctx.shadowColor=e.color;ctx.lineWidth=e.kind==='puppetString'?1.5:5;ctx.beginPath();ctx.moveTo(a.x,a.y);for(let i=1;i<6;i++){const q=i/6;ctx.lineTo(lerp(a.x,b.x,q)+Math.sin(i*4+e.branch)*5,lerp(a.y,b.y,q)+Math.cos(i*3)*3)}ctx.lineTo(b.x,b.y);ctx.stroke();if(e.kind==='worldrootBranch'){ctx.fillStyle=colorAlpha('#a9e38e',.7*f);for(let i=1;i<5;i++){const q=i/5;ctx.beginPath();ctx.arc(lerp(a.x,b.x,q)+Math.sin(i)*4,lerp(a.y,b.y,q)-4,3,0,TAU);ctx.fill()}}ctx.restore();return;
    }
    if(e.kind==='starbreakerBurst'||e.kind==='sunforgedBurst'){
      ctx.save();ctx.translate(s.x,s.y);ctx.globalCompositeOperation='lighter';ctx.strokeStyle=colorAlpha(e.color,.9*f);ctx.shadowBlur=18;ctx.shadowColor=e.color;ctx.lineWidth=3;const r=(e.r||1.6)*TILE_W*.45*(.3+t);for(let i=0;i<(e.kind==='starbreakerBurst'?12:8);i++){const a=i*TAU/(e.kind==='starbreakerBurst'?12:8);ctx.beginPath();ctx.moveTo(Math.cos(a)*r*.2,Math.sin(a)*r*.1);ctx.lineTo(Math.cos(a)*r,Math.sin(a)*r*.5);ctx.stroke()}ctx.restore();return;
    }
    if(e.kind==='railAim'||e.kind==='railBeam'){
      const a=worldToScreen(e.x,e.y,18),b=worldToScreen(e.toX,e.toY,7);ctx.save();ctx.globalCompositeOperation='lighter';ctx.lineCap='round';ctx.shadowBlur=e.kind==='railBeam'?22:8;ctx.shadowColor=e.color;ctx.strokeStyle=colorAlpha(e.color,(e.kind==='railBeam'?.9:.35)*f);ctx.lineWidth=e.kind==='railBeam'?10:2;ctx.beginPath();ctx.moveTo(a.x,a.y-12);ctx.lineTo(b.x,b.y);ctx.stroke();if(e.kind==='railBeam'){ctx.strokeStyle=`rgba(255,255,255,${.95*f})`;ctx.lineWidth=2;ctx.stroke()}ctx.restore();return;
    }
    if(e.kind==='runicBarrage'||e.kind==='choirBellPulse'){
      ctx.save();ctx.translate(s.x,s.y-12);ctx.globalCompositeOperation='lighter';const count=e.kind==='runicBarrage'?6:8,r=(e.kind==='runicBarrage'?42:58)*(1-.2*t);ctx.strokeStyle=colorAlpha(e.color,.75*f);ctx.fillStyle=colorAlpha(e.color,.18*f);ctx.lineWidth=2;for(let i=0;i<count;i++){const a=i*TAU/count+(e.base||0)+elapsed*.3,x=Math.cos(a)*r,y=Math.sin(a)*r*.52;ctx.save();ctx.translate(x,y);ctx.rotate(-a);ctx.strokeRect(-6,-6,12,12);ctx.restore()}ctx.restore();return;
    }
    if(e.kind==='runeRay'||e.kind==='frostbiteProc'||e.kind==='starbreakerCast'){
      const a=worldToScreen(e.x,e.y,18),b=worldToScreen(e.x+e.dir.x*(e.range||3.5),e.y+e.dir.y*(e.range||3.5),8);ctx.save();ctx.globalCompositeOperation='lighter';ctx.strokeStyle=colorAlpha(e.color,.75*f);ctx.shadowBlur=12;ctx.shadowColor=e.color;ctx.lineWidth=3;ctx.beginPath();ctx.moveTo(a.x,a.y-10);ctx.lineTo(b.x,b.y);ctx.stroke();ctx.restore();return;
    }
    if(['sunMothBurst','mimicWake','cinderRicochet','stormRamCrash','ashChoirPulse','verdantBloom','bloomBreak','gravityRipple','mirrorFlash','mirrorBlock','riftStep','cinderheartBurst'].includes(e.kind)){
      const r=(e.r||1)*TILE_W*.45*(.25+t);ctx.save();ctx.globalCompositeOperation='lighter';ctx.strokeStyle=colorAlpha(e.color,.8*f);ctx.shadowBlur=14;ctx.shadowColor=e.color;ctx.lineWidth=3;ctx.beginPath();ctx.ellipse(s.x,s.y,r,r*.5,0,0,TAU);ctx.stroke();ctx.restore();return;
    }
    if(e.kind==='mirrorClone'){
      ctx.save();ctx.globalAlpha=.45*f;ctx.fillStyle=e.color;ctx.strokeStyle='#fff';ctx.beginPath();ctx.ellipse(s.x,s.y-18,13,22,0,0,TAU);ctx.fill();ctx.stroke();ctx.restore();return;
    }
    if(e.kind==='bogBubble'){
      const r=(e.r||.8)*TILE_W*.5*(.7+t*.2);ctx.save();ctx.globalAlpha=.65*f;ctx.strokeStyle=e.color;ctx.fillStyle=colorAlpha(e.color,.12);ctx.shadowBlur=12;ctx.shadowColor=e.color;ctx.beginPath();ctx.arc(s.x,s.y-18,r*.45,0,TAU);ctx.fill();ctx.stroke();ctx.restore();return;
    }
    if(e.kind==='burrowRidge'){
      ctx.save();ctx.fillStyle=colorAlpha(e.color,.55*f);ctx.shadowBlur=8;ctx.shadowColor=e.color;for(let i=-1;i<=1;i++){ctx.beginPath();ctx.moveTo(s.x+i*9,s.y);ctx.lineTo(s.x+i*9+5,s.y-13);ctx.lineTo(s.x+i*9+10,s.y);ctx.fill()}ctx.restore();return;
    }
    return baseDrawEffect(e,front);
  };

  const baseDrawProjectiles=drawProjectiles;
  drawProjectiles=function(){
    baseDrawProjectiles();
    for(const q of game.projectiles)if(q.kind==='starbreaker'){
      const s=worldToScreen(q.x,q.y,q.z);ctx.save();ctx.translate(s.x,s.y);ctx.globalCompositeOperation='lighter';ctx.shadowBlur=22;ctx.shadowColor='#ffe788';ctx.strokeStyle='#fff5b2';ctx.fillStyle='#fffbe0';for(let i=0;i<8;i++){const a=i*TAU/8+elapsed*2.5;ctx.beginPath();ctx.moveTo(Math.cos(a)*4,Math.sin(a)*4);ctx.lineTo(Math.cos(a)*15,Math.sin(a)*15);ctx.stroke()}ctx.beginPath();ctx.arc(0,0,6,0,TAU);ctx.fill();ctx.restore();
    }
  };

  const baseDrawEnemy=drawEnemy;
  drawEnemy=function(e){
    baseDrawEnemy(e);
    if(!e?.type?.startsWith('aw30_'))return;
    const s=worldToScreen(e.x,e.y,12),pulse=.5+.5*Math.sin(elapsed*5+e.phase);ctx.save();ctx.globalCompositeOperation='lighter';
    if(e.type==='aw30_sunmoth'){ctx.strokeStyle=colorAlpha('#ffd96e',.45);ctx.lineWidth=2;for(const side of [-1,1]){ctx.beginPath();ctx.ellipse(s.x+side*16,s.y-20,16,8,side*.35,0,TAU);ctx.stroke()}ctx.fillStyle=colorAlpha('#ffd96e',.45+.25*pulse);ctx.beginPath();ctx.arc(s.x,s.y-18,5,0,TAU);ctx.fill()}
    else if(e.type==='aw30_mimic'&&e.awDormant){ctx.globalCompositeOperation='source-over';ctx.fillStyle='#69676e';ctx.fillRect(s.x-10,s.y-35,20,32);ctx.fillStyle='#4d4b54';ctx.fillRect(s.x-15,s.y-6,30,6)}
    else if(e.type==='aw30_cinderwheel'){ctx.strokeStyle=colorAlpha('#ff8a4f',.7);ctx.lineWidth=4;ctx.shadowBlur=12;ctx.shadowColor='#ff6b35';ctx.beginPath();ctx.arc(s.x,s.y-15,15,elapsed*5,elapsed*5+TAU*1.7);ctx.stroke()}
    else if(e.type==='aw30_mirrorknight'){ctx.strokeStyle=colorAlpha('#e5f7ff',.65);ctx.lineWidth=3;ctx.beginPath();ctx.moveTo(s.x+10,s.y-33);ctx.lineTo(s.x+24,s.y-20);ctx.lineTo(s.x+12,s.y-3);ctx.closePath();ctx.stroke()}
    else if(e.type==='aw30_boglantern'){ctx.fillStyle=colorAlpha('#c7ef7c',.35+.25*pulse);ctx.shadowBlur=18;ctx.shadowColor='#b8e66f';ctx.beginPath();ctx.arc(s.x,s.y-22,12,0,TAU);ctx.fill()}
    else if(e.type==='aw30_stormram'){ctx.strokeStyle=colorAlpha('#a8efff',.75);ctx.lineWidth=3;ctx.shadowBlur=10;ctx.shadowColor='#8cecff';ctx.beginPath();ctx.arc(s.x-12,s.y-28,11,.2,2.8);ctx.arc(s.x+12,s.y-28,11,.35,2.95);ctx.stroke()}
    else if(e.type==='aw30_puppeteer'){for(let i=0;i<4;i++){const a=i*TAU/4+elapsed;ctx.strokeStyle=colorAlpha('#d08cff',.45);ctx.beginPath();ctx.moveTo(s.x,s.y-25);ctx.lineTo(s.x+Math.cos(a)*20,s.y-25+Math.sin(a)*10);ctx.stroke()}}
    else if(e.type==='aw30_burrower'&&e.state==='burrow'){ctx.globalAlpha=.3;ctx.fillStyle='#baf4ff';ctx.beginPath();ctx.ellipse(s.x,s.y,26,7,0,0,TAU);ctx.fill()}
    else if(e.type==='aw30_ashchoir'){for(let i=0;i<(e.awMasks||5);i++){const a=i*TAU/(e.awMasks||5)+elapsed*.35;ctx.fillStyle=colorAlpha('#ece5df',.75);ctx.beginPath();ctx.ellipse(s.x+Math.cos(a)*19,s.y-24+Math.sin(a)*8,5,7,0,0,TAU);ctx.fill()}}
    else if(e.type==='aw30_behemoth'&&e.awBlooms>0){for(let i=0;i<e.awBlooms;i++){const a=i*TAU/3+elapsed*.25;ctx.fillStyle='#d6f49c';ctx.beginPath();ctx.arc(s.x+Math.cos(a)*20,s.y-42+Math.sin(a)*8,5,0,TAU);ctx.fill()}}
    ctx.restore();
  };
})();