'use strict';
/* One bespoke sixth mutation for every spell. Jobs advance in simulation time, never wall time. */
(() => {
  let jobs=[];
  function later(delay,action){jobs.push({delay,action});}
  function repeat(count,interval,action){for(let i=0;i<count;i++)later(i*interval,()=>action(i));}
  const point=o=>({x:o.x,y:o.y});
  const nearby=(at,r)=>game.enemies.filter(e=>e.hp>0&&dist(e,at)<r+(e.r||0));
  function pulse(at,r,damage,color,tag='arcane'){
    radialDamage(at.x,at.y,r,damage,tag);fx('shockRing',at.x,at.y,.45,color,{r});
  }
  function bolt(at,dir,damage,color,extra={}){
    return magicProjectile({x:at.x,y:at.y,vx:dir.x*7,vy:dir.y*7,r:.12,life:1.8,damage,color,kind:'arcane',...extra});
  }
  function ring(at,count,damage,color,extra={}){
    for(let i=0;i<count;i++){const a=i*TAU/count;bolt(at,{x:Math.cos(a),y:Math.sin(a)},damage,color,extra);}
  }
  function field(at,r,life,damage,color,extra={}){
    return groundEffect('lightningField',at.x,at.y,r,life,color,damage,.5,extra);
  }
  function ray(from,to,damage,color,width=.4){
    fx('lightning',from.x,from.y,.3,color,{toX:to.x,toY:to.y,width:3});
    const dx=to.x-from.x,dy=to.y-from.y,l2=dx*dx+dy*dy;
    for(const e of [...game.enemies]){const t=l2?clamp(((e.x-from.x)*dx+(e.y-from.y)*dy)/l2,0,1):0;
      if(Math.hypot(e.x-from.x-dx*t,e.y-from.y-dy*t)<width+e.r)damageEnemy(e,damage,'arcane');}
  }
  function mine(at,damage,color,r=1){
    fx('castRing',at.x,at.y,3,color,{r:r*.65});
    let used=false;repeat(12,.25,()=>{if(!used&&nearby(at,r).length){used=true;pulse(at,r,damage,color);}});
  }
  function capture(at,r,max=4){
    const shots=game.projectiles.filter(q=>q.owner==='enemy'&&q.life>0&&dist(q,at)<r).slice(0,max);
    for(const q of shots)q.life=0;
    // The legacy projectile loop still checks collisions for life=0 shots; remove them now.
    if(shots.length)game.projectiles=game.projectiles.filter(q=>q.life>0);
    return shots.length;
  }
  function ward(amount,time){const p=game.player;p.shield=Math.max(p.shield||0,amount);p.shieldTime=Math.max(p.shieldTime||0,time);}
  // id: [name, visible description, behavior]. All additions use the same persistent upgrade registry.
  const mutations={
    firebolt:['Cinder Traps','Casting plants three proximity embers ahead of you for 3 seconds.',c=>{
      for(let i=1;i<=3;i++)mine(c.ahead(i*1.3),c.damage*.35,'#ff985b');
    }],
    frostnova:['Winter Refuge','For 3 seconds, your casting point destroys nearby hostile projectiles.',c=>{
      fx('timeBubble',c.origin.x,c.origin.y,3,'#a4efff',{r:1.8});repeat(12,.25,()=>capture(c.origin,1.8));
    }],
    thorns:['Bloodroot Seed','Plant a seed ahead that strikes after 1 second and heals 2 health per victim, up to 8.',c=>{
      fx('bloom',c.target.x,c.target.y,1,'#d8a382',{r:1.7});later(1,()=>{const n=nearby(c.target,1.7).length;pulse(c.target,1.7,c.damage*.45,'#b5e993','nature');if(n)healPlayer(Math.min(8,n*2));});
    }],
    arcaneMissiles:['Crossfire Glyphs','Two side glyphs each fire three aimed bolts over 1 second.',c=>{
      for(const side of [-1,1]){const at=c.offset(0,side*1.8);fx('castRing',at.x,at.y,1.5,'#b9a7ff',{r:.5});repeat(3,.5,()=>{const e=nearestEnemy(at,8);if(e)bolt(at,norm(e.x-at.x,e.y-at.y),c.damage*.4,'#b9a7ff');});}
    }],
    gust:['Undertow','After the blast, pull nearby enemies toward your original casting point.',c=>{
      later(.45,()=>{for(const e of nearby(c.origin,4)){const d=norm(c.origin.x-e.x,c.origin.y-e.y),amount=e.boss?.25:.9;e.x=clamp(e.x+d.x*amount,.5,ROOM_W-.5);e.y=clamp(e.y+d.y*amount,.5,ROOM_H-.5);}fx('riftOpen',c.origin.x,c.origin.y,.6,'#b8eaff',{r:4});});
    }],
    ward:['Prism Battery','For 4 seconds, consume nearby hostile shots and fire a seeking shard for each.',c=>{
      repeat(16,.25,()=>{const p=point(game.player),n=capture(p,1.4,2);ring(p,n,12+game.level,'#a8eaff',{seek:1.5});});
    }],
    chain:['Lightning Rod','Anchor a rod ahead that shocks a line from you to it three times.',c=>{
      fx('castRing',c.target.x,c.target.y,2,'#90f3ff',{r:.5});repeat(3,.6,()=>ray(point(game.player),c.target,c.damage*.25,'#90f3ff'));
    }],
    poison:['Spore Hosts','Mark up to three nearby foes; if they die within 4 seconds, they leave venom pools.',c=>{
      for(const e of nearby(c.target,3).slice(0,3)){let used=false,last=point(e);repeat(16,.25,()=>{if(used)return;last=point(e);if(e.hp<=0){used=true;groundEffect('poison',last.x,last.y,1.2,3,'#a9e876',c.damage*.35,.5);}});}
    }],
    chakram:['Moon Scissors','Two perpendicular crescents cross your aim point after half a second.',c=>{
      later(.5,()=>{for(const side of [-1,1])bolt(c.offset(3,side*3),{x:-c.dir.y*-side,y:c.dir.x*-side},c.damage*.45,'#d5bbff',{kind:'chakram',pierce:5,life:1});});
    }],
    spirits:['Spirit Beacon','Leave a beacon that launches four healing wisps at enemies; each hit restores 1 health.',c=>{
      fx('summon',c.origin.x,c.origin.y,3,'#d1faff',{r:.7});repeat(4,.7,()=>{const e=nearestEnemy(c.origin,9);if(e)bolt(c.origin,norm(e.x-c.origin.x,e.y-c.origin.y),c.damage*.5,'#d1faff',{seek:2,onHit:()=>healPlayer(1)});});
    }],
    quake:['Fault Junction','A perpendicular fissure opens ahead, then closes for a second line hit.',c=>{
      const a=c.offset(3,-3),b=c.offset(3,3);ray(a,b,c.damage*.3,'#dcb88b',.55);later(.8,()=>ray(b,a,c.damage*.3,'#e9d1ae',.55));
    }],
    meteor:['Impact Shelter','Raise a 4-second ward; nearby hostile shots are destroyed when the ward is raised.',c=>{
      const n=capture(c.origin,3,8);ward(18+n*3,4);fx('ward',c.origin.x,c.origin.y,4,'#ffc17d',{follow:true});
    }],
    voidrift:['Rift Bridge','Connect your casting point and aim point with four damaging void pulses.',c=>{
      repeat(4,.65,()=>ray(c.origin,c.target,c.damage*.65,'#bd8aff',.65));
    }],
    icelance:['Rime Barricade','Plant five slowing ice patches across the path ahead.',c=>{
      for(let i=-2;i<=2;i++){const at=c.offset(3,i*.8);groundEffect('iceShard',at.x,at.y,.55,3,'#b7eeff',c.damage*.12,.5,{slow:true});}
    }],
    soulflame:['Soul Inheritance','If the closest foe dies within 5 seconds, it releases six seeking spirit bolts.',c=>{
      const e=nearestEnemy(c.origin,9);if(!e)return;let used=false;repeat(20,.25,()=>{if(!used&&e.hp<=0){used=true;ring(point(e),6,c.damage*.2,'#dfadff',{seek:2});}});
    }],
    tempest:['Storm Rails','Two parallel lightning rails pulse three times along your aimed direction.',c=>{
      repeat(3,.65,()=>{for(const side of [-1,1])ray(c.offset(0,side),c.offset(7,side),c.damage*.3,'#a7eeff');});
    }],
    timestop:['Rewound Volley','For 3 seconds, nearby hostile projectiles reverse direction as friendly shots.',c=>{
      repeat(12,.25,()=>{for(const q of game.projectiles.filter(q=>q.owner==='enemy'&&q.life>0&&dist(q,c.target)<3).slice(0,6)){q.owner='player';q.vx=-q.vx;q.vy=-q.vy;q.hit=new Set();q.onHit=null;q.damage=14+game.level;q.color='#afcaff';}});
    }],
    phoenix:['Feather Nests','Leave four explosive feather traps along your flight line for 3 seconds.',c=>{
      for(let i=0;i<4;i++)mine(c.offset(1+i*1.5,i%2?.7:-.7),c.damage*.2,'#ffd596',.85);
    }],
    starfall:['North Star','Mark the strongest foe; three beams follow it from your casting point.',c=>{
      const e=[...game.enemies].filter(e=>e.hp>0).sort((a,b)=>b.hp-a.hp)[0];if(!e)return;
      repeat(3,.8,()=>{if(e.hp>0)ray(c.origin,point(e),c.damage*.3,'#e5eaff',.5);});
    }],
    singularity:['Captured Starlight','The aim point consumes hostile shots for 2 seconds, then releases up to 12 seeking shards.',c=>{
      let count=0;repeat(8,.25,()=>{count=Math.min(12,count+capture(c.target,3));});later(2,()=>{if(count)ring(c.target,count,c.damage*.16,'#bf9aff',{seek:1.4});});
    }],
    solarLance:['Sunbridge','Lay a narrow burning bridge of six sun patches along your aim.',c=>{
      for(let i=1;i<=6;i++){const at=c.ahead(i);groundEffect('fire',at.x,at.y,.42,2,'#ffd687',c.damage*.09,.5);}
    }],
    stormSpear:['Conductive Harpoon','The first spear victim draws two later lightning strikes from your casting point.',c=>{
      c.onProjectileHit(e=>{const at=point(e);repeat(2,.5,()=>ray(c.origin,at,c.damage*.25,'#94eeff'));},true);
    }],
    emberComet:['Comet Wake','Leave a widening fan of five embers behind your casting point.',c=>{
      for(let i=-2;i<=2;i++){const a=Math.atan2(c.dir.y,c.dir.x)+Math.PI+i*.25;bolt(c.origin,{x:Math.cos(a),y:Math.sin(a)},c.damage*.25,'#ffa176',{kind:'ember',splash:.35});}
    }],
    glassWinter:['Brittle Mirror','Two glass traps appear on either side, bursting when approached.',c=>{
      for(const side of [-1,1])mine(c.offset(0,side*2.5),c.damage*.65,'#d7f6ff',1.4);
    }],
    briarCrown:['Royal Procession','Three root patches grow behind you at half-second intervals as you move.',c=>{
      repeat(3,.5,()=>{const at=point(game.player);groundEffect('thorns',at.x,at.y,1,3,'#e69bbb',c.damage*.2,.5,{slow:true});});
    }],
    novaSwarm:['Star Eggs','Three stationary eggs hatch into seeking missiles after staggered delays.',c=>{
      for(let i=0;i<3;i++){const at=c.offset(2,(i-1)*1.5);fx('castRing',at.x,at.y,1.5,'#e1c8ff',{r:.4});later(.4+i*.35,()=>ring(at,2,c.damage*.4,'#e1c8ff',{seek:2}));}
    }],
    cycloneWall:['Windbreak','A moving three-point screen destroys hostile projectiles for 2 seconds.',c=>{
      repeat(10,.2,i=>{for(const side of [-1,0,1]){const at=c.offset(i*.5,side);capture(at,.9,3);fx('castRing',at.x,at.y,.25,'#bbf3ea',{r:.5});}});
    }],
    mirrorAegis:['Mirror Step','Leave an afterimage that explodes after 1 second and refreshes dodge if it hits.',c=>{
      fx('ward',c.origin.x,c.origin.y,1,'#eadcff');later(1,()=>{const hit=nearby(c.origin,2).length;pulse(c.origin,2,22+game.level*2,'#eadcff');if(hit)game.player.dodgeCd=0;});
    }],
    heavensCircuit:['Circuit Breaker','Stun up to four distant enemies around your aim point and link them to its center.',c=>{
      for(const e of nearby(c.target,4).filter(e=>dist(e,c.target)>1.5).slice(0,4)){ray(c.target,point(e),c.damage*.25,'#eaffae');if(!e.boss)e.stun=Math.max(e.stun||0,.55);}
    }],
    rotBloom:['Fungal Harvest','Three delayed spore bursts around the garden heal 1 health per nearby foe, up to 3 each.',c=>{
      repeat(3,.8,i=>{const a=i*TAU/3,at={x:c.target.x+Math.cos(a)*1.4,y:c.target.y+Math.sin(a)*1.4};const n=nearby(at,1).length;pulse(at,1,c.damage*.5,'#d2e98f','poison');if(n)healPlayer(Math.min(3,n));});
    }],
    eclipseDisc:['Eclipse Umbra','For 2 seconds, a shadow at your casting point slows nearby enemies and absorbs shots.',c=>{
      field(c.origin,1.8,2,c.damage*.1,'#c29ce9',{slow:true});repeat(8,.25,()=>capture(c.origin,1.8,2));
    }],
    ancestorChoir:['Ancestral Council','Three stationary ancestors each fire one piercing bolt at the nearest foe.',c=>{
      for(let i=0;i<3;i++){const a=i*TAU/3,at={x:c.origin.x+Math.cos(a)*2,y:c.origin.y+Math.sin(a)*2};fx('summon',at.x,at.y,1.3,'#d4f0e7',{r:.5});later(.6,()=>{const e=nearestEnemy(at,10);if(e)bolt(at,norm(e.x-at.x,e.y-at.y),c.damage*.8,'#d4f0e7',{pierce:4});});}
    }],
    runicNeedle:['Stitchwork','The first needle victim is stitched to up to two neighbors by damaging threads.',c=>{
      c.onProjectileHit(e=>{for(const other of nearby(e,3).filter(o=>o!==e).slice(0,2))ray(point(e),point(other),c.damage*.3,'#b8bfff');},true);
    }],
    celestialFurnace:['Walking Dawn','For 3 seconds, your movement leaves six small damaging sunspots.',c=>{
      repeat(6,.5,()=>field(point(game.player),.7,1.5,c.damage*.15,'#ffe09e'));
    }],
    eventideGate:['Threshold Toll','A cross-shaped threshold pulses twice across the gate center.',c=>{
      repeat(2,1,()=>{ray(c.offset(3,-3),c.offset(3,3),c.damage*.5,'#d3a4ff');ray(c.ahead(0),c.ahead(6),c.damage*.5,'#d3a4ff');});
    }],
    seraphicArray:['Mercy Between Stars','Three protective sigils restore 3 health each if you stand near them when they light.',c=>{
      for(let i=0;i<3;i++){const at=c.offset(1+i,i-1);fx('castRing',at.x,at.y,2,'#ffecad',{r:.8});later(1+i*.3,()=>{pulse(at,.8,c.damage*.2,'#ffecad');if(dist(game.player,at)<1)healPlayer(3);});}
    }],
    voidGuillotine:['Severed Space','Two parallel follow-up cuts close from either side of your aim line.',c=>{
      for(const side of [-1,1])later(.65,()=>ray(c.offset(0,side),c.offset(7,side),c.damage*.3,'#da9aff',.5));
    }],
    thunderCathedral:['Sanctuary Bells','Three bells destroy hostile shots at your casting point and shock nearby enemies.',c=>{
      repeat(3,.8,()=>{capture(c.origin,2.2,6);pulse(c.origin,2.2,c.damage*.35,'#c2efff');});
    }],
    dragonfireTorrent:['Dragon Teeth','A semicircle of five fire traps protects the space behind you.',c=>{
      for(let i=0;i<5;i++){const a=Math.atan2(c.dir.y,c.dir.x)+Math.PI+(i-2)*.5;mine({x:c.origin.x+Math.cos(a)*2,y:c.origin.y+Math.sin(a)*2},c.damage*.8,'#ffb67e',.75);}
    }],
    crystalRailway:['Branch Line','An intersecting line of crystals erupts sequentially across the railway.',c=>{
      repeat(5,.16,i=>{const at=c.offset(3,(i-2)*1.1);pulse(at,.65,c.damage*.3,'#c6f3ff','frost');fx('stonePillar',at.x,at.y,.65,'#c6f3ff',{r:.5});});
    }],
    moonfall:['Lunar Tides','After 1 second, four outward moonblades launch from the marked area.',c=>{
      later(1,()=>ring(c.target,4,c.damage*.25,'#eee1ff',{kind:'chakram',pierce:5,life:1.2}));
    }],
    spiritStampede:['Pack Ambush','Two spectral hunters fire across the charge route from opposite flanks.',c=>{
      for(const side of [-1,1]){const at=c.offset(3,side*3);fx('summon',at.x,at.y,1,'#c4efed',{r:.8});later(.6,()=>bolt(at,{x:c.dir.y*side,y:-c.dir.x*side},c.damage*.5,'#c4efed',{seek:.6,pierce:3}));}
    }],
    worldroot:['Root Network','Connect up to four nearby enemies in a damaging chain of living roots.',c=>{
      const targets=nearby(c.target,5).slice(0,4);let last=c.origin;for(const e of targets){ray(last,point(e),c.damage*.3,'#a2e9a4');last=point(e);}
    }],
    starbreaker:['Dying Star','The first star impact leaves a core that releases eight shards after half a second.',c=>{
      c.onProjectileHit((e,q)=>{const at=point(q);fx('castRing',at.x,at.y,.5,'#fff0ae',{r:.8});later(.5,()=>ring(at,8,c.damage*.12,'#fff0ae'));},true);
    }],
    arcaneRailgun:['Return Address','After 1 second, a return beam strikes back along your original aim line.',c=>{
      later(1,()=>ray(c.ahead(12),c.origin,c.damage*.35,'#f5cbff',.6));
    }],
    runicBarrage:['Runic Tripwires','A triangular lattice around your casting point pulses twice.',c=>{
      const points=[0,1,2].map(i=>({x:c.origin.x+Math.cos(i*TAU/3)*2.5,y:c.origin.y+Math.sin(i*TAU/3)*2.5}));
      repeat(2,.7,()=>points.forEach((p,i)=>ray(p,points[(i+1)%3],c.damage*.4,'#ceb5ff')));
    }]
  };
  for(const [id,[name,description]] of Object.entries(mutations)){
    if(!SPELLS[id])continue;
    (UPGRADE_POOLS[id]||(UPGRADE_POOLS[id]=[])).push([`${id}_signature`,name,SPELLS[id].icon,description,'signature']);
  }
  // Wrap each cast key once: shared base casts still dispatch to the selected spell's own mutation.
  for(const key of new Set(Object.values(SPELLS).map(s=>s.cast))){
    const base=SPELL_CASTS[key];if(!base)continue;
    SPELL_CASTS[key]=function(id,s,m){
      const firstProjectile=game.projectiles.length,origin=point(game.player),dir={...spellAim()};
      const result=base(id,s,m);
      if(!mutations[id]||!hasUpgrade(id,'signature'))return result;
      const offset=(forward,side)=>({x:clamp(origin.x+dir.x*forward-dir.y*side,.5,ROOM_W-.5),y:clamp(origin.y+dir.y*forward+dir.x*side,.5,ROOM_H-.5)});
      const onProjectileHit=(action,once=false)=>{
        let done=false;for(const q of game.projectiles.slice(firstProjectile)){
          const old=q.onHit;q.onHit=function(e,p){old?.(e,p);if(!once||!done){done=true;action(e,p);}};
        }
      };
      mutations[id][2]({origin,dir,target:offset(3,0),ahead:n=>offset(n,0),offset,damage:s.damage*m.power,onProjectileHit});
      return result;
    };
  }
  const baseUpdate=updateSpellEntities;
  updateSpellEntities=function(dt){
    baseUpdate(dt);
    const current=jobs;jobs=[];
    for(const job of current){job.delay-=dt;if(job.delay<=0)job.action();else jobs.push(job);}
  };
  const baseLoad=loadRoom;
  loadRoom=function(){jobs=[];return baseLoad();};
  window.AWSpellMutations={ids:Object.keys(mutations),pending:()=>jobs.length};
})();
