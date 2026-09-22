'use strict';
/* Continental grimoire. Persistent IDs live in the existing spellbook/save registry;
   combat state is room-local and advances only with the simulation. */
(() => {
  const rows = [
    ['briarCage','Briar Cage','🌿','Uncommon','Nature','verdant',8,24,'briarwatch','Trap foes in a thorn circle for 5 seconds; crossing the edge hurts.'],
    ['wildstep','Wildstep','🍃','Uncommon','Nature','verdant',7,18,'sunmere','Dash through foes, slowing everything along your path.'],
    ['sporeburst','Sporeburst','🍄','Rare','Nature','verdant',9,9,'verdant-dungeon','Launch a pod that blooms into a cloud of stacking poison.'],
    ['guardianTreant','Guardian Treant','🌳','Epic','Summoning','verdant',18,22,'verdant-boss2','Summon a sturdy, slow treant that blocks foes, absorbs attacks and strikes nearby enemies.'],
    ['predatorsMark',"Predator’s Mark",'🐾','Rare','Nature','verdant',12,0,'briarwatch','Mark a target for 8 seconds: +30% weapon damage, summon priority and a visible tracking line. A kill refunds half the remaining cooldown.'],
    ['emeraldRain','Emerald Rain','🌧','Epic','Nature','verdant',16,0,'verdant-ruler','Create a healing grove for 7 seconds. Mend yourself and summons; preserve poison on enemies inside.'],
    ['thunderstep','Thunderstep','⚡','Rare','Storm','meridian',8,28,'stormrest','Blink forward and shock enemies at both ends.'],
    ['magneticField','Magnetic Field','🧲','Rare','Storm','meridian',11,7,'stormrest','Pull ranged foes inward and weaken incoming projectiles by 35%, once per shot.'],
    ['crystalBarricade','Crystal Barricade','💎','Uncommon','Arcane','meridian',12,0,'prismhold','Raise three destructible crystals that block foes and intercept hostile projectiles for 7 seconds.'],
    ['prismRebound','Prism Rebound','🔷','Rare','Arcane','meridian',7,26,'prismhold','Fire a shard that rebounds off room edges and between new targets, gaining 12% damage per bounce (up to 60%).'],
    ['avalanche','Avalanche','❄','Epic','Frost','meridian',13,52,'meridian-boss2','Send a broad advancing snow wave that carries light foes and heavily slows survivors.'],
    ['stormBeacon','Storm Beacon','⛈','Epic','Storm','meridian',15,24,'meridian-ruler','Plant a rod for 8 seconds. It repeatedly strikes nearby foes; lightning deals 20% more damage while you stand near it.'],
    ['mirrorWalk','Mirror Walk','🪞','Epic','Arcane','meridian',13,0,'meridian-dungeon','Leave a moving decoy for 5 seconds, distract nearby foes and gain a burst of speed.'],
    ['soulChain','Soul Chain','⛓','Rare','Shadow/Void','gloam',12,0,'bonehaven','Link up to four foes for 6 seconds. Copy 25% of actual damage to the other linked foes.'],
    ['umbralPassage','Umbral Passage','🌑','Epic','Shadow/Void','gloam',15,42,'redwatch','Phase for 2 seconds: move faster and ignore damage, but cannot attack or cast. Exit with a void blast.'],
    ['astralSentinel','Astral Sentinel','👁','Epic','Celestial','gloam',18,25,'astral','Summon a guardian for 10 seconds; its beams prioritize marked foes, winding-up threats, ranged foes, then nearby enemies.'],
    ['gravityInversion','Gravity Inversion','🌀','Legendary','Arcane','gloam',20,78,'gloam-boss3','Destabilize an area, then slam it after 1 second. Light foes are suspended; heavy foes are slowed.'],
    ['eclipse','Eclipse','🌘','Legendary','Shadow/Void','gloam',24,0,'gloam-ruler','For 8 seconds, darken the battlefield, conceal yourself from distant idle foes, empower shadow damage and let celestial hits critically flare.'],
    ['constellationSpear','Constellation Spear','🌠','Legendary','Celestial','gloam',18,125,'gloam-boss4','Designate an aimed line for 0.8 seconds, then release a piercing starlight beam. Keep aiming during the charge.'],
    ['rewind','Rewind','⌛','Legendary','Arcane','gloam',32,0,'gloam-dungeon','Save position, health and shield for 5 seconds. Cast again to return. Does not revive you or reset other cooldowns.'],
    ['seedSentry','Seed Sentry','🌱','Uncommon','Nature','verdant',10,13,'briarwatch','Plant a stationary seed turret for 8 seconds that shoots at nearby foes.'],
    ['brambleTether','Bramble Tether','🌾','Rare','Nature','verdant',9,27,'verdant-boss1','Latch a vine to the closest aimed foe; drag light foes toward you and root them briefly.'],
    ['pollenVeil','Pollen Veil','🌼','Rare','Nature','verdant',12,0,'verdant-shrine','A moving pollen veil weakens nearby enemy shots and gives you a short ward.'],
    ['cinderMine','Cinder Mine','🔥','Rare','Fire','meridian',9,48,'emberfall','Plant a proximity mine; a nearby foe triggers a fiery blast after a short arming delay.'],
    ['hailOrbit','Hail Orbit','🧊','Rare','Frost','meridian',11,17,'prismhold','Three orbiting hailstones strike nearby foes and chill them for 6 seconds.'],
    ['fulguriteLance','Fulgurite Lance','🔱','Epic','Storm','meridian',10,36,'meridian-boss1','Pierce a line with lightning glass. Chilled foes take extra damage and shatter sparks toward a second foe.'],
    ['emberRecall','Ember Recall','🕯','Epic','Fire','meridian',14,32,'meridian-boss3','Blink backward, leaving a burning decoy that detonates when destroyed or expired.'],
    ['soulHarvest','Soul Harvest','💜','Epic','Shadow/Void','gloam',16,44,'gloam-boss1','Harvest nearby wounded foes: double damage below 30% health; kills restore health, up to 12 per cast.'],
    ['starweave','Starweave','✨','Epic','Celestial','gloam',14,22,'gloam-shrine','Raise a triangle of stars around you. Its edges pulse damage; standing inside restores a small shield.'],
    ['hourglassMine','Hourglass Mine','⏳','Legendary','Arcane','gloam',19,58,'gloam-boss2','Arm a temporal mine: approaching foes are slowed, then struck by a delayed time collapse.']
  ];
  const definitions={};
  for(const [id,name,icon,rarity,category,region,cooldown,damage,source,desc] of rows){
    const tags=/step|Walk|Passage|rewind|Recall/.test(id)?['Utility']:[];
    if(/Treant|Sentinel|Sentry/.test(id))tags.push('Summoning');
    if(/Rain|Barricade|Veil|rewind|starweave/.test(id))tags.push('Defensive');
    definitions[id]={name,icon,rarity,category,region,cooldown,damage,cast:id,tags,source,desc};
    SPELLS[id]=definitions[id];
    // Every spell has three meaningful branches; bespoke options are below.
    UPGRADE_POOLS[id]=[
      [`${id}_potency`,'Deep Channel','✦',damage?'Increase damage by 30%.':'Increase healing, shields, duration or shared damage by 30%.','potency'],
      [`${id}_reach`,'Far Inscription','◎','Increase reach, area or travel distance by 30%.','reach'],
      [`${id}_lasting`,'Enduring Script','⌛','Extend persistent effects by 40%; direct attacks instead recover 20% faster.','lasting']
    ];
    const D=window.AWCampaignData;
    if(D.towns[source]){
      const town=D.towns[source];town.spells.push(id);
      if(!town.roles.some(role=>['Arcanist','Spell Scribe','Enchanter','Weaponsmith'].includes(role)))town.roles.push('Spell Scribe');
    }
    else if(D.nodes[source])(D.nodes[source].rewardSpells||=[]).push(id);
    SPELLS[id].desc+=` Found at ${D.towns[source]?.name||D.nodes[source]?.name}.`;
  }
  UPGRADE_POOLS.rewind[1]=['rewind_reach','Anchored Ward','🛡','Gain 15 additional shield for 3 seconds when returning.','reach'];
  UPGRADE_POOLS.mirrorWalk[1][3]='Increase the distance from which the decoy attracts enemies by 30%.';
  const extra=(id,key,name,desc)=>UPGRADE_POOLS[id].push([`${id}_${key}`,name,SPELLS[id].icon,desc,key]);
  extra('briarCage','iron','Iron Thorns','Double damage when a foe crosses the thorn barrier.');
  extra('briarCage','prison','Living Prison','Slow enemies inside the cage.');
  extra('briarCage','bloodroot','Bloodroot','Cage damage restores 1 health per hit.');
  extra('wildstep','charge','Second Growth','Store a second dash; both charges share the normal recharge timer.');
  extra('wildstep','thorns','Thornwake','Leave three damaging thorn patches behind the dash.');
  extra('wildstep','haste','Fleet Leaves','Gain 3 seconds of movement speed after dashing.');
  extra('sporeburst','spread','Spore Succession','A poisoned foe dying in the main cloud leaves a smaller cloud. Secondary clouds cannot spread.');
  const colors={Nature:'#9cdd89',Storm:'#8beaff',Arcane:'#c4a6ff',Frost:'#bceeff','Shadow/Void':'#cf8cff',Celestial:'#ffe4a3',Summoning:'#a7df9b',Fire:'#ffa266'};
  let fields=[],actors=[],pods=[],ricochets=[],anchor=null,phase=null,eclipse=0,marks=new Map(),poisons=new Map(),links=[],clock=0,sharing=false;
  const live=()=>running&&!paused&&!modalPause&&!roomTransition&&game.player?.hp>0;
  const point=o=>({x:o.x,y:o.y});
  const area=(id,r)=>r*(hasUpgrade(id,'reach')?1.3:1)*(window.AWRegionalContent?.spellArea(id)||1);
  const duration=(id,n)=>n*(hasUpgrade(id,'lasting')&&!direct.has(id)?1.4:1)*(SPELLS[id].damage===0&&hasUpgrade(id,'potency')?1.3:1);
  const power=(id,s,m)=>s.damage*m.power*(hasUpgrade(id,'potency')?1.3:1);
  const foes=(at,r)=>game.enemies.filter(e=>!e.dead&&dist(e,at)<=r+e.r);
  const ranged=e=>e.campaignRole==='ranged'||/archer|mage|witch|caster|oracle|eye|wisp|sentinel|necro|shaman/i.test(`${e.type} ${e.ai}`);
  const heavy=e=>e.boss||e.r>=.6;
  function field(id,at,r,life,damage=0,extra={}){
    const f={id,...at,r:area(id,r),life:duration(id,life),maxLife:duration(id,life),damage,tick:0,color:colors[SPELLS[id].category],...extra};
    fields.push(f);if(fields.length>48)fields.shift();return f;
  }
  function pulse(at,r,damage,tag,color){radialDamage(at.x,at.y,r,damage,tag);fx('shockRing',at.x,at.y,.5,color,{r});}
  function beam(a,b,width,damage,tag,color){
    fx('lightning',a.x,a.y,.28,color,{toX:b.x,toY:b.y,width:3});
    for(const e of [...game.enemies])if(!e.dead&&segmentDistance(e,a,b)<width+e.r)damageEnemy(e,damage,tag);
  }
  function segmentDistance(p,a,b){const x=b.x-a.x,y=b.y-a.y,t=clamp(((p.x-a.x)*x+(p.y-a.y)*y)/(x*x+y*y||1),0,1);return Math.hypot(p.x-a.x-x*t,p.y-a.y-y*t);}
  function shift(d,n){const p=game.player;p.x=clamp(p.x+d.x*n,.7,ROOM_W-.7);p.y=clamp(p.y+d.y*n,.7,ROOM_H-.7);}
  function actor(id,kind,at,life,hp,damage,extra={}){
    const a={id,kind,...at,r:kind==='treant'?.65:.4,life:duration(id,life),hp,maxHp:hp,damage,tick:0,color:colors[SPELLS[id].category],...extra};
    actors.push(a);if(actors.length>12)actors.shift();return a;
  }
  function target(at,r){return nearestEnemy(at,r,e=>marks.has(e))||nearestEnemy(at,r);}
  SPELL_CASTS.briarCage=(id,s,m)=>{const f=field(id,aimPoint(4),2,5,power(id,s,m),{inside:new Map()});for(const e of game.enemies)f.inside.set(e,dist(e,f)<f.r);};
  SPELL_CASTS.wildstep=(id,s,m)=>{
    const a=point(game.player),d=spellAim();shift(d,area(id,3.4));const b=point(game.player);
    for(const e of [...game.enemies])if(segmentDistance(e,a,b)<.7+e.r){damageEnemy(e,power(id,s,m),'nature');e.slow=2.5;}
    if(hasUpgrade(id,'thorns'))for(let i=0;i<3;i++)groundEffect('thorns',lerp(a.x,b.x,i/2),lerp(a.y,b.y,i/2),.65,3,'#9cdd89',s.damage*.2,.5);
    if(hasUpgrade(id,'haste'))game.player.tailwind=3;
    fx('lightning',a.x,a.y,.3,'#9cdd89',{toX:b.x,toY:b.y,width:3});
    if(hasUpgrade(id,'charge')){const st=game.player.spellState[id]||=( {cd:0});if(!st.reserveCd){st.reserveCd=s.cooldown*m.cdr;st.reserveUsed=false;st.cd=0;}else{st.reserveUsed=true;st.cd=st.reserveCd;}}
  };
  SPELL_CASTS.sporeburst=(id,s,m)=>{const d=spellAim();const q=magicProjectile({vx:d.x*6,vy:d.y*6,damage:power(id,s,m),life:.7,color:'#b4df76',kind:'spore'});pods.push({q,id,damage:power(id,s,m)});};
  SPELL_CASTS.guardianTreant=(id,s,m)=>actor(id,'treant',aimPoint(1.6),12,120+game.level*8,power(id,s,m));
  SPELL_CASTS.predatorsMark=id=>{const e=nearestEnemy(aimPoint(3),area(id,7));if(e)marks.set(e,{life:duration(id,8),bonus:hasUpgrade(id,'potency')?.39:.3});};
  SPELL_CASTS.emeraldRain=id=>field(id,aimPoint(2),2.8,7,0,{healing:hasUpgrade(id,'potency')?6.5:5});
  SPELL_CASTS.thunderstep=(id,s,m)=>{const a=point(game.player);shift(spellAim(),area(id,3.8));for(const at of [a,game.player]){pulse(at,1.5,power(id,s,m),'lightning','#8beaff');for(const e of foes(at,1.5))e.slow=2;}game.player.invuln=Math.max(game.player.invuln,.2);};
  SPELL_CASTS.magneticField=(id,s,m)=>field(id,aimPoint(3),3,6,power(id,s,m));
  SPELL_CASTS.crystalBarricade=id=>{const d=spellAim(),at=aimPoint(2);for(let i=-1;i<=1;i++)actor(id,'crystal',{x:clamp(at.x-d.y*i*area(id,.95),.7,ROOM_W-.7),y:clamp(at.y+d.x*i*area(id,.95),.7,ROOM_H-.7)},7,(45+game.level*4)*(hasUpgrade(id,'potency')?1.3:1),0,{r:.52});};
  SPELL_CASTS.prismRebound=(id,s,m)=>{
    const d=spellAim(),q=magicProjectile({vx:d.x*9,vy:d.y*9,damage:power(id,s,m),life:duration(id,3),pierce:7,kind:'iceLance',color:'#bcefff',onHit:(e,p)=>{rebound(p);const t=nearestEnemy(e,area(id,7),o=>o!==e&&!p.hit.has(o));if(t){const n=norm(t.x-p.x,t.y-p.y);p.vx=n.x*9;p.vy=n.y*9;}}});q.reboundBase=q.damage;q.bounces=0;ricochets.push(q);
  };
  function rebound(q){q.bounces++;q.damage=q.reboundBase*(1+Math.min(5,q.bounces)*.12);if(q.bounces>=8)q.life=0;}
  SPELL_CASTS.avalanche=(id,s,m)=>field(id,point(game.player),2,1.1,power(id,s,m),{dir:{...spellAim()},hit:new Set(),carried:new Set()});
  SPELL_CASTS.stormBeacon=(id,s,m)=>field(id,aimPoint(3),3.8,8,power(id,s,m));
  SPELL_CASTS.mirrorWalk=id=>{actor(id,'mirror',point(game.player),5,45,0,{dir:{...game.player.facing}});game.player.tailwind=duration(id,3);};
  SPELL_CASTS.soulChain=id=>{links=foes(aimPoint(3),area(id,4)).slice(0,4).map(e=>({e,life:duration(id,6),share:hasUpgrade(id,'potency')?.325:.25}));};
  SPELL_CASTS.umbralPassage=(id,s,m)=>{phase={life:duration(id,2),damage:power(id,s,m),r:area(id,2.4)};};
  SPELL_CASTS.astralSentinel=(id,s,m)=>actor(id,'sentinel',point(game.player),10,65+game.level*3,power(id,s,m));
  SPELL_CASTS.gravityInversion=(id,s,m)=>{const f=field(id,aimPoint(4),2.8,1,power(id,s,m));for(const e of foes(f,f.r)){e.slow=3;if(!heavy(e))e.stun=Math.max(e.stun||0,f.life);}};
  SPELL_CASTS.eclipse=id=>{eclipse=duration(id,8);};
  SPELL_CASTS.constellationSpear=(id,s,m)=>field(id,point(game.player),.38,.8,power(id,s,m),{dir:{...spellAim()}});
  SPELL_CASTS.rewind=id=>{const p=game.player;anchor={...point(p),hp:p.hp,shield:p.shield,shieldTime:p.shieldTime,life:duration(id,5)};toastMsg('Rewind anchored — cast again to return.');};
  SPELL_CASTS.seedSentry=(id,s,m)=>actor(id,'seed',aimPoint(2),8,35+game.level*2,power(id,s,m));
  SPELL_CASTS.brambleTether=(id,s,m)=>{const e=nearestEnemy(aimPoint(3),area(id,5));if(!e)return;damageEnemy(e,power(id,s,m),'nature');const d=norm(game.player.x-e.x,game.player.y-e.y);if(!heavy(e)){e.x+=d.x*Math.min(2,dist(e,game.player)-.8);e.y+=d.y*Math.min(2,dist(e,game.player)-.8);e.stun=.8;}e.slow=2;fx('lightning',game.player.x,game.player.y,.4,'#9cdd89',{toX:e.x,toY:e.y,width:3});};
  SPELL_CASTS.pollenVeil=id=>{field(id,point(game.player),2.4,5);game.player.shield=Math.max(game.player.shield,(20+game.level)*(hasUpgrade(id,'potency')?1.3:1));game.player.shieldTime=duration(id,5);};
  SPELL_CASTS.cinderMine=(id,s,m)=>field(id,aimPoint(3),1.5,9,power(id,s,m),{armed:.5});
  SPELL_CASTS.hailOrbit=(id,s,m)=>field(id,point(game.player),1.8,6,power(id,s,m),{angle:0});
  SPELL_CASTS.fulguriteLance=(id,s,m)=>{const d=spellAim();magicProjectile({vx:d.x*11,vy:d.y*11,life:area(id,1),pierce:5,damage:power(id,s,m),color:'#b5eeff',kind:'iceLance',tag:'lightning',onHit:e=>{if(e.slow>0){damageEnemy(e,power(id,s,m)*.4,'lightning');const t=nearestEnemy(e,3,o=>o!==e);if(t)beam(e,t,.1,power(id,s,m)*.3,'lightning','#b5eeff');}}});};
  SPELL_CASTS.emberRecall=(id,s,m)=>{actor(id,'ember',point(game.player),3,35,power(id,s,m));const d=spellAim();shift({x:-d.x,y:-d.y},area(id,3));};
  SPELL_CASTS.soulHarvest=(id,s,m)=>{let healed=0;for(const e of foes(game.player,area(id,3.5))){damageEnemy(e,power(id,s,m)*(e.hp<e.maxHp*.3?2:1),'soul');if(e.dead&&healed<12){healPlayer(4);healed+=4;}}fx('shockRing',game.player.x,game.player.y,.6,'#cf8cff',{r:area(id,3.5)});};
  SPELL_CASTS.starweave=(id,s,m)=>{const f=field(id,point(game.player),2.7,6,power(id,s,m));f.points=[0,1,2].map(i=>({x:f.x+Math.cos(i*TAU/3)*f.r,y:f.y+Math.sin(i*TAU/3)*f.r}));};
  SPELL_CASTS.hourglassMine=(id,s,m)=>field(id,aimPoint(3),2.2,10,power(id,s,m),{armed:.5,triggered:false});
  const baseMods=spellMods;
  const direct=new Set(['wildstep','thunderstep','brambleTether','fulguriteLance','soulHarvest','gravityInversion','constellationSpear']);
  for(const id of direct)UPGRADE_POOLS[id][2][3]='Reduce this spell’s cooldown by 20%.';
  spellMods=function(id){const m=baseMods(id);if(direct.has(id)&&hasUpgrade(id,'lasting'))m.cdr*=.8;return m;};
  const baseCast=castSpell;
  castSpell=function(slot){
    if(!live()||phase)return;
    const id=game.player.activeSpells[slot];
    if(id==='rewind'&&anchor){
      if(window.AWCampaign?.inCampaign())AWCampaign.state().riding=false;
      const p=game.player;p.x=anchor.x;p.y=anchor.y;p.hp=Math.min(p.maxHp,anchor.hp);p.shield=anchor.shield;p.shieldTime=anchor.shieldTime;if(hasUpgrade('rewind','reach')){p.shield+=15;p.shieldTime=Math.max(p.shieldTime,3);}p.invuln=Math.max(p.invuln,.3);
      fx('shockRing',p.x,p.y,.6,'#c4a6ff',{r:2});anchor=null;updateHUD();return;
    }
    return baseCast(slot);
  };
  const baseAuto=autoAttack;autoAttack=function(dt){if(!phase)return baseAuto(dt);};
  const baseHurt=damagePlayer;damagePlayer=function(...args){if(!phase)return baseHurt(...args);};
  const baseMove=playerMovement;
  playerMovement=function(dt){const p=game.player,old=p.speed;if(phase)p.speed*=1.75;try{return baseMove(dt);}finally{p.speed=old;}};
  const baseDamage=damageEnemy;
  damageEnemy=function(e,amount,tag='',dot=false){
    if(!e||e.dead)return;
    const mark=marks.get(e);
    if(mark&&tag.includes('weapon'))amount*=1+mark.bonus;
    if(tag==='lightning'&&fields.some(f=>f.id==='stormBeacon'&&dist(game.player,f)<f.r))amount*=1.2;
    if(eclipse>0){if(/soul|void|shadow/.test(tag))amount*=1.25;if(/star|celestial/.test(tag)&&Math.random()<.25)amount*=1.6;}
    const hp=e.hp;baseDamage(e,amount,tag,dot);const dealt=Math.max(0,hp-Math.max(0,e.hp));
    if(!sharing&&dealt&&links.some(l=>l.e===e)){
      sharing=true;try{for(const l of links)if(l.e!==e&&!l.e.dead)baseDamage(l.e,dealt*l.share,'soulLink',true);}finally{sharing=false;}
    }
    if(e.dead&&mark){marks.delete(e);const st=game.player.spellState.predatorsMark;if(st)st.cd*=.5;}
    if(dealt&&hasUpgrade('briarCage','bloodroot')&&fields.some(f=>f.id==='briarCage'&&dist(e,f)<f.r))healPlayer(Math.min(1,dealt*.05));
  };
  const baseNearest=nearestEnemy;
  nearestEnemy=function(origin,range=999,filter=()=>true){
    // Existing wisps also honor the hunt mark, without changing player auto-aim.
    if(game.summons?.includes(origin))return baseNearest(origin,range,e=>marks.has(e)&&filter(e))||baseNearest(origin,range,filter);
    return baseNearest(origin,range,filter);
  };
  const baitFor=e=>actors.find(a=>a.life>0&&a.hp>0&&['treant','mirror','ember'].includes(a.kind)&&dist(e,a)<area(a.id,e.boss?2.5:5));
  const baseAI=updateEnemyAI;
  updateEnemyAI=function(e,d,range,speed,dt){
    if(eclipse>0&&!e.boss&&e.state==='idle'&&range>area('eclipse',5))return;
    const bait=baitFor(e);
    if(bait&&e.state==='idle'){
      const delta=norm(bait.x-e.x,bait.y-e.y),dd=dist(e,bait);
      if(dd>e.r+bait.r+.15)moveEnemy(e,delta,speed,dt);
      if(e.attack<=0){if(ranged(e)){enemyProjectile(e,delta,4.5);e.attack=1.6;}else if(dd<e.r+bait.r+.35){bait.hp-=e.damage;e.attack=1.2;}}
      return;
    }
    return baseAI(e,d,range,speed,dt);
  };
  const baseEnemies=updateEnemies;
  updateEnemies=function(dt){
    baseEnemies(dt);
    for(const e of game.enemies)for(const a of actors)if(a.hp>0&&a.life>0&&['treant','crystal'].includes(a.kind)){
      const dd=dist(e,a),r=e.r+a.r;if(dd<r){const d=dd>.001?norm(e.x-a.x,e.y-a.y):{x:1,y:0};e.x=clamp(a.x+d.x*r,.4,ROOM_W-.4);e.y=clamp(a.y+d.y*r,.4,ROOM_H-.4);
        if(a.kind==='crystal'&&e.attack<=0){a.hp-=e.damage;e.attack=1;}}
    }
  };
  const baseProjectiles=updateProjectiles;
  updateProjectiles=function(dt){
    for(const q of game.projectiles){
      if(q.life<=0)continue;
      if(q.owner==='enemy'){
        const end={x:q.x+q.vx*dt,y:q.y+q.vy*dt};
        for(const a of actors)if(a.hp>0&&a.life>0&&segmentDistance(a,q,end)<a.r+q.r){a.hp-=q.damage;q.life=0;break;}
        for(const f of fields)if(['magneticField','pollenVeil'].includes(f.id)&&dist(q,f)<f.r&&!q.continentalWeakened){q.continentalWeakened=true;q.damage*=.65;q.vx*=.8;q.vy*=.8;}
      }
    }
    // Resolve room-edge rebounds before the legacy loop discards out-of-bounds shots.
    for(const q of ricochets)if(q.life>0){let bounced=false;
      if(q.x+q.vx*dt<.3||q.x+q.vx*dt>ROOM_W-.3){q.vx*=-1;bounced=true;}
      if(q.y+q.vy*dt<.3||q.y+q.vy*dt>ROOM_H-.3){q.vy*=-1;bounced=true;}
      if(bounced)rebound(q);
    }
    game.projectiles=game.projectiles.filter(q=>q.life>0);baseProjectiles(dt);
    ricochets=ricochets.filter(q=>q.life>0);
  };
  function updateActor(a,dt){
    a.life-=dt;a.tick-=dt;
    if(a.kind==='mirror'){a.x=clamp(a.x+a.dir.x*dt*1.5,.7,ROOM_W-.7);a.y=clamp(a.y+a.dir.y*dt*1.5,.7,ROOM_H-.7);}
    if(a.kind==='sentinel'){a.x=lerp(a.x,game.player.x+1,Math.min(1,dt*3));a.y=lerp(a.y,game.player.y-1,Math.min(1,dt*3));}
    const t=target(a,area(a.id,7));
    if(a.kind==='treant'&&t&&dist(t,a)>a.r+t.r+.2){const d=norm(t.x-a.x,t.y-a.y);a.x+=d.x*dt*.8;a.y+=d.y*dt*.8;}
    if(a.life<=0||a.hp<=0){if(a.kind==='ember')pulse(a,area(a.id,2),a.damage,'fire',a.color);return;}
    if(a.tick>0||!t)return;
    if(a.kind==='treant'){if(dist(a,t)<area(a.id,1.6)){pulse(a,area(a.id,1.4),a.damage,'nature',a.color);a.tick=1.3;}}
    if(a.kind==='seed'){const d=norm(t.x-a.x,t.y-a.y);magicProjectile({x:a.x,y:a.y,vx:d.x*7,vy:d.y*7,damage:a.damage,life:1.3,seek:1,color:a.color,tag:'nature'});a.tick=.85;}
    if(a.kind==='sentinel'){
      const candidates=foes(a,area(a.id,8)).sort((x,y)=>(marks.has(y)?100:0)+(y.telegraph?30:0)+(ranged(y)?10:0)-dist(y,a)-((marks.has(x)?100:0)+(x.telegraph?30:0)+(ranged(x)?10:0)-dist(x,a)));
      const e=candidates[0]||t;beam(a,e,.08,a.damage,'celestial',a.color);a.tick=.85;
    }
  }
  function poison(e,f){const p=poisons.get(e)||{life:3,tick:0,stacks:0,damage:f.damage};p.life=3;p.stacks=Math.min(5,p.stacks+1);p.damage=Math.max(p.damage,f.damage);poisons.set(e,p);}
  function updateField(f,dt){
    f.life-=dt;f.tick-=dt;const tick=f.tick<=0;if(tick)f.tick=.5;
    switch(f.id){
      case 'briarCage':
        for(const e of game.enemies){const inside=dist(e,f)<f.r,old=f.inside.get(e);if(old!==undefined&&inside!==old){damageEnemy(e,f.damage*(hasUpgrade(f.id,'iron')?2:1),'nature');if(old&&!heavy(e)){const d=norm(e.x-f.x,e.y-f.y);e.x=f.x+d.x*(f.r-e.r);e.y=f.y+d.y*(f.r-e.r);}}
          f.inside.set(e,dist(e,f)<f.r);if(inside&&hasUpgrade(f.id,'prison'))e.slow=Math.max(e.slow||0,.6);}
        break;
      case 'sporeburst':
        if(tick)for(const e of foes(f,f.r))poison(e,f);
        if(!f.secondary&&hasUpgrade(f.id,'spread'))for(const [e,p] of poisons)if(e.dead&&!p.spread&&dist(e,f)<f.r){p.spread=true;field(f.id,point(e),1,2.5,f.damage*.55,{secondary:true});}
        break;
      case 'emeraldRain':
        if(dist(game.player,f)<f.r)healPlayer(f.healing*dt);
        for(const a of actors)if(dist(a,f)<f.r)a.hp=Math.min(a.maxHp,a.hp+f.healing*dt*2);
        for(const [e,p] of poisons)if(!e.dead&&dist(e,f)<f.r)p.life=Math.max(p.life,3);
        break;
      case 'magneticField':
        for(const e of foes(f,f.r))if(ranged(e)&&!heavy(e)){const d=norm(f.x-e.x,f.y-e.y);e.x+=d.x*dt*.6;e.y+=d.y*dt*.6;}
        if(tick)pulse(f,f.r,f.damage,'lightning',f.color);break;
      case 'avalanche':
        f.x+=f.dir.x*dt*6;f.y+=f.dir.y*dt*6;
        for(const e of foes(f,f.r)){if(!f.hit.has(e)){f.hit.add(e);damageEnemy(e,f.damage,'frost');}e.slow=Math.max(e.slow||0,3);if(!heavy(e)){e.x=clamp(e.x+f.dir.x*dt*4,.4,ROOM_W-.4);e.y=clamp(e.y+f.dir.y*dt*4,.4,ROOM_H-.4);}}break;
      case 'stormBeacon':if(tick){const e=target(f,f.r);if(e)beam(f,e,.08,f.damage,'lightning',f.color);f.tick=.85;}break;
      case 'gravityInversion':if(f.life<=0)pulse(f,f.r,f.damage,'arcane',f.color);break;
      case 'constellationSpear':
        f.dir={...spellAim()};
        if(f.life<=0)beam(f,{x:f.x+f.dir.x*24,y:f.y+f.dir.y*24},f.r,f.damage,'celestial',f.color);break;
      case 'pollenVeil':f.x=game.player.x;f.y=game.player.y;break;
      case 'cinderMine':case 'hourglassMine':
        f.armed-=dt;if(!f.triggered&&f.armed<=0&&foes(f,f.r).length){f.triggered=true;f.life=f.id==='hourglassMine'?1:.15;}
        if(f.triggered){for(const e of foes(f,f.r))e.slow=Math.max(e.slow||0,1.5);if(f.life<=0)pulse(f,f.r,f.damage,f.id==='cinderMine'?'fire':'arcane',f.color);}break;
      case 'hailOrbit':
        f.x=game.player.x;f.y=game.player.y;f.angle+=dt*3;
        if(tick)for(let i=0;i<3;i++){const a=f.angle+i*TAU/3,at={x:f.x+Math.cos(a)*f.r,y:f.y+Math.sin(a)*f.r};for(const e of foes(at,.65)){damageEnemy(e,f.damage,'frost');e.slow=1.5;}}break;
      case 'starweave':
        if(tick)for(let i=0;i<3;i++)beam(f.points[i],f.points[(i+1)%3],.3,f.damage,'celestial',f.color);
        // Inscribed circle is fully inside the triangle; no ward outside its edges.
        if(dist(game.player,f)<f.r*.5){if(game.player.shield<35)game.player.shield=Math.min(35,game.player.shield+dt*5);game.player.shieldTime=Math.max(game.player.shieldTime,1);}break;
    }
  }
  const baseUpdate=updateSpellEntities;
  updateSpellEntities=function(dt){
    if(!live())return;baseUpdate(dt);clock+=dt;
    const st=game.player.spellState.wildstep;
    if(st?.reserveCd>0){st.reserveCd=Math.max(0,st.reserveCd-dt);if(st.reserveCd===0){st.reserveUsed=false;st.cd=0;}}
    if(anchor){anchor.life-=dt;if(anchor.life<=0)anchor=null;}
    if(phase){phase.life-=dt;if(phase.life<=0){const f=phase;phase=null;pulse(game.player,f.r,f.damage,'void','#cf8cff');}}
    eclipse=Math.max(0,eclipse-dt);
    for(const [e,m] of marks){m.life-=dt;if(m.life<=0||e.dead)marks.delete(e);}
    links=links.filter(l=>{l.life-=dt;return l.life>0&&!l.e.dead;});
    for(const entry of pods)if(entry.q.life<=0){field(entry.id,point(entry.q),1.7,6,entry.damage);entry.done=true;}
    pods=pods.filter(p=>!p.done);
    for(const a of actors)updateActor(a,dt);actors=actors.filter(a=>a.hp>0&&a.life>0);
    for(const f of [...fields])updateField(f,dt);fields=fields.filter(f=>f.life>0);
    for(const [e,p] of poisons){p.life-=dt;p.tick-=dt;if(!e.dead&&p.tick<=0){p.tick=.5;damageEnemy(e,p.damage*p.stacks*.3,'poison',true);}if(p.life<=0||(e.dead&&p.spread))poisons.delete(e);}
  };
  function reset(){fields=[];actors=[];pods=[];ricochets=[];anchor=null;phase=null;eclipse=0;marks.clear();poisons.clear();links=[];clock=0;}
  const baseLoad=loadRoom;loadRoom=function(){reset();const result=baseLoad();grantEarned();return result;};
  const baseDeath=playerDeath;playerDeath=function(){reset();return baseDeath();};

  // Town stock uses the existing purchase flow. Site rewards are awarded through
  // the campaign's own clear/claim paths and are recoverable for existing saves.
  function grantEarned(){
    if(!window.AWCampaign?.inCampaign())return false;
    const state=AWCampaign.state();let changed=false;
    for(const n of Object.values(AWCampaignData.nodes))if((['shrine','landmark','event'].includes(n.type)?state.claimed:state.cleared).includes(n.id))for(const id of n.rewardSpells||[])if(!game.player.unlocked.includes(id)){AWCampaign.unlockSpell(id);changed=true;}
    return changed;
  }
  const baseClear=markRoomCleared;markRoomCleared=function(){const wasCleared=game.roomData?.cleared;const result=baseClear();if(!wasCleared&&grantEarned())saveGame();return result;};
  const baseClaim=AWCampaign.claimSite;AWCampaign.claimSite=function(){const result=baseClaim();if(result){grantEarned();saveGame();}return result;};
  const basePool=weightedSpellPool;
  weightedSpellPool=function(){
    const region=window.AWCampaign?.current()?.continent,known=game.player?.unlocked||[];
    return basePool().filter(id=>{const s=definitions[id];if(!s||known.includes(id))return true;
      // Named discoveries cannot leak through the level-up/seer RNG. Shop spells
      // can also be discovered while adventuring on their own continent.
      return !!AWCampaignData.towns[s.source]&&(!region||region===s.region);
    });
  };
  const baseBar=renderSpellBar;renderSpellBar=function(){
    baseBar();if(!game.player)return;
    game.player.activeSpells.forEach((id,i)=>{const b=$('spells').children[i];if(!b||!definitions[id])return;
      if(id==='rewind'){b.querySelector('.spell-name').textContent=anchor?'Return':'Rewind';if(anchor){b.classList.remove('aw-unavailable');b.style.setProperty('--cooldown','0deg');b.querySelector('.cooldown-text').textContent=anchor.life.toFixed(1);b.setAttribute('aria-label',`Return to Rewind anchor · ${anchor.life.toFixed(1)} seconds remaining`);}}
      if(id==='wildstep'&&hasUpgrade(id,'charge'))b.querySelector('.aw-spell-tier').textContent=game.player.spellState[id]?.reserveCd>0?(game.player.spellState[id].reserveUsed?'0 charges':'1 charge'):'2 charges';
    });
  };
  function line(a,b,color,width=2){const x=worldToScreen(a.x,a.y,5),y=worldToScreen(b.x,b.y,5);ctx.strokeStyle=color;ctx.lineWidth=width;ctx.beginPath();ctx.moveTo(x.x,x.y);ctx.lineTo(y.x,y.y);ctx.stroke();}
  function ring(at,r,color){ctx.strokeStyle=color;ctx.lineWidth=2;ctx.beginPath();for(let i=0;i<=32;i++){const a=i*TAU/32,p=worldToScreen(at.x+Math.cos(a)*r,at.y+Math.sin(a)*r,2);if(i===0)ctx.moveTo(p.x,p.y);else ctx.lineTo(p.x,p.y);}ctx.stroke();}
  const baseFront=drawEffectsFront;
  drawEffectsFront=function(){
    baseFront();if(!game.player)return;ctx.save();
    if(eclipse>0){ctx.fillStyle='rgba(13,5,34,.32)';ctx.fillRect(0,0,W,H);ring(game.player,1.1,'#eee0ff');const d=game.player.facing;line(game.player,{x:game.player.x-d.x*1.5,y:game.player.y-d.y*1.5},'#eee0ff',3);}
    for(const f of fields){
      ctx.globalAlpha=Math.min(.8,f.life*2);ring(f,f.r,f.color);
      if(f.id==='constellationSpear')line(f,{x:f.x+f.dir.x*24,y:f.y+f.dir.y*24},f.color,1);
      if(f.id==='starweave')for(let i=0;i<3;i++)line(f.points[i],f.points[(i+1)%3],f.color);
      if(f.id==='briarCage')for(let i=0;i<12;i++){const a=i*TAU/12,p={x:f.x+Math.cos(a)*f.r,y:f.y+Math.sin(a)*f.r};const s=worldToScreen(p.x,p.y);ctx.strokeStyle=f.color;ctx.beginPath();ctx.moveTo(s.x-4,s.y);ctx.lineTo(s.x,s.y-14);ctx.lineTo(s.x+4,s.y);ctx.stroke();}
      if(f.id==='hailOrbit')for(let i=0;i<3;i++){const a=f.angle+i*TAU/3,p=worldToScreen(f.x+Math.cos(a)*f.r,f.y+Math.sin(a)*f.r,12);ctx.fillStyle=f.color;ctx.beginPath();ctx.arc(p.x,p.y,5,0,TAU);ctx.fill();}
      if(f.id==='stormBeacon'){const p=worldToScreen(f.x,f.y);ctx.strokeStyle=f.color;ctx.beginPath();ctx.moveTo(p.x,p.y);ctx.lineTo(p.x,p.y-40);ctx.stroke();}
      if(f.id==='emeraldRain')for(let i=0;i<8;i++){const a=i*TAU/8,p=worldToScreen(f.x+Math.cos(a)*f.r*.7,f.y+Math.sin(a)*f.r*.7,10+(clock*30+i*7)%24);ctx.strokeStyle=f.color;ctx.beginPath();ctx.moveTo(p.x,p.y);ctx.lineTo(p.x-2,p.y+7);ctx.stroke();}
    }
    ctx.globalAlpha=1;
    for(const a of actors){const p=worldToScreen(a.x,a.y);ctx.save();ctx.translate(p.x,p.y);ctx.fillStyle=a.color;ctx.strokeStyle=a.color;ctx.lineWidth=3;
      if(a.kind==='treant'){ctx.fillStyle='#786248';ctx.fillRect(-9,-34,18,35);ctx.fillStyle='#9cdd89';for(const [x,y,r] of [[0,-42,19],[-15,-30,12],[15,-30,12]]){ctx.beginPath();ctx.arc(x,y,r,0,TAU);ctx.fill();}ctx.fillStyle='#fff4b5';ctx.fillRect(-5,-25,3,4);ctx.fillRect(3,-25,3,4);}
      else if(a.kind==='crystal'){ctx.beginPath();ctx.moveTo(0,-40);ctx.lineTo(13,-9);ctx.lineTo(0,4);ctx.lineTo(-13,-9);ctx.closePath();ctx.fill();}
      else if(a.kind==='mirror'||a.kind==='ember'){ctx.globalAlpha=.55;ctx.beginPath();ctx.ellipse(0,-17,12,22,0,0,TAU);ctx.fill();}
      else {ctx.beginPath();ctx.arc(0,-18+Math.sin(clock*3)*3,a.kind==='seed'?9:13,0,TAU);ctx.stroke();ctx.fillRect(-3,-22,6,6);}
      ctx.globalAlpha=1;ctx.fillStyle='#1e2631';ctx.fillRect(-15,7,30,3);ctx.fillStyle=a.color;ctx.fillRect(-15,7,30*Math.max(0,a.hp/a.maxHp),3);ctx.restore();
    }
    for(const [e] of marks){line(game.player,e,'#c8ee9b',1);const p=worldToScreen(e.x,e.y,45);ctx.fillStyle='#c8ee9b';ctx.font='bold 16px sans-serif';ctx.fillText('⌖',clamp(p.x,18,W-30),clamp(p.y,26,H-30));}
    for(let i=1;i<links.length;i++)line(links[i-1].e,links[i].e,'#cf8cff',2);
    if(anchor)ring(anchor,.8,'#dbcaff');if(phase)ring(game.player,.8,'#cf8cff');ctx.restore();
  };
  window.AWContinentalSpells={ids:Object.keys(definitions),grantEarned,canRecast:id=>id==='rewind'&&!!anchor&&!phase,state:()=>({fields,actors,anchor,phase,eclipse,links,marks,poisons})};
})();
