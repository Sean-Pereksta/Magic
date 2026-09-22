'use strict';
/* Regional spells reuse projectiles, summons, reactions and simulation-time effects. */
(() => {
  const definitions={
    groveGuardian:{name:'Grove Guardians',icon:'🌿',rarity:'Rare',cooldown:13,damage:17,cast:'groveGuardian',category:'Nature',tags:['Summoning','Defensive'],region:'verdant',exclusive:true,desc:'Summon two healing woodland wisps for 10 seconds. Their shots defend your route.'},
    stormBridge:{name:'Storm Bridge',icon:'ϟ',rarity:'Epic',cooldown:10,damage:36,cast:'stormBridge',category:'Storm',tags:['Utility'],region:'meridian',exclusive:true,desc:'Blink forward and leave a slowing lightning path behind you.'},
    mirrorBastion:{name:'Mirror Bastion',icon:'◇',rarity:'Legendary',cooldown:15,damage:30,cast:'mirrorBastion',category:'Arcane',tags:['Defensive'],region:'gloam',exclusive:true,desc:'Raise a shield for 5 seconds. Nearby enemy projectiles become seeking mirror shards.'},
    springSnare:{name:'Spring Snare',icon:'❧',rarity:'Uncommon',cooldown:7,damage:42,cast:'springSnare',category:'Nature',tags:['Utility'],region:'verdant',desc:'Plant a hidden root trap at your aim point. An approaching enemy triggers a rooting bloom.'},
    prismRicochet:{name:'Prism Ricochet',icon:'💎',rarity:'Rare',cooldown:6,damage:29,cast:'prismRicochet',category:'Frost',tags:['Arcane'],region:'meridian',desc:'Launch a crystal shard that redirects toward a new enemy after each hit.'},
    starCauseway:{name:'Star Causeway',icon:'✧',rarity:'Epic',cooldown:11,damage:16,cast:'starCauseway',category:'Celestial',tags:['Utility'],region:'gloam',desc:'Lay a luminous road of five persistent runes that damage and slow crossing foes.'}
  };
  Object.assign(SPELLS,definitions);
  const mutations=[['wide','Wide Inscription','⭕','Increase the spell’s reach or area.'],['long','Enduring Rune','⌛','Extend summons, traps, fields or projectile lifetime.'],['power','Deep Channel','✦','Increase this spell’s damage by 30%.'],['ward','Keeper’s Pact','🛡','Casting grants an additional short ward.'],['echo','Twin Script','♊','Add a companion, trap, shard or field.'],['mend','Gentle Return','💚','Casting restores a small amount of health.']];
  for(const id of Object.keys(definitions))UPGRADE_POOLS[id]=mutations.map(([key,name,icon,desc])=>[`${id}_${key}`,name,icon,desc,key]);
  let fields=[];
  function prepare(id,s,m){
    const p=game.player;
    if(hasUpgrade(id,'ward')){p.shield=Math.max(p.shield,15+game.level);p.shieldTime=Math.max(p.shieldTime,4);}
    if(hasUpgrade(id,'mend'))healPlayer(5+game.level*.25);
    return s.damage*m.power*(hasUpgrade(id,'power')?1.3:1);
  }
  SPELL_CASTS.groveGuardian=(id,s,m)=>{
    const damage=prepare(id,s,m),count=hasUpgrade(id,'echo')?3:2,life=hasUpgrade(id,'long')?14:10;
    for(let i=0;i<count;i++)game.summons.push({kind:'wisp',x:game.player.x,y:game.player.y,z:20,angle:TAU*i/count,life,maxLife:life,damage,heal:true,guard:true,chain:hasUpgrade(id,'wide'),shot:.2*i,color:'#c1ec93'});
    fx('summon',game.player.x,game.player.y,.6,'#c1ec93',{r:1.5});
  };
  SPELL_CASTS.stormBridge=(id,s,m)=>{
    const damage=prepare(id,s,m),p=game.player,d=spellAim(),length=hasUpgrade(id,'wide')?5:3.5,start={x:p.x,y:p.y},count=hasUpgrade(id,'echo')?7:5;
    p.x=clamp(p.x+d.x*length,.7,ROOM_W-.7);p.y=clamp(p.y+d.y*length,.7,ROOM_H-.7);p.invuln=Math.max(p.invuln,.3);
    for(let i=0;i<count;i++){const t=i/(count-1);groundEffect('lightningField',lerp(start.x,p.x,t),lerp(start.y,p.y,t),.65,hasUpgrade(id,'long')?5:3,'#99efff',damage*.18,.5,{slow:true});}
    fx('lightning',start.x,start.y,.3,'#99efff',{toX:p.x,toY:p.y,width:4});
  };
  SPELL_CASTS.mirrorBastion=(id,s,m)=>{
    const damage=prepare(id,s,m),life=hasUpgrade(id,'long')?7:5;
    game.player.shield=Math.max(game.player.shield,35+game.level*2);game.player.shieldTime=Math.max(game.player.shieldTime,life);
    fields.push({kind:'mirror',life,damage,r:hasUpgrade(id,'wide')?2.4:1.6,echo:hasUpgrade(id,'echo'),tick:0});fx('ward',game.player.x,game.player.y,life,'#d7c4ff',{follow:true});
  };
  SPELL_CASTS.springSnare=(id,s,m)=>{
    const damage=prepare(id,s,m),p=aimPoint(4),life=hasUpgrade(id,'long')?12:8,r=hasUpgrade(id,'wide')?2:1.3;
    for(let i=0;i<(hasUpgrade(id,'echo')?2:1);i++){const x=clamp(p.x+i*1.6,.7,ROOM_W-.7),y=p.y;fields.push({kind:'trap',x,y,r,life,damage});fx('castRing',x,y,life,'#a9dd84',{r:r*.6});}
  };
  SPELL_CASTS.prismRicochet=(id,s,m)=>{
    const damage=prepare(id,s,m),d=spellAim();
    for(let i=0;i<(hasUpgrade(id,'echo')?2:1);i++)magicProjectile({vx:d.x*8+d.y*i,vy:d.y*8-d.x*i,damage,color:'#b0edff',kind:'iceLance',r:.12,life:hasUpgrade(id,'long')?4:2.6,pierce:5,onHit:(e,p)=>{
      const target=nearestEnemy(e,hasUpgrade(id,'wide')?8:5,a=>a!==e&&!p.hit.has(a.id));if(target){const dir=norm(target.x-p.x,target.y-p.y);p.vx=dir.x*8;p.vy=dir.y*8;}e.slow=Math.max(e.slow||0,1.2);
    }});
  };
  SPELL_CASTS.starCauseway=(id,s,m)=>{
    const damage=prepare(id,s,m),d=spellAim(),count=hasUpgrade(id,'echo')?7:5;
    for(let i=0;i<count;i++){const x=clamp(game.player.x+d.x*(i+1),.7,ROOM_W-.7),y=clamp(game.player.y+d.y*(i+1),.7,ROOM_H-.7);groundEffect('lightningField',x,y,hasUpgrade(id,'wide')?.95:.65,hasUpgrade(id,'long')?7:5,'#fff0bc',damage,.6,{slow:true});}
  };
  const baseUpdate=updateSpellEntities;
  updateSpellEntities=function(dt){
    for(const summon of game.summons)if(!summon.campaignKinApplied){if(equippedItems().some(t=>t.special==='groveKin'))summon.damage*=1.2;summon.campaignKinApplied=true;}
    baseUpdate(dt);if(!running||paused||modalPause||roomTransition)return;
    for(const f of fields){
      f.life-=dt;
      if(f.kind==='trap'&&game.enemies.some(e=>!e.dead&&dist(e,f)<f.r)){
        radialDamage(f.x,f.y,f.r,f.damage,'nature');for(const e of game.enemies)if(!e.dead&&dist(e,f)<f.r)e.stun=Math.max(e.stun,e.boss?.2:1.4);
        fx('bloom',f.x,f.y,.6,'#bcea9b',{r:f.r});f.life=0;
      }else if(f.kind==='mirror'){
        f.tick-=dt;if(f.tick>0)continue;f.tick=.15;
        const shots=game.projectiles.filter(p=>p.owner==='enemy'&&p.life>0&&dist(p,game.player)<f.r).slice(0,f.echo?4:2);
        for(const p of shots){p.owner='player';p.vx=-p.vx;p.vy=-p.vy;p.hit=new Set();p.damage=f.damage;p.seek=1.8;p.color='#dfcaff';p.onHit=null;}
      }
    }
    fields=fields.filter(f=>f.life>0).slice(-20);
  };
  const baseLoad=loadRoom;loadRoom=function(){fields=[];return baseLoad();};
  const baseWeighted=weightedSpellPool;
  weightedSpellPool=function(){
    const all=baseWeighted(),known=game.player?.unlocked||[],region=window.AWCampaign?.current()?.continent;
    const result=all.filter(id=>!definitions[id]?.exclusive||known.includes(id));
    if(region)for(const id of result.slice())if(definitions[id]?.region===region)result.push(id);
    return result;
  };
  INTENSITY_REACTIONS[intensityReactionKey('stormBridge','frostnova')]='conductive';
  INTENSITY_REACTIONS[intensityReactionKey('springSnare','firebolt')]='toxic';
  INTENSITY_REACTIONS[intensityReactionKey('starCauseway','tempest')]='stormmoon';
})();
