/* Per-journey talents. This module is deterministic given the run and the
   engine's seeded random source. Ability time advances in combat only. */
(() => {
  'use strict';
  const upgrades = Object.create(null), abilities = [];
  const fmt = n => Number(n.toFixed(2)).toString();
  const pct = n => fmt(100*n)+'%';
  const linear = step => rank => step*rank;
  // Percent chances and reductions approach a ceiling, while each rank still helps.
  const soft = (first, ceiling) => rank => ceiling*rank/(rank+ceiling/first-1);
  const rankOf = (run,id) => run.skills?.[id]||0;
  const magnitude = (r,id) => upgrades[id].value(rankOf(r,id));
  function passive(id,name,category,element,icon,value,explain,extra={}) {
    upgrades[id]={id,name,category,element,icon,kind:'passive',value,explain,description:explain(1),...extra};
  }
  function ability(id,name,element,icon,cooldown,power,growth,description,explain) {
    const d={id,name,element,icon,category:'Abilities',kind:'ability',cooldown,power,growth,description,explain,pattern:id};
    upgrades[id]=d;abilities.push(d);
  }
  const bonus=(id,name,cat,el,icon,step,prefix,extra)=>passive(id,name,cat,el,icon,linear(step),n=>prefix+' +'+pct(step*n)+'.',extra);
  const chance=(id,name,cat,el,icon,first,limit,action,extra)=>{const fn=soft(first,limit);passive(id,name,cat,el,icon,fn,n=>pct(fn(n))+' chance '+action+'.',extra);};
  // Six affinities each support two different builds: larger hits or shorter cooldowns.
  const schools=[
    ['leaf','Bramble Script','Quickening Sap','stick','nettle'],
    ['fire','Kiln Hymn','Flash Wick','torch','matchbox'],
    ['frost','Glacier Oath','Slipstream Ice','shard','skate'],
    ['storm','Thunderhead','Live Circuit','drum','crank'],
    ['stone','Mountain Grip','Millwheel','mallet','quoit'],
    ['spirit','Midnight Ink','Ghost Hands','brush','ribbon']
  ];
  for(const [el,focus,quick,picture,clock] of schools) {
    bonus(el+'_focus',focus,'Offense',el,picture,.20,'Power of '+el+' relics',{affinity:el});
    const haste=soft(.12,.65);
    passive(el+'_tempo',quick,'Tempo',el,clock,haste,n=>'Cooldown of '+el+' relics shortened by '+pct(haste(n))+'.',{affinity:el});
  }
  // Situational offensive bonuses add together before multiplying the direct hit.
  const strikes=[
    ['long_arm','Long Reach','stone','sling',.11,'All relic hits',()=>true],
    ['first_light','First Light','fire','marble',.22,'Relic hits against foes above 75% health',(r,e)=>e.hp>e.maxHp*.75],
    ['finishing_cut','Finishing Cut','frost','blade',.28,'Relic hits against foes below 40% health',(r,e)=>e.hp<e.maxHp*.4],
    ['duelist','Duelist’s Knot','spirit','ribbon',.19,'Relic hits against a lone remaining foe',(r,e,count)=>count===1],
    ['crowdwork','Crowd Work','stone','boomerang',.23,'Relic hits while two or more foes live',(r,e,count)=>count>=2],
    ['cornered','Cornered Courage','fire','fang',.35,'Relic hits while Pip is below half health',r=>r.hp<r.maxHp*.5],
    ['iron_stance','Iron Stance','stone','shell',.22,'Relic hits while Pip has a shield',r=>r.shield>0],
    ['giantkiller','Giantkiller','storm','vane',.32,'Relic hits against guardians',(r,e)=>e.boss],
    ['thermal_shock','Thermal Shock','fire','tongs',.27,'Relic hits against chilled, frozen or stunned foes',(r,e)=>e.chill>0||e.stun>0||e.freeze>0],
    ['felt_flame','Fever Pitch','leaf','pepper',.25,'Relic hits against burning or poisoned foes',(r,e)=>e.burn>0||e.poison>0]
  ];
  for(const [id,name,el,icon,delta,when] of strikes)bonus(id,name,'Offense',el,icon,delta,when);
  // Disciplines apply to the potency of their offensive and utility styles.
  const disciplines=[
    ['needlework','Needlework','frost','needle',['pierce','rapid'],'Pierce and rapid relic hits'],
    ['smokehouse','Smokehouse','fire','oil',['burn','poison'],'Burn and poison relic hits'],
    ['dreamcraft','Dreamcraft','spirit','seal',['chill','shock','stun','freeze'],'Chill, shock, stun and freeze relic power'],
    ['field_medic','Field Medic','leaf','gourd',['mend','drain','heal'],'Mend, drain and healing relic power'],
    ['anvil_school','Anvil School','stone','hammer',['guard','strike','shield'],'Guard, strike and shield relic power'],
    ['fused_edge','Fused Edge','storm','coil',null,'Power of forged relics']
  ];
  for(const [id,name,el,icon,styles,what] of disciplines)bonus(id,name,'Discipline',el,icon==='hammer'?'mallet':icon,.19,what,styles?{styles}:{forged:true});
  const defenses=[
    ['ironbark','Ironbark','leaf','shell',.085,.64,'Enemy strike damage',()=>true],
    ['boss_ward','Gatekeeper','spirit','mask',.19,.74,'Damage from guardians',(r,e)=>!!e?.boss],
    ['last_stand','Last Stand','stone','idol',.17,.70,'Enemy strike damage while below half health',r=>r.hp<r.maxHp*.5],
    ['wardweave','Wardweave','frost','mirror',.15,.68,'Enemy strike damage while shielded',r=>r.shield>0],
    ['steel_nerves','Steel Nerves','storm','washer',.14,.66,'Enemy strike damage while above 75% health',r=>r.hp>r.maxHp*.75]
  ];
  for(const [id,name,el,icon,first,limit,what] of defenses){
    const fn=soft(first,limit);passive(id,name,'Survival',el,icon,fn,n=>what+' reduced by '+pct(fn(n))+'.');
  }
  chance('sidestep','Sidestep','Survival','storm','sail',.08,.48,'to evade an incoming hit entirely');
  bonus('green_thumb','Green Thumb','Recovery','leaf','clover',.16,'Health restored by healing');
  passive('restorative','Campfire Soup','Recovery','fire','mortar',linear(3),n=>'Heal '+fmt(3*n)+' extra HP after each cleared wave.');
  passive('carry_ward','Bark Lining','Recovery','leaf','acorn',linear(4),n=>'Gain '+fmt(4*n)+' shield after each cleared wave, up to the shield cap.');
  chance('high_grade','Prospector’s Eye','Foraging','stone','pick',.24,.86,'for a newly generated drop to gain one level');
  const haste=soft(.07,.50);
  passive('quick_hands','Quick Hands','Tempo','storm','feather',haste,n=>'All relic cooldowns shortened by '+pct(haste(n))+'.');
  const overture=soft(.12,.65);
  passive('light_touch','Light Touch','Tempo','leaf','seed',overture,n=>'Relics with a base cooldown of 2 seconds or less recharge '+pct(overture(n))+' sooner.',{fast:true});
  passive('counterweight','Counterweight','Tempo','stone','idol',overture,n=>'Relics with a base cooldown of at least 2.6 seconds recharge '+pct(overture(n))+' sooner.',{heavy:true});
  const spellHaste=soft(.115,.62);
  passive('sky_dial','Sky Dial','Tempo','storm','hourglass',spellHaste,n=>'Ability cooldowns shortened by '+pct(spellHaste(n))+'.');
  // On-hit talents deal secondary damage only once per relic activation.
  chance('ricochet','Ricochet','Technique','stone','sling',.32,.90,'to bounce 55% of a relic hit onto another foe');
  chance('scatter','Splinter Shell','Technique','leaf','shell',.22,.78,'to spray every other foe for 32% of a relic hit');
  chance('wicklash','Kindling Thread','Technique','fire','torch',.28,.84,'for a relic hit to add a burn worth 60% of its damage');
  chance('nettletip','Nettle Tips','Technique','leaf','thorn',.24,.80,'for a relic hit to add poison worth 14% of its damage per second');
  chance('shiver','Cold Snap','Technique','frost','shard',.22,.76,'for a relic hit to chill its target for 1.4 seconds');
  chance('interrupt','Static Hitch','Technique','storm','fork',.19,.72,'for a relic hit to stun its target for 0.45 seconds');
  chance('sip','Night Sip','Technique','spirit','wick',.24,.83,'for a relic hit to restore HP equal to 18% of damage dealt');
  chance('safety_net','Safety Net','Technique','leaf','kite',.22,.84,'for a relic hit to grant shield equal to 30% of its damage');
  bonus('spellcraft','Spellcraft','Abilities','spirit','crown',.18,'Damage, healing and shield strength of abilities');
  const bronze=soft(.11,.72);
  passive('second_wind','Second Wind','Tempo','storm','sail',bronze,n=>'A wave clear removes '+pct(bronze(n))+' of remaining ability cooldowns.');
  passive('scavenger','Scavenger’s Stash','Foraging','leaf','gourd',linear(1),n=>'A newly generated drop gains '+fmt(n)+' levels while the backpack holds at least eight relics.');
  chance('double_stamp','Double Stamp','Foraging','fire','seal',.12,.65,'for a newly generated drop to gain two extra levels');
  passive('moss_mat','Moss Mat','Recovery','leaf','ribbon',linear(.015),n=>'After a wave, restore '+pct(.015*n)+' of Pip’s maximum health.');
  passive('homeward','Homeward Chime','Recovery','spirit','bell',linear(4),n=>'Clearing a guardian wave restores '+fmt(4*n)+' extra HP and grants as much shield.');
  // Ability descriptions quote base magnitude at wave 1. Each later wave adds
  // 8% of that base. Spellcraft scales that same magnitude, including healing.
  const base=(p,g,n)=>p+g*(n-1);
  function spell(id,name,el,icon,cd,p,g,summary,details){
    ability(id,name,el,icon,cd,p,g,summary,n=>details(base(p,g,n))+' Scales with wave number.');
  }
  spell('razor_bloom','Razor Bloom','leaf','boomerang',5.2,11,5,'Leaf blades cut up to three foes.',n=>'Slash up to three foes for '+fmt(n)+' damage each.');
  spell('seismic_bell','Seismic Bell','stone','bell',8,12,6,'A fault line hits the whole formation and interrupts attack windups.',n=>'Deal '+fmt(n)+' damage to every foe and rewind each attack by 25%.');
  spell('cinder_swarm','Cinder Swarm','fire','matchbox',7,7,4,'Embers hit and ignite every living foe.',n=>'Hit all foes for '+fmt(n)+' and add a burn worth '+fmt(n*.8)+' damage.');
  spell('whiteout','Whiteout','frost','flute',8.5,11,5,'A sheet of ice chills the whole formation.',n=>'Deal '+fmt(n)+' damage to each foe and chill them for 2.1 seconds.');
  spell('forked_sky','Forked Sky','storm','fork',6.5,13,6,'Two bolts stun the two foes closest to attacking.',n=>'Strike the two fullest attack meters for '+fmt(n)+' damage and stun them for 0.65 seconds.');
  spell('jade_shelter','Jade Shelter','leaf','clover',9,8,4,'A leaf canopy heals Pip and gives him a shield.',n=>'Restore '+fmt(n)+' HP and grant '+fmt(n*.8)+' shield.');
  spell('moon_siphon','Moon Siphon','spirit','mirror',6.8,15,7,'Moonlight drains the foe with the most current health.',n=>'Deal '+fmt(n)+' to the healthiest foe and recover 40% of the HP removed.');
  spell('clockwork_lunge','Clockwork Lunge','stone','crank',7.5,12,6,'A clockwork jab winds every equipped relic forward.',n=>'Hit the front foe for '+fmt(n)+' damage and advance each relic cooldown by 0.85 seconds.');
  const read = (r,id) => rankOf(r,id) ? magnitude(r,id) : 0;
  const boundedHp = (value,max) => Math.max(0,Math.min(max,value));
  function describe(id,rank=1) {
    const d=upgrades[id];
    return d ? d.explain(Math.max(1,rank)) : '';
  }
  function duration(r,id) {
    const d=upgrades[id];return d?.kind==='ability' ? d.cooldown*(1-read(r,'sky_dial')) : 0;
  }
  function apply(r,id) {
    const d=upgrades[id];if(!d)return false;
    r.skills??={};r.abilityTimers??={};
    const next=1+rankOf(r,id);r.skills[id]=next;
    if(d.kind==='ability'&&next===1)r.abilityTimers[id]=Math.min(.4,d.cooldown);
    if(id==='sky_dial')for(const a of abilities){
      if(r.abilityTimers[a.id]!=null)r.abilityTimers[a.id]=Math.min(r.abilityTimers[a.id],duration(r,a.id));
    }
    return true;
  }
  function cooldown(r,d) {
    let cd=d.cooldown*(1-read(r,d.element+'_tempo'))*(1-read(r,'quick_hands'));
    if(d.cooldown<=2)cd*=1-read(r,'light_touch');
    if(d.cooldown>=2.6)cd*=1-read(r,'counterweight');
    return Math.max(.15,cd);
  }
  function hitMultiplier(r,d,e,livingCount=1) {
    let extra=read(r,d.element+'_focus');
    for(const entry of strikes){const id=entry[0],applies=entry.at(-1);if(rankOf(r,id)&&applies(r,e,livingCount))extra+=magnitude(r,id);}
    for(const [id,,,,styles] of disciplines)if(rankOf(r,id)&&(styles?styles.includes(d.style):d.recipe))extra+=magnitude(r,id);
    return 1+extra;
  }
  function utilityMultiplier(r,d) {
    if(!r)return 1;
    let extra=read(r,d.element+'_focus');
    for(const [id,,,,styles] of disciplines)if(rankOf(r,id)&&(styles?styles.includes(d.style):d.recipe))extra+=magnitude(r,id);
    return 1+extra;
  }
  function event(ev,kind,element,pattern,label,e,extra) {
    const v={kind,element,pattern,label,age:0,strength:1,...extra};
    if(e){v.targetUid=e.uid;v.enemyId=e.id;v.lane=e.lane;}
    ev.push(v);
  }
  function deal(e,value){const removed=Math.min(e.hp,value);e.hp=Math.max(0,e.hp-value);return removed;}
  function healing(r,value){return value*(1+read(r,'green_thumb'))*(1+.02*(r.legacy?.ranks?.medicinal_sap||0));}
  function shield(r,value){const old=r.shield;r.shield=boundedHp(old+value,r.maxHp*.4);return r.shield-old;}
  function restore(r,value){if(r.hp<=0)return 0;const old=r.hp,total=healing(r,value);r.hp=boundedHp(old+total,r.maxHp);globalThis.Progression.afterHealing?.(r,total,r.hp-old);return r.hp-old;}
  function incoming(r,value,e,ev,rng) {
    if(value<=0)return 0;
    if(rankOf(r,'sidestep')&&rng()<magnitude(r,'sidestep')){
      event(ev,'status','storm','guard','Evaded');return 0;
    }
    for(const entry of defenses){const id=entry[0],applies=entry.at(-1);if(rankOf(r,id)&&applies(r,e))value*=1-magnitude(r,id);}
    return value;
  }
  function afterHit(r,enemies,e,damage,d,ev,rng) {
    if(damage<=0||['heal','shield','stun','freeze'].includes(d.style))return;
    const proc=id=>rankOf(r,id)>0&&rng()<magnitude(r,id);
    if(rankOf(r,'ricochet')||rankOf(r,'scatter')){
      const others=enemies.filter(target=>target!==e&&target.hp>0);
      if(others.length&&proc('ricochet')){
        const t=others[Math.floor(rng()*others.length)],n=deal(t,damage*.55);
        event(ev,'hit',d.element,'rapid',String(Math.ceil(n)),t);
      }
      if(others.length&&proc('scatter'))for(const t of others)if(t.hp>0){
        const n=deal(t,damage*.32);event(ev,'hit','leaf','rapid',String(Math.ceil(n)),t);
      }
    }
    if(e.hp>0){
      if(proc('wicklash')){e.burn+=damage*.60*(1+.05*(r.legacy?.masteries?.fire||0));event(ev,'status','fire','burn','Burn',e);}
      if(proc('nettletip')){e.poison+=damage*.14*(1+.05*(r.legacy?.masteries?.leaf||0));event(ev,'status','leaf','poison','Poison',e);}
      if(proc('shiver')){e.chill=Math.max(e.chill,1.4);event(ev,'status','frost','chill','Chill',e);}
      if(proc('interrupt')){e.stun=Math.max(e.stun,.45);event(ev,'status','storm','shock','Stun',e);}
    }
    if(proc('sip')){const n=restore(r,damage*.18*(1+.04*(r.legacy?.masteries?.spirit||0)));if(n>.01)event(ev,'heal','spirit','lifesteal','+'+Math.ceil(n),e,{sourceUid:e.uid});}
    if(proc('safety_net')){const n=shield(r,damage*.30);if(n>.01)event(ev,'status','leaf','guard','Shield +'+Math.ceil(n));}
  }
  function victory(r,ev){
    const guardian=r.round%4===0,extra=guardian?read(r,'homeward'):0;
    const hp=restore(r,read(r,'restorative')+r.maxHp*read(r,'moss_mat')+extra);
    const ward=shield(r,read(r,'carry_ward')+extra);
    if(hp>.01)event(ev,'heal','leaf','heal','+'+Math.ceil(hp));
    if(ward>.01)event(ev,'status','leaf','guard','Shield +'+Math.ceil(ward));
    const cut=read(r,'second_wind');
    if(cut&&r.abilityTimers)for(const key of Object.keys(r.abilityTimers))r.abilityTimers[key]*=(1-cut);
  }
  function dropBonus(r,rng){
    let level=r.pack.length>=8?read(r,'scavenger'):0;
    if(rankOf(r,'high_grade')&&rng()<magnitude(r,'high_grade'))level++;
    if(rankOf(r,'double_stamp')&&rng()<magnitude(r,'double_stamp'))level+=2;
    return level;
  }
  function cast(r,targets,d,ev){
    const id=d.id,n=base(d.power,d.growth,rankOf(r,id))*(1+.08*(r.round-1))*(1+read(r,'spellcraft'));
    const extra={abilityId:id,strength:rankOf(r,id)};
    if(id==='jade_shelter'){
      const heal=restore(r,n),ward=shield(r,n*.8);
      event(ev,'ability',d.element,id,'+'+Math.ceil(heal)+' HP · +'+Math.ceil(ward)+' shield',null,extra);return;
    }
    let chosen=targets;
    if(id==='razor_bloom')chosen=targets.slice(0,3);
    if(id==='forked_sky')chosen=targets.slice().sort((a,b)=>b.attackProgress-a.attackProgress).slice(0,2);
    if(id==='moon_siphon')chosen=[targets.reduce((a,b)=>a.hp>=b.hp?a:b)];
    if(id==='clockwork_lunge'){
      chosen=targets.slice(0,1);
      for(const it of r.active)it.cd=Math.max(0,it.cd-.85);
    }
    for(const target of chosen){
      const removed=deal(target,n);
      if(id==='seismic_bell')target.attackProgress=Math.max(0,target.attackProgress-.25);
      if(id==='cinder_swarm')target.burn+=n*.8*(1+.05*(r.legacy?.masteries?.fire||0));
      if(id==='whiteout')target.chill=Math.max(target.chill,2.1);
      if(id==='forked_sky')target.stun=Math.max(target.stun,.65);
      if(id==='moon_siphon'){const hp=restore(r,removed*.4*(1+.04*(r.legacy?.masteries?.spirit||0)));if(hp>.01)event(ev,'heal','spirit','lifesteal','+'+Math.ceil(hp),target,{sourceUid:target.uid});}
      event(ev,'ability',d.element,id,String(Math.ceil(removed)),target,extra);
    }
  }
  function tickAbilities(r,enemies,dt,ev) {
    if(!r.abilityTimers||!(dt>0)||!enemies.some(e=>e.hp>0))return;
    for(const d of abilities){
      if(!rankOf(r,d.id))continue;
      const remaining=(r.abilityTimers[d.id]??.4)-dt;
      if(remaining>0){r.abilityTimers[d.id]=remaining;continue;}
      const living=enemies.filter(e=>e.hp>0);
      if(!living.length){r.abilityTimers[d.id]=0;continue;}
      cast(r,living,d,ev);
      r.abilityTimers[d.id]=duration(r,d.id);
    }
  }
  const all=Object.values(upgrades),starterChoices=['long_arm','quick_hands','ironbark','razor_bloom','jade_shelter','high_grade'];
  function offer(r,rng,count=3){
    const gear=[...r.active,...r.pack].map(i=>globalThis.Catalog.items[i.id]).filter(Boolean);
    const suited=d=>(!d.affinity||gear.some(i=>i.element===d.affinity))&&(!d.styles||gear.some(i=>d.styles.includes(i.style)))&&(!d.forged||gear.some(i=>i.recipe))&&(!d.fast||gear.some(i=>i.cooldown<=2))&&(!d.heavy||gear.some(i=>i.cooldown>=2.6));
    const pool=all.filter(d=>suited(d)||rankOf(r,d.id)>0);
    const out=[];
    const take=(filter)=>{
      const choices=pool.filter(filter).filter(d=>!out.includes(d));if(!choices.length)return false;
      const d=choices[Math.min(choices.length-1,Math.floor(rng()*choices.length))];out.push(d);return true;
    };
    // A starter draw always offers an active spell plus a broadly useful passive.
    // Later encounters often bring back an owned talent so a build can specialize.
    if(count>0){
      const owned=pool.some(d=>rankOf(r,d.id));
      if(!owned||rng()<.33)take(d=>d.kind==='ability');
      else if(rng()<.65)take(d=>rankOf(r,d.id)>0);
    }
    if(out.length<count&&!pool.some(d=>rankOf(r,d.id)))take(d=>starterChoices.includes(d.id)&&d.kind==='passive');
    while(out.length<count&&out.length<pool.length){
      if(!take(d=>out.every(v=>d.category!==v.category)))take(()=>true);
    }
    return out.map(d=>d.id);
  }
  globalThis.Progression={upgrades,abilities,starterChoices,rankOf,apply,offer,describe,duration,cooldown,hitMultiplier,utilityMultiplier,afterHit,incoming,healing,victory,dropBonus,tickAbilities};
})();
