/* Living Legacy combat. Only primary relic activations trigger activation
   talents. Secondary hits, summons, death bursts and spells never re-enter the
   primary path. All timers advance exclusively through the engine combat clock. */
(() => {
  'use strict';
  const P=globalThis.Progression,C=globalThis.Catalog,old={...P},U=P.upgrades;
  const elements=Object.keys(C.elements),support=new Set(['heal','shield','stun','freeze']);
  const rank=(r,id)=>P.rankOf(r,id),permanent=(r,id)=>r.legacy?.ranks?.[id]||0;
  const mastery=(r,id)=>r.legacy?.masteries?.[id]||0,oath=(r,id)=>r.legacy?.oaths?.includes(id);
  const capstone=(r,id)=>r.legacy?.milestones?.includes('ascendant_'+id);
  const speciesById=new Map(C.enemies.map(e=>[e.id,e.species])),speciesCache=new WeakMap();
  function knowsSpecies(r,e){if(!r.legacy)return false;let known=speciesCache.get(r.legacy);if(!known){known=new Set((r.legacy.seenEnemies||[]).map(id=>speciesById.get(id)));speciesCache.set(r.legacy,known);}return known.has(e.species);}
  const equipped=r=>[...new Set(r.active.map(it=>C.items[it.id]?.element).filter(Boolean))];
  const fmt=n=>Number(n.toFixed(2)).toString(),pct=n=>fmt(n*100)+'%';
  const add=(id,name,element,description,extra={})=>U[id]={id,name,element,icon:({leaf:'thorn',fire:'torch',frost:'shard',storm:'fork',stone:'idol',spirit:'crown'})[element],category:'Elemental',kind:'passive',rarity:'rare',maxRank:5,value:n=>n,description,explain:n=>description+(extra.maxRank===1?'':' Rank '+n+': effects scale by '+n+'×.'),requires:[element],...extra};
  const ordinary=[
    ['thorn_volley','Thorn Volley','leaf','Every third damaging Leaf activation fires two thorns, each for 35% of the hit per rank.'],
    ['creeping_grove','Creeping Grove','leaf','Rooted enemies take 12% more damage per rank; roots last 15% longer per rank.'],
    ['nectar_bloom','Nectar Bloom','leaf','Every fourth healing or shielding Leaf activation restores 4% maximum HP per rank.'],
    ['vine_companion','Vine Companion','leaf','Summon a permanent vine that attacks, poisons and roots. Each rank strengthens it.'],
    ['cinderburst','Cinderburst','fire','Burning deaths explode for 18% of the defeated enemy’s maximum HP per rank.'],
    ['kindled_momentum','Kindled Momentum','fire','Fire activations advance other Fire relics by 0.15 seconds per rank.'],
    ['wildfire_furnace','Wildfire Furnace','fire','Burn damage ticks 20% faster per rank.'],
    ['phoenix_feather','Phoenix Feather','fire','Prevent fatal damage once every eight waves, restoring 20% HP and igniting foes; each rank reduces the interval by one wave.'],
    ['glass_shards','Glass Shards','frost','Hits against frozen foes shatter for 40% additional damage per rank.'],
    ['chill_cascade','Chill Cascade','frost','Chilled deaths spread two seconds of chill per rank to other foes.'],
    ['winters_grip','Winter’s Grip','frost','Freeze duration increases 15% per rank, up to the eight second control limit.'],
    ['frostbite_bulwark','Frostbite Bulwark','frost','Frost activations grant 2 shield per chilled or frozen foe per rank.'],
    ['arc_jump','Arc Jump','storm','Storm hits have a 35% chance to jump for 45% damage per rank.'],
    ['overclock','Overclock','storm','Storm activations against controlled foes advance equipment by 0.12 seconds per rank.'],
    ['capacitor','Capacitor','storm','Every fifth Storm activation releases a bolt for 250% hit damage per rank.'],
    ['charged_barrier','Charged Barrier','storm','Absorbing shield damage retaliates for 40% of absorbed damage per rank.'],
    ['rolling_stone','Rolling Stone','stone','Stone hits bounce to the next enemy for 40% damage per rank.'],
    ['fracture_point','Fracture Point','stone','Stone hits add fracture up to 5 + 2 per rank and amplify hits by 2% per fracture per rank.'],
    ['granite_echo','Granite Echo','stone','Every third shield-generating activation releases a Stone counterattack for 20 damage per rank, scaling with waves.'],
    ['titans_patience','Titan’s Patience','stone','Relics with base cooldowns of at least 2.6 seconds gain 25% damage per rank.'],
    ['phantom_twin','Phantom Twin','spirit','Spirit hits have a 25% chance to repeat at 45% strength per rank.'],
    ['soul_satchel','Soul Satchel','spirit','Enemy deaths restore 2% maximum HP and grant 2% maximum HP as shield per rank.'],
    ['haunt_amplifier','Haunt Amplifier','spirit','Spirit hits add 35% hit damage as a haunting echo per rank.'],
    ['phantom_surge','Phantom Surge','spirit','Automatic abilities add a Spirit strike for 12 damage per rank, scaling with waves.']
  ];
  ordinary.forEach(x=>add(...x));
  const pairs=[
    ['wildfire_bloom','Wildfire Bloom','leaf','fire','Poisoned, burning targets spread both ailments and suffer a 35% damage burst per rank.'],
    ['stormglass','Stormglass','frost','storm','Storm hits on chilled or frozen foes chain for 60% damage per rank.'],
    ['molten_fault','Molten Fault','fire','stone','Fire hits on burning foes add two fracture per rank.'],
    ['wraithvine','Wraithvine','leaf','spirit','Hits against rooted foes create a Spirit eruption for 45% hit damage per rank.'],
    ['ghostflame','Ghostflame','spirit','fire','Spirit hits against burning foes restore 20% of damage per rank.'],
    ['icehammer','Icehammer','stone','frost','Stone hits against frozen foes cause 65% splash damage per rank.'],
    ['tempest_bloom','Tempest Bloom','leaf','storm','Storm hits spread 40% of existing poison per rank to other foes.']
  ];
  for(const [id,name,a,b,description] of pairs)add(id,name,a,description,{category:'Combinations',requires:[a,b],rarity:'epic'});
  add('prismatic_oath','Prismatic Oath','spirit','Each distinct equipped element increases relic potency by 8% per rank.',{category:'Combinations',requires:[],minElements:2,rarity:'epic'});
  const generals=[
    ['rapid_overload','Rapid Overload','storm','Rapid relic activations add a 30% follow-up hit per rank.'],
    ['heavy_followthrough','Heavy Followthrough','stone','Heavy relic activations splash 35% hit damage per rank.'],
    ['ritual_gear','Ritual Gear','spirit','Every third support activation advances automatic abilities by 0.5 seconds per rank.'],
    ['overflowing_tonic','Overflowing Tonic','leaf','Convert 30% of overhealing per rank into shields.'],
    ['last_light','Last Light','spirit','Survive one fatal hit per expedition at 15% HP per rank, up to 75%.'],
    ['guardian_spoils','Guardian Spoils','stone','Guardian victories grant one extra relic find per rank.'],
    ['relic_appraiser','Relic Appraiser','stone','New finds have a 12% chance per rank, up to 60%, to gain an extra level.'],
    ['scavenger_cache','Scavenger Cache','leaf','Every sixth cleared wave grants one extra relic find per rank.']
  ];
  for(const x of generals)add(...x,{category:'General',requires:[],rarity:'uncommon'});
  const spells=[
    ['briar_barrage','Briar Barrage','leaf',6,18,8,'Three poisonous thorn attacks.'],
    ['inferno_ring','Inferno Ring','fire',9,27,12,'Ignite all foes; burning foes suffer a second explosion.'],
    ['crystal_chorus','Crystal Chorus','frost',10,2,0.3,'Freeze the two foes closest to attacking. No direct damage.'],
    ['chainstorm','Chainstorm','storm',7,22,10,'Four lightning bolts jump between foes.'],
    ['avalanche_guard','Avalanche Guard','stone',9,30,14,'Gain a large shield and 35% damage resistance for three seconds. No direct damage.'],
    ['spectral_guardian','Spectral Guardian','spirit',12,18,8,'Summon a ghost for eight seconds that attacks and restores health.']
  ];
  for(const [id,name,el,cooldown,power,growth,description] of spells){
    const d=add(id,name,el,description,{kind:'ability',category:'Abilities',cooldown,power,growth,pattern:id,explain:n=>description+' Potency '+fmt(power+growth*(n-1))+', scaling with waves. Cooldown '+cooldown+' seconds.'});P.abilities.push(d);
  }
  const myths=[
    ['worldroot_awakening','Worldroot Awakening','leaf',['leaf'],'Summon two permanent vines that attack, poison and root.'],
    ['cinder_apocalypse','Cinder Apocalypse','fire',['fire'],'Every ten seconds, explode the entire formation for immense Fire damage. Burning deaths chain explosions.'],
    ['crystal_cathedral','Crystal Cathedral','frost',['frost'],'Guardians and their escorts begin frozen for eight seconds; shatter hits deal triple damage.'],
    ['storm_kings_wrath','Storm King’s Wrath','storm',['storm'],'Every sixth Storm activation unleashes six thunderbolts.'],
    ['mountain_awakening','Mountain Awakening','stone',['stone'],'Start battles with 35% HP shielding, take 35% less damage and unleash crushing counters every fourth strike.'],
    ['second_soul','Second Soul','spirit',['spirit'],'Once every eight waves, fatal damage restores 50% HP and summons a Spirit duplicate.'],
    ['chronobloom','Chronobloom','leaf',[],'Every twelve seconds recharge all equipped relics and advance automatic abilities by five seconds.'],
    ['ancestral_forge','Ancestral Forge','stone',[],'Immediately raise every equipped relic by two levels; each guardian raises another equipped relic.'],
    ['swarm_sovereign','Swarm Sovereign','leaf',['leaf'],'Command three permanent vine familiars with twice normal vine potency.'],
    ['worldroot_network','Worldroot Network','leaf',['leaf','spirit'],'Every defeated enemy becomes a Spirit-infused plant until the encounter ends.'],
    ['infernal_domino','Infernal Domino','fire',['fire'],'Burning deaths explode for 80% maximum HP and ignite the entire formation, causing chain reactions.'],
    ['solar_cataclysm','Solar Cataclysm','fire',['fire','storm'],'Every nine seconds strike all foes twice with Fire and lightning; ignite and stun them.'],
    ['absolute_zero','Absolute Zero','frost',['frost'],'Every twelve seconds freeze every living enemy for five seconds.'],
    ['shattered_eternity','Shattered Eternity','frost',['frost','stone'],'Stone hits against frozen enemies send 300% damage shockwaves through the formation.'],
    ['thunder_tyrant','Thunder Tyrant','storm',['storm'],'Every Storm hit creates three additional lightning chains for 80% damage each.'],
    ['perpetual_circuit','Perpetual Circuit','storm',[],'Every seventh primary relic activation creates one independent echo of each equipped relic, preserving support roles.'],
    ['colossus_awakens','Colossus Awakens','stone',['stone'],'Summon an enormous permanent Stone guardian that attacks and shields Pip.'],
    ['the_shattered_earth','The Shattered Earth','stone',['stone','fire'],'Heavy Stone or Fire attacks create lava fissures for 220% damage across the formation.'],
    ['phantom_legion','Phantom Legion','spirit',['spirit'],'Defeated enemies create phantom warriors until the battle ends.'],
    ['soul_of_eternity','Soul of Eternity','spirit',['spirit'],'Spirit attacks grant lifesteal and shields; frozen deaths restore 12% HP and shields.'],
    ['tempest_of_seasons','Tempest of Seasons','leaf',['leaf','frost','storm'],'Every eight seconds unleash a Leaf, Frost and Storm pulse that roots, freezes and shocks the formation.'],
    ['the_sixfold_crown','The Sixfold Crown','spirit',[],'Master all six elements permanently. Every seven seconds unleash all six elements; potency scales with equipped diversity.']
  ];
  for(const [id,name,el,requires,description] of myths)add(id,name,el,description,{category:'Mythic',rarity:'mythic',maxRank:1,requires,minRegion:id==='the_sixfold_crown'?15:3,allMasteries:id==='the_sixfold_crown'});
  const constellations=[
    ['ashroot','Ashroot','leaf','fire','Burning poisoned foes burst.'],['wintervine','Wintervine','leaf','frost','Chilled foes are rooted more readily.'],
    ['sapwire','Sapwire','leaf','storm','Lightning spreads poison.'],['ironroot','Ironroot','leaf','stone','Roots grant shielding.'],
    ['wraithbloom','Wraithbloom','leaf','spirit','Poisoned deaths heal Pip.'],['thermal_fracture','Thermal Fracture','fire','frost','Fire shatters frozen foes.'],
    ['voltage_furnace','Voltage Furnace','fire','storm','Lightning against burning foes releases sparks.'],['magma_fault','Magma Fault','fire','stone','Fire builds fracture.'],
    ['cursed_ember','Cursed Ember','fire','spirit','Spirit haunting ignites foes.'],['prism_circuit','Prism Circuit','frost','storm','Lightning chains through frozen foes.'],
    ['avalanche_heart','Avalanche Heart','frost','stone','Stone shatters frozen foes.'],['moonwinter','Moonwinter','frost','spirit','Frozen deaths heal and shield Pip.'],
    ['magnetfall','Magnetfall','storm','stone','Fractured foes take stronger lightning chains.'],['soul_voltage','Soul Voltage','storm','spirit','Spirit attacks accelerate abilities.'],
    ['grave_monument','Grave Monument','stone','spirit','Shielded haunting is stronger; guardian damage is reduced.']
  ];
  for(const [id,name,a,b,description] of constellations)add('constellation_'+id,name,a,description+' Expedition talent multiplies the permanent effect by five per rank.',{category:'Constellations',rarity:'epic',requires:[a,b],constellation:id,maxRank:3});
  // Existing talent rarity remains independent of relic rarity.
  for(const d of Object.values(U))d.rarity??=d.kind==='ability'?'uncommon':'common';
  function state(r){return r.living??={timers:{},counters:{},buffs:{},companions:[],processedDeaths:[],phoenixUsed:false,lastForestWave:-4,lastFeatherWave:-99};}
  function event(ev,kind,element,pattern,label,e,extra={}){
    if(!ev)return;const v={kind,element,pattern,label,age:0,strength:1,...extra};if(e)Object.assign(v,{targetUid:e.uid,enemyId:e.id,lane:e.lane});
    // Aggregate the busiest builds without dropping combat resolution.
    if(ev.length<180)ev.push(v);
  }
  function hit(r,e,n,el,ev,pattern='nova'){
    if(!e||e.hp<=0||n<=0)return 0;
    const match=tag=>Array.isArray(tag)?tag.includes(el):tag===el;
    n*=match(e.weak)?1.68:match(e.resist)?.6:1;
    const removed=Math.min(e.hp,n);e.hp=Math.max(0,e.hp-n);event(ev,'ability',el,pattern,String(Math.ceil(removed)),e);return removed;
  }
  const alive=enemies=>enemies.filter(e=>e.hp>0);
  function shield(r,n,ev,el='stone'){const before=r.shield;r.shield=Math.min(r.maxHp*.4,r.shield+n);if(r.shield>before)event(ev,'status',el,'guard','Shield +'+Math.ceil(r.shield-before));return r.shield-before;}
  function heal(r,n,ev,el='spirit'){if(r.hp<=0)return 0;const before=r.hp,total=P.healing(r,n);r.hp=Math.min(r.maxHp,r.hp+total);P.afterHealing(r,total,r.hp-before);if(r.hp>before)event(ev,'heal',el,'heal','+'+Math.ceil(r.hp-before));return r.hp-before;}
  const power=r=>(12+r.round*2)*(1+rank(r,'spellcraft')*.18);
  function advance(r,n,abilities=false,element=null){
    if(abilities){for(const k of Object.keys(r.abilityTimers))r.abilityTimers[k]=Math.max(0,r.abilityTimers[k]-n);}
    else for(const it of r.active)if(!element||C.items[it.id].element===element)it.cd=Math.max(0,it.cd-n);
  }
  function count(r,id,mod){const s=state(r);s.counters[id]=((s.counters[id]||0)+1)%mod;return s.counters[id]===0;}
  function constellation(r,id){const def=constellations.find(x=>x[0]===id),els=equipped(r);return r.legacy?.constellations?.includes(id)&&def.slice(2,4).every(e=>els.includes(e))?1+5*rank(r,'constellation_'+id):0;}
  function companion(r,id,element,n,life=0,extra={}){
    const s=state(r);if(s.companions.length>=24){const first=s.companions.find(x=>!x.permanent);if(first){first.power+=n;first.life=Math.max(first.life,life);}return;}
    s.companions.push({id,element,power:n,timer:.2,life,permanent:life===0,poison:0,root:0,lifesteal:0,...extra});
  }
  function syncCompanions(r){
    const s=state(r),ensure=(id,num,el,mult,extra={})=>{
      const have=s.companions.filter(c=>c.id===id).length;
      for(let i=have;i<num;i++)companion(r,id,el,power(r)*mult,0,extra);
      for(const c of s.companions.filter(c=>c.id===id))c.power=power(r)*mult;
    };
    if(rank(r,'vine_companion'))ensure('vine',1,'leaf',.6*rank(r,'vine_companion'),{poison:.12,root:.5});
    if(oath(r,'worldroot'))ensure('worldroot',1,'leaf',1.3*(1+Math.floor((r.guardians||0)/4)*.4),{poison:.2,root:.7});
    if(rank(r,'worldroot_awakening'))ensure('awakening',2,'leaf',1.4,{poison:.2,root:1});
    if(rank(r,'swarm_sovereign'))ensure('swarm',3,'leaf',2,{poison:.25,root:1.2});
    if(rank(r,'colossus_awakens'))ensure('colossus',1,'stone',5);
  }
  P.onStart=(r,ev=[])=>{state(r);syncCompanions(r);};
  P.onSpawn=(r,ev=[])=>{
    const s=state(r);s.processedDeaths=[];s.companions=s.companions.filter(c=>c.permanent);syncCompanions(r);
    if(oath(r,'mountain')||rank(r,'mountain_awakening'))shield(r,r.maxHp*(rank(r,'mountain_awakening')?.35:.25),ev);
    const foes=alive(r.enemies);
    if(oath(r,'eternal_winter')&&foes.length){const e=foes.reduce((a,b)=>a.damage/a.period>b.damage/b.period?a:b);e.freeze=Math.max(e.freeze,3);event(ev,'ability','frost','crystal_chorus','Eternal Winter',e);}
    if(rank(r,'crystal_cathedral')&&r.round%4===0)for(const e of foes){e.freeze=8;event(ev,'ability','frost','crystal_chorus','Crystal Cathedral',e);}
  };
  P.apply=(r,id)=>{
    const d=U[id];if(!d||d.maxRank&&rank(r,id)>=d.maxRank)return false;
    if(!old.apply(r,id))return false;
    state(r);if(id==='ancestral_forge')for(const it of r.active)it.level+=2;
    syncCompanions(r);return true;
  };
  P.cooldown=(r,d)=>Math.max(.15,old.cooldown(r,d)*(d.element==='storm'?1-.015*mastery(r,'storm'):1));
  P.duration=(r,id)=>old.duration(r,id);
  P.hitMultiplier=(r,d,e,livingCount)=>{
    let n=old.hitMultiplier(r,d,e,livingCount);
    n+=rank(r,'prismatic_oath')*.08*equipped(r).length;
    if(e.rooted>0)n+=rank(r,'creeping_grove')*.12;
    if(d.cooldown>=2.6)n+=rank(r,'titans_patience')*.25;
    n+=rank(r,'fracture_point')*.02*(e.fracture||0);
    if(oath(r,'chimera'))n+=equipped(r).length*.08;
    if(d.element==='stone'&&capstone(r,'stone')&&state(r).buffs.stoneCharge>0)n+=.5;
    if(permanent(r,'monster_scholar')&&(r.legacy.seenEnemies?.includes(e.id)||knowsSpecies(r,e)))n+=.03*permanent(r,'monster_scholar');
    return n;
  };
  P.utilityMultiplier=(r,d)=>old.utilityMultiplier(r,d)*(d.element==='stone'&&['shield','guard'].includes(d.style)?1+.04*mastery(r,'stone'):1);
  P.healing=(r,n)=>old.healing(r,n);
  P.afterHealing=(r,total,restored)=>{if(rank(r,'overflowing_tonic')&&total>restored)shield(r,(total-restored)*.3*rank(r,'overflowing_tonic'));};
  P.incoming=(r,n,e,ev,rng)=>{
    n=old.incoming(r,n,e,ev,rng)*(1-.015*permanent(r,'thickened_bark'));
    if(rank(r,'mountain_awakening'))n*=.65;
    if(state(r).buffs.guard>0)n*=.65;
    if(e?.boss&&r.shield>0&&constellation(r,'grave_monument'))n*=Math.max(.5,1-.04*constellation(r,'grave_monument'));
    return n;
  };
  P.onActivation=(r,enemies,e,d,damage,ev,rng)=>{
    const s=state(r),n=power(r),el=d.element,offense=!support.has(d.style)&&damage>0,others=alive(enemies).filter(t=>t!==e),owned=id=>rank(r,id);
    if(owned('kindled_momentum')&&el==='fire')advance(r,.15*owned('kindled_momentum'),false,'fire');
    if(owned('nectar_bloom')&&el==='leaf'&&['heal','shield','mend','guard'].includes(d.style)&&count(r,'nectar',4))heal(r,r.maxHp*.04*owned('nectar_bloom'),ev,'leaf');
    if(support.has(d.style)&&owned('ritual_gear')&&count(r,'ritual',3))advance(r,.5*owned('ritual_gear'),true);
    if(el==='frost'&&owned('frostbite_bulwark'))shield(r,enemies.filter(t=>t.chill>0||t.freeze>0).length*2*owned('frostbite_bulwark'),ev,'frost');
    if(el==='frost'&&e){e.freeze=Math.min(8,(e.freeze||0)*(1+.15*owned('winters_grip')));e.chill=Math.min(8,(e.chill||0)*(1+.03*mastery(r,'frost')));}
    if(el==='leaf'&&e&&e.rooted>0)e.rooted=Math.min(8,e.rooted*(1+.15*owned('creeping_grove')+.02*mastery(r,'leaf')));
    if(el==='storm'&&owned('overclock')&&(e?.stun>0||e?.freeze>0))advance(r,.12*owned('overclock'));
    if(el==='spirit'&&constellation(r,'soul_voltage'))advance(r,.08*constellation(r,'soul_voltage'),true);
    if(el==='leaf'&&capstone(r,'leaf')&&e?.rooted>0)for(const t of others.slice(0,1))t.poison=Math.max(t.poison,e.poison*.25);
    if(el==='frost'&&capstone(r,'frost')&&e?.freeze>0)shield(r,r.maxHp*.015,ev,'frost');
    if(el==='storm'&&capstone(r,'storm')&&count(r,'stormAscendant',6))advance(r,.6);
    if(el==='stone'&&capstone(r,'stone')){if(['shield','guard'].includes(d.style))s.buffs.stoneCharge=1;else if(offense)s.buffs.stoneCharge=0;}
    if(el==='spirit'&&capstone(r,'spirit')&&['heal','mend','drain'].includes(d.style))shield(r,Math.max(2,damage*.08),ev,'spirit');
    if(el==='spirit'&&support.has(d.style)&&owned('phantom_twin')&&rng()<.25){
      const recent=ev[ev.length-1],echo=.45*owned('phantom_twin');
      if(d.style==='heal')heal(r,(recent?.amount||0)*echo,ev,'spirit');
      else if(d.style==='shield')shield(r,(recent?.amount||0)*echo,ev,'spirit');
      else if(e)e[d.style]=Math.min(8,(e[d.style]||0)*(1+echo));
      event(ev,'ability','spirit','spectral_guardian','Phantom Twin',e);
    }
    if((d.style==='shield'||d.style==='guard')&&owned('granite_echo')&&count(r,'granite',3)&&alive(enemies).length)hit(r,alive(enemies)[0],n*1.6*owned('granite_echo'),'stone',ev,'avalanche_guard');
    if(owned('perpetual_circuit')&&count(r,'circuit',7)){
      for(const it of r.active){const def=C.items[it.id],base=def.power*(1+.31*(it.level-1));
        if(def.style==='heal')heal(r,base,ev,def.element);else if(def.style==='shield')shield(r,base,ev,def.element);
        else if(['stun','freeze'].includes(def.style)){const t=alive(enemies)[0];if(t)t[def.style]=Math.min(8,Math.max(t[def.style]||0,2));}
        else hit(r,alive(enemies)[0],base*1.5,def.element,ev,'perpetual_circuit');
      }
    }
    if(el==='storm'&&(oath(r,'tempest')||owned('storm_kings_wrath'))&&count(r,'tempest',6))for(let i=0;i<6;i++){const ts=alive(enemies),t=ts[i%ts.length];if(t){hit(r,t,n*3*(1+(r.guardians||0)*.08),'storm',ev,'chainstorm');t.stun=Math.max(t.stun,.5);}}
    if(el==='storm'&&owned('capacitor')&&count(r,'capacitor',5))hit(r,alive(enemies)[0],(offense?damage:n)*2.5*owned('capacitor'),'storm',ev,'chainstorm');
    if(!offense)return;
    const strike=(t,v,element=el,pattern='nova')=>hit(r,t,v,element,ev,pattern);
    if(el==='leaf'&&owned('thorn_volley')&&count(r,'thorn',3))for(let i=0;i<2;i++)strike(alive(enemies)[i%Math.max(1,alive(enemies).length)],damage*.35*owned('thorn_volley'),'leaf','briar_barrage');
    if(el==='storm'){
      if(owned('arc_jump')&&rng()<.35)strike(others[0],damage*.45*owned('arc_jump'),'storm','chainstorm');
      if(owned('thunder_tyrant'))for(const t of others.slice(0,3))strike(t,damage*.8,'storm','chainstorm');
    }
    if(el==='stone'){
      if(owned('rolling_stone'))strike(others[0],damage*.4*owned('rolling_stone'));
      if(owned('fracture_point'))e.fracture=Math.min(5+2*owned('fracture_point'),e.fracture+owned('fracture_point'));
    }
    if(el==='spirit'){
      const spiritBoost=1+.04*mastery(r,'spirit');
      if(owned('phantom_twin')&&rng()<.25)strike(e,damage*.45*owned('phantom_twin')*spiritBoost,'spirit','spectral_guardian');
      if(owned('haunt_amplifier'))strike(e,damage*.35*owned('haunt_amplifier')*spiritBoost,'spirit','soulmark');
      if(owned('soul_of_eternity')){heal(r,damage*.3*spiritBoost,ev);shield(r,damage*.25,ev,'spirit');}
    }
    if(e.freeze>0){
      const bonus=.4*owned('glass_shards')+(owned('crystal_cathedral')?2:0);
      if(bonus)strike(e,damage*bonus,'frost','glass_shards');
      if(el==='stone'&&(owned('icehammer')||owned('shattered_eternity')))for(const t of alive(enemies))strike(t,damage*(.65*owned('icehammer')+3*owned('shattered_eternity')),'stone','avalanche_heart');
    }
    if(owned('wildfire_bloom')&&e.burn>0&&e.poison>0){strike(e,damage*.35*owned('wildfire_bloom'),'fire','wildfire');for(const t of others){t.burn=Math.max(t.burn,e.burn*.2);t.poison=Math.max(t.poison,e.poison*.2);}}
    if(el==='storm'&&owned('stormglass')&&(e.chill>0||e.freeze>0))for(const t of others)strike(t,damage*.6*owned('stormglass'),'storm','chainstorm');
    if(el==='fire'&&owned('molten_fault')&&e.burn>0)e.fracture=Math.min(15,e.fracture+2*owned('molten_fault'));
    if(owned('wraithvine')&&e.rooted>0)strike(e,damage*.45*owned('wraithvine'),'spirit','soulmark');
    if(el==='spirit'&&owned('ghostflame')&&e.burn>0)heal(r,damage*.2*owned('ghostflame'),ev);
    if(el==='storm'&&owned('tempest_bloom')&&e.poison>0)for(const t of others)t.poison=Math.max(t.poison,e.poison*.4*owned('tempest_bloom'));
    if(d.style==='rapid'&&owned('rapid_overload'))strike(e,damage*.3*owned('rapid_overload'));
    if(d.cooldown>=2.6&&owned('heavy_followthrough'))for(const t of others)strike(t,damage*.35*owned('heavy_followthrough'));
    if(d.cooldown>=2.6&&['stone','fire'].includes(el)&&owned('the_shattered_earth'))for(const t of alive(enemies)){strike(t,damage*2.2,'fire','magma_fault');t.burn+=damage*.5;}
    applyConstellations(r,enemies,e,d,damage,ev,rng);
  };
  function applyConstellations(r,enemies,e,d,n,ev,rng){
    const el=d.element,others=alive(enemies).filter(t=>t!==e),strength=id=>constellation(r,id)*(oath(r,'chimera')?1+.2*equipped(r).length:1);
    let k;
    if((k=strength('ashroot'))&&e.poison>0&&e.burn>0&&rng()<.2)hit(r,e,n*.12*k,'fire',ev,'wildfire');
    if((k=strength('wintervine'))&&e.chill>0&&el==='leaf'&&rng()<Math.min(.8,.1*k))e.rooted=Math.max(e.rooted,Math.min(8,.8*k));
    if((k=strength('sapwire'))&&el==='storm'&&e.poison>0)for(const t of others)t.poison=Math.max(t.poison,e.poison*.08*k);
    if((k=strength('ironroot'))&&e.rooted>0&&el==='leaf')shield(r,2*k,ev,'leaf');
    if((k=strength('thermal_fracture'))&&el==='fire'&&e.freeze>0)hit(r,e,n*.2*k,'fire',ev,'thermal_fracture');
    if((k=strength('voltage_furnace'))&&el==='storm'&&e.burn>0)for(const t of others.slice(0,2))hit(r,t,n*.1*k,'storm',ev,'chainstorm');
    if((k=strength('magma_fault'))&&el==='fire')e.fracture=Math.min(15,e.fracture+.2*k);
    if((k=strength('cursed_ember'))&&el==='spirit'&&(e.haunt===0||rank(r,'haunt_amplifier')))for(const t of [e,...others])t.burn=Math.max(t.burn,n*.08*k);
    if((k=strength('prism_circuit'))&&el==='storm'&&e.freeze>0)for(const t of others.slice(0,Math.ceil(k)))hit(r,t,n*.12*k,'storm',ev,'chainstorm');
    if((k=strength('avalanche_heart'))&&el==='stone'&&e.freeze>0)for(const t of others)hit(r,t,n*.15*k,'stone',ev,'avalanche_heart');
    if((k=strength('magnetfall'))&&el==='storm'&&e.fracture>0)for(const t of [e,...others.slice(0,1)])hit(r,t,n*.03*e.fracture*k,'storm',ev,'chainstorm');
    if((k=strength('grave_monument'))&&el==='spirit'&&r.shield>0)hit(r,e,n*.08*k,'spirit',ev,'soulmark');
  }
  P.onHurt=(r,n,e,ev,absorbed=0)=>{
    const s=state(r);
    if(absorbed>0&&rank(r,'charged_barrier'))hit(r,e,absorbed*.4*rank(r,'charged_barrier'),'storm',ev,'chainstorm');
    if((oath(r,'mountain')||rank(r,'mountain_awakening'))&&count(r,'mountain',4))for(const t of alive(r.enemies))hit(r,t,power(r)*(rank(r,'mountain_awakening')?5:3),'stone',ev,'avalanche_guard');
    if(r.hp>0&&r.hp<r.maxHp*.2&&r.legacy?.milestones?.includes('heart_of_forest')&&r.round-s.lastForestWave>=4){s.lastForestWave=r.round;heal(r,r.maxHp*.15,ev,'leaf');shield(r,r.maxHp*.12,ev,'leaf');event(ev,'ability','leaf','jade_shelter','Heart of the Forest');}
  };
  P.survive=(r,enemies,ev)=>{
    if(r.hp>0)return false;const s=state(r);let fraction=0,label='',el='fire';
    if(oath(r,'phoenix')&&!s.phoenixUsed){s.phoenixUsed=true;fraction=.35;label='Oath of the Phoenix';for(const e of alive(enemies)){hit(r,e,power(r)*12,'fire',ev,'inferno_ring');e.burn+=power(r)*8;}}
    else if(rank(r,'second_soul')&&r.round-(s.counters.secondSoulWave??-99)>=8){s.counters.secondSoulWave=r.round;fraction=.5;label='Second Soul';el='spirit';companion(r,'duplicate','spirit',power(r)*4,10,{lifesteal:.4});}
    else if(rank(r,'phoenix_feather')&&r.round-s.lastFeatherWave>=Math.max(3,9-rank(r,'phoenix_feather'))){s.lastFeatherWave=r.round;fraction=.2;label='Phoenix Feather';for(const e of alive(enemies))e.burn+=power(r)*3;}
    else if(rank(r,'last_light')&&!s.counters.lastLightUsed){s.counters.lastLightUsed=1;fraction=Math.min(.75,.15*rank(r,'last_light'));label='Last Light';el='spirit';}
    if(!fraction)return false;const total=P.healing(r,r.maxHp*fraction);r.hp=Math.min(r.maxHp,total);P.afterHealing(r,total,r.hp);r.deathCause=null;event(ev,'ability',el,'phoenix',label,null,{strength:10});return true;
  };
  P.onDeaths=(r,dead,enemies,ev)=>{
    const s=state(r);for(const e of Array.isArray(dead)?dead:[dead]){
      if(s.processedDeaths.includes(e.uid))continue;s.processedDeaths.push(e.uid);
      const burning=e.burn>0,poison=e.poison>0,frozen=e.freeze>0,burst=burning?(rank(r,'cinderburst')*.18+(rank(r,'infernal_domino')?.8:rank(r,'cinder_apocalypse')?.45:0)):0;
      if(burst)for(const t of alive(enemies)){hit(r,t,e.maxHp*burst,'fire',ev,'cinderburst');if(rank(r,'infernal_domino')||rank(r,'cinder_apocalypse'))t.burn+=power(r)*3;}
      if(burning&&capstone(r,'fire')){const t=alive(enemies)[0];if(t)t.burn=Math.max(t.burn,e.burn*.5+power(r));}
      if(e.chill>0&&rank(r,'chill_cascade'))for(const t of alive(enemies))t.chill=Math.min(8,Math.max(t.chill,2*rank(r,'chill_cascade')));
      const recover=.02*rank(r,'soul_satchel')+(poison?.008*constellation(r,'wraithbloom'):0)+(frozen?.01*constellation(r,'moonwinter'):0)+(frozen&&rank(r,'soul_of_eternity')?.12:0);
      if(recover){heal(r,r.maxHp*recover,ev);if(rank(r,'soul_satchel')||frozen)shield(r,r.maxHp*recover,ev,'spirit');}
      if(oath(r,'phantom_king')||rank(r,'phantom_legion'))companion(r,'phantom','spirit',power(r)*1.5,999,{lifesteal:.15});
      if(rank(r,'worldroot_network'))companion(r,'plant','leaf',power(r)*1.7,999,{poison:.2,root:.5,lifesteal:.1});
    }
  };
  P.victory=(r,ev)=>{
    old.victory(r,ev);const guardian=r.round%4===0;
    heal(r,r.maxHp*(.01*permanent(r,'regenerative_roots')+(guardian?.02*permanent(r,'guardian_blessing'):0)),ev,'leaf');
    if(guardian&&rank(r,'ancestral_forge')&&r.active.length)r.active[(Math.floor(r.round/4)-1)%r.active.length].level++;
    r.bonusDrops=(guardian?rank(r,'guardian_spoils'):0)+(r.round%6===0?rank(r,'scavenger_cache'):0);
    state(r).companions=state(r).companions.filter(c=>c.permanent);
  };
  P.dropBonus=(r,rng)=>old.dropBonus(r,rng)+(rank(r,'relic_appraiser')&&rng()<Math.min(.6,.12*rank(r,'relic_appraiser'))?1:0);
  function castNew(r,enemies,d,ev){
    const n=(d.power+d.growth*(rank(r,d.id)-1))*(1+.08*(r.round-1))*(1+rank(r,'spellcraft')*.18),targets=alive(enemies);
    if(d.id==='crystal_chorus'){for(const e of targets.sort((a,b)=>b.attackProgress-a.attackProgress).slice(0,2)){e.freeze=Math.min(8,Math.max(e.freeze,n*(1+.03*mastery(r,'frost'))));event(ev,'ability','frost',d.id,'Frozen',e);}return;}
    if(d.id==='avalanche_guard'){shield(r,n*(1+.04*mastery(r,'stone')),ev);state(r).buffs.guard=3;event(ev,'ability','stone',d.id,'Avalanche Guard');return;}
    if(d.id==='spectral_guardian'){companion(r,'spectral','spirit',n,8,{lifesteal:.3});event(ev,'ability','spirit',d.id,'Spectral Guardian');return;}
    if(d.id==='chainstorm'){for(let i=0;i<4;i++){const ts=alive(enemies);if(!ts.length)break;const e=ts[i%ts.length];hit(r,e,n,'storm',ev,d.id);e.stun=Math.max(e.stun,.25);}}
    else for(const e of targets.slice(0,d.id==='briar_barrage'?3:targets.length)){
      const burning=e.burn>0;hit(r,e,n,d.element,ev,d.id);
      if(d.id==='briar_barrage')e.poison+=n*.25*(1+.05*mastery(r,'leaf'));
      if(d.id==='inferno_ring'){if(burning)hit(r,e,n*.8,'fire',ev,d.id);e.burn+=n*(1+.05*mastery(r,'fire'));}
    }
  }
  function periodic(r,id,interval,dt,fn){const s=state(r);s.timers[id]=(s.timers[id]??interval)-dt;if(s.timers[id]<=0){s.timers[id]+=interval;fn();}}
  P.tickAbilities=(r,enemies,dt,ev)=>{
    if(!(dt>0)||!alive(enemies).length)return;const s=state(r);
    // Base spells retain their existing behavior. Temporarily separate new
    // ability definitions so the original dispatcher cannot resolve them twice.
    const newIds=new Set(spells.map(x=>x[0])),saved=P.abilities.filter(d=>newIds.has(d.id));
    P.abilities.splice(0,P.abilities.length,...P.abilities.filter(d=>!newIds.has(d.id)));
    const eventStart=ev.length;old.tickAbilities(r,enemies,dt,ev);P.abilities.push(...saved);
    let casts=ev.slice(eventStart).filter(e=>e.kind==='ability').length;
    for(const d of saved){if(!rank(r,d.id))continue;const left=(r.abilityTimers[d.id]??.4)-dt;if(left>0){r.abilityTimers[d.id]=left;continue;}if(!alive(enemies).length)break;castNew(r,enemies,d,ev);r.abilityTimers[d.id]=P.duration(r,d.id);casts++;}
    if(casts&&rank(r,'phantom_surge'))hit(r,alive(enemies)[0],power(r)*rank(r,'phantom_surge')*casts,'spirit',ev,'spectral_guardian');
    if(s.buffs.guard>0)s.buffs.guard=Math.max(0,s.buffs.guard-dt);
    for(const e of enemies)if(e.burn>0&&e.hp>0&&rank(r,'wildfire_furnace')){
      const extra=Math.min(e.burn,e.burn*.64*dt*.2*rank(r,'wildfire_furnace'));e.burn-=extra;e.hp=Math.max(0,e.hp-extra);
      if(extra>.5)event(ev,'hit','fire','burn',String(Math.ceil(extra)),e);
    }
    syncCompanions(r);
    for(const c of s.companions){
      if(!c.permanent)c.life=Math.max(0,c.life-dt);if(!c.permanent&&c.life<=0)continue;
      c.timer-=dt;if(c.timer>0)continue;c.timer+=c.id==='colossus'?2.2:1.4;
      const ts=alive(enemies);if(!ts.length)break;const t=ts[(s.counters.companionCursor||0)%ts.length];s.counters.companionCursor=(s.counters.companionCursor||0)+1;
      const removed=hit(r,t,c.power,c.element,ev,'companion');if(c.poison)t.poison+=removed*c.poison*(1+.05*mastery(r,'leaf'));if(c.root)t.rooted=Math.min(8,Math.max(t.rooted,c.root*(1+.02*mastery(r,'leaf'))));
      if(c.lifesteal)heal(r,removed*c.lifesteal*(1+.04*mastery(r,'spirit')),ev);if(c.id==='colossus')shield(r,r.maxHp*.04,ev);
      event(ev,'companion',c.element,c.id,c.id,t,{companionId:c.id});
    }
    s.companions=s.companions.filter(c=>c.permanent||c.life>0);
    const all=(n,el,pattern,each)=>{for(const e of alive(enemies)){if(n)hit(r,e,n,el,ev,pattern);each?.(e);}};
    if(oath(r,'eternal_winter'))periodic(r,'winter',16,dt,()=>all(0,'frost','crystal_chorus',e=>{e.freeze=Math.max(e.freeze,2);event(ev,'ability','frost','crystal_chorus','Winter pulse',e);}));
    if(rank(r,'absolute_zero'))periodic(r,'zero',12,dt,()=>all(0,'frost','crystal_chorus',e=>{e.freeze=Math.max(e.freeze,5);event(ev,'ability','frost','crystal_chorus','Absolute Zero',e);}));
    if(rank(r,'cinder_apocalypse'))periodic(r,'apocalypse',10,dt,()=>all(power(r)*10,'fire','inferno_ring',e=>e.burn+=power(r)*5));
    if(rank(r,'solar_cataclysm'))periodic(r,'solar',9,dt,()=>{all(power(r)*7,'fire','inferno_ring',e=>e.burn+=power(r)*4);all(power(r)*7,'storm','chainstorm',e=>e.stun=Math.max(e.stun,1.5));});
    if(rank(r,'chronobloom'))periodic(r,'chrono',12,dt,()=>{advance(r,999);advance(r,5,true);event(ev,'ability','leaf','chronobloom','Chronobloom');});
    if(rank(r,'tempest_of_seasons'))periodic(r,'seasons',8,dt,()=>{for(const el of ['leaf','frost','storm'])all(power(r)*5,el,'tempest_of_seasons',e=>{e.rooted=Math.max(e.rooted,2);e.freeze=Math.max(e.freeze,2);e.stun=Math.max(e.stun,1);});});
    if(oath(r,'chimera')&&equipped(r).length>=4)periodic(r,'chimera',10,dt,()=>all(power(r)*6,'spirit','the_sixfold_crown'));
    if(rank(r,'the_sixfold_crown'))periodic(r,'crown',7,dt,()=>{for(const el of elements)all(power(r)*(2+equipped(r).length),el,'the_sixfold_crown',e=>{e.poison+=power(r)*.4;e.burn+=power(r);e.freeze=Math.max(e.freeze,1);});});
  };
  P.eligible=(r,d)=>{
    if(!d||r.banished?.includes(d.id)||(d.maxRank&&rank(r,d.id)>=d.maxRank))return false;
    const gear=[...r.active,...r.pack].map(it=>C.items[it.id]).filter(Boolean),els=new Set(gear.map(d=>d.element));
    if(d.requires?.some(el=>!els.has(el)))return false;
    if(d.minElements&&els.size<d.minElements)return false;
    if(d.constellation&&!r.legacy?.constellations?.includes(d.constellation))return false;
    if(d.allMasteries&&!elements.every(el=>mastery(r,el)>=8))return false;
    if(d.minRegion!=null&&r.region<d.minRegion)return false;
    return (!d.affinity||els.has(d.affinity))&&(!d.styles||gear.some(i=>d.styles.includes(i.style)))&&(!d.forged||gear.some(i=>i.recipe))&&(!d.fast||gear.some(i=>i.cooldown<=2))&&(!d.heavy||gear.some(i=>i.cooldown>=2.6));
  };
  P.talentOdds=r=>{const depth=Math.min(1,Math.max(0,r.region)/20),guardians=r.guardians??Math.floor(r.kills/4);return {common:.68-.35*depth,uncommon:.24,rare:.07+.15*depth,epic:.009+.17*depth,mythic:r.region<3?0:Math.min(.025,.001+.0007*guardians+.005*depth)};};
  P.offer=(r,rng,count=3,context='level')=>{
    count=Math.min(5,count+permanent(r,'expanded_destiny'));const out=[],pool=Object.values(U).filter(d=>P.eligible(r,d)),odds=P.talentOdds(r);
    const favored=r.legacy?.config?.favoredElement,build=r.legacy?.config?.build||'',boostBuild=d=>{
      const tags=(d.name+' '+d.description+' '+d.category+' '+d.element).toLowerCase();
      const key=build.toLowerCase(),words=key.includes('poison')?['poison','leaf']:key.includes('fire')?['fire','burn','explos']:key.includes('frost')?['frost','freeze','chill']:key.includes('lightning')||key.includes('storm')?['storm','cooldown','lightning']:key.includes('defens')?['shield','damage resistance','survival']:key.includes('heal')?['heal','lifesteal','recovery']:key.includes('summon')?['companion','summon','phantom','vine']:key.includes('fusion')?['combinations','constellations']:key.includes('mythic')?['mythic']:[];
      return words.some(w=>tags.includes(w))?2:1;
    };
    const take=filter=>{
      const choices=pool.filter(d=>!out.includes(d)&&(!filter||filter(d)));if(!choices.length)return false;
      const weights=choices.map(d=>(odds[d.rarity]||0)*boostBuild(d)*(d.element===favored?1+.2*permanent(r,'elemental_favor'):1)*(rank(r,d.id)?1+.12*permanent(r,'focused_training'):1));
      let roll=rng()*weights.reduce((a,b)=>a+b,0);if(!weights.some(n=>n>0))return false;
      let index=0;for(;index<weights.length-1;index++){roll-=weights[index];if(roll<0)break;}out.push(choices[index]);return true;
    };
    const mythmaker=context==='level'&&!r.draftIsBonus&&oath(r,'mythmaker')&&(r.guardians||0)>=3&&!state(r).counters.mythmakerUsed;
    if(mythmaker&&take(d=>d.rarity==='mythic'))state(r).counters.mythmakerUsed=1;
    const echo=(r.echoChoices||[]).find(x=>(typeof x==='string'||x.drafts>0)&&pool.some(d=>d.id===(typeof x==='string'?x:x.id)));
    if(echo){const d=pool.find(d=>d.id===(typeof echo==='string'?echo:echo.id));if(!out.includes(d))out.push(d);}
    if(context!=='reroll')r.echoChoices=(r.echoChoices||[]).map(x=>typeof x==='string'?{id:x,drafts:0}:{...x,drafts:Math.max(0,x.drafts-1)}).filter(x=>x.drafts>0);
    if(!out.length&&count>0&&(!Object.keys(r.skills).length||rng()<.33))take(d=>d.kind==='ability');
    while(out.length<count&&out.length<pool.length)if(!take())break;
    return out.map(d=>d.id);
  };
  P.validateLivingState=s=>{
    if(s===undefined)return undefined;
    if(!s||typeof s!=='object'||Array.isArray(s))throw new TypeError('Invalid living combat state');
    const number=(n,max=1e12)=>{if(typeof n!=='number'||!Number.isFinite(n)||n<0||n>max)throw new TypeError('Invalid living timer');return n;};
    const map=value=>{if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).length>100)throw new TypeError('Invalid living counters');return Object.fromEntries(Object.entries(value).map(([k,v])=>{if(!/^[a-zA-Z_]+$/.test(k))throw new TypeError('Invalid living key');return [k,number(v)];}));};
    const out={timers:map(s.timers),counters:map(s.counters),buffs:map(s.buffs),companions:[],processedDeaths:[],phoenixUsed:!!s.phoenixUsed,lastForestWave:s.lastForestWave,lastFeatherWave:s.lastFeatherWave};
    for(const key of ['lastForestWave','lastFeatherWave'])if(!Number.isInteger(out[key])||out[key]<-100||out[key]>1e12)throw new TypeError('Invalid healing milestone');
    if(!Array.isArray(s.companions)||s.companions.length>24||!Array.isArray(s.processedDeaths)||s.processedDeaths.length>5)throw new TypeError('Invalid companions');
    for(const c of s.companions){if(!c||!elements.includes(c.element)||!['vine','worldroot','awakening','swarm','colossus','duplicate','phantom','plant','spectral'].includes(c.id)||typeof c.permanent!=='boolean')throw new TypeError('Invalid companion');out.companions.push({id:c.id,element:c.element,power:number(c.power),timer:number(c.timer,3),life:number(c.life,999),permanent:c.permanent,poison:number(c.poison,1),root:number(c.root,8),lifesteal:number(c.lifesteal,1)});}
    out.processedDeaths=s.processedDeaths.map(n=>number(n));return out;
  };
  P.livingCatalog={ordinary:ordinary.map(x=>x[0]),pairs:pairs.map(x=>x[0]),generals:generals.map(x=>x[0]),spells:spells.map(x=>x[0]),mythics:myths.map(x=>x[0]),constellations:constellations.map(x=>x[0])};
})();
