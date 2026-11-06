/* Camp of Echoes. Permanent data lives in the profile; expeditions use a snapshot.
   Costs are deterministic. Refunds return the recorded spend, never a recomputed price. */
(() => {
  'use strict';
  const C=()=>globalThis.Catalog;
  const elements=['leaf','fire','frost','storm','stone','spirit'];
  const areas=[
    ['heartwood','Heartwood Sanctuary','leaf','acorn','Health, healing and protection.','Complete an expedition.'],
    ['wayfinder',"Wayfinder’s Lodge",'storm','vane','Discovery, treasure and exploration.','Complete an expedition.'],
    ['forge','Ancient Forge','fire','mallet','Equipment, fusion and heirloom kits.','Defeat a guardian.'],
    ['fate','Fateweaver','spirit','hourglass','Guide the talents in your next expedition.','Defeat three lifetime guardians.'],
    ['grove','Elemental Grove','frost','clover','Master six elements and connect their constellations.','Defeat three lifetime guardians.'],
    ['mythic','Mythic Shrine','spirit','crown','Permanent oaths that transform future journeys.','Reach Region 8.']
  ].map(([id,name,element,icon,description,requirement])=>({id,name,element,icon,description,requirement}));
  const upgrades={};
  function add(id,name,area,max,baseCost,step,icon,description,explain){upgrades[id]={id,name,area,max,baseCost,step,icon,description,explain,kind:'ranked'};}
  const percent=(n,d=0)=>Number(n.toFixed(d))+'%';
  const entries=[
    ['heart','Heartwood','heartwood',100,1,2,'acorn','Starting maximum HP.',n=>`${88+14*n} starting maximum HP`],
    ['claw','Iron Grip','heartwood',100,1,2,'fang','Base relic damage.',n=>`${100+8*n}% base relic power`],
    ['barkskin','Barkskin','heartwood',25,3,2,'shell','Additional starting shield.',n=>`${4*n} starting shield (40% HP cap)`],
    ['regenerative_roots','Regenerative Roots','heartwood',10,4,3,'seed','Recover maximum HP after each wave.',n=>`${n}% maximum HP after each wave`],
    ['guardian_blessing',"Guardian’s Blessing",'heartwood',10,5,3,'idol','Additional guardian healing.',n=>`${n*2}% maximum HP after guardians`],
    ['thickened_bark','Thickened Bark','heartwood',12,6,3,'shell','Reduce incoming damage.',n=>`${percent(n*1.5,1)} less incoming damage`],
    ['medicinal_sap','Medicinal Sap','heartwood',15,3,2,'gourd','Improve every healing effect.',n=>`${2*n}% stronger healing`],
    ['venom_resistance','Venom Resistance','heartwood',10,4,3,'mortar','Reduce incoming damage over time.',n=>`${5*n}% less damage over time`],
    ['pace','Trail Memory','wayfinder',3,2,0,'feather','Faster travel through cleared rounds.',n=>`${n+1}× maximum replay speed`],
    ['treasure_nose','Treasure Nose','wayfinder',10,3,3,'pick','Additional relic drop chance.',n=>`+${n} percentage points to wave drop chance`],
    ['guardian_hoard','Guardian Hoard','wayfinder',10,5,4,'crown','Bonus guardian relic opportunity.',n=>`${n*5}% bonus guardian drop chance`],
    ['legendary_scent','Legendary Scent','wayfinder',10,8,4,'clover','Shift Common odds into Epic and Mythic.',n=>`+${percent(n*.35,2)} Epic and +${percent(n*.08,2)} Mythic odds`],
    ['relic_hunter','Relic Hunter','wayfinder',8,4,3,'vane','Prioritize undiscovered relics within their rarity.',n=>`${n*8}% discovery preference`],
    ['ingredient_sense','Ingredient Sense','wayfinder',10,4,3,'tongs','Prefer useful fusion ingredients within their rarity.',n=>`${n*6}% ingredient preference`],
    ['lucky_streak','Lucky Streak','wayfinder',3,12,10,'clover','Guarantee a find after consecutive empty waves.',n=>n?`Guaranteed relic after ${7-n} empty waves`:'No guaranteed relic'],
    ['monster_scholar','Monster Scholar','wayfinder',8,5,4,'brush','Damage against previously discovered species.',n=>`${n*3}% damage to known species`],
    ['heirloom_temper','Heirloom Temper','forge',4,8,7,'mallet','Improve the first equipped starting relic.',n=>`First relic starts at level ${1+n}`],
    ['second_heirloom','Second Heirloom','forge',3,12,9,'hammer','Improve the second equipped starting relic.',n=>`Second relic starts at level ${1+n}`],
    ['starter_satchel','Starter Satchel','forge',2,15,15,'gourd','Additional base relics in the starting backpack.',n=>`${n} additional starting base relics`],
    ['mastercraft','Mastercraft','forge',10,6,5,'tongs','Increase forged relic potency.',n=>`${n*3}% stronger forged relics`],
    ['fusion_fortune','Fusion Fortune','forge',8,8,5,'clover','Preserve a level-one copy of an ingredient.',n=>`${percent(n*2.5,1)} chance per fusion`],
    ['relic_calling','Relic Calling','forge',2,16,16,'bell','Favor an equipment style before a journey.',n=>n?`${n===1?'Offensive':'All'} equipment style preferences unlocked`:'No style preference'],
    ['anvil_blessing',"Anvil’s Blessing",'forge',10,8,5,'mallet','Chance newly found relics gain one level.',n=>`${n*3}% chance of an extra drop level`],
    ['guardian_hammer',"Guardian’s Hammer",'forge',1,60,0,'hammer','Improve one equipped relic every fourth guardian.',n=>n?'Every fourth guardian raises one equipped relic level':'No guardian tempering'],
    ['expanded_destiny','Expanded Destiny','fate',2,18,20,'hourglass','More normal level-up choices.',n=>`${3+n} talent choices`],
    ['fate_rerolls','Fate Rerolls','fate',3,10,10,'crank','Additional expedition draft rerolls.',n=>`${n} rerolls per expedition`],
    ['banishing_ink','Banishing Ink','fate',3,12,12,'brush','Banish unwanted talents for this expedition.',n=>`${n} banishes per expedition`],
    ['elemental_favor','Elemental Favor','fate',5,7,6,'seal','Favor talents matching the favored element.',n=>`${n*20}% added matching draft weight`],
    ['echo_destiny','Echo of Destiny','fate',2,20,18,'mirror','Save an unchosen talent for the next two drafts.',n=>`${n} echoes per expedition`],
    ['focused_training','Focused Training','fate',8,6,5,'needle','Favor repeated ranks of owned talents.',n=>`${n*12}% added owned-talent weight`],
    ['guardian_wisdom','Guardian Wisdom','fate',10,5,4,'crown','Increase all expedition XP.',n=>`${n*5}% additional XP`],
    ['first_blessing','First Blessing','fate',1,50,0,'clover','Choose a regular talent when an expedition begins.',n=>n?'One starting regular talent choice':'No starting talent choice']
  ];
  entries.forEach(x=>add(...x));
  const milestones={};
  for(const [id,name,area,cost,ranks,description] of [
    ['heart_of_forest','Heart of the Forest','heartwood',45,25,'Once every four waves, falling below 20% HP restores 15% maximum HP and grants 12% maximum HP shielding.'],
    ['legendary_expedition','Legendary Expedition','wayfinder',40,15,'Choose a favored element; its drops and useful fusion ingredients appear more often.'],
    ['heirloom_vault','The Heirloom Vault','forge',55,12,'Save and switch between three expedition kits using discovered, unlocked base relics.'],
    ['destiny_board','The Destiny Board','fate',55,12,'Choose a build direction that guides future talent drafts.']
  ])milestones[id]=upgrades[id]={id,name,area,cost,max:1,icon:'crown',description,explain:n=>n?'Unlocked':description,kind:'milestone',branchRanks:ranks};
  const masteryText={leaf:n=>`${n*5}% poison potency; ${n*2}% longer roots`,fire:n=>`${n*5}% burn potency`,frost:n=>`${n*3}% longer chill and freeze (8 second cap)`,storm:n=>`${percent(n*1.5,1)} shorter Storm cooldowns (0.15 second minimum)`,stone:n=>`${n*4}% more Stone shielding`,spirit:n=>`${n*4}% stronger lifesteal and haunting`};
  const masteries={};
  for(const el of elements){const id='mastery_'+el;masteries[el]=upgrades[id]={id,name:el[0].toUpperCase()+el.slice(1)+' Mastery',area:'grove',element:el,max:8,baseCost:7,step:6,icon:{leaf:'thorn',fire:'torch',frost:'shard',storm:'fork',stone:'mallet',spirit:'mask'}[el],description:masteryText[el](1),explain:masteryText[el],kind:'mastery'};
    const cap='ascendant_'+el;milestones[cap]=upgrades[cap]={id:cap,name:el[0].toUpperCase()+el.slice(1)+' Ascendance',area:'grove',element:el,max:1,cost:80,icon:masteries[el].icon,description:'Mastery capstone: '+{leaf:'roots spread poison to a second enemy.',fire:'burning deaths ignite another enemy.',frost:'freezing grants a small shield.',storm:'every sixth Storm activation advances all relics.',stone:'Stone shields improve the next Stone strike.',spirit:'Spirit healing also grants shielding.'}[el],explain:n=>n?'Unlocked':'Requires rank 8 mastery',kind:'milestone',mastery:el};
  }
  const constellations={};
  for(const [a,b,id,name,description] of [
    ['leaf','fire','ashroot','Ashroot','Burning poisoned enemies occasionally release damage bursts.'],['leaf','frost','wintervine','Wintervine','Chilled enemies are easier to root.'],['leaf','storm','sapwire','Sapwire','Lightning spreads existing poison.'],['leaf','stone','ironroot','Ironroot','Rooting enemies grants shielding.'],['leaf','spirit','wraithbloom','Wraithbloom','Poisoned deaths release healing energy.'],['fire','frost','thermal_fracture','Thermal Fracture','Fire strikes burst against frozen enemies.'],['fire','storm','voltage_furnace','Voltage Furnace','Lightning striking burning enemies releases sparks.'],['fire','stone','magma_fault','Magma Fault','Fire attacks build fracture.'],['fire','spirit','cursed_ember','Cursed Ember','Haunting bursts ignite nearby enemies.'],['frost','storm','prism_circuit','Prism Circuit','Lightning chains farther through frozen enemies.'],['frost','stone','avalanche_heart','Avalanche Heart','Stone strikes shatter frozen enemies for splash damage.'],['frost','spirit','moonwinter','Moonwinter','Frozen deaths restore health and shields.'],['storm','stone','magnetfall','Magnetfall','Fracture increases lightning-chain damage.'],['storm','spirit','soul_voltage','Soul Voltage','Spirit attacks accelerate automatic abilities.'],['stone','spirit','grave_monument','Grave Monument','Shielded Pip gains stronger haunting and guardian resistance.']
  ])constellations[id]=upgrades[id]={id,name,area:'grove',elements:[a,b],max:1,cost:40,icon:'seal',description,explain:n=>n?'Passive active with both elements; empowered expedition talent unlocked':description,kind:'constellation'};
  const oaths={};
  for(const [id,name,cost,description] of [
    ['worldroot','Oath of the Worldroot',90,'A permanent attacking vine companion poisons and roots; every fourth guardian strengthens it.'],['phoenix','Oath of the Phoenix',110,'Once per expedition, fatal damage restores 35% HP and burns the whole formation.'],['tempest','Oath of the Tempest',110,'Every sixth Storm activation releases a repeated thunderstorm.'],['eternal_winter','Oath of Eternal Winter',100,'Freeze the most dangerous foe at battle start, then release periodic freezing pulses.'],['mountain','Oath of the Mountain',100,'Begin every battle shielded; enemy strikes charge a crushing counterattack.'],['phantom_king','Oath of the Phantom King',130,'Defeated foes become attacking, healing phantom allies until encounter end.'],['relic_monarch','Oath of the Relic Monarch',150,'Start with a third equipped relic and stronger gear; guardians offer extra equipment upgrades.'],['fateweaver','Oath of the Fateweaver',150,'An extra starting talent choice and a bonus talent after each fourth guardian.'],['chimera','Oath of the Chimera',175,'Equipped elemental diversity strengthens combinations; four elements create repeating pulses.'],['mythmaker','Oath of the Mythmaker',225,'After three guardians, the next eligible XP level-up offers a guaranteed Mythic.']
  ]){const key='oath_'+id;oaths[id]=upgrades[key]={id:key,oath:id,name,area:'mythic',max:1,cost,icon:'crown',description,explain:n=>n?'Owned — equip at camp':description,kind:'oath'};}
  upgrades.convergence={id:'convergence',name:'Convergence',area:'mythic',max:1,cost:300,icon:'crown',kind:'convergence',description:'Equip two owned Mythic Oaths together.',explain:n=>n?'Two equipped Oath slots':'One equipped Oath slot'};
  upgrades.celestial_canopy={id:'celestial_canopy',name:'Celestial Canopy',area:'mythic',max:10,baseCost:100,step:50,icon:'crown',kind:'ranked',description:'Endgame mastery: increase all relic potency by 5% per rank and illuminate the camp.',explain:n=>`${n*5}% relic potency · prestige ${n}`};
  const builds=['Poison Specialist','Fire and Explosions','Frost and Crowd Control','Lightning and Attack Speed','Defensive Fortress','Healing and Lifesteal','Summoning and Companions','Elemental Fusion','Mythic Hunter'];
  const styles=['','strike','rapid','burn','poison','chill','shock','pierce','drain','mend','guard','heal','shield','stun','freeze'];
  const ok=message=>({ok:true,message}),fail=message=>({ok:false,message});
  const oldCost=(key,n)=>key==='pace'?[2,6,12][n]:1+n*2+Math.floor(n*n/4);
  const empty=()=>({version:2,ranks:{},milestones:[],masteries:Object.fromEntries(elements.map(el=>[el,0])),constellations:[],oaths:[],activeOaths:[],convergence:false,spent:{},unlocks:[],stats:{guardians:0,expeditions:0,highestRegion:0,regions:[],achievements:[]},config:{favoredElement:'',style:'',build:''},kits:[null,null,null],selectedKit:-1});
  function ensure(p){
    if(!p.legacy){p.legacy=empty();for(const key of ['heart','claw','pace']){const n=p.perks?.[key]||0;p.legacy.spent[key]=Array.from({length:n},(_,i)=>oldCost(key,i)).reduce((a,b)=>a+b,0);}p.legacy.stats.guardians=Math.floor((p.best||0)/4);p.legacy.stats.expeditions=p.deaths||0;p.legacy.stats.highestRegion=p.best?Math.min(C().regions.length,Math.floor((p.best-1)/4)+1):0;p.legacy.stats.regions=Array.from({length:Math.min(C().regions.length,Math.floor((p.best||0)/4))},(_,i)=>i);}
    updateUnlocks(p);return p.legacy;
  }
  function updateUnlocks(p){const l=p.legacy;if(!l)return;const s=l.stats;const grant=id=>{if(!l.unlocks.includes(id))l.unlocks.push(id);};if(s.expeditions||p.deaths||Object.values(p.perks||{}).some(Boolean)){grant('heartwood');grant('wayfinder');}if(s.guardians>=1)grant('forge');if(s.guardians>=3){grant('fate');grant('grove');}if(s.highestRegion>=4)grant('constellations');if(s.highestRegion>=8)grant('mythic');if(s.highestRegion>=16)grant('convergence');if(s.highestRegion>=35)grant('endgame');}
  function rank(p,id){const l=ensure(p),d=upgrades[id];if(!d)return 0;if(['heart','claw','pace'].includes(id))return p.perks[id]||0;if(d.kind==='mastery')return l.masteries[d.element]||0;if(d.kind==='milestone')return +l.milestones.includes(id);if(d.kind==='constellation')return +l.constellations.includes(id);if(d.kind==='oath')return +l.oaths.includes(d.oath);if(d.kind==='convergence')return +l.convergence;return l.ranks[id]||0;}
  function cost(p,id){const d=upgrades[id];if(!d||rank(p,id)>=d.max)return null;const n=rank(p,id);return ['heart','claw','pace'].includes(id)?oldCost(id,n):d.cost??d.baseCost+d.step*n+Math.floor(n*n*.6);}
  function branchRanks(p,area){return Object.values(upgrades).filter(d=>d.area===area&&d.kind==='ranked').reduce((sum,d)=>sum+rank(p,d.id),0);}
  const atCamp=p=>!p.run||p.run.phase==='dead';
  function areaUnlocked(p,id){return ensure(p).unlocks.includes(id);}
  function available(p,id){const l=ensure(p),d=upgrades[id];if(!d)return fail('Unknown upgrade.');if(!atCamp(p))return fail('Return to camp between expeditions.');if(rank(p,id)>=d.max)return fail('Maximum rank reached.');if(!areaUnlocked(p,d.area))return fail(areas.find(a=>a.id===d.area).requirement);if(d.branchRanks&&branchRanks(p,d.area)<d.branchRanks)return fail(`Invest ${d.branchRanks} ranks in this branch.`);if(d.mastery&&l.masteries[d.mastery]<8)return fail('Requires rank 8 '+d.mastery+' mastery.');if(d.kind==='constellation'&&(!areaUnlocked(p,'constellations')||d.elements.some(el=>l.masteries[el]<3)))return fail('Reach Region 4 and rank 3 in both masteries.');if(id==='convergence'&&!areaUnlocked(p,'convergence'))return fail('Reach Region 16.');if(id==='celestial_canopy'&&!areaUnlocked(p,'endgame'))return fail('Reach Region 35.');if((p.points||0)<cost(p,id))return fail(`Requires ${cost(p,id)} Trail Seeds.`);return ok('Available');}
  function preview(p,id){const d=upgrades[id];if(!d)return null;const n=rank(p,id),a=available(p,id);return {rank:n,max:d.max,current:d.explain(n),next:n<d.max?d.explain(n+1):null,cost:cost(p,id),available:a.ok,reason:a.message};}
  function purchase(p,id){const a=available(p,id);if(!a.ok)return a;const l=ensure(p),d=upgrades[id],price=cost(p,id);p.points-=price;l.spent[id]=(l.spent[id]||0)+price;if(['heart','claw','pace'].includes(id))p.perks[id]++;else if(d.kind==='mastery')l.masteries[d.element]++;else if(d.kind==='milestone')l.milestones.push(id);else if(d.kind==='constellation')l.constellations.push(id);else if(d.kind==='oath'){l.oaths.push(d.oath);if(l.activeOaths.length<oathSlots(p))l.activeOaths.push(d.oath);}else if(d.kind==='convergence')l.convergence=true;else l.ranks[id]=(l.ranks[id]||0)+1;return ok(d.name+' purchased.');}
  function respec(p){if(!atCamp(p))return fail('Refunds are available only at camp.');const l=ensure(p),refund=Object.values(l.spent).reduce((a,b)=>a+b,0),fresh=empty();fresh.stats=l.stats;fresh.unlocks=l.unlocks;fresh.kits=l.kits;fresh.selectedKit=-1;p.points+=refund;p.perks={heart:0,claw:0,pace:0};p.legacy=fresh;return ok(`Refunded ${refund} Trail Seeds.`);}
  function oathSlots(p){return ensure(p).convergence?2:1;}
  function selectOaths(p,ids){const l=ensure(p);if(!atCamp(p))return fail('Choose Oaths at camp.');if(!Array.isArray(ids)||new Set(ids).size!==ids.length||ids.length>oathSlots(p)||ids.some(id=>!l.oaths.includes(id)))return fail('Choose owned Oaths within the available slots.');l.activeOaths=ids.slice();return ok('Mythic Oaths prepared.');}
  function configure(p,patch){if(!atCamp(p))return fail('Prepare your next expedition at camp.');const l=ensure(p);if(!patch||typeof patch!=='object')return fail('Choose valid preferences.');const next={...l.config};for(const [key,value] of Object.entries(patch)){if(key==='favoredElement'){if(value&&(!elements.includes(value)||!l.milestones.includes('legendary_expedition')))return fail('Unlock Legendary Expedition first.');}else if(key==='style'){if(!styles.includes(value)||(value&&!rank(p,'relic_calling'))||(['heal','shield','stun','freeze'].includes(value)&&rank(p,'relic_calling')<2))return fail('Unlock the matching Relic Calling rank.');}else if(key==='build'){if(value&&(!builds.includes(value)||!l.milestones.includes('destiny_board')))return fail('Unlock the Destiny Board first.');}else return fail('Unknown preparation preference.');next[key]=value;}l.config=next;return ok('Expedition preferences saved.');}
  function kitAllowed(p,ids){return Array.isArray(ids)&&ids.length>=1&&ids.length<=2+(ensure(p).activeOaths.includes('relic_monarch')?1:0)&&ids.every(id=>C().items[id]&&!C().items[id].recipe&&p.seenItems.includes(id)&&(C().items[id].unlockRound||1)<=Math.max(1,p.best+1));}
  function saveKit(p,index,kit){const l=ensure(p);if(!atCamp(p)||!l.milestones.includes('heirloom_vault'))return fail('Unlock the Heirloom Vault at camp.');if(!Number.isInteger(index)||index<0||index>2||!kit||typeof kit.name!=='string'||!kitAllowed(p,kit.items))return fail('Kits use up to two discovered, unlocked base relics (three with Relic Monarch).');l.kits[index]={name:kit.name.trim().slice(0,32)||'Expedition kit',items:kit.items.slice()};return ok('Expedition kit saved.');}
  function selectKit(p,index){const l=ensure(p);if(!atCamp(p)||!Number.isInteger(index)||index< -1||index>2)return fail('Choose a kit at camp.');if(index>=0&&(!l.milestones.includes('heirloom_vault')||!l.kits[index]||!kitAllowed(p,l.kits[index].items)))return fail('This kit requires the Heirloom Vault and valid unlocked relics.');l.selectedKit=index;return ok(index<0?'Default kit selected.':'Expedition kit selected.');}
  function snapshot(p){const l=ensure(p);return {version:2,ranks:{...l.ranks,...p.perks},masteries:{...l.masteries},milestones:l.milestones.slice(),constellations:l.constellations.slice(),oaths:l.activeOaths.slice(),config:{...l.config},seenItems:p.seenItems.slice(),seenEnemies:p.seenEnemies.slice()};}
  function validateSnapshot(raw,p){
    const bad=()=>{throw Error('Invalid expedition Legacy snapshot.');};if(!raw||typeof raw!=='object'||raw.version!==2)bad();
    const out={version:2,ranks:{},masteries:{},milestones:[],constellations:[],oaths:[],config:{},seenItems:[],seenEnemies:[]};
    for(const [id,n] of Object.entries(raw.ranks||{})){if(!upgrades[id]||upgrades[id].kind!=='ranked'||!Number.isSafeInteger(n)||n<0||n>upgrades[id].max)bad();out.ranks[id]=n;}
    for(const el of elements){const n=raw.masteries?.[el];if(!Number.isSafeInteger(n)||n<0||n>8)bad();out.masteries[el]=n;}
    const list=(v,allowed,cap=allowed.length)=>{if(!Array.isArray(v)||v.length>cap||new Set(v).size!==v.length||v.some(id=>!allowed.includes(id)))bad();return v.slice();};
    out.milestones=list(raw.milestones,Object.keys(milestones));out.constellations=list(raw.constellations,Object.keys(constellations));out.oaths=list(raw.oaths,Object.keys(oaths),2);out.seenItems=list(raw.seenItems,Object.keys(C().items));out.seenEnemies=list(raw.seenEnemies,C().enemies.map(e=>e.id));
    for(const key of ['favoredElement','style','build']){const value=raw.config?.[key];if(!(key==='favoredElement'?['',...elements]:key==='style'?styles:['',...builds]).includes(value))bad();out.config[key]=value;}
    return out;
  }
  function applyPrepared(p,r,loadout){
    if(!r||r.round!==1||r.time!==0||!loadout||!Array.isArray(loadout.active)||!Array.isArray(loadout.pack))return fail('Use a prepared starting kit.');
    const l=ensure(p);r.active=JSON.parse(JSON.stringify(loadout.active));r.pack=JSON.parse(JSON.stringify(loadout.pack));r.nextUid=loadout.nextUid||Math.max(0,...r.active.concat(r.pack).map(it=>it.uid))+1;
    if(r.active.length>4||r.pack.length>400)return fail('Starting kit exceeds capacity.');
    r.active.forEach((it,i)=>{it.level+=i===0?rank(p,'heirloom_temper'):i===1?rank(p,'second_heirloom'):0;if(l.activeOaths.includes('relic_monarch'))it.level++;});
    if(l.activeOaths.includes('relic_monarch')&&r.active.length<3){const id=C().starter[0];r.active.push({uid:r.nextUid++,id,level:2,cd:0,slot:r.active.length});}
    for(let i=0;i<rank(p,'starter_satchel');i++)r.pack.push({uid:r.nextUid++,id:C().starter[i%C().starter.length],level:1,cd:0,slot:r.pack.length});
    return ok('Prepared kit ready.');
  }
  function start(p,r,make){const l=ensure(p);r.legacy=snapshot(p);r.campUnlocksAtStart=l.unlocks.slice();r.guardians=0;r.dropsThisWave=0;r.emptyWaves=0;r.legacyRewards=0;r.legacyAwards=[];r.relicDiscoveries=0;r.draftRerolls=rank(p,'fate_rerolls');r.banishes=rank(p,'banishing_ink');r.banished=[];r.echoChoices=[];r.echoesLeft=rank(p,'echo_destiny');r.bonusTalentCredits=rank(p,'first_blessing')+(l.activeOaths.includes('fateweaver')?1:0);r.startingTalentCredits=r.bonusTalentCredits;r.bonusTalentsChosen=0;
    const kit=l.kits[l.selectedKit];if(l.milestones.includes('heirloom_vault')&&kit&&kitAllowed(p,kit.items))r.active=kit.items.map((id,slot)=>({...make(id),slot}));
    if(l.activeOaths.includes('relic_monarch')&&r.active.length<3){const id=C().starter.find(id=>!r.active.some(x=>x.id===id))||C().starter[0];r.active.push({...make(id),slot:r.active.length});}
    r.active.forEach((it,i)=>{it.level+=i===0?rank(p,'heirloom_temper'):i===1?rank(p,'second_heirloom'):0;if(l.activeOaths.includes('relic_monarch'))it.level++;});
    const bases=Object.values(C().items).filter(d=>!d.recipe&&p.seenItems.includes(d.id)&&(d.unlockRound||1)<=Math.max(1,p.best+1));for(let i=0;i<rank(p,'starter_satchel');i++){const d=bases.find(d=>d.element===l.config.favoredElement)||bases[i%bases.length]||C().items[C().starter[0]];r.pack.push({...make(d.id),slot:r.pack.length});}
    r.shield=Math.min(r.maxHp*.4,rank(p,'barkskin')*4);
  }
  function recordRegion(p,r){const l=ensure(p);l.stats.highestRegion=Math.max(l.stats.highestRegion,r.region+1);updateUnlocks(p);}
  function award(p,r,label,amount){r.legacyRewards=(r.legacyRewards||0)+amount;r.legacyAwards??=[];r.legacyAwards.push({label,amount});}
  function guardian(p,r){const l=ensure(p);l.stats.guardians++;r.guardians=(r.guardians||0)+1;const region=r.region;if(!l.stats.regions.includes(region)){l.stats.regions.push(region);award(p,r,'First guardian in Region '+(region+1),3+Math.floor(region/4));}for(const n of [4,8,16,35])if(region+1>=n&&!l.stats.achievements.includes('region_'+n)){l.stats.achievements.push('region_'+n);award(p,r,'Region '+n+' milestone',n===35?35:n*2);}if(rank(p,'guardian_hammer')&&r.guardians%4===0&&r.active.length)r.active[(r.guardians/4-1)%r.active.length].level++;if(r.legacy?.oaths.includes('relic_monarch')&&r.active.length)r.active[(r.guardians-1)%r.active.length].level++;if(r.legacy?.oaths.includes('fateweaver')&&r.guardians%4===0)r.bonusTalentCredits++;recordRegion(p,r);}
  function completed(p,r){const l=ensure(p);l.stats.expeditions++;updateUnlocks(p);for(const [id,met,amount] of [['discover_25',p.seenItems.length>=25,5],['discover_100',p.seenItems.length>=100,15],['enemies_50',p.seenEnemies.length>=50,10]])if(met&&!l.stats.achievements.includes(id)){l.stats.achievements.push(id);award(p,r,'Discovery achievement',amount);}}
  function migrate(p,raw){
    if(raw===undefined){ensure(p);return;}
    const bad=()=>{throw Error('Invalid Legacy progression.');},obj=x=>{if(!x||typeof x!=='object'||Array.isArray(x))bad();return x;},num=(x,max=Number.MAX_SAFE_INTEGER)=>{if(!Number.isSafeInteger(x)||x<0||x>max)bad();return x;},list=(x,allowed,max=allowed.length)=>{if(!Array.isArray(x)||x.length>max||new Set(x).size!==x.length||x.some(v=>!allowed.includes(v)))bad();return x.slice();};
    obj(raw);if(raw.version!==2)bad();const l=empty();p.legacy=l;
    for(const [id,value] of Object.entries(obj(raw.ranks))){if(!upgrades[id]||upgrades[id].kind!=='ranked'||['heart','claw','pace'].includes(id))bad();l.ranks[id]=num(value,upgrades[id].max);}
    for(const el of elements)l.masteries[el]=num(raw.masteries?.[el]??0,8);
    l.milestones=list(raw.milestones,Object.keys(milestones));l.constellations=list(raw.constellations,Object.keys(constellations));l.oaths=list(raw.oaths,Object.keys(oaths));if(typeof raw.convergence!=='boolean')bad();l.convergence=raw.convergence;l.activeOaths=list(raw.activeOaths,l.oaths,l.convergence?2:1);
    for(const [id,value] of Object.entries(obj(raw.spent))){if(!upgrades[id])bad();l.spent[id]=num(value);}
    // The ledger must equal the exact deterministic spend for every purchased rank.
    for(const d of Object.values(upgrades)){const n=rank(p,d.id);let expected=0;for(let i=0;i<n;i++)expected+=['heart','claw','pace'].includes(d.id)?oldCost(d.id,i):d.cost??d.baseCost+d.step*i+Math.floor(i*i*.6);if((l.spent[d.id]||0)!==expected)bad();}
    l.unlocks=list(raw.unlocks,[...areas.map(a=>a.id),'constellations','convergence','endgame']);const stats=obj(raw.stats);for(const key of ['guardians','expeditions','highestRegion'])l.stats[key]=num(stats[key],key==='highestRegion'?C().regions.length:Number.MAX_SAFE_INTEGER);l.stats.regions=list(stats.regions,Array.from({length:C().regions.length},(_,i)=>i));l.stats.achievements=list(stats.achievements,['region_4','region_8','region_16','region_35','discover_25','discover_100','enemies_50']);
    obj(raw.config);for(const key of ['favoredElement','style','build']){const v=raw.config[key];if(typeof v!=='string'||!(key==='favoredElement'?['',...elements]:key==='style'?styles:['',...builds]).includes(v))bad();l.config[key]=v;}
    if(!Array.isArray(raw.kits)||raw.kits.length!==3)bad();l.kits=raw.kits.map(k=>{if(k===null)return null;obj(k);if(typeof k.name!=='string'||k.name.length>32||!Array.isArray(k.items)||k.items.length<1||k.items.length>3||k.items.some(id=>!C().items[id]||C().items[id].recipe))bad();return {name:k.name,items:k.items.slice()};});if(!Number.isInteger(raw.selectedKit)||raw.selectedKit< -1||raw.selectedKit>2)bad();l.selectedKit=raw.selectedKit;updateUnlocks(p);
    for(const id of l.milestones){const d=milestones[id];if(d.branchRanks&&branchRanks(p,d.area)<d.branchRanks)bad();if(d.mastery&&l.masteries[d.mastery]<8)bad();}for(const id of l.constellations)if(constellations[id].elements.some(el=>l.masteries[el]<3))bad();
  }
  globalThis.Legacy={version:2,areas,upgrades,masteries,milestones,constellations,oaths,builds,elements,styles,ensure,rank,cost,preview,available,purchase,respec,configure,saveKit,selectKit,selectOaths,oathSlots,areaUnlocked,branchRanks,snapshot,validateSnapshot,start,applyPrepared,recordRegion,guardian,completed,migrate,atCamp};
})();
