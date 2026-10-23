// DOM-free expedition rules. Distances use accepted tracker meters exclusively.
import {ELEMENTS,DIRECTIONS,TYPE_CHART,SPECIES,MOVES,HYBRIDS,GEAR,ITEMS,STARTERS} from './data.mjs';
import {normalizeLands,advanceLand} from './lands.mjs';
import {recordRegionEvent} from './challenges.mjs';
export {ELEMENTS,DIRECTIONS,TYPE_CHART,SPECIES,MOVES,HYBRIDS,GEAR,ITEMS,STARTERS};
export {LANDS,landProgress,selectLand} from './lands.mjs';
export const VERSION=2, FEET=.3048, RESOURCE_STEP=250*FEET, ENCOUNTER_STEP=2000*FEET;
export const CONFIG={partySize:3,discoveryFeet:250,maxDiscoveryAcceleration:.20,encounterFeet:2000,campFeet:6000,
  walkingRecovery:{feet:100,fraction:.02},
  rarity:{common:{stats:1,speed:1,energy:0},uncommon:{stats:1.25,speed:1.03,energy:1},rare:{stats:1.5,speed:1.06,energy:2},ultra:{stats:1.85,speed:1.10,energy:3}},
  elemental:{strong:2.25,resisted:.4,min:.25,max:4,stab:1.25},
  evolution:{firstEssence:6,stats:{hp:20,atk:4,def:3,speed:2,maxEnergy:3}},
  xp:{walkingFeet:100,earlyLevels:3,earlyMultiplier:2,midLevels:5,midMultiplier:1.5,contributionHP:10,maxCombatBase:120},
  captureAttempts:3,capturePity:4,ultraFirstFeet:5000,ultraRepeatFeet:18000,ultraPityFeet:24000,
  ultraApproachMeters:250,ultraChallengeFeet:1300,maxLevel:60,fragmentCost:5,
  tutorial:{hpScales:[.60,.85],loadout:['strike','brace','breathe'],enemyCriticals:false,revivesAfterFirst:1},
  director:{bandsFeet:[4000,10000,20000],levelStepFeet:2600,rareMinFeet:10000,hybridMinFeet:10000,evolutionFeet:[8000,10000,20000],
    pairCycle:2,fullTeamCycle:3,eliteCycle:3,ultraSupportFeet:20000,ultraLevelBonus:2,ultraMinLevel:3},
  masteries:{
    leaf:{name:'Worldroot Vitality',desc:'Maximum health increases by 8%.',multipliers:{hp:1.08}},
    fire:{name:'Crownfire Resolve',desc:'Attack increases by 10%.',multipliers:{atk:1.10}},
    water:{name:'Riverguard Poise',desc:'Defense increases by 10%.',multipliers:{def:1.10}},
    storm:{name:'Skyborne Tempo',desc:'Speed increases by 12%.',multipliers:{speed:1.12}},
    earth:{name:'Bedrock Heart',desc:'Defense increases by 12%.',multipliers:{def:1.12}},
    ice:{name:'Permafrost Mantle',desc:'Maximum health increases by 6% and defense by 4%.',multipliers:{hp:1.06,def:1.04}},
    light:{name:'Radiant Focus',desc:'Maximum energy increases by 2.',flat:{maxEnergy:2}},
    shadow:{name:'Eclipse Prowess',desc:'Attack and speed each increase by 6%.',multipliers:{atk:1.06,speed:1.06}}
  },
  fatigueRound:35,discoveryWeights:{essence:.60,fruit:.25,item:.12,fragment:.03},difficulty:{relaxed:.85,standard:1,challenging:1.18}};
export const ENCOUNTER_STYLES={
  balanced:{roles:[['attacker','caster','utility'],['defender','healer'],['relay','controller']]},
  'guardian-healer':{roles:[['defender'],['healer'],['attacker','caster']]},
  'speed-control':{roles:[['relay','attacker'],['controller'],['utility']]},
  'burn-pressure':{roles:[['caster','attacker'],['drainer'],['controller']],elements:['fire','shadow','leaf']},
  'counter-switch':{roles:[['controller'],['relay','defender'],['attacker']]}
};
export const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
export function random(s){let x=s.rng|0;x^=x<<13;x^=x>>>17;x^=x<<5;s.rng=(x>>>0)||1234567;return s.rng/4294967296;}
const choose=(s,a)=>a[Math.floor(random(s)*a.length)];
const chooseSpecies=(s,ids)=>{const total=ids.reduce((sum,id)=>sum+(SPECIES[id].encounterWeight??1),0);let roll=random(s)*total;for(const id of ids){roll-=SPECIES[id].encounterWeight??1;if(roll<0)return id;}return ids.at(-1);};
const copy=o=>JSON.parse(JSON.stringify(o));
const alive=p=>p&&p.hp>0;
const nativeTypes=id=>[...(SPECIES[id]?.types||[])];
const itemId=id=>({charm:'bondCharm',ribbon:'trailRibbon',scent:'treasureScent',feather:'scoutFeather',campSeed:'campfireSeed'})[id]||id;
export function team(s){return(s.run?.party||[]).map(id=>s.collection[id]).filter(Boolean);}
export function sanctuary(s){return Object.values(s.collection||{});}
const getMonkey=(s,id)=>id?s.collection[id]:s.run.monkey;
function syncActive(s){const r=s.run;r.monkey=s.collection[r.activeId]||s.collection[r.party[0]];r.activeId=r.monkey.id;}
function uid(s,prefix){s.meta.serial=(s.meta.serial||0)+1;return`${prefix}-${(s.rng>>>0).toString(36)}-${s.meta.serial.toString(36)}`;}
export function hybrid(types){return HYBRIDS[[...types].sort().join('+')]||null;}
export function formName(p){return`${p.stage===3?'Ascendant ':p.stage===2?'Elder ':p.stage===1?'Awakened ':''}${p.stage?(hybrid(p.types)?.name||ELEMENTS[p.types[0]]?.name||'Trail')+' ':''}${SPECIES[p.species]?.name||'Monkey'}`;}
export function effectiveness(type,defenders=[]){
  if(type==='neutral'||!type)return 1;
  const types=Array.isArray(defenders)?[...new Set(defenders)]:[];
  return clamp(types.reduce((n,t)=>n*(TYPE_CHART[type]?.[t]??1),1),CONFIG.elemental.min,CONFIG.elemental.max);
}
// A dual attack splits its power evenly instead of selecting its best matchup.
export function moveEffect(move,defenders){const types=move?.types?.length?move.types:[move?.type||'neutral'];return types.reduce((n,t)=>n+effectiveness(t,defenders),0)/types.length;}
// Existing online fights replay with the precise rules they began with.
export function moveEffectLegacy(move,defenders=[]){
  const types=move?.types?.length?move.types:[move?.type||'neutral'];
  const oldEffect=type=>!type||type==='neutral'?1:defenders.reduce((n,t)=>{const value=TYPE_CHART[type]?.[t]??1;return n*(value===CONFIG.elemental.strong?2:value===CONFIG.elemental.resisted?.5:value);},1);
  return types.reduce((n,t)=>n+oldEffect(t),0)/types.length;
}
export function effectInfo(multiplier=1){
  const value=Number.isFinite(multiplier)&&multiplier>0?multiplier:1,kind=value>1+1e-9?'strong':value<1-1e-9?'resisted':'neutral';
  return{multiplier:value,kind,label:kind==='strong'?'Very effective!':kind==='resisted'?'Not effective':'Normal effectiveness'};
}
export function stats(p,gear={}){return calculatedStats(p,gear,false);}
export function statsLegacy(p,gear={}){return calculatedStats(p,gear,true);}
export const legacyStats=statsLegacy;
function calculatedStats(p,gear={},legacy=false){
  const b=SPECIES[p.species],lv=Math.max(0,p.level-1),stage=p.stage||0;
  const step=legacy?{hp:14,atk:2.4,def:1.7,speed:1,maxEnergy:2}:CONFIG.evolution.stats;
  const out={hp:b.hp+lv*8+stage*step.hp,atk:b.atk+lv*2+stage*step.atk,def:b.def+lv*1.4+stage*step.def,speed:b.speed+lv*.65+stage*step.speed,maxEnergy:12+stage*step.maxEnergy};
  if(stage&&p.types.length===1){const defensive=['leaf','water','earth','light','ice'].includes(p.types[0]);out[defensive?'def':'atk']+=stage*2;}
  // Rarity strengthens permanent growth, while equipment keeps its listed value.
  if(!legacy){const rarity=CONFIG.rarity[b.rarity]||CONFIG.rarity.common;for(const key of ['hp','atk','def'])out[key]*=rarity.stats;out.speed*=rarity.speed;out.maxEnergy+=rarity.energy;}
  for(const[id,tier]of Object.entries(gear))if(GEAR[id]&&GEAR[id].stat in out)out[GEAR[id].stat]+=GEAR[id].amount*tier;
  const mastery=stage===3?CONFIG.masteries[p.mastery]:null;if(mastery){for(const[key,factor]of Object.entries(mastery.multipliers||{}))out[key]*=factor;for(const[key,amount]of Object.entries(mastery.flat||{}))out[key]+=amount;}
  if(p.elite)for(const key of['hp','atk','def'])out[key]*=1.12;if(p.bossOnly)out.hp*=1.45;
  if(Number.isFinite(p.tutorialHpScale))out.hp*=p.tutorialHpScale;
  for(const k in out)out[k]=Math.round(out[k]);return out;
}
export function equipped(s,p=s.run.monkey){const g={};for(const id of Object.values(p.equipment||{}))if(id&&s.meta.gear[id])g[id]=s.meta.gear[id];return g;}
export const playerStats=s=>stats(s.run.monkey,equipped(s));
export function unlocked(p){
  const curated=new Set(['strike','brace','breathe',...(SPECIES[p.species]?.learnset||[]),...(p.learned||[])]);
  if(STARTERS.includes(p.species))for(const type of p.types){let n=0;for(const[id,m]of Object.entries(MOVES))if(m.type===type&&!m.types&&!m.signature&&n++<2)curated.add(id);}
  return[...curated].filter(id=>MOVES[id]&&(p.stage||0)>=(MOVES[id].minStage||0)&&(!MOVES[id].signature||SPECIES[p.species]?.signature===id));
}
// UI cards and the equip action share this catalogue/stage eligibility. Learned
// techniques survive evolution; Ultra signatures remain exclusive to their owner.
export function abilityInfo(p,id){
  const move=MOVES[id];if(!p||!move)return null;const species=SPECIES[p.species],requiredStage=move.minStage||0,eligible=unlocked(p).includes(id),equippedSlot=p.loadout?.indexOf(id)??-1;
  let lockedReason=null;if(!eligible){
    if(move.signature&&species?.signature!==id){const owner=Object.values(SPECIES).find(s=>s.signature===id);lockedReason=`Exclusive to ${owner?.name||'its Ultra species'}.`;}
    else if((p.stage||0)<requiredStage)lockedReason=`Unlocks at ${['Base','Awakened','Elder','Ascendant'][requiredStage]||`stage ${requiredStage}`}.`;
    else lockedReason='Outside this monkey’s curated learnset.';
  }
  return{id,move,eligible,requiredStage,lockedReason,required:id==='strike',equippedSlot};
}
export function say(s,text){s.log.push(text);s.log=s.log.slice(-16);}
function monkey(s,species,level=1,types=nativeTypes(species),stage=0,prefix='m'){
  const p={id:uid(s,prefix),species,nickname:'',level,types:[...types],stage,hp:1,energy:12,status:{},statusPower:{},
    loadout:['strike','brace','breathe'],training:0,xpRemainder:0,bond:0,learned:[],discovered:[],highestForm:{stage:0,types:nativeTypes(species)},
    equipment:{weapon:null,armor:null,charm:null},cooldowns:{},uses:{}};
  p.learned=unlocked(p);const specials=p.learned.filter(id=>!['strike','brace','breathe'].includes(id));
  p.loadout=['strike',...specials.slice(0,2),'brace'];if(p.loadout.length<4)p.loadout.splice(p.loadout.length-1,0,'breathe');const maximum=stats(p);p.hp=maximum.hp;p.energy=maximum.maxEnergy;return p;
}
function resetExpeditionMonkey(p){p.level=1;p.types=nativeTypes(p.species);p.stage=0;p.status={};p.statusPower={};p.training=0;p.xpRemainder=0;delete p.statusCredit;p.cooldowns={};p.uses={};p.shield=0;delete p.elite;delete p.bossOnly;delete p.mastery;const available=unlocked(p);p.loadout=['strike',...p.loadout.filter(id=>id!=='strike'&&available.includes(id)),...available].filter((id,i,a)=>a.indexOf(id)===i).slice(0,4);p.hp=stats(p).hp;p.energy=stats(p).maxEnergy;}
export function newSave(name,species='capuchin',mode='gps',seed=Date.now()>>>0){
  if(!STARTERS.includes(species))throw Error('Choose one of the six starters.');
  const s={version:VERSION,name:String(name).slice(0,30),mode,rng:(seed>>>0)||1234567,difficulty:'standard',collection:{},
    meta:{runs:0,legacy:0,best:0,serial:0,gear:{},fragments:{},equipped:{weapon:null,armor:null,charm:null},history:[],claims:{},captureFailures:0,bondMaterials:{}},run:null,log:[]};
  const p=monkey(s,species);s.collection[p.id]=p;restart(s,species);return s;
}
export function restart(s,species=s.run?.monkey.species||'capuchin'){
  if(s.run&&s.run.phase!=='dead')throw Error('Your expedition is still active.');
  normalizeLands(s);
  const oldParty=s.run?.party?.filter(id=>s.collection[id])||[Object.keys(s.collection)[0]];for(const p of sanctuary(s))resetExpeditionMonkey(p);
  s.meta.runs++;const id=uid(s,'run'),bonus=Math.min(20,s.meta.legacy||0);
  s.run={id,phase:'trail',distance:0,nextCache:RESOURCE_STEP,nextEncounter:ENCOUNTER_STEP,nextCamp:CONFIG.campFeet*FEET,
    discoveryProgress:0,discoveries:0,wins:0,encounters:0,ordinaryWins:0,party:oldParty.slice(0,3),activeId:oldParty[0],monkey:null,
    fruit:8+bonus,essence:Object.fromEntries(Object.keys(ELEMENTS).map(t=>[t,0])),
    items:Object.fromEntries(Object.keys(ITEMS).map(id=>[id,({berry:2,tonic:1,salve:1})[id]||0])),
    vector:{east:0,north:0},weights:{N:0,S:0,E:0,W:0},direction:'N',battle:null,capture:null,camp:null,events:[],
    bonuses:{ribbon:0,scent:0,feather:0,campSeed:0},injuries:{},ultra:null,nextUltra:CONFIG.ultraFirstFeet*FEET,startedAt:Date.now()};
  s.run.items.charmMaterial=0;syncActive(s);for(const p of sanctuary(s))p.hp=stats(p,equipped(s,p)).hp;
  s.log=[];say(s,`Expedition ${s.meta.runs}: choose up to three companions before tracking. Your sanctuary and equipment endure.`);return s;
}
export function direction(east,north,fallback='N'){if(Math.hypot(east,north)<2)return fallback;return Math.abs(north)>=Math.abs(east)?north>=0?'N':'S':east>=0?'E':'W';}
export function odds(s){const w=s.run.weights,total=Object.values(w).reduce((a,b)=>a+b,0);return Object.fromEntries(Object.keys(w).map(d=>[DIRECTIONS[d],total?.1+.6*w[d]/total:.25]));}
function rollElement(s){if(random(s)<.22)return choose(s,['earth','ice','light','shadow']);let n=random(s);for(const[type,p]of Object.entries(odds(s))){n-=p;if(n<=0)return type;}return'leaf';}
export function discoveryInterval(s){const satchel=team(s).some(p=>Object.values(p.equipment||{}).some(id=>GEAR[id]?.discoveryReduction));const reduction=Math.min(CONFIG.maxDiscoveryAcceleration,(satchel?.05:0)+(s.run.bonuses.ribbon>0?.15:0));return CONFIG.discoveryFeet*FEET*(1-reduction);}
function updateNextCache(s){s.run.nextCache=s.run.distance+(1-s.run.discoveryProgress)*discoveryInterval(s);}
function cache(s){
  const r=s.run;r.discoveries++;let roll=random(s);if(r.bonuses.scent>0){r.bonuses.scent--;roll=Math.min(.999,roll+.06);}const w=CONFIG.discoveryWeights;
  if(roll<w.essence){const type=random(s)<.22?choose(s,['earth','ice','light','shadow']):DIRECTIONS[r.direction];r.essence[type]++;say(s,`Discovery: one ${ELEMENTS[type].name} essence.`);r.lastDiscovery={category:'essence',id:type,amount:1};}
  else if(roll<w.essence+w.fruit){r.fruit+=2;say(s,'Discovery: two training fruit.');r.lastDiscovery={category:'fruit',amount:2};}
  else if(roll<w.essence+w.fruit+w.item){const id=choose(s,['berry','tonic','salve','bondCharm','trailRibbon','treasureScent','scoutFeather','campfireSeed']);r.items[id]++;say(s,`Discovery: ${ITEMS[id].name}.`);r.lastDiscovery={category:'item',id,amount:1};}
  else{const id=choose(s,Object.keys(GEAR));s.meta.fragments[id]=(s.meta.fragments[id]||0)+1;say(s,`Discovery: a ${GEAR[id].name} fragment (${s.meta.fragments[id]}/${CONFIG.fragmentCost}).`);r.lastDiscovery={category:'fragment',id,amount:1};}
  recordRegionEvent(s,'discoveries');
}
function acceptedDiscoveries(s,meters,onDiscovery){
  const r=s.run;let remaining=meters;while(remaining>1e-8){const interval=discoveryInterval(s),untilDiscovery=(1-r.discoveryProgress)*interval,untilBonus=r.bonuses.ribbon>0?r.bonuses.ribbon:Infinity;
    const chunk=Math.min(remaining,untilDiscovery,untilBonus);r.discoveryProgress+=chunk/interval;remaining-=chunk;if(r.bonuses.ribbon>0)r.bonuses.ribbon=Math.max(0,r.bonuses.ribbon-chunk);
    if(r.discoveryProgress>=1-1e-9){r.discoveryProgress=Math.max(0,r.discoveryProgress-1);cache(s);onDiscovery?.({...r.lastDiscovery});}if(chunk<1e-10&&r.discoveryProgress<1-1e-9)break;
  }
}
export function activateBonus(s,id){id=itemId(id);const r=s.run;if(!['trail','camp'].includes(r.phase)||!(r.items[id]>0))throw Error('That walking supply is unavailable.');
  if(!['trailRibbon','treasureScent','scoutFeather','campfireSeed'].includes(id))throw Error('Choose a walking bonus.');r.items[id]--;
  if(id==='trailRibbon')r.bonuses.ribbon=Math.max(r.bonuses.ribbon,1000*FEET);if(id==='treasureScent')r.bonuses.scent=Math.min(6,r.bonuses.scent+3);if(id==='scoutFeather'){if(r.ultra&&r.ultra.status!=='claimed'){r.ultra.scouted=true;r.ultra.revealed=true;}else r.bonuses.feather=1;}if(id==='campfireSeed')r.bonuses.campSeed=1;updateNextCache(s);return r.bonuses;
}
function maybeUltra(s){
  const r=s.run;if(r.ultra&&r.ultra.status!=='claimed'||r.ordinaryWins<2||r.distance+1e-6<r.nextUltra)return;
  const pool=Object.keys(SPECIES).filter(id=>SPECIES[id].rarity==='ultra'||SPECIES[id].ultra);if(!pool.length)return;
  const species=choose(s,pool),plan=encounterPlan(s,true);if(MOVES[SPECIES[species].signature]?.switch)plan.size=Math.max(2,plan.size);
  r.ultra={id:uid(s,'ultra'),species,difficulty:plan.level,plan,bearing:Math.round(random(s)*359),scouted:!!r.bonuses.feather,revealed:!!r.bonuses.feather,
    spawnDistance:CONFIG.ultraApproachMeters,challengeRemaining:CONFIG.ultraChallengeFeet*FEET,tracked:false,status:'sighting',safeAlternative:false,claimed:false};r.bonuses.feather=0;
  say(s,`Ultra sighting: ${SPECIES[species].name}. Track it or choose your own safe route; there is no countdown.`);
}
function enqueue(s,event){if(!s.run.events.some(e=>e.id===event.id))s.run.events.push(event);}
function pump(s){
  const r=s.run;if(r.phase!=='trail'||r.capture)return;r.events.sort((a,b)=>({encounter:0,ultra:1,camp:2}[a.type]??3)-({encounter:0,ultra:1,camp:2}[b.type]??3));
  const event=r.events.shift();if(!event){maybeUltra(s);return;}if(event.type==='encounter')encounter(s);else if(event.type==='ultra')encounter(s,true);else if(event.type==='camp'){
    r.phase='camp';r.camp={id:event.id,heals:2+(r.bonuses.campSeed?1:0),seedBoost:!!r.bonuses.campSeed};r.bonuses.campSeed=0;say(s,'Trail camp: limited recovery, crafting, and conscious teammate changes. Revive fallen teammates before replacing them.');}
}
export function advance(s,meters,east=0,north=0,onDiscovery){
  const r=s.run;if(r.phase!=='trail'||![meters,east,north].every(Number.isFinite)||meters<=0)return 0;
  const next=Math.min(r.nextEncounter,r.nextCamp),used=Math.max(0,Math.min(meters,next-r.distance)),ratio=used/meters;
  advanceLand(s,used,east*ratio,north*ratio);
  for(const p of team(s)){
    grantXp(p,used/(CONFIG.xp.walkingFeet*FEET));
    // Fractional HP preserves exactly 2% per 100 feet, even for low-HP species.
    if(alive(p)){const max=stats(p,equipped(s,p)).hp;p.hp=Math.min(max,p.hp+max*CONFIG.walkingRecovery.fraction*used/(CONFIG.walkingRecovery.feet*FEET));}
  }
  r.vector.east+=east*ratio;r.vector.north+=north*ratio;r.direction=direction(r.vector.east,r.vector.north,r.direction);r.weights[r.direction]+=used;acceptedDiscoveries(s,used,onDiscovery);r.distance+=used;
  if(r.ultra?.safeAlternative&&r.ultra.status==='challenge'){r.ultra.challengeRemaining=Math.max(0,r.ultra.challengeRemaining-used);if(r.ultra.challengeRemaining<=1e-7){r.ultra.status='ready';say(s,'Your safe-route Ultra challenge is complete. Stop somewhere safe and begin when ready.');}}
  if(r.distance+1e-6>=r.nextEncounter){const milestone=r.nextEncounter;enqueue(s,{type:'encounter',id:`encounter-${milestone}`});r.nextEncounter+=CONFIG.encounterFeet*FEET;}
  if(r.distance+1e-6>=r.nextCamp){const milestone=r.nextCamp;enqueue(s,{type:'camp',id:`camp-${milestone}`});r.nextCamp+=CONFIG.campFeet*FEET;}
  updateNextCache(s);maybeUltra(s);pump(s);return used;
}
export function encounterPool(s,ultraSupport=false){const feet=s.run.distance/FEET,d=CONFIG.director;return Object.keys(SPECIES).filter(id=>SPECIES[id].rarity!=='ultra'&&!SPECIES[id].ultra&&(ultraSupport||((!SPECIES[id].expansion||feet>=SPECIES[id].minFeet)&&(SPECIES[id].rarity!=='rare'||feet>=d.rareMinFeet)&&(nativeTypes(id).length<2||feet>=d.hybridMinFeet))));}
export function encounterPlan(s,ultra=false){
  const feet=s.run.distance/FEET,index=s.run.encounters,scale=CONFIG.difficulty[s.difficulty]||1,d=CONFIG.director,[early,middle,deep]=d.bandsFeet;
  const size=feet<early?1:feet<middle?(index%d.pairCycle?2:1):feet<deep?(index%d.fullTeamCycle===0?3:2):3,
    level=clamp(Math.round((1+Math.floor(feet/d.levelStepFeet))*scale),1,CONFIG.maxLevel),stage=d.evolutionFeet.filter(threshold=>feet>=threshold).length;
  return{size:ultra?(feet>=d.ultraSupportFeet?2:1):size,level:ultra?Math.max(d.ultraMinLevel,level+d.ultraLevelBonus):level,stage,style:choose(s,Object.keys(ENCOUNTER_STYLES)),elite:feet>=deep&&index%d.eliteCycle===0};
}
function encounter(s,isUltra=false){
  const r=s.run;r.encounters++;const plan=isUltra&&r.ultra.plan?r.ultra.plan:encounterPlan(s,isUltra),pool=encounterPool(s,isUltra),enemies=[];
  for(let i=0;i<plan.size;i++){
    const style=ENCOUNTER_STYLES[plan.style],roles=style.roles[i%style.roles.length],rolePool=pool.filter(id=>roles.includes(SPECIES[id].role)),themed=rolePool.filter(id=>!style.elements||nativeTypes(id).some(t=>style.elements.includes(t)));
    let species=isUltra&&i===0?r.ultra.species:chooseSpecies(s,themed.length?themed:rolePool.length?rolePool:pool);if(!isUltra&&r.ordinaryWins<2)species=r.ordinaryWins===0?'mossmop':'cinderpaw';
    const tutorial=r.ordinaryWins<2&&!isUltra,stage=tutorial?0:plan.stage,p=monkey(s,species,plan.level,nativeTypes(species),stage,'enemy');p.elite=plan.elite;p.bossOnly=isUltra&&i===0;
    if(tutorial)p.tutorialHpScale=CONFIG.tutorial.hpScales[r.ordinaryWins];p.hp=stats(p).hp;p.energy=stats(p).maxEnergy;
    const moves=unlocked(p).filter(id=>!['strike','brace','breathe'].includes(id));
    const priority=m=>MOVES[m].signature?100:SPECIES[species].role==='healer'?(MOVES[m].teamHeal&&plan.size>1?90:MOVES[m].heal?70:0):SPECIES[species].role==='defender'?(MOVES[m].teamShield&&plan.size>1?85:MOVES[m].shield||MOVES[m].counter?65:0):SPECIES[species].role==='relay'?(MOVES[m].switch?60:0):SPECIES[species].role==='controller'?(MOVES[m].root?60:0):0;
    const ranked=moves.sort((a,b)=>priority(b)-priority(a)||(MOVES[b].power||18)-(MOVES[a].power||18));
    // One defining utility, one elemental strike, and an energy recovery action.
    const first=ranked[0],attack=ranked.find(id=>MOVES[id].power&&id!==first)||ranked[1];p.loadout=tutorial?[...CONFIG.tutorial.loadout]:['strike',first,attack,'breathe'].filter((id,index,a)=>id&&a.indexOf(id)===index);enemies.push(p);
  }
  r.battle={id:isUltra?r.ultra.id:uid(s,'encounter'),enemy:enemies[0],enemies,enemyActive:0,turn:1,round:1,ready:false,boss:isUltra,ultra:isUltra,style:plan.style,intent:'strike',enemyAction:{type:'move',id:'strike'},rewarded:false,bondBonuses:{},pendingRally:null,enemyRally:null};
  for(const p of team(s)){p.cooldowns={};p.uses={};}
  r.phase='battle';if(isUltra)r.ultra.status='battle';planEnemy(s);say(s,`${isUltra?'Ultra arrival':'Wild team'}: ${enemies.map(p=>formName(p)).join(' · ')}. Stop safely before beginning.`);
}
export function trackUltra(s){if(!s.run.ultra||s.run.ultra.status==='claimed')throw Error('No current Ultra sighting.');s.run.ultra.tracked=true;return s.run.ultra;}
export function safeUltraAlternative(s){const u=trackUltra(s);if(u.status==='battle')throw Error('Finish this battle first.');u.safeAlternative=true;u.status=u.challengeRemaining<=0?'ready':'challenge';say(s,'Safe route selected: the same Ultra awaits after additional accepted walking distance.');return u;}
export function beginUltra(s,{gpsFresh=false,withinRadius=false}={}){
  const r=s.run,u=r.ultra;if(!u||u.claimed||!['sighting','challenge','ready'].includes(u.status))throw Error('That Ultra is unavailable.');if(u.safeAlternative?u.challengeRemaining>1e-6:!(gpsFresh&&withinRadius))throw Error(u.safeAlternative?'Finish the accepted-distance challenge first.':'Approach with fresh acceptable GPS, or choose the safe-route alternative.');
  u.tracked=true;enqueue(s,{type:'ultra',id:u.id});if(r.phase==='trail')pump(s);return r.phase==='battle'&&r.battle.ultra;
}
function planEnemy(s){
  const b=s.run.battle,p=s.run.monkey,e=b.enemy;if(!e)return;const options=e.loadout.filter(id=>MOVES[id]&&(MOVES[id].cost||0)<=e.energy&&moveAvailable(b,e,id)&&(!MOVES[id].switch||!e.status.root&&b.enemies.some(m=>m.id!==e.id&&alive(m))));
  let id=options.find(id=>MOVES[id].cleanse&&Object.keys(e.status).some(k=>['burn','poison','weaken','root'].includes(k)));
  if(!id)id=options.find(id=>MOVES[id].heal&&e.hp<stats(e).hp*.40);
  if(!id)id=options.find(id=>MOVES[id].teamHeal&&b.enemies.some(m=>alive(m)&&m.hp<stats(m).hp*.55));
  if(!id)id=options.find(id=>MOVES[id].shield&&!e.shield&&e.hp<stats(e).hp*.65&&random(s)<.6);
  if(!id)id=options.find(id=>MOVES[id].root&&team(s).filter(alive).length>1&&!p.status.root&&random(s)<.55);
  if(!id)id=options.find(id=>MOVES[id].teamShield&&MOVES[id].switch&&e.hp<stats(e).hp*.45);
  if(!id&&e.energy<3&&options.includes('breathe'))id='breathe';
  if(!id){const ranked=options.filter(id=>MOVES[id].power).sort((a,c)=>MOVES[c].power*moveEffect(MOVES[c],p.types)-MOVES[a].power*moveEffect(MOVES[a],p.types));id=(random(s)<.8?ranked[0]:choose(s,ranked.slice(0,3)))||'strike';}
  const choices=b.enemies.filter(m=>m.id!==e.id&&alive(m));if(!e.status.root&&choices.length&&(e.hp<stats(e).hp*.23||moveEffect(MOVES[id],p.types)<.8)&&random(s)<.35)b.enemyAction={type:'switch',id:choices[0].id};else b.enemyAction={type:'move',id};b.intent=b.enemyAction.type==='switch'?'brace':id;
}
export function readyBattle(s){if(s.run.phase==='battle')s.run.battle.ready=true;}
export function score(s){const r=s.run;if(r.phase==='dead'&&Number.isFinite(r.finalScore))return r.finalScore;return Math.floor(r.distance/FEET+1e-6)+r.wins*500+team(s).reduce((n,p)=>n+p.stage*300+(p.level-1)*40,0);}
function endRun(s){
  const r=s.run;if(r.phase==='dead')return;const points=score(s),active=r.monkey,earned=Math.max(1,Math.floor(r.wins/2)+Math.floor(r.distance/(4000*FEET)));
  s.meta.legacy=Math.min(100,s.meta.legacy+earned);s.meta.best=Math.max(s.meta.best,points);s.meta.history.unshift({id:r.id,name:s.name,score:points,feet:Math.floor(r.distance/FEET+1e-6),wins:r.wins,level:active.level,form:formName(active),mode:s.mode,endedAt:Date.now()});s.meta.history=s.meta.history.slice(0,25);
  r.finalScore=points;r.phase='dead';r.battle=null;r.capture=null;r.events=[];r.fruit=0;for(const k in r.essence)r.essence[k]=0;for(const k in r.items)r.items[k]=0;
  for(const p of sanctuary(s)){resetExpeditionMonkey(p);p.hp=0;}say(s,`The whole expedition team has fallen. ${points} points; sanctuary companions, bonds, branches, and equipment are preserved.`);
}
export function xpMultiplier(p){return p.level<=CONFIG.xp.earlyLevels?CONFIG.xp.earlyMultiplier:p.level<=CONFIG.xp.midLevels?CONFIG.xp.midMultiplier:1;}
function grantXp(p,base){
  const total=(p.xpRemainder||0)+base*xpMultiplier(p),earned=Math.floor(total+1e-9);
  p.training+=earned;p.xpRemainder=Math.max(0,total-earned);return earned;
}
function awardXp(s,pool){
  const members=team(s),max=Math.max(...members.map(p=>p.level)),weights=members.map(p=>(p.id===s.run.activeId?1.1:1)*(p.level<max?1.2:1)),total=weights.reduce((a,b)=>a+b,0);let left=pool,earned=0;
  members.forEach((p,i)=>{const xp=i===members.length-1?left:Math.floor(pool*weights[i]/total);earned+=grantXp(p,xp);left-=xp;p.bond++;});return earned;
}
function contribution(s,p,damage=0,healing=0){
  const b=s.run.battle;if(!b||!p||s.collection[p.id]!==p||!s.run.party.includes(p.id))return;
  b.contributions??={};const c=b.contributions[p.id]??={damage:0,healing:0,base:0,xp:0};
  c.damage+=Math.max(0,damage);c.healing+=Math.max(0,healing);
  // Count actual HP, never overkill/overheal, and bound repeat-healing rewards.
  const cap=Math.min(CONFIG.xp.maxCombatBase,Math.max(10,Math.ceil(b.enemies.reduce((n,e)=>n+stats(e).hp,0)*.3)));
  const next=Math.min(cap,(c.damage+c.healing)/CONFIG.xp.contributionHP);
  c.xp+=grantXp(p,Math.max(0,next-c.base));c.base=next;
}
function victory(s){
  const r=s.run,b=r.battle;if(!b||b.rewarded||s.meta.claims[`reward:${b.id}`])return;b.rewarded=true;s.meta.claims[`reward:${b.id}`]=true;r.wins++;if(!b.ultra)r.ordinaryWins++;recordRegionEvent(s,'victories');
  const baseXp=18+b.enemies.reduce((n,e)=>n+e.level*2,0)+b.enemies.length*4,xp=awardXp(s,baseXp);r.fruit+=2;const type=b.enemy.types[0]||rollElement(s);r.essence[type]+=2;r.items.charmMaterial=(r.items.charmMaterial||0)+1;
  const tutorial=!b.ultra&&r.ordinaryWins<=2;if(tutorial)r.items.bondCharm+=2;if(!b.ultra&&r.ordinaryWins===1){r.items.revive+=CONFIG.tutorial.revivesAfterFirst;say(s,'Introductory revival supply: restore a fallen teammate before replacing them at camp.');}
  // An Ultra's support squad cannot consume the guaranteed boss invitation.
  const eligible=b.ultra?[b.enemies.find(e=>e.species===r.ultra.species)||b.enemies[0]]:b.enemies;
  r.capture={id:b.id,candidates:eligible.map(e=>({...copy(e),bondBonus:b.bondBonuses[e.id]||0})),attempts:0,maxAttempts:CONFIG.captureAttempts,guaranteed:tutorial||b.ultra&&!sanctuary(s).some(p=>p.species===r.ultra.species),ultra:b.ultra,claimed:false};
  r.lastBattleFeedback=copy(b.hitFeedback||[]);r.lastBattleHits=r.lastBattleFeedback;
  r.battle=null;r.phase='befriend';r.vector={east:0,north:0};r.weights={N:0,S:0,E:0,W:0};say(s,`Victory: ${xp} experience shared across the team, two fruit, essence, and charm material. Choose one defeated companion to befriend.`);
}
export function captureChance(s,id){const c=s.run.capture,p=c?.candidates.find(p=>p.id===id);if(!p||c.claimed)return 0;if(c.guaranteed||s.meta.captureFailures>=CONFIG.capturePity)return 1;return clamp((({common:.85,uncommon:.65,rare:.45,ultra:.65})[SPECIES[p.species].rarity]??.85)+(p.bondBonus||0),.05,.99);}
function finishCapture(s){s.run.capture=null;s.run.phase='trail';maybeUltra(s);pump(s);}
export function befriend(s,id,{duplicate='keep',nickname=''}={}){
  const r=s.run,c=r.capture,p=c?.candidates.find(p=>p.id===id);if(r.phase!=='befriend'||!p||c.claimed||s.meta.claims[`capture:${c.id}`])throw Error('This recruitment opportunity is already resolved.');if(c.attempts>=c.maxAttempts)throw Error('No attempts remain.');
  const free=c.ultra&&c.guaranteed;if(!free&&!(r.items.bondCharm>0))throw Error('A Bond Charm is required. Craft one at a camp.');const chance=captureChance(s,id);if(!free)r.items.bondCharm--;c.attempts++;
  if(chance<1&&random(s)>=chance){s.meta.captureFailures++;say(s,`The charm did not connect. Bad-luck protection: ${s.meta.captureFailures}/${CONFIG.capturePity}.`);return{success:false,chance,remaining:c.maxAttempts-c.attempts};}
  const duplicateFound=sanctuary(s).some(m=>m.species===p.species),level=Math.max(1,Math.min(3,Math.floor(team(s).reduce((n,m)=>n+m.level,0)/team(s).length)-1));
  const recruit=monkey(s,p.species,level);recruit.nickname=String(nickname).trim().slice(0,24);recruit.bond=1;recruit.capturedEncounter=c.id;s.meta.captureFailures=0;s.meta.claims[`capture:${c.id}`]=true;c.claimed=true;s.collection[recruit.id]=recruit;
  if(r.party.length<CONFIG.partySize){r.party.push(recruit.id);r.injuries[recruit.id]={hp:recruit.hp,energy:recruit.energy};}r.lastCapture={id:recruit.id,species:recruit.species,duplicate:duplicateFound,converted:false};
  if(c.ultra){r.ultra.claimed=true;r.ultra.status='claimed';r.nextUltra=r.distance+CONFIG.ultraRepeatFeet*FEET;}
  const result={success:true,monkey:recruit,duplicate:duplicateFound,bondConverted:false,chance};if(duplicate==='bond'&&duplicateFound){convertDuplicate(s,recruit.id);result.bondConverted=true;}
  say(s,`${SPECIES[recruit.species].name} joined ${r.party.includes(recruit.id)?'your expedition team':'the sanctuary; bring it at a camp or before your next expedition'}.`);finishCapture(s);return result;
}
export function skipBefriend(s){if(s.run.phase!=='befriend')return;const c=s.run.capture;if(c.ultra){s.run.ultra.claimed=true;s.run.ultra.status='claimed';s.run.nextUltra=s.run.distance+CONFIG.ultraRepeatFeet*FEET;}s.meta.claims[`capture:${c.id}`]='passed';finishCapture(s);}
export function renameMonkey(s,id,name){const p=s.collection[id];if(!p)throw Error('Unknown companion.');p.nickname=String(name).trim().slice(0,24);return p;}
export function convertDuplicate(s,id){const p=s.collection[id],r=s.run,last=r.lastCapture;if(!p||last?.id!==id||last.converted||!last.duplicate||!alive(p)||!sanctuary(s).some(other=>other.id!==id&&other.species===p.species))throw Error('Only the newly recruited conscious duplicate may be converted.');
  if(r.party.includes(id)){if(!r.party.some(other=>other!==id&&alive(s.collection[other])))throw Error('Keep a conscious expedition companion.');r.party=r.party.filter(other=>other!==id);if(r.activeId===id){r.activeId=r.party.find(other=>alive(s.collection[other]));syncActive(s);}}
  s.meta.bondMaterials[p.species]=(s.meta.bondMaterials[p.species]||0)+3;delete s.collection[id];last.converted=true;return 3;}
function heal(p,amount,max){if(!alive(p))return 0;const before=p.hp;p.hp=Math.min(max,p.hp+Math.round(amount*(p.species==='langur'?1.25:1)));return Math.max(0,p.hp-before);}
function moveAvailable(b,p,id){const m=MOVES[id];return!(p.cooldowns[id]>b.round)&&!(m.uses&&((p.uses[id]||0)>=m.uses));}
function applyStatus(p,id,duration=2,power){p.status[id]=Math.max(p.status[id]||0,duration);if(power!==undefined)p.statusPower[id]=power;}
function enter(s,p,side){
  const b=s.run.battle;if(side==='player'){s.run.activeId=p.id;syncActive(s);}else{b.enemyActive=b.enemies.findIndex(m=>m.id===p.id);b.enemy=p;}
  const rally=side==='player'?b.pendingRally:b.enemyRally;if(rally&&(!rally.expires||rally.expires>=b.round)){if(rally.shield){p.shield=Math.max(p.shield||0,Math.round(stats(p).hp*rally.shield));applyStatus(p,'shield',rally.duration||2);}if(rally.prepare){p.prepare=rally.prepare;applyStatus(p,'prepare',rally.duration||2);}if(side==='player')b.pendingRally=null;else b.enemyRally=null;}
}
function replacement(s,id,side='player',forced=false){
  const b=s.run.battle,actor=side==='player'?s.run.monkey:b.enemy,members=side==='player'?team(s):b.enemies,incoming=members.find(p=>p.id===id);
  if(!incoming||incoming.id===actor.id||!alive(incoming))throw Error('Choose another conscious teammate.');if(!forced&&actor.status.root)throw Error('Rooted Challenge briefly prevents voluntary switching.');enter(s,incoming,side);say(s,`${side==='player'?'You':'Rival'} bring in ${incoming.nickname||SPECIES[incoming.species].name}.`);return incoming;
}
function execute(s,actor,target,id,actorStats,targetStats,label,targetId,side='player'){
  const b=s.run.battle,m=MOVES[id],duration=m.duration||2,members=side==='player'?team(s):b.enemies;actor.energy-=m.cost||0;actor.uses[id]=(actor.uses[id]||0)+1;if(m.cooldown)actor.cooldowns[id]=b.round+m.cooldown;
  let restored=0;
  if(m.cleanse){actor.statusCredit={};actor.status={};actor.statusPower={};actor.shield=0;actor.counter=0;actor.prepare=0;}
  if(m.guard){actor.guard=actor.species==='mandrill'?.3:.45;actor.energy=Math.min(actorStats.maxEnergy,actor.energy+2);}
  if(m.energy)actor.energy=Math.min(actorStats.maxEnergy,actor.energy+m.energy);if(m.heal)restored+=heal(actor,actorStats.hp*m.heal,actorStats.hp);
  if(m.teamHeal)for(const p of members)restored+=heal(p,stats(p,side==='player'?equipped(s,p):{}).hp*m.teamHeal,stats(p,side==='player'?equipped(s,p):{}).hp);
  if(m.shield){actor.shield=Math.max(actor.shield||0,Math.round(actorStats.hp*m.shield));applyStatus(actor,'shield',duration);}
  if(m.counter){actor.counter=m.counter;applyStatus(actor,'counter',m.duration||1);}
  if(m.prepare){actor.prepare=m.prepare;applyStatus(actor,'prepare',duration);}
  if(m.statusSelf)applyStatus(actor,m.statusSelf,duration);
  if(m.rally){const value={prepare:typeof m.rally==='number'?m.rally:.18,duration:m.duration||3,expires:b.round+(m.duration||3)};if(side==='player')b.pendingRally=value;else b.enemyRally=value;}
  if(m.teamShield&&!m.switch){const value={shield:m.teamShield,duration,expires:b.round+duration};if(side==='player')b.pendingRally={...b.pendingRally,...value};else b.enemyRally={...b.enemyRally,...value};}
  if(m.root)applyStatus(target,'root',typeof m.root==='number'?m.root:2);if(m.weaken)applyStatus(target,'weaken',duration,typeof m.weaken==='number'?m.weaken:.25);
  if(m.bondBonus)b.bondBonuses[target.id]=Math.min(.25,(b.bondBonuses[target.id]||0)+m.bondBonus);
  if(m.power){
    if(m.breakShield){target.shield=0;target.guard=0;delete target.status.shield;}
    const eff=moveEffect(m,target.types),stab=(m.types||[m.type]).some(t=>actor.types.includes(t))?CONFIG.elemental.stab:1,critRoll=random(s),crit=(!actor.tutorialHpScale||CONFIG.tutorial.enemyCriticals)&&critRoll<(actor.species==='macaque'?.14:.07),howl=actor.species==='howler'&&m.type!=='neutral'?1.12:1;
    let damage=Math.max(1,Math.round((m.power+actorStats.atk*.70-targetStats.def*.42)*eff*stab*howl*(crit?1.4:1)*(.94+random(s)*.12)*(target.guard||1)*(actor.prepare?1+actor.prepare:1)*(actor.status.weaken?1-(actor.statusPower.weaken||.25):1)));
    if(actor.prepare){actor.prepare=0;delete actor.status.prepare;}if(actor.status.weaken){delete actor.status.weaken;delete actor.statusPower.weaken;}if(target.species==='spider'&&random(s)<.08)damage=0;
    if(target.shield){const absorbed=Math.min(target.shield,damage);target.shield-=absorbed;damage-=absorbed;}const actualDamage=Math.min(target.hp,damage);target.hp=Math.max(0,target.hp-damage);if(side==='player')contribution(s,actor,actualDamage);
    const hit={actorId:actor.id,targetId:target.id,actorName:actor.nickname||SPECIES[actor.species].name,targetName:target.nickname||SPECIES[target.species].name,moveId:id,side,multiplier:eff,damage:actualDamage,critical:crit,round:b.round};
    b.lastHit=hit;(b.hitFeedback??=[]).push(hit);b.lastHits=b.hitFeedback;
    const feedback=effectInfo(eff);say(s,`${label}: ${m.name} → ${actualDamage}${crit?' critical':''}${feedback.kind!=='neutral'?` · ${feedback.label} (×${eff})`:''}.`);
    if(target.counter&&damage){const reflected=Math.min(actor.hp,Math.max(1,Math.round(damage*target.counter)));actor.hp=Math.max(0,actor.hp-reflected);contribution(s,target,reflected);target.counter=0;delete target.status.counter;}
    if(m.status&&alive(target)){const immune=m.status==='burn'&&target.types.includes('fire')||m.status==='poison'&&target.types.includes('leaf');if(!immune){applyStatus(target,m.status,duration);target.statusCredit??={};if(side==='player')target.statusCredit[m.status]=actor.id;else delete target.statusCredit[m.status];}}
    if(m.drain)restored+=heal(actor,actualDamage*m.drain,actorStats.hp);if(m.drainEnergy){const drained=Math.min(target.energy,m.drainEnergy);target.energy-=drained;actor.energy=Math.min(actorStats.maxEnergy,actor.energy+drained);}
  }else say(s,`${label} uses ${m.name}.`);
  if(side==='player')contribution(s,actor,0,restored);
  if(m.switch&&alive(actor)){
    const options=members.filter(p=>alive(p)&&p.id!==actor.id),incoming=options.find(p=>p.id===targetId)||options[0];if(incoming){replacement(s,incoming.id,side,false);
      if(m.teamShield){incoming.shield=Math.max(incoming.shield||0,Math.round(stats(incoming).hp*m.teamShield));applyStatus(incoming,'shield',duration);}
      if(m.transferSpeed){const turns=actor.status.speed||duration;applyStatus(incoming,'speed',turns,actor.statusPower.speed);delete actor.status.speed;delete actor.statusPower.speed;}
      if(m.entryEnergy&&!(incoming.cooldowns.secondWind>b.round)){incoming.energy=Math.min(stats(incoming).maxEnergy,incoming.energy+m.entryEnergy);incoming.cooldowns.secondWind=b.round+Math.max(4,m.cooldown||0);}
    }
  }
}
function tick(p,max,label,s){
  if(!alive(p))return;for(const[status,turns]of Object.entries(p.status)){
    if(status==='burn'||status==='poison'){const n=Math.min(p.hp,Math.max(1,Math.round(max*.04)));p.hp=Math.max(0,p.hp-n);contribution(s,s.collection[p.statusCredit?.[status]],n);say(s,`${label} takes ${n} ${status} damage.`);}
    if(turns<=1){if(p.statusCredit)delete p.statusCredit[status];delete p.status[status];delete p.statusPower[status];if(status==='shield')p.shield=0;if(status==='counter')p.counter=0;if(status==='prepare')p.prepare=0;}else p.status[status]--;
  }
}
function resolveRound(s){
  const r=s.run,b=r.battle;if(b.round===CONFIG.fatigueRound-3)say(s,`Both teams are tiring. From round ${CONFIG.fatigueRound}, escalating fatigue will end a prolonged battle.`);
  for(const p of[...team(s),...b.enemies]){const st=stats(p,s.collection[p.id]?equipped(s,p):{});tick(p,st.hp,p.nickname||SPECIES[p.species].name,s);p.guard=0;
    if(b.round>=CONFIG.fatigueRound&&alive(p))p.hp=Math.max(0,p.hp-Math.max(1,Math.round(st.hp*Math.min(.6,.04*(b.round-CONFIG.fatigueRound+1)))));
    if(alive(p))p.energy=Math.min(st.maxEnergy,p.energy+1);}
  if(b.pendingRally?.expires<=b.round)b.pendingRally=null;if(b.enemyRally?.expires<=b.round)b.enemyRally=null;
  if(!team(s).some(alive)){endRun(s);return;}if(!b.enemies.some(alive)){
    if(!alive(r.monkey)){b.pendingVictory=true;r.phase='replacement';say(s,'Your team won, and the active companion fell. Choose a conscious teammate before collecting the rewards.');return;}
    victory(s);return;
  }if(!alive(b.enemy))enter(s,b.enemies.find(alive),'enemy');b.round++;b.turn=b.round;
  if(!alive(r.monkey)){r.phase='replacement';say(s,'Choose a conscious replacement. This forced switch costs no turn and gives no extra enemy attack.');return;}planEnemy(s);
}
function itemApply(s,id,targetId){
  id=itemId(id);const r=s.run,p=getMonkey(s,targetId);if(!p||!r.party.includes(p.id)||!ITEMS[id]||!(r.items[id]>0))throw Error('That item is unavailable for this teammate.');const st=stats(p,equipped(s,p));
  if(id==='revive'){if(p.hp>0)throw Error('That teammate is already conscious.');r.items[id]--;p.hp=Math.max(1,Math.round(st.hp*.30));p.status={};p.statusCredit={};p.statusPower={};say(s,'A revival supply restored your teammate.');return;}if(!alive(p))throw Error('Use an actual revival supply for a knocked-out teammate.');
  if(id==='berry'&&p.hp>=st.hp||id==='tonic'&&p.energy>=st.maxEnergy||id==='salve'&&!Object.keys(p.status).length&&p.hp>=st.hp)throw Error('That supply is not needed.');if(!['berry','tonic','salve'].includes(id))throw Error('Use this utility from the pack or camp.');r.items[id]--;
  if(id==='berry')heal(p,st.hp*.35,st.hp);if(id==='tonic')p.energy=Math.min(st.maxEnergy,p.energy+8);if(id==='salve'){p.statusCredit={};p.status={};p.statusPower={};heal(p,st.hp*.15,st.hp);}say(s,`Used ${ITEMS[id].name}.`);
}
function performTurn(s,action){
  const r=s.run,b=r.battle;if(r.phase!=='battle'||!b?.ready)throw Error('Stop safely and begin the encounter first.');const p=r.monkey,e=b.enemy,enemyAction=copy(b.enemyAction||{type:'move',id:b.intent}),em=MOVES[enemyAction.id],ps=stats(p,equipped(s,p)),es=stats(e),pm=MOVES[action.id];
  if(action.type==='move'){
    if(!p.loadout.includes(action.id)||!pm||p.energy<(pm.cost||0)||!moveAvailable(b,p,action.id))throw Error('Ability unavailable, cooling down, or insufficient energy.');if(pm.switch&&p.status.root)throw Error('Rooted Challenge prevents voluntary retreat this round.');if(pm.switch&&!team(s).some(m=>alive(m)&&m.id!==p.id))throw Error('No conscious teammate can receive the relay.');
  }
  if(action.type==='switch'){const incoming=team(s).find(m=>m.id===action.id);if(!incoming||!alive(incoming)||incoming.id===p.id||p.status.root)throw Error('Choose another conscious, unrooted switch.');}
  b.hitFeedback=[];b.lastHits=b.hitFeedback;delete b.lastHit;
  const initiative=(actor,st,m)=>(m?.priority||0)*1000+st.speed*(actor.status.slow?.65:1)*(actor.status.speed?1.2:1),playerFirst=action.type!=='move'||initiative(p,ps,pm)>=initiative(e,es,em);
  const playerAction=()=>{if(!alive(p))return;if(action.type==='switch')replacement(s,action.id,'player');else execute(s,p,b.enemy,action.id,stats(p,equipped(s,p)),stats(b.enemy),'You',action.targetId,'player');};
  const enemyAct=()=>{if(!alive(e))return;const cannotRetreat=e.status.root||!b.enemies.some(m=>m.id!==e.id&&alive(m));if(enemyAction.type==='switch'&&!cannotRetreat)replacement(s,enemyAction.id,'enemy');else{const chosen=enemyAction.type==='switch'||MOVES[enemyAction.id]?.switch&&cannotRetreat||e.energy<(MOVES[enemyAction.id]?.cost||0)?'strike':enemyAction.id;execute(s,e,r.monkey,chosen,stats(e),stats(r.monkey,equipped(s,r.monkey)),'Rival',null,'enemy');}};
  if(playerFirst){playerAction();enemyAct();}else{enemyAct();playerAction();}resolveRound(s);
}
export function turn(s,id,isItem=false,options={}){if(isItem)return useItem(s,id,options.targetId);return performTurn(s,{type:'move',id,targetId:options.targetId});}
export function switchMonkey(s,id){return performTurn(s,{type:'switch',id});}
export function chooseReplacement(s,id){if(s.run.phase!=='replacement')throw Error('No forced replacement is pending.');replacement(s,id,'player',true);s.run.phase='battle';if(s.run.battle.pendingVictory)victory(s);else planEnemy(s);return s.run.monkey;}
export function useItem(s,id,targetId){
  const r=s.run;if(r.phase==='dead')throw Error('Begin a new expedition.');
  if(['replacement','befriend'].includes(r.phase))throw Error('Finish choosing your companion first.');
  if(r.phase==='battle'){
    const b=r.battle;if(!b?.ready)throw Error('Stop safely and begin the encounter first.');
    if(b.itemUsedRound===b.round)throw Error('One supply per round. Choose an ability or attack next.');
    itemApply(s,id,targetId);b.itemUsedRound=b.round;say(s,'Supply used — your attack or ability is still available.');return;
  }itemApply(s,id,targetId);
}
export function setParty(s,ids){
  const r=s.run;if(!(r.phase==='camp'||r.phase==='trail'&&r.distance===0&&r.encounters===0))throw Error('Change teammates before departure or at an earned camp.');
  if(!Array.isArray(ids)||!ids.length||ids.length>3||new Set(ids).size!==ids.length||ids.some(id=>!s.collection[id]||!alive(s.collection[id])))throw Error('Choose one to three different conscious sanctuary companions.');if(r.party.filter(id=>!ids.includes(id)).some(id=>!alive(s.collection[id])))throw Error('Revive a knocked-out teammate before replacing it.');
  for(const p of team(s))r.injuries[p.id]={hp:p.hp,energy:p.energy,status:copy(p.status)};r.party=[...ids];if(!ids.includes(r.activeId))r.activeId=ids[0];syncActive(s);say(s,'Expedition team updated. Every returning companion keeps its injuries.');return team(s);
}
export function campHeal(s,id=s.run.activeId){const r=s.run,p=getMonkey(s,id);if(r.phase!=='camp'||!r.party.includes(id)||!r.camp.heals||!alive(p))throw Error('Camp recovery needs a conscious teammate and a remaining recovery action.');r.camp.heals--;heal(p,stats(p,equipped(s,p)).hp*(r.camp.seedBoost?.4:.3),stats(p,equipped(s,p)).hp);p.energy=Math.min(stats(p).maxEnergy,p.energy+3);return p;}
export function leaveCamp(s){if(s.run.phase!=='camp')return;s.run.camp=null;s.run.phase='trail';pump(s);}
export function trainingCost(p){return{fruit:p.level<=3?[0,3,5,8][p.level]:8+p.level*4,xp:12+p.level*10};}
export function train(s,id){
  const r=s.run,p=getMonkey(s,id);if(!p||!alive(p)||!['trail','camp'].includes(r.phase)||!r.party.includes(p.id))throw Error('Train a conscious expedition companion between encounters.');if(p.level>=CONFIG.maxLevel)throw Error('Maximum level reached.');const cost=trainingCost(p);if(r.fruit<cost.fruit||p.training<cost.xp)throw Error(`Leveling needs ${cost.xp} XP and ${cost.fruit} fruit.`);
  const before=stats(p,equipped(s,p)).hp;r.fruit-=cost.fruit;p.training-=cost.xp;p.level++;p.hp=Math.min(stats(p,equipped(s,p)).hp,p.hp+stats(p,equipped(s,p)).hp-before);say(s,`${p.nickname||SPECIES[p.species].name} reached level ${p.level}.`);return p;
}
export function evolutionCost(s,type,id){const p=getMonkey(s,id),next=(p?.stage||0)+1,required=[0,3,6,10][next];if(!p||!ELEMENTS[type]||next>3)return null;
  const native=nativeTypes(p.species);if(next===1&&native.length&&!native.includes(type))return null;
  const types=next===1?(native.length?native:[type]):[...new Set([...p.types,type])];if(types.length>2||next===3&&!p.types.includes(type))return null;
  return{level:required,cost:next===1?Object.fromEntries(types.map(t=>[t,CONFIG.evolution.firstEssence])):next===2?{[type]:24}:Object.fromEntries(types.map(t=>[t,types.length===1?40:28])),types};}
export function evolutionPreview(s,type,id){const p=getMonkey(s,id),offer=evolutionCost(s,type,id);if(!offer)return null;const preview={...p,types:offer.types,stage:p.stage+1},mastery=preview.stage===3?CONFIG.masteries[type]:null;if(mastery)preview.mastery=type;
  const beforeStats=stats(p,equipped(s,p)),afterStats=stats(preview,equipped(s,p)),statGains=Object.fromEntries(Object.keys(afterStats).map(key=>[key,afterStats[key]-beforeStats[key]]));
  return{...offer,monkey:preview,name:formName(preview),stats:afterStats,beforeStats,statGains,abilities:unlocked(preview),specialization:mastery?.name,desc:mastery?`Ascendant mastery: ${mastery.desc} Choose which existing element to master for this expedition.`:undefined,passive:mastery?`${mastery.name}: ${mastery.desc}`:offer.types.length===1?'Pure harmony: greater specialization in attack or defense.':'Mixed harmony: broader coverage with combined weaknesses and resistances.',matchups:Object.fromEntries(Object.keys(ELEMENTS).map(t=>[t,effectiveness(t,preview.types)]))};}
export function evolve(s,type,id){
  const r=s.run,p=getMonkey(s,id),offer=evolutionCost(s,type,id);if(!p||!alive(p)||!['trail','camp'].includes(r.phase)||!offer||!r.party.includes(p.id))throw Error('That evolution is unavailable for a fallen companion.');if(p.level<offer.level)throw Error(`Reach level ${offer.level} first.`);if(Object.entries(offer.cost).some(([t,n])=>r.essence[t]<n))throw Error('Gather the required essence or use the slower resource exchange.');
  const old=stats(p,equipped(s,p)).hp;for(const[t,n]of Object.entries(offer.cost))r.essence[t]-=n;p.types=[...offer.types];p.stage++;if(p.stage===3)p.mastery=type;p.hp=Math.min(stats(p,equipped(s,p)).hp,p.hp+stats(p,equipped(s,p)).hp-old);const key=`${p.stage}:${[...p.types].sort().join('+')}${p.stage===3?`:mastery=${p.mastery}`:''}`;if(!p.discovered.includes(key))p.discovered.push(key);if(p.stage>=p.highestForm.stage)p.highestForm={stage:p.stage,types:[...p.types],...(p.stage===3?{mastery:p.mastery}:{})};p.learned=[...new Set([...p.learned,...unlocked(p)])];say(s,`Evolved into ${formName(p)}${p.stage===3?` with ${CONFIG.masteries[p.mastery].name}`:''}. This branch stays discovered in the sanctuary.`);return p;
}
export function equip(s,id,monkeyId){
  const p=getMonkey(s,monkeyId);if(!p||!['trail','camp'].includes(s.run.phase)||!s.meta.gear[id]||!GEAR[id])throw Error('Equip owned relics between battles.');const slot=GEAR[id].slot;for(const other of sanctuary(s))if(other.equipment[slot]===id)other.equipment[slot]=null;p.equipment[slot]=id;
  for(const other of sanctuary(s))other.hp=Math.min(other.hp,stats(other,equipped(s,other)).hp);if(p.id===s.run.activeId)s.meta.equipped={...p.equipment};updateNextCache(s);return p;
}
export function setMove(s,slot,id,monkeyId){const p=getMonkey(s,monkeyId);if(!p||!['trail','camp'].includes(s.run.phase)||!Number.isInteger(slot)||slot<0||slot>3||(slot===0&&id!=='strike')||!abilityInfo(p,id)?.eligible)throw Error('Change curated abilities between encounters; keep Palm Strike in slot one.');if(p.loadout.includes(id)&&p.loadout[slot]!==id)throw Error('That ability is already equipped.');p.loadout[slot]=id;return p;}
export function exchangeEssence(s,from,to){if(!['trail','camp'].includes(s.run.phase)||!ELEMENTS[from]||!ELEMENTS[to]||from===to||s.run.essence[from]<4)throw Error('Exchange four of one essence for one other essence between encounters.');s.run.essence[from]-=4;s.run.essence[to]++;}
export function craft(s,id){
  id=itemId(id);const r=s.run;if(r.phase!=='camp')throw Error('Craft supplies at an earned trail camp.');if(GEAR[id]){if((s.meta.fragments[id]||0)<CONFIG.fragmentCost)throw Error(`Collect ${CONFIG.fragmentCost} equipment fragments.`);s.meta.fragments[id]-=CONFIG.fragmentCost;s.meta.gear[id]=Math.min(12,(s.meta.gear[id]||0)+1);return;}
  const recipes={bondCharm:{fruit:2,material:1},revive:{fruit:8,material:4},berry:{fruit:3,material:0}},recipe=recipes[id];if(!recipe||r.fruit<recipe.fruit||(r.items.charmMaterial||0)<recipe.material)throw Error('Gather more fruit and charm material for this recipe.');r.fruit-=recipe.fruit;r.items.charmMaterial-=recipe.material;r.items[id]++;
}
export function migrate(s){
  if(!s||s.version!==1)return s;const r=s.run,p=r?.monkey;if(!r||!p||!SPECIES[p.species])throw Error('Invalid legacy checkpoint.');s.version=VERSION;s.difficulty=s.difficulty||'standard';s.meta.serial=s.meta.serial||0;s.meta.fragments={};s.meta.claims={};s.meta.captureFailures=0;s.meta.bondMaterials={};s.collection={};
  const fresh=monkey(s,p.species,p.level,p.types,p.stage);Object.assign(fresh,p);fresh.id=uid(s,'legacy');fresh.nickname=p.nickname||'';fresh.equipment={...s.meta.equipped};fresh.learned=[...new Set([...fresh.learned,...p.loadout,...unlocked(fresh)])];if(p.stage===3)fresh.mastery=p.mastery||p.types[0];fresh.discovered=p.stage?[`${p.stage}:${[...p.types].sort().join('+')}${p.stage===3?`:mastery=${fresh.mastery}`:''}`]:[];fresh.highestForm={stage:p.stage,types:[...p.types],...(p.stage===3?{mastery:fresh.mastery}:{})};fresh.statusPower={};fresh.cooldowns={};fresh.uses={};fresh.bond=0;s.collection[fresh.id]=fresh;
  r.monkey=fresh;r.party=[fresh.id];r.activeId=fresh.id;r.ordinaryWins=r.wins||0;r.discoveryProgress=clamp(1-(r.nextCache-r.distance)/RESOURCE_STEP,0,.999999);r.discoveries=Math.floor(r.distance/RESOURCE_STEP);r.nextCamp=(Math.floor(r.distance/(CONFIG.campFeet*FEET))+1)*CONFIG.campFeet*FEET;r.bonuses={ribbon:0,scent:0,feather:0,campSeed:0};r.injuries={};r.events=[];r.capture=null;r.camp=null;r.ultra=null;r.nextUltra=Math.max(CONFIG.ultraFirstFeet*FEET,r.distance+2000*FEET);
  for(const t of Object.keys(ELEMENTS))r.essence[t]??=0;for(const id of Object.keys(ITEMS))r.items[id]??=0;r.items.charmMaterial=0;
  if(r.battle){const b=r.battle;b.id=uid(s,'legacy-battle');b.enemies=[b.enemy];b.enemyActive=0;b.round=b.turn||1;b.ultra=false;b.rewarded=false;b.bondBonuses={};b.pendingRally=null;b.enemyRally=null;b.enemyAction={type:'move',id:b.intent||'strike'};for(const e of b.enemies){e.id=uid(s,'enemy');e.statusPower={};e.cooldowns={};e.uses={};e.equipment={};e.learned=e.loadout||['strike'];}}
  updateNextCache(s);say(s,'Your original companion, expedition, equipment, and progression were restored into sanctuary slot one.');return s;
}
export function validate(s){
  migrate(s);if(!s||s.version!==VERSION||!['gps','practice'].includes(s.mode)||typeof s.name!=='string'||s.name.length>30)throw Error('Unsupported save.');const r=s.run;
  if(!r||!['trail','battle','replacement','befriend','camp','dead'].includes(r.phase)||!s.collection||!Array.isArray(r.party)||r.party.length<1||r.party.length>3||new Set(r.party).size!==r.party.length||r.party.some(id=>!s.collection[id])||!r.party.includes(r.activeId))throw Error('Invalid expedition team.');syncActive(s);const finite=n=>Number.isFinite(n)&&n>=0;
  for(const n of[r.distance,r.nextCache,r.nextEncounter,r.nextCamp,r.fruit,r.discoveryProgress,s.rng,s.meta?.legacy])if(!finite(n))throw Error('Invalid save numbers.');if(r.discoveryProgress>=1)throw Error('Invalid discovery progress.');
  for(const[id,p]of Object.entries(s.collection)){
    p.xpRemainder??=0;if(!finite(p.xpRemainder)||p.xpRemainder>=1)throw Error('Invalid XP remainder.');
    if(id!==p.id||!SPECIES[p.species]||![p.hp,p.energy,p.level,p.training,p.bond].every(finite)||!Number.isInteger(p.level)||p.level<1||p.level>60||!Number.isInteger(p.stage)||p.stage<0||p.stage>3||!Array.isArray(p.types)||p.types.length>2||p.types.some(t=>!ELEMENTS[t])||new Set(p.types).size!==p.types.length)throw Error('Invalid sanctuary companion.');
    // Earlier v2 checkpoints predate explicit mastery choices; retain the primary path.
    if(p.stage===3&&!p.mastery){p.mastery=p.types[0];if(p.highestForm?.stage===3&&!p.highestForm.mastery)p.highestForm.mastery=p.mastery;const key=`3:${[...p.types].sort().join('+')}:mastery=${p.mastery}`;if(!p.discovered.includes(key))p.discovered.push(key);}
    if(p.mastery&&(!CONFIG.masteries[p.mastery]||!p.types.includes(p.mastery)))throw Error('Invalid mastery choice.');
    if(!Array.isArray(p.loadout)||p.loadout[0]!=='strike'||p.loadout.length>4||new Set(p.loadout).size!==p.loadout.length||p.loadout.some(id=>!MOVES[id]||!unlocked(p).includes(id)))throw Error('Invalid ability loadout.');if(!p.status||Object.values(p.status).some(n=>!finite(n)||n>10)||!p.equipment||Object.values(p.equipment).some(id=>id&&!s.meta.gear[id]))throw Error('Invalid companion equipment or effects.');
  }
  if(!r.essence||Object.keys(ELEMENTS).some(t=>!finite(r.essence[t]))||!r.items||Object.keys(ITEMS).some(t=>!finite(r.items[t])))throw Error('Invalid inventory.');if(!r.vector||!Object.values(r.vector).every(Number.isFinite)||!r.weights||!Object.values(r.weights).every(finite)||!r.bonuses||!Object.values(r.bonuses).every(finite))throw Error('Invalid walking checkpoint.');
  if(['battle','replacement'].includes(r.phase)){const b=r.battle;if(!b||!Array.isArray(b.enemies)||b.enemies.length<1||b.enemies.length>3||!b.enemies.every(p=>SPECIES[p.species]&&finite(p.hp)&&finite(p.energy))||!b.enemies.some(e=>e.id===b.enemy?.id)||!MOVES[b.intent]||!finite(b.round))throw Error('Invalid battle checkpoint.');b.enemy=b.enemies.find(e=>e.id===b.enemy.id);}
  if(r.phase==='befriend'&&(!r.capture?.id||!r.capture.candidates?.length||!finite(r.capture.attempts)||r.capture.attempts>CONFIG.captureAttempts))throw Error('Invalid recruitment checkpoint.');if(r.ultra&&(!SPECIES[r.ultra.species]||!r.ultra.id||!finite(r.ultra.challengeRemaining)||!finite(r.ultra.spawnDistance)))throw Error('Invalid Ultra sighting.');
  if(!s.meta.gear||!s.meta.equipped||!Array.isArray(s.meta.history)||s.meta.history.length>25||Object.entries(s.meta.gear).some(([id,n])=>!GEAR[id]||!Number.isInteger(n)||n<1||n>12)||!s.meta.claims)throw Error('Invalid persistent inventory.');normalizeLands(s);if(!Array.isArray(s.log)||s.log.length>16||JSON.stringify(s).length>650000)throw Error('Save is too large.');return s;
}
