// Optional journal, camp and sanctuary additions. Existing progress stays the source of truth.
import {SPECIES} from './data.mjs';
import {LANDS} from './lands.mjs';
import {REGION_ADVENTURES} from './regional-adventures.mjs';
import {habitatAtPosition} from './habitats.mjs';
import {currentWorldEvent} from './world-events.mjs';

const record=value=>value&&typeof value==='object'&&!Array.isArray(value);
const count=value=>Number.isFinite(value)?Math.max(0,Math.min(1e9,Math.floor(value))):0;
const strings=value=>[...new Set(Array.isArray(value)?value.filter(x=>typeof x==='string'):[])];
const companions=s=>Object.values(s.collection||{});
const party=s=>(s.run?.party||[]).map(id=>s.collection?.[id]).filter(Boolean);
const slots=['left','center','right'];
const log=(s,message)=>{s.log??=[];s.log.push(message);s.log=s.log.slice(-16);};

export const JOURNAL_GOALS=Object.freeze([
 {id:'first-paths',name:'A thousand little steps',description:'Walk 1,000 meters across your expeditions. Previous exploration counts.',target:1000,unit:'m',artKey:'journal-explorer',reward:{decoration:'trail-lantern'},rewardText:'Trail Lantern decoration'},
 {id:'growing-family',name:'Room for new friends',description:'Keep three different monkey species together in your sanctuary.',target:3,unit:'species',artKey:'journal-friends',reward:{decoration:'friendship-banner'},rewardText:'Friendship Banner decoration'},
 {id:'guardian-story',name:'A guardian’s respect',description:'Defeat a regional guardian. Previous guardian victories count.',target:1,unit:'guardian',artKey:'journal-guardian',reward:{decoration:'bloom-planter'},rewardText:'Bloom Planter decoration'}
].map(Object.freeze));
export const SANCTUARY_AREAS=Object.freeze([
 {id:'clearing',name:'Canopy Clearing',description:'A warm patch of sunlight where every companion is welcome.',artKey:'sanctuary-clearing'},
 {id:'river',name:'River Rest',description:'A peaceful riverbank, unlocked after 1,000 meters of exploration.',artKey:'sanctuary-river'}
].map(Object.freeze));
export const SANCTUARY_DECORATIONS=Object.freeze([
 {id:'welcome-stone',name:'Welcome Stone',description:'A little landmark for a growing home.',artKey:'decoration-stone'},
 {id:'trail-lantern',name:'Trail Lantern',description:'A warm light earned by exploring 1,000 meters.',artKey:'decoration-lantern'},
 {id:'friendship-banner',name:'Friendship Banner',description:'A bright banner celebrating three different friends.',artKey:'decoration-banner'},
 {id:'bloom-planter',name:'Bloom Planter',description:'Flowers celebrating a guardian’s respect.',artKey:'decoration-bloom'}
].map(Object.freeze));
const goalById=new Map(JOURNAL_GOALS.map(row=>[row.id,row]));

function knownSpecies(s){
 return strings([...companions(s).map(p=>p.species),...Object.values(s.meta?.adventures?.regions||{}).flatMap(r=>r.seen||[]),...(s.run?.capture?.candidates||[]).map(p=>p.species)]).filter(id=>SPECIES[id]);
}
function walking(s){return Math.max(count(s.world?.totalDistance),count(s.run?.distance),...companions(s).map(p=>count(p.memories?.distance)));}
function guardianRegions(s){
 const regions=new Set(companions(s).map(p=>p.memories?.firstGuardian).filter(Boolean));
 for(const adventure of REGION_ADVENTURES){const step=adventure.steps.find(row=>row.kind==='guardian'),saved=s.meta?.adventures?.regions?.[adventure.id];if(saved?.claimed?.includes(step?.id)||(saved?.stepId===step?.id&&saved.current>=1))regions.add(adventure.id);}
 for(const [key,value] of Object.entries(s.meta?.trailWisdom?.claims||{}))if(value&&key.startsWith('guardian:')&&LANDS.some(l=>l.id===key.slice(9)))regions.add(key.slice(9));
 return regions;
}
export function normalizeAdventureLife(s){
 s.meta??={};const old=record(s.meta.adventureLife)?s.meta.adventureLife:{};
 const claimed=strings(old.claimed).filter(id=>goalById.has(id));
 const owned=strings(['welcome-stone',...claimed.map(id=>goalById.get(id).reward.decoration)]);
 const placement=record(old.placements)?old.placements:{};
 // Existing tribal decorations and regional keepsakes remain owned in their original ledgers.
 const existing=[...Object.keys(s.meta.cosmetics||{}).filter(id=>s.meta.cosmetics[id]),...strings(s.meta.adventures?.cosmetics)];
 const allowed=new Set([...owned,...existing]),used=new Set(),placements={};
 for(const slot of slots){const id=placement[slot];if(allowed.has(id)&&!used.has(id)){placements[slot]=id;used.add(id);}else placements[slot]=null;}
 s.meta.adventureLife={version:1,claimed,tracked:goalById.has(old.tracked)?old.tracked:null,seen:strings([...strings(old.seen),...knownSpecies(s)]).filter(id=>SPECIES[id]),area:old.area==='river'&&walking(s)>=1000?'river':'clearing',placements};
 if(s.run){const r=record(s.run.adventureLife)?s.run.adventureLife:{};s.run.adventureLife={mealRounds:Math.min(3,count(r.mealRounds)),studyHint:typeof r.studyHint==='string'?r.studyHint.slice(0,600):null,merchantReceipts:strings(r.merchantReceipts).filter(id=>id.length<160).slice(-100)};}
 return s.meta.adventureLife;
}
export function recordJournalEncounter(s,battle){
 const saved=normalizeAdventureLife(s);for(const p of battle?.enemies||[])if(SPECIES[p.species]&&!saved.seen.includes(p.species))saved.seen.push(p.species);return saved.seen;
}
export function journalView(s){
 const saved=normalizeAdventureLife(s),owned=new Set(companions(s).map(p=>p.species)),guardians=guardianRegions(s),progress={'first-paths':walking(s),'growing-family':owned.size,'guardian-story':Math.max(guardians.size,...companions(s).map(p=>count(p.memories?.guardianVictories)))};
 const goals=JOURNAL_GOALS.map(row=>({...row,current:Math.min(row.target,progress[row.id]),complete:progress[row.id]>=row.target,claimed:saved.claimed.includes(row.id),tracked:saved.tracked===row.id}));
 const discoveries=Math.max(count(s.run?.discoveries),Object.values(s.meta?.regionChallenges?.regions||{}).reduce((n,r)=>n+count(r.discoveries),0));
 return{goals,trackedGoal:goals.find(row=>row.tracked)||null,regions:LANDS.map(land=>({id:land.id,name:land.name,description:land.description,discovered:Object.hasOwn(s.world?.regions||{},land.id),distance:count(s.world?.regions?.[land.id]?.distance),artKey:'journal-explorer',guardianDefeated:guardians.has(land.id)})),companions:{owned:owned.size,individuals:companions(s).length,encountered:saved.seen.length},guardians:progress['guardian-story'],expeditions:count(s.meta.runs),discoveries,distance:walking(s)};
}
export function trackJournalGoal(s,id){const saved=normalizeAdventureLife(s);if(id!==null&&!goalById.has(id))throw Error('Choose a journal goal.');saved.tracked=id;return journalView(s).trackedGoal;}
export function claimJournalGoal(s,id){
 const row=journalView(s).goals.find(goal=>goal.id===id);if(!row||!row.complete)throw Error('Complete this optional goal first.');if(row.claimed)throw Error('This journal keepsake is already yours.');
 s.meta.adventureLife.claimed.push(id);if(s.meta.adventureLife.tracked===id)s.meta.adventureLife.tracked=null;log(s,`${row.name}: ${row.rewardText} is ready to display in your sanctuary.`);return{...row.reward};
}
function decorationCatalogue(s){
 const rows=SANCTUARY_DECORATIONS.map(row=>({...row})),known=new Set(rows.map(row=>row.id));
 const title=id=>id.replaceAll('-',' ').replace(/\b\w/g,c=>c.toUpperCase());
 for(const id of Object.keys(s.meta?.cosmetics||{}).filter(id=>s.meta.cosmetics[id]))if(!known.has(id)){rows.push({id,name:title(id),description:'A decoration earned with your tribe.',artKey:id.includes('moonwell')?'decoration-bloom':id.includes('sunstone')?'decoration-lantern':'decoration-banner'});known.add(id);}
 for(const id of strings(s.meta?.adventures?.cosmetics))if(!known.has(id)){const adventure=REGION_ADVENTURES.find(row=>`${row.id}:keepsake`===id);rows.push({id,name:adventure?.keepsake||title(id.replace(':keepsake','')),description:'A keepsake brought home from a regional adventure.',artKey:'journal-explorer'});known.add(id);}
 return rows;
}
export function sanctuaryView(s){
 const saved=normalizeAdventureLife(s),owned=new Set(['welcome-stone',...saved.claimed.map(id=>goalById.get(id).reward.decoration),...Object.keys(s.meta.cosmetics||{}).filter(id=>s.meta.cosmetics[id]),...strings(s.meta.adventures?.cosmetics)]);
 const decorations=decorationCatalogue(s).map(row=>({...row,owned:owned.has(row.id),placed:Object.values(saved.placements).includes(row.id),slot:slots.find(slot=>saved.placements[slot]===row.id)||null}));
 const areas=SANCTUARY_AREAS.map(row=>({...row,unlocked:row.id==='clearing'||walking(s)>=1000,selected:saved.area===row.id}));
 const favorites=(s.meta.companions?.favorites||[]).filter(id=>s.collection?.[id]);const displayIds=strings([...favorites,...s.run?.party||[],...Object.keys(s.collection||{})]).slice(0,6);
 return{areas,activeArea:areas.find(row=>row.selected),decorations,slots:slots.map(id=>({id,decoration:decorations.find(row=>row.id===saved.placements[id])||null})),monkeys:displayIds.map(id=>s.collection[id]).filter(Boolean)};
}
export function setSanctuaryArea(s,id){const row=sanctuaryView(s).areas.find(area=>area.id===id);if(!row?.unlocked)throw Error('Explore 1,000 meters to unlock River Rest.');s.meta.adventureLife.area=id;return row;}
export function placeDecoration(s,id,slot){
 const view=sanctuaryView(s),decoration=view.decorations.find(row=>row.id===id);if(!slots.includes(slot)||!decoration?.owned)throw Error('Choose an owned decoration and a display spot.');
 for(const spot of slots)if(s.meta.adventureLife.placements[spot]===id)s.meta.adventureLife.placements[spot]=null;s.meta.adventureLife.placements[slot]=id;return sanctuaryView(s);
}
export function clearDecoration(s,slot){normalizeAdventureLife(s);if(!slots.includes(slot))throw Error('Choose a display spot.');s.meta.adventureLife.placements[slot]=null;return sanctuaryView(s);}

function camp(s){if(s.run?.phase!=='camp'||!s.run.camp?.id)throw Error('Visit an earned camp first.');normalizeAdventureLife(s);return s.run.camp;}
function merchantEvent(options={}){
 const now=options.now??Date.now(),actual=currentWorldEvent(now),requested=options.activeEvent;
 return actual?.type==='traveling-merchant'&&(!requested||requested.id===actual.id)?actual:null;
}
export function campMoments(s,options={}){
 normalizeAdventureLife(s);const atCamp=s.run?.phase==='camp'&&!!s.run.camp?.id,used=s.run?.camp?.adventureMoment||null,event=merchantEvent(options);
 return[
  {id:'meal',name:'Share a meal',description:'Spend 2 fruit. Conscious teammates restore 1 extra energy each round for the next 3 battle rounds.',artKey:'camp-meal',available:atCamp&&!used&&s.run.fruit>=2&&s.run.adventureLife.mealRounds===0,reason:!atCamp?'Visit a camp first.':used?'You already shared a moment at this camp.':s.run.adventureLife.mealRounds?'Your previous meal is still keeping the team warm.':s.run.fruit<2?'Needs 2 fruit.':'',used:used==='meal'},
  {id:'study',name:'Study the trail',description:'Share a quiet moment: reveal the waiting encounter, or survey the habitat 250 meters ahead on the fantasy map.',artKey:'camp-study',available:atCamp&&!used,reason:!atCamp?'Visit a camp first.':used?'You already shared a moment at this camp.':'',used:used==='study'},
  {id:'pack',name:'Prepare supplies',description:'Open your Pack to craft, trade, and organize supplies. Always available.',artKey:'camp-pack',available:atCamp,reason:atCamp?'':'Visit a camp first.',used:false},
  {id:'merchant',name:'Meet the merchant',description:event?'A traveling visitor has two small supply trades. Each can be taken once this event, per expedition.':'The merchant returns periodically; check the World Forecast.',artKey:'camp-merchant',available:atCamp&&!!event,reason:!atCamp?'Visit a camp first.':!event?'No merchant is visiting right now.':'',used:false}
 ];
}
export function chooseCampMoment(s,id,options={}){
 const current=camp(s),row=campMoments(s,options).find(moment=>moment.id===id);if(!row?.available)throw Error(row?.reason||'Choose an available camp moment.');
 if(id==='pack')return{action:'pack',message:'Your Pack is ready.'};
 if(id==='merchant')return{action:'merchant',message:'The visitor unfolds a small selection of supplies.',offers:merchantOffers(s,options)};
 let message;
 if(id==='meal'){s.run.fruit-=2;s.run.adventureLife.mealRounds=3;message='A warm meal: +1 energy for every conscious teammate after each of the next 3 battle rounds.';}
 else{const encounter=s.run.pendingEncounter;if(encounter?.enemies?.length)message=`Waiting encounter: ${encounter.enemies.map(p=>`${SPECIES[p.species]?.name||'Monkey'} · level ${p.level}`).join(', ')}. You may approach or pass when ready.`;
  else{const landId=s.world?.activeLand||LANDS[0].id,region=s.world?.regions?.[landId],angle=(region?.heading||0)*Math.PI/180,position=region?.position||{east:0,north:0},habitat=habitatAtPosition(landId,position.east+Math.sin(angle)*250,position.north+Math.cos(angle)*250);message=`Trail notes: about 250 meters ahead along your current fantasy-map heading lies ${habitat.name}. ${habitat.resources}. Turning changes your path; always choose a safe real-world route.`;}
  s.run.adventureLife.studyHint=message;}
 current.adventureMoment=id;current.adventureMessage=message;log(s,message);return{action:id,message};
}
export function applyCampMealRound(s,statsFor){
 if(!s.run?.adventureLife?.mealRounds||s.run.battle?.online||s.run.battle?.social)return 0;
 const saved=s.run.adventureLife;saved.mealRounds=Math.max(0,Math.min(3,count(saved.mealRounds))-1);let restored=0;
 for(const p of party(s))if(p.hp>0){const max=statsFor(p).maxEnergy,amount=Math.max(0,Math.min(1,max-p.energy));p.energy+=amount;restored+=amount;}
 if(restored)log(s,`Camp warmth restores ${restored} team energy · ${saved.mealRounds} rounds remain.`);return restored;
}
export const MERCHANT_OFFERS=Object.freeze([
 {id:'berry-basket',name:'Berry basket',description:'Two healing berries for 4 fruit.',fruit:4,items:{berry:2},artKey:'camp-merchant'},
 {id:'crafting-bundle',name:'Crafting bundle',description:'Three charm materials for 5 fruit.',fruit:5,items:{charmMaterial:3},artKey:'camp-merchant'}
].map(Object.freeze));
export function merchantOffers(s,options={}){
 normalizeAdventureLife(s);const event=merchantEvent(options),atCamp=s.run?.phase==='camp'&&!!s.run.camp?.id;
 return MERCHANT_OFFERS.map(offer=>{const receipt=event?`${event.id}:${offer.id}`:null,claimed=!!receipt&&s.run.adventureLife.merchantReceipts.includes(receipt),available=atCamp&&!!event&&!claimed&&s.run.fruit>=offer.fruit;return{...offer,eventId:event?.id||null,claimed,available,reason:!atCamp?'Visit a camp first.':!event?'The merchant has departed.':claimed?'Already traded for this event.':s.run.fruit<offer.fruit?`Needs ${offer.fruit} fruit.`:''};});
}
export function merchantTrade(s,id,options={}){
 camp(s);const row=merchantOffers(s,options).find(offer=>offer.id===id);if(!row?.available)throw Error(row?.reason||'Choose an available trade.');
 if(Object.entries(row.items).some(([key,n])=>!Number.isSafeInteger(s.run.items?.[key])||s.run.items[key]<0||s.run.items[key]+n>1e9))throw Error('Your supply inventory cannot hold this trade.');
 s.run.fruit-=row.fruit;for(const[key,n]of Object.entries(row.items))s.run.items[key]+=n;s.run.adventureLife.merchantReceipts.push(`${row.eventId}:${row.id}`);log(s,`Traveling Merchant: ${row.name} traded for ${row.fruit} fruit.`);return{fruit:row.fruit,items:{...row.items}};
}
