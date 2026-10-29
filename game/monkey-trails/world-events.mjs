// A versioned recurring game calendar works identically online and offline.
// No request can leave one screen using a newer schedule than the reward engine.
import {SPECIES} from './data.mjs';
export const WORLD_CALENDAR_VERSION=1;
export const WORLD_DAY=86400000,WORLD_EVENT_EPOCH=Date.UTC(2026,9,31),WORLD_EVENT_INTERVAL=3*WORLD_DAY;
export const EVENT_POLICY='Bonuses apply when a trail discovery or encounter is created. A waiting encounter keeps its event and rewards after the event ends. Camp trades require the event to be active when you trade. Every event returns; essential progress is never exclusive.';
export const EVENT_TYPES=Object.freeze([
  Object.freeze({id:'fruit-bloom',name:'Fruit Bloom',art:'fruit-bloom',description:'Flowering branches fill the trail with ripe fruit.',effect:'Each fruit discovery gives one extra fruit, including foraging finds.',condition:'Walk normally; no entry cost or special route is required.',location:'All regions and routes'}),
  Object.freeze({id:'monkey-migration',name:'Monkey Migration',art:'monkey-migration',description:'Leaf companions travel together through the canopy.',effect:'Eligible Leaf monkeys have 50% more encounter weight.',condition:'Existing rarity and walking unlocks still apply; no companion is exclusive.',location:'Ordinary encounters in all regions'}),
  Object.freeze({id:'essence-winds',name:'Essence Winds',art:'essence-winds',description:'Leaf-colored currents carry a little extra magic.',effect:'Each essence discovery also gives one Leaf essence.',condition:'Applies to trail discoveries and foraging; all other elements remain available.',location:'All regions and routes'}),
  Object.freeze({id:'guardian-stirring',name:'Guardian Stirring',art:'guardian-stirring',description:'The Bloomwatch Guardian offers a friendly test of resolve.',effect:'An optional guardian battle awards three extra Leaf essence once per event.',condition:'Win two introductory battles and walk 2,000 feet this expedition first. The guardian returns every cycle.',location:'Challenge from the forecast while on the trail'}),
  Object.freeze({id:'traveling-merchant',name:'Traveling Merchant',art:'traveling-merchant',description:'A lantern-lit cart brings useful supplies to camp.',effect:'A visitor at earned camps offers a small selection of optional trades.',condition:'Trade while the event is active. Normal crafting and camp trading stay available.',location:'Earned trail camps in every region'})
]);
const mod=(n,m)=>(n%m+m)%m;
function occurrence(index,now){
  const type=EVENT_TYPES[mod(index,EVENT_TYPES.length)],startsAt=WORLD_EVENT_EPOCH+index*WORLD_EVENT_INTERVAL,endsAt=startsAt+WORLD_DAY;
  return{...type,id:`world-v${WORLD_CALENDAR_VERSION}:${index}`,type:type.id,startsAt,endsAt,active:startsAt<=now&&now<endsAt,element:'leaf',calendarVersion:WORLD_CALENDAR_VERSION};
}
export function currentWorldEvent(now=Date.now()){
  if(!Number.isFinite(now))return null;const event=occurrence(Math.floor((now-WORLD_EVENT_EPOCH)/WORLD_EVENT_INTERVAL),now);return event.active?event:null;
}
export function worldForecast(now=Date.now(),days=21){
  if(!Number.isFinite(now))return{version:WORLD_CALENDAR_VERSION,source:'Clock unavailable · event timing cannot be confirmed',policy:EVENT_POLICY,events:[],current:null,confirmed:false};
  const horizon=now+Math.max(14,Math.min(42,Number.isFinite(days)?days:21))*WORLD_DAY,first=Math.floor((now-WORLD_EVENT_EPOCH)/WORLD_EVENT_INTERVAL),events=[];
  for(let i=first;i<=first+15;i++){const event=occurrence(i,now);if(event.startsAt>horizon)break;if(event.endsAt>now)events.push(event);}
  return{version:WORLD_CALENDAR_VERSION,source:'Built-in recurring calendar · available offline',policy:EVENT_POLICY,events,current:currentWorldEvent(now),confirmed:true,generatedAt:now};
}
export function eventSnapshot(now=Date.now()){
  const event=currentWorldEvent(now);return event?{id:event.id,type:event.type,name:event.name,art:event.art,startsAt:event.startsAt,endsAt:event.endsAt,element:event.element,calendarVersion:event.calendarVersion}:null;
}
export function worldEncounterWeight(species,event=eventSnapshot()){return event?.type==='monkey-migration'&&SPECIES[species]?.types.includes(event.element)?1.5:1;}
export function worldDiscovery(pickup,event=eventSnapshot(),occurrences=1){
  const bonuses=[];if(!event)return bonuses;
  // Guaranteed lineup foraging batches multiple earned intervals into one pickup.
  // Pay per interval, so a long GPS sample earns exactly the same bonus as small ones.
  const count=Number.isSafeInteger(occurrences)&&occurrences>0?occurrences:1;
  if(event.type==='fruit-bloom'&&pickup.category==='fruit'){pickup.amount+=count;pickup.eventBonus=count;}
  if(event.type==='essence-winds'&&pickup.category==='essence')bonuses.push({category:'essence',id:event.element,amount:count,source:event.name});
  pickup.worldEvent=event;return bonuses;
}
export function worldGuardianView(s,now=Date.now()){
  const event=currentWorldEvent(now),id=event?.type==='guardian-stirring'?`world-guardian:${event.id}`:null;
  const claimed=!!id&&!!s.meta?.claims?.[`reward:${id}`],eligible=(s.run?.ordinaryWins||0)>=2&&(s.run?.distance||0)>=2000*.3048;
  return{event:event?.type==='guardian-stirring'?event:null,id,claimed,eligible,canChallenge:!!id&&!claimed&&eligible&&s.run?.phase==='trail',reason:claimed?'Guardian reward already earned for this event.':!eligible?'Win two introductory battles and walk 2,000 feet this expedition.':s.run?.phase!=='trail'?'Return to the trail to challenge the guardian.':''};
}
