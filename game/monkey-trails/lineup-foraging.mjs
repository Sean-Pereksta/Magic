// Guaranteed walking rewards are separate from the active explorer's random finds.
// Each conscious lineup member earns their own progress on accepted trail distance.
import {SPECIES,ELEMENTS} from './data.mjs';
const FOOT=.3048, INTERVAL_FEET=250;
const fruit=(amount)=>({category:'fruit',amount});
const essence=(id,amount=1)=>({category:'essence',id,amount});
const bonus=(name,...rewards)=>({name,intervalFeet:INTERVAL_FEET,rewards});
const BONUS={
  mossmop:bonus('Orchard Gathering',fruit(1)),
  bloomtail:bonus('Blossom Harvest',fruit(1)),
  acornbuckler:bonus('Acorn Cache',fruit(1)),
  fernflick:bonus('Fern Forage',fruit(1)),
  lilypadleap:bonus('Pondside Picking',fruit(1)),
  vineknuckle:bonus('Vine Harvest',fruit(1)),
  wildvine:bonus('Wild Orchard',fruit(2)),
  rootstone:bonus('Root Harvest',fruit(2)),
  solkar:bonus('Crownfire Harvest',fruit(1),essence('fire')),
  verdara:bonus('Worldroot Orchard',fruit(3),essence('leaf')),
  tiderex:bonus('Abyssal Catch',fruit(1),essence('water')),
  voltaro:bonus('Sky Harvest',fruit(1),essence('storm')),
  cryomane:bonus('Frozen Harvest',fruit(1),essence('ice')),
  obsidrax:bonus('Mountain Harvest',fruit(1),essence('earth')),
  noctyra:bonus('Eclipse Harvest',fruit(1),essence('shadow')),
  oroluma:bonus('River Canopy',fruit(2),essence('water')),
  chronogibbon:bonus('Timewind Gathering',fruit(2),essence('storm')),
  basaltoracle:bonus('Monolith Harvest',fruit(2),essence('earth')),
  mirecrown:bonus('Spore Orchard',fruit(2),essence('shadow')),
  phoenixmacaque:bonus('Everflow Harvest',fruit(2),essence('fire')),
  ashwing:bonus('Phoenix Embers',essence('fire'),essence('shadow')),
  nightbloom:bonus('Midnight Garden',fruit(2),essence('leaf')),
  lumenkeeper:bonus('Aurora Harvest',fruit(1),essence('light')),
  chronomane:bonus('Time Crown Gathering',essence('light'),essence('shadow'))
};

export function lineupForageProfile(p){
  const data=BONUS[p?.species];if(!data||!SPECIES[p.species])return null;
  const labels=data.rewards.map(r=>r.category==='fruit'?`${r.amount} fruit`:`${r.amount} ${ELEMENTS[r.id].name} essence`);
  const meters=Number.isFinite(p.forageWalkMeters)?Math.max(0,p.forageWalkMeters):0;
  return {name:data.name,description:`+${labels.join(' +')} every ${data.intervalFeet} ft`,intervalFeet:data.intervalFeet,
    rewards:data.rewards.map(r=>({...r})),progressFeet:Math.min(data.intervalFeet,meters/FOOT)};
}

// Returns awards to apply through the same inventory path as ordinary finds.
export function advanceLineupForaging(p,meters){
  const data=BONUS[p?.species];if(!data||!Number.isFinite(meters)||meters<=0)return [];
  const interval=data.intervalFeet*FOOT,previous=Number.isFinite(p.forageWalkMeters)?Math.max(0,Math.min(interval,p.forageWalkMeters)):0;
  const total=previous+meters,count=Math.floor((total+1e-8)/interval);
  p.forageWalkMeters=Math.max(0,total-count*interval);
  return count?data.rewards.map(r=>({...r,amount:r.amount*count,monkeyId:p.id})):[];
}
