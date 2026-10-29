import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {TRAIL_ROUTES,routeView,chooseRoute,routeEncounterWeight} from './trail-routes.mjs';
import {EVENT_TYPES,WORLD_EVENT_EPOCH as epoch,WORLD_DAY as day,WORLD_EVENT_INTERVAL as interval,currentWorldEvent,worldForecast,worldGuardianView} from './world-events.mjs';
import {REGION_ADVENTURES,adventureSteps} from './regional-adventures.mjs';

const at=(now,fn)=>{const original=Date.now;Date.now=()=>now;try{return fn();}finally{Date.now=original;}};
const quiet=epoch+day+1;
const fresh=(seed=12345)=>at(quiet,()=>C.newSave('World traveler','capuchin','practice',seed));
const reload=s=>C.validate(JSON.parse(JSON.stringify(s)));
const win=s=>{C.readyBattle(s);for(const enemy of s.run.battle.enemies)enemy.hp=0;C.turn(s,'strike');};

test('forecast has all five event identities, quiet days and at least two weeks of notice',()=>{
  const forecast=worldForecast(epoch-2*day,21);
  assert.equal(forecast.current,null);assert.equal(forecast.confirmed,true);
  assert.equal(new Set(forecast.events.map(e=>e.type)).size,5);
  assert.ok(forecast.events.at(-1).startsAt>=epoch+14*day);
  assert.match(forecast.source,/offline/);assert.match(forecast.policy,/waiting encounter keeps/);
  for(const event of forecast.events){assert.ok(event.effect&&event.condition&&event.location&&event.art);assert.equal(event.endsAt-event.startsAt,day);}
});
test('every event starts inclusively, ends exclusively, and returns after quiet days',()=>{
  for(let i=0;i<EVENT_TYPES.length;i++){
    const start=epoch+i*interval,end=start+day;
    assert.equal(currentWorldEvent(start-1),null);
    assert.equal(currentWorldEvent(start).type,EVENT_TYPES[i].id);
    assert.equal(currentWorldEvent(end-1).id,currentWorldEvent(start).id);
    assert.equal(currentWorldEvent(end),null);
    assert.equal(currentWorldEvent(start+EVENT_TYPES.length*interval).type,EVENT_TYPES[i].id);
  }
});
test('forecast and active evaluator agree through a daylight-saving boundary',()=>{
  for(const time of[epoch,epoch+day-1,epoch+day,epoch+2*day,epoch+3*day]){
    const forecast=worldForecast(time);assert.deepEqual(forecast.current,currentWorldEvent(time));
    assert.deepEqual(forecast.events.filter(e=>e.active).map(e=>e.id),forecast.current?[forecast.current.id]:[]);
  }
  const event=worldForecast(epoch).events[0];
  assert.match(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',timeZoneName:'long',dateStyle:undefined,hour:'numeric'}).format(event.startsAt),/Eastern/);
  assert.equal(worldForecast(NaN).confirmed,false);assert.equal(currentWorldEvent(NaN),null);
});
test('a trail choice survives reload, cannot be changed mid-stretch, and never blocks walking',()=>at(quiet,()=>{
  let s=fresh();chooseRoute(s,'berry-grove');s=reload(s);
  assert.equal(routeView(s).selected.id,'berry-grove');assert.equal(routeView(s).canChoose,false);
  assert.throws(()=>chooseRoute(s,'ancient-ruins'),/once/);
  assert.equal(C.advance(s,150),150);assert.equal(s.run.distance,150);
  const unchosen=fresh();assert.equal(C.advance(unchosen,150),150);
  assert.deepEqual(s.meta.trailRoutes.visited,['berry-grove']);
}));
test('earned camp opens exactly one new choice without repeating the old selection',()=>at(quiet,()=>{
  const s=fresh();chooseRoute(s,'berry-grove');C.advance(s,6000*C.FEET);
  assert.equal(s.run.phase,'camp');assert.equal(routeView(s).canChoose,true);assert.equal(routeView(s).selected,null);
  chooseRoute(s,'ancient-ruins');C.leaveCamp(s);assert.equal(routeView(s).selected.id,'ancient-ruins');
  assert.throws(()=>chooseRoute(s,'misty-canopy'));assert.equal(s.meta.trailRoutes.choices,2);
  assert.equal(routeView(reload(s)).selected.id,'ancient-ruins');
}));
test('older checkpoints gain only additive route state and retain all existing inventory and companions',()=>at(quiet,()=>{
  const s=reload(fresh());delete s.meta.trailRoutes;delete s.run.trailRoute;
  s.run.fruit=37;s.run.essence.fire=12;s.run.items.berry=7;s.collection[s.run.activeId].nickname='Old friend';
  const before=structuredClone({collection:s.collection,fruit:s.run.fruit,essence:s.run.essence,items:s.run.items,gear:s.meta.gear});
  const loaded=reload(s);assert.deepEqual({collection:loaded.collection,fruit:loaded.run.fruit,essence:loaded.run.essence,items:loaded.run.items,gear:loaded.meta.gear},before);
  assert.equal(routeView(loaded).selected,null);assert.equal(routeView(loaded).canChoose,true);
}));
test('unknown saved routes fall back without spending resources; battle choices are rejected',()=>at(quiet,()=>{
  const s=fresh();s.run.trailRoute={id:'removed-content',untilDistance:100,selectedDistance:0};assert.equal(routeView(s).selected,null);
  assert.throws(()=>chooseRoute(s,'not-a-route'));C.advance(s,C.ENCOUNTER_STEP);C.beginPendingEncounter(s);
  assert.throws(()=>chooseRoute(s,'berry-grove'));assert.equal(s.run.fruit>=8,true);
}));
test('Berry Grove changes actual fruit discovery rewards without changing accepted distance or RNG',()=>at(quiet,()=>{
  let fruitDiscoveries=0;
  for(let seed=1;seed<=25;seed++){
    const base=fresh(seed),grove=structuredClone(base);chooseRoute(grove,'berry-grove');const a=[],b=[];
    C.advance(base,C.RESOURCE_STEP*3,0,0,p=>a.push(p));C.advance(grove,C.RESOURCE_STEP*3,0,0,p=>b.push(p));
    const count=a.filter(p=>p.category==='fruit').length;fruitDiscoveries+=count;
    assert.equal(grove.run.fruit-base.run.fruit,count);assert.equal(base.rng,grove.rng);assert.equal(base.run.distance,grove.run.distance);
    b.filter(p=>p.category==='fruit').forEach(p=>assert.equal(p.amount,3));
  }
  assert.ok(fruitDiscoveries>0);
}));
test('Ancient Ruins gives actual materials every third discovery and preserves progress across reload',()=>at(quiet,()=>{
  const base=fresh(),ruins=fresh();chooseRoute(ruins,'ancient-ruins');C.advance(base,3*C.RESOURCE_STEP);C.advance(ruins,2*C.RESOURCE_STEP);
  const restored=reload(ruins);C.advance(restored,C.RESOURCE_STEP);
  assert.equal(restored.run.items.charmMaterial-base.run.items.charmMaterial,1);
  assert.equal(restored.run.lastDiscovery.worldBonuses[0].source,'Ancient Ruins');
}));
test('ruins difficulty starts after tutorials and encounter snapshots retain the selected route',()=>at(quiet,()=>{
  for(const wins of[0,2]){
    const base=fresh(),ruins=fresh();base.run.ordinaryWins=wins;ruins.run.ordinaryWins=wins;chooseRoute(ruins,'ancient-ruins');
    C.advance(base,C.ENCOUNTER_STEP);C.advance(ruins,C.ENCOUNTER_STEP);
    assert.equal(ruins.run.pendingEncounter.plan.level,base.run.pendingEncounter.plan.level+(wins>=2?1:0));
    assert.equal(reload(ruins).run.pendingEncounter.trailRoute,'ancient-ruins');
  }
}));
test('Misty Canopy favors unusual companions without opening rarity/depth gates',()=>at(quiet,()=>{
  const s=fresh(),before=C.encounterPool(s);chooseRoute(s,'misty-canopy');
  assert.deepEqual(C.encounterPool(s),before);assert.ok(before.every(id=>C.SPECIES[id].rarity!=='rare'));
  const rare=Object.keys(C.SPECIES).find(id=>C.SPECIES[id].rarity==='rare'),common=Object.keys(C.SPECIES).find(id=>C.SPECIES[id].rarity==='common');
  assert.ok(routeEncounterWeight(s,rare)>routeEncounterWeight(s,common));
  assert.equal(TRAIL_ROUTES.length,3);
}));
test('Fruit Bloom begins and ends during an ongoing expedition and stacks modestly with Berry Grove',()=>{
  let seen=0;
  for(let seed=1;seed<=20;seed++){
    const s=fresh(seed);chooseRoute(s,'berry-grove');
    at(epoch,()=>C.advance(s,3*C.RESOURCE_STEP,0,0,p=>{if(p.category==='fruit'){seen++;assert.equal(p.amount,4);assert.equal(p.worldEvent.type,'fruit-bloom');}}));
    at(epoch+day,()=>C.advance(s,C.RESOURCE_STEP,0,0,p=>{if(p.category==='fruit')assert.equal(p.amount,3);assert.equal(p.worldEvent,undefined);}));
  }
  assert.ok(seen>0);
});
test('Essence Winds awards extra Leaf essence into the real inventory and describes the source',()=>{
  let found=0;
  for(let seed=1;seed<=20;seed++){
    const base=fresh(seed),wind=fresh(seed);at(quiet,()=>C.advance(base,C.RESOURCE_STEP));
    at(epoch+2*interval,()=>C.advance(wind,C.RESOURCE_STEP));
    if(wind.run.lastDiscovery.category==='essence'){found++;assert.ok(wind.run.essence.leaf>base.run.essence.leaf);assert.ok(wind.run.lastDiscovery.worldBonuses.some(p=>p.id==='leaf'&&p.source==='Essence Winds'));}
    assert.equal(base.rng,wind.rng);
  }
  assert.ok(found>0);
});
test('waiting encounters keep event identity after expiry, reload, and deliberate battle start',()=>{
  const s=fresh();at(epoch+interval,()=>C.advance(s,C.ENCOUNTER_STEP));const id=s.run.pendingEncounter.worldEvent.id;
  const loaded=at(epoch+interval+day,()=>reload(s));assert.equal(currentWorldEvent(epoch+interval+day),null);
  at(epoch+interval+day,()=>C.beginPendingEncounter(loaded));assert.equal(loaded.run.battle.worldEvent.id,id);
});
test('Guardian Stirring is optional, requires normal progress, and pays its saved extra reward only once',()=>{
  const now=epoch+3*interval,s=fresh();at(now,()=>assert.throws(()=>C.startWorldGuardian(s),/introductory/));
  s.run.ordinaryWins=2;at(now,()=>C.advance(s,C.ENCOUNTER_STEP));
  const waiting=s.run.pendingEncounter.id;
  at(now,()=>C.startWorldGuardian(s));assert.equal(s.run.pendingEncounter.id,waiting);
  const eventId=s.run.battle.worldEvent.id,claimId=s.run.battle.id;
  const loaded=reload(s),before=loaded.run.essence.leaf;
  at(now+day,()=>win(loaded));assert.ok(loaded.run.essence.leaf>=before+3);
  assert.equal(loaded.meta.claims[`reward:${claimId}`],true);assert.ok(eventId);
  C.skipBefriend(loaded);at(now,()=>assert.throws(()=>C.startWorldGuardian(loaded),/already earned/));
  assert.equal(worldGuardianView(loaded,now).claimed,true);assert.equal(worldGuardianView(loaded,now+day).event,null);
});
test('route effects are identical for one full stretch or small accepted samples up to camp',()=>at(quiet,()=>{
  for(const route of TRAIL_ROUTES){
    const whole=fresh(932718),chunks=fresh(932718);chooseRoute(whole,route.id);chooseRoute(chunks,route.id);
    C.advance(whole,6000*C.FEET);for(let i=0;i<600;i++)C.advance(chunks,10*C.FEET);
    assert.equal(whole.run.phase,'camp');assert.equal(chunks.run.phase,'camp');
    assert.equal(whole.run.fruit,chunks.run.fruit);assert.deepEqual(whole.run.items,chunks.run.items);assert.deepEqual(whole.run.essence,chunks.run.essence);
    assert.equal(whole.run.trailRoute.discoveries,chunks.run.trailRoute.discoveries);
    assert.equal(routeView(whole).selected,null);assert.equal(routeView(chunks).selected,null);
  }
}));
test('the event guardian cannot complete a regional guardian step or grant regional Wisdom early',()=>at(epoch+3*interval,()=>{
  const s=fresh(),adventure=REGION_ADVENTURES[0],guardian=adventure.steps.find(step=>step.kind==='guardian');
  s.run.ordinaryWins=2;C.advance(s,C.ENCOUNTER_STEP);
  const region=s.meta.adventures.regions[adventure.id];region.claimed=adventure.steps.slice(0,guardian.index).map(step=>step.id);region.stepId=guardian.id;region.current=0;
  assert.equal(adventureSteps(s,adventure.id).find(step=>step.id===guardian.id).complete,false);
  C.startWorldGuardian(s);win(s);
  assert.equal(s.meta.adventures.regions[adventure.id].current,0);
  assert.equal(adventureSteps(s,adventure.id).find(step=>step.id===guardian.id).complete,false);
  assert.equal(s.meta.trailWisdom.claims[`guardian:${adventure.id}`],undefined);
  assert.equal(s.run.monkey.memories.guardianVictories,0);
  assert.equal(s.run.wins,1);assert.equal(s.run.monkey.memories.victories,1);
}));
test('event foraging bonuses pay per earned interval for long, small, and resumed GPS samples',()=>{
  for(const time of[epoch,epoch+2*interval])at(time,()=>{
    const whole=fresh(34192),friend=structuredClone(whole.run.monkey);friend.id='foraging-friend';friend.species='verdara';friend.types=[...C.SPECIES.verdara.types];friend.learned=[];friend.loadout=['strike','brace','breathe'];C.syncProgression(friend);friend.hp=C.stats(friend).hp;friend.energy=C.stats(friend).maxEnergy;whole.collection[friend.id]=friend;C.setParty(whole,[whole.run.activeId,friend.id]);whole.run.nextEncounter=whole.run.nextCamp=1e9;
    const chunks=structuredClone(whole);let resumed=structuredClone(whole);
    C.advance(whole,1250*C.FEET);for(let i=0;i<50;i++)C.advance(chunks,25*C.FEET);
    C.advance(resumed,400*C.FEET);resumed=reload(resumed);C.advance(resumed,850*C.FEET);
    for(const other of[chunks,resumed]){assert.equal(other.run.fruit,whole.run.fruit);assert.deepEqual(other.run.essence,whole.run.essence);assert.deepEqual(other.run.items,whole.run.items);assert.equal(other.rng,whole.rng);}
    const control=fresh(34192);control.run.nextEncounter=control.run.nextCamp=1e9;C.advance(control,1250*C.FEET);
    assert.equal(whole.run.fruit-control.run.fruit,time===epoch?20:15);
    assert.equal(whole.run.essence.leaf-control.run.essence.leaf,time===epoch?5:10);
  });
});
