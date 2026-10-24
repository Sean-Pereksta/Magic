import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {LANDS} from './lands.mjs';
import {regionChallenges,recordRegionEvent} from './challenges.mjs';
import * as R from './regional-adventures.mjs';
import {habitatUI,speciesHuntUI,regionAdventureUI} from './regional-ui.mjs';
const save=()=>C.newSave('Regional adventure','capuchin','practice',12345);
function unlock(s){s.world.totalDistance=150000;s.world.unlocked=LANDS.map(l=>l.id);s.run.distance=40000*C.FEET;s.run.ordinaryWins=3;}

test('all 110 species have depth-valid obtainable regional homes, including all sixteen Ultras',()=>{
 const s=save();unlock(s);const ordinary=new Set(),ultras=new Set();
 for(const land of LANDS){C.selectLand(s,land.id);const pool=R.regionSpeciesPool(s),ultra=R.regionSpeciesPool(s,Object.keys(C.SPECIES),{ultra:true});assert.ok(pool.some(id=>C.SPECIES[id].rarity==='common'),land.id);assert.ok(pool.some(id=>C.SPECIES[id].rarity==='uncommon'),land.id);assert.ok(pool.some(id=>C.SPECIES[id].rarity==='rare'),land.id);assert.ok(ultra.length,land.id);pool.forEach(id=>ordinary.add(id));ultra.forEach(id=>ultras.add(id));}
 assert.equal(ordinary.size,94);assert.equal(ultras.size,16);assert.deepEqual(new Set([...ordinary,...ultras]),new Set(Object.keys(C.SPECIES)));
 assert.ok(!R.speciesHomes('pyreclasp').includes('emerald-jungle'));assert.ok(R.speciesHomes('pyreclasp').includes('ember-volcano'));
 assert.equal(R.speciesMinimumFeet('bramblebat'),10000);assert.equal(R.speciesMinimumFeet('chronogibbon'),5000); // existing Ultra timing stays unchanged
});

test('habitat hints match the real pool and weight influences without route constraints',()=>{
 const s=save();unlock(s);const seen=new Set();
 for(const land of LANDS){C.selectLand(s,land.id);for(const [east,north]of [[62,36],[50,-48],[0,0],[100,100],[250,250]]){s.world.regions[land.id].position={east,north};const habitat=R.habitatView(s);seen.add(habitat.id);assert.ok(habitat.likely.length);assert.ok(habitat.likely.every(id=>R.regionSpeciesPool(s).includes(id)));assert.match(habitat.description,/safe, accessible route/);for(const id of habitat.likely)assert.ok(R.regionalWeight(s,id)>=1);}}
 assert.ok(seen.has('landmark'));assert.ok(seen.has('riverbank'));
 const before=s.world.totalDistance;s.world.regions[s.world.activeLand].position={east:0,north:-48};assert.ok(C.advance(s,1,1,0)>=0);assert.ok(s.world.totalDistance>=before);
});

test('tracking shows locked homes and research cannot bypass region or depth requirements',()=>{
 const s=save();let h=R.trackHunt(s,'pyreclasp');assert.equal(h.unlocked,false);R.advanceAdventures(s,100000);assert.equal(R.huntView(s).current,0);assert.equal(R.consumeReadyHunt(s),null);
 unlock(s);C.selectLand(s,'ember-volcano');s.run.distance=0;R.advanceAdventures(s,4000);assert.equal(R.huntView(s).current,0);
 s.run.distance=10000*C.FEET;R.advanceAdventures(s,3200);h=R.huntView(s);assert.equal(h.ready,true);const stored=JSON.parse(JSON.stringify(s));assert.equal(R.huntView(stored).ready,true);
 C.selectLand(s,'emerald-jungle');assert.equal(R.consumeReadyHunt(s),null);C.selectLand(s,'ember-volcano');const encounter=R.consumeReadyHunt(s);assert.equal(encounter.species,'pyreclasp');assert.equal(encounter.landId,'ember-volcano');assert.equal(R.huntView(s).current,0);assert.equal(R.consumeReadyHunt(s),null);
});

test('research and adventure progress survive restart while injuries never change',()=>{
 const s=save();unlock(s);C.selectLand(s,'emerald-jungle');R.trackHunt(s,'rootstone');R.advanceAdventures(s,600);const hp=s.run.monkey.hp=7;
 const h=R.huntView(s);assert.equal(h.current,600);assert.equal(s.run.monkey.hp,hp);s.run.phase='dead';C.restart(s);assert.equal(R.huntView(s).current,600);assert.equal(R.huntView(s).ready,false);assert.equal(s.meta.pinnedGoal.kind,'hunt');
});

test('all ten adventures have distinct connected steps, modest explicit rewards, and one permanent keepsake',()=>{
 const s=save();unlock(s);
 for(const land of LANDS){C.selectLand(s,land.id);let safety=0;while(safety++<10){const a=R.adventureView(s,land.id),row=a.steps.find(step=>step.active);if(!row)break;R.trackAdventure(s,row.id);assert.throws(()=>R.claimAdventure(s,row.id),/Complete/);
   if(['distance','objects'].includes(row.kind))R.advanceAdventures(s,row.goal);
   else if(row.kind==='seen'){const ids=R.regionSpeciesPool(s).filter(id=>C.SPECIES[id].types.some(type=>land.types.includes(type)));R.recordAdventureEvent(s,'seen',1,land.id,{species:ids});}
   else if(row.kind==='guardian'){const descriptor=R.guardianChallenge(s,land.id);assert.equal(descriptor.guardian,true);R.recordAdventureEvent(s,'guardian',1,land.id);}
   else R.recordAdventureEvent(s,row.kind,row.goal,land.id);
   const ready=R.adventureSteps(s,land.id).find(step=>step.id===row.id);assert.equal(ready.complete,true,row.title);const before=s.run.fruit,reward=R.claimAdventure(s,row.id);assert.equal(s.run.fruit,before+reward.fruit);assert.throws(()=>R.claimAdventure(s,row.id),/already claimed/);
   assert.equal(R.adventureSteps(JSON.parse(JSON.stringify(s)),land.id).find(step=>step.id===row.id).claimed,true);
  }
  assert.equal(R.adventureSteps(s,land.id).filter(row=>row.claimed).length,7);
 }
 assert.equal(s.meta.adventures.cosmetics.length,10);assert.equal(new Set(s.meta.adventures.cosmetics).size,10);
});

test('reopening or repeatedly claiming regional objects never awards supplies and battle claims are blocked',()=>{
 const s=save();R.normalizeAdventures(s);const row=R.adventureSteps(s)[0];const before=s.run.fruit;for(let i=0;i<30;i++){R.adventureView(s);R.habitatView(s);assert.throws(()=>R.claimAdventure(s,row.id),/Complete/);}assert.equal(s.run.fruit,before);
 R.advanceAdventures(s,row.goal);s.run.phase='battle';assert.throws(()=>R.claimAdventure(s,row.id),/living trail/);assert.equal(s.run.fruit,before);s.run.phase='trail';R.claimAdventure(s,row.id);assert.equal(s.run.fruit,before+row.reward.fruit);
});

test('old challenges and saved claims remain intact; origin victory credit requires the actual battle',()=>{
 const s=save();unlock(s);const first=regionChallenges(s)[0];s.meta.regionChallenges.claimed=[first.id];R.normalizeAdventures(s);assert.equal(regionChallenges(s)[0].claimed,true);
 C.selectLand(s,'sunstone-savannah');assert.throws(()=>recordRegionEvent(s,'victories',1,'emerald-jungle'),/region you are exploring/);s.run.battle={id:'frozen-battle',originLandId:'emerald-jungle'};recordRegionEvent(s,'victories',1,'emerald-jungle',{battleId:'frozen-battle'});assert.equal(regionChallenges(s,'emerald-jungle').find(row=>row.kind==='victories').current,1);
 assert.throws(()=>recordRegionEvent(s,'victories',1,'emerald-jungle',{battleId:'other-battle'}),/region you are exploring/);
});

test('regional surfaces expose real hints, saved research, and explicit track and claim actions',()=>{
 const s=save();assert.match(habitatUI(s),/LOCAL HABITAT/);assert.match(speciesHuntUI(s,'cryomane'),/Track this monkey/);assert.match(speciesHuntUI(s,'cryomane'),/Frostpeak Mountains/);assert.match(regionAdventureUI(s),/data-action="adventure-track"/);assert.match(regionAdventureUI(s),/data-action="adventure-claim"/);
});
