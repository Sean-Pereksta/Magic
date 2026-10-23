import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {REGION_CHALLENGES,normalizeChallenges,regionChallenges,trackedChallenge,trackChallenge,claimChallenge,recordRegionEvent} from './challenges.mjs';

const save=(mode='gps')=>C.newSave('Regional Explorer','capuchin',mode,98765);
const walk=(s,meters)=>{s.run.nextEncounter=1e8;s.run.nextCamp=1e8;return C.advance(s,meters,meters,0);};
const goal=(s,kind,landId)=>regionChallenges(s,landId).find(row=>row.kind===kind);

test('all ten regions have three distinct themed challenges with real item rewards',()=>{
 assert.equal(REGION_CHALLENGES.length,30);assert.equal(new Set(REGION_CHALLENGES.map(row=>row.id)).size,30);
 for(const land of C.LANDS){
  const rows=REGION_CHALLENGES.filter(row=>row.landId===land.id);assert.deepEqual(rows.map(row=>row.kind),['distance','discoveries','victories']);
  for(const row of rows){assert.ok(row.title&&row.description&&row.goal>0);assert.ok(row.reward.fruit>0);assert.ok(Object.keys(row.reward.items).length);for(const [id,n]of Object.entries(row.reward.items)){assert.ok(C.ITEMS[id]);assert.ok(Number.isInteger(n)&&n>0);}}
 }
});

test('walking challenges use accepted distance, show partial bars and never auto-claim',()=>{
 const s=save(),first=goal(s,'distance'),before={fruit:s.run.fruit,berry:s.run.items.berry};
 trackChallenge(s,first.id);walk(s,first.goal/2);let row=trackedChallenge(s);assert.equal(row.current,375);assert.equal(row.percent,50);assert.equal(row.complete,false);
 walk(s,first.goal/2);row=trackedChallenge(s);assert.equal(row.complete,true);assert.equal(row.percent,100);assert.equal(row.claimed,false);
 assert.ok(s.run.fruit>=before.fruit); // Ordinary discoveries still keep their original rewards.
 const preClaim={fruit:s.run.fruit,berry:s.run.items.berry},reward=claimChallenge(s,first.id);
 assert.equal(s.run.fruit,preClaim.fruit+reward.fruit);assert.equal(s.run.items.berry,preClaim.berry+reward.items.berry);assert.equal(trackedChallenge(s),null);
 assert.equal(goal(s,'distance').claimed,true);assert.throws(()=>claimChallenge(s,first.id),/already claimed/);
});

test('actual discovery and victory hooks advance regional counters once',()=>{
 const s=save();C.advance(s,C.ENCOUNTER_STEP);const finds=goal(s,'discoveries');
 assert.equal(finds.current,s.run.discoveries);assert.equal(finds.current,8);assert.equal(finds.complete,true);
 if(s.run.pendingEncounter)C.beginPendingEncounter(s);C.readyBattle(s);s.run.battle.enemy.hp=1;C.turn(s,'strike');
 assert.equal(goal(s,'victories').current,1);assert.equal(s.run.phase,'befriend');
 assert.throws(()=>C.turn(s,'strike'));assert.equal(goal(s,'victories').current,1);
});

test('each region has independent progress and crossing an unlock keeps the walked region',()=>{
 const s=save();walk(s,2005);const jungle=regionChallenges(s);assert.ok(jungle.find(row=>row.kind==='distance').current>=2005);
 recordRegionEvent(s,'victories');C.selectLand(s,'sunstone-savannah');
 assert.equal(goal(s,'distance').current,0);assert.equal(goal(s,'discoveries').current,0);assert.equal(goal(s,'victories').current,0);
 walk(s,100);recordRegionEvent(s,'victories');recordRegionEvent(s,'victories');
 assert.equal(goal(s,'distance').current,100);assert.equal(goal(s,'victories').current,2);
 assert.equal(goal(s,'victories','emerald-jungle').current,1);assert.equal(goal(s,'distance','emerald-jungle').current,2005);
 assert.throws(()=>recordRegionEvent(s,'victories',1,'emerald-jungle'),/region you are exploring/);
});

test('claimed rewards, progress and tracked selection survive reload and expedition restart',()=>{
 let s=save();walk(s,800);const distance=goal(s,'distance');claimChallenge(s,distance.id);
 const victory=goal(s,'victories');trackChallenge(s,victory.id);recordRegionEvent(s,'victories');
 s=C.validate(structuredClone(s));assert.equal(goal(s,'distance').claimed,true);assert.equal(trackedChallenge(s).id,victory.id);assert.equal(goal(s,'victories').current,1);
 s.run.phase='dead';C.restart(s);assert.equal(goal(s,'distance').claimed,true);assert.equal(goal(s,'victories').current,1);assert.equal(trackedChallenge(s).id,victory.id);
 assert.throws(()=>claimChallenge(s,distance.id),/already claimed/);
});

test('rewards require completion and a living trail or camp, with no partial inventory changes',()=>{
 for(const mode of ['gps','practice']){
  const s=save(mode),distance=goal(s,'distance');assert.throws(()=>claimChallenge(s,distance.id),/Complete/);walk(s,distance.goal);
  for(const phase of ['battle','replacement','befriend','dead']){s.run.phase=phase;const before={fruit:s.run.fruit,items:structuredClone(s.run.items)};assert.throws(()=>claimChallenge(s,distance.id),/living trail or camp/);assert.deepEqual({fruit:s.run.fruit,items:s.run.items},before);assert.equal(goal(s,'distance').claimed,false);}
  s.run.phase='trail';s.run.monkey.hp=0;assert.throws(()=>claimChallenge(s,distance.id),/living trail or camp/);s.run.monkey.hp=1;s.run.phase='camp';assert.ok(claimChallenge(s,distance.id).fruit>0);
 }
});

test('tracking is limited to one unclaimed unlocked goal and does not grant locked rewards',()=>{
 const s=save(),first=goal(s,'distance'),second=goal(s,'discoveries');
 trackChallenge(s,first.id);trackChallenge(s,second.id);assert.equal(trackedChallenge(s).id,second.id);assert.equal(goal(s,'distance').tracked,false);
 trackChallenge(s,null);assert.equal(trackedChallenge(s),null);
 const locked=goal(s,'distance','sunstone-savannah');assert.equal(locked.unlocked,false);assert.throws(()=>trackChallenge(s,locked.id),/Unlock/);assert.throws(()=>claimChallenge(s,locked.id),/Complete/);
 assert.throws(()=>trackChallenge(s,'bad-goal'),/Choose/);assert.throws(()=>claimChallenge(s,'bad-goal'),/Choose/);assert.throws(()=>regionChallenges(s,'bad-region'),/Choose/);
});

test('new challenge metadata does not invent historical wins and sanitizes malformed imports',()=>{
 const s=save();s.run.wins=99;s.run.discoveries=99;s.meta.history=[{feet:10000,wins:80}];let rows=regionChallenges(s);
 assert.equal(rows.find(row=>row.kind==='victories').current,0);assert.equal(rows.find(row=>row.kind==='discoveries').current,0);
 const id=rows[0].id;s.meta.regionChallenges={version:1,regions:{'emerald-jungle':{discoveries:Infinity,victories:-2},'bad-region':{victories:999}},claimed:[id,id,'bad-goal'],tracked:id};
 const normalized=normalizeChallenges(s);assert.equal(Object.keys(normalized.regions).length,10);assert.deepEqual(normalized.regions['emerald-jungle'],{discoveries:0,victories:0});assert.deepEqual(normalized.claimed,[id]);assert.equal(normalized.tracked,null);
 s.meta.regionChallenges.tracked='sunstone-savannah:distance';assert.equal(normalizeChallenges(s).tracked,null);
 const before=structuredClone(s.meta.regionChallenges);
 for(const [kind,n] of [['distance',1],['discoveries',-1],['victories',NaN],['victories',1.5],['victories',1001]])assert.throws(()=>recordRegionEvent(s,kind,n),/Invalid/);
 assert.deepEqual(s.meta.regionChallenges,before);
});

test('encounter clipping and rejected movement do not inflate tracked distance',()=>{
 const s=save(),id=goal(s,'distance').id;trackChallenge(s,id);const accepted=C.advance(s,C.ENCOUNTER_STEP,C.ENCOUNTER_STEP,0);
 assert.equal(accepted,C.ENCOUNTER_STEP);assert.equal(trackedChallenge(s).current,accepted);assert.ok(s.run.pendingEncounter);C.beginPendingEncounter(s);assert.equal(C.advance(s,1000,1000,0),0);assert.equal(trackedChallenge(s).current,accepted);
});
