import test,{beforeEach} from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {advanceLineupForaging} from './lineup-foraging.mjs';

// These exact assertions isolate base lineup rewards. Event additions and their
// GPS sample-size invariance are exercised separately in world-adventure-tests.
// A test-scoped mock leaves storage timers and every other test file untouched.
beforeEach(t=>t.mock.method(Date,'now',()=>Date.UTC(2026,10,1)));

function add(s,species,id){
  const p=structuredClone(s.run.monkey);p.id=id;p.species=species;p.types=[...C.SPECIES[species].types];p.learned=[];p.loadout=['strike','brace','breathe'];C.syncProgression(p);p.hp=C.stats(p).hp;p.energy=C.stats(p).maxEnergy;s.collection[id]=p;return p;
}
function fixture(){const s=C.newSave('Foraging','capuchin','practice',12345);s.run.nextEncounter=s.run.nextCamp=1e9;return s;}
function walk(s,feet){return C.advance(s,feet*C.FEET);}

test('all Ultras have unique, previewable rewards and several ordinary monkeys gather fruit',()=>{
  const ultras=Object.keys(C.SPECIES).filter(id=>C.SPECIES[id].rarity==='ultra');
  assert.equal(ultras.length,16);
  const descriptions=ultras.map(species=>C.lineupForageProfile({species}).description);
  assert.equal(new Set(descriptions).size,ultras.length);
  assert.ok(Object.values(C.SPECIES).filter(p=>p.rarity!=='ultra'&&C.lineupForageProfile({species:p.id})?.description.includes('fruit')).length>=5);
  assert.equal(C.lineupForageProfile({species:'capuchin'}),null);
});

test('foraging pays per conscious lineup monkey on accepted 250-foot intervals and stacks',()=>{
  const s=fixture(),a=add(s,'mossmop','a'),b=add(s,'rootstone','b');C.setParty(s,[s.run.activeId,a.id,b.id]);
  const control=structuredClone(s);control.collection.a.species='capuchin';control.collection.b.species='capuchin';
  walk(s,249);walk(control,249);assert.equal(s.run.fruit,control.run.fruit);
  walk(s,1);walk(control,1);assert.equal(s.run.fruit-control.run.fruit,3);
  assert.equal(C.lineupForageProfile(a).progressFeet,0);assert.equal(C.lineupForageProfile(b).progressFeet,0);
  walk(s,500);walk(control,500);assert.equal(s.run.fruit-control.run.fruit,9);
});

test('progress is per individual, survives reload, and pauses while removed or knocked out',()=>{
  const s=fixture(),a=add(s,'mossmop','a'),b=add(s,'rootstone','b');C.setParty(s,[s.run.activeId,a.id,b.id]);
  walk(s,100);C.setParty(s,[s.run.activeId,b.id]);walk(s,100);assert.ok(Math.abs(C.lineupForageProfile(a).progressFeet-100)<1e-6);
  C.setParty(s,[s.run.activeId,a.id,b.id]);a.hp=0;walk(s,100);assert.ok(Math.abs(C.lineupForageProfile(a).progressFeet-100)<1e-6);
  const reopened=C.validate(JSON.parse(JSON.stringify(s)));assert.ok(Math.abs(C.lineupForageProfile(reopened.collection.a).progressFeet-100)<1e-6);
  reopened.run.phase='battle';assert.equal(walk(reopened,100),0);assert.ok(Math.abs(C.lineupForageProfile(reopened.collection.a).progressFeet-100)<1e-6);
});

test('a monkey revived mid-walk forages only after waking',()=>{
  const s=fixture(),a=add(s,'mossmop','a');C.setParty(s,[s.run.activeId,a.id]);a.hp=0;a.reviveWalkMeters=1900*C.FEET;
  walk(s,200);assert.ok(a.hp>0);assert.ok(Math.abs(C.lineupForageProfile(a).progressFeet-100)<1e-6);
});

test('bonus amounts and saved remainders are independent of GPS sample size',()=>{
  const p={id:'one',species:'verdara'},q={id:'two',species:'verdara'};
  const one=advanceLineupForaging(p,625*C.FEET),many=[];for(let i=0;i<25;i++)many.push(...advanceLineupForaging(q,25*C.FEET));
  assert.deepEqual(one.map(x=>[x.category,x.id,x.amount]),[['fruit',undefined,6],['essence','leaf',2]]);
  assert.equal(many.filter(x=>x.category==='fruit').reduce((n,x)=>n+x.amount,0),6);
  assert.ok(Math.abs(C.lineupForageProfile(p).progressFeet-C.lineupForageProfile(q).progressFeet)<1e-6);
});

test('Ultra lineup rewards are credited to the proper inventory without advancing discovery',()=>{
  const s=fixture(),p=add(s,'solkar','ultra');C.setParty(s,[s.run.activeId,p.id]);
  const control=structuredClone(s);control.collection.ultra.species='capuchin';
  walk(s,250);walk(control,250);
  assert.equal(s.run.fruit-control.run.fruit,1);
  assert.equal(s.run.essence.fire-control.run.essence.fire,1);
  assert.equal(s.run.discoveries,control.run.discoveries);
  const before=JSON.stringify(s);assert.equal(C.advance(s,NaN),0);assert.equal(JSON.stringify(s),before);
});
