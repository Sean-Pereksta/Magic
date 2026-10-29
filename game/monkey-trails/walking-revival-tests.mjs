import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';

function fixture(){
  const s=C.newSave('Walking revival','capuchin','practice',74);
  for(let i=0;i<2;i++){
    const p=structuredClone(C.newSave('Friend','langur','practice',i+1).run.monkey);
    p.id=`friend-${i}`;s.collection[p.id]=p;
  }
  C.setParty(s,[s.run.activeId,'friend-0','friend-1']);
  s.collection['friend-0'].hp=0;s.collection['friend-1'].hp=0;
  return s;
}
const walk=(s,feet)=>C.advance(s,feet*C.FEET,feet*C.FEET,0);
const close=(a,b)=>assert.ok(Math.abs(a-b)<1e-7,`${a} != ${b}`);

test('knocked-out lineup monkeys revive independently at 2,000 accepted feet with 25% equipped HP',()=>{
  const s=fixture(),a=s.collection['friend-0'],b=s.collection['friend-1'];
  walk(s,750);close(a.reviveWalkMeters,750*C.FEET);close(b.reviveWalkMeters,750*C.FEET);
  a.reviveWalkMeters=1000*C.FEET;
  walk(s,1000);close(a.hp,C.stats(a,C.equipped(s,a)).hp*.25);assert.equal(a.reviveWalkMeters,0);
  assert.equal(b.hp,0);close(b.reviveWalkMeters,1750*C.FEET);
  walk(s,250);close(b.hp,C.stats(b,C.equipped(s,b)).hp*.25);assert.equal(b.reviveWalkMeters,0);
});

test('only accepted trail distance counts, with no progress outside the lineup',()=>{
  const s=fixture(),p=s.collection['friend-0'];C.setParty(s,[s.run.activeId,'friend-1']);
  walk(s,500);assert.equal(p.reviveWalkMeters||0,0);close(s.collection['friend-1'].reviveWalkMeters,500*C.FEET);
  s.run.phase='camp';assert.equal(walk(s,500),0);close(s.collection['friend-1'].reviveWalkMeters,500*C.FEET);
  s.run.phase='trail';assert.equal(C.advance(s,NaN,0,0),0);assert.equal(C.advance(s,-10,0,0),0);
  s.run.nextCamp=s.run.distance+100*C.FEET;close(walk(s,500),100*C.FEET);
  assert.equal(s.run.phase,'camp');assert.equal(s.collection['friend-1'].reviveWalkMeters,0);
  assert.ok(s.collection['friend-1'].hp>0); // Camp revival clears the counter.
});

test('progress survives save validation and reload and older saves start at zero',()=>{
  const s=fixture();walk(s,1000);const saved=C.validate(JSON.parse(JSON.stringify(s)));
  close(saved.collection['friend-0'].reviveWalkMeters,1000*C.FEET);walk(saved,1000);
  close(saved.collection['friend-0'].hp,C.stats(saved.collection['friend-0'],C.equipped(saved,saved.collection['friend-0'])).hp*.25);
  const old=fixture();delete old.collection['friend-0'].reviveWalkMeters;C.validate(old);assert.equal(old.collection['friend-0'].reviveWalkMeters,0);
});

test('revival supplies clear accumulated distance before a later knockout',()=>{
  const s=fixture(),p=s.collection['friend-0'];walk(s,1500);s.run.items.revive=1;C.useItem(s,'revive',p.id);
  assert.equal(p.reviveWalkMeters,0);p.hp=0;walk(s,500);assert.equal(p.hp,0);close(p.reviveWalkMeters,500*C.FEET);
});

test('only distance after the revival threshold applies ordinary walking healing',()=>{
  const s=fixture(),p=s.collection['friend-0'];walk(s,1900);walk(s,200);
  const max=C.stats(p,C.equipped(s,p)).hp;close(p.hp,max*.27);
});
