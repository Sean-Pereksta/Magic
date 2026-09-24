import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame, declareWar, projectedBattleLosses, resolveMovement, sizeOf } from '../core.mjs';
import { emptyUnits } from '../economy.mjs';
import { battlePreview } from '../battle-preview.mjs';
import { BattleEffects, eventTroopLosses } from '../battle-effects.mjs';

function encounter(){
  const s=createGame(321),a=s.armies[0],d=s.armies[1];
  declareWar(s,a.owner,d.owner);s.armies=[a,d];
  Object.assign(a,{tile:'10,10',units:{...emptyUnits(),levy:120,archer:50,cavalry:30},path:['11,10'],target:'11,10',order:'attack'});
  Object.assign(d,{tile:'11,10',units:{...emptyUnits(),levy:160,archer:40},path:[],order:'hold'});
  for(const t of Object.values(s.tiles))Object.assign(t,{terrain:'plains',owner:null,building:null,walls:0,fortIntegrity:0,road:false,river:false,levels:{}});
  s.tiles[d.tile].owner=d.owner;return {s,a,d};
}
test('forecasts are deterministic, bounded and do not mutate any campaign data',()=>{
  const {s,a,d}=encounter(),before=structuredClone(s);
  const first=projectedBattleLosses(s,a.id,d.tile);
  assert.deepEqual(projectedBattleLosses(s,a.id,d.tile),first);assert.deepEqual(s,before);
  for(const [range,army] of [[first.yours,a],[first.theirs,d]])assert.ok(range.low>=0&&range.high>=range.low&&range.high<=sizeOf(army));
  assert.match(battlePreview(s,a.id,d.tile),/You <b>−\d+–\d+/);
  assert.match(battlePreview(s,a.id,d.tile),/Them <b>−\d+–\d+/);
  assert.equal(battlePreview(s,d.id,a.tile),'');
  s.wars=[];assert.equal(projectedBattleLosses(s,a.id,d.tile),null);
});
test('forecasts include the real trapped-retreat penalty and match a sampled actual clash',()=>{
  const {s,a,d}=encounter();
  for(const t of Object.values(s.tiles))if(![a.tile,d.tile].includes(t.id))t.terrain='mountain';
  const forecast=projectedBattleLosses(s,a.id,d.tile),copy=structuredClone(s);
  copy.rng=Math.imul(1,0x9e3779b1)>>>0;resolveMovement(copy);
  const e=copy.militaryEvents.find(e=>e.action==='battle'),losses=eventTroopLosses(e);
  for(const [i,r] of [forecast.yours,forecast.theirs].entries())assert.ok(losses[i]>=r.low&&losses[i]<=r.high);
});
test('siege projections and floating losses never count walls as troops',()=>{
  const {s,a,d}=encounter();Object.assign(s.tiles[d.tile],{building:'city',walls:100,levels:{city:1,wall:2}});
  const before=structuredClone(s),forecast=projectedBattleLosses(s,a.id,d.tile);
  assert.equal(forecast.kind,'siege');assert.deepEqual(forecast.theirs,{low:0,high:0});assert.deepEqual(s,before);
  resolveMovement(s);const e=s.militaryEvents.find(e=>e.action==='siege');
  assert.deepEqual(eventTroopLosses(e),e.troopLosses);
  assert.equal(eventTroopLosses({action:'siege',before:[100,100],after:[97,50]}),null);
  assert.equal(eventTroopLosses({action:'structure',damage:40}),null);
});
test('a wiped out defender is removed and reported as destroyed, without a phantom retreat',()=>{
  const {s,a,d}=encounter();a.units={...emptyUnits(),crossbow:3000};d.units={...emptyUnits(),levy:200};
  resolveMovement(s);const e=s.militaryEvents.find(e=>e.action==='battle');
  assert.ok(!s.armies.some(x=>x.id===d.id));assert.equal(e.retreat,null);
  assert.equal(eventTroopLosses(e)[1],200);assert.match(s.events[0].message,/destroyed/);
});
test('actual loss labels identify both sides, rise, fade, honor reduced motion and expire',()=>{
  const effects=new BattleEffects(),s={seed:1,turn:1,militaryEvents:[]};effects.ingest(s,0);
  const e={id:1,turn:1,action:'battle',attacker:'ashen',defender:'wintermere',tile:'1,1',before:[100,150],after:[81,118]};
  s.militaryEvents.push(e);effects.ingest(s,10);assert.equal(effects.active.length,1);
  effects.ingest(s,20);assert.equal(effects.active.length,1,'do not replay an event');
  const texts=[],c={save(){},restore(){},scale(){},strokeText(){},fillText(label,x,y){texts.push({label,y,alpha:this.globalAlpha});}};
  effects.drawLosses(c,e,.2,1,false,0);effects.drawLosses(c,e,.8,1,false,0);
  assert.equal(texts[0].label,'You −19');assert.match(texts[1].label,/−32$/);
  assert.ok(texts[2].y<texts[0].y);assert.ok(texts[2].alpha<texts[0].alpha);
  texts.length=0;effects.drawLosses(c,e,.2,.3,true,0);effects.drawLosses(c,e,.8,.3,true,0);
  assert.equal(texts[0].y,texts[2].y);assert.equal(effects.animating(50,true),true);
  effects.ingest(s,2100);assert.equal(effects.active.length,0);assert.equal(effects.animating(2100,true),false);
});
