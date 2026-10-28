import test from 'node:test';
import assert from 'node:assert/strict';
import {createExpansion} from '../engine.mjs';
import {MISSIONS} from '../campaign.mjs';
import {campaignSupport} from '../operations.mjs';

function setup(){
  let id=1,units=new Map(),nearby=false,workers=false,result,base=true,escorts=[];
  const adapter={begin(){units=new Map();base=true;},createObjective(o){if(['destroy','protect','escort'].includes(o.type)){const e={id:id++,x:o.x,y:o.y};units.set(e.id,e);return e.id;}return null;},entity:id=>units.get(id),hasBase:()=>base,hasForces:()=>base,nearby:(x,y,r,f)=>f===0&&nearby?[{id:99}]:[],workersNearby:()=>workers?[{id:100}]:[],escort(e,to){escorts.push(e.id);},stopEscort(){},snapshot:()=>({units:[...units]}),restore(s){units=new Map(s.units);},spawn(type){const e={id:id++,type};units.set(e.id,e);return e.id;},army:()=>({}),getIdentity:()=>({username:'Tester'}),getFaction:()=> 'frontier',finish(r){result=r;},stats:()=>({unitsLost:2}),paused:()=>false,getFactions:()=>[],getUnits:()=>[],getMaps:()=>[]};
  const engine=createExpansion(adapter,{storage:{getItem:()=>null,setItem(){},removeItem(){}},emit(){}});
  return {engine,adapter,get units(){return units;},get result(){return result;},setNear(v){nearby=v;},setWorkers(v){workers=v;},loseBase(){base=false;},get escorts(){return escorts;}};
}

test('every authored campaign completes from its own battlefield objectives, with staged restore and optional-task rewards',()=>{
  for(const mission of MISSIONS){
    const h=setup();h.engine.startCampaign(mission.id,{seed:190,difficulty:'hard'});
    const initial=h.engine.getState();assert.equal(initial.operation.stage,0,mission.id);assert.equal(h.engine.nextWave(),null);
    // Time and enemy-wave clears alone must never complete the tactical campaign.
    for(let n=0;n<12;n++)h.engine.tick(1);assert.equal(h.result,undefined,mission.id);
    h.setNear(true);h.setWorkers(true);
    let restored=false;
    for(let n=0;n<900&&!h.result;n++){
      const state=h.engine.getState();
      for(const o of state.objectives){
        if(o.type==='destroy'&&o.stage===state.operation.stage)h.units.delete(o.entityId);
        if(o.type==='escort'&&o.stage===state.operation.stage){const e=h.units.get(o.entityId);if(e){e.x=o.to.x;e.y=o.to.y;}}
      }
      for(const id of state.activeIds)h.units.delete(id);
      h.engine.tick(1);
      if(!restored&&h.engine.getState().operation.stage>0&&!h.result){
        const saved=h.engine.checkpoint(),stage=saved.operation.stage;h.engine.abandon();h.engine.resume(saved);
        assert.equal(h.engine.getState().operation.stage,stage,mission.id+' restored stage');restored=true;
      }
    }
    assert.equal(h.result?.won,true,mission.id+' should finish by its tactical stages');
    assert.equal(h.result.completedStages.length,mission.operation.stages.length,mission.id);
    assert.equal(h.result.campaignRewards[0].id,mission.operation.reward,mission.id);
    assert.ok(h.result.completedObjectives.length>0,mission.id);
  }
});

test('worker-operated devices require workers and keep contested/absent progress stable',()=>{
  const h=setup();h.engine.startCampaign('sundered-coast-1');h.setNear(true);
  for(let n=0;n<60;n++)h.engine.tick(1);
  assert.equal(h.engine.getState().objectives.find(o=>o.id==='beacon-repair').progress,0);
  h.setWorkers(true);for(let n=0;n<46;n++)h.engine.tick(1);
  assert.ok(h.engine.getState().objectives.find(o=>o.id==='beacon-repair').completed);
});

test('protect failures end tactical runs once and progression cannot reward defeat',()=>{
  const h=setup();h.engine.startCampaign('amber-wastes-4');
  const target=h.engine.getState().objectives.find(o=>o.id==='refinery');h.units.delete(target.entityId);h.engine.tick(1);
  assert.equal(h.result.won,false);assert.deepEqual(h.result.campaignRewards,[]);
  assert.equal(h.engine.finish(true),null);
});

test('optional campaign support is bounded and derived from saved objectives',()=>{
  const support=campaignSupport({campaign:{completedMissions:{one:{optionalObjectives:['miners',...Array.from({length:100},(_,i)=>'bonus'+i)]}}}},3);
  assert.deepEqual(support,{minerals:180,gas:60,repairCrew:true});
  assert.deepEqual(campaignSupport({},0),{minerals:0,gas:0,repairCrew:false});
});

test('the optional final evacuation group grants a survivor reward and its loss never fails the required evacuation',()=>{
  for(const saveFinal of [false,true]){
    const h=setup();h.engine.startCampaign('sundered-coast-4');
    assert.equal(h.engine.getState().objectives.find(o=>o.id==='evacuation-three').entityId,null,'Optional convoy waits for its departure stage');
    const deliver=id=>{const o=h.engine.getState().objectives.find(o=>o.id===id),e=h.units.get(o.entityId);e.x=o.to.x;e.y=o.to.y;};
    deliver('evacuation');h.engine.tick(1);
    const optional=h.engine.getState().objectives.find(o=>o.id==='evacuation-three');assert.ok(optional.entityId);
    if(saveFinal)deliver(optional.id);else h.units.delete(optional.entityId);
    h.engine.tick(1);assert.equal(h.result,undefined);
    deliver('evacuation-two');h.engine.tick(1);
    for(let n=0;n<80&&!h.result;n++){for(const id of h.engine.getState().activeIds)h.units.delete(id);h.engine.tick(1);}
    assert.equal(h.result.won,true);assert.equal(h.result.evacuatedGroups,saveFinal?3:2);assert.equal(h.result.optionalObjectives.includes(optional.id),saveFinal);assert.ok(h.result.campaignRewards.some(r=>r.id==='evacuation-survivors'));
  }
});
