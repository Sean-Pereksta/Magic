import test from 'node:test';
import assert from 'node:assert/strict';
import { UNITS } from '../data.mjs';
import { emptyUnits } from '../economy.mjs';
import { resolveFieldBattle, troopTotal } from '../warfare.mjs';

const army=(units,owner='ashen',formation='balanced')=>({units:{...emptyUnits(),...units},owner,formation,morale:1,retreats:0});
const ground=(terrain='plains')=>({terrain,owner:'wintermere',walls:0,building:null,levels:{}});

test('sizeable levy armies suffer meaningful losses and a defeated line loses readiness',()=>{
  const a=army({levy:200}),d=army({levy:200},'wintermere');
  const report=resolveFieldBattle(a,d,ground());
  for(const x of [a,d]){
    assert.ok(troopTotal(x)<=150,'at least one quarter lost in an open clash');
    assert.ok(troopTotal(x)>=120,'most levies survive to withdraw');
  }
  assert.ok([a,d][report.loser].morale<.55,'loser reaches the existing AI regroup threshold');
});

test('repeated levy clashes break a sizeable force within four engagements',()=>{
  const a=army({levy:200}),d=army({levy:200},'wintermere');
  let clashes=0;
  while(Math.min(troopTotal(a),troopTotal(d))>60&&clashes<5){
    resolveFieldBattle(a,d,ground());clashes++;
    for(const x of [a,d])x.morale=Math.min(1,x.morale+.04);
  }
  assert.ok(clashes<=4);
  assert.ok(Math.min(troopTotal(a),troopTotal(d))>0);
});

test('overwhelming forces cannot erase a large army in one field battle, even with exposure and pursuit',()=>{
  for(const id of Object.keys(UNITS))for(const formation of ['balanced','charge','spearWall'])for(const roll of [0,.5,1]){
    const a=army({knight:1000,lightCavalry:1000,crossbow:1000});
    const d=army({[id]:200},'wintermere',formation);d.morale=.4;
    const report=resolveFieldBattle(a,d,ground(),{roll:()=>roll});
    assert.ok(troopTotal(d)>=80,`${id}/${formation}/${roll}: at least 40% survive`);
    const recorded=report.phases.reduce((n,p)=>n+p.loss[1],0);
    assert.equal(recorded,200-troopTotal(d));
    assert.equal(Object.values(report.casualties[1]).reduce((n,v)=>n+v,0),recorded);
    for(const x of [a,d])for(const n of Object.values(x.units))assert.ok(Number.isInteger(n)&&n>=0);
  }
});

test('terrain and defensive formation still reduce losses at the faster pace',()=>{
  const fight=(terrain,formation)=>{
    const a=army({levy:120,archer:50,cavalry:30}),d=army({levy:200},'wintermere',formation);
    resolveFieldBattle(a,d,ground(terrain));return troopTotal(d);
  };
  assert.ok(fight('hills','defensive')>fight('plains','balanced'));
});
