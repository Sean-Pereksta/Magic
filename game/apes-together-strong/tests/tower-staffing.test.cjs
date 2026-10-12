'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function setup(n=8){
 const c=loadEngine(),g=new c.ATSGame('TOWER-CREW'),s={id:'v',x:0,y:0,population:n,food:500,wood:500};
 g.settlements.push(s);for(let i=0;i<n;i++)g.makeApe(i*20,0,'settled',s.id);
 g.refreshSettlements();g.colonies.init(s);
 s.facilities=['spearTower','spearBattery','spearBallista'].map((kind,i)=>({id:'t'+i,kind,x:i*100,y:0,hp:200,stage:4}));
 for(const a of g.apes)a.job='workshop';
 return {g,s};
}
test('attack immediately crews each defense once, without taking the remaining residents',()=>{
 const {g,s}=setup();g.colonies.staffFacilities(s);assert.ok(s.facilities.every(t=>!t.staffedBy));
 s.attack=true;g.colonies.staffFacilities(s);
 assert.equal(new Set(s.facilities.map(t=>t.staffedBy)).size,3);
 assert.equal(g.apes.filter(a=>a.towerId).length,3);
 const ids=s.facilities.map(t=>t.staffedBy);g.time+=1;g.colonies.staffFacilities(s);
 assert.deepEqual(s.facilities.map(t=>t.staffedBy),ids);
 const a=g.apes.find(a=>a.towerId);a.garrisonPost={role:'gate',x:900,y:900};
 const goal=g.colonies.activityTarget(a,s);assert.equal(goal.x,g.colonies.structure(s,a.towerId).station.x);
});
test('scarce crew never duplicate and unavailable or manually ordered apes stay free',()=>{
 const {g,s}=setup(5);s.attack=true;
 g.apes[0].armyOrder={};g.apes[1].divisionId='squad';g.apes[2].state='young';g.apes[3].kingdomMission={};
 g.colonies.staffFacilities(s);
 assert.equal(s.facilities.filter(t=>t.staffedBy).length,1);assert.equal(g.apes.filter(a=>a.towerId).length,1);
 const crew=g.apes[4];crew.hp=0;g.apes[0].armyOrder=null;g.time+=1;g.colonies.staffFacilities(s);
 assert.equal(s.facilities.filter(t=>t.staffedBy).length,1);assert.equal(g.apes[0].towerId,'t0');assert.equal(crew.towerId,undefined);
 s.facilities[0].hp=0;g.time+=1;g.colonies.staffFacilities(s);
 assert.equal(s.facilities[0].staffedBy,null);assert.equal(g.apes[0].towerId,'t1');
});
