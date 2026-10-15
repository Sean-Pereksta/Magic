const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
test('automatic nearest, personality and area attacks bypass civilian town housing',()=>{
 const c=loadEngine(),g=new c.ATSGame('TOWN-TARGETS');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};
 g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;
 g.king.x=-400;const a=g.makeApe(0,0,'charge');a.species='gorilla';
 const add=(id,type,x,extra={})=>{const o={id,type,x,y:0,r:10,hp:200,maxHp:200,solid:true,discovered:true,...extra};g.world.objects.set(id,o);g.world._indexObject(o);return o;};
 const house=add('civilian-house','house',30,{civilian:true,combatTarget:false}),radio=add('military-radio','radio',95);
 g.syncIndexes();g.apeGrid.rebuild(g.apes,g.king);g.humanGrid.rebuild([]);g.vehicleGrid.rebuild([]);g.navigation.beginFrame(g.time);g.performance.beginStep(g);
 assert.equal(g.nearestTargetFor(a).id,radio.id);
 assert.equal(g.tactics.pick(a,150).id,radio.id);
 a.armyOrder={areaArrived:true};g.siege.attackArea(a,{x:60,y:0},1/60);assert.equal(a.areaTarget.id,radio.id);
 assert.equal(house.hp,house.maxHp);
});
