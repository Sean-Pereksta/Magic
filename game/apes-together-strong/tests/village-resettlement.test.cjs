'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');

function quiet(g){
 g.world.ensure=()=>{};g.world.stream=()=>{};g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',water:false});g.world.blocked=()=>false;g.world.waterBlocked=()=>false;
 g.world.syncSettlementBuildings=()=>{};g.colonies.plan=()=>{};g.colonies.work=()=>{};
}
function village(n=4){
 const c=loadEngine(),g=new c.ATSGame('RETURN-TO-OUR-VILLAGE');g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();quiet(g);
 for(let i=0;i<n;i++)g.makeApe(20+i*5,0,'follow');g.food=200;assert.equal(g.command('settleAll'),true);const s=g.settlements[0];
 s.huts.push({id:s.id+'-preserved-home',kind:'hut',x:180,y:0,hp:73,maxHp:100,capacity:10,stage:4,progress:1});
 s.facilities.push({id:s.id+'-nursery',kind:'nursery',x:0,y:180,hp:87,maxHp:100,stage:4,progress:1});g.colonies.init(s);
 s.food=90;s.wood=80;const result=g.colonies.commission(s.id,'hut');assert.equal(result.ok,true);const project=s.projects.find(p=>p.id===result.projectId);project.work=7;
 s.birthTimer=29.8;s.age=123;g.refreshSettlements();return{c,g,s,project};
}
function evacuate(g,s){assert.equal(g.command('recallAll'),true);assert.equal(s.population,0);assert.equal(g.settlementMembers.get(s.id).length,0);g.commandCD=0}
function step(g,s,n=1){for(let i=0;i<n;i++){g.time++;g.refreshSettlements();g.colonies.tick(s)}}
function preserved(s){return JSON.stringify({id:s.id,name:s.name,level:s.level,age:s.age,housing:s.housing,birthTimer:s.birthTimer,wood:s.wood,huts:s.huts,facilities:s.facilities,projects:s.projects,commissions:s.commissions})}

test('Z and Shift Z restore an evacuated village instead of replacing its development with a new camp',()=>{
 for(const command of ['settle','settleAll']){
  const {g,s}=village(),child=g.makeApe(40,30,'young',s.id,true);child.age=12;g.refreshSettlements();const childHp=child.hp,stats=g.stats.settlements;
  evacuate(g,s);const before=preserved(s),food=s.food,births=g.stats.born;step(g,s,3);assert.equal(preserved(s),before);assert.equal(g.stats.born,births,'an empty village cannot produce children');
  assert.equal(g.command(command),true);assert.equal(g.settlements.length,1);assert.equal(g.stats.settlements,stats);assert.equal(g.settlements[0],s);assert.equal(preserved(s),before);assert.equal(s.food,food+12);
  assert.equal(s.population,5);assert.equal(s.children,1);for(const a of g.apes){assert.equal(a.settlementId,s.id);assert.equal(a.homeX,s.x);assert.equal(a.homeY,s.y);assert.equal(a.recallOrder,undefined)}assert.equal(child.state,'young');assert.equal(child.age,12);assert.equal(child.hp,childHp);
  step(g,s);assert.equal(g.stats.born,births+1);assert.equal(s.population,6);assert.ok(s.growthRate>0);assert.ok(s.birthTimer<1);assert.ok(s.food<food+12);assert.equal(s.huts[0].hp,73);
 }
});

test('an evacuated village keeps its identity and family progress through saving before the return',()=>{
 const {c,g,s}=village();evacuate(g,s);const before=preserved(s),save=JSON.parse(JSON.stringify(g.serialize())),loaded=c.ATSGame.fromJSON(save),home=loaded.settlement(s.id);quiet(loaded);
 assert.equal(home.population,0);assert.equal(preserved(home),before);assert.equal(loaded.command('settleAll'),true);assert.equal(loaded.settlements.length,1);assert.equal(home.population,4);assert.equal(home.birthTimer,29.8);assert.equal(home.huts[0].hp,73);assert.equal(home.projects[0].work,7);step(loaded,home);assert.equal(loaded.stats.born,1);assert.equal(home.population,5);
});

test('returning followers choose the nearest old village even when a farther overlapping village is occupied',()=>{
 const {g,s}=village();evacuate(g,s);const farther={id:'older-overlapping-camp',name:'Other camp',x:180,y:0,population:1,food:40,livingFounding:true};g.settlements.unshift(farther);const resident=g.makeApe(180,0,'settled',farther.id);g.colonies.init(farther);g.refreshSettlements();const count=g.settlements.length,stats=g.stats.settlements;
 assert.equal(g.command('settleAll'),true);assert.equal(g.settlements.length,count);assert.equal(g.stats.settlements,stats);assert.equal(s.population,4);assert.equal(farther.population,1);assert.equal(resident.settlementId,farther.id);assert.ok(g.apes.filter(a=>a!==resident).every(a=>a.settlementId===s.id));
});

test('resettling recalled families leaves wild, foreign-owned, captive, uncalled and assigned young alone',()=>{
 const {g,s}=village();evacuate(g,s);const returning=g.makeApe(25,20,'young',null,true),wild=g.makeApe(30,20,'young',null,true),other=g.makeApe(35,20,'young',null,true),assigned=g.makeApe(40,20,'young',s.id,true),uncalled=g.makeApe(45,20,'young',null,true),captive=g.makeApe(50,20,'young',null,true);returning.recallOrder={kind:'all'};wild.hordeOwner=null;wild.recallOrder={kind:'all'};other.hordeOwner='other';other.recallOrder={kind:'all'};assigned.recallOrder={kind:'all'};captive.recallOrder={kind:'all'};captive.captive=true;g.refreshSettlements();
 assert.equal(g.command('settleAll'),true);assert.equal(returning.settlementId,s.id);assert.equal(returning.state,'young');assert.equal(returning.recallOrder,undefined);for(const a of [wild,other,uncalled,captive])assert.equal(a.settlementId,null);assert.equal(captive.captive,true);assert.equal(assigned.recallOrder.kind,'all');assert.equal(assigned.settlementId,s.id);
});

test('resumed families still require food and actual empty housing without repairing destroyed homes',()=>{
 const {g,s}=village(6);g.colonies.damageHut(s,s.huts[0],100);assert.equal(s.housing,6);s.food=0;s.birthTimer=30;g.food=0;evacuate(g,s);assert.equal(g.command('settleAll'),true);step(g,s,3);assert.equal(g.stats.born,0);assert.equal(s.housing,6);assert.equal(s.huts[0].hp,0);assert.equal(s.growthStatus,'Homes full');
 const adult=g.apes[0];adult.settlementId=null;adult.state='follow';g.refreshSettlements();step(g,s);assert.equal(g.stats.born,0);assert.equal(s.growthStatus,'Growth paused · food shortage');g.food=30;assert.equal(g.colonies.action(s.id,'supply'),true);step(g,s);assert.equal(g.stats.born,1);assert.equal(s.population,s.housing);assert.equal(s.huts[0].hp,0);
});

test('resettling an empty village does not bypass the kingdom population cap',()=>{
 const {c,g,s}=village();evacuate(g,s);for(let i=g.population;i<c.MAX_APE_POPULATION;i++)g.makeApe(2000+i*5,0,'follow');assert.equal(g.command('settle'),true);assert.equal(s.population,4);s.birthTimer=30;step(g,s,3);assert.equal(g.stats.born,0);assert.equal(g.population,c.MAX_APE_POPULATION);assert.equal(s.growthStatus,'Kingdom population full');
});
