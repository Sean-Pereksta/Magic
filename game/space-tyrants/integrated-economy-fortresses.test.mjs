import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {createSimulation} from './simulation-harness.mjs';

function lab(owner=0){
  const h=createSimulation();
  h.run(`state.ships=[];state.wars=[];state.deepSpaceBases=[];state.deepSpaceOperations=[];stxDSPersist();
    state.empires.forEach(e=>{e.credits=5000;e.stxPriorityProjectId=null;e.stxNextEconomyPlanAt=1e9;});
    state.p=state.planets.find(p=>p.owner===${owner});state.donor=state.planets.find(p=>p!==state.p&&p.owner===null);state.donor.owner=${owner};
    for(const p of state.planets){p.localProject=p.orbitalProject=p.expansionProject=p.reconstruction=p.scanProject=p.tradeStationProject=null;p.buildQueue=[];p.orders=[];p.underAttack=false;}
    for(const p of [state.p,state.donor]){Object.assign(p,{pop:1,capacity:3,unrest:0,infra:{mine:2,factory:2,training:0,shipyard:0,research:0,city:1,defense:1},stxEconomicFocus:'balanced',stxFreightCapacity:12});for(const r of [...RESOURCES,'components','equipment','trained'])p.stock[r]=r==='trained'?0:1000;}
    state.donor.x=state.p.x+200;state.donor.y=state.p.y;stxIFReset();`);
  return h;
}
function project(h,resource='iron',amount=10){
  h.run(`state.p.localProject={type:'city',cost:10,progress:0,startedAt:state.simTime,stxRecipeNeed:{${resource}:${amount}}};state.d=stxSDDescriptors(state.p).find(d=>d.kind==='local');`);
}
function prioritize(h,age=90){h.run(`empire(state.p.owner).stxPriorityProjectId=state.d.id;empire(state.p.owner).stxPriorityPlanetId=state.p.id;empire(state.p.owner).stxPrioritySetAt=state.simTime-${age};stxIFLastPlan=-999;stxIFPlan()`)}
function station(h,type='military',owner=0){
  h.run(`state.p.infra.shipyard=2;state.base=stxDSEnsureBase({id:'test-base',owner:${owner},sponsorPlanetId:state.p.id,type:'${type}',tier:1,status:'operational',x:state.p.x+600,y:state.p.y,name:'Test Station',supplyReadiness:1,stxSupplyStock:{helium:100,equipment:100,components:100}});state.deepSpaceBases.push(state.base);stxDSPersist()`);
}
function approx(a,b,eps=1e-8){assert.ok(Math.abs(a-b)<eps,`${a} != ${b}`)}

test('runtime loads the economy once, after dependencies and before inspector stabilization',()=>{
  const source=readFileSync(new URL('./integrated-economy-fortresses.js',import.meta.url),'utf8'),loader=readFileSync(new URL('../space-tyrants.html',import.meta.url),'utf8');
  assert.doesNotThrow(()=>new Function(source));const at=loader.indexOf('integrated-economy-fortresses.js');
  assert.ok(at>loader.indexOf('strategic-policies.js'));assert.ok(at<loader.indexOf('inspector-scroll-stability.js'));assert.equal(loader.match(/integrated-economy-fortresses.js/g).length,1);
});
test('priority dispatch moves existing stock into physical freight and delivers to the intended world',()=>{
  const h=lab();project(h);h.run('state.p.stock.iron=0;state.donor.stock.iron=10');prioritize(h);
  h.run('stxSDEnsureOrders(state.d);fillOrder(state.p,state.p.orders.find(o=>o.id===state.d.q.stxSupply.orderIds.iron))');
  assert.equal(h.run('state.p.stock.iron'),0);assert.equal(h.run('state.donor.stock.iron'),0);assert.equal(h.run('state.ships[0].cargo.iron'),10);
  h.run('tickShips(30);stxSDAllocatePlanet(state.p)');assert.equal(h.run('state.d.q.stxSupply.delivered.iron'),10);
});
test('destroyed freight reopens demand and is never credited as delivered',()=>{
  const h=lab();project(h);h.run('state.p.stock.iron=0;state.donor.stock.iron=30');prioritize(h);
  h.run('stxIFRoutingTick();state.ships=[];stxSDEnsureOrders(state.d)');assert.equal(h.run('stxIFNeed(state.d,"iron")'),10);
  h.run('fillOrder(state.p,state.p.orders.find(o=>o.id===state.d.q.stxSupply.orderIds.iron))');assert.equal(h.run('state.ships[0].cargo.iron'),10);assert.equal(h.run('state.d.q.stxSupply.delivered.iron'),0);
});
test('tiny material and crew tails retain orders and finish through physical delivery',()=>{
  for(const [r,n] of [['iron',.01],['trained',.000001]]){const h=lab();project(h,r,1);h.run(`state.d.q.stxSupply.delivered.${r}=1-${n};state.p.stock.${r}=0;state.donor.stock.${r}=${n}`);prioritize(h);
    h.run('stxIFRoutingTick();tickShips(30);stxSDAllocatePlanet(state.p)');assert.equal(h.run('stxSDAllDelivered(state.d)'),true);approx(h.run(`state.donor.stock.${r}`),0);
  }
});
test('all five priority stages escalate in simulation time and reset for a replacement',()=>{
  const h=lab();project(h);h.run('state.p.stock.iron=0');
  for(const [age,stage] of [[0,0],[12,1],[30,2],[50,3],[80,4]]){prioritize(h,age);assert.equal(h.run('stxIFPlans.get(0).stage'),stage)}
});
test('normal recruitment replenishes personnel without consuming civilian population or Equipment',()=>{
  const h=lab();h.run('state.p.infra.training=2;stxLPEnsure(state.p);state.p.factoryModes=["components","components"];state.pop=state.p.pop;state.gear=state.p.stock.equipment;stxLPPassiveIndustry(state.p,10)');assert.ok(h.run('state.p.stock.trained')>0);approx(h.run('state.p.pop'),h.run('state.pop'));approx(h.run('state.p.stock.equipment'),h.run('state.gear'));
});
test('passive recruitment works with no Equipment and creates no upkeep order',()=>{
  const h=lab();h.run('state.p.stock.equipment=0;state.p.infra.training=2;stxLPEnsure(state.p);stxLPPassiveIndustry(state.p,10)');assert.ok(h.run('state.p.stock.trained')>0);assert.equal(h.run('state.p.orders.some(o=>o.type==="equipment")'),false);
});
test('priority crew shortages resolve gradually through normal recruitment',()=>{
  const h=lab();project(h,'trained',.001);prioritize(h);h.run('for(let i=0;i<200;i++){stxLPEnsure(state.p);stxLPPassiveIndustry(state.p,1);stxSDAllocatePlanet(state.p)}');assert.equal(h.run('stxSDAllDelivered(state.d)'),true);approx(h.run('state.p.pop'),1);
});
test('EXPORT releases deeper reserves; LOCAL retains stock across domestic order paths',()=>{
  for(const [mode,expect] of [['balanced',false],['project',false],['export',true]]){const h=lab();project(h);h.run(`state.p.stock.iron=0;owned(0).forEach(p=>{if(p!==state.donor)p.stock.iron=0});state.donor.stock.iron=20;state.donor.stxResourceRouting.iron='${mode}';stxSDEnsureOrders(state.d);fillOrder(state.p,state.p.orders.find(o=>o.id===state.d.q.stxSupply.orderIds.iron))`);assert.equal(h.run('state.ships.length>0'),expect)}
});
test('local project commitments cannot be exported even under emergency priority',()=>{
  const h=lab();h.run('state.donor.localProject={type:"city",cost:10,progress:0,stxRecipeNeed:{iron:10}};state.donor.stock.iron=10;stxSDAllocatePlanet(state.donor)');project(h);h.run('state.p.stock.iron=0');prioritize(h);h.run('stxIFRoutingTick()');assert.equal(h.run('stxSDDescriptors(state.donor)[0].q.stxSupply.delivered.iron'),10);assert.equal(h.run('state.ships.some(s=>s.from===state.donor.id&&s.cargo.iron>0)'),false);
});
test('nearby suppliers are preferred to a distant stockpile',()=>{
  const h=lab();project(h);h.run('state.far=state.planets.find(p=>p.owner===null);state.far.owner=0;state.far.stock.iron=5000;state.far.x=state.p.x+2500;state.p.stock.iron=0');prioritize(h);h.run('stxIFRoutingTick()');assert.equal(h.run('state.ships.find(s=>s.cargo.iron>0).from'),h.run('state.donor.id'));
});
test('expansion costs capital, multiple materials, trained crew and productive capacity',()=>{
  const h=lab();const credits=h.run('empire(0).credits');h.run('state.target=state.planets.find(p=>p.owner===null);startExpansionProject(empire(0),state.p,state.target,true)');assert.ok(h.run('empire(0).credits')<credits);assert.ok(h.run('stxIFExpansionDescriptor(state.p).need.trained')>0);assert.equal(h.run('Object.keys(stxIFExpansionDescriptor(state.p).need).length'),7);
  assert.ok(h.run('stxIFMobilization(state.p).factory')<.8);assert.ok(h.run('stxIFMobilization(state.p).mine')<1);h.run('state.p.expansionProject=null');assert.equal(h.run('stxIFMobilization(state.p).factory'),1);
});
test('expansion actually reduces measured factory output and restores it afterward',()=>{
  const h=lab();h.run('tickPlanet(state.p,1)');const baseline=h.run('state.p.stxAllocationOutput.componentRate');h.run('startExpansionProject(empire(0),state.p,state.planets.find(p=>p.owner===null),true);tickPlanet(state.p,1)');assert.ok(h.run('state.p.stxAllocationOutput.componentRate')<baseline*.8);h.run('state.p.expansionProject=null;tickPlanet(state.p,1)');assert.ok(h.run('state.p.stxAllocationOutput.componentRate')>baseline*.95);
});
test('colony launch never consumes settlers when the technical ship bound is full',()=>{
  const h=lab();h.run('state.target=state.planets.find(p=>p.owner===null);startExpansionProject(empire(0),state.p,state.target,true);state.p.stock.trained=1;stxSDAllocatePlanet(state.p);state.p.expansionProject.volunteers=state.p.expansionProject.goal;state.ships=Array.from({length:280},(_,i)=>({id:`limit${i}`}));tickExpansionProject(state.p,1)');assert.equal(h.run('state.p.pop'),1);assert.ok(h.run('state.p.expansionProject'));
});
test('new colony keeps exactly transported population and develops without upkeep',()=>{
  const h=lab();h.run('state.target=state.planets.find(p=>p.owner===null);state.target.x=state.p.x+150;startExpansionProject(empire(0),state.p,state.target,true);state.p.stock.trained=1;stxSDAllocatePlanet(state.p);state.p.expansionProject.volunteers=state.p.expansionProject.goal;tickExpansionProject(state.p,1);state.settlers=state.ships.find(s=>s.type==="colony").cargo.population;tickShips(30)');approx(h.run('state.target.pop'),h.run('state.settlers'));approx(h.run('state.p.pop+state.target.pop'),1);assert.equal(h.run('state.target.stock.components'),0);assert.equal(h.run('state.target.stxColony.stage'),'landing');
  h.run('stxIFColonyTick(state.target,20)');assert.equal(h.run('state.target.orders.some(o=>o.type==="colony-support")'),false);assert.equal(h.run('state.target.stxColony.development'),20);
});
test('supplying a colony advances all development stages; isolation slows rather than deletes it',()=>{
  const h=lab();h.run('state.p.stxColony={stage:"landing",age:0,development:0,readiness:1,isolatedFor:0};stxIFColonyTick(state.p,70)');assert.equal(h.run('state.p.stxColony.stage'),'young colony');h.run('stxIFColonyTick(state.p,120)');assert.equal(h.run('state.p.stxColony.stage'),'established');h.run('stxIFColonyTick(state.p,180)');assert.equal(h.run('state.p.stxColony.stage'),'developed');
});
test('shared industrial capacity slows parallel projects and favors the designated priority',()=>{
  const h=lab();project(h);const single=h.run('stxIFCapacityShare(state.d)');h.run('state.p.orbitalProject={type:"station",need:{iron:10},progress:0};state.p.scanProject={need:{iron:10},progress:0};state.p.stock.iron=0');const parallel=h.run('stxIFCapacityShare(state.d)');assert.ok(parallel<single);prioritize(h);assert.ok(h.run('stxIFCapacityShare(stxSDDescriptors(state.p).find(d=>d.kind==="local"))')>h.run('stxIFCapacityShare(stxSDDescriptors(state.p).find(d=>d.kind==="orbital"))'));
});
test('infrastructure completed during a mobilized tick survives temporary production scaling',()=>{
  const h=lab();h.run('state.p.localProject={type:"factory",cost:1,progress:.9999,stxRecipeNeed:{iron:1},stxSupply:{delivered:{iron:1},orderIds:{}}};startExpansionProject(empire(0),state.p,state.planets.find(p=>p.owner===null),true);tickPlanet(state.p,1)');assert.equal(h.run('state.p.infra.factory'),3);
});
test('installed factories produce with empty input stocks without recurring costs',()=>{
  const h=lab();h.run('state.p.infra.mine=0;for(const r of RESOURCES)state.p.stock[r]=0;state.p.stock.components=0;tickPlanet(state.p,1)');assert.ok(h.run('state.p.stock.components')>h.run('stxActionLocalRate(state.p,"components")'));for(const r of ['iron','silicates','rare'])approx(h.run(`state.p.stock.${r}`),h.run(`stxActionLocalRate(state.p,'${r}')`));
});
test('station upgrade remains tier one until physical deliveries and assembly finish',()=>{
  const h=lab();station(h);h.run('stxIFStartUpgrade(state.base.id);stxIFSourceUpgrade(state.base)');assert.equal(h.run('state.base.tier'),1);assert.equal(h.run('stxIFUpgradeRatio(state.base.upgradeProject)'),0);assert.ok(h.run('state.ships.some(s=>s.stxDeepMission==="fortress-upgrade")'));
  h.run('state.base.upgradeProject.progress=1;stxIFCompleteUpgrade(state.base)');assert.equal(h.run('state.base.tier'),1);
  h.run('for(let i=0;i<260;i++){state.simTime+=1;tickShips(1);stxIFUpgradeTick(1)}');assert.equal(h.run('state.base.tier'),2);assert.equal(h.run('state.base.upgradeProject'),undefined);
});
test('destroyed station cargo reopens sponsor demand without duplicating staged stock',()=>{
  const h=lab();station(h);h.run('stxIFStartUpgrade(state.base.id);stxIFSourceUpgrade(state.base);state.lost=state.ships.find(s=>s.stxDeepMission==="fortress-upgrade");state.r=Object.keys(state.lost.cargo)[0];state.amount=state.lost.cargo[state.r];state.ships=state.ships.filter(s=>s!==state.lost);state.remote=stxIFRemoteDescriptor(state.base,state.p)');approx(h.run('stxSDRemaining(state.remote,state.r)'),h.run('state.amount'));
});
test('station staging yields to a local Imperial priority needing the same material',()=>{
  const h=lab();station(h);h.run('stxIFStartUpgrade(state.base.id);state.p.stock.iron=10');project(h);prioritize(h);h.run('stxIFSourceUpgrade(state.base)');assert.equal(h.run('state.d.q.stxSupply.delivered.iron'),10);assert.equal(h.run('state.base.upgradeProject.staged.iron||0'),0);
});
test('the player can prioritize a station upgrade through the normal project control',()=>{
  const h=lab();station(h);h.run('stxIFStartUpgrade(state.base.id);state.remote=stxIFRemoteDescriptor(state.base,state.p);stxPSSetPriority(state.remote.id,state.p.id)');assert.equal(h.run('stxIFPriorityProject().kind'),'deep-upgrade');
});
test('tiers strengthen launch and service capacity without player upkeep',()=>{
  const h=lab();station(h);const levels=[];for(const tier of [1,2,3]){h.run(`state.base.tier=${tier}`);levels.push(h.run('({speed:stxDSLaunchBonus(state.base).speed,capacity:stxIFServiceCapacity(state.base),cost:Object.keys(stxIFUpkeep(state.base)).length})'))}for(let i=1;i<3;i++){assert.ok(levels[i].speed>levels[i-1].speed);assert.ok(levels[i].capacity>levels[i-1].capacity)}assert.ok(levels.every(x=>x.cost===0));
});
test('player station readiness is maintained without reserve upkeep shipments',()=>{
  const h=lab();station(h);h.run('state.base.stxSupplyStock={};empire(0).credits=0;for(let i=0;i<100;i++)stxDSTickBases(1)');assert.ok(h.run('state.base.supplyReadiness')>.99);assert.equal(h.run('state.ships.length'),0);
});
test('station maintenance preserves stored helium Equipment and imperial Credits',()=>{
  const h=lab();station(h);h.run('state.before=JSON.stringify({stock:state.base.stxSupplyStock,credits:empire(0).credits});stxDSTickBases(10)');assert.equal(h.run('JSON.stringify({stock:state.base.stxSupplyStock,credits:empire(0).credits})'),h.run('state.before'));
});
test('logistics stations improve freight routes and finite freight capacity',()=>{
  const h=lab();const before=h.run('stxIFFreightLimit(0)');station(h,'logistics');h.run('state.base.x=state.p.x+100');assert.ok(h.run('stxIFFreightLimit(0)')>before);assert.ok(h.run('stxIFRouteBonus(state.p,state.donor,0)')>0);
});
test('AI selects production/routing priorities and pays the same colony costs',()=>{
  const h=lab(1);project(h,'equipment',12);h.run('state.p.stock.equipment=0;empire(1).stxNextEconomyPlanAt=0;stxIFPlanOwner(empire(1))');assert.ok(h.run('empire(1).stxPriorityProjectId'));assert.equal(h.run('state.p.stxEconomicFocus'),'equipment');h.run('startExpansionProject(empire(1),state.p,state.planets.find(p=>p.owner===null),true)');assert.ok(h.run('stxIFExpansionLoad(state.p)')>0);assert.ok(h.run('stxIFMobilization(state.p).factory')<1);
});
test('AI can authorize and complete physically supplied station upgrades',()=>{
  const h=lab(1);station(h,'logistics',1);h.run('empire(1).stxNextEconomyPlanAt=0;stxIFPlanOwner(empire(1))');assert.ok(h.run('state.base.upgradeProject'));assert.ok(h.run('empire(1).credits')<5000);h.run('for(let i=0;i<260;i++){state.simTime++;tickShips(1);stxIFUpgradeTick(1)}');assert.equal(h.run('state.base.tier'),2);
});
test('save/load retains routing, station staging, upgrades, colonies and exact cargo',()=>{
  const h=lab();station(h);h.run('stxIFSetRoute(state.p.id,"iron","export");stxIFStartUpgrade(state.base.id);stxIFSourceUpgrade(state.base);state.p.stxColony={stage:"young colony",age:100,development:70,readiness:.7,isolatedFor:0};saveGame(false);state.savedId=state.p.id;state.savedCargo=JSON.stringify(state.ships.map(s=>s.cargo));loadGame();state.p=state.planets.find(p=>p.id===state.savedId);state.base=stxDSBase("test-base")');assert.equal(h.run('stxIFRoute(state.p,"iron")'),'export');assert.equal(h.run('state.base.upgradeProject.targetTier'),2);assert.equal(h.run('state.p.stxColony.stage'),'young colony');assert.equal(h.run('JSON.stringify(state.ships.map(s=>s.cargo))'),h.run('state.savedCargo'));
});
test('old saves initialize balanced routing and tier one without adding mobilization',()=>{
  const h=lab();station(h);h.run('delete state.p.stxResourceRouting;delete state.base.tier;delete state.p.stxEconomicTotals;saveGame(false);loadGame();state.p=owned(0)[0];state.base=stxDSBase("test-base")');assert.equal(h.run('stxIFRoute(state.p,"iron")'),'balanced');assert.equal(h.run('state.base.tier'),1);assert.equal(h.run('stxIFPlans.size'),0);
});
test('new game resets planner clocks after a long saved campaign',()=>{
  const h=lab();h.run('state.simTime=9000;stxIFTick();generateGalaxy();simulate(.5)');assert.equal(h.run('stxIFLastPlan'),0);assert.ok(h.run('stxIFPlans.size')>0);
});
test('reserve-release mandate never invents stock or crew',()=>{
  const h=lab();project(h,'trained',.005);h.run('owned(0).forEach(p=>p.stock.trained=0);stxSDReserveRelease("trained",{projects:[{desc:state.d}],need:.005})');assert.equal(h.run('owned(0).reduce((n,p)=>n+p.stock.trained,0)'),0);assert.equal(h.run('state.ships.reduce((n,s)=>n+(s.cargo?.trained||0),0)'),0);
});
test('station and economic inspectors render with construction and combat state',()=>{
  const h=lab();station(h);h.run('stxIFStartUpgrade(state.base.id);stxIFPlan();state.selected=state.p;renderPlanet();renderTransmissions();state.camera.zoom=1;stxDSDrawBase(state.base)');assert.match(h.elements.get('planetBody').innerHTML,/Integrated Economic Flow/);assert.match(h.elements.get('transmissionList').innerHTML,/EMPIRE ECONOMY/);assert.match(h.run('stxIFUpgradePanel(state.base)'),/PRIORITIZE UPGRADE/);
});
test('existing physical infrastructure shares the single Imperial priority and delivery ledger',()=>{
  const h=lab();h.run('state.p.physicalProjects=[{id:"old-physical",kind:"factory",name:"Factory",phase:"sourcing",progress:0,need:{iron:10},delivered:{iron:0},startedAt:0,activity:[],shipmentIds:[],option:{}}];state.p.stock.iron=0;state.donor.stock.iron=10;stxOLSetPriority("old-physical");empire(0).stxPrioritySetAt=-90;stxIFLastPlan=-999;stxIFPlan();stxOLSourceProject(state.p,state.p.physicalProjects[0]);tickShips(30);stxSDAllocatePlanet(state.p)');assert.equal(h.run('stxIFPriorityProject().kind'),'physical');assert.equal(h.run('state.p.physicalProjects[0].delivered.iron'),10);assert.equal(h.run('stxOLProjectPriority(state.p.physicalProjects[0])'),true);
});
test('legacy physical-project ships remain inbound supply after migration',()=>{
  const h=lab();h.run('state.p.physicalProjects=[{id:"old-physical",kind:"factory",name:"Factory",phase:"transit",progress:0,need:{iron:10},delivered:{iron:0},startedAt:0,activity:[],shipmentIds:[],option:{}}];state.donor.stock.iron-=10;createShip("construction",state.donor,state.p,0,{cargo:{iron:10},projectId:"old-physical",projectDelivery:true});state.d=stxSDDescriptors(state.p).find(d=>d.kind==="physical")');assert.equal(h.run('stxSDIncomingAmount(state.d,"iron")'),10);h.run('tickShips(30)');assert.equal(h.run('state.p.physicalProjects[0].delivered.iron'),10);
});
test('emergency priority reroutes domestic commercial cargo from its current position',()=>{
  const h=lab();project(h);h.run('state.p.stock.iron=0;state.donor.stock.iron-=10;state.cargoShip=createShip("freighter",state.p,state.donor,0,{cargo:{iron:10},commercial:true});state.cargoShip.x=state.p.x+80;state.cargoShip.y=state.p.y+50;state.beforeCargo=state.cargoShip.cargo.iron');prioritize(h);assert.equal(h.run('state.cargoShip.to'),h.run('state.p.id'));assert.equal(h.run('state.cargoShip.startX'),h.run('state.p.x+80'));assert.equal(h.run('state.cargoShip.cargo.iron'),10);assert.equal(h.run('state.cargoShip.stxIFRequisitioned'),true);
});
test('failed multi-leg trade returns charter fees and fuel as well as cargo',()=>{
  const h=lab();h.run('owned(1).forEach(p=>{p.stock.iron=1000;p.stock.helium=1000});state.tradeBefore=JSON.stringify({credits:state.empires.map(e=>e.credits),stock:state.planets.map(p=>p.stock)});state.realDispatcher=stxRTDispatchCargo;state.dispatchCount=0;stxRTDispatchCargo=(...args)=>++state.dispatchCount===2?null:state.realDispatcher(...args);state.tradeResult=stxRTExchange(1,0,"iron",50,{resource:"equipment",amount:20});stxRTDispatchCargo=state.realDispatcher');assert.equal(h.run('state.tradeResult'),false);assert.equal(h.run('JSON.stringify({credits:state.empires.map(e=>e.credits),stock:state.planets.map(p=>p.stock)})'),h.run('state.tradeBefore'));
});
test('emergency procurement preserves fractional crew instead of rounding to millions',()=>{
  const h=lab();h.run('state.foreign=owned(1)[0];state.foreign.stock.trained=.01;state.proposal={stxProcurement:true,from:1,projectPlanetId:state.p.id,offer:{resource:"trained",amount:.005},request:{credits:10}};state.beforeForeign=state.foreign.stock.trained;acceptTradeProposal(state.proposal)');assert.equal(h.run('state.ships.find(s=>s.tradeKind==="emergency-procurement").cargo.trained'),.005);approx(h.run('state.beforeForeign-state.foreign.stock.trained'),.005);
});
test('routine factory upgrades cannot drain a different Imperial priority',()=>{
  const h=lab();project(h,'components',10);h.run('state.p.stock.components=0;state.donor.stock.components=10;state.donor.localProject={type:"factory",cost:10,progress:0,stxRecipeNeed:{components:10}}');prioritize(h);h.run('stxSDAllocatePlanet(state.donor);stxIFRoutingTick()');assert.equal(h.run('state.donor.localProject.stxSupply.delivered.components'),0);assert.ok(h.run('state.ships.some(s=>s.to===state.p.id&&s.cargo.components===10)'));
});
test('crew priority cannot authorize an unrequested paid training camp',()=>{
  const h=lab();project(h,'trained',.005);h.run('owned(0).forEach(p=>{p.infra.training=0;p.infra.shipyard=0});state.beforeCampCredits=empire(0).credits');prioritize(h);h.run('stxIFCrewMobilization(1)');assert.equal(h.run('owned(0).some(p=>p.physicalProjects.some(q=>q.stxEmergencyTraining))'),false);assert.equal(h.run('empire(0).credits'),h.run('state.beforeCampCredits'));
});
test('a citadel requires two completed physical upgrades, never a direct tier jump',()=>{
  const h=lab();station(h);
  for(const target of [2,3]){h.run('stxIFStartUpgrade(state.base.id);for(let i=0;i<350;i++){state.simTime++;tickShips(1);stxIFUpgradeTick(1)}');assert.equal(h.run('state.base.tier'),target);assert.equal(h.run('state.base.upgradeProject'),undefined)}
  assert.equal(h.run('stxIFStartUpgrade(state.base.id)'),false);assert.equal(h.run('stxIFTierName(state.base)'),'System Citadel');
});
test('orbital projects cannot generate extra Components beyond normal factories and local recovery',()=>{
  const h=lab();h.run('state.p.infra.mine=0;for(const r of RESOURCES)state.p.stock[r]=0;state.p.stock.components=0;state.p.orbitalProject={type:"station",need:{components:10},progress:0};tickPlanet(state.p,1)');approx(h.run('state.p.stock.components'),h.run('stxActionLocalRate(state.p,"components")+state.p.stxAllocationOutput.componentRate'));assert.equal(h.run('state.p.orbitalProject.progress'),0);
});
