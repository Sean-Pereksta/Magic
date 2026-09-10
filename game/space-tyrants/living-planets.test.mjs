import assert from 'node:assert/strict';
import test from 'node:test';
import {createSimulation} from './simulation-harness.mjs';
const json=(h,s)=>JSON.parse(h.run(`JSON.stringify(${s})`));
const near=(a,b)=>assert.ok(Math.abs(a-b)<1e-8,`${a} != ${b}`);
function lab(){const h=createSimulation();h.run(`state.p=playerWorlds()[0];state.p.infra.factory=3;state.p.infra.shipyard=1;state.p.infra.training=2;state.p.pop=1;state.p.capacity=2;state.p.underAttack=false;state.p.stock.trained=.05;state.p.reserveVessels=60;empire(0).credits=10000;for(const r of RESOURCES.concat(['components','equipment']))state.p.stock[r]=1000;stxLPEnsure(state.p);state.p.factoryModes=['components','equipment','war'];state.p.stxLocalProjects=[];state.p.localProject=null;state.p.stxOrbitalProjects=[];state.p.orbitalProject=null;state.p.buildQueue=[];state.p.physicalProjects=[];state.p.orders=[];state.ships=[];state.fleets=[];`);return h}
test('two cities and a factory advance together and complete exactly once',()=>{
 const h=lab();h.run(`startLocalProject(state.p,'city');startLocalProject(state.p,'city');startLocalProject(state.p,'factory');state.cities=state.p.infra.city;state.cap=state.p.capacity;state.pop=state.p.pop;stxSDAllocatePlanet(state.p);tickLocalProject(state.p,10)`);
 assert.equal(h.run('state.p.stxLocalProjects.length'),3);assert.ok(h.run('state.p.stxLocalProjects.every(q=>q.progress>0)'));
 h.run('for(let i=0;i<600;i++){stxSDAllocatePlanet(state.p);tickLocalProject(state.p,1)}');assert.equal(h.run('state.p.stxLocalProjects.length'),0);assert.equal(h.run('state.p.infra.city'),h.run('state.cities+2'));near(h.run('state.p.capacity'),h.run('state.cap+2*state.p.stxCityCapacity'));near(h.run('state.p.pop'),h.run('state.pop'));
 h.run('tickLocalProject(state.p,1000)');assert.equal(h.run('state.p.infra.city'),h.run('state.cities+2'));
});
test('parallel projects retain independent shortages and battle pause reasons',()=>{
 const h=lab();h.run(`startLocalProject(state.p,'city');startLocalProject(state.p,'factory');for(const r of Object.keys(state.p.stock))state.p.stock[r]=0;stxSDAllocatePlanet(state.p);tickLocalProject(state.p,10)`);assert.ok(h.run('stxLPProjectMarkup(state.p).includes("Awaiting")'));
 h.run('state.p.underAttack=true;tickLocalProject(state.p,10)');assert.ok(h.run('state.p.stxLocalProjects.every(q=>q.stxAction.state==="BLOCKED")'));
});
test('independent ground and orbital projects have no shared slot',()=>{
 const h=lab();h.run('empire(0).tech.orbital=5');assert.equal(h.run('queueOrbitalProject(state.p,"station",true)'),true);assert.equal(h.run('queueOrbitalProject(state.p,"base",true)'),true);
 for(const type of ['factory','factory','shipyard','mining'])assert.equal(h.run(`stxOLQueueProject(state.p,'${type}')`),true);
 assert.equal(h.run('state.p.physicalProjects.length'),4);assert.equal(h.run('state.p.stxOrbitalProjects.length'),2);
});
test('population approaches capacity without overshoot or resource charges',()=>{
 const h=lab();h.run('state.p.pop=state.p.capacity-.00001;state.before=JSON.stringify(state.p.stock)');assert.ok(h.run('stxLPGrowPopulation(state.p,1)>state.p.pop'));assert.ok(h.run('stxLPGrowPopulation(state.p,1e8)<=state.p.capacity'));assert.equal(h.run('JSON.stringify(state.p.stock)'),h.run('state.before'));
 h.run('state.p.pop=state.p.capacity');near(h.run('stxLPGrowPopulation(state.p,100)'),h.run('state.p.capacity'));
});
test('factory conversion is free and trades production modes without adding facilities',()=>{
 const h=lab();h.run('state.credits=empire(0).credits;state.before=JSON.stringify(state.p.stock)');assert.equal(h.run('stxLPMode(state.p,0,"war")'),true);assert.equal(h.run('stxLPMode(state.p,99,"war")'),false);assert.equal(h.run('state.p.infra.factory'),3);near(h.run('empire(0).credits'),h.run('state.credits'));assert.equal(h.run('JSON.stringify(state.p.stock)'),h.run('state.before'));assert.equal(h.run('stxLPRates(state.p).components'),0);assert.equal(h.run('stxLPRates(state.p).counts.war'),2);
});
test('labor scales all factory outputs and zero infrastructure cannot produce hulls',()=>{
 const h=lab();const high=h.run('stxLPRates(state.p).vessels');h.run('state.p.pop=.005');assert.ok(h.run('stxLPRates(state.p).vessels')<high*.2);h.run('state.p.infra.factory=0;stxLPEnsure(state.p)');assert.equal(h.run('stxLPRates(state.p).vessels'),0);
});
test('one minute of unordered life grows industry and reserves without spending or mobilizing',()=>{
 const h=lab();h.run(`state.p.infra.research=2;state.p.stock.trained=0;state.before=Object.fromEntries(Object.keys(state.p.stock).map(r=>[r,empireResource(0,r)]));state.credits=empire(0).credits;state.reserve=state.p.reserveVessels;for(let i=0;i<60;i++)simulate(1)`);
 for(const [r,v] of Object.entries(json(h,'state.before')))assert.ok(h.run(`empireResource(0,'${r}')`)>=v-1e-8,r);
 assert.ok(h.run('empire(0).credits')>=h.run('state.credits'));assert.ok(h.run('state.p.reserveVessels')>h.run('state.reserve'));assert.ok(h.run('state.p.stock.trained')>0);assert.equal(h.run('state.fleets.filter(f=>f.owner===0).length'),0);assert.equal(h.run('playerWorlds().flatMap(p=>stxSDDescriptors(p)).length'),0);
});
test('mobilization consumes exactly selected local hulls and personnel without money',()=>{
 const h=lab();h.run('state.credits=empire(0).credits;state.f=stxLPMobilize(state.p,20,"fleet","Cerberus")');assert.ok(h.run('state.f'));near(h.run('state.p.reserveVessels'),40);near(h.run('state.p.stock.trained'),.046);near(h.run('empire(0).credits'),h.run('state.credits'));assert.equal(h.run('state.f.location'),h.run('state.p.id'));assert.equal(h.run('state.f.vesselCount'),20);assert.equal(h.run('state.f.mobilizationSources[0].planetId'),h.run('state.p.id'));assert.equal(h.run('state.f.name'),'Cerberus');
});
test('insufficient crew, invalid counts and missing shipyards leave reserves untouched',()=>{
 const h=lab();h.run('state.p.stock.trained=0;state.before=JSON.stringify([state.p.reserveVessels,state.p.stock,empire(0).credits])');for(const n of ['20','-1','0','1.5','NaN','Infinity'])assert.equal(h.run(`stxLPMobilize(state.p,${n})`),false);assert.equal(h.run('JSON.stringify([state.p.reserveVessels,state.p.stock,empire(0).credits])'),h.run('state.before'));assert.equal(h.run('state.fleets.length'),0);
});
test('emergency commission spends stated costs once and adds reserve hulls only',()=>{
 const h=lab();h.run('state.before={credits:empire(0).credits,c:state.p.stock.components,e:state.p.stock.equipment,v:state.p.reserveVessels}');assert.equal(h.run('stxLPRush(state.p)'),true);near(h.run('empire(0).credits'),h.run('state.before.credits-50'));near(h.run('state.p.stock.components'),h.run('state.before.c-30'));near(h.run('state.p.stock.equipment'),h.run('state.before.e-20'));near(h.run('state.p.reserveVessels'),h.run('state.before.v+10'));assert.equal(h.run('state.fleets.length'),0);
});
function destination(h){h.run(`state.dest=state.planets.find(p=>p.owner===null);state.dest.owner=0;state.dest.x=state.p.x+300;state.dest.y=state.p.y;state.dest.infra.shipyard=1;stxLPEnsure(state.dest);state.dest.reserveVessels=0;state.dest.stock.trained=0;`)}
test('regional reserves travel physically and cannot be commissioned before arrival',()=>{
 const h=lab();destination(h);assert.equal(h.run('stxLPTransfer(state.p,state.dest,15)'),true);assert.equal(h.run('stxLPMobilize(state.dest,15)'),false);near(h.run('state.p.reserveVessels'),45);near(h.run('state.dest.reserveVessels'),0);h.run('state.s=state.ships[0];tickShips(100)');near(h.run('state.dest.reserveVessels'),15);near(h.run('state.dest.stock.trained'),.003);assert.ok(h.run('stxLPMobilize(state.dest,15)'));h.run('stxActionArrive(state.s,()=>arriveShip(state.s,state.dest))');near(h.run('state.dest.reserveVessels'),0);
});
test('failed dispatch preserves reserves and out-of-range worlds cannot stage',()=>{
 const h=lab();destination(h);h.run('state.originalCreate=createShip;createShip=()=>null');assert.equal(h.run('stxLPTransfer(state.p,state.dest,15)'),false);near(h.run('state.p.reserveVessels'),60);near(h.run('state.p.stock.trained'),.05);h.run('createShip=state.originalCreate;state.dest.x=state.p.x+901');assert.equal(h.run('stxLPTransfer(state.p,state.dest,15)'),false);
});
test('captured staging destination returns military cargo without granting it to the captor',()=>{
 const h=lab();destination(h);h.run('stxLPTransfer(state.p,state.dest,15);state.dest.owner=1;tickShips(100)');near(h.run('state.dest.reserveVessels'),0);h.run('tickShips(100)');near(h.run('state.p.reserveVessels'),60);near(h.run('state.p.stock.trained'),.05);
});
test('save round trip preserves parallel progress, modes, reserves and fleet records',()=>{
 const h=lab();h.run(`startLocalProject(state.p,'city');startLocalProject(state.p,'city');stxSDAllocatePlanet(state.p);tickLocalProject(state.p,20);stxLPMobilize(state.p,6);state.id=state.p.id;state.before=JSON.stringify({pop:state.p.pop,cap:state.p.capacity,modes:state.p.factoryModes,vessels:state.p.reserveVessels,crew:state.p.stock.trained,projects:state.p.stxLocalProjects.map(q=>q.progress),fleet:{id:state.fleets[0].id,name:state.fleets[0].name,strength:state.fleets[0].strength,location:state.fleets[0].location,vesselCount:state.fleets[0].vesselCount,personnel:state.fleets[0].personnel,sources:state.fleets[0].mobilizationSources}});saveGame(false);loadGame();state.p=state.planets.find(p=>p.id===state.id)`);
 assert.equal(h.run('JSON.stringify({pop:state.p.pop,cap:state.p.capacity,modes:state.p.factoryModes,vessels:state.p.reserveVessels,crew:state.p.stock.trained,projects:state.p.stxLocalProjects.map(q=>q.progress),fleet:{id:state.fleets[0].id,name:state.fleets[0].name,strength:state.fleets[0].strength,location:state.fleets[0].location,vesselCount:state.fleets[0].vesselCount,personnel:state.fleets[0].personnel,sources:state.fleets[0].mobilizationSources}})'),h.run('state.before'));assert.equal(h.run('state.p.localProject===state.p.stxLocalProjects[0]'),true);assert.equal(h.run('stxSDDescriptors(state.p).filter(d=>d.kind==="local").length'),2);
});
test('legacy migration is idempotent and keeps mature population and existing capacity',()=>{
 const h=lab();h.run('delete state.p.stxLivingVersion;delete state.p.factoryModes;delete state.p.reserveVessels;state.p.pop=5;state.p.capacity=7;state.p.infra.city=12;state.p.localProject={type:"city",progress:.4,cost:36};stxLPEnsure(state.p);state.before=JSON.stringify(state.p);stxLPEnsure(state.p)');assert.equal(h.run('JSON.stringify(state.p)'),h.run('state.before'));near(h.run('state.p.pop'),5);near(h.run('state.p.capacity'),7);assert.equal(h.run('state.p.infra.city'),12);assert.equal(h.run('state.p.stxLocalProjects.length'),1);
});
test('AI worlds use population factories and reserve-limited commissioning',()=>{
 const h=lab();h.run('state.ai=owned(1)[0];state.ai.infra.factory=2;state.ai.infra.shipyard=1;stxLPEnsure(state.ai);state.ai.factoryModes=["war","components"];state.ai.reserveVessels=0;state.ai.stock.trained=.02;state.ai.pop=.4;state.ai.capacity=1;state.aiPop=state.ai.pop');assert.equal(h.run('stxQueueFleetCommission(state.ai)'),false);h.run('tickPlanet(state.ai,10)');assert.ok(h.run('state.ai.reserveVessels')>0);assert.ok(h.run('state.ai.pop')>h.run('state.aiPop'));h.run('state.ai.reserveVessels=15');assert.equal(h.run('stxQueueFleetCommission(state.ai)'),true);near(h.run('state.ai.reserveVessels'),0);
});
test('transmissions expose authoritative totals and domestic requests cannot queue',()=>{
 const h=lab();h.run('renderTransmissions()');assert.match(h.elements.get('stxLivingResources').innerHTML,/Credits 10.0k/);assert.match(h.elements.get('stxLivingResources').innerHTML,/Uranium/);h.run(`state.proposals=[{id:'city',kind:'governor',project:'city',planetId:state.p.id,status:'pending',cost:20}];state.militaryRequests=[{id:'fleet',type:'fleet',planetId:state.p.id,status:'pending'}]`);assert.equal(h.run('stxTXEnqueue("proposal","city")'),false);assert.equal(h.run('stxTXEnqueue("military","fleet")'),false);assert.doesNotMatch(h.run('stxTXActionMarkup("proposal",state.proposals[0])'),/data-tx-action="queue"/);
});
