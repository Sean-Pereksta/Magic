import assert from 'node:assert/strict';
import test from 'node:test';
import {createSimulation} from './simulation-harness.mjs';

function lab(seed=937145){const h=createSimulation({seed});h.run(`
  state.ships=[];state.fleets=[];state.wars=[];state.battles=[];state.deepSpaceBases=[];state.deepSpaceBattles=[];state.deepSpaceOperations=[];state.contracts=[];state.proposals=[];
  for(const e of state.empires){e.credits=5000;e.stxPriorityProjectId=null;e.stxNextEconomyPlanAt=1e9;e.invasionPlans=[];}
  for(const p of state.planets){p.localProject=p.orbitalProject=p.expansionProject=p.reconstruction=p.scanProject=p.tradeStationProject=null;p.buildQueue=[];p.orders=[];p.physicalProjects=[];p.underAttack=false;p.stxFreightCapacity=20;for(const r of STX_IF_RESOURCES)p.stock[r]=r==='trained'?1:1000;}
  state.p=owned(0)[0];state.target=state.planets.find(p=>p.owner===null);state.p.pop=1;state.p.capacity=3;state.p.infra.shipyard=2;stxIFReset();stxFCACommitted.clear();stxDSPersist();
`);return h}
function value(h,s){return JSON.parse(h.run(`JSON.stringify(${s})`))}
function approx(a,b){assert.ok(Math.abs(a-b)<1e-8,`${a} != ${b}`)}
function colony(h){assert.equal(h.run(`startExpansionProject(empire(0),state.p,state.target,true)`),true);h.run(`state.q=state.p.expansionProject;state.q.volunteers=state.q.goal;stxSDAllocatePlanet(state.p);state.d=stxIFExpansionDescriptor(state.p)`)}
function build(h,type='fleet'){h.run(`state.q={type:'${type}',progress:0,need:{components:10}};state.p.buildQueue=[state.q];stxSDAllocatePlanet(state.p);state.q.progress=1`)}
function fleets(h,n=3){h.run(`state.destination=owned(0).find(p=>p!==state.p);for(let i=0;i<${n};i++)registerFleet(0,state.p,20,'fleet');state.dest={key:'planet:'+state.destination.id,type:'planet',id:state.destination.id,name:state.destination.name,planet:state.destination}`)}

test('first four colony missions launch in the completion update and arrive once',()=>{
  const h=lab();for(let i=0;i<4;i++){h.run(`state.target=state.planets.find(p=>p.owner===null);state.target.x=state.p.x+100;state.target.y=state.p.y`);colony(h);h.run('tickExpansionProject(state.p,0)');assert.equal(h.run('state.p.expansionProject'),null);assert.equal(h.run('state.ships.filter(s=>s.type==="colony").length'),1);assert.equal(h.run('state.q.stxAction.state'),'COMPLETED');h.run('tickShips(60)');assert.equal(h.run('state.target.owner'),0);assert.equal(h.run('state.ships.filter(s=>s.type==="colony").length'),0)}
});
test('failed colony launch retains population and goods through save/load, then retries once',()=>{
  const h=lab();colony(h);const before=value(h,'({pop:state.p.pop,stock:state.p.stock,delivered:state.q.stxSupply.delivered})');h.run('state.originalCreate=createShip;createShip=()=>null;tickExpansionProject(state.p,0)');assert.deepEqual(value(h,'({pop:state.p.pop,stock:state.p.stock,delivered:state.q.stxSupply.delivered})'),before);assert.equal(h.run('state.q.stxAction.state'),'RETRYING');h.run('saveGame(false);loadGame();state.p=owned(0)[0];state.q=state.p.expansionProject;state.target=state.planets.find(p=>p.id===state.q.targetId);createShip=state.originalCreate;tickExpansionProject(state.p,0);stxActionFinalizeColony(state.p,state.q,stxIFExpansionDescriptor({...state.p,expansionProject:state.q}),state.target)');assert.equal(h.run('state.ships.filter(s=>s.type==="colony").length'),1);assert.equal(h.run('state.p.expansionProject'),null);
});
test('colony readiness includes Equipment and every recipe input',()=>{
  const h=lab();colony(h);h.run('state.q.stxSupply.delivered.equipment=0;state.p.stock.equipment=0;tickExpansionProject(state.p,0);stxActionProgress(state.d)');assert.equal(h.run('stxSDProgress(state.d)'),0);assert.equal(h.run('state.ships.length'),0);assert.match(h.run('state.q.stxAction.blocker'),/Equipment/i);
});
test('ship launch failure preserves the queue and successful retries create one vessel',()=>{
  const h=lab();build(h);h.run('state.originalLaunch=launchBuiltShip;launchBuiltShip=()=>null;tickBuildQueue(state.p,0)');assert.equal(h.run('state.p.buildQueue.length'),1);assert.equal(h.run('state.q.stxAction.state'),'RETRYING');h.run('launchBuiltShip=state.originalLaunch;stxActionFinalizeShip(state.p,state.q);stxActionFinalizeShip(state.p,state.q)');assert.equal(h.run('state.p.buildQueue.length'),0);assert.equal(h.run('state.fleets.length'),1);
});
test('combat construction commissions a new fleet instead of reusing an existing fleet',()=>{
  const h=lab();h.run('state.oldFleet=registerFleet(0,state.p,70,"fleet");chooseShipTarget=()=>owned(0).find(p=>p!==state.p)');build(h);h.run('tickBuildQueue(state.p,0)');assert.equal(h.run('state.fleets.length'),2);assert.equal(h.run('state.oldFleet.strength'),70);assert.notEqual(h.run('state.ships[0].fleetId'),h.run('state.oldFleet.id'));
});
test('unassigned patrols and civilian ships remain physically stationed',()=>{
  const h=lab();h.run('chooseShipTarget=()=>null');build(h,'patrol');h.run('tickBuildQueue(state.p,0)');assert.equal(h.run('state.fleets[0].location'),h.run('state.p.id'));build(h,'freighter');h.run('tickBuildQueue(state.p,0);tickShips(100)');assert.equal(h.run('state.ships.length'),1);assert.equal(h.run('state.ships[0].stxStationed'),true);assert.equal(h.run('state.ships[0].progress'),0);
});
test('arrival exceptions roll back effects and can be acknowledged only once',()=>{
  const h=lab();h.run('state.target.owner=0;state.ship=createShip("supply",state.p,state.target,0,{cargo:{iron:5}});state.ship.progress=1;state.apply=()=>{state.target.stock.iron+=5;empire(0).credits+=10;throw Error("injected arrival failure")};state.beforeCredits=empire(0).credits;stxActionArrive(state.ship,state.apply)');assert.equal(h.run('state.target.stock.iron'),1000);approx(h.run('empire(0).credits'),h.run('state.beforeCredits'));h.run('state.simTime++;state.apply=()=>{state.target.stock.iron+=5};stxActionArrive(state.ship,state.apply);stxActionArrive(state.ship,state.apply)');assert.equal(h.run('state.target.stock.iron'),1005);
});
test('99.9 percent deep-space missions are not delivered to their sponsor planet',()=>{
  const h=lab();h.run('state.ship=createShip("supply",state.p,state.target,0,{cargo:{iron:5},stxDeepTransit:true});state.ship.progress=.999;stxMRFinishReadyShips()');assert.equal(h.run('state.ships.length'),1);assert.equal(h.run('state.target.stock.iron'),1000);
});
test('failed second barter dispatch rolls back stock, fuel, credits and all ships',()=>{
  const h=lab(),before=value(h,'({stocks:state.planets.map(p=>p.stock),credits:state.empires.map(e=>e.credits),ships:state.ships})');h.run('state.originalDispatch=stxRTDispatchCargo;state.calls=0;stxRTDispatchCargo=(...args)=>{if(++state.calls===2)throw Error("injected second leg failure");return state.originalDispatch(...args)}');assert.equal(h.run('stxRTExchange(1,0,"iron",40,{resource:"components",amount:20})'),false);assert.deepEqual(value(h,'({stocks:state.planets.map(p=>p.stock),credits:state.empires.map(e=>e.credits),ships:state.ships})'),before);
});
test('aid larger than one old cargo chunk dispatches its full amount once',()=>{
  const h=lab();h.run('state.q={id:"aid",from:1,to:0,kind:"resource",actionKind:"resource",resource:"iron",amount:90,status:"pending",title:"Aid",expiresAt:100};state.rivalDiplomacy.requests.push(state.q)');assert.equal(h.run('stxRDRespondRequest("aid","accept")'),true);approx(h.run('state.ships.filter(s=>s.requestId==="aid").reduce((n,s)=>n+s.cargo.iron,0)'),90);assert.equal(h.run('stxRDRespondRequest("aid","accept")'),false);
});
test('request validation reports insufficient stock, freight, embargo and expiry',()=>{
  const h=lab();h.run('state.q={id:"aid",from:1,to:0,actionKind:"resource",resource:"iron",amount:40,status:"pending",expiresAt:100};for(const p of owned(0))p.stock.iron=0;state.p.stock.iron=23');assert.match(h.run('stxActionRequestCheck(state.q)'),/required/);h.run('state.p.stock.iron=1000;stxIFFreightLimit=()=>0');assert.match(h.run('stxActionRequestCheck(state.q)'),/freight/);h.run('stxIFFreightLimit=()=>100;stxDSAddAgreement("embargo",1,0,100)');assert.match(h.run('stxActionRequestCheck(state.q)'),/embargo/);h.run('state.q.expiresAt=0');assert.match(h.run('stxActionRequestCheck(state.q)'),/expired/);
});
test('trade offer acceptance dispatches once and rejects a repeated click',()=>{
  const h=lab();h.run('state.req={id:"request",status:"open",resource:"iron",desired:40,accepted:0,offerIds:["offer"],contacts:[],expiresAt:100};state.offer={id:"offer",requestId:"request",from:1,resource:"iron",amount:40,payment:{credits:10},status:"pending",expiresAt:100};state.resourceTradeEconomy.requests.push(state.req);state.resourceTradeEconomy.offers.push(state.offer)');assert.equal(h.run('stxRTAcceptOffer("offer")'),true);const n=h.run('state.ships.length');assert.ok(n>0);assert.equal(h.run('stxRTAcceptOffer("offer")'),false);assert.equal(h.run('state.ships.length'),n);
});
test('governor approval cannot charge credits if the project fails to start',()=>{
  const h=lab();h.run('state.q={id:"gov",kind:"governor",planetId:state.p.id,project:"factory",cost:20,status:"pending",expiresAt:100};state.proposals.push(state.q);startLocalProject=()=>false');assert.equal(h.run('respondProposal("gov","approve")'),false);assert.equal(h.run('empire(0).credits'),5000);assert.equal(h.run('state.q.status'),'pending');assert.equal(h.run('state.p.localProject'),null);
});
test('empty worlds recover slowly without creating personnel or special resources',()=>{
  const h=lab();h.run('for(const k of Object.keys(state.p.infra))state.p.infra[k]=0;for(const r of Object.keys(state.p.stock))state.p.stock[r]=0;state.p.stock.uranium=0;tickPlanet(state.p,1)');for(const r of ['iron','silicates','titanium','helium','rare','components','equipment']){const n=h.run(`state.p.stock.${r}`);assert.ok(n>0&&n<.02,`${r}: ${n}`)}assert.equal(h.run('state.p.stock.trained'),0);assert.equal(h.run('state.p.stock.uranium'),0);
});
test('local recovery scales down with empire size and real mines dominate it',()=>{
  const h=lab(),small=h.run('stxActionLocalRate(state.p,"iron")');h.run('state.planets.filter(p=>p.owner===null).slice(0,20).forEach(p=>p.owner=0)');assert.ok(h.run('stxActionLocalRate(state.p,"iron")')<small/2);h.run('state.p.infra.mine=5;state.p.stock.iron=0;state.p.quality.iron=5;state.p.reserve.iron=100000;state.p.stxResourceYield.iron=1;tickPlanet(state.p,1)');assert.ok(h.run('state.p.stock.iron')>small*10);
});
test('concentrate assigns exactly ten fleets immediately and ETA decreases',()=>{
  const h=lab();fleets(h,10);assert.equal(h.run('stxFCAExecuteMove("concentrate",state.dest,10)'),10);assert.equal(h.run('state.ships.filter(s=>s.fleetId).length'),10);assert.equal(h.run('state.fleets.filter(f=>f.location===null).length'),10);const eta=h.run('shipEta(state.ships[0])');h.run('tickShips(.1)');assert.ok(h.run('shipEta(state.ships[0])')<eta);
});
test('a failed fleet in a batch restores every assignment and rejects oversized orders',()=>{
  const h=lab();fleets(h);const before=value(h,'state.fleets');h.run('state.originalCreate=createShip;state.calls=0;createShip=(...args)=>++state.calls===2?null:state.originalCreate(...args)');assert.equal(h.run('stxFCAExecuteMove("concentrate",state.dest,3)'),0);assert.deepEqual(value(h,'state.fleets'),before);assert.equal(h.run('state.ships.length'),0);assert.equal(h.run('stxFCACommitted.size'),0);assert.equal(h.run('stxFCAExecuteMove("concentrate",state.dest,4)'),0);
});
test('redirect starts at the current coordinates including a zero x coordinate',()=>{
  const h=lab();fleets(h,1);h.run('stxFCAExecuteMove("concentrate",state.dest,1);state.ship=state.ships[0];state.ship.x=0;state.ship.y=120;state.ship.progress=.8;stxFCARedirectTransit(state.fleets[0],state.ship,{...state.dest,planet:state.p},"concentrate")');assert.equal(h.run('state.ship.startX'),0);assert.equal(h.run('state.ship.startY'),120);assert.equal(h.run('state.ship.progress'),0);assert.equal(h.run('state.ship.stxAction.state'),'ACTIVE');
});
test('neutral encounter creates a peaceful standoff and withdrawal lowers tension',()=>{
  const h=lab();h.run('state.enemy=owned(1)[0];state.ship=createShip("fleet",state.p,state.enemy,0,{strength:20});state.ship.progress=1;tickShips(0)');assert.equal(h.run('state.wars.length'),0);assert.equal(h.run('state.battles.length'),0);assert.equal(h.run('state.ships.some(s=>s.stxPeacefulWithdrawal)'),true);const tension=h.run('stxRDPair(0,1).tension');assert.equal(h.run('stxConflictRespondIncident(state.rivalDiplomacy.incidents.find(q=>q.kind==="standoff").id,"withdraw")'),true);assert.ok(h.run('stxRDPair(0,1).tension')<tension);
});
test('wartime arrival joins battle immediately and a defeated side resolves',()=>{
  const h=lab();h.run('state.enemy=owned(1)[0];declareWar(0,1,"a direct imperial declaration");state.ship=createShip("fleet",state.p,state.enemy,0,{strength:60});state.ship.progress=1;tickShips(0);state.battle=activeBattleAt(state.enemy)');assert.ok(h.run('!!state.battle'));const strength=h.run('state.battle.attackerStrength');h.run('state.reinforcement=createShip("fleet",state.p,state.enemy,0,{strength:15,battleId:state.battle.id});state.reinforcement.progress=1;tickShips(0)');assert.ok(h.run('state.battle.attackerStrength')>strength);h.run('state.battle.attackerStrength=0;resolveBattle(state.battle,state.enemy,state.battles.indexOf(state.battle))');assert.equal(h.run('state.battles.includes(state.battle)'),false);
});
test('normal early wars and low-tension station attack declarations are blocked',()=>{
  const h=lab();h.run('state.simTime=45');assert.equal(h.run('declareWar(1,2,"station attack")'),null);h.run('state.simTime=500;stxRDPair(1,2).tension=5');assert.equal(h.run('declareWar(1,2,"station raid")'),null);assert.equal(h.run('state.wars.length'),0);
});
test('AI declarations require warning, preparation and a strategic grievance',()=>{
  const h=lab();h.run('state.simTime=500;state.pair=stxRDPair(1,2);state.pair.tension=100;empire(1).relations[2]=empire(2).relations[1]=-.8;stxRDThreatSupport=()=>1');assert.equal(h.run('stxConflictCanDeclare(1,2,"station attack")'),false);h.run('stxRDAddGrievance(1,2,"frontier dispute","Border dispute",3);state.pair.stxCrisisWarnedAt=470;state.pair.stxMobilizedAt=470');assert.equal(h.run('!!declareWar(1,2,"station attack")'),true);
});
test('peace cooldown persists through save/load and prevents immediate redeclaration',()=>{
  const h=lab();h.run('state.war=declareWar(0,1,"a direct imperial declaration");endWar(state.war,"treaty");saveGame(false);loadGame()');assert.ok(h.run('stxRDPair(0,1).warCooldownUntil-state.simTime')>=180);assert.equal(h.run('declareWar(0,1,"a direct imperial declaration")'),null);
});
test('embargo pressure raises tension, commerce relieves it, and personality matters',()=>{
  const h=lab();h.run('state.simTime=500;state.pair=stxRDPair(0,1);state.pair.tension=30;state.pair.stxPressureAt=490;stxDSAddAgreement("embargo",0,1,100);stxConflictTick()');assert.ok(h.run('state.pair.tension')>30);h.run('state.rivalDiplomacy.agreements=[];stxDSAddAgreement("trade-agreement",0,1,100);state.pair.tension=30;state.pair.stxPressureAt=490;stxConflictTick()');assert.ok(h.run('state.pair.tension')<30);h.run('empire(1).foreignPolicy.aggression=0;state.pair.tension=0;stxRDAddGrievance(1,0,"test-a","test",2);state.calm=state.pair.tension;empire(1).foreignPolicy.aggression=1;state.pair.tension=0;stxRDAddGrievance(1,0,"test-b","test",2)');assert.ok(h.run('state.pair.tension')>h.run('state.calm'));
});
test('watchdog reopens orphan freight and reroutes invalid destinations from current position',()=>{
  const h=lab();h.run('state.p.orders.push({id:"orphan",resource:"iron",amount:10,status:"in transit"});state.ship=createShip("supply",state.p,state.target,0,{cargo:{iron:4}});state.ship.to="missing";state.ship.x=250;state.ship.y=400;stxActionWatchdog()');assert.equal(h.run('state.p.orders.find(o=>o.id==="orphan").status'),'waiting');assert.equal(h.run('state.ship.startX'),250);assert.equal(h.run('state.ship.startY'),400);assert.equal(h.run('state.ship.stxIFReturnCargo'),true);
});
test('three deterministic opening galaxies reach one minute without routine wars',()=>{
  for(const seed of [137,937145,82024]){const h=createSimulation({seed});h.run('for(let i=0;i<61;i++)simulate(1)');assert.equal(h.run('state.wars.filter(w=>w.active).length'),0,`seed ${seed}`);assert.equal(h.run('state.battles.length'),0)}
});
test('battle card exposes current strength and named reinforcement ETA',()=>{
  const h=lab();h.run('state.enemy=owned(1)[0];state.battle={planetId:state.enemy.id,attacker:0,defender:1,attackerStrength:12.4,defenderStrength:8.2,attackerInitial:20,defenderInitial:20};state.ship=createShip("fleet",state.p,state.enemy,0,{strength:5});state.card=stxActionBattleCard(state.battle)');assert.match(h.run('state.card'),/<meter/);assert.match(h.run('state.card'),/12.4 strength/);assert.ok(h.run('state.card.includes(state.ship.vesselName+" · reinforcing · ETA")'));
});
test('peace closes a live battle immediately without granting conquest',()=>{
  const h=lab();h.run('state.enemy=owned(1)[0];state.war=declareWar(0,1,"a direct imperial declaration");state.ship=createShip("fleet",state.p,state.enemy,0,{strength:60});state.ship.progress=1;tickShips(0);endWar(state.war,"treaty")');assert.equal(h.run('state.enemy.owner'),1);assert.equal(h.run('state.enemy.underAttack'),false);assert.equal(h.run('state.battles.length'),0);
});
test('physical project completion is idempotent',()=>{
  const h=lab();h.run('state.q={id:"factory-test",kind:"factory",name:"Factory",option:{},phase:"construction",progress:1,activity:[]};state.beforeFactory=state.p.infra.factory;stxOLCompleteProject(state.p,state.q);stxOLCompleteProject(state.p,state.q)');assert.equal(h.run('state.p.infra.factory'),h.run('state.beforeFactory+1'));assert.equal(h.run('state.q.stxAction.state'),'COMPLETED');
});
